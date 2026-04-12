(** Lighter WebSocket client.
    Manages dual persistent connections (public and private) to access 
    market data and authenticated endpoints while avoiding Durable Object message limits.
    
    Public and private connections have independent lifecycles — a failure
    on one side reconnects only that side, without tearing down the other.
    This avoids the full reconnect+resubscribe+order-rebuild churn that
    previously occurred ~30 times/hour when either side flapped. *)

open Lwt.Infix

let section = "lighter_ws"

(** Remembered subscription parameters for autonomous reconnect loops.
    Set once by [subscribe_to_feeds], used by per-side reconnect callbacks
    so they can resubscribe without supervisor involvement. *)
let subscribed_symbols : string list ref = ref []
let subscribed_account_index : int ref = ref 0
let subscribed_auth_token : string ref = ref ""

(** Handle returned to consumers of [subscribe_market_data]. *)
type subscription = {
  stream: Yojson.Safe.t Lwt_stream.t;
  close: unit -> unit;
}

type connection_state = {
  active_connection : Websocket_lwt_unix.conn option ref;
  connection_mutex : Lwt_mutex.t;
  is_connected_ref : bool Atomic.t;
  connected_wakeup : unit Lwt_condition.t;
  last_pong_time : float ref;
  pong_condition : unit Lwt_condition.t;
}

let create_connection_state () = {
  active_connection = ref None;
  connection_mutex = Lwt_mutex.create ();
  is_connected_ref = Atomic.make false;
  connected_wakeup = Lwt_condition.create ();
  last_pong_time = ref 0.0;
  pong_condition = Lwt_condition.create ();
}

let public_state = create_connection_state ()
let private_state = create_connection_state ()

let public_connected () = Atomic.get public_state.is_connected_ref
let private_connected () = Atomic.get private_state.is_connected_ref
let is_connected () = public_connected () && private_connected ()

let wait_for_connected () =
  let wait_one state =
    if Atomic.get state.is_connected_ref then Lwt.return_unit
    else Lwt_condition.wait state.connected_wakeup
  in
  Lwt.join [wait_one public_state; wait_one private_state]

let signal_new_data () = Concurrency.Exchange_wakeup.signal_all ()

(** Global list of subscriber push functions. *)
let pushers : (Yojson.Safe.t option -> bool) list ref = ref []
let pushers_mutex = Mutex.create ()

(** Counter of consecutive ping failures, read by the supervisor. *)
let ping_failures = Atomic.make 0
let reset_ping_failures () = Atomic.set ping_failures 0
let get_ping_failures () = Atomic.get ping_failures
let incr_ping_failures () = Atomic.incr ping_failures

(** Guards pinging the private side until we've received real data
    confirming the DO proxy's upstream relay is working. *)
let private_stream_confirmed = Atomic.make false

(** Diagnostic counters for message types flowing through the WS. *)

let msg_counter_orderbook = Atomic.make 0
let msg_counter_account = Atomic.make 0
let msg_counter_other = Atomic.make 0
let msg_counter_total = Atomic.make 0

(** Close all subscriber streams on disconnect. *)
let close_all_subscribers () =
  Mutex.lock pushers_mutex;
  let ps = !pushers in
  pushers := [];
  Mutex.unlock pushers_mutex;
  let count = List.length ps in
  if count > 0 then begin
    Logging.debug_f ~section "Closing %d subscriber streams on disconnect" count;
    List.iter (fun push ->
      (try ignore (push None) with _ -> ())
    ) ps
  end

(** Broadcast a JSON message to all registered subscribers. *)
let broadcast_message json =
  Mutex.lock pushers_mutex;
  let ps = !pushers in
  Mutex.unlock pushers_mutex;
  let alive = ref [] in
  List.iter (fun push ->
    try
      if push (Some json) then alive := push :: !alive
    with _ -> ()
  ) ps;
  if List.length !alive <> List.length ps then begin
    let dead = List.filter (fun p -> not (List.memq p !alive)) ps in
    Mutex.lock pushers_mutex;
    pushers := List.filter (fun p -> not (List.memq p dead)) !pushers;
    Mutex.unlock pushers_mutex
  end

(** Creates a bounded subscriber stream for incoming messages. *)
let subscribe_market_data () =
  let (stream, push_source) = Lwt_stream.create_bounded 16 in
  let closed = Atomic.make false in
  let close_internal () =
    if not (Atomic.exchange closed true) then begin
      push_source#close;
      true
    end else
      false
  in
  let push_fn item =
    if Atomic.get closed then
      false
    else
    match item with
    | None ->
        close_internal ()
    | Some json ->
        let p = push_source#push json in
        if Lwt.is_sleeping p then begin
          ignore (close_internal ());
          false
        end else
          true
  in
  Mutex.lock pushers_mutex;
  pushers := push_fn :: !pushers;
  Mutex.unlock pushers_mutex;
  let close () =
    Mutex.lock pushers_mutex;
    pushers := List.filter (fun p -> p != push_fn) !pushers;
    Mutex.unlock pushers_mutex;
    ignore (close_internal ())
  in
  { stream; close }

(** Sends a JSON message over the active WebSocket connection. *)
let send_json_on state json label =
  Lwt_mutex.with_lock state.connection_mutex (fun () ->
    match !(state.active_connection) with
    | Some conn ->
        let msg = Yojson.Safe.to_string json in
        Logging.debug_f ~section "[%s] Sending WS message: %s" label
          (if String.length msg > 500 then String.sub msg 0 500 ^ "..." else msg);
        Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg ())
    | None ->
        Logging.warn_f ~section "[%s] Cannot send: WebSocket not connected" label;
        Lwt.return_unit
  )

let send_public_json = send_json_on public_state
let send_private_json = send_json_on private_state

let subscribe_public_orderbook ~symbols =
  let public_stream = Lwt_stream.of_list symbols in
  Concurrency.Lwt_util.consume_stream_s (fun symbol ->
    match Lighter_instruments_feed.get_market_index ~symbol with
    | Some market_index ->
        let mi_str = string_of_int market_index in
        let%lwt () = send_public_json (`Assoc [
          ("type", `String "subscribe");
          ("channel", `String ("order_book/" ^ mi_str))
        ]) "Public" in
        Logging.debug_f ~section "Subscribed to order_book/%s for %s" mi_str symbol;
        Lwt.return_unit
    | None ->
        Logging.error_f ~section "Cannot subscribe: no market_index for symbol %s" symbol;
        Lwt.return_unit
  ) public_stream


(** Subscribe to feeds routed properly between public and private dual connections.
    Also remembers subscription parameters so per-side reconnect loops can
    resubscribe autonomously without supervisor involvement. *)
let subscribe_to_feeds ~symbols ~account_index ~auth_token =
  (* Remember subscription parameters for autonomous reconnects *)
  subscribed_symbols := symbols;
  subscribed_account_index := account_index;
  subscribed_auth_token := auth_token;

  (* Subscribe to ticker and orderbook for each symbol on the public connection *)
  let%lwt () = subscribe_public_orderbook ~symbols in

  (* Subscribe to authenticated account channels on the private connection *)
  let acct_str = string_of_int account_index in
  let private_commands = [
    `Assoc [("type", `String "subscribe"); ("channel", `String ("account_all_orders/" ^ acct_str)); ("auth", `String auth_token)];
    `Assoc [("type", `String "subscribe"); ("channel", `String ("account_all/" ^ acct_str)); ("auth", `String auth_token)];
    `Assoc [("type", `String "subscribe"); ("channel", `String ("account_all_assets/" ^ acct_str)); ("auth", `String auth_token)];
    `Assoc [("type", `String "subscribe"); ("channel", `String ("user_stats/" ^ acct_str)); ("auth", `String auth_token)]
  ] in
  let private_stream = Lwt_stream.of_list private_commands in
  let%lwt () = Concurrency.Lwt_util.consume_stream_s (fun cmd ->
    send_private_json cmd "Private"
  ) private_stream in
  
  Logging.debug_f ~section "Subscribed to private channels for %s" acct_str;
  Lwt.return_unit

(** Send a signed transaction over private WebSocket. *)
let send_tx_ws ~tx_type ~tx_info =
  let json = `Assoc [
    ("type", `String "jsonapi/sendtx");
    ("data", `Assoc [
      ("tx_type", `Int tx_type);
      ("tx_info", Yojson.Safe.from_string tx_info)
    ])
  ] in
  send_private_json json "Private"

(** Process a single WebSocket frame. *)
let handle_frame ~state ~on_heartbeat (frame : Websocket.Frame.t) =
  match frame.Websocket.Frame.opcode with
  | Websocket.Frame.Opcode.Text ->
      Concurrency.Tick_event_bus.publish_tick ();
      on_heartbeat ();
      (* Mark private stream as confirmed on first real data *)
      if state == private_state && not (Atomic.get private_stream_confirmed) then
        Atomic.set private_stream_confirmed true;
      (try
        let json = Yojson.Safe.from_string frame.Websocket.Frame.content in
        let msg_type =
          let open Yojson.Safe.Util in
          try member "type" json |> to_string with _ -> ""
        in
        let channel =
          let open Yojson.Safe.Util in
          try member "channel" json |> to_string with _ -> ""
        in

        let market_index_from_channel ch =
          try
            let sep_pos =
              try String.index ch ':'
              with Not_found -> String.index ch '/'
            in
            int_of_string (String.sub ch (sep_pos + 1) (String.length ch - sep_pos - 1))
          with _ -> -1
        in

        (match msg_type with

         | "snapshot/order_book" | "subscribed/order_book" ->
             Atomic.incr msg_counter_orderbook;
             let mi = market_index_from_channel channel in
             if mi >= 0 then Lighter_orderbook_feed.process_orderbook_snapshot ~market_index:mi json
         | "update/order_book" ->
             Atomic.incr msg_counter_orderbook;
             let mi = market_index_from_channel channel in
             if mi >= 0 then Lighter_orderbook_feed.process_orderbook_update ~market_index:mi json
         | "update/account_all_orders" | "snapshot/account_all_orders" | "subscribed/account_all_orders" ->
             Atomic.incr msg_counter_account;
             Lighter_executions_feed.process_account_orders_update json
         | "update/account_all" | "snapshot/account_all" | "subscribed/account_all"
         | "update/account_all_assets" | "snapshot/account_all_assets" | "subscribed/account_all_assets"
         | "update/user_stats" | "snapshot/user_stats" | "subscribed/user_stats" ->
             Atomic.incr msg_counter_account;
             broadcast_message json
         | "pong" ->
              state.last_pong_time := Unix.gettimeofday ();
              (try Lwt_condition.broadcast state.pong_condition () with _ -> ());
              on_heartbeat ()
         | t ->
             Atomic.incr msg_counter_other;
             if Atomic.get msg_counter_other <= 5 then
               Logging.debug_f ~section "Unmatched WS message type: '%s' channel='%s' (len=%d)" t channel
                 (String.length frame.Websocket.Frame.content));
        Atomic.incr msg_counter_total
      with exn ->
        Logging.warn_f ~section "Failed to parse WS message: %s (content_prefix=%s)"
          (Printexc.to_string exn)
          (let c = frame.Websocket.Frame.content in
           if String.length c > 100 then String.sub c 0 100 ^ "..." else c));
      Lwt.return_unit
  | Websocket.Frame.Opcode.Pong ->
      on_heartbeat ();
      Lwt.return_unit
  | Websocket.Frame.Opcode.Close ->
      Logging.debug_f ~section "WebSocket connection closed by server";
      Lwt_mutex.with_lock state.connection_mutex (fun () ->
        state.active_connection := None;
        Atomic.set state.is_connected_ref false;
        Lwt.return_unit
      ) >>= fun () ->
      (* Don't close_all_subscribers here — a single-side disconnect
         should not destroy consumer streams. The per-side reconnect
         loop will re-establish the connection. *)
      signal_new_data ();
      Lwt.return_unit
  | _ -> Lwt.return_unit

(** Establishes a single WebSocket connection and blocks while reading frames.
    Returns unit when the connection closes (for any reason).
    Does NOT call close_all_subscribers — single-side disconnects are handled
    by the per-side reconnect loop in [connect_and_monitor]. *)
let rec connect_one ~state ~connect_target ~ws_url ~on_failure ~on_connected ~on_heartbeat ~label =
  let (connect_host, connect_port) = connect_target () in
  let url = ws_url () in
  Logging.debug_f ~section "Connecting to [%s] Lighter WebSocket: %s (host=%s:%d)" label url connect_host connect_port;
  let uri = Uri.of_string url in
  Lwt.catch (fun () ->
    Lwt_unix.getaddrinfo connect_host (string_of_int connect_port) [Unix.AI_FAMILY Unix.PF_INET] >>= fun addresses ->
    let ip = match addresses with
      | {Unix.ai_addr = Unix.ADDR_INET (addr, _); _} :: _ ->
          Ipaddr_unix.of_inet_addr addr
      | _ -> failwith (Printf.sprintf "Failed to resolve %s" connect_host)
    in
    let client = `TLS (`Hostname connect_host, `IP ip, `Port connect_port) in
    let ctx = Lazy.force Conduit_lwt_unix.default_ctx in
    Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->
    Lwt_mutex.with_lock state.connection_mutex (fun () ->
      state.active_connection := Some conn;
      Atomic.set state.is_connected_ref true;
      (* Reset private stream confirmation — pings are suppressed
         until we receive real data confirming the relay is alive. *)
      if state == private_state then
        Atomic.set private_stream_confirmed false;
      Lwt_condition.broadcast state.connected_wakeup ();
      Lwt.return_unit
    ) >>= fun () ->
    on_connected ();

    let stream = Lwt_stream.from (fun () ->
      if not (Atomic.get state.is_connected_ref) then Lwt.return_none
      else Lwt.catch (fun () ->
        Websocket_lwt_unix.read conn >>= fun frame ->
        Lwt.return_some frame
      ) (function
        | End_of_file -> Lwt.return_none
        | exn -> Lwt.fail exn)
    ) in

    let done_p =
      Concurrency.Lwt_util.consume_stream_s
        (fun frame ->
          Lwt.catch
            (fun () -> handle_frame ~state ~on_heartbeat frame)
            (fun exn ->
              Logging.error_f ~section "[%s] Error handling frame: %s" label (Printexc.to_string exn);
              Lwt.return_unit))
        stream
    in

    Lwt.catch (fun () -> done_p) (function
      | End_of_file ->
          Logging.debug_f ~section "[%s] WebSocket connection closed (End_of_file)" label;
          Lwt_mutex.with_lock state.connection_mutex (fun () ->
            state.active_connection := None;
            Atomic.set state.is_connected_ref false;
            Lwt.return_unit
          ) >>= fun () ->
          Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit) >>= fun () ->
          Lwt.fail_with ("[" ^ label ^ "] Connection closed unexpectedly (End_of_file)")
      | exn ->
          Logging.debug_f ~section "[%s] WebSocket read error: %s" label (Printexc.to_string exn);
          Lwt_mutex.with_lock state.connection_mutex (fun () ->
            state.active_connection := None;
            Atomic.set state.is_connected_ref false;
            Lwt.return_unit
          ) >>= fun () ->
          Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit) >>= fun () ->
          Lwt.fail exn
    ) >>= fun () ->
    Lwt_mutex.with_lock state.connection_mutex (fun () ->
      state.active_connection := None;
      Atomic.set state.is_connected_ref false;
      Lwt.return_unit
    ) >>= fun () ->
    Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit) >>= fun () ->
    Lwt.fail_with ("[" ^ label ^ "] WebSocket closed by server")
  ) (fun exn ->
    let error_msg = Printexc.to_string exn in
    Logging.debug_f ~section "[%s] WebSocket connection error: %s" label error_msg;
    let is_server_error =
      let rec contains_5xx i =
        if i > String.length error_msg - 3 then false
        else
          let sub = String.sub error_msg i 3 in
          if sub = "500" || sub = "502" || sub = "503" || sub = "504" then true
          else contains_5xx (i + 1)
      in
      contains_5xx 0
    in
    if label = "Private" && is_server_error then begin
      Lighter_proxy.rotate_proxy ();
      if Lighter_proxy.has_more_proxies () then begin
        Atomic.set state.is_connected_ref false;
        connect_one ~state ~connect_target ~ws_url ~on_failure ~on_connected ~on_heartbeat ~label
      end else begin
        Atomic.set state.is_connected_ref false;
        on_failure error_msg;
        Lwt.return_unit
      end
    end else begin
      Atomic.set state.is_connected_ref false;
      on_failure error_msg;
      Lwt.return_unit
    end
  )

let close () : unit Lwt.t =
  let close_one state label =
    Lwt_mutex.with_lock state.connection_mutex (fun () ->
      match !(state.active_connection) with
      | None -> Lwt.return None
      | Some conn ->
          state.active_connection := None;
          Atomic.set state.is_connected_ref false;
          Lwt.return (Some conn)
    ) >>= function
    | None -> Lwt.return_unit
    | Some conn ->
        Logging.info_f ~section "Closing %s Lighter WebSocket connection" label;
        Lwt.catch
          (fun () -> Websocket_lwt_unix.close_transport conn)
          (fun _ -> Lwt.return_unit)
  in
  close_all_subscribers ();
  signal_new_data ();
  Lwt.join [
    close_one public_state "Public";
    close_one private_state "Private";
  ]

(** Close only the public WebSocket connection. *)
let close_public () : unit Lwt.t =
  Lwt_mutex.with_lock public_state.connection_mutex (fun () ->
    match !(public_state.active_connection) with
    | None -> Lwt.return None
    | Some conn ->
        public_state.active_connection := None;
        Atomic.set public_state.is_connected_ref false;
        Lwt.return (Some conn)
  ) >>= function
  | None -> Lwt.return_unit
  | Some conn ->
      Logging.info ~section "Closing Public Lighter WebSocket connection";
      Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit)

(** Close only the private WebSocket connection. *)
let close_private () : unit Lwt.t =
  Lwt_mutex.with_lock private_state.connection_mutex (fun () ->
    match !(private_state.active_connection) with
    | None -> Lwt.return None
    | Some conn ->
        private_state.active_connection := None;
        Atomic.set private_state.is_connected_ref false;
        Lwt.return (Some conn)
  ) >>= function
  | None -> Lwt.return_unit
  | Some conn ->
      Logging.info ~section "Closing Private Lighter WebSocket connection";
      Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit)

let connect_and_monitor ~on_failure:_on_failure ~on_connected ~on_heartbeat =
  let pub_ready = Atomic.make false in
  let priv_ready = Atomic.make false in
  let both_ready_fired = Atomic.make false in

  (* Fire on_connected when both sides are up. Idempotent within a session — 
     only fires once when transitioning from "not both ready" to "both ready". *)
  let check_both_ready () =
    if Atomic.get pub_ready && Atomic.get priv_ready then begin
      if not (Atomic.exchange both_ready_fired true) then begin
        reset_ping_failures ();
        Lighter_proxy.reset_proxy_failures ();
        on_connected ()
      end
    end
  in

  (* Self-healing reconnect loop for one side.
     Reconnects indefinitely on disconnect. Single-side flaps are handled
     internally without escalating to the supervisor. The supervisor's
     passive heartbeat monitoring catches genuine prolonged outages. *)
  let rec reconnect_loop ~state ~connect_target ~ws_url ~label
      ~ready_flag ~other_ready_flag ~on_side_reconnected =
    let side_on_failure msg =
      Atomic.set ready_flag false;
      Atomic.set both_ready_fired false;
      Logging.warn_f ~section "[%s] Disconnected: %s (reconnecting independently)" label msg;
      (* Don't escalate to supervisor's on_failure — the self-healing
         reconnect loop will recover. The supervisor's passive heartbeat
         monitoring (60s data silence → Failed) catches genuine outages.
         Reporting here would race with internal recovery and could cause
         the monitor loop to spawn duplicate instances via start_async. *)
      if not (Atomic.get other_ready_flag) then begin
        Logging.error_f ~section "[lighter_ws] Both sides down — internal loops will recover";
        close_all_subscribers ()
      end
    in
    let side_on_connected () =
      Atomic.set ready_flag true;
      Lwt.async (fun () ->
        Lwt.catch
          (fun () -> on_side_reconnected ())
          (fun exn ->
            Logging.error_f ~section "[%s] Error in reconnect callback: %s" label (Printexc.to_string exn);
            Lwt.return_unit)
      );
      check_both_ready ()
    in
    Lwt.catch (fun () ->
      connect_one ~state ~connect_target ~ws_url
        ~on_failure:side_on_failure
        ~on_connected:side_on_connected
        ~on_heartbeat ~label
    ) (fun exn ->
      Logging.debug_f ~section "[%s] connect_one raised: %s" label (Printexc.to_string exn);
      Lwt.return_unit
    ) >>= fun () ->
    (* Connection ended — brief pause then reconnect *)
    Atomic.set ready_flag false;
    Atomic.set both_ready_fired false;
    Logging.debug_f ~section "[%s] Connection ended, reconnecting in 0.5s" label;
    Lwt_unix.sleep 0.5 >>= fun () ->
    reconnect_loop ~state ~connect_target ~ws_url ~label
      ~ready_flag ~other_ready_flag ~on_side_reconnected
  in

  Lwt.join [
    reconnect_loop
      ~state:public_state
      ~connect_target:Lighter_proxy.public_ws_connect_target
      ~ws_url:Lighter_proxy.public_ws_url
      ~label:"Public"
      ~ready_flag:pub_ready
      ~other_ready_flag:priv_ready
      ~on_side_reconnected:(fun () ->
        (* Lightweight: just resubscribe orderbooks for remembered symbols *)
        let symbols = !subscribed_symbols in
        if List.length symbols > 0 then begin
          Logging.debug_f ~section "Public WS reconnected — resubscribing %d orderbook channels" (List.length symbols);
          subscribe_public_orderbook ~symbols
        end else
          Lwt.return_unit
      );
    reconnect_loop
      ~state:private_state
      ~connect_target:Lighter_proxy.private_ws_connect_target
      ~ws_url:Lighter_proxy.private_ws_url
      ~label:"Private"
      ~ready_flag:priv_ready
      ~other_ready_flag:pub_ready
      ~on_side_reconnected:(fun () ->
        (* Heavier: resubscribe private channels *)
        let symbols = !subscribed_symbols in
        let account_index = !subscribed_account_index in
        let auth_token = !subscribed_auth_token in
        if List.length symbols > 0 then begin
          Logging.info_f ~section "Private WS reconnected — resubscribing private channels";
          let acct_str = string_of_int account_index in
          let private_commands = [
            `Assoc [("type", `String "subscribe"); ("channel", `String ("account_all_orders/" ^ acct_str)); ("auth", `String auth_token)];
            `Assoc [("type", `String "subscribe"); ("channel", `String ("account_all/" ^ acct_str)); ("auth", `String auth_token)];
            `Assoc [("type", `String "subscribe"); ("channel", `String ("account_all_assets/" ^ acct_str)); ("auth", `String auth_token)];
            `Assoc [("type", `String "subscribe"); ("channel", `String ("user_stats/" ^ acct_str)); ("auth", `String auth_token)]
          ] in
          let private_stream = Lwt_stream.of_list private_commands in
          Concurrency.Lwt_util.consume_stream_s (fun cmd ->
            send_private_json cmd "Private"
          ) private_stream
        end else
          Lwt.return_unit
      );
  ]

let send_ping ~req_id:_ ~timeout_ms =
  let ping_msg = `Assoc [("type", `String "ping")] in
  let timeout = float_of_int timeout_ms /. 1000.0 in
  
  let ping_one state label =
    let send_time = Unix.gettimeofday () in
    Lwt.catch (fun () ->
      send_json_on state ping_msg label >>= fun () ->
      Lwt.pick [
        (Lwt_condition.wait state.pong_condition >>= fun () -> Lwt.return true);
        (Lwt_unix.sleep timeout >>= fun () ->
         if !(state.last_pong_time) > send_time then Lwt.return true
         else Lwt.return false)
      ]
    ) (fun _ -> Lwt.return false)
  in
  
  (* Don't ping the private side until the stream is confirmed alive.
     The DO proxy needs time to establish its upstream relay; pinging
     before any data arrives is meaningless and causes false failures. *)
  let priv_p =
    if not (Atomic.get private_stream_confirmed) then Lwt.return true
    else ping_one private_state "Private"
  in
  Lwt.both (ping_one public_state "Public") priv_p >>= fun (pub_ok, priv_ok) ->
  if pub_ok && priv_ok then begin
    reset_ping_failures ();
    Lwt.return true
  end else begin
    Logging.warn_f ~section "Ping failed (pub=%b, priv=%b)" pub_ok priv_ok;
    incr_ping_failures ();
    let%lwt () = if not pub_ok then close_public () else Lwt.return_unit in
    let%lwt () = if not priv_ok then close_private () else Lwt.return_unit in
    Lwt.return false
  end
