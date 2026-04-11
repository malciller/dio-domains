(** Lighter WebSocket client.
    Manages a single persistent connection to [mainnet.zklighter.elliot.ai/stream]
    for market data feeds and order submission. Incoming frames are demultiplexed
    to subscriber streams. Supports WebSocket-first order submission via
    [jsonapi/sendtx] with REST fallback. *)

open Lwt.Infix

let section = "lighter_ws"

(** Handle returned to consumers of [subscribe_market_data]. *)
type subscription = {
  stream: Yojson.Safe.t Lwt_stream.t;
  close: unit -> unit;
}

let is_connected_ref = Atomic.make false
let is_connected () = Atomic.get is_connected_ref

(** Mvar signaled each time a new connection is established. *)
let connected_wakeup : unit Lwt_mvar.t = Lwt_mvar.create_empty ()

let wait_for_connected () =
  if Atomic.get is_connected_ref then Lwt.return_unit
  else Lwt_mvar.take connected_wakeup

let active_connection = ref None
let connection_mutex = Lwt_mutex.create ()

let signal_new_data () = Concurrency.Exchange_wakeup.signal_all ()

(** Global list of subscriber push functions. *)
let pushers : (Yojson.Safe.t option -> bool) list ref = ref []
let pushers_mutex = Mutex.create ()

(** Counter of consecutive ping failures, read by the supervisor. *)
let ping_failures = Atomic.make 0
let reset_ping_failures () = Atomic.set ping_failures 0
let get_ping_failures () = Atomic.get ping_failures
let incr_ping_failures () = Atomic.incr ping_failures

(** Pong tracking state.
    Lighter pong responses are app-level messages with type "pong".
    [send_ping] waits on [pong_condition], which is broadcast when
    a "pong" message arrives in [handle_frame]. Falls back to
    checking [last_pong_time] in case the signal arrived before waiting. *)
let last_pong_time = ref 0.0
let pong_condition = Lwt_condition.create ()

(** Diagnostic counters for message types flowing through the WS. *)
let msg_counter_ticker = Atomic.make 0
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
  let dead = ref [] in
  List.iter (fun push ->
    try ignore (push (Some json))
    with _ -> dead := push :: !dead
  ) ps;
  if !dead <> [] then begin
    Mutex.lock pushers_mutex;
    pushers := List.filter (fun p -> not (List.memq p !dead)) !pushers;
    Mutex.unlock pushers_mutex
  end

(** Creates a bounded subscriber stream for incoming messages. *)
let subscribe_market_data () =
  let (stream, push_source) = Lwt_stream.create_bounded 16 in
  let push_fn item =
    match item with
    | None ->
        push_source#close;
        true
    | Some json ->
        let p = push_source#push json in
        if Lwt.is_sleeping p then begin
          Lwt.cancel p;
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
    Mutex.unlock pushers_mutex
  in
  { stream; close }

(** Sends a JSON message over the active WebSocket connection. *)
let send_json json =
  Lwt_mutex.with_lock connection_mutex (fun () ->
    match !active_connection with
    | Some conn ->
        let msg = Yojson.Safe.to_string json in
        Logging.debug_f ~section "Sending WS message: %s"
          (if String.length msg > 500 then String.sub msg 0 500 ^ "..." else msg);
        Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg ())
    | None ->
        Logging.warn ~section "Cannot send: WebSocket not connected";
        Lwt.return_unit
  )

(** Send a subscription message. *)
let subscribe json = send_json json

(** Subscribe to all required Lighter WebSocket channels for the given symbols.
    Subscribes to ticker and orderbook for each market_index, plus account-level
    channels for order updates and balance tracking. *)
let subscribe_to_feeds ~symbols ~account_index ~auth_token =
  (* Subscribe to ticker and orderbook for each symbol *)
  let%lwt () = Lwt_list.iter_s (fun symbol ->
    match Lighter_instruments_feed.get_market_index ~symbol with
    | Some market_index ->
        let mi_str = string_of_int market_index in
        let%lwt () = subscribe (`Assoc [
          ("type", `String "subscribe");
          ("channel", `String ("ticker/" ^ mi_str))
        ]) in
        let%lwt () = subscribe (`Assoc [
          ("type", `String "subscribe");
          ("channel", `String ("order_book/" ^ mi_str))
        ]) in
        Logging.debug_f ~section "Subscribed to ticker/%s and order_book/%s for %s" mi_str mi_str symbol;
        Lwt.return_unit
    | None ->
        Logging.error_f ~section "Cannot subscribe: no market_index for symbol %s" symbol;
        Lwt.return_unit
  ) symbols in

  (* Subscribe to authenticated account channels *)
  let acct_str = string_of_int account_index in
  let%lwt () = subscribe (`Assoc [
    ("type", `String "subscribe");
    ("channel", `String ("account_all_orders/" ^ acct_str));
    ("auth", `String auth_token)
  ]) in
  let%lwt () = subscribe (`Assoc [
    ("type", `String "subscribe");
    ("channel", `String ("account_all/" ^ acct_str));
    ("auth", `String auth_token)
  ]) in
  (* Subscribe to account_all_assets for spot asset balances (ETH etc.).
     The account_all channel may not include all spot balances. *)
  let%lwt () = subscribe (`Assoc [
    ("type", `String "subscribe");
    ("channel", `String ("account_all_assets/" ^ acct_str));
    ("auth", `String auth_token)
  ]) in
  (* Subscribe to user_stats to get USDC collateral/available_balance.
     account_all_assets only delivers non-quote base assets (e.g. ETH);
     USDC as the quote currency appears in user_stats.stats.collateral. *)
  let%lwt () = subscribe (`Assoc [
    ("type", `String "subscribe");
    ("channel", `String ("user_stats/" ^ acct_str));
    ("auth", `String auth_token)
  ]) in
  Logging.debug_f ~section "Subscribed to account_all_orders/%s, account_all/%s, account_all_assets/%s, and user_stats/%s" acct_str acct_str acct_str acct_str;
  Lwt.return_unit

(** Send a signed transaction over WebSocket (primary order submission path). *)
let send_tx_ws ~tx_type ~tx_info =
  let json = `Assoc [
    ("type", `String "jsonapi/sendtx");
    ("data", `Assoc [
      ("tx_type", `Int tx_type);
      ("tx_info", Yojson.Safe.from_string tx_info)
    ])
  ] in
  send_json json

(** Process a single WebSocket frame. *)
let handle_frame ~on_heartbeat (frame : Websocket.Frame.t) =
  match frame.Websocket.Frame.opcode with
  | Websocket.Frame.Opcode.Text ->
      Concurrency.Tick_event_bus.publish_tick ();
      on_heartbeat ();
      (try
        let json = Yojson.Safe.from_string frame.Websocket.Frame.content in
        let msg_type =
          let open Yojson.Safe.Util in
          try member "type" json |> to_string with _ -> ""
        in

        (* Route messages to appropriate feed handlers *)
        let channel =
          let open Yojson.Safe.Util in
          try member "channel" json |> to_string with _ -> ""
        in

        Logging.debug_f ~section "WS message: type=%s channel=%s (len=%d)"
          msg_type channel (String.length frame.Websocket.Frame.content);

        (* Extract market_index from channel strings like "ticker:0" or "ticker/0".
           Lighter uses colon as separator in channel names. *)
        let market_index_from_channel ch =
          try
            (* Try colon first (Lighter's actual format), then slash as fallback *)
            let sep_pos =
              try String.index ch ':'
              with Not_found -> String.index ch '/'
            in
            int_of_string (String.sub ch (sep_pos + 1) (String.length ch - sep_pos - 1))
          with _ -> -1
        in

        (* Route based on message type/channel *)
        (match msg_type with
         | "update/ticker" | "snapshot/ticker" | "subscribed/ticker" ->
             Atomic.incr msg_counter_ticker;
             let mi = market_index_from_channel channel in
             if mi >= 0 then begin
               if Atomic.get msg_counter_ticker <= 3 then
                 Logging.debug_f ~section "Received ticker message: type=%s channel=%s mi=%d (len=%d)"
                   msg_type channel mi (String.length frame.Websocket.Frame.content);
               Lighter_ticker_feed.process_ticker_update ~market_index:mi json
             end else
               Logging.warn_f ~section "Ticker message with invalid market_index: channel=%s" channel
         | "snapshot/order_book" | "subscribed/order_book" ->
             Atomic.incr msg_counter_orderbook;
             let mi = market_index_from_channel channel in
             if mi >= 0 then Lighter_orderbook_feed.process_orderbook_snapshot ~market_index:mi json
         | "update/order_book" ->
             Atomic.incr msg_counter_orderbook;
             let mi = market_index_from_channel channel in
             if mi >= 0 then Lighter_orderbook_feed.process_orderbook_update ~market_index:mi json
         | "update/account_all_orders" | "snapshot/account_all_orders"
         | "subscribed/account_all_orders" ->
             Atomic.incr msg_counter_account;
             Lighter_executions_feed.process_account_orders_update json
         | "update/account_all" | "snapshot/account_all"
         | "subscribed/account_all"
         | "update/account_all_assets" | "snapshot/account_all_assets"
         | "subscribed/account_all_assets"
         | "update/user_stats" | "subscribed/user_stats" ->
             (* Routed to Lighter_balances._processor_task via broadcast_message below. *)
             Atomic.incr msg_counter_account;
             broadcast_message json
         | "pong" ->
              (* Application-level pong response to our keepalive ping *)
              last_pong_time := Unix.gettimeofday ();
              (try Lwt_condition.broadcast pong_condition () with _ -> ());
              reset_ping_failures ();
              on_heartbeat ()
         | t ->
             Atomic.incr msg_counter_other;
             (* Log first few unmatched types to diagnose relay issues *)
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
      reset_ping_failures ();
      Lwt.return_unit
  | Websocket.Frame.Opcode.Close ->
      Logging.info ~section "WebSocket connection closed by server";
      Lwt_mutex.with_lock connection_mutex (fun () ->
        active_connection := None;
        Atomic.set is_connected_ref false;
        Lwt.return_unit
      ) >>= fun () ->
      close_all_subscribers ();
      signal_new_data ();
      Lwt.return_unit
  | _ -> Lwt.return_unit

(** Establishes a TLS WebSocket connection to the Lighter API.
    Enters a read loop dispatching frames through [handle_frame].
    On failure, invokes [on_failure] for supervisor-driven reconnection. *)
let connect_and_monitor ~on_failure ~on_connected ~on_heartbeat =
  let (connect_host, connect_port) = Lighter_proxy.ws_connect_target () in
  let url = Lighter_proxy.ws_url () in
  Logging.debug_f ~section "Connecting to Lighter WebSocket: %s (host=%s:%d)" url connect_host connect_port;
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
    Lwt_mutex.with_lock connection_mutex (fun () ->
      active_connection := Some conn;
      Atomic.set is_connected_ref true;
      if Lwt_mvar.is_empty connected_wakeup then
        Lwt.async (fun () -> Lwt_mvar.put connected_wakeup ());
      Lwt.return_unit
    ) >>= fun () ->
    on_connected ();
    reset_ping_failures ();

    (* Frame counter for first-frame diagnostics *)
    let frame_count = Atomic.make 0 in

    let stream = Lwt_stream.from (fun () ->
      if not (Atomic.get is_connected_ref) then Lwt.return_none
      else Lwt.catch (fun () ->
        Websocket_lwt_unix.read conn >>= fun frame ->
        let n = Atomic.fetch_and_add frame_count 1 in
        if n = 0 then
          Logging.debug_f ~section "First WS frame received (opcode=%s, len=%d)"
            (match frame.Websocket.Frame.opcode with
             | Websocket.Frame.Opcode.Text -> "text"
             | Websocket.Frame.Opcode.Binary -> "binary"
             | Websocket.Frame.Opcode.Ping -> "ping"
             | Websocket.Frame.Opcode.Pong -> "pong"
             | Websocket.Frame.Opcode.Close -> "close"
             | _ -> "other")
            (String.length frame.Websocket.Frame.content);
        Lwt.return_some frame
      ) (function
        | End_of_file -> Lwt.return_none
        | exn -> Lwt.fail exn)
    ) in

    let process_frame frame =
      Lwt.async (fun () ->
        Lwt.catch
          (fun () -> handle_frame ~on_heartbeat frame)
          (fun exn -> Logging.error_f ~section "Error handling frame: %s" (Printexc.to_string exn); Lwt.return_unit)
      )
    in

    let done_p =
      Lwt.catch
        (fun () -> Concurrency.Lwt_util.consume_stream process_frame stream)
        (fun _exn -> Lwt.return_unit)
    in

    Lwt.catch (fun () -> done_p) (function
      | End_of_file ->
          Logging.debug ~section "WebSocket connection closed (End_of_file), reconnecting...";
          Lwt_mutex.with_lock connection_mutex (fun () ->
            active_connection := None;
            Atomic.set is_connected_ref false;
            Lwt.return_unit
          ) >>= fun () ->
          close_all_subscribers ();
          Lwt.fail_with "Connection closed unexpectedly (End_of_file)"
      | exn ->
          Logging.error_f ~section "WebSocket read error: %s" (Printexc.to_string exn);
          Lwt_mutex.with_lock connection_mutex (fun () ->
            active_connection := None;
            Atomic.set is_connected_ref false;
            Lwt.return_unit
          ) >>= fun () ->
          close_all_subscribers ();
          Lwt.fail exn
    ) >>= fun () ->
    Lwt_mutex.with_lock connection_mutex (fun () ->
      active_connection := None;
      Atomic.set is_connected_ref false;
      Lwt.return_unit
    ) >>= fun () ->
    close_all_subscribers ();
    Lwt.fail_with "WebSocket closed by server"
  ) (fun exn ->
    let error_msg = Printexc.to_string exn in
    Logging.error_f ~section "WebSocket connection error: %s" error_msg;
    Atomic.set is_connected_ref false;
    close_all_subscribers ();
    on_failure error_msg;
    Lwt.return_unit
  )

(** Gracefully closes the Lighter WebSocket connection. *)
let close () : unit Lwt.t =
  Lwt_mutex.with_lock connection_mutex (fun () ->
    match !active_connection with
    | None -> Lwt.return None
    | Some conn ->
        active_connection := None;
        Atomic.set is_connected_ref false;
        Lwt.return (Some conn)
  ) >>= function
  | None -> Lwt.return_unit
  | Some conn ->
      Logging.info ~section "Closing Lighter WebSocket connection";
      close_all_subscribers ();
      signal_new_data ();
      Lwt.catch
        (fun () -> Websocket_lwt_unix.close_transport conn)
        (fun _ -> Lwt.return_unit)

(** Sends an application-level ping and waits for a pong within [timeout_ms].
    Returns [true] if a pong was received, [false] on timeout or send failure.
    Uses [pong_condition] since Lighter pong responses are matched by message
    type, not request ID. Falls back to [last_pong_time] in case the condition
    was broadcast before this function began waiting. *)
let send_ping ~req_id:_ ~timeout_ms =
  let ping_msg = `Assoc [("type", `String "ping")] in
  let send_time = Unix.gettimeofday () in
  Lwt.catch (fun () ->
    send_json ping_msg >>= fun () ->
    let timeout = float_of_int timeout_ms /. 1000.0 in
    Lwt.pick [
      (Lwt_condition.wait pong_condition >>= fun () ->
       Logging.debug ~section "Pong received";
       reset_ping_failures ();
       Lwt.return true);
      (Lwt_unix.sleep timeout >>= fun () ->
       (* Check timestamp in case the condition was signaled before we waited. *)
       if !last_pong_time > send_time then begin
         reset_ping_failures ();
         Lwt.return true
       end else begin
         Logging.warn ~section "Ping timed out (no pong received)";
         incr_ping_failures ();
         Lwt.return false
       end)
    ]
  ) (fun exn ->
    Logging.warn_f ~section "Ping send failed: %s" (Printexc.to_string exn);
    incr_ping_failures ();
    Lwt.return false
  )
