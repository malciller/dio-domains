[@@@ocaml.text
  " Lighter WebSocket client: dual persistent connections (public market data, private\n\
  \    authenticated) to the Lighter mainnet. Each side reconnects\n\
  \    independently; one side's failure does not tear down the other or force a full \
   order\n\
  \    book rebuild. "]

open Lwt.Infix

let section = "lighter_ws"

let subscribed_symbols : string list ref = ref []
[@@ocaml.doc
  " Subscription parameters cached by [subscribe_to_feeds] and replayed by the\n\
  \    per-connection reconnect loops. "]
;;

let subscribed_account_index : int ref = ref 0
let subscribed_auth_token : string ref = ref ""

type subscription =
  { stream : Yojson.Safe.t Lwt_stream.t
  ; close : unit -> unit
  }
[@@ocaml.doc " Subscription handle from [subscribe_market_data]. "]

type connection_state =
  { active_connection : Ws_lwt.conn option ref
  ; connection_mutex : Lwt_mutex.t
  ; is_connected_ref : bool Atomic.t
  ; connected_wakeup : unit Lwt_condition.t
  ; last_pong_time : float ref
  ; pong_condition : unit Lwt_condition.t
  ; connected_at : float ref
  ; frames_since_connect : int ref
  }

let create_connection_state () =
  { active_connection = ref None
  ; connection_mutex = Lwt_mutex.create ()
  ; is_connected_ref = Atomic.make false
  ; connected_wakeup = Lwt_condition.create ()
  ; last_pong_time = ref 0.0
  ; pong_condition = Lwt_condition.create ()
  ; connected_at = ref 0.0
  ; frames_since_connect = ref 0
  }
;;

(* A leg that has delivered no data this long after connecting lost its subscriptions on
   the way up; the transport is dropped so the supervisor replays them. *)
let public_first_frame_grace_s = 20.0

(* Account channels snapshot their state on subscribe, so silence here means the replay
   did not take; the wider window covers slow account snapshots. *)
let private_first_frame_grace_s = 60.0
let public_state = create_connection_state ()
let private_state = create_connection_state ()
let public_connected () = Atomic.get public_state.is_connected_ref
let private_connected () = Atomic.get private_state.is_connected_ref
let is_connected () = public_connected () && private_connected ()

let note_data_frame state =
  state.frames_since_connect := !(state.frames_since_connect) + 1
[@@ocaml.doc
  " Counts venue data frames only. Control frames (pongs, acks) must not count, or an\n\
  \    unsubscribed-but-ponging leg would look healthy. "]
;;

let first_frame_overdue ~now ~connected_at ~frames_since_connect ~grace_s =
  frames_since_connect = 0 && now -. connected_at > grace_s
[@@ocaml.doc
  " True when a connected leg has not produced its first data frame inside the grace\n\
  \    window. Pure so the boundary stays testable. "]
;;

let wait_for_connected () =
  let wait_one state =
    if Atomic.get state.is_connected_ref
    then Lwt.return_unit
    else Lwt_condition.wait state.connected_wakeup
  in
  Lwt.join [ wait_one public_state; wait_one private_state ]
;;

let signal_new_data () = Concurrency.Exchange_wakeup.signal_all ()

let pushers : (Yojson.Safe.t option -> bool) list ref = ref []
[@@ocaml.doc " Global mutable list of stream push functions for all active subscribers. "]
;;

let pushers_mutex = Mutex.create ()

let private_stream_confirmed = Atomic.make false
[@@ocaml.doc
  " Gates pings on the private connection until the first inbound frame proves the\n\
  \    connection is live. "]
;;

[@@@ocaml.text " Diagnostic counters by message category. "]

let msg_counter_orderbook = Atomic.make 0
let msg_counter_account = Atomic.make 0
let msg_counter_other = Atomic.make 0
let msg_counter_total = Atomic.make 0

let close_all_subscribers () =
  Mutex.lock pushers_mutex;
  let ps = !pushers in
  pushers := [];
  Mutex.unlock pushers_mutex;
  let count = List.length ps in
  if count > 0
  then (
    Logging.debug_f ~section "Closing %d subscriber streams on disconnect" count;
    List.iter
      (fun push ->
        try ignore (push None) with
        | _ -> ())
      ps)
[@@ocaml.doc " Closes all subscriber streams; used on a full disconnect. "]
;;

let broadcast_message json =
  Mutex.lock pushers_mutex;
  let ps = !pushers in
  Mutex.unlock pushers_mutex;
  let alive = ref [] in
  List.iter
    (fun push ->
      try if push (Some json) then alive := push :: !alive with
      | _ -> ())
    ps;
  if List.length !alive <> List.length ps
  then (
    let dead = List.filter (fun p -> not (List.memq p !alive)) ps in
    Mutex.lock pushers_mutex;
    pushers := List.filter (fun p -> not (List.memq p dead)) !pushers;
    Mutex.unlock pushers_mutex)
[@@ocaml.doc " Pushes a message to all subscribers and prunes dead pushers. "]
;;

let subscribe_market_data () =
  let stream, push_source = Lwt_stream.create_bounded 16 in
  let closed = Atomic.make false in
  let close_internal () =
    if not (Atomic.exchange closed true)
    then (
      push_source#close;
      true)
    else false
  in
  let push_fn item =
    if Atomic.get closed
    then false
    else (
      match item with
      | None -> close_internal ()
      | Some json ->
        let p = push_source#push json in
        if Lwt.is_sleeping p
        then (
          ignore (close_internal ());
          false)
        else true)
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
[@@ocaml.doc
  " Creates a subscriber with a bounded stream. A push that would block (slow consumer) or\n\
  \    arrive after close returns false and closes the stream. "]
;;

let write_timeout_s = 5.0
[@@ocaml.doc
  " Deadline for one frame write. A peer that stops draining leaves [Lwt_io.flush] pending\n\
  \    forever; the write holds [connection_mutex], so that would also starve the \
   heartbeat\n\
  \    that would otherwise detect the stall. "]
;;

(* A timed-out or failed write leaves a partial frame on the wire, so the stream cannot be
   reused: abort the socket (never flush a stalled buffer) and let the reconnect loop
   rebuild and resubscribe. *)
let write_frame_locked state conn frame =
  Lwt.catch
    (fun () ->
      Lwt_unix.with_timeout write_timeout_s (fun () -> Ws_lwt.write conn frame)
      >|= fun () -> Ok ())
    (fun exn ->
      state.active_connection := None;
      Atomic.set state.is_connected_ref false;
      Ws_lwt.abort_transport conn
      >>= fun () -> Lwt.return (Error (Printexc.to_string exn)))
;;

let send_json_on state json label =
  Lwt_mutex.with_lock state.connection_mutex (fun () ->
    match !(state.active_connection) with
    | Some conn ->
      let msg = Yojson.Safe.to_string json in
      Logging.debug_f
        ~section
        "[%s] Sending WS message: %s"
        label
        (if String.length msg > 500 then String.sub msg 0 500 ^ "..." else msg);
      write_frame_locked state conn (Websocket.Frame.create ~content:msg ())
      >>= (function
       | Ok () -> Lwt.return (Ok ())
       | Error err ->
         Logging.error_f ~section "[%s] Write failed, connection dropped: %s" label err;
         Lwt.return (Error err))
    | None ->
      Logging.warn_f ~section "[%s] Cannot send: WebSocket not connected" label;
      Lwt.return (Error "WebSocket not connected"))
[@@ocaml.doc
  " Serializes and sends a JSON payload on the given connection under its lock, reporting\n\
  \    whether the frame reached the socket. "]
;;

let send_public_json json label = send_json_on public_state json label >|= fun _ -> ()
let send_private_json json label = send_json_on private_state json label >|= fun _ -> ()

(* Tx submission needs the failure, not just the log line: a silently dropped order is
   worse than a reported one. *)
let send_private_tx json = send_json_on private_state json "Private"

let subscribe_public_orderbook ~symbols =
  let public_stream = Lwt_stream.of_list symbols in
  Concurrency.Lwt_util.consume_stream_s
    (fun symbol ->
      match Lighter_instruments_feed.get_market_index ~symbol with
      | Some market_index ->
        let mi_str = string_of_int market_index in
        let __ppx_lwt_0 =
          send_public_json
            (`Assoc
              [ "type", `String "subscribe"; "channel", `String ("order_book/" ^ mi_str) ])
            "Public"
        in
        Lwt.backtrace_bind
          "src/integrations/venues/lighter/lighter_ws.ml"
          183
          (fun exn ->
            try Lwt.reraise exn with
            | exn -> exn)
          __ppx_lwt_0
          (fun () ->
            Logging.debug_f ~section "Subscribed to order_book/%s for %s" mi_str symbol;
            Lwt.return_unit)
      | None ->
        Logging.error_f ~section "Cannot subscribe: no market_index for symbol %s" symbol;
        Lwt.return_unit)
    public_stream
;;

let subscribe_public_market_stats ~symbols =
  let perp_symbols = List.filter (fun s -> not (String.contains s '/')) symbols in
  let public_stream = Lwt_stream.of_list perp_symbols in
  Concurrency.Lwt_util.consume_stream_s
    (fun symbol ->
      match Lighter_instruments_feed.get_market_index ~symbol with
      | Some market_index ->
        let mi_str = string_of_int market_index in
        let __ppx_lwt_0 =
          send_public_json
            (`Assoc
              [ "type", `String "subscribe"
              ; "channel", `String ("market_stats/" ^ mi_str)
              ])
            "Public"
        in
        Lwt.backtrace_bind
          "src/integrations/venues/lighter/lighter_ws.ml"
          196
          (fun exn ->
            try Lwt.reraise exn with
            | exn -> exn)
          __ppx_lwt_0
          (fun () ->
            Logging.debug_f ~section "Subscribed to market_stats/%s for %s" mi_str symbol;
            Lwt.return_unit)
      | None ->
        Logging.error_f ~section "Cannot subscribe: no market_index for symbol %s" symbol;
        Lwt.return_unit)
    public_stream
;;

let private_subscribe_commands ~account_index ~auth_token =
  let acct_str = string_of_int account_index in
  [ `Assoc
      [ "type", `String "subscribe"
      ; "channel", `String ("account_all_orders/" ^ acct_str)
      ; "auth", `String auth_token
      ]
  ; `Assoc
      [ "type", `String "subscribe"
      ; "channel", `String ("account_all/" ^ acct_str)
      ; "auth", `String auth_token
      ]
  ; `Assoc
      [ "type", `String "subscribe"
      ; "channel", `String ("account_all_assets/" ^ acct_str)
      ; "auth", `String auth_token
      ]
  ; `Assoc
      [ "type", `String "subscribe"
      ; "channel", `String ("user_stats/" ^ acct_str)
      ; "auth", `String auth_token
      ]
  ]
;;

let replay_private_subscriptions () =
  if !subscribed_symbols = []
  then Lwt.return_unit
  else (
    let account_index = !subscribed_account_index in
    let commands =
      private_subscribe_commands ~account_index ~auth_token:!subscribed_auth_token
    in
    Concurrency.Lwt_util.consume_stream_s
      (fun cmd -> send_private_json cmd "Private")
      (Lwt_stream.of_list commands)
    >>= fun () ->
    Logging.debug_f
      ~section
      "Replayed %d private subscriptions for account %d"
      (List.length commands)
      account_index;
    Lwt.return_unit)
;;

let replay_public_subscriptions () =
  let symbols = !subscribed_symbols in
  if symbols = []
  then Lwt.return_unit
  else
    (* Subscribing needs the market index table; bounded so a failed instruments fetch
       cannot wedge the leg's watchdog. A timeout leaves it to the next verify window. *)
    Lwt.catch
      (fun () ->
        Lwt_unix.with_timeout 10.0 Lighter_instruments_feed.wait_until_ready
        >>= fun () ->
        Lwt.join
          [ subscribe_public_orderbook ~symbols; subscribe_public_market_stats ~symbols ])
      (fun _ -> Lwt.return_unit)
;;

let configure_subscriptions ~symbols ~account_index ~auth_token =
  subscribed_symbols := symbols;
  subscribed_account_index := account_index;
  subscribed_auth_token := auth_token
[@@ocaml.doc
  " Caches the subscription targets before the first connect so each leg can replay its\n\
  \    own channels without waiting for the other leg (and without a duplicate subscribe\n\
  \    once both are up). "]
;;

let subscribe_to_feeds ~symbols ~account_index ~auth_token =
  configure_subscriptions ~symbols ~account_index ~auth_token;
  Lwt.join [ replay_public_subscriptions (); replay_private_subscriptions () ]
[@@ocaml.doc
  " Subscribes orderbook channels (public) and account channels (private); caches the\n\
  \    parameters so the reconnect loops and the data watchdog can replay them. "]
;;

let tx_id_counter = Atomic.make 0
let next_tx_id () = Atomic.fetch_and_add tx_id_counter 1

(** Pure frame builder: one tx becomes [jsonapi/sendtx], several [jsonapi/sendtxbatch].
    Batch fields are JSON *strings* holding arrays, per the reference client. *)
let tx_frame txs =
  match txs with
  | [ (tx_type, tx_info, id) ] ->
    `Assoc
      [ "type", `String "jsonapi/sendtx"
      ; ( "data"
        , `Assoc
            [ "id", `String id
            ; "tx_type", `Int tx_type
            ; "tx_info", Yojson.Safe.from_string tx_info
            ] )
      ]
  | (_, _, first_id) :: _ as many ->
    let types = `List (List.map (fun (t, _, _) -> `Int t) many) in
    let infos =
      `List (List.map (fun (_, info, _) -> Yojson.Safe.from_string info) many)
    in
    `Assoc
      [ "type", `String "jsonapi/sendtxbatch"
      ; ( "data"
        , `Assoc
            [ "id", `String first_id
            ; "tx_types", `String (Yojson.Safe.to_string types)
            ; "tx_infos", `String (Yojson.Safe.to_string infos)
            ] )
      ]
  | [] -> `Assoc []
;;

(** [txs] is [(tx_type, tx_info, id)] in nonce order. Both forms are a single frame, so a
    burst costs one write. Resolves with the id assigned to each tx, in input order. *)
let send_txs_ws txs =
  let txs =
    List.map
      (fun (tx_type, tx_info, id) ->
        let id =
          match id with
          | Some i -> i
          | None -> Printf.sprintf "dio-%d" (next_tx_id ())
        in
        tx_type, tx_info, id)
      txs
  in
  let frame = tx_frame txs in
  match txs with
  | [] -> Lwt.return (Ok [])
  | _ ->
    if not (private_connected ())
    then Lwt.return (Error "lighter private WS not connected")
    else (
      let ids = List.map (fun (_, _, id) -> id) txs in
      let id_str = String.concat "," ids in
      Lwt.catch
        (fun () ->
          send_private_tx frame
          >|= function
          | Ok () ->
            Logging.debug_f ~section "tx over WS: n=%d ids=[%s]" (List.length txs) id_str;
            Ok ids
          | Error e -> Error (Printf.sprintf "tx over WS failed (ids=[%s]): %s" id_str e))
        (fun exn ->
          let err = Printexc.to_string exn in
          Logging.error_f ~section "tx over WS failed (ids=[%s]): %s" id_str err;
          Lwt.return (Error err)))
[@@ocaml.doc
  " Sends signed txs over the private WS ([jsonapi/sendtx] or [jsonapi/sendtxbatch]),\n\
  \    resolving once the frame is written. [Error] means the frame was not written;\n\
  \    acceptance is confirmed by the account order stream, as the API documents for both\n\
  \    transports. "]
;;

let record_orderbook_latency json =
  let open Yojson.Safe.Util in
  let ms =
    match member "timestamp" json with
    | `Int i -> Some (float_of_int i)
    | `Intlit s ->
      (try Some (float_of_string s) with
       | _ -> None)
    | `Float f -> Some f
    | `String s ->
      (try Some (float_of_string s) with
       | _ -> None)
    | _ -> None
  in
  match ms with
  | Some ms -> Network_latency.record_feed_event_ms "lighter" ~event_ms:ms ()
  | None -> ()
[@@ocaml.doc
  " Records the clock-corrected one-way network latency of an order-book frame from its\n\
  \    top-level [timestamp] (milliseconds since epoch). No-op when the frame carries no\n\
  \    parseable timestamp. "]
;;

type text_frame_kind =
  | Frame_data
  | Frame_pong
  | Frame_tx_ack of int * string
  | Frame_unmatched of string * string
[@@ocaml.doc " Classification of an inbound text frame. "]

let decode_text_frame content : text_frame_kind =
  let json = Yojson.Safe.from_string content in
  let channel =
    let open Yojson.Safe.Util in
    try member "channel" json |> to_string with
    | _ -> ""
  in
  let msg_type =
    let raw_type =
      let open Yojson.Safe.Util in
      try member "type" json |> to_string with
      | _ -> ""
    in
    if channel <> ""
    then (
      let ch_prefix =
        try
          String.sub
            channel
            0
            (min
               (try String.index channel '/' with
                | Not_found -> String.length channel)
               (try String.index channel ':' with
                | Not_found -> String.length channel))
        with
        | _ -> channel
      in
      if raw_type = "update" || raw_type = "snapshot" || raw_type = "subscribed"
      then raw_type ^ "/" ^ ch_prefix
      else raw_type)
    else raw_type
  in
  let market_index_from_channel ch =
    try
      let sep_pos =
        try String.index ch ':' with
        | Not_found -> String.index ch '/'
      in
      int_of_string (String.sub ch (sep_pos + 1) (String.length ch - sep_pos - 1))
    with
    | _ -> -1
  in
  match msg_type with
  | "snapshot/order_book" | "subscribed/order_book" ->
    Atomic.incr msg_counter_orderbook;
    record_orderbook_latency json;
    let mi = market_index_from_channel channel in
    if mi >= 0
    then Lighter_orderbook_feed.process_orderbook_snapshot ~market_index:mi json;
    Frame_data
  | "update/order_book" ->
    Atomic.incr msg_counter_orderbook;
    record_orderbook_latency json;
    let mi = market_index_from_channel channel in
    if mi >= 0 then Lighter_orderbook_feed.process_orderbook_update ~market_index:mi json;
    Frame_data
  | "update/account_all_orders"
  | "snapshot/account_all_orders"
  | "subscribed/account_all_orders" ->
    Atomic.incr msg_counter_account;
    Lighter_executions_feed.process_account_orders_update json;
    Frame_data
  | "update/account_all" ->
    (* Delta: only the markets that changed are present. Merging keeps every other live
       position, so a one-market update cannot clear the rest of the book. *)
    Atomic.incr msg_counter_account;
    Lighter_balances.process_market_data json;
    ignore (Lighter_positions.process_account_all ~replace:false json);
    Frame_data
  | "snapshot/account_all" | "subscribed/account_all" ->
    Atomic.incr msg_counter_account;
    Lighter_balances.process_market_data json;
    ignore (Lighter_positions.process_account_all json);
    Frame_data
  | "update/account_all_assets"
  | "snapshot/account_all_assets"
  | "subscribed/account_all_assets" ->
    Atomic.incr msg_counter_account;
    Lighter_balances.process_market_data json;
    Frame_data
  | "update/user_stats" | "snapshot/user_stats" | "subscribed/user_stats" ->
    Atomic.incr msg_counter_account;
    Lighter_balances.process_market_data json;
    ignore (Lighter_positions.process_user_stats json);
    Frame_data
  | "update/market_stats" | "snapshot/market_stats" | "subscribed/market_stats" ->
    Lighter_market_stats.process_market_data json;
    Frame_data
  | "pong" -> Frame_pong
  | "jsonapi/sendtx" | "jsonapi/sendtxbatch" ->
    (* Acceptance reply for a submitted tx. The schema is undocumented beyond "id is
       returned", so read [code]/[message] from the payload or the envelope and default to
       accepted. *)
    let open Yojson.Safe.Util in
    let data = member "data" json in
    let pick f =
      (* A successful tx reply often has [data: null] and carries [code] on the envelope.
         Reading a member of null raises, so fall back instead of failing the whole frame. *)
      match
        try f data with
        | _ -> None
      with
      | Some v -> Some v
      | None ->
        (try f json with
         | _ -> None)
    in
    let code =
      Option.value (pick (fun j -> to_int_option (member "code" j))) ~default:200
    in
    let message =
      Option.value (pick (fun j -> to_string_option (member "message" j))) ~default:""
    in
    Frame_tx_ack (code, message)
  | t -> Frame_unmatched (t, channel)
[@@ocaml.doc
  " Pure decode of one inbound text frame: routes market-data and execution frames to the\n\
  \    appropriate feed, and reports pong/unmatched frames to the caller. Parse-domain \
   safe:\n\
  \    no Lwt and no connection state. "]
;;

let handle_frame ~state ~on_heartbeat (frame : Websocket.Frame.t) =
  match frame.Websocket.Frame.opcode with
  | Websocket.Frame.Opcode.Text ->
    Concurrency.Tick_event_bus.publish_tick ();
    if state == private_state && not (Atomic.get private_stream_confirmed)
    then Atomic.set private_stream_confirmed true;
    (try
       (match decode_text_frame frame.Websocket.Frame.content with
        | Frame_data ->
          note_data_frame state;
          (* Only data proves the subscription is alive; acks and pongs must not feed the
             supervisor's data timeout. *)
          on_heartbeat ()
        | Frame_tx_ack (code, message) ->
          if code = 200
          then Logging.debug_f ~section "Tx accepted (code=%d)" code
          else (
            Logging.warn_f
              ~section
              "Tx rejected by exchange: code=%d message=%s"
              code
              message;
            (* Without this a desync rejects every later tx on this transport. *)
            if Error_handling.string_contains
                 (String.lowercase_ascii message)
                 "invalid nonce"
            then
              Lwt.async (fun () ->
                Lighter_signer.initialize_nonce
                  ~timeout_s:2.0
                  ~base_url:Lighter_endpoints.api_base_url
                  ~api_key_index:(Lighter_signer.get_api_key_index ())
                  ~account_index:(Lighter_signer.get_account_index ())
                  ()))
        | Frame_pong ->
          state.last_pong_time := Unix.gettimeofday ();
          (try Lwt_condition.broadcast state.pong_condition () with
           | _ -> ())
        | Frame_unmatched (t, channel) ->
          Atomic.incr msg_counter_other;
          if Atomic.get msg_counter_other <= 5
          then
            Logging.debug_f
              ~section
              "Unmatched WS message type: '%s' channel='%s' (len=%d)"
              t
              channel
              (String.length frame.Websocket.Frame.content));
       Atomic.incr msg_counter_total
     with
     | exn ->
       Logging.warn_f
         ~section
         "Failed to parse WS message: %s (content_prefix=%s)"
         (Printexc.to_string exn)
         (let c = frame.Websocket.Frame.content in
          if String.length c > 100 then String.sub c 0 100 ^ "..." else c));
    Lwt.return_unit
  | Websocket.Frame.Opcode.Pong ->
    state.last_pong_time := Unix.gettimeofday ();
    (try Lwt_condition.broadcast state.pong_condition () with
     | _ -> ());
    Lwt.return_unit
  | Websocket.Frame.Opcode.Close ->
    Logging.debug_f ~section "WebSocket connection closed by server";
    Lwt_mutex.with_lock state.connection_mutex (fun () ->
      state.active_connection := None;
      Atomic.set state.is_connected_ref false;
      Lwt.return_unit)
    >>= fun () ->
    signal_new_data ();
    Lwt.return_unit
  | _ -> Lwt.return_unit
[@@ocaml.doc " Dispatches one WS frame by opcode and message type. "]
;;

let close_state state label =
  Lwt_mutex.with_lock state.connection_mutex (fun () ->
    match !(state.active_connection) with
    | None -> Lwt.return None
    | Some conn ->
      state.active_connection := None;
      Atomic.set state.is_connected_ref false;
      Lwt.return (Some conn))
  >>= function
  | None -> Lwt.return_unit
  | Some conn ->
    Logging.info_f ~section "Closing %s Lighter WebSocket connection" label;
    Lwt.catch (fun () -> Ws_lwt.close_transport conn) (fun _ -> Lwt.return_unit)
;;

let connect_one
  ~state
  ~connect_target
  ~ws_url
  ~on_failure
  ~on_connected
  ~on_heartbeat
  ~label
  ~first_frame_grace_s
  =
  let connect_host, connect_port = connect_target () in
  let url = ws_url () in
  let uri = Uri.of_string url in
  (* Dial target may differ from the URL host: the private socket is pinned to the egress
     relay, while TLS must still validate Lighter's real hostname. *)
  let tls_host = Uri.host uri |> Option.value ~default:connect_host in
  Logging.debug_f
    ~section
    "Connecting to [%s] Lighter WebSocket: %s (host=%s:%d tls=%s)"
    label
    url
    connect_host
    connect_port
    tls_host;
  Lwt.catch
    (fun () ->
      Lwt_unix.getaddrinfo
        connect_host
        (string_of_int connect_port)
        [ Unix.AI_FAMILY Unix.PF_INET ]
      >>= fun addresses ->
      let ip =
        match addresses with
        | { Unix.ai_addr = Unix.ADDR_INET (addr, _); _ } :: _ ->
          Ipaddr_unix.of_inet_addr addr
        | _ -> failwith (Printf.sprintf "Failed to resolve %s" connect_host)
      in
      let client = `TLS (`Hostname tls_host, `IP ip, `Port connect_port) in
      let ctx = Ws_lwt.resolve_ctx () in
      Lwt_unix.with_timeout 20.0 (fun () -> Ws_lwt.connect ~ctx client uri)
      >>= fun conn ->
      Lwt_mutex.with_lock state.connection_mutex (fun () ->
        state.active_connection := Some conn;
        Atomic.set state.is_connected_ref true;
        state.connected_at := Unix.gettimeofday ();
        state.frames_since_connect := 0;
        if state == private_state then Atomic.set private_stream_confirmed false;
        Lwt_condition.broadcast state.connected_wakeup ();
        Lwt.return_unit)
      >>= fun () ->
      on_connected ();
      (* A leg that never produced data is dropped here; the supervisor retries the
         connect and the replay. *)
      Lwt.async (fun () ->
        Lwt_unix.sleep first_frame_grace_s
        >>= fun () ->
        if Atomic.get state.is_connected_ref
           && first_frame_overdue
                ~now:(Unix.gettimeofday ())
                ~connected_at:!(state.connected_at)
                ~frames_since_connect:!(state.frames_since_connect)
                ~grace_s:first_frame_grace_s
        then (
          Logging.error_f
            ~section
            "[%s] No data %.0fs after connect \226\128\148 dropping the leg"
            label
            first_frame_grace_s;
          close_state state label)
        else Lwt.return_unit);
      let stream =
        Lwt_stream.from (fun () ->
          if not (Atomic.get state.is_connected_ref)
          then Lwt.return_none
          else
            Lwt.catch
              (fun () -> Ws_lwt.read conn >>= fun frame -> Lwt.return_some frame)
              (function
                | End_of_file -> Lwt.return_none
                | exn -> Lwt.fail exn))
      in
      let done_p =
        Concurrency.Lwt_util.consume_stream_s
          (fun frame ->
            Lwt.catch
              (fun () -> handle_frame ~state ~on_heartbeat frame)
              (fun exn ->
                Logging.error_f
                  ~section
                  "[%s] Error handling frame: %s"
                  label
                  (Printexc.to_string exn);
                Lwt.return_unit))
          stream
      in
      Lwt.catch
        (fun () -> done_p)
        (function
          | End_of_file ->
            Logging.debug_f
              ~section
              "[%s] WebSocket connection closed (End_of_file)"
              label;
            Lwt_mutex.with_lock state.connection_mutex (fun () ->
              state.active_connection := None;
              Atomic.set state.is_connected_ref false;
              Lwt.return_unit)
            >>= fun () ->
            Lwt.catch (fun () -> Ws_lwt.close_transport conn) (fun _ -> Lwt.return_unit)
            >>= fun () ->
            Lwt.fail_with ("[" ^ label ^ "] Connection closed unexpectedly (End_of_file)")
          | exn ->
            Logging.debug_f
              ~section
              "[%s] WebSocket read error: %s"
              label
              (Printexc.to_string exn);
            Lwt_mutex.with_lock state.connection_mutex (fun () ->
              state.active_connection := None;
              Atomic.set state.is_connected_ref false;
              Lwt.return_unit)
            >>= fun () ->
            Lwt.catch (fun () -> Ws_lwt.close_transport conn) (fun _ -> Lwt.return_unit)
            >>= fun () -> Lwt.fail exn)
      >>= fun () ->
      Lwt_mutex.with_lock state.connection_mutex (fun () ->
        state.active_connection := None;
        Atomic.set state.is_connected_ref false;
        Lwt.return_unit)
      >>= fun () ->
      Lwt.catch (fun () -> Ws_lwt.close_transport conn) (fun _ -> Lwt.return_unit)
      >>= fun () -> Lwt.fail_with ("[" ^ label ^ "] WebSocket closed by server"))
    (fun exn ->
      let error_msg = Printexc.to_string exn in
      Logging.debug_f ~section "[%s] WebSocket connection error: %s" label error_msg;
      Atomic.set state.is_connected_ref false;
      on_failure error_msg;
      Lwt.return_unit)
[@@ocaml.doc
  " Connects one leg and reads frames until the transport dies. Calls [on_failure] before\n\
  \    returning, so the supervisor owns the retry and backoff. "]
;;

let close () : unit Lwt.t =
  close_all_subscribers ();
  signal_new_data ();
  Lwt.join [ close_state public_state "Public"; close_state private_state "Private" ]
;;

let connect_leg
  ~state
  ~label
  ~connect_target
  ~ws_url
  ~on_failure
  ~on_connected
  ~on_heartbeat
  ~replay
  ~first_frame_grace_s
  =
  connect_one
    ~state
    ~connect_target
    ~ws_url
    ~on_failure
    ~on_connected:(fun () ->
      on_connected ();
      Lwt.async (fun () ->
        Lwt.catch replay (fun exn ->
          Logging.error_f
            ~section
            "[%s] Subscription replay failed: %s"
            label
            (Printexc.to_string exn);
          Lwt.return_unit)))
    ~on_heartbeat
    ~label
    ~first_frame_grace_s
;;

let connect_public_and_monitor ~on_failure ~on_connected ~on_heartbeat =
  connect_leg
    ~state:public_state
    ~label:"Public"
    ~connect_target:Lighter_endpoints.ws_connect_target
    ~ws_url:Lighter_endpoints.public_ws_url
    ~on_failure
    ~on_connected
    ~on_heartbeat
    ~replay:replay_public_subscriptions
    ~first_frame_grace_s:public_first_frame_grace_s
[@@ocaml.doc
  " Connects the market-data leg and reads until it dies. The supervisor restarts it with\n\
  \    backoff; this function never loops. "]
;;

let connect_private_and_monitor ~on_failure ~on_connected ~on_heartbeat =
  connect_leg
    ~state:private_state
    ~label:"Private"
    ~connect_target:Lighter_endpoints.private_ws_connect_target
    ~ws_url:Lighter_endpoints.private_ws_url
    ~on_failure
    ~on_connected
    ~on_heartbeat
    ~replay:replay_private_subscriptions
    ~first_frame_grace_s:private_first_frame_grace_s
[@@ocaml.doc
  " Connects the authenticated leg and reads until it dies. The supervisor restarts it\n\
  \    with backoff. A quiet account stops sending after its snapshot, so the account\n\
  \    snapshot is the proof that the replay took. "]
;;

let ping_reply_timeout_s = 5.0

type ping_result =
  | Ping_ok
  | Ping_not_sent
  | Ping_no_reply

type leg =
  | Public
  | Private

let ping_side state =
  let send_time = Unix.gettimeofday () in
  Lwt_mutex.with_lock state.connection_mutex (fun () ->
    match !(state.active_connection) with
    | None -> Lwt.return false
    | Some conn ->
      write_frame_locked
        state
        conn
        (Websocket.Frame.create ~opcode:Websocket.Frame.Opcode.Ping ())
      >|= (function
       | Ok () -> true
       | Error _ -> false))
  >>= fun sent ->
  if not sent
  then Lwt.return Ping_not_sent
  else if !(state.last_pong_time) >= send_time
  then Lwt.return Ping_ok
  else
    (* The reply wait must not hold the write lock: a silent peer would otherwise block
       order submission on this leg for the whole timeout. *)
    Lwt.pick
      [ (Lwt_condition.wait state.pong_condition
         >|= fun () ->
         Network_latency.record_ping_s "lighter" (Unix.gettimeofday () -. send_time);
         Ping_ok)
      ; (Lwt_unix.sleep ping_reply_timeout_s
         >|= fun () ->
         if !(state.last_pong_time) >= send_time
         then (
           Network_latency.record_ping_s "lighter" (Unix.gettimeofday () -. send_time);
           Ping_ok)
         else Ping_no_reply)
      ]
[@@ocaml.doc
  " Probes one leg and reports whether the probe was answered, never sent (the leg is\n\
  \    between sockets), or sent and ignored. "]
;;

let send_ping ~leg ~req_id:_ ~timeout_ms:_ =
  match leg with
  | Public -> ping_side public_state >|= fun result -> result = Ping_ok
  | Private ->
    if not (Atomic.get private_stream_confirmed)
    then Lwt.return true
    else ping_side private_state >|= fun result -> result = Ping_ok
[@@ocaml.doc
  " One leg's pong verdict for the supervisor. The private probe stays silent until the\n\
  \    first authenticated frame proves the socket, matching [private_stream_confirmed]. "]
;;
