(** Hyperliquid WebSocket client.
    Manages a single persistent connection for market data and user event
    subscriptions. Incoming frames are demultiplexed to subscriber streams
    and to a request/response table keyed by integer IDs. *)

open Lwt.Infix

let section = "hyperliquid_ws"

(** Handle returned to consumers of [subscribe_market_data]. *)
type subscription = {
  stream: Yojson.Safe.t Lwt_stream.t;
  close: unit -> unit;
}

type raw_subscription = {
  stream: string Lwt_stream.t;
  close: unit -> unit;
}

let is_connected_ref = Atomic.make false
let is_connected () = Atomic.get is_connected_ref

(** Mvar signaled each time a new connection is established.
    Consumers call [wait_for_connected] to block on this rather than polling.
    Refilled after each successful connect so subsequent callers also wake. *)
let connected_wakeup : unit Lwt_mvar.t = Lwt_mvar.create_empty ()

(** Returns immediately if already connected, otherwise blocks until
    [connected_wakeup] is signaled by a successful connection. *)
let wait_for_connected () =
  if Atomic.get is_connected_ref then Lwt.return_unit
  else Lwt_mvar.take connected_wakeup

let active_connection = ref None
let connection_mutex = Lwt_mutex.create ()

(** Broadcasts to [Concurrency.Exchange_wakeup] to unblock domain workers
    waiting on Hyperliquid data. Called on every incoming frame and on disconnect. *)
let signal_new_data () = Concurrency.Exchange_wakeup.signal_all ()

(** Global list of subscriber push functions.
    Each entry accepts [Some json] to deliver a message or [None] to close
    the stream. Returns [true] on success, [false] if the message was dropped. *)
let pushers : (Yojson.Safe.t option -> bool) list ref = ref []
let pushers_mutex = Mutex.create ()

let raw_pushers : (string option -> bool) list ref = ref []
let raw_pushers_mutex = Mutex.create ()

module Response_table = Hashtbl.Make (struct
  type t = int
  let equal = Int.equal
  let hash = Hashtbl.hash
end)

let responses : (Yojson.Safe.t Lwt.u * float) Response_table.t = Response_table.create 32
let responses_mutex = Lwt_mutex.create ()

(** Counter of consecutive ping failures, read by the supervisor. *)
let ping_failures = Atomic.make 0
let reset_ping_failures () = Atomic.set ping_failures 0
let get_ping_failures () = Atomic.get ping_failures
let incr_ping_failures () = Atomic.incr ping_failures

(** Pong tracking state.
    Hyperliquid pong responses carry no id field, so the generic
    [send_request] ID-matching mechanism cannot be used. Instead,
    [send_ping] waits on [pong_condition], which is broadcast when
    a frame with channel "pong" arrives. *)
let last_pong_time = ref 0.0
let pong_condition = Lwt_condition.create ()

(** Rejects all pending [send_request] waiters with a [Failure] exception.
    Called on disconnect to prevent callers from blocking until their
    individual timeouts expire. *)
let fail_all_pending reason =
  Lwt.async (fun () ->
    Lwt_mutex.with_lock responses_mutex (fun () ->
      let count = Response_table.length responses in
      if count > 0 then
        Logging.info_f ~section "Failing %d pending response waiters (reason: %s)" count reason;
      Response_table.iter (fun req_id (wakener, _timestamp) ->
        (try
          Lwt.wakeup_later_exn wakener (Failure (Printf.sprintf "WebSocket disconnected: %s" reason))
        with Invalid_argument _ ->
          Logging.debug_f ~section "Pending req_id %d already resolved during fail_all_pending" req_id)
      ) responses;
      Response_table.clear responses;
      Lwt.return_unit
    )
  )

(** Removes response table entries older than 30 seconds.
    Each stale waiter is rejected with a timeout [Failure].
    Prevents unbounded growth of the response table. *)
let cleanup_stale_responses () =
  Lwt_mutex.with_lock responses_mutex (fun () ->
    let now = Unix.time () in
    let stale = ref [] in
    Response_table.iter (fun req_id (wakener, timestamp) ->
      if now -. timestamp > 30.0 then
        stale := (req_id, wakener, timestamp) :: !stale
    ) responses;
    List.iter (fun (req_id, wakener, timestamp) ->
      Response_table.remove responses req_id;
      Logging.debug_f ~section "Cleaned up stale response entry for req_id=%d (age: %.1fs)" req_id (now -. timestamp);
      (try Lwt.wakeup_later_exn wakener (Failure (Printf.sprintf "Request timed out after %.1fs" (now -. timestamp)))
       with Invalid_argument _ -> ())
    ) !stale;
    if !stale <> [] then
      Logging.debug_f ~section "Cleaned up %d stale response table entries" (List.length !stale);
    Lwt.return_unit
  )

(** Pushes [None] to every subscriber stream to signal termination, then
    clears the pushers list. Called on disconnect to unblock consumers
    waiting in [Lwt_stream.iter] and to release push closures. *)
let close_all_subscribers () =
  Mutex.lock pushers_mutex;
  let ps = !pushers in
  pushers := [];
  Mutex.unlock pushers_mutex;
  let count = List.length ps in
  if count > 0 then begin
    Logging.info_f ~section "Closing %d subscriber streams on disconnect" count;
    List.iter (fun push ->
      (try ignore (push None) with _ -> ())
    ) ps
  end;
  Mutex.lock raw_pushers_mutex;
  let r_ps = !raw_pushers in
  raw_pushers := [];
  Mutex.unlock raw_pushers_mutex;
  let count_raw = List.length r_ps in
  if count_raw > 0 then begin
    Logging.info_f ~section "Closing %d raw subscriber streams on disconnect" count_raw;
    List.iter (fun push ->
      (try ignore (push None) with _ -> ())
    ) r_ps
  end

(** Delivers a JSON message to all registered subscribers.
    Push is non-blocking: if a subscriber's bounded stream is full, the
    message is silently dropped for that subscriber to prevent unbounded
    memory growth from slow consumers. Dead pushers are evicted. *)
let broadcast_message json =
  Mutex.lock pushers_mutex;
  let ps = !pushers in
  Mutex.unlock pushers_mutex;
  if ps = [] then Logging.warn ~section "No subscribers for WebSocket message!";
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

let broadcast_raw_message msg =
  Mutex.lock raw_pushers_mutex;
  let ps = !raw_pushers in
  Mutex.unlock raw_pushers_mutex;
  if ps = [] then () else begin
    let dead = ref [] in
    List.iter (fun push ->
      try ignore (push (Some msg))
      with _ -> dead := push :: !dead
    ) ps;
    if !dead <> [] then begin
      Mutex.lock raw_pushers_mutex;
      raw_pushers := List.filter (fun p -> not (List.memq p !dead)) !raw_pushers;
      Mutex.unlock raw_pushers_mutex
    end
  end

(** Creates a bounded subscriber stream (capacity 16) for incoming messages.
    Returns a [subscription] with the stream and a [close] function that
    removes the subscriber from the global list. Messages are silently
    dropped when the stream buffer is full. *)
let subscribe_market_data () =
  let (stream, push_source) = Lwt_stream.create_bounded 16 in
  (* Non-blocking push wrapper. Returns true on success, false if dropped. *)
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
  ({ stream; close } : subscription)

let subscribe_raw_market_data () =
  let (stream, push_source) = Lwt_stream.create_bounded 16 in
  let push_fn item =
    match item with
    | None ->
        push_source#close;
        true
    | Some msg ->
        let p = push_source#push msg in
        if Lwt.is_sleeping p then begin
          Lwt.cancel p;
          false
        end else
          true
  in
  Mutex.lock raw_pushers_mutex;
  raw_pushers := push_fn :: !raw_pushers;
  Mutex.unlock raw_pushers_mutex;
  let close () =
    Mutex.lock raw_pushers_mutex;
    raw_pushers := List.filter (fun p -> p != push_fn) !raw_pushers;
    Mutex.unlock raw_pushers_mutex
  in
  ({ stream; close } : raw_subscription)

(** Sends a JSON message over the active WebSocket connection.
    No-op with a warning if not currently connected. *)
let subscribe json =
  Lwt_mutex.with_lock connection_mutex (fun () ->
    match !active_connection with
    | Some conn ->
        let msg = Yojson.Safe.to_string json in
        Logging.debug_f ~section "Sending subscription: %s" msg;
        Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg ())
    | None ->
        Logging.warn ~section "Cannot subscribe: WebSocket not connected";
        Lwt.return_unit
  )

(** Sends subscription messages for all configured channels.
    If [wallet] is non-empty,
    subscribes to user-specific channels: webData2, userEvents, spotState,
    userFills, userFundings, userNonFundingLedgerUpdates, orderUpdates.
    Subscribes to l2Book for each symbol, using the coin identifier
    resolved via [Hyperliquid_instruments_feed]. *)
let subscribe_to_feeds ~symbols ~wallet =
  let%lwt () = 
    if wallet <> "" then
      let%lwt () = subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "webData2"); ("user", `String wallet)])]) in
      let%lwt () = subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "userEvents"); ("user", `String wallet)])]) in
      let%lwt () = subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "spotState"); ("user", `String wallet)])]) in
      let%lwt () = subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "userFills"); ("user", `String wallet)])]) in
      let%lwt () = subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "userFundings"); ("user", `String wallet)])]) in
      let%lwt () = subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "userNonFundingLedgerUpdates"); ("user", `String wallet)])]) in
      subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "orderUpdates"); ("user", `String wallet)])])
    else Lwt.return_unit
  in
  
  Lwt_list.iter_s (fun symbol ->
    (* Perps use the base name (e.g. "HYPE"); spot pairs use "@N" format. *)
    let coin = Hyperliquid_instruments_feed.get_subscription_coin symbol in
    Logging.info_f ~section "Subscribing to l2Book for %s (coin=%s)" symbol coin;
    subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "l2Book"); ("coin", `String coin)])])
  ) symbols

(** Processes a single WebSocket frame.
    Text frames are parsed as JSON, then either matched to a pending
    request via the response table or broadcast to all subscribers.
    Close frames trigger connection teardown and subscriber cleanup. *)
let handle_frame ~on_heartbeat (frame : Websocket.Frame.t) =
  match frame.Websocket.Frame.opcode with
  | Websocket.Frame.Opcode.Text ->
      Concurrency.Tick_event_bus.publish_tick ();
      on_heartbeat ();
      let content = frame.Websocket.Frame.content in
      if String.starts_with ~prefix:"{\"channel\":\"l2Book\"," content then begin
        broadcast_raw_message content;
        Lwt.return_unit
      end else begin
        (try
          let json = Yojson.Safe.from_string content in
        let channel = 
          let open Yojson.Safe.Util in
          try member "channel" json |> to_string with _ -> ""
        in
        
        (* Update pong tracking on pong responses. *)
        if channel = "pong" then begin
          last_pong_time := Unix.gettimeofday ();
          (try Lwt_condition.broadcast pong_condition () with _ -> ())
        end;

        (* Filter high-frequency channels from debug logging. *)
        let is_noisy = List.mem channel ["pong"] in

        if not is_noisy then begin
          let log_msg =
            if String.length frame.Websocket.Frame.content > 1000 then
              String.sub frame.Websocket.Frame.content 0 1000 ^ "... [TRUNCATED]"
            else frame.Websocket.Frame.content
          in
          if channel = "webData2" then
            Logging.debug_f ~section "webData2 received: %s" log_msg
          else
            Logging.debug_f ~section "Raw WS message: %s" log_msg
        end;
        
        (* Attempt to extract an integer ID from the JSON payload.
           Searches top-level, then inside "data" and "response" keys. *)
        let is_response = 
          let id_opt = 
            let rec find_id node =
              match node with
              | `Assoc pairs ->
                  (match List.assoc_opt "id" pairs with
                   | Some (`Int id) -> Some id
                   | Some (`Intlit s) -> (try Some (int_of_string s) with _ -> None)
                   | Some (`String s) -> (try Some (int_of_string s) with _ -> None)
                   | Some (`Float f) -> Some (int_of_float f)
                   | _ -> 
                       (match List.assoc_opt "data" pairs with
                        | Some next_node -> find_id next_node
                        | None -> 
                            (match List.assoc_opt "response" pairs with
                             | Some next_node -> find_id next_node
                             | None -> None)))
              | _ -> None
            in
            find_id json
          in
          match id_opt with
          | Some id ->
              (* Wake the corresponding [send_request] waiter. *)
              Lwt.async (fun () ->
                Lwt_mutex.with_lock responses_mutex (fun () ->
                  try
                    let (wakener, _) = Response_table.find responses id in
                    Response_table.remove responses id;
                    Lwt.wakeup_later wakener json;
                    Lwt.return_unit
                  with Not_found -> 
                    if channel = "post" then
                      Logging.info_f ~section "Received 'post' response for unknown req_id %d: %s" id (Yojson.Safe.to_string json);
                    Lwt.return_unit
                )
              );
              true
          | None -> 
              if channel = "post" then
                Logging.warn_f ~section "Received 'post' message without identifiable ID: %s" (Yojson.Safe.to_string json);
              false
        in
        
        (* Messages without a matching request ID are broadcast to subscribers. *)
        if not is_response then begin
          if not is_noisy then
            if channel = "webData2" then
              Logging.debug ~section "Broadcasting webData2 data message"
            else
              Logging.debug_f ~section "Broadcasting data message: channel=%s" channel;
          broadcast_message json
        end
      with exn ->
        Logging.warn_f ~section "Failed to parse WS message: %s" (Printexc.to_string exn));
      Lwt.return_unit
      end
  | Websocket.Frame.Opcode.Close ->
      Logging.debug_f ~section "WebSocket connection closed by server";
      Lwt_mutex.with_lock connection_mutex (fun () ->
        active_connection := None;
        Atomic.set is_connected_ref false;
        Lwt.return_unit
      ) >>= fun () ->
      fail_all_pending "Connection closed by server";
      close_all_subscribers ();
      signal_new_data ();
      Lwt.return_unit
  | _ -> Lwt.return_unit

(** Establishes a TLS WebSocket connection to the Hyperliquid API.
    Resolves the hostname, connects via TLS, then enters a read loop
    that dispatches frames through [handle_frame]. On connection loss,
    cleans up state, fails pending requests, closes subscribers, and
    invokes [on_failure]. The supervisor is responsible for reconnection. *)
let connect_and_monitor ~on_failure ~on_connected ~on_heartbeat ~testnet =
  let base_url = if testnet then "api.hyperliquid-testnet.xyz" else "api.hyperliquid.xyz" in
  let url = Printf.sprintf "wss://%s/ws" base_url in
  let hostname = base_url in
  let port = 443 in
  Logging.info_f ~section "Connecting to Hyperliquid WebSocket: %s" url;
  let uri = Uri.of_string url in
  Lwt.catch (fun () ->
    Lwt_unix.getaddrinfo hostname (string_of_int port) [Unix.AI_FAMILY Unix.PF_INET] >>= fun addresses ->
    let ip = match addresses with
      | {Unix.ai_addr = Unix.ADDR_INET (addr, _); _} :: _ ->
          Ipaddr_unix.of_inet_addr addr
      | _ -> failwith (Printf.sprintf "Failed to resolve %s" hostname)
    in
    let client = `TLS (`Hostname hostname, `IP ip, `Port port) in
    let ctx = Lazy.force Conduit_lwt_unix.default_ctx in
    Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->
    Lwt_mutex.with_lock connection_mutex (fun () ->
      active_connection := Some conn;
      Atomic.set is_connected_ref true;
      (* Signal waiters blocked in [wait_for_connected].
         Guard on [is_empty] to avoid queuing a second value on rapid reconnect. *)
      if Lwt_mvar.is_empty connected_wakeup then
        Lwt.async (fun () -> Lwt_mvar.put connected_wakeup ());
      Lwt.return_unit
    ) >>= fun () ->
    on_connected ();
    reset_ping_failures ();
    
    (* Ping/pong monitoring is owned by the supervisor, not this module. *)

    let stream = Lwt_stream.from (fun () ->
      if not (Atomic.get is_connected_ref) then Lwt.return_none
      else Lwt.catch (fun () ->
        Websocket_lwt_unix.read conn >>= fun frame ->
        Lwt.return_some frame
      ) (function
        | End_of_file -> Lwt.return_none
        | exn -> Lwt.fail exn)
    ) in

    let process_frame frame =
      Lwt.async (fun () ->
        Lwt.catch
          (fun () -> handle_frame ~on_heartbeat frame)
          (fun exn -> Logging.error_f ~section "Error handling Hyperliquid frame: %s" (Printexc.to_string exn); Lwt.return_unit)
      )
    in

    let done_p =
      Lwt.catch
        (fun () -> Concurrency.Lwt_util.consume_stream process_frame stream)
        (fun _exn -> Lwt.return_unit)
    in
    Lwt.catch (fun () -> done_p) (function
      | End_of_file ->
          Logging.warn ~section "WebSocket connection closed unexpectedly (End_of_file)";
          Lwt_mutex.with_lock connection_mutex (fun () ->
            active_connection := None;
            Atomic.set is_connected_ref false;
            Lwt.return_unit
          ) >>= fun () ->
          fail_all_pending "Connection closed unexpectedly (End_of_file)";
          close_all_subscribers ();
          Lwt.fail_with "Connection closed unexpectedly (End_of_file)"
      | exn ->
          Logging.error_f ~section "WebSocket read error: %s" (Printexc.to_string exn);
          Lwt_mutex.with_lock connection_mutex (fun () ->
            active_connection := None;
            Atomic.set is_connected_ref false;
            Lwt.return_unit
          ) >>= fun () ->
          fail_all_pending (Printexc.to_string exn);
          close_all_subscribers ();
          Lwt.fail exn
    ) >>= fun () ->
    (* Normal read loop exit (e.g. server-initiated close frame).
       Treated as a failure so the supervisor triggers reconnection. *)
    Lwt_mutex.with_lock connection_mutex (fun () ->
      active_connection := None;
      Atomic.set is_connected_ref false;
      Lwt.return_unit
    ) >>= fun () ->
    fail_all_pending "WebSocket closed by server";
    close_all_subscribers ();
    Lwt.fail_with "WebSocket closed by server"
  ) (fun exn ->
    let error_msg = Printexc.to_string exn in
    Logging.error_f ~section "WebSocket connection error: %s" error_msg;
    Atomic.set is_connected_ref false;
    fail_all_pending error_msg;
    close_all_subscribers ();
    on_failure error_msg;
    Lwt.return_unit
  )

(** Gracefully closes the Hyperliquid WebSocket connection.
    Clears the active connection reference, marks the connection as
    disconnected, fails all pending request waiters, closes all subscriber
    streams, and tears down the underlying TLS transport. *)
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
      Logging.info ~section "Closing Hyperliquid WebSocket connection";
      fail_all_pending "Client requested close";
      close_all_subscribers ();
      signal_new_data ();
      Lwt.catch
        (fun () -> Websocket_lwt_unix.close_transport conn)
        (fun _ -> Lwt.return_unit)

(** Sends a JSON request over the WebSocket and blocks until a response
    with the matching [req_id] arrives or [timeout_ms] elapses.
    Logs a warning when the response table exceeds 10 pending entries. *)
let send_request ~json ~req_id ~timeout_ms =
  let waiter, wakener = Lwt.wait () in
  Lwt_mutex.with_lock responses_mutex (fun () ->
    let pending_count = Response_table.length responses in
    if pending_count > 10 then
      Logging.warn_f ~section "Response table size is high: %d pending requests" pending_count;
    Response_table.add responses req_id (wakener, Unix.time ());
    Lwt.return_unit
  ) >>= fun () ->
  subscribe json >>= fun () ->
  
  let timeout = float_of_int timeout_ms /. 1000.0 in
  Lwt.pick [
    waiter;
    (Lwt_unix.sleep timeout >>= fun () ->
      Lwt_mutex.with_lock responses_mutex (fun () ->
        if Response_table.mem responses req_id then begin
          Response_table.remove responses req_id;
          Lwt.return `Timeout
        end else Lwt.return `Resolved
      ) >>= function
      | `Timeout -> Lwt.fail_with (Printf.sprintf "Timeout waiting for response to req_id %d" req_id)
      | `Resolved -> waiter
    )
  ]

(** Sends a ping and waits for a pong within [timeout_ms].
    Returns [true] if a pong was received, [false] on timeout or send failure.
    Uses [pong_condition] since Hyperliquid pong responses carry no id field.
    Falls back to checking [last_pong_time] in case the condition signal
    was delivered before this function began waiting. *)
let send_ping ~req_id:_ ~timeout_ms =
  let ping_msg = `Assoc [("method", `String "ping")] in
  let send_time = Unix.gettimeofday () in
  Lwt.catch (fun () ->
    subscribe ping_msg >>= fun () ->
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