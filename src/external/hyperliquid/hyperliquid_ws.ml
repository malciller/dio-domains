(** Hyperliquid WebSocket client - market data subscription hub *)

open Lwt.Infix

let section = "hyperliquid_ws"

(** Subscription handle returned to consumers *)
type subscription = {
  stream: Yojson.Safe.t Lwt_stream.t;
  close: unit -> unit;
}

let is_connected_ref = Atomic.make false
let is_connected () = Atomic.get is_connected_ref

(** Mvar filled each time a new connection is established.
    Consumers take from it to wait without polling; the mvar is
    refilled after each successful connect so it remains available
    for the next caller (e.g. after reconnect). *)
let connected_wakeup : unit Lwt_mvar.t = Lwt_mvar.create_empty ()

(** Block until the WS is connected (or return immediately if already connected).
    Uses connected_wakeup for zero-latency notification instead of sleep polling. *)
let wait_for_connected () =
  if Atomic.get is_connected_ref then Lwt.return_unit
  else Lwt_mvar.take connected_wakeup

let active_connection = ref None
let connection_mutex = Lwt_mutex.create ()

(** Condition variable used to unblock domain workers waiting for Hyperliquid data.
    Signaled on every incoming WS frame and on WS disconnect.
    Delegates to Concurrency.Exchange_wakeup which is visible from all library layers. *)
let signal_new_data () = Concurrency.Exchange_wakeup.signal ()
let wait_for_data () = Concurrency.Exchange_wakeup.wait ()

(** Global subscriber list.
    Each pusher is a function that accepts a Yojson.Safe.t option.
    Bounded internally: returns true if the push succeeded, false if dropped. *)
let pushers : (Yojson.Safe.t option -> bool) list ref = ref []
let pushers_mutex = Mutex.create ()

module Response_table = Hashtbl.Make (struct
  type t = int
  let equal = Int.equal
  let hash = Hashtbl.hash
end)

let responses : (Yojson.Safe.t Lwt.u * float) Response_table.t = Response_table.create 32
let responses_mutex = Lwt_mutex.create ()

(** Consecutive ping failures tracked by supervisor *)
let ping_failures = Atomic.make 0
let reset_ping_failures () = Atomic.set ping_failures 0
let get_ping_failures () = Atomic.get ping_failures
let incr_ping_failures () = Atomic.incr ping_failures

(** Pong tracking for active ping/pong monitoring.
    Hyperliquid pong responses are {"channel": "pong"} with NO id field,
    so we cannot use the ID-matched send_request mechanism. Instead we
    fire-and-forget the ping and wait on a dedicated condition variable. *)
let last_pong_time = ref 0.0
let pong_condition = Lwt_condition.create ()

(** Fail all pending response waiters on disconnect — mirrors Kraken's
    fail_all_pending / reset_state pattern. Without this, in-flight
    send_request calls hang until their individual timeouts (up to 5s). *)
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

(** Clean up stale response table entries older than 30s — prevents memory leaks.
    Mirrors Kraken's cleanup_stale_response_entries pattern. *)
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

(** Close all subscriber streams — called on WS disconnect to unblock
    Lwt_stream.iter in processor tasks and prevent orphaned push closures
    from leaking memory. Pushing None terminates the Lwt_stream. *)
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
  end

(** Broadcast to all subscribers.
    Push is non-blocking: if a consumer's bounded stream is full, the message
    is silently dropped for that consumer. This prevents unbounded memory growth
    from slow consumers accumulating Yojson.Safe.t trees. *)
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
  (* Evict any dead pushers *)
  if !dead <> [] then begin
    Mutex.lock pushers_mutex;
    pushers := List.filter (fun p -> not (List.memq p !dead)) !pushers;
    Mutex.unlock pushers_mutex
  end;
  signal_new_data ()  (* wake any domain workers blocked in wait_for_data *)

(** Subscribe to all incoming market data messages.
    Uses a bounded stream (capacity 64) to prevent unbounded memory growth.
    When the stream is full, new messages are silently dropped — consumers
    will process the next available message when they catch up. *)
let subscribe_market_data () =
  let (stream, push_source) = Lwt_stream.create_bounded 16 in
  (* Non-blocking push wrapper compatible with pushers list signature.
     Returns true if pushed, false if dropped/closed. *)
  let push_fn item =
    match item with
    | None ->
        (* Close signal — always deliver *)
        push_source#close;
        true
    | Some json ->
        let p = push_source#push json in
        if Lwt.is_sleeping p then begin
          (* Stream is full — drop this message *)
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

(** Send a subscription message to the active connection *)
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

(** Send initial subscriptions to Hyperliquid *)
let subscribe_to_feeds ~symbols ~wallet =
  let%lwt () = subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "allMids")])]) in
  
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
    (* Use instruments feed to get correct coin identifier:
       perps use base name (e.g. "HYPE"), spot pairs use "@N" format *)
    let coin = Hyperliquid_instruments_feed.get_subscription_coin symbol in
    Logging.info_f ~section "Subscribing to l2Book for %s (coin=%s)" symbol coin;
    subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "l2Book"); ("coin", `String coin)])])
  ) symbols

let handle_frame ~on_heartbeat (frame : Websocket.Frame.t) =
  match frame.Websocket.Frame.opcode with
  | Websocket.Frame.Opcode.Text ->
      Concurrency.Tick_event_bus.publish_tick ();
      on_heartbeat ();
      (try
        let json = Yojson.Safe.from_string frame.Websocket.Frame.content in
        let channel = 
          let open Yojson.Safe.Util in
          try member "channel" json |> to_string with _ -> ""
        in
        
        (* Detect pong responses for active ping monitoring *)
        if channel = "pong" then begin
          last_pong_time := Unix.gettimeofday ();
          (try Lwt_condition.broadcast pong_condition () with _ -> ())
        end;

        (* post responses are logged in hyperliquid_module as "Hyperliquid order raw response" *)
        (* pong is noisy, but we want to see post for now for debugging timeouts *)
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
        
        (* Check if it's a response to a request *)
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
                       (* If not at this level, check inside "data" or "response" *)
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
              (* It's a response with an ID *)
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
        
        if not is_response then begin
          (* Debug log for data messages occasionally *)
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
  | Websocket.Frame.Opcode.Close ->
      Logging.info ~section "WebSocket connection closed by server";
      Lwt_mutex.with_lock connection_mutex (fun () ->
        active_connection := None;
        Atomic.set is_connected_ref false;
        Lwt.return_unit
      ) >>= fun () ->
      fail_all_pending "Connection closed by server";
      close_all_subscribers ();
      signal_new_data ();  (* unblock waiting domain workers so they re-check is_running *)
      Lwt.return_unit
  | _ -> Lwt.return_unit

(** Connect to Hyperliquid WebSocket and broadcast messages to all subscribers. *)
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
      (* Signal any waiters blocked in wait_for_connected().
         is_empty guard ensures we don't queue a blocking Lwt continuation
         when the mvar already holds a signal (rapid reconnect scenario). *)
      if Lwt_mvar.is_empty connected_wakeup then
        Lwt.async (fun () -> Lwt_mvar.put connected_wakeup ());
      Lwt.return_unit
    ) >>= fun () ->
    on_connected ();
    reset_ping_failures ();
    
    (* No internal heartbeat loop — supervisor owns ping/pong monitoring,
       matching the Kraken auth WS pattern for consistent supervision. *)

    let done_p, done_u = Lwt.wait () in
    let rec loop () =
      Lwt.catch (fun () ->
        Websocket_lwt_unix.read conn >>= fun frame ->
        handle_frame ~on_heartbeat frame >>= fun () ->
        if Atomic.get is_connected_ref then begin
          (* Spawn next iteration independently — breaks Forward chain *)
          Lwt.async loop;
          Lwt.return_unit
        end else begin
          Lwt.wakeup_later done_u ();
          Lwt.return_unit
        end
      ) (fun exn ->
        Lwt.wakeup_later_exn done_u exn;
        Lwt.return_unit
      )
    in
    Lwt.async loop;
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
    (* Read loop exited normally (e.g. server Close frame).
       Treat this as a connection failure so supervisor triggers restart. *)
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

(** Send a request and wait for a response using the given req_id. *)
let send_request ~json ~req_id ~timeout_ms =
  let waiter, wakener = Lwt.wait () in
  Lwt_mutex.with_lock responses_mutex (fun () ->
    (* Backpressure check: clean up stale entries if table is growing large *)
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

(** Send a ping and wait for pong — used by supervisor for active health monitoring.
    Hyperliquid pong = {"channel":"pong"} with NO id field, so we use a
    dedicated condition variable instead of the ID-matched send_request. *)
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
       (* Check if pong arrived but we missed the condition signal *)
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