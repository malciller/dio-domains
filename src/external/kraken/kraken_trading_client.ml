(**
   Kraken Trading WebSocket Client.
   Manages an authenticated WebSocket connection to the Kraken v2 API for order
   placement, amendment, and cancellation. Provides connection lifecycle management,
   request/response correlation via req_id, automatic reconnection with subscription
   replay, and stale request cleanup.
*)

open Lwt.Infix
open Concurrency


let section = "kraken_trading_client"

open Kraken_common_types

(** Re-export of shared Conduit TLS context constructor. *)
let get_conduit_ctx = get_conduit_ctx

(** Atomic flag indicating that a graceful shutdown has been requested. *)
let shutdown_requested = Atomic.make false

(** Atomic flag preventing concurrent execution of cleanup handlers. *)
let cleanup_handler_started = Atomic.make false

(** Sets [shutdown_requested] to true, causing reader and periodic loops to exit. *)
let signal_shutdown () =
  Atomic.set shutdown_requested true

let string_contains str substr =
  let str_len = String.length str and substr_len = String.length substr in
  if substr_len > str_len then false
  else
    let rec loop i =
      if i + substr_len > str_len then false
      else if String.sub str i substr_len = substr then true
      else loop (i + 1)
    in
    loop 0

module Response_table = Hashtbl.Make (struct
  type t = int
  let equal = Int.equal
  let hash = Hashtbl.hash
end)

module Heartbeat_bus = Event_bus.Make (struct
  type t = unit
end)

module Connection_bus = Event_bus.Make (struct
  type t = [ `Connected | `Disconnected of string ]
end)

type connection_with_generation = {
  conn: Websocket_lwt_unix.conn;
  generation: int;
}

module RingBuffer = Ring_buffer.RingBuffer
let message_buffer = RingBuffer.create 32
let message_condition = Lwt_condition.create ()

type state = {
  mutex: Lwt_mutex.t;
  mutable conn: connection_with_generation option;
  mutable connecting: bool;
  mutable connection_generation: int;
  responses: (Kraken_common_types.ws_response Lwt.u * string * float) Response_table.t;  (** Pending requests keyed by req_id. Values: (wakener, expected_method, send_timestamp). *)
  mutable on_failure: (string -> unit) option;
  connected: bool Atomic.t;
  mutable subscriptions: Yojson.Safe.t list; (** Registered subscription messages replayed on each reconnection. *)
  connection_ready: unit Lwt_condition.t;  (** Broadcast when a connection attempt completes, whether successful or failed. *)
}

let state = {
  mutex = Lwt_mutex.create ();
  conn = None;
  connecting = false;
  connection_generation = 0;
  responses = Response_table.create 32;
  on_failure = None;
  connected = Atomic.make false;
  subscriptions = [];
  connection_ready = Lwt_condition.create ();
}

(** Condition broadcast when the reader loop terminates for any reason.
    [ensure_connection] blocks on this signal so the supervisor detects
    connection loss without holding a persistent promise reference. *)
let reader_done : unit Lwt_condition.t = Lwt_condition.create ()

let get_message_buffer () = message_buffer
let get_message_condition () = message_condition

let heartbeat_bus = Heartbeat_bus.create "kraken_trading_heartbeat"
let connection_bus = Connection_bus.create "kraken_trading_connection"

let heartbeat_stream () = Heartbeat_bus.subscribe heartbeat_bus
let connection_stream () = Connection_bus.subscribe connection_bus

let notify_heartbeat () = Heartbeat_bus.publish heartbeat_bus ()
let notify_connection status = Connection_bus.publish connection_bus status

let rec json_to_string_precise ?field_name symbol json =
  match json with
  | `Null -> "null"
  | `Bool b -> if b then "true" else "false"
  | `Int i -> string_of_int i
  | `Float f ->
      let precision =
        match symbol with
        | Some sym ->
            (match Kraken_instruments_feed.get_precision_info sym with
             | Some (price_precision, qty_precision) ->
                 (match field_name with
                  | Some name when string_contains name "price" -> price_precision
                  | Some name when string_contains name "qty" -> qty_precision
                  | Some name when string_contains name "order_qty" -> qty_precision
                  | Some name when string_contains name "limit_price" -> price_precision
                  | Some name when string_contains name "trigger_price" -> price_precision
                  | Some name when string_contains name "display_qty" -> qty_precision
                  | _ -> qty_precision)
             | None -> 8)
        | None -> 8
      in
      Printf.sprintf "%.*f" precision f
  | `String s ->
      "\""
      ^ (String.concat "" (List.map (function
          | '"' -> "\\\""
          | '\\' -> "\\\\"
          | '\b' -> "\\b"
          | '\012' -> "\\f"
          | '\n' -> "\\n"
          | '\r' -> "\\r"
          | '\t' -> "\\t"
          | c -> String.make 1 c
        ) (List.init (String.length s) (String.get s))))
      ^ "\""
  | `List items ->
      "[" ^ String.concat "," (List.map (json_to_string_precise ?field_name symbol) items) ^ "]"
  | `Assoc pairs ->
      "{" ^ String.concat "," (List.map (fun (k, v) ->
        "\"" ^ k ^ "\":" ^ json_to_string_precise ~field_name:k symbol v) pairs) ^ "}"
  | `Intlit s -> s
  | `Tuple _ | `Variant _ -> failwith "json_to_string_precise: Tuple and Variant not supported"

let parse_ws_response json : Kraken_common_types.ws_response =
  let method_ = Yojson.Safe.Util.(member "method" json |> to_string) in
  if String.equal method_ "pong" then begin
    let req_id = Yojson.Safe.Util.(member "req_id" json |> to_int_option) in
    let time_in = Yojson.Safe.Util.(member "time_in" json |> to_string) in
    let time_out = Yojson.Safe.Util.(member "time_out" json |> to_string) in
    { method_; success = true; req_id; time_in; time_out; result = None; error = None; warnings = None }
  end else begin
    let success = Yojson.Safe.Util.(member "success" json |> to_bool) in
    let req_id = Yojson.Safe.Util.(member "req_id" json |> to_int_option) in
    let time_in = Yojson.Safe.Util.(member "time_in" json |> to_string) in
    let time_out = Yojson.Safe.Util.(member "time_out" json |> to_string) in
    let result = Yojson.Safe.Util.(member "result" json |> Option.some) in
    let error = Yojson.Safe.Util.(member "error" json |> to_string_option) in
    let warnings =
      match Yojson.Safe.Util.member "warnings" json with
      | `List lst -> Some (List.map Yojson.Safe.Util.to_string lst)
      | _ -> None
    in
    { method_; success; req_id; time_in; time_out; result; error; warnings }
  end

let resolve_response req_id (response : Kraken_common_types.ws_response) =

  (* Acquire mutex only for the table lookup and removal. *)
  Lwt_mutex.with_lock state.mutex (fun () ->

    try
      let wakener, expected_method, _timestamp = Response_table.find state.responses req_id in
      Response_table.remove state.responses req_id;

      Lwt.return (Some (wakener, expected_method))
    with Not_found -> begin

      Lwt.return None
    end
  ) >>= function
  | Some (wakener, expected_method) ->
      (* Validate that the response method matches the expected method. ping/pong is an allowed alias. *)
      let is_valid_response =
        expected_method = response.method_ ||
        (expected_method = "ping" && response.method_ = "pong")
      in
      if not is_valid_response then begin
        Logging.error_f ~section "Response method mismatch for req_id=%d: expected '%s', got '%s' (response discarded)"
          req_id expected_method response.method_;
        Lwt.return_unit
      end else begin
        (* Resolve the promise outside the mutex to reduce contention. *)
        (try
          Lwt.wakeup_later wakener response
        with Invalid_argument _ ->
          (* Promise already resolved, typically by a preceding timeout. *)
          Logging.debug_f ~section "Promise for req_id=%d was already resolved (likely timeout), response discarded" req_id);
        Lwt.return_unit
      end
  | None ->
      Logging.warn_f ~section "No waiter found for req_id=%d (response arrived after timeout, method=%s, success=%b)"
        req_id response.method_ response.success;
      Lwt.return_unit

let fail_all_pending reason =
  let pending =
    Lwt_mutex.with_lock state.mutex (fun () ->
      let wakers = Response_table.fold (fun _ (wak, _, _) acc -> wak :: acc) state.responses [] in
      Response_table.clear state.responses;
      Lwt.return wakers
    )
  in
  pending >|= List.iter (fun wak ->
    try
      Lwt.wakeup_later_exn wak (Failure reason)
    with Invalid_argument _ ->
      Logging.debug ~section "Attempted to wake already-resolved promise during fail_all_pending"
  )

let reset_state conn ~notify_failure reason =
  (* Snapshot and clear connection state under the mutex. *)
  Lwt_mutex.with_lock state.mutex (fun () ->
    match state.conn with
    | Some current when current.conn == conn ->
        let old_generation = state.connection_generation in
        let new_generation = old_generation + 1 in
        let wakers = Response_table.fold (fun _ (wak, _, _) acc -> wak :: acc) state.responses [] in
        let pending_count = Response_table.length state.responses in
        ignore pending_count;
        Response_table.clear state.responses;
        state.conn <- None;
        state.connecting <- false;
        state.connection_generation <- new_generation;
        (* Unblock any ensure_connection callers waiting in the `Wait branch. *)
        Lwt_condition.broadcast state.connection_ready ();
        Atomic.set state.connected false;
        let failure_cb = if notify_failure then state.on_failure else None in
        Lwt.return (wakers, true, failure_cb, pending_count)
    | _ ->

        state.connecting <- false;
        Lwt_condition.broadcast state.connection_ready ();
        Lwt.return ([], false, None, 0)
  ) >>= fun (wakers, should_reset, failure_cb, pending_count) ->
  if not should_reset then Lwt.return_unit
  else begin
    (* Reader loop terminates naturally upon detecting a generation mismatch. *)
    ignore pending_count;

    (* Tear down the underlying TCP/TLS transport. *)
    Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit) >>= fun () ->

    (* Fail all pending request promises with the reset reason. *)

    List.iter (fun wak ->
      try
        Lwt.wakeup_later_exn wak (Failure reason)
      with Invalid_argument _ ->
        ()
    ) wakers;


    notify_connection (`Disconnected reason);
    (match failure_cb with Some f -> f reason | None -> ());
    Lwt.return_unit
  end


let handle_frame frame ~expected_generation =
  Concurrency.Tick_event_bus.publish_tick ();
  notify_heartbeat ();


  (* Parse the frame JSON, then atomically verify generation and response table membership. *)
  (try
     let json = Yojson.Safe.from_string frame.Websocket.Frame.content in
     let open Yojson.Safe.Util in
     let method_opt = member "method" json |> to_string_option in
     match method_opt with
     | Some _ ->
         let response = parse_ws_response json in
         (match response.req_id with
          | Some req_id ->
              (* Atomically check generation match or pending req_id presence; only discard if neither holds. *)
              Lwt_mutex.with_lock state.mutex (fun () ->
                let current_gen = state.connection_generation in
                let req_id_exists = Response_table.mem state.responses req_id in
                (* Process if current generation matches or req_id is still pending (delayed response). *)
                let should_process = expected_generation = current_gen || req_id_exists in
                Lwt.return (should_process, req_id_exists, current_gen)
              ) >>= fun (should_process, _, _) ->
              if not should_process then begin
                Lwt.return_unit
              end else begin


                (* Resolve synchronously to prevent a timeout race. *)
                resolve_response req_id response >>= fun () ->
                Lwt.return_unit
              end
          | None ->

             Lwt.return_unit)
     | None ->
         (match member "channel" json |> to_string_option with
          | Some "heartbeat" -> Lwt.return_unit
          | _ ->
              RingBuffer.write message_buffer json;
              Lwt_condition.broadcast message_condition ();
              Lwt.return_unit)
   with exn ->
     Logging.error ~section
       (Printf.sprintf "Failed to handle trading frame synchronously: %s" (Printexc.to_string exn));
     Lwt.return_unit) >>= fun () ->

  Lwt.return_unit

(** Starts the WebSocket reader loop for a given connection generation.
    Each frame read spawns the next iteration via [Lwt.async] to prevent
    promise chain accumulation. Signals [reader_done] on termination. *)
let start_reader conn generation =

  let stream = Lwt_stream.from (fun () ->
    if Atomic.get shutdown_requested then Lwt.return_none
    else if state.connection_generation <> generation then Lwt.return_none
    else Lwt.catch (fun () ->
      Websocket_lwt_unix.read conn >>= fun frame ->
      Lwt.return_some frame
    ) (function
      | End_of_file -> Lwt.return_none
      | exn -> Lwt.fail exn)
  ) in

  let process_frame = function
    | { Websocket.Frame.opcode = Websocket.Frame.Opcode.Close; _ } ->
        Logging.warn_f ~section "Trading WebSocket closed by server (generation %d)" generation;
        Lwt.fail (Failure "Connection closed by server")
    | frame ->
        Lwt.catch
          (fun () -> handle_frame frame ~expected_generation:generation)
          (fun exn ->
             Logging.error_f ~section "Error handling Kraken trading frame: %s" (Printexc.to_string exn);
             Lwt.return_unit)
  in

  let done_p =
    Lwt.catch
      (fun () -> Concurrency.Lwt_util.consume_stream (fun frame -> Lwt.async (fun () -> process_frame frame)) stream)
      (fun exn ->
         match exn with
         | Failure msg when msg = "Connection closed by server" ->
             reset_state conn ~notify_failure:true "Connection closed by server"
         | _ ->
             let reason = Printf.sprintf "WebSocket read error (generation %d): %s" generation (Printexc.to_string exn) in
             Logging.error ~section reason;
             reset_state conn ~notify_failure:true reason)
  in

  Lwt.async (fun () ->
    done_p >>= fun () ->
    if Atomic.get shutdown_requested then begin
      Logging.info_f ~section "Reader loop shutting down (generation %d)" generation;
      Lwt_condition.broadcast reader_done ();
      Lwt.return_unit
    end else if state.connection_generation <> generation then begin
      Lwt_condition.broadcast reader_done ();
      Lwt.return_unit
    end else begin
      Logging.warn_f ~section "Trading WebSocket connection closed unexpectedly (End_of_file, generation %d)" generation;
      reset_state conn ~notify_failure:true "Connection closed unexpectedly (End_of_file)" >>= fun () ->
      Lwt_condition.broadcast reader_done ();
      Lwt.return_unit
    end
  );
  ()

let connect _token : Websocket_lwt_unix.conn Lwt.t =
  let uri = Uri.of_string "wss://ws-auth.kraken.com/v2" in
  Logging.info ~section "Connecting to Kraken authenticated WebSocket for trading...";
  Lwt_unix.getaddrinfo "ws-auth.kraken.com" "443" [ Unix.AI_FAMILY Unix.PF_INET ] >>= fun addresses ->
  let ip =
    match addresses with
    | { Unix.ai_addr = Unix.ADDR_INET (addr, _); _ } :: _ -> Ipaddr_unix.of_inet_addr addr
    | _ -> failwith "Failed to resolve ws-auth.kraken.com"
  in
  let client = `TLS (`Hostname "ws-auth.kraken.com", `IP ip, `Port 443) in
  let ctx = get_conduit_ctx () in
  Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->
  Logging.info ~section "Trading WebSocket connection established";
  Lwt.return conn

let ensure_connection ?on_failure ?on_connected token =

  Lwt_mutex.with_lock state.mutex (fun () ->
    (match on_failure with Some f -> state.on_failure <- Some f | None -> ());
    if state.conn <> None then begin
      (* Reuse existing connection; invoke on_connected callback if registered. *)

      (match on_connected with Some f -> f () | None -> ());
      Lwt.return `Already
    end else if state.connecting then begin

      Lwt.return `Wait
    end else begin
      Logging.info ~section "Starting new connection";
      state.connecting <- true;
      Lwt.return `Connect
    end
  ) >>= function
  | `Already -> Lwt.return_unit
  | `Wait ->
      (* Block on [connection_ready] condition until the in-progress attempt completes. *)
      let rec wait_for_connection () =
        Lwt_condition.wait state.connection_ready >>= fun () ->
        Lwt_mutex.with_lock state.mutex (fun () ->
          if state.conn <> None then begin
            (match on_connected with Some f -> f () | None -> ());
            Lwt.return true
          end else if state.connecting then
            Lwt.return false
          else
            (* Connection attempt failed; stop waiting. *)
            Lwt.return true
        ) >>= fun done_ ->
        if done_ then Lwt.return_unit else wait_for_connection ()
      in
      wait_for_connection ()
  | `Connect ->
      Lwt.catch
        (fun () ->
           connect token >>= fun conn ->
           Lwt_mutex.with_lock state.mutex (fun () ->
             let generation = state.connection_generation in
             state.connecting <- false;
             state.conn <- Some { conn; generation };
             Atomic.set state.connected true;
             Lwt_condition.broadcast state.connection_ready ();
             Lwt.return (Some (conn, generation)))
        )
        (fun exn ->
           Lwt_mutex.with_lock state.mutex (fun () ->
             state.connecting <- false;
             Lwt_condition.broadcast state.connection_ready ();
             Lwt.return_unit) >>= fun () ->
           Lwt.fail exn
        ) >>= function
      | None ->
          Logging.error ~section "Connection establishment failed, no connection returned";
          Lwt.return_unit
      | Some (conn, generation) ->
          Logging.info_f ~section "Connection established successfully (generation %d)" generation;
          start_reader conn generation;

          notify_connection `Connected;

          (* Replay all registered subscriptions on the new connection. *)
          Lwt_mutex.with_lock state.mutex (fun () ->
            Lwt.return state.subscriptions
          ) >>= fun subs ->
          Lwt_list.iter_s (fun sub ->
            let content = Yojson.Safe.to_string sub in

            Websocket_lwt_unix.write conn (Websocket.Frame.create ~content ())
          ) (List.rev subs) >>= fun () ->

          (* Invoke on_connected callback after subscription replay. *)
          (match on_connected with Some f -> f () | None -> ());

          (* Block on reader_done to keep this promise alive for the supervisor.
             Lwt_condition.wait creates a fresh promise each invocation,
             avoiding forwarding chain accumulation. *)
          Lwt_condition.wait reader_done >>= fun () ->
          Logging.warn_f ~section "Reader completed (generation %d), connection function returning" generation;
          Lwt.return_unit

let cleanup_mvar : unit Lwt_mvar.t = Lwt_mvar.create_empty ()

let request_cleanup () =
  if Lwt_mvar.is_empty cleanup_mvar then
    Lwt.async (fun () -> Lwt_mvar.put cleanup_mvar ())

let cleanup_stale_response_entries ~reason () =
  if Atomic.get shutdown_requested then Lwt.return_unit
  else
    Lwt.catch
      (fun () ->
        let now = Unix.time () in
        let stale_entries = ref [] in
        Lwt_mutex.with_lock state.mutex (fun () ->
          if Atomic.get shutdown_requested then
            Lwt.return_unit
          else begin
            Response_table.iter (fun req_id (wakener, expected_method, timestamp) ->
              if now -. timestamp > 30.0 then
                stale_entries := (req_id, wakener, expected_method, timestamp) :: !stale_entries
            ) state.responses;
            List.iter (fun (req_id, wakener, _, timestamp) ->
              Response_table.remove state.responses req_id;

              (* Fail the stale promise to release associated memory. *)
              try
                Lwt.wakeup_later_exn wakener (Failure (Printf.sprintf "Request timed out after %.1fs" (now -. timestamp)))
              with Invalid_argument _ ->
                ()
            ) !stale_entries;

            Lwt.return_unit
          end
        )
      )
      (fun exn ->
        if not (Atomic.get shutdown_requested) then
          Logging.error_f ~section "Error during response table cleanup (reason=%s): %s" reason (Printexc.to_string exn);
        Lwt.return_unit
      )

let periodic_tasks_started = Atomic.make false

let start_periodic_tasks () =
  if not (Atomic.exchange periodic_tasks_started true) then begin
    let rec run () =
      Lwt_mvar.take cleanup_mvar >>= fun () ->

      cleanup_stale_response_entries ~reason:"backpressure" () >>= fun () ->
      (* Spawn the next cleanup cycle asynchronously to break promise chain growth. *)
      Lwt.async run;
      Lwt.return_unit
    in
    Lwt.async run
  end

let send_message ~message_str ~req_id ~expected_method ~timeout_ms =
  (* Reject requests if a shutdown is in progress. *)
  if Atomic.get shutdown_requested then begin

    Lwt.fail_with (Printf.sprintf "Request req_id %d cancelled due to shutdown" req_id)
  end else begin

    Lwt_mutex.with_lock state.mutex (fun () ->
      match state.conn with
      | None -> begin
          Logging.warn_f ~section "Cannot send request req_id=%d: WebSocket not connected" req_id;
          Lwt.return (Error (Failure "Trading WebSocket not connected"))
        end
      | Some conn_with_gen -> begin

          (* Guard against req_id collision before registering the wakener. *)
          if Response_table.mem state.responses req_id then begin
            Logging.error_f ~section "Request ID collision detected: req_id %d already in use" req_id;
            Lwt.return (Error (Failure (Printf.sprintf "Request ID collision detected: req_id %d already in use" req_id)))
          end else begin
            let waiter, wakener = Lwt.wait () in
            Response_table.add state.responses req_id (wakener, expected_method, Unix.time ());
            let pending_count = Response_table.length state.responses in

            (* Trigger cleanup if the pending request count exceeds the threshold. *)
            if pending_count > 10 then begin
              Logging.warn_f ~section "Response table size is high: %d pending requests" pending_count;
              request_cleanup ()
            end;
            Lwt.catch
              (fun () ->
                 Websocket_lwt_unix.write conn_with_gen.conn (Websocket.Frame.create ~content:message_str ()) >|= fun () ->
                 Ok waiter)
              (fun exn ->
                 Logging.error_f ~section "Request req_id=%d: failed to write to WebSocket: %s" req_id (Printexc.to_string exn);
                 Response_table.remove state.responses req_id;
                 Lwt.return (Error exn))
          end
        end
    ) >>= function
    | Error exn -> Lwt.fail exn
    | Ok waiter ->
        (* Register a cancellation callback to evict the entry from the response table. *)
        Lwt.on_cancel waiter (fun () ->
          Lwt.async (fun () ->
            Lwt_mutex.with_lock state.mutex (fun () ->
              if Response_table.mem state.responses req_id then begin
                Response_table.remove state.responses req_id;
                Lwt.return_unit
              end else Lwt.return_unit
            )
          )
        );

        let timeout = float_of_int timeout_ms /. 1000.0 in

        Lwt.pick [
          (waiter >|= fun response -> `Response response);
          (Lwt_unix.sleep timeout >|= fun () -> `Timeout)
        ] >>= function
        | `Response response ->

            Lwt.return response
        | `Timeout ->
            (* Atomically check pending status before evicting the timed-out entry. *)
            Lwt_mutex.with_lock state.mutex (fun () ->
              let still_pending = Response_table.mem state.responses req_id in
              if still_pending then begin
                Response_table.remove state.responses req_id;

                Lwt.return `Was_pending
              end else begin

                Lwt.return `Already_removed
              end) >>= fun removal_status ->

            match removal_status with
            | `Was_pending ->
                (* Genuine timeout: request was still in the response table. *)
                Logging.warn_f ~section "Request timeout: req_id=%d, timeout_ms=%d, sent %.3fs ago" req_id timeout_ms timeout;

                Lwt.cancel waiter;
                Lwt.fail_with (Printf.sprintf "Timeout waiting for response to req_id %d" req_id)
            | `Already_removed ->
                (* Entry removed from table by resolve_response or reset_state.
                   Yield to scheduler to let pending Lwt.wakeup_later execute. *)

                Lwt.pause () >>= fun () ->
                if Lwt.is_sleeping waiter then begin
                  (* Waiter still unresolved after yield; should not occur in normal operation. *)
                  Logging.warn_f ~section "Request timeout (post-yield): req_id=%d, timeout_ms=%d, sent %.3fs ago" req_id timeout_ms timeout;
                  Lwt.fail_with (Printf.sprintf "Timeout waiting for response to req_id %d" req_id)
                end else begin
                  (* Waiter resolved or failed by resolve_response or reset_state. *)
                  waiter >|= fun response ->
                  response
                end
  end

let get_connection () : Websocket_lwt_unix.conn Lwt.t =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match state.conn with
    | Some conn_with_gen -> Lwt.return (Ok conn_with_gen.conn)
    | None -> Lwt.return (Error "Trading WebSocket not connected. Call init() first."))
  >>= function
  | Ok conn -> Lwt.return conn
  | Error msg -> Lwt.fail_with msg

let send_ping ~req_id ~timeout_ms : Kraken_common_types.ws_response Lwt.t =
  get_connection () >>= fun _ ->
  let message = `Assoc [ ("method", `String "ping"); ("req_id", `Int req_id) ] in
  let message_str = json_to_string_precise None message in
  send_message ~message_str ~req_id ~expected_method:"ping" ~timeout_ms

let send_request ~symbol ~method_ ~params ~req_id ~timeout_ms : Kraken_common_types.ws_response Lwt.t =
  let message = `Assoc [
      ("method", `String method_);
      ("params", params);
      ("req_id", `Int req_id)
    ] in
  let message_str = json_to_string_precise symbol message in

  send_message ~message_str ~req_id ~expected_method:method_ ~timeout_ms >>= fun response ->
  Lwt.return response

let extract_channel_name message =
  try
    let open Yojson.Safe.Util in
    match member "params" message with
    | `Null -> None
    | params ->
        (match member "channel" params with
         | `Null -> None
         | channel_json -> Some (to_string channel_json))
  with _ -> None

let is_same_channel msg1 msg2 =
  match extract_channel_name msg1, extract_channel_name msg2 with
  | Some c1, Some c2 -> String.equal c1 c2
  | _ -> false

let subscribe message =
  Lwt_mutex.with_lock state.mutex (fun () ->
    let has_channel = match extract_channel_name message with | Some _ -> true | None -> false in
    if has_channel then
      state.subscriptions <- message :: (List.filter (fun existing -> not (is_same_channel message existing)) state.subscriptions)
    else if not (List.mem message state.subscriptions) then
      state.subscriptions <- message :: state.subscriptions;
    match state.conn with
    | Some conn_with_gen ->
        let content = Yojson.Safe.to_string message in

        Websocket_lwt_unix.write conn_with_gen.conn (Websocket.Frame.create ~content ())
    | None ->
        Logging.info_f ~section "Subscription registered, will be sent when connected: %s" (Yojson.Safe.to_string message);
        Lwt.return_unit
  )


(** Returns true if the trading WebSocket connection is currently established. *)
let is_connected () = Atomic.get state.connected

let init token : unit Lwt.t = 
  request_cleanup ();
  ensure_connection token

let connect_and_monitor token ~on_failure ~on_connected = 
  start_periodic_tasks ();
  Lwt.async (fun () -> cleanup_stale_response_entries ~reason:"connect" ());
  ensure_connection ~on_failure ~on_connected token

let close () : unit Lwt.t =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match state.conn with
    | None -> Lwt.return None
    | Some conn_with_gen ->
        state.conn <- None;
        state.connecting <- false;
        state.connection_generation <- state.connection_generation + 1;
        Atomic.set state.connected false;
        let pending = Response_table.fold (fun _ (wak, _, _) acc -> wak :: acc) state.responses [] in
        Response_table.clear state.responses;
        Lwt.return (Some (conn_with_gen.conn, pending))
  ) >>= function
  | None -> Lwt.return_unit
  | Some (conn, pending) ->
      List.iter (fun wak -> Lwt.wakeup_later_exn wak (Failure "Client requested close")) pending;
      notify_connection (`Disconnected "client_closed");
      Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit)

let heartbeat_stream = heartbeat_stream
let connection_stream = connection_stream