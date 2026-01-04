(** Kraken Trading WebSocket Client

    This module provides a WebSocket client for sending authenticated trading requests
    (place orders, amend orders, cancel orders) to Kraken's WebSocket API v2.
*)

open Lwt.Infix
open Concurrency
module Memory_events = Dio_memory_tracing.Memory_events

let section = "kraken_trading_client"

(** Safely force Conduit context with error handling *)
let get_conduit_ctx () =
  try
    Lazy.force Conduit_lwt_unix.default_ctx
  with
  | CamlinternalLazy.Undefined ->
      Logging.error ~section "Conduit context was accessed before initialization - this should not happen";
      raise (Failure "Conduit context not initialized - ensure main.ml initializes it before domain spawning")
  | exn ->
      Logging.error_f ~section "Failed to get Conduit context: %s" (Printexc.to_string exn);
      raise exn

(** Global shutdown flag for trading client *)
let shutdown_requested = Atomic.make false

(** Global flag to track if cleanup handlers are running *)
let cleanup_handler_started = Atomic.make false

(** Signal shutdown to trading client *)
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

type state = {
  mutex: Lwt_mutex.t;
  mutable conn: connection_with_generation option;
  mutable reader: unit Lwt.t option;
  mutable connecting: bool;
  mutable connection_generation: int;
  responses: (Kraken_common_types.ws_response Lwt.u * string * float) Response_table.t;  (* wakener * expected_method * timestamp *)
  mutable on_failure: (string -> unit) option;
  connected: bool Atomic.t;
}

let state = {
  mutex = Lwt_mutex.create ();
  conn = None;
  reader = None;
  connecting = false;
  connection_generation = 0;
  responses = Response_table.create 32;
  on_failure = None;
  connected = Atomic.make false;
}

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
  Logging.debug_f ~section "resolve_response called for req_id=%d (method=%s, success=%b)"
    req_id response.method_ response.success;
  (* Minimize mutex hold time - only hold during critical section *)
  Lwt_mutex.with_lock state.mutex (fun () ->
    let pending_count = Response_table.length state.responses in
    try
      let wakener, expected_method, _timestamp = Response_table.find state.responses req_id in
      Response_table.remove state.responses req_id;
      Logging.debug_f ~section "Found waiter for req_id=%d (expected_method=%s, remaining pending: %d)"
        req_id expected_method (pending_count - 1);
      Lwt.return (Some (wakener, expected_method))
    with Not_found -> begin
      Logging.debug_f ~section "No waiter found for req_id=%d (total pending: %d)" req_id pending_count;
      Lwt.return None
    end
  ) >>= function
  | Some (wakener, expected_method) ->
      (* Validate response method matches expected method, with special handling for ping/pong *)
      let is_valid_response =
        expected_method = response.method_ ||
        (expected_method = "ping" && response.method_ = "pong")
      in
      if not is_valid_response then begin
        Logging.error_f ~section "Response method mismatch for req_id=%d: expected '%s', got '%s' (response discarded)"
          req_id expected_method response.method_;
        Lwt.return_unit
      end else begin
        (* Perform wakeup outside mutex to reduce contention *)
        (try
          Logging.debug_f ~section "Waking up waiter for req_id=%d" req_id;
          Lwt.wakeup_later wakener response;
          Logging.debug_f ~section "Successfully matched response for req_id=%d (method=%s, success=%b)"
            req_id response.method_ response.success
        with Invalid_argument _ ->
          (* Promise was already resolved (likely by timeout) - this is expected *)
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
  (* Capture reader task and other state before clearing *)
  Lwt_mutex.with_lock state.mutex (fun () ->
    match state.conn with
    | Some current when current.conn == conn ->
        let old_generation = state.connection_generation in
        let new_generation = old_generation + 1 in
        Logging.debug_f ~section "Resetting state: incrementing generation from %d to %d (reason: %s)"
          old_generation new_generation reason;
        let wakers = Response_table.fold (fun _ (wak, _, _) acc -> wak :: acc) state.responses [] in
        let reader = state.reader in
        let pending_count = Response_table.length state.responses in
        if pending_count > 0 then
          Logging.debug_f ~section "Resetting state: failing %d pending requests" pending_count;
        Response_table.clear state.responses;
        state.conn <- None;
        state.reader <- None;
        state.connecting <- false;
        state.connection_generation <- new_generation;
        Atomic.set state.connected false;
        let failure_cb = if notify_failure then state.on_failure else None in
        Lwt.return (wakers, reader, true, failure_cb, pending_count)
    | _ ->
        Logging.debug_f ~section "Resetting state: connection mismatch, not resetting (reason: %s)" reason;
        state.connecting <- false;
        Lwt.return ([], None, false, None, 0)
  ) >>= fun (wakers, reader_task, should_reset, failure_cb, pending_count) ->
  if not should_reset then Lwt.return_unit
  else begin
    (* Cancel reader loop first to prevent it from processing more frames *)
    (match reader_task with
     | Some reader ->
         Logging.debug_f ~section "Resetting state: cancelling reader task (%d pending requests)" pending_count;
         Lwt.cancel reader;
         Logging.debug ~section "Cancelled reader loop during reset_state"
     | None -> ());

    (* Close transport connection *)
    Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit) >>= fun () ->

    (* Wake up all pending waiters *)
    Logging.debug_f ~section "Resetting state: waking up %d pending waiters" (List.length wakers);
    List.iter (fun wak ->
      try
        Lwt.wakeup_later_exn wak (Failure reason)
      with Invalid_argument _ ->
        Logging.debug ~section "Attempted to wake already-resolved promise during reset_state"
    ) wakers;

    Logging.debug_f ~section "Resetting state: sending disconnected notification (reason: %s)" reason;
    notify_connection (`Disconnected reason);
    (match failure_cb with Some f -> f reason | None -> ());
    Lwt.return_unit
  end


let handle_frame frame ~expected_generation =
  notify_heartbeat ();
  (* Track frame processing *)

  (* Parse frame first to extract req_id, then check generation and response table atomically *)
  (try
     let json = Yojson.Safe.from_string frame.Websocket.Frame.content in
     let open Yojson.Safe.Util in
     let method_opt = member "method" json |> to_string_option in
     match method_opt with
     | Some method_name ->
         let response = parse_ws_response json in
         (match response.req_id with
          | Some req_id ->
              (* Check generation and response table atomically - only ignore if generation doesn't match AND req_id not in table *)
              Lwt_mutex.with_lock state.mutex (fun () ->
                let current_gen = state.connection_generation in
                let req_id_exists = Response_table.mem state.responses req_id in
                (* Process frame if: generation matches OR req_id is still in response table (delayed response) *)
                let should_process = expected_generation = current_gen || req_id_exists in
                Lwt.return (should_process, req_id_exists, current_gen)
              ) >>= fun (should_process, req_id_exists, current_gen) ->
              if not should_process then begin
                Logging.debug_f ~section "Ignoring frame from old connection (expected_gen=%d, current_gen=%d, req_id=%d, req_id_in_table=%b)"
                  expected_generation current_gen req_id req_id_exists;
                Lwt.return_unit
              end else begin
                if expected_generation <> current_gen && req_id_exists then begin
                  Logging.debug_f ~section "Processing delayed response from old connection (expected_gen=%d, current_gen=%d, req_id=%d still pending)"
                    expected_generation current_gen req_id;
                end;
                Logging.debug_f ~section "Processing frame: req_id=%d, method=%s, generation=%d/%d"
                  req_id method_name expected_generation current_gen;
                (* Synchronously resolve response to prevent timeout race *)
                resolve_response req_id response >>= fun () ->
                Lwt.return_unit
              end
          | None ->
             Logging.debug ~section
               (Printf.sprintf "Unhandled trading response without req_id (method=%s)" method_name);
             Lwt.return_unit)
     | None ->
         (match member "channel" json |> to_string_option with
          | Some "heartbeat" -> Lwt.return_unit
          | _ ->
             Logging.debug ~section
               (Printf.sprintf "Unhandled trading message: %s" (Yojson.Safe.to_string json));
             Lwt.return_unit)
   with exn ->
     Logging.error ~section
       (Printf.sprintf "Failed to handle trading frame synchronously: %s" (Printexc.to_string exn));
     Lwt.return_unit) >>= fun () ->

  (* Any additional processing can happen here asynchronously *)
  Lwt.return_unit

let rec reader_loop conn generation =
  Logging.debug_f ~section "Reader loop iteration (generation %d)" generation;
  if Atomic.get shutdown_requested then begin
    Logging.info_f ~section "Reader loop shutting down (generation %d)" generation;
    Lwt.return_unit
  end else
  Lwt.catch
    (fun () ->
      Websocket_lwt_unix.read conn >>= function
      | { Websocket.Frame.opcode = Websocket.Frame.Opcode.Close; _ } ->
          Logging.warn_f ~section "Trading WebSocket closed by server (generation %d)" generation;
          reset_state conn ~notify_failure:true "Connection closed by server"
      | frame ->
          Logging.debug_f ~section "Received WebSocket frame (generation %d, content length: %d)"
            generation (String.length frame.Websocket.Frame.content);
          handle_frame frame ~expected_generation:generation >>= fun () ->
          reader_loop conn generation
    )
    (function
      | End_of_file ->
          Logging.warn_f ~section "Trading WebSocket connection closed unexpectedly (End_of_file, generation %d)" generation;
          reset_state conn ~notify_failure:true "Connection closed unexpectedly (End_of_file)"
      | exn ->
          let reason = Printf.sprintf "WebSocket read error (generation %d): %s" generation (Printexc.to_string exn) in
          Logging.error ~section reason;
          reset_state conn ~notify_failure:true reason
    )

let start_reader conn generation =
  Logging.info_f ~section "Starting WebSocket reader loop (generation %d)" generation;
  let task =
    Lwt.catch
      (fun () ->
        reader_loop conn generation >>= fun () ->
        Logging.info_f ~section "Reader loop completed normally (generation %d)" generation;
        Lwt.return_unit
      )
      (fun exn ->
         let reason = Printf.sprintf "Reader loop failed (generation %d): %s" generation (Printexc.to_string exn) in
         Logging.error ~section reason;
         reset_state conn ~notify_failure:true reason
      )
  in
  Lwt.async (fun () -> task);
  Logging.debug_f ~section "Reader task started asynchronously (generation %d)" generation;
  task

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
  Logging.debug_f ~section "ensure_connection called (connected: %b, connecting: %b)"
    (Atomic.get state.connected) state.connecting;
  Lwt_mutex.with_lock state.mutex (fun () ->
    (match on_failure with Some f -> state.on_failure <- Some f | None -> ());
    if state.conn <> None then begin
      (* Connection already exists, call on_connected if provided *)
      Logging.debug ~section "Connection already exists, reusing";
      (match on_connected with Some f -> f () | None -> ());
      Lwt.return `Already
    end else if state.connecting then begin
      Logging.debug ~section "Connection in progress, waiting";
      Lwt.return `Wait
    end else begin
      Logging.info ~section "Starting new connection";
      state.connecting <- true;
      Lwt.return `Connect
    end
  ) >>= function
  | `Already -> Lwt.return_unit
  | `Wait ->
      (* Wait for connection to complete, then call on_connected *)
      let rec wait_for_connection () =
        Lwt_unix.sleep 0.1 >>= fun () ->
        Lwt_mutex.with_lock state.mutex (fun () ->
          if state.conn <> None then begin
            (match on_connected with Some f -> f () | None -> ());
            Lwt.return true
          end else if state.connecting then
            Lwt.return false
          else
            (* Connection failed, give up *)
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
             state.reader <- None;
             Atomic.set state.connected true;
             Lwt.return (Some (conn, generation)))
        )
        (fun exn ->
           Lwt_mutex.with_lock state.mutex (fun () ->
             state.connecting <- false;
             Lwt.return_unit) >>= fun () ->
           Lwt.fail exn
        ) >>= function
      | None ->
          Logging.error ~section "Connection establishment failed, no connection returned";
          Lwt.return_unit
      | Some (conn, generation) ->
          Logging.info_f ~section "Connection established successfully (generation %d)" generation;
          let reader = start_reader conn generation in
          Lwt_mutex.with_lock state.mutex (fun () ->
            state.reader <- Some reader;
            Lwt.return_unit) >>= fun () ->
          Logging.debug_f ~section "Reader task registered in state (generation %d)" generation;
          notify_connection `Connected;
          Logging.debug_f ~section "Connection `Connected notification sent (generation %d)" generation;
          (* Call on_connected callback if provided *)
          (match on_connected with Some f -> f () | None -> ());
          Logging.debug_f ~section "on_connected callback called (generation %d)" generation;
          (* Wait for the reader task to complete (only happens on error/disconnect) *)
          Logging.info_f ~section "Waiting for reader task to complete (generation %d)" generation;
          reader >>= fun () ->
          Logging.warn_f ~section "Reader task completed unexpectedly (generation %d)" generation;
          Lwt.return_unit

let cleanup_stale_response_entries ~reason () =
  if Atomic.get shutdown_requested then Lwt.return_unit
  else
    Lwt.catch
      (fun () ->
        let now = Unix.time () in
        let stale_entries = ref [] in
        (* Protect table access with mutex and bound time spent waiting *)
        Lwt.pick [
          (Lwt_mutex.with_lock state.mutex (fun () ->
            if Atomic.get shutdown_requested then
              Lwt.return_unit
            else begin
              Response_table.iter (fun req_id (wakener, expected_method, timestamp) ->
                if now -. timestamp > 30.0 then
                  stale_entries := (req_id, wakener, expected_method, timestamp) :: !stale_entries
              ) state.responses;
              List.iter (fun (req_id, wakener, expected_method, timestamp) ->
                Response_table.remove state.responses req_id;
                Logging.debug_f ~section "Cleaned up stale response entry for req_id=%d (age: %.1fs, method: %s, reason=%s)"
                  req_id (now -. timestamp) expected_method reason;
                (* Wake up the stale promise to prevent memory leaks *)
                try
                  Lwt.wakeup_later_exn wakener (Failure (Printf.sprintf "Request timed out after %.1fs" (now -. timestamp)))
                with Invalid_argument _ ->
                  Logging.debug_f ~section "Promise for stale req_id=%d was already resolved" req_id
              ) !stale_entries;
              if !stale_entries <> [] then
                Logging.debug_f ~section "Cleaned up %d stale response table entries (reason=%s)" (List.length !stale_entries) reason;
              Lwt.return_unit
            end
          ));
          (Lwt_unix.sleep 0.1 >|= fun () -> ())  (* 100ms timeout to prevent hanging *)
        ]
      )
      (fun exn ->
        if not (Atomic.get shutdown_requested) then
          Logging.error_f ~section "Error during response table cleanup (reason=%s): %s" reason (Printexc.to_string exn);
        Lwt.return_unit
      )

(** Start listening to memory pressure events to trigger cleanup as needed *)
let start_response_cleanup_handlers () =
  if Atomic.compare_and_set cleanup_handler_started false true then begin
    Logging.info ~section "Starting event-driven response cleanup handlers";
    let subscription = Memory_events.subscribe_memory_events () in
    Lwt.async (fun () ->
      let rec loop () =
        Lwt_stream.get subscription.stream >>= function
        | Some (Memory_events.MemoryPressure _) ->
            cleanup_stale_response_entries ~reason:"memory_pressure" () >>= fun () ->
            loop ()
        | Some (Memory_events.CleanupRequested | Memory_events.Heartbeat) ->
            loop ()

        | None ->
            (* Stream closed; explicitly close subscription to free resources *)
            subscription.close ();
            Logging.info ~section "Response cleanup memory event stream closed";
            Lwt.return_unit
      in
      loop ()
    )
  end

let send_message ~message_str ~req_id ~expected_method ~timeout_ms =
  (* Check for shutdown before sending new requests *)
  if Atomic.get shutdown_requested then begin
    Logging.debug_f ~section "Request req_id=%d cancelled due to shutdown" req_id;
    Lwt.fail_with (Printf.sprintf "Request req_id %d cancelled due to shutdown" req_id)
  end else begin
    Logging.debug_f ~section "Sending request: req_id=%d, method=%s, timeout_ms=%d" req_id expected_method timeout_ms;
    Lwt_mutex.with_lock state.mutex (fun () ->
      match state.conn with
      | None -> begin
          Logging.warn_f ~section "Cannot send request req_id=%d: WebSocket not connected" req_id;
          Lwt.return (Error (Failure "Trading WebSocket not connected"))
        end
      | Some conn_with_gen -> begin
          Logging.debug_f ~section "Request req_id=%d: connection generation=%d" req_id conn_with_gen.generation;
          (* Check for req_id collision before registering wakener *)
          if Response_table.mem state.responses req_id then begin
            Logging.error_f ~section "Request ID collision detected: req_id %d already in use" req_id;
            Lwt.return (Error (Failure (Printf.sprintf "Request ID collision detected: req_id %d already in use" req_id)))
          end else begin
            let waiter, wakener = Lwt.wait () in
            Response_table.add state.responses req_id (wakener, expected_method, Unix.time ());
            let pending_count = Response_table.length state.responses in
            Logging.debug_f ~section "Request req_id=%d registered in response table (pending requests: %d)"
              req_id pending_count;
            (* Log warning if response table is growing large *)
            if pending_count > 10 then begin
              Logging.warn_f ~section "Response table size is high: %d pending requests" pending_count;
              Lwt.async (fun () -> cleanup_stale_response_entries ~reason:"backpressure" ())
            end;
            Lwt.catch
              (fun () ->
                 Logging.debug_f ~section "Request req_id=%d: writing to WebSocket" req_id;
                 Websocket_lwt_unix.write conn_with_gen.conn (Websocket.Frame.create ~content:message_str ()) >|= fun () ->
                 Logging.debug_f ~section "Request req_id=%d: successfully sent to WebSocket" req_id;
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
        (* Add cancellation handler to remove from table if request is cancelled *)
        Lwt.on_cancel waiter (fun () ->
          Lwt.async (fun () ->
            Lwt_mutex.with_lock state.mutex (fun () ->
              if Response_table.mem state.responses req_id then begin
                Response_table.remove state.responses req_id;
                Logging.debug_f ~section "Request req_id=%d cancelled, removed from response table" req_id;
                Lwt.return_unit
              end else Lwt.return_unit
            )
          )
        );

        let timeout = float_of_int timeout_ms /. 1000.0 in
        Logging.debug_f ~section "Request req_id=%d: waiting for response (timeout: %.3fs)" req_id timeout;
        Lwt.pick [
          (waiter >|= fun response -> `Response response);
          (Lwt_unix.sleep timeout >|= fun () -> `Timeout)
        ] >>= function
        | `Response response ->
            (* Track successful response *)
            Logging.debug_f ~section "Request req_id=%d: response received successfully" req_id;
            Lwt.return response
        | `Timeout ->
            (* Track timeout for monitoring *)

            (* Check if request is still in table before removing *)
            Lwt_mutex.with_lock state.mutex (fun () ->
              let still_pending = Response_table.mem state.responses req_id in
              if still_pending then begin
                Response_table.remove state.responses req_id;
                Logging.debug_f ~section "Request req_id=%d: removed from response table due to timeout" req_id;
                Lwt.return `Was_pending
              end else begin
                Logging.debug_f ~section "Request req_id=%d: already removed from response table - response may have arrived" req_id;
                Lwt.return `Already_removed
              end) >>= fun removal_status ->

            match removal_status with
            | `Was_pending ->
                (* Actual timeout - request was still pending *)
                Logging.warn_f ~section "Request timeout: req_id=%d, timeout_ms=%d, sent %.3fs ago" req_id timeout_ms timeout;
                Logging.debug_f ~section "Request req_id=%d: cancelling waiter promise due to actual timeout" req_id;
                Lwt.cancel waiter;
                Lwt.fail_with (Printf.sprintf "Timeout waiting for response to req_id %d" req_id)
            | `Already_removed ->
                (* Response may have arrived just before timeout - wait briefly for waiter to resolve *)
                Logging.debug_f ~section "Request req_id=%d: possible race condition - waiting briefly for response" req_id;
                Lwt.pick [
                  waiter >|= (fun response -> `Response response);
                  Lwt_unix.sleep 0.01 >|= (fun () -> `Still_timeout)
                ] >>= function
                | `Response response ->
                    (* Response arrived after timeout but before we gave up - success! *)
                    Logging.info_f ~section "Request req_id=%d: response arrived just after timeout (within 10ms grace period)" req_id;
                    (* Track successful response *)
                    Logging.debug_f ~section "Request req_id=%d: response received successfully (timeout race resolved)" req_id;
                    Lwt.return response
                | `Still_timeout ->
                    (* Waiter still not resolved - this is an actual timeout despite req_id removal *)
                    Logging.warn_f ~section "Request timeout (race condition): req_id=%d, timeout_ms=%d, sent %.3fs ago" req_id timeout_ms timeout;
                    Logging.debug_f ~section "Request req_id=%d: waiter not resolved after race condition wait" req_id;
                    (* Don't cancel waiter - it might still resolve, but fail this call *)
                    Lwt.fail_with (Printf.sprintf "Timeout waiting for response to req_id %d" req_id)
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
  Logging.debug ~section (Printf.sprintf "Sending WebSocket message: %s" message_str);
  send_message ~message_str ~req_id ~expected_method:method_ ~timeout_ms >>= fun response ->
  Lwt.return response

(** Clean up stale response table entries (older than 30 seconds) - event-driven *)
let is_connected () = Atomic.get state.connected

let init token : unit Lwt.t = 
  start_response_cleanup_handlers ();
  Lwt.async (fun () -> cleanup_stale_response_entries ~reason:"init" ());
  ensure_connection token

let connect_and_monitor token ~on_failure ~on_connected = 
  start_response_cleanup_handlers ();
  Lwt.async (fun () -> cleanup_stale_response_entries ~reason:"connect" ());
  ensure_connection ~on_failure ~on_connected token

let close () : unit Lwt.t =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match state.conn with
    | None -> Lwt.return None
    | Some conn_with_gen ->
        state.conn <- None;
        state.reader <- None;
        state.connecting <- false;
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