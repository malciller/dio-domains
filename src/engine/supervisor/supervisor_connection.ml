(** Connection lifecycle management. Owns registration, state transitions,
    circuit breaker, reconnection dispatch, and graceful shutdown.
    All connection primitives consumed by other supervisor submodules
    are defined here. *)

open Supervisor_types

let section = "supervisor"

(* Atomic shutdown flag and condition variable for graceful termination. *)
let shutdown_requested = Atomic.make false
let shutdown_mutex = Mutex.create ()
let shutdown_cond = Condition.create ()

(* Timestamp of the last Supervisor_cache.force_update call.
   Rate-limits cache refreshes to prevent allocation bursts during
   rapid reconnect cycles. *)
let last_supervisor_cache_update = ref 0.0

(** Polls the shutdown flag in 100ms increments and returns early if set. *)
let interruptible_sleep seconds =
  if Atomic.get shutdown_requested
  then ()
  else (
    Mutex.lock shutdown_mutex;
    if Atomic.get shutdown_requested
    then Mutex.unlock shutdown_mutex
    else (
      Mutex.unlock shutdown_mutex;
      let rec sleep_loop remaining =
        if remaining <= 0.0 || Atomic.get shutdown_requested
        then ()
        else (
          let sleep_time = min remaining 0.1 in
          Thread.delay sleep_time;
          sleep_loop (remaining -. sleep_time))
      in
      sleep_loop seconds))
;;

(** Global authentication token store. Single-writer (supervisor init),
    lock-free reads via Atomic snapshot. *)
module Token_store = struct
  let token : string option Atomic.t = Atomic.make None
  let set value = Atomic.set token value
  let get () = Atomic.get token
end

(** Generates monotonically increasing ping request IDs starting at 1000001
    to avoid collisions with trading request IDs. *)
let next_ping_req_id =
  let counter = ref 1000000 in
  fun () ->
    incr counter;
    !counter
;;

(** Registers a new supervised connection in the global registry.
    Initializes all state fields and stores the optional connect_fn
    for automatic reconnection. *)
let register ~name ~connect_fn =
  Mutex.lock registry_mutex;
  let conn =
    { name
    ; state = Disconnected
    ; last_connected = None
    ; last_disconnected = Some (Unix.time ())
    ; (* Seed with current time to suppress immediate auto-restart *)
      last_connecting = None
    ; last_data_received = None
    ; last_ping_sent = None
    ; ping_failures = Atomic.make 0
    ; reconnect_attempts = 0
    ; total_connections = 0
    ; circuit_breaker = Closed
    ; circuit_breaker_failures = 0
    ; circuit_breaker_last_failure = None
    ; connect_fn
    ; mutex = Mutex.create ()
    }
  in
  Hashtbl.replace connections name conn;
  Mutex.unlock registry_mutex;
  Logging.debug_f ~section "Registered supervised connection: %s" name;
  conn
;;

(** Registers an existing connection for health monitoring only.
    No connect_fn is provided, so automatic reconnection is disabled. *)
let register_for_monitoring ~name =
  Mutex.lock registry_mutex;
  let conn =
    { name
    ; state = Connected
    ; (* Assumed already connected since this is monitor-only *)
      last_connected = Some (Unix.time ())
    ; last_disconnected = None
    ; last_connecting = None
    ; last_data_received = Some (Unix.time ())
    ; (* Seed heartbeat timestamp *)
      last_ping_sent = None
    ; ping_failures = Atomic.make 0
    ; reconnect_attempts = 0
    ; total_connections = 1
    ; circuit_breaker = Closed
    ; circuit_breaker_failures = 0
    ; circuit_breaker_last_failure = None
    ; connect_fn = None
    ; (* No connect_fn: reconnection disabled *)
      mutex = Mutex.create ()
    }
  in
  Hashtbl.replace connections name conn;
  Mutex.unlock registry_mutex;
  Logging.debug_f ~section "Registered connection for monitoring: %s" name;
  conn
;;

(** Transitions connection to [new_state], updating timestamps and counters
    under the per-connection mutex. Propagates changes to the supervisor
    cache at most once per second. *)
let set_state conn new_state =
  Mutex.lock conn.mutex;
  let old_state = conn.state in
  conn.state <- new_state;
  (match new_state with
   | Connected ->
     conn.last_connected <- Some (Unix.time ());
     conn.last_connecting <- None;
     conn.last_data_received <- Some (Unix.time ());
     conn.last_ping_sent <- None;
     Atomic.set conn.ping_failures 0;
     conn.reconnect_attempts <- 0;
     conn.total_connections <- conn.total_connections + 1;
     Logging.info_f
       ~section
       "[%s] Connection established (total: %d)"
       conn.name
       conn.total_connections
   | Disconnected ->
     conn.last_disconnected <- Some (Unix.time ());
     conn.last_connecting <- None;
     Logging.warn_f ~section "[%s] Connection lost" conn.name
   | Connecting ->
     conn.last_connecting <- Some (Unix.time ());
     Logging.debug_f
       ~section
       "[%s] Attempting connection (attempt #%d)"
       conn.name
       (conn.reconnect_attempts + 1)
   | Failed reason ->
     conn.last_disconnected <- Some (Unix.time ());
     conn.last_connecting <- None;
     conn.reconnect_attempts <- conn.reconnect_attempts + 1;
     Logging.error_f
       ~section
       "[%s] Connection failed: %s (attempt #%d)"
       conn.name
       reason
       conn.reconnect_attempts);
  Mutex.unlock conn.mutex;
  (* Propagate state transitions to the supervisor cache *)
  if old_state <> new_state
  then (
    (* Rate-limit cache updates to at most once per second.
       The dashboard state_broadcaster picks up interim deltas
       at its next 500ms tick regardless. *)
    let now = Unix.gettimeofday () in
    let last = !last_supervisor_cache_update in
    if now -. last >= 1.0
    then (
      last_supervisor_cache_update := now;
      Supervisor_cache.force_update ()))
;;

(** Returns the current connection state under mutex. *)
let get_state conn =
  Mutex.lock conn.mutex;
  let state = conn.state in
  Mutex.unlock conn.mutex;
  state
;;

(** Returns elapsed seconds since the connection entered Connected state,
    or None if currently disconnected. *)
let get_uptime conn =
  Mutex.lock conn.mutex;
  let uptime =
    match conn.last_connected, conn.state with
    | Some t, Connected -> Some (Unix.time () -. t)
    | _ -> None
  in
  Mutex.unlock conn.mutex;
  uptime
;;

(** Replaces the stored connect_fn (used for deferred registration). *)
let set_connect_fn conn connect_fn =
  Mutex.lock conn.mutex;
  conn.connect_fn <- connect_fn;
  Mutex.unlock conn.mutex
;;

(** Records current time as the last data heartbeat for this connection. *)
let update_data_heartbeat conn =
  Mutex.lock conn.mutex;
  conn.last_data_received <- Some (Unix.time ());
  Mutex.unlock conn.mutex
;;

(** Checks whether the circuit breaker permits a connection attempt.
    Caller must hold conn.mutex. Transitions Open to HalfOpen after
    a 300s (5 min) cooldown. *)
let circuit_breaker_allows_connection_unlocked conn =
  let current_time = Unix.time () in
  match conn.circuit_breaker with
  | Closed -> true
  | Open ->
    (match conn.circuit_breaker_last_failure with
     | Some failure_time when current_time -. failure_time > 300.0 ->
       (* 5 min cooldown *)
       conn.circuit_breaker <- HalfOpen;
       Logging.info_f
         ~section
         "[%s] Circuit breaker HALF-OPEN (testing recovery)"
         conn.name;
       true
     | _ -> false)
  | HalfOpen -> true (* Permit one probe attempt *)
;;

(** Thread-safe wrapper around [circuit_breaker_allows_connection_unlocked]. *)
let circuit_breaker_allows_connection conn =
  Mutex.lock conn.mutex;
  let allowed = circuit_breaker_allows_connection_unlocked conn in
  Mutex.unlock conn.mutex;
  allowed
;;

(** Updates circuit breaker state. On success, resets to Closed.
    On failure, increments the counter and opens the circuit
    after 5 consecutive failures. *)
let update_circuit_breaker conn success =
  Mutex.lock conn.mutex;
  if success
  then (
    (* Reset circuit breaker on success *)
    conn.circuit_breaker <- Closed;
    conn.circuit_breaker_failures <- 0;
    conn.circuit_breaker_last_failure <- None)
  else (
    (* Increment failure counter; open circuit at threshold *)
    conn.circuit_breaker_failures <- conn.circuit_breaker_failures + 1;
    conn.circuit_breaker_last_failure <- Some (Unix.time ());
    if conn.circuit_breaker_failures >= 5
    then (
      (* Threshold: 5 consecutive failures *)
      conn.circuit_breaker <- Open;
      Logging.warn_f
        ~section
        "[%s] Circuit breaker OPEN after %d consecutive failures"
        conn.name
        conn.circuit_breaker_failures));
  Mutex.unlock conn.mutex
;;

(** Schedules connect_fn in the Lwt event loop if the circuit breaker
    permits and the connection is not already in Connecting state.
    Transitions state to Connecting under mutex before launching. *)
let start_async conn =
  match conn.connect_fn with
  | None ->
    Logging.warn_f
      ~section
      "[%s] Cannot start connection - no connect function provided (monitoring only)"
      conn.name
  | Some connect_fn ->
    Mutex.lock conn.mutex;
    let should_start, attempt_num_opt =
      match conn.state with
      | Connecting ->
        Mutex.unlock conn.mutex;
        false, None
      | _ ->
        if not (circuit_breaker_allows_connection_unlocked conn)
        then (
          conn.state <- Failed "Circuit breaker open";
          conn.last_disconnected <- Some (Unix.time ());
          conn.last_connecting <- None;
          conn.reconnect_attempts <- conn.reconnect_attempts + 1;
          let attempt_num = conn.reconnect_attempts in
          Mutex.unlock conn.mutex;
          Logging.warn_f
            ~section
            "[%s] Circuit breaker blocks connection attempt"
            conn.name;
          Logging.error_f
            ~section
            "[%s] Connection failed: Circuit breaker open (attempt #%d)"
            conn.name
            attempt_num;
          false, None)
        else (
          conn.state <- Connecting;
          conn.last_connecting <- Some (Unix.time ());
          conn.reconnect_attempts <- conn.reconnect_attempts + 1;
          let attempt_num = conn.reconnect_attempts in
          Mutex.unlock conn.mutex;
          Logging.debug_f
            ~section
            "[%s] Attempting connection (attempt #%d)"
            conn.name
            attempt_num;
          let now = Unix.gettimeofday () in
          let last = !last_supervisor_cache_update in
          if now -. last >= 1.0
          then (
            last_supervisor_cache_update := now;
            Supervisor_cache.force_update ());
          Logging.debug_f
            ~section
            "[%s] Starting supervised connection (attempt #%d)"
            conn.name
            attempt_num;
          true, Some attempt_num)
    in
    if should_start
    then (
      let attempt_num = Option.get attempt_num_opt in
      let open Lwt.Infix in
      Lwt.async (fun () ->
        (* connect_fn manages its own Connected/Failed transitions *)
        Lwt.catch
          (fun () ->
             connect_fn ()
             >>= fun () ->
             (* WebSocket connect_fn should block indefinitely; early return is abnormal *)
             Mutex.lock conn.mutex;
             let already_failed =
               match conn.state with
               | Failed _ -> true
               | _ -> false
             in
             let latest_attempt = conn.reconnect_attempts in
             Mutex.unlock conn.mutex;
             if (not already_failed) && latest_attempt = attempt_num
             then (
               Logging.warn_f
                 ~section
                 "[%s] Connection function completed unexpectedly"
                 conn.name;
               set_state conn (Failed "connection completed unexpectedly"));
             Lwt.return_unit)
          (fun exn ->
             let error_msg = Printexc.to_string exn in
             Mutex.lock conn.mutex;
             let latest_attempt = conn.reconnect_attempts in
             Mutex.unlock conn.mutex;
             if latest_attempt = attempt_num
             then (
               Logging.error_f
                 ~section
                 "[%s] Unexpected error in connection function: %s"
                 conn.name
                 error_msg;
               (* Transition to Failed on unhandled exception *)
               set_state conn (Failed error_msg))
             else
               Logging.debug_f
                 ~section
                 "[%s] Muting error from superseded connection attempt: %s"
                 conn.name
                 error_msg;
             Lwt.return_unit)))
;;

(** Forces a reconnect by resetting state to Disconnected and
    clearing the reconnect counter before calling [start_async]. *)
let restart conn =
  Logging.info_f ~section "[%s] Manually restarting connection" conn.name;
  set_state conn Disconnected;
  (* Reset backoff counter for manual restart *)
  Mutex.lock conn.mutex;
  conn.reconnect_attempts <- 0;
  Mutex.unlock conn.mutex;
  start_async conn
;;

(** Looks up a connection by name. Returns None if unregistered. *)
let get_connection_opt name =
  Mutex.lock registry_mutex;
  let conn = Hashtbl.find_opt connections name in
  Mutex.unlock registry_mutex;
  conn
;;

(** Looks up a connection by name. Raises [Failure] if unregistered. *)
let get_connection name =
  match get_connection_opt name with
  | Some c -> c
  | None -> failwith (Printf.sprintf "Connection '%s' not found" name)
;;

(** Returns a snapshot list of all registered connections. *)
let get_all_connections () =
  Mutex.lock registry_mutex;
  let conns = Hashtbl.to_seq_values connections |> List.of_seq in
  Mutex.unlock registry_mutex;
  conns
;;

(** Sets the shutdown flag and broadcasts the condition variable
    to interrupt any sleeping threads. *)
let stop_order_processing () =
  Atomic.set shutdown_requested true;
  (* Signal all waiters on the shutdown condition *)
  Mutex.lock shutdown_mutex;
  Condition.broadcast shutdown_cond;
  Mutex.unlock shutdown_mutex;
  Logging.info ~section "Order processing loop shutdown requested"
;;

(** Stops order processing and transitions all registered connections
    to Disconnected. Includes a 500ms drain window for in-flight orders.
    Also stops the supervised capital-oracle runtime (registered by
    [Supervisor.start_oracle]) so its pass loop exits on the same shutdown
    signal as every other supervised module. *)
let stop_all () =
  stop_order_processing ();
  (* The capital oracle is a supervised module: signal its loop to stop
     (the loop checks its own flag at every wait slice and on the next
     pass). *)
  (try Dio_oracle.Oracle_runtime.shutdown () with
   | _ -> ());
  interruptible_sleep 0.5;
  (* Drain window for in-flight order iterations *)
  Logging.warn ~section "Stopping all supervised connections";
  Mutex.lock registry_mutex;
  let conn_list = Hashtbl.to_seq_values connections |> List.of_seq in
  Mutex.unlock registry_mutex;
  List.iter (fun conn -> set_state conn Disconnected) conn_list
;;
