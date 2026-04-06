(** Supervisor cache: periodically snapshots connection state from
    the supervisor registry for read-only consumption by monitoring
    subsystems. Publishes updates via an event bus and an Lwt condition
    variable, with exponential backoff on consecutive snapshot failures. *)


open Concurrency

open Supervisor_types

(** Immutable point-in-time snapshot of a single connection's state.
    Derived from the mutable [Supervisor_types.connection] record under
    its per-connection mutex. *)
type connection_snapshot = {
  name: string;
  state: Supervisor_types.connection_state;
  uptime: float;
  heartbeat_active: bool;
  circuit_breaker_state: float;
  circuit_breaker_failures: int;
  reconnect_attempts: int;
  total_connections: int;
  last_updated: float;
}

(** Typed event bus carrying the full list of connection snapshots
    on each cache refresh cycle. *)
module ConnectionSnapshotEventBus = Event_bus.Make(struct
  type t = connection_snapshot list
end)

(** Mutable cache state. Holds the latest snapshot list, the event bus
    for downstream subscribers, an Lwt condition for synchronous waiters,
    and backoff bookkeeping for fault tolerance during snapshot failures. *)
type t = {
  mutable current_snapshots: connection_snapshot list;
  snapshot_event_bus: ConnectionSnapshotEventBus.t;
  update_condition: unit Lwt_condition.t;
  mutable last_update: float;
  update_interval: float;
  mutable consecutive_failures: int;
  mutable backoff_until: float;
}

(** Broadcasts the update condition, suppressing [Invalid_argument]
    from already-resolved promises that can arise under concurrent
    broadcast races. Other exceptions are logged as warnings. *)
let safe_broadcast_update_condition cache =
  try
    Lwt_condition.broadcast cache.update_condition ()
  with
  | Invalid_argument _ ->
      (* Promise already resolved under concurrent broadcast; non-fatal. *)
      ()
  | exn ->
      Logging.warn_f ~section:"supervisor_cache" "Error broadcasting update_condition: %s" (Printexc.to_string exn)

(** Module-level singleton cache instance. Initialized with empty snapshots,
    a 5-second update interval, and zero backoff state. *)
let cache = {
  current_snapshots = [];
  snapshot_event_bus = ConnectionSnapshotEventBus.create "connection_snapshots";
  update_condition = Lwt_condition.create ();
  last_update = 0.0;
  update_interval = 5.0;
  consecutive_failures = 0;
  backoff_until = 0.0;
}

let initialized = ref false


(** Reads all connections from the supervisor registry and produces an
    immutable snapshot list. Acquires [registry_mutex] briefly to copy
    the connection list, then locks each connection's individual mutex
    to read its fields. Uptime is computed only for connections in the
    [Connected] state. Circuit breaker state is encoded as a float:
    0.0 = Closed, 0.5 = HalfOpen, 1.0 = Open. *)
let create_connection_snapshots () : connection_snapshot list =
  let now = Unix.time () in

  (* Copy connection values under the registry mutex to minimize hold time. *)
  Mutex.lock Supervisor_types.registry_mutex;
  let conn_list = Hashtbl.to_seq_values Supervisor_types.connections |> List.of_seq in
  Mutex.unlock Supervisor_types.registry_mutex;

  (* Lock each connection individually to read its mutable fields. *)
  List.map (fun conn ->
    Mutex.lock conn.mutex;
    let snapshot = {
      name = conn.name;
      state = conn.state;
      uptime = (match conn.last_connected, conn.state with
        | Some t, Supervisor_types.Connected -> now -. t
        | _ -> 0.0);
      heartbeat_active = (match conn.state with Supervisor_types.Connected -> true | _ -> false);
      circuit_breaker_state = (match conn.circuit_breaker with
        | Supervisor_types.Closed -> 0.0
        | Supervisor_types.Open -> 1.0
        | Supervisor_types.HalfOpen -> 0.5);
      circuit_breaker_failures = conn.circuit_breaker_failures;
      reconnect_attempts = conn.reconnect_attempts;
      total_connections = conn.total_connections;
      last_updated = now;
    } in
    Mutex.unlock conn.mutex;
    snapshot
  ) conn_list


(** Forces an immediate cache refresh. On success, replaces the stored
    snapshots, resets failure counters, publishes via the event bus, and
    broadcasts the update condition. On failure, increments the consecutive
    failure counter and sets an exponential backoff ceiling (capped at 60s). *)
let force_update () =
  let now = Unix.time () in
  try
    let snapshots = create_connection_snapshots () in
    cache.current_snapshots <- snapshots;
    cache.last_update <- now;
    cache.consecutive_failures <- 0;
    cache.backoff_until <- 0.0;

    ConnectionSnapshotEventBus.publish cache.snapshot_event_bus snapshots;
    safe_broadcast_update_condition cache;
  with exn ->
    cache.consecutive_failures <- cache.consecutive_failures + 1;
    let backoff_seconds = min 60.0 (2.0 *. (2.0 ** float_of_int (min cache.consecutive_failures 5))) in
    cache.backoff_until <- now +. backoff_seconds;
    Logging.warn_f ~section:"supervisor_cache" "Failed to update connection snapshots: %s" (Printexc.to_string exn)

(** Idempotent initialization guard. Performs the first cache refresh
    and logs startup. Subsequent calls are no-ops. *)
let init () =
  if !initialized then ()
  else (
    initialized := true;
    force_update ();
    Logging.info ~section:"supervisor_cache" "Supervisor cache initialized"
  )

(** Returns the most recent snapshot list. Lock-free read of the
    mutable field; safe for single-writer/multi-reader access patterns. *)
let get_snapshots () = cache.current_snapshots
