(** Supervisor cache for connection metrics
    Provides safe read-only access to connection state for monitoring,
    respecting the concurrency model.
*)

open Lwt.Infix
open Concurrency

(** Open supervisor types to access types *)
open Supervisor_types

(** Connection state snapshot for monitoring *)
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

(** Event bus for connection snapshots *)
module ConnectionSnapshotEventBus = Event_bus.Make(struct
  type t = connection_snapshot list
end)

(** Cache state *)
type t = {
  mutable current_snapshots: connection_snapshot list;
  snapshot_event_bus: ConnectionSnapshotEventBus.t;
  update_condition: unit Lwt_condition.t;
  mutable last_update: float;
  update_interval: float;
  mutable consecutive_failures: int;
  mutable backoff_until: float;
}

(** Safe broadcast helper for cache update condition *)
let safe_broadcast_update_condition cache =
  try
    Lwt_condition.broadcast cache.update_condition ()
  with
  | Invalid_argument _ ->
      (* Promise already resolved - this can happen with concurrent broadcasts *)
      (* Not fatal, just means a waiter was already woken up *)
      ()
  | exn ->
      (* Other errors should still be logged *)
      Logging.warn_f ~section:"supervisor_cache" "Error broadcasting update_condition: %s" (Printexc.to_string exn)

(** Global cache instance *)
let cache = {
  current_snapshots = [];
  snapshot_event_bus = ConnectionSnapshotEventBus.create "connection_snapshots";
  update_condition = Lwt_condition.create ();
  last_update = 0.0;
  update_interval = 5.0;  (* Update every 5 seconds *)
  consecutive_failures = 0;
  backoff_until = 0.0;
}

let initialized = ref false
let polling_loop_started = Atomic.make false

(** Create connection snapshots by safely reading from supervisor *)
let create_connection_snapshots () : connection_snapshot list =
  let now = Unix.time () in

  (* Get connection list - lock registry briefly, then release before locking individual connections *)
  Mutex.lock Supervisor_types.registry_mutex;
  let conn_list = Hashtbl.to_seq_values Supervisor_types.connections |> List.of_seq in
  Mutex.unlock Supervisor_types.registry_mutex;

  (* Now iterate through connections and lock each individually *)
  List.map (fun conn ->
    (* Read all fields while holding mutex *)
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

(** Update telemetry metrics from snapshots (called from cache updater) *)
let update_telemetry_from_snapshots snapshots =
  List.iter (fun snapshot ->
    (* Pre-create all metric objects once *)
    let uptime_gauge = Telemetry.gauge "connection_uptime_seconds" ~labels:[("name", snapshot.name)] () in
    let heartbeat_gauge = Telemetry.gauge "connection_heartbeat_active" ~labels:[("name", snapshot.name)] () in
    let cb_state_gauge = Telemetry.gauge "circuit_breaker_state" ~labels:[("name", snapshot.name)] () in
    let cb_failures_gauge = Telemetry.gauge "circuit_breaker_failures" ~labels:[("name", snapshot.name)] () in
    let reconnect_gauge = Telemetry.gauge "connection_reconnect_attempts" ~labels:[("name", snapshot.name)] () in
    let total_gauge = Telemetry.gauge "connection_total_count" ~labels:[("name", snapshot.name)] () in

    (* Update all metrics *)
    Telemetry.set_gauge uptime_gauge snapshot.uptime;
    Telemetry.set_gauge heartbeat_gauge (if snapshot.heartbeat_active then 1.0 else 0.0);
    Telemetry.set_gauge cb_state_gauge snapshot.circuit_breaker_state;
    Telemetry.set_gauge cb_failures_gauge (float_of_int snapshot.circuit_breaker_failures);
    Telemetry.set_gauge reconnect_gauge (float_of_int snapshot.reconnect_attempts);
    Telemetry.set_gauge total_gauge (float_of_int snapshot.total_connections);
  ) snapshots

(** Start background updater - singleton pattern *)
let start_cache_updater () =
  if Atomic.compare_and_set polling_loop_started false true then begin
    Logging.info ~section:"supervisor_cache" "Starting singleton supervisor cache polling loop";
    Lwt.async (fun () ->
      let rec polling_loop () =
        let now = Unix.time () in
        let time_since_last = now -. cache.last_update in
        let in_backoff = now < cache.backoff_until in

        if in_backoff then
          Lwt_unix.sleep 1.0 >>= polling_loop
        else if time_since_last >= cache.update_interval then (
          (try
            let snapshots = create_connection_snapshots () in
            cache.current_snapshots <- snapshots;
            cache.last_update <- now;
            cache.consecutive_failures <- 0;
            cache.backoff_until <- 0.0;

            (* Update telemetry from snapshots *)
            update_telemetry_from_snapshots snapshots;

            (* Publish to event bus *)
            ConnectionSnapshotEventBus.publish cache.snapshot_event_bus snapshots;
            safe_broadcast_update_condition cache;
          with exn ->
            cache.consecutive_failures <- cache.consecutive_failures + 1;
            let backoff_seconds = min 60.0 (2.0 *. (2.0 ** float_of_int (min cache.consecutive_failures 5))) in
            cache.backoff_until <- now +. backoff_seconds;
            Logging.warn_f ~section:"supervisor_cache" "Failed to update connection snapshots: %s" (Printexc.to_string exn)
          );
          Lwt_unix.sleep 1.0 >>= polling_loop
        ) else
          Lwt_unix.sleep 1.0 >>= polling_loop
      in
      Lwt_unix.sleep 1.0 >>= polling_loop
    )
  end

(** Initialize cache *)
let init () =
  if !initialized then ()
  else (
    initialized := true;
    start_cache_updater ();
    Logging.info ~section:"supervisor_cache" "Supervisor cache initialized"
  )

(** Get current snapshots (read-only, no mutex) *)
let get_snapshots () = cache.current_snapshots
