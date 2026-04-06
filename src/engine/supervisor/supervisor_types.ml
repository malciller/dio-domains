(** Shared type definitions for the connection supervisor and supervisor cache. *)

(** Represents the lifecycle state of a supervised connection. *)
type connection_state =
  | Disconnected
  | Connecting
  | Connected
  | Failed of string  (** Includes the error reason. *)

(** Tri-state circuit breaker following the standard pattern. *)
type circuit_breaker_state =
  | Closed    (** Normal operation; requests pass through. *)
  | Open      (** Failure threshold exceeded; requests are blocked. *)
  | HalfOpen  (** Trial state; allows a single request to test recovery. *)

(** Mutable record tracking the full state of a single supervised connection,
    including health metrics, circuit breaker status, and reconnection logic. *)
type supervised_connection = {
  name: string;
  mutable state: connection_state;
  mutable last_connected: float option;
  mutable last_disconnected: float option;
  mutable last_connecting: float option;        (** Timestamp of last transition to [Connecting]. *)
  mutable last_data_received: float option;     (** Used for heartbeat timeout detection. *)
  mutable last_ping_sent: float option;         (** Used for ping/pong liveness checks. *)
  ping_failures: int Atomic.t;                  (** Atomic counter of consecutive ping failures. *)
  mutable reconnect_attempts: int;
  mutable total_connections: int;
  mutable circuit_breaker: circuit_breaker_state;
  mutable circuit_breaker_failures: int;        (** Consecutive failure count for the circuit breaker. *)
  mutable circuit_breaker_last_failure: float option;
  mutable connect_fn: (unit -> unit Lwt.t) option;  (** [None] for monitoring-only (passive) connections. *)
  mutex: Mutex.t;
}

(** Global hashtable registry of all supervised connections, keyed by name. *)
let connections : (string, supervised_connection) Hashtbl.t = Hashtbl.create 16

(** Guards concurrent access to the [connections] registry. *)
let registry_mutex = Mutex.create ()

(** Snapshots the current connection list under the registry mutex,
    then applies [f] to the snapshot outside the critical section. *)
let with_connections_list f =
  Mutex.lock registry_mutex;
  let conn_list = Hashtbl.to_seq_values connections |> List.of_seq in
  Mutex.unlock registry_mutex;
  f conn_list
