(** Shared type definitions for the connection supervisor and supervisor cache. *)

(** Lifecycle state of a supervised connection. *)
type connection_state =
  | Disconnected
  | Connecting
  | Connected
  | Failed of string (** Failure reason. *)

(** Circuit breaker states. *)
type circuit_breaker_state =
  | Closed (** Normal operation; requests pass through. *)
  | Open (** Failure threshold exceeded; requests are blocked. *)
  | HalfOpen (** Trial state; allows a single request to test recovery. *)

(** Mutable state of a single supervised connection: health metrics, circuit breaker
    status, and reconnection bookkeeping. *)
type supervised_connection =
  { name : string
  ; mutable state : connection_state
  ; mutable last_connected : float option
  ; mutable last_disconnected : float option
  ; mutable last_connecting : float option
  (** Timestamp of last transition to [Connecting]. *)
  ; mutable last_data_received : float option
  (** Data freshness timestamp; drives heartbeat timeout detection. *)
  ; mutable last_ping_sent : float option
  (** Timestamp of the last ping; drives ping/pong liveness. *)
  ; ping_failures : int Atomic.t (** Atomic counter of consecutive ping failures. *)
  ; mutable reconnect_attempts : int
  ; mutable total_connections : int
  ; mutable circuit_breaker : circuit_breaker_state
  ; mutable circuit_breaker_failures : int
  (** Consecutive failure count for the circuit breaker. *)
  ; mutable circuit_breaker_last_failure : float option
  ; mutable connect_fn : (unit -> unit Lwt.t) option
  (** [None] for monitoring-only (passive) connections. *)
  ; mutex : Mutex.t
  }

(** Global hashtable registry of all supervised connections, keyed by name. *)
let connections : (string, supervised_connection) Hashtbl.t = Hashtbl.create 16

(** Guards concurrent access to the [connections] registry. *)
let registry_mutex = Mutex.create ()

(** Snapshots the connection list under [registry_mutex], then applies [f] to the snapshot
    outside the critical section. *)
let with_connections_list f =
  Mutex.lock registry_mutex;
  let conn_list = Hashtbl.to_seq_values connections |> List.of_seq in
  Mutex.unlock registry_mutex;
  f conn_list
;;
