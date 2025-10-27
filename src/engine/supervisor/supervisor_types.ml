(** Supervisor types - shared between supervisor and supervisor_cache *)

(** Connection state *)
type connection_state =
  | Disconnected
  | Connecting
  | Connected
  | Failed of string

(** Circuit breaker state *)
type circuit_breaker_state =
  | Closed  (* Normal operation *)
  | Open    (* Failing too much, temporarily disabled *)
  | HalfOpen  (* Testing if service recovered *)

(** Supervised connection *)
type supervised_connection = {
  name: string;
  mutable state: connection_state;
  mutable last_connected: float option;
  mutable last_disconnected: float option;
  mutable last_data_received: float option;  (* For heartbeat monitoring *)
  mutable last_ping_sent: float option;  (* For ping/pong monitoring *)
  mutable ping_failures: int;  (* Consecutive ping failures *)
  mutable reconnect_attempts: int;
  mutable total_connections: int;
  mutable circuit_breaker: circuit_breaker_state;
  mutable circuit_breaker_failures: int;  (* Consecutive failures *)
  mutable circuit_breaker_last_failure: float option;
  mutable connect_fn: (unit -> unit Lwt.t) option;  (* Optional - None for monitoring-only connections *)
  mutex: Mutex.t;
}

(** Global registry of supervised connections *)
let connections : (string, supervised_connection) Hashtbl.t = Hashtbl.create 16
let registry_mutex = Mutex.create ()

(** Safely get connection list for cache *)
let with_connections_list f =
  Mutex.lock registry_mutex;
  let conn_list = Hashtbl.to_seq_values connections |> List.of_seq in
  Mutex.unlock registry_mutex;
  f conn_list
