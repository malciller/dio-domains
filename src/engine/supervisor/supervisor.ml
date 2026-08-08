(** Connection supervisor. Thin orchestrator that wires together the
    submodules and re-exports the public API consumed by main.ml and
    the dashboard.

    Submodules:
    - Supervisor_connection: lifecycle, circuit breaker, shutdown
    - Supervisor_health:     monitor loop, non-active asset monitor
    - Supervisor_feeds:      per-exchange WS setup, readiness gates, fees
    - Supervisor_orders:     order processing loop *)

(* Re-export public API from Supervisor_connection so callers
   (main.ml, dashboard) can continue using Supervisor.X directly. *)

let shutdown_requested = Supervisor_connection.shutdown_requested
let interruptible_sleep = Supervisor_connection.interruptible_sleep

module Token_store = Supervisor_connection.Token_store

let register = Supervisor_connection.register
let register_for_monitoring = Supervisor_connection.register_for_monitoring
let set_state = Supervisor_connection.set_state
let get_state = Supervisor_connection.get_state
let get_uptime = Supervisor_connection.get_uptime
let set_connect_fn = Supervisor_connection.set_connect_fn
let update_data_heartbeat = Supervisor_connection.update_data_heartbeat
let update_circuit_breaker = Supervisor_connection.update_circuit_breaker

let circuit_breaker_allows_connection =
  Supervisor_connection.circuit_breaker_allows_connection
;;

let start_async = Supervisor_connection.start_async
let restart = Supervisor_connection.restart
let get_connection_opt = Supervisor_connection.get_connection_opt
let get_connection = Supervisor_connection.get_connection
let get_all_connections = Supervisor_connection.get_all_connections
let stop_order_processing = Supervisor_connection.stop_order_processing
let stop_all = Supervisor_connection.stop_all
let section = "supervisor"

(** Entry point: starts the monitor loop, non-active asset monitor, and
    order processing loop, then runs [initialize_feeds] synchronously.
    Returns enriched trading configs with fee data. *)
let start_monitoring () =
  Logging.info ~section "Starting connection supervisor";
  (* Launch health monitor on tick event bus *)
  Supervisor_health.monitor_loop ();
  (* Launch non-active asset ticker subscription loop *)
  Supervisor_health.monitor_non_active_assets ();
  (* Launch order processing loop *)
  Supervisor_orders.order_processing_loop ();
  (* Run feed initialization synchronously via Lwt_main.run *)
  let configs_with_fees, _auth_token =
    Lwt_main.run (Supervisor_feeds.initialize_feeds ())
  in
  configs_with_fees
;;

(** Initializes the Order_executor module. Retrieves the stored auth
    token or generates a fresh one if absent. *)
let start_order_executor () : unit Lwt.t =
  (* Retrieve or regenerate auth token *)
  let _auth_token =
    match Token_store.get () with
    | Some token -> token
    | None ->
      Logging.warn ~section "No stored auth token found, generating new one";
      let token = Lwt_main.run (Kraken.Kraken_generate_auth_token.get_token ()) in
      Token_store.set (Some token);
      token
  in
  Dio_engine.Order_executor.init
;;
