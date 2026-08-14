(** Connection supervisor. Thin orchestrator that wires together the
    submodules and re-exports the public API consumed by main.ml and
    the dashboard.

    Submodules:
    - Supervisor_connection: lifecycle, circuit breaker, shutdown
    - Supervisor_health:     monitor loop, non-active asset monitor
    - Supervisor_feeds:      per-exchange WS setup, readiness gates, fees
    - Supervisor_orders:     order processing loop *)

open Lwt.Infix

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

(* ------------------------------------------------------------------ *)
(* Capital-oracle as a supervised module.                             *)
(* ------------------------------------------------------------------ *)

(** Capital-oracle runtime (wrapped library: explicit alias avoids opening the
    whole Dio_oracle namespace). *)
module Oracle_runtime = Dio_oracle.Oracle_runtime

(** Heartbeat interval for the oracle connection's liveness ticker: well under
    the health monitor's 60s passive-data timeout so a healthy loop (which can
    legitimately sleep the full refresh cadence between passes) never reads as
    dead, while a truly wedged loop is still flagged and restarted. *)
let oracle_heartbeat_interval = 10.0

(** The oracle's supervised connect_fn, run through the standard supervisor
    machinery ([start_async], circuit breaker, health monitor, auto-restart):

    - Transitions the connection to Connected as soon as the loop is
      scheduled (the loop itself can take minutes on a slow first pass; the
      meaningful liveness is "the loop is running", not "a pass finished").
    - Keeps the connection heartbeat alive while the loop runs: a liveness
      ticker every [oracle_heartbeat_interval] seconds, plus a heartbeat on
      every published pass (via the composed [on_publish]).
    - Resolves when the oracle loop ends - normally on engine shutdown
      (graceful), or as a failure that the health monitor picks up and
      restarts with exponential backoff. *)
let oracle_connect_fn
      (conn : Supervisor_types.supervised_connection)
      ~(config : Oracle_runtime.runtime_config)
      ~(trading : Dio_strategies.Strategy_common.trading_config list)
      ~(classes : (string * Oracle_runtime.class_pool) list)
      ~(on_publish : string list -> Oracle_runtime.decision list -> unit)
      ()
  : unit Lwt.t
  =
  set_state conn Connected;
  update_data_heartbeat conn;
  let rec liveness () =
    if Atomic.get shutdown_requested
    then Lwt.return_unit
    else
      Lwt_unix.sleep oracle_heartbeat_interval
      >>= fun () ->
      if Atomic.get shutdown_requested
      then Lwt.return_unit
      else (
        update_data_heartbeat conn;
        liveness ())
  in
  Lwt.pick
    [ Oracle_runtime.run_loop ~config ~trading ~classes ~on_publish (); liveness () ]
  >>= fun () ->
  (* The loop ended: normal when either shutdown flag is set (the engine's
     supervisor shutdown sets both); abnormal otherwise - surface it as a
     connection failure so the health monitor restarts the oracle. *)
  if Atomic.get shutdown_requested || Oracle_runtime.is_stopped ()
  then Lwt.return_unit
  else Lwt.fail (Failure "capital-oracle loop ended unexpectedly")
;;

(** Start the capital oracle as a supervised module: registered in the
    connection registry like every other module ("oracle"), started through
    the standard supervisor machinery, heartbeated on liveness ticks and
    published passes, and auto-restarted by the health monitor if the loop
    ever dies. [on_publish] is composed with the oracle's own pass hook - the
    engine uses it to wake trading domains; the supervisor adds the
    connection heartbeat. *)
let start_oracle
      ~(config : Oracle_runtime.runtime_config)
      ~(trading : Dio_strategies.Strategy_common.trading_config list)
      ~(classes : (string * Oracle_runtime.class_pool) list)
      ~(on_publish : string list -> Oracle_runtime.decision list -> unit)
      ()
  =
  let conn = register ~name:"oracle" ~connect_fn:None in
  let supervised_loop () =
    oracle_connect_fn
      conn
      ~config
      ~trading
      ~classes
      ~on_publish:(fun changed decisions ->
        (* Every published pass is a data heartbeat for the supervisor. *)
        update_data_heartbeat conn;
        on_publish changed decisions)
      ()
  in
  set_connect_fn conn (Some supervised_loop);
  start_async conn
;;
