

(** Graceful shutdown flag *)
let shutdown_requested = Atomic.make false

(** Signal handler for graceful shutdown *)
let setup_signal_handlers () =
  let handle_signal _signum =
    if not (Atomic.get shutdown_requested) then (
      Atomic.set shutdown_requested true;
      Logging.warn ~section:"main" "Shutdown signal received, cleaning up...";

      (* Stop all supervised domains *)
      Dio_engine.Domain_spawner.stop_all_domains ();

      (* Stop all websocket connections *)
      Supervisor.stop_all ();

      exit 0
    )
  in
  Sys.set_signal Sys.sigint (Sys.Signal_handle handle_signal);
  Sys.set_signal Sys.sigterm (Sys.Signal_handle handle_signal)

(** Command line argument parsing *)
let dashboard_mode = ref false

let speclist = [
  ("--dashboard", Arg.Set dashboard_mode, "Run in dashboard mode");
  ("-d", Arg.Set dashboard_mode, "Run in dashboard mode");
]

let usage_msg = "Dio Trading Engine\n\nUsage: " ^ Sys.argv.(0) ^ " [options]\n\nOptions:"

(** Initialize the trading engine synchronously (for websocket setup) *)
let init_trading_engine_sync () =
  (* Start supervisor monitoring for websocket connections and get configs with fees *)
  let configs_with_fees = Supervisor.start_monitoring () in

  (* Initialize supervised domains for consuming asset-specific data *)
  Logging.info ~section:"main" "Initializing supervised asset domains...";
  let _supervisor_thread = Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets (fun x -> x) configs_with_fees in
  Logging.info_f ~section:"main" "%d supervised asset domains initialized!" (List.length configs_with_fees);

  configs_with_fees

(** Initialize order executor asynchronously *)
let init_order_executor_async () =
  Logging.info ~section:"main" "Initializing order executor...";
  Supervisor.start_order_executor ()

let () =
  (* Parse command line arguments *)
  Arg.parse speclist (fun _ -> ()) usage_msg;

  (* Initialize logging system *)
  Logging.init ();

  if !dashboard_mode then begin
    (* Initialize logs cache for dashboard mode *)
    Dio_ui_logs.Logs_cache.init ();

    (* Set up log streaming to dashboard cache - silence stdout logging, redirect to dashboard *)
    Logging.set_quiet_mode true;
    Logging.set_log_callback (fun level section message ->
      let level_str = Logging.level_to_string level in
      Dio_ui_logs.Logs_cache.add_log_entry level_str section message
    )
  end;

  (* Initialize random number generator for crypto operations *)
  Mirage_crypto_rng_unix.use_default ();

  (* Setup signal handlers for graceful shutdown *)
  setup_signal_handlers ();

  if !dashboard_mode then (
    Logging.info ~section:"main" "Starting in dashboard mode...";

    try
      (* Initialize trading engine synchronously before starting dashboard *)
      Logging.info ~section:"main" "Initializing trading engine...";
      let _configs = init_trading_engine_sync () in

      (* Initialize order executor asynchronously - this can run in background *)
      let _order_executor_promise = init_order_executor_async () in

      (* Signal that feeds are ready for cache initialization *)
      Dio_ui_balance.Balance_cache.signal_feeds_ready ();
      Dio_ui_telemetry.Telemetry_cache.signal_system_ready ();

      (* Now run dashboard with trading engine fully initialized *)
      Logging.info ~section:"main" "Trading engine ready, starting dashboard...";
      Lwt_main.run (Dio_ui.Dashboard.run ());

      Logging.info ~section:"main" "Dashboard closed, shutting down..."
    with e ->
      Logging.error_f ~section:"main" "Exception in dashboard mode: %s" (Printexc.to_string e);
      raise e
  ) else (
    Logging.info ~section:"main" "Starting trading engine...";

    (* Initialize trading engine synchronously *)
    let _configs = init_trading_engine_sync () in

    (* Initialize order executor asynchronously *)
    let _order_executor_promise = init_order_executor_async () in

    (* Keep the main Lwt scheduler running to handle websockets *)
    let forever, _ = Lwt.wait () in
    Lwt_main.run forever;

    Logging.info ~section:"main" "Shutting down gracefully..."
  )
