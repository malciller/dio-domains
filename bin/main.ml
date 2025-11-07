

(** Graceful shutdown flag *)
let shutdown_requested = Atomic.make false
let shutdown_condition = Lwt_condition.create ()

(** Signal handler for graceful shutdown *)
let setup_signal_handlers () =
  let handle_signal _signum =
    if not (Atomic.get shutdown_requested) then (
      Atomic.set shutdown_requested true;
      Logging.warn ~section:"main" "Shutdown signal received, cleaning up...";

      (* Signal shutdown to all components *)
      Dio_ui_cache.System_cache.signal_shutdown ();
      Dio_ui_cache.Telemetry_cache.signal_shutdown ();
      Dio_ui_cache.Balance_cache.signal_shutdown ();
      Dio_engine.Order_executor.signal_shutdown ();
      Kraken.Kraken_trading_client.signal_shutdown ();

      (* Stop all supervised domains *)
      Dio_engine.Domain_spawner.stop_all_domains ();

      (* Stop all websocket connections *)
      Supervisor.stop_all ();

      (* Signal shutdown condition to allow graceful server shutdown *)
      Lwt_condition.broadcast shutdown_condition ();

      Logging.info ~section:"main" "Shutdown cleanup initiated, waiting up to 5 seconds for graceful exit..."
    )
  in
  Sys.set_signal Sys.sigint (Sys.Signal_handle handle_signal);
  Sys.set_signal Sys.sigterm (Sys.Signal_handle handle_signal)

(** Command line argument parsing - no options needed for metrics broadcast mode *)
let speclist = []

let usage_msg = "Dio Trading Engine with Metrics Broadcast\n\nUsage: " ^ Sys.argv.(0) ^ "\n\nStarts the trading engine and metrics broadcast server on the configured port."

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

  (* Initialize memory tracing if requested *)
  (* Note: Enhanced memory monitoring is built-in via Gc.stat() calls *)
  (* Memory monitoring is handled by system_cache.ml and telemetry.ml *)();

  (* Read and apply logging configuration *)
  let config = Dio_engine.Config.read_config () in
  Logging.set_level config.logging.level;
  Logging.set_enabled_sections config.logging.sections;

  (* Initialize caches for metrics broadcast if enabled *)
  if config.metrics_broadcast.active then (
    Logging.info ~section:"main" "Metrics broadcast enabled, initializing caches...";
    Dio_ui_cache.Logs_cache.init ();
    Dio_ui_cache.Telemetry_cache.init ();
    Dio_ui_cache.System_cache.init ();
    Dio_ui_cache.Balance_cache.init ();
  ) else (
    Logging.info ~section:"main" "Metrics broadcast disabled, skipping cache initialization";
  );

  (* Set up log streaming to logs cache if metrics broadcast is active *)
  if config.metrics_broadcast.active then (
    Logging.set_log_callback (fun level section message ->
      (* Skip broadcast section logs to prevent infinite recursion *)
      if section <> "broadcast" then (
        let level_str = Logging.level_to_string level in
        Dio_ui_cache.Logs_cache.add_log_entry level_str section message
      ) else
        Lwt.return_unit
    );
  );

  (* Initialize random number generator for crypto operations *)
  Mirage_crypto_rng_unix.use_default ();

  (* Initialize Conduit context early to prevent CamlinternalLazy.Undefined race conditions *)
  (* This must be done before any domains spawn or websockets connect *)
  (try
    let _ctx = Lazy.force Conduit_lwt_unix.default_ctx in
    Logging.debug ~section:"main" "Conduit context initialized successfully"
  with exn ->
    Logging.error_f ~section:"main" "Failed to initialize Conduit context: %s" (Printexc.to_string exn);
    raise exn
  );

  (* Setup signal handlers for graceful shutdown *)
  setup_signal_handlers ();

  Logging.info ~section:"main" "Starting Dio Trading Engine with Metrics Broadcast...";

  (* Signal that caches are ready before any initialization if metrics broadcast is active *)
  if config.metrics_broadcast.active then (
    Dio_ui_cache.Balance_cache.signal_feeds_ready ();
    Dio_ui_cache.Telemetry_cache.signal_system_ready ();
  );

  try
    (* Initialize trading engine synchronously *)
    Logging.info ~section:"main" "Initializing trading engine...";
    let _configs = init_trading_engine_sync () in

    (* Initialize order executor asynchronously - this can run in background *)
    let _order_executor_promise = init_order_executor_async () in

    (* Start metrics broadcast server if enabled - runs until shutdown signal *)
    if config.metrics_broadcast.active then (
      Logging.info_f ~section:"main" "Trading engine ready, starting metrics broadcast server on port %d..." config.metrics_broadcast.port;

      (* Run server until shutdown signal is received *)
      Lwt_main.run (Dio_ui.Metrics_broadcast.start_server config.metrics_broadcast shutdown_condition);

      Logging.info ~section:"main" "Metrics broadcast server closed gracefully, shutting down...";
    ) else (
      Logging.info ~section:"main" "Trading engine ready, waiting for shutdown signal...";

      (* Wait for shutdown signal without starting server *)
      Lwt_main.run (Lwt_condition.wait shutdown_condition);

      Logging.info ~section:"main" "Shutdown signal received, shutting down...";
    );

    (* Force exit to ensure process terminates even if background tasks are still running *)
    exit 0
  with e ->
    Logging.error_f ~section:"main" "Exception during startup: %s" (Printexc.to_string e);
    raise e
