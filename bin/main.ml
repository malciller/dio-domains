

(** Enable backtraces for better error reporting *)
let () = Printexc.record_backtrace true

(** Set up atexit handler for final logging *)
let () =
  at_exit (fun () ->
    Logging.info ~section:"main" "Process exiting - final cleanup complete"
  )

(** Graceful shutdown flag *)
let shutdown_requested = Atomic.make false
let shutdown_condition = Lwt_condition.create ()

(** Fatal signal tracking - None if no fatal signal received, Some signal_number if fatal signal occurred *)
let fatal_signal_received = Atomic.make None

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

(** Fatal signal handler for crashes *)
let setup_fatal_signal_handlers () =
  let handle_fatal_signal signum =
    let signal_name = match signum with
      | 11 -> "SIGSEGV (segmentation fault)"
      | 6 -> "SIGABRT (abort)"
      | 7 -> "SIGBUS (bus error)"
      | 10 -> "SIGBUS/SIGUSR1 (bus error or user signal)"  (* Signal 10 can be SIGBUS or SIGUSR1 depending on system *)
      | 8 -> "SIGFPE (floating point exception)"
      | _ -> Printf.sprintf "signal %d" signum
    in

    (* Diagnostic logging for crash investigation *)
    let backtrace = Printexc.get_backtrace () in
    let gc_stats = Gc.stat () in
    Logging.critical_f ~section:"main" "=== FATAL SIGNAL DIAGNOSTICS ===";
    Logging.critical_f ~section:"main" "Signal: %s (signal number: %d)" signal_name signum;
    Logging.critical_f ~section:"main" "Memory: heap=%dMB live=%dMB free=%dMB fragments=%d compactions=%d"
      (gc_stats.heap_words * (Sys.word_size / 8) / 1048576)
      (gc_stats.live_words * (Sys.word_size / 8) / 1048576)
      ((gc_stats.heap_words - gc_stats.live_words) * (Sys.word_size / 8) / 1048576)
      gc_stats.fragments
      gc_stats.compactions;
    Logging.critical_f ~section:"main" "Backtrace:\n%s" backtrace;
    Logging.critical_f ~section:"main" "=== END DIAGNOSTICS ===";

    Logging.critical_f ~section:"main" "Fatal signal received: %s - attempting emergency shutdown" signal_name;

    (* Track that a fatal signal was received *)
    Atomic.set fatal_signal_received (Some signum);

    (* Attempt emergency shutdown *)
    if not (Atomic.get shutdown_requested) then (
      Atomic.set shutdown_requested true;

      (* Quick emergency cleanup *)
      Dio_engine.Domain_spawner.stop_all_domains ();
      Supervisor.stop_all ();
      Lwt_condition.broadcast shutdown_condition ();

      Logging.critical ~section:"main" "Emergency shutdown initiated";
    );

    (* Give a moment for cleanup before exit *)
    Thread.delay 0.1;
    Logging.critical_f ~section:"main" "Process terminating due to fatal signal: %s" signal_name;
    (* Don't exit immediately - let main loop handle graceful shutdown *)
    ()
  in

  (* Register handlers for fatal signals - using integer values as OCaml doesn't define them in Sys *)
  List.iter (fun signal ->
    Sys.set_signal signal (Sys.Signal_handle handle_fatal_signal)
  ) [11; 6; 7; 8; 10]  (* SIGSEGV=11, SIGABRT=6, SIGBUS=7, SIGFPE=8, SIGBUS/SIGUSR1=10 *)

(** Set up Lwt unhandled exception handler *)
let setup_lwt_exception_handler () =
  Lwt.async_exception_hook := (fun exn ->
    let backtrace = Printexc.get_backtrace () in
    Logging.critical_f ~section:"main" "Unhandled exception in Lwt.async: %s\nBacktrace:\n%s"
      (Printexc.to_string exn) backtrace;
    (* Don't re-raise - this would cause the process to exit *)
    ()
  )

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

  (* Setup fatal signal handlers for crashes *)
  setup_fatal_signal_handlers ();

  (* Setup Lwt unhandled exception handler *)
  setup_lwt_exception_handler ();

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
      (try
        Lwt_main.run (Dio_ui.Metrics_broadcast.start_server config.metrics_broadcast shutdown_condition)
      with e ->
        let backtrace = Printexc.get_backtrace () in
        Logging.critical_f ~section:"main" "Fatal exception in Lwt event loop (metrics broadcast): %s\nBacktrace:\n%s"
          (Printexc.to_string e) backtrace;
        exit 1
      );

      Logging.info ~section:"main" "Metrics broadcast server closed gracefully, shutting down...";
    ) else (
      Logging.info ~section:"main" "Trading engine ready, waiting for shutdown signal...";

      (* Wait for shutdown signal without starting server *)
      (try
        Lwt_main.run (Lwt_condition.wait shutdown_condition)
      with e ->
        let backtrace = Printexc.get_backtrace () in
        Logging.critical_f ~section:"main" "Fatal exception in Lwt event loop (no server): %s\nBacktrace:\n%s"
          (Printexc.to_string e) backtrace;
        exit 1
      );

      Logging.info ~section:"main" "Shutdown signal received, shutting down...";
    );

    (* Check if a fatal signal was received during execution *)
    match Atomic.get fatal_signal_received with
    | Some signal_num ->
        Logging.critical_f ~section:"main" "Process terminating due to fatal signal %d received during execution" signal_num;
        exit 1
    | None ->
        (* Force exit to ensure process terminates even if background tasks are still running *)
        Logging.info ~section:"main" "Process terminating normally";
        exit 0
  with e ->
    let backtrace = Printexc.get_backtrace () in
    Logging.critical_f ~section:"main" "Fatal exception during startup: %s\nBacktrace:\n%s"
      (Printexc.to_string e) backtrace;
    exit 1
