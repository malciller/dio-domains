

(** Enable backtraces for better error reporting *)
let () = Printexc.record_backtrace true

(** Check if memory tracing is available (environment variable or command line) *)
let is_memory_tracing_available () =
  try
    ignore (Sys.getenv "DIO_MEMORY_TRACING");
    true
  with Not_found -> false

(** Set up atexit handler for final logging and cleanup *)
let () =
  at_exit (fun () ->
    Logging.info ~section:"main" "Process exiting - final cleanup complete";
    (* Shutdown memory cleanup service if it was initialized *)
    if is_memory_tracing_available () then (
      try Dio_memory_cleanup.Memory_cleanup_service.stop ()
      with _ -> ()
    );
    (* Shutdown memory tracing if it was initialized *)
    if is_memory_tracing_available () then (
      try Dio_memory_tracing.Memory_tracing.shutdown ()
      with _ -> ()
    )
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

      (* Force-kill all websocket connections immediately *)
      Supervisor.stop_all_immediate ();

      (* Signal shutdown condition to allow graceful server shutdown *)
      Lwt_condition.broadcast shutdown_condition ();

      Logging.critical ~section:"main" "IMMEDIATE SHUTDOWN: All connections terminated, exiting now...";

      (* Force exit immediately after brief cleanup delay *)
      Thread.delay 0.05;  (* Give 50ms for critical cleanup *)
      Logging.critical ~section:"main" "Process terminating immediately";
      exit 0
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
      Supervisor.stop_all_immediate ();
      Lwt_condition.broadcast shutdown_condition ();

      Logging.critical ~section:"main" "Emergency shutdown initiated";

      (* Force exit immediately after brief cleanup delay *)
      Thread.delay 0.02;  (* Give 20ms for critical cleanup *)
      Logging.critical_f ~section:"main" "Process terminating immediately due to fatal signal: %s" signal_name;
      exit 1
    );

    (* Don't reach here - fatal signal should have exited *)
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

(** Import startup coordinator for phase coordination *)
open Concurrency.Startup_coordinator

(** Check if a port is available for binding *)
let check_port_available port =
  try
    let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    Unix.setsockopt socket Unix.SO_REUSEADDR true;
    (try
      Unix.bind socket (Unix.ADDR_INET (Unix.inet_addr_any, port));
      Unix.close socket;
      true
    with Unix.Unix_error (Unix.EADDRINUSE, "bind", _) ->
      Unix.close socket;
      false
    | exn ->
      Unix.close socket;
      raise exn)
  with exn ->
    Logging.warn_f ~section:"main" "Error checking port availability: %s" (Printexc.to_string exn);
    false

(** Validate startup state before proceeding *)
let validate_startup_state () =
  let issues = ref [] in

  (* Check startup coordinator phases *)
  if not (is_phase_completed FeedsInit) then
    issues := "Feeds initialization not completed" :: !issues;
  if not (is_phase_completed DomainsInit) then
    issues := "Domain initialization not completed" :: !issues;

  (* Log any issues found *)
  if !issues <> [] then begin
    Logging.warn ~section:"main" "Startup validation found issues:";
    List.iter (fun issue -> Logging.warn_f ~section:"main" "  - %s" issue) !issues;
  end;

  (* Return true if no critical issues *)
  !issues = []

(** Parse and set environment variables from command line arguments like KEY=VALUE *)
let set_env_vars_from_args () =
  for i = 1 to Array.length Sys.argv - 1 do
    let arg = Sys.argv.(i) in
    (* Check if argument looks like KEY=VALUE *)
    match String.index_opt arg '=' with
    | Some eq_pos when eq_pos > 0 ->
        let key = String.sub arg 0 eq_pos in
        let value = String.sub arg (eq_pos + 1) (String.length arg - eq_pos - 1) in
        (try
          Unix.putenv key value;
          Logging.debug_f ~section:"main" "Set environment variable from command line: %s=%s" key value
        with _ ->
          Logging.warn_f ~section:"main" "Failed to set environment variable: %s" key)
    | _ -> ()
  done

(** Command line argument parsing *)
let speclist = []

let usage_msg = "Dio Trading Engine with Metrics Broadcast\n\nUsage: " ^ Sys.argv.(0) ^ " [ENV_VAR=value ...]\n\nStarts the trading engine and metrics broadcast server on the configured port.\n\nEnvironment variables can be set via command line arguments:\n  " ^ Sys.argv.(0) ^ " DIO_MEMORY_TRACING=true\n  " ^ Sys.argv.(0) ^ " DIO_MEMORY_REPORT_INTERVAL=60"

(** Initialize the trading engine (now async for proper promise chaining) *)
let init_trading_engine () =
  (* Start supervisor monitoring for websocket connections and get configs with fees *)
  let%lwt (configs_with_fees, order_processing_promise) = Supervisor.start_monitoring () in

  (* Start order processing in background within Lwt context *)
  Lwt.async (fun () -> order_processing_promise);

  (* Initialize supervised domains for consuming asset-specific data *)
  Logging.info ~section:"main" "Initializing supervised asset domains...";
  let%lwt () = Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets (fun x -> x) configs_with_fees in
  Logging.info_f ~section:"main" "%d supervised asset domains initialized!" (List.length configs_with_fees);

  Lwt.return configs_with_fees

(** Initialize order executor asynchronously *)
let init_order_executor_async () =
  Logging.info ~section:"main" "Initializing order executor...";
  Supervisor.start_order_executor ()

let () =
  (* Parse and set environment variables from command line *)
  set_env_vars_from_args ();

  (* Parse command line arguments *)
  Arg.parse speclist (fun _ -> ()) usage_msg;

  (* Initialize logging system *)
  Logging.init ();

  (* Initialize memory tracing if enabled *)
  let memory_tracing_initialized = ref false in
  if is_memory_tracing_available () then (
    try
      if Dio_memory_tracing.Memory_tracing.init () then begin
        memory_tracing_initialized := true;
        Logging.info ~section:"main" "Memory tracing system initialized";

        (* Initialize memory cleanup events subscription after memory tracing is set up *)
        Dio_memory_cleanup.Memory_cleanup_events.init ();
        Dio_memory_cleanup.Memory_cleanup_events.ensure_allocation_tracker_subscription ();
        
        (* Initialize and start memory cleanup service *)
        Dio_memory_cleanup.Memory_cleanup_service.init ();
        Lwt.async (fun () -> Dio_memory_cleanup.Memory_cleanup_service.start ());
      end
    with _ ->
      Logging.debug ~section:"main" "Memory tracing module not available (compile without DIO_MEMORY_TRACING)"
  );

  (* Read and apply logging configuration *)
  let config = Dio_engine.Config.read_config () in
  Logging.set_level config.logging.level;
  Logging.set_enabled_sections config.logging.sections;

  (* Initialize random number generator for crypto operations *)
  Mirage_crypto_rng_unix.use_default ();

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

  Lwt_main.run (
    try%lwt
      (* Force Conduit context initialization inside Lwt event loop to prevent nested Lwt_main.run *)
      (* This ensures the lazy context is properly initialized before any HTTP operations *)
      let%lwt () = Lwt.wrap (fun () ->
        try
          (* Actually force the lazy Conduit context within the Lwt context *)
          let _ctx = Lazy.force Conduit_lwt_unix.default_ctx in
          Logging.debug ~section:"main" "Conduit context initialized successfully"
        with exn ->
          Logging.error_f ~section:"main" "Failed to initialize Conduit context: %s" (Printexc.to_string exn);
          raise exn
      ) in

      (* Initialize trading engine and domains asynchronously *)
      Logging.info ~section:"main" "Initializing trading engine...";
      let%lwt _configs = init_trading_engine () in

      (* Initialize order executor asynchronously - this can run in background *)
      let _order_executor_promise = init_order_executor_async () in

      (* Validate startup state *)
      let _startup_valid = validate_startup_state () in

      (* Start metrics broadcast server if enabled - runs until shutdown signal *)
      if config.metrics_broadcast.active then (
        (* Validate port availability before starting server *)
        if not (check_port_available config.metrics_broadcast.port) then
          Logging.warn_f ~section:"main" "Port %d may not be available, but proceeding with server startup (server will handle binding errors gracefully)" config.metrics_broadcast.port;

        Logging.info_f ~section:"main" "Trading engine ready, starting metrics broadcast server on port %d..." config.metrics_broadcast.port;

        (* Run server until shutdown signal is received *)
        let%lwt () = Dio_ui.Metrics_broadcast.start_server config.metrics_broadcast shutdown_condition in

        (* Signal that the server has started successfully *)
        signal_phase_complete ServerStart;

        Logging.info ~section:"main" "Metrics broadcast server closed gracefully, shutting down...";
        Lwt.return_unit
      ) else (
        Logging.info ~section:"main" "Trading engine ready, waiting for shutdown signal...";

        (* Wait for shutdown signal without starting server *)
        let%lwt () = Lwt_condition.wait shutdown_condition in

        Logging.info ~section:"main" "Shutdown signal received, shutting down...";
        Lwt.return_unit
      )
    with e ->
      let backtrace = Printexc.get_backtrace () in
      Logging.critical_f ~section:"main" "Fatal exception during startup: %s\nBacktrace:\n%s"
        (Printexc.to_string e) backtrace;
      exit 1
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
