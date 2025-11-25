

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

(** Force exit flag - for double SIGINT *)
let force_exit_requested = Atomic.make false

(** Fatal signal tracking - None if no fatal signal received, Some signal_number if fatal signal occurred *)
let fatal_signal_received = Atomic.make None

(** Signal handler for graceful shutdown *)
let setup_signal_handlers () =
  let handle_force_exit _signum =
    Logging.critical ~section:"main" "Force exit signal received - terminating immediately";
    exit 1
  in
  let handle_graceful_shutdown _signum =
    if not (Atomic.get shutdown_requested) then (
      Atomic.set shutdown_requested true;
      Logging.warn ~section:"main" "Shutdown signal received, cleaning up...";

      (* Install force exit handler for subsequent signals *)
      Sys.set_signal Sys.sigint (Sys.Signal_handle handle_force_exit);

      (* Signal shutdown to all components *)
      Dio_engine.Order_executor.signal_shutdown ();
      Kraken.Kraken_trading_client.signal_shutdown ();

      (* Stop all supervised domains *)
      Dio_engine.Domain_spawner.stop_all_domains ();

      (* Stop all websocket connections *)
      Supervisor.stop_all ();

      (* Signal shutdown condition to allow graceful shutdown *)
      Lwt_condition.broadcast shutdown_condition ();

      Logging.info ~section:"main" "Shutdown cleanup initiated, force exit available with second Ctrl+C..."
    ) else if not (Atomic.get force_exit_requested) then (
      Atomic.set force_exit_requested true;
      Logging.critical ~section:"main" "Second shutdown signal received - forcing immediate exit";
      exit 1
    )
  in
  Sys.set_signal Sys.sigint (Sys.Signal_handle handle_graceful_shutdown);
  Sys.set_signal Sys.sigterm (Sys.Signal_handle handle_graceful_shutdown)

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
      Dio_engine.Order_executor.signal_shutdown ();
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

(** Command line argument parsing *)
let speclist = []

let usage_msg = "Dio Trading Engine\n\nUsage: " ^ Sys.argv.(0) ^ "\n\nStarts the trading engine."

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

  (* Start memory cleanup coordinator *)
  Logging.info ~section:"main" "Starting memory cleanup coordinator...";
  Dio_memory_tracing.Cleanup_coordinator.start_cleanup_coordinator ();
  Logging.info ~section:"main" "Memory cleanup coordinator started";

  (* Basic GC statistics reporting *)
  let start_time = Unix.gettimeofday () in
  let last_report_time = ref start_time in
  let report_interval = 30.0 in  (* Report every 30 seconds *)

  let report_memory_stats () =
    if Atomic.get shutdown_requested then ()
    else
      let now = Unix.gettimeofday () in
      if now -. !last_report_time >= report_interval then (
        last_report_time := now;
        let runtime = now -. start_time in
        let current_gc_stats = Gc.stat () in

        if Atomic.get shutdown_requested then ()
        else (
          let heap_mb = current_gc_stats.heap_words * (Sys.word_size / 8) / 1048576 in
          let live_mb = current_gc_stats.live_words * (Sys.word_size / 8) / 1048576 in

          Logging.info_f ~section:"memory" "=== MEMORY STATISTICS (Runtime: %.1fs) ===" runtime;
          Logging.info_f ~section:"memory" "Main Domain: heap=%dMB live=%dMB free=%dMB"
            heap_mb live_mb (heap_mb - live_mb);

          Logging.info_f ~section:"memory" "GC Activity: minor=%d major=%d compactions=%d"
            current_gc_stats.minor_collections
            current_gc_stats.major_collections
            current_gc_stats.compactions;

          (* Subsystem counts *)
          Logging.info ~section:"memory" "";
          Logging.info ~section:"memory" "--- SUBSYSTEM COUNTS ---";

          let ticker_stores = Hashtbl.length Kraken.Kraken_ticker_feed.stores in
          let orderbook_stores = Hashtbl.length Kraken.Kraken_orderbook_feed.stores in
          let executions_stores = Hashtbl.length Kraken.Kraken_executions_feed.symbol_stores in
          let balances_stores = Hashtbl.length Kraken.Kraken_balances_feed.balance_stores in

          Logging.info_f ~section:"memory" "Stores: ticker=%d orderbook=%d executions=%d balances=%d"
            ticker_stores orderbook_stores executions_stores balances_stores;

          let telemetry_metrics = Hashtbl.length Telemetry.metrics in
          let domain_count = Hashtbl.length Dio_engine.Domain_spawner.domain_registry in
          
          Logging.info_f ~section:"memory" "Telemetry: %d metrics" telemetry_metrics;
          Logging.info_f ~section:"memory" "Domains: %d registered" domain_count;

          Logging.info ~section:"memory" "=== END MEMORY STATISTICS ===";
        )
      )
  in

  (* Start periodic memory reporting thread *)
  let memory_reporter_thread = Thread.create (fun () ->
    try
      while not (Atomic.get shutdown_requested) do
        let now = Unix.gettimeofday () in
        if now -. !last_report_time >= report_interval then (
          report_memory_stats ()
        );
        let delay_time = if Atomic.get shutdown_requested then 0.001 else 0.1 in
        Thread.delay delay_time
      done
    with _ -> ()
  ) () in

  (* Read and apply logging configuration *)
  let config = Dio_engine.Config.read_config () in
  Logging.set_level config.logging.level;
  Logging.set_enabled_sections config.logging.sections;

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

  Logging.info ~section:"main" "Starting Dio Trading Engine...";

  try
    (* Initialize trading engine synchronously *)
    Logging.info ~section:"main" "Initializing trading engine...";
    let _configs = init_trading_engine_sync () in

    (* Initialize order executor asynchronously - this can run in background *)
    let _order_executor_promise = init_order_executor_async () in

    Logging.info ~section:"main" "Trading engine ready, waiting for shutdown signal...";

    (* Wait for shutdown signal *)
    (try
      Lwt_main.run (Lwt_condition.wait shutdown_condition)
    with e ->
      let backtrace = Printexc.get_backtrace () in
      Logging.critical_f ~section:"main" "Fatal exception in Lwt event loop: %s\nBacktrace:\n%s"
        (Printexc.to_string e) backtrace;
      exit 1
    );

    Logging.info ~section:"main" "Shutdown signal received, shutting down...";

    (* Start aggressive shutdown timer - force exit after 3 seconds total *)
    let shutdown_start = Unix.gettimeofday () in
    let force_exit_timeout = 3.0 in

    (* Wait briefly for memory reporter thread, but don't wait forever *)
    let memory_thread_timeout = 1.0 in  (* Only wait 1 second for memory thread *)
    let memory_join_start = Unix.gettimeofday () in
    let memory_joined = ref false in
    let _memory_join_thread = Thread.create (fun () ->
      try
        Thread.join memory_reporter_thread;
        memory_joined := true
      with _ -> ()  (* Thread.join can raise exceptions *)
    ) () in

    (* Wait for memory thread with short timeout *)
    while not !memory_joined && Unix.gettimeofday () -. memory_join_start < memory_thread_timeout do
      Thread.delay 0.05  (* Very short delay for responsiveness *)
    done;

    if !memory_joined then
      Logging.info ~section:"main" "Memory monitoring thread completed gracefully"
    else
      Logging.warn ~section:"main" "Memory monitoring thread did not complete, forcing shutdown";

    (* Force exit after total timeout to ensure process terminates *)
    let elapsed = Unix.gettimeofday () -. shutdown_start in
    if elapsed < force_exit_timeout then (
      let remaining = force_exit_timeout -. elapsed in
      Logging.info_f ~section:"main" "Waiting %.1fs before force exit..." remaining;
      Thread.delay remaining
    );

    (* Check if a fatal signal was received during execution *)
    match Atomic.get fatal_signal_received with
    | Some signal_num ->
        Logging.critical_f ~section:"main" "Process terminating due to fatal signal %d received during execution" signal_num;
        exit 1
    | None ->
        Logging.info ~section:"main" "Force exiting process to ensure clean shutdown";
        exit 0
  with e ->
    let backtrace = Printexc.get_backtrace () in
    Logging.critical_f ~section:"main" "Fatal exception during startup: %s\nBacktrace:\n%s"
      (Printexc.to_string e) backtrace;
    exit 1