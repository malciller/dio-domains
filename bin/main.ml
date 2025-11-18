

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

  (* Initialize enhanced memory monitoring for leak detection *)
  Logging.info ~section:"main" "Initializing enhanced memory monitoring...";

  (* Track GC statistics over time for leak detection *)
  let initial_gc_stats = Gc.stat () in
  let gc_stats_history = ref [] in
  let max_history_size = 50 in  (* Reduced to save memory *)

  (* Lightweight allocation tracking - NO backtrace strings stored *)
  let allocation_samples = Hashtbl.create 100 in  (* hash -> (count, total_words) *)
  let max_allocation_samples = 100 in  (* Hard limit - reduced from 500 *)
  let last_gc_live_words = ref (Gc.stat ()).live_words in
  let sample_interval = 7.0 in
  let last_sample_time = ref (Unix.gettimeofday ()) in
  let sampling_in_progress = ref false in
  let sample_counter = ref 0 in

  (* Component-level memory tracking *)
  let component_sizes = Hashtbl.create 20 in  (* component_name -> (size, timestamp) *)
  let last_component_check = ref (Unix.gettimeofday ()) in

  (* Immediate LRU eviction - no separate cleanup function needed *)
  let evict_lru_sample_if_needed () =
    if Hashtbl.length allocation_samples >= max_allocation_samples then (
      (* Find and remove smallest allocation to make room *)
      let smallest_hash = ref None in
      let smallest_words = ref Int64.max_int in
      Hashtbl.iter (fun hash (_, total_words) ->
        if total_words < !smallest_words then (
          smallest_words := total_words;
          smallest_hash := Some hash
        )
      ) allocation_samples;
      match !smallest_hash with
      | Some hash -> Hashtbl.remove allocation_samples hash
      | None -> ()
    )
  in

  let sample_allocations () =
    (* Early exit if shutdown is requested *)
    if Atomic.get shutdown_requested then () else
    let now = Unix.gettimeofday () in
    (* Sample ~70% of the time when interval is reached *)
    let should_sample = !sample_counter mod 10 < 7 in
    incr sample_counter;
    if now -. !last_sample_time >= sample_interval && not !sampling_in_progress && should_sample then (
      last_sample_time := now;
      sampling_in_progress := true;
      (try
        let current_gc_stats = Gc.stat () in
        let allocated_words = Int64.of_int (max 0 (current_gc_stats.live_words - !last_gc_live_words)) in
        last_gc_live_words := current_gc_stats.live_words;

        if allocated_words > 50000L then (* >400KB allocation *)
          (* Get backtrace but DON'T store the string - only use hash *)
          let backtrace = Printexc.get_callstack 5 in  (* Reduced depth from 10 to 5 *)
          let backtrace_hash = Hashtbl.hash (Printexc.raw_backtrace_to_string backtrace) in
          
          (* Evict LRU entry if at capacity BEFORE adding *)
          evict_lru_sample_if_needed ();
          
          let count, total_words =
            try Hashtbl.find allocation_samples backtrace_hash
            with Not_found -> (0L, 0L)
          in
          (* Store only hash + counts, NO backtrace string *)
          Hashtbl.replace allocation_samples backtrace_hash 
            (Int64.add count 1L, Int64.add total_words allocated_words)
      with _ -> ()  (* Ignore backtrace errors *)
      );
      sampling_in_progress := false
    )
  in

  (* Track component memory usage via hashtable sizes *)
  let track_component_memory () =
    if Atomic.get shutdown_requested then () else
    let now = Unix.gettimeofday () in
    if now -. !last_component_check >= 30.0 then (
      last_component_check := now;
      (* Store component sizes for growth tracking *)
      Hashtbl.replace component_sizes "allocation_samples" 
        (Hashtbl.length allocation_samples, now)
    )
  in

  (* Periodic memory statistics reporting *)
  let start_time = Unix.gettimeofday () in
  let last_report_time = ref start_time in
  let report_interval = 30.0 in  (* Report every 5 minutes *)

  let report_memory_stats () =
    (* Early exit if shutdown is requested *)
    if Atomic.get shutdown_requested then () else
    let now = Unix.gettimeofday () in
    if now -. !last_report_time >= report_interval then (
      last_report_time := now;
      let runtime = now -. start_time in
      let current_gc_stats = Gc.stat () in

      (* Early exit if shutdown requested after GC stat collection *)
      if Atomic.get shutdown_requested then () else (

      (* Store current stats in history for trend analysis *)
      gc_stats_history := current_gc_stats :: !gc_stats_history;
      if List.length !gc_stats_history > max_history_size then
        gc_stats_history := List.rev (List.tl (List.rev !gc_stats_history));

      (* Early exit if shutdown requested after history update *)
      if Atomic.get shutdown_requested then () else (

      (* Calculate memory growth trends *)
      let heap_growth = current_gc_stats.heap_words - initial_gc_stats.heap_words in
      let live_growth = current_gc_stats.live_words - initial_gc_stats.live_words in
      let collections_growth = current_gc_stats.major_collections - initial_gc_stats.major_collections in

      (* Early exit if shutdown requested before logging *)
      if Atomic.get shutdown_requested then () else (

      Logging.info_f ~section:"memory" "=== MEMORY STATISTICS (Runtime: %.1fs) ===" runtime;

      (* Early exit if shutdown requested after header *)
      if Atomic.get shutdown_requested then () else (

      Logging.info_f ~section:"memory" "Current GC Stats: heap=%dMB live=%dMB free=%dMB"
        (current_gc_stats.heap_words * (Sys.word_size / 8) / 1048576)
        (current_gc_stats.live_words * (Sys.word_size / 8) / 1048576)
        ((current_gc_stats.heap_words - current_gc_stats.live_words) * (Sys.word_size / 8) / 1048576);

      (* Early exit if shutdown requested after GC stats *)
      if Atomic.get shutdown_requested then () else (

      Logging.info_f ~section:"memory" "GC Activity: minor_collections=%d major_collections=%d compactions=%d"
        current_gc_stats.minor_collections
        current_gc_stats.major_collections
        current_gc_stats.compactions;

      Logging.info_f ~section:"memory" "Memory Growth: heap %+dMB live %+dMB (+%d major GC cycles)"
        (heap_growth * (Sys.word_size / 8) / 1048576)
        (live_growth * (Sys.word_size / 8) / 1048576)
        collections_growth;

      (* Calculate allocation rate *)
      let allocation_rate_kb_per_sec = float_of_int (live_growth * (Sys.word_size / 8) / 1024) /. runtime in
      Logging.info_f ~section:"memory" "Allocation Rate: %.1f KB/s average since startup" allocation_rate_kb_per_sec;

      (* Memory leak warning *)
      if live_growth > 10000000 then (* 10MB growth *)
        Logging.warn_f ~section:"memory" "POTENTIAL MEMORY LEAK: Live memory grew by %dMB since startup"
          (live_growth * (Sys.word_size / 8) / 1048576);

      (* Monitor monitoring system's own memory usage - much lighter now *)
      let monitoring_memory_kb = (Hashtbl.length allocation_samples * 16) / 1024 in  (* Only 16 bytes per entry now *)
      if monitoring_memory_kb > 100 then (* More than 100KB used by monitoring *)
        Logging.warn_f ~section:"memory" "MONITORING SYSTEM MEMORY USAGE: %dKB (%d allocation samples)"
          monitoring_memory_kb (Hashtbl.length allocation_samples);
      if Hashtbl.length allocation_samples >= max_allocation_samples then
        Logging.info_f ~section:"memory" "Allocation samples at limit (%d), LRU eviction active" max_allocation_samples;

      (* Early exit if shutdown requested before fragmentation analysis *)
      if Atomic.get shutdown_requested then () else (

      (* Fragmentation analysis *)
      let fragmentation_ratio = float_of_int current_gc_stats.fragments /. float_of_int current_gc_stats.heap_words in
      Logging.info_f ~section:"memory" "Fragmentation: %d words (%.2f%% of heap)"
        current_gc_stats.fragments (fragmentation_ratio *. 100.0);

      (* Early exit if shutdown requested before allocation sites processing *)
      if Atomic.get shutdown_requested then () else (

      (* Report top allocation sites - lightweight version without backtrace strings *)
      if not (Atomic.get shutdown_requested) && Hashtbl.length allocation_samples > 0 then (
        Logging.info ~section:"memory" "Top memory allocation sites (by hash):";
        let top_sites = Hashtbl.fold (fun hash (count, total_words) acc ->
          if Atomic.get shutdown_requested then acc else
          (hash, count, total_words) :: acc
        ) allocation_samples []
        |> (fun acc -> if Atomic.get shutdown_requested then [] else List.sort (fun (_, _, w1) (_, _, w2) -> compare w2 w1) acc)
        |> (fun l -> if Atomic.get shutdown_requested then [] else let rec take n acc = function | [] -> List.rev acc | h::t when n > 0 -> take (n-1) (h::acc) t | _ -> List.rev acc in take 5 [] l)
        in
        if not (Atomic.get shutdown_requested) then (
          List.iteri (fun i (hash, count, total_words) ->
            if not (Atomic.get shutdown_requested) then (
              let bytes = Int64.to_int total_words * (Sys.word_size / 8) in
              Logging.info_f ~section:"memory" "  %d. %d bytes (%Ld words) in %Ld samples [hash:%d]"
                (i + 1) bytes total_words count hash
            )
          ) top_sites;
          if not (Atomic.get shutdown_requested) then
            Logging.info_f ~section:"memory" "  (Lightweight tracking: %d unique sites, max %d, ~%dKB overhead)" 
              (Hashtbl.length allocation_samples) max_allocation_samples ((Hashtbl.length allocation_samples * 16) / 1024)
        )
      );

      Logging.info ~section:"memory" "=== END MEMORY STATISTICS ==="
      )
      )
      )
      )
      )
      )
      )
    )
  in

  (* Start periodic memory reporting thread *)
  let memory_reporter_thread = Thread.create (fun () ->
    let last_report = ref (Unix.gettimeofday ()) in
    let last_sample = ref (Unix.gettimeofday ()) in
    try
      while not (Atomic.get shutdown_requested) do
        let now = Unix.gettimeofday () in
        (* Sample allocations at the proper intervals *)
        if now -. !last_sample >= 1.0 then (
          last_sample := now;
          sample_allocations ();
          track_component_memory ()  (* Also track component sizes *)
        );
        (* Report memory stats at regular intervals *)
        if now -. !last_report >= report_interval then (
          report_memory_stats ();
          last_report := now
        );
        (* Very responsive shutdown check - even shorter delay when shutdown requested *)
        let delay_time = if Atomic.get shutdown_requested then 0.001 else 0.01 in
        Thread.delay delay_time
      done;
      (* Skip final memory report during shutdown to prevent race conditions *)
      ()
    with _ ->
      (* If anything goes wrong during shutdown, just exit the thread *)
      ()
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