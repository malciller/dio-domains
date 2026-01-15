(** Memory Cleanup Coordinator

    Subscribes to memory pressure events and coordinates cleanup across
    all subsystems to prevent unbounded memory growth.
*)

open Lwt.Infix

(** Cleanup statistics tracking *)
type cleanup_stats = {
  mutable telemetry_cleaned: int;
  mutable event_bus_cleaned: int;
  mutable event_registry_cleaned: int;
  mutable gc_cycles: int;
  mutable last_cleanup: float;
}

let stats = {
  telemetry_cleaned = 0;
  event_bus_cleaned = 0;
  event_registry_cleaned = 0;
  gc_cycles = 0;
  last_cleanup = Unix.gettimeofday ();
}

(** Registry of event registries to cleanup *)
let event_registry_registry : (string * (unit -> (int option * int option) option)) list ref = ref []

(** Register an event registry for cleanup *)
let register_event_registry name cleanup_fn =
  event_registry_registry := (name, cleanup_fn) :: !event_registry_registry;
  Logging.debug_f ~section:"cleanup_coordinator" "Registered event registry for cleanup: %s" name

(** Telemetry cleanup - no-op since telemetry is removed *)
let cleanup_telemetry () = 0

(** Perform event bus cleanup *)
let cleanup_event_buses () =
  let acc = ref 0 in
  Concurrency.Event_bus.iter_buses (fun { topic; cleanup; _ } ->
    try
      match cleanup () with
      | Some count ->
          if count > 0 then
            Logging.info_f ~section:"cleanup_coordinator" "Event bus %s: cleaned %d stale subscribers" topic count;
          acc := !acc + count
      | None -> ()
    with exn ->
      Logging.warn_f ~section:"cleanup_coordinator" "Exception during event bus cleanup for %s: %s" topic (Printexc.to_string exn)
  );
  !acc

(** Perform event registry cleanup *)
let cleanup_event_registries () =
  let rec cleanup_fold acc = function
    | [] -> acc
    | (name, cleanup_fn) :: rest ->
        let acc' =
          try
            match cleanup_fn () with
            | Some (size_drift, size_trimmed) ->
                (match size_drift with
                 | Some drift ->
                     Logging.debug_f ~section:"cleanup_coordinator" "Event registry %s: size drift corrected by %d" name drift
                 | None -> ());
                (match size_trimmed with
                 | Some trimmed ->
                     Logging.info_f ~section:"cleanup_coordinator" "Event registry %s: trimmed %d entries" name trimmed;
                     acc + trimmed
                 | None -> acc)
            | None -> acc
          with exn ->
            Logging.warn_f ~section:"cleanup_coordinator" "Exception during event registry cleanup for %s: %s" name (Printexc.to_string exn);
            acc
        in
        cleanup_fold acc' rest
  in
  cleanup_fold 0 !event_registry_registry

(** Perform GC operations based on pressure level *)
let perform_gc_cleanup pressure_level =
  match pressure_level with
  | `High ->
      (* Aggressive cleanup on high pressure *)
      Gc.compact ();
      stats.gc_cycles <- stats.gc_cycles + 1;
      Logging.info ~section:"cleanup_coordinator" "Performed GC compaction due to high memory pressure"
  | `Medium ->
      (* Moderate cleanup on medium pressure *)
      ignore (Gc.major_slice 0);
      stats.gc_cycles <- stats.gc_cycles + 1;
      Logging.info ~section:"cleanup_coordinator" "Performed major GC slice due to medium memory pressure"

(** Handle memory pressure event *)
let handle_memory_pressure_event event =
  let start_time = Unix.gettimeofday () in

  match event with
  | Memory_events.MemoryPressure { level; heap_mb; live_mb; fragmentation_percent } ->
      Logging.info_f ~section:"cleanup_coordinator" "Memory pressure cleanup triggered (level=%s, heap=%dMB, live=%dMB, frag=%.1f%%)"
        (match level with `Medium -> "medium" | `High -> "high")
        heap_mb live_mb fragmentation_percent;

      (* Perform cleanup based on pressure level *)
      let telemetry_cleaned = cleanup_telemetry () in
      let event_bus_cleaned = cleanup_event_buses () in
      let event_registry_cleaned = cleanup_event_registries () in
      perform_gc_cleanup level;

      (* Update statistics *)
      stats.telemetry_cleaned <- stats.telemetry_cleaned + telemetry_cleaned;
      stats.event_bus_cleaned <- stats.event_bus_cleaned + event_bus_cleaned;
      stats.event_registry_cleaned <- stats.event_registry_cleaned + event_registry_cleaned;
      stats.last_cleanup <- Unix.gettimeofday ();

      let end_time = Unix.gettimeofday () in
      Logging.info_f ~section:"cleanup_coordinator" "Cleanup completed in %.3fs: telemetry=%d, event_buses=%d, event_registries=%d"
        (end_time -. start_time) telemetry_cleaned event_bus_cleaned event_registry_cleaned

  | Memory_events.CleanupRequested ->
      Logging.info ~section:"cleanup_coordinator" "Manual cleanup requested";
      let telemetry_cleaned = cleanup_telemetry () in
      let event_bus_cleaned = cleanup_event_buses () in
      let event_registry_cleaned = cleanup_event_registries () in

      stats.telemetry_cleaned <- stats.telemetry_cleaned + telemetry_cleaned;
      stats.event_bus_cleaned <- stats.event_bus_cleaned + event_bus_cleaned;
      stats.event_registry_cleaned <- stats.event_registry_cleaned + event_registry_cleaned;
      stats.last_cleanup <- Unix.gettimeofday ();

      Logging.info_f ~section:"cleanup_coordinator" "Manual cleanup completed: telemetry=%d, event_buses=%d, event_registries=%d"
        telemetry_cleaned event_bus_cleaned event_registry_cleaned

  | Memory_events.Heartbeat ->
      (* Heartbeat handled implicitly by loop *)
      ()

(** Get statistics for all registered event buses *)
let get_all_event_bus_stats () =
  let total_acc = ref 0 in
  let active_acc = ref 0 in
  let stale_acc = ref 0 in
  let per_bus_acc = ref [] in

  Concurrency.Event_bus.iter_buses (fun { topic; stats; _ } ->
    let (total, active, stale) =
      try
        stats ()
      with exn ->
        Logging.warn_f ~section:"cleanup_coordinator" "Exception getting stats for event bus %s: %s" topic (Printexc.to_string exn);
        (0, 0, 0)
    in
    total_acc := !total_acc + total;
    active_acc := !active_acc + active;
    stale_acc := !stale_acc + stale;
    per_bus_acc := (topic, total, active, stale) :: !per_bus_acc
  );
  (!total_acc, !active_acc, !stale_acc, !per_bus_acc)

(** Global flag to track if cleanup coordinator was ever started (singleton init) *)
let coordinator_initialized = Atomic.make false

(** Global flag to track if cleanup coordinator is currently running *)
let coordinator_running = Atomic.make false

(** Generation counter to prevent zombie loops from updating heartbeats after restart *)
let run_generation = Atomic.make 0

(** Last heartbeat timestamp for stall detection by supervisor
    Uses Atomic float for thread-safe access without mutexes *)
let last_heartbeat = Atomic.make (Unix.gettimeofday ())

(** Get last heartbeat timestamp for external monitoring - thread-safe *)
let get_last_heartbeat () =
  Atomic.get last_heartbeat

(** Update heartbeat timestamp - thread-safe *)
let update_heartbeat () =
  Atomic.set last_heartbeat (Unix.gettimeofday ())

(** Memory monitor configuration - passed from main.ml via gc_config *)
type monitor_config = {
  check_interval_seconds: float;
  target_heap_mb: int;
  high_heap_mb: int;
  medium_heap_mb: int;
  high_fragmentation_percent: float;
  medium_fragmentation_percent: float;
}

(** Global shutdown flag for memory monitor *)
let monitor_shutdown = Atomic.make false

(** Signal shutdown to memory monitor *)
let signal_shutdown () =
  Logging.info ~section:"cleanup_coordinator" "Cleanup coordinator shutdown signaled";
  Atomic.set monitor_shutdown true;
  Atomic.set coordinator_running false

(** Stop cleanup coordinator to allow restart *)
let stop_cleanup_coordinator () =
  Logging.info ~section:"cleanup_coordinator" "Stopping cleanup coordinator for restart";
  Atomic.set monitor_shutdown true;
  Atomic.set coordinator_running false

(** Start the automated memory monitor loop - returns Lwt.t that completes when stopped *)
let start_memory_monitor ~config ~generation () =
  Logging.info_f ~section:"cleanup_coordinator" "Starting memory monitor loop (check interval: %.1fs, generation: %d)" 
    config.check_interval_seconds generation;
  
  let rec monitor_loop () =
    (* Check generation - if we're not the current generation, exit immediately *)
    if Atomic.get run_generation <> generation then begin
      Logging.info_f ~section:"cleanup_coordinator" "Memory monitor (gen %d) detected new generation (%d), exiting"
        generation (Atomic.get run_generation);
      Lwt.return_unit
    end else if Atomic.get monitor_shutdown || not (Atomic.get coordinator_running) then begin
      Logging.info_f ~section:"cleanup_coordinator" "Memory monitor (gen %d) shutting down" generation;
      Lwt.return_unit
    end else begin
      (* Use a simpler sleep that is friendly to Lwt *)
      Lwt_unix.sleep config.check_interval_seconds >>= fun () ->
      
      if Atomic.get run_generation <> generation then
        Lwt.return_unit
      else if Atomic.get monitor_shutdown || not (Atomic.get coordinator_running) then
        Lwt.return_unit
      else begin
        (* Publish heartbeat to verify consumer is alive *)
        Memory_events.publish_heartbeat ();
        
        (* Check GC stats for memory pressure *)
        let gc_stats = Gc.stat () in
        let heap_mb = gc_stats.heap_words * (Sys.word_size / 8) / 1048576 in
        let live_mb = gc_stats.live_words * (Sys.word_size / 8) / 1048576 in
        let fragmentation_percent =
          if gc_stats.heap_words > 0 then
            100.0 *. (1.0 -. (float_of_int gc_stats.live_words /. float_of_int gc_stats.heap_words))
          else 0.0
        in
        
        (* HARD LIMIT: Always compact if heap exceeds target *)
        if heap_mb > config.target_heap_mb then begin
          Logging.info_f ~section:"cleanup_coordinator" 
            "Heap exceeded target (%dMB > %dMB), forcing compaction (live=%dMB, frag=%.1f%%)"
            heap_mb config.target_heap_mb live_mb fragmentation_percent;
          Gc.compact ();
          let post_gc = Gc.stat () in
          let post_heap_mb = post_gc.heap_words * (Sys.word_size / 8) / 1048576 in
          let post_live_mb = post_gc.live_words * (Sys.word_size / 8) / 1048576 in
          Logging.info_f ~section:"cleanup_coordinator" 
            "Post-compaction: heap=%dMB, live=%dMB (freed %dMB)"
            post_heap_mb post_live_mb (heap_mb - post_heap_mb)
        end;
        
        (* Determine pressure level and publish event if warranted *)
        let pressure_level =
          if heap_mb >= config.high_heap_mb || fragmentation_percent >= config.high_fragmentation_percent then
            Some `High
          else if heap_mb >= config.medium_heap_mb || fragmentation_percent >= config.medium_fragmentation_percent then
            Some `Medium
          else
            None
        in
        
        (match pressure_level with
        | Some level ->
            Logging.debug_f ~section:"cleanup_coordinator" 
              "Memory pressure detected: level=%s, heap=%dMB, live=%dMB, frag=%.1f%%"
              (match level with `High -> "high" | `Medium -> "medium")
              heap_mb live_mb fragmentation_percent;
            Memory_events.publish_memory_pressure level heap_mb live_mb fragmentation_percent
        | None -> ());
        
        monitor_loop ()
      end
    end
  in
  monitor_loop ()

(** Start the cleanup coordinator - can be restarted after stopping
    Returns an Lwt.t that completes when the coordinator stops (for supervisor restart detection) *)
let start_cleanup_coordinator_supervised ~monitor_config () =
  (* Reset shutdown flag for restart *)
  Atomic.set monitor_shutdown false;
  
  (* Mark as running *)
  if not (Atomic.compare_and_set coordinator_running false true) then begin
    Logging.warn ~section:"cleanup_coordinator" "Cleanup coordinator already running, skipping start";
    Lwt.return_unit
  end else begin
    (* Increment generation for this new run *)
    let generation = Atomic.fetch_and_add run_generation 1 + 1 in
    Logging.info_f ~section:"cleanup_coordinator" "Starting supervised memory cleanup coordinator (generation %d)" generation;
    
    (* Initialize heartbeat *)
    update_heartbeat ();

    (* Subscribe to memory events only on first initialization *)
    let subscription = 
      if Atomic.compare_and_set coordinator_initialized false true then begin
        Logging.debug ~section:"cleanup_coordinator" "First initialization, subscribing to memory events";
        Memory_events.subscribe_memory_events ~persistent:true ()
      end else begin
        Logging.debug ~section:"cleanup_coordinator" "Restarting, resubscribing to memory events";
        Memory_events.subscribe_memory_events ~persistent:true ()
      end
    in

    (* Start event processing loop in background *)
    Lwt.async (fun () ->
      let rec process_events () =
        if Atomic.get run_generation <> generation || not (Atomic.get coordinator_running) then
          Lwt.return_unit
        else
          Lwt.catch (fun () ->
            Lwt_stream.get subscription.stream >>= function
            | Some event ->
                if Atomic.get run_generation = generation then begin
                  (* Critical: consumer updates heartbeat, proving liveness *)
                  update_heartbeat ();
                  
                  (* Handle event with local error handling to prevent consumer death *)
                  (try 
                    handle_memory_pressure_event event
                   with exn ->
                     Logging.error_f ~section:"cleanup_coordinator" 
                       "Exception handling memory event: %s" (Printexc.to_string exn)
                  );
                end;
                process_events ()
            | None ->
                Logging.info_f ~section:"cleanup_coordinator" "Memory event stream closed (gen %d)" generation;
                Lwt.return_unit
          ) (fun exn ->
            Logging.error_f ~section:"cleanup_coordinator" 
              "Fatal error in event processing loop (gen %d): %s" 
              generation (Printexc.to_string exn);
            (* Wait a bit before restarting loop to avoid tight failure loops *)
            Lwt_unix.sleep 1.0 >>= fun () ->
            process_events ()
          )
      in
      process_events ()
    );

    (* Run the memory monitor - this returns when shutdown *)
    start_memory_monitor ~config:monitor_config ~generation () >>= fun () ->
    
    (* Mark as no longer running *)
    Atomic.set coordinator_running false;
    Logging.info ~section:"cleanup_coordinator" "Cleanup coordinator stopped";
    Lwt.return_unit
  end

(** Legacy start function for backwards compatibility - uses Lwt.async internally *)
let start_cleanup_coordinator ~monitor_config () =
  (* Update heartbeat synchronously BEFORE Lwt.async to ensure watchdog can track properly
     even if the Lwt event loop hasn't processed the async task yet *)
  update_heartbeat ();
  Lwt.async (fun () -> start_cleanup_coordinator_supervised ~monitor_config ())

(** Get cleanup statistics *)
let get_cleanup_stats () = {
  telemetry_cleaned = stats.telemetry_cleaned;
  event_bus_cleaned = stats.event_bus_cleaned;
  event_registry_cleaned = stats.event_registry_cleaned;
  gc_cycles = stats.gc_cycles;
  last_cleanup = stats.last_cleanup;
}
