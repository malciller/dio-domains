(** Domain Profiler - Tracks memory usage per OCaml domain

    Monitors heap usage, allocation rates, and domain-specific memory patterns
    to identify domain-level memory leaks and performance issues.
*)

(** Helper function to take first n elements from a list *)
let rec take n = function
  | [] -> []
  | x :: xs -> if n <= 0 then [] else x :: take (n - 1) xs

(** Domain memory statistics *)
type domain_stats = {
  domain_id: int;
  mutable heap_words: int;
  mutable live_words: int;
  mutable free_words: int;
  mutable fragments: int;
  mutable compactions: int;
  mutable minor_collections: int;
  mutable major_collections: int;
  mutable allocations_since_gc: int;
  mutable last_gc_time: float;
  created_at: float;
  mutable last_updated: float;
}

(** Domain allocation tracking *)
type domain_allocation = {
  timestamp: float;
  size: int;
  structure_type: string;
  allocation_site: Allocation_tracker.allocation_site;
}

(** Global domain statistics table *)
let domain_stats_table : (int, domain_stats) Allocation_tracker.TrackedHashtbl.t = Allocation_tracker.TrackedHashtbl.create 16
let domain_allocations_table : (int, domain_allocation list) Allocation_tracker.TrackedHashtbl.t = Allocation_tracker.TrackedHashtbl.create 16
let stats_mutex = Mutex.create ()

(** Get current domain ID safely *)
let get_current_domain_id () =
  try Obj.magic (Domain.self ())
  with _ -> 0

(** Initialize domain tracking for a new domain *)
let init_domain_tracking domain_id =
  (* Use try_lock to avoid blocking domain initialization *)
  if Mutex.try_lock stats_mutex then begin
    if not (Allocation_tracker.TrackedHashtbl.mem domain_stats_table domain_id) then begin
      let now = Unix.gettimeofday () in
      (* Capture initial GC stats to avoid starting with zeros *)
      let gc_stats = Gc.quick_stat () in
      let stats = {
        domain_id;
        heap_words = gc_stats.heap_words;
        live_words = gc_stats.live_words;
        free_words = gc_stats.free_words;
        fragments = gc_stats.fragments;
        compactions = gc_stats.compactions;
        minor_collections = 0;
        major_collections = 0;
        allocations_since_gc = 0;
        last_gc_time = now;
        created_at = now;
        last_updated = now;
      } in
      Allocation_tracker.TrackedHashtbl.add domain_stats_table domain_id stats;
      Allocation_tracker.TrackedHashtbl.add domain_allocations_table domain_id [];
      Logging.debug_f ~section:"domain_profiler" "Initialized tracking for domain %d (heap: %dMB, live: %dMB)"
        domain_id
        (gc_stats.heap_words * (Sys.word_size / 8) / 1048576)
        (gc_stats.live_words * (Sys.word_size / 8) / 1048576);
    end;
    Mutex.unlock stats_mutex
  end else begin
    (* Skip domain profiling initialization if mutex is contended to avoid deadlocks *)
    Logging.debug_f ~section:"domain_profiler" "Skipped domain tracking initialization for domain %d (mutex contended)" domain_id;
  end

(** Update domain statistics from GC stats *)
let rec update_domain_stats domain_id gc_stats =
  (* Use try_lock to avoid blocking during domain initialization *)
  if Mutex.try_lock stats_mutex then begin
    match Allocation_tracker.TrackedHashtbl.find_opt domain_stats_table domain_id with
    | Some stats ->
        stats.heap_words <- gc_stats.Gc.heap_words;
        stats.live_words <- gc_stats.Gc.live_words;
        stats.free_words <- gc_stats.Gc.free_words;
        stats.fragments <- gc_stats.Gc.fragments;
        stats.compactions <- gc_stats.Gc.compactions;
        stats.last_updated <- Unix.gettimeofday ();
    | None ->
        init_domain_tracking domain_id;
        (* Retry after initialization *)
        update_domain_stats domain_id gc_stats
    ;
    Mutex.unlock stats_mutex
  end else begin
    (* Skip update if mutex is contended to avoid deadlocks *)
    Logging.debug_f ~section:"domain_profiler" "Skipped domain stats update for domain %d (mutex contended)" domain_id;
  end

(** Record domain allocation *)
let rec record_domain_allocation domain_id allocation =
  if Config.is_memory_tracing_enabled () then begin
    (* Use try_lock to avoid blocking during domain initialization *)
    if Mutex.try_lock stats_mutex then begin
      match Allocation_tracker.TrackedHashtbl.find_opt domain_stats_table domain_id with
      | Some stats ->
          stats.allocations_since_gc <- stats.allocations_since_gc + 1;
          let alloc_record = {
            timestamp = Unix.gettimeofday ();
            size = allocation.Allocation_tracker.size;
            structure_type = allocation.Allocation_tracker.structure_type;
            allocation_site = allocation.Allocation_tracker.site;
          } in
          let current_allocs = Allocation_tracker.TrackedHashtbl.find domain_allocations_table domain_id in
          let new_allocs = alloc_record :: current_allocs in
          (* Limit allocation history per domain to prevent unbounded growth *)
          let trimmed_allocs = if List.length new_allocs > 100 then
            take 100 new_allocs  (* Keep most recent 100 *)
          else new_allocs in
          Allocation_tracker.TrackedHashtbl.replace domain_allocations_table domain_id trimmed_allocs;
      | None ->
          init_domain_tracking domain_id;
          (* Retry after initialization *)
          record_domain_allocation domain_id allocation
      ;
      Mutex.unlock stats_mutex
    end else begin
      (* Skip allocation recording if mutex is contended to avoid deadlocks *)
      Logging.debug_f ~section:"domain_profiler" "Skipped domain allocation recording for domain %d (mutex contended)" domain_id;
    end
  end

(** Record GC collection for domain *)
let rec record_gc_collection domain_id is_major =
  (* Use try_lock to avoid blocking during domain initialization *)
  if Mutex.try_lock stats_mutex then begin
    match Allocation_tracker.TrackedHashtbl.find_opt domain_stats_table domain_id with
    | Some stats ->
        if is_major then
          stats.major_collections <- stats.major_collections + 1
        else
          stats.minor_collections <- stats.minor_collections + 1;
        stats.last_gc_time <- Unix.gettimeofday ();
        stats.allocations_since_gc <- 0;  (* Reset counter *)
    | None ->
        init_domain_tracking domain_id;
        (* Retry after initialization *)
        record_gc_collection domain_id is_major
    ;
    Mutex.unlock stats_mutex
  end else begin
    (* Skip GC recording if mutex is contended to avoid deadlocks *)
    Logging.debug_f ~section:"domain_profiler" "Skipped GC collection recording for domain %d (mutex contended)" domain_id;
  end

(** Get domain memory usage in MB *)
let get_domain_memory_mb domain_id =
  (* Use try_lock with timeout to avoid hanging *)
  let timeout_ms = 100 in  (* 100ms timeout *)
  let rec try_lock_with_timeout attempts_left =
    if attempts_left <= 0 then (
      Logging.debug_f ~section:"domain_profiler" "Timeout waiting for stats mutex in get_domain_memory_mb";
      (0.0, 0.0)  (* Return default values on timeout *)
    ) else if Mutex.try_lock stats_mutex then (
      let result = match Allocation_tracker.TrackedHashtbl.find_opt domain_stats_table domain_id with
        | Some stats ->
            let word_size_bytes = Sys.word_size / 8 in
            let heap_mb = float_of_int (stats.heap_words * word_size_bytes) /. (1024.0 *. 1024.0) in
            let live_mb = float_of_int (stats.live_words * word_size_bytes) /. (1024.0 *. 1024.0) in
            (heap_mb, live_mb)
        | None -> (0.0, 0.0)
      in
      Mutex.unlock stats_mutex;
      result
    ) else (
      Thread.delay 0.001;  (* Wait 1ms before retry *)
      try_lock_with_timeout (attempts_left - 1)
    )
  in
  try_lock_with_timeout timeout_ms

(** Get allocation rate for domain (allocations per second since last GC) *)
let get_domain_allocation_rate domain_id =
  (* Use try_lock with timeout to avoid hanging *)
  let timeout_ms = 100 in  (* 100ms timeout *)
  let rec try_lock_with_timeout attempts_left =
    if attempts_left <= 0 then (
      Logging.debug_f ~section:"domain_profiler" "Timeout waiting for stats mutex in get_domain_allocation_rate";
      0.0  (* Return default value on timeout *)
    ) else if Mutex.try_lock stats_mutex then (
      let result = match Allocation_tracker.TrackedHashtbl.find_opt domain_stats_table domain_id with
        | Some stats ->
            let time_since_gc = Unix.gettimeofday () -. stats.last_gc_time in
            if time_since_gc > 0.0 then
              float_of_int stats.allocations_since_gc /. time_since_gc
            else 0.0
        | None -> 0.0
      in
      Mutex.unlock stats_mutex;
      result
    ) else (
      Thread.delay 0.001;  (* Wait 1ms before retry *)
      try_lock_with_timeout (attempts_left - 1)
    )
  in
  try_lock_with_timeout timeout_ms

(** Get domain statistics *)
let get_domain_stats domain_id =
  (* Use try_lock with timeout to avoid hanging *)
  let timeout_ms = 100 in  (* 100ms timeout *)
  let rec try_lock_with_timeout attempts_left =
    if attempts_left <= 0 then (
      Logging.debug_f ~section:"domain_profiler" "Timeout waiting for stats mutex in get_domain_stats";
      None  (* Return None on timeout *)
    ) else if Mutex.try_lock stats_mutex then (
      let result = Allocation_tracker.TrackedHashtbl.find_opt domain_stats_table domain_id in
      Mutex.unlock stats_mutex;
      result
    ) else (
      Thread.delay 0.001;  (* Wait 1ms before retry *)
      try_lock_with_timeout (attempts_left - 1)
    )
  in
  try_lock_with_timeout timeout_ms

(** Get all domain statistics *)
let get_all_domain_stats () =
  (* Use try_lock with timeout to avoid hanging *)
  let timeout_ms = 100 in  (* 100ms timeout *)
  let rec try_lock_with_timeout attempts_left =
    if attempts_left <= 0 then (
      Logging.debug_f ~section:"domain_profiler" "Timeout waiting for stats mutex in get_all_domain_stats";
      []  (* Return empty list on timeout *)
    ) else if Mutex.try_lock stats_mutex then (
      let stats_list = Allocation_tracker.TrackedHashtbl.fold (fun _ stats acc -> stats :: acc) domain_stats_table [] in
      Mutex.unlock stats_mutex;
      stats_list
    ) else (
      Thread.delay 0.001;  (* Wait 1ms before retry *)
      try_lock_with_timeout (attempts_left - 1)
    )
  in
  try_lock_with_timeout timeout_ms

(** Get domain allocation history *)
let get_domain_allocations domain_id =
  (* Use try_lock with timeout to avoid hanging *)
  let timeout_ms = 100 in  (* 100ms timeout *)
  let rec try_lock_with_timeout attempts_left =
    if attempts_left <= 0 then (
      Logging.debug_f ~section:"domain_profiler" "Timeout waiting for stats mutex in get_domain_allocations";
      []  (* Return empty list on timeout *)
    ) else if Mutex.try_lock stats_mutex then (
      let result = Allocation_tracker.TrackedHashtbl.find_opt domain_allocations_table domain_id |> Option.value ~default:[] in
      Mutex.unlock stats_mutex;
      result
    ) else (
      Thread.delay 0.001;  (* Wait 1ms before retry *)
      try_lock_with_timeout (attempts_left - 1)
    )
  in
  try_lock_with_timeout timeout_ms

(** Calculate memory pressure for domain *)
let get_domain_memory_pressure domain_id =
  match get_domain_stats domain_id with
  | Some stats ->
      let word_size_bytes = Sys.word_size / 8 in
      let heap_mb = float_of_int (stats.heap_words * word_size_bytes) /. (1024.0 *. 1024.0) in
      let free_ratio = if stats.heap_words > 0 then 1.0 -. (float_of_int stats.live_words /. float_of_int stats.heap_words) else 0.0 in

      if heap_mb > 2048.0 || free_ratio < 0.1 then `Critical
      else if heap_mb > 1024.0 || free_ratio < 0.2 then `High
      else if heap_mb > 512.0 || free_ratio < 0.3 then `Moderate
      else `Normal
  | None -> `Normal

(** Periodic cleanup of old allocation history across all domains *)
let cleanup_old_allocation_history () =
  (* Use try_lock to avoid blocking during normal operation *)
  if Mutex.try_lock stats_mutex then
    let now = Unix.gettimeofday () in
    let cutoff_time = now -. 3600.0 in  (* Remove allocations older than 1 hour *)
    let total_cleaned = ref 0 in

    Allocation_tracker.TrackedHashtbl.iter (fun domain_id allocs ->
      let filtered_allocs = List.filter (fun alloc -> alloc.timestamp >= cutoff_time) allocs in
      let cleaned_count = List.length allocs - List.length filtered_allocs in
      if cleaned_count > 0 then begin
        Allocation_tracker.TrackedHashtbl.replace domain_allocations_table domain_id filtered_allocs;
        total_cleaned := !total_cleaned + cleaned_count;
      end
    ) domain_allocations_table;

    Mutex.unlock stats_mutex;

    (* Log results *)
    if !total_cleaned > 0 then
      Logging.debug_f ~section:"domain_profiler" "Cleaned up %d old allocation records across all domains" !total_cleaned
    else
      Logging.debug ~section:"domain_profiler" "No old allocation records to clean up"
  else
    Logging.debug ~section:"domain_profiler" "Skipped periodic allocation history cleanup (mutex contended)"

(** Clean up domain tracking when domain terminates *)
let cleanup_domain_tracking domain_id =
  (* Use try_lock to avoid blocking during domain cleanup *)
  if Mutex.try_lock stats_mutex then begin
    Allocation_tracker.TrackedHashtbl.remove domain_stats_table domain_id;
    Allocation_tracker.TrackedHashtbl.remove domain_allocations_table domain_id;
    Logging.debug_f ~section:"domain_profiler" "Cleaned up tracking for terminated domain %d" domain_id;
    Mutex.unlock stats_mutex
  end else begin
    (* Skip cleanup if mutex is contended - domain is terminating anyway *)
    Logging.debug_f ~section:"domain_profiler" "Skipped cleanup for terminated domain %d (mutex contended)" domain_id;
  end

(** Memory summary record *)
type memory_summary = {
  total_heap_mb: float;
  total_live_mb: float;
  domain_count: int;
}

(** Get memory usage summary across all domains *)
let get_memory_summary () =
  (* Use try_lock with timeout to avoid hanging *)
  let timeout_ms = 100 in  (* 100ms timeout *)
  let rec try_lock_with_timeout attempts_left =
    if attempts_left <= 0 then (
      Logging.debug_f ~section:"domain_profiler" "Timeout waiting for stats mutex in get_memory_summary";
      (* Return basic GC stats without domain count on timeout *)
      let gc_stats = Gc.stat () in
      let word_size_bytes = Sys.word_size / 8 in
      {
        total_heap_mb = float_of_int (gc_stats.heap_words * word_size_bytes) /. (1024.0 *. 1024.0);
        total_live_mb = float_of_int (gc_stats.live_words * word_size_bytes) /. (1024.0 *. 1024.0);
        domain_count = 0;  (* Unknown on timeout *)
      }
    ) else if Mutex.try_lock stats_mutex then (
      let domain_count = Allocation_tracker.TrackedHashtbl.length domain_stats_table in
      Mutex.unlock stats_mutex;

      (* Use Gc.stat() for accurate current memory usage - this gives us global stats *)
      let gc_stats = Gc.stat () in
      let word_size_bytes = Sys.word_size / 8 in

      {
        total_heap_mb = float_of_int (gc_stats.heap_words * word_size_bytes) /. (1024.0 *. 1024.0);
        total_live_mb = float_of_int (gc_stats.live_words * word_size_bytes) /. (1024.0 *. 1024.0);
        domain_count;
      }
    ) else (
      Thread.delay 0.001;  (* Wait 1ms before retry *)
      try_lock_with_timeout (attempts_left - 1)
    )
  in
  try_lock_with_timeout timeout_ms

(** Initialize domain profiler - set up runtime events callbacks *)
let init () =
  (* Register callbacks for runtime events *)
  Runtime_events_tracer.add_callback (function
    | Runtime_events_tracer.Minor_collection { domain_id; heap_words; live_words; _ } ->
        record_gc_collection domain_id false;
        let real_stats = Gc.quick_stat () in
        let gc_stats = { real_stats with Gc.heap_words; live_words; free_words = heap_words - live_words } in
        update_domain_stats domain_id gc_stats;
        Logging.debug_f ~section:"domain_profiler" "Updated domain %d stats from minor GC: heap=%dMB live=%dMB"
          domain_id (gc_stats.heap_words * (Sys.word_size / 8) / 1048576) (gc_stats.live_words * (Sys.word_size / 8) / 1048576)

    | Runtime_events_tracer.Major_collection { domain_id; heap_words; live_words; free_words; fragments; compactions; _ } ->
        record_gc_collection domain_id true;
        let real_stats = Gc.quick_stat () in
        let gc_stats = { real_stats with Gc.heap_words; live_words; free_words; fragments; compactions } in
        update_domain_stats domain_id gc_stats;
        Logging.debug_f ~section:"domain_profiler" "Updated domain %d stats from major GC: heap=%dMB live=%dMB free=%dMB"
          domain_id (gc_stats.heap_words * (Sys.word_size / 8) / 1048576) (gc_stats.live_words * (Sys.word_size / 8) / 1048576)
          (gc_stats.free_words * (Sys.word_size / 8) / 1048576)

    | Runtime_events_tracer.Domain_spawn { domain_id; _ } ->
        init_domain_tracking domain_id;
        Logging.debug_f ~section:"domain_profiler" "Domain %d spawned and tracking initialized" domain_id

    | Runtime_events_tracer.Domain_terminate { domain_id; _ } ->
        cleanup_domain_tracking domain_id;
        Logging.debug_f ~section:"domain_profiler" "Domain %d terminated and tracking cleaned up" domain_id
  );

  (* Register callbacks for allocation events *)
  Allocation_tracker.add_callback (function
    | Allocation_tracker.Allocation record ->
        record_domain_allocation record.Allocation_tracker.domain_id record
    | Allocation_tracker.Deallocation _ -> ()  (* Deallocations don't need special domain handling *)
    | Allocation_tracker.SizeUpdate _ -> ()    (* Size updates don't need special domain handling *)
  );

  (* Initialize tracking for main domain *)
  init_domain_tracking (get_current_domain_id ());

  (* Start periodic fallback GC trigger (every 30 seconds) *)
  (* This ensures GC runs periodically to keep stats fresh, even if runtime events aren't working *)
  let fallback_thread = Thread.create (fun () ->
    while true do
      Thread.delay 30.0;  (* Wait 30 seconds *)
      try
        (* Trigger a minor GC to ensure stats are fresh *)
        Gc.minor ();
        Logging.debug ~section:"domain_profiler" "Fallback GC trigger completed";
      with exn ->
        Logging.debug_f ~section:"domain_profiler" "Fallback GC trigger failed: %s" (Printexc.to_string exn);
    done
  ) () in

  (* Start periodic allocation history cleanup (every 5 minutes) *)
  let cleanup_thread = Thread.create (fun () ->
    while true do
      Thread.delay 300.0;  (* Wait 5 minutes *)
      try
        cleanup_old_allocation_history ();
      with exn ->
        Logging.debug_f ~section:"domain_profiler" "Allocation history cleanup failed: %s" (Printexc.to_string exn);
    done
  ) () in

  (* Store thread references for cleanup (though they run forever) *)
  ignore fallback_thread;
  ignore cleanup_thread;

  Logging.info ~section:"domain_profiler" "Domain profiler initialized with fallback GC refresh and allocation cleanup"

(** Performance metrics record *)
type performance_metrics = {
  domain_id: int;
  heap_mb: float;
  live_mb: float;
  alloc_rate: float;
  pressure: [`Normal | `Moderate | `High | `Critical];
  uptime: float;
  minor_collections: int;
  major_collections: int;
}

(** Get domain performance metrics for monitoring *)
let get_domain_performance_metrics () =
  let domains = get_all_domain_stats () in
  List.map (fun (ds : domain_stats) ->
    let (heap_mb, live_mb) = get_domain_memory_mb ds.domain_id in
    let alloc_rate = get_domain_allocation_rate ds.domain_id in
    let pressure = get_domain_memory_pressure ds.domain_id in
    let uptime = Unix.gettimeofday () -. ds.created_at in

    {
      domain_id = ds.domain_id;
      heap_mb;
      live_mb;
      alloc_rate;
      pressure;
      uptime;
      minor_collections = ds.minor_collections;
      major_collections = ds.major_collections;
    }
  ) domains

(** Get domain stats count for monitoring *)
let get_domain_stats_count () =
  (* Use try_lock with timeout to avoid hanging *)
  let timeout_ms = 100 in  (* 100ms timeout *)
  let rec try_lock_with_timeout attempts_left =
    if attempts_left <= 0 then (
      Logging.debug_f ~section:"domain_profiler" "Timeout waiting for stats mutex in get_domain_stats_count";
      0  (* Return default value on timeout *)
    ) else if Mutex.try_lock stats_mutex then (
      let count = Allocation_tracker.TrackedHashtbl.length domain_stats_table in
      Mutex.unlock stats_mutex;
      count
    ) else (
      Thread.delay 0.001;  (* Wait 1ms before retry *)
      try_lock_with_timeout (attempts_left - 1)
    )
  in
  try_lock_with_timeout timeout_ms

(** Get domain allocations count for monitoring *)
let get_domain_allocations_count () =
  (* Use try_lock with timeout to avoid hanging *)
  let timeout_ms = 100 in  (* 100ms timeout *)
  let rec try_lock_with_timeout attempts_left =
    if attempts_left <= 0 then (
      Logging.debug_f ~section:"domain_profiler" "Timeout waiting for stats mutex in get_domain_allocations_count";
      0  (* Return default value on timeout *)
    ) else if Mutex.try_lock stats_mutex then (
      let total_count = ref 0 in
      Allocation_tracker.TrackedHashtbl.iter (fun _ allocs ->
        total_count := !total_count + List.length allocs
      ) domain_allocations_table;
      Mutex.unlock stats_mutex;
      !total_count
    ) else (
      Thread.delay 0.001;  (* Wait 1ms before retry *)
      try_lock_with_timeout (attempts_left - 1)
    )
  in
  try_lock_with_timeout timeout_ms
