(** Logs cache for metrics broadcast

    Provides access to log data for the metrics broadcast system,
    respecting the concurrency model.
*)

(** Import memory tracing for tracked data structures *)
let () = try ignore (Sys.getenv "DIO_MEMORY_TRACING") with Not_found -> ()

open Concurrency

module LogMap = Dio_memory_tracing.Memory_tracing.Tracked.Map.Make(Int)

(** Helper function to take first n elements from a list *)
let rec take n = function
  | [] -> []
  | x :: xs -> if n <= 0 then [] else x :: take (n - 1) xs

(** Log entry type for JSON serialization *)
type log_entry = {
  id: int;
  timestamp: string;
  level: string;
  section: string;
  message: string;
}

(** Event bus for log entries *)
module LogEntryEventBus = Event_bus.Make(struct
  type t = log_entry
end)

(** Logs cache state *)
type t = {
  mutable log_entries: log_entry LogMap.t;
  mutable entry_keys: int Dio_memory_tracing.Memory_tracing.Tracked.Queue.t;
  mutable max_entries: int;
  mutable next_id: int;
  mutable dropped_logs: int;
  log_entry_event_bus: LogEntryEventBus.t;
  entries_mutex: Mutex.t;  (* Protect log_entries access *)
}

(** Global logs cache instance with tighter memory bounds *)
let cache = {
  log_entries = LogMap.empty ();
  entry_keys = Dio_memory_tracing.Memory_tracing.Tracked.Queue.create ();
  max_entries = 200;  (* Reduced from 500 for more aggressive memory management *)
  next_id = 0;
  dropped_logs = 0;
  log_entry_event_bus = LogEntryEventBus.create "log_entry";
  entries_mutex = Mutex.create ();
}

(** Thread-safe queue for pending log entries with proactive limits *)
let pending_logs_queue = Dio_memory_tracing.Memory_tracing.Tracked.Queue.create ()
let pending_logs_mutex = Mutex.create ()
let max_pending_logs = 500  (* Reduced from 1000 for more aggressive memory management *)
let proactive_pending_threshold = 400  (* Start dropping when we reach 400 pending logs *)
let (notification_r, notification_w) = Lwt_unix.pipe ()
let notification_buffer = Bytes.create 1

(* Set notification pipe write end to non-blocking to prevent blocking on full buffer *)
let () = Lwt_unix.set_blocking notification_w false

(** Initialization guard *)
let initialized = ref false

(** Subscribe to log entry updates *)
let subscribe_log_entries () =
  let subscription = LogEntryEventBus.subscribe ~persistent:true cache.log_entry_event_bus in
  (subscription.stream, subscription.close)

(** Get logs event bus subscriber statistics *)
let get_logs_subscriber_stats () =
  LogEntryEventBus.get_subscriber_stats cache.log_entry_event_bus

(** Force cleanup stale subscribers for dashboard memory management *)
let force_cleanup_stale_subscribers () =
  LogEntryEventBus.force_cleanup_stale_subscribers cache.log_entry_event_bus ()

(** Get current subscriber count for memory monitoring *)
let get_subscriber_count () =
  LogEntryEventBus.get_subscriber_count cache.log_entry_event_bus

(** Format timestamp for display *)
let format_display_timestamp () =
  let time = Unix.gettimeofday () in
  let sec = floor time in
  let ms = int_of_float ((time -. sec) *. 1000.) in
  let tm = Unix.localtime time in
  Printf.sprintf "%02d:%02d:%02d.%03d"
    tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec ms

(** Add a new log entry to the cache from any thread *)
let add_log_entry level section message =
  let timestamp = format_display_timestamp () in

  Mutex.lock pending_logs_mutex;
  let id = cache.next_id in
  cache.next_id <- id + 1;
  let entry = { id; timestamp; level; section; message } in

  (* Proactive dropping: drop oldest entries when approaching limit *)
  while Dio_memory_tracing.Memory_tracing.Tracked.Queue.length pending_logs_queue >= proactive_pending_threshold do
    try
      ignore (Dio_memory_tracing.Memory_tracing.Tracked.Queue.take pending_logs_queue);
      cache.dropped_logs <- cache.dropped_logs + 1;
    with Queue.Empty ->
      (* Queue was unexpectedly empty, just continue *)
      ()
  done;

  (* Hard limit: if still at max, drop this new entry *)
  if Dio_memory_tracing.Memory_tracing.Tracked.Queue.length pending_logs_queue >= max_pending_logs then (
    cache.dropped_logs <- cache.dropped_logs + 1;
    Mutex.unlock pending_logs_mutex;
    Lwt.return_unit  (* Don't add this entry *)
  ) else (
    Dio_memory_tracing.Memory_tracing.Tracked.Queue.push entry pending_logs_queue;
    Mutex.unlock pending_logs_mutex;

    (* Publish log entry to event bus for real-time broadcasting *)
    LogEntryEventBus.publish cache.log_entry_event_bus entry;

    (* Asynchronously notify the ingestion loop, returning a promise *)
    (* Pipe is non-blocking, so write will not block even if buffer is full *)
    try%lwt
      let%lwt _ = Lwt_unix.write notification_w (Bytes.of_string "n") 0 1 in
      Lwt.return_unit
    with
    | Unix.Unix_error (Unix.EPIPE, _, _) ->
        (* Pipe closed, ingestion loop probably stopped *)
        Lwt.return_unit
    | Unix.Unix_error (Unix.EAGAIN, _, _) | Unix.Unix_error (Unix.EWOULDBLOCK, _, _) ->
        (* Pipe buffer full, ingestion loop will catch up eventually *)
        Lwt.return_unit
    | _ ->
        (* Other write errors, continue anyway *)
        Lwt.return_unit
  )

(** Get current log entries *)
let current_log_entries () =
  Mutex.lock cache.entries_mutex;
  let result = LogMap.bindings cache.log_entries |> List.map snd in
  Mutex.unlock cache.entries_mutex;
  result

(** Hard-cap entry_keys queue to max_entries to prevent unbounded growth *)
let cap_entry_keys_queue () =
  while Dio_memory_tracing.Memory_tracing.Tracked.Queue.length cache.entry_keys > cache.max_entries do
    try
      ignore (Dio_memory_tracing.Memory_tracing.Tracked.Queue.take cache.entry_keys)
    with Queue.Empty ->
      (* Queue was unexpectedly empty, just continue *)
      ()
  done

(** Integrity check: remove keys from entry_keys that don't exist in log_entries *)
let check_entry_keys_integrity () =
  Mutex.lock cache.entries_mutex;
  let valid_keys = ref [] in
  Dio_memory_tracing.Memory_tracing.Tracked.Queue.iter (fun key ->
    if Option.is_some (LogMap.find_opt key cache.log_entries) then
      valid_keys := key :: !valid_keys
  ) cache.entry_keys;
  (* Clear and repopulate queue with only valid keys *)
  Dio_memory_tracing.Memory_tracing.Tracked.Queue.clear cache.entry_keys;
  List.iter (fun key -> Dio_memory_tracing.Memory_tracing.Tracked.Queue.push key cache.entry_keys) (List.rev !valid_keys);
  Mutex.unlock cache.entries_mutex

(** Efficiently trim the cache to max_entries *)
let trim_cache () =
  Mutex.lock cache.entries_mutex;

  (* Event-driven removal: batch all removals for efficiency *)
  if LogMap.cardinal cache.log_entries > cache.max_entries then begin
    let excess_count = LogMap.cardinal cache.log_entries - cache.max_entries in
    let keys_to_remove = ref [] in

    (* Collect keys to remove *)
    for _i = 1 to excess_count do
      try
        let oldest_key = Dio_memory_tracing.Memory_tracing.Tracked.Queue.take cache.entry_keys in
        keys_to_remove := oldest_key :: !keys_to_remove
      with Queue.Empty ->
        (* This should not happen if cardinal is > 0, but as a safeguard *)
        ()
    done;

    (* Batch remove all keys at once using replace_with_bindings *)
    if !keys_to_remove <> [] then begin
      let current_bindings = LogMap.bindings cache.log_entries in
      (* Use tracked operations for all filtering with proper cleanup *)
      let tracked_current_bindings = Dio_memory_tracing.Memory_tracing.Tracked.List.of_list current_bindings in
      let tracked_keys_to_remove = Dio_memory_tracing.Memory_tracing.Tracked.List.of_list !keys_to_remove in
      let filtered_bindings = Dio_memory_tracing.Memory_tracing.Tracked.List.destroy_and_filter (fun (key, _) ->
        not (Dio_memory_tracing.Memory_tracing.Tracked.List.mem key tracked_keys_to_remove)
      ) tracked_current_bindings in
      (* Destroy the keys list since we no longer need it *)
      Dio_memory_tracing.Memory_tracing.Tracked.List.destroy tracked_keys_to_remove;
      let final_bindings = Dio_memory_tracing.Memory_tracing.Tracked.List.to_list filtered_bindings in
      Dio_memory_tracing.Memory_tracing.Tracked.List.destroy filtered_bindings;
      let old_map = cache.log_entries in
      cache.log_entries <- LogMap.replace_with_bindings cache.log_entries final_bindings;
      (* Destroy the old map only if it was replaced *)
      if old_map != cache.log_entries then
        LogMap.destroy old_map;
    end;
  end;

  Mutex.unlock cache.entries_mutex;
  (* Ensure entry_keys queue doesn't exceed max_entries *)
  cap_entry_keys_queue ()

(** Set maximum number of log entries to keep *)
let set_max_entries max_entries =
  cache.max_entries <- max_entries;
  trim_cache ()

(** Clear all log entries *)
let clear_logs () =
  Mutex.lock cache.entries_mutex;
  cache.log_entries <- LogMap.destroy_and_empty cache.log_entries;
  Mutex.unlock cache.entries_mutex;
  Dio_memory_tracing.Memory_tracing.Tracked.Queue.clear cache.entry_keys

(** Filter log entries by level *)
let filter_by_level level =
  let filter_func =
    if level = "ALL" then fun _ -> true
    else fun entry -> entry.level = level
  in
  Mutex.lock cache.entries_mutex;
  let result = LogMap.fold (fun _ entry acc ->
    if filter_func entry then entry :: acc else acc
  ) cache.log_entries [] in
  Mutex.unlock cache.entries_mutex;
  List.rev result

(** Filter log entries by section *)
let filter_by_section section =
  let filter_func =
    if section = "ALL" then fun _ -> true
    else fun entry -> entry.section = section
  in
  Mutex.lock cache.entries_mutex;
  let result = LogMap.fold (fun _ entry acc ->
    if filter_func entry then entry :: acc else acc
  ) cache.log_entries [] in
  Mutex.unlock cache.entries_mutex;
  List.rev result

(** Get unique sections from log entries *)
let get_sections () =
  Mutex.lock cache.entries_mutex;
  let sections = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.create 16 in
  LogMap.iter (fun _ entry ->
    Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.replace sections entry.section true
  ) cache.log_entries;
  let result = "ALL" :: (Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.fold (fun section _ acc -> section :: acc) sections []) in
  Mutex.unlock cache.entries_mutex;
  result

(** Get and reset dropped logs counter (for monitoring) *)
let get_and_reset_dropped_logs () =
  Mutex.lock pending_logs_mutex;
  let count = cache.dropped_logs in
  cache.dropped_logs <- 0;
  Mutex.unlock pending_logs_mutex;
  count

(** Get current dropped logs count without resetting *)
let get_dropped_logs_count () =
  Mutex.lock pending_logs_mutex;
  let count = cache.dropped_logs in
  Mutex.unlock pending_logs_mutex;
  count

(** Event-driven memory cleanup callback - simplified for pure event-driven destruction *)
let memory_pressure_callback event =
  match event with
  | Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.SizeUpdate { id=_; old_size=_; new_size=_; structure_type; _ } ->
      (* Monitor map structure changes for potential memory pressure *)
      if structure_type = "Map" then begin
        (* Log when map structures change size - destruction happens automatically in add/remove operations *)
        Logging.debug ~section:"logs_cache" "Map structure size change detected";
      end
  | _ -> ()  (* Ignore other allocation events *)

(** Register for memory tracing events *)
let register_memory_events () =
  if Dio_memory_tracing.Memory_tracing.is_enabled () then begin
    Logging.debug ~section:"logs_cache" "Registering for memory tracing events";
    Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.add_callback memory_pressure_callback;
  end else begin
    Logging.debug ~section:"logs_cache" "Memory tracing disabled, skipping event registration";
  end

(** Start background logs updater *)
let start_logs_updater () =

  (* High-performance, yielding ingestion loop *)
  Lwt.async (fun () ->
    let rec ingestion_loop () =
      (* Wait for a notification that a log has been added *)
      let%lwt _ = Lwt_unix.read notification_r notification_buffer 0 1 in

      (* Process one batch of logs from the queue *)
      Mutex.lock pending_logs_mutex;
      let batch_size = 200 in
      let count = min batch_size (Dio_memory_tracing.Memory_tracing.Tracked.Queue.length pending_logs_queue) in
      let entries_to_process =
        if count > 0 then
          let rec collect_entries acc remaining =
            if remaining = 0 then List.rev acc
            else
              try
                let entry = Dio_memory_tracing.Memory_tracing.Tracked.Queue.take pending_logs_queue in
                collect_entries (entry :: acc) (remaining - 1)
              with Queue.Empty ->
                (* Queue became empty during collection, return what we have *)
                List.rev acc
          in
          collect_entries [] count
        else [] in
      Mutex.unlock pending_logs_mutex;

      if entries_to_process <> [] then (
        Mutex.lock cache.entries_mutex;

        (* Event-driven destruction: destroy old map before creating new one *)
        List.iter (fun entry ->
          (* Store reference to current map before modification *)
          let old_map = cache.log_entries in
          (* Create new map - this allocates a new structure *)
          cache.log_entries <- LogMap.add entry.id entry cache.log_entries;
          (* Immediately destroy the old map structure *)
          if old_map != cache.log_entries then
            LogMap.destroy old_map;
          Dio_memory_tracing.Memory_tracing.Tracked.Queue.push entry.id cache.entry_keys
        ) entries_to_process;
        Mutex.unlock cache.entries_mutex;
        trim_cache ();

        (* Event-driven destruction: if map became empty, destroy it immediately *)
        if LogMap.is_empty cache.log_entries then begin
          Logging.debug ~section:"logs_cache" "Immediately destroying empty log map after processing";
          cache.log_entries <- LogMap.destroy_and_empty cache.log_entries;
        end;

        (* No reactive updates needed - logs are published individually when added *)
      );

      (* Loop to wait for the next notification *)
      ingestion_loop ()
    in
    ingestion_loop ()
  );

  (* No longer need the async UI update loop - reactive variable is updated on access *)

  (* Periodic memory monitoring and integrity check *)
  Lwt.async (fun () ->
    let rec monitoring_loop () =
      let%lwt () = Lwt_unix.sleep 30.0 in (* Update every 30 seconds *)
      let dropped_count = get_dropped_logs_count () in
      (* Update telemetry with current dropped logs count *)
      Telemetry.set_logs_dropped_count dropped_count;
      (* Perform periodic integrity check on entry_keys queue *)
      check_entry_keys_integrity ();
      (* Skip periodic cleanup of broadcast event bus to prevent TCP streams from being interrupted *)
      (* let _ = force_cleanup_stale_subscribers () in *)
      monitoring_loop ()
    in
    monitoring_loop ()
  )

(** Subscribe to memory cleanup events for coordinated batch processing *)
let _memory_cleanup_subscription =
  try
    let (stream, close) = Dio_memory_cleanup.Memory_cleanup_events.subscribe_cleanup_events () in
    Lwt.async (fun () ->
      let rec process_events () =
        Lwt.catch
          (fun () -> Lwt.bind (Lwt_stream.get stream) (function
            | Some event ->
                (match event with
                | Dio_memory_cleanup.Memory_cleanup_events.AllocationThresholdExceeded { structure_type = "Map"; allocation_site; _ } ->
                    (* High-frequency Map allocations detected - could trigger immediate trim *)
                    Logging.debug_f ~section:"logs_cache" "Received Map allocation threshold event for %s" allocation_site;
                    trim_cache ()
                | Dio_memory_cleanup.Memory_cleanup_events.BatchMapUpdate { module_name = "logs_cache"; _ } ->
                    (* Batch Map update event - could trigger immediate trim *)
                    Logging.debug ~section:"logs_cache" "Received batch Map update event for logs_cache";
                    trim_cache ()
                 | _ -> ());
                process_events ()
            | None -> Lwt.return_unit))
          (fun exn ->
            Logging.debug_f ~section:"logs_cache" "Memory cleanup event subscription error: %s" (Printexc.to_string exn);
            Lwt.return_unit)
      in
      process_events ()
    );
    Some close
  with exn ->
    Logging.debug_f ~section:"logs_cache" "Failed to subscribe to memory cleanup events: %s" (Printexc.to_string exn);
    None

(** Initialize the logs cache system *)
let init () =
  if !initialized then (
    Logging.debug ~section:"logs_cache" "Logs cache already initialized, skipping";
    ()
  ) else (
    initialized := true;
    register_memory_events ();
    start_logs_updater ();
    Logging.info ~section:"logs_cache" "Logs cache initialized"
  )
