(** Logs cache for metrics broadcast

    Provides access to log data for the metrics broadcast system,
    respecting the concurrency model.
*)


open Concurrency

module LogMap = Map.Make(Int)

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
  mutable entry_keys: int Queue.t;
  mutable max_entries: int;
  mutable next_id: int;
  mutable dropped_logs: int;
  log_entry_event_bus: LogEntryEventBus.t;
  entries_mutex: Mutex.t;  (* Protect log_entries access *)
}

(** Global logs cache instance *)
let cache = {
  log_entries = LogMap.empty;
  entry_keys = Queue.create ();
  max_entries = 500;
  next_id = 0;
  dropped_logs = 0;
  log_entry_event_bus = LogEntryEventBus.create "log_entry";
  entries_mutex = Mutex.create ();
}

(** Thread-safe queue for pending log entries *)
let pending_logs_queue = Queue.create ()
let pending_logs_mutex = Mutex.create ()
let max_pending_logs = 1000 (* Limit the queue size *)
let (notification_r, notification_w) = Lwt_unix.pipe ()
let notification_buffer = Bytes.create 1

(* Set notification pipe write end to non-blocking to prevent blocking on full buffer *)
let () = Lwt_unix.set_blocking notification_w false

(** Initialization guard *)
let initialized = ref false

(** Subscribe to log entry updates *)
let subscribe_log_entries () = LogEntryEventBus.subscribe cache.log_entry_event_bus

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
  if Queue.length pending_logs_queue >= max_pending_logs then (
    try
      ignore (Queue.take pending_logs_queue);
      cache.dropped_logs <- cache.dropped_logs + 1;
    with Queue.Empty ->
      (* Queue was unexpectedly empty, just continue *)
      ()
  );
  Queue.push entry pending_logs_queue;
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

(** Get current log entries *)
let current_log_entries () =
  Mutex.lock cache.entries_mutex;
  let result = LogMap.bindings cache.log_entries |> List.map snd in
  Mutex.unlock cache.entries_mutex;
  result

(** Hard-cap entry_keys queue to max_entries to prevent unbounded growth *)
let cap_entry_keys_queue () =
  while Queue.length cache.entry_keys > cache.max_entries do
    try
      ignore (Queue.take cache.entry_keys)
    with Queue.Empty ->
      (* Queue was unexpectedly empty, just continue *)
      ()
  done

(** Integrity check: remove keys from entry_keys that don't exist in log_entries *)
let check_entry_keys_integrity () =
  Mutex.lock cache.entries_mutex;
  let valid_keys = ref [] in
  Queue.iter (fun key ->
    if Option.is_some (LogMap.find_opt key cache.log_entries) then
      valid_keys := key :: !valid_keys
  ) cache.entry_keys;
  (* Clear and repopulate queue with only valid keys *)
  Queue.clear cache.entry_keys;
  List.iter (fun key -> Queue.push key cache.entry_keys) (List.rev !valid_keys);
  Mutex.unlock cache.entries_mutex

(** Efficiently trim the cache to max_entries *)
let trim_cache () =
  Mutex.lock cache.entries_mutex;
  while LogMap.cardinal cache.log_entries > cache.max_entries do
    try
      let oldest_key = Queue.take cache.entry_keys in
      cache.log_entries <- LogMap.remove oldest_key cache.log_entries
    with Queue.Empty ->
      (* This should not happen if cardinal is > 0, but as a safeguard *)
      ()
  done;
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
  cache.log_entries <- LogMap.empty;
  Mutex.unlock cache.entries_mutex;
  Queue.clear cache.entry_keys

(** Filter log entries by level *)
let filter_by_level level =
  let filter_func =
    if level = "ALL" then fun _ -> true
    else fun entry -> entry.level = level
  in
  Mutex.lock cache.entries_mutex;
  let result = LogMap.filter (fun _ entry -> filter_func entry) cache.log_entries
              |> LogMap.bindings |> List.map snd in
  Mutex.unlock cache.entries_mutex;
  result

(** Filter log entries by section *)
let filter_by_section section =
  let filter_func =
    if section = "ALL" then fun _ -> true
    else fun entry -> entry.section = section
  in
  Mutex.lock cache.entries_mutex;
  let result = LogMap.filter (fun _ entry -> filter_func entry) cache.log_entries
              |> LogMap.bindings |> List.map snd in
  Mutex.unlock cache.entries_mutex;
  result

(** Get unique sections from log entries *)
let get_sections () =
  Mutex.lock cache.entries_mutex;
  let sections = Hashtbl.create 16 in
  LogMap.iter (fun _ entry ->
    Hashtbl.replace sections entry.section true
  ) cache.log_entries;
  let result = "ALL" :: (Hashtbl.fold (fun section _ acc -> section :: acc) sections []) in
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
      let count = min batch_size (Queue.length pending_logs_queue) in
      let entries_to_process =
        if count > 0 then
          List.init count (fun _ ->
            try Some (Queue.take pending_logs_queue)
            with Queue.Empty -> None
          ) |> List.filter_map Fun.id
        else [] in
      Mutex.unlock pending_logs_mutex;

      if entries_to_process <> [] then (
        Mutex.lock cache.entries_mutex;
        List.iter (fun entry ->
          cache.log_entries <- LogMap.add entry.id entry cache.log_entries;
          Queue.push entry.id cache.entry_keys
        ) entries_to_process;
        Mutex.unlock cache.entries_mutex;
        trim_cache ();

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
      monitoring_loop ()
    in
    monitoring_loop ()
  )

(** Initialize the logs cache system *)
let init () =
  if !initialized then (
    Logging.debug ~section:"logs_cache" "Logs cache already initialized, skipping";
    ()
  ) else (
    initialized := true;
    start_logs_updater ();
    Logging.info ~section:"logs_cache" "Logs cache initialized"
  )
