(** Logs cache for the logs view

    Provides reactive access to log data for the logs dashboard UI,
    respecting the concurrency model.
*)

module LogMap = Map.Make(Int)

(** Helper function to take first n elements from a list *)
let rec take n = function
  | [] -> []
  | x :: xs -> if n <= 0 then [] else x :: take (n - 1) xs

(** Log entry type for UI display *)
type log_entry = {
  id: int;
  timestamp: string;
  level: string;
  section: string;
  message: string;
  color: Notty.A.t;
}

(** Logs cache state *)
type t = {
  mutable log_entries: log_entry LogMap.t;
  mutable entry_keys: int Queue.t;
  mutable max_entries: int;
  mutable next_id: int;
  mutable dropped_logs: int;
}

(** Global logs cache instance *)
let cache = {
  log_entries = LogMap.empty;
  entry_keys = Queue.create ();
  max_entries = 500;
  next_id = 0;
  dropped_logs = 0;
}

(** Thread-safe queue for pending log entries *)
let pending_logs_queue = Queue.create ()
let pending_logs_mutex = Mutex.create ()
let max_pending_logs = 1000 (* Limit the queue size *)
let (notification_r, notification_w) = Lwt_unix.pipe ()
let notification_buffer = Bytes.create 1

(** Initialization guard *)
let initialized = ref false

(** Reactive variable for log entries *)
let log_entries_var = Lwd.var (LogMap.empty, 0)

(** Update counter to force reactivity *)
let update_counter = ref 0

(** Get reactive log entries variable *)
let get_log_entries_var () =
  (* Update the reactive variable with current state if needed *)
  incr update_counter;
  Lwd.set log_entries_var (cache.log_entries, !update_counter);
  log_entries_var

(** Convert log level to color *)
let level_to_color level =
  match level with
  | "DEBUG" -> Notty.A.(fg cyan)
  | "INFO" -> Notty.A.(fg green)
  | "WARN" -> Notty.A.(fg yellow)
  | "ERROR" -> Notty.A.(fg red)
  | "CRITICAL" -> Notty.A.(fg red ++ st bold ++ bg red)
  | _ -> Notty.A.(fg white)

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
  let color = level_to_color level in
  
  Mutex.lock pending_logs_mutex;
  let id = cache.next_id in
  cache.next_id <- id + 1;
  let entry = { id; timestamp; level; section; message; color } in
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

  (* Asynchronously notify the ingestion loop, returning a promise *)
  try%lwt
    let%lwt _ = Lwt_unix.write notification_w (Bytes.of_string "n") 0 1 in
    Lwt.return_unit
  with Unix.Unix_error (EPIPE, _, _) ->
    Lwt.return_unit

(** Get current log entries *)
let current_log_entries () =
  LogMap.bindings cache.log_entries |> List.map snd

(** Efficiently trim the cache to max_entries *)
let trim_cache () =
  while LogMap.cardinal cache.log_entries > cache.max_entries do
    try
      let oldest_key = Queue.take cache.entry_keys in
      cache.log_entries <- LogMap.remove oldest_key cache.log_entries
    with Queue.Empty ->
      (* This should not happen if cardinal is > 0, but as a safeguard *)
      ()
  done

(** Set maximum number of log entries to keep *)
let set_max_entries max_entries =
  cache.max_entries <- max_entries;
  trim_cache ()

(** Clear all log entries *)
let clear_logs () =
  cache.log_entries <- LogMap.empty;
  Queue.clear cache.entry_keys;
  incr update_counter;
  Lwd.set log_entries_var (LogMap.empty, !update_counter)

(** Filter log entries by level *)
let filter_by_level level =
  let filter_func =
    if level = "ALL" then fun _ -> true
    else fun entry -> entry.level = level
  in
  LogMap.filter (fun _ entry -> filter_func entry) cache.log_entries
  |> LogMap.bindings |> List.map snd

(** Filter log entries by section *)
let filter_by_section section =
  let filter_func =
    if section = "ALL" then fun _ -> true
    else fun entry -> entry.section = section
  in
  LogMap.filter (fun _ entry -> filter_func entry) cache.log_entries
  |> LogMap.bindings |> List.map snd

(** Get unique sections from log entries *)
let get_sections () =
  let sections = Hashtbl.create 16 in
  LogMap.iter (fun _ entry ->
    Hashtbl.replace sections entry.section true
  ) cache.log_entries;
  "ALL" :: (Hashtbl.fold (fun section _ acc -> section :: acc) sections [])

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
        List.iter (fun entry ->
          cache.log_entries <- LogMap.add entry.id entry cache.log_entries;
          Queue.push entry.id cache.entry_keys
        ) entries_to_process;
        trim_cache ();
      );
      
      (* Loop to wait for the next notification *)
      ingestion_loop ()
    in
    ingestion_loop ()
  );

  (* No longer need the async UI update loop - reactive variable is updated on access *)

  (* Periodic memory monitoring update *)
  Lwt.async (fun () ->
    let rec monitoring_loop () =
      let%lwt () = Lwt_unix.sleep 10.0 in (* Update every 10 seconds *)
      let _dropped_count = get_dropped_logs_count () in
      (* set_logs_dropped_count removed, replaced with no-op *)
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
    incr update_counter;
    Lwd.set log_entries_var (LogMap.empty, !update_counter);
    start_logs_updater ();
    Logging.info ~section:"logs_cache" "Logs cache initialized"
  )
