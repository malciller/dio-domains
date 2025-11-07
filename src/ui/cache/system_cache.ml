(** System monitoring cache for the system view

    Provides reactive access to system information for the system monitoring UI,
    respecting the concurrency model.
*)

open Lwt.Infix
open Concurrency
open Ui_types

(** Helper function to take first n elements from a list *)
let rec take n = function
  | [] -> []
  | x :: xs -> if n <= 0 then [] else x :: take (n - 1) xs

(* CPU information using cpuid and fallback methods *)
module Cpu_info = struct
  let get_cpu_cores () =
    (* Try cpuid first *)
    match Cpuid.cores () with
    | Ok cores -> Some cores
    | Error _ ->
        (* Fallback: try sysctl on macOS/Linux *)
        try
          let cmd = Unix.open_process_in "sysctl -n hw.ncpu 2>/dev/null" in
          Fun.protect
            ~finally:(fun () -> ignore (Unix.close_process_in cmd))
            (fun () ->
              let result = String.trim (input_line cmd) in
              Some (int_of_string result)
            )
        with _ ->
          (* Last fallback: try nproc command *)
          try
            let cmd = Unix.open_process_in "nproc 2>/dev/null" in
            Fun.protect
              ~finally:(fun () -> ignore (Unix.close_process_in cmd))
              (fun () ->
                let result = String.trim (input_line cmd) in
                Some (int_of_string result)
              )
          with _ -> None

  let get_cpu_vendor () =
    match Cpuid.vendor () with
    | Ok v ->
        let vendor_str = Format.asprintf "%a" Cpuid.pp_vendor v in
        Some vendor_str
    | Error _ ->
        (* On ARM/macOS, we can't get vendor via cpuid *)
        try
          let cmd = Unix.open_process_in "sysctl -n machdep.cpu.vendor 2>/dev/null" in
          Fun.protect
            ~finally:(fun () -> ignore (Unix.close_process_in cmd))
            (fun () ->
              let vendor_str = String.trim (input_line cmd) in
              Some vendor_str
            )
        with _ ->
          (* Try uname -m for architecture *)
          try
            let cmd = Unix.open_process_in "uname -m 2>/dev/null" in
            Fun.protect
              ~finally:(fun () -> ignore (Unix.close_process_in cmd))
              (fun () ->
                let arch = String.trim (input_line cmd) in
                match arch with
                | "arm64" | "aarch64" -> Some "Apple Silicon"
                | "x86_64" -> Some "Intel/AMD"
                | _ -> Some arch
              )
          with _ -> None

  let get_cpu_model () =
    match Cpuid.model () with
    | Ok (family, model, stepping) -> Some (Printf.sprintf "%d/%d/%d" family model stepping)
    | Error _ ->
        (* Try to get CPU model on macOS/ARM *)
        try
          let cmd = Unix.open_process_in "sysctl -n machdep.cpu.brand_string 2>/dev/null" in
          Fun.protect
            ~finally:(fun () -> ignore (Unix.close_process_in cmd))
            (fun () ->
              let model_str = String.trim (input_line cmd) in
              Some model_str
            )
        with _ ->
          (* Fallback to uname -p for processor type *)
          try
            let cmd = Unix.open_process_in "uname -p 2>/dev/null" in
            Fun.protect
              ~finally:(fun () -> ignore (Unix.close_process_in cmd))
              (fun () ->
                let proc = String.trim (input_line cmd) in
                Some proc
              )
          with _ -> None
end

(* Temperature monitoring module *)
module Temperature = struct
  (** Try to get temperature from macOS *)
  let get_macos_temps () =
    try
      (* Try osx-cpu-temp if installed *)
      let cmd = Unix.open_process_in "osx-cpu-temp 2>/dev/null" in
      Fun.protect
        ~finally:(fun () -> ignore (Unix.close_process_in cmd))
        (fun () ->
          let output = String.trim (input_line cmd) in

          (* Parse output like "61.2°C" - extract digits before the degree symbol *)
          let temp_str =
            let parts = Str.split (Str.regexp "[°C]") output in
            match parts with
            | temp :: _ -> String.trim temp
            | [] -> output
          in
          let temp = float_of_string temp_str in
          Some (Some temp, None, [])
        )
    with _ ->
      (* Try powermetrics (requires sudo, so may fail) *)
      try
        let cmd = Unix.open_process_in "powermetrics --samplers smc -i1 -n1 2>/dev/null | grep -i 'CPU die temperature'" in
        Fun.protect
          ~finally:(fun () -> ignore (Unix.close_process_in cmd))
          (fun () ->
            let line = input_line cmd in

            (* Parse line like "CPU die temperature: 45.67 C" *)
            let parts = String.split_on_char ':' line in
            if List.length parts >= 2 then
              let temp_part = List.nth parts 1 |> String.trim in
              let temp_str = String.split_on_char ' ' temp_part |> List.hd |> String.trim in
              let temp = float_of_string temp_str in
              Some (Some temp, None, [])
            else
              None
          )
      with _ ->
        (* Try istats if installed *)
        try
          let cmd = Unix.open_process_in "istats cpu temp 2>/dev/null" in
          Fun.protect
            ~finally:(fun () -> ignore (Unix.close_process_in cmd))
            (fun () ->
              let line = input_line cmd in

              (* Parse output like "CPU temp: 45.5°C" *)
              let parts = String.split_on_char ':' line in
              if List.length parts >= 2 then
                let temp_part = List.nth parts 1 |> String.trim in
                let temp_str =
                  let parts = Str.split (Str.regexp "[°C]") temp_part in
                  match parts with
                  | temp :: _ -> String.trim temp
                  | [] -> temp_part
                in
                let temp = float_of_string temp_str in
                Some (Some temp, None, [])
              else
                None
            )
        with _ -> None

  (** Try to get temperature from Linux thermal zones *)
  let get_linux_temps () =
    try
      let thermal_zones = Sys.readdir "/sys/class/thermal" |> Array.to_list in
      let zones = List.filter (fun name -> String.starts_with ~prefix:"thermal_zone" name) thermal_zones in
      
      let read_zone zone =
        try
          let type_path = Printf.sprintf "/sys/class/thermal/%s/type" zone in
          let temp_path = Printf.sprintf "/sys/class/thermal/%s/temp" zone in
          
          let ic_type = open_in type_path in
          let zone_type = String.trim (input_line ic_type) in
          close_in ic_type;
          
          let ic_temp = open_in temp_path in
          let temp_millic = String.trim (input_line ic_temp) in
          close_in ic_temp;
          
          let temp = float_of_string temp_millic /. 1000.0 in
          Some (zone_type, temp)
        with _ -> None
      in
      
      let temps = List.filter_map read_zone zones in
      
      (* Try to identify CPU and GPU temperatures *)
      let cpu_temp = List.find_opt (fun (name, _) ->
        let lower = String.lowercase_ascii name in
        String.contains lower 'c' && String.contains lower 'p' && String.contains lower 'u' ||
        String.starts_with ~prefix:"x86_pkg_temp" lower ||
        String.starts_with ~prefix:"coretemp" lower ||
        String.starts_with ~prefix:"k10temp" lower
      ) temps |> Option.map snd in
      
      let gpu_temp = List.find_opt (fun (name, _) ->
        let lower = String.lowercase_ascii name in
        String.contains lower 'g' && String.contains lower 'p' && String.contains lower 'u'
      ) temps |> Option.map snd in
      
      Some (cpu_temp, gpu_temp, temps)
    with _ ->
      (* Fallback: try lm-sensors command *)
      try
        let cmd = Unix.open_process_in "sensors 2>/dev/null" in
        Fun.protect
          ~finally:(fun () -> ignore (Unix.close_process_in cmd))
          (fun () ->
            let rec read_sensors cpu_temp gpu_temp all_temps =
              try
                let line = input_line cmd in

                (* Look for temperature lines like "Core 0:        +45.0°C" or "temp1:        +50.0°C" *)
                if String.contains line ':' && String.contains line '+' then
                  let parts = String.split_on_char ':' line in
                  match parts with
                  | name :: value_part :: _ ->
                      let name = String.trim name in
                      let value_str = String.trim value_part in

                      (* Extract temperature value - look for pattern like "+45.0" *)
                      let temp_opt = try
                        let temp_start = String.index value_str '+' in
                        (* Find the end of the number - could be space, degree symbol, or end of string *)
                        let rec find_number_end str pos =
                          if pos >= String.length str then pos
                          else match str.[pos] with
                          | '0'..'9' | '.' -> find_number_end str (pos + 1)
                          | _ -> pos
                        in
                        let temp_end = find_number_end value_str (temp_start + 1) in
                        let temp_str = String.sub value_str (temp_start + 1) (temp_end - temp_start - 1) in
                        Some (float_of_string temp_str)
                      with _ -> None
                      in

                      (match temp_opt with
                       | Some temp ->
                           let lower = String.lowercase_ascii name in
                           let new_cpu = if cpu_temp = None && (String.contains lower 'c' || String.contains lower 'p') then Some temp else cpu_temp in
                           let new_gpu = if gpu_temp = None && String.contains lower 'g' then Some temp else gpu_temp in
                           let new_all = (name, temp) :: all_temps in
                           read_sensors new_cpu new_gpu new_all
                       | None -> read_sensors cpu_temp gpu_temp all_temps)
                  | _ -> read_sensors cpu_temp gpu_temp all_temps
                else
                  read_sensors cpu_temp gpu_temp all_temps
              with End_of_file ->
                (cpu_temp, gpu_temp, List.rev all_temps)
            in
            let cpu, gpu, all = read_sensors None None [] in
            Some (cpu, gpu, all)
          )
      with _ -> None

  (** Try to get temperature from Windows (via WMI) *)
  let get_windows_temps () =
    try
      (* Try using wmic to query temperature sensors *)
      let cmd = Unix.open_process_in "wmic /namespace:\\\\\\\\root\\\\wmi PATH MSAcpi_ThermalZoneTemperature get CurrentTemperature 2>nul" in
      Fun.protect
        ~finally:(fun () -> ignore (Unix.close_process_in cmd))
        (fun () ->
          let rec read_temps temps =
            try
              let line = input_line cmd in
              let trimmed = String.trim line in
              (* WMI returns temperature in tenths of Kelvin, convert to Celsius *)
              if trimmed <> "" && trimmed <> "CurrentTemperature" then
                try
                  let temp_tenths_k = float_of_string trimmed in
                  let temp_c = (temp_tenths_k /. 10.0) -. 273.15 in
                  read_temps (temp_c :: temps)
                with _ -> read_temps temps
              else
                read_temps temps
            with End_of_file ->
              List.rev temps
          in
          let temps = read_temps [] in
          match temps with
          | [] -> None
          | cpu_temp :: _ ->
              (* First temp is usually CPU, but we don't have GPU info from this method *)
              Some (Some cpu_temp, None, List.mapi (fun i t -> (Printf.sprintf "Sensor%d" i, t)) temps)
        )
    with _ -> None

  (** Get system temperatures for current platform *)
  let get_temperatures () =
    match Sys.os_type with
    | "Unix" ->
        (* Try to detect which Unix variant *)
        (try
          let uname_cmd = Unix.open_process_in "uname 2>/dev/null" in
          Fun.protect
            ~finally:(fun () -> ignore (Unix.close_process_in uname_cmd))
            (fun () ->
              let uname = String.trim (input_line uname_cmd) in

              match uname with
              | "Darwin" -> get_macos_temps ()
              | "Linux" -> get_linux_temps ()
              | _ -> get_linux_temps ()  (* Default to Linux method *)
            )
        with _ -> get_linux_temps ())  (* Default to Linux method *)
    | "Win32" -> get_windows_temps ()
    | _ -> None
end

(** Event bus for system stats *)
module SystemStatsEventBus = Event_bus.Make(struct
  type t = Ui_types.system_stats
end)

(** System cache state *)
type t = {
  mutable current_system_stats: system_stats option;
  system_stats_event_bus: SystemStatsEventBus.t;
  mutable last_update: float;
  update_interval: float;  (* Minimum time between system updates *)
}

(** Global shutdown flag for background tasks *)
let shutdown_requested = Atomic.make false

(** Signal shutdown to background tasks *)
let signal_shutdown () =
  Atomic.set shutdown_requested true

(** Global system cache instance *)
let cache = {
  current_system_stats = None;
  system_stats_event_bus = SystemStatsEventBus.create "system_stats";
  last_update = 0.0;
  update_interval = 1.0;  (* Update system stats at most every 1.0 seconds for dashboard mode *)
}

(** Memory usage tracking for leak detection *)
let memory_usage_history = ref []
let max_memory_history = 100  (* Keep last 100 memory readings *)
let memory_leak_threshold_mb = 100.0  (* Alert if memory grows by 100MB in last readings *)
let last_gc_time = ref 0.0
let last_memory_log_time = ref 0.0  (* Track when we last logged memory stats *)

(** Per-domain heap tracking for multicore environments *)
let domain_heap_history : (int, (float * float) list) Hashtbl.t = Hashtbl.create 16  (* Domain ID -> [(time, heap_mb)] list *)
let domain_allocation_rates : (int, (float * float) list) Hashtbl.t = Hashtbl.create 16  (* Domain ID -> [(time, allocs_per_sec)] list *)
let max_domain_history = 50  (* Keep last 50 readings per domain *)

(** Per-feed memory tracking for leak detection *)
type feed_memory_stats = {
  timestamp: float;
  event_bus_subscribers_total: int;
  event_bus_subscribers_active: int;
  event_bus_subscribers_stale: int;
  cache_entries: int;
  estimated_memory_mb: float;
  last_growth_rate: float;  (* MB per second *)
}

let feed_memory_history : (string, feed_memory_stats list) Hashtbl.t = Hashtbl.create 16
let max_feed_history = 20  (* Keep last 20 readings per feed *)

(** Memory breakdown record type *)
type memory_breakdown_info = {
  total_heap_mb: float;
  feed_breakdown: (string * float) list;
  other_memory_mb: float;
  timestamp: float;
}

(** Get memory statistics for telemetry feed *)
let get_telemetry_feed_stats () =
  let now = Unix.time () in
  let (total, active, stale) = 
    try Telemetry_cache.get_telemetry_subscriber_stats ()
    with _ -> (0, 0, 0)
  in
  (* Safely get cache size - snapshot may be None if cache was cleared *)
  let cache_size = 
    try
      let snapshot = Telemetry_cache.current_telemetry_snapshot () in
      (* Snapshot is immutable, safe to access fields *)
      List.length snapshot.metrics
    with 
    | Invalid_argument _ -> 0  (* Handle invalid access *)
    | _ -> 0  (* Handle any other exception *)
  in
  let estimated_mb = float_of_int (total + cache_size) *. 0.001 in  (* Rough estimate *)
  {
    timestamp = now;
    event_bus_subscribers_total = total;
    event_bus_subscribers_active = active;
    event_bus_subscribers_stale = stale;
    cache_entries = cache_size;
    estimated_memory_mb = estimated_mb;
    last_growth_rate = 0.0;  (* Will be calculated from history *)
  }

(** Get memory statistics for system stats feed *)
let get_system_feed_stats () =
  let now = Unix.time () in
  let (total, active, stale) = 
    try SystemStatsEventBus.get_subscriber_stats cache.system_stats_event_bus
    with _ -> (0, 0, 0)
  in
  let cache_size = 
    try
      match cache.current_system_stats with
      | Some _ -> 1
      | None -> 0
    with _ -> 0
  in
  let estimated_mb = float_of_int (total + cache_size) *. 0.001 in
  {
    timestamp = now;
    event_bus_subscribers_total = total;
    event_bus_subscribers_active = active;
    event_bus_subscribers_stale = stale;
    cache_entries = cache_size;
    estimated_memory_mb = estimated_mb;
    last_growth_rate = 0.0;
  }

(** Get memory statistics for balance feed *)
let get_balance_feed_stats () =
  let now = Unix.time () in
  let (total, active, stale) = 
    try Balance_cache.get_balance_subscriber_stats ()
    with _ -> (0, 0, 0)
  in
  (* Safely get cache size - snapshot may be None if cache was cleared *)
  let cache_size = 
    try
      let snapshot = Balance_cache.current_balance_snapshot () in
      (* Snapshot is immutable, safe to access fields *)
      List.length snapshot.balances + List.length snapshot.open_orders
    with 
    | Invalid_argument _ -> 0  (* Handle invalid access *)
    | _ -> 0  (* Handle any other exception *)
  in
  let estimated_mb = float_of_int (total + cache_size) *. 0.001 in
  {
    timestamp = now;
    event_bus_subscribers_total = total;
    event_bus_subscribers_active = active;
    event_bus_subscribers_stale = stale;
    cache_entries = cache_size;
    estimated_memory_mb = estimated_mb;
    last_growth_rate = 0.0;
  }

(** Get memory statistics for logs feed *)
let get_logs_feed_stats () =
  let now = Unix.time () in
  let (total, active, stale) = 
    try Logs_cache.get_logs_subscriber_stats ()
    with _ -> (0, 0, 0)
  in
  let cache_size = 
    try Logs_cache.current_log_entries () |> List.length
    with _ -> 0
  in
  let estimated_mb = float_of_int (total + cache_size) *. 0.01 in  (* Logs take more memory *)
  {
    timestamp = now;
    event_bus_subscribers_total = total;
    event_bus_subscribers_active = active;
    event_bus_subscribers_stale = stale;
    cache_entries = cache_size;
    estimated_memory_mb = estimated_mb;
    last_growth_rate = 0.0;
  }

(** Mutex to protect feed memory tracking updates *)
let feed_tracking_mutex = Mutex.create ()

(** Update per-feed memory tracking *)
let update_feed_memory_tracking () =
  (* Protect against concurrent access *)
  if Mutex.try_lock feed_tracking_mutex then (
    try
      (* Safely get current stats for all feeds with error handling *)
      let feeds = 
        try [
          ("telemetry", get_telemetry_feed_stats ());
          ("system", get_system_feed_stats ());
          ("balance", get_balance_feed_stats ());
          ("logs", get_logs_feed_stats ());
        ] with exn ->
          Logging.debug_f ~section:"memory_monitoring" "Error collecting feed stats: %s" (Printexc.to_string exn);
          []
      in

      try
        List.iter (fun (feed_name, (feed_stats : feed_memory_stats)) ->
          (* Get existing history *)
          let existing_history = try Hashtbl.find feed_memory_history feed_name with Not_found -> [] in

          (* Calculate growth rate if we have previous data *)
          let stats_with_growth : feed_memory_stats = match existing_history with
            | (prev_stats : feed_memory_stats) :: _ ->
                let time_diff = feed_stats.timestamp -. prev_stats.timestamp in
                if time_diff > 0.0 then
                  let growth_rate = (feed_stats.estimated_memory_mb -. prev_stats.estimated_memory_mb) /. time_diff in
                  { feed_stats with last_growth_rate = growth_rate }
                else
                  feed_stats
            | [] -> feed_stats
          in

          (* Add to history and trim *)
          let new_history = stats_with_growth :: existing_history in
          let trimmed_history = if List.length new_history > max_feed_history
                               then take max_feed_history new_history
                               else new_history in
          Hashtbl.replace feed_memory_history feed_name trimmed_history
        ) feeds
      with exn ->
        Logging.debug_f ~section:"memory_monitoring" "Error updating feed memory tracking: %s" (Printexc.to_string exn);
      
      Mutex.unlock feed_tracking_mutex
    with exn ->
      Mutex.unlock feed_tracking_mutex;
      Logging.debug_f ~section:"memory_monitoring" "Error in feed tracking (unlocked mutex): %s" (Printexc.to_string exn)
  ) else (
    (* Mutex is locked, skip this update to avoid blocking *)
    ()
  )

(** Get current OCaml memory usage in MB *)
let get_ocaml_memory_usage () =
  let stat = Gc.stat () in
  let heap_words = stat.heap_words in
  let word_size = Sys.word_size / 8 in  (* bytes per word *)
  float_of_int (heap_words * word_size) /. (1024.0 *. 1024.0)

(** Get detailed GC statistics *)
let get_gc_stats () =
  Gc.stat ()

(** Get current domain ID (for multicore tracking) *)
let get_current_domain_id () =
  try Obj.magic (Domain.self ()) 
  with _ -> 0  (* Fallback for single-core or if Domain.self fails *)

(** Track memory usage per domain *)
let track_domain_memory_usage () =
  let domain_id = get_current_domain_id () in
  let now = Unix.time () in
  let heap_mb = get_ocaml_memory_usage () in

  (* Update domain heap history *)
  let current_history = try Hashtbl.find domain_heap_history domain_id with Not_found -> [] in
  let new_history = (now, heap_mb) :: current_history in
  let trimmed_history = if List.length new_history > max_domain_history
                        then take max_domain_history new_history
                        else new_history in
  Hashtbl.replace domain_heap_history domain_id trimmed_history

(** Check for memory leaks and trigger cleanup if needed *)
let check_memory_leak () =
  let current_mem = get_ocaml_memory_usage () in
  let now = Unix.time () in

  (* Add current reading to history *)
  memory_usage_history := (now, current_mem) :: !memory_usage_history;

  (* Trim history to max size *)
  if List.length !memory_usage_history > max_memory_history then
    memory_usage_history := take max_memory_history !memory_usage_history;

  (* Update domain-specific tracking *)
  track_domain_memory_usage ();

  (* Check for memory growth trend *)
  match !memory_usage_history with
  | (oldest_time, oldest_mem) :: _ when List.length !memory_usage_history >= 10 ->
      let time_diff = now -. oldest_time in
      let mem_diff = current_mem -. oldest_mem in

      (* If memory has grown significantly over the last readings, trigger GC *)
      if mem_diff > memory_leak_threshold_mb && time_diff > 300.0 then (  (* 5 minutes *)
        Logging.warn_f ~section:"memory_monitoring" "Memory leak detected: %.1f MB growth in %.0f seconds, triggering GC"
          mem_diff time_diff;
        Gc.full_major ();
        last_gc_time := now;
        Logging.info_f ~section:"memory_monitoring" "GC completed, memory now: %.1f MB" (get_ocaml_memory_usage ())
      )
  | _ -> ()

(** Log detailed memory statistics periodically *)
let log_detailed_memory_stats () =
  let now = Unix.time () in
  let gc_stats = get_gc_stats () in
  let heap_mb = get_ocaml_memory_usage () in

  Logging.info_f ~section:"memory_monitoring" "GC Stats: heap=%.1fMB live=%.1fMB free=%.1fMB fragments=%d compactions=%d"
    heap_mb
    (float_of_int gc_stats.live_words *. float_of_int (Sys.word_size / 8) /. (1024.0 *. 1024.0))
    (float_of_int gc_stats.free_words *. float_of_int (Sys.word_size / 8) /. (1024.0 *. 1024.0))
    gc_stats.fragments
    gc_stats.compactions;

  (* Log per-feed statistics *)
  let feeds = ["telemetry"; "system"; "balance"; "logs"] in
  List.iter (fun feed_name ->
    match try Some (Hashtbl.find feed_memory_history feed_name) with Not_found -> None with
    | Some (latest :: _) ->
        Logging.info_f ~section:"memory_monitoring" "Feed %s: subs=%d/%d/%d cache=%d mem=%.3fMB growth=%.6fMB/s"
          feed_name
          latest.event_bus_subscribers_total
          latest.event_bus_subscribers_active
          latest.event_bus_subscribers_stale
          latest.cache_entries
          latest.estimated_memory_mb
          latest.last_growth_rate
    | _ ->
        Logging.debug_f ~section:"memory_monitoring" "Feed %s: no data available" feed_name
  ) feeds;

  (* Log per-domain statistics if available *)
  Hashtbl.iter (fun domain_id history ->
    match history with
    | (latest_time, latest_heap) :: _ when now -. latest_time < 60.0 ->
        Logging.debug_f ~section:"memory_monitoring" "Domain %d: heap=%.1fMB (last update %.1fs ago)"
          domain_id latest_heap (now -. latest_time)
    | _ -> ()
  ) domain_heap_history

(** Check for per-feed memory leaks and issue alerts *)
let check_per_feed_leaks () =
  let feeds = ["telemetry"; "system"; "balance"; "logs"] in

  List.iter (fun feed_name ->
    match try Some (Hashtbl.find feed_memory_history feed_name) with Not_found -> None with
    | Some history when List.length history >= 3 ->
        (* Check last 3 readings for trends *)
        let recent = take 3 history in
        begin match recent with
        | latest :: _ :: oldest :: _ ->
            let time_span = latest.timestamp -. oldest.timestamp in
            let (subscriber_growth, cache_growth, memory_growth) =
              if time_span > 60.0 then (  (* Only check if we have at least 1 minute of data *)
                let sub_growth = float_of_int (latest.event_bus_subscribers_total - oldest.event_bus_subscribers_total) /. time_span in
                let cache_growth_rate = float_of_int (latest.cache_entries - oldest.cache_entries) /. time_span in
                let mem_growth_rate = (latest.estimated_memory_mb -. oldest.estimated_memory_mb) /. time_span in
                (sub_growth, cache_growth_rate, mem_growth_rate)
              ) else (0.0, 0.0, 0.0)
            in

            (* Alert thresholds *)
            let sub_threshold = 1.0 /. 60.0 in  (* More than 1 new subscriber per minute *)
            let cache_threshold = 100.0 /. 60.0 in  (* More than 100 new cache entries per minute *)
            let mem_threshold = 1.0 /. 60.0 in  (* More than 1MB growth per minute *)

            (* Issue alerts for concerning growth patterns *)
            if subscriber_growth > sub_threshold then
              Logging.warn_f ~section:"memory_monitoring" "Feed %s: High subscriber growth %.2f subs/min (total: %d)"
                feed_name subscriber_growth latest.event_bus_subscribers_total;

            if cache_growth > cache_threshold then
              Logging.warn_f ~section:"memory_monitoring" "Feed %s: High cache growth %.1f entries/min (total: %d)"
                feed_name cache_growth latest.cache_entries;

            if memory_growth > mem_threshold then
              Logging.warn_f ~section:"memory_monitoring" "Feed %s: High memory growth %.3f MB/min (total: %.3f MB)"
                feed_name memory_growth latest.estimated_memory_mb;

            (* Check for stale subscriber accumulation *)
            if latest.event_bus_subscribers_stale > latest.event_bus_subscribers_total / 2 then
              Logging.warn_f ~section:"memory_monitoring" "Feed %s: High stale subscriber ratio %d/%d (%d%%)"
                feed_name latest.event_bus_subscribers_stale latest.event_bus_subscribers_total
                (latest.event_bus_subscribers_stale * 100 / max 1 latest.event_bus_subscribers_total);
        | _ -> ()
        end

    | _ -> ()  (* Not enough data yet *)
  ) feeds

(** Get detailed memory breakdown by component *)
let get_memory_breakdown () : memory_breakdown_info =
  let total_heap = get_ocaml_memory_usage () in
  let feed_breakdown =
    Hashtbl.fold (fun feed_name history acc ->
      match history with
      | latest :: _ ->
          (feed_name, latest.estimated_memory_mb) :: acc
      | [] -> acc
    ) feed_memory_history []
  in

  let total_feed_memory = List.fold_left (fun acc (_, mem) -> acc +. mem) 0.0 feed_breakdown in
  let other_memory = max 0.0 (total_heap -. total_feed_memory) in

  {
    total_heap_mb = total_heap;
    feed_breakdown = feed_breakdown;
    other_memory_mb = other_memory;
    timestamp = Unix.time ();
  }

(** Get memory consumption ranking (feeds with highest memory usage) *)
let get_memory_consumption_ranking () =
  let feed_breakdown =
    Hashtbl.fold (fun feed_name history acc ->
      match history with
      | latest :: _ -> (feed_name, latest) :: acc
      | [] -> acc
    ) feed_memory_history []
  in

  (* Sort by estimated memory usage, descending *)
  let sorted = List.sort (fun (_, a) (_, b) ->
    compare b.estimated_memory_mb a.estimated_memory_mb
  ) feed_breakdown in

  List.map (fun (name, stats) ->
    (name, stats.estimated_memory_mb, stats.last_growth_rate)
  ) sorted

(** Force cleanup of stale subscribers across all feeds *)
let force_cleanup_all_stale_subscribers () =
  let telemetry_cleaned = Telemetry_cache.force_cleanup_stale_subscribers () in
  let system_cleaned = SystemStatsEventBus.force_cleanup_stale_subscribers cache.system_stats_event_bus () in
  let balance_cleaned = Balance_cache.force_cleanup_stale_subscribers () in
  let logs_cleaned = Logs_cache.force_cleanup_stale_subscribers () in

  let total_cleaned = Option.value telemetry_cleaned ~default:0 +
                     Option.value system_cleaned ~default:0 +
                     Option.value balance_cleaned ~default:0 +
                     Option.value logs_cleaned ~default:0 in

  if total_cleaned > 0 then
    Logging.info_f ~section:"memory_monitoring" "Force cleaned %d stale subscribers across all feeds" total_cleaned;

  total_cleaned

(** Generate comprehensive memory diagnostic report *)
let generate_memory_diagnostic_report () =
  let mem_breakdown = get_memory_breakdown () in
  let ranking = get_memory_consumption_ranking () in
  let gc_stats = get_gc_stats () in

  Logging.info ~section:"memory_monitoring" "=== MEMORY DIAGNOSTIC REPORT ===";
  Logging.info_f ~section:"memory_monitoring" "Total heap: %.2f MB" mem_breakdown.total_heap_mb;
  Logging.info_f ~section:"memory_monitoring" "GC stats: live=%.2fMB free=%.2fMB fragments=%d"
    (float_of_int gc_stats.live_words *. float_of_int (Sys.word_size / 8) /. (1024.0 *. 1024.0))
    (float_of_int gc_stats.free_words *. float_of_int (Sys.word_size / 8) /. (1024.0 *. 1024.0))
    gc_stats.fragments;

  Logging.info ~section:"memory_monitoring" "Feed memory ranking (by estimated usage):";
  List.iteri (fun i (name, mem_mb, growth_rate) ->
    Logging.info_f ~section:"memory_monitoring" "  %d. %s: %.3f MB (growth: %.6f MB/s)"
      (i + 1) name mem_mb growth_rate
  ) ranking;

  Logging.info_f ~section:"memory_monitoring" "Other memory: %.2f MB" mem_breakdown.other_memory_mb;

  (* Show detailed feed stats *)
  Logging.info ~section:"memory_monitoring" "Detailed feed statistics:";
  Hashtbl.iter (fun feed_name history ->
    match history with
    | latest :: _ ->
        Logging.info_f ~section:"memory_monitoring" "  %s: subs=%d/%d/%d cache=%d mem=%.3fMB"
          feed_name
          latest.event_bus_subscribers_total
          latest.event_bus_subscribers_active
          latest.event_bus_subscribers_stale
          latest.cache_entries
          latest.estimated_memory_mb
    | _ -> ()
  ) feed_memory_history;

  Logging.info ~section:"memory_monitoring" "=== END MEMORY DIAGNOSTIC REPORT ==="

(** Periodic memory maintenance *)
let perform_memory_maintenance () =
  let now = Unix.time () in
  let time_since_gc = now -. !last_gc_time in

  (* Force GC every 15 minutes for dashboard mode to prevent gradual memory creep *)
  if time_since_gc > 900.0 then (
    Logging.debug ~section:"memory_monitoring" "Performing scheduled GC (15-minute interval for dashboard mode)";
    Gc.full_major ();
    last_gc_time := now
  );

  (* Update per-feed memory tracking *)
  update_feed_memory_tracking ();

  (* Check for per-component memory leaks *)
  check_per_feed_leaks ();

  (* Check for memory leaks *)
  check_memory_leak ();

  (* Log detailed memory stats every 10 seconds *)
  let time_since_last_log = now -. !last_memory_log_time in
  if time_since_last_log >= 10.0 then (
    log_detailed_memory_stats ();
    last_memory_log_time := now
  )

(** Initialization guard with mutex protection *)
let initialized = ref false
let init_mutex = Mutex.create ()

(** Get system memory information *)
let get_memory_stats () =
  try
    (* Try Linux /proc/meminfo first *)
    let ic = open_in "/proc/meminfo" in
    let rec read_lines mem_total mem_free swap_total swap_free =
      try
        let line = input_line ic in
        let parts = String.split_on_char ':' line in
        match parts with
        | ["MemTotal"; value] ->
            let total = int_of_string (String.trim (List.hd (String.split_on_char ' ' (String.trim value)))) in
            read_lines total mem_free swap_total swap_free
        | ["MemAvailable"; value] ->
            let avail = int_of_string (String.trim (List.hd (String.split_on_char ' ' (String.trim value)))) in
            read_lines mem_total avail swap_total swap_free
        | ["SwapTotal"; value] ->
            let total = int_of_string (String.trim (List.hd (String.split_on_char ' ' (String.trim value)))) in
            read_lines mem_total mem_free total swap_free
        | ["SwapFree"; value] ->
            let free = int_of_string (String.trim (List.hd (String.split_on_char ' ' (String.trim value)))) in
            read_lines mem_total mem_free swap_total free
        | _ -> read_lines mem_total mem_free swap_total swap_free
      with End_of_file ->
        close_in ic;
        (mem_total, mem_total - mem_free, mem_free, swap_total, swap_total - swap_free)
    in
    let mem_total, mem_used, mem_free, swap_total, swap_used = read_lines 0 0 0 0 in
    Some (mem_total, mem_used, mem_free, swap_total, swap_used)
  with _ ->
    (* macOS implementation *)
    try
      let mem_total_cmd = Unix.open_process_in "sysctl -n hw.memsize" in
      let mem_total = Fun.protect
        ~finally:(fun () -> ignore (Unix.close_process_in mem_total_cmd))
        (fun () ->
          let mem_total_bytes = int_of_string (String.trim (input_line mem_total_cmd)) in
          mem_total_bytes / 1024  (* Convert to KB *)
        )
      in

      let vm_stats_cmd = Unix.open_process_in "vm_stat" in
      let mem_total, mem_used, mem_free, swap_total, swap_used = Fun.protect
        ~finally:(fun () -> ignore (Unix.close_process_in vm_stats_cmd))
        (fun () ->
          let rec read_vm_stats pages_free pages_active pages_inactive pages_wired =
            try
              let line = input_line vm_stats_cmd in
              if String.contains line ':' then
                let parts = String.split_on_char ':' line in
                match List.map String.trim parts with
                | ["Pages free"; value] ->
                    let free = int_of_string (String.trim (String.sub value 0 (String.length value - 1))) in
                    read_vm_stats free pages_active pages_inactive pages_wired
                | ["Pages active"; value] ->
                    let active = int_of_string (String.trim (String.sub value 0 (String.length value - 1))) in
                    read_vm_stats pages_free active pages_inactive pages_wired
                | ["Pages inactive"; value] ->
                    let inactive = int_of_string (String.trim (String.sub value 0 (String.length value - 1))) in
                    read_vm_stats pages_free pages_active inactive pages_wired
                | ["Pages wired down"; value] ->
                    let wired = int_of_string (String.trim (String.sub value 0 (String.length value - 1))) in
                    read_vm_stats pages_free pages_active pages_inactive wired
                | _ -> read_vm_stats pages_free pages_active pages_inactive pages_wired
              else
                read_vm_stats pages_free pages_active pages_inactive pages_wired
            with End_of_file ->
              let page_size_kb = 16384 / 1024 in (* Apple Silicon uses 16KB pages *)
              let mem_used_pages = pages_active + pages_wired in
              let mem_free_pages = pages_free + pages_inactive in
              let mem_used = mem_used_pages * page_size_kb in
              let mem_free = mem_free_pages * page_size_kb in
              (mem_total, mem_used, mem_free, 0, 0)  (* macOS swap is handled differently *)
          in
          read_vm_stats 0 0 0 0
        )
      in
      Some (mem_total, mem_used, mem_free, swap_total, swap_used)
    with _ ->
      (* Unable to collect memory stats *)
      None

(** Read CPU stats from /proc/stat *)
let read_cpu_stats () =
  let ic = open_in "/proc/stat" in
  let rec read_cores cores =
    try
      let line = input_line ic in
      let parts = String.split_on_char ' ' line |> List.filter (fun s -> s <> "") in
      match parts with
      | "cpu" :: user :: nice :: system :: idle :: iowait :: irq :: softirq :: _ ->
          (* Total CPU line *)
          let user = float_of_string user in
          let nice = float_of_string nice in
          let system = float_of_string system in
          let idle = float_of_string idle in
          let iowait = float_of_string iowait in
          let irq = float_of_string irq in
          let softirq = float_of_string softirq in
          let total = user +. nice +. system +. idle +. iowait +. irq +. softirq in
          let idle_total = idle +. iowait in
          let busy_total = total -. idle_total in
          read_cores ((busy_total, total) :: cores)
      | core_name :: user :: nice :: system :: idle :: iowait :: irq :: softirq :: _ when String.starts_with ~prefix:"cpu" core_name && core_name <> "cpu" ->
          (* Individual core line *)
          let user = float_of_string user in
          let nice = float_of_string nice in
          let system = float_of_string system in
          let idle = float_of_string idle in
          let iowait = float_of_string iowait in
          let irq = float_of_string irq in
          let softirq = float_of_string softirq in
          let total = user +. nice +. system +. idle +. iowait +. irq +. softirq in
          let idle_total = idle +. iowait in
          let busy_total = total -. idle_total in
          read_cores ((busy_total, total) :: cores)
      | _ -> read_cores cores
    with End_of_file ->
      close_in ic;
      List.rev cores (* Return in correct order cpu, cpu0, cpu1... *)
  in
  read_cores []

(** Cache for previous CPU stats *)
let last_cpu_stats = ref None

(** Get CPU usage and per-core usage *)
let get_cpu_usage () =
  try
    let current_stats = read_cpu_stats () in
    match !last_cpu_stats, current_stats with
    | Some ((busy1, total1) :: core_stats1), (busy2, total2) :: core_stats2 ->
        last_cpu_stats := Some current_stats; (* Update cache *)
        
        (* Calculate total CPU usage over the interval *)
        let busy_delta = busy2 -. busy1 in
        let total_delta = total2 -. total1 in
        let total_percentage = if total_delta > 0.0 then (busy_delta /. total_delta) *. 100.0 else 0.0 in

        (* Calculate per-core usage over the interval *)
        let core_percentages =
          try
            List.map2 (fun (busy1, total1) (busy2, total2) ->
              let busy_delta = busy2 -. busy1 in
              let total_delta = total2 -. total1 in
              if total_delta > 0.0 then (busy_delta /. total_delta) *. 100.0 else 0.0
            ) core_stats1 core_stats2
          with Invalid_argument _ -> [] (* Handle case where core counts mismatch *)
        in

        Lwt.return_some (total_percentage, core_percentages)
    | _ ->
        (* First run, or one of the stats lists was empty. Just populate cache and wait for next call. *)
        last_cpu_stats := Some current_stats;
        Lwt.return_none
  with _ ->
    (* macOS implementation using top command *)
    (* Helper function to check if substring exists - case insensitive *)
    let contains_substring haystack needle =
      let needle_len = String.length needle in
      if needle_len = 0 then true else
      try
        let start_pos = String.index_from haystack 0 needle.[0] in
        let rec check_from pos =
          if pos + needle_len > String.length haystack then false
          else
            let rec match_chars i =
              if i >= needle_len then true
              else if haystack.[pos + i] = needle.[i] then match_chars (i + 1)
              else false
            in
            if match_chars 0 then true
            else check_from (pos + 1)
        in
        check_from start_pos
      with Not_found -> false
    in 
    try
      let top_cmd = Unix.open_process_in "top -l 1 -n 0 | grep -i 'cpu usage'" in
      let line = Fun.protect
        ~finally:(fun () -> ignore (Unix.close_process_in top_cmd))
        (fun () ->
          input_line top_cmd
        )
      in

      (* Parse line like: "CPU usage: 15.23% user, 12.45% sys, 72.32% idle" *)
      let clean_line = String.trim line in
      Logging.debug_f ~section:"system_cache" "Parsing top output: '%s'" clean_line;
      
      let parts = String.split_on_char ',' clean_line in
      let rec parse_usage parts user_pct sys_pct =
        match parts with
        | [] -> (user_pct, sys_pct)
        | part :: rest ->
            let trimmed = String.trim part in
            try
              if String.contains trimmed '%' then
                (* Extract the percentage number *)
                let percent_str = 
                  let idx = String.index trimmed '%' in
                  String.sub trimmed 0 idx |> String.trim
                in
                let percent = float_of_string percent_str in
                
                (* Check for keywords - case insensitive *)
                let lower_part = String.lowercase_ascii trimmed in
                if contains_substring lower_part "user" then
                  parse_usage rest percent sys_pct
                else if contains_substring lower_part "sys" then
                  parse_usage rest user_pct percent
                else if contains_substring lower_part "idle" then
                  parse_usage rest user_pct sys_pct  (* Ignore idle *)
                else
                  (* Unknown percentage, log and ignore *)
                  let () = Logging.debug_f ~section:"system_cache" "Unknown percentage part: '%s' (%.2f%%)" trimmed percent in
                  parse_usage rest user_pct sys_pct
              else
                parse_usage rest user_pct sys_pct
            with e ->
              Logging.debug_f ~section:"system_cache" "Failed to parse part '%s': %s" trimmed (Printexc.to_string e);
              parse_usage rest user_pct sys_pct
      in
      
      let user_pct, sys_pct = parse_usage parts 0.0 0.0 in
      let total_usage = min 100.0 (user_pct +. sys_pct) in
      
      if total_usage > 0.0 then begin
        Logging.debug_f ~section:"system_cache" "CPU usage parsed: user=%.2f%%, sys=%.2f%%, total=%.2f%%" 
          user_pct sys_pct total_usage;
        Lwt.return_some (total_usage, [total_usage])
      end else begin
        Logging.warn_f ~section:"system_cache" "Failed to parse valid CPU usage from: %s" clean_line;
        Lwt.return_none
      end
    with
    | End_of_file -> 
        Logging.debug_f ~section:"system_cache" "No output from top command";
        Lwt.return_none
    | Unix.Unix_error (err, _, _) -> 
        Logging.warn_f ~section:"system_cache" "Unix error running top: %s" (Unix.error_message err);
        Lwt.return_none
    | Failure msg -> 
        Logging.warn_f ~section:"system_cache" "Parse failure: %s" msg;
        Lwt.return_none
    | e -> 
        Logging.error_f ~section:"system_cache" "Unexpected error in CPU parsing: %s" (Printexc.to_string e);
        Lwt.return_none

(** Get system load averages *)
let get_load_avg () =
  try
    (* Try Linux /proc/loadavg first *)
    let ic = open_in "/proc/loadavg" in
    let line = input_line ic in
    close_in ic;
    let parts = String.split_on_char ' ' line in
    match parts with
    | load1 :: load5 :: load15 :: _ ->
        Some (float_of_string load1, float_of_string load5, float_of_string load15)
    | _ -> None
  with _ ->
    (* macOS implementation *)
    try
      let loadavg_cmd = Unix.open_process_in "sysctl -n vm.loadavg" in
      let loadavg_str = Fun.protect
        ~finally:(fun () -> ignore (Unix.close_process_in loadavg_cmd))
        (fun () ->
          String.trim (input_line loadavg_cmd)
        )
      in
      (* Parse output like "{ 3.59 5.28 6.46 }" *)
      let clean_str = String.sub loadavg_str 1 (String.length loadavg_str - 2) in  (* Remove { } *)
      let parts = String.split_on_char ' ' clean_str |> List.filter (fun s -> s <> "") in
      match parts with
      | [load1; load5; load15] ->
          Some (float_of_string load1, float_of_string load5, float_of_string load15)
      | _ -> None
    with _ ->
      (* Unable to collect load averages *)
      None

(** Get number of processes *)
let get_process_count () =
  try
    let ic = Unix.open_process_in "ps aux 2>/dev/null | wc -l 2>/dev/null" in
    let count_str = Fun.protect
      ~finally:(fun () -> ignore (Unix.close_process_in ic))
      (fun () ->
        String.trim (input_line ic)
      )
    in
    let count = int_of_string count_str in
    Some (max 1 (count - 1))  (* Subtract header line, ensure at least 1 *)
  with _ ->
    (* Unable to collect process count *)
    None

(** Create a fresh system stats snapshot (Lwt-friendly) *)
let create_system_stats_lwt () : system_stats option Lwt.t =
  let%lwt cpu_stats = get_cpu_usage () in
  let mem_stats = get_memory_stats () in
  let load_stats = get_load_avg () in
  let proc_count = get_process_count () in
  let cpu_cores = Cpu_info.get_cpu_cores () in
  let cpu_vendor = Cpu_info.get_cpu_vendor () in
  let cpu_model = Cpu_info.get_cpu_model () in
  let temp_data = Temperature.get_temperatures () in

  match cpu_stats, mem_stats, load_stats, proc_count with
  | Some (total_cpu_usage, core_usages), Some (mem_total, mem_used, mem_free, swap_total, swap_used),
    Some (load1, load5, load15), Some processes ->
      let (cpu_temp, gpu_temp, temps) = match temp_data with
        | Some (cpu, gpu, all) -> (cpu, gpu, all)
        | None -> (None, None, [])
      in
      Lwt.return_some {
        cpu_usage = total_cpu_usage;
        core_usages = core_usages;
        cpu_cores = cpu_cores;
        cpu_vendor = cpu_vendor;
        cpu_model = cpu_model;
        memory_total = mem_total;
        memory_used = mem_used;
        memory_free = mem_free;
        swap_total = swap_total;
        swap_used = swap_used;
        load_avg_1 = load1;
        load_avg_5 = load5;
        load_avg_15 = load15;
        processes = processes;
        cpu_temp = cpu_temp;
        gpu_temp = gpu_temp;
        temps = temps;
      }
  | _ -> Lwt.return_none

(** Update system stats cache with throttling (Lwt-friendly) *)
let update_system_stats_cache_lwt () =
  let now = Unix.time () in
  let time_since_last = now -. cache.last_update in

  (* Always try to update if enough time has passed, OR if it's been too long since last publish (keepalive) *)
  let should_update = time_since_last >= cache.update_interval in
  let needs_keepalive = time_since_last >= 30.0 in  (* Keepalive every 30 seconds max *)

  if should_update || needs_keepalive then (
    let%lwt stats_opt = create_system_stats_lwt () in
    (match stats_opt with
     | Some stats ->
         cache.current_system_stats <- Some stats;
         cache.last_update <- now;
         SystemStatsEventBus.publish cache.system_stats_event_bus stats
     | None when needs_keepalive ->
         (* No new data but we need keepalive - republish last known stats if available *)
         (match cache.current_system_stats with
          | Some last_stats ->
              (* Update timestamp and republish (system stats don't have explicit timestamps, but we can republish) *)
              SystemStatsEventBus.publish cache.system_stats_event_bus last_stats;
              cache.last_update <- now;
              Logging.debug_f ~section:"system_cache" "System stats keepalive published at %.2f" now
          | None ->
              (* No stats available, just update timestamp *)
              cache.last_update <- now;
              Logging.debug_f ~section:"system_cache" "No system stats available for keepalive at %.2f" now
         )
     | None ->
         (* Keep previous stats if collection fails - don't create dummy data *)
         (* The UI will show the last known good values or handle the None case *)
         ());
    Lwt.return_unit
  ) else
    Lwt.return_unit

(** Subscribe to system stats updates *)
let subscribe_system_stats () = SystemStatsEventBus.subscribe ~persistent:true cache.system_stats_event_bus

(** Get current system stats (can block on first call if not ready) *)
let current_system_stats () =
  match cache.current_system_stats with
  | Some stats -> stats
  | None ->
      (* This is a fallback for direct calls, might block briefly *)
      Lwt_main.run (
        let%lwt stats_opt = create_system_stats_lwt () in
        match stats_opt with
        | Some stats ->
            cache.current_system_stats <- Some stats;
            Lwt.return stats
        | None ->
            Lwt.return {
              cpu_usage = 0.0;
              core_usages = [];
              cpu_cores = None;
              cpu_vendor = None;
              cpu_model = None;
              memory_total = 0;
              memory_used = 0;
              memory_free = 0;
              swap_total = 0;
              swap_used = 0;
              load_avg_1 = 0.0;
              load_avg_5 = 0.0;
              load_avg_15 = 0.0;
              processes = 0;
              cpu_temp = None;
              gpu_temp = None;
              temps = [];
            }
      )

(** Publish initial system stats snapshot synchronously *)
let publish_initial_snapshot () =
  Logging.debug ~section:"system_cache" "Publishing initial system stats snapshot";
  (* Publish a placeholder snapshot immediately so subscriptions have data *)
  (* The background updater will replace it with real data very soon (within 1 second) *)
  let placeholder_stats = {
    cpu_usage = 0.0;
    core_usages = [];
    cpu_cores = None;
    cpu_vendor = None;
    cpu_model = None;
    memory_total = 0;
    memory_used = 0;
    memory_free = 0;
    swap_total = 0;
    swap_used = 0;
    load_avg_1 = 0.0;
    load_avg_5 = 0.0;
    load_avg_15 = 0.0;
    processes = 0;
    cpu_temp = None;
    gpu_temp = None;
    temps = [];
  } in
  cache.current_system_stats <- Some placeholder_stats;
  cache.last_update <- 0.0;  (* Set to 0 so updater runs immediately *)
  SystemStatsEventBus.publish cache.system_stats_event_bus placeholder_stats;
  Logging.debug ~section:"system_cache" "Initial placeholder system stats snapshot published"

(** Start background system stats updater *)
let start_system_updater () =
  Logging.debug ~section:"system_cache" "Starting background system stats updater";

  (* Start periodic system metrics updater *)
  let _system_updater = Lwt.async (fun () ->
    (* Update immediately on startup to replace placeholder with real data *)
    let%lwt () = update_system_stats_cache_lwt () in
    let rec system_loop () =
      (* Check for shutdown request *)
      if Atomic.get shutdown_requested then (
        Logging.debug ~section:"system_cache" "System cache updater shutting down due to shutdown request";
        Lwt.return_unit
      ) else (
      Telemetry.update_system_metrics ();
      (* Always update system stats cache for real-time dashboard *)
      let%lwt () = update_system_stats_cache_lwt () in
      (* Perform periodic memory maintenance *)
      perform_memory_maintenance ();
      Lwt_unix.sleep 1.0 >>= system_loop
      )
    in
    system_loop ()
  ) in
  ()

  (* Add periodic event bus cleanup - every 5 minutes *)
  let _cleanup_loop = Lwt.async (fun () ->
    Logging.debug ~section:"system_cache" "Starting event bus cleanup loop";
    let rec cleanup_loop () =
      (* Check for shutdown request *)
      if Atomic.get shutdown_requested then (
        Logging.debug ~section:"system_cache" "System cache cleanup loop shutting down due to shutdown request";
        Lwt.return_unit
      ) else (
      let%lwt () = Lwt_unix.sleep 300.0 in (* Clean up every 5 minutes *)
      let removed_opt = SystemStatsEventBus.cleanup_stale_subscribers cache.system_stats_event_bus () in
      (match removed_opt with
       | Some removed_count ->
           if removed_count > 0 then
             Logging.info_f ~section:"system_cache" "Cleaned up %d stale system stats subscribers" removed_count
       | None -> ());

      (* Clear event bus latest reference periodically to prevent memory retention *)
      SystemStatsEventBus.clear_latest cache.system_stats_event_bus;

      (* Report subscriber statistics to telemetry *)
      let (total, active, _) = SystemStatsEventBus.get_subscriber_stats cache.system_stats_event_bus in
      Telemetry.set_event_bus_subscribers_total total;
      Telemetry.set_event_bus_subscribers_active active;
      cleanup_loop ()
      )
    in
    cleanup_loop ()
  )

(** Force cleanup stale subscribers for dashboard memory management *)
let force_cleanup_stale_subscribers () =
  SystemStatsEventBus.force_cleanup_stale_subscribers cache.system_stats_event_bus ()

(** Initialize the system cache with double-checked locking *)
let init () =
  (* First check without locking (fast path) *)
  if !initialized then (
    Logging.debug ~section:"system_cache" "System cache already initialized, skipping";
    ()
  ) else (
    (* Acquire lock for initialization *)
    Mutex.lock init_mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock init_mutex)
      (fun () ->
        (* Double-check after acquiring lock *)
        if !initialized then (
          Logging.debug ~section:"system_cache" "System cache already initialized during lock acquisition";
          ()
        ) else (
          initialized := true;
          try
            (* Publish initial snapshot synchronously before starting background updater *)
            publish_initial_snapshot ();
            start_system_updater ();
            Logging.info ~section:"system_cache" "System cache initialized"
          with exn ->
            Logging.error_f ~section:"system_cache" "Failed to initialize system cache: %s" (Printexc.to_string exn);
            Logging.debug ~section:"system_cache" "System cache initialization failed"
        )
      )
  )
