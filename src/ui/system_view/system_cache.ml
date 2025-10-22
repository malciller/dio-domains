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
          let result = String.trim (input_line cmd) in
          ignore (Unix.close_process_in cmd);
          Some (int_of_string result)
        with _ ->
          (* Last fallback: try nproc command *)
          try
            let cmd = Unix.open_process_in "nproc 2>/dev/null" in
            let result = String.trim (input_line cmd) in
            ignore (Unix.close_process_in cmd);
            Some (int_of_string result)
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
          let vendor_str = String.trim (input_line cmd) in
          ignore (Unix.close_process_in cmd);
          Some vendor_str
        with _ ->
          (* Try uname -m for architecture *)
          try
            let cmd = Unix.open_process_in "uname -m 2>/dev/null" in
            let arch = String.trim (input_line cmd) in
            ignore (Unix.close_process_in cmd);
            match arch with
            | "arm64" | "aarch64" -> Some "Apple Silicon"
            | "x86_64" -> Some "Intel/AMD"
            | _ -> Some arch
          with _ -> None

  let get_cpu_model () =
    match Cpuid.model () with
    | Ok (family, model, stepping) -> Some (Printf.sprintf "%d/%d/%d" family model stepping)
    | Error _ ->
        (* Try to get CPU model on macOS/ARM *)
        try
          let cmd = Unix.open_process_in "sysctl -n machdep.cpu.brand_string 2>/dev/null" in
          let model_str = String.trim (input_line cmd) in
          ignore (Unix.close_process_in cmd);
          Some model_str
        with _ ->
          (* Fallback to uname -p for processor type *)
          try
            let cmd = Unix.open_process_in "uname -p 2>/dev/null" in
            let proc = String.trim (input_line cmd) in
            ignore (Unix.close_process_in cmd);
            Some proc
          with _ -> None
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

(** Global system cache instance *)
let cache = {
  current_system_stats = None;
  system_stats_event_bus = SystemStatsEventBus.create "system_stats";
  last_update = 0.0;
  update_interval = 0.5;  (* Update system stats at most every 0.5 seconds *)
}

(** Memory usage tracking for leak detection *)
let memory_usage_history = ref []
let max_memory_history = 100  (* Keep last 100 memory readings *)
let memory_leak_threshold_mb = 100.0  (* Alert if memory grows by 100MB in last readings *)
let last_gc_time = ref 0.0

(** Get current OCaml memory usage in MB *)
let get_ocaml_memory_usage () =
  let stat = Gc.stat () in
  let heap_words = stat.heap_words in
  let word_size = Sys.word_size / 8 in  (* bytes per word *)
  float_of_int (heap_words * word_size) /. (1024.0 *. 1024.0)

(** Check for memory leaks and trigger cleanup if needed *)
let check_memory_leak () =
  let current_mem = get_ocaml_memory_usage () in
  let now = Unix.time () in

  (* Add current reading to history *)
  memory_usage_history := (now, current_mem) :: !memory_usage_history;

  (* Trim history to max size *)
  if List.length !memory_usage_history > max_memory_history then
    memory_usage_history := take max_memory_history !memory_usage_history;

  (* Check for memory growth trend *)
  match !memory_usage_history with
  | (oldest_time, oldest_mem) :: _ when List.length !memory_usage_history >= 10 ->
      let time_diff = now -. oldest_time in
      let mem_diff = current_mem -. oldest_mem in

      (* If memory has grown significantly over the last readings, trigger GC *)
      if mem_diff > memory_leak_threshold_mb && time_diff > 300.0 then (  (* 5 minutes *)
        Logging.warn_f ~section:"system_cache" "Memory leak detected: %.1f MB growth in %.0f seconds, triggering GC"
          mem_diff time_diff;
        Gc.full_major ();
        last_gc_time := now;
        Logging.info_f ~section:"system_cache" "GC completed, memory now: %.1f MB" (get_ocaml_memory_usage ())
      )
  | _ -> ()

(** Periodic memory maintenance *)
let perform_memory_maintenance () =
  let now = Unix.time () in
  let time_since_gc = now -. !last_gc_time in

  (* Force GC every 30 minutes to prevent gradual memory creep *)
  if time_since_gc > 1800.0 then (
    Logging.debug ~section:"system_cache" "Performing scheduled GC";
    Gc.full_major ();
    last_gc_time := now
  );

  (* Check for memory leaks *)
  check_memory_leak ()

(** Initialization guard *)
let initialized = ref false

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
      let mem_total_bytes = int_of_string (String.trim (input_line mem_total_cmd)) in
      ignore (Unix.close_process_in mem_total_cmd);
      let mem_total = mem_total_bytes / 1024 in  (* Convert to KB *)

      let vm_stats_cmd = Unix.open_process_in "vm_stat" in
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
          ignore (Unix.close_process_in vm_stats_cmd);
          let page_size_kb = 16384 / 1024 in (* Apple Silicon uses 16KB pages *)
          let mem_used_pages = pages_active + pages_wired in
          let mem_free_pages = pages_free + pages_inactive in
          let mem_used = mem_used_pages * page_size_kb in
          let mem_free = mem_free_pages * page_size_kb in
          (mem_total, mem_used, mem_free, 0, 0)  (* macOS swap is handled differently *)
      in
      let mem_total, mem_used, mem_free, swap_total, swap_used = read_vm_stats 0 0 0 0 in
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
      cores
  in
  read_cores []

(** Get CPU usage and per-core usage using proper time-interval sampling (Lwt-friendly) *)
let get_cpu_usage_lwt () =
  try
    (* Take first reading *)
    let stats1 = read_cpu_stats () in

    (* Sleep for 100ms to get a meaningful time interval *)
    let%lwt () = Lwt_unix.sleep 0.1 in

    (* Take second reading *)
    let stats2 = read_cpu_stats () in

    match stats1, stats2 with
    | (busy1, total1) :: core_stats1, (busy2, total2) :: core_stats2 ->
        (* Calculate total CPU usage over the interval *)
        let busy_delta = busy2 -. busy1 in
        let total_delta = total2 -. total1 in
        let total_percentage = if total_delta > 0.0 then (busy_delta /. total_delta) *. 100.0 else 0.0 in

        (* Calculate per-core usage over the interval *)
        let core_percentages =
          List.map2 (fun (busy1, total1) (busy2, total2) ->
            let busy_delta = busy2 -. busy1 in
            let total_delta = total2 -. total1 in
            if total_delta > 0.0 then (busy_delta /. total_delta) *. 100.0 else 0.0
          ) core_stats1 core_stats2
        in

        Lwt.return_some (total_percentage, core_percentages)
    | _ -> Lwt.return_none
  with _ ->
    (* macOS implementation using top command *)
    try
      let top_cmd = Unix.open_process_in "top -l 1 -n 0 | grep 'CPU usage'" in
      let line = input_line top_cmd in
      ignore (Unix.close_process_in top_cmd);

      (* Parse line like: "CPU usage: 15.23% user, 12.45% sys, 72.32% idle" *)
      let parts = String.split_on_char ',' line in
      let rec parse_usage parts user_sys =
        match parts with
        | [] -> user_sys
        | part :: rest ->
            try
              if String.contains part '%' then
                let percent_part = String.trim part in
                let percent_str = String.trim (List.hd (String.split_on_char '%' percent_part)) in
                let percent = float_of_string percent_str in
                if String.contains percent_part 'i' && String.contains percent_part 'd' &&
                   String.contains percent_part 'l' && String.contains percent_part 'e' then
                  parse_usage rest user_sys  (* Don't add idle time *)
                else
                  parse_usage rest (user_sys +. percent)  (* Add user/sys time *)
              else
                parse_usage rest user_sys
            with _ -> parse_usage rest user_sys
      in
      let user_sys_usage = parse_usage parts 0.0 in
      let total_usage = min 100.0 user_sys_usage in  (* User + sys time, capped at 100% *)

      (* For macOS, we can't easily get per-core data, so show total as single core *)
      let core_usages = [total_usage] in

      Lwt.return_some (total_usage, core_usages)
    with _ ->
      (* Unable to collect CPU usage *)
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
      let loadavg_str = String.trim (input_line loadavg_cmd) in
      ignore (Unix.close_process_in loadavg_cmd);
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
    let count_str = String.trim (input_line ic) in
    ignore (Unix.close_process_in ic);
    let count = int_of_string count_str in
    Some (max 1 (count - 1))  (* Subtract header line, ensure at least 1 *)
  with _ ->
    (* Unable to collect process count *)
    None

(** Create a fresh system stats snapshot (Lwt-friendly) *)
let create_system_stats_lwt () : system_stats option Lwt.t =
  let%lwt cpu_stats = get_cpu_usage_lwt () in
  let mem_stats = get_memory_stats () in
  let load_stats = get_load_avg () in
  let proc_count = get_process_count () in
  let cpu_cores = Cpu_info.get_cpu_cores () in
  let cpu_vendor = Cpu_info.get_cpu_vendor () in
  let cpu_model = Cpu_info.get_cpu_model () in

  match cpu_stats, mem_stats, load_stats, proc_count with
  | Some (total_cpu_usage, core_usages), Some (mem_total, mem_used, mem_free, swap_total, swap_used),
    Some (load1, load5, load15), Some processes ->
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
      }
  | _ -> Lwt.return_none

(** Update system stats cache with throttling (Lwt-friendly) *)
let update_system_stats_cache_lwt () =
  let now = Unix.time () in
  if now -. cache.last_update >= cache.update_interval then (
    let%lwt stats_opt = create_system_stats_lwt () in
    (match stats_opt with
     | Some stats ->
         cache.current_system_stats <- Some stats;
         cache.last_update <- now;
         SystemStatsEventBus.publish cache.system_stats_event_bus stats
     | None ->
         (* Keep previous stats if collection fails - don't create dummy data *)
         (* The UI will show the last known good values or handle the None case *)
         ());
    Lwt.return_unit
  ) else
    Lwt.return_unit

(** Subscribe to system stats updates *)
let subscribe_system_stats () = SystemStatsEventBus.subscribe cache.system_stats_event_bus

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
            }
      )

(** Start background system stats updater *)
let start_system_updater () =
  (* Ensure we have initial system stats, but do it asynchronously *)
  Lwt.async (fun () ->
    let%lwt initial_stats_opt = create_system_stats_lwt () in
    (match initial_stats_opt with
     | Some initial_stats ->
         cache.current_system_stats <- Some initial_stats;
         cache.last_update <- Unix.time ();
         SystemStatsEventBus.publish cache.system_stats_event_bus initial_stats
     | None -> ());
    Lwt.return_unit
  );

  (* Start periodic system metrics updater *)
  let _system_updater = Lwt.async (fun () ->
    let rec system_loop () =
      Telemetry.update_system_metrics ();
      (* Always update system stats cache for real-time dashboard *)
      let%lwt () = update_system_stats_cache_lwt () in
      (* Perform periodic memory maintenance *)
      perform_memory_maintenance ();
      Lwt_unix.sleep 0.5 >>= system_loop
    in
    system_loop ()
  ) in
  ()

(** Initialize the system cache *)
let init () =
  if !initialized then (
    Logging.debug ~section:"system_cache" "System cache already initialized, skipping";
    ()
  ) else (
    initialized := true;
    start_system_updater ();
    Logging.info ~section:"system_cache" "System cache initialized"
  )
