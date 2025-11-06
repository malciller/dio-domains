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

  (* Force GC every 15 minutes for dashboard mode to prevent gradual memory creep *)
  if time_since_gc > 900.0 then (
    Logging.debug ~section:"system_cache" "Performing scheduled GC (15-minute interval for dashboard mode)";
    Gc.full_major ();
    last_gc_time := now
  );

  (* Check for memory leaks *)
  check_memory_leak ()

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
              cpu_temp = None;
              gpu_temp = None;
              temps = [];
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
      Lwt_unix.sleep 1.0 >>= system_loop
    in
    system_loop ()
  ) in
  ()

  (* Add periodic event bus cleanup - every 5 minutes *)
  let _cleanup_loop = Lwt.async (fun () ->
    Logging.debug ~section:"system_cache" "Starting event bus cleanup loop";
    let rec cleanup_loop () =
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
            start_system_updater ();
            Logging.info ~section:"system_cache" "System cache initialized"
          with exn ->
            Logging.error_f ~section:"system_cache" "Failed to initialize system cache: %s" (Printexc.to_string exn);
            Logging.debug ~section:"system_cache" "System cache initialization failed"
        )
      )
  )
