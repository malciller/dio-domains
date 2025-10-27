(** System Monitoring View (btop-like)

    Displays real-time system information including CPU, memory, disk, network,
    and process statistics in a terminal UI using Nottui and LWD.
*)

open Nottui.Ui
open Nottui_widgets
open Ui_types


(** Format bytes to human readable *)
let format_bytes bytes =
  let units = ["B"; "KB"; "MB"; "GB"; "TB"] in
  let rec format value unit_list =
    match unit_list with
    | [] -> Printf.sprintf "%.0f TB" value
    | unit :: rest ->
        if value < 1024.0 then
          Printf.sprintf "%.1f %s" value unit
        else
          format (value /. 1024.0) rest
  in
  format (float_of_int bytes) units

(** Create progress bar *)
let progress_bar width percentage =
  let filled = min width (max 0 (int_of_float (percentage *. float_of_int width /. 100.0))) in
  let bar = Printf.sprintf "%s%s" (String.make filled '#') (String.make (width - filled) '.') in
  string ~attr:Notty.A.(fg white) bar

(** Create colored percentage display *)
let colored_percentage percentage =
  let color = if percentage > 90.0 then Notty.A.(fg red)
             else if percentage > 70.0 then Notty.A.(fg yellow)
             else Notty.A.(fg green) in
  let text = Printf.sprintf "%.1f%%" percentage in
  string ~attr:color text

(** Format CPU vendor *)
let format_cpu_vendor = function
  | Some vendor -> vendor
  | None -> "Unknown"

(** Format CPU model *)
let format_cpu_model = function
  | Some model -> model
  | None -> "Unknown"

(** Create responsive system monitoring UI *)
let system_view (stats : Ui_types.system_stats) (_snapshot : Ui_types.telemetry_snapshot) ~available_width ~available_height =
  (* Determine screen size for adaptive display *)
  let screen_size = classify_screen_size available_width available_height in

  (* Adaptive CPU info - truncate on small screens *)
  let cpu_info = match stats.cpu_cores, stats.cpu_vendor, stats.cpu_model with
    | Some cores, vendor, model ->
        let full_info = Printf.sprintf "CPU: %s %s (%d cores)" (format_cpu_vendor vendor) (format_cpu_model model) cores in
        begin match screen_size with
        | Mobile -> truncate_string 20 full_info
        | Small -> truncate_string 30 full_info
        | Medium | Large -> full_info
        end
    | None, vendor, model ->
        let full_info = Printf.sprintf "CPU: %s %s" (format_cpu_vendor vendor) (format_cpu_model model) in
        begin match screen_size with
        | Mobile -> truncate_string 15 full_info
        | Small -> truncate_string 25 full_info
        | Medium | Large -> full_info
        end
  in
  let cpu_info_display = string ~attr:Notty.A.(fg white) cpu_info in

  let header = cpu_info_display in

  (* CPU Section - adaptive per-core usage display *)
  let cpu_title = string ~attr:Notty.A.(st bold ++ fg cyan) "CPU" in
  let total_cpu_color = if stats.cpu_usage > 90.0 then Notty.A.(fg red) else if stats.cpu_usage > 70.0 then Notty.A.(fg yellow) else Notty.A.(fg green) in
  let total_cpu_text = string ~attr:total_cpu_color (Printf.sprintf "Total: %.1f%%" stats.cpu_usage) in

  (* Adaptive load average display *)
  let load_text = match screen_size with
    | Mobile -> string ~attr:Notty.A.(fg white) (Printf.sprintf "Load: %.1f" stats.load_avg_1)
    | Small -> string ~attr:Notty.A.(fg white) (Printf.sprintf "Load: %.1f %.1f" stats.load_avg_1 stats.load_avg_5)
    | Medium | Large -> string ~attr:Notty.A.(fg white) (Printf.sprintf "Load: %.2f %.2f %.2f" stats.load_avg_1 stats.load_avg_5 stats.load_avg_15)
  in

  (* Adaptive core display based on screen size and available space *)
  let (core_bars, cores_info) = match screen_size with
    | Mobile ->
        (* No per-core display on mobile - too cramped *)
        ([], string ~attr:Notty.A.(fg white) "")
    | Small | Medium | Large ->
        let bar_width = adaptive_progress_bar_width screen_size in
        let max_cores_to_show = match stats.cpu_cores with
          | Some cores -> cores  (* Show all cores *)
          | None -> List.length stats.core_usages  (* Show all available cores *)
        in

        (* For macOS where we only have total CPU, show it as "All Cores" *)
        let (cores_to_show, core_names) =
          if List.length stats.core_usages = 1 && Option.is_some stats.cpu_cores then
            (* macOS case: show total across all cores *)
            let total_cores = Option.get stats.cpu_cores in
            let usage_per_core = (List.hd stats.core_usages) /. float_of_int total_cores in
            let core_usages = List.init total_cores (fun _ -> usage_per_core) in
            (core_usages, List.init (List.length core_usages) (fun i -> Printf.sprintf "Core %d" i))
          else
            (* Linux case: show actual per-core data *)
            let cores = List.filteri (fun i _ -> i < max_cores_to_show) stats.core_usages in
            (cores, List.init (List.length cores) (fun i -> Printf.sprintf "Core %d" i))
        in

        (* Limit cores shown based on available height *)
        let max_cores_display = max 1 (available_height - 8) in (* Reserve space for other sections *)
        let cores_to_display = min (List.length cores_to_show) max_cores_display in
        let displayed_cores = List.filteri (fun i _ -> i < cores_to_display) cores_to_show in
        let displayed_names = List.filteri (fun i _ -> i < cores_to_display) core_names in

        let core_bars = List.map2 (fun usage core_name ->
          let bar = progress_bar bar_width usage in
          let percent = colored_percentage usage in
          let name_width = match screen_size with
            | Small -> 4  (* Shorter names *)
            | Medium -> 6
            | Large -> 8
            | Mobile -> 4 (* Won't be used *)
          in
          hcat [
            string ~attr:Notty.A.(fg white) (Printf.sprintf "%-*s" name_width core_name);
            bar;
            string ~attr:Notty.A.empty " ";
            percent
          ]
        ) displayed_cores displayed_names in

        (* Show core count info if we have cpuid data *)
        let cores_info = match stats.cpu_cores with
          | Some total_cores when screen_size = Large ->
              string ~attr:Notty.A.(fg white) (Printf.sprintf " (%d cores)" total_cores)
          | _ -> string ~attr:Notty.A.empty ""
        in

        (core_bars, cores_info)
  in

  let cpu_section = vcat ([
    hcat [cpu_title; cores_info];
    total_cpu_text;
    load_text;
    string ~attr:Notty.A.empty "";
  ] @ core_bars) in

  (* Memory Section - adaptive display *)
  let mem_title = string ~attr:Notty.A.(st bold ++ fg cyan) "Memory" in
  let mem_total_mb = stats.memory_total / 1024 in
  let mem_used_mb = stats.memory_used / 1024 in
  let mem_usage = if mem_total_mb > 0 then float_of_int mem_used_mb /. float_of_int mem_total_mb *. 100.0 else 0.0 in
  let mem_bar_width = adaptive_progress_bar_width screen_size in
  let mem_bar = progress_bar mem_bar_width mem_usage in
  let mem_percent = colored_percentage mem_usage in

  (* Adaptive memory text based on screen size *)
  let mem_text = match screen_size with
    | Mobile -> string ~attr:Notty.A.(fg white) (Printf.sprintf "%.0f%%" mem_usage)
    | Small -> string ~attr:Notty.A.(fg white) (Printf.sprintf "%dM/%dM" mem_used_mb mem_total_mb)
    | Medium | Large -> string ~attr:Notty.A.(fg white) (Printf.sprintf "%d/%d MB" mem_used_mb mem_total_mb)
  in
  let mem_section = vcat [
    mem_title;
    hcat [mem_bar; string ~attr:Notty.A.empty " "; mem_percent];
    mem_text;
  ] in

  (* Swap Section - hide on very small screens *)
  let swap_section = match screen_size with
    | Mobile -> empty  (* No swap info on mobile *)
    | Small | Medium | Large ->
        let swap_title = string ~attr:Notty.A.(st bold ++ fg cyan) "Swap" in
        let swap_usage = if stats.swap_total > 0 then float_of_int stats.swap_used /. float_of_int stats.swap_total *. 100.0 else 0.0 in
        let swap_bar = progress_bar (adaptive_progress_bar_width screen_size) swap_usage in
        let swap_percent = colored_percentage swap_usage in

        (* Adaptive swap text *)
        let swap_text = match screen_size with
          | Small -> string ~attr:Notty.A.(fg white) (Printf.sprintf "%.0f%%" swap_usage)
          | Medium -> string ~attr:Notty.A.(fg white) (Printf.sprintf "%s/%s" (format_bytes (stats.swap_used * 1024)) (format_bytes (stats.swap_total * 1024)))
          | Large -> string ~attr:Notty.A.(fg white) (Printf.sprintf "%s/%s" (format_bytes stats.swap_used) (format_bytes stats.swap_total))
          | Mobile -> empty  (* Won't be used *)
        in
        vcat [
          swap_title;
          hcat [swap_bar; string ~attr:Notty.A.empty " "; swap_percent];
          swap_text;
        ]
  in

  (* Process Section *)
  let proc_title = string ~attr:Notty.A.(st bold ++ fg cyan) "Processes" in
  let proc_text = string ~attr:Notty.A.(fg white) (Printf.sprintf "%d running" stats.processes) in
  let proc_section = vcat [
    proc_title;
    proc_text;
  ] in

  (* Temperature Section - hide on small screens *)
  let temp_section =
    if not (should_show_temperature screen_size) then
      empty  (* Hide temperature on small screens *)
    else
      match stats.cpu_temp, stats.gpu_temp with
      | None, None -> empty  (* No temperature data available *)
      | _, _ ->
          let temp_title = string ~attr:Notty.A.(st bold ++ fg cyan) "Temperature" in
          let temp_lines = [] in

          (* Add CPU temperature *)
          let temp_lines = match stats.cpu_temp with
            | Some temp ->
                let temp_color =
                  if temp > 90.0 then Notty.A.(fg red)
                  else if temp > 75.0 then Notty.A.(fg yellow)
                  else Notty.A.(fg green)
                in
                let cpu_temp_text = string ~attr:temp_color (Printf.sprintf "CPU: %.1f°C" temp) in
                temp_lines @ [cpu_temp_text]
            | None -> temp_lines
          in

          (* Add GPU temperature *)
          let temp_lines = match stats.gpu_temp with
            | Some temp ->
                let temp_color =
                  if temp > 85.0 then Notty.A.(fg red)
                  else if temp > 70.0 then Notty.A.(fg yellow)
                  else Notty.A.(fg green)
                in
                let gpu_temp_text = string ~attr:temp_color (Printf.sprintf "GPU: %.1f°C" temp) in
                temp_lines @ [gpu_temp_text]
            | None -> temp_lines
          in

          (* Add other sensors if we have them and neither CPU nor GPU was found *)
          let temp_lines =
            if stats.cpu_temp = None && stats.gpu_temp = None && List.length stats.temps > 0 then
              (* Show first few sensors as fallback - limit based on screen size *)
              let max_sensors = match screen_size with
                | Mobile -> 1
                | Small -> 2
                | Medium | Large -> 3
              in
              let sensor_lines = List.filteri (fun i _ -> i < max_sensors) stats.temps |> List.map (fun (name, temp) ->
                let temp_color =
                  if temp > 80.0 then Notty.A.(fg red)
                  else if temp > 65.0 then Notty.A.(fg yellow)
                  else Notty.A.(fg green)
                in
                string ~attr:temp_color (Printf.sprintf "%s: %.1f°C" name temp)
              ) in
              temp_lines @ sensor_lines
            else
              temp_lines
          in

          if temp_lines = [] then
            empty
          else
            vcat (temp_title :: temp_lines)
  in

  (* Combine all sections *)
  let sections = [
    header;
    string ~attr:Notty.A.empty "";
    cpu_section;
    string ~attr:Notty.A.empty "";
    mem_section;
    string ~attr:Notty.A.empty "";
    swap_section;
    string ~attr:Notty.A.empty "";
    proc_section;
  ] in
  
  (* Only add temperature section if it's not empty *)
  let sections = if temp_section <> empty then
    sections @ [string ~attr:Notty.A.empty ""; temp_section]
  else
    sections
  in
  
  vcat (sections @ [string ~attr:Notty.A.empty ""])

(** Create reactive system view with shared stats and snapshot variables *)
let make_system_view stats_var snapshot_var ~available_width ~available_height =
  (* Reactive UI *)
  let ui = Lwd.map2 (Lwd.get snapshot_var) (Lwd.get stats_var) ~f:(fun snapshot stats ->
    system_view stats snapshot ~available_width ~available_height
  ) in

  (* Event handling - only handle quit key *)
  let handle_event ev =
    match ev with
    | `Key (`ASCII 'q', []) -> `Quit
    | _ -> `Continue
  in

  (ui, handle_event)
