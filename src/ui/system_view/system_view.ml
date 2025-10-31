(** System Monitoring View (btop-like)

    Displays real-time system information including CPU, memory, disk, network,
    and process statistics in a terminal UI using Nottui and LWD.
*)

open Nottui_widgets
open Nottui.Ui
open Ui_types

module Ui_components = Dio_ui_shared.Ui_components


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

(** Create progress bar using UI components *)
let progress_bar width percentage =
Ui_components.create_progress_bar width percentage

(** Create colored percentage display using UI components *)
let colored_percentage percentage =
  let text = Printf.sprintf "%.1f%%" percentage in
  if percentage > 90.0 then Ui_components.create_error_status text
  else if percentage > 70.0 then Ui_components.create_warning_status text
  else Ui_components.create_success_status text

(** Format CPU vendor *)
let format_cpu_vendor = function
  | Some vendor -> vendor
  | None -> "Unknown"

(** Format CPU model *)
let format_cpu_model = function
  | Some model -> model
  | None -> "Unknown"

(** Calculate minimum width required for system view content *)
let calculate_system_view_min_width stats =
  let cpu_info_width = match stats.cpu_cores, stats.cpu_vendor, stats.cpu_model with
    | Some cores, vendor, model ->
        let full_info = Printf.sprintf "CPU: %s %s (%d cores)" (format_cpu_vendor vendor) (format_cpu_model model) cores in
        String.length full_info
    | None, vendor, model ->
        let full_info = Printf.sprintf "CPU: %s %s" (format_cpu_vendor vendor) (format_cpu_model model) in
        String.length full_info
  in

  (* Memory text width - format_bytes gives us string length *)
  let mem_text_width = String.length (Printf.sprintf "%s/%s" (format_bytes (stats.memory_used * 1024)) (format_bytes (stats.memory_total * 1024))) in

  (* Swap text width (if swap exists) *)
  let swap_text_width = if stats.swap_total > 0 then
    String.length (Printf.sprintf "%s/%s" (format_bytes (stats.swap_used * 1024)) (format_bytes (stats.swap_total * 1024)))
  else 0 in

  (* Process count text *)
  let proc_text_width = String.length (Printf.sprintf "%d running" stats.processes) in

  (* Temperature text widths (if available) *)
  let temp_widths = match stats.cpu_temp, stats.gpu_temp with
    | Some cpu_temp, Some gpu_temp ->
        [String.length (Printf.sprintf "CPU: %.1f°C" cpu_temp);
         String.length (Printf.sprintf "GPU: %.1f°C" gpu_temp)]
    | Some cpu_temp, None ->
        [String.length (Printf.sprintf "CPU: %.1f°C" cpu_temp)]
    | None, Some gpu_temp ->
        [String.length (Printf.sprintf "GPU: %.1f°C" gpu_temp)]
    | None, None -> []
  in
  let temp_widths = temp_widths @ List.map (fun (name, temp) -> String.length (Printf.sprintf "%s: %.1f°C" name temp)) stats.temps in
  let max_temp_width = if temp_widths = [] then 0 else List.fold_left max 0 temp_widths in

  (* Progress bar width - use adaptive width function *)
  let progress_bar_width = Ui_types.adaptive_progress_bar_width (Ui_types.classify_screen_size 80 24) in

  (* Find the maximum width needed across all elements *)
  let widths = [cpu_info_width; mem_text_width; swap_text_width; proc_text_width; max_temp_width; progress_bar_width + 10] in
  List.fold_left max 20 widths  (* Minimum 20 chars to be reasonable *)

(** Create responsive system monitoring UI *)
let system_view (stats : Ui_types.system_stats) (_snapshot : Ui_types.telemetry_snapshot) ~available_width ~available_height =
  (* Determine screen size for adaptive display *)
  let screen_size = classify_screen_size available_width available_height in

  match screen_size with
  | Mobile ->
      (* Ultra-compact mobile layout: 2-3 lines maximum *)
      let cpu_info = match stats.cpu_cores with
        | Some cores -> Printf.sprintf "%s (%dc)" (truncate_tiny (format_cpu_model (Some "CPU"))) cores
        | None -> truncate_tiny (format_cpu_model (Some "CPU"))
      in

      (* Single-line summary: CPU% | Mem% | Load | Procs *)
      let cpu_pct = Ui_types.format_mobile_percentage stats.cpu_usage in
      let mem_pct = Ui_types.format_mobile_percentage ((float_of_int stats.memory_used /. float_of_int stats.memory_total) *. 100.0) in
      let load_val = Ui_types.format_mobile_number stats.load_avg_1 in
      let proc_count = Ui_types.format_mobile_number (float_of_int stats.processes) in

      let summary_line = Printf.sprintf "%s: %s | Mem: %s | Load: %s | Procs: %s"
        cpu_info cpu_pct mem_pct load_val proc_count in
      let summary_display = Ui_components.create_text (Ui_types.truncate_exact (available_width - 4) summary_line) in

      (* Second line: uptime (if space allows) *)
      let uptime_hours = int_of_float (Unix.time () -. !Telemetry.start_time) / 3600 in
      let uptime_line = if available_height >= 3 then
        Printf.sprintf "Uptime: %dh" uptime_hours
      else "" in
      let uptime_display = if uptime_line <> "" then
Ui_components.create_text (Ui_types.truncate_exact (available_width - 4) uptime_line)
      else empty in

      let content = if uptime_display = empty then
        summary_display
      else
        vcat [summary_display; uptime_display]
      in
      content

  | Small | Medium | Large ->
      (* Existing full layout for larger screens *)
      let cpu_info = match stats.cpu_cores, stats.cpu_vendor, stats.cpu_model with
        | Some cores, vendor, model ->
            let full_info = Printf.sprintf "CPU: %s %s (%d cores)" (format_cpu_vendor vendor) (format_cpu_model model) cores in
            begin match screen_size with
            | Small -> truncate_string 30 full_info
            | Medium | Large -> full_info
            | Mobile -> "" (* Won't reach here *)
            end
        | None, vendor, model ->
            let full_info = Printf.sprintf "CPU: %s %s" (format_cpu_vendor vendor) (format_cpu_model model) in
            begin match screen_size with
            | Small -> truncate_string 25 full_info
            | Medium | Large -> full_info
            | Mobile -> "" (* Won't reach here *)
            end
      in
      let cpu_info_display = Ui_components.create_text cpu_info in

      let header = cpu_info_display in

  (* CPU Section - adaptive per-core usage display *)
  let cpu_title = Ui_components.create_label "CPU" in
  let total_cpu_text = colored_percentage stats.cpu_usage in

  (* Adaptive load average display *)
  let load_text = match screen_size with
    | Mobile -> Ui_components.create_text (Printf.sprintf "Load: %.1f" stats.load_avg_1)
    | Small -> Ui_components.create_text (Printf.sprintf "Load: %.1f %.1f" stats.load_avg_1 stats.load_avg_5)
    | Medium | Large -> Ui_components.create_text (Printf.sprintf "Load: %.2f %.2f %.2f" stats.load_avg_1 stats.load_avg_5 stats.load_avg_15)
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
Ui_components.create_text (Printf.sprintf "%-*s" name_width core_name);
            bar;
Ui_components.create_text " ";
            percent
          ]
        ) displayed_cores displayed_names in

        (* Show core count info if we have cpuid data *)
        let cores_info = match stats.cpu_cores with
          | Some total_cores when screen_size = Large ->
Ui_components.create_text (Printf.sprintf " (%d cores)" total_cores)
          | _ -> empty
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
  let mem_title = Ui_components.create_label "Memory" in
  let mem_total_mb = stats.memory_total / 1024 in
  let mem_used_mb = stats.memory_used / 1024 in
  let mem_usage = if mem_total_mb > 0 then float_of_int mem_used_mb /. float_of_int mem_total_mb *. 100.0 else 0.0 in
  let mem_bar_width = adaptive_progress_bar_width screen_size in
  let mem_bar = progress_bar mem_bar_width mem_usage in
  let mem_percent = colored_percentage mem_usage in

  (* Adaptive memory text based on screen size *)
  let mem_text = match screen_size with
    | Mobile -> Ui_components.create_text (Printf.sprintf "%.0f%%" mem_usage)
    | Small -> Ui_components.create_text (Printf.sprintf "%dM/%dM" mem_used_mb mem_total_mb)
    | Medium | Large -> Ui_components.create_text (Printf.sprintf "%d/%d MB" mem_used_mb mem_total_mb)
  in
  let mem_section = vcat [
    mem_title;
    hcat [mem_bar; Ui_components.create_text " "; mem_percent];
    mem_text;
  ] in

  (* Swap Section - hide on very small screens *)
  let swap_section = match screen_size with
    | Mobile -> empty  (* No swap info on mobile *)
    | Small | Medium | Large ->
        let swap_title = Ui_components.create_label "Swap" in
        let swap_usage = if stats.swap_total > 0 then float_of_int stats.swap_used /. float_of_int stats.swap_total *. 100.0 else 0.0 in
        let swap_bar = progress_bar (adaptive_progress_bar_width screen_size) swap_usage in
        let swap_percent = colored_percentage swap_usage in

        (* Adaptive swap text *)
        let swap_text = match screen_size with
          | Small -> Ui_components.create_text (Printf.sprintf "%.0f%%" swap_usage)
          | Medium -> Ui_components.create_text (Printf.sprintf "%s/%s" (format_bytes (stats.swap_used * 1024)) (format_bytes (stats.swap_total * 1024)))
          | Large -> Ui_components.create_text (Printf.sprintf "%s/%s" (format_bytes stats.swap_used) (format_bytes stats.swap_total))
          | Mobile -> empty  (* Won't be used *)
        in
        vcat [
          swap_title;
          hcat [swap_bar; Ui_components.create_text " "; swap_percent];
          swap_text;
        ]
  in

  (* Process Section *)
  let proc_title = Ui_components.create_label "Processes" in
  let proc_text = Ui_components.create_text (Printf.sprintf "%d running" stats.processes) in
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
          let temp_title = Ui_components.create_label "Temperature" in
          let temp_lines = [] in

          (* Add CPU temperature *)
          let temp_lines = match stats.cpu_temp with
            | Some temp ->
                let cpu_temp_text = if temp > 90.0 then Ui_components.create_error_status (Printf.sprintf "CPU: %.1f°C" temp)
                                   else if temp > 75.0 then Ui_components.create_warning_status (Printf.sprintf "CPU: %.1f°C" temp)
                                   else Ui_components.create_success_status (Printf.sprintf "CPU: %.1f°C" temp) in
                temp_lines @ [cpu_temp_text]
            | None -> temp_lines
          in

          (* Add GPU temperature *)
          let temp_lines = match stats.gpu_temp with
            | Some temp ->
                let gpu_temp_text = if temp > 85.0 then Ui_components.create_error_status (Printf.sprintf "GPU: %.1f°C" temp)
                                   else if temp > 70.0 then Ui_components.create_warning_status (Printf.sprintf "GPU: %.1f°C" temp)
                                   else Ui_components.create_success_status (Printf.sprintf "GPU: %.1f°C" temp) in
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
                let sensor_text = if temp > 80.0 then Ui_components.create_error_status (Printf.sprintf "%s: %.1f°C" name temp)
                                 else if temp > 65.0 then Ui_components.create_warning_status (Printf.sprintf "%s: %.1f°C" name temp)
                                 else Ui_components.create_success_status (Printf.sprintf "%s: %.1f°C" name temp) in
                sensor_text
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

  (* Combine all sections with selective spacing *)
  let sections = [
    header;
    string ~attr:Notty.A.empty "";
    cpu_section;
    mem_section;
    swap_section;
    string ~attr:Notty.A.empty "";
    proc_section;
  ] in

  (* Only add temperature section if it's not empty, with minimal spacing *)
  let sections = if temp_section <> empty then
    sections @ [temp_section]
  else
    sections
  in

  (* Minimal bottom padding only *)
  vcat sections

(** Create reactive system view with shared stats and snapshot variables *)
let make_system_view stats_var snapshot_var viewport_var =
  (* Reactive UI *)
  let ui = Lwd.map2
    (Lwd.map2 (Lwd.get snapshot_var) (Lwd.get stats_var) ~f:(fun snapshot stats -> (snapshot, stats)))
    (Lwd.get viewport_var)
    ~f:(fun (snapshot, stats) (available_width, available_height) ->
      system_view stats snapshot ~available_width ~available_height
    ) in

  (* Event handling - only handle quit key *)
  let handle_event ev =
    match ev with
    | `Key (`ASCII 'q', []) -> `Quit
    | _ -> `Continue
  in

  (ui, handle_event)
