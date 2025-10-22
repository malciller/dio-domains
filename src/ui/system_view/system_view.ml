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

(** Create system monitoring UI *)
let system_view (stats : Ui_types.system_stats) (_snapshot : Ui_types.telemetry_snapshot) =
  (* CPU info from cpuid *)
  let cpu_info = match stats.cpu_cores, stats.cpu_vendor, stats.cpu_model with
    | Some cores, vendor, model ->
        Printf.sprintf "CPU: %s %s (%d cores)" (format_cpu_vendor vendor) (format_cpu_model model) cores
    | None, vendor, model ->
        Printf.sprintf "CPU: %s %s" (format_cpu_vendor vendor) (format_cpu_model model)
  in
  let cpu_info_display = string ~attr:Notty.A.(fg white) cpu_info in

  let header = cpu_info_display in

  (* CPU Section - show per-core usage *)
  let cpu_title = string ~attr:Notty.A.(st bold ++ fg cyan) "CPU" in
  let total_cpu_color = if stats.cpu_usage > 90.0 then Notty.A.(fg red) else if stats.cpu_usage > 70.0 then Notty.A.(fg yellow) else Notty.A.(fg green) in
  let total_cpu_text = string ~attr:total_cpu_color (Printf.sprintf "Total: %.1f%%" stats.cpu_usage) in
  let load_text = string ~attr:Notty.A.(fg white) (Printf.sprintf "Load: %.2f %.2f %.2f" stats.load_avg_1 stats.load_avg_5 stats.load_avg_15) in

  (* Create core bars - show all available cores *)
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

  let core_bars = List.map2 (fun usage core_name ->
    let bar = progress_bar 10 usage in  (* Shorter bars to fit more cores *)
    let percent = colored_percentage usage in
    hcat [
      string ~attr:Notty.A.(fg white) (Printf.sprintf "%-6s" core_name);
      bar;
      string ~attr:Notty.A.empty " ";
      percent
    ]
  ) cores_to_show core_names in

  (* Show core count info if we have cpuid data *)
  let cores_info = match stats.cpu_cores with
    | Some total_cores ->
        string ~attr:Notty.A.(fg white) (Printf.sprintf " (%d cores)" total_cores)
    | None -> string ~attr:Notty.A.empty ""
  in

  let cpu_section = vcat ([
    hcat [cpu_title; cores_info];
    total_cpu_text;
    load_text;
    string ~attr:Notty.A.empty "";
  ] @ core_bars) in

  (* Memory Section *)
  let mem_title = string ~attr:Notty.A.(st bold ++ fg cyan) "Memory" in
  let mem_total_mb = stats.memory_total / 1024 in
  let mem_used_mb = stats.memory_used / 1024 in
  let mem_usage = if mem_total_mb > 0 then float_of_int mem_used_mb /. float_of_int mem_total_mb *. 100.0 else 0.0 in
  let mem_bar = progress_bar 20 mem_usage in
  let mem_percent = colored_percentage mem_usage in
  let mem_text = string ~attr:Notty.A.(fg white) (Printf.sprintf "%d/%d MB" mem_used_mb mem_total_mb) in
  let mem_section = vcat [
    mem_title;
    hcat [mem_bar; string ~attr:Notty.A.empty " "; mem_percent];
    mem_text;
  ] in

  (* Swap Section *)
  let swap_title = string ~attr:Notty.A.(st bold ++ fg cyan) "Swap" in
  let swap_usage = if stats.swap_total > 0 then float_of_int stats.swap_used /. float_of_int stats.swap_total *. 100.0 else 0.0 in
  let swap_bar = progress_bar 20 swap_usage in
  let swap_percent = colored_percentage swap_usage in
  let swap_text = string ~attr:Notty.A.(fg white) (Printf.sprintf "%s/%s" (format_bytes stats.swap_used) (format_bytes stats.swap_total)) in
  let swap_section = vcat [
    swap_title;
    hcat [swap_bar; string ~attr:Notty.A.empty " "; swap_percent];
    swap_text;
  ] in

  (* Process Section *)
  let proc_title = string ~attr:Notty.A.(st bold ++ fg cyan) "Processes" in
  let proc_text = string ~attr:Notty.A.(fg white) (Printf.sprintf "%d running" stats.processes) in
  let proc_section = vcat [
    proc_title;
    proc_text;
  ] in

  (* Combine all sections *)
  vcat [
    header;
    string ~attr:Notty.A.empty "";
    cpu_section;
    string ~attr:Notty.A.empty "";
    mem_section;
    string ~attr:Notty.A.empty "";
    swap_section;
    string ~attr:Notty.A.empty "";
    proc_section;
    string ~attr:Notty.A.empty "";
  ]

(** Create reactive system view with shared stats and snapshot variables *)
let make_system_view stats_var snapshot_var =
  (* Reactive UI *)
  let ui = Lwd.map2 (Lwd.get snapshot_var) (Lwd.get stats_var) ~f:(fun snapshot stats ->
    system_view stats snapshot
  ) in

  (* Event handling - only handle quit key *)
  let handle_event ev =
    match ev with
    | `Key (`ASCII 'q', []) -> `Quit
    | _ -> `Continue
  in

  (ui, handle_event)
