(** Interactive Telemetry Dashboard View

    Displays real-time telemetry metrics in a rich terminal UI using Nottui and LWD.
*)


open Nottui_widgets
open Nottui.Ui
open Ui_types

module Ui_components = Dio_ui_shared.Ui_components

(** UI state for telemetry view *)
type state = {
  selected_category: int;
  scroll_offset: int;
  expanded_category: string option;
}

let initial_state = {
  selected_category = 0;
  scroll_offset = 0;
  expanded_category = None;
}

(** Helper function to take first n elements from a list *)
let rec take_first n = function
  | [] -> []
  | _ when n <= 0 -> []
  | x :: xs -> x :: take_first (n - 1) xs

(** Format metric value with colors - using cached snapshot data *)
let format_metric_value_ui metric =
  try
    match metric.Telemetry.metric_type with
    | Telemetry.Counter r ->
        let value = !r in  (* Safe since we're accessing from cache snapshot *)
        let text = Printf.sprintf "%d" value in
        let widget = if value > 1000 then Ui_components.create_error_status text
                     else if value > 100 then Ui_components.create_warning_status text
                     else Ui_components.create_success_status text in
        (text, widget)
    | Telemetry.SlidingCounter sliding ->
        let value = !(sliding.Telemetry.total_count) in  (* Safe since we're accessing from cache snapshot *)
        (* Validate the value to prevent display issues *)
        let value = if value < 0 || value > 1_000_000_000 then 0 else value in
        let text = Printf.sprintf "%d" value in
        let widget = if value > 1000000 then Ui_components.create_error_status text
                     else if value > 100000 then Ui_components.create_warning_status text
                     else Ui_components.create_success_status text in
        (text, widget)
    | Telemetry.Gauge r ->
        let value = !r in  (* Safe since we're accessing from cache snapshot *)
        let text = Printf.sprintf "%.1f" value in
        let widget = if value > 100.0 then Ui_components.create_error_status text
                     else if value > 10.0 then Ui_components.create_warning_status text
                     else Ui_components.create_success_status text in
        (text, widget)
    | Telemetry.Histogram _ ->
        let (mean, p50, p95, p99, count) = Telemetry.histogram_stats metric in  (* Safe since we're accessing from cache snapshot *)
        let text = if count > 0 then
          (* More compact histogram format to prevent truncation *)
          Printf.sprintf "%d | avg=%.1fµs p50=%.1f p95=%.1f p99=%.1f"
             count
             (mean *. 1_000_000.0)
             (p50 *. 1_000_000.0)
             (p95 *. 1_000_000.0)
             (p99 *. 1_000_000.0)
        else "no samples" in
        let widget = (* Special coloring for domain_cycle_duration_seconds *)
                     if metric.Telemetry.name = "domain_cycle_duration_seconds" then
                       if mean >= 0.0001 then Ui_components.create_error_status text  (* >= 100µs *)
                       else if mean >= 0.00002 then Ui_components.create_warning_status text  (* >= 20µs *)
                       else Ui_components.create_success_status text  (* < 20µs *)
                     else
                       (* Color based on average runtime for all other histogram metrics *)
                       if mean >= 0.0001 then Ui_components.create_error_status text  (* >= 100µs *)
                       else if mean >= 0.00001 then Ui_components.create_warning_status text  (* >= 10µs *)
                       else Ui_components.create_success_status text  (* < 10µs *) in
        (text, widget)
  with exn ->
    Logging.error_f ~section:"telemetry_view" "Exception formatting metric %s: %s" metric.Telemetry.name (Printexc.to_string exn);
    ("ERROR", Ui_components.create_error_status "ERROR")

(** Create a metric display row *)
let metric_row metric _is_expanded _snapshot =
  try
    let name = metric.Telemetry.name in

    (* Skip the generic domain_cycles counter when it's 0 and has no labels *)
    match metric.Telemetry.metric_type, metric.Telemetry.name, metric.Telemetry.labels with
    | Telemetry.Counter r, "domain_cycles", [] when !r = 0 ->
        empty  (* Don't display the generic domain_cycles when it's 0 *)
    | _ ->
        let label_str =
          if metric.Telemetry.labels = [] then ""
          else " (" ^ (metric.Telemetry.labels |> List.map (fun (k, v) -> k ^ "=" ^ v) |> String.concat ", ") ^ ")"
        in

        let name_widget = Ui_components.create_text (name ^ ":") in
        let label_widget = Ui_components.create_dimmed_text label_str in

        (* Add special handling for domain_cycles rate *)
        match metric.Telemetry.metric_type, metric.Telemetry.name with
        | Telemetry.Counter _, "domain_cycles" ->
            let rate = Telemetry.get_counter_rate metric in
            let rate = if rate < 0.0 || rate > 1_000_000.0 then 0.0 else rate in  (* Validate rate *)
            let rate_text = Printf.sprintf "%.0f cycles/sec" rate in
            let rate_widget = if rate >= 100000.0 then Ui_components.create_success_status rate_text
                             else if rate >= 50000.0 then Ui_components.create_warning_status rate_text
                             else Ui_components.create_error_status rate_text in
            hcat [name_widget; Ui_components.create_text " "; rate_widget; label_widget]
        | Telemetry.SlidingCounter _, "domain_cycles" ->
            (* Use rate tracker samples for more stable rate calculation *)
            let rate = Telemetry.get_counter_rate metric in
            let rate = if rate < 0.0 || rate > 1_000_000.0 then 0.0 else rate in  (* Validate rate *)
            let rate_text = Printf.sprintf "%.0f cycles/sec" rate in
            let rate_widget = if rate >= 100000.0 then Ui_components.create_success_status rate_text
                             else if rate >= 50000.0 then Ui_components.create_warning_status rate_text
                             else Ui_components.create_error_status rate_text in
            hcat [name_widget; Ui_components.create_text " "; rate_widget; label_widget]
        | _ ->
            (* For other metrics, use the formatted value component *)
            let (_, value_widget) = format_metric_value_ui metric in
            hcat [name_widget; Ui_components.create_text " "; value_widget; label_widget]
  with exn ->
    Logging.error_f ~section:"telemetry_view" "Exception creating metric row for %s: %s" metric.Telemetry.name (Printexc.to_string exn);
Ui_components.create_error_status (Printf.sprintf "ERROR: %s" metric.Telemetry.name)

(** Create category header *)
let category_header category_name metric_count selected =
  let title = Printf.sprintf "[%s] (%d metrics)" category_name metric_count in
  if selected then Ui_components.create_highlighted_status title
  else Ui_components.create_label title

(** Calculate minimum width required for telemetry view content *)
let calculate_telemetry_view_min_width snapshot =
  let rec calculate_metric_widths metrics max_name_len max_value_len =
    match metrics with
    | [] -> (max_name_len, max_value_len)
    | metric :: rest ->
        let name_len = String.length metric.Telemetry.name in
        let (value_str, _) = format_metric_value_ui metric in
        let value_len = String.length value_str in

        (* Account for labels - add some padding for " (" and ") " *)
        let label_len = match metric.Telemetry.labels with
          | [] -> 0
          | labels -> 2 + String.length (String.concat ", " (List.map (fun (k, v) -> k ^ "=" ^ v) labels)) + 2
        in

        let new_max_name = max max_name_len (name_len + label_len + 1) in  (* +1 for ":" *)
        let new_max_value = max max_value_len value_len in

        calculate_metric_widths rest new_max_name new_max_value
  in

  let rec calculate_category_widths categories max_width =
    match categories with
    | [] -> max_width
    | (cat_name, metrics) :: rest ->
        let cat_header_len = String.length (Printf.sprintf "[%s] (%d metrics)" cat_name (List.length metrics)) in
        let (name_width, value_width) = calculate_metric_widths metrics 0 0 in
        let indented_metric_width = name_width + value_width + 4 in  (* +2 for indentation "  ", +2 for spacing *)
        let category_max_width = max cat_header_len indented_metric_width in
        calculate_category_widths rest (max max_width category_max_width)
  in

  (* Add some padding for borders and readability *)
  max 40 (calculate_category_widths snapshot.categories 0 + 4)

(** Create telemetry view widget with size constraints *)
let telemetry_view (snapshot : Ui_types.telemetry_snapshot) state ~available_width ~available_height ~is_constrained_layout =
  let start_time = Unix.time () in
  try
    Logging.debug_f ~section:"telemetry_view" "Rendering telemetry view with %d categories at %.3f"
      (List.length snapshot.categories) start_time;

    (* Determine screen size for adaptive display *)
    let screen_size = Ui_types.classify_screen_size available_width available_height in

    match screen_size with
    | Mobile ->
        (* Ultra-compact mobile layout: maximum 5 lines *)
        let header_str = Printf.sprintf "Upd:%02d:%02d"
          (Unix.localtime (Unix.time ())).Unix.tm_hour
          (Unix.localtime (Unix.time ())).Unix.tm_min in
        let header = string ~attr:Notty.A.(fg white) (Ui_types.truncate_exact (available_width - 4) header_str) in

        (* Get memory pressure for mobile header *)
        let mem_pressure = try
          let mem_med_metric = List.find_opt (fun m -> m.Telemetry.name = "telemetry_memory_pressure_medium") snapshot.metrics in
          let mem_high_metric = List.find_opt (fun m -> m.Telemetry.name = "telemetry_memory_pressure_high") snapshot.metrics in

          let get_gauge_value metric_opt =
            match metric_opt with
            | Some m -> (match m.Telemetry.metric_type with
                         | Telemetry.Gauge r -> !r
                         | _ -> 0.0)
            | None -> 0.0
          in
          let high_val = get_gauge_value mem_high_metric in
          let med_val = get_gauge_value mem_med_metric in

          if high_val > 0.5 then "HIGH"
          else if med_val > 0.5 then "MED"
          else "LOW"
        with _ -> "UNK" in

        let mem_line = Printf.sprintf "Mem:%s" mem_pressure in
        let mem_display = string ~attr:Notty.A.(fg white) (Ui_types.truncate_exact (available_width - 4) mem_line) in

        (* Show only top 1-2 categories with 1-2 most critical metrics each *)
        let top_categories = match available_height with
          | h when h <= 3 -> []  (* No space for categories *)
          | h when h <= 4 -> take_first 1 snapshot.categories  (* 1 category max *)
          | _ -> take_first 2 snapshot.categories  (* 2 categories max *)
        in

        (* Format categories and their top metrics in single lines *)
        let category_lines = List.flatten (List.map (fun (cat_name, metrics) ->
          let cat_short = Ui_types.truncate_tiny cat_name in

          (* Get top 1-2 most critical metrics *)
          let top_metrics = take_first (if available_height >= 5 then 2 else 1) metrics in

          List.map (fun metric ->
            let (value_str, _) = format_metric_value_ui metric in
            let metric_name = Ui_types.truncate_tiny metric.Telemetry.name in
            let line = Printf.sprintf "[%s] %s:%s" cat_short metric_name value_str in
            string ~attr:Notty.A.(fg white) (Ui_types.truncate_exact (available_width - 4) line)
          ) top_metrics
        ) top_categories) in

        (* Combine all lines *)
        let all_lines = header :: mem_display :: category_lines in
        vcat all_lines

    | Small | Medium | Large ->
        (* Existing layout for larger screens *)
        (* Header *)
        let updated_attr = Notty.A.(fg (gray 2)) in  (* Dim gray for secondary info *)
        let tm = Unix.localtime (Unix.time ()) in
        let updated = string ~attr:updated_attr (Printf.sprintf "Updated: %02d:%02d:%02d" tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec) in

        (* Add memory pressure indicator *)
        let memory_pressure_info =
          try
            let mem_low_metric = List.find_opt (fun m -> m.Telemetry.name = "telemetry_memory_pressure_low") snapshot.metrics in
            let mem_med_metric = List.find_opt (fun m -> m.Telemetry.name = "telemetry_memory_pressure_medium") snapshot.metrics in
            let mem_high_metric = List.find_opt (fun m -> m.Telemetry.name = "telemetry_memory_pressure_high") snapshot.metrics in

            let (pressure_text, pressure_color) =
              let get_gauge_value metric_opt =
                match metric_opt with
                | Some m -> (match m.Telemetry.metric_type with
                             | Telemetry.Gauge r -> !r
                             | _ -> 0.0)
                | None -> 0.0
              in
              let high_val = get_gauge_value mem_high_metric in
              let med_val = get_gauge_value mem_med_metric in
              let low_val = get_gauge_value mem_low_metric in

              if high_val > 0.5 then ("HIGH", Notty.A.(fg red ++ st bold))
              else if med_val > 0.5 then ("MED", Notty.A.(fg yellow ++ st bold))
              else if low_val > 0.5 then ("LOW", Notty.A.(fg green ++ st bold))
              else ("UNK", Notty.A.(fg (gray 2)))
            in
            let mem_attr = Notty.A.(fg white ++ st bold) in
            let mem_label = string ~attr:mem_attr "Mem:" in
            let pressure_widget = string ~attr:pressure_color pressure_text in
            hcat [mem_label; string ~attr:Notty.A.empty " "; pressure_widget]
          with _ -> string ~attr:Notty.A.(fg (gray 2)) "Mem:UNK"
        in

        let header = hcat [updated; string ~attr:Notty.A.empty " | "; memory_pressure_info] in

    (* Calculate adaptive limits based on available space *)
    let screen_size = Ui_types.classify_screen_size available_width available_height in
    let reserved_lines = if is_constrained_layout then 2 else 4 in (* Reserve less space in compact mode *)
    let max_categories = Ui_types.max_visible_categories screen_size available_height reserved_lines in (* Reserve space for header/footer *)

    (* Limit categories to fit available space *)
    let display_categories = if List.length snapshot.categories > max_categories then
      let rec take_first n acc = function
        | [] -> List.rev acc
        | _ when n <= 0 -> List.rev acc
        | h::t -> take_first (n-1) (h::acc) t
      in take_first max_categories [] snapshot.categories
    else snapshot.categories in

    (* Categories list - limit processing to prevent hangs *)
    let categories_widgets =
      display_categories
      |> List.mapi (fun i (category_name, metrics) ->
           (* Check if we're taking too long *)
           let current_time = Unix.time () in
           if current_time -. start_time > 0.5 then begin
             Logging.debug_f ~section:"telemetry_view" "Rendering timeout after %.3f seconds, category %s (%d metrics)"
               (current_time -. start_time) category_name (List.length metrics);
             let error_attr = Notty.A.(fg yellow ++ st bold) in
             string ~attr:error_attr (Printf.sprintf "TIMEOUT: %s (%d metrics)" category_name (List.length metrics))
           end else
             try
               let selected = i = state.selected_category in
               let header = category_header category_name (List.length metrics) selected in
               let expanded = state.expanded_category = Some category_name in

               if expanded && metrics <> [] then
                 (* Show all metrics in expanded categories - no limiting *)
                 let display_metrics = metrics in
                 let metric_rows = List.map (fun metric -> metric_row metric false snapshot) display_metrics in
                 let indented_rows = List.map (fun row -> hcat [string ~attr:Notty.A.empty "  "; row]) metric_rows in
                 vcat (header :: indented_rows)
               else
                 header
             with exn ->
               Logging.error_f ~section:"telemetry_view" "Exception rendering category %s: %s" category_name (Printexc.to_string exn);
               let error_attr = Notty.A.(fg red ++ st bold) in
               string ~attr:error_attr (Printf.sprintf "ERROR: %s" category_name)
         )
    in

    let categories_list = vcat categories_widgets in

    (* Footer with controls *)
    let footer = string ~attr:Notty.A.empty "↑↓: Navigate categories | Space: Expand/collapse | q: Quit" in

    (* Combine everything with conditional spacing *)
    let render_time = Unix.time () -. start_time in
    Logging.debug_f ~section:"telemetry_view" "Telemetry view rendered successfully in %.3f seconds" render_time;
    let spacing = if is_constrained_layout then [] else [string ~attr:Notty.A.empty ""] in
    vcat ([
      header;
    ] @ spacing @ [
      categories_list;
    ] @ spacing @ [
      footer;
    ])
  with exn ->
    let render_time = Unix.time () -. start_time in
    Logging.error_f ~section:"telemetry_view" "Exception in telemetry_view after %.3f seconds: %s" render_time (Printexc.to_string exn);
    let error_attr = Notty.A.(fg red ++ st bold) in
    vcat [
      string ~attr:error_attr "TELEMETRY VIEW ERROR";
      string ~attr:Notty.A.(fg (gray 2)) (Printf.sprintf "%.3fs: %s" render_time (Printexc.to_string exn));
      string ~attr:Notty.A.empty "↑↓: Navigate categories | Space: Expand/collapse | q: Quit";
    ]

(** Handle keyboard input *)
let handle_key state (snapshot : Ui_types.telemetry_snapshot) = function
  | `ASCII 'q', [] -> `Quit
  | `ASCII ' ', [] ->
      let (category_name, _) = List.nth snapshot.categories state.selected_category in
      let currently_expanded = state.expanded_category = Some category_name in
      let new_expanded_category = if currently_expanded then None else Some category_name in
      `Continue { state with expanded_category = new_expanded_category }
  | `Arrow `Up, [] ->
      let new_selected = max 0 (state.selected_category - 1) in
      `Continue { state with selected_category = new_selected }
  | `Arrow `Down, [] ->
      let max_index = List.length snapshot.categories - 1 in
      let new_selected = min max_index (state.selected_category + 1) in
      `Continue { state with selected_category = new_selected }
  | _ -> `Continue state

(** Create reactive telemetry view with shared snapshot variable *)
let make_telemetry_view snapshot_var viewport_var ~is_constrained_layout =
  let state_var = Lwd.var initial_state in

  (* Reactive UI that depends on snapshot, state, and viewport changes *)
  let ui = Lwd.map2
    (Lwd.map2
      (Lwd.map2 (Lwd.get snapshot_var) (Lwd.get state_var) ~f:(fun snapshot state -> (snapshot, state)))
      (Lwd.get viewport_var)
      ~f:(fun (snapshot, state) (available_width, available_height) -> (snapshot, state, available_width, available_height)))
    is_constrained_layout
    ~f:(fun (snapshot, state, available_width, available_height) constrained ->
      telemetry_view snapshot state ~available_width ~available_height ~is_constrained_layout:constrained
    ) in

  (* Event handling *)
  let handle_event ev =
    match ev with
    | `Key key ->
        let current_state = Lwd.peek state_var in
        let current_snapshot = Lwd.peek snapshot_var in
        begin match handle_key current_state current_snapshot key with
        | `Quit -> `Quit
        | `Continue new_state ->
            Lwd.set state_var new_state;
            `Continue
        end
    | _ -> `Continue
  in

  (ui, handle_event)

(** Run the telemetry view *)
let run () =
  Telemetry_cache.init ();

  (* Create our own snapshot and viewport variables for standalone execution *)
  let empty_snapshot = {
    uptime = 0.0;
    metrics = [];
    categories = [];
    timestamp = 0.0;
  } in
  let snapshot_var = Lwd.var empty_snapshot in
  let viewport_var = Lwd.var (80, 24) in
  let constrained_var = Lwd.var false in

  let (ui, handle_event) = make_telemetry_view snapshot_var viewport_var ~is_constrained_layout:(Lwd.get constrained_var) in

  (* Create quit promise *)
  let quit_promise, quit_resolver = Lwt.wait () in

  (* Create custom event stream that calls our handler *)
  let term = Notty_lwt.Term.create () in
  let event_stream = Notty_lwt.Term.events term in

  let custom_event_stream =
    Lwt_stream.map (fun event ->
      (* Call our event handler *)
      Lwt.async (fun () ->
        match handle_event event with
        | `Quit -> Lwt.wakeup quit_resolver (); Lwt.return_unit
        | `Continue -> Lwt.return_unit
      );
      event
    ) event_stream
  in

  (* Run the UI using render directly *)
  let size = Notty_lwt.Term.size term in
  let image_stream = Nottui_lwt.render ~quit:quit_promise ~size custom_event_stream ui in

  Lwt_main.run (
    Lwt.finalize
      (fun () -> Lwt_stream.iter_s (Notty_lwt.Term.image term) image_stream)
      (fun () -> Notty_lwt.Term.release term)
  )