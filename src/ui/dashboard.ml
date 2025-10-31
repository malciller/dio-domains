(** Main Dashboard Application

    Orchestrates the different dashboard views and provides navigation
    between telemetry, trading, and other system information displays.
*)

open Ui_types
open Nottui_widgets
open Nottui.Ui
open Dio_ui_balance.Balance_cache



type panel_focus =
  | SystemPanel
  | BalancesPanel
  | TelemetryPanel
  | LogsPanel
  | NoFocus

type dashboard_state = {
  focused_panel: panel_focus;
  is_loading: bool;
  open_modules: panel_focus list;        (* Modules currently displayed *)
  collapsed_modules: panel_focus list;  (* Modules in hamburger menu *)
  viewport_constraints: (int * int);    (* (width, height) available for modules *)
  single_module_mode: bool;             (* True when only one module can be open *)
}

(** Calculate panel dimensions based on available space and layout *)
let calculate_panel_dimensions state panel_count =
  (* Handle empty panel count to prevent division by zero *)
  if panel_count = 0 then (
    let available_w, _ = state.viewport_constraints in
    let default_width = max Ui_types.min_panel_width available_w in
    let default_height = max Ui_types.min_panel_height Ui_types.standard_panel_height in
    (default_width, default_height, default_width, default_height)
  ) else (
    let available_w, available_h = state.viewport_constraints in
    let screen_size = Ui_types.classify_screen_size available_w available_h in

    (* Account for panel borders (3 lines total) *)
    let border_overhead = 3 in

    if state.single_module_mode then
    (* Single module mode: use full available space minus borders *)
    let panel_height = max Ui_types.min_panel_height (available_h - border_overhead) in
    let panel_width = max Ui_types.min_panel_width available_w in
    (panel_width, panel_height, panel_width, panel_height)  (* Return 4-tuple for consistency *)
  else
    (* Multi-module mode: calculate based on screen size and layout *)
    match screen_size with
    | Ui_types.Mobile ->
        (* Stack panels vertically - divide height equally *)
        let panels_per_column = panel_count in
        let panel_height = max Ui_types.min_panel_height ((available_h / panels_per_column) - border_overhead) in
        let panel_width = max Ui_types.min_panel_width available_w in
        (panel_width, panel_height, panel_width, panel_height)
    | Ui_types.Small | Ui_types.Medium ->
        (* Two column layout *)
        let panels_per_column = (panel_count + 1) / 2 in  (* Ceiling division *)
        let panel_height = max Ui_types.min_panel_height ((available_h / panels_per_column) - border_overhead) in
        let panel_width = max Ui_types.min_panel_width (available_w / 2) in
        (panel_width, panel_height, panel_width, panel_height)
    | Ui_types.Large ->
        (* Special handling for Large mode with logs in bottom row *)
        if List.mem LogsPanel state.open_modules && List.length state.open_modules > 1 then
          (* Logs in bottom row - need to handle differently for logs vs non-logs *)
          let non_logs_count = List.length (List.filter (fun p -> p <> LogsPanel) state.open_modules) in
          if non_logs_count > 0 then
            (* Top row height for non-logs panels *)
            let top_row_height = max Ui_types.min_panel_height ((available_h / 2) - border_overhead) in
            let top_row_width = max Ui_types.min_panel_width (available_w / non_logs_count) in
            (* Bottom row height for logs panel *)
            let bottom_row_height = max Ui_types.min_panel_height ((available_h / 2) - border_overhead) in
            let bottom_row_width = max Ui_types.min_panel_width available_w in
            (top_row_width, top_row_height, bottom_row_width, bottom_row_height)
          else
            (* Only logs panel - use full width *)
            let panel_height = max Ui_types.min_panel_height (available_h - border_overhead) in
            let panel_width = max Ui_types.min_panel_width available_w in
            (panel_width, panel_height, panel_width, panel_height)
        else
          (* No logs or logs is the only panel - standard layout *)
          let panels_per_row = min panel_count 3 in  (* Max 3 panels per row *)
          let rows = (panel_count + panels_per_row - 1) / panels_per_row in
          let panel_height = max Ui_types.min_panel_height ((available_h / rows) - border_overhead) in
          let panel_width = max Ui_types.min_panel_width (available_w / panels_per_row) in
          (panel_width, panel_height, panel_width, panel_height)
    )

(** Calculate viewport dimensions for panel content (accounting for borders) *)
let calculate_panel_viewport panel_type state panel_count =
  let (top_w, top_h, bottom_w, bottom_h) = calculate_panel_dimensions state panel_count in

  (* Panel border overhead: 2 lines (top + bottom borders) and 4 chars (borders + padding) *)
  let border_height_overhead = 2 in
  let border_width_overhead = 4 in

  (* Get panel-specific dimensions *)
  let (panel_width, panel_height) = match panel_type with
    | LogsPanel -> (bottom_w, bottom_h)
    | _ -> (top_w, top_h)
  in

  (* Calculate content viewport dimensions *)
  let content_width = max 1 (panel_width - border_width_overhead) in
  let content_height = max 1 (panel_height - border_height_overhead) in

  (content_width, content_height)

(** Create bordered panel with title that dynamically scales and respects size constraints *)
let create_panel ~title ~content ~focused ?max_width ?max_height () =
Dio_ui_shared.Ui_components.create_bordered_panel ~title ~focused ?max_width ?max_height content

(** Create top status bar with system info - adaptive to screen size *)
let create_status_bar system_stats is_loading screen_size =
  (* Adaptive title based on screen size *)
  let title_text = match screen_size with
    | Ui_types.Mobile -> if is_loading then "DIO (LOAD)" else "DIO"
    | Ui_types.Small -> if is_loading then "DIO (LOAD)" else "DIO"
    | Ui_types.Medium -> if is_loading then "DIO TRADING (LOAD)" else "DIO TRADING"
    | Ui_types.Large -> if is_loading then " DIO TRADING TERMINAL (LOADING...) " else " DIO TRADING TERMINAL "
  in
  let title = if is_loading then Dio_ui_shared.Ui_components.create_warning_status title_text
              else Dio_ui_shared.Ui_components.create_primary_status title_text in

  (* Adaptive uptime display *)
  let uptime_str = match screen_size with
    | Ui_types.Mobile ->
        let uptime = Unix.time () -. !Telemetry.start_time in
        let hours = int_of_float (uptime /. 3600.0) in
        Printf.sprintf "%dh" hours
    | Ui_types.Small ->
        let uptime = Unix.time () -. !Telemetry.start_time in
        let hours = int_of_float (uptime /. 3600.0) mod 24 in
        let mins = int_of_float (uptime /. 60.0) mod 60 in
        Printf.sprintf "%02d:%02d" hours mins
    | Ui_types.Medium | Ui_types.Large ->
        let uptime = Unix.time () -. !Telemetry.start_time in
        let days = int_of_float (uptime /. 86400.0) in
        let hours = int_of_float (uptime /. 3600.0) mod 24 in
        let mins = int_of_float (uptime /. 60.0) mod 60 in
        if days > 0 then Printf.sprintf "%dd %02d:%02d" days hours mins
        else Printf.sprintf "%02d:%02d" hours mins
  in
  let uptime = Dio_ui_shared.Ui_components.create_primary_status ("UP:" ^ uptime_str) in

  (* Adaptive CPU display *)
  let cpu_str = Printf.sprintf "%.0f%%" system_stats.cpu_usage in
  let cpu_element = if system_stats.cpu_usage > 90.0 then Dio_ui_shared.Ui_components.create_error_status ("CPU:" ^ cpu_str)
                    else if system_stats.cpu_usage > 70.0 then Dio_ui_shared.Ui_components.create_warning_status ("CPU:" ^ cpu_str)
                    else Dio_ui_shared.Ui_components.create_success_status ("CPU:" ^ cpu_str) in

  (* Adaptive memory display - hide on very small screens *)
  let mem_element = match screen_size with
    | Ui_types.Mobile -> Dio_ui_shared.Ui_components.create_secondary_status ""  (* No memory info on mobile *)
    | Ui_types.Small | Ui_types.Medium | Ui_types.Large ->
        let mem_usage_pct = (float_of_int system_stats.memory_used /. float_of_int system_stats.memory_total) *. 100.0 in
        let mem_str = match screen_size with
          | Ui_types.Small -> Printf.sprintf "%.0f%%" mem_usage_pct
          | Ui_types.Medium -> Printf.sprintf "%dM" (system_stats.memory_used / 1024)
          | Ui_types.Large -> Printf.sprintf "%d/%d MB" (system_stats.memory_used / 1024) (system_stats.memory_total / 1024)
          | _ -> ""
        in
        let mem_text = "MEM:" ^ mem_str in
        if mem_usage_pct > 90.0 then Dio_ui_shared.Ui_components.create_error_status mem_text
        else if mem_usage_pct > 70.0 then Dio_ui_shared.Ui_components.create_warning_status mem_text
        else Dio_ui_shared.Ui_components.create_success_status mem_text
  in

  (* Adaptive timestamp - simplified on small screens *)
  let timestamp_str = match screen_size with
    | Ui_types.Mobile ->
        let tm = Unix.localtime (Unix.time ()) in
        Printf.sprintf "%02d:%02d" tm.Unix.tm_hour tm.Unix.tm_min
    | Ui_types.Small | Ui_types.Medium | Ui_types.Large ->
        let tm = Unix.localtime (Unix.time ()) in
        Printf.sprintf "%02d:%02d:%02d" tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec
  in
  let timestamp = Dio_ui_shared.Ui_components.create_primary_status timestamp_str in

  (* Combine elements based on screen size using the new status bar component *)
  let elements = match screen_size with
    | Ui_types.Mobile -> [title; uptime; cpu_element]
    | Ui_types.Small -> [title; uptime; cpu_element; mem_element]
    | Ui_types.Medium -> [title; uptime; cpu_element; mem_element; timestamp]
    | Ui_types.Large -> [title; uptime; cpu_element; mem_element; timestamp]
  in
Dio_ui_shared.Ui_components.create_status_bar elements

(** Create bottom status bar with navigation hints - adaptive to screen size *)
let create_bottom_bar focused_panel screen_size =
  (* Adaptive panel hints based on screen size *)

  let (s_hint, b_hint, t_hint, l_hint, refresh_hint, quit_hint) = match screen_size with
    | Ui_types.Mobile ->
        (* Minimal hints for mobile - just single letters *)
        let s = Dio_ui_shared.Ui_components.create_panel_hint SystemPanel "S" (focused_panel = SystemPanel) in
        let b = Dio_ui_shared.Ui_components.create_panel_hint BalancesPanel "B" (focused_panel = BalancesPanel) in
        let t = Dio_ui_shared.Ui_components.create_panel_hint TelemetryPanel "T" (focused_panel = TelemetryPanel) in
        let l = Dio_ui_shared.Ui_components.create_panel_hint LogsPanel "L" (focused_panel = LogsPanel) in
        let r = if focused_panel = BalancesPanel then Dio_ui_shared.Ui_components.create_primary_status "R" else Dio_ui_shared.Ui_components.create_secondary_status "" in
        let q = Dio_ui_shared.Ui_components.create_primary_status "Q" in
        (s, b, t, l, r, q)
    | Ui_types.Small ->
        (* Short hints for small screens *)
        let s = Dio_ui_shared.Ui_components.create_panel_hint SystemPanel "S:Sys" (focused_panel = SystemPanel) in
        let b = Dio_ui_shared.Ui_components.create_panel_hint BalancesPanel "B:Bal" (focused_panel = BalancesPanel) in
        let t = Dio_ui_shared.Ui_components.create_panel_hint TelemetryPanel "T:Tel" (focused_panel = TelemetryPanel) in
        let l = Dio_ui_shared.Ui_components.create_panel_hint LogsPanel "L:Log" (focused_panel = LogsPanel) in
        let r = if focused_panel = BalancesPanel then Dio_ui_shared.Ui_components.create_primary_status "R:Ref" else Dio_ui_shared.Ui_components.create_secondary_status "" in
        let q = Dio_ui_shared.Ui_components.create_primary_status "Q:Quit" in
        (s, b, t, l, r, q)
    | Ui_types.Medium | Ui_types.Large ->
        (* Full hints for larger screens *)
        let s = Dio_ui_shared.Ui_components.create_panel_hint SystemPanel " S:SYSTEM " (focused_panel = SystemPanel) in
        let b = Dio_ui_shared.Ui_components.create_panel_hint BalancesPanel " B:BALANCES " (focused_panel = BalancesPanel) in
        let t = Dio_ui_shared.Ui_components.create_panel_hint TelemetryPanel " T:TELEMETRY " (focused_panel = TelemetryPanel) in
        let l = Dio_ui_shared.Ui_components.create_panel_hint LogsPanel " L:LOGS " (focused_panel = LogsPanel) in
        let r = if focused_panel = BalancesPanel then Dio_ui_shared.Ui_components.create_primary_status " R:REFRESH " else Dio_ui_shared.Ui_components.create_secondary_status "" in
        let q = Dio_ui_shared.Ui_components.create_primary_status " Q:QUIT " in
        (s, b, t, l, r, q)
  in

  (* Combine elements using the status bar component *)
  let elements = [s_hint; b_hint; t_hint; l_hint] in
  let elements = if refresh_hint <> Dio_ui_shared.Ui_components.create_secondary_status "" then elements @ [refresh_hint] else elements in
  let elements = elements @ [quit_hint] in
Dio_ui_shared.Ui_components.create_status_bar elements


(** Handle module open/close toggle behavior *)
let handle_module_toggle _state = function
  | `ASCII 's', [] -> SystemPanel
  | `ASCII 'b', [] -> BalancesPanel
  | `ASCII 't', [] -> TelemetryPanel
  | `ASCII 'l', [] -> LogsPanel
  | _ -> NoFocus

(** Update dashboard state for module open/close *)
let update_module_state state target_panel =
  let is_open = List.mem target_panel state.open_modules in
  let is_focused = state.focused_panel = target_panel in

  if is_open && is_focused then
    (* Module is open and focused - close it *)
    let new_open = List.filter (fun p -> p <> target_panel) state.open_modules in
    let new_collapsed = target_panel :: state.collapsed_modules in
    { state with
      open_modules = new_open;
      collapsed_modules = new_collapsed;
      focused_panel = if new_open = [] then NoFocus else List.hd new_open;
    }
  else if is_open then
    (* Module is open but not focused - just change focus *)
    { state with focused_panel = target_panel }
  else
    (* Module is closed - open it *)
    let new_collapsed = List.filter (fun p -> p <> target_panel) state.collapsed_modules in
    let new_open = state.open_modules @ [target_panel] in
    { state with
      open_modules = new_open;
      collapsed_modules = new_collapsed;
      focused_panel = target_panel;
    }

(** Create hamburger menu showing collapsed modules *)
let create_hamburger_menu collapsed_modules screen_size =
  if collapsed_modules = [] then
    empty  (* No hamburger menu if no modules are collapsed *)
  else
    let hamburger_icon = string ~attr:Notty.A.(fg cyan ++ st bold) "[☰]" in
    let module_names = List.map (function
      | SystemPanel -> "System"
      | BalancesPanel -> "Balances"
      | TelemetryPanel -> "Telemetry"
      | LogsPanel -> "Logs"
      | NoFocus -> "None"
    ) collapsed_modules in

    (* Create compact or full format based on screen size *)
    let menu_text = match screen_size with
      | Ui_types.Mobile ->
          (* Ultra-compact for mobile: [☰] S·B·T·L *)
          let compact_names = List.map (function
            | "System" -> "S" | "Balances" -> "B" | "Telemetry" -> "T" | "Logs" -> "L" | _ -> "?"
          ) module_names in
          String.concat "·" compact_names
      | Ui_types.Small ->
          (* Short names: [☰] Sys|Bal|Tel|Log *)
          let short_names = List.map (function
            | "System" -> "Sys" | "Balances" -> "Bal" | "Telemetry" -> "Tel" | "Logs" -> "Log" | x -> x
          ) module_names in
          String.concat "|" short_names
      | Ui_types.Medium | Ui_types.Large ->
          (* Full names: [☰] System | Balances | Telemetry | Logs *)
          String.concat " | " module_names
    in

    let menu_widget = string ~attr:Notty.A.(fg (gray 2)) menu_text in
    hcat [hamburger_icon; string ~attr:Notty.A.empty " "; menu_widget]


let make_dashboard () =

  (* Terminal size tracking *)
  let terminal_size_var = Lwd.var (80, 24) in  (* Default size *)

  (* Forward declaration of viewport variables and update function *)
  let system_viewport_var = Lwd.var (80, 24) in
  let balances_viewport_var = Lwd.var (80, 24) in
  let telemetry_viewport_var = Lwd.var (80, 24) in
  let logs_viewport_var = Lwd.var (80, 24) in

  (* Function to update panel-specific viewport variables based on current state *)
  let update_panel_viewports state =
    let panel_count = List.length state.open_modules in
    Lwd.set system_viewport_var (calculate_panel_viewport SystemPanel state panel_count);
    Lwd.set balances_viewport_var (calculate_panel_viewport BalancesPanel state panel_count);
    Lwd.set telemetry_viewport_var (calculate_panel_viewport TelemetryPanel state panel_count);
    Lwd.set logs_viewport_var (calculate_panel_viewport LogsPanel state panel_count);
  in

  (* Shared system stats and telemetry snapshot variables - start with empty defaults *)
  let empty_system_stats = {
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

  let empty_telemetry_snapshot = {
    uptime = 0.0;
    metrics = [];
    categories = [];
    timestamp = 0.0;
  } in

  let system_stats_var = Lwd.var empty_system_stats in
  let telemetry_snapshot_var = Lwd.var empty_telemetry_snapshot in
  let time_var = Lwd.var (Unix.time ()) in
  let balance_snapshot_var = Lwd.var ({
    Dio_ui_balance.Balance_cache.balances = [];
    open_orders = [];
    timestamp = 0.0;
  }, 0) in

  (* Start periodic time updates for uptime display - reduced frequency for performance *)
  Lwt.async (fun () ->
    let rec update_time () =
      let%lwt () = Lwt_unix.sleep 2.5 in
      Lwd.set time_var (Unix.time ());
      update_time ()
    in
    update_time ()
  );

  (* Initialize viewport constraints based on initial state *)
  let initial_terminal_size = Lwd.peek terminal_size_var in
  let initial_available_space = Ui_types.calculate_available_space
    (fst initial_terminal_size) (snd initial_terminal_size) 4 in  (* Start with all modules collapsed *)

  (* Create reactive viewport variables *)
  let viewport_var = Lwd.var initial_available_space in

  (* Dashboard state - starts with no focus and loading *)
  let state_var = Lwd.var {
    focused_panel = NoFocus;
    is_loading = true;
    open_modules = [];
    collapsed_modules = [SystemPanel; BalancesPanel; TelemetryPanel; LogsPanel];
    viewport_constraints = (80, 24);
    single_module_mode = false;
  } in

  (* Initialize viewport constraints based on initial state *)
  let initial_terminal_size = Lwd.peek terminal_size_var in
  let initial_state = Lwd.peek state_var in
  let (initial_available_w, initial_available_h) = Ui_types.calculate_available_space
    (fst initial_terminal_size) (snd initial_terminal_size) (List.length initial_state.collapsed_modules) in
  let initial_single_module_mode = not (Ui_types.can_support_multiple_modules initial_available_w initial_available_h) in
  Lwd.set state_var { initial_state with
    viewport_constraints = (initial_available_w, initial_available_h);
    single_module_mode = initial_single_module_mode;
  };

  (* Initialize panel viewports with current state *)
  update_panel_viewports (Lwd.peek state_var);

  (* Create reactive views that adapt to viewport changes *)
  let telemetry_ui, telemetry_handler = Dio_ui_telemetry.Telemetry_view.make_telemetry_view telemetry_snapshot_var telemetry_viewport_var in
  let system_ui, system_handler = Dio_ui_system.System_view.make_system_view system_stats_var telemetry_snapshot_var system_viewport_var in
  let balances_ui, balances_handler = Dio_ui_balance.Balance_view.make_balances_view balance_snapshot_var balances_viewport_var in
  let log_entries_var = Dio_ui_logs.Logs_cache.get_log_entries_var () in
  let logs_ui, logs_handler = Dio_ui_logs.Logs_view.make_logs_view log_entries_var logs_viewport_var in

  (* Create reactive status bar *)
  let status_bar_ui = Lwd.map2
    (Lwd.map2 (Lwd.get system_stats_var) (Lwd.get time_var) ~f:(fun ss t -> (ss, t)))
    (Lwd.map2 (Lwd.get state_var) (Lwd.get terminal_size_var) ~f:(fun state (w, h) ->
      (state, Ui_types.classify_screen_size w h)))
    ~f:(fun (system_stats, _) (state, screen_size) ->
      create_status_bar system_stats state.is_loading screen_size
    ) in

  (* Create reactive panels with content-based width allocation *)
  let system_panel_ui = Lwd.map2 (Lwd.get state_var) (Lwd.map2 system_ui (Lwd.get system_stats_var) ~f:(fun content stats -> (content, stats)))
    ~f:(fun state (system_content, system_stats) ->
      let panel_count = List.length state.open_modules in
      let (uniform_w, top_h, _, _) = calculate_panel_dimensions state panel_count in
      (* Calculate content-based width for system panel *)
      let content_width = Dio_ui_system.System_view.calculate_system_view_min_width system_stats in
      (* Use the larger of uniform allocation and content needs, but cap at uniform to prevent overflow *)
      let panel_width = min uniform_w (max content_width Ui_types.min_panel_width) in
      create_panel ~title:"SYSTEM" ~content:system_content ~focused:(state.focused_panel = SystemPanel) ~max_width:panel_width ~max_height:top_h ()
    ) in

  let balances_panel_ui = Lwd.map2 (Lwd.get state_var) (Lwd.map2 balances_ui (Lwd.map (Lwd.get balance_snapshot_var) ~f:(fun (snapshot, _) -> snapshot)) ~f:(fun content snapshot -> (content, snapshot)))
    ~f:(fun state (balances_content, balance_snapshot) ->
      let panel_count = List.length state.open_modules in
      let (uniform_w, top_h, _, _) = calculate_panel_dimensions state panel_count in
      (* Calculate content-based width for balances panel *)
      let content_width = Dio_ui_balance.Balance_view.calculate_balance_view_min_width balance_snapshot in
      (* Use the larger of uniform allocation and content needs, but cap at uniform to prevent overflow *)
      let panel_width = min uniform_w (max content_width Ui_types.min_panel_width) in
      create_panel ~title:"BALANCES" ~content:balances_content ~focused:(state.focused_panel = BalancesPanel) ~max_width:panel_width ~max_height:top_h ()
    ) in

  let telemetry_panel_ui = Lwd.map2 (Lwd.get state_var) (Lwd.map2 telemetry_ui (Lwd.get telemetry_snapshot_var) ~f:(fun content snapshot -> (content, snapshot)))
    ~f:(fun state (telemetry_content, telemetry_snapshot) ->
      let panel_count = List.length state.open_modules in
      let (uniform_w, top_h, _, _) = calculate_panel_dimensions state panel_count in
      (* Calculate content-based width for telemetry panel *)
      let content_width = Dio_ui_telemetry.Telemetry_view.calculate_telemetry_view_min_width telemetry_snapshot in
      (* Allow telemetry panel to expand significantly when content requires it *)
      (* User reports display space is available when all views are open *)
      let available_w, _ = state.viewport_constraints in
      let max_telemetry_width = if panel_count >= 3 then min content_width (available_w / 2) else min content_width (uniform_w * 3) in
      let panel_width = max uniform_w max_telemetry_width in
      create_panel ~title:"TELEMETRY" ~content:telemetry_content ~focused:(state.focused_panel = TelemetryPanel) ~max_width:panel_width ~max_height:top_h ()
    ) in

  let logs_panel_ui = Lwd.map2 (Lwd.get state_var) logs_ui
    ~f:(fun state logs_content ->
      let panel_count = List.length state.open_modules in
      let (_, _, bottom_w, bottom_h) = calculate_panel_dimensions state panel_count in
      create_panel ~title:"LOGS" ~content:logs_content ~focused:(state.focused_panel = LogsPanel) ~max_width:bottom_w ~max_height:bottom_h ()
    ) in

  (* Create reactive bottom bar *)
  let bottom_bar_ui = Lwd.map2 (Lwd.get state_var) (Lwd.get terminal_size_var) ~f:(fun state (w, h) ->
    create_bottom_bar state.focused_panel (Ui_types.classify_screen_size w h)
  ) in

  (* Create reactive hamburger menu *)
  let hamburger_ui = Lwd.map2 (Lwd.get state_var) (Lwd.get terminal_size_var) ~f:(fun state (w, h) ->
    create_hamburger_menu state.collapsed_modules (Ui_types.classify_screen_size w h)
  ) in

  (* Combine all panel UIs into a single reactive value to reduce nesting *)
  let combined_panels_ui = Lwd.map2
    (Lwd.map2 system_panel_ui balances_panel_ui ~f:(fun sp bp -> (sp, bp)))
    (Lwd.map2 telemetry_panel_ui logs_panel_ui ~f:(fun tp lp -> (tp, lp)))
    ~f:(fun (system_panel, balances_panel) (telemetry_panel, logs_panel) ->
      (system_panel, balances_panel, telemetry_panel, logs_panel)) in

  (* Create adaptive main panel layout based on dashboard state *)
  let main_panels_ui = Lwd.map2 (Lwd.get state_var) combined_panels_ui
    ~f:(fun state (system_panel, balances_panel, telemetry_panel, logs_panel) ->
      (* Filter panels to only show those in open_modules *)
      let get_panel panel_type =
        match panel_type with
        | SystemPanel -> system_panel
        | BalancesPanel -> balances_panel
        | TelemetryPanel -> telemetry_panel
        | LogsPanel -> logs_panel
        | NoFocus -> empty
      in

      let open_panels = List.map get_panel state.open_modules in

      if state.single_module_mode then
        (* Single module mode: show only the focused panel or first open panel *)
        match state.focused_panel with
        | NoFocus when open_panels <> [] -> List.hd open_panels
        | NoFocus -> empty
        | focused -> get_panel focused
      else
        (* Multi-module mode: use screen size based layouts *)
        let screen_size = Ui_types.classify_screen_size (fst state.viewport_constraints) (snd state.viewport_constraints) in
        match screen_size with
        | Ui_types.Mobile ->
            (* Stack open panels vertically on mobile *)
            vcat open_panels
        | Ui_types.Small ->
            (* Two columns layout for small screens *)
            let left_panels = List.filteri (fun i _ -> i mod 2 = 0) open_panels in
            let right_panels = List.filteri (fun i _ -> i mod 2 = 1) open_panels in
            let left_column = vcat left_panels in
            let right_column = vcat right_panels in
            hcat [left_column; right_column]
        | Ui_types.Medium ->
            (* Two columns layout for medium screens *)
            let left_panels = List.filteri (fun i _ -> i mod 2 = 0) open_panels in
            let right_panels = List.filteri (fun i _ -> i mod 2 = 1) open_panels in
            let left_column = vcat left_panels in
            let right_column = vcat right_panels in
            hcat [left_column; right_column]
        | Ui_types.Large ->
            (* Three panels on top, logs full-width below (if logs is open) *)
            let non_logs_panels = List.filter (fun p -> p <> LogsPanel) state.open_modules in
            let non_logs_ui = List.map get_panel non_logs_panels in
            let logs_ui = if List.mem LogsPanel state.open_modules then [logs_panel] else [] in
            let top_row = if non_logs_ui <> [] then hcat non_logs_ui else empty in
            let bottom_row = if logs_ui <> [] then List.hd logs_ui else empty in
            if top_row <> empty && bottom_row <> empty then
              vcat [top_row; bottom_row]
            else if top_row <> empty then
              top_row
            else if bottom_row <> empty then
              bottom_row
            else
              empty
    ) in

  (* Combine all UI elements *)
  let ui = Lwd.map2 status_bar_ui
    (Lwd.map2 hamburger_ui
      (Lwd.map2 main_panels_ui bottom_bar_ui ~f:(fun main_panels bottom_bar -> (main_panels, bottom_bar)))
      ~f:(fun hamburger (main_panels, bottom_bar) -> (hamburger, main_panels, bottom_bar)))
    ~f:(fun status_bar (hamburger, main_panels, bottom_bar) ->
      vcat [
        status_bar;
        string ~attr:Notty.A.(fg (gray 2)) "";  (* Separator line *)
        hamburger;  (* Hamburger menu for collapsed modules *)
        main_panels;
        string ~attr:Notty.A.(fg (gray 2)) "";  (* Separator line *)
        bottom_bar;
      ]
    ) in


  let handle_event ev =
    let state = Lwd.peek state_var in

    (* Ignore all events while loading to prevent freezes *)
    if state.is_loading then `Continue
    else
      match ev with
      | `Key key ->
          (* Handle letter keys for panel navigation *)
          let is_navigation_key = match key with
            | `ASCII ('s' | 'b' | 't' | 'l' | 'q'), [] -> true
            | _ -> false
          in

          if is_navigation_key then (
            match key with
            | `ASCII 'q', [] -> `Quit
            | `ASCII 's', [] | `ASCII 'b', [] | `ASCII 't', [] | `ASCII 'l', [] ->
                let target_panel = handle_module_toggle state key in
                if target_panel <> NoFocus then (
                  let new_state = update_module_state state target_panel in
                  Lwd.set state_var new_state;
                  update_panel_viewports new_state;
                );
                `Continue
            | _ -> `Continue
          ) else (
            (* Handle dashboard-level keys for focused panels *)
            match ev with
            | `Key (`ASCII 'r', []) when state.focused_panel = BalancesPanel ->
                (* Manual refresh is now handled by the cache's polling loop *)
                `Continue
            | _ ->
                (* Pass through to focused panel handler *)
                match state.focused_panel with
                | SystemPanel -> system_handler ev
                | BalancesPanel -> balances_handler ev
                | TelemetryPanel -> telemetry_handler ev
                | LogsPanel -> logs_handler ev
                | NoFocus -> `Continue
          )
      | _ -> `Continue
  in

  (ui, handle_event, telemetry_snapshot_var, system_stats_var, balance_snapshot_var, state_var, terminal_size_var, viewport_var, update_panel_viewports)

(** Verify that all required caches are initialized *)
let verify_cache_initialization () =
  let telemetry_initialized = try Dio_ui_telemetry.Telemetry_cache.init (); true with _ -> false in
  let system_initialized = try Dio_ui_system.System_cache.init (); true with _ -> false in
  let balance_initialized = try Dio_ui_balance.Balance_cache.init (); true with _ -> false in

  if not telemetry_initialized then
    Logging.warn ~section:"dashboard" "Telemetry cache initialization failed";
  if not system_initialized then
    Logging.warn ~section:"dashboard" "System cache initialization failed";
  if not balance_initialized then
    Logging.warn ~section:"dashboard" "Balance cache initialization failed";

  telemetry_initialized && system_initialized && balance_initialized

(** Initialize and run the dashboard *)
let run () : unit Lwt.t =
  Logging.info ~section:"dashboard" "Starting Dio Dashboard...";

  (* Set up exception hook to ensure errors bubble up to stderr *)
  Lwt.async_exception_hook := (fun exn ->
    prerr_endline ("Dashboard async exception: " ^ Printexc.to_string exn);
    Printexc.print_backtrace stderr;
    (* Don't re-raise in dashboard mode to prevent crashes *)
    Logging.error_f ~section:"dashboard" "Async exception caught: %s" (Printexc.to_string exn)
  );

  (* Verify that all caches are initialized before proceeding *)
  let caches_ready = verify_cache_initialization () in
  if not caches_ready then
    Logging.warn ~section:"dashboard" "Some caches failed to initialize, dashboard may not function correctly";

  (* Create dashboard and reactive variables first *)
  let (ui, handle_event, telemetry_snapshot_var, system_stats_var, balance_snapshot_var, state_var, terminal_size_var, viewport_var, update_panel_viewports) = make_dashboard () in

  (* Performance monitoring counters and memory tracking (scoped to run) *)
  let telemetry_update_count = ref 0 in
  let system_update_count = ref 0 in
  let balance_update_count = ref 0 in
  let last_memory_check = ref (Unix.time ()) in
  let last_memory_usage = ref 0.0 in

  (* Create quit promise *)
  let quit_promise, quit_resolver = Lwt.wait () in

  (* Flags to track initial data load *)
  let telemetry_loaded = ref false in
  let system_loaded = ref false in
  let balance_loaded = ref false in

  let check_all_loaded () =
    if !telemetry_loaded && !system_loaded && !balance_loaded then
      let current_state = Lwd.peek state_var in
      if current_state.is_loading then (
        (* Initialize modules based on viewport size *)
        let initial_open_modules, initial_focused =
          let screen_size = Ui_types.classify_screen_size (fst current_state.viewport_constraints) (snd current_state.viewport_constraints) in
          match screen_size with
          | Ui_types.Large | Ui_types.Medium ->
              (* Start with System module open on larger screens *)
              ([SystemPanel], SystemPanel)
          | Ui_types.Mobile | Ui_types.Small ->
              (* Start with all modules collapsed on smaller screens *)
              ([], NoFocus)
        in
        let initial_collapsed = List.filter (fun p -> not (List.mem p initial_open_modules)) [SystemPanel; BalancesPanel; TelemetryPanel; LogsPanel] in

        Lwd.set state_var { current_state with
          is_loading = false;
          open_modules = initial_open_modules;
          collapsed_modules = initial_collapsed;
          focused_panel = initial_focused;
        };
        update_panel_viewports { current_state with
          is_loading = false;
          open_modules = initial_open_modules;
          collapsed_modules = initial_collapsed;
          focused_panel = initial_focused;
        };
        Logging.info ~section:"dashboard" "All data sources loaded, dashboard is live."
      )
  in

  (* Subscribe to event streams for real-time updates with defensive checks *)
  let _telemetry_sub = Lwt.async (fun () ->
    try
      let stream = Dio_ui_telemetry.Telemetry_cache.subscribe_telemetry_snapshot () in
      Lwt_stream.iter (fun snapshot ->
        Logging.debug_f ~section:"dashboard" "Received telemetry snapshot update at %.3f" (Unix.time ());
        if not !telemetry_loaded then (
          telemetry_loaded := true;
          check_all_loaded ()
        );
        (* Performance monitoring - track update count *)
        incr telemetry_update_count;
        Lwd.set telemetry_snapshot_var snapshot;
        (* Trigger minor GC after large snapshot updates to encourage cleanup of old reactive graph nodes *)
        if !telemetry_update_count mod 50 = 0 then Gc.minor ();

        (* Memory monitoring - check every 100 telemetry updates *)
        if !telemetry_update_count mod 100 = 0 then (
          let now = Unix.time () in
          let time_since_check = now -. !last_memory_check in
          if time_since_check > 300.0 then (  (* Check every 5 minutes *)
            let current_memory = (Gc.stat ()).heap_words * (Sys.word_size / 8) / (1024 * 1024) in
            let memory_diff = float_of_int current_memory -. !last_memory_usage in
            if memory_diff > 50.0 then (
              Logging.warn_f ~section:"dashboard" "Memory usage increased by %.1f MB in last %.0f minutes (%.1f MB total)"
                memory_diff time_since_check !last_memory_usage
            );
            last_memory_check := now;
            last_memory_usage := float_of_int current_memory
          )
        )
      ) stream
    with exn ->
      Logging.error_f ~section:"dashboard" "Failed to subscribe to telemetry updates: %s" (Printexc.to_string exn);
      Lwt.return_unit
  ) in

  let _system_stats_sub = Lwt.async (fun () ->
    try
      let stream = Dio_ui_system.System_cache.subscribe_system_stats () in
      Lwt_stream.iter (fun stats ->
        if not !system_loaded then (
          system_loaded := true;
          check_all_loaded ()
        );
        (* Performance monitoring - track update count *)
        incr system_update_count;
        Lwd.set system_stats_var stats
      ) stream
    with exn ->
      Logging.error_f ~section:"dashboard" "Failed to subscribe to system stats updates: %s" (Printexc.to_string exn);
      Lwt.return_unit
  ) in

  let _balance_sub = Lwt.async (fun () ->
    try
      let stream = Dio_ui_balance.Balance_cache.subscribe_balance_snapshot () in
      let update_counter = ref 0 in
      Lwt_stream.iter (fun snapshot ->
        (* Only consider balances loaded when we have actual balance data, not just empty snapshots *)
        if not !balance_loaded && snapshot.balances <> [] then (
          balance_loaded := true;
          check_all_loaded ()
        );
        incr update_counter;
        (* Performance monitoring - track global update count *)
        incr balance_update_count;
        Lwd.set balance_snapshot_var (snapshot, !update_counter)
      ) stream
    with exn ->
      Logging.error_f ~section:"dashboard" "Failed to subscribe to balance updates: %s" (Printexc.to_string exn);
      Lwt.return_unit
  ) in

  (* Initialize subsystems in the background to not block the UI *)
  Lwt.async (fun () ->
    try
      Dio_ui_telemetry.Telemetry_cache.init ();
      Dio_ui_system.System_cache.init ();
      Dio_ui_balance.Balance_cache.init ();
      Lwt.return_unit
    with exn ->
      Logging.error_f ~section:"dashboard" "Failed to initialize subsystems: %s" (Printexc.to_string exn);
      Lwt.return_unit
  );

  (* Wait asynchronously for cache initialization to complete *)
  (* Don't use blocking sleep - let the async threads run *)
  let cache_init_wait = Lwt_unix.sleep 0.1 in  (* Small delay to let cache init threads start *)
  let%lwt () = cache_init_wait in

  (* The loading state will be cleared when data actually arrives via the streams *)
  Logging.info ~section:"dashboard" "Dashboard waiting for data sources to initialize...";

  (* Periodic performance monitoring - log update statistics every 10 minutes *)
  Lwt.async (fun () ->
    Logging.info ~section:"dashboard" "Starting performance monitoring loop";
    let rec monitor_performance () =
      let%lwt () = Lwt_unix.sleep 600.0 in (* Log every 10 minutes *)
      let total_updates = !telemetry_update_count + !system_update_count + !balance_update_count in
      let current_memory = (Gc.stat ()).heap_words * (Sys.word_size / 8) / (1024 * 1024) in
      Logging.info_f ~section:"dashboard" "Performance stats - Updates: telemetry=%d, system=%d, balance=%d (total=%d), Memory: %d MB"
        !telemetry_update_count !system_update_count !balance_update_count total_updates current_memory;
      monitor_performance ()
    in
    monitor_performance ()
  );

  (* Aggressive GC triggers for dashboard mode to prevent memory accumulation *)
  Lwt.async (fun () ->
    Logging.info ~section:"dashboard" "Starting aggressive GC triggers for dashboard mode";
    let last_gc_compact = ref (Unix.time ()) in
    let last_memory_check = ref (Unix.time ()) in
    let last_memory_mb = ref 0.0 in

    let rec gc_loop () =
      let now = Unix.time () in

      (* Trigger minor GC every 2 minutes to encourage prompt cleanup of old snapshots *)
      let%lwt () = Lwt_unix.sleep 120.0 in  (* Every 2 minutes *)
      Gc.minor ();
      Logging.debug ~section:"dashboard" "Triggered minor GC for dashboard memory management";

      (* Check memory growth every 2 minutes *)
      let current_memory_mb = float_of_int ((Gc.stat ()).heap_words * (Sys.word_size / 8) / (1024 * 1024)) in
      let time_since_check = now -. !last_memory_check in
      if time_since_check >= 120.0 then (  (* Every 2 minutes *)
        let memory_growth = current_memory_mb -. !last_memory_mb in
        let growth_rate_per_hour = memory_growth *. (3600.0 /. time_since_check) in
        if growth_rate_per_hour > 20.0 then (
          Logging.warn_f ~section:"dashboard" "High memory growth detected: %.1f MB/hour (%.1f MB -> %.1f MB in %.0f seconds)"
            growth_rate_per_hour !last_memory_mb current_memory_mb time_since_check;
          (* Trigger major GC to try to reclaim memory *)
          Gc.major ();
          Logging.debug ~section:"dashboard" "Triggered major GC due to high memory growth"
        );
        last_memory_check := now;
        last_memory_mb := current_memory_mb
      );

      (* Trigger full compaction every 30 minutes to defragment heap *)
      let time_since_compact = now -. !last_gc_compact in
      if time_since_compact >= 1800.0 then (  (* Every 30 minutes *)
        Logging.info ~section:"dashboard" "Triggering full GC compaction for dashboard mode";
        Gc.compact ();
        last_gc_compact := now;
        Logging.info_f ~section:"dashboard" "GC compaction completed, memory: %.1f MB" current_memory_mb
      );

      gc_loop ()
    in
    gc_loop ()
  );

  (* Dashboard main loop with TTY reconnection handling *)
  let rec run_dashboard_loop () =
    try
      (* Create terminal with error handling *)
      let term = Notty_lwt.Term.create () in

      (* Get terminal size immediately after creation *)
      let initial_size = Notty_lwt.Term.size term in
      Lwd.set terminal_size_var initial_size;

      (* Update initial state with real size *)
      let current_state = Lwd.peek state_var in
      let (available_w, available_h) = Ui_types.calculate_available_space
        (fst initial_size) (snd initial_size) (List.length current_state.collapsed_modules) in
      let new_single_module_mode = not (Ui_types.can_support_multiple_modules available_w available_h) in
      Lwd.set viewport_var (available_w, available_h);
      Lwd.set state_var { current_state with
        viewport_constraints = (available_w, available_h);
        single_module_mode = new_single_module_mode;
      };
      update_panel_viewports { current_state with
        viewport_constraints = (available_w, available_h);
        single_module_mode = new_single_module_mode;
      };

      let event_stream = Notty_lwt.Term.events term in

      let custom_event_stream =
        Lwt_stream.filter_map (fun event ->
          (* Call our event handler synchronously *)
          match handle_event event with
          | `Quit ->
              Lwt.wakeup quit_resolver ();
              None  (* Don't pass the event through when quitting *)
          | `Continue ->
              Some event  (* Pass the event through for normal processing *)
        ) event_stream
      in

      (* Run the UI using render directly - now using real terminal size *)
      let image_stream = Nottui_lwt.render ~quit:quit_promise ~size:initial_size custom_event_stream ui in

      (* Start size monitoring thread *)
      let _ = Lwt.async (fun () ->
        let rec monitor_size first_iter =
          let%lwt () = if not first_iter then
            Lwt_unix.sleep 0.5  (* Check size every 500ms, but not on first iteration *)
          else
            Lwt.return_unit
          in
          let current_size = Notty_lwt.Term.size term in
          Lwd.set terminal_size_var current_size;

          (* Update viewport constraints and single-module mode *)
          let current_state = Lwd.peek state_var in
          let (available_w, available_h) = Ui_types.calculate_available_space
            (fst current_size) (snd current_size) (List.length current_state.collapsed_modules) in
          let new_single_module_mode = not (Ui_types.can_support_multiple_modules available_w available_h) in

          (* Update reactive viewport variable for views *)
          Lwd.set viewport_var (available_w, available_h);

          (* Update state if viewport constraints changed *)
          if current_state.viewport_constraints <> (available_w, available_h) ||
             current_state.single_module_mode <> new_single_module_mode then (
            let new_state = { current_state with
              viewport_constraints = (available_w, available_h);
              single_module_mode = new_single_module_mode;
            } in
            Lwd.set state_var new_state;
            update_panel_viewports new_state;
          );

          monitor_size false
        in
        monitor_size true  (* Start with immediate check *)
      ) in

      Lwt.finalize
        (fun () ->
          Lwt.catch
            (fun () -> Lwt_stream.iter_s (Notty_lwt.Term.image term) image_stream)
            (fun exn ->
              (* Handle terminal errors gracefully *)
              Logging.warn_f ~section:"dashboard" "Terminal error, attempting recovery: %s" (Printexc.to_string exn);
              Lwt.return_unit
            )
        )
        (fun () ->
          (* Clean up terminal *)
          Notty_lwt.Term.release term
        )
    with exn ->
      Logging.error_f ~section:"dashboard" "Failed to create terminal: %s" (Printexc.to_string exn);
      (* Wait a bit before retrying *)
      let%lwt () = Lwt_unix.sleep 1.0 in
      run_dashboard_loop ()
    in

    run_dashboard_loop ()