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

  (* Create reactive views that adapt to viewport changes *)
  let telemetry_ui, telemetry_handler = Dio_ui_telemetry.Telemetry_view.make_telemetry_view telemetry_snapshot_var viewport_var in
  let system_ui, system_handler = Dio_ui_system.System_view.make_system_view system_stats_var telemetry_snapshot_var viewport_var in
  let balances_ui, balances_handler = Dio_ui_balance.Balance_view.make_balances_view balance_snapshot_var viewport_var in
  let log_entries_var = Dio_ui_logs.Logs_cache.get_log_entries_var () in
  let logs_ui, logs_handler = Dio_ui_logs.Logs_view.make_logs_view log_entries_var viewport_var in

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

  (* Create reactive status bar *)
  let status_bar_ui = Lwd.map2
    (Lwd.map2 (Lwd.get system_stats_var) (Lwd.get time_var) ~f:(fun ss t -> (ss, t)))
    (Lwd.map2 (Lwd.get state_var) (Lwd.get terminal_size_var) ~f:(fun state (w, h) ->
      (state, Ui_types.classify_screen_size w h)))
    ~f:(fun (system_stats, _) (state, screen_size) ->
      create_status_bar system_stats state.is_loading screen_size
    ) in

  (* Create reactive panels with focus indication *)
  let system_panel_ui = Lwd.map2 (Lwd.get state_var) system_ui
    ~f:(fun state system_content ->
      create_panel ~title:"SYSTEM" ~content:system_content ~focused:(state.focused_panel = SystemPanel) ()
    ) in

  let balances_panel_ui = Lwd.map2 (Lwd.get state_var) balances_ui
    ~f:(fun state balances_content ->
      create_panel ~title:"BALANCES" ~content:balances_content ~focused:(state.focused_panel = BalancesPanel) ()
    ) in

  let telemetry_panel_ui = Lwd.map2 (Lwd.get state_var) telemetry_ui
    ~f:(fun state telemetry_content ->
      create_panel ~title:"TELEMETRY" ~content:telemetry_content ~focused:(state.focused_panel = TelemetryPanel) ()
    ) in

  let logs_panel_ui = Lwd.map2 (Lwd.get state_var) logs_ui
    ~f:(fun state logs_content ->
      create_panel ~title:"LOGS" ~content:logs_content ~focused:(state.focused_panel = LogsPanel) ()
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

  (ui, handle_event, telemetry_snapshot_var, system_stats_var, balance_snapshot_var, state_var, terminal_size_var, viewport_var)

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
  let (ui, handle_event, telemetry_snapshot_var, system_stats_var, balance_snapshot_var, state_var, terminal_size_var, viewport_var) = make_dashboard () in

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

  (* Dashboard main loop with TTY reconnection handling *)
  let rec run_dashboard_loop () =
    try
      (* Create terminal with error handling *)
      let term = Notty_lwt.Term.create () in

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

      (* Run the UI using render directly - start with default size to avoid blocking *)
      let size = Lwd.peek terminal_size_var in
      let image_stream = Nottui_lwt.render ~quit:quit_promise ~size custom_event_stream ui in

      (* Start size monitoring thread *)
      let _ = Lwt.async (fun () ->
        let rec monitor_size () =
          let%lwt () = Lwt_unix.sleep 0.5 in  (* Check size every 500ms *)
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
            Lwd.set state_var { current_state with
              viewport_constraints = (available_w, available_h);
              single_module_mode = new_single_module_mode;
            };
          );

          monitor_size ()
        in
        monitor_size ()
      ) in

      (* Immediate size update after terminal creation to get real size without blocking *)
      let _ = Lwt.async (fun () ->
        let%lwt () = Lwt_unix.sleep 0.1 in  (* Small delay to allow terminal to initialize *)
        try
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
            Lwd.set state_var { current_state with
              viewport_constraints = (available_w, available_h);
              single_module_mode = new_single_module_mode;
            };
          );
          Lwt.return_unit
        with exn ->
          Logging.debug_f ~section:"dashboard" "Failed to get initial terminal size: %s" (Printexc.to_string exn);
          Lwt.return_unit
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