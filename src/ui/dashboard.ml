(** Main Dashboard Application

    Orchestrates the different dashboard views and provides navigation
    between telemetry, trading, and other system information displays.
*)

open Nottui_widgets
open Ui_types
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
}

(** Create bordered panel with title that dynamically scales and respects size constraints *)
let create_panel ~title ~content ~focused ?max_width ?max_height () =
  let open Nottui.Ui in
  let border_char = "|" in

  (* Adaptive border characters based on available space *)
  let (top_left, top_right, bottom_left, bottom_right) = match max_width with
    | Some w when w < 20 -> ("+", "+", "+", "+")  (* Simple borders for narrow panels *)
    | _ -> ("/", "\\", "\\", "/")  (* Fancy borders for wider panels *)
  in

  let title_attr = if focused then Notty.A.(st bold ++ st reverse ++ bg cyan ++ fg black)
                   else Notty.A.(st bold ++ fg white) in  (* Use cyan for focus, dim gray for inactive *)
  let border_attr = if focused then Notty.A.(fg cyan) else Notty.A.(fg (gray 2)) in

  (* Adaptive title - truncate if too long for available space *)
  let title_base = " " ^ title ^ " " in
  let title_display = match max_width with
    | Some max_w when String.length title_base > max_w - 4 ->
        (* Leave space for borders, truncate title *)
        let available_title_space = max 3 (max_w - 4) in
        let truncated_title = Ui_types.truncate_string (available_title_space - 2) title in
        string ~attr:title_attr (" " ^ truncated_title ^ " ")
    | _ -> string ~attr:title_attr title_base
  in

  (* Create border lines *)
  let top_border = hcat [
    string ~attr:border_attr top_left;
    title_display;
    string ~attr:border_attr top_right;
  ] in

  let bottom_border = hcat [
    string ~attr:border_attr bottom_left;
    string ~attr:border_attr bottom_right;
  ] in

  (* Apply size constraints to content if specified *)
  let constrained_content = match max_width, max_height with
    | Some w, Some h ->
        (* Constrain both dimensions *)
        resize ~w ~h content
    | Some w, None ->
        (* Constrain width only *)
        resize ~w content
    | None, Some h ->
        (* Constrain height only - let width expand *)
        resize ~h content
    | None, None ->
        (* No constraints *)
        content
  in

  (* Content with side borders *)
  let content_with_borders = hcat [
    string ~attr:border_attr border_char;
    string ~attr:Notty.A.empty " ";
    constrained_content;
    string ~attr:Notty.A.empty " ";
    string ~attr:border_attr border_char;
  ] in

  (* Stack vertically: top border, content, bottom border *)
  vcat [top_border; content_with_borders; bottom_border]

(** Create top status bar with system info - adaptive to screen size *)
let create_status_bar system_stats is_loading screen_size =
  let open Nottui.Ui in
  let primary_attr = Notty.A.(fg white ++ st bold) in  (* Primary text for status bar *)
  let separator_attr = Notty.A.(fg (gray 2)) in     (* Dim gray for separators *)
  let loading_attr = Notty.A.(fg yellow ++ st bold) in (* Yellow for loading indicator *)

  (* Adaptive title based on screen size *)
  let title_text = match screen_size with
    | Ui_types.Mobile -> if is_loading then "DIO (LOAD)" else "DIO"
    | Ui_types.Small -> if is_loading then "DIO (LOAD)" else "DIO"
    | Ui_types.Medium -> if is_loading then "DIO TRADING (LOAD)" else "DIO TRADING"
    | Ui_types.Large -> if is_loading then " DIO TRADING TERMINAL (LOADING...) " else " DIO TRADING TERMINAL "
  in
  let title = string ~attr:(if is_loading then loading_attr else primary_attr) title_text in

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
  let uptime = hcat [string ~attr:separator_attr " │ "; string ~attr:primary_attr ("UP:" ^ uptime_str)] in

  (* Adaptive CPU display *)
  let cpu_str = Printf.sprintf "%.0f%%" system_stats.cpu_usage in
  let cpu_color = if system_stats.cpu_usage > 90.0 then Notty.A.(fg red) else if system_stats.cpu_usage > 70.0 then Notty.A.(fg yellow) else Notty.A.(fg green) in
  let cpu = hcat [string ~attr:separator_attr " │ "; string ~attr:cpu_color ("CPU:" ^ cpu_str)] in

  (* Adaptive memory display - hide on very small screens *)
  let mem_part = match screen_size with
    | Ui_types.Mobile -> empty  (* No memory info on mobile *)
    | Ui_types.Small | Ui_types.Medium | Ui_types.Large ->
        let mem_usage_pct = (float_of_int system_stats.memory_used /. float_of_int system_stats.memory_total) *. 100.0 in
        let mem_str = match screen_size with
          | Ui_types.Small -> Printf.sprintf "%.0f%%" mem_usage_pct
          | Ui_types.Medium -> Printf.sprintf "%dM" (system_stats.memory_used / 1024)
          | Ui_types.Large -> Printf.sprintf "%d/%d MB" (system_stats.memory_used / 1024) (system_stats.memory_total / 1024)
          | _ -> ""
        in
        let mem_color = if mem_usage_pct > 90.0 then Notty.A.(fg red) else if mem_usage_pct > 70.0 then Notty.A.(fg yellow) else Notty.A.(fg green) in
        hcat [string ~attr:separator_attr " │ "; string ~attr:mem_color ("MEM:" ^ mem_str)]
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
  let timestamp = hcat [string ~attr:separator_attr " │ "; string ~attr:primary_attr timestamp_str] in

  (* Combine elements based on screen size *)
  match screen_size with
  | Ui_types.Mobile -> hcat [title; uptime; cpu]
  | Ui_types.Small -> hcat [title; uptime; cpu; mem_part]
  | Ui_types.Medium -> hcat [title; uptime; cpu; mem_part; timestamp]
  | Ui_types.Large -> hcat [title; uptime; cpu; mem_part; timestamp]

(** Create bottom status bar with navigation hints - adaptive to screen size *)
let create_bottom_bar focused_panel screen_size =
  let open Nottui.Ui in
  let inactive_attr = Notty.A.(fg (gray 2)) in      (* Dim gray for inactive panels *)
  let active_attr = Notty.A.(fg cyan ++ st bold) in (* Cyan for active/focused panel *)
  let primary_attr = Notty.A.(fg white) in          (* Primary text for general hints *)
  let separator_attr = Notty.A.(fg (gray 2)) in     (* Dim gray for separators *)

  (* Adaptive panel hints based on screen size *)
  let (s_hint, b_hint, t_hint, l_hint, spacer, refresh_hint, quit_hint) = match screen_size with
    | Ui_types.Mobile ->
        (* Minimal hints for mobile - just single letters *)
        let s = string ~attr:(if focused_panel = SystemPanel then active_attr else inactive_attr) "S" in
        let b = string ~attr:(if focused_panel = BalancesPanel then active_attr else inactive_attr) "B" in
        let t = string ~attr:(if focused_panel = TelemetryPanel then active_attr else inactive_attr) "T" in
        let l = string ~attr:(if focused_panel = LogsPanel then active_attr else inactive_attr) "L" in
        let r = if focused_panel = BalancesPanel then string ~attr:primary_attr "R" else empty in
        let q = string ~attr:primary_attr "Q" in
        (s, b, t, l, string ~attr:separator_attr "|", r, q)
    | Ui_types.Small ->
        (* Short hints for small screens *)
        let s = string ~attr:(if focused_panel = SystemPanel then active_attr else inactive_attr) "S:Sys" in
        let b = string ~attr:(if focused_panel = BalancesPanel then active_attr else inactive_attr) "B:Bal" in
        let t = string ~attr:(if focused_panel = TelemetryPanel then active_attr else inactive_attr) "T:Tel" in
        let l = string ~attr:(if focused_panel = LogsPanel then active_attr else inactive_attr) "L:Log" in
        let r = if focused_panel = BalancesPanel then string ~attr:primary_attr "R:Ref" else empty in
        let q = string ~attr:primary_attr "Q:Quit" in
        (s, b, t, l, string ~attr:separator_attr " | ", r, q)
    | Ui_types.Medium | Ui_types.Large ->
        (* Full hints for larger screens *)
        let s = string ~attr:(if focused_panel = SystemPanel then active_attr else inactive_attr) " S:SYSTEM " in
        let b = string ~attr:(if focused_panel = BalancesPanel then active_attr else inactive_attr) " B:BALANCES " in
        let t = string ~attr:(if focused_panel = TelemetryPanel then active_attr else inactive_attr) " T:TELEMETRY " in
        let l = string ~attr:(if focused_panel = LogsPanel then active_attr else inactive_attr) " L:LOGS " in
        let r = if focused_panel = BalancesPanel then
          hcat [string ~attr:primary_attr " R:REFRESH "; string ~attr:separator_attr "│ "]
        else empty in
        let q = string ~attr:primary_attr " Q:QUIT " in
        (s, b, t, l, string ~attr:separator_attr " │ ", r, q)
  in

  (* Combine elements - adapt spacing based on screen size *)
  match screen_size with
  | Ui_types.Mobile -> hcat [s_hint; spacer; b_hint; spacer; t_hint; spacer; l_hint; spacer; refresh_hint; quit_hint]
  | Ui_types.Small -> hcat [s_hint; spacer; b_hint; spacer; t_hint; spacer; l_hint; spacer; refresh_hint; quit_hint]
  | Ui_types.Medium -> hcat [s_hint; spacer; b_hint; spacer; t_hint; spacer; l_hint; spacer; refresh_hint; quit_hint]
  | Ui_types.Large -> hcat [s_hint; spacer; b_hint; spacer; t_hint; spacer; l_hint; spacer; refresh_hint; quit_hint]


let handle_navigation_key focused_panel = function
  | `ASCII 's', [] -> SystemPanel
  | `ASCII 'b', [] -> BalancesPanel
  | `ASCII 't', [] -> TelemetryPanel
  | `ASCII 'l', [] -> LogsPanel
  | _ -> focused_panel


let make_dashboard () =
  let open Nottui.Ui in

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

  (* Start periodic time updates for uptime display *)
  Lwt.async (fun () ->
    let rec update_time () =
      let%lwt () = Lwt_unix.sleep 1.0 in
      Lwd.set time_var (Unix.time ());
      update_time ()
    in
    update_time ()
  );

  (* Pre-create views - telemetry, system, balances, and logs views are reactive and size-aware *)
  let telemetry_ui, telemetry_handler = Dio_ui_telemetry.Telemetry_view.make_telemetry_view telemetry_snapshot_var in
  let system_ui, system_handler = Dio_ui_system.System_view.make_system_view system_stats_var telemetry_snapshot_var ~available_width:80 ~available_height:24 in
  let balances_ui, balances_handler = Dio_ui_balance.Balance_view.make_balances_view balance_snapshot_var in
  let log_entries_var = Dio_ui_logs.Logs_cache.get_log_entries_var () in
  let logs_ui, logs_handler = Dio_ui_logs.Logs_view.make_logs_view log_entries_var in

  (* Dashboard state - starts with no focus and loading *)
  let state_var = Lwd.var { focused_panel = NoFocus; is_loading = true } in

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

  (* Create adaptive main panel layout based on terminal size *)
  let main_panels_ui = Lwd.map2
    (Lwd.map2
      (Lwd.map2 (Lwd.get terminal_size_var)
                (Lwd.map2 (Lwd.map2 system_panel_ui balances_panel_ui ~f:(fun sp bp -> (sp, bp)))
                          telemetry_panel_ui ~f:(fun (sp, bp) tp -> (sp, bp, tp)))
                ~f:(fun (w, h) (system_panel, balances_panel, telemetry_panel) ->
                  let screen_size = Ui_types.classify_screen_size w h in
                  (screen_size, system_panel, balances_panel, telemetry_panel)))
      logs_panel_ui
      ~f:(fun (screen_size, system_panel, balances_panel, telemetry_panel) logs_panel ->
        match screen_size with
        | Ui_types.Mobile ->
            (* Stack all panels vertically on mobile *)
            vcat [system_panel; balances_panel; telemetry_panel; logs_panel]
        | Ui_types.Small ->
            (* Two columns: System+Balance on left, Telemetry+Logs on right *)
            let left_column = vcat [system_panel; balances_panel] in
            let right_column = vcat [telemetry_panel; logs_panel] in
            hcat [left_column; right_column]
        | Ui_types.Medium ->
            (* Two columns: System+Balance on left, Telemetry+Logs on right *)
            let left_column = vcat [system_panel; balances_panel] in
            let right_column = vcat [telemetry_panel; logs_panel] in
            hcat [left_column; right_column]
        | Ui_types.Large ->
            (* Original layout: three panels on top, logs full-width below *)
            let top_row = hcat [system_panel; balances_panel; telemetry_panel] in
            let bottom_row = logs_panel in
            vcat [top_row; bottom_row]
      ))
    (Lwd.get terminal_size_var)
    ~f:(fun layout _ -> layout) in

  (* Combine all UI elements *)
  let ui = Lwd.map2 status_bar_ui
    (Lwd.map2 main_panels_ui bottom_bar_ui ~f:(fun main_panels bottom_bar -> (main_panels, bottom_bar)))
    ~f:(fun status_bar (main_panels, bottom_bar) ->
      vcat [
        status_bar;
        string ~attr:Notty.A.(fg (gray 2)) "";  (* Separator line *)
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
                let new_focus = handle_navigation_key state.focused_panel key in
                if new_focus <> state.focused_panel then (
                  Lwd.set state_var { state with focused_panel = new_focus };
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

  (ui, handle_event, telemetry_snapshot_var, system_stats_var, balance_snapshot_var, state_var, terminal_size_var)

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

  (* Create dashboard and reactive variables first *)
  let (ui, handle_event, telemetry_snapshot_var, system_stats_var, balance_snapshot_var, state_var, terminal_size_var) = make_dashboard () in

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
        Lwd.set state_var { current_state with is_loading = false };
        Logging.info ~section:"dashboard" "All data sources loaded, dashboard is live."
      )
  in

  (* Subscribe to event streams for real-time updates *)
  let _telemetry_sub = Lwt.async (fun () ->
    let stream = Dio_ui_telemetry.Telemetry_cache.subscribe_telemetry_snapshot () in
    Lwt_stream.iter (fun snapshot ->
      Logging.debug_f ~section:"dashboard" "Received telemetry snapshot update at %.3f" (Unix.time ());
      if not !telemetry_loaded then (
        telemetry_loaded := true;
        check_all_loaded ()
      );
      Lwd.set telemetry_snapshot_var snapshot
    ) stream
  ) in

  let _system_stats_sub = Lwt.async (fun () ->
    let stream = Dio_ui_system.System_cache.subscribe_system_stats () in
    Lwt_stream.iter (fun stats ->
      if not !system_loaded then (
        system_loaded := true;
        check_all_loaded ()
      );
      Lwd.set system_stats_var stats
    ) stream
  ) in

  let _balance_sub = Lwt.async (fun () ->
    let stream = Dio_ui_balance.Balance_cache.subscribe_balance_snapshot () in
    let update_counter = ref 0 in
    Lwt_stream.iter (fun snapshot ->
      (* Only consider balances loaded when we have actual balance data, not just empty snapshots *)
      if not !balance_loaded && snapshot.balances <> [] then (
        balance_loaded := true;
        check_all_loaded ()
      );
      incr update_counter;
      Lwd.set balance_snapshot_var (snapshot, !update_counter)
    ) stream
  ) in

  (* Initialize subsystems in the background to not block the UI *)
  Lwt.async (fun () ->
    Dio_ui_telemetry.Telemetry_cache.init ();
    Dio_ui_system.System_cache.init ();
    Dio_ui_balance.Balance_cache.init ();
    Lwt.return_unit
  );

  (* Wait asynchronously for cache initialization to complete *)
  (* Don't use blocking sleep - let the async threads run *)
  let cache_init_wait = Lwt_unix.sleep 0.1 in  (* Small delay to let cache init threads start *)
  let%lwt () = cache_init_wait in

  (* The loading state will be cleared when data actually arrives via the streams *)
  Logging.info ~section:"dashboard" "Dashboard waiting for data sources to initialize...";

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

      (* Run the UI using render directly *)
      let size = Notty_lwt.Term.size term in
      let image_stream = Nottui_lwt.render ~quit:quit_promise ~size custom_event_stream ui in

      (* Start size monitoring thread *)
      let _ = Lwt.async (fun () ->
        let rec monitor_size () =
          let%lwt () = Lwt_unix.sleep 0.5 in  (* Check size every 500ms *)
          let current_size = Notty_lwt.Term.size term in
          Lwd.set terminal_size_var current_size;
          monitor_size ()
        in
        monitor_size ()
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