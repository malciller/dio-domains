(** Interactive Logs Dashboard View

    Displays real-time log entries in a rich terminal UI using Nottui and LWD.
*)

open Nottui.Ui
open Nottui_widgets
open Logs_cache

(** Helper function to get a sublist *)
let rec sublist start len = function
  | [] -> []
  | x :: xs -> 
      if start > 0 then sublist (start - 1) len xs
      else if len > 0 then x :: sublist 0 (len - 1) xs
      else []

(** UI state for logs view *)
type state = {
  scroll_offset: int;
  auto_scroll: bool;
}

let initial_state = {
  scroll_offset = 0;
  auto_scroll = true;
}

(** Format a log entry for display *)
let format_log_entry entry =
  let timestamp_part = string ~attr:Notty.A.(fg (gray 2)) (entry.timestamp ^ " ") in  (* Dim gray for timestamps *)
  let level_part = string ~attr:entry.color (Printf.sprintf "[%s] " entry.level) in
  let section_part = string ~attr:Notty.A.(fg cyan) (Printf.sprintf "[%s] " entry.section) in  (* Cyan for section names *)
  let message_part = string ~attr:Notty.A.(fg white) entry.message in  (* Primary text for messages *)

  hcat [timestamp_part; level_part; section_part; message_part]

(** Create logs view widget with size constraints *)
let logs_view log_entries_map state ~available_width ~available_height =
  (* Determine screen size for adaptive display *)
  let screen_size = Ui_types.classify_screen_size available_width available_height in

  match screen_size with
  | Mobile ->
      (* Ultra-compact mobile layout: maximum 6 lines *)
      let entry_count = LogMap.cardinal log_entries_map in
      let count_str = Printf.sprintf "%d logs" entry_count in
      let header = string ~attr:Notty.A.(fg white) (Ui_types.truncate_exact (available_width - 4) count_str) in

      (* Auto-scroll indicator - simplified *)
      let scroll_str = if state.auto_scroll then "Auto:ON" else "Auto:OFF" in
      let controls = string ~attr:Notty.A.(fg cyan) (Ui_types.truncate_exact (available_width - 4) scroll_str) in

      (* Show only last 3-5 entries with simplified format *)
      let max_entries = match available_height with
        | h when h <= 4 -> 2  (* Minimum space *)
        | h when h <= 5 -> 3
        | _ -> 4  (* Maximum 4 entries on mobile *)
      in

      let visible_entries =
        LogMap.to_rev_seq log_entries_map
        |> Seq.map snd
        |> Seq.take max_entries
        |> List.of_seq
        |> List.rev
      in

      (* Format entries in ultra-compact mobile style *)
      let log_widgets = List.map (fun entry ->
        let timestamp_str = Ui_types.truncate_exact 5 entry.timestamp in  (* Take first 5 chars of timestamp *)
        let level_short = match entry.level with
          | "DEBUG" -> "D" | "INFO" -> "I" | "WARN" -> "W" | "ERROR" -> "E" | _ -> "?" in
        let section_short = Ui_types.truncate_tiny entry.section in
        let message_trunc = Ui_types.truncate_exact (available_width - 15) entry.message in  (* Reserve space for prefix *)
        let line = Printf.sprintf "%s [%s] [%s] %s" timestamp_str level_short section_short message_trunc in
        string ~attr:entry.color (Ui_types.truncate_exact (available_width - 4) line)
      ) visible_entries in

      (* Combine everything - no footer on mobile to save space *)
      let all_lines = [header; controls] @ log_widgets in
      vcat all_lines

  | Small | Medium | Large ->
      (* Existing layout for larger screens *)
      (* Header *)
      let entry_count = LogMap.cardinal log_entries_map in
      let count_str = Printf.sprintf "%d entries" entry_count in
      let count_attr = Notty.A.(fg white) in  (* Primary text for count *)
      let count = string ~attr:count_attr count_str in

      let header = count in

      (* Auto-scroll control *)
      let auto_scroll_str = if state.auto_scroll then "Auto-scroll: ON" else "Auto-scroll: OFF" in
      let controls = string ~attr:Notty.A.(fg cyan) auto_scroll_str in  (* Cyan for controls *)

      (* Log entries - adapt to available space *)
      let header_footer_space = 4 in (* Reserve space for header, controls, and footer *)
      let view_height = Ui_types.adaptive_log_view_height screen_size available_height header_footer_space in

      let effective_offset = if state.auto_scroll then 0 else state.scroll_offset in
      let max_offset = max 0 (entry_count - view_height) in
      let offset = min effective_offset max_offset in

      let visible_entries =
        LogMap.to_rev_seq log_entries_map
        |> Seq.map snd
        |> Seq.drop offset
        |> Seq.take view_height
        |> List.of_seq
        |> List.rev
      in
      let log_widgets = List.map format_log_entry visible_entries in

      let padding_count = max 0 (view_height - List.length log_widgets) in
      let padding = List.init padding_count (fun _ -> string "") in

      let logs_content =
        if List.is_empty log_widgets then
          string ~attr:Notty.A.(fg white) "No log entries"
        else
          vcat (padding @ log_widgets)
      in

      (* Footer with controls *)
      let footer = string ~attr:Notty.A.(fg (gray 2))
        "↑↓: Scroll | a: Auto-scroll | c: Clear logs | q: Quit" in  (* Dim gray for secondary info *)

      (* Combine everything *)
      vcat [
        header;
        string ~attr:Notty.A.empty "";
        controls;
        string ~attr:Notty.A.empty "";
        logs_content;
        string ~attr:Notty.A.empty "";
        footer;
      ]

(** Handle keyboard input *)
let handle_key state _log_entries = function
  | `ASCII 'q', [] -> `Quit
  | `ASCII 'c', [] ->
      Logs_cache.clear_logs ();
      `Continue state
  | `ASCII 'a', [] ->
      `Continue { state with auto_scroll = not state.auto_scroll }
  | `Arrow `Up, [] ->
      let new_offset = state.scroll_offset + 1 in
      `Continue { scroll_offset = new_offset; auto_scroll = false }
  | `Arrow `Down, [] ->
      let new_offset = max 0 (state.scroll_offset - 1) in
      let auto_scroll_on = new_offset = 0 in
      `Continue { scroll_offset = new_offset; auto_scroll = auto_scroll_on }
  | `Home, [] ->
      `Continue { scroll_offset = 0; auto_scroll = false }
  | `End, [] ->
      let new_state = { scroll_offset = 0; auto_scroll = true } in
      `Continue new_state
  | _ -> `Continue state

(** Create reactive logs view with shared log entries variable *)
let make_logs_view log_entries_var viewport_var =
  let state_var = Lwd.var initial_state in

  (* Reactive UI that depends on log entries, state, and viewport changes *)
  let ui = Lwd.map2
    (Lwd.map2 (Lwd.get log_entries_var) (Lwd.get state_var) ~f:(fun (log_entries_map, _) state -> (log_entries_map, state)))
    (Lwd.get viewport_var)
    ~f:(fun (log_entries_map, state) (available_width, available_height) ->
      logs_view log_entries_map state ~available_width ~available_height
    ) in

  (* Event handling *)
  let handle_event ev =
    match ev with
    | `Key key ->
        let current_state = Lwd.peek state_var in
        let (log_entries_map, _) = Lwd.peek log_entries_var in
        begin match handle_key current_state log_entries_map key with
        | `Quit -> `Quit
        | `Continue new_state ->
            Lwd.set state_var new_state;
            `Continue
        end
    | _ -> `Continue
  in

  (ui, handle_event)

