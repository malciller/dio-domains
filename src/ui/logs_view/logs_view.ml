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

(** Create logs view widget *)
let logs_view log_entries_map state =
  (* Header *)

  let entry_count = LogMap.cardinal log_entries_map in
  let count_str = Printf.sprintf "%d entries" entry_count in
  let count_attr = Notty.A.(fg white) in  (* Primary text for count *)
  let count = string ~attr:count_attr count_str in

  let header = count in 

  (* Auto-scroll control *)
  let auto_scroll_str = if state.auto_scroll then "Auto-scroll: ON" else "Auto-scroll: OFF" in
  let controls = string ~attr:Notty.A.(fg cyan) auto_scroll_str in  (* Cyan for controls *)

  (* Log entries *)
  let view_height = 50 in (* Max number of entries to show *)
  
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
let make_logs_view log_entries_var =
  let state_var = Lwd.var initial_state in

  (* Reactive UI that depends on both log entries and state changes *)
  let ui = Lwd.map2 (Lwd.get log_entries_var) (Lwd.get state_var) ~f:(fun (log_entries_map, _) state ->
    logs_view log_entries_map state
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

