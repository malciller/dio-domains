(** Interactive Telemetry Dashboard View

    Displays real-time telemetry metrics in a rich terminal UI using Nottui and LWD.
*)


open Nottui.Ui
open Nottui_widgets
open Ui_types

(** UI state for telemetry view *)
type state = {
  selected_category: int;
  scroll_offset: int;
  expanded_metrics: (string, bool) Hashtbl.t;
}

let initial_state = {
  selected_category = 0;
  scroll_offset = 0;
  expanded_metrics = Hashtbl.create 32;
}

(** Format metric value with colors - using cached snapshot data *)
let format_metric_value_ui metric =
  match metric.Telemetry.metric_type with
  | Telemetry.Counter r ->
      let value = !r in  (* Safe since we're accessing from cache snapshot *)
      let color = if value > 1000 then Notty.A.(fg red ++ st bold) else if value > 100 then Notty.A.(fg yellow ++ st bold) else Notty.A.(fg green ++ st bold) in
      (Printf.sprintf "%d" value, color)
  | Telemetry.Gauge r ->
      let value = !r in  (* Safe since we're accessing from cache snapshot *)
      let color = if value > 100.0 then Notty.A.(fg red ++ st bold) else if value > 10.0 then Notty.A.(fg yellow ++ st bold) else Notty.A.(fg green ++ st bold) in
      (Printf.sprintf "%.1f" value, color)
  | Telemetry.Histogram _ ->
      let (_, p50, _, _, count) = Telemetry.histogram_stats metric in  (* Safe since we're accessing from cache snapshot *)
      let color = if count > 1000 then Notty.A.(fg red ++ st bold) else if count > 100 then Notty.A.(fg yellow ++ st bold) else Notty.A.(fg green ++ st bold) in
      if count > 0 then
        (Printf.sprintf "%d samples, p50=%.1fµs" count (p50 *. 1_000_000.0), color)
      else
        ("no samples", Notty.A.(fg (gray 2)))  (* Dim gray for no data *)

(** Create a metric display row *)
let metric_row metric _is_expanded snapshot =
  let name = metric.Telemetry.name in

  (* Skip the generic domain_cycles counter when it's 0 and has no labels *)
  match metric.Telemetry.metric_type, metric.Telemetry.name, metric.Telemetry.labels with
  | Telemetry.Counter r, "domain_cycles", [] when !r = 0 ->
      empty  (* Don't display the generic domain_cycles when it's 0 *)
  | _ ->
      let (value_str, color) = format_metric_value_ui metric in

      let label_str =
        if metric.Telemetry.labels = [] then ""
        else " (" ^ (metric.Telemetry.labels |> List.map (fun (k, v) -> k ^ "=" ^ v) |> String.concat ", ") ^ ")"
      in

      let name_attr = Notty.A.(fg white) in  (* Primary text for metric names *)
      let name_widget = string ~attr:name_attr (name ^ ":") in
      let value_widget = string ~attr:color value_str in
      let label_widget = string ~attr:Notty.A.(fg (gray 2)) label_str in  (* Dim gray for labels *)

      let row = hcat [name_widget; string ~attr:Notty.A.empty " "; value_widget; label_widget] in

      (* Add special handling for domain_cycles rate *)
      match metric.Telemetry.metric_type, metric.Telemetry.name with
      | Telemetry.Counter r, "domain_cycles" ->
          let uptime = snapshot.uptime in  (* Use cached uptime instead of direct access *)
          if uptime > 0.0 then
            let rate = float_of_int !r /. uptime in
            let rate_attr = Notty.A.(fg cyan) in  (* Cyan for accent/special info *)
            let rate_widget = string ~attr:rate_attr (Printf.sprintf " (%.0f cycles/sec)" rate) in
            hcat [row; rate_widget]
          else row
      | _ -> row

(** Create category header *)
let category_header category_name metric_count selected =
  let title = Printf.sprintf "[%s] (%d metrics)" category_name metric_count in
  let base_attr = Notty.A.(fg cyan ++ st bold) in  (* Cyan for headers *)
  let header_attr = if selected then Notty.A.(base_attr ++ st reverse ++ bg black) else base_attr in
  string ~attr:header_attr title

(** Create telemetry view widget *)
let telemetry_view (snapshot : Ui_types.telemetry_snapshot) state =
  (* Header *)
  let updated_attr = Notty.A.(fg (gray 2)) in  (* Dim gray for secondary info *)
  let tm = Unix.localtime (Unix.time ()) in
  let updated = string ~attr:updated_attr (Printf.sprintf "Updated: %02d:%02d:%02d" tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec) in
  let header = updated in

  (* Categories list *)
  let categories_widgets =
    snapshot.categories
    |> List.mapi (fun i (category_name, metrics) ->
         let selected = i = state.selected_category in
         let header = category_header category_name (List.length metrics) selected in
         let expanded = Hashtbl.find_opt state.expanded_metrics category_name |> Option.value ~default:false in

         if expanded && metrics <> [] then
           let metric_rows = List.map (fun metric -> metric_row metric false snapshot) metrics in
           let indented_rows = List.map (fun row -> hcat [string ~attr:Notty.A.empty "  "; row]) metric_rows in
           vcat (header :: indented_rows)
         else
           header
       )
  in

  let categories_list = vcat categories_widgets in

  (* Footer with controls *)
  let footer = string ~attr:Notty.A.empty "↑↓: Navigate categories | Space: Expand/collapse | q: Quit" in

  (* Combine everything *)
  vcat [
    header;
    string ~attr:Notty.A.empty "";
    categories_list;
    string ~attr:Notty.A.empty "";
    footer;
  ]

(** Handle keyboard input *)
let handle_key state (snapshot : Ui_types.telemetry_snapshot) = function
  | `ASCII 'q', [] -> `Quit
  | `ASCII ' ', [] ->
      let (category_name, _) = List.nth snapshot.categories state.selected_category in
      let currently_expanded = Hashtbl.find_opt state.expanded_metrics category_name |> Option.value ~default:false in
      Hashtbl.replace state.expanded_metrics category_name (not currently_expanded);
      `Continue state
  | `Arrow `Up, [] ->
      let new_selected = max 0 (state.selected_category - 1) in
      `Continue { state with selected_category = new_selected }
  | `Arrow `Down, [] ->
      let max_index = List.length snapshot.categories - 1 in
      let new_selected = min max_index (state.selected_category + 1) in
      `Continue { state with selected_category = new_selected }
  | _ -> `Continue state

(** Create reactive telemetry view with shared snapshot variable *)
let make_telemetry_view snapshot_var =
  let state_var = Lwd.var initial_state in

  (* Reactive UI that depends on both snapshot and state changes *)
  let ui = Lwd.map2 (Lwd.get snapshot_var) (Lwd.get state_var) ~f:(fun snapshot state ->
    telemetry_view snapshot state
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

  (* Create our own snapshot variable for standalone execution *)
  let empty_snapshot = {
    uptime = 0.0;
    metrics = [];
    categories = [];
    timestamp = 0.0;
  } in
  let snapshot_var = Lwd.var empty_snapshot in

  let (ui, handle_event) = make_telemetry_view snapshot_var in

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