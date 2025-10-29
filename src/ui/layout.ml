(** Layout System Module

    Provides reusable layout utilities for consistent spacing and arrangement
    of UI components in the Bloomberg-style terminal interface.
*)

open Nottui
open Design_tokens

(** Grid layout - arrange items in a multi-column grid *)
let grid_layout ?(gutter = Spacing.panel_gutter) columns items =
  let rec arrange_in_rows acc remaining =
    match remaining with
    | [] -> List.rev acc
    | _ ->
        let row_items = List.filteri (fun i _ -> i < columns) remaining in
        let remaining_items = List.filteri (fun i _ -> i >= columns) remaining in
        let row = List.fold_left (fun acc item ->
          match acc with
          | [] -> [item]
          | hd :: tl -> item :: Ui_components.create_text (String.make gutter ' ') :: hd :: tl
        ) [] (List.rev row_items) |> List.rev in
        arrange_in_rows (row :: acc) remaining_items
  in
  let rows = arrange_in_rows [] items in
  let row_widgets = List.map (fun row -> Ui.hcat row) rows in
  Ui.vcat row_widgets

(** Stack layout - vertical stacking with consistent spacing *)
let stack_layout ?(spacing = Spacing.section_spacing) items =
  let rec add_spacing = function
    | [] -> []
    | [last] -> [last]
    | hd :: tl ->
        hd :: List.init spacing (fun _ -> Ui.string ~attr:Ui.A.empty "") @ add_spacing tl
  in
  Ui.vcat (add_spacing items)

(** Side by side layout - horizontal arrangement with gutter *)
let side_by_side ?(gutter = Spacing.panel_gutter) left right =
  Ui.hcat [left; Ui.string ~attr:Ui.A.empty (String.make gutter ' '); right]

(** Constrain dimensions - size constraint helpers *)
let constrain_dimensions ?max_width ?max_height ?min_width ?min_height widget =
  let constrained = match max_width, max_height with
    | Some w, Some h -> Ui.resize ~w ~h widget
    | Some w, None -> Ui.resize ~w widget
    | None, Some h -> Ui.resize ~h widget
    | None, None -> widget
  in
  (* Note: Nottui doesn't have min constraints, so we just return the max-constrained widget *)
  constrained

(** Panel layout - standard panel arrangement with consistent padding *)
let panel_layout ?(padding_x = Spacing.panel_padding_x) ?(padding_y = Spacing.panel_padding_y) content =
  let padding_top = List.init padding_y (fun _ -> Ui.string ~attr:Ui.A.empty "") in
  let padding_bottom = List.init padding_y (fun _ -> Ui.string ~attr:Ui.A.empty "") in
  let padding_left = Ui.string ~attr:Ui.A.empty (String.make padding_x ' ') in
  let padding_right = Ui.string ~attr:Ui.A.empty (String.make padding_x ' ') in

  let padded_content = Ui.hcat [padding_left; content; padding_right] in
  Ui.vcat (padding_top @ [padded_content] @ padding_bottom)

(** Centered layout - center content within available space *)
let centered ?(available_width = 80) ?(available_height = 24) content =
  let content_size = Ui.layout_size content in
  let content_width = Ui.size_width content_size in
  let content_height = Ui.size_height content_size in

  let left_padding = max 0 ((available_width - content_width) / 2) in
  let right_padding = max 0 (available_width - content_width - left_padding) in
  let top_padding = max 0 ((available_height - content_height) / 2) in
  let bottom_padding = max 0 (available_height - content_height - top_padding) in

  let top_space = List.init top_padding (fun _ -> Ui.empty) in
  let bottom_space = List.init bottom_padding (fun _ -> Ui.empty) in
  let left_space = Ui.string ~attr:Ui.A.empty (String.make left_padding ' ') in
  let right_space = Ui.string ~attr:Ui.A.empty (String.make right_padding ' ') in

  Ui.vcat (top_space @ [Ui.hcat [left_space; content; right_space]] @ bottom_space)

(** Split layout - divide available space between two components *)
let split_layout ?(ratio = 0.5) ?(gutter = Spacing.panel_gutter) left right available_width available_height =
  let left_width = int_of_float (float_of_int available_width *. ratio) - (gutter / 2) in
  let right_width = available_width - left_width - gutter in

  let left_constrained = Ui.resize ~w:left_width ~h:available_height left in
  let right_constrained = Ui.resize ~w:right_width ~h:available_height right in

  Ui.hcat [left_constrained; Ui.string ~attr:Ui.A.empty (String.make gutter ' '); right_constrained]

(** Responsive layout helper - choose layout based on screen size *)
let responsive_layout screen_size layouts =
  match screen_size with
  | Ui_types.Mobile -> layouts.mobile
  | Ui_types.Small -> layouts.small
  | Ui_types.Medium -> layouts.medium
  | Ui_types.Large -> layouts.large

(** Layout configuration for responsive design *)
type 'a layout_configs = {
  mobile: 'a;
  small: 'a;
  medium: 'a;
  large: 'a;
}

(** Flow layout - arrange items in a flowing grid that wraps to next line *)
let flow_layout ?(item_width = 20) ?(gutter = Spacing.panel_gutter) ?(available_width = 80) items =
  let items_per_row = max 1 ((available_width + gutter) / (item_width + gutter)) in
  grid_layout items_per_row items
