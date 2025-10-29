(** Layout System Module

    Provides reusable layout utilities for consistent spacing and arrangement
    of UI components in the Bloomberg-style terminal interface.
*)

open Notty
open Nottui_widgets
open Nottui.Ui
open Design_tokens
open Ui_types

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
  let row_widgets = List.map (fun row -> hcat row) rows in
  vcat row_widgets

(** Stack layout - vertical stacking with consistent spacing *)
let stack_layout ?(spacing = Spacing.section_spacing) items =
  let rec add_spacing = function
    | [] -> []
    | [last] -> [last]
    | hd :: tl ->
        hd :: List.init spacing (fun _ -> string ~attr:A.empty "") @ add_spacing tl
  in
  vcat (add_spacing items)

(** Side by side layout - horizontal arrangement with gutter *)
let side_by_side ?(gutter = Spacing.panel_gutter) left right =
  hcat [left; string ~attr:A.empty (String.make gutter ' '); right]

(** Constrain dimensions - size constraint helpers *)
let constrain_dimensions ?max_width ?max_height widget =
  let constrained = match max_width, max_height with
    | Some w, Some h -> resize ~w ~h widget
    | Some w, None -> resize ~w widget
    | None, Some h -> resize ~h widget
    | None, None -> widget
  in
  (* Note: Nottui doesn't have min constraints, so we just return the max-constrained widget *)
  constrained

(** Panel layout - standard panel arrangement with consistent padding *)
let panel_layout ?(padding_x = Spacing.panel_padding_x) ?(padding_y = Spacing.panel_padding_y) content =
  let padding_top = List.init padding_y (fun _ -> string ~attr:A.empty "") in
  let padding_bottom = List.init padding_y (fun _ -> string ~attr:A.empty "") in
  let padding_left = string ~attr:A.empty (String.make padding_x ' ') in
  let padding_right = string ~attr:A.empty (String.make padding_x ' ') in

  let padded_content = hcat [padding_left; content; padding_right] in
  vcat (padding_top @ [padded_content] @ padding_bottom)

(** Centered layout - center content within available space *)
let centered ?(available_width = 80) ?(available_height = 24) content =
  let content_size = layout_spec content in
  let content_width = content_size.w in
  let content_height = content_size.h in

  let left_padding = max 0 ((available_width - content_width) / 2) in
  let right_padding = max 0 (available_width - content_width - left_padding) in
  let top_padding = max 0 ((available_height - content_height) / 2) in
  let bottom_padding = max 0 (available_height - content_height - top_padding) in

  let top_space = List.init top_padding (fun _ -> empty) in
  let bottom_space = List.init bottom_padding (fun _ -> empty) in
  let left_space = string ~attr:A.empty (String.make left_padding ' ') in
  let right_space = string ~attr:A.empty (String.make right_padding ' ') in

  vcat (top_space @ [hcat [left_space; content; right_space]] @ bottom_space)

(** Split layout - divide available space between two components *)
let split_layout ?(ratio = 0.5) ?(gutter = Spacing.panel_gutter) left right available_width available_height =
  let left_width = int_of_float (float_of_int available_width *. ratio) - (gutter / 2) in
  let right_width = available_width - left_width - gutter in

  let left_constrained = resize ~w:left_width ~h:available_height left in
  let right_constrained = resize ~w:right_width ~h:available_height right in

  hcat [left_constrained; string ~attr:A.empty (String.make gutter ' '); right_constrained]

(** Layout configuration for responsive design *)
type 'a layout_configs = {
  mobile: 'a;
  small: 'a;
  medium: 'a;
  large: 'a;
}

(** Responsive layout helper - choose layout based on screen size *)
let responsive_layout screen_size layouts =
  match screen_size with
  | Mobile -> layouts.mobile
  | Small -> layouts.small
  | Medium -> layouts.medium
  | Large -> layouts.large

(** Flow layout - arrange items in a flowing grid that wraps to next line *)
let flow_layout ?(item_width = 20) ?(gutter = Spacing.panel_gutter) ?(available_width = 80) items =
  let items_per_row = max 1 ((available_width + gutter) / (item_width + gutter)) in
  grid_layout items_per_row items
