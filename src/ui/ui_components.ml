(** UI Components Module

    Reusable UI components for the Bloomberg-style terminal interface.
    Implements standardized panels, tables, bars, and typography helpers.
*)

open Nottui
open Design_tokens

(** Typography helper for labels/titles *)
let create_label ?(style = Typography.label) text =
  Ui.string ~attr:style text

(** Typography helper for body text *)
let create_text ?(style = Typography.body) text =
  Ui.string ~attr:style text

(** Typography helper for secondary text *)
let create_secondary_text text =
  Ui.string ~attr:Typography.secondary text

(** Typography helper for dimmed/inactive text *)
let create_dimmed_text text =
  Ui.string ~attr:Typography.dimmed text

(** Create horizontal separator line *)
let create_separator ?(width = 80) ?(char = Borders.(get_border_style ()).horizontal) () =
  let line = String.make width char.[0] in
  Ui.string ~attr:Colors.border line

(** Create vertical separator line *)
let create_vertical_separator ?(height = 24) ?(char = Borders.(get_border_style ()).vertical) () =
  let rec make_vertical n acc =
    if n <= 0 then acc
    else make_vertical (n - 1) (Ui.string ~attr:Colors.border char :: acc)
  in
  Ui.vcat (make_vertical height [])

(** Create progress bar with consistent styling *)
let create_progress_bar width percentage =
  let filled = min width (max 0 (int_of_float (percentage *. float_of_int width /. 100.0))) in
  let filled_bar = String.make filled '#'
  and empty_bar = String.make (width - filled) '.' in
  let bar = filled_bar ^ empty_bar in
  Ui.string ~attr:Colors.primary_text bar

(** Create colored progress bar with status indication *)
let create_colored_progress_bar width percentage =
  let filled = min width (max 0 (int_of_float (percentage *. float_of_int width /. 100.0))) in
  let color = if percentage > 90.0 then Colors.error
              else if percentage > 70.0 then Colors.warning
              else Colors.success in
  let filled_bar = String.make filled '#'
  and empty_bar = String.make (width - filled) '.' in
  let bar = Ui.string ~attr:color filled_bar ^ Ui.string ~attr:Colors.secondary_text empty_bar in
  bar

(** Create bordered panel with standardized styling *)
let create_bordered_panel ~title ?(focused = false) ?max_width ?max_height content =
  let border_style = Borders.get_border_style () in
  let border_char = border_style.vertical in

  (* Border colors based on focus state *)
  let border_attr = if focused then Colors.border_active else Colors.border in
  let title_attr = if focused then Typography.focus_highlight else Typography.heading in

  (* Adaptive border characters based on available space *)
  let (top_left, top_right, bottom_left, bottom_right) =
    if focused then (border_style.top_left, border_style.top_right,
                     border_style.bottom_left, border_style.bottom_right)
    else (border_style.top_left, border_style.top_right,
          border_style.bottom_left, border_style.bottom_right)
  in

  (* Adaptive title - truncate if too long for available space *)
  let title_base = " " ^ title ^ " " in
  let title_display = match max_width with
    | Some max_w when String.length title_base > max_w - 4 ->
        (* Leave space for borders, truncate title *)
        let available_title_space = max 3 (max_w - 4) in
        let truncated_title = String.sub title 0 (min (String.length title) (available_title_space - 2)) in
        Ui.string ~attr:title_attr (" " ^ truncated_title ^ " ")
    | _ -> Ui.string ~attr:title_attr title_base
  in

  (* Create border lines *)
  let create_border_line left_char right_char =
    Ui.hcat [
      Ui.string ~attr:border_attr left_char;
      Ui.string ~attr:border_attr right_char;
    ]
  in

  let top_border = Ui.hcat [
    Ui.string ~attr:border_attr top_left;
    title_display;
    Ui.string ~attr:border_attr top_right;
  ] in

  let bottom_border = create_border_line bottom_left bottom_right in

  (* Apply size constraints to content if specified *)
  let constrained_content = match max_width, max_height with
    | Some w, Some h ->
        (* Constrain both dimensions *)
        Ui.resize ~w ~h content
    | Some w, None ->
        (* Constrain width only *)
        Ui.resize ~w content
    | None, Some h ->
        (* Constrain height only - let width expand *)
        Ui.resize ~h content
    | None, None ->
        (* No constraints *)
        content
  in

  (* Content with side borders *)
  let content_with_borders = Ui.hcat [
    Ui.string ~attr:border_attr border_char;
    Ui.string ~attr:Ui.A.empty " ";
    constrained_content;
    Ui.string ~attr:Ui.A.empty " ";
    Ui.string ~attr:border_attr border_char;
  ] in

  (* Stack vertically: top border, content, bottom border *)
  Ui.vcat [top_border; content_with_borders; bottom_border]

(** Create table row with consistent styling *)
let create_table_row columns widths alignments =
  let create_cell content width alignment =
    let padded_content = match alignment with
      | `Left -> Printf.sprintf "%-*s" width content
      | `Right -> Printf.sprintf "%*s" width content
      | `Center ->
          let left_pad = (width - String.length content) / 2 in
          let right_pad = width - String.length content - left_pad in
          Printf.sprintf "%*s%s%*s" left_pad "" content right_pad ""
    in
    Ui.string ~attr:Typography.body padded_content
  in
  Ui.hcat (List.map2 (fun (content, alignment) width ->
    create_cell content width alignment
  ) (List.combine columns alignments) widths)

(** Create table header with bold styling *)
let create_table_header columns widths alignments =
  let create_header_cell content width alignment =
    let padded_content = match alignment with
      | `Left -> Printf.sprintf "%-*s" width content
      | `Right -> Printf.sprintf "%*s" width content
      | `Center ->
          let left_pad = (width - String.length content) / 2 in
          let right_pad = width - String.length content - left_pad in
          Printf.sprintf "%*s%s%*s" left_pad "" content right_pad ""
    in
    Ui.string ~attr:Typography.label padded_content
  in
  Ui.hcat (List.map2 (fun (content, alignment) width ->
    create_header_cell content width alignment
  ) (List.combine columns alignments) widths)

(** Create data table with headers and rows *)
let create_table headers rows column_widths alignments =
  let header_row = create_table_header headers column_widths alignments in
  let separator = create_separator ~width:(List.fold_left (+) 0 column_widths) () in
  let data_rows = List.map (fun row ->
    create_table_row row column_widths alignments
  ) rows in
  Ui.vcat (header_row :: separator :: data_rows)

(** Create status bar with consistent separators *)
let create_status_bar ?(separator = " │ ") elements =
  let separator_widget = Ui.string ~attr:Colors.secondary_text separator in
  let rec build_bar = function
    | [] -> Ui.empty
    | [last] -> last
    | hd :: tl -> Ui.hcat [hd; separator_widget; build_bar tl]
  in
  build_bar elements

(** Create status bar element with specific styling *)
let create_status_element text attr =
  Ui.string ~attr text

(** Helper to create common status bar elements *)
let create_primary_status text =
  create_status_element text Typography.body

let create_secondary_status text =
  create_status_element text Typography.secondary

let create_highlighted_status text =
  create_status_element text Typography.label_accent

let create_success_status text =
  create_status_element text Typography.success_text

let create_warning_status text =
  create_status_element text Typography.warning_text

let create_error_status text =
  create_status_element text Typography.error_text
