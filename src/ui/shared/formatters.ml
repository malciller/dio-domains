(** Data Formatting Module

    Standardized formatting utilities for consistent data display
    across all UI components in the Bloomberg-style terminal interface.
*)

(** Number formatting with consistent precision *)
let format_decimal value precision =
  Printf.sprintf "%.*f" precision value

let format_percentage value =
  Printf.sprintf "%.1f%%" (value *. 100.0)

let format_currency value =
  if abs_float value >= 1000000.0 then
    Printf.sprintf "$%.1fM" (value /. 1000000.0)
  else if abs_float value >= 1000.0 then
    Printf.sprintf "$%.1fK" (value /. 1000.0)
  else if abs_float value >= 100.0 then
    Printf.sprintf "$%.1f" value
  else
    Printf.sprintf "$%.2f" value

(** Right-align numbers in a fixed width field *)
let align_right text width =
  let len = String.length text in
  if len >= width then text
  else String.make (width - len) ' ' ^ text

(** Left-align text in a fixed width field *)
let align_left text width =
  let len = String.length text in
  if len >= width then text
  else text ^ String.make (width - len) ' '

(** Center-align text in a fixed width field *)
let align_center text width =
  let len = String.length text in
  if len >= width then text
  else
    let left_pad = (width - len) / 2 in
    let right_pad = width - len - left_pad in
    String.make left_pad ' ' ^ text ^ String.make right_pad ' '

(** Format integers with optional thousands separators *)
let format_integer ?(thousands_sep = false) value =
  if not thousands_sep then
    string_of_int value
  else
    let rec add_separators str =
      let len = String.length str in
      if len <= 3 then str
      else
        let prefix = String.sub str 0 (len - 3) in
        let suffix = String.sub str (len - 3) 3 in
        add_separators prefix ^ "," ^ suffix
    in
    add_separators (string_of_int value)

(** Format byte sizes with appropriate units *)
let format_bytes bytes =
  let units = ["B"; "KB"; "MB"; "GB"; "TB"; "PB"] in
  let rec format value unit_list =
    match unit_list with
    | [] -> Printf.sprintf "%.1f PB" value
    | unit :: rest ->
        if value < 1024.0 then
          Printf.sprintf "%.1f %s" value unit
        else
          format (value /. 1024.0) rest
  in
  format (float_of_int bytes) units

(** Format time durations *)
let format_duration seconds =
  let hours = int_of_float (seconds /. 3600.0) in
  let minutes = int_of_float (seconds /. 60.0) mod 60 in
  let secs = int_of_float seconds mod 60 in

  if hours > 0 then
    Printf.sprintf "%dh%02dm%02ds" hours minutes secs
  else if minutes > 0 then
    Printf.sprintf "%dm%02ds" minutes secs
  else
    Printf.sprintf "%ds" secs

(** Format timestamps *)
let format_timestamp ?(format = "%H:%M:%S") timestamp =
  let tm = Unix.localtime timestamp in
  let fmt =
    match format with
    | "%H:%M:%S" -> Printf.sprintf "%02d:%02d:%02d" tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec
    | "%H:%M" -> Printf.sprintf "%02d:%02d" tm.Unix.tm_hour tm.Unix.tm_min
    | "%Y-%m-%d %H:%M:%S" -> Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d"
        (tm.Unix.tm_year + 1900) (tm.Unix.tm_mon + 1) tm.Unix.tm_mday
        tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec
    | _ -> Printf.sprintf "%02d:%02d:%02d" tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec
  in
  fmt

(** Truncate strings with ellipsis *)
let truncate_with_ellipsis text max_length =
  if String.length text <= max_length then text
  else if max_length <= 3 then String.sub text 0 max_length
  else String.sub text 0 (max_length - 3) ^ "..."

(** Truncate strings without ellipsis (hard cut) *)
let truncate_hard text max_length =
  if String.length text <= max_length then text
  else String.sub text 0 max_length

(** Format rate values (per second) *)
let format_rate value =
  if value >= 1000000.0 then
    Printf.sprintf "%.1fM/sec" (value /. 1000000.0)
  else if value >= 1000.0 then
    Printf.sprintf "%.1fK/sec" (value /. 1000.0)
  else if value >= 1.0 then
    Printf.sprintf "%.1f/sec" value
  else
    Printf.sprintf "%.3f/sec" value

(** Format ratios as percentages *)
let format_ratio numerator denominator =
  if denominator = 0.0 then "N/A"
  else Printf.sprintf "%.1f%%" ((numerator /. denominator) *. 100.0)

(** Safe division that returns 0.0 on division by zero *)
let safe_divide numerator denominator =
  if denominator = 0.0 then 0.0 else numerator /. denominator

(** Format boolean values consistently *)
let format_bool value =
  if value then "Yes" else "No"

(** Format status indicators *)
let format_status ?(success = "OK") ?(failure = "FAIL") condition =
  if condition then success else failure

(** Table formatting helpers *)
type column_alignment = Left | Right | Center

type table_column = {
  header: string;
  width: int;
  alignment: column_alignment;
}

(** Format a table row with consistent alignment *)
let format_table_row columns data =
  let format_cell content alignment width =
    match alignment with
    | Left -> align_left content width
    | Right -> align_right content width
    | Center -> align_center content width
  in
  let formatted_cells = List.map2 (fun cell col ->
    format_cell cell col.alignment col.width
  ) data columns in
  String.concat " " formatted_cells

(** Format table header *)
let format_table_header columns =
  let headers = List.map (fun col -> col.header) columns in
  format_table_row columns headers

(** Format table separator *)
let format_table_separator columns =
  let separators = List.map (fun col ->
    String.make col.width '-'
  ) columns in
  String.concat "-" separators
