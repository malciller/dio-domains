open Notty

(* JSON helpers *)

let ( |?> ) json key =
  match json with
  | `Assoc l -> (try List.assoc key l with Not_found -> `Null)
  | _ -> `Null

let to_string_d d = function `String s -> s | _ -> d
let to_float_d d = function `Float f -> f | `Int i -> float_of_int i | _ -> d
let to_int_d d = function `Int i -> i | `Float f -> int_of_float f | _ -> d
let to_bool_d d = function `Bool b -> b | _ -> d
let to_list_d = function `List l -> l | _ -> []

(* Formatting *)

let format_duration secs =
  let s = int_of_float secs in
  if s < 60 then Printf.sprintf "%ds" s
  else if s < 3600 then Printf.sprintf "%dm%02ds" (s / 60) (s mod 60)
  else if s < 86400 then Printf.sprintf "%dh%02dm" (s / 3600) ((s mod 3600) / 60)
  else Printf.sprintf "%dd%02dh" (s / 86400) ((s mod 86400) / 3600)

let format_price f =
  if f >= 10000.0 then Printf.sprintf "$%.0f" f
  else if f >= 100.0 then Printf.sprintf "$%.1f" f
  else if f >= 1.0 then Printf.sprintf "$%.2f" f
  else Printf.sprintf "$%.4f" f

let format_qty f =
  if f >= 1.0 then Printf.sprintf "%.4f" f
  else Printf.sprintf "%.6f" f

let format_pnl f =
  if f >= 0.0 then Printf.sprintf "+$%.2f" f
  else Printf.sprintf "-$%.2f" (abs_float f)

let format_latency_us f =
  if f >= 1000.0 then Printf.sprintf "%.1fms" (f /. 1000.0)
  else Printf.sprintf "%.0fus" f

let truncate_string n s =
  if String.length s <= n then s
  else String.sub s 0 (n - 1) ^ "."

(* Color palette: RGB-888 constants for the TUI theme *)

let c_bg         = A.rgb_888 ~r:14  ~g:14  ~b:18
let c_panel      = A.rgb_888 ~r:24  ~g:24  ~b:32
let c_section_bg = A.rgb_888 ~r:30  ~g:30  ~b:40
let c_border     = A.rgb_888 ~r:60  ~g:60  ~b:75
let c_title      = A.rgb_888 ~r:255 ~g:255 ~b:255
let c_accent     = A.rgb_888 ~r:167 ~g:139 ~b:250
let c_label      = A.rgb_888 ~r:150 ~g:150 ~b:165
let c_text       = A.rgb_888 ~r:230 ~g:230 ~b:240
let c_bright     = A.rgb_888 ~r:255 ~g:255 ~b:255
let c_green      = A.rgb_888 ~r:52  ~g:211 ~b:153
let c_red        = A.rgb_888 ~r:248 ~g:113 ~b:113
let c_yellow     = A.rgb_888 ~r:251 ~g:191 ~b:36
let c_cyan       = A.rgb_888 ~r:34  ~g:211 ~b:238
let c_dim        = A.rgb_888 ~r:100 ~g:100 ~b:120
let c_near_fill  = A.rgb_888 ~r:25  ~g:45  ~b:35
let c_near_sell  = A.rgb_888 ~r:75  ~g:35  ~b:35

(* Per-exchange brand colors *)
let c_exch_hl    = A.rgb_888 ~r:52  ~g:211 ~b:153
let c_exch_kr    = A.rgb_888 ~r:192 ~g:132 ~b:252
let c_exch_li    = A.rgb_888 ~r:96  ~g:165 ~b:250
let c_exch_ib    = A.rgb_888 ~r:251 ~g:146 ~b:60

(* Attribute constructors: foreground + background + optional style *)

let a_label      = A.(fg c_label  ++ bg c_bg)
let a_text       = A.(fg c_text   ++ bg c_bg)
let a_bright     = A.(fg c_bright ++ bg c_bg         ++ st bold)
let a_green      = A.(fg c_green  ++ bg c_bg)
let a_red        = A.(fg c_red    ++ bg c_bg)
let a_yellow     = A.(fg c_yellow ++ bg c_bg)
let a_cyan       = A.(fg c_cyan   ++ bg c_bg)
let a_dim        = A.(fg c_dim    ++ bg c_bg)
let a_border     = A.(fg c_border ++ bg c_bg)

let a_near_fill  = A.(fg c_bright ++ bg c_near_fill ++ st bold)
let a_near_fill_green = A.(fg c_green ++ bg c_near_fill ++ st bold)

let a_near_sell  = A.(fg c_bright ++ bg c_near_sell ++ st bold)
let a_near_sell_red = A.(fg c_red ++ bg c_near_sell ++ st bold)

(** Exchange-specific color for the SYMBOL column. *)
let exch_sym_attr ?(dim=false) exchange =
  let c = match exchange with
    | "hyperliquid" -> c_exch_hl
    | "kraken"      -> c_exch_kr
    | "lighter"     -> c_exch_li
    | "ibkr"        -> c_exch_ib
    | _             -> c_bright
  in
  if dim then A.(fg c ++ bg c_bg)
  else A.(fg c ++ bg c_bg ++ st bold)

let exch_tag_of = function
  | "kraken" -> "krkn" | "hyperliquid" -> "hypr"
  | "lighter" -> "ltr" | "ibkr" -> "ibkr"
  | e -> String.sub e 0 (min 3 (String.length e))

(* Drawing primitives *)

let pad_right w s =
  let len = String.length s in
  if len >= w then String.sub s 0 w
  else s ^ String.make (w - len) ' '

let col w attr s = I.string attr (pad_right w s)

let pad_left w s =
  let len = String.length s in
  if len >= w then String.sub s 0 w
  else String.make (w - len) ' ' ^ s

let col_right w attr s = I.string attr (pad_left w s)

let format_pct f =
  if abs_float f < 0.01 then "<0.01%"
  else if abs_float f >= 10.0 then Printf.sprintf "%.1f%%" f
  else Printf.sprintf "%.2f%%" f

let format_spread_bps bid ask =
  if bid > 0.0 && ask > 0.0 then
    let spread_bps = ((ask -. bid) /. ((bid +. ask) /. 2.0)) *. 10000.0 in
    if spread_bps >= 100.0 then Printf.sprintf "%.0fbp" spread_bps
    else Printf.sprintf "%.1fbp" spread_bps
  else "--"

(* Drawing helpers *)

let section_title w label =
  let lbl = " ╭── " ^ label ^ " " in
  let pad_count = max 0 (w - String.length lbl) in
  let pad_buf = Buffer.create (pad_count * 3) in
  for _ = 1 to pad_count do Buffer.add_string pad_buf "─" done;
  I.hcat [
    I.string A.(fg c_title ++ bg c_bg ++ st bold) lbl;
    I.string A.(fg c_border ++ bg c_bg) (Buffer.contents pad_buf);
  ]
