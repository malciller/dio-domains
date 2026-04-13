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
  else Printf.sprintf "%.0fµs" f

let utf8_len s =
  let len = ref 0 in
  for i = 0 to String.length s - 1 do
    if Char.code s.[i] land 0xC0 <> 0x80 then incr len
  done;
  !len

let truncate_string n s =
  if utf8_len s <= n then s
  else String.sub s 0 (n - 1) ^ "."

(* Color palette: Tokyo Night RGB-888 constants for the TUI theme *)

let c_bg         = A.rgb_888 ~r:26  ~g:27  ~b:38
let c_panel      = A.rgb_888 ~r:22  ~g:22  ~b:30
let c_section_bg = A.rgb_888 ~r:36  ~g:40  ~b:59
let c_border     = A.rgb_888 ~r:65  ~g:72  ~b:104
let c_title      = A.rgb_888 ~r:192 ~g:202 ~b:245
let c_accent     = A.rgb_888 ~r:187 ~g:154 ~b:247
let c_label      = A.rgb_888 ~r:86  ~g:95  ~b:137
let c_text       = A.rgb_888 ~r:169 ~g:177 ~b:214
let c_bright     = A.rgb_888 ~r:255 ~g:255 ~b:255
let c_green      = A.rgb_888 ~r:158 ~g:206 ~b:106
let c_red        = A.rgb_888 ~r:247 ~g:118 ~b:142
let c_yellow     = A.rgb_888 ~r:224 ~g:175 ~b:104
let c_cyan       = A.rgb_888 ~r:125 ~g:207 ~b:255
let c_dim        = A.rgb_888 ~r:86  ~g:95  ~b:137
let c_near_fill  = A.rgb_888 ~r:29  ~g:42  ~b:60
let c_near_sell  = A.rgb_888 ~r:65  ~g:35  ~b:55

(* Per-exchange brand colors (adapted for dark mode) *)
let c_exch_hl    = A.rgb_888 ~r:73  ~g:177 ~b:121
let c_exch_kr    = A.rgb_888 ~r:187 ~g:154 ~b:247
let c_exch_li    = A.rgb_888 ~r:125 ~g:207 ~b:255
let c_exch_ib    = A.rgb_888 ~r:255 ~g:158 ~b:100

(* Basis Point generic styling (Pink/Magenta scale to avoid exchange collision) *)
let c_bps_tight  = A.rgb_888 ~r:247 ~g:118 ~b:142 (* Tokyo Night Red/Pink *)
let c_bps_norm   = A.rgb_888 ~r:226 ~g:104 ~b:160 (* Magenta *)
let c_bps_wide   = A.rgb_888 ~r:164 ~g:60  ~b:100  (* Dark Magenta *)
let c_bps_xtrm   = A.rgb_888 ~r:86  ~g:95  ~b:137  (* Dim *)

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

let a_bps_tight  = A.(fg c_bps_tight ++ bg c_bg)
let a_bps_norm   = A.(fg c_bps_norm  ++ bg c_bg)
let a_bps_wide   = A.(fg c_bps_wide  ++ bg c_bg)
let a_bps_xtrm   = A.(fg c_bps_xtrm  ++ bg c_bg)

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
  let len = utf8_len s in
  if len >= w then s (* return intact to prevent splitting utf8 bytes *)
  else s ^ String.make (w - len) ' '

let col w attr s = I.string attr (pad_right w s)

let pad_left w s =
  let len = utf8_len s in
  if len >= w then s (* return intact to prevent splitting utf8 bytes *)
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

let render_progress_bar w ratio attr =
  let fill_w = int_of_float (float (max 0 (w - 2)) *. ratio) in
  let fill_w = max 0 (min (w - 2) fill_w) in
  let empty_w = w - 2 - fill_w in
  let fill_str = String.concat "" (List.init fill_w (fun _ -> "⣿")) in
  let empty_str = String.concat "" (List.init empty_w (fun _ -> "─")) in
  I.hcat [
    I.string a_border "[";
    I.string attr fill_str;
    I.string a_dim empty_str;
    I.string a_border "]";
  ]

let block_chars = [| "⠀"; "⣀"; "⣄"; "⣤"; "⣦"; "⣶"; "⣷"; "⣿" |]

let render_sparkline w data max_val attr_fn =
  let len = Array.length data in
  let start_idx = max 0 (len - w) in
  let visible_len = min w len in
  let empty_w = w - visible_len in
  let blocks = List.init visible_len (fun i ->
    let v = data.(start_idx + i) in
    let ratio = if max_val > 0.0 then v /. max_val else 0.0 in
    let ratio = max 0.0 (min 1.0 ratio) in
    let block_idx = int_of_float (ratio *. 7.0) in
    let block_idx = max 0 (min 7 block_idx) in
    I.string (attr_fn v) block_chars.(block_idx)
  ) in
  I.hcat (I.string a_dim (String.make empty_w ' ') :: blocks)

(* Gradient utilities for 3D/shaded aesthetics *)

let color_blend (r1, g1, b1) (r2, g2, b2) ratio =
  let clamp x = max 0 (min 255 x) in
  let r = r1 + int_of_float (float (r2 - r1) *. ratio) in
  let g = g1 + int_of_float (float (g2 - g1) *. ratio) in
  let b = b1 + int_of_float (float (b2 - b1) *. ratio) in
  A.rgb_888 ~r:(clamp r) ~g:(clamp g) ~b:(clamp b)

let section_title w label =
  let lbl = " ╭── " ^ label ^ " " in
  let lbl_img = I.string A.(fg c_title ++ bg c_bg ++ st bold) lbl in
  let len = I.width lbl_img in
  let pad_count = max 0 (w - len - 1) in
  
  (* Extract direct RGB values for the gradient *)
  let left_rgb = (187, 154, 247) (* c_accent *) in
  let right_rgb = (26, 27, 38)   (* c_bg *) in
  
  let gradient_lines = List.init pad_count (fun i ->
    let ratio = float i /. float (max 1 (pad_count - 1)) in
    (* Ease out the gradient for a smoother fade effect using x^2 *)
    let fade = ratio *. ratio in
    let c = color_blend left_rgb right_rgb fade in
    I.string A.(fg c ++ bg c_bg) "─"
  ) in
  
  let end_border = I.string A.(fg c_border ++ bg c_bg) "╮" in
  
  I.hcat (
    lbl_img ::
    gradient_lines @ [end_border]
  )

let section_footer w =
  let pad_count = max 0 (w - 5) in
  let pad_buf = Buffer.create pad_count in
  for _ = 1 to pad_count do Buffer.add_string pad_buf "─" done;
  I.string A.(fg c_border ++ bg c_bg) (" ╰──" ^ Buffer.contents pad_buf ^ "╯")

let close_row target_w img =
  let d = target_w - I.width img - 2 in
  let d = max 0 d in
  I.hcat [ img; I.void d 1; I.string A.(fg c_border ++ bg c_bg) " │" ]
