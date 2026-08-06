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

let add_commas s =
  let len = String.length s in
  let rec find_first_digit i =
    if i >= len then None
    else match s.[i] with
    | '0'..'9' -> Some i
    | _ -> find_first_digit (i + 1)
  in
  match find_first_digit 0 with
  | None -> s
  | Some prefix_end ->
      let rec find_int_end i =
        if i >= len then i
        else match s.[i] with
        | '0'..'9' -> find_int_end (i + 1)
        | _ -> i
      in
      let int_end = find_int_end prefix_end in
      let int_len = int_end - prefix_end in
      if int_len <= 3 then s
      else
        let int_str = String.sub s prefix_end int_len in
        let rem = int_len mod 3 in
        let buf = Buffer.create (int_len + int_len / 3) in
        if rem > 0 then begin
          Buffer.add_substring buf int_str 0 rem;
          Buffer.add_char buf ','
        end;
        let rec loop i =
          if i < int_len then begin
            if i > rem && (i - rem) mod 3 = 0 then Buffer.add_char buf ',';
            Buffer.add_char buf int_str.[i];
            loop (i + 1)
          end
        in
        loop rem;
        let prefix = String.sub s 0 prefix_end in
        let suffix = String.sub s int_end (len - int_end) in
        prefix ^ Buffer.contents buf ^ suffix

let format_duration secs =
  let s = int_of_float secs in
  if s < 60 then Printf.sprintf "%ds" s
  else if s < 3600 then Printf.sprintf "%dm%02ds" (s / 60) (s mod 60)
  else if s < 86400 then Printf.sprintf "%dh%02dm" (s / 3600) ((s mod 3600) / 60)
  else Printf.sprintf "%dd%02dh" (s / 86400) ((s mod 86400) / 3600)

let subscript_digit = function
  | '0' -> "₀" | '1' -> "₁" | '2' -> "₂" | '3' -> "₃" | '4' -> "₄"
  | '5' -> "₅" | '6' -> "₆" | '7' -> "₇" | '8' -> "₈" | '9' -> "₉"
  | c -> String.make 1 c

let format_subscript_zeros count sig_digits =
  let count_str = string_of_int count in
  let sub_str = String.concat "" (List.init (String.length count_str) (fun i -> subscript_digit count_str.[i])) in
  "$0.0" ^ sub_str ^ sig_digits

let format_price f =
  if f <= 0.0 then "--" else
  if f >= 1000.0 then
    add_commas (Printf.sprintf "$%.2f" f)
  else if f >= 1.0 then
    add_commas (Printf.sprintf "$%.4f" f)
  else if f >= 0.0001 then
    Printf.sprintf "$%.6f" f
  else
    let s = Printf.sprintf "%.10f" f in
    match String.index_opt s '.' with
    | None -> add_commas (Printf.sprintf "$%.4f" f)
    | Some dot_idx ->
        let rest = String.sub s (dot_idx + 1) (String.length s - dot_idx - 1) in
        let rec count_zeros i =
          if i >= String.length rest then i
          else if rest.[i] = '0' then count_zeros (i + 1)
          else i
        in
        let zero_cnt = count_zeros 0 in
        if zero_cnt >= 4 then
          let sig_part = String.sub rest zero_cnt (min 4 (String.length rest - zero_cnt)) in
          format_subscript_zeros zero_cnt sig_part
        else
          Printf.sprintf "$%.6f" f



let format_usd f =
  let raw =
    if f < 0.0 then Printf.sprintf "-$%.2f" (abs_float f)
    else Printf.sprintf "$%.2f" f
  in
  add_commas raw

let trim_zeros s =
  if String.contains s '.' then
    let len = String.length s in
    let rec find_end i =
      if i <= 0 then i
      else match s.[i] with
      | '0' -> find_end (i - 1)
      | '.' -> i - 1
      | _ -> i
    in
    let last_idx = find_end (len - 1) in
    String.sub s 0 (last_idx + 1)
  else s

let format_qty f =
  let raw =
    if f >= 1000.0 then Printf.sprintf "%.2f" f
    else if f >= 1.0 then Printf.sprintf "%.4f" f
    else if f >= 0.0001 then Printf.sprintf "%.6f" f
    else Printf.sprintf "%.8f" f
  in
  trim_zeros (add_commas raw)


let format_pnl f =
  let raw =
    if f >= 0.0 then Printf.sprintf "+$%.2f" f
    else Printf.sprintf "-$%.2f" (abs_float f)
  in
  add_commas raw

let format_latency_us f =
  let raw =
    if f >= 1000.0 then Printf.sprintf "%.1fms" (f /. 1000.0)
    else Printf.sprintf "%.0fµs" f
  in
  add_commas raw

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
let c_magenta    = A.rgb_888 ~r:226 ~g:104 ~b:160

(* Per-exchange brand colors (adapted for dark mode) *)
let c_exch_hl    = A.rgb_888 ~r:73  ~g:177 ~b:121
let c_exch_kr    = A.rgb_888 ~r:187 ~g:154 ~b:247
let c_exch_li    = A.rgb_888 ~r:125 ~g:207 ~b:255
let c_exch_ib    = A.rgb_888 ~r:255 ~g:158 ~b:100

(* Basis Point generic styling (Pink/Magenta scale to avoid exchange collision) *)
let c_bps_tight  = A.rgb_888 ~r:125 ~g:207 ~b:255 (* Cyan *)
let c_bps_norm   = A.rgb_888 ~r:86  ~g:95  ~b:137 (* Dim *)
let c_bps_wide   = A.rgb_888 ~r:226 ~g:104 ~b:160 (* Magenta *)
let c_bps_xtrm   = A.rgb_888 ~r:247 ~g:118 ~b:142 (* Tokyo Night Red/Pink *)

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
  let raw =
    if abs_float f < 0.01 then "<0.01%"
    else if abs_float f >= 10.0 then Printf.sprintf "%.1f%%" f
    else Printf.sprintf "%.2f%%" f
  in
  add_commas raw

let format_spread_bps bid ask =
  if bid > 0.0 && ask > 0.0 then
    let spread_bps = ((ask -. bid) /. ((bid +. ask) /. 2.0)) *. 10000.0 in
    let raw =
      if spread_bps >= 100.0 then Printf.sprintf "%.0fbp" spread_bps
      else Printf.sprintf "%.1fbp" spread_bps
    in
    add_commas raw
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
  let max_inner = max 0 (target_w - 2) in
  let img_cropped = I.hsnap ~align:`Left max_inner img in
  I.hcat [ img_cropped; I.string A.(fg c_border ++ bg c_bg) " │" ]

let render_proximity_slider w pos_pct_opt =
  let inner_w = max 3 (w - 2) in
  match pos_pct_opt with
  | None ->
      let mid_w = inner_w / 2 in
      let left_dashes = String.concat "" (List.init mid_w (fun _ -> "─")) in
      let right_dashes = String.concat "" (List.init (inner_w - 1 - mid_w) (fun _ -> "─")) in
      I.hcat [
        I.string a_border "├";
        I.string a_dim left_dashes;
        I.string a_cyan "•";
        I.string a_dim right_dashes;
        I.string a_border "┤";
      ]
  | Some pos ->
      let clamped = max 0.0 (min 100.0 pos) in
      let dot_idx = int_of_float ((clamped /. 100.0) *. float (inner_w - 1)) in
      let dot_idx = max 0 (min (inner_w - 1) dot_idx) in
      let left_w = dot_idx in
      let right_w = inner_w - 1 - left_w in
      let dot_attr =
        if clamped <= 20.0 then A.(fg c_green ++ st bold)
        else if clamped >= 80.0 then A.(fg c_red ++ st bold)
        else A.(fg c_cyan ++ st bold)
      in
      I.hcat [
        I.string a_green "┠";
        I.string a_green (String.concat "" (List.init left_w (fun _ -> "━")));
        I.string dot_attr "◈";
        I.string a_red (String.concat "" (List.init right_w (fun _ -> "━")));
        I.string a_red "┨";
      ]

let render_card w title content_rows =
  let title_str = " ╭── " ^ title ^ " " in
  let title_img = I.string A.(fg c_title ++ bg c_bg ++ st bold) title_str in
  let title_len = I.width title_img in
  let fill_w = max 0 (w - title_len - 1) in
  let top_bar = I.hcat [
    title_img;
    I.string A.(fg c_border ++ bg c_bg) (String.concat "" (List.init fill_w (fun _ -> "─")));
    I.string A.(fg c_border ++ bg c_bg) "╮";
  ] in
  let inner_w = max 0 (w - 5) in
  let body_rows = List.map (fun row_img ->
    let row_cropped = I.hsnap ~align:`Left inner_w row_img in
    I.hcat [
      I.string A.(fg c_border ++ bg c_bg) " │ ";
      row_cropped;
      I.string A.(fg c_border ++ bg c_bg) " │";
    ]
  ) content_rows in
  let bot_fill = max 0 (w - 2) in
  let bot_bar = I.hcat [
    I.string A.(fg c_border ++ bg c_bg) " ╰";
    I.string A.(fg c_border ++ bg c_bg) (String.concat "" (List.init bot_fill (fun _ -> "─")));
    I.string A.(fg c_border ++ bg c_bg) "╯";
  ] in
  I.vcat (top_bar :: body_rows @ [bot_bar])




