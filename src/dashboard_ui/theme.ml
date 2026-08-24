open Notty

(* JSON helpers *)

let ( |?> ) json key =
  match json with
  | `Assoc l ->
    (try List.assoc key l with
     | Not_found -> `Null)
  | _ -> `Null
;;

let to_string_d d = function
  | `String s -> s
  | _ -> d
;;

let to_float_d d = function
  | `Float f -> f
  | `Int i -> float_of_int i
  | _ -> d
;;

let to_int_d d = function
  | `Int i -> i
  | `Float f -> int_of_float f
  | _ -> d
;;

let to_bool_d d = function
  | `Bool b -> b
  | _ -> d
;;

let to_list_d = function
  | `List l -> l
  | _ -> []
;;

(* Formatting *)

let add_commas s =
  let len = String.length s in
  let rec find_first_digit i =
    if i >= len
    then None
    else (
      match s.[i] with
      | '0' .. '9' -> Some i
      | _ -> find_first_digit (i + 1))
  in
  match find_first_digit 0 with
  | None -> s
  | Some prefix_end ->
    let rec find_int_end i =
      if i >= len
      then i
      else (
        match s.[i] with
        | '0' .. '9' -> find_int_end (i + 1)
        | _ -> i)
    in
    let int_end = find_int_end prefix_end in
    let int_len = int_end - prefix_end in
    if int_len <= 3
    then s
    else (
      let int_str = String.sub s prefix_end int_len in
      let rem = int_len mod 3 in
      let buf = Buffer.create (int_len + (int_len / 3)) in
      if rem > 0
      then (
        Buffer.add_substring buf int_str 0 rem;
        Buffer.add_char buf ',');
      let rec loop i =
        if i < int_len
        then (
          if i > rem && (i - rem) mod 3 = 0 then Buffer.add_char buf ',';
          Buffer.add_char buf int_str.[i];
          loop (i + 1))
      in
      loop rem;
      let prefix = String.sub s 0 prefix_end in
      let suffix = String.sub s int_end (len - int_end) in
      prefix ^ Buffer.contents buf ^ suffix)
;;

let format_duration secs =
  let s = int_of_float secs in
  if s < 60
  then Printf.sprintf "%ds" s
  else if s < 3600
  then Printf.sprintf "%dm%02ds" (s / 60) (s mod 60)
  else if s < 86400
  then Printf.sprintf "%dh%02dm" (s / 3600) (s mod 3600 / 60)
  else Printf.sprintf "%dd%02dh" (s / 86400) (s mod 86400 / 3600)
;;

let subscript_digit = function
  | '0' -> "₀"
  | '1' -> "₁"
  | '2' -> "₂"
  | '3' -> "₃"
  | '4' -> "₄"
  | '5' -> "₅"
  | '6' -> "₆"
  | '7' -> "₇"
  | '8' -> "₈"
  | '9' -> "₉"
  | c -> String.make 1 c
;;

let format_subscript_zeros count sig_digits =
  let count_str = string_of_int count in
  let sub_str =
    String.concat
      ""
      (List.init (String.length count_str) (fun i -> subscript_digit count_str.[i]))
  in
  "$0.0" ^ sub_str ^ sig_digits
;;

let format_price f =
  if f <= 0.0
  then "--"
  else if f >= 1000.0
  then add_commas (Printf.sprintf "$%.2f" f)
  else if f >= 1.0
  then add_commas (Printf.sprintf "$%.4f" f)
  else if f >= 0.0001
  then Printf.sprintf "$%.6f" f
  else (
    let s = Printf.sprintf "%.10f" f in
    match String.index_opt s '.' with
    | None -> add_commas (Printf.sprintf "$%.4f" f)
    | Some dot_idx ->
      let rest = String.sub s (dot_idx + 1) (String.length s - dot_idx - 1) in
      let rec count_zeros i =
        if i >= String.length rest
        then i
        else if rest.[i] = '0'
        then count_zeros (i + 1)
        else i
      in
      let zero_cnt = count_zeros 0 in
      if zero_cnt >= 4
      then (
        let sig_part = String.sub rest zero_cnt (min 4 (String.length rest - zero_cnt)) in
        format_subscript_zeros zero_cnt sig_part)
      else Printf.sprintf "$%.6f" f)
;;

let format_usd f =
  let raw =
    if f < 0.0 then Printf.sprintf "-$%.2f" (abs_float f) else Printf.sprintf "$%.2f" f
  in
  add_commas raw
;;

let trim_zeros s =
  if String.contains s '.'
  then (
    let len = String.length s in
    let rec find_end i =
      if i <= 0
      then i
      else (
        match s.[i] with
        | '0' -> find_end (i - 1)
        | '.' -> i - 1
        | _ -> i)
    in
    let last_idx = find_end (len - 1) in
    String.sub s 0 (last_idx + 1))
  else s
;;

let format_qty f =
  let raw =
    if f >= 1000.0
    then Printf.sprintf "%.2f" f
    else if f >= 1.0
    then Printf.sprintf "%.4f" f
    else if f >= 0.0001
    then Printf.sprintf "%.6f" f
    else Printf.sprintf "%.8f" f
  in
  trim_zeros (add_commas raw)
;;

let format_pnl f =
  let abs_f = abs_float f in
  let sign = if f >= 0.0 then "+" else "-" in
  let raw =
    if abs_f >= 1000.0
    then sign ^ add_commas (Printf.sprintf "$%.2f" abs_f)
    else if abs_f >= 1.0
    then sign ^ Printf.sprintf "$%.2f" abs_f
    else if abs_f >= 0.0001
    then sign ^ Printf.sprintf "$%.4f" abs_f
    else if abs_f > 0.0
    then sign ^ Printf.sprintf "$%.6f" abs_f
    else "$0.00"
  in
  raw
;;

let format_latency_us f =
  let raw =
    if f >= 1_000_000.0
    then Printf.sprintf "%.1fs" (f /. 1_000_000.0)
    else if f >= 1000.0
    then Printf.sprintf "%.1fms" (f /. 1000.0)
    else if f >= 1.0
    then Printf.sprintf "%.0fµs" f
    else if f > 0.0
    then Printf.sprintf "%.0fns" (f *. 1000.0)
    else "0µs"
  in
  add_commas raw
;;

(** True for a sub-microsecond latency value (a fraction of a microsecond,
    rendered as nanoseconds) so callers can style nanosecond cells
    distinctly from microsecond cells. *)
let is_sub_us f = f > 0.0 && f < 1.0

let utf8_len s =
  let len = ref 0 in
  for i = 0 to String.length s - 1 do
    if Char.code s.[i] land 0xC0 <> 0x80 then incr len
  done;
  !len
;;

let truncate_string n s = if utf8_len s <= n then s else String.sub s 0 (n - 1) ^ "."

(* -------------------------------------------------------------------------- *)
(* Multi-Theme Palette System                                                 *)
(* -------------------------------------------------------------------------- *)

type theme_palette =
  { id : string
  ; name : string
  ; desc : string
  ; accent_rgb : int * int * int
  ; bg_rgb : int * int * int
  ; c_bg : A.color
  ; c_panel : A.color
  ; c_section_bg : A.color
  ; c_border : A.color
  ; c_title : A.color
  ; c_accent : A.color
  ; c_label : A.color
  ; c_text : A.color
  ; c_bright : A.color
  ; c_green : A.color
  ; c_green_dark : A.color
  ; c_red : A.color
  ; c_yellow : A.color
  ; c_cyan : A.color
  ; c_dim : A.color
  ; c_near_fill : A.color
  ; c_near_sell : A.color
  ; c_magenta : A.color
  ; c_selected : A.color
  ; c_exch_hl : A.color
  ; c_exch_kr : A.color
  ; c_exch_li : A.color
  ; c_exch_ib : A.color
  ; c_exch_alp : A.color
  ; c_bps_tight : A.color
  ; c_bps_norm : A.color
  ; c_bps_wide : A.color
  ; c_bps_xtrm : A.color
  ; a_label : A.t
  ; a_text : A.t
  ; a_bright : A.t
  ; a_green : A.t
  ; a_green_dark : A.t
  ; a_red : A.t
  ; a_yellow : A.t
  ; a_cyan : A.t
  ; a_dim : A.t
  ; a_border : A.t
  ; a_bps_tight : A.t
  ; a_bps_norm : A.t
  ; a_bps_wide : A.t
  ; a_bps_xtrm : A.t
  ; a_near_fill : A.t
  ; a_near_fill_green : A.t
  ; a_near_sell : A.t
  ; a_near_sell_red : A.t
  }

let make_theme
    ~id
    ~name
    ~desc
    ~accent_rgb
    ~bg_rgb
    ~c_bg
    ~c_panel
    ~c_section_bg
    ~c_border
    ~c_title
    ~c_accent
    ~c_label
    ~c_text
    ~c_bright
    ~c_green
    ~c_green_dark
    ~c_red
    ~c_yellow
    ~c_cyan
    ~c_dim
    ~c_near_fill
    ~c_near_sell
    ~c_magenta
    ~c_selected
    ~c_exch_hl
    ~c_exch_kr
    ~c_exch_li
    ~c_exch_ib
    ~c_exch_alp
    ~c_bps_tight
    ~c_bps_norm
    ~c_bps_wide
    ~c_bps_xtrm
  =
  let a_label = A.(fg c_label ++ bg c_bg) in
  let a_text = A.(fg c_text ++ bg c_bg) in
  let a_bright = A.(fg c_bright ++ bg c_bg ++ st bold) in
  let a_green = A.(fg c_green ++ bg c_bg) in
  let a_green_dark = A.(fg c_green_dark ++ bg c_bg) in
  let a_red = A.(fg c_red ++ bg c_bg) in
  let a_yellow = A.(fg c_yellow ++ bg c_bg) in
  let a_cyan = A.(fg c_cyan ++ bg c_bg) in
  let a_dim = A.(fg c_dim ++ bg c_bg) in
  let a_border = A.(fg c_border ++ bg c_bg) in
  let a_bps_tight = A.(fg c_bps_tight ++ bg c_bg) in
  let a_bps_norm = A.(fg c_bps_norm ++ bg c_bg) in
  let a_bps_wide = A.(fg c_bps_wide ++ bg c_bg) in
  let a_bps_xtrm = A.(fg c_bps_xtrm ++ bg c_bg) in
  let a_near_fill = A.(fg c_bright ++ bg c_near_fill ++ st bold) in
  let a_near_fill_green = A.(fg c_green ++ bg c_near_fill ++ st bold) in
  let a_near_sell = A.(fg c_bright ++ bg c_near_sell ++ st bold) in
  let a_near_sell_red = A.(fg c_red ++ bg c_near_sell ++ st bold) in
  { id
  ; name
  ; desc
  ; accent_rgb
  ; bg_rgb
  ; c_bg
  ; c_panel
  ; c_section_bg
  ; c_border
  ; c_title
  ; c_accent
  ; c_label
  ; c_text
  ; c_bright
  ; c_green
  ; c_green_dark
  ; c_red
  ; c_yellow
  ; c_cyan
  ; c_dim
  ; c_near_fill
  ; c_near_sell
  ; c_magenta
  ; c_selected
  ; c_exch_hl
  ; c_exch_kr
  ; c_exch_li
  ; c_exch_ib
  ; c_exch_alp
  ; c_bps_tight
  ; c_bps_norm
  ; c_bps_wide
  ; c_bps_xtrm
  ; a_label
  ; a_text
  ; a_bright
  ; a_green
  ; a_green_dark
  ; a_red
  ; a_yellow
  ; a_cyan
  ; a_dim
  ; a_border
  ; a_bps_tight
  ; a_bps_norm
  ; a_bps_wide
  ; a_bps_xtrm
  ; a_near_fill
  ; a_near_fill_green
  ; a_near_sell
  ; a_near_sell_red
  }
;;

(* 1. Tokyo Night (Original & Default) *)
let tokyo_night =
  make_theme
    ~id:"tokyo-night"
    ~name:"Tokyo Night"
    ~desc:"Original midnight blue palette with soft violet and cyan accents"
    ~accent_rgb:(187, 154, 247)
    ~bg_rgb:(26, 27, 38)
    ~c_bg:(A.rgb_888 ~r:26 ~g:27 ~b:38)
    ~c_panel:(A.rgb_888 ~r:22 ~g:22 ~b:30)
    ~c_section_bg:(A.rgb_888 ~r:36 ~g:40 ~b:59)
    ~c_border:(A.rgb_888 ~r:65 ~g:72 ~b:104)
    ~c_title:(A.rgb_888 ~r:192 ~g:202 ~b:245)
    ~c_accent:(A.rgb_888 ~r:187 ~g:154 ~b:247)
    ~c_label:(A.rgb_888 ~r:86 ~g:95 ~b:137)
    ~c_text:(A.rgb_888 ~r:169 ~g:177 ~b:214)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:158 ~g:206 ~b:106)
    ~c_green_dark:(A.rgb_888 ~r:82 ~g:136 ~b:89)
    ~c_red:(A.rgb_888 ~r:247 ~g:118 ~b:142)
    ~c_yellow:(A.rgb_888 ~r:224 ~g:175 ~b:104)
    ~c_cyan:(A.rgb_888 ~r:125 ~g:207 ~b:255)
    ~c_dim:(A.rgb_888 ~r:86 ~g:95 ~b:137)
    ~c_near_fill:(A.rgb_888 ~r:29 ~g:42 ~b:60)
    ~c_near_sell:(A.rgb_888 ~r:65 ~g:35 ~b:55)
    ~c_magenta:(A.rgb_888 ~r:226 ~g:104 ~b:160)
    ~c_selected:(A.rgb_888 ~r:55 ~g:60 ~b:115)
    ~c_exch_hl:(A.rgb_888 ~r:73 ~g:177 ~b:121)
    ~c_exch_kr:(A.rgb_888 ~r:187 ~g:154 ~b:247)
    ~c_exch_li:(A.rgb_888 ~r:125 ~g:207 ~b:255)
    ~c_exch_ib:(A.rgb_888 ~r:255 ~g:158 ~b:100)
    ~c_exch_alp:(A.rgb_888 ~r:246 ~g:193 ~b:119)
    ~c_bps_tight:(A.rgb_888 ~r:125 ~g:207 ~b:255)
    ~c_bps_norm:(A.rgb_888 ~r:86 ~g:95 ~b:137)
    ~c_bps_wide:(A.rgb_888 ~r:226 ~g:104 ~b:160)
    ~c_bps_xtrm:(A.rgb_888 ~r:247 ~g:118 ~b:142)
;;

(* 2. Cyberpunk Synthwave *)
let cyberpunk =
  make_theme
    ~id:"cyberpunk"
    ~name:"Cyberpunk"
    ~desc:"High-voltage neon pink, electric cyan, and deep midnight violet"
    ~accent_rgb:(255, 42, 133)
    ~bg_rgb:(18, 14, 36)
    ~c_bg:(A.rgb_888 ~r:18 ~g:14 ~b:36)
    ~c_panel:(A.rgb_888 ~r:12 ~g:10 ~b:26)
    ~c_section_bg:(A.rgb_888 ~r:32 ~g:22 ~b:58)
    ~c_border:(A.rgb_888 ~r:140 ~g:45 ~b:155)
    ~c_title:(A.rgb_888 ~r:255 ~g:120 ~b:220)
    ~c_accent:(A.rgb_888 ~r:255 ~g:42 ~b:133)
    ~c_label:(A.rgb_888 ~r:130 ~g:95 ~b:160)
    ~c_text:(A.rgb_888 ~r:235 ~g:225 ~b:255)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:0 ~g:255 ~b:159)
    ~c_green_dark:(A.rgb_888 ~r:0 ~g:145 ~b:95)
    ~c_red:(A.rgb_888 ~r:255 ~g:40 ~b:80)
    ~c_yellow:(A.rgb_888 ~r:255 ~g:230 ~b:0)
    ~c_cyan:(A.rgb_888 ~r:0 ~g:240 ~b:255)
    ~c_dim:(A.rgb_888 ~r:95 ~g:80 ~b:125)
    ~c_near_fill:(A.rgb_888 ~r:20 ~g:45 ~b:65)
    ~c_near_sell:(A.rgb_888 ~r:65 ~g:20 ~b:50)
    ~c_magenta:(A.rgb_888 ~r:255 ~g:0 ~b:128)
    ~c_selected:(A.rgb_888 ~r:80 ~g:32 ~b:105)
    ~c_exch_hl:(A.rgb_888 ~r:0 ~g:255 ~b:170)
    ~c_exch_kr:(A.rgb_888 ~r:255 ~g:42 ~b:133)
    ~c_exch_li:(A.rgb_888 ~r:0 ~g:240 ~b:255)
    ~c_exch_ib:(A.rgb_888 ~r:255 ~g:150 ~b:0)
    ~c_exch_alp:(A.rgb_888 ~r:255 ~g:230 ~b:0)
    ~c_bps_tight:(A.rgb_888 ~r:0 ~g:240 ~b:255)
    ~c_bps_norm:(A.rgb_888 ~r:95 ~g:80 ~b:125)
    ~c_bps_wide:(A.rgb_888 ~r:255 ~g:0 ~b:128)
    ~c_bps_xtrm:(A.rgb_888 ~r:255 ~g:40 ~b:80)
;;

(* 3. Nord (Arctic Frost) *)
let nord =
  make_theme
    ~id:"nord"
    ~name:"Nord"
    ~desc:"Clean arctic polar night slate with frosty blue and aurora accents"
    ~accent_rgb:(136, 192, 208)
    ~bg_rgb:(46, 52, 64)
    ~c_bg:(A.rgb_888 ~r:46 ~g:52 ~b:64)
    ~c_panel:(A.rgb_888 ~r:36 ~g:41 ~b:51)
    ~c_section_bg:(A.rgb_888 ~r:59 ~g:66 ~b:82)
    ~c_border:(A.rgb_888 ~r:76 ~g:86 ~b:106)
    ~c_title:(A.rgb_888 ~r:236 ~g:239 ~b:244)
    ~c_accent:(A.rgb_888 ~r:136 ~g:192 ~b:208)
    ~c_label:(A.rgb_888 ~r:140 ~g:152 ~b:175)
    ~c_text:(A.rgb_888 ~r:216 ~g:222 ~b:233)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:163 ~g:190 ~b:140)
    ~c_green_dark:(A.rgb_888 ~r:100 ~g:140 ~b:90)
    ~c_red:(A.rgb_888 ~r:191 ~g:97 ~b:106)
    ~c_yellow:(A.rgb_888 ~r:235 ~g:203 ~b:139)
    ~c_cyan:(A.rgb_888 ~r:143 ~g:188 ~b:187)
    ~c_dim:(A.rgb_888 ~r:94 ~g:104 ~b:125)
    ~c_near_fill:(A.rgb_888 ~r:35 ~g:55 ~b:65)
    ~c_near_sell:(A.rgb_888 ~r:65 ~g:40 ~b:45)
    ~c_magenta:(A.rgb_888 ~r:180 ~g:142 ~b:173)
    ~c_selected:(A.rgb_888 ~r:67 ~g:76 ~b:94)
    ~c_exch_hl:(A.rgb_888 ~r:163 ~g:190 ~b:140)
    ~c_exch_kr:(A.rgb_888 ~r:180 ~g:142 ~b:173)
    ~c_exch_li:(A.rgb_888 ~r:136 ~g:192 ~b:208)
    ~c_exch_ib:(A.rgb_888 ~r:208 ~g:135 ~b:112)
    ~c_exch_alp:(A.rgb_888 ~r:235 ~g:203 ~b:139)
    ~c_bps_tight:(A.rgb_888 ~r:136 ~g:192 ~b:208)
    ~c_bps_norm:(A.rgb_888 ~r:94 ~g:104 ~b:125)
    ~c_bps_wide:(A.rgb_888 ~r:180 ~g:142 ~b:173)
    ~c_bps_xtrm:(A.rgb_888 ~r:191 ~g:97 ~b:106)
;;

(* 4. Catppuccin Mocha *)
let catppuccin =
  make_theme
    ~id:"catppuccin"
    ~name:"Catppuccin Mocha"
    ~desc:"Soothing espresso velvet with mauve, sapphire, and pastel peach"
    ~accent_rgb:(203, 166, 247)
    ~bg_rgb:(30, 30, 46)
    ~c_bg:(A.rgb_888 ~r:30 ~g:30 ~b:46)
    ~c_panel:(A.rgb_888 ~r:24 ~g:24 ~b:37)
    ~c_section_bg:(A.rgb_888 ~r:49 ~g:50 ~b:68)
    ~c_border:(A.rgb_888 ~r:88 ~g:91 ~b:112)
    ~c_title:(A.rgb_888 ~r:205 ~g:214 ~b:244)
    ~c_accent:(A.rgb_888 ~r:203 ~g:166 ~b:247)
    ~c_label:(A.rgb_888 ~r:127 ~g:132 ~b:156)
    ~c_text:(A.rgb_888 ~r:186 ~g:194 ~b:222)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:166 ~g:227 ~b:161)
    ~c_green_dark:(A.rgb_888 ~r:90 ~g:150 ~b:90)
    ~c_red:(A.rgb_888 ~r:243 ~g:139 ~b:168)
    ~c_yellow:(A.rgb_888 ~r:249 ~g:226 ~b:175)
    ~c_cyan:(A.rgb_888 ~r:137 ~g:220 ~b:235)
    ~c_dim:(A.rgb_888 ~r:108 ~g:112 ~b:134)
    ~c_near_fill:(A.rgb_888 ~r:35 ~g:45 ~b:60)
    ~c_near_sell:(A.rgb_888 ~r:60 ~g:35 ~b:45)
    ~c_magenta:(A.rgb_888 ~r:245 ~g:194 ~b:231)
    ~c_selected:(A.rgb_888 ~r:69 ~g:71 ~b:90)
    ~c_exch_hl:(A.rgb_888 ~r:166 ~g:227 ~b:161)
    ~c_exch_kr:(A.rgb_888 ~r:203 ~g:166 ~b:247)
    ~c_exch_li:(A.rgb_888 ~r:116 ~g:199 ~b:236)
    ~c_exch_ib:(A.rgb_888 ~r:250 ~g:179 ~b:135)
    ~c_exch_alp:(A.rgb_888 ~r:249 ~g:226 ~b:175)
    ~c_bps_tight:(A.rgb_888 ~r:137 ~g:220 ~b:235)
    ~c_bps_norm:(A.rgb_888 ~r:108 ~g:112 ~b:134)
    ~c_bps_wide:(A.rgb_888 ~r:245 ~g:194 ~b:231)
    ~c_bps_xtrm:(A.rgb_888 ~r:243 ~g:139 ~b:168)
;;

(* 5. Gruvbox Dark *)
let gruvbox =
  make_theme
    ~id:"gruvbox"
    ~name:"Gruvbox Dark"
    ~desc:"Warm retro charcoal with bright amber-orange and earthy gold"
    ~accent_rgb:(254, 128, 25)
    ~bg_rgb:(29, 32, 33)
    ~c_bg:(A.rgb_888 ~r:29 ~g:32 ~b:33)
    ~c_panel:(A.rgb_888 ~r:20 ~g:22 ~b:23)
    ~c_section_bg:(A.rgb_888 ~r:50 ~g:48 ~b:47)
    ~c_border:(A.rgb_888 ~r:102 ~g:92 ~b:84)
    ~c_title:(A.rgb_888 ~r:251 ~g:241 ~b:199)
    ~c_accent:(A.rgb_888 ~r:254 ~g:128 ~b:25)
    ~c_label:(A.rgb_888 ~r:146 ~g:131 ~b:116)
    ~c_text:(A.rgb_888 ~r:235 ~g:219 ~b:178)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:184 ~g:187 ~b:38)
    ~c_green_dark:(A.rgb_888 ~r:110 ~g:120 ~b:30)
    ~c_red:(A.rgb_888 ~r:251 ~g:73 ~b:52)
    ~c_yellow:(A.rgb_888 ~r:250 ~g:189 ~b:47)
    ~c_cyan:(A.rgb_888 ~r:142 ~g:192 ~b:124)
    ~c_dim:(A.rgb_888 ~r:124 ~g:111 ~b:100)
    ~c_near_fill:(A.rgb_888 ~r:45 ~g:45 ~b:30)
    ~c_near_sell:(A.rgb_888 ~r:60 ~g:30 ~b:30)
    ~c_magenta:(A.rgb_888 ~r:211 ~g:134 ~b:155)
    ~c_selected:(A.rgb_888 ~r:80 ~g:73 ~b:69)
    ~c_exch_hl:(A.rgb_888 ~r:184 ~g:187 ~b:38)
    ~c_exch_kr:(A.rgb_888 ~r:211 ~g:134 ~b:155)
    ~c_exch_li:(A.rgb_888 ~r:142 ~g:192 ~b:124)
    ~c_exch_ib:(A.rgb_888 ~r:254 ~g:128 ~b:25)
    ~c_exch_alp:(A.rgb_888 ~r:250 ~g:189 ~b:47)
    ~c_bps_tight:(A.rgb_888 ~r:142 ~g:192 ~b:124)
    ~c_bps_norm:(A.rgb_888 ~r:124 ~g:111 ~b:100)
    ~c_bps_wide:(A.rgb_888 ~r:211 ~g:134 ~b:155)
    ~c_bps_xtrm:(A.rgb_888 ~r:251 ~g:73 ~b:52)
;;

(* 6. Matrix Phosphor *)
let matrix =
  make_theme
    ~id:"matrix"
    ~name:"Matrix Phosphor"
    ~desc:"Obsidian hacker terminal with electric green phosphor glow"
    ~accent_rgb:(0, 255, 102)
    ~bg_rgb:(10, 16, 12)
    ~c_bg:(A.rgb_888 ~r:10 ~g:16 ~b:12)
    ~c_panel:(A.rgb_888 ~r:6 ~g:10 ~b:8)
    ~c_section_bg:(A.rgb_888 ~r:18 ~g:32 ~b:22)
    ~c_border:(A.rgb_888 ~r:30 ~g:80 ~b:45)
    ~c_title:(A.rgb_888 ~r:150 ~g:255 ~b:180)
    ~c_accent:(A.rgb_888 ~r:0 ~g:255 ~b:102)
    ~c_label:(A.rgb_888 ~r:60 ~g:120 ~b:80)
    ~c_text:(A.rgb_888 ~r:180 ~g:235 ~b:195)
    ~c_bright:(A.rgb_888 ~r:230 ~g:255 ~b:235)
    ~c_green:(A.rgb_888 ~r:50 ~g:250 ~b:120)
    ~c_green_dark:(A.rgb_888 ~r:20 ~g:140 ~b:60)
    ~c_red:(A.rgb_888 ~r:255 ~g:85 ~b:85)
    ~c_yellow:(A.rgb_888 ~r:255 ~g:184 ~b:108)
    ~c_cyan:(A.rgb_888 ~r:80 ~g:250 ~b:220)
    ~c_dim:(A.rgb_888 ~r:45 ~g:85 ~b:55)
    ~c_near_fill:(A.rgb_888 ~r:15 ~g:45 ~b:30)
    ~c_near_sell:(A.rgb_888 ~r:50 ~g:25 ~b:30)
    ~c_magenta:(A.rgb_888 ~r:255 ~g:121 ~b:198)
    ~c_selected:(A.rgb_888 ~r:25 ~g:60 ~b:35)
    ~c_exch_hl:(A.rgb_888 ~r:50 ~g:250 ~b:120)
    ~c_exch_kr:(A.rgb_888 ~r:0 ~g:255 ~b:102)
    ~c_exch_li:(A.rgb_888 ~r:80 ~g:250 ~b:220)
    ~c_exch_ib:(A.rgb_888 ~r:255 ~g:184 ~b:108)
    ~c_exch_alp:(A.rgb_888 ~r:240 ~g:250 ~b:100)
    ~c_bps_tight:(A.rgb_888 ~r:80 ~g:250 ~b:220)
    ~c_bps_norm:(A.rgb_888 ~r:45 ~g:85 ~b:55)
    ~c_bps_wide:(A.rgb_888 ~r:255 ~g:121 ~b:198)
    ~c_bps_xtrm:(A.rgb_888 ~r:255 ~g:85 ~b:85)
;;

(* 7. Monokai Pro *)
let monokai =
  make_theme
    ~id:"monokai"
    ~name:"Monokai Pro"
    ~desc:"Vibrant charcoal with rose magenta, mint green, and sunny amber"
    ~accent_rgb:(255, 97, 136)
    ~bg_rgb:(45, 42, 46)
    ~c_bg:(A.rgb_888 ~r:45 ~g:42 ~b:46)
    ~c_panel:(A.rgb_888 ~r:34 ~g:31 ~b:34)
    ~c_section_bg:(A.rgb_888 ~r:64 ~g:60 ~b:65)
    ~c_border:(A.rgb_888 ~r:114 ~g:112 ~b:114)
    ~c_title:(A.rgb_888 ~r:252 ~g:252 ~b:250)
    ~c_accent:(A.rgb_888 ~r:255 ~g:97 ~b:136)
    ~c_label:(A.rgb_888 ~r:147 ~g:146 ~b:147)
    ~c_text:(A.rgb_888 ~r:220 ~g:220 ~b:215)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:169 ~g:220 ~b:118)
    ~c_green_dark:(A.rgb_888 ~r:95 ~g:145 ~b:60)
    ~c_red:(A.rgb_888 ~r:255 ~g:97 ~b:136)
    ~c_yellow:(A.rgb_888 ~r:255 ~g:216 ~b:102)
    ~c_cyan:(A.rgb_888 ~r:120 ~g:220 ~b:232)
    ~c_dim:(A.rgb_888 ~r:114 ~g:112 ~b:114)
    ~c_near_fill:(A.rgb_888 ~r:35 ~g:55 ~b:50)
    ~c_near_sell:(A.rgb_888 ~r:65 ~g:35 ~b:45)
    ~c_magenta:(A.rgb_888 ~r:171 ~g:157 ~b:242)
    ~c_selected:(A.rgb_888 ~r:80 ~g:75 ~b:85)
    ~c_exch_hl:(A.rgb_888 ~r:169 ~g:220 ~b:118)
    ~c_exch_kr:(A.rgb_888 ~r:171 ~g:157 ~b:242)
    ~c_exch_li:(A.rgb_888 ~r:120 ~g:220 ~b:232)
    ~c_exch_ib:(A.rgb_888 ~r:252 ~g:152 ~b:103)
    ~c_exch_alp:(A.rgb_888 ~r:255 ~g:216 ~b:102)
    ~c_bps_tight:(A.rgb_888 ~r:120 ~g:220 ~b:232)
    ~c_bps_norm:(A.rgb_888 ~r:114 ~g:112 ~b:114)
    ~c_bps_wide:(A.rgb_888 ~r:171 ~g:157 ~b:242)
    ~c_bps_xtrm:(A.rgb_888 ~r:255 ~g:97 ~b:136)
;;

(* 8. Solarized Dark *)
let solarized =
  make_theme
    ~id:"solarized"
    ~name:"Solarized Dark"
    ~desc:"Classic teal obsidian with cyan-blue accents and violet hues"
    ~accent_rgb:(38, 139, 210)
    ~bg_rgb:(0, 43, 54)
    ~c_bg:(A.rgb_888 ~r:0 ~g:43 ~b:54)
    ~c_panel:(A.rgb_888 ~r:7 ~g:54 ~b:66)
    ~c_section_bg:(A.rgb_888 ~r:10 ~g:65 ~b:78)
    ~c_border:(A.rgb_888 ~r:88 ~g:110 ~b:117)
    ~c_title:(A.rgb_888 ~r:253 ~g:246 ~b:227)
    ~c_accent:(A.rgb_888 ~r:38 ~g:139 ~b:210)
    ~c_label:(A.rgb_888 ~r:101 ~g:123 ~b:131)
    ~c_text:(A.rgb_888 ~r:147 ~g:161 ~b:161)
    ~c_bright:(A.rgb_888 ~r:253 ~g:246 ~b:227)
    ~c_green:(A.rgb_888 ~r:133 ~g:153 ~b:0)
    ~c_green_dark:(A.rgb_888 ~r:75 ~g:100 ~b:20)
    ~c_red:(A.rgb_888 ~r:220 ~g:50 ~b:47)
    ~c_yellow:(A.rgb_888 ~r:181 ~g:137 ~b:0)
    ~c_cyan:(A.rgb_888 ~r:42 ~g:161 ~b:152)
    ~c_dim:(A.rgb_888 ~r:88 ~g:110 ~b:117)
    ~c_near_fill:(A.rgb_888 ~r:10 ~g:60 ~b:60)
    ~c_near_sell:(A.rgb_888 ~r:65 ~g:40 ~b:40)
    ~c_magenta:(A.rgb_888 ~r:211 ~g:54 ~b:130)
    ~c_selected:(A.rgb_888 ~r:15 ~g:80 ~b:95)
    ~c_exch_hl:(A.rgb_888 ~r:133 ~g:153 ~b:0)
    ~c_exch_kr:(A.rgb_888 ~r:108 ~g:113 ~b:196)
    ~c_exch_li:(A.rgb_888 ~r:42 ~g:161 ~b:152)
    ~c_exch_ib:(A.rgb_888 ~r:203 ~g:75 ~b:22)
    ~c_exch_alp:(A.rgb_888 ~r:181 ~g:137 ~b:0)
    ~c_bps_tight:(A.rgb_888 ~r:42 ~g:161 ~b:152)
    ~c_bps_norm:(A.rgb_888 ~r:88 ~g:110 ~b:117)
    ~c_bps_wide:(A.rgb_888 ~r:211 ~g:54 ~b:130)
    ~c_bps_xtrm:(A.rgb_888 ~r:220 ~g:50 ~b:47)
;;

(* 9. Midnight Emerald (Bloomberg Pro) *)
let emerald =
  make_theme
    ~id:"emerald"
    ~name:"Midnight Emerald"
    ~desc:"Hunter slate with crisp emerald jade, amber, and mint accents"
    ~accent_rgb:(16, 185, 129)
    ~bg_rgb:(11, 22, 19)
    ~c_bg:(A.rgb_888 ~r:11 ~g:22 ~b:19)
    ~c_panel:(A.rgb_888 ~r:7 ~g:16 ~b:13)
    ~c_section_bg:(A.rgb_888 ~r:18 ~g:36 ~b:30)
    ~c_border:(A.rgb_888 ~r:40 ~g:75 ~b:62)
    ~c_title:(A.rgb_888 ~r:240 ~g:253 ~b:244)
    ~c_accent:(A.rgb_888 ~r:16 ~g:185 ~b:129)
    ~c_label:(A.rgb_888 ~r:75 ~g:125 ~b:105)
    ~c_text:(A.rgb_888 ~r:209 ~g:250 ~b:229)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:52 ~g:211 ~b:153)
    ~c_green_dark:(A.rgb_888 ~r:20 ~g:120 ~b:85)
    ~c_red:(A.rgb_888 ~r:244 ~g:63 ~b:94)
    ~c_yellow:(A.rgb_888 ~r:245 ~g:158 ~b:11)
    ~c_cyan:(A.rgb_888 ~r:45 ~g:212 ~b:191)
    ~c_dim:(A.rgb_888 ~r:60 ~g:100 ~b:85)
    ~c_near_fill:(A.rgb_888 ~r:15 ~g:50 ~b:40)
    ~c_near_sell:(A.rgb_888 ~r:55 ~g:25 ~b:35)
    ~c_magenta:(A.rgb_888 ~r:236 ~g:72 ~b:153)
    ~c_selected:(A.rgb_888 ~r:25 ~g:65 ~b:50)
    ~c_exch_hl:(A.rgb_888 ~r:52 ~g:211 ~b:153)
    ~c_exch_kr:(A.rgb_888 ~r:16 ~g:185 ~b:129)
    ~c_exch_li:(A.rgb_888 ~r:45 ~g:212 ~b:191)
    ~c_exch_ib:(A.rgb_888 ~r:245 ~g:158 ~b:11)
    ~c_exch_alp:(A.rgb_888 ~r:251 ~g:191 ~b:36)
    ~c_bps_tight:(A.rgb_888 ~r:45 ~g:212 ~b:191)
    ~c_bps_norm:(A.rgb_888 ~r:60 ~g:100 ~b:85)
    ~c_bps_wide:(A.rgb_888 ~r:236 ~g:72 ~b:153)
    ~c_bps_xtrm:(A.rgb_888 ~r:244 ~g:63 ~b:94)
;;

(* 10. Dracula *)
let dracula =
  make_theme
    ~id:"dracula"
    ~name:"Dracula"
    ~desc:"Classic dark gothic palette with vibrant purple, green, and pink"
    ~accent_rgb:(189, 147, 249)
    ~bg_rgb:(40, 42, 54)
    ~c_bg:(A.rgb_888 ~r:40 ~g:42 ~b:54)
    ~c_panel:(A.rgb_888 ~r:30 ~g:31 ~b:41)
    ~c_section_bg:(A.rgb_888 ~r:68 ~g:71 ~b:90)
    ~c_border:(A.rgb_888 ~r:98 ~g:114 ~b:164)
    ~c_title:(A.rgb_888 ~r:248 ~g:248 ~b:242)
    ~c_accent:(A.rgb_888 ~r:189 ~g:147 ~b:249)
    ~c_label:(A.rgb_888 ~r:98 ~g:114 ~b:164)
    ~c_text:(A.rgb_888 ~r:248 ~g:248 ~b:242)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:80 ~g:250 ~b:123)
    ~c_green_dark:(A.rgb_888 ~r:40 ~g:160 ~b:80)
    ~c_red:(A.rgb_888 ~r:255 ~g:85 ~b:85)
    ~c_yellow:(A.rgb_888 ~r:241 ~g:250 ~b:140)
    ~c_cyan:(A.rgb_888 ~r:139 ~g:233 ~b:253)
    ~c_dim:(A.rgb_888 ~r:98 ~g:114 ~b:164)
    ~c_near_fill:(A.rgb_888 ~r:35 ~g:55 ~b:65)
    ~c_near_sell:(A.rgb_888 ~r:65 ~g:35 ~b:45)
    ~c_magenta:(A.rgb_888 ~r:255 ~g:121 ~b:198)
    ~c_selected:(A.rgb_888 ~r:68 ~g:71 ~b:90)
    ~c_exch_hl:(A.rgb_888 ~r:80 ~g:250 ~b:123)
    ~c_exch_kr:(A.rgb_888 ~r:189 ~g:147 ~b:249)
    ~c_exch_li:(A.rgb_888 ~r:139 ~g:233 ~b:253)
    ~c_exch_ib:(A.rgb_888 ~r:255 ~g:184 ~b:108)
    ~c_exch_alp:(A.rgb_888 ~r:241 ~g:250 ~b:140)
    ~c_bps_tight:(A.rgb_888 ~r:139 ~g:233 ~b:253)
    ~c_bps_norm:(A.rgb_888 ~r:98 ~g:114 ~b:164)
    ~c_bps_wide:(A.rgb_888 ~r:255 ~g:121 ~b:198)
    ~c_bps_xtrm:(A.rgb_888 ~r:255 ~g:85 ~b:85)
;;

(* 11. Rosé Pine *)
let rose_pine =
  make_theme
    ~id:"rose-pine"
    ~name:"Rosé Pine"
    ~desc:"Dreamy dark rose and pine with blush copper and warm gold"
    ~accent_rgb:(235, 188, 186)
    ~bg_rgb:(25, 23, 36)
    ~c_bg:(A.rgb_888 ~r:25 ~g:23 ~b:36)
    ~c_panel:(A.rgb_888 ~r:31 ~g:29 ~b:46)
    ~c_section_bg:(A.rgb_888 ~r:38 ~g:35 ~b:58)
    ~c_border:(A.rgb_888 ~r:82 ~g:79 ~b:103)
    ~c_title:(A.rgb_888 ~r:224 ~g:222 ~b:244)
    ~c_accent:(A.rgb_888 ~r:235 ~g:188 ~b:186)
    ~c_label:(A.rgb_888 ~r:110 ~g:106 ~b:134)
    ~c_text:(A.rgb_888 ~r:224 ~g:222 ~b:244)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:86 ~g:148 ~b:159)
    ~c_green_dark:(A.rgb_888 ~r:49 ~g:116 ~b:143)
    ~c_red:(A.rgb_888 ~r:235 ~g:111 ~b:146)
    ~c_yellow:(A.rgb_888 ~r:246 ~g:193 ~b:119)
    ~c_cyan:(A.rgb_888 ~r:156 ~g:207 ~b:216)
    ~c_dim:(A.rgb_888 ~r:110 ~g:106 ~b:134)
    ~c_near_fill:(A.rgb_888 ~r:30 ~g:45 ~b:55)
    ~c_near_sell:(A.rgb_888 ~r:55 ~g:30 ~b:45)
    ~c_magenta:(A.rgb_888 ~r:196 ~g:167 ~b:231)
    ~c_selected:(A.rgb_888 ~r:64 ~g:61 ~b:82)
    ~c_exch_hl:(A.rgb_888 ~r:86 ~g:148 ~b:159)
    ~c_exch_kr:(A.rgb_888 ~r:196 ~g:167 ~b:231)
    ~c_exch_li:(A.rgb_888 ~r:156 ~g:207 ~b:216)
    ~c_exch_ib:(A.rgb_888 ~r:235 ~g:188 ~b:186)
    ~c_exch_alp:(A.rgb_888 ~r:246 ~g:193 ~b:119)
    ~c_bps_tight:(A.rgb_888 ~r:156 ~g:207 ~b:216)
    ~c_bps_norm:(A.rgb_888 ~r:110 ~g:106 ~b:134)
    ~c_bps_wide:(A.rgb_888 ~r:196 ~g:167 ~b:231)
    ~c_bps_xtrm:(A.rgb_888 ~r:235 ~g:111 ~b:146)
;;

(* 12. Kanagawa *)
let kanagawa =
  make_theme
    ~id:"kanagawa"
    ~name:"Kanagawa"
    ~desc:"Japanese woodblock ink, wave aqua, autumn red, and Fuji snow"
    ~accent_rgb:(149, 127, 184)
    ~bg_rgb:(31, 31, 40)
    ~c_bg:(A.rgb_888 ~r:31 ~g:31 ~b:40)
    ~c_panel:(A.rgb_888 ~r:22 ~g:22 ~b:29)
    ~c_section_bg:(A.rgb_888 ~r:42 ~g:42 ~b:55)
    ~c_border:(A.rgb_888 ~r:84 ~g:84 ~b:109)
    ~c_title:(A.rgb_888 ~r:220 ~g:215 ~b:186)
    ~c_accent:(A.rgb_888 ~r:149 ~g:127 ~b:184)
    ~c_label:(A.rgb_888 ~r:114 ~g:113 ~b:105)
    ~c_text:(A.rgb_888 ~r:220 ~g:215 ~b:186)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:152 ~g:187 ~b:108)
    ~c_green_dark:(A.rgb_888 ~r:80 ~g:120 ~b:70)
    ~c_red:(A.rgb_888 ~r:228 ~g:104 ~b:118)
    ~c_yellow:(A.rgb_888 ~r:230 ~g:195 ~b:132)
    ~c_cyan:(A.rgb_888 ~r:122 ~g:168 ~b:159)
    ~c_dim:(A.rgb_888 ~r:114 ~g:113 ~b:105)
    ~c_near_fill:(A.rgb_888 ~r:30 ~g:45 ~b:45)
    ~c_near_sell:(A.rgb_888 ~r:55 ~g:30 ~b:35)
    ~c_magenta:(A.rgb_888 ~r:210 ~g:126 ~b:153)
    ~c_selected:(A.rgb_888 ~r:54 ~g:54 ~b:70)
    ~c_exch_hl:(A.rgb_888 ~r:152 ~g:187 ~b:108)
    ~c_exch_kr:(A.rgb_888 ~r:149 ~g:127 ~b:184)
    ~c_exch_li:(A.rgb_888 ~r:122 ~g:168 ~b:159)
    ~c_exch_ib:(A.rgb_888 ~r:255 ~g:160 ~b:102)
    ~c_exch_alp:(A.rgb_888 ~r:230 ~g:195 ~b:132)
    ~c_bps_tight:(A.rgb_888 ~r:122 ~g:168 ~b:159)
    ~c_bps_norm:(A.rgb_888 ~r:114 ~g:113 ~b:105)
    ~c_bps_wide:(A.rgb_888 ~r:210 ~g:126 ~b:153)
    ~c_bps_xtrm:(A.rgb_888 ~r:228 ~g:104 ~b:118)
;;

(* 13. Synthwave '84 *)
let synthwave84 =
  make_theme
    ~id:"synthwave84"
    ~name:"Synthwave '84"
    ~desc:"Outrun neon glow with laser magenta, sunset yellow, and hot cyan"
    ~accent_rgb:(255, 126, 219)
    ~bg_rgb:(38, 35, 53)
    ~c_bg:(A.rgb_888 ~r:38 ~g:35 ~b:53)
    ~c_panel:(A.rgb_888 ~r:30 ~g:26 ~b:43)
    ~c_section_bg:(A.rgb_888 ~r:52 ~g:41 ~b:79)
    ~c_border:(A.rgb_888 ~r:97 ~g:77 ~b:133)
    ~c_title:(A.rgb_888 ~r:249 ~g:42 ~b:173)
    ~c_accent:(A.rgb_888 ~r:255 ~g:126 ~b:219)
    ~c_label:(A.rgb_888 ~r:132 ~g:139 ~b:189)
    ~c_text:(A.rgb_888 ~r:240 ~g:239 ~b:241)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:114 ~g:241 ~b:184)
    ~c_green_dark:(A.rgb_888 ~r:45 ~g:155 ~b:110)
    ~c_red:(A.rgb_888 ~r:254 ~g:68 ~b:80)
    ~c_yellow:(A.rgb_888 ~r:254 ~g:222 ~b:93)
    ~c_cyan:(A.rgb_888 ~r:54 ~g:249 ~b:246)
    ~c_dim:(A.rgb_888 ~r:132 ~g:139 ~b:189)
    ~c_near_fill:(A.rgb_888 ~r:30 ~g:55 ~b:65)
    ~c_near_sell:(A.rgb_888 ~r:65 ~g:25 ~b:50)
    ~c_magenta:(A.rgb_888 ~r:255 ~g:126 ~b:219)
    ~c_selected:(A.rgb_888 ~r:74 ~g:56 ~b:105)
    ~c_exch_hl:(A.rgb_888 ~r:114 ~g:241 ~b:184)
    ~c_exch_kr:(A.rgb_888 ~r:255 ~g:126 ~b:219)
    ~c_exch_li:(A.rgb_888 ~r:54 ~g:249 ~b:246)
    ~c_exch_ib:(A.rgb_888 ~r:254 ~g:154 ~b:0)
    ~c_exch_alp:(A.rgb_888 ~r:254 ~g:222 ~b:93)
    ~c_bps_tight:(A.rgb_888 ~r:54 ~g:249 ~b:246)
    ~c_bps_norm:(A.rgb_888 ~r:132 ~g:139 ~b:189)
    ~c_bps_wide:(A.rgb_888 ~r:255 ~g:126 ~b:219)
    ~c_bps_xtrm:(A.rgb_888 ~r:254 ~g:68 ~b:80)
;;

(* 14. Deep Sea Abyss *)
let abyss =
  make_theme
    ~id:"abyss"
    ~name:"Deep Sea Abyss"
    ~desc:"Bioluminescent ocean depths with electric cyan and seafoam glow"
    ~accent_rgb:(0, 210, 255)
    ~bg_rgb:(8, 16, 24)
    ~c_bg:(A.rgb_888 ~r:8 ~g:16 ~b:24)
    ~c_panel:(A.rgb_888 ~r:4 ~g:8 ~b:14)
    ~c_section_bg:(A.rgb_888 ~r:13 ~g:32 ~b:48)
    ~c_border:(A.rgb_888 ~r:27 ~g:67 ~b:99)
    ~c_title:(A.rgb_888 ~r:124 ~g:232 ~b:255)
    ~c_accent:(A.rgb_888 ~r:0 ~g:210 ~b:255)
    ~c_label:(A.rgb_888 ~r:65 ~g:107 ~b:138)
    ~c_text:(A.rgb_888 ~r:208 ~g:240 ~b:253)
    ~c_bright:(A.rgb_888 ~r:255 ~g:255 ~b:255)
    ~c_green:(A.rgb_888 ~r:0 ~g:245 ~b:160)
    ~c_green_dark:(A.rgb_888 ~r:0 ~g:135 ~b:90)
    ~c_red:(A.rgb_888 ~r:255 ~g:75 ~b:114)
    ~c_yellow:(A.rgb_888 ~r:255 ~g:209 ~b:102)
    ~c_cyan:(A.rgb_888 ~r:0 ~g:229 ~b:255)
    ~c_dim:(A.rgb_888 ~r:65 ~g:107 ~b:138)
    ~c_near_fill:(A.rgb_888 ~r:10 ~g:45 ~b:55)
    ~c_near_sell:(A.rgb_888 ~r:55 ~g:20 ~b:35)
    ~c_magenta:(A.rgb_888 ~r:199 ~g:125 ~b:255)
    ~c_selected:(A.rgb_888 ~r:18 ~g:51 ~b:77)
    ~c_exch_hl:(A.rgb_888 ~r:0 ~g:245 ~b:160)
    ~c_exch_kr:(A.rgb_888 ~r:199 ~g:125 ~b:255)
    ~c_exch_li:(A.rgb_888 ~r:0 ~g:229 ~b:255)
    ~c_exch_ib:(A.rgb_888 ~r:255 ~g:170 ~b:70)
    ~c_exch_alp:(A.rgb_888 ~r:255 ~g:209 ~b:102)
    ~c_bps_tight:(A.rgb_888 ~r:0 ~g:229 ~b:255)
    ~c_bps_norm:(A.rgb_888 ~r:65 ~g:107 ~b:138)
    ~c_bps_wide:(A.rgb_888 ~r:199 ~g:125 ~b:255)
    ~c_bps_xtrm:(A.rgb_888 ~r:255 ~g:75 ~b:114)
;;

let all_themes_list =
  [ tokyo_night
  ; cyberpunk
  ; nord
  ; catppuccin
  ; gruvbox
  ; matrix
  ; monokai
  ; solarized
  ; emerald
  ; dracula
  ; rose_pine
  ; kanagawa
  ; synthwave84
  ; abyss
  ]
;;

let active_theme_ref = ref tokyo_night

let current () = !active_theme_ref

let all_themes () = all_themes_list

let theme_count () = List.length all_themes_list

let current_theme_index () =
  let rec find i = function
    | [] -> 0
    | (t : theme_palette) :: rest ->
      if t.id = (!active_theme_ref).id then i else find (i + 1) rest
  in
  find 0 all_themes_list
;;

let set_theme_by_index idx =
  let len = List.length all_themes_list in
  let safe_idx = ((idx mod len) + len) mod len in
  active_theme_ref := List.nth all_themes_list safe_idx
;;

let normalize_theme_str s =
  let buf = Buffer.create (String.length s) in
  String.iter
    (fun c ->
       match c with
       | 'a' .. 'z' | '0' .. '9' -> Buffer.add_char buf c
       | 'A' .. 'Z' -> Buffer.add_char buf (Char.lowercase_ascii c)
       | _ -> ())
    s;
  Buffer.contents buf
;;

let set_theme_by_id id =
  let id_clean = String.trim (String.lowercase_ascii id) in
  let id_norm = normalize_theme_str id in
  let match_opt =
    List.find_opt (fun (t : theme_palette) -> t.id = id_clean) all_themes_list
  in
  let match_opt =
    match match_opt with
    | Some _ -> match_opt
    | None ->
      List.find_opt
        (fun (t : theme_palette) ->
           normalize_theme_str t.id = id_norm
           || normalize_theme_str t.name = id_norm)
        all_themes_list
  in
  let match_opt =
    match match_opt with
    | Some _ -> match_opt
    | None ->
      List.find_opt
        (fun (t : theme_palette) ->
           let t_norm = normalize_theme_str t.id in
           String.starts_with ~prefix:id_norm t_norm
           || String.starts_with ~prefix:id_norm (normalize_theme_str t.name))
        all_themes_list
  in
  match match_opt with
  | Some t ->
    active_theme_ref := t;
    true
  | None -> false
;;

let next_theme () =
  let idx = current_theme_index () in
  set_theme_by_index (idx + 1);
  !active_theme_ref
;;

let prev_theme () =
  let idx = current_theme_index () in
  set_theme_by_index (idx - 1);
  !active_theme_ref
;;

(* Theme persistence *)
let config_paths () =
  let home =
    try Sys.getenv "HOME" with
    | _ -> "."
  in
  [ home ^ "/.dio_theme"; "./.dio_theme" ]
;;

let save_theme id =
  try
    let home =
      try Sys.getenv "HOME" with
      | _ -> "."
    in
    let oc = open_out (home ^ "/.dio_theme") in
    output_string oc (id ^ "\n");
    close_out oc
  with
  | _ -> ()
;;

let load_saved_theme ?(config_file = "config.json") () =
  let loaded_from_config = ref false in
  (* 1. Check config.json paths (including /app/config.json in Docker) for "theme" *)
  let config_candidates =
    [ config_file; "./config.json"; "/app/config.json"; "../config.json" ]
  in
  let rec try_configs = function
    | [] -> ()
    | path :: rest ->
      if Sys.file_exists path
      then (
        try
          let json = Yojson.Basic.from_file path in
          match json |?> "theme" with
          | `String theme_str when String.trim theme_str <> "" ->
            if set_theme_by_id theme_str then loaded_from_config := true
          | _ -> try_configs rest
        with
        | _ -> try_configs rest)
      else try_configs rest
  in
  try_configs config_candidates;
  (* 2. Fall back to ~/.dio_theme if config.json didn't specify a theme *)
  if not !loaded_from_config
  then (
    let rec try_paths = function
      | [] -> ()
      | p :: rest ->
        if Sys.file_exists p
        then (
          try
            let ic = open_in p in
            let line = String.trim (input_line ic) in
            close_in ic;
            ignore (set_theme_by_id line)
          with
          | _ -> try_paths rest)
        else try_paths rest
    in
    try_paths (config_paths ()))
;;

(* Dynamic getters & top-level compatibility attributes *)

let c_bg () = (!active_theme_ref).c_bg
let c_panel () = (!active_theme_ref).c_panel
let c_section_bg () = (!active_theme_ref).c_section_bg
let c_border () = (!active_theme_ref).c_border
let c_title () = (!active_theme_ref).c_title
let c_accent () = (!active_theme_ref).c_accent
let c_label () = (!active_theme_ref).c_label
let c_text () = (!active_theme_ref).c_text
let c_bright () = (!active_theme_ref).c_bright
let c_green () = (!active_theme_ref).c_green
let c_green_dark () = (!active_theme_ref).c_green_dark
let c_red () = (!active_theme_ref).c_red
let c_yellow () = (!active_theme_ref).c_yellow
let c_cyan () = (!active_theme_ref).c_cyan
let c_dim () = (!active_theme_ref).c_dim
let c_near_fill () = (!active_theme_ref).c_near_fill
let c_near_sell () = (!active_theme_ref).c_near_sell
let c_magenta () = (!active_theme_ref).c_magenta
let c_selected () = (!active_theme_ref).c_selected
let c_exch_hl () = (!active_theme_ref).c_exch_hl
let c_exch_kr () = (!active_theme_ref).c_exch_kr
let c_exch_li () = (!active_theme_ref).c_exch_li
let c_exch_ib () = (!active_theme_ref).c_exch_ib
let c_exch_alp () = (!active_theme_ref).c_exch_alp

let a_label () = (!active_theme_ref).a_label
let a_text () = (!active_theme_ref).a_text
let a_bright () = (!active_theme_ref).a_bright
let a_green () = (!active_theme_ref).a_green
let a_green_dark () = (!active_theme_ref).a_green_dark
let a_red () = (!active_theme_ref).a_red
let a_yellow () = (!active_theme_ref).a_yellow
let a_cyan () = (!active_theme_ref).a_cyan
let a_dim () = (!active_theme_ref).a_dim
let a_border () = (!active_theme_ref).a_border
let a_bps_tight () = (!active_theme_ref).a_bps_tight
let a_bps_norm () = (!active_theme_ref).a_bps_norm
let a_bps_wide () = (!active_theme_ref).a_bps_wide
let a_bps_xtrm () = (!active_theme_ref).a_bps_xtrm
let a_near_fill () = (!active_theme_ref).a_near_fill
let a_near_fill_green () = (!active_theme_ref).a_near_fill_green
let a_near_sell () = (!active_theme_ref).a_near_sell
let a_near_sell_red () = (!active_theme_ref).a_near_sell_red

(** Exchange-specific color for the SYMBOL column. *)
let exch_sym_attr ?(dim = false) exchange =
  let t = !active_theme_ref in
  let c =
    match exchange with
    | "hyperliquid" -> t.c_exch_hl
    | "kraken" -> t.c_exch_kr
    | "lighter" -> t.c_exch_li
    | "ibkr" -> t.c_exch_ib
    | "alpaca" -> t.c_exch_alp
    | _ -> t.c_bright
  in
  if dim then A.(fg c ++ bg t.c_bg) else A.(fg c ++ bg t.c_bg ++ st bold)
;;

let exch_tag_of = function
  | "kraken" -> "krkn"
  | "hyperliquid" -> "hypr"
  | "lighter" -> "ltr"
  | "ibkr" -> "ibkr"
  | "alpaca" -> "alpc"
  | e -> String.sub e 0 (min 3 (String.length e))
;;

(* Drawing primitives *)

let pad_right w s =
  let len = utf8_len s in
  if len >= w
  then s (* Return the string intact to avoid splitting UTF-8 bytes. *)
  else s ^ String.make (w - len) ' '
;;

let col w attr s = I.string attr (pad_right w s)

let pad_left w s =
  let len = utf8_len s in
  if len >= w
  then s (* Return the string intact to avoid splitting UTF-8 bytes. *)
  else String.make (w - len) ' ' ^ s
;;

let col_right w attr s = I.string attr (pad_left w s)

let format_pct f =
  let raw =
    if abs_float f < 0.01
    then "<0.01%"
    else if abs_float f >= 10.0
    then Printf.sprintf "%.1f%%" f
    else Printf.sprintf "%.2f%%" f
  in
  add_commas raw
;;

let format_spread_bps bid ask =
  if bid > 0.0 && ask > 0.0
  then (
    let spread_bps = (ask -. bid) /. ((bid +. ask) /. 2.0) *. 10000.0 in
    let raw =
      if spread_bps >= 100.0
      then Printf.sprintf "%.0fbp" spread_bps
      else Printf.sprintf "%.1fbp" spread_bps
    in
    add_commas raw)
  else "--"
;;

(* Drawing helpers *)

let render_progress_bar w ratio attr =
  let t = !active_theme_ref in
  let fill_w = int_of_float (float (max 0 (w - 2)) *. ratio) in
  let fill_w = max 0 (min (w - 2) fill_w) in
  let empty_w = w - 2 - fill_w in
  let fill_str = String.concat "" (List.init fill_w (fun _ -> "⣿")) in
  let empty_str = String.concat "" (List.init empty_w (fun _ -> "─")) in
  I.hcat
    [ I.string t.a_border "["
    ; I.string attr fill_str
    ; I.string t.a_dim empty_str
    ; I.string t.a_border "]"
    ]
;;

let block_chars = [| "⠀"; "⣀"; "⣄"; "⣤"; "⣦"; "⣶"; "⣷"; "⣿" |]

let render_sparkline w data max_val attr_fn =
  let t = !active_theme_ref in
  let len = Array.length data in
  let start_idx = max 0 (len - w) in
  let visible_len = min w len in
  let empty_w = w - visible_len in
  let blocks =
    List.init visible_len (fun i ->
      let v = data.(start_idx + i) in
      let ratio = if max_val > 0.0 then v /. max_val else 0.0 in
      let ratio = max 0.0 (min 1.0 ratio) in
      let block_idx = int_of_float (ratio *. 7.0) in
      let block_idx = max 0 (min 7 block_idx) in
      I.string (attr_fn v) block_chars.(block_idx))
  in
  I.hcat (I.string t.a_dim (String.make empty_w ' ') :: blocks)
;;

(* Gradient utilities for 3D/shaded aesthetics *)

let color_blend (r1, g1, b1) (r2, g2, b2) ratio =
  let clamp x = max 0 (min 255 x) in
  let r = r1 + int_of_float (float (r2 - r1) *. ratio) in
  let g = g1 + int_of_float (float (g2 - g1) *. ratio) in
  let b = b1 + int_of_float (float (b2 - b1) *. ratio) in
  A.rgb_888 ~r:(clamp r) ~g:(clamp g) ~b:(clamp b)
;;

let section_title ?title_attr w label =
  let t = !active_theme_ref in
  let title_attr =
    match title_attr with
    | Some attr -> attr
    | None -> A.(fg t.c_title ++ bg t.c_bg ++ st bold)
  in
  let lbl = " ╭── " ^ label ^ " " in
  let lbl_img = I.string title_attr lbl in
  let len = I.width lbl_img in
  let pad_count = max 0 (w - len - 1) in
  let left_rgb = t.accent_rgb in
  let right_rgb = t.bg_rgb in
  let gradient_lines =
    List.init pad_count (fun i ->
      let ratio = float i /. float (max 1 (pad_count - 1)) in
      (* Ease out the gradient for a smoother fade effect using x^2 *)
      let fade = ratio *. ratio in
      let c = color_blend left_rgb right_rgb fade in
      I.string A.(fg c ++ bg t.c_bg) "─")
  in
  let end_border = I.string A.(fg t.c_border ++ bg t.c_bg) "╮" in
  I.hcat ((lbl_img :: gradient_lines) @ [ end_border ])
;;

let section_footer w =
  let t = !active_theme_ref in
  let pad_count = max 0 (w - 5) in
  let pad_buf = Buffer.create pad_count in
  for _ = 1 to pad_count do
    Buffer.add_string pad_buf "─"
  done;
  I.string A.(fg t.c_border ++ bg t.c_bg) (" ╰──" ^ Buffer.contents pad_buf ^ "╯")
;;

let close_row target_w img =
  let t = !active_theme_ref in
  let max_inner = max 0 (target_w - 2) in
  let img_cropped = I.hsnap ~align:`Left max_inner img in
  I.hcat [ img_cropped; I.string A.(fg t.c_border ++ bg t.c_bg) " │" ]
;;

let render_proximity_slider w pos_pct_opt =
  let t = !active_theme_ref in
  let inner_w = max 3 (w - 2) in
  match pos_pct_opt with
  | None ->
    let mid_w = inner_w / 2 in
    let left_dashes = String.concat "" (List.init mid_w (fun _ -> "─")) in
    let right_dashes =
      String.concat "" (List.init (inner_w - 1 - mid_w) (fun _ -> "─"))
    in
    I.hcat
      [ I.string t.a_border "├"
      ; I.string t.a_dim left_dashes
      ; I.string t.a_cyan "•"
      ; I.string t.a_dim right_dashes
      ; I.string t.a_border "┤"
      ]
  | Some pos ->
    let clamped = max 0.0 (min 100.0 pos) in
    let dot_idx = int_of_float (clamped /. 100.0 *. float (inner_w - 1)) in
    let dot_idx = max 0 (min (inner_w - 1) dot_idx) in
    let left_w = dot_idx in
    let right_w = inner_w - 1 - left_w in
    let dot_attr =
      if clamped <= 20.0
      then A.(fg t.c_green ++ st bold)
      else if clamped >= 80.0
      then A.(fg t.c_red ++ st bold)
      else A.(fg t.c_cyan ++ st bold)
    in
    I.hcat
      [ I.string t.a_green "┠"
      ; I.string t.a_green (String.concat "" (List.init left_w (fun _ -> "━")))
      ; I.string dot_attr "◈"
      ; I.string t.a_red (String.concat "" (List.init right_w (fun _ -> "━")))
      ; I.string t.a_red "┨"
      ]
;;

let render_card w title content_rows =
  let t = !active_theme_ref in
  let title_str = " ╭── " ^ title ^ " " in
  let title_img = I.string A.(fg t.c_title ++ bg t.c_bg ++ st bold) title_str in
  let title_len = I.width title_img in
  let fill_w = max 0 (w - title_len - 1) in
  let top_bar =
    I.hcat
      [ title_img
      ; I.string
          A.(fg t.c_border ++ bg t.c_bg)
          (String.concat "" (List.init fill_w (fun _ -> "─")))
      ; I.string A.(fg t.c_border ++ bg t.c_bg) "╮"
      ]
  in
  let inner_w = max 0 (w - 5) in
  let body_rows =
    List.map
      (fun row_img ->
         let row_cropped = I.hsnap ~align:`Left inner_w row_img in
         I.hcat
           [ I.string A.(fg t.c_border ++ bg t.c_bg) " │ "
           ; row_cropped
           ; I.string A.(fg t.c_border ++ bg t.c_bg) " │"
           ])
      content_rows
  in
  let bot_fill = max 0 (w - 2) in
  let bot_bar =
    I.hcat
      [ I.string A.(fg t.c_border ++ bg t.c_bg) " ╰"
      ; I.string
          A.(fg t.c_border ++ bg t.c_bg)
          (String.concat "" (List.init bot_fill (fun _ -> "─")))
      ; I.string A.(fg t.c_border ++ bg t.c_bg) "╯"
      ]
  in
  I.vcat ((top_bar :: body_rows) @ [ bot_bar ])
;;

(* -------------------------------------------------------------------------- *)
(* Theme Selector Modal Dialog with Windowed Scrolling                        *)
(* -------------------------------------------------------------------------- *)

let repeat_str s n =
  let buf = Buffer.create (String.length s * max 0 n) in
  for _ = 1 to max 0 n do
    Buffer.add_string buf s
  done;
  Buffer.contents buf
;;

let render_theme_modal ~target_w ~target_h ~cursor_idx =
  let t = !active_theme_ref in
  let themes = all_themes_list in
  let total_themes = List.length themes in
  let modal_w = min (target_w - 4) 68 in
  let inner_w = modal_w - 4 in
  (* Compute maximum number of themes visible on screen to fit vertically *)
  let max_visible = max 5 (min total_themes (target_h - 7)) in
  let start_idx =
    if total_themes <= max_visible
    then 0
    else (
      let half = max_visible / 2 in
      let ideal = cursor_idx - half in
      max 0 (min (total_themes - max_visible) ideal))
  in
  let visible_themes =
    List.filteri (fun i _ -> i >= start_idx && i < start_idx + max_visible) themes
  in
  (* Title top with indicator *)
  let title_str =
    Printf.sprintf " ╭── 🎨 SELECT THEME (%d/%d) " (cursor_idx + 1) total_themes
  in
  let title_img = I.string A.(fg t.c_accent ++ bg t.c_panel ++ st bold) title_str in
  let fill_top = max 0 (modal_w - I.width title_img - 1) in
  let top_row =
    I.hcat
      [ title_img
      ; I.string A.(fg t.c_border ++ bg t.c_panel) (repeat_str "─" fill_top)
      ; I.string A.(fg t.c_border ++ bg t.c_panel) "╮"
      ]
  in
  let item_rows =
    List.mapi
      (fun relative_i (theme_item : theme_palette) ->
         let abs_i = start_idx + relative_i in
         let is_cursor = abs_i = cursor_idx in
         let is_active = theme_item.id = t.id in
         let row_bg = if is_cursor then t.c_selected else t.c_panel in
         let prefix =
           if is_cursor
           then I.string A.(fg t.c_accent ++ bg row_bg ++ st bold) " ▶ "
           else I.string A.(fg t.c_dim ++ bg row_bg) "   "
         in
         let check =
           if is_active
           then I.string A.(fg t.c_green ++ bg row_bg ++ st bold) "[●] "
           else I.string A.(fg t.c_dim ++ bg row_bg) "[ ] "
         in
         let id_attr =
           if is_cursor
           then A.(fg t.c_bright ++ bg row_bg ++ st bold)
           else if is_active
           then A.(fg t.c_title ++ bg row_bg ++ st bold)
           else A.(fg t.c_text ++ bg row_bg ++ st bold)
         in
         let id_img = I.string id_attr (Printf.sprintf "%-14s" theme_item.id) in
         let name_attr =
           if is_cursor
           then A.(fg t.c_cyan ++ bg row_bg)
           else A.(fg t.c_dim ++ bg row_bg)
         in
         let name_img =
           I.string name_attr (Printf.sprintf "%-19s" ("(" ^ theme_item.name ^ ")"))
         in
         let swatches =
           I.hcat
             [ I.string A.(fg theme_item.c_accent ++ bg row_bg) "■ "
             ; I.string A.(fg theme_item.c_green ++ bg row_bg) "■ "
             ; I.string A.(fg theme_item.c_red ++ bg row_bg) "■ "
             ; I.string A.(fg theme_item.c_cyan ++ bg row_bg) "■ "
             ; I.string A.(fg theme_item.c_yellow ++ bg row_bg) "■ "
             ; I.string A.(fg theme_item.c_border ++ bg row_bg) "■"
             ]
         in
         let line1_content = I.hcat [ prefix; check; id_img; name_img; swatches ] in
         let pad1 = max 0 (inner_w - I.width line1_content) in
         let line1 =
           I.hcat
             [ I.string A.(fg t.c_border ++ bg t.c_panel) " │ "
             ; line1_content
             ; I.string A.(bg row_bg) (String.make pad1 ' ')
             ; I.string A.(fg t.c_border ++ bg t.c_panel) " │"
             ]
         in
         line1)
      visible_themes
  in
  let div_row =
    I.hcat
      [ I.string A.(fg t.c_border ++ bg t.c_panel) " ├"
      ; I.string A.(fg t.c_border ++ bg t.c_panel) (repeat_str "─" (modal_w - 3))
      ; I.string A.(fg t.c_border ++ bg t.c_panel) "┤"
      ]
  in
  let scroll_hint =
    if start_idx > 0 && start_idx + max_visible < total_themes
    then "  [▲/▼ more]"
    else if start_idx > 0
    then "  [▲ more above]"
    else if start_idx + max_visible < total_themes
    then "  [▼ more below]"
    else ""
  in
  let hint_str = "  ↑/↓: Preview  │  Enter: Select  │  Esc: Close" ^ scroll_hint in
  let hint_img = I.string A.(fg t.c_dim ++ bg t.c_panel) hint_str in
  let pad_hint = max 0 (inner_w - I.width hint_img) in
  let hint_row =
    I.hcat
      [ I.string A.(fg t.c_border ++ bg t.c_panel) " │ "
      ; hint_img
      ; I.string A.(bg t.c_panel) (String.make pad_hint ' ')
      ; I.string A.(fg t.c_border ++ bg t.c_panel) " │"
      ]
  in
  let bot_row =
    I.hcat
      [ I.string A.(fg t.c_border ++ bg t.c_panel) " ╰"
      ; I.string A.(fg t.c_border ++ bg t.c_panel) (repeat_str "─" (modal_w - 3))
      ; I.string A.(fg t.c_border ++ bg t.c_panel) "╯"
      ]
  in
  let modal_content = I.vcat ([ top_row ] @ item_rows @ [ div_row; hint_row; bot_row ]) in
  let modal_h = I.height modal_content in
  let modal_w_real = I.width modal_content in
  let pad_left_w = max 0 ((target_w - modal_w_real) / 2) in
  let pad_top_h = max 0 ((target_h - modal_h) / 2) in
  let padded =
    I.hpad pad_left_w 0 modal_content
    |> I.vpad pad_top_h 0
    |> I.hsnap ~align:`Left target_w
    |> I.vsnap ~align:`Top target_h
  in
  padded
;;

