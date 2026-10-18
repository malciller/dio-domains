open Notty
open Theme

(** Alert popup: batched WARN/ERROR/CRITICAL log lines surfaced as a modal overlay on top
    of the dashboard until the user acknowledges them.

    Batching is what keeps this from spamming: identical lines (same severity/section/
    message) collapse into one row with a `x N` counter, and the row count is bounded, so
    a storm of the same error is one line, not thousands. The severity that triggers the
    popup is configurable via the top-level `alert_level` config key.

    The modal is deliberately loud: a red double-line frame, a filled title bar, a
    blinking acknowledge hint, and (via {!App}) a dimmed background behind it, so it
    cannot be mistaken for part of the dashboard. *)

(** Which severities raise the popup. Default [Warn] (WARN and above). *)
type threshold =
  | Off
  | Info
  | Warn
  | Error
  | Critical

let severity_rank = function
  | "DEBUG" | "TRACE" -> 0
  | "WARN" | "WARNING" -> 2
  | "ERROR" -> 3
  | "CRITICAL" -> 4
  | _ -> 1 (* INFO *)
;;

let threshold_rank = function
  | Off -> max_int
  | Info -> 1
  | Warn -> 2
  | Error -> 3
  | Critical -> 4
;;

let threshold_of_string s =
  match String.lowercase_ascii (String.trim s) with
  | "off" | "none" | "false" | "0" -> Off
  | "info" -> Info
  | "warn" | "warning" -> Warn
  | "error" -> Error
  | "critical" | "crit" -> Critical
  | _ -> Warn
;;

let threshold_to_string = function
  | Off -> "off"
  | Info -> "info"
  | Warn -> "warn"
  | Error -> "error"
  | Critical -> "critical"
;;

(** [notifies t level] is true when a log at [level] should raise the popup. *)
let notifies t level = severity_rank (String.uppercase_ascii level) >= threshold_rank t

(* ---- Batched alert set --------------------------------------------------- *)

type entry =
  { a_level : string
  ; a_section : string
  ; a_message : string
  ; mutable a_count : int
  }

(** Cap on distinct batched rows; beyond this the oldest are dropped. Duplicates never
    grow the list (they bump [a_count]), so a repeated storm stays one row. *)
let max_rows = 24

(** Insert [level]/[section]/[message] into [entries] (newest first), collapsing an
    identical existing row into a counter bump. Returns the (possibly extended) list. *)
let add entries ~level ~section ~message =
  let level = String.uppercase_ascii level in
  match
    List.find_opt
      (fun e -> e.a_level = level && e.a_section = section && e.a_message = message)
      entries
  with
  | Some e ->
    e.a_count <- e.a_count + 1;
    entries
  | None ->
    let e = { a_level = level; a_section = section; a_message = message; a_count = 1 } in
    let entries = e :: entries in
    if List.length entries > max_rows
    then List.filteri (fun i _ -> i < max_rows) entries
    else entries
;;

let level_color (t : Theme.theme_palette) lvl =
  match String.uppercase_ascii lvl with
  | "CRITICAL" | "ERROR" -> t.c_red
  | "WARN" | "WARNING" -> t.c_yellow
  | _ -> t.c_cyan
;;

let wrap_words ~width s =
  if width <= 0
  then [ s ]
  else (
    let words = String.split_on_char ' ' s in
    let lines = ref [] in
    let cur = Buffer.create width in
    List.iter
      (fun wd ->
        let cl = Buffer.length cur in
        if cl = 0
        then Buffer.add_string cur wd
        else if cl + 1 + String.length wd <= width
        then (
          Buffer.add_char cur ' ';
          Buffer.add_string cur wd)
        else (
          lines := Buffer.contents cur :: !lines;
          Buffer.clear cur;
          Buffer.add_string cur wd))
      words;
    lines := Buffer.contents cur :: !lines;
    List.rev !lines)
;;

(** One rendered row: a wrapped message segment plus the columns to show on its first
    segment. *)
type row =
  { r_first : bool
  ; r_level : string
  ; r_count : int
  ; r_section : string
  ; r_text : string
  }

(** Render the alert modal, centered over a [target_w] x [target_h] frame. Newest alerts
    first; rows that overflow the height are dropped (with a "+K more" footer). *)
let render_modal ~target_w ~target_h (entries : entry list) =
  let t = Theme.current () in
  let n = List.length entries in
  let modal_w = max 48 (min (target_w - 6) 100) in
  let modal_w = min modal_w (max 16 target_w) in
  let inner_w = max 10 (modal_w - 2) in
  let level_w = 9 in
  let section_w = 13 in
  (* Inner content layout: " " + level + " " + section + " " + message. *)
  let msg_w = max 8 (inner_w - level_w - section_w - 3) in
  let a_frame = A.(fg t.c_red ++ bg t.c_panel) in
  let a_panel = A.(bg t.c_panel) in
  (* Title bar: black on red, filled to the inner width. *)
  let title_str = Printf.sprintf " ⚠  ALERTS (%d) " n in
  let title_img = I.string A.(fg black ++ bg t.c_red ++ st bold) title_str in
  let title_pad = max 0 (inner_w - I.width title_img) in
  let top_row =
    I.hcat
      [ I.string a_frame "╔"
      ; title_img
      ; I.string A.(bg t.c_red) (String.make title_pad ' ')
      ; I.string a_frame "╗"
      ]
  in
  let rows =
    List.concat_map
      (fun (e : entry) ->
        let segs =
          match wrap_words ~width:msg_w e.a_message with
          | [] -> [ "" ]
          | l -> l
        in
        List.mapi
          (fun i seg ->
            { r_first = i = 0
            ; r_level = e.a_level
            ; r_count = e.a_count
            ; r_section = e.a_section
            ; r_text = seg
            })
          segs)
      entries
  in
  let max_body = max 3 (target_h - 8) in
  let shown =
    if List.length rows <= max_body
    then rows
    else List.filteri (fun i _ -> i < max_body) rows
  in
  let overflow = List.length rows - List.length shown in
  let wrap_inner (inner : I.t) =
    let pad = max 0 (inner_w - I.width inner) in
    I.hcat
      [ I.string a_frame "║"
      ; inner
      ; I.string a_panel (String.make pad ' ')
      ; I.string a_frame "║"
      ]
  in
  let body_row (r : row) =
    let cols =
      if r.r_first
      then (
        let lvl =
          if r.r_count > 1 then Printf.sprintf "%s×%d" r.r_level r.r_count else r.r_level
        in
        I.hcat
          [ I.string
              A.(fg (level_color t r.r_level) ++ bg t.c_panel ++ st bold)
              (pad_right level_w (truncate_string level_w lvl))
          ; I.string a_panel " "
          ; I.string
              A.(fg t.c_bright ++ bg t.c_panel)
              (pad_right section_w (truncate_string section_w r.r_section))
          ; I.string a_panel " "
          ])
      else I.string a_panel (String.make (level_w + section_w + 2) ' ')
    in
    wrap_inner
      (I.hcat
         [ I.string a_panel " "; cols; I.string A.(t.a_text ++ bg t.c_panel) r.r_text ])
  in
  let body_rows = List.map body_row shown in
  let overflow_row =
    if overflow > 0
    then
      [ wrap_inner
          (I.string
             A.(t.a_dim ++ bg t.c_panel)
             ("  … +" ^ string_of_int overflow ^ " more (acknowledge to clear)"))
      ]
    else []
  in
  let div_row =
    I.hcat
      [ I.string a_frame "╟"
      ; I.string a_frame (repeat_str "─" inner_w)
      ; I.string a_frame "╢"
      ]
  in
  let hint_row =
    wrap_inner
      (I.string
         A.(fg t.c_red ++ bg t.c_panel ++ st bold ++ st blink)
         "  Enter / Esc : acknowledge")
  in
  let bot_row =
    I.hcat
      [ I.string a_frame "╚"
      ; I.string a_frame (repeat_str "═" inner_w)
      ; I.string a_frame "╝"
      ]
  in
  let modal_content =
    I.vcat ((top_row :: body_rows) @ overflow_row @ [ div_row; hint_row; bot_row ])
  in
  let modal_h = I.height modal_content in
  let modal_w_real = I.width modal_content in
  let pad_left_w = max 0 ((target_w - modal_w_real) / 2) in
  let pad_top_h = max 0 ((target_h - modal_h) / 2) in
  I.hpad pad_left_w 0 modal_content
  |> I.vpad pad_top_h 0
  |> I.hsnap ~align:`Left target_w
  |> I.vsnap ~align:`Top target_h
;;
