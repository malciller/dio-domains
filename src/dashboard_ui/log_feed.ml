open Notty
open Theme

(** ENGINE LOGS: a live, auto-scrolling feed of the engine's log stream — every line the
    configured logging level/sections admit, keyed by severity — newest at the bottom. It
    occupies the same main-view slot as ENGINE LATENCY; the two are swapped with `v` (see
    {!App}). Rows come from the engine's bounded log ring, shipped in the snapshot
    ([logs]) and the fast [type="log"] deltas.

    Layout: fixed `TIME | LEVEL | SECTION` columns, then a MESSAGE column that word-wraps.
    A message too long for one row continues on the next rows indented under the message
    column (hanging indent), so it never spills back under the TIME/LEVEL/SECTION headers.
    Color is coordinated: LEVEL and MESSAGE carry the severity accent, and SECTION uses
    the exchange's hue for symbol sections (falling back to a stable per-section hue). *)

(** Rows kept visible. A fixed height keeps the surrounding layout stable no matter how
    many lines arrived since the last frame; older lines scroll off the top. *)
let visible_rows = 12

let upper = String.uppercase_ascii

(** Severity -> accent, over the row background. Drives the LEVEL cell. *)
let level_attrs (t : Theme.theme_palette) row_bg lvl =
  let base = A.(bg row_bg) in
  match upper lvl with
  | "DEBUG" | "TRACE" -> A.(t.a_dim ++ base)
  | "WARN" | "WARNING" -> A.(fg t.c_yellow ++ base ++ st bold)
  | "ERROR" -> A.(fg t.c_red ++ base ++ st bold)
  | "CRITICAL" -> A.(fg t.c_red ++ base ++ st bold ++ st blink)
  | _ -> A.(fg t.c_green ++ base)
;;

(* INFO *)

(** Severity -> accent for the MESSAGE body, so an error row reads as one severity-colored
    line rather than a colored tag followed by neutral text. *)
let message_attrs (t : Theme.theme_palette) row_bg lvl =
  let base = A.(bg row_bg) in
  match upper lvl with
  | "WARN" | "WARNING" -> A.(fg t.c_yellow ++ base)
  | "ERROR" | "CRITICAL" -> A.(fg t.c_red ++ base)
  | "DEBUG" | "TRACE" -> A.(fg t.c_dim ++ base)
  | _ -> A.(t.a_text ++ base)
;;

(** Section accent: the exchange hue for a symbol section (coordinated with the rest of
    the dashboard), else a stable hue from a palette that avoids the severity colors so
    section and severity never collide. *)
let section_attrs (t : Theme.theme_palette) row_bg ~exch_of name =
  let base = A.(bg row_bg) in
  match exch_of name with
  | Some exch -> A.(Theme.exch_sym_attr exch ++ base)
  | None ->
    let palette = [| t.c_cyan; t.c_magenta; t.c_bright; t.c_label |] in
    let c = palette.(Hashtbl.hash name mod Array.length palette) in
    A.(fg c ++ base)
;;

let fmt_time ts =
  if ts <= 0.0
  then "            "
  else (
    let whole = int_of_float ts in
    let ms = int_of_float (((ts -. float_of_int whole) *. 1000.0) +. 0.5) in
    let ms = min 999 (max 0 ms) in
    let tm = Unix.localtime (float_of_int whole) in
    Printf.sprintf "%02d:%02d:%02d.%03d" tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec ms)
;;

(** Collapse a (possibly multi-line) log message onto logical text: newlines become a
    middle-dot separator so a flow block's continuation rows stay legible inline. *)
let flatten_message s =
  let b = Buffer.create (String.length s) in
  String.iter
    (fun c -> if c = '\n' then Buffer.add_string b " · " else Buffer.add_char b c)
    s;
  Buffer.contents b
;;

(** Greedy word-wrap to [width] cells. Splits only on spaces, so multi-byte glyphs (×, ·,
    Δ, ▲) are never split mid-character. A single over-long token is emitted whole and
    left for the outer renderer to crop. *)
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

(** One rendered row: the wrapped segment of a log line plus the columns to show (only on
    the first segment). *)
type row =
  { r_ts : float
  ; r_level : string
  ; r_section : string
  ; r_text : string
  ; r_first : bool
  }

let rows_of_entries ~msg_w entries =
  List.concat_map
    (fun (e : Snapshot.log_entry) ->
      let segs =
        match wrap_words ~width:msg_w (flatten_message e.le_message) with
        | [] -> [ "" ]
        | l -> l
      in
      List.mapi
        (fun i seg ->
          { r_ts = e.le_ts
          ; r_level = e.le_level
          ; r_section = e.le_section
          ; r_text = seg
          ; r_first = i = 0
          })
        segs)
    entries
;;

(** Section title with the panel selector embedded: the active panel is wrapped in ◀ ▶ and
    the switch key is shown, matching the latency panel's tab styling. *)
let title w =
  let t = Theme.current () in
  let left = I.string A.(fg t.c_title ++ bg t.c_bg ++ st bold) " ╭── ENGINE LOGS ── " in
  let active = I.string A.(fg t.c_cyan ++ bg t.c_bg ++ st bold) " ◀LOGS▶ " in
  let other = I.string t.a_dim "  LATENCY " in
  let hint = I.string t.a_dim "v" in
  let prefix = I.hcat [ left; active; other; hint; I.string A.(bg t.c_bg) " " ] in
  let len = I.width prefix in
  let pad_count = max 0 (w - len - 1) in
  let gradient =
    List.init pad_count (fun i ->
      let ratio = float i /. float (max 1 (pad_count - 1)) in
      let c = color_blend t.accent_rgb t.bg_rgb (ratio *. ratio) in
      I.string A.(fg c ++ bg t.c_bg) "─")
  in
  let end_border = I.string A.(fg t.c_border ++ bg t.c_bg) "╮" in
  I.hcat ((prefix :: gradient) @ [ end_border ])
;;

(* Column geometry: " │ " (3) + TIME (12) + " " (2) + LEVEL (8) + " " + SECTION (14) + " "
   = 41 cells before the message; close_row reserves 2 more for its trailing " │". *)
let prefix_cols = 41
let cont_cols = 3 + 12 + 2 + 8 + 1 + 14 + 1
let border_cols = 2

let render_logs w (snapshot : Snapshot.t) (entries : Snapshot.log_entry list) =
  let t = Theme.current () in
  let msg_w = max 4 (w - prefix_cols - border_cols) in
  let sym_to_exch =
    List.map (fun (sym, (s : Snapshot.strategy)) -> sym, s.exchange) snapshot.strategies
  in
  let exch_of name =
    match List.assoc_opt name sym_to_exch with
    | Some e when e <> "" -> Some e
    | _ -> None
  in
  let all_rows = rows_of_entries ~msg_w entries in
  let n = List.length all_rows in
  let shown =
    if n <= visible_rows
    then all_rows
    else List.filteri (fun i _ -> i >= n - visible_rows) all_rows
  in
  let close_row w row_bg img =
    let d = w - I.width img - 2 in
    I.hcat
      [ img
      ; I.string A.(bg row_bg) (String.make (max 0 d) ' ')
      ; I.string A.(bg row_bg) " "
      ; I.string A.(fg t.c_border ++ bg t.c_bg) "│"
      ]
  in
  let header =
    let row_bg = t.c_panel in
    let a_hdr = A.(t.a_dim ++ bg row_bg) in
    let a_b = A.(fg t.c_border ++ bg row_bg) in
    close_row
      w
      row_bg
      (I.hcat
         [ I.string a_b " │ "
         ; I.string a_hdr (pad_right 12 "TIME")
         ; I.string a_b "  "
         ; I.string a_hdr (pad_right 8 "LEVEL")
         ; I.string a_b " "
         ; I.string a_hdr (pad_right 14 "SECTION")
         ; I.string a_b " "
         ; I.string a_hdr "MESSAGE"
         ])
  in
  let render_row i (r : row) =
    let row_bg = if i mod 2 = 1 then t.c_panel else t.c_bg in
    let a_b = A.(fg t.c_border ++ bg row_bg) in
    let a_msg = message_attrs t row_bg r.r_level in
    let cols =
      if r.r_first
      then
        I.hcat
          [ I.string a_b " │ "
          ; I.string A.(t.a_dim ++ bg row_bg) (pad_right 12 (fmt_time r.r_ts))
          ; I.string a_b "  "
          ; I.string
              (level_attrs t row_bg r.r_level)
              (pad_right 8 (truncate_string 8 r.r_level))
          ; I.string a_b " "
          ; I.string
              (section_attrs t row_bg ~exch_of r.r_section)
              (pad_right 14 (truncate_string 14 r.r_section))
          ; I.string a_b " "
          ]
      else I.hcat [ I.string a_b " │ "; I.string a_b (String.make (cont_cols - 3) ' ') ]
    in
    close_row w row_bg (I.hcat [ cols; I.string a_msg r.r_text ])
  in
  (* Bottom-aligned: pad above the rows so the newest line stays on the last row. *)
  let rows = List.mapi render_row shown in
  let pad_rows =
    List.init
      (max 0 (visible_rows - List.length rows))
      (fun _ -> close_row w t.c_bg (I.string A.(bg t.c_bg) ""))
  in
  I.vcat ((title w :: header :: pad_rows) @ rows @ [ section_footer w ])
;;
