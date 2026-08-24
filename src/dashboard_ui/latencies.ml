open Notty
open Theme

(** ENGINE LATENCY, a paginated metric table.

    The section renders one row per domain, but instead of cramming every
    measurement into a fixed set of columns (which capped the old design at
    4 metrics / about 170 columns), the columns are organized into PAGES.
    Each page is a named group of metric columns; only the active page's
    columns are rendered. The rows, trend sparkline, and exec-rate columns
    adapt to the active page.

    Adding a new latency measurement, for example a per-domain network or
    request latency, is now just a matter of adding its label to a page in
    [metric_pages]; the engine publishes it under the domain's latency map
    and it appears. No layout math, no width crisis. Switch pages with
    ←/→ in the main view.
*)

let history_len = 15
let hist_tbl : (string, float array) Hashtbl.t = Hashtbl.create 32
let hist_max = 128

(** Active latency page (index into [metric_pages]). Switched with ←/→ from
    the main view via [next_page]/[prev_page]. *)
let active_page_ref = ref 0

(** Exponential moving average over a history array. Smooths the windowed
    p99 samples so the sparkline tracks trend rather than single-window noise. *)
let ema_smooth (arr : float array) ~alpha =
  let out = Array.copy arr in
  let prev = ref arr.(0) in
  for i = 1 to Array.length arr - 1 do
    prev := (alpha *. arr.(i)) +. ((1.0 -. alpha) *. !prev);
    out.(i) <- !prev
  done;
  out
;;

(** Rolling p99 history keyed by (symbol, metric) so each latency page keeps
    its own sparkline, and switching pages never clobbers another page's
    trend. Evicts everything when the table outgrows [hist_max]; the
    sparklines repopulate within seconds. *)
let update_hist symbol metric p99 =
  if Hashtbl.length hist_tbl > hist_max then Hashtbl.clear hist_tbl;
  let key = symbol ^ "\x00" ^ metric in
  let arr =
    try Hashtbl.find hist_tbl key with
    | Not_found ->
      let a = Array.make history_len 0.0 in
      Hashtbl.add hist_tbl key a;
      a
  in
  for i = 0 to history_len - 2 do
    arr.(i) <- arr.(i + 1)
  done;
  arr.(history_len - 1) <- p99;
  arr
;;

(** Last measured window values per (symbol, metric), persisted across idle
    windows so that short-lived measurements, such as signer, ws ping, or
    rest request, which only get samples when an event actually happens,
    keep showing their last value instead of flipping back to "idle". Only
    refreshed by windows with samples; a window with zero samples keeps the
    previous values rendered dimmed until a fresh one arrives. Evicts
    everything when the table outgrows [last_vals_max]. *)
let last_vals : (string, float * float * float) Hashtbl.t = Hashtbl.create 64

let last_vals_max = 256
let last_value symbol metric = Hashtbl.find_opt last_vals (symbol ^ "\x00" ^ metric)

(** Freshness tolerance per metric label: the oracle windows are published
    once per analysis pass (~5 min cadence + jitter), everything else every
    ~15s. *)
let freshness_tolerance = function
  | "oracle" -> 600.0
  | _ -> 15.0
;;

(** A latency page: a named group of per-domain metric columns. Each page
    renders the same domain rows with a different set of measurement columns,
    so the ENGINE LATENCY section can grow arbitrarily many metrics without
    widening the table. *)
type metric_group =
  { page_label : string
  ; metrics : string list
  ; trend_metric : string (* metric label plotted in the trend column *)
  ; trend_label : string (* header shown above the trend column *)
  ; trend_max_us : float (* full-scale value for the trend sparkline *)
  }

(** The latency pages.
    - CORE: the per-domain pipeline measurements, namely the oracle pass,
      orderbook update, strategy run, execution broadcast, and the full
      cycle span (wake -> consume -> strategy -> exec), all in one table.
    - NETWORK: per-domain network/request latencies (ws ping RTT, ws feed
      gap, REST round-trip, signer time). The engine does not publish these
      yet, so the cells render "--" until it does. *)
let metric_pages =
  [ { page_label = "CORE"
    ; metrics = [ "oracle"; "orderbook"; "strategy"; "execution"; "cycle" ]
    ; trend_metric = "oracle"
    ; trend_label = "(ORACLE P99)"
    ; trend_max_us = 10.0
    }
  ; { page_label = "NETWORK"
    ; metrics = [ "ws_ping"; "ws_feed"; "rest_request"; "signer" ]
    ; trend_metric = "ws_feed"
    ; trend_label = "(FEED P99)"
    ; trend_max_us = 50_000.0
    }
  ]
;;

let page_count () = List.length metric_pages
let current_page_index () = min !active_page_ref (max 0 (page_count () - 1))
let next_page () = active_page_ref := (current_page_index () + 1) mod page_count ()

let prev_page () =
  active_page_ref := (current_page_index () - 1 + page_count ()) mod page_count ()
;;

let set_page i = active_page_ref := max 0 (min (page_count () - 1) i)

let page_metrics i =
  match List.nth_opt metric_pages i with
  | Some p -> p.metrics
  | None -> []
;;

let page_trend_label i =
  match List.nth_opt metric_pages i with
  | Some p -> p.trend_label
  | None -> ""
;;

(** Width of the trend column (sparkline + header). The trend header label
    and the sparkline must both stay within this width; a longer label
    would silently overrun the column and shift every border to its right
    out of alignment. Guarded by a test in test_dashboard_holdings. *)
let trend_col_w = 12

(** Short header label for a latency metric. *)
let short_label = function
  | "oracle" -> "ORACLE"
  | "orderbook" -> "OB"
  | "strategy" -> "STRAT"
  | "execution" -> "EXEC"
  | "cycle" -> "CYCLE"
  | "ws_ping" -> "PING"
  | "ws_feed" -> "FEED"
  | "rest_request" -> "REST"
  | "signer" -> "SIGN"
  | l -> String.uppercase_ascii (truncate_string 6 l)
;;

let take_first n l =
  let rec aux acc n = function
    | [] -> List.rev acc
    | h :: t -> if n <= 0 then List.rev acc else aux (h :: acc) (n - 1) t
  in
  aux [] n l
;;

(** Section title with the page tabs embedded: the active page is wrapped in
    ◀ ▶ (bold cyan), inactive pages are dim, and the ←/→ hint marks the
    switch keys. *)
let render_latency_title w =
  let t = Theme.current () in
  let left = I.string A.(fg t.c_title ++ bg t.c_bg ++ st bold) " ╭── ENGINE LATENCY ── " in
  let tabs =
    List.mapi
      (fun i p ->
         if i = current_page_index ()
         then I.string A.(fg t.c_cyan ++ bg t.c_bg ++ st bold) (" ◀" ^ p.page_label ^ "▶ ")
         else I.string t.a_dim ("  " ^ p.page_label ^ " "))
      metric_pages
  in
  let hint = I.string t.a_dim "←/→" in
  let prefix = I.hcat (left :: (tabs @ [ hint; I.string A.(bg t.c_bg) " " ])) in
  let len = I.width prefix in
  let pad_count = max 0 (w - len - 1) in
  let left_rgb = t.accent_rgb in
  let right_rgb = t.bg_rgb in
  let gradient_lines =
    List.init pad_count (fun i ->
      let ratio = float i /. float (max 1 (pad_count - 1)) in
      let fade = ratio *. ratio in
      let c = color_blend left_rgb right_rgb fade in
      I.string A.(fg c ++ bg t.c_bg) "─")
  in
  let end_border = I.string A.(fg t.c_border ++ bg t.c_bg) "╮" in
  I.hcat ((prefix :: gradient_lines) @ [ end_border ])
;;

let render_latencies w json =
  let t = Theme.current () in
  let lats =
    match json |?> "latencies" with
    | `Assoc l -> l
    | _ -> []
  in
  (* Build a symbol -> exchange lookup table from the strategies. *)
  let sym_to_exch =
    match json |?> "strategies" with
    | `Assoc l ->
      List.map (fun (sym, data) -> sym, data |?> "exchange" |> to_string_d "") l
    | _ -> []
  in
  let exch_of_symbol sym =
    match List.assoc_opt sym sym_to_exch with
    | Some e when e <> "" -> e
    | _ -> ""
  in
  (* Filter to rows with at least one fresh metric window (published within
     the snapshot timestamp's freshness tolerance for that metric). A running
     domain always publishes a window even with zero samples, so idle-but-
     running domains stay visible instead of flickering out between resets.
     Freshness is checked across ALL pages so rows stay stable when the user
     flips pages. *)
  let snapshot_ts = json |?> "timestamp" |> to_float_d 0.0 in
  let all_page_labels = List.concat_map (fun p -> p.metrics) metric_pages in
  let row_is_active (_symbol, metrics) =
    let mlist =
      match metrics with
      | `Assoc l -> l
      | _ -> []
    in
    List.exists
      (fun label ->
         match List.assoc_opt label mlist with
         | Some data ->
           let window_end = data |?> "window_end" |> to_float_d 0.0 in
           window_end > 0.0
           && snapshot_ts > 0.0
           && snapshot_ts -. window_end < freshness_tolerance label
         | None -> false)
      all_page_labels
  in
  let active_lats = List.filter row_is_active lats in
  if active_lats = []
  then I.empty
  else (
    (* Per-metric latency thresholds: (yellow_us, red_us). *)
    let latency_thresholds label =
      match label with
      | "orderbook" -> 10.0, 30.0
      | "strategy" -> 30.0, 75.0
      | "execution" -> 50.0, 150.0
      | "oracle" -> 1_000_000.0, 10_000_000.0
      | "cycle" -> 100.0, 1_000.0
      | "ws_ping" -> 20_000.0, 100_000.0
      | "ws_feed" -> 50_000.0, 200_000.0
      | "rest_request" -> 100_000.0, 500_000.0
      | "signer" -> 1_000.0, 10_000.0
      | _ -> 50.0, 100.0
    in
    let severity label f samples =
      if samples = 0
      then 3 (* dim *)
      else (
        let warn, crit = latency_thresholds label in
        if f > crit
        then 2 (* red *)
        else if f > warn
        then 1 (* yellow *)
        else 0 (* green *))
    in
    let page = List.nth metric_pages (current_page_index ()) in
    let n_metrics = List.length page.metrics in
    let full_page_w = 71 + (27 * (n_metrics - 1)) in
    let page_cols = if w < full_page_w then take_first 2 page.metrics else page.metrics in
    let page_labels = List.map short_label page_cols in
    let metric_cell_w = 8 in
    (* Two-row header: metric names on the first row and p50/p99/p999
       sub-headers on the second. *)
    let header_row1 =
      I.hcat
        ([ I.string t.a_border " │  "
         ; col 13 t.a_label ""
         ; I.string t.a_border " │ "
         ; col trend_col_w t.a_label (pad_right trend_col_w "   TREND   ")
         ; I.string t.a_border " │ "
         ]
         @ List.mapi
             (fun i lbl ->
                let len = String.length lbl in
                let pad = (24 - len) / 2 in
                let s = String.make pad ' ' ^ lbl in
                let img = col 24 t.a_label s in
                if i = 0 then img else I.hcat [ I.string t.a_border " │ "; img ])
             page_labels
         @ [ I.string t.a_border " │ "; col 7 t.a_label "STRAT/S" ])
    in
    let header_row2 =
      I.hcat
        ([ I.string t.a_border " │  "
         ; col 13 t.a_label "DOMAIN"
         ; I.string t.a_border " │ "
         ; col trend_col_w t.a_dim (truncate_string trend_col_w page.trend_label)
         ; I.string t.a_border " │ "
         ]
         @ List.mapi
             (fun i _lbl ->
                let img =
                  I.hcat
                    [ col_right metric_cell_w t.a_dim "p50"
                    ; col_right metric_cell_w t.a_dim "p99"
                    ; col_right metric_cell_w t.a_dim "p999"
                    ]
                in
                if i = 0 then img else I.hcat [ I.string t.a_border " │ "; img ])
             page_labels
         @ [ I.string t.a_border " │ "; col 7 t.a_dim "  rate  " ])
    in
    let header = I.vcat [ close_row w header_row1; close_row w header_row2 ] in
    let rows =
      List.mapi
        (fun i (symbol, metrics) ->
           let bg_color = if i mod 2 = 1 then t.c_panel else t.c_bg in
           let a_text = A.(t.a_text ++ bg bg_color) in
           let a_green = A.(t.a_green ++ bg bg_color) in
           let a_green_dark = A.(t.a_green_dark ++ bg bg_color) in
           let a_red = A.(t.a_red ++ bg bg_color) in
           let a_yellow = A.(t.a_yellow ++ bg bg_color) in
           let a_dim = A.(t.a_dim ++ bg bg_color) in
           let a_border = A.(t.a_border ++ bg bg_color) in
           let a_border_outer = A.(t.a_border ++ bg t.c_bg) in
           let a_bright = A.(t.a_bright ++ bg bg_color) in
           let exch_sym_attr ?dim exch =
             A.(Theme.exch_sym_attr ?dim exch ++ bg bg_color)
           in
           let attr_of_sev = function
             | 2 -> a_red
             | 1 -> a_yellow
             | 0 -> a_green
             | _ -> a_dim
           in
           let latency_cell_attr sev f =
             if Theme.is_sub_us f then a_green_dark else sev
           in
           let col w attr s = I.string attr (Theme.pad_right w s) in
           let col_right w attr s = I.string attr (Theme.pad_left w s) in
           let close_row w img =
             let d = w - I.width img - 2 in
             I.hcat
               [ img
               ; I.string A.(bg bg_color) (String.make (max 0 d) ' ')
               ; I.string A.(bg bg_color) " "
               ; I.string a_border_outer "│"
               ]
           in
           let render_sparkline_local w data max_val attr_fn =
             let len = Array.length data in
             let start_idx = max 0 (len - w) in
             let visible_len = min w len in
             let empty_w = w - visible_len in
             let blocks =
               List.init visible_len (fun idx ->
                 let v = data.(start_idx + idx) in
                 let ratio = if max_val > 0.0 then v /. max_val else 0.0 in
                 let ratio = max 0.0 (min 1.0 ratio) in
                 let block_idx = int_of_float (ratio *. 7.0) in
                 let block_idx = max 0 (min 7 block_idx) in
                 I.string (attr_fn v) Theme.block_chars.(block_idx))
             in
             I.hcat (I.string a_dim (String.make empty_w ' ') :: blocks)
           in
           let mlist =
             match metrics with
             | `Assoc l -> l
             | _ -> []
           in
           let find_metric label =
             match List.assoc_opt label mlist with
             | Some data ->
               let p50 = data |?> "p50" |> to_float_d 0.0 in
               let p99 = data |?> "p99" |> to_float_d 0.0 in
               let p999 = data |?> "p999" |> to_float_d 0.0 in
               let samples = data |?> "samples" |> to_int_d 0 in
               p50, p99, p999, samples
             | None -> 0.0, 0.0, 0.0, 0
           in
           let worst_sev =
             List.fold_left
               (fun worst label ->
                  let _, p99, _, samples = find_metric label in
                  if samples = 0 then worst else max worst (severity label p99 samples))
               0
               page_cols
           in
           let dot_attr = attr_of_sev worst_sev in
           let metric_cells =
             List.mapi
               (fun i label ->
                  let p50, p99, p999, samples = find_metric label in
                  let known = List.mem_assoc label mlist in
                  let img =
                    if samples > 0
                    then (
                      Hashtbl.replace last_vals (symbol ^ "\x00" ^ label) (p50, p99, p999);
                      let s50 = severity label p50 samples in
                      let s99 = max s50 (severity label p99 samples) in
                      let s999 = max s99 (severity label p999 samples) in
                      I.hcat
                        [ col_right
                            metric_cell_w
                            (latency_cell_attr (attr_of_sev s50) p50)
                            (format_latency_us p50)
                        ; col_right
                            metric_cell_w
                            (latency_cell_attr (attr_of_sev s99) p99)
                            (format_latency_us p99)
                        ; col_right
                            metric_cell_w
                            (latency_cell_attr (attr_of_sev s999) p999)
                            (format_latency_us p999)
                        ])
                    else (
                      match last_value symbol label with
                      | Some (lp50, lp99, lp999) ->
                        I.hcat
                          [ col_right metric_cell_w a_dim (format_latency_us lp50)
                          ; col_right metric_cell_w a_dim (format_latency_us lp99)
                          ; col_right metric_cell_w a_dim (format_latency_us lp999)
                          ]
                      | None ->
                        let cell_w = 3 * metric_cell_w in
                        let pad = (cell_w - 4) / 2 in
                        col
                          cell_w
                          a_dim
                          (String.make pad ' ' ^ if known then "idle" else "--"))
                  in
                  if i = 0 then img else I.hcat [ I.string a_border " │ "; img ])
               page_cols
           in
           let _, trend_p99, _, trend_samples = find_metric page.trend_metric in
           let trend_p99, trend_stale =
             if trend_samples > 0
             then trend_p99, false
             else (
               match last_value symbol page.trend_metric with
               | Some (_, lp99, _) -> lp99, true
               | None -> trend_p99, false)
           in
           let t_arr = update_hist symbol page.trend_metric trend_p99 in
           let t_smooth = ema_smooth t_arr ~alpha:0.5 in
           let trend_spark =
             render_sparkline_local trend_col_w t_smooth page.trend_max_us (fun v ->
               if trend_stale
               then a_dim
               else latency_cell_attr (attr_of_sev (severity page.trend_metric v 1)) v)
           in
           let exch = exch_of_symbol symbol in
           let sym_attr = if exch <> "" then exch_sym_attr exch else a_bright in
           let exec_s_cell =
             match List.assoc_opt "strategy" mlist with
             | Some data ->
               let eps = data |?> "executions_per_sec" |> to_float_d 0.0 in
               let execs = data |?> "executions" |> to_int_d 0 in
               if execs > 0
               then col_right 7 a_bright (Printf.sprintf "%.1f/s" eps)
               else col_right 7 a_dim "idle"
             | None -> col_right 7 a_dim "--"
           in
           close_row
             w
             (I.hcat
                ([ I.string a_border_outer " │"
                 ; I.string A.(bg bg_color) "  "
                 ; I.string dot_attr "●"
                 ; I.string a_text " "
                 ; col 11 sym_attr (truncate_string 10 symbol)
                 ; I.string a_border " │ "
                 ; trend_spark
                 ; I.string a_border " │ "
                 ]
                 @ metric_cells
                 @ [ I.string a_border " │ "; exec_s_cell ])))
        active_lats
    in
    let title = render_latency_title w in
    I.vcat ((title :: header :: rows) @ [ section_footer w ]))
;;
