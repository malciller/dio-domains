open Notty
open Theme

(** KPI Cards Component.
    Renders top-row summary cards in a unified Btop/Terminal panel container. *)

let render_card_row w cards =
  let t = Theme.current () in
  let n = List.length cards in
  let inner_total = max 0 (w - 2 - (n * 2)) in
  let base_w = inner_total / n in
  let rem = inner_total mod n in
  let card_inner_w i = base_w + if i < rem then 1 else 0 in
  let top_imgs = ref [ I.string A.(fg t.c_border ++ bg t.c_bg) " ╭" ] in
  let bot_imgs = ref [ I.string A.(fg t.c_border ++ bg t.c_bg) " ╰" ] in
  let body_row1_imgs = ref [ I.string A.(fg t.c_border ++ bg t.c_bg) " │" ] in
  let body_row2_imgs = ref [ I.string A.(fg t.c_border ++ bg t.c_bg) " │" ] in
  List.iteri
    (fun i (title, r1, r2) ->
       let iw = card_inner_w i in
       let is_last = i = n - 1 in
       (* Build the top bar piece for this card, spanning the title and
          the remaining dashes. *)
       let title_str = "── " ^ title ^ " " in
       let title_img = I.string A.(fg t.c_title ++ bg t.c_bg ++ st bold) title_str in
       let title_len = I.width title_img in
       let dash_count = max 0 (iw + 1 - title_len) in
       let dashes = I.string A.(fg t.c_border ++ bg t.c_bg) (repeat_str "─" dash_count) in
       let div_top =
         I.string A.(fg t.c_border ++ bg t.c_bg) (if is_last then "╮" else "┬")
       in
       top_imgs := !top_imgs @ [ title_img; dashes; div_top ];
       (* Build the two body rows for this card. *)
       let div_mid = I.string A.(fg t.c_border ++ bg t.c_bg) "│" in
       let c_r1 =
         I.hcat [ I.string A.(bg t.c_bg) " "; I.hsnap ~align:`Left iw r1; div_mid ]
       in
       let c_r2 =
         I.hcat [ I.string A.(bg t.c_bg) " "; I.hsnap ~align:`Left iw r2; div_mid ]
       in
       body_row1_imgs := !body_row1_imgs @ [ c_r1 ];
       body_row2_imgs := !body_row2_imgs @ [ c_r2 ];
       (* Build the bottom bar piece for this card. *)
       let bot_dashes =
         I.string A.(fg t.c_border ++ bg t.c_bg) (repeat_str "─" (iw + 1))
       in
       let div_bot =
         I.string A.(fg t.c_border ++ bg t.c_bg) (if is_last then "╯" else "┴")
       in
       bot_imgs := !bot_imgs @ [ bot_dashes; div_bot ])
    cards;
  I.vcat
    [ I.hcat !top_imgs; I.hcat !body_row1_imgs; I.hcat !body_row2_imgs; I.hcat !bot_imgs ]
;;

let render_kpi_cards w (s : Snapshot.t) =
  let t = Theme.current () in
  let strats = s.strategies in
  let all_balances = s.balances in
  let total_hold_strats =
    List.fold_left
      (fun hv_acc (_sym, (st : Snapshot.strategy)) ->
         hv_acc +. (st.market.base_balance *. st.market.mid))
      0.0
      strats
  in
  let total_hold_bals, total_quote_val =
    List.fold_left
      (fun (hv_acc, q_acc) (b : Snapshot.balance) ->
         if b.balance <= 0.0
         then hv_acc, q_acc
         else if Snapshot.is_quote_asset b.asset
         then hv_acc, q_acc +. b.balance
         else hv_acc +. (b.balance *. b.mid), q_acc)
      (0.0, 0.0)
      all_balances
  in
  let total_hold_val = total_hold_strats +. total_hold_bals in
  let net_worth = total_hold_val +. total_quote_val in
  (* Animate headline money so updates roll rather than snap. Cheap now that
     the renderer transmits only changed rows. *)
  let net_worth_t = Anim.tween ~key:"kpi.networth" ~target:net_worth ~tau:0.35 in
  let cash_t = Anim.tween ~key:"kpi.cash" ~target:total_quote_val ~tau:0.35 in
  let c1_row1 =
    I.hcat
      [ col 10 t.a_dim "NET WORTH"; col_right 12 t.a_bright (format_usd net_worth_t) ]
  in
  (* The PORTFOLIO card shows cash on the second line: accumulated value
     already has its own slot in the HOLDINGS & STRATEGY summary bar. *)
  let c1_row2 =
    I.hcat [ col 10 t.a_dim "CASH"; col_right 12 t.a_cyan (format_usd cash_t) ]
  in
  let card1 = "PORTFOLIO", c1_row1, c1_row2 in
  let uptime = s.uptime_s in
  let recent_fills = s.fills in
  let lats = s.latencies in
  let snapshot_ts = s.timestamp in
  (* Classify strategy activity from consistent windows: a strategy is
     active when it ran this window and idle when it is running with a
     fresh cycle window but executed nothing (the S1/S2 states). *)
  let strat_active, strat_idle, exec_per_sec =
    List.fold_left
      (fun (a, i, e) (_sym, (metrics : (string * Snapshot.latency_metric) list)) ->
         match List.assoc_opt "strategy" metrics with
         | Some m ->
           let fresh =
             m.window_end > 0.0 && snapshot_ts > 0.0 && snapshot_ts -. m.window_end < 15.0
           in
           if not fresh
           then a, i, e
           else if m.executions > 0
           then a + 1, i, e +. m.executions_per_sec
           else a, i + 1, e
         | None -> a, i, e)
      (0, 0, 0.0)
      lats
  in
  let c2_row1 =
    I.hcat
      [ col 10 t.a_dim "STRATEGIES"
      ; col_right
          20
          t.a_green
          (Printf.sprintf "%d active / %d idle" strat_active strat_idle)
      ]
  in
  let c2_row2 =
    I.hcat
      [ col 10 t.a_dim "UPTIME"
      ; col_right
          28
          t.a_text
          (format_duration uptime
           ^ " │ "
           ^ string_of_int (List.length recent_fills)
           ^ " fills │ "
           ^ Printf.sprintf "%.1f/s" exec_per_sec)
      ]
  in
  let card2 = "SYSTEM ENGINE", c2_row1, c2_row2 in
  (* Capital-oracle engine latency, taken from the oracle runtime's per-pass
     window, replaces the old per-domain cycle column: it shows the p50/p99
     of the most recently completed oracle pass. The reading is fresh when a
     pass window exists within the refresh horizon, since the oracle
     re-analyzes roughly every 5 minutes. *)
  let oracle_lat = List.assoc_opt "pass" s.oracle_latency in
  let oracle_p50, oracle_p99, oracle_fresh =
    match oracle_lat with
    | Some m ->
      let fresh =
        m.window_end > 0.0 && snapshot_ts > 0.0 && snapshot_ts -. m.window_end < 600.0
      in
      m.p50, m.p99, fresh && m.samples > 0
    | None -> 0.0, 0.0, false
  in
  (* Oracle pass thresholds: a pass normally completes in a few seconds
     (history fetches dominate); 5s+ warrants yellow, 30s+ red. *)
  let lat_attr p =
    if not oracle_fresh
    then t.a_dim
    else if p > 30_000_000.0
    then t.a_red
    else if p > 5_000_000.0
    then t.a_yellow
    else t.a_green
  in
  (* Sub-microsecond readings render dark green (nanosecond-resolution);
     everything else keeps the severity color. *)
  let latency_cell_attr p = if is_sub_us p then t.a_green_dark else lat_attr p in
  let c3_row1 =
    I.hcat
      [ col 10 t.a_dim "ORACLE P50"
      ; col_right 12 (latency_cell_attr oracle_p50) (format_latency_us oracle_p50)
      ]
  in
  let c3_row2 =
    I.hcat
      [ col 10 t.a_dim "ORACLE P99"
      ; col_right 12 (latency_cell_attr oracle_p99) (format_latency_us oracle_p99)
      ]
  in
  let card3 = "LATENCY", c3_row1, c3_row2 in
  let mem = s.memory in
  let heap_mb = mem.heap_mb in
  let live_kb = mem.live_kb in
  let free_kb = mem.free_kb in
  let total_kb = float_of_int (live_kb + free_kb) in
  let live_pct =
    if total_kb > 0.0 then float_of_int live_kb /. total_kb *. 100.0 else 0.0
  in
  let c4_row1 =
    I.hcat
      [ col 10 t.a_dim "HEAP SIZE"
      ; col_right 12 t.a_yellow (Printf.sprintf "%d MB" heap_mb)
      ]
  in
  let c4_row2 =
    I.hcat
      [ col 10 t.a_dim "LIVE RATIO"
      ; col_right 12 t.a_green (Printf.sprintf "%.1f%%" live_pct)
      ]
  in
  let card4 = "MEMORY / GC", c4_row1, c4_row2 in
  if w < 100
  then I.vcat [ render_card_row w [ card1; card2 ]; render_card_row w [ card3; card4 ] ]
  else render_card_row w [ card1; card2; card3; card4 ]
;;
