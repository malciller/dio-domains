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
       let dashes =
         I.string
           A.(fg t.c_border ++ bg t.c_bg)
           (String.concat "" (List.init dash_count (fun _ -> "─")))
       in
       let div_top = I.string A.(fg t.c_border ++ bg t.c_bg) (if is_last then "╮" else "┬") in
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
         I.string
           A.(fg t.c_border ++ bg t.c_bg)
           (String.concat "" (List.init (iw + 1) (fun _ -> "─")))
       in
       let div_bot = I.string A.(fg t.c_border ++ bg t.c_bg) (if is_last then "╯" else "┴") in
       bot_imgs := !bot_imgs @ [ bot_dashes; div_bot ])
    cards;
  I.vcat
    [ I.hcat !top_imgs; I.hcat !body_row1_imgs; I.hcat !body_row2_imgs; I.hcat !bot_imgs ]
;;

let render_kpi_cards w json =
  let t = Theme.current () in
  let strats =
    match json |?> "strategies" with
    | `Assoc l -> l
    | _ -> []
  in
  let all_balances = json |?> "all_balances" |> to_list_d in
  let total_hold_strats =
    List.fold_left
      (fun hv_acc (_sym, data) ->
         let market = data |?> "market" in
         let bid = market |?> "bid" |> to_float_d 0.0 in
         let ask = market |?> "ask" |> to_float_d 0.0 in
         let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else max bid ask in
         let base_bal = market |?> "base_balance" |> to_float_d 0.0 in
         hv_acc +. (base_bal *. mid))
      0.0
      strats
  in
  let total_hold_bals, total_quote_val =
    List.fold_left
      (fun (hv_acc, q_acc) bal_json ->
         let balance = bal_json |?> "balance" |> to_float_d 0.0 in
         let asset = bal_json |?> "asset" |> to_string_d "?" in
         if balance <= 0.0
         then hv_acc, q_acc
         else (
           let is_quote =
             asset = "USD"
             || asset = "USDC"
             || asset = "USDT"
             || asset = "ZUSD"
             || asset = "USDe"
           in
           if is_quote
           then hv_acc, q_acc +. balance
           else (
             let bid = bal_json |?> "bid" |> to_float_d 0.0 in
             let ask = bal_json |?> "ask" |> to_float_d 0.0 in
             let mid =
               if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else max bid ask
             in
             hv_acc +. (balance *. mid), q_acc)))
      (0.0, 0.0)
      all_balances
  in
  let total_hold_val = total_hold_strats +. total_hold_bals in
  let net_worth = total_hold_val +. total_quote_val in
  let c1_row1 =
    I.hcat [ col 10 t.a_dim "NET WORTH"; col_right 12 t.a_bright (format_usd net_worth) ]
  in
  (* The PORTFOLIO card shows cash on the second line: accumulated value
     already has its own slot in the HOLDINGS & STRATEGY summary bar. *)
  let c1_row2 =
    I.hcat [ col 10 t.a_dim "CASH"; col_right 12 t.a_cyan (format_usd total_quote_val) ]
  in
  let card1 = "PORTFOLIO", c1_row1, c1_row2 in
  let uptime = json |?> "uptime_s" |> to_float_d 0.0 in
  let recent_fills = json |?> "recent_fills" |> to_list_d in
  let lats =
    match json |?> "latencies" with
    | `Assoc l -> l
    | _ -> []
  in
  let snapshot_ts = json |?> "timestamp" |> to_float_d 0.0 in
  (* Classify strategy activity from consistent windows: a strategy is
     active when it ran this window and idle when it is running with a
     fresh cycle window but executed nothing (the S1/S2 states). *)
  let strat_active, strat_idle, exec_per_sec =
    List.fold_left
      (fun (a, i, e) (_sym, metrics) ->
         let mlist =
           match metrics with
           | `Assoc l -> l
           | _ -> []
         in
         match List.assoc_opt "strategy" mlist with
         | Some data ->
           let window_end = data |?> "window_end" |> to_float_d 0.0 in
           let fresh =
             window_end > 0.0 && snapshot_ts > 0.0 && snapshot_ts -. window_end < 15.0
           in
           if not fresh
           then a, i, e
           else (
             let execs = data |?> "executions" |> to_int_d 0 in
             let eps = data |?> "executions_per_sec" |> to_float_d 0.0 in
             if execs > 0 then a + 1, i, e +. eps else a, i + 1, e)
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
  let oracle_lat =
    match json |?> "oracle_latency" with
    | `Assoc l ->
      (match List.assoc_opt "pass" l with
       | Some data -> Some data
       | None -> None)
    | _ -> None
  in
  let oracle_p50, oracle_p99, oracle_fresh =
    match oracle_lat with
    | Some data ->
      let window_end = data |?> "window_end" |> to_float_d 0.0 in
      let samples = data |?> "samples" |> to_int_d 0 in
      let fresh =
        window_end > 0.0 && snapshot_ts > 0.0 && snapshot_ts -. window_end < 600.0
      in
      ( data |?> "p50" |> to_float_d 0.0
      , data |?> "p99" |> to_float_d 0.0
      , fresh && samples > 0 )
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
  let mem = json |?> "memory" in
  let heap_mb = mem |?> "heap_mb" |> to_int_d 0 in
  let live_kb = mem |?> "live_kb" |> to_int_d 0 in
  let free_kb = mem |?> "free_kb" |> to_int_d 0 in
  let total_kb = float_of_int (live_kb + free_kb) in
  let live_pct =
    if total_kb > 0.0 then float_of_int live_kb /. total_kb *. 100.0 else 0.0
  in
  let c4_row1 =
    I.hcat
      [ col 10 t.a_dim "HEAP SIZE"; col_right 12 t.a_yellow (Printf.sprintf "%d MB" heap_mb) ]
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
