open Notty
open Theme

(** KPI Cards Component.
    Renders top-row summary cards in a unified Btop/Terminal panel container. *)

let render_card_row w cards =
  let n = List.length cards in
  let inner_total = max 0 (w - 2 - (n * 2)) in
  let base_w = inner_total / n in
  let rem = inner_total mod n in
  let card_inner_w i = base_w + if i < rem then 1 else 0 in
  let top_imgs = ref [ I.string A.(fg c_border ++ bg c_bg) " ╭" ] in
  let bot_imgs = ref [ I.string A.(fg c_border ++ bg c_bg) " ╰" ] in
  let body_row1_imgs = ref [ I.string A.(fg c_border ++ bg c_bg) " │" ] in
  let body_row2_imgs = ref [ I.string A.(fg c_border ++ bg c_bg) " │" ] in
  List.iteri
    (fun i (title, r1, r2) ->
       let iw = card_inner_w i in
       let is_last = i = n - 1 in
       (* Top bar piece *)
       let title_str = "── " ^ title ^ " " in
       let title_img = I.string A.(fg c_title ++ bg c_bg ++ st bold) title_str in
       let title_len = I.width title_img in
       let dash_count = max 0 (iw + 1 - title_len) in
       let dashes =
         I.string
           A.(fg c_border ++ bg c_bg)
           (String.concat "" (List.init dash_count (fun _ -> "─")))
       in
       let div_top = I.string A.(fg c_border ++ bg c_bg) (if is_last then "╮" else "┬") in
       top_imgs := !top_imgs @ [ title_img; dashes; div_top ];
       (* Body rows *)
       let div_mid = I.string A.(fg c_border ++ bg c_bg) "│" in
       let c_r1 =
         I.hcat [ I.string A.(bg c_bg) " "; I.hsnap ~align:`Left iw r1; div_mid ]
       in
       let c_r2 =
         I.hcat [ I.string A.(bg c_bg) " "; I.hsnap ~align:`Left iw r2; div_mid ]
       in
       body_row1_imgs := !body_row1_imgs @ [ c_r1 ];
       body_row2_imgs := !body_row2_imgs @ [ c_r2 ];
       (* Bottom bar piece *)
       let bot_dashes =
         I.string
           A.(fg c_border ++ bg c_bg)
           (String.concat "" (List.init (iw + 1) (fun _ -> "─")))
       in
       let div_bot = I.string A.(fg c_border ++ bg c_bg) (if is_last then "╯" else "┴") in
       bot_imgs := !bot_imgs @ [ bot_dashes; div_bot ])
    cards;
  I.vcat
    [ I.hcat !top_imgs; I.hcat !body_row1_imgs; I.hcat !body_row2_imgs; I.hcat !bot_imgs ]
;;

let render_kpi_cards w json =
  let strats =
    match json |?> "strategies" with
    | `Assoc l -> l
    | _ -> []
  in
  let all_balances = json |?> "all_balances" |> to_list_d in
  let total_hold_strats, total_accum_strats, active_count =
    List.fold_left
      (fun (hv_acc, av_acc, cnt) (_sym, data) ->
         let market = data |?> "market" in
         let bid = market |?> "bid" |> to_float_d 0.0 in
         let ask = market |?> "ask" |> to_float_d 0.0 in
         let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else max bid ask in
         let base_bal = market |?> "base_balance" |> to_float_d 0.0 in
         let strat = data |?> "strategy" in
         let sell_orders = strat |?> "sell_orders" |> to_list_d in
         let pending_sell_qty =
           List.fold_left
             (fun q s -> q +. (s |?> "qty" |> to_float_d 0.0))
             0.0
             sell_orders
         in
         let accum_qty = max 0.0 (base_bal -. pending_sell_qty) in
         hv_acc +. (base_bal *. mid), av_acc +. (accum_qty *. mid), cnt + 1)
      (0.0, 0.0, 0)
      strats
  in
  let total_hold_bals, total_accum_bals, total_quote_val =
    List.fold_left
      (fun (hv_acc, av_acc, q_acc) bal_json ->
         let balance = bal_json |?> "balance" |> to_float_d 0.0 in
         let asset = bal_json |?> "asset" |> to_string_d "?" in
         if balance <= 0.0
         then hv_acc, av_acc, q_acc
         else (
           let is_quote =
             asset = "USD"
             || asset = "USDC"
             || asset = "USDT"
             || asset = "ZUSD"
             || asset = "USDe"
           in
           if is_quote
           then hv_acc, av_acc, q_acc +. balance
           else (
             let bid = bal_json |?> "bid" |> to_float_d 0.0 in
             let ask = bal_json |?> "ask" |> to_float_d 0.0 in
             let mid =
               if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else max bid ask
             in
             let sell_orders = bal_json |?> "sell_orders" |> to_list_d in
             let pending_sell_qty =
               List.fold_left
                 (fun q s -> q +. (s |?> "qty" |> to_float_d 0.0))
                 0.0
                 sell_orders
             in
             let accum_qty = max 0.0 (balance -. pending_sell_qty) in
             hv_acc +. (balance *. mid), av_acc +. (accum_qty *. mid), q_acc)))
      (0.0, 0.0, 0.0)
      all_balances
  in
  let total_hold_val = total_hold_strats +. total_hold_bals in
  let total_accum_val = total_accum_strats +. total_accum_bals in
  let net_worth = total_hold_val +. total_quote_val in
  let c1_row1 =
    I.hcat [ col 10 a_dim "NET WORTH"; col_right 12 a_bright (format_usd net_worth) ]
  in
  let c1_row2 =
    I.hcat [ col 10 a_dim "ACCUM VAL"; col_right 12 a_cyan (format_usd total_accum_val) ]
  in
  let card1 = "PORTFOLIO", c1_row1, c1_row2 in
  let uptime = json |?> "uptime_s" |> to_float_d 0.0 in
  let recent_fills = json |?> "recent_fills" |> to_list_d in
  let c2_row1 =
    I.hcat
      [ col 10 a_dim "STRATEGIES"
      ; col_right 12 a_green (Printf.sprintf "%d active" active_count)
      ]
  in
  let c2_row2 =
    I.hcat
      [ col 10 a_dim "UPTIME   "
      ; col_right
          12
          a_text
          (format_duration uptime
           ^ " │ "
           ^ string_of_int (List.length recent_fills)
           ^ " fills")
      ]
  in
  let card2 = "SYSTEM ENGINE", c2_row1, c2_row2 in
  let lats =
    match json |?> "latencies" with
    | `Assoc l -> l
    | _ -> []
  in
  let cycle_p50, cycle_p99 =
    List.fold_left
      (fun (p50_acc, p99_acc) (_sym, metrics) ->
         let mlist =
           match metrics with
           | `Assoc l -> l
           | _ -> []
         in
         match List.assoc_opt "cycle" mlist with
         | Some data ->
           let p50 = data |?> "p50" |> to_float_d 0.0 in
           let p99 = data |?> "p99" |> to_float_d 0.0 in
           max p50_acc p50, max p99_acc p99
         | None -> p50_acc, p99_acc)
      (0.0, 0.0)
      lats
  in
  let lat_attr p = if p > 100.0 then a_red else if p > 50.0 then a_yellow else a_green in
  let c3_row1 =
    I.hcat
      [ col 10 a_dim "CYCLE P50 "
      ; col_right 12 (lat_attr cycle_p50) (format_latency_us cycle_p50)
      ]
  in
  let c3_row2 =
    I.hcat
      [ col 10 a_dim "CYCLE P99 "
      ; col_right 12 (lat_attr cycle_p99) (format_latency_us cycle_p99)
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
      [ col 10 a_dim "HEAP SIZE"; col_right 12 a_yellow (Printf.sprintf "%d MB" heap_mb) ]
  in
  let c4_row2 =
    I.hcat
      [ col 10 a_dim "LIVE RATIO"
      ; col_right 12 a_green (Printf.sprintf "%.1f%%" live_pct)
      ]
  in
  let card4 = "MEMORY / GC", c4_row1, c4_row2 in
  if w < 100
  then I.vcat [ render_card_row w [ card1; card2 ]; render_card_row w [ card3; card4 ] ]
  else render_card_row w [ card1; card2; card3; card4 ]
;;
