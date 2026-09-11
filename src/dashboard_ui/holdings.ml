open Notty
open Theme

(** Whether the capital oracle's decision for this strategy entry says
    INACTIVE - the oracle-paused state (the grid should not place new
    orders). Kept as aliases so existing callers/tests keep working. *)
let oracle_inactive = Snapshot.oracle_inactive

let strategy_paused = Snapshot.strategy_paused

(** Accumulated quantity: the inventory held that is not committed to a
    resting sell. [staked] is part of [base] but can never be covered by a
    pending sell (it is not tradeable), so it is never reduced by
    [pending_sell_qty]. *)
let accum_qty_of ~staked ~pending base =
  staked +. Float.max 0.0 (base -. staked -. pending)
;;

let render_strategies ?(selected_index = None) w (snapshot : Snapshot.t) =
  let t = Theme.current () in
  let a_border = t.a_border in
  let a_label = t.a_label in
  let c_bg = t.c_bg in
  let c_panel = t.c_panel in
  let c_border = t.c_border in
  let c_green = t.c_green in
  let c_red = t.c_red in
  let c_cyan = t.c_cyan in
  let c_label = t.c_label in
  let c_bright = t.c_bright in
  let c_near_fill = t.c_near_fill in
  let c_near_sell = t.c_near_sell in
  let strats = snapshot.strategies in
  let all_balances = snapshot.balances in
  let is_compact = w < 120 in
  (* Build the column header row, which differs between compact and wide
     layouts. *)
  let header =
    close_row
      w
      (if is_compact
       then
         I.hcat
           [ I.string a_border " │  "
           ; col 14 a_label "SYMBOL"
           ; col 5 a_label "STGY"
           ; col 3 a_label "ST"
           ; col_right 12 a_label "PRICE"
           ; I.string a_border " │ "
           ; col_right 11 a_label "BUY @"
           ; col 17 a_label ""
           ; col_right 11 a_label "SELL @"
           ; I.string a_border " │ "
           ; col_right 10 a_label "HOLD QTY"
           ; col_right 10 a_label "HOLD VAL"
           ; col_right 10 a_label "ACCUM QTY"
           ; col_right 10 a_label "ACCUM VAL"
           ]
       else
         I.hcat
           [ I.string a_border " │  "
           ; col 16 a_label "SYMBOL"
           ; col 5 a_label "STGY"
           ; col 3 a_label "ST"
           ; col_right 12 a_label "PRICE"
           ; col_right 8 a_label "SPREAD"
           ; I.string a_border " │ "
           ; col_right 12 a_label "BUY @"
           ; col_right 8 a_label "Δ BUY"
           ; col 17 a_label ""
           ; col_right 8 a_label "Δ SELL"
           ; col_right 12 a_label "SELL @"
           ; col_right 6 a_label "SELLS"
           ; col_right 12 a_label "SELL VAL"
           ; I.string a_border " │ "
           ; col_right 12 a_label "HOLD QTY"
           ; col_right 10 a_label "HOLD VAL"
           ; col_right 12 a_label "ACCUM QTY"
           ; col_right 10 a_label "ACCUM VAL"
           ])
  in
  let build_strategy_row ?(is_selected = false) is_even (symbol, (s : Snapshot.strategy)) =
    let exchange = s.exchange in
    let stype = s.type_ in
    let cap_low = s.capital_low in
    let bid = s.market.bid in
    let ask = s.market.ask in
    let mid = s.market.mid in
    let base_bal = s.market.base_balance in
    let staked_bal = s.market.staked_balance in
    let hold_value = base_bal *. mid in
    (* The resting buy price prefers the strategy's tracked price because it
       follows amendments, then falls back to the exchange's real open buy
       orders. This keeps a committed buy visible even when the domain is
       halted by an oracle INACTIVE decision and its state goes stale. *)
    let buy_price =
      if s.buy_price > 0.0
      then s.buy_price
      else (
        match s.market.buy_orders with
        | [] -> 0.0
        | o :: rest ->
          List.fold_left (fun acc (o : Snapshot.order) -> min acc o.price) o.price rest)
    in
    let sell_orders =
      if s.sell_orders <> [] then s.sell_orders else s.market.sell_orders
    in
    let sell_count = s.sell_count in
    let unrealized_profit, pending_sell_qty =
      List.fold_left
        (fun (up, qty_acc) (o : Snapshot.order) ->
           if o.price > 0.0 && o.qty > 0.0
           then up +. (o.price *. o.qty), qty_acc +. o.qty
           else up, qty_acc)
        (0.0, 0.0)
        sell_orders
    in
    let accum_holding =
      accum_qty_of ~staked:staked_bal ~pending:pending_sell_qty base_bal
    in
    let accum_hold_value = accum_holding *. mid in
    let buy_dist_pct =
      if (not cap_low) && buy_price > 0.0 && mid > 0.0
      then Some ((buy_price -. mid) /. mid *. 100.0)
      else None
    in
    let closest_sell_dist_pct =
      let sell_prices =
        List.filter_map
          (fun (o : Snapshot.order) ->
             if o.price > 0.0 && mid > 0.0
             then Some ((o.price -. mid) /. mid *. 100.0)
             else None)
          sell_orders
      in
      match sell_prices with
      | [] -> None
      | prices ->
        Some
          (List.fold_left
             (fun acc p -> if abs_float p < abs_float acc then p else acc)
             (List.hd prices)
             prices)
    in
    let grid_interval_lo = s.grid_interval_lo in
    let grid_interval = if grid_interval_lo > 0.0 then grid_interval_lo else 1.0 in
    let close_thresh = 0.5 *. grid_interval in
    let far_thresh = 2.0 *. grid_interval in
    let closest_sell_price_opt =
      let sell_prices =
        List.filter_map
          (fun (o : Snapshot.order) -> if o.price > 0.0 then Some o.price else None)
          sell_orders
      in
      match sell_prices with
      | [] -> None
      | prices -> Some (List.fold_left min (List.hd prices) prices)
    in
    let execution_proximity_opt =
      match closest_sell_price_opt with
      | None -> None
      | Some sell_price ->
        if (not cap_low) && buy_price > 0.0 && mid > 0.0 && sell_price > buy_price
        then (
          let range = sell_price -. buy_price in
          Some ((mid -. buy_price) /. range *. 100.0))
        else None
    in
    let near_buy, near_sell =
      let cap_pct_thresh = 0.25 in
      let abs_near_buy =
        match buy_dist_pct with
        | Some d -> abs_float d <= cap_pct_thresh
        | None -> false
      in
      let abs_near_sell =
        match closest_sell_dist_pct with
        | Some d -> abs_float d <= cap_pct_thresh
        | None -> false
      in
      match execution_proximity_opt with
      | Some pos -> pos < 25.0 && abs_near_buy, pos > 75.0 && abs_near_sell
      | None ->
        let is_near_buy =
          match buy_dist_pct with
          | Some d -> abs_float d < close_thresh && abs_near_buy
          | None -> false
        in
        is_near_buy, false
    in
    (* Blink the near-fill tint while price sits close to execution. A solid
       tint (the previous [Anim.flash]-while-active behavior) read as a static
       highlight, losing the "about to fill" cue. *)
    if near_buy || near_sell then Anim.motion_pending := true;
    let blink_on = Anim.blink () in
    let flash_buy = near_buy && blink_on in
    let flash_sell = near_sell && blink_on in
    let bg_color =
      if flash_buy
      then c_near_fill
      else if flash_sell
      then c_near_sell
      else if is_even
      then c_panel
      else c_bg
    in
    let a_text = A.(t.a_text ++ bg bg_color) in
    let a_green = A.(t.a_green ++ bg bg_color) in
    let a_red = A.(t.a_red ++ bg bg_color) in
    let a_yellow = A.(t.a_yellow ++ bg bg_color) in
    let a_cyan = A.(t.a_cyan ++ bg bg_color) in
    let a_dim = A.(t.a_dim ++ bg bg_color) in
    let a_border = A.(t.a_border ++ bg bg_color) in
    let a_border_outer = A.(t.a_border ++ bg c_bg) in
    let a_bps_tight = A.(t.a_bps_tight ++ bg bg_color) in
    let a_bps_norm = A.(t.a_bps_norm ++ bg bg_color) in
    let a_bps_wide = A.(t.a_bps_wide ++ bg bg_color) in
    let a_bps_xtrm = A.(t.a_bps_xtrm ++ bg bg_color) in
    let a_near_fill_green = A.(t.a_near_fill_green ++ bg bg_color) in
    let a_near_sell_red = A.(t.a_near_sell_red ++ bg bg_color) in
    let a_near_fill = A.(t.a_near_fill ++ bg bg_color) in
    let a_near_sell = A.(t.a_near_sell ++ bg bg_color) in
    let exch_sym_attr ?dim exch = A.(Theme.exch_sym_attr ?dim exch ++ bg bg_color) in
    let col w attr s = I.string attr (pad_right w s) in
    let col_right w attr s = I.string attr (pad_left w s) in
    let close_row w img = close_row w img in
    let cursor_img =
      if is_selected
      then I.string A.(fg c_cyan ++ bg bg_color ++ st bold) " ▶"
      else I.string a_border_outer " │"
    in
    let gauge_img = render_proximity_slider 17 execution_proximity_opt in
    let market_is_closed = s.market_is_closed in
    let oracle_paused = oracle_inactive s in
    let has_resting_buy = buy_price > 0.0 in
    let status_str, status_attr =
      if ((not has_resting_buy) && (oracle_paused || cap_low)) || market_is_closed
      then "⏸", a_yellow
      else "▶", a_green
    in
    let exch_tag = exch_tag_of exchange in
    let spread_str = format_spread_bps bid ask in
    let spread_attr =
      if bid <= 0.0 || ask <= 0.0
      then a_dim
      else (
        let bps = (ask -. bid) /. ((bid +. ask) /. 2.0) *. 10000.0 in
        if bps < 5.0
        then a_bps_tight
        else if bps < 20.0
        then a_bps_norm
        else if bps < 50.0
        then a_bps_wide
        else a_bps_xtrm)
    in
    let buy_dist_str, buy_dist_attr =
      match buy_dist_pct with
      | None -> "--", a_dim
      | Some d ->
        let abs_d = abs_float d in
        let attr =
          if abs_d < close_thresh
          then a_green
          else if abs_d < far_thresh
          then a_cyan
          else a_dim
        in
        format_pct d, attr
    in
    let sell_dist_str, sell_dist_attr =
      match closest_sell_dist_pct with
      | None -> "--", a_dim
      | Some d ->
        let abs_d = abs_float d in
        let attr =
          if abs_d < close_thresh
          then a_yellow
          else if abs_d < far_thresh
          then a_cyan
          else a_dim
        in
        format_pct d, attr
    in
    let sell_price_str, sell_price_attr =
      match closest_sell_price_opt with
      | Some sp -> format_price sp, if flash_sell then a_near_sell_red else a_yellow
      | None -> "--", a_dim
    in
    let row_text =
      if flash_buy then a_near_fill else if flash_sell then a_near_sell else a_text
    in
    let sym_attr =
      if flash_buy
      then a_near_fill
      else if flash_sell
      then a_near_sell
      else exch_sym_attr exchange
    in
    let p_fg =
      match buy_dist_pct, closest_sell_dist_pct with
      | Some b, Some s -> if abs_float b < abs_float s then c_green else c_red
      | Some _, None -> c_green
      | None, Some _ -> c_red
      | None, None -> c_border
    in
    let p_border_attr = A.(fg p_fg ++ bg bg_color) in
    let price_str = if mid > 0.0 then format_price mid else "--" in
    let price_cell =
      I.hcat
        [ I.string p_border_attr "["
        ; col_right 10 row_text price_str
        ; I.string p_border_attr "]"
        ]
    in
    if is_compact
    then
      close_row
        w
        (I.hcat
           [ cursor_img
           ; I.string A.(bg bg_color) "  "
           ; col 14 sym_attr (Printf.sprintf "%s(%s)" (truncate_string 8 symbol) exch_tag)
           ; col 5 a_cyan (truncate_string 4 stype)
           ; I.hcat [ I.string status_attr status_str; I.string a_text "  " ]
           ; price_cell
           ; I.string a_border " │ "
           ; col_right
               11
               (if flash_buy
                then a_near_fill_green
                else if flash_sell
                then a_near_sell_red
                else if buy_price > 0.0
                then a_green
                else a_dim)
               (if buy_price > 0.0 then format_price buy_price else "--")
           ; gauge_img
           ; col_right 11 sell_price_attr sell_price_str
           ; I.string a_border " │ "
           ; col_right 10 row_text (if base_bal > 0.0 then format_qty base_bal else "0")
           ; col_right
               10
               row_text
               (if hold_value > 0.01 then format_usd hold_value else "--")
           ; col_right
               10
               row_text
               (if accum_holding > 0.0001 then format_qty accum_holding else "0")
           ; col_right
               10
               row_text
               (if accum_hold_value > 0.01 then format_usd accum_hold_value else "--")
           ])
    else
      close_row
        w
        (I.hcat
           [ cursor_img
           ; I.string A.(bg bg_color) "  "
           ; col
               16
               sym_attr
               (Printf.sprintf "%s(%s)" (truncate_string 10 symbol) exch_tag)
           ; col 5 a_cyan (truncate_string 4 stype)
           ; I.hcat [ I.string status_attr status_str; I.string a_text "  " ]
           ; price_cell
           ; col_right 8 spread_attr spread_str
           ; I.string a_border " │ "
           ; col_right
               12
               (if flash_buy
                then a_near_fill_green
                else if flash_sell
                then a_near_sell_red
                else if buy_price > 0.0
                then a_green
                else a_dim)
               (if buy_price > 0.0 then format_price buy_price else "--")
           ; col_right 8 buy_dist_attr buy_dist_str
           ; gauge_img
           ; col_right 8 sell_dist_attr sell_dist_str
           ; col_right 12 sell_price_attr sell_price_str
           ; col_right
               6
               (if sell_count > 0 then a_yellow else a_dim)
               (add_commas (string_of_int sell_count))
           ; col_right
               12
               (if unrealized_profit >= 0.0 then a_green else a_red)
               (format_pnl unrealized_profit)
           ; I.string a_border " │ "
           ; col_right 12 row_text (if base_bal > 0.0 then format_qty base_bal else "0")
           ; col_right
               10
               row_text
               (if hold_value > 0.01 then format_usd hold_value else "--")
           ; col_right
               12
               row_text
               (if accum_holding > 0.0001 then format_qty accum_holding else "0")
           ; col_right
               10
               row_text
               (if accum_hold_value > 0.01 then format_usd accum_hold_value else "--")
           ])
  in
  let active_rows_data, paused_rows_data =
    List.partition (fun (_symbol, s) -> not (strategy_paused s)) strats
  in
  let build_balance_row ?(is_selected = false) is_even (b : Snapshot.balance) img_is_quote
    =
    let exchange = b.exchange in
    let asset = b.asset in
    let balance = b.balance in
    let exch_tag = exch_tag_of exchange in
    let bid = b.bid in
    let ask = b.ask in
    let mid = b.mid in
    let hold_value = balance *. mid in
    let sell_orders = b.sell_orders in
    let sell_count = b.sell_count in
    let unrealized_profit, pending_sell_qty =
      List.fold_left
        (fun (up, qty_acc) (o : Snapshot.order) ->
           if o.price > 0.0 && o.qty > 0.0
           then up +. (o.price *. o.qty), qty_acc +. o.qty
           else up, qty_acc)
        (0.0, 0.0)
        sell_orders
    in
    let is_quote = img_is_quote in
    let staked_bal = b.staked_balance in
    let accum_holding =
      if is_quote
      then 0.0
      else accum_qty_of ~staked:staked_bal ~pending:pending_sell_qty balance
    in
    let accum_hold_value = accum_holding *. mid in
    let closest_sell_dist_pct =
      let sell_prices =
        List.filter_map
          (fun (o : Snapshot.order) ->
             if o.price > 0.0 && mid > 0.0
             then Some ((o.price -. mid) /. mid *. 100.0)
             else None)
          sell_orders
      in
      match sell_prices with
      | [] -> None
      | prices ->
        Some
          (List.fold_left
             (fun acc p -> if abs_float p < abs_float acc then p else acc)
             (List.hd prices)
             prices)
    in
    let closest_sell_price_opt =
      let sell_prices =
        List.filter_map
          (fun (o : Snapshot.order) -> if o.price > 0.0 then Some o.price else None)
          sell_orders
      in
      match sell_prices with
      | [] -> None
      | prices -> Some (List.fold_left min (List.hd prices) prices)
    in
    let near_sell =
      match closest_sell_dist_pct with
      | Some d -> abs_float d <= 0.25
      | None -> false
    in
    if near_sell then Anim.motion_pending := true;
    let flash_sell = near_sell && Anim.blink () in
    let bg_color =
      if flash_sell then c_near_sell else if is_even then c_panel else c_bg
    in
    let a_text = A.(t.a_text ++ bg bg_color) in
    let a_green = A.(t.a_green ++ bg bg_color) in
    let a_red = A.(t.a_red ++ bg bg_color) in
    let a_yellow = A.(t.a_yellow ++ bg bg_color) in
    let a_cyan = A.(t.a_cyan ++ bg bg_color) in
    let a_dim = A.(t.a_dim ++ bg bg_color) in
    let a_border = A.(t.a_border ++ bg bg_color) in
    let a_border_outer = A.(t.a_border ++ bg c_bg) in
    let a_bps_tight = A.(t.a_bps_tight ++ bg bg_color) in
    let a_bps_norm = A.(t.a_bps_norm ++ bg bg_color) in
    let a_bps_wide = A.(t.a_bps_wide ++ bg bg_color) in
    let a_bps_xtrm = A.(t.a_bps_xtrm ++ bg bg_color) in
    let a_near_sell = A.(t.a_near_sell ++ bg bg_color) in
    let a_near_sell_red = A.(t.a_near_sell_red ++ bg bg_color) in
    let exch_sym_attr ?dim exch = A.(Theme.exch_sym_attr ?dim exch ++ bg bg_color) in
    let row_text = if flash_sell then a_near_sell else a_text in
    let sym_attr = if flash_sell then a_near_sell else exch_sym_attr ~dim:true exchange in
    let col w attr s = I.string attr (pad_right w s) in
    let col_right w attr s = I.string attr (pad_left w s) in
    let close_row w img = close_row w img in
    let cursor_img =
      if is_selected
      then I.string A.(fg c_cyan ++ bg bg_color ++ st bold) " ▶"
      else I.string a_border_outer " │"
    in
    let sell_dist_str, sell_dist_attr =
      match closest_sell_dist_pct with
      | None -> "--", a_dim
      | Some d ->
        let abs_d = abs_float d in
        let attr =
          if abs_d < 0.5 then a_yellow else if abs_d < 2.0 then a_cyan else a_dim
        in
        format_pct d, attr
    in
    let sell_price_str, sell_price_attr =
      match closest_sell_price_opt with
      | Some sp -> format_price sp, if flash_sell then a_near_sell_red else a_yellow
      | None -> "--", a_dim
    in
    let render_gauge b_pct s_pct =
      let half = 7 in
      let pos d =
        if d < 0.0
        then half
        else if d < 0.25
        then 0
        else if d < 0.75
        then 1
        else if d < 1.5
        then 2
        else if d < 3.0
        then 3
        else if d < 6.0
        then 4
        else if d < 12.0
        then 5
        else 6
      in
      let b_pos =
        match b_pct with
        | Some d -> pos (abs_float d)
        | None -> half + 1
      in
      let s_pos =
        match s_pct with
        | Some d -> pos (abs_float d)
        | None -> half + 1
      in
      let left_side =
        List.init half (fun i -> if half - 1 - i = b_pos then "B" else "─")
        |> String.concat ""
      in
      let right_side =
        List.init half (fun i -> if i = s_pos then "S" else "─") |> String.concat ""
      in
      I.hcat
        [ I.string a_border "["
        ; I.string (if b_pos <= 1 then a_green else a_dim) left_side
        ; I.string a_border "┼"
        ; I.string (if s_pos <= 1 then a_yellow else a_dim) right_side
        ; I.string a_border "]"
        ]
    in
    let gauge_img = render_gauge None closest_sell_dist_pct in
    let status_str, status_attr = if is_quote then "$", a_green else "⏹", a_red in
    let spread_str = format_spread_bps bid ask in
    let spread_attr =
      if bid <= 0.0 || ask <= 0.0
      then a_dim
      else (
        let bps = (ask -. bid) /. ((bid +. ask) /. 2.0) *. 10000.0 in
        if bps < 5.0
        then a_bps_tight
        else if bps < 20.0
        then a_bps_norm
        else if bps < 50.0
        then a_bps_wide
        else a_bps_xtrm)
    in
    let p_fg =
      match closest_sell_dist_pct with
      | Some _ -> c_red
      | None -> c_border
    in
    let p_border_attr = A.(fg p_fg ++ bg bg_color) in
    let price_str = if mid > 0.0 then format_price mid else "--" in
    let price_cell =
      I.hcat
        [ I.string p_border_attr "["
        ; col_right 10 row_text price_str
        ; I.string p_border_attr "]"
        ]
    in
    if is_compact
    then
      close_row
        w
        (I.hcat
           [ cursor_img
           ; I.string A.(bg bg_color) "  "
           ; col 14 sym_attr (Printf.sprintf "%s(%s)" (truncate_string 8 asset) exch_tag)
           ; col 5 a_dim "--"
           ; I.hcat [ I.string status_attr status_str; I.string row_text "  " ]
           ; price_cell
           ; I.string a_border " │ "
           ; col_right 11 a_dim "--"
           ; gauge_img
           ; col_right 11 sell_price_attr sell_price_str
           ; I.string a_border " │ "
           ; col_right 10 row_text (format_qty balance)
           ; col_right
               10
               row_text
               (if hold_value > 0.01 then format_usd hold_value else "--")
           ; col_right
               10
               row_text
               (if accum_holding > 0.0001 then format_qty accum_holding else "0")
           ; col_right
               10
               row_text
               (if accum_hold_value > 0.01 then format_usd accum_hold_value else "--")
           ])
    else
      close_row
        w
        (I.hcat
           [ cursor_img
           ; I.string A.(bg bg_color) "  "
           ; col 16 sym_attr (Printf.sprintf "%s(%s)" (truncate_string 10 asset) exch_tag)
           ; col 5 a_dim "--"
           ; I.hcat [ I.string status_attr status_str; I.string row_text "  " ]
           ; price_cell
           ; col_right 8 spread_attr spread_str
           ; I.string a_border " │ "
           ; col_right 12 a_dim "--"
           ; col_right 8 a_dim "--"
           ; gauge_img
           ; col_right 8 sell_dist_attr sell_dist_str
           ; col_right 12 sell_price_attr sell_price_str
           ; col_right
               6
               (if sell_count > 0 then a_yellow else a_dim)
               (add_commas (string_of_int sell_count))
           ; col_right
               12
               (if unrealized_profit >= 0.0 && sell_count > 0
                then a_green
                else if unrealized_profit > 0.0
                then a_dim
                else a_dim)
               (if sell_count > 0 then format_pnl unrealized_profit else "--")
           ; I.string a_border " │ "
           ; col_right 12 row_text (format_qty balance)
           ; col_right
               10
               row_text
               (if hold_value > 0.01 then format_usd hold_value else "--")
           ; col_right
               12
               row_text
               (if accum_holding > 0.0001 then format_qty accum_holding else "0")
           ; col_right
               10
               row_text
               (if accum_hold_value > 0.01 then format_usd accum_hold_value else "--")
           ])
  in
  let strat_keys =
    List.map
      (fun (sym, (s : Snapshot.strategy)) ->
         let base = if s.market.base_asset = "" then sym else s.market.base_asset in
         (s.exchange, sym), (s.exchange, base))
      strats
  in
  let valid_balances =
    List.filter
      (fun (b : Snapshot.balance) ->
         let is_strat_asset =
           List.exists
             (fun ((ex1, s1), (ex2, b2)) ->
                (ex1 = b.exchange && (s1 = b.symbol || s1 = b.asset))
                || (ex2 = b.exchange && b2 = b.asset))
             strat_keys
         in
         b.balance > 0.0 && not is_strat_asset)
      all_balances
  in
  let inactive_rows_data =
    List.filter
      (fun (b : Snapshot.balance) -> not (Snapshot.is_quote_asset b.asset))
      valid_balances
  in
  let quote_rows_data =
    List.filter
      (fun (b : Snapshot.balance) -> b.balance > 0.0 && Snapshot.is_quote_asset b.asset)
      all_balances
  in
  let curr_row_idx = ref 0 in
  let active_images =
    List.map
      (fun row_data ->
         let idx = !curr_row_idx in
         incr curr_row_idx;
         build_strategy_row
           ~is_selected:(selected_index = Some idx)
           (idx mod 2 = 1)
           row_data)
      active_rows_data
  in
  let paused_images =
    List.map
      (fun row_data ->
         let idx = !curr_row_idx in
         incr curr_row_idx;
         build_strategy_row
           ~is_selected:(selected_index = Some idx)
           (idx mod 2 = 1)
           row_data)
      paused_rows_data
  in
  let inactive_rows =
    List.map
      (fun (b : Snapshot.balance) ->
         let idx = !curr_row_idx in
         incr curr_row_idx;
         build_balance_row
           ~is_selected:(selected_index = Some idx)
           (idx mod 2 = 1)
           b
           false)
      inactive_rows_data
  in
  let quote_rows =
    List.mapi
      (fun idx (b : Snapshot.balance) ->
         build_balance_row ~is_selected:false (idx mod 2 = 1) b true)
      quote_rows_data
  in
  let total_up_strats, total_hold_strats, total_accum_val_strats =
    List.fold_left
      (fun (up_acc, hold_acc, accum_val_acc) (_symbol, (s : Snapshot.strategy)) ->
         let mid = s.market.mid in
         let base_bal = s.market.base_balance in
         let staked_bal = s.market.staked_balance in
         let sell_orders =
           if s.sell_orders <> [] then s.sell_orders else s.market.sell_orders
         in
         let strat_up, pending_sell_qty =
           List.fold_left
             (fun (a, q_acc) (o : Snapshot.order) ->
                if o.price > 0.0 && o.qty > 0.0
                then a +. (o.price *. o.qty), q_acc +. o.qty
                else a, q_acc)
             (0.0, 0.0)
             sell_orders
         in
         let accum_holding =
           accum_qty_of ~staked:staked_bal ~pending:pending_sell_qty base_bal
         in
         let accum_hold_value = accum_holding *. mid in
         ( up_acc +. strat_up
         , hold_acc +. (base_bal *. mid)
         , accum_val_acc +. accum_hold_value ))
      (0.0, 0.0, 0.0)
      strats
  in
  let total_up_bals, total_hold_bals, total_accum_val_bals =
    List.fold_left
      (fun (up_acc, hold_acc, accum_val_acc) (b : Snapshot.balance) ->
         if b.balance <= 0.0
         then up_acc, hold_acc, accum_val_acc
         else (
           let mid = b.mid in
           let is_quote = Snapshot.is_quote_asset b.asset in
           let bal_up, pending_sell_qty =
             List.fold_left
               (fun (a, q_acc) (o : Snapshot.order) ->
                  if o.price > 0.0 && o.qty > 0.0
                  then a +. (o.price *. o.qty), q_acc +. o.qty
                  else a, q_acc)
               (0.0, 0.0)
               b.sell_orders
           in
           let accum_holding =
             if is_quote
             then 0.0
             else
               accum_qty_of ~staked:b.staked_balance ~pending:pending_sell_qty b.balance
           in
           let accum_hold_value = accum_holding *. mid in
           ( up_acc +. bal_up
           , hold_acc +. (b.balance *. mid)
           , accum_val_acc +. accum_hold_value )))
      (0.0, 0.0, 0.0)
      all_balances
  in
  let total_up = total_up_strats +. total_up_bals in
  let total_hold_val = total_hold_strats +. total_hold_bals in
  let total_accum_val = total_accum_val_strats +. total_accum_val_bals in
  let total_quote_val =
    List.fold_left
      (fun acc (b : Snapshot.balance) ->
         if Snapshot.is_quote_asset b.asset && b.balance > 0.0
         then acc +. b.balance
         else acc)
      0.0
      all_balances
  in
  let title = section_title w "HOLDINGS & STRATEGY" in
  let thin_sep label =
    let lbl = " ├── " ^ label ^ " " in
    let lbl_img = I.string A.(fg c_border ++ bg c_bg) lbl in
    let pad_count = max 0 (w - I.width lbl_img - 1) in
    let pad_buf = Buffer.create (pad_count * 3) in
    for _ = 1 to pad_count do
      Buffer.add_string pad_buf "─"
    done;
    I.hcat
      [ lbl_img
      ; I.string A.(fg c_border ++ bg c_bg) (Buffer.contents pad_buf)
      ; I.string A.(fg c_border ++ bg c_bg) "┤"
      ]
  in
  let has_inactive = inactive_rows <> [] in
  let has_quote = quote_rows <> [] in
  let rows =
    [ title; header ]
    @ active_images
    @ paused_images
    @ (if has_inactive then [ thin_sep "balances" ] @ inactive_rows else [])
    @ if has_quote then [ thin_sep "cash" ] @ quote_rows else []
  in
  let main_table = I.vcat rows in
  let up_attr =
    if total_up >= 0.0
    then A.(fg c_green ++ bg c_bg ++ st bold)
    else A.(fg c_red ++ bg c_bg ++ st bold)
  in
  let pipe = I.string A.(fg c_border ++ bg c_bg) "  │  " in
  let kv lbl value_s vattr =
    I.hcat
      [ I.string A.(fg c_label ++ bg c_bg) ("  " ^ lbl ^ ": "); I.string vattr value_s ]
  in
  let summary_bar =
    let cash_t = Anim.tween ~key:"holdings.cash" ~target:total_quote_val ~tau:0.35 in
    let accum_t = Anim.tween ~key:"holdings.accum" ~target:total_accum_val ~tau:0.35 in
    let hold_t = Anim.tween ~key:"holdings.hold" ~target:total_hold_val ~tau:0.35 in
    let sell_t = Anim.tween ~key:"holdings.sell" ~target:total_up ~tau:0.35 in
    close_row
      w
      (I.hcat
         [ I.string A.(fg c_border ++ bg c_bg) " │"
         ; kv "Cash" (format_usd cash_t) A.(fg c_cyan ++ bg c_bg ++ st bold)
         ; pipe
         ; kv "Accum Val" (format_usd accum_t) A.(fg c_bright ++ bg c_bg ++ st bold)
         ; pipe
         ; kv "Hold Val" (format_usd hold_t) A.(fg c_bright ++ bg c_bg ++ st bold)
         ; pipe
         ; kv "Sell Val" (format_pnl sell_t) up_attr
         ])
  in
  let summary_section =
    I.vcat (List.filter (fun img -> I.height img > 0) [ thin_sep "summary"; summary_bar ])
  in
  I.vcat [ main_table; summary_section; section_footer w ]
;;
