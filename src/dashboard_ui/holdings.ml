open Notty
open Theme

let render_strategies w json =
  let now = Unix.gettimeofday () in
  let flash_on = fst (modf (now *. 1.5)) < 0.5 in
  let strats = match json |?> "strategies" with `Assoc l -> l | _ -> [] in
  let all_balances = json |?> "all_balances" |> to_list_d in

  (* Column header row *)
  let header = close_row w (I.hcat [
    I.string a_border " │  ";
    col 16 a_label "SYMBOL";
    col 5 a_label "STGY";
    col 3 a_label "ST";
    col_right 12 a_label "PRICE";
    col_right 8 a_label "SPREAD";
    I.string a_border " │ ";
    col_right 12 a_label "BUY @";
    col_right 8 a_label "Δ BUY";
    col 17 a_label "";
    col_right 8 a_label "Δ SELL";
    col_right 12 a_label "SELL @";
    col_right 6 a_label "SELLS";
    col_right 12 a_label "SELL VAL";
    I.string a_border " │ ";
    col_right 12 a_label "HOLDING";
    col_right 10 a_label "HOLD VAL";
    col_right 12 a_label "ACCUM QTY";
    col_right 10 a_label "ACCUM VAL";
  ]) in

  let build_strategy_row is_even (symbol, data) =
    let exchange = data |?> "exchange" |> to_string_d "?" in
    let strat = data |?> "strategy" in
    let market = data |?> "market" in
    let stype = strat |?> "type" |> to_string_d "?" in
    let cap_low = strat |?> "capital_low" |> to_bool_d false in

    let bid = market |?> "bid" |> to_float_d 0.0 in
    let ask = market |?> "ask" |> to_float_d 0.0 in
    let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else (max bid ask) in
    let base_bal = market |?> "base_balance" |> to_float_d 0.0 in
    let hold_value = base_bal *. mid in

    let buy_price = strat |?> "buy_price" |> to_float_d 0.0 in
    let sell_count = strat |?> "sell_count" |> to_int_d 0 in

    let sell_orders = strat |?> "sell_orders" |> to_list_d in
    let unrealized_profit, pending_sell_qty = List.fold_left (fun (up, qty_acc) s ->
      let sp = s |?> "price" |> to_float_d 0.0 in
      let sq = s |?> "qty" |> to_float_d 0.0 in
      if sp > 0.0 && sq > 0.0 then (up +. (sp *. sq), qty_acc +. sq) else (up, qty_acc)
    ) (0.0, 0.0) sell_orders in
    
    let accum_holding = max 0.0 (base_bal -. pending_sell_qty) in
    let accum_hold_value = accum_holding *. mid in

    let buy_dist_pct =
      if (not cap_low) && buy_price > 0.0 && mid > 0.0 then
        Some (((buy_price -. mid) /. mid) *. 100.0)
      else None
    in

    let closest_sell_dist_pct =
      let sell_prices = List.filter_map (fun s ->
        let sp = s |?> "price" |> to_float_d 0.0 in
        if sp > 0.0 && mid > 0.0 then Some (((sp -. mid) /. mid) *. 100.0) else None
      ) sell_orders in
      match sell_prices with
      | [] -> None
      | prices -> Some (List.fold_left (fun acc p -> if abs_float p < abs_float acc then p else acc) (List.hd prices) prices)
    in

    let grid_interval_lo = data |?> "grid_interval_lo" |> to_float_d 1.0 in
    let grid_interval = if grid_interval_lo > 0.0 then grid_interval_lo else 1.0 in
    let close_thresh = 0.5 *. grid_interval in
    let far_thresh = 2.0 *. grid_interval in

    let closest_sell_price_opt =
      let sell_prices = List.filter_map (fun s -> let sp = s |?> "price" |> to_float_d 0.0 in if sp > 0.0 then Some sp else None) sell_orders in
      match sell_prices with [] -> None | prices -> Some (List.fold_left min (List.hd prices) prices)
    in

    let near_buy, near_sell =
      let cap_pct_thresh = 0.25 in
      let abs_near_buy = match buy_dist_pct with Some d -> abs_float d <= cap_pct_thresh | None -> false in
      let abs_near_sell = match closest_sell_dist_pct with Some d -> abs_float d <= cap_pct_thresh | None -> false in

      let execution_proximity =
        match closest_sell_price_opt with
        | None -> None
        | Some sell_price ->
            if (not cap_low) && buy_price > 0.0 && mid > 0.0 && sell_price > buy_price then
              let range = sell_price -. buy_price in
              Some (((mid -. buy_price) /. range) *. 100.0)
            else None
      in
      match execution_proximity with
      | Some pos -> (pos < 25.0 && abs_near_buy, pos > 75.0 && abs_near_sell)
      | None ->
          let is_near_buy = match buy_dist_pct with Some d -> abs_float d < close_thresh && abs_near_buy | None -> false in
          (is_near_buy, false)
    in

    let flash_buy = near_buy && flash_on in
    let flash_sell = near_sell && flash_on in

    let bg_color =
      if flash_buy then c_near_fill
      else if flash_sell then c_near_sell
      else if is_even then c_panel
      else c_bg
    in
    
    let a_text       = A.(Theme.a_text   ++ bg bg_color) in
    let a_green      = A.(Theme.a_green  ++ bg bg_color) in
    let a_red        = A.(Theme.a_red    ++ bg bg_color) in
    let a_yellow     = A.(Theme.a_yellow ++ bg bg_color) in
    let a_cyan       = A.(Theme.a_cyan   ++ bg bg_color) in
    let a_dim        = A.(Theme.a_dim    ++ bg bg_color) in
    let a_border     = A.(Theme.a_border ++ bg bg_color) in
    let a_border_outer = A.(Theme.a_border ++ bg c_bg) in
    let a_bps_tight  = A.(Theme.a_bps_tight ++ bg bg_color) in
    let a_bps_norm   = A.(Theme.a_bps_norm  ++ bg bg_color) in
    let a_bps_wide   = A.(Theme.a_bps_wide  ++ bg bg_color) in
    let a_bps_xtrm   = A.(Theme.a_bps_xtrm  ++ bg bg_color) in
    let a_near_fill_green = A.(Theme.a_near_fill_green ++ bg bg_color) in
    let a_near_sell_red   = A.(Theme.a_near_sell_red ++ bg bg_color) in
    let a_near_fill  = A.(Theme.a_near_fill ++ bg bg_color) in
    let a_near_sell  = A.(Theme.a_near_sell ++ bg bg_color) in
    let exch_sym_attr ?dim exch = A.(Theme.exch_sym_attr ?dim exch ++ bg bg_color) in
    
    let col w attr s = I.string attr (pad_right w s) in
    let col_right w attr s = I.string attr (pad_left w s) in
    let close_row w img =
      let d = w - I.width img - 2 in
      I.hcat [ img; I.string A.(bg bg_color) (String.make (max 0 d) ' '); I.string A.(bg bg_color) " "; I.string a_border_outer "│" ]
    in

    let render_gauge b_pct s_pct =
      let half = 7 in
      let pos d =
        if d < 0.0 then half
        else if d < 0.25 *. grid_interval then 0
        else if d < 0.75 *. grid_interval then 1
        else if d < 1.5 *. grid_interval  then 2
        else if d < 3.0 *. grid_interval  then 3
        else if d < 6.0 *. grid_interval  then 4
        else if d < 12.0 *. grid_interval then 5
        else 6
      in
      let b_pos = match b_pct with Some d -> pos (abs_float d) | None -> half + 1 in
      let s_pos = match s_pct with Some d -> pos (abs_float d) | None -> half + 1 in
      let left_side = List.init half (fun i -> if half - 1 - i = b_pos then "B" else "─") |> String.concat "" in
      let right_side = List.init half (fun i -> if i = s_pos then "S" else "─") |> String.concat "" in
      I.hcat [
        I.string a_border "[";
        I.string (if b_pos <= 1 then a_green else a_dim) left_side;
        I.string a_border "┼";
        I.string (if s_pos <= 1 then a_yellow else a_dim) right_side;
        I.string a_border "]";
      ]
    in
    let gauge_img = render_gauge buy_dist_pct closest_sell_dist_pct in

    let status_str, status_attr =
      if cap_low then "⏸", a_yellow else "▶", a_green
    in

    let exch_tag = exch_tag_of exchange in
    let spread_str = format_spread_bps bid ask in
    let spread_attr =
      if bid <= 0.0 || ask <= 0.0 then a_dim
      else
        let bps = ((ask -. bid) /. ((bid +. ask) /. 2.0)) *. 10000.0 in
        if bps < 5.0 then a_bps_tight
        else if bps < 20.0 then a_bps_norm
        else if bps < 50.0 then a_bps_wide
        else a_bps_xtrm
    in

    let buy_dist_str, buy_dist_attr = match buy_dist_pct with
      | None -> "--", a_dim
      | Some d ->
          let abs_d = abs_float d in
          let attr = if abs_d < close_thresh then a_green else if abs_d < far_thresh then a_cyan else a_dim in
          format_pct d, attr
    in
    let sell_dist_str, sell_dist_attr = match closest_sell_dist_pct with
      | None -> "--", a_dim
      | Some d ->
          let abs_d = abs_float d in
          let attr = if abs_d < close_thresh then a_yellow else if abs_d < far_thresh then a_cyan else a_dim in
          format_pct d, attr
    in

    let sell_price_str, sell_price_attr =
      match closest_sell_price_opt with
      | Some sp -> format_price sp, (if flash_sell then a_near_sell_red else a_yellow)
      | None -> "--", a_dim
    in

    let row_text =
      if flash_buy then a_near_fill
      else if flash_sell then a_near_sell
      else a_text
    in
    let sym_attr =
      if flash_buy then a_near_fill
      else if flash_sell then a_near_sell
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
    let price_cell = I.hcat [
      I.string p_border_attr "[";
      col_right 10 row_text price_str;
      I.string p_border_attr "]";
    ] in

    close_row w (I.hcat [
      I.string a_border_outer " │";
      I.string A.(bg bg_color) "  ";
      col 16 sym_attr (Printf.sprintf "%s(%s)" (truncate_string 10 symbol) exch_tag);
      col 5 a_cyan (truncate_string 4 stype);
      I.hcat [ I.string status_attr status_str; I.string a_text "  " ];
      price_cell;
      col_right 8 spread_attr spread_str;
      I.string a_border " │ ";
      col_right 12 (if flash_buy then a_near_fill_green
              else if flash_sell then a_near_sell_red
              else if buy_price > 0.0 then a_green else a_dim)
        (if buy_price > 0.0 then format_price buy_price else "--");
      col_right 8 buy_dist_attr buy_dist_str;
      gauge_img;
      col_right 8 sell_dist_attr sell_dist_str;
      col_right 12 sell_price_attr sell_price_str;
      col_right 6 (if sell_count > 0 then a_yellow else a_dim)
        (string_of_int sell_count);
      col_right 12 (if unrealized_profit >= 0.0 then a_green else a_red)
        (format_pnl unrealized_profit);
      I.string a_border " │ ";
      col_right 12 row_text (if base_bal > 0.0 then format_qty base_bal else "0");
      col_right 10 row_text (if hold_value > 0.01 then format_price hold_value else "--");
      col_right 12 row_text (if accum_holding > 0.0001 then format_qty accum_holding else "0");
      col_right 10 row_text (if accum_hold_value > 0.01 then format_price accum_hold_value else "--");
    ])
  in

  let active_rows_data, paused_rows_data = List.partition (fun (_symbol, data) ->
    let strat = data |?> "strategy" in
    not (strat |?> "capital_low" |> to_bool_d false)
  ) strats in

  let active_images = List.mapi (fun i row_data -> build_strategy_row (i mod 2 = 1) row_data) active_rows_data in
  let paused_images = List.mapi (fun i row_data -> build_strategy_row (i mod 2 = 1) row_data) paused_rows_data in

  let build_balance_row is_even bal_json img_is_quote =
    let exchange = bal_json |?> "exchange" |> to_string_d "?" in
    let asset = bal_json |?> "asset" |> to_string_d "?" in
    let balance = bal_json |?> "balance" |> to_float_d 0.0 in

    let exch_tag = exch_tag_of exchange in
    let bid = bal_json |?> "bid" |> to_float_d 0.0 in
    let ask = bal_json |?> "ask" |> to_float_d 0.0 in
    let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else (max bid ask) in
    let hold_value = balance *. mid in

    let sell_orders = bal_json |?> "sell_orders" |> to_list_d in
    let sell_count = bal_json |?> "sell_count" |> to_int_d 0 in

    let unrealized_profit, pending_sell_qty = List.fold_left (fun (up, qty_acc) s ->
      let sp = s |?> "price" |> to_float_d 0.0 in
      let sq = s |?> "qty" |> to_float_d 0.0 in
      if sp > 0.0 && sq > 0.0 then (up +. (sp *. sq), qty_acc +. sq) else (up, qty_acc)
    ) (0.0, 0.0) sell_orders in
    
    let is_quote = img_is_quote in
    
    let accum_holding = if is_quote then 0.0 else max 0.0 (balance -. pending_sell_qty) in
    let accum_hold_value = accum_holding *. mid in

    let closest_sell_dist_pct =
      let sell_prices = List.filter_map (fun s ->
        let sp = s |?> "price" |> to_float_d 0.0 in
        if sp > 0.0 && mid > 0.0 then Some (((sp -. mid) /. mid) *. 100.0) else None
      ) sell_orders in
      match sell_prices with [] -> None | prices -> Some (List.fold_left (fun acc p -> if abs_float p < abs_float acc then p else acc) (List.hd prices) prices)
    in
    
    let closest_sell_price_opt =
      let sell_prices = List.filter_map (fun s -> let sp = s |?> "price" |> to_float_d 0.0 in if sp > 0.0 then Some sp else None) sell_orders in
      match sell_prices with [] -> None | prices -> Some (List.fold_left min (List.hd prices) prices)
    in

    let bg_color = if is_even then c_panel else c_bg in
    
    let a_text       = A.(Theme.a_text   ++ bg bg_color) in
    let a_green      = A.(Theme.a_green  ++ bg bg_color) in
    let a_red        = A.(Theme.a_red    ++ bg bg_color) in
    let a_yellow     = A.(Theme.a_yellow ++ bg bg_color) in
    let a_cyan       = A.(Theme.a_cyan   ++ bg bg_color) in
    let a_dim        = A.(Theme.a_dim    ++ bg bg_color) in
    let a_border     = A.(Theme.a_border ++ bg bg_color) in
    let a_border_outer = A.(Theme.a_border ++ bg c_bg) in
    let a_bps_tight  = A.(Theme.a_bps_tight ++ bg bg_color) in
    let a_bps_norm   = A.(Theme.a_bps_norm  ++ bg bg_color) in
    let a_bps_wide   = A.(Theme.a_bps_wide  ++ bg bg_color) in
    let a_bps_xtrm   = A.(Theme.a_bps_xtrm  ++ bg bg_color) in
    let exch_sym_attr ?dim exch = A.(Theme.exch_sym_attr ?dim exch ++ bg bg_color) in
    
    let col w attr s = I.string attr (pad_right w s) in
    let col_right w attr s = I.string attr (pad_left w s) in
    let close_row w img =
      let d = w - I.width img - 2 in
      I.hcat [ img; I.string A.(bg bg_color) (String.make (max 0 d) ' '); I.string A.(bg bg_color) " "; I.string a_border_outer "│" ]
    in

    let sell_dist_str, sell_dist_attr = match closest_sell_dist_pct with
      | None -> "--", a_dim
      | Some d ->
          let abs_d = abs_float d in
          let attr = if abs_d < 0.5 then a_yellow else if abs_d < 2.0 then a_cyan else a_dim in
          format_pct d, attr
    in

    let sell_price_str, sell_price_attr =
      match closest_sell_price_opt with Some sp -> format_price sp, a_yellow | None -> "--", a_dim
    in

    let render_gauge b_pct s_pct =
      let half = 7 in
      let pos d =
        if d < 0.0 then half else if d < 0.25 then 0 else if d < 0.75 then 1 else if d < 1.5 then 2 else if d < 3.0 then 3 else if d < 6.0 then 4 else if d < 12.0 then 5 else 6
      in
      let b_pos = match b_pct with Some d -> pos (abs_float d) | None -> half + 1 in
      let s_pos = match s_pct with Some d -> pos (abs_float d) | None -> half + 1 in
      let left_side = List.init half (fun i -> if half - 1 - i = b_pos then "B" else "─") |> String.concat "" in
      let right_side = List.init half (fun i -> if i = s_pos then "S" else "─") |> String.concat "" in
      I.hcat [
        I.string a_border "[";
        I.string (if b_pos <= 1 then a_green else a_dim) left_side;
        I.string a_border "┼";
        I.string (if s_pos <= 1 then a_yellow else a_dim) right_side;
        I.string a_border "]";
      ]
    in
    let gauge_img = render_gauge None closest_sell_dist_pct in

    let status_str, status_attr = if is_quote then "$", a_green else "⏹", a_red in
    let spread_str = format_spread_bps bid ask in
    let spread_attr =
      if bid <= 0.0 || ask <= 0.0 then a_dim
      else
        let bps = ((ask -. bid) /. ((bid +. ask) /. 2.0)) *. 10000.0 in
        if bps < 5.0 then a_bps_tight else if bps < 20.0 then a_bps_norm else if bps < 50.0 then a_bps_wide else a_bps_xtrm
    in
    let p_fg = match closest_sell_dist_pct with Some _ -> c_red | None -> c_border in
    let p_border_attr = A.(fg p_fg ++ bg bg_color) in
    let price_str = if mid > 0.0 then format_price mid else "--" in
    let price_cell = I.hcat [
      I.string p_border_attr "[";
      col_right 10 a_text price_str;
      I.string p_border_attr "]";
    ] in

    close_row w (I.hcat [
      I.string a_border_outer " │";
      I.string A.(bg bg_color) "  ";
      col 16 (exch_sym_attr ~dim:true exchange) (Printf.sprintf "%s(%s)" (truncate_string 10 asset) exch_tag);
      col 5 a_dim "--";
      I.hcat [ I.string status_attr status_str; I.string a_text "  " ];
      price_cell;
      col_right 8 spread_attr spread_str;
      I.string a_border " │ ";
      col_right 12 a_dim "--";
      col_right 8 a_dim "--";
      gauge_img;
      col_right 8 sell_dist_attr sell_dist_str;
      col_right 12 sell_price_attr sell_price_str;
      col_right 6 (if sell_count > 0 then a_yellow else a_dim) (string_of_int sell_count);
      col_right 12 (if unrealized_profit >= 0.0 && sell_count > 0 then a_green else if unrealized_profit > 0.0 then a_dim else a_dim)
        (if sell_count > 0 then format_pnl unrealized_profit else "--");
      I.string a_border " │ ";
      col_right 12 a_text (format_qty balance);
      col_right 10 a_text (if hold_value > 0.01 then format_price hold_value else "--");
      col_right 12 a_text (if accum_holding > 0.0001 then format_qty accum_holding else "0");
      col_right 10 a_text (if accum_hold_value > 0.01 then format_price accum_hold_value else "--");
    ])
  in

  let valid_balances = List.filter (fun bal_json ->
    let balance = bal_json |?> "balance" |> to_float_d 0.0 in
    balance > 0.0
  ) all_balances in

  let inactive_jsons = List.filter (fun bal_json ->
    let asset = bal_json |?> "asset" |> to_string_d "?" in
    let is_quote = asset = "USD" || asset = "USDC" || asset = "USDT" || asset = "ZUSD" || asset = "USDe" in
    not is_quote
  ) valid_balances in

  let quote_jsons = List.filter (fun bal_json ->
    let asset = bal_json |?> "asset" |> to_string_d "?" in
    let is_quote = asset = "USD" || asset = "USDC" || asset = "USDT" || asset = "ZUSD" || asset = "USDe" in
    is_quote
  ) valid_balances in

  let inactive_rows = List.mapi (fun i bal -> build_balance_row (i mod 2 = 1) bal false) inactive_jsons in
  let quote_rows = List.mapi (fun i bal -> build_balance_row (i mod 2 = 1) bal true) quote_jsons in

  let total_up_strats, total_hold_strats, total_accum_val_strats = List.fold_left (fun (up_acc, hold_acc, accum_val_acc) (_symbol, data) ->
    let strat = data |?> "strategy" in
    let market = data |?> "market" in
    let bid = market |?> "bid" |> to_float_d 0.0 in
    let ask = market |?> "ask" |> to_float_d 0.0 in
    let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else (max bid ask) in
    let base_bal = market |?> "base_balance" |> to_float_d 0.0 in
    
    let sell_orders = strat |?> "sell_orders" |> to_list_d in
    let strat_up, pending_sell_qty = List.fold_left (fun (a, q_acc) s ->
      let sp = s |?> "price" |> to_float_d 0.0 in
      let sq = s |?> "qty" |> to_float_d 0.0 in
      if sp > 0.0 && sq > 0.0 then (a +. (sp *. sq), q_acc +. sq) else (a, q_acc)
    ) (0.0, 0.0) sell_orders in
    
    let accum_holding = max 0.0 (base_bal -. pending_sell_qty) in
    let accum_hold_value = accum_holding *. mid in
    
    (up_acc +. strat_up, hold_acc +. (base_bal *. mid), accum_val_acc +. accum_hold_value)
  ) (0.0, 0.0, 0.0) strats in

  let total_up_bals, total_hold_bals, total_accum_val_bals = List.fold_left (fun (up_acc, hold_acc, accum_val_acc) bal_json ->
    let balance = bal_json |?> "balance" |> to_float_d 0.0 in
    let asset = bal_json |?> "asset" |> to_string_d "?" in
    if balance <= 0.0 then (up_acc, hold_acc, accum_val_acc) else
    let bid = bal_json |?> "bid" |> to_float_d 0.0 in
    let ask = bal_json |?> "ask" |> to_float_d 0.0 in
    let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else (max bid ask) in
    
    let is_quote = asset = "USD" || asset = "USDC" || asset = "USDT" || asset = "ZUSD" || asset = "USDe" in
    
    let sell_orders = bal_json |?> "sell_orders" |> to_list_d in
    let bal_up, pending_sell_qty = List.fold_left (fun (a, q_acc) s ->
      let sp = s |?> "price" |> to_float_d 0.0 in
      let sq = s |?> "qty" |> to_float_d 0.0 in
      if sp > 0.0 && sq > 0.0 then (a +. (sp *. sq), q_acc +. sq) else (a, q_acc)
    ) (0.0, 0.0) sell_orders in
    
    let accum_holding = if is_quote then 0.0 else max 0.0 (balance -. pending_sell_qty) in
    let accum_hold_value = accum_holding *. mid in
    
    (up_acc +. bal_up, hold_acc +. (balance *. mid), accum_val_acc +. accum_hold_value)
  ) (0.0, 0.0, 0.0) all_balances in
  
  let total_up = total_up_strats +. total_up_bals in
  let total_hold_val = total_hold_strats +. total_hold_bals in
  let total_accum_val = total_accum_val_strats +. total_accum_val_bals in

  let total_quote_val = List.fold_left (fun acc bal_json ->
    let asset   = bal_json |?> "asset"   |> to_string_d "" in
    let balance = bal_json |?> "balance" |> to_float_d 0.0 in
    let is_quote = asset = "USD" || asset = "USDC" || asset = "USDT" || asset = "ZUSD" || asset = "USDe" in
    if is_quote && balance > 0.0 then acc +. balance else acc
  ) 0.0 all_balances in

  let title = section_title w "HOLDINGS & STRATEGY" in

  let thin_sep label =
    let lbl = " ├── " ^ label ^ " " in
    let lbl_img = I.string A.(fg c_border ++ bg c_bg) lbl in
    let pad_count = max 0 (w - I.width lbl_img - 1) in
    let pad_buf = Buffer.create (pad_count * 3) in
    for _ = 1 to pad_count do Buffer.add_string pad_buf "─" done;
    I.hcat [
      lbl_img;
      I.string A.(fg c_border ++ bg c_bg) (Buffer.contents pad_buf);
      I.string A.(fg c_border ++ bg c_bg) "┤"
    ]
  in

  let has_inactive = inactive_rows <> [] in
  let has_quote = quote_rows <> [] in

  let rows = [title; header]
    @ active_images
    @ paused_images
    @ (if has_inactive then [thin_sep "balances"] @ inactive_rows else [])
    @ (if has_quote then [thin_sep "cash"] @ quote_rows else [])
  in
  let main_table = I.vcat rows in

  let up_attr = if total_up >= 0.0 then A.(fg c_green ++ bg c_bg ++ st bold)
                else A.(fg c_red   ++ bg c_bg ++ st bold) in
  let pipe = I.string A.(fg c_border ++ bg c_bg) "  │  " in
  let kv lbl value_s vattr =
    I.hcat [
      I.string A.(fg c_label ++ bg c_bg) ("  " ^ lbl ^ ": ");
      I.string vattr value_s;
    ]
  in
  let summary_bar = close_row w (I.hcat [
    I.string A.(fg c_border ++ bg c_bg) " │";
    kv "Cash"      (format_price total_quote_val) A.(fg c_cyan   ++ bg c_bg ++ st bold);
    pipe;
    kv "Accum Val" (format_price total_accum_val) A.(fg c_bright ++ bg c_bg ++ st bold);
    pipe;
    kv "Hold Val"  (format_price total_hold_val)  A.(fg c_bright ++ bg c_bg ++ st bold);
    pipe;
    kv "Sell Val"  (format_pnl   total_up)        up_attr;
  ]) in
  let summary_section = I.vcat (List.filter (fun img -> I.height img > 0) [
    thin_sep "summary";
    summary_bar;
  ]) in

  I.vcat [ main_table; summary_section; section_footer w ]
