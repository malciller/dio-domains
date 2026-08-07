open Notty
open Theme

let repeat_utf8 str count =
  let count = max 0 count in
  let buf = Buffer.create (String.length str * count) in
  for _ = 1 to count do Buffer.add_string buf str done;
  Buffer.contents buf

let render_asset_detail w h asset_key json =
  let assets = Holdings.get_selectable_assets json in
  let asset_opt =
    if asset_key = "" then (match assets with head :: _ -> Some head | [] -> None)
    else List.find_opt (fun (a : Holdings.selectable_asset) -> a.key = asset_key) assets
  in

  match asset_opt with
  | None ->
      let msg = I.string A.(fg c_yellow ++ bg c_bg) "No asset selected or available."
                |> I.hsnap ~align:`Left w
                |> I.vsnap ~align:`Top h in
      msg
  | Some a ->
      let exch_tag = exch_tag_of a.exchange in
      let title_str = Printf.sprintf " ASSET GRAPH & PENDING ORDERS: %s (%s) " a.symbol exch_tag in
      let header_bar = close_row w (
        I.hcat [
          I.string A.(fg c_accent ++ bg c_section_bg ++ st bold) title_str;
          I.string A.(fg c_border ++ bg c_section_bg) (repeat_utf8 "─" (max 0 (w - String.length title_str - 1)))
        ]
      ) in

      (* Extract market data *)
      let market = if a.is_strategy then a.data |?> "market" else a.data in
      let bid = market |?> "bid" |> to_float_d 0.0 in
      let ask = market |?> "ask" |> to_float_d 0.0 in
      let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else (max bid ask) in
      let base_bal = if a.is_strategy then market |?> "base_balance" |> to_float_d 0.0 else a.data |?> "balance" |> to_float_d 0.0 in
      let quote_bal = market |?> "quote_balance" |> to_float_d 0.0 in
      let hold_val = base_bal *. mid in

      (* Extract strategy & order details *)
      let strat_json = if a.is_strategy then a.data |?> "strategy" else `Null in
      let stype = if a.is_strategy then strat_json |?> "type" |> to_string_d "Grid" else "Balance" in
      let accum_profit = strat_json |?> "accumulated_profit" |> to_float_d 0.0 in
      let reserved_base = strat_json |?> "reserved_base" |> to_float_d 0.0 in
      let cap_low = strat_json |?> "capital_low" |> to_bool_d false in
      let last_buy_fill = strat_json |?> "last_buy_fill" |> to_float_d 0.0 in
      let last_sell_fill = strat_json |?> "last_sell_fill" |> to_float_d 0.0 in

      (* Collect sell orders *)
      let sell_orders_json =
        if a.is_strategy then strat_json |?> "sell_orders" |> to_list_d
        else a.data |?> "sell_orders" |> to_list_d
      in
      let sell_orders = List.filter_map (fun s ->
        let id = s |?> "id" |> to_string_d "?" in
        let price = s |?> "price" |> to_float_d 0.0 in
        let qty = s |?> "qty" |> to_float_d 0.0 in
        if price > 0.0 && qty > 0.0 then Some (id, price, qty) else None
      ) sell_orders_json in

      (* Collect buy orders *)
      let buy_orders =
        if a.is_strategy && (not cap_low) then
          let bp = strat_json |?> "buy_price" |> to_float_d 0.0 in
          let bq = strat_json |?> "buy_qty" |> to_float_d (strat_json |?> "grid_qty" |> to_float_d 0.0) in
          let bid_id = strat_json |?> "buy_id" |> to_string_d "buy" in
          if bp > 0.0 then [(bid_id, bp, bq)] else []
        else []
      in

      (* Asset Summary Card *)
      let summary_card = close_row w (
        I.hcat [
          I.string a_border " │ ";
          I.string a_label "STGY: "; I.string a_cyan (pad_right 6 stype);
          I.string a_label " MID: "; I.string (if mid > 0.0 then a_bright else a_dim) (pad_right 11 (if mid > 0.0 then format_price mid else "--"));
          I.string a_label " BID/ASK: "; I.string a_text (Printf.sprintf "%.2f / %.2f" bid ask);
          I.string a_border "  │  ";
          I.string a_label "HOLDING: "; I.string a_text (format_qty base_bal ^ " " ^ a.asset);
          I.string a_label " ("; I.string a_text (format_usd hold_val); I.string a_text ")";
          I.string a_border "  │  ";
          (if a.is_strategy then
            I.hcat [
              I.string a_label "ACCUM PROFIT: "; I.string (if accum_profit >= 0.0 then a_green else a_red) (format_pnl accum_profit);
              I.string a_label "  RES BASE: "; I.string a_yellow (format_qty reserved_base);
              (if last_buy_fill > 0.0 then I.hcat [ I.string a_label "  LAST BUY: "; I.string a_green (format_price last_buy_fill) ] else I.empty);
              (if last_sell_fill > 0.0 then I.hcat [ I.string a_label "  LAST SELL: "; I.string a_yellow (format_price last_sell_fill) ] else I.empty);
            ]
           else
            I.hcat [ I.string a_label "QUOTE BAL: "; I.string a_text (format_usd quote_bal) ]
          );
        ]
      ) in

      (* Determine lowest buy price anchor *)
      let lowest_buy_price_opt =
        match buy_orders with
        | [] -> None
        | orders ->
            let prices = List.map (fun (_, p, _) -> p) orders in
            Some (List.fold_left min (List.hd prices) prices)
      in

      (* Anchor min_p at the buy price so the Buy Order is the visible bottom anchor *)
      let min_p, max_p =
        match lowest_buy_price_opt with
        | Some bp ->
            let base_min = bp *. 0.998 in
            let span = if mid > bp then mid -. bp else bp *. 0.02 in
            let base_max = mid +. (span *. 2.5) in
            let sell_prices = List.map (fun (_, p, _) -> p) sell_orders in
            let max_p_val =
              match sell_prices with
              | [] -> base_max
              | prices ->
                  let max_sell = List.fold_left max (List.hd prices) prices in
                  min (max_sell *. 1.005) (max base_max (mid +. span *. 3.0))
            in
            (base_min, max_p_val)
        | None ->
            let all_prices = (if mid > 0.0 then [mid] else []) @ List.map (fun (_, p, _) -> p) sell_orders in
            (match all_prices with
             | [] -> (100.0, 110.0)
             | [p] -> (p *. 0.97, p *. 1.03)
             | prices ->
                 let low = List.fold_left min (List.hd prices) prices in
                 let high = List.fold_left max (List.hd prices) prices in
                 (low *. 0.995, high *. 1.005))
      in

      (* Recent fills for this asset *)
      let recent_fills = json |?> "recent_fills" |> to_list_d in
      let asset_fills = List.filter (fun f ->
        let fsym = f |?> "symbol" |> to_string_d "" in
        fsym = a.symbol || fsym = a.asset || (String.contains fsym '/' && List.hd (String.split_on_char '/' fsym) = a.asset)
      ) recent_fills in

      let fills_count = min 3 (List.length asset_fills) in
      let fills_h = if fills_count > 0 then 1 + fills_count else 0 in

      (* Render Price Graph Ladder *)
      let graph_height = max 6 (min 12 (h - 18 - fills_h)) in
      let price_step = (max_p -. min_p) /. float_of_int (max 1 (graph_height - 1)) in

      let graph_rows = List.init graph_height (fun row_idx ->
        let level_price = max_p -. (float_of_int row_idx *. price_step) in
        let lower_bound = level_price -. (price_step /. 2.0) in
        let upper_bound = level_price +. (price_step /. 2.0) in

        let is_mid_level = mid >= lower_bound && mid < upper_bound in
        let s_orders_at_level = List.filter (fun (_, p, _) -> p >= lower_bound && p < upper_bound) sell_orders in
        let b_orders_at_level = List.filter (fun (_, p, _) -> p >= lower_bound && p < upper_bound) buy_orders in

        let price_lbl = pad_left 11 (format_price level_price) in

        let bar_width = max 20 (w - 45) in

        let content_img =
          if is_mid_level then
            let mid_str = Printf.sprintf " ═════════◄ MARKET MID %s (Bid: %.2f / Ask: %.2f) ═════════"
              (format_price mid) bid ask in
            I.string A.(fg c_green ++ bg c_panel ++ st bold) (pad_right bar_width mid_str)
          else if s_orders_at_level <> [] then
            let total_qty = List.fold_left (fun acc (_, _, q) -> acc +. q) 0.0 s_orders_at_level in
            let dist_pct = if mid > 0.0 then ((level_price -. mid) /. mid) *. 100.0 else 0.0 in
            let bar_len = min bar_width (max 4 (int_of_float (total_qty *. 10.0))) in
            let bar_str = repeat_utf8 "█" bar_len in
            let info_str = Printf.sprintf " SELL %s (%s @ %s [%s])"
              bar_str (format_qty total_qty) (format_price level_price) (format_pct dist_pct) in
            I.string A.(fg c_red ++ bg c_bg ++ st bold) (pad_right bar_width info_str)
          else if b_orders_at_level <> [] then
            let total_qty = List.fold_left (fun acc (_, _, q) -> acc +. q) 0.0 b_orders_at_level in
            let dist_pct = if mid > 0.0 then ((level_price -. mid) /. mid) *. 100.0 else 0.0 in
            let bar_len = min bar_width (max 4 (int_of_float (total_qty *. 10.0))) in
            let bar_str = repeat_utf8 "█" bar_len in
            let info_str = Printf.sprintf " BUY  %s (%s @ %s [%s])"
              bar_str (format_qty total_qty) (format_price level_price) (format_pct dist_pct) in
            I.string A.(fg c_cyan ++ bg c_bg ++ st bold) (pad_right bar_width info_str)
          else
            let dot_pattern = String.concat " " (List.init (bar_width / 2) (fun _ -> "·")) in
            I.string A.(fg c_border ++ bg c_bg) (pad_right bar_width dot_pattern)
        in

        close_row w (
          I.hcat [
            I.string a_border " │ ";
            I.string A.(fg c_title ++ bg c_bg) price_lbl;
            I.string a_border " │ ";
            content_img;
            I.string a_border " │";
          ]
        )
      ) in

      let graph_section = I.vcat [
        close_row w (I.string A.(fg c_border ++ bg c_bg) (" ├── PRICE LADDER & ORDER GRAPH " ^ repeat_utf8 "─" (max 0 (w - 32))));
        I.vcat graph_rows;
      ] in

      (* Open Orders Table *)
      let order_headers = close_row w (
        I.hcat [
          I.string a_border " │  ";
          col 6 a_label "SIDE";
          col 18 a_label "ORDER ID";
          col_right 12 a_label "PRICE";
          col_right 12 a_label "QTY";
          col_right 14 a_label "VALUE ($)";
          col_right 12 a_label "Δ MID";
        ]
      ) in

      let render_order_row is_sell (id, price, qty) =
        let side_str, side_attr = if is_sell then "SELL", a_red else "BUY ", a_green in
        let val_usd = price *. qty in
        let dist_pct = if mid > 0.0 then ((price -. mid) /. mid) *. 100.0 else 0.0 in
        close_row w (
          I.hcat [
            I.string a_border " │  ";
            col 6 side_attr side_str;
            col 18 a_text (truncate_string 16 id);
            col_right 12 (if is_sell then a_yellow else a_green) (format_price price);
            col_right 12 a_text (format_qty qty);
            col_right 14 a_text (format_usd val_usd);
            col_right 12 (if is_sell then a_yellow else a_cyan) (format_pct dist_pct);
          ]
        )
      in

      let sorted_sell_orders = List.sort (fun (_, p1, _) (_, p2, _) -> Float.compare p1 p2) sell_orders in

      let static_h = 1 (* header *) + 1 (* summary *) + 1 (* graph title *) + graph_height + 2 (* orders title+header *) + fills_h + 3 (* footers *) in
      let total_available_order_rows = max 3 (h - static_h) in
      let buy_order_count = List.length buy_orders in
      let max_sell_display = max 1 (total_available_order_rows - buy_order_count - 1) in

      let rec take n = function
        | [] -> []
        | head :: tail -> if n <= 0 then [] else head :: take (n - 1) tail
      in
      let display_sell_orders = take max_sell_display sorted_sell_orders in
      let hidden_sell_count = List.length sorted_sell_orders - List.length display_sell_orders in
      let max_sell_price = List.fold_left (fun acc (_, p, _) -> max acc p) 0.0 sorted_sell_orders in

      let sell_order_rows = List.map (render_order_row true) (List.rev display_sell_orders) in
      let buy_order_rows = List.map (render_order_row false) buy_orders in

      let hidden_sell_row =
        if hidden_sell_count > 0 then
          [close_row w (
            I.hcat [
              I.string a_border " │  ";
              I.string a_dim (Printf.sprintf "... plus %d more sell orders higher up (up to %s)" hidden_sell_count (format_price max_sell_price));
            ]
          )]
        else []
      in

      let orders_table = I.vcat (
        [
          close_row w (I.string A.(fg c_border ++ bg c_bg) (" ├── ACTIVE PENDING ORDERS (" ^ string_of_int (List.length sell_orders + List.length buy_orders) ^ ") " ^ repeat_utf8 "─" (max 0 (w - 32))));
          order_headers;
        ] @
        hidden_sell_row @
        sell_order_rows @
        buy_order_rows @
        (if sell_orders = [] && buy_orders = [] then
          [close_row w (I.hcat [ I.string a_border " │  "; I.string a_dim "No pending orders active for this asset." ])]
         else [])
      ) in

      let fills_section =
        if asset_fills = [] then I.empty
        else
          let fill_rows = List.mapi (fun _idx f ->
            let side = f |?> "side" |> to_string_d "?" in
            let fp = f |?> "fill_price" |> to_float_d 0.0 in
            let amt = f |?> "amount" |> to_float_d 0.0 in
            let fval = f |?> "value" |> to_float_d 0.0 in
            let side_attr = if String.lowercase_ascii side = "buy" then a_green else a_red in
            close_row w (
              I.hcat [
                I.string a_border " │  ";
                col 6 side_attr (String.uppercase_ascii side);
                col_right 12 a_text (format_price fp);
                col_right 12 a_text (format_qty amt);
                col_right 14 a_text (format_usd fval);
              ]
            )
          ) (List.filteri (fun i _ -> i < 3) asset_fills) in
          I.vcat ([
            close_row w (I.string A.(fg c_border ++ bg c_bg) (" ├── RECENT FILLS FOR " ^ a.symbol ^ " " ^ repeat_utf8 "─" (max 0 (w - 25))));
          ] @ fill_rows)
      in

      (* Navigation Footer *)
      let nav_footer = close_row w (
        I.hcat [
          I.string A.(fg c_cyan ++ bg c_section_bg ++ st bold) " [↑/↓ or ←/→] ";
          I.string A.(fg c_text ++ bg c_section_bg) "Prev/Next Asset   ";
          I.string A.(fg c_accent ++ bg c_section_bg ++ st bold) " [Esc / b / Backspace] ";
          I.string A.(fg c_text ++ bg c_section_bg) "Return to Dashboard   ";
          I.string A.(fg c_yellow ++ bg c_section_bg ++ st bold) " [q] ";
          I.string A.(fg c_text ++ bg c_section_bg) "Quit";
        ]
      ) in

      I.vcat [
        header_bar;
        summary_card;
        graph_section;
        orders_table;
        fills_section;
        section_footer w;
        nav_footer;
      ]
