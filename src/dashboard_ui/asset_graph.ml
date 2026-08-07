open Notty
open Theme

type level_info = {
  level_price: float;
  is_mid: bool;
  sell_orders: (string * float * float) list;
  buy_orders: (string * float * float) list;
}

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

      (* Determine price range bounds (min_p, max_p) to encompass ALL orders *)
      let min_p, max_p =
        let all_buy_prices = List.map (fun (_, p, _) -> p) buy_orders in
        let all_sell_prices = List.map (fun (_, p, _) -> p) sell_orders in
        let all_prices = (if mid > 0.0 then [mid] else []) @ all_buy_prices @ all_sell_prices in
        match all_prices with
        | [] -> (100.0, 110.0)
        | [p] -> (p *. 0.97, p *. 1.03)
        | prices ->
            let low = List.fold_left min (List.hd prices) prices in
            let high = List.fold_left max (List.hd prices) prices in
            let span = max (high -. low) (low *. 0.005) in
            (low -. (span *. 0.03), high +. (span *. 0.03))
      in

      (* Render Price Graph Ladder — takes 100% of remaining terminal height *)
      (* Overhead: header(1) + summary(1) + graph_title(1) + footers(2) = 5 lines *)
      let raw_height = max 10 (h - 5) in
      let price_step = (max_p -. min_p) /. float_of_int (max 1 (raw_height - 1)) in

      let all_levels = List.init raw_height (fun row_idx ->
        let level_price = max_p -. (float_of_int row_idx *. price_step) in
        let lower_bound = level_price -. (price_step /. 2.0) in
        let upper_bound = level_price +. (price_step /. 2.0) in

        let is_mid = mid >= lower_bound && mid < upper_bound in
        let s_orders = List.filter (fun (_, p, _) -> p >= lower_bound && p < upper_bound) sell_orders in
        let b_orders = List.filter (fun (_, p, _) -> p >= lower_bound && p < upper_bound) buy_orders in

        { level_price; is_mid; sell_orders = s_orders; buy_orders = b_orders }
      ) in

      let is_empty lvl = (not lvl.is_mid) && lvl.sell_orders = [] && lvl.buy_orders = [] in

      let rec compress acc current_empty = function
        | [] ->
            (match current_empty with
             | [] -> List.rev acc
             | [single] -> List.rev (`Single single :: acc)
             | [e1; e2] -> List.rev (`Single e2 :: `Single e1 :: acc)
             | elist -> List.rev (`Gap elist :: acc))
        | lvl :: rest ->
            if is_empty lvl then
              compress acc (current_empty @ [lvl]) rest
            else
              let acc' = match current_empty with
                | [] -> acc
                | [single] -> `Single single :: acc
                | [e1; e2] -> `Single e2 :: `Single e1 :: acc
                | elist -> `Gap elist :: acc
              in
              compress (`Single lvl :: acc') [] rest
      in

      let compressed_items = compress [] [] all_levels in
      let bar_width = max 20 (w - 45) in

      let graph_rows = List.map (function
        | `Single lvl ->
            let price_lbl = pad_left 11 (format_price lvl.level_price) in
            let content_img =
              if lvl.buy_orders <> [] then
                let count = List.length lvl.buy_orders in
                let total_qty = List.fold_left (fun acc (_, _, q) -> acc +. q) 0.0 lvl.buy_orders in
                let dist_pct = if mid > 0.0 then ((lvl.level_price -. mid) /. mid) *. 100.0 else 0.0 in
                let bar_len = min 6 (max 3 (int_of_float (log10 (max 1.0 total_qty) *. 1.5 +. 3.0))) in
                let bar_str = repeat_utf8 "█" bar_len in
                let count_tag = if count > 1 then Printf.sprintf "[%dx] " count else "" in
                let mid_tag = if lvl.is_mid then Printf.sprintf " ◄ MARKET MID %s" (format_price mid) else "" in
                let info_str = Printf.sprintf " BUY  %s%s (%s @ %s [%s])%s"
                  count_tag bar_str (format_qty total_qty) (format_price lvl.level_price) (format_pct dist_pct) mid_tag in
                I.string A.(fg c_cyan ++ bg c_bg ++ st bold) (pad_right bar_width info_str)
              else if lvl.sell_orders <> [] then
                let count = List.length lvl.sell_orders in
                let total_qty = List.fold_left (fun acc (_, _, q) -> acc +. q) 0.0 lvl.sell_orders in
                let dist_pct = if mid > 0.0 then ((lvl.level_price -. mid) /. mid) *. 100.0 else 0.0 in
                let bar_len = min 6 (max 3 (int_of_float (log10 (max 1.0 total_qty) *. 1.5 +. 3.0))) in
                let bar_str = repeat_utf8 "█" bar_len in
                let count_tag = if count > 1 then Printf.sprintf "[%dx] " count else "" in
                let mid_tag = if lvl.is_mid then Printf.sprintf " ◄ MARKET MID %s" (format_price mid) else "" in
                let info_str = Printf.sprintf " SELL %s%s (%s @ %s [%s])%s"
                  count_tag bar_str (format_qty total_qty) (format_price lvl.level_price) (format_pct dist_pct) mid_tag in
                I.string A.(fg c_red ++ bg c_bg ++ st bold) (pad_right bar_width info_str)
              else if lvl.is_mid then
                let mid_str = Printf.sprintf " ═════════◄ MARKET MID %s (Bid: %.2f / Ask: %.2f) ═════════"
                  (format_price mid) bid ask in
                I.string A.(fg c_green ++ bg c_panel ++ st bold) (pad_right bar_width mid_str)
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
        | `Gap elist ->
            let top_lvl = List.hd elist in
            let bot_lvl = List.hd (List.rev elist) in
            let price_lbl = pad_left 11 (format_price bot_lvl.level_price) in
            let gap_str = Printf.sprintf " ─────── ░░ PRICE GAP: %s ── %s (%d empty steps) ░░ ───────"
              (format_price bot_lvl.level_price) (format_price top_lvl.level_price) (List.length elist) in
            close_row w (
              I.hcat [
                I.string a_border " │ ";
                I.string A.(fg c_dim ++ bg c_bg) price_lbl;
                I.string a_border " │ ";
                I.string A.(fg c_dim ++ bg c_bg) (pad_right bar_width gap_str);
                I.string a_border " │";
              ]
            )
      ) compressed_items in

      let num_sells = List.length sell_orders in
      let num_buys = List.length buy_orders in
      let orders_summary = Printf.sprintf " (%d Sell, %d Buy Pending) " num_sells num_buys in
      let graph_title_text = " ├── PRICE LADDER & ORDER GRAPH" ^ orders_summary in

      let graph_section = I.vcat [
        close_row w (I.string A.(fg c_border ++ bg c_bg) (graph_title_text ^ repeat_utf8 "─" (max 0 (w - String.length graph_title_text - 1))));
        I.vcat graph_rows;
      ] in

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
        section_footer w;
        nav_footer;
      ]



