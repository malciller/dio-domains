open Notty
open Theme

type price_snapshot =
  { timestamp : float
  ; mid_p : float
  ; buy_ps : float list
  ; sell_ps : float list
  }

let repeat_utf8 str count =
  let count = max 0 count in
  let buf = Buffer.create (String.length str * count) in
  for _ = 1 to count do
    Buffer.add_string buf str
  done;
  Buffer.contents buf
;;

(** Convert 8-bit Braille dot bitmask to a 3-byte UTF-8 string. *)
let braille_to_utf8 mask =
  let code = 0x2800 lor (mask land 0xFF) in
  let b1 = Char.chr (0xE0 lor (code lsr 12)) in
  let b2 = Char.chr (0x80 lor ((code lsr 6) land 0x3F)) in
  let b3 = Char.chr (0x80 lor (code land 0x3F)) in
  String.init 3 (function
    | 0 -> b1
    | 1 -> b2
    | _ -> b3)
;;

(** Returns Braille bit flag for subpixel (sub_x, sub_y) where sub_x ∈ {0,1}, sub_y ∈ {0..3} top-down. *)
let braille_bit sub_x sub_y =
  match sub_x, sub_y with
  | 0, 0 -> 0x01
  | 0, 1 -> 0x02
  | 0, 2 -> 0x04
  | 0, 3 -> 0x40
  | 1, 0 -> 0x08
  | 1, 1 -> 0x10
  | 1, 2 -> 0x20
  | 1, 3 -> 0x80
  | _ -> 0
;;

(** Fixed 15-minute time window in seconds (900s). *)
let window_seconds = 900.0

(** Rolling price & order history buffer per asset key.
    Tracks live mid prices as well as active buy and sell order price levels over 15 minutes. *)
let price_history : (string, price_snapshot Queue.t) Hashtbl.t = Hashtbl.create 32

(** Global zoom level per asset key.
    0 = Full view showing all orders.
    Higher values = zoomed in around mid price (capped at 1 order on each side). *)
let zoom_levels : (string, int) Hashtbl.t = Hashtbl.create 16

let get_zoom asset_key =
  try Hashtbl.find zoom_levels asset_key with
  | Not_found -> 0
;;

let set_zoom asset_key z = Hashtbl.replace zoom_levels asset_key (max 0 z)
let zoom_in asset_key = set_zoom asset_key (get_zoom asset_key + 1)
let zoom_out asset_key = set_zoom asset_key (max 0 (get_zoom asset_key - 1))

let record_all_prices json =
  let assets = Holdings.get_selectable_assets json in
  let now = Unix.gettimeofday () in
  List.iter
    (fun (a : Holdings.selectable_asset) ->
       let market = if a.is_strategy then a.data |?> "market" else a.data in
       let bid = market |?> "bid" |> to_float_d 0.0 in
       let ask = market |?> "ask" |> to_float_d 0.0 in
       let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else max bid ask in
       let strat_json = if a.is_strategy then a.data |?> "strategy" else `Null in
       let sell_orders_json =
         if a.is_strategy
         then strat_json |?> "sell_orders" |> to_list_d
         else a.data |?> "sell_orders" |> to_list_d
       in
       let sell_ps =
         List.filter_map
           (fun s ->
              let p = s |?> "price" |> to_float_d 0.0 in
              let q = s |?> "qty" |> to_float_d 0.0 in
              if p > 0.0 && q > 0.0 then Some p else None)
           sell_orders_json
       in
       let buy_orders_json =
         if a.is_strategy
         then strat_json |?> "buy_orders" |> to_list_d
         else a.data |?> "buy_orders" |> to_list_d
       in
       let buy_orders_parsed =
         List.filter_map
           (fun s ->
              let p = s |?> "price" |> to_float_d 0.0 in
              let q = s |?> "qty" |> to_float_d 0.0 in
              if p > 0.0 && q > 0.0 then Some p else None)
           buy_orders_json
       in
       let buy_ps =
         if buy_orders_parsed <> []
         then buy_orders_parsed
         else if a.is_strategy
         then (
           let bp = strat_json |?> "buy_price" |> to_float_d 0.0 in
           if bp > 0.0 then [ bp ] else [])
         else []
       in
       if mid > 0.0
       then (
         let q =
           match Hashtbl.find_opt price_history a.key with
           | Some q -> q
           | None ->
             let q = Queue.create () in
             Hashtbl.add price_history a.key q;
             q
         in
         (* Prune entries older than the 15 minute window. *)
         while
           (not (Queue.is_empty q)) && now -. (Queue.peek q).timestamp > window_seconds
         do
           ignore (Queue.pop q)
         done;
         let should_push =
           if Queue.is_empty q
           then true
           else (
             let last_snap =
               Queue.fold
                 (fun _ item -> item)
                 { timestamp = 0.0; mid_p = 0.0; buy_ps = []; sell_ps = [] }
                 q
             in
             now -. last_snap.timestamp >= 1.0
             || abs_float (mid -. last_snap.mid_p) > 0.000001
             || buy_ps <> last_snap.buy_ps
             || sell_ps <> last_snap.sell_ps)
         in
         if should_push
         then Queue.push { timestamp = now; mid_p = mid; buy_ps; sell_ps } q))
    assets
;;

let render_asset_detail w h asset_key json =
  let assets = Holdings.get_selectable_assets json in
  let asset_opt =
    if asset_key = ""
    then (
      match assets with
      | head :: _ -> Some head
      | [] -> None)
    else List.find_opt (fun (a : Holdings.selectable_asset) -> a.key = asset_key) assets
  in
  match asset_opt with
  | None ->
    let msg =
      I.string A.(fg c_yellow ++ bg c_bg) "No asset selected or available."
      |> I.hsnap ~align:`Left w
      |> I.vsnap ~align:`Top h
    in
    msg
  | Some a ->
    let exch_tag = exch_tag_of a.exchange in
    let title_str = Printf.sprintf "%s (%s)" a.symbol exch_tag in
    let exch_attr = exch_sym_attr a.exchange in
    let header_bar = section_title ~title_attr:exch_attr w title_str in
    (* Extract the market data block, which lives under "market" for
       strategy assets and directly on the entry for balances. *)
    let market = if a.is_strategy then a.data |?> "market" else a.data in
    let bid = market |?> "bid" |> to_float_d 0.0 in
    let ask = market |?> "ask" |> to_float_d 0.0 in
    let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else max bid ask in
    let base_bal =
      if a.is_strategy
      then market |?> "base_balance" |> to_float_d 0.0
      else a.data |?> "balance" |> to_float_d 0.0
    in
    let quote_bal = market |?> "quote_balance" |> to_float_d 0.0 in
    let hold_val = base_bal *. mid in
    (* Record the live mid price and active order levels into the rolling
       history buffer. *)
    record_all_prices json;
    (* Extract the strategy and order details for the summary card. *)
    let strat_json = if a.is_strategy then a.data |?> "strategy" else `Null in
    let stype =
      if a.is_strategy then strat_json |?> "type" |> to_string_d "Ladder" else "Balance"
    in
    let last_buy_fill = strat_json |?> "last_buy_fill" |> to_float_d 0.0 in
    let last_sell_fill = strat_json |?> "last_sell_fill" |> to_float_d 0.0 in
    (* Collect the sell orders, preferring the strategy's own list and
       falling back to the exchange feed. *)
    let market_json = a.data |?> "market" in
    let sell_orders_json =
      let strat_sells =
        if a.is_strategy then strat_json |?> "sell_orders" |> to_list_d else []
      in
      if strat_sells <> []
      then strat_sells
      else if a.is_strategy
      then market_json |?> "sell_orders" |> to_list_d
      else a.data |?> "sell_orders" |> to_list_d
    in
    let sell_orders =
      List.filter_map
        (fun s ->
           let id = s |?> "id" |> to_string_d "?" in
           let price = s |?> "price" |> to_float_d 0.0 in
           let qty = s |?> "qty" |> to_float_d 0.0 in
           if price > 0.0 && qty > 0.0 then Some (id, price, qty) else None)
        sell_orders_json
    in
    (* Calculate the accumulated holding and its value, excluding the
       pending sell quantity. *)
    let pending_sell_qty =
      List.fold_left (fun acc (_, _, q) -> acc +. q) 0.0 sell_orders
    in
    let staked_bal =
      if a.is_strategy
      then market |?> "staked_balance" |> to_float_d 0.0
      else a.data |?> "staked_balance" |> to_float_d 0.0
    in
    (* Staked HYPE is part of [base_bal] but can never be covered by a
       resting sell (it is not tradeable), so it is never reduced by the
       pending sell quantity. *)
    let accum_qty =
      staked_bal +. Float.max 0.0 (base_bal -. staked_bal -. pending_sell_qty)
    in
    let accum_val = accum_qty *. mid in
    (* Collect the buy orders and distinguish real exchange orders from
       synthetic strategy targets. *)
    let buy_orders_json =
      let strat_buys =
        if a.is_strategy then strat_json |?> "buy_orders" |> to_list_d else []
      in
      if strat_buys <> []
      then strat_buys
      else if a.is_strategy
      then market_json |?> "buy_orders" |> to_list_d
      else a.data |?> "buy_orders" |> to_list_d
    in
    let buy_orders_parsed =
      List.filter_map
        (fun s ->
           let id = s |?> "id" |> to_string_d "?" in
           let price = s |?> "price" |> to_float_d 0.0 in
           let qty = s |?> "qty" |> to_float_d 0.0 in
           if price > 0.0 && qty > 0.0 then Some (id, price, qty) else None)
        buy_orders_json
    in
    let cap_low =
      if a.is_strategy then strat_json |?> "capital_low" |> to_bool_d false else false
    in
    let buy_orders, is_synthetic_buy =
      if buy_orders_parsed <> []
      then buy_orders_parsed, false
      else if a.is_strategy
      then (
        let bp = strat_json |?> "buy_price" |> to_float_d 0.0 in
        let bq =
          strat_json
          |?> "buy_qty"
          |> to_float_d (strat_json |?> "grid_qty" |> to_float_d 0.0)
        in
        let bid_id = strat_json |?> "buy_id" |> to_string_d "buy" in
        if bp > 0.0 then [ bid_id, bp, bq ], cap_low else [], false)
      else [], false
    in
    (* Asset summary card layout: line 1 shows the strategy type, the
       bid/mid/ask prices, and the holding; line 2 shows the last buy/sell
       fills, the quote balance, and the accumulated quantity. *)
    let r1 =
      I.hcat
        [ I.string a_label " Strategy: "
        ; I.string a_cyan stype
        ; I.string a_dim " │ "
        ; I.string a_label "B: "
        ; I.string a_text (if bid > 0.0 then format_price bid else "--")
        ; I.string a_dim " "
        ; I.string a_label "M: "
        ; I.string
            (if mid > 0.0 then a_bright else a_dim)
            (if mid > 0.0 then format_price mid else "--")
        ; I.string a_dim " "
        ; I.string a_label "A: "
        ; I.string a_text (if ask > 0.0 then format_price ask else "--")
        ; I.string a_dim " │ "
        ; I.string a_label "Holding: "
        ; I.string a_bright (format_qty base_bal ^ " " ^ a.asset)
        ; I.string a_dim " ("
        ; I.string a_cyan (format_usd hold_val)
        ; I.string a_dim ")"
        ]
    in
    let last_trade_item =
      if last_buy_fill > 0.0 && last_sell_fill > 0.0
      then
        I.hcat
          [ I.string a_green ("BUY " ^ format_price last_buy_fill)
          ; I.string a_dim " / "
          ; I.string a_red ("SELL " ^ format_price last_sell_fill)
          ]
      else if last_buy_fill > 0.0
      then I.string a_green ("BUY " ^ format_price last_buy_fill)
      else if last_sell_fill > 0.0
      then I.string a_red ("SELL " ^ format_price last_sell_fill)
      else I.string a_dim "--"
    in
    let r2 =
      if a.is_strategy
      then
        I.hcat
          [ I.string a_label " Last BUY/SELL: "
          ; last_trade_item
          ; I.string a_dim " │ "
          ; I.string a_label "Quote Balance: "
          ; I.string a_text (format_usd quote_bal)
          ; I.string a_dim " │ "
          ; I.string a_label "Accum Qty: "
          ; I.string a_cyan (format_qty accum_qty ^ " " ^ a.asset)
          ; I.string
              a_dim
              (if staked_bal > 0.0
               then " (incl " ^ format_qty staked_bal ^ " staked)"
               else "")
          ; I.string a_dim " ("
          ; I.string a_cyan (format_usd accum_val)
          ; I.string a_dim ")"
          ]
      else
        I.hcat
          [ I.string a_label " Last BUY/SELL: "
          ; last_trade_item
          ; I.string a_dim " │ "
          ; I.string a_label "Quote Balance: "
          ; I.string a_text (format_usd quote_bal)
          ]
    in
    let summary_card =
      (* Capital-oracle line: the published decision for this asset, namely
         the ACTIVE/INACTIVE verdict (the oracle-paused state), the sizing,
         and the reason. It is rendered only when a decision exists. *)
      let oracle_line =
        let oracle = if a.is_strategy then a.data |?> "oracle" else `Null in
        match oracle with
        | `Assoc _ ->
          let o_active = oracle |?> "active" |> to_bool_d false in
          let o_buy_qty =
            match oracle |?> "buy_qty" with
            | `Float q -> q
            | _ -> oracle |?> "qty" |> to_float_d 0.0
          in
          let o_sell_qty = oracle |?> "sell_qty" |> to_float_d 0.0 in
          let o_gi = oracle |?> "grid_interval" |> to_float_d 0.0 in
          let o_dsurv = oracle |?> "d_surv" |> to_float_d 0.0 in
          let o_reason = oracle |?> "reason" |> to_string_d "" in
          let status_img =
            if o_active
            then I.string a_green "ACTIVE"
            else I.string a_yellow "INACTIVE"
          in
          let metrics_items =
            [ I.string a_label " Oracle: "
            ; status_img
            ; I.string a_dim " │ "
            ; I.string a_label "GI: "
            ; I.string a_text (if o_gi > 0.0 then Printf.sprintf "%.4f%%" o_gi else "--")
            ; I.string a_dim " │ "
            ; I.string a_label "Buy Qty: "
            ; I.string a_cyan (if o_buy_qty > 0.0 then format_qty o_buy_qty ^ " " ^ a.asset else "--")
            ; I.string a_dim " │ "
            ; I.string a_label "Sell Qty: "
            ; I.string a_cyan (if o_sell_qty > 0.0 then format_qty o_sell_qty ^ " " ^ a.asset else "--")
            ; I.string a_dim " │ "
            ; I.string a_label "D_surv: "
            ; I.string a_bright (Printf.sprintf "%.1f%%" (o_dsurv *. 100.0))
            ]
          in
          let reason_items =
            if o_reason <> ""
            then [ I.string a_dim " │ "; I.string a_label "Reason: "; I.string a_dim o_reason ]
            else []
          in
          Some (I.hcat (metrics_items @ reason_items))
        | _ -> None
      in
      let summary =
        I.vcat
          [ close_row w (I.hcat [ I.string a_border " │"; r1 ])
          ; close_row w (I.hcat [ I.string a_border " │"; r2 ])
          ]
      in
      match oracle_line with
      | Some line ->
        I.vcat [ summary; close_row w (I.hcat [ I.string a_border " │"; line ]) ]
      | None -> summary
    in
    (* Compute the time bounds of the 15 minute history window. *)
    let now = Unix.gettimeofday () in
    let window_start = now -. window_seconds in
    (* Retrieve and prune the 15 minute history points for this asset. *)
    let hist_points =
      match Hashtbl.find_opt price_history a.key with
      | None -> []
      | Some q ->
        while
          (not (Queue.is_empty q)) && now -. (Queue.peek q).timestamp > window_seconds
        do
          ignore (Queue.pop q)
        done;
        Queue.fold (fun acc pt -> pt :: acc) [] q |> List.rev
    in
    let hist_mid_prices = List.map (fun s -> s.mid_p) hist_points in
    let hist_buy_prices = List.concat_map (fun s -> s.buy_ps) hist_points in
    let hist_sell_prices = List.concat_map (fun s -> s.sell_ps) hist_points in
    (* The price scale must cover all buy and sell orders, the fills, and
       the 15 minute historical mid and order prices. *)
    let all_prices =
      (if mid > 0.0 then [ mid ] else [])
      @ (if last_buy_fill > 0.0 then [ last_buy_fill ] else [])
      @ (if last_sell_fill > 0.0 then [ last_sell_fill ] else [])
      @ hist_mid_prices
      @ hist_buy_prices
      @ hist_sell_prices
      @ List.map (fun (_, p, _) -> p) buy_orders
      @ List.map (fun (_, p, _) -> p) sell_orders
    in
    let full_min_p, full_max_p =
      match all_prices with
      | [] -> 100.0, 110.0
      | [ p ] -> p *. 0.95, p *. 1.05
      | prices ->
        let low = List.fold_left min (List.hd prices) prices in
        let high = List.fold_left max (List.hd prices) prices in
        let span = max (high -. low) (low *. 0.01) in
        low -. (span *. 0.06), high +. (span *. 0.06)
    in
    (* Find the nearest buy order (price below mid) and the nearest sell
       order (price above mid). *)
    let nearest_buy_p =
      let buy_ps =
        List.filter_map
          (fun (_, p, _) -> if p < mid && p > 0.0 then Some p else None)
          buy_orders
      in
      match buy_ps with
      | [] -> mid *. 0.985
      | l -> List.fold_left max 0.0 l
    in
    let nearest_sell_p =
      let sell_ps =
        List.filter_map
          (fun (_, p, _) -> if p > mid && p > 0.0 then Some p else None)
          sell_orders
      in
      match sell_ps with
      | [] -> mid *. 1.015
      | l -> List.fold_left min Float.max_float l
    in
    (* Hard cap bounds: at most one order is visible on each side, and
       zooming never goes inside that span. *)
    let cap_span_low = nearest_buy_p *. 0.995 in
    let cap_span_high = nearest_sell_p *. 1.005 in
    (* Determine max_z, the zoom level at which the zoomed bounds reach the
       cap span. *)
    let max_z =
      let rec find_max i =
        if i >= 15
        then 15
        else (
          let f = 1.0 -. (0.65 ** float i) in
          let t_min = full_min_p +. ((cap_span_low -. full_min_p) *. f) in
          let t_max = full_max_p -. ((full_max_p -. cap_span_high) *. f) in
          if
            t_min >= cap_span_low -. (cap_span_low *. 0.0001)
            && t_max <= cap_span_high +. (cap_span_high *. 0.0001)
          then i
          else find_max (i + 1))
      in
      find_max 1
    in
    let raw_z = get_zoom a.key in
    let z = min max_z raw_z in
    if raw_z > max_z then set_zoom a.key max_z;
    let min_p, max_p =
      if z = 0
      then full_min_p, full_max_p
      else (
        let zoom_factor = 1.0 -. (0.65 ** float z) in
        let target_min = full_min_p +. ((cap_span_low -. full_min_p) *. zoom_factor) in
        let target_max = full_max_p -. ((full_max_p -. cap_span_high) *. zoom_factor) in
        let clamped_min = min cap_span_low target_min in
        let clamped_max = max cap_span_high target_max in
        max full_min_p clamped_min, min full_max_p clamped_max)
    in
    (* Compute the available canvas dimensions from the window size. *)
    let ob_col_w = max 24 (w / 5) in
    let chart_area_w = max 35 (w - ob_col_w - 4) in
    let y_axis_w = 14 in
    let pin_col_w = min 32 (max 18 (chart_area_w / 3)) in
    let canvas_w = max 15 (chart_area_w - y_axis_w - pin_col_w - 4) in
    let canvas_h = max 8 (h - 8) in
    let sub_h = canvas_h * 4 in
    let sub_w = canvas_w * 2 in
    (* Extract the L2 order book depth from the market data. *)
    let ob_bids_json = market |?> "bids" |> to_list_d in
    let ob_asks_json = market |?> "asks" |> to_list_d in
    let ob_bids_raw =
      List.filter_map
        (fun s ->
           let p = s |?> "price" |> to_float_d 0.0 in
           let q = s |?> "qty" |> to_float_d 0.0 in
           if p > 0.0 then Some (p, q) else None)
        ob_bids_json
    in
    let ob_asks_raw =
      List.filter_map
        (fun s ->
           let p = s |?> "price" |> to_float_d 0.0 in
           let q = s |?> "qty" |> to_float_d 0.0 in
           if p > 0.0 then Some (p, q) else None)
        ob_asks_json
    in
    (* Fall back to synthesized levels when the order book feed has only a
       single top-of-book level or is missing entirely. *)
    let ob_asks_clean =
      if ob_asks_raw <> []
      then ob_asks_raw
      else if ask > 0.0
      then List.init 5 (fun i -> ask *. (1.0 +. (float i *. 0.001)), 1.0)
      else []
    in
    let ob_bids_clean =
      if ob_bids_raw <> []
      then ob_bids_raw
      else if bid > 0.0
      then List.init 5 (fun i -> bid *. (1.0 -. (float i *. 0.001)), 1.0)
      else []
    in
    (* Extract trade prints when present, for example from Alpaca or other
       trade feeds. *)
    let ob_trades_json = market |?> "trades" |> to_list_d in
    let ob_trades_raw =
      List.filter_map
        (fun s ->
           let p = s |?> "price" |> to_float_d 0.0 in
           let q = s |?> "qty" |> to_float_d 0.0 in
           let ts = s |?> "timestamp" |> to_float_d 0.0 in
           let side = s |?> "side" |> to_string_d "trade" in
           if p > 0.0 then Some (p, q, ts, side) else None)
        ob_trades_json
    in
    let is_alpaca = String.equal (String.lowercase_ascii a.exchange) "alpaca" in
    let show_trade_prints = is_alpaca || ob_trades_raw <> [] in
    (* Prepare the rows that make up the order book sidebar. *)
    let ob_rows =
      Array.make canvas_h (I.string A.(fg c_bg ++ bg c_bg) (String.make ob_col_w ' '))
    in
    let has_fill_footer = canvas_h >= 10 in
    let bot_fill_rows = if has_fill_footer then 1 else 0 in
    let avail_level_rows = max 2 (canvas_h - 2 - bot_fill_rows) in
    let ask_rows_cnt = max 1 (avail_level_rows / 2) in
    let bid_rows_cnt = max 1 (avail_level_rows - ask_rows_cnt) in
    if show_trade_prints
    then (
      ob_rows.(0)
      <- I.string
           A.(fg c_title ++ bg c_bg ++ st bold)
           (pad_right ob_col_w " ══ RECENT TRADES ══");
      if canvas_h > 1
      then
        ob_rows.(1)
        <- I.string
             A.(fg c_dim ++ bg c_bg)
             (pad_right ob_col_w " TIME     PRICE      QTY");
      if ob_trades_raw = []
      then (
        if canvas_h > 2
        then
          ob_rows.(2)
          <- I.string
               A.(fg c_dim ++ bg c_bg)
               (pad_right ob_col_w "  Waiting for trades..."))
      else (
        let avail_rows = canvas_h - 2 in
        let trades_to_show = List.filteri (fun i _ -> i < avail_rows) ob_trades_raw in
        List.iteri
          (fun idx (p, q, ts, side) ->
             let r = 2 + idx in
             if r < canvas_h
             then (
               let time_str =
                 if ts > 0.0
                 then (
                   let tm = Unix.localtime ts in
                   Printf.sprintf
                     "%02d:%02d:%02d"
                     tm.Unix.tm_hour
                     tm.Unix.tm_min
                     tm.Unix.tm_sec)
                 else "--:--:--"
               in
               let p_str = format_price p in
               let q_str = format_qty q in
               let attr =
                 match String.lowercase_ascii side with
                 | "buy" -> A.(fg c_cyan ++ bg c_bg)
                 | "sell" -> A.(fg c_magenta ++ bg c_bg)
                 | _ -> A.(fg c_text ++ bg c_bg)
               in
               let line_txt = Printf.sprintf " %s  %s  %s" time_str p_str q_str in
               ob_rows.(r) <- I.string attr (pad_right ob_col_w line_txt)))
          trades_to_show))
    else (
      (* Sort asks ascending so the best ask sits nearest the mid and
         higher asks stack above it. *)
      let sorted_asks = List.sort (fun (p1, _) (p2, _) -> compare p1 p2) ob_asks_clean in
      let asks_to_show =
        let taken = List.filteri (fun i _ -> i < ask_rows_cnt) sorted_asks in
        List.rev taken
      in
      (* Sort bids descending so the best bid sits nearest the mid and
         lower bids fall below it. *)
      let sorted_bids = List.sort (fun (p1, _) (p2, _) -> compare p2 p1) ob_bids_clean in
      let bids_to_show = List.filteri (fun i _ -> i < bid_rows_cnt) sorted_bids in
      let max_ask_q = List.fold_left (fun acc (_, q) -> max acc q) 0.0 ob_asks_clean in
      let max_bid_q = List.fold_left (fun acc (_, q) -> max acc q) 0.0 ob_bids_clean in
      let is_my_order_level order_p level_p =
        abs_float (order_p -. level_p) < 0.000001
        || String.equal (format_price order_p) (format_price level_p)
      in
      (* Render the order book title row. *)
      ob_rows.(0)
      <- I.string
           A.(fg c_title ++ bg c_bg ++ st bold)
           (pad_right ob_col_w " ══ L2 ORDER BOOK ══");
      (* Render the ask levels with depth bars; levels that match our own
         sell orders are marked. *)
      List.iteri
        (fun idx (p, q) ->
           let r = 1 + idx in
           if r < 1 + ask_rows_cnt && r < canvas_h
           then (
             let has_my_sell =
               List.exists (fun (_, up, _) -> is_my_order_level up p) sell_orders
             in
             let p_str = format_price p in
             let q_str = format_qty q in
             let bar_max_len =
               max
                 3
                 (ob_col_w
                  - String.length p_str
                  - String.length q_str
                  - if has_my_sell then 8 else 4)
             in
             let bar_len =
               if max_ask_q > 0.0
               then max 1 (int_of_float (q /. max_ask_q *. float bar_max_len))
               else 1
             in
             let bar_str = repeat_utf8 "█" bar_len in
             let line_img =
               if has_my_sell
               then (
                 let line_txt = Printf.sprintf " %s %s " p_str q_str in
                 I.hcat
                   [ I.string A.(fg c_magenta ++ bg c_bg ++ st bold) line_txt
                   ; I.string A.(fg c_magenta ++ bg c_bg) bar_str
                   ; I.string A.(fg c_yellow ++ bg c_bg ++ st bold) " ★MY"
                   ])
               else (
                 let line_txt = Printf.sprintf " %s %s " p_str q_str in
                 I.hcat
                   [ I.string A.(fg c_magenta ++ bg c_bg) line_txt
                   ; I.string A.(fg c_magenta ++ bg c_bg) bar_str
                   ])
             in
             ob_rows.(r) <- I.hsnap ~align:`Left ob_col_w line_img))
        asks_to_show;
      (* Render the mid price and spread banner between the asks and bids. *)
      let mid_row_idx = 1 + ask_rows_cnt in
      if mid_row_idx < canvas_h
      then (
        let spread = if ask > 0.0 && bid > 0.0 then max 0.0 (ask -. bid) else 0.0 in
        let sprd_str =
          if spread > 0.0 then Printf.sprintf " (±%s)" (format_price spread) else ""
        in
        let mid_str = Printf.sprintf "▶ MID %s%s" (format_price mid) sprd_str in
        ob_rows.(mid_row_idx)
        <- I.string A.(fg c_bg ++ bg c_green ++ st bold) (pad_right ob_col_w mid_str));
      (* Render the bid levels with depth bars; levels that match our own
         buy orders are marked. *)
      List.iteri
        (fun idx (p, q) ->
           let r = mid_row_idx + 1 + idx in
           if r < canvas_h - bot_fill_rows
           then (
             let has_my_buy =
               List.exists (fun (_, up, _) -> is_my_order_level up p) buy_orders
             in
             let p_str = format_price p in
             let q_str = format_qty q in
             let bar_max_len =
               max
                 3
                 (ob_col_w
                  - String.length p_str
                  - String.length q_str
                  - if has_my_buy then 8 else 4)
             in
             let bar_len =
               if max_bid_q > 0.0
               then max 1 (int_of_float (q /. max_bid_q *. float bar_max_len))
               else 1
             in
             let bar_str = repeat_utf8 "█" bar_len in
             let line_img =
               if has_my_buy
               then (
                 let line_txt = Printf.sprintf " %s %s " p_str q_str in
                 I.hcat
                   [ I.string A.(fg c_cyan ++ bg c_bg ++ st bold) line_txt
                   ; I.string A.(fg c_cyan ++ bg c_bg) bar_str
                   ; I.string A.(fg c_yellow ++ bg c_bg ++ st bold) " ★MY"
                   ])
               else (
                 let line_txt = Printf.sprintf " %s %s " p_str q_str in
                 I.hcat
                   [ I.string A.(fg c_cyan ++ bg c_bg) line_txt
                   ; I.string A.(fg c_cyan ++ bg c_bg) bar_str
                   ])
             in
             ob_rows.(r) <- I.hsnap ~align:`Left ob_col_w line_img))
        bids_to_show);
    (* Render the most recent fills as a footer footprint line. *)
    if has_fill_footer
    then (
      let fill_idx = canvas_h - 1 in
      let fill_str =
        if last_buy_fill > 0.0 && last_sell_fill > 0.0
        then
          Printf.sprintf
            " B:%s S:%s"
            (format_price last_buy_fill)
            (format_price last_sell_fill)
        else if last_buy_fill > 0.0
        then Printf.sprintf " L.BUY: %s" (format_price last_buy_fill)
        else if last_sell_fill > 0.0
        then Printf.sprintf " L.SELL: %s" (format_price last_sell_fill)
        else " NO RECENT FILLS"
      in
      ob_rows.(fill_idx) <- I.string A.(fg c_dim ++ bg c_bg) (pad_right ob_col_w fill_str));
    let price_to_sub_y p =
      let ratio = (max_p -. p) /. max 0.000001 (max_p -. min_p) in
      let sy = int_of_float (ratio *. float (sub_h - 1)) in
      max 0 (min (sub_h - 1) sy)
    in
    let price_to_row p =
      let sy = price_to_sub_y p in
      min (canvas_h - 1) (sy / 4)
    in
    let mid_row = price_to_row mid in
    (* Group the sell orders by their exact canvas row. *)
    let sell_by_row = Hashtbl.create 16 in
    List.iter
      (fun (id, p, q) ->
         let r = price_to_row p in
         let existing =
           try Hashtbl.find sell_by_row r with
           | Not_found -> []
         in
         Hashtbl.replace sell_by_row r ((id, p, q) :: existing))
      sell_orders;
    (* Group the buy orders by their exact canvas row. *)
    let buy_by_row = Hashtbl.create 16 in
    List.iter
      (fun (id, p, q) ->
         let r = price_to_row p in
         let existing =
           try Hashtbl.find buy_by_row r with
           | Not_found -> []
         in
         Hashtbl.replace buy_by_row r ((id, p, q) :: existing))
      buy_orders;
    (* Subpixel line drawing over the 2x4 Braille grid. *)
    let draw_line grid x0 y0 x1 y1 =
      let dx = abs (x1 - x0) in
      let dy = abs (y1 - y0) in
      let sx = if x0 < x1 then 1 else -1 in
      let sy = if y0 < y1 then 1 else -1 in
      let err = ref (dx - dy) in
      let x = ref x0 in
      let y = ref y0 in
      let loop = ref true in
      while !loop do
        if !x >= 0 && !x < sub_w && !y >= 0 && !y < sub_h then grid.(!x).(!y) <- true;
        if !x = x1 && !y = y1
        then loop := false
        else (
          let e2 = 2 * !err in
          if e2 > -dy
          then (
            err := !err - dy;
            x := !x + sx);
          if e2 < dx
          then (
            err := !err + dx;
            y := !y + sy))
      done
    in
    (* Plot the continuous mid price and order level curves across the
       15 minute timeline. *)
    let mid_grid = Array.make_matrix sub_w sub_h false in
    let buy_grid = Array.make_matrix sub_w sub_h false in
    let sell_grid = Array.make_matrix sub_w sub_h false in
    let mid_sub_y = Array.make sub_w (-1) in
    (* Connect pin traces across columns using one-to-one greedy matching
       within a small vertical threshold. *)
    let connect_pin_traces grid sub_y_list =
      let max_delta = 5 in
      for sx = 0 to sub_w - 2 do
        let sys0 = sub_y_list.(sx) in
        let sys1 = sub_y_list.(sx + 1) in
        match sys0, sys1 with
        | [], [] -> ()
        | l0, [] -> List.iter (fun sy -> draw_line grid sx sy sx sy) l0
        | [], l1 -> List.iter (fun sy -> draw_line grid (sx + 1) sy (sx + 1) sy) l1
        | l0, l1 ->
          let l0_idx = List.mapi (fun i sy -> i, sy) l0 in
          let l1_idx = List.mapi (fun j sy -> j, sy) l1 in
          let candidates =
            List.concat_map
              (fun (i, sy0) ->
                 List.filter_map
                   (fun (j, sy1) ->
                      let d = abs (sy0 - sy1) in
                      if d <= max_delta then Some (i, sy0, j, sy1, d) else None)
                   l1_idx)
              l0_idx
          in
          let sorted =
            List.sort (fun (_, _, _, _, d1) (_, _, _, _, d2) -> compare d1 d2) candidates
          in
          let used0 = Hashtbl.create 8 in
          let used1 = Hashtbl.create 8 in
          List.iter
            (fun (i, sy0, j, sy1, _) ->
               if not (Hashtbl.mem used0 i || Hashtbl.mem used1 j)
               then (
                 Hashtbl.add used0 i true;
                 Hashtbl.add used1 j true;
                 draw_line grid sx sy0 (sx + 1) sy1))
            sorted;
          List.iter
            (fun (i, sy0) ->
               if not (Hashtbl.mem used0 i) then draw_line grid sx sy0 sx sy0)
            l0_idx;
          List.iter
            (fun (j, sy1) ->
               if not (Hashtbl.mem used1 j) then draw_line grid (sx + 1) sy1 (sx + 1) sy1)
            l1_idx
      done;
      if sub_w > 0
      then
        List.iter
          (fun sy -> draw_line grid (sub_w - 1) sy (sub_w - 1) sy)
          sub_y_list.(sub_w - 1)
    in
    (match hist_points with
     | [] ->
       let sy_mid = price_to_sub_y mid in
       draw_line mid_grid 0 sy_mid (sub_w - 1) sy_mid;
       for sx = 0 to sub_w - 1 do
         mid_sub_y.(sx) <- sy_mid
       done;
       List.iter
         (fun (_, p, _) ->
            let sy = price_to_sub_y p in
            draw_line buy_grid 0 sy (sub_w - 1) sy)
         buy_orders;
       List.iter
         (fun (_, p, _) ->
            let sy = price_to_sub_y p in
            draw_line sell_grid 0 sy (sub_w - 1) sy)
         sell_orders
     | pts ->
       let t_earliest = (List.hd pts).timestamp in
       let rec mid_at_t target_t = function
         | [] -> mid
         | [ s0 ] -> s0.mid_p
         | s0 :: s1 :: rest ->
           if target_t >= s0.timestamp && target_t <= s1.timestamp
           then (
             let ratio =
               (target_t -. s0.timestamp) /. max 0.0001 (s1.timestamp -. s0.timestamp)
             in
             s0.mid_p +. ((s1.mid_p -. s0.mid_p) *. ratio))
           else mid_at_t target_t (s1 :: rest)
       in
       let rec snap_at_t target_t = function
         | [] -> None
         | [ s0 ] -> Some s0
         | s0 :: (s1 :: _ as rest) ->
           if target_t >= s0.timestamp && target_t < s1.timestamp
           then Some s0
           else if target_t >= s1.timestamp
           then snap_at_t target_t rest
           else Some s0
       in
       let buy_sub_y_list = Array.make sub_w [] in
       let sell_sub_y_list = Array.make sub_w [] in
       for sx = 0 to sub_w - 1 do
         let ratio_x = float sx /. float (max 1 (sub_w - 1)) in
         let target_t = window_start +. (ratio_x *. window_seconds) in
         if target_t >= t_earliest
         then (
           let p = mid_at_t target_t pts in
           mid_sub_y.(sx) <- price_to_sub_y p;
           match snap_at_t target_t pts with
           | Some snap ->
             buy_sub_y_list.(sx) <- List.map price_to_sub_y snap.buy_ps;
             sell_sub_y_list.(sx) <- List.map price_to_sub_y snap.sell_ps
           | None -> ())
       done;
       (* Interpolate the mid price curve across consecutive columns. *)
       for sx = 0 to sub_w - 2 do
         let sy0 = mid_sub_y.(sx) in
         let sy1 = mid_sub_y.(sx + 1) in
         if sy0 >= 0 && sy1 >= 0
         then draw_line mid_grid sx sy0 (sx + 1) sy1
         else if sy0 >= 0
         then draw_line mid_grid sx sy0 sx sy0
         else if sy1 >= 0
         then draw_line mid_grid (sx + 1) sy1 (sx + 1) sy1
       done;
       if mid_sub_y.(sub_w - 1) >= 0
       then (
         let sy_last = mid_sub_y.(sub_w - 1) in
         draw_line mid_grid (sub_w - 1) sy_last (sub_w - 1) sy_last;
         (* Interpolate the buy and sell order level pin traces across
             columns. *)
         connect_pin_traces buy_grid buy_sub_y_list;
         connect_pin_traces sell_grid sell_sub_y_list));
    let buy_row_opt =
      if buy_orders <> []
      then (
        let r_best =
          Hashtbl.fold
            (fun r _ acc ->
               match acc with
               | None -> Some r
               | Some r_prev -> Some (min r r_prev))
            buy_by_row
            None
        in
        r_best)
      else if is_synthetic_buy
      then (
        match buy_orders_parsed with
        | [] ->
          let bp = strat_json |?> "buy_price" |> to_float_d 0.0 in
          if bp > 0.0 then Some (price_to_row bp) else None
        | _ -> None)
      else None
    in
    (* Compute which rows get a Y-axis price tick so labels stay readable
       and never crowd the mid or order rows. *)
    let show_y_label = Array.make canvas_h false in
    let label_prices = Array.make canvas_h 0.0 in
    for r = 0 to canvas_h - 1 do
      label_prices.(r)
      <- max_p -. (float r /. float (max 1 (canvas_h - 1)) *. (max_p -. min_p))
    done;
    show_y_label.(mid_row) <- true;
    (match buy_row_opt with
     | Some br -> show_y_label.(br) <- true
     | None -> ());
    Hashtbl.iter (fun r _ -> show_y_label.(r) <- true) buy_by_row;
    Hashtbl.iter (fun r _ -> show_y_label.(r) <- true) sell_by_row;
    for r = 0 to canvas_h - 1 do
      if (not show_y_label.(r)) && (r = 0 || r = canvas_h - 1 || r mod 4 = 0)
      then (
        let has_adj =
          (r > 0 && show_y_label.(r - 1)) || (r < canvas_h - 1 && show_y_label.(r + 1))
        in
        if not has_adj then show_y_label.(r) <- true)
    done;
    (* Render each row of the 2D Braille canvas. *)
    let canvas_rows =
      List.init canvas_h (fun r ->
        let is_grid_line = r mod 3 = 0 || r = canvas_h - 1 in
        let is_mid_l = r = mid_row in
        let s_orders_l =
          try Hashtbl.find sell_by_row r with
          | Not_found -> []
        in
        let b_orders_l =
          try Hashtbl.find buy_by_row r with
          | Not_found -> []
        in
        let is_sell_l = s_orders_l <> [] in
        let is_buy_l = Some r = buy_row_opt || b_orders_l <> [] in
        let y_label_str =
          if show_y_label.(r)
          then pad_left 11 (format_price label_prices.(r))
          else "           "
        in
        let y_attr =
          if is_mid_l
          then A.(fg c_green ++ bg c_bg ++ st bold)
          else if is_buy_l
          then
            if is_synthetic_buy
            then A.(fg c_yellow ++ bg c_bg ++ st bold)
            else A.(fg c_cyan ++ bg c_bg ++ st bold)
          else if is_sell_l
          then A.(fg c_magenta ++ bg c_bg ++ st bold)
          else if is_grid_line
          then A.(fg c_title ++ bg c_bg)
          else A.(fg c_dim ++ bg c_bg)
        in
        (* Render the price time series curve cells for this row. *)
        let cells =
          List.init canvas_w (fun c ->
            let sx0 = c * 2 in
            let sx1 = (c * 2) + 1 in
            let mid_sy0 = mid_sub_y.(sx0) in
            let mid_sy1 = mid_sub_y.(sx1) in
            let cell_sy_start = r * 4 in
            let mid_mask = ref 0 in
            let buy_mask = ref 0 in
            let sell_mask = ref 0 in
            for sub_y = 0 to 3 do
              let current_sy = cell_sy_start + sub_y in
              if current_sy >= 0 && current_sy < sub_h
              then (
                if mid_grid.(sx0).(current_sy)
                then mid_mask := !mid_mask lor braille_bit 0 sub_y;
                if mid_grid.(sx1).(current_sy)
                then mid_mask := !mid_mask lor braille_bit 1 sub_y;
                if buy_grid.(sx0).(current_sy)
                then buy_mask := !buy_mask lor braille_bit 0 sub_y;
                if buy_grid.(sx1).(current_sy)
                then buy_mask := !buy_mask lor braille_bit 1 sub_y;
                if sell_grid.(sx0).(current_sy)
                then sell_mask := !sell_mask lor braille_bit 0 sub_y;
                if sell_grid.(sx1).(current_sy)
                then sell_mask := !sell_mask lor braille_bit 1 sub_y)
            done;
            let valid_sy =
              if mid_sy0 >= 0 && mid_sy1 >= 0
              then min mid_sy0 mid_sy1
              else if mid_sy0 >= 0
              then mid_sy0
              else mid_sy1
            in
            let is_in_liquid = valid_sy >= 0 && cell_sy_start + 2 > valid_sy in
            let fill_dist =
              if is_in_liquid
              then float (cell_sy_start - valid_sy) /. float sub_h
              else 0.0
            in
            let fill_rgb =
              color_blend (75, 62, 32) (26, 27, 38) (min 1.0 (fill_dist *. 1.5))
            in
            let bg_attr = if is_in_liquid then A.bg fill_rgb else A.bg c_bg in
            let combined_mask = !mid_mask lor !buy_mask lor !sell_mask in
            if combined_mask <> 0
            then (
              let str = braille_to_utf8 combined_mask in
              let fg_color =
                if !mid_mask <> 0 && !buy_mask <> 0
                then color_blend (158, 206, 106) (125, 207, 255) 0.5
                else if !mid_mask <> 0 && !sell_mask <> 0
                then color_blend (158, 206, 106) (226, 104, 160) 0.5
                else if !buy_mask <> 0 && !sell_mask <> 0
                then color_blend (125, 207, 255) (226, 104, 160) 0.5
                else if !mid_mask <> 0
                then c_green
                else if !buy_mask <> 0
                then if is_synthetic_buy then c_yellow else c_cyan
                else if !sell_mask <> 0
                then c_magenta
                else c_bright
              in
              I.string A.(fg fg_color ++ bg_attr ++ st bold) str)
            else if is_grid_line && c mod 8 = 0
            then (
              let g_attr =
                if is_in_liquid
                then A.(fg fill_rgb ++ bg c_bg)
                else A.(fg c_border ++ bg c_bg)
              in
              I.string g_attr "┼")
            else if is_grid_line
            then (
              let g_attr =
                if is_in_liquid
                then A.(fg fill_rgb ++ bg c_bg)
                else A.(fg c_border ++ bg c_bg)
              in
              I.string g_attr "╌")
            else if c mod 8 = 0
            then (
              let g_attr =
                if is_in_liquid
                then A.(fg fill_rgb ++ bg c_bg)
                else A.(fg c_border ++ bg c_bg)
              in
              I.string g_attr "┊")
            else if is_in_liquid
            then I.string A.(fg fill_rgb ++ bg c_bg) "░"
            else I.string A.(fg c_bg ++ bg c_bg) " ")
        in
        let chart_line_img = I.hcat cells in
        (* Render the right-docked order target pins with their badges. *)
        let right_pin_img =
          if is_mid_l
          then (
            let tracer = repeat_utf8 "╌" 4 in
            let mid_badge = Printf.sprintf " ◀ LIVE NOW %s " (format_price mid) in
            I.hcat
              [ I.string A.(fg c_green ++ bg c_bg) tracer
              ; I.string
                  A.(fg c_bright ++ bg c_green ++ st bold)
                  (pad_right (pin_col_w - 4) mid_badge)
              ])
          else if is_buy_l
          then (
            let orders = if b_orders_l <> [] then b_orders_l else buy_orders in
            let count = List.length orders in
            let total_q = List.fold_left (fun acc (_, _, q) -> acc +. q) 0.0 orders in
            let avg_p =
              if count > 0
              then
                List.fold_left (fun acc (_, p, _) -> acc +. p) 0.0 orders /. float count
              else mid
            in
            let dist_pct = if mid > 0.0 then (avg_p -. mid) /. mid *. 100.0 else 0.0 in
            let count_str = if count > 1 then Printf.sprintf "[%dx] " count else "" in
            let tracer = repeat_utf8 "╌" 4 in
            if is_synthetic_buy
            then (
              let buy_badge =
                Printf.sprintf
                  " ◇ EST BUY %s%s %s [%s] "
                  count_str
                  (format_qty total_q)
                  a.asset
                  (format_pct dist_pct)
              in
              I.hcat
                [ I.string A.(fg c_yellow ++ bg c_bg) tracer
                ; I.string
                    A.(fg c_bg ++ bg c_yellow ++ st bold)
                    (pad_right (pin_col_w - 4) buy_badge)
                ])
            else (
              let buy_badge =
                Printf.sprintf
                  " ◆ BUY %s%s %s [%s] "
                  count_str
                  (format_qty total_q)
                  a.asset
                  (format_pct dist_pct)
              in
              I.hcat
                [ I.string A.(fg c_cyan ++ bg c_bg) tracer
                ; I.string
                    A.(fg c_bg ++ bg c_cyan ++ st bold)
                    (pad_right (pin_col_w - 4) buy_badge)
                ]))
          else if is_sell_l
          then (
            let count = List.length s_orders_l in
            let total_q = List.fold_left (fun acc (_, _, q) -> acc +. q) 0.0 s_orders_l in
            let avg_p =
              List.fold_left (fun acc (_, p, _) -> acc +. p) 0.0 s_orders_l /. float count
            in
            let dist_pct = if mid > 0.0 then (avg_p -. mid) /. mid *. 100.0 else 0.0 in
            let count_str = if count > 1 then Printf.sprintf "[%dx] " count else "" in
            let sell_badge =
              Printf.sprintf
                " ◆ SELL %s%s %s [%s] "
                count_str
                (format_qty total_q)
                a.asset
                (format_pct dist_pct)
            in
            let tracer = repeat_utf8 "╌" 4 in
            I.hcat
              [ I.string A.(fg c_magenta ++ bg c_bg) tracer
              ; I.string
                  A.(fg c_bg ++ bg c_magenta ++ st bold)
                  (pad_right (pin_col_w - 4) sell_badge)
              ])
          else I.string A.(fg c_bg ++ bg c_bg) (String.make pin_col_w ' ')
        in
        close_row
          w
          (I.hcat
             [ I.string a_border " │"
             ; ob_rows.(r)
             ; I.string a_border " │ "
             ; I.string y_attr y_label_str
             ; I.string a_border " │ "
             ; chart_line_img
             ; right_pin_img
             ]))
    in
    (* Render the X-axis tick bar and the realtime timeline labels. *)
    let x_axis_ticks =
      let tick_bar = repeat_utf8 "─" (canvas_w + pin_col_w) in
      close_row
        w
        (I.hcat
           [ I.string a_border " │"
           ; I.string
               A.(fg c_title ++ bg c_bg ++ st bold)
               (pad_right ob_col_w "  EXCHANGE DEPTH  ")
           ; I.string a_border " │ "
           ; I.string A.(fg c_border ++ bg c_bg) " REALTIME ──"
           ; I.string a_border " ┴─"
           ; I.string A.(fg c_border ++ bg c_bg) tick_bar
           ])
    in
    let x_axis_labels =
      let now = Unix.gettimeofday () in
      let fmt_time tm_float =
        let tm = Unix.localtime tm_float in
        Printf.sprintf "%02d:%02d:%02d" tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec
      in
      let t_15m = now -. 900.0 in
      let t_10m = now -. 600.0 in
      let t_5m = now -. 300.0 in
      let t_now = now in
      let step_w = canvas_w / 4 in
      let lbl0 = pad_right step_w (fmt_time t_15m) in
      let lbl1 = pad_right step_w (fmt_time t_10m) in
      let lbl2 = pad_right step_w (fmt_time t_5m) in
      let lbl3 = pad_right step_w (fmt_time t_now) in
      let time_str = lbl0 ^ lbl1 ^ lbl2 ^ lbl3 in
      let pin_title = pad_left pin_col_w "ORDER TARGET PINS ──▶" in
      close_row
        w
        (I.hcat
           [ I.string a_border " │"
           ; I.string a_dim (pad_right ob_col_w "  L2 BOOK FEED    ")
           ; I.string a_border " │ "
           ; I.string a_dim "             "
           ; I.string a_border "   "
           ; I.string A.(fg c_title ++ bg c_bg ++ st bold) (pad_right canvas_w time_str)
           ; I.string A.(fg c_accent ++ bg c_bg ++ st bold) pin_title
           ])
    in
    let num_sells = List.length sell_orders in
    let num_buys = List.length buy_orders in
    let buy_summary_str =
      if is_synthetic_buy then "1 Est Buy" else Printf.sprintf "%d Buy" num_buys
    in
    let orders_summary =
      Printf.sprintf "(%d Sell, %s pending)" num_sells buy_summary_str
    in
    let graph_title =
      section_title w ("LIVE EXCHANGE ORDERBOOK & 15M PRICE HISTORY " ^ orders_summary)
    in
    let zoom_tag = if z > 0 then Printf.sprintf " [Zoom: %dx] " z else "" in
    let nav_footer =
      close_row
        w
        (I.hcat
           [ I.string a_border " │ "
           ; I.string A.(fg c_cyan ++ bg c_bg ++ st bold) "[↑/↓ or ←/→] "
           ; I.string A.(fg c_text ++ bg c_bg) "Prev/Next Asset    "
           ; I.string A.(fg c_yellow ++ bg c_bg ++ st bold) "[+/= / -] "
           ; I.string A.(fg c_text ++ bg c_bg) ("Zoom In/Out" ^ zoom_tag ^ "    ")
           ; I.string A.(fg c_accent ++ bg c_bg ++ st bold) "[Esc/b] "
           ; I.string A.(fg c_text ++ bg c_bg) "Return to Dashboard    "
           ; I.string A.(fg c_yellow ++ bg c_bg ++ st bold) "[q] "
           ; I.string A.(fg c_text ++ bg c_bg) "Quit"
           ])
    in
    I.vcat
      [ header_bar
      ; summary_card
      ; graph_title
      ; I.vcat canvas_rows
      ; x_axis_ticks
      ; x_axis_labels
      ; section_footer w
      ; nav_footer
      ]
;;
