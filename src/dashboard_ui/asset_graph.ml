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

(** Convert 8-bit Braille dot bitmask to a 3-byte UTF-8 string. *)
let braille_to_utf8 mask =
  let code = 0x2800 lor (mask land 0xFF) in
  let b1 = Char.chr (0xE0 lor (code lsr 12)) in
  let b2 = Char.chr (0x80 lor ((code lsr 6) land 0x3F)) in
  let b3 = Char.chr (0x80 lor (code land 0x3F)) in
  String.init 3 (function 0 -> b1 | 1 -> b2 | _ -> b3)

(** Returns Braille bit flag for subpixel (sub_x, sub_y) where sub_x ∈ {0,1}, sub_y ∈ {0..3} top-down. *)
let braille_bit sub_x sub_y =
  match (sub_x, sub_y) with
  | (0, 0) -> 0x01 | (0, 1) -> 0x02 | (0, 2) -> 0x04 | (0, 3) -> 0x40
  | (1, 0) -> 0x08 | (1, 1) -> 0x10 | (1, 2) -> 0x20 | (1, 3) -> 0x80
  | _ -> 0

(** Fixed 15-minute time window in seconds (900s). *)
let window_seconds = 900.0

(** Rolling price history buffer per asset key.
    Tracks live mid prices across the 15-minute window for all assets. *)
let price_history : (string, (float * float) Queue.t) Hashtbl.t = Hashtbl.create 32

(** Global zoom level per asset key.
    0 = Full view showing all orders.
    Higher values = zoomed in around mid price (capped at 1 order on each side). *)
let zoom_levels : (string, int) Hashtbl.t = Hashtbl.create 16

let get_zoom asset_key =
  try Hashtbl.find zoom_levels asset_key with Not_found -> 0

let set_zoom asset_key z =
  Hashtbl.replace zoom_levels asset_key (max 0 z)

let zoom_in asset_key =
  set_zoom asset_key (get_zoom asset_key + 1)

let zoom_out asset_key =
  set_zoom asset_key (max 0 (get_zoom asset_key - 1))

let record_all_prices json =
  let assets = Holdings.get_selectable_assets json in
  let now = Unix.gettimeofday () in
  List.iter (fun (a : Holdings.selectable_asset) ->
    let market = if a.is_strategy then a.data |?> "market" else a.data in
    let bid = market |?> "bid" |> to_float_d 0.0 in
    let ask = market |?> "ask" |> to_float_d 0.0 in
    let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else max bid ask in
    if mid > 0.0 then begin
      let q = match Hashtbl.find_opt price_history a.key with
        | Some q -> q
        | None ->
            let q = Queue.create () in
            Hashtbl.add price_history a.key q;
            q
      in
      (* Prune entries older than 15 minutes *)
      while (not (Queue.is_empty q)) && (now -. fst (Queue.peek q) > window_seconds) do
        ignore (Queue.pop q)
      done;
      let should_push =
        if Queue.is_empty q then true
        else
          let (last_t, last_p) = Queue.fold (fun _ item -> item) (0.0, 0.0) q in
          now -. last_t >= 1.0 || abs_float (mid -. last_p) > 0.000001
      in
      if should_push then Queue.push (now, mid) q
    end
  ) assets

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
      let title_str = Printf.sprintf "15-MIN PRICE TIME SERIES & DOCKED ORDERS: %s (%s)" a.symbol exch_tag in
      let header_bar = section_title w title_str in

      (* Extract market data *)
      let market = if a.is_strategy then a.data |?> "market" else a.data in
      let bid = market |?> "bid" |> to_float_d 0.0 in
      let ask = market |?> "ask" |> to_float_d 0.0 in
      let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else (max bid ask) in
      let base_bal = if a.is_strategy then market |?> "base_balance" |> to_float_d 0.0 else a.data |?> "balance" |> to_float_d 0.0 in
      let quote_bal = market |?> "quote_balance" |> to_float_d 0.0 in
      let hold_val = base_bal *. mid in

      (* Record live mid price into time series history buffer *)
      record_all_prices json;

      (* Extract strategy & order details *)
      let strat_json = if a.is_strategy then a.data |?> "strategy" else `Null in
      let stype = if a.is_strategy then strat_json |?> "type" |> to_string_d "Grid" else "Balance" in
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

      (* Calculate accumulated holding and value *)
      let pending_sell_qty = List.fold_left (fun acc (_, _, q) -> acc +. q) 0.0 sell_orders in
      let accum_qty = Float.max 0.0 (base_bal -. pending_sell_qty) in
      let accum_val = accum_qty *. mid in

      (* Collect buy orders and differentiate real exchange orders vs synthetic strategy targets *)
      let buy_orders_json =
        if a.is_strategy then strat_json |?> "buy_orders" |> to_list_d
        else a.data |?> "buy_orders" |> to_list_d
      in
      let buy_orders_parsed = List.filter_map (fun s ->
        let id = s |?> "id" |> to_string_d "?" in
        let price = s |?> "price" |> to_float_d 0.0 in
        let qty = s |?> "qty" |> to_float_d 0.0 in
        if price > 0.0 && qty > 0.0 then Some (id, price, qty) else None
      ) buy_orders_json in

      let cap_low = if a.is_strategy then strat_json |?> "capital_low" |> to_bool_d false else false in

      let buy_orders, is_synthetic_buy =
        if buy_orders_parsed <> [] then
          (buy_orders_parsed, false)
        else if a.is_strategy then
          let bp = strat_json |?> "buy_price" |> to_float_d 0.0 in
          let bq = strat_json |?> "buy_qty" |> to_float_d (strat_json |?> "grid_qty" |> to_float_d 0.0) in
          let bid_id = strat_json |?> "buy_id" |> to_string_d "buy" in
          if bp > 0.0 then ([(bid_id, bp, bq)], cap_low) else ([], false)
        else ([], false)
      in

      (* Asset Summary Card *)
      let r1 = I.hcat [
        I.string a_label " STRATEGY: "; I.string a_cyan (pad_right 7 stype);
        I.string a_dim "│";
        I.string a_label " MID: "; I.string (if mid > 0.0 then a_bright else a_dim) (pad_right 11 (if mid > 0.0 then format_price mid else "--"));
        I.string a_dim "│";
        I.string a_label " BID/ASK: "; I.string a_text (Printf.sprintf "%s / %s" (if bid > 0.0 then format_price bid else "--") (if ask > 0.0 then format_price ask else "--"));
        I.string a_dim " │ ";
        I.string a_label "HOLDING: "; I.string a_bright (format_qty base_bal ^ " " ^ a.asset);
        I.string a_dim " ("; I.string a_cyan (format_usd hold_val); I.string a_dim ")";
      ] in
      let r2 =
        if a.is_strategy then
          I.hcat [
            I.string a_label " ACCUM QTY: "; I.string a_cyan (format_qty accum_qty ^ " " ^ a.asset);
            I.string a_dim " ("; I.string a_cyan (format_usd accum_val); I.string a_dim ")";
            I.string a_dim "  │  ";
            I.string a_label "QUOTE BAL: "; I.string a_text (format_usd quote_bal);
            (if last_buy_fill > 0.0 then I.hcat [ I.string a_dim "  │  "; I.string a_label "LAST BUY: "; I.string a_green (format_price last_buy_fill) ] else I.empty);
            (if last_sell_fill > 0.0 then I.hcat [ I.string a_dim "  │  "; I.string a_label "LAST SELL: "; I.string a_red (format_price last_sell_fill) ] else I.empty);
          ]
        else
          I.hcat [
            I.string a_label " QUOTE BAL: "; I.string a_text (format_usd quote_bal);
          ]
      in
      let summary_card = I.vcat [
        close_row w (I.hcat [ I.string a_border " │"; r1 ]);
        close_row w (I.hcat [ I.string a_border " │"; r2 ]);
      ] in

      (* 15-minute window time bounds *)
      let now = Unix.gettimeofday () in
      let window_start = now -. window_seconds in

      (* Retrieve and prune 15-minute history points for this asset *)
      let hist_points =
        match Hashtbl.find_opt price_history a.key with
        | None -> []
        | Some q ->
            while (not (Queue.is_empty q)) && (now -. fst (Queue.peek q) > window_seconds) do
              ignore (Queue.pop q)
            done;
            Queue.fold (fun acc pt -> pt :: acc) [] q |> List.rev
      in
      let hist_prices = List.map snd hist_points in

      (* Scale to show ALL buy and sell orders + fills + 15m historical mid prices *)
      let all_prices =
        (if mid > 0.0 then [mid] else []) @
        (if last_buy_fill > 0.0 then [last_buy_fill] else []) @
        (if last_sell_fill > 0.0 then [last_sell_fill] else []) @
        hist_prices @
        List.map (fun (_, p, _) -> p) buy_orders @
        List.map (fun (_, p, _) -> p) sell_orders
      in

      let full_min_p, full_max_p =
        match all_prices with
        | [] -> (100.0, 110.0)
        | [p] -> (p *. 0.95, p *. 1.05)
        | prices ->
            let low = List.fold_left min (List.hd prices) prices in
            let high = List.fold_left max (List.hd prices) prices in
            let span = max (high -. low) (low *. 0.01) in
            (low -. (span *. 0.06), high +. (span *. 0.06))
      in

      (* Nearest buy order (price < mid) and nearest sell order (price > mid) *)
      let nearest_buy_p =
        let buy_ps = List.filter_map (fun (_, p, _) -> if p < mid && p > 0.0 then Some p else None) buy_orders in
        match buy_ps with
        | [] -> mid *. 0.985
        | l -> List.fold_left max 0.0 l
      in

      let nearest_sell_p =
        let sell_ps = List.filter_map (fun (_, p, _) -> if p > mid && p > 0.0 then Some p else None) sell_orders in
        match sell_ps with
        | [] -> mid *. 1.015
        | l -> List.fold_left min Float.max_float l
      in

      (* Hard cap bounds: showing 1 order on each side, no zooming inside that *)
      let cap_span_low = nearest_buy_p *. 0.995 in
      let cap_span_high = nearest_sell_p *. 1.005 in

      (* Determine max_z where zoom bounds hit cap_span_low & cap_span_high *)
      let max_z =
        let rec find_max i =
          if i >= 15 then 15
          else
            let f = 1.0 -. (0.65 ** float i) in
            let t_min = full_min_p +. (cap_span_low -. full_min_p) *. f in
            let t_max = full_max_p -. (full_max_p -. cap_span_high) *. f in
            if t_min >= cap_span_low -. (cap_span_low *. 0.0001) &&
               t_max <= cap_span_high +. (cap_span_high *. 0.0001) then i
            else find_max (i + 1)
        in
        find_max 1
      in

      let raw_z = get_zoom a.key in
      let z = min max_z raw_z in
      if raw_z > max_z then set_zoom a.key max_z;

      let min_p, max_p =
        if z = 0 then (full_min_p, full_max_p)
        else
          let zoom_factor = 1.0 -. (0.65 ** float z) in
          let target_min = full_min_p +. (cap_span_low -. full_min_p) *. zoom_factor in
          let target_max = full_max_p -. (full_max_p -. cap_span_high) *. zoom_factor in
          let clamped_min = min cap_span_low target_min in
          let clamped_max = max cap_span_high target_max in
          (max full_min_p clamped_min, min full_max_p clamped_max)
      in

      (* Available canvas dimensions *)
      let y_axis_w = 14 in
      let pin_col_w = min 36 (max 26 (w / 3)) in
      let canvas_w = max 20 (w - y_axis_w - pin_col_w - 4) in
      let canvas_h = max 8 (h - 8) in
      let sub_h = canvas_h * 4 in
      let sub_w = canvas_w * 2 in

      let price_to_sub_y p =
        let ratio = (max_p -. p) /. (max 0.000001 (max_p -. min_p)) in
        let sy = int_of_float (ratio *. float (sub_h - 1)) in
        max 0 (min (sub_h - 1) sy)
      in

      let price_to_row p =
        let sy = price_to_sub_y p in
        min (canvas_h - 1) (sy / 4)
      in

      (* Plot live price dots across 15m timeline. Subpixel Y = -1 for uncollected past *)
      let curve_sub_y = Array.make sub_w (-1) in

      (match hist_points with
       | [] ->
           (* If no points yet, plot dot at far right (LIVE NOW) *)
           curve_sub_y.(sub_w - 1) <- price_to_sub_y mid
       | pts ->
           let t_earliest = fst (List.hd pts) in

           let rec price_at_t target_t = function
             | [] -> mid
             | [(_, p0)] -> p0
             | (t0, p0) :: (t1, p1) :: rest ->
                 if target_t >= t0 && target_t <= t1 then
                   let ratio = (target_t -. t0) /. (max 0.0001 (t1 -. t0)) in
                   p0 +. (p1 -. p0) *. ratio
                 else
                   price_at_t target_t ((t1, p1) :: rest)
           in

           for sx = 0 to sub_w - 1 do
             let ratio_x = float sx /. float (max 1 (sub_w - 1)) in
             let target_t = window_start +. (ratio_x *. window_seconds) in
             if target_t >= t_earliest then begin
               let p = price_at_t target_t pts in
               curve_sub_y.(sx) <- price_to_sub_y p
             end
           done);

      let mid_row = price_to_row mid in

      let buy_row_opt =
        match buy_orders with
        | (_, bp, _) :: _ when bp > 0.0 ->
            let r = price_to_row bp in
            let r_distinct =
              if r = mid_row && canvas_h > 2 then min (canvas_h - 1) (mid_row + 1)
              else r
            in
            Some r_distinct
        | _ -> None
      in

      (* Group sell orders by canvas row *)
      let sell_by_row = Hashtbl.create 16 in
      List.iter (fun (id, p, q) ->
        let r = price_to_row p in
        let existing = try Hashtbl.find sell_by_row r with Not_found -> [] in
        Hashtbl.replace sell_by_row r ((id, p, q) :: existing)
      ) sell_orders;

      (* Group buy orders by canvas row *)
      let buy_by_row = Hashtbl.create 16 in
      (match buy_row_opt with
       | Some br -> Hashtbl.replace buy_by_row br buy_orders
       | None ->
           List.iter (fun (id, p, q) ->
             let r = price_to_row p in
             let existing = try Hashtbl.find buy_by_row r with Not_found -> [] in
             Hashtbl.replace buy_by_row r ((id, p, q) :: existing)
           ) buy_orders);

      (* Clean Y-axis price ticks *)
      let show_y_label = Array.make canvas_h false in
      let label_prices = Array.make canvas_h 0.0 in

      for r = 0 to canvas_h - 1 do
        label_prices.(r) <- max_p -. (float r /. float (max 1 (canvas_h - 1))) *. (max_p -. min_p)
      done;

      show_y_label.(mid_row) <- true;
      label_prices.(mid_row) <- mid;

      (match buy_row_opt with
       | Some br ->
           show_y_label.(br) <- true;
           (match buy_orders with (_, bp, _) :: _ -> label_prices.(br) <- bp | [] -> ())
       | None -> ());

      Hashtbl.iter (fun r orders ->
        show_y_label.(r) <- true;
        match orders with (_, p, _) :: _ -> label_prices.(r) <- p | [] -> ()
      ) sell_by_row;

      for r = 0 to canvas_h - 1 do
        if not show_y_label.(r) && (r = 0 || r = canvas_h - 1 || r mod 4 = 0) then begin
          let has_adj =
            (r > 0 && show_y_label.(r - 1)) ||
            (r < canvas_h - 1 && show_y_label.(r + 1))
          in
          if not has_adj then show_y_label.(r) <- true
        end
      done;

      let last_badge_row = ref (-3) in

      (* Render rows of the 2D matrix canvas *)
      let canvas_rows = List.init canvas_h (fun r ->
        let is_grid_line = (r mod 3 = 0) || (r = canvas_h - 1) in
        let is_mid_l = (r = mid_row) in
        let s_orders_l = try Hashtbl.find sell_by_row r with Not_found -> [] in
        let b_orders_l = try Hashtbl.find buy_by_row r with Not_found -> [] in
        let is_sell_l = s_orders_l <> [] in
        let is_buy_l = (Some r = buy_row_opt) || b_orders_l <> [] in

        let y_label_str =
          if show_y_label.(r) then pad_left 11 (format_price label_prices.(r))
          else "           "
        in

        let y_attr =
          if is_mid_l then A.(fg c_green ++ bg c_bg ++ st bold)
          else if is_buy_l then
            if is_synthetic_buy then A.(fg c_yellow ++ bg c_bg ++ st bold)
            else A.(fg c_cyan ++ bg c_bg ++ st bold)
          else if is_sell_l then A.(fg c_magenta ++ bg c_bg ++ st bold)
          else if is_grid_line then A.(fg c_title ++ bg c_bg)
          else A.(fg c_dim ++ bg c_bg)
        in

        (* Render real price time series curve cell columns *)
        let cells = List.init canvas_w (fun c ->
          let sx0 = c * 2 in
          let sx1 = c * 2 + 1 in
          let sy_sub0 = curve_sub_y.(sx0) in
          let sy_sub1 = curve_sub_y.(sx1) in

          let cell_sy_start = r * 4 in
          let mask = ref 0 in
          let on_curve = ref false in

          for sub_y = 0 to 3 do
            let current_sy = cell_sy_start + sub_y in
            if sy_sub0 >= 0 && current_sy = sy_sub0 then begin
              mask := !mask lor braille_bit 0 sub_y;
              on_curve := true
            end;
            if sy_sub1 >= 0 && current_sy = sy_sub1 then begin
              mask := !mask lor braille_bit 1 sub_y;
              on_curve := true
            end
          done;

          let valid_sy =
            if sy_sub0 >= 0 && sy_sub1 >= 0 then min sy_sub0 sy_sub1
            else if sy_sub0 >= 0 then sy_sub0
            else sy_sub1
          in

          if !on_curve && !mask <> 0 then
            let str = braille_to_utf8 !mask in
            I.string A.(fg c_green ++ bg c_bg ++ st bold) str
          else if valid_sy >= 0 && cell_sy_start > valid_sy then
            let fill_dist = float (cell_sy_start - valid_sy) /. float sub_h in
            let fill_rgb =
              color_blend (35, 65, 80) (26, 27, 38) (min 1.0 (fill_dist *. 1.5))
            in
            let fill_char = if is_grid_line && c mod 4 = 0 then "┼" else if c mod 6 = 0 then "┊" else "░" in
            I.string A.(fg fill_rgb ++ bg c_bg) fill_char
          else if is_grid_line then
            let g_char = if c mod 8 = 0 then "┼" else "╌" in
            I.string A.(fg c_border ++ bg c_bg) g_char
          else if c mod 8 = 0 then
            I.string A.(fg c_border ++ bg c_bg) "┊"
          else
            I.string A.(fg c_bg ++ bg c_bg) " "
        ) in

        let chart_line_img = I.hcat cells in

        (* Render Right-Docked Order Target Pins *)
        let can_draw_badge = r - !last_badge_row >= 2 || is_mid_l || is_buy_l in

        let right_pin_img =
          if is_mid_l then begin
            last_badge_row := r;
            let tracer = repeat_utf8 "╌" 4 in
            let mid_badge = Printf.sprintf " ◀ LIVE NOW %s " (format_price mid) in
            I.hcat [
              I.string A.(fg c_green ++ bg c_bg) tracer;
              I.string A.(fg c_bright ++ bg c_green ++ st bold) (pad_right (pin_col_w - 4) mid_badge);
            ]
          end
          else if is_buy_l && can_draw_badge then begin
            last_badge_row := r;
            let orders = if b_orders_l <> [] then b_orders_l else buy_orders in
            let count = List.length orders in
            let total_q = List.fold_left (fun acc (_, _, q) -> acc +. q) 0.0 orders in
            let avg_p = if count > 0 then (List.fold_left (fun acc (_, p, _) -> acc +. p) 0.0 orders) /. float count else mid in
            let dist_pct = if mid > 0.0 then ((avg_p -. mid) /. mid) *. 100.0 else 0.0 in
            let count_str = if count > 1 then Printf.sprintf "[%dx] " count else "" in
            let tracer = repeat_utf8 "╌" 4 in
            if is_synthetic_buy then
              let buy_badge = Printf.sprintf " ◇ EST BUY %s%s %s [%s] " count_str (format_qty total_q) a.asset (format_pct dist_pct) in
              I.hcat [
                I.string A.(fg c_yellow ++ bg c_bg) tracer;
                I.string A.(fg c_bg ++ bg c_yellow ++ st bold) (pad_right (pin_col_w - 4) buy_badge);
              ]
            else
              let buy_badge = Printf.sprintf " ◆ BUY %s%s %s [%s] " count_str (format_qty total_q) a.asset (format_pct dist_pct) in
              I.hcat [
                I.string A.(fg c_cyan ++ bg c_bg) tracer;
                I.string A.(fg c_bg ++ bg c_cyan ++ st bold) (pad_right (pin_col_w - 4) buy_badge);
              ]
          end
          else if is_sell_l && can_draw_badge then begin
            last_badge_row := r;
            let count = List.length s_orders_l in
            let total_q = List.fold_left (fun acc (_, _, q) -> acc +. q) 0.0 s_orders_l in
            let avg_p = (List.fold_left (fun acc (_, p, _) -> acc +. p) 0.0 s_orders_l) /. float count in
            let dist_pct = if mid > 0.0 then ((avg_p -. mid) /. mid) *. 100.0 else 0.0 in
            let count_str = if count > 1 then Printf.sprintf "[%dx] " count else "" in
            let sell_badge = Printf.sprintf " ◆ SELL %s%s %s [%s] " count_str (format_qty total_q) a.asset (format_pct dist_pct) in
            let tracer = repeat_utf8 "╌" 4 in
            I.hcat [
              I.string A.(fg c_magenta ++ bg c_bg) tracer;
              I.string A.(fg c_bg ++ bg c_magenta ++ st bold) (pad_right (pin_col_w - 4) sell_badge);
            ]
          end
          else
            I.string A.(fg c_bg ++ bg c_bg) (String.make pin_col_w ' ')
        in

        close_row w (
          I.hcat [
            I.string a_border " │ ";
            I.string y_attr y_label_str;
            I.string a_border " │ ";
            chart_line_img;
            right_pin_img;
          ]
        )
      ) in

      (* X-Axis Ticks & Labels: Fixed 15-Minute Timeline *)
      let x_axis_ticks =
        let tick_bar = repeat_utf8 "─" (canvas_w + pin_col_w) in
        close_row w (
          I.hcat [
            I.string a_border " │ ";
            I.string A.(fg c_border ++ bg c_bg) " 15M TIME ──";
            I.string a_border " ┴─";
            I.string A.(fg c_border ++ bg c_bg) tick_bar;
          ]
        )
      in

      let x_axis_labels =
        let step_w = canvas_w / 4 in
        let lbl0 = pad_right step_w "-15m" in
        let lbl1 = pad_right step_w "-10m" in
        let lbl2 = pad_right step_w "-5m" in
        let lbl3 = pad_right step_w "-1m" in
        let time_str = lbl0 ^ lbl1 ^ lbl2 ^ lbl3 in
        let pin_title = pad_left pin_col_w "ORDER TARGET PINS ──▶" in
        close_row w (
          I.hcat [
            I.string a_border " │ ";
            I.string a_dim "             ";
            I.string a_border "   ";
            I.string A.(fg c_title ++ bg c_bg ++ st bold) (pad_right canvas_w time_str);
            I.string A.(fg c_accent ++ bg c_bg ++ st bold) pin_title;
          ]
        )
      in

      let num_sells = List.length sell_orders in
      let num_buys = List.length buy_orders in
      let buy_summary_str = if is_synthetic_buy then "1 Est Buy" else Printf.sprintf "%d Buy" num_buys in
      let orders_summary = Printf.sprintf "(%d Sell, %s pending)" num_sells buy_summary_str in
      let graph_title = section_title w ("15-MIN PRICE TIME SERIES & DOCKED ORDERS " ^ orders_summary) in

      let zoom_tag = if z > 0 then Printf.sprintf " [Zoom: %dx] " z else "" in

      let nav_footer = close_row w (
        I.hcat [
          I.string a_border " │ ";
          I.string A.(fg c_cyan ++ bg c_bg ++ st bold) "[↑/↓ or ←/→] ";
          I.string A.(fg c_text ++ bg c_bg) "Prev/Next Asset    ";
          I.string A.(fg c_yellow ++ bg c_bg ++ st bold) "[+/= / -] ";
          I.string A.(fg c_text ++ bg c_bg) ("Zoom In/Out" ^ zoom_tag ^ "    ");
          I.string A.(fg c_accent ++ bg c_bg ++ st bold) "[Esc/b] ";
          I.string A.(fg c_text ++ bg c_bg) "Return to Dashboard    ";
          I.string A.(fg c_yellow ++ bg c_bg ++ st bold) "[q] ";
          I.string A.(fg c_text ++ bg c_bg) "Quit";
        ]
      ) in

      I.vcat [
        header_bar;
        summary_card;
        graph_title;
        I.vcat canvas_rows;
        x_axis_ticks;
        x_axis_labels;
        section_footer w;
        nav_footer;
      ]
