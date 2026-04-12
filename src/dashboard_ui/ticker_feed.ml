open Notty
open Theme

(** Ticker Feed Component.
    Scrolls assets horizontally across the screen that are NOT:
    - Active strategies
    - Cash (quote currencies)
    - Active balances
    This primarily targets paused strategies or tracked tickers with zero balance.
*)

let current_offset = ref 0.0
let last_time = ref None

let render_ticker w json =
  let strats = match json |?> "strategies" with `Assoc l -> l | _ -> [] in
  let all_balances = json |?> "all_balances" |> to_list_d in

  (* Find all strategies *)
  let all_strategies =
    List.filter_map (fun (symbol, data) ->
      let market = data |?> "market" in
      let bid = market |?> "bid" |> to_float_d 0.0 in
      let ask = market |?> "ask" |> to_float_d 0.0 in
      let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else max bid ask in
      let exchange = data |?> "exchange" |> to_string_d "?" in
      Some (exchange, symbol, mid, bid, ask)
    ) strats
  in

  (* Find all non-quote balances from all_balances just in case the engine 
     decides to push them in the future. *)
  let non_quote_bals =
    List.filter_map (fun bal_json ->
      let asset = bal_json |?> "asset" |> to_string_d "?" in
      let exchange = bal_json |?> "exchange" |> to_string_d "?" in
      let is_quote = asset = "USD" || asset = "USDC" || asset = "USDT" || asset = "ZUSD" || asset = "USDe" in
      if not is_quote then
        let bid = bal_json |?> "bid" |> to_float_d 0.0 in
        let ask = bal_json |?> "ask" |> to_float_d 0.0 in
        let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else max bid ask in
        let symbol = bal_json |?> "symbol" |> to_string_d asset in
        Some (exchange, symbol, mid, bid, ask)
      else None
    ) all_balances
  in

  let combined = all_strategies @ non_quote_bals in

  if combined = [] then
    I.empty
  else
    (* Build the ticker string chunks *)
    let chunks = List.map (fun (exchange, symbol, mid, bid, ask) ->
      let exch_tag = exch_tag_of exchange in
      let sym_attr = exch_sym_attr exchange in
      let price_str = if mid > 0.0 then format_price mid else "--" in
      let spread_str = format_spread_bps bid ask in
      
      let spread_attr =
        if bid <= 0.0 || ask <= 0.0 then a_dim
        else
          let bps = ((ask -. bid) /. ((bid +. ask) /. 2.0)) *. 10000.0 in
          if bps < 5.0 then a_green
          else if bps < 20.0 then a_cyan
          else if bps < 50.0 then a_yellow
          else a_red
      in

      I.hcat [
        I.string sym_attr (Printf.sprintf " %s(%s) " symbol exch_tag);
        I.string a_text price_str;
        I.string a_dim " ";
        I.string spread_attr spread_str;
      ]
    ) combined in

    let separator = I.string a_border "  •  " in
    let ticker_line =
      List.fold_left (fun acc chunk ->
        if I.width acc = 0 then chunk
        else I.hcat [acc; separator; chunk]
      ) I.empty chunks
    in

    let line_w = I.width ticker_line in
    
    (* If there's nothing to show or it's empty, return empty *)
    if line_w = 0 then I.empty
    else
      (* Add padding to ticker line so it can wrap nicely *)
      let padded_ticker = I.hcat [ticker_line; I.string a_text "          "] in
      let padded_w = I.width padded_ticker in

      (* Smooth continuous scroll left-to-right based on delta time to prevent string-length-change jitter. *)
      let now = Unix.gettimeofday () in
      let dt = match !last_time with
        | None -> 0.0
        | Some t -> now -. t
      in
      last_time := Some now;

      (* Prevent huge jumps if the app stalls, suspends, or during the first frame *)
      let dt = if dt > 1.0 then 0.0 else dt in

      let scroll_speed = 6.0 in 
      current_offset := !current_offset +. (dt *. scroll_speed);
      
      let max_w = float_of_int padded_w in
      if !current_offset >= max_w then
        current_offset := mod_float !current_offset max_w;

      let offset = int_of_float !current_offset in
      
      (* To scroll left-to-right, the view must shift left over the image. 
         We achieve this by cropping `padded_w - offset` from the left. *)
      let invert_offset = padded_w - offset - 1 in
      let invert_offset = if invert_offset < 0 then 0 else invert_offset in

      let scroll_region =
        if padded_w <= w then
          (* if the ticker is smaller than screen, duplicate it to fill *)
          let repeats = (w / padded_w) + 2 in
          let repeated = I.hcat (List.init repeats (fun _ -> padded_ticker)) in
          I.crop ~l:invert_offset ~r:0 ~t:0 ~b:0 repeated
        else
          (* if larger, just append one more copy to wrap around cleanly *)
          let repeated = I.hcat [padded_ticker; padded_ticker] in
          I.crop ~l:invert_offset ~r:0 ~t:0 ~b:0 repeated
      in
      
      let final_img = I.hsnap ~align:`Left w scroll_region in
      I.(final_img </> I.string A.(bg c_bg) (String.make w ' '))
