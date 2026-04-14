open Notty
open Theme

(** Ticker Feed Component.
    Scrolls assets horizontally across the screen that are NOT:
    - Active strategies
    - Cash (quote currencies)
    - Active balances
    This primarily targets paused strategies or tracked tickers with zero balance.
*)

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
      if mid <= 0.0 then None else
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
        if mid <= 0.0 then None else
        let symbol = bal_json |?> "symbol" |> to_string_d asset in
        Some (exchange, symbol, mid, bid, ask)
      else None
    ) all_balances
  in

  let combined = all_strategies @ non_quote_bals in

  let grouped = 
    let base_asset_of symbol = 
      match String.split_on_char '/' symbol with
      | h :: _ -> h
      | [] -> symbol
    in
    let tbl = Hashtbl.create 16 in
    List.iter (fun (ex, sym, mid, bid, ask) ->
      let asset = base_asset_of sym in
      let exch_tag = exch_tag_of ex in
      let sym_attr = exch_sym_attr ex in
      let entry = (sym_attr, exch_tag, mid, bid, ask) in
      let existing = try Hashtbl.find tbl asset with Not_found -> [] in
      Hashtbl.replace tbl asset (entry :: existing)
    ) combined;
    Hashtbl.fold (fun asset entries acc -> (asset, List.rev entries) :: acc) tbl []
    |> List.sort (fun (a1, _) (a2, _) -> String.compare a1 a2)
  in

  if grouped = [] then I.empty
  else
    (* Build the ticker string chunks grouped by asset incrementally as they appear in queue *)
    let chunks = List.map (fun (asset, entries) ->
      let asset_header = I.string A.(fg c_text ++ st bold) (Printf.sprintf " %s " asset) in
      
      let price_images = List.map (fun (sym_attr, _exch_tag, mid, bid, ask) ->
        let price_str = if mid > 0.0 then format_price mid else "--" in
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
        
        I.hcat [
          I.string sym_attr price_str;
          I.string a_dim " ";
          I.string spread_attr spread_str;
        ]
      ) entries in
      
      let spacing = I.string a_dim "  " in
      let entries_combined = 
        List.fold_left (fun acc ch -> 
          if I.width acc = 0 then ch else I.hcat [acc; spacing; ch]
        ) I.empty price_images
      in
      
      I.hcat [asset_header; entries_combined; I.string a_text " "]
    ) grouped in

    let separator = I.string A.(fg c_accent ++ bg c_bg) "  ❖  " in
    let feed_start = I.string A.(fg c_accent ++ bg c_bg) "  ❖ LIVE TICKER ❖  " in
    let max_w = w - 2 in
    
    let rec pack_pages chunks current_line current_w acc =
      match chunks with
      | [] -> 
          if current_line = [] then List.rev acc
          else List.rev (List.rev current_line :: acc)
      | c :: cs ->
          if current_line = [] then
            pack_pages cs [c] (I.width feed_start + I.width c) acc
          else
            let added_w = I.width separator + I.width c in
            if current_w + added_w > max_w then
              (* page full *)
              pack_pages cs [c] (I.width feed_start + I.width c) (List.rev current_line :: acc)
            else
              pack_pages cs (c :: current_line) (current_w + added_w) acc
    in
    let pages = pack_pages chunks [] 0 [] in

    if pages = [] then I.empty
    else
      let num_pages = List.length pages in
      let cycle_time = 5.0 in
      let page_index = (int_of_float (Unix.gettimeofday () /. cycle_time)) mod num_pages in
      let current_page_chunks = List.nth pages page_index in

      let final_img =
        List.fold_left (fun acc chunk ->
          if I.width acc = I.width feed_start then I.hcat [acc; chunk] else I.hcat [acc; separator; chunk]
        ) feed_start current_page_chunks
      in

      let padded = I.hsnap ~align:`Left w final_img in
      I.(padded </> I.string A.(bg c_bg) (String.make w ' '))
