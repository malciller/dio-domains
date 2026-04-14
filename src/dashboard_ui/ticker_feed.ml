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
    let one_price, multi_price = List.partition (fun (_, entries) -> List.length entries = 1) grouped in
    
    let rec pair_ones acc = function
      | [] -> List.rev acc
      | [x] -> List.rev ([x] :: acc)
      | x :: y :: rest -> pair_ones ([x; y] :: acc) rest
    in
    let paired_ones = pair_ones [] one_price in
    let multi_wrapped = List.map (fun x -> [x]) multi_price in
    
    let combined_groups = paired_ones @ multi_wrapped in
    let sorted_groups = List.sort (fun g1 g2 -> String.compare (fst (List.hd g1)) (fst (List.hd g2))) combined_groups in

    (* Build the ticker string chunks grouped by asset and dense-packed by pairs if single-venue *)
    let chunks_list = List.map (fun group ->
      let group_imgs = List.map (fun (asset, entries) ->
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
      ) group in
      
      let sub_separator = I.string A.(fg c_dim) " │ " in
      List.fold_left (fun acc img ->
        if I.width acc = 0 then img else I.hcat [acc; sub_separator; img]
      ) I.empty group_imgs
    ) sorted_groups in

    let chunks = Array.of_list chunks_list in
    let n = Array.length chunks in
    let separator = I.string A.(fg c_accent ++ bg c_bg) "  ❖  " in
    let sep_w = I.width separator in
    let feed_start = I.string A.(fg c_accent ++ bg c_bg) "  ❖ LIVE TICKER ❖  " in
    let feed_w = I.width feed_start in
    let max_w = w - 2 in
    
    let get_col_slice cols c =
      let base = n / cols in
      let rem = n mod cols in
      let split_c = cols - rem in
      if c < split_c then
        c * base, base
      else
        split_c * base + (c - split_c) * (base + 1), base + 1
    in

    let get_col_widths cols =
      let widths = Array.make cols 0 in
      for c = 0 to cols - 1 do
        let (start_idx, size) = get_col_slice cols c in
        let max_w = ref 0 in
        for i = 0 to size - 1 do
          max_w := max !max_w (I.width chunks.(start_idx + i))
        done;
        widths.(c) <- !max_w
      done;
      widths
    in
    
    let fits cols =
      let widths = get_col_widths cols in
      let total_w = ref feed_w in
      for c = 0 to cols - 1 do
        total_w := !total_w + widths.(c) + (if c > 0 then sep_w else 0)
      done;
      !total_w <= max_w
    in
    
    (* Find maximum columns that fit from n down to 1 *)
    let rec find_cols c =
      if c <= 1 then 1
      else if fits c then c
      else find_cols (c - 1)
    in
    
    let cols = if n = 0 then 1 else find_cols n in
    let col_widths = get_col_widths cols in
    
    (* Cycle independently on clock *)
    let cycle_time = 5.0 in
    let page = int_of_float (Unix.gettimeofday () /. cycle_time) in
    
    let slot_images =
      List.init cols (fun c ->
        let (start_idx, size) = get_col_slice cols c in
        let items = List.init size (fun i -> chunks.(start_idx + i)) in
        if items = [] then I.empty
        else
          let len = List.length items in
          let item = List.nth items (page mod len) in
          (* Pad strictly to col width so adjacent elements never bounce horizontally *)
          I.hsnap ~align:`Left col_widths.(c) item
      )
    in
    
    let valid_slots = List.filter (fun img -> I.width img > 0) slot_images in
    
    let final_img =
      List.fold_left (fun acc slot ->
        if I.width acc = feed_w then I.hcat [acc; slot] else I.hcat [acc; separator; slot]
      ) feed_start valid_slots
    in

    let padded = I.hsnap ~align:`Left w final_img in
    I.(padded </> I.string A.(bg c_bg) (String.make w ' '))
