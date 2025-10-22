(** Interactive Balances Dashboard View

    Displays real-time balance and open orders data in a rich terminal UI using Nottui and LWD.
*)

open Nottui.Ui
open Nottui_widgets
open Balance_cache

(** Assets to exclude from balance visualization and calculation *)
let excluded_assets = [
  "VTI";
  "SCHD";
  "QQQ";
  (* Add more assets here as needed *)
]

(** Simple price cache to avoid repeated lookups during UI render *)
let price_cache = Hashtbl.create 32
let price_cache_timestamp = ref 0.0

(** Get USD price for an asset - cached and defensive *)
let get_usd_price asset =
  try
    if asset = "USD" then Some 1.0
    else
      let symbol = asset ^ "/USD" in
      let now = Unix.time () in
      (* Cache prices for 30 seconds *)
      if now -. !price_cache_timestamp > 30.0 then (
        Hashtbl.clear price_cache;
        price_cache_timestamp := now;
      );
      match Hashtbl.find_opt price_cache symbol with
      | Some price -> Some price
      | None ->
          try
            match Kraken.Kraken_ticker_feed.get_latest_price symbol with
            | Some price ->
                Hashtbl.add price_cache symbol price;
                Some price
            | None -> None
          with _ -> None
  with _ -> None

(** Calculate pending amounts per asset from open orders *)
let calculate_pending_amounts orders =
  let pending = Hashtbl.create 16 in
  List.iter (fun order ->
    try
      let parts = String.split_on_char '/' order.symbol in
      let base = List.hd parts in
      let quote = List.nth parts 1 in

      (* Get price for the order - use limit price if available, otherwise current market price *)
      let price = match order.limit_price with
        | Some p -> p
        | None -> match get_usd_price base with Some p -> p | None -> 0.0
      in

      (* For sell orders: pending amount is the remaining base currency to be sold *)
      if order.side = "sell" then (
        let current = try Hashtbl.find pending base with Not_found -> 0.0 in
        Hashtbl.replace pending base (current +. order.remaining_qty)
      )
      (* For buy orders: pending amount is the remaining USD to be spent *)
      else if order.side = "buy" && quote = "USD" then (
        let usd_pending = order.remaining_qty *. price in
        let current = try Hashtbl.find pending "USD" with Not_found -> 0.0 in
        Hashtbl.replace pending "USD" (current +. usd_pending)
      )
    with _ -> () (* Skip malformed orders *)
  ) orders;
  pending

(** UI state for balance view *)
type state = {
  scroll_offset: int;
  auto_scroll: bool;
}

let initial_state = {
  scroll_offset = 0;
  auto_scroll = true;
}

(** Calculate totals for all balances *)
let calculate_balance_totals balances pending_amounts =
  let total_current_value = ref 0.0 in
  let total_accumulated_value = ref 0.0 in

  List.iter (fun entry ->
    (* Current Value (USD equivalent of total balance) *)
    let current_value = match get_usd_price entry.asset with
      | Some price -> entry.total_balance *. price
      | None -> 0.0
    in
    total_current_value := !total_current_value +. current_value;

    (* Exclude USD from accumulated value calculations as an edge case *)
    if entry.asset <> "USD" then (
      (* Accumulated Value (USD equivalent of accumulated balance) *)
      let pending_amount = try Hashtbl.find pending_amounts entry.asset with Not_found -> 0.0 in
      let accumulated_balance = entry.total_balance -. pending_amount in
      let accumulated_value = match get_usd_price entry.asset with
        | Some price -> accumulated_balance *. price
        | None -> 0.0
      in
      total_accumulated_value := !total_accumulated_value +. accumulated_value;
    )
  ) balances;

  (!total_current_value, !total_accumulated_value)

(** Format a total row for display *)
let format_balance_total total_current_value total_accumulated_value =
  let asset_attr = Notty.A.(st bold ++ fg white) in
  let asset_part = string ~attr:asset_attr (Printf.sprintf "%-8s" "TOTAL") in

  (* Total Balance - empty for totals *)
  let total_balance_part = string ~attr:Notty.A.(fg white) (Printf.sprintf "%15s" "-") in

  (* Current Value total - use green for positive PnL, red for negative *)
  let current_value_str = Printf.sprintf "%.2f" total_current_value in
  let current_value_attr = if total_current_value >= 0.0 then Notty.A.(fg green ++ st bold) else Notty.A.(fg red ++ st bold) in
  let current_value_part = string ~attr:current_value_attr (Printf.sprintf "%12s" current_value_str) in

  (* Accumulated Balance - empty for totals *)
  let accumulated_balance_part = string ~attr:Notty.A.(fg white) (Printf.sprintf "%15s" "-") in

  (* Accumulated Value total - use green for positive PnL, red for negative *)
  let accumulated_value_str = Printf.sprintf "%.2f" total_accumulated_value in
  let accumulated_value_attr = if total_accumulated_value >= 0.0 then Notty.A.(fg green ++ st bold) else Notty.A.(fg red ++ st bold) in
  let accumulated_value_part = string ~attr:accumulated_value_attr (Printf.sprintf "%12s" accumulated_value_str) in

  hcat [asset_part; total_balance_part; current_value_part; accumulated_balance_part; accumulated_value_part]

(** Format a balance entry for display with USD values and accumulated amounts *)
let format_balance_entry entry pending_amounts =
  let asset_attr = Notty.A.(st bold ++ fg white) in
  let asset_part = string ~attr:asset_attr (Printf.sprintf "%-8s" entry.asset) in

  (* Total Balance *)
  let total_balance_str = Printf.sprintf "%.8f" entry.total_balance in
  let balance_attr = Notty.A.(fg white) in  (* Primary text for balance amounts *)
  let total_balance_part = string ~attr:balance_attr (Printf.sprintf "%15s" total_balance_str) in

  (* Current Value (USD equivalent of total balance) - use green/red for PnL *)
  let current_value = match get_usd_price entry.asset with
    | Some price -> entry.total_balance *. price
    | None -> 0.0
  in
  let current_value_str = Printf.sprintf "%.2f" current_value in
  let current_value_attr = Notty.A.(fg white) in
  let current_value_part = string ~attr:current_value_attr (Printf.sprintf "%12s" current_value_str) in

  (* Accumulated Balance (total balance minus pending amounts) *)
  (* Exclude USD from accumulated calculations as an edge case *)
  let (accumulated_balance_str, accumulated_balance_attr, accumulated_value_str, accumulated_value_attr) =
    if entry.asset = "USD" then
      (* For USD, show "-" for accumulated fields since they don't apply *)
      ("-", Notty.A.(fg white), "-", Notty.A.(fg white))
    else
      let pending_amount = try Hashtbl.find pending_amounts entry.asset with Not_found -> 0.0 in
      let accumulated_balance = entry.total_balance -. pending_amount in
      let accumulated_balance_str = Printf.sprintf "%.8f" accumulated_balance in
      let accumulated_balance_attr = Notty.A.(fg white) in  (* Primary text for balance amounts *)

      (* Accumulated Value (USD equivalent of accumulated balance) - use green/red for PnL *)
      let accumulated_value = match get_usd_price entry.asset with
        | Some price -> accumulated_balance *. price
        | None -> 0.0
      in
      let accumulated_value_str = Printf.sprintf "%.2f" accumulated_value in
      let accumulated_value_attr = Notty.A.(fg white) in

      (accumulated_balance_str, accumulated_balance_attr, accumulated_value_str, accumulated_value_attr)
  in
  let accumulated_balance_part = string ~attr:accumulated_balance_attr (Printf.sprintf "%15s" accumulated_balance_str) in
  let accumulated_value_part = string ~attr:accumulated_value_attr (Printf.sprintf "%12s" accumulated_value_str) in

  hcat [asset_part; total_balance_part; current_value_part; accumulated_balance_part; accumulated_value_part]

(** Format an open order entry for display *)
let format_open_order_entry entry =
  let side_attr = match entry.side with
    | "buy" -> Notty.A.(fg green ++ st bold)  (* Success/green for buy *)
    | "sell" -> Notty.A.(fg red ++ st bold)   (* Error/red for sell *)
    | _ -> Notty.A.(fg white)
  in
  let side_part = string ~attr:side_attr (Printf.sprintf "%-4s" entry.side) in

  let symbol_attr = Notty.A.(fg white) in  (* Primary text for symbols *)
  let symbol_part = string ~attr:symbol_attr (Printf.sprintf "%-10s" entry.symbol) in

  let qty_part = string ~attr:Notty.A.(fg white) (Printf.sprintf "%10.4f/%-10.4f" entry.remaining_qty entry.order_qty) in

  let price_str = match entry.limit_price with
    | Some p -> Printf.sprintf "@ %.2f" p
    | None -> "@ market"
  in
  let price_part = string ~attr:Notty.A.(fg white) (Printf.sprintf "%-12s" price_str) in

  let status_attr = match entry.order_status with
    | "new" -> Notty.A.(fg green)           (* Success for new orders *)
    | "partially_filled" -> Notty.A.(fg yellow) (* Warning for partial fills *)
    | "filled" -> Notty.A.(fg green ++ st bold) (* Success for filled *)
    | "canceled" -> Notty.A.(fg red)        (* Error for canceled *)
    | _ -> Notty.A.(fg white)
  in
  let status_part = string ~attr:status_attr (Printf.sprintf "[%s]" entry.order_status) in

  hcat [side_part; symbol_part; qty_part; price_part; status_part]

(** Format aggregated open orders entry for display *)
let format_aggregated_orders_entry (symbol, buy_count, sell_count, buy_cost, sell_value, total_gain) =
  let symbol_attr = Notty.A.(fg white ++ st bold) in  (* Primary text for symbols *)
  let symbol_part = string ~attr:symbol_attr (Printf.sprintf "%-12s" symbol) in

  (* Buy count *)
  let buy_count_attr = Notty.A.(fg green) in
  let buy_count_part = string ~attr:buy_count_attr (Printf.sprintf "%6d" buy_count) in

  (* Buy Cost - show as green for costs *)
  let buy_cost_attr = Notty.A.(fg green) in
  let buy_cost_part = string ~attr:buy_cost_attr (Printf.sprintf "%10.2f" buy_cost) in

  (* Sell count *)
  let sell_count_attr = Notty.A.(fg red) in
  let sell_count_part = string ~attr:sell_count_attr (Printf.sprintf "%6d" sell_count) in

  (* Sell Gain - show as red for gains from selling *)
  let sell_value_attr = Notty.A.(fg red) in
  let sell_value_part = string ~attr:sell_value_attr (Printf.sprintf "%10.2f" sell_value) in

  (* Total count *)
  let total_count = buy_count + sell_count in
  let total_count_attr = Notty.A.(fg white) in
  let total_count_part = string ~attr:total_count_attr (Printf.sprintf "%6d" total_count) in

  (* Total Gain - use green for positive, red for negative *)
  let total_gain_attr = if total_gain >= 0.0 then Notty.A.(fg green ++ st bold) else Notty.A.(fg red ++ st bold) in
  let total_gain_part = string ~attr:total_gain_attr (Printf.sprintf "%11.2f" total_gain) in

  hcat [symbol_part; buy_count_part; buy_cost_part; sell_count_part; sell_value_part; total_count_part; total_gain_part]

(** Aggregate open orders by symbol with counts and financial values *)
let aggregate_orders_by_symbol orders =
  let rec aggregate_orders acc = function
    | [] -> acc
    | order :: rest ->
        let symbol = order.symbol in
        let current = try List.assoc symbol acc with Not_found -> (0, 0, 0.0, 0.0, 0.0) in
        let (buy_count, sell_count, buy_cost, sell_value, total_gain) = current in

        (* Get price for calculation - use limit price if available, otherwise market price *)
        let price = match order.limit_price with
          | Some p -> p
          | None ->
              let parts = String.split_on_char '/' symbol in
              let base = List.hd parts in
              match get_usd_price base with Some p -> p | None -> 0.0
        in

        let value = order.remaining_qty *. price in
        let new_aggregates = match order.side with
          | "buy" -> (buy_count + 1, sell_count, buy_cost +. value, sell_value, total_gain -. value)
          | "sell" -> (buy_count, sell_count + 1, buy_cost, sell_value +. value, total_gain +. value)
          | _ -> current
        in
        let new_acc = (symbol, new_aggregates) :: List.remove_assoc symbol acc in
        aggregate_orders new_acc rest
  in
  let symbol_aggregates = aggregate_orders [] orders in
  (* Sort by symbol for consistent display *)
  List.sort (fun (s1, _) (s2, _) -> String.compare s1 s2) symbol_aggregates

(** Create balances view widget *)
let balances_view balance_snapshot _state =
  (* Header *)
  let timestamp_str = Printf.sprintf "Updated: %s"
    (if balance_snapshot.timestamp > 0.0 then
       let tm = Unix.localtime balance_snapshot.timestamp in
       Printf.sprintf "%02d:%02d:%02d"
         tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec
     else "Never")
  in
  let timestamp_attr = Notty.A.(fg white) in  (* Primary text for timestamps *)
  let timestamp = string ~attr:timestamp_attr timestamp_str in

  let header = timestamp in

  (* Balances section *)
  let balances_title = string ~attr:Notty.A.(st bold ++ fg white) "\nBalances:" in
  let balances_header = string ~attr:Notty.A.(fg white)
    (Printf.sprintf "\n%-8s %15s %12s %15s %12s" "Asset" "Total Bal" "Cur Value" "Acc Bal" "Acc Value") in

  (* Calculate pending amounts from open orders *)
  let pending_amounts = calculate_pending_amounts balance_snapshot.open_orders in

  (* Filter out zero balances and excluded assets *)
  let filtered_balances = List.filter (fun entry ->
    entry.total_balance <> 0.0 && not (List.mem entry.asset excluded_assets)
  ) balance_snapshot.balances in

  (* Sort balances alphabetically by asset name *)
  let sorted_balances = List.sort (fun a b -> String.compare a.asset b.asset) filtered_balances in

  let balance_entries = List.map (fun entry -> format_balance_entry entry pending_amounts) sorted_balances in

  (* Calculate and format totals *)
  let (total_current_value, total_accumulated_value) = calculate_balance_totals sorted_balances pending_amounts in
  let total_row = format_balance_total total_current_value total_accumulated_value in

  (* Add separator line before total *)
  let separator = string ~attr:Notty.A.(fg (gray 2)) (String.make 70 '-') in  (* Dim gray for gridlines *)

  let balances_list = if balance_entries = [] then
    empty
  else
    vcat (balance_entries @ [string ~attr:Notty.A.empty ""; separator; total_row]) in

  let balances_section = if filtered_balances = [] then
    vcat [balances_title; balances_header; string ~attr:Notty.A.(fg red) "\n  No balance data available"]
  else
    vcat [balances_title; balances_header; balances_list] in

  (* Open orders section *)
  let orders_title = string ~attr:Notty.A.(st bold ++ fg white) "\n\nOpen Orders:" in
  let orders_header = string ~attr:Notty.A.(fg white)
    (Printf.sprintf "\n%-12s %6s %10s %6s %10s %6s %11s" "Pair" "Buy" "Buy Cost" "Sell" "Sell Gain" "Total" "Total Gain") in

  let aggregated_orders = aggregate_orders_by_symbol balance_snapshot.open_orders in
  let order_entries = List.map (fun (symbol, (buy_count, sell_count, buy_cost, sell_value, total_gain)) ->
    format_aggregated_orders_entry (symbol, buy_count, sell_count, buy_cost, sell_value, total_gain)
  ) aggregated_orders in

  (* Calculate totals for open orders *)
  let total_buy_count = List.fold_left (fun acc (_, (buy_count, _, _, _, _)) -> acc + buy_count) 0 aggregated_orders in
  let total_sell_count = List.fold_left (fun acc (_, (_, sell_count, _, _, _)) -> acc + sell_count) 0 aggregated_orders in
  let total_buy_cost = List.fold_left (fun acc (_, (_, _, buy_cost, _, _)) -> acc +. buy_cost) 0.0 aggregated_orders in
  let total_sell_value = List.fold_left (fun acc (_, (_, _, _, sell_value, _)) -> acc +. sell_value) 0.0 aggregated_orders in
  let total_orders_gain = List.fold_left (fun acc (_, (_, _, _, _, total_gain)) -> acc +. total_gain) 0.0 aggregated_orders in

  (* Format totals row for open orders *)
  let format_orders_total total_buy_count total_sell_count total_buy_cost total_sell_value total_orders_gain =
    let symbol_attr = Notty.A.(st bold ++ fg white) in
    let symbol_part = string ~attr:symbol_attr (Printf.sprintf "%-12s" "TOTAL") in

    let total_count = total_buy_count + total_sell_count in

    let buy_count_attr = Notty.A.(fg green ++ st bold) in
    let buy_count_part = string ~attr:buy_count_attr (Printf.sprintf "%6d" total_buy_count) in

    let buy_cost_attr = Notty.A.(fg green ++ st bold) in
    let buy_cost_part = string ~attr:buy_cost_attr (Printf.sprintf "%10.2f" total_buy_cost) in

    let sell_count_attr = Notty.A.(fg red ++ st bold) in
    let sell_count_part = string ~attr:sell_count_attr (Printf.sprintf "%6d" total_sell_count) in

    let sell_value_attr = Notty.A.(fg red ++ st bold) in
    let sell_value_part = string ~attr:sell_value_attr (Printf.sprintf "%10.2f" total_sell_value) in

    let total_count_attr = Notty.A.(fg white ++ st bold) in
    let total_count_part = string ~attr:total_count_attr (Printf.sprintf "%6d" total_count) in

    let total_gain_attr = if total_orders_gain >= 0.0 then Notty.A.(fg green ++ st bold) else Notty.A.(fg red ++ st bold) in
    let total_gain_part = string ~attr:total_gain_attr (Printf.sprintf "%11.2f" total_orders_gain) in

    hcat [symbol_part; buy_count_part; buy_cost_part; sell_count_part; sell_value_part; total_count_part; total_gain_part]
  in

  let orders_total_row = format_orders_total total_buy_count total_sell_count total_buy_cost total_sell_value total_orders_gain in

  (* Add separator and total row to orders list *)
  let orders_separator = string ~attr:Notty.A.(fg (gray 2)) (String.make 67 '-') in
  let orders_list = if order_entries = [] then
    empty
  else
    vcat (order_entries @ [string ~attr:Notty.A.empty ""; orders_separator; orders_total_row]) in

  let orders_section = if balance_snapshot.open_orders = [] then
    vcat [orders_title; orders_header; string ~attr:Notty.A.(fg white) "\n  No open orders"]
  else
    vcat [orders_title; orders_header; orders_list] in

  (* Combine all sections *)
  let content = vcat [header; balances_section; orders_section] in

  (* Add some padding at the bottom *)
  vcat [content; string ~attr:Notty.A.empty "\n"]

(** Create reactive balances view *)
let make_balances_view balance_snapshot_var =
  let state_var = Lwd.var initial_state in

  (* Reactive UI that updates when data changes *)
  let ui = Lwd.map2 (Lwd.get balance_snapshot_var) (Lwd.get state_var) ~f:(fun (snapshot, _) state ->
    balances_view snapshot state
  ) in

  (* Event handler - for now just handle basic navigation *)
  let handle_event ev =
    let state = Lwd.peek state_var in
    match ev with
    | `Key (`ASCII 'q', []) -> `Quit
    | `Key (`ASCII 'c', [`Ctrl]) -> `Quit
    | `Key (`ASCII 'g', []) ->
        (* Go to top *)
        Lwd.set state_var { state with scroll_offset = 0 };
        `Continue
    | `Key (`ASCII 'G', []) ->
        (* Go to bottom - for future scrolling implementation *)
        `Continue
    | `Key (`ASCII 'a', []) ->
        (* Toggle auto scroll *)
        let new_auto_scroll = not state.auto_scroll in
        Lwd.set state_var { state with auto_scroll = new_auto_scroll };
        `Continue
    | _ -> `Continue
  in

  (ui, handle_event)
