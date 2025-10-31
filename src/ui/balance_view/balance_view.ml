(** Interactive Balances Dashboard View

    Displays real-time balance and open orders data in a rich terminal UI using Nottui and LWD.
*)

open Nottui_widgets
open Nottui.Ui
open Balance_cache

module Ui_components = Dio_ui_shared.Ui_components

(** Assets to exclude from balance visualization and calculation *)
let excluded_assets = [
  "VTI";
  "SCHD";
  "QQQ";
  (* Add more assets here as needed *)
]

(** Helper function to take first n elements from a list *)
let rec take_first n = function
  | [] -> []
  | _ when n <= 0 -> []
  | x :: xs -> x :: take_first (n - 1) xs

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

(** Calculate minimum width required for balance view content *)
let calculate_balance_view_min_width balance_snapshot =
  (* Balances table width calculation - based on actual column content *)
  let asset_max_len = List.fold_left (fun max_len entry ->
    max max_len (String.length entry.asset)
  ) 0 balance_snapshot.balances in

  let balances_table_width = asset_max_len + 8 + 15 + 12 + 15 + 12 + 20 in (* Asset + Total Bal + Cur Value + Acc Bal + Acc Value + spacing *)

  (* Orders table width calculation - based on actual order data *)
  let symbol_max_len = List.fold_left (fun max_len order ->
    max max_len (String.length order.symbol)
  ) 0 balance_snapshot.open_orders in

  let orders_table_width = max symbol_max_len 12 + 7 + 7 + 6 + 10 + 6 + 10 + 6 + 11 + 20 in (* Symbol + columns + spacing *)

  (* Calculate based on the wider of the two tables, plus some padding *)
  let table_width = max balances_table_width orders_table_width in

  (* Add padding for readability and borders *)
  max 60 (table_width + 8)

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
  let asset_part = Ui_components.create_label (Printf.sprintf "%-8s" "TOTAL") in

  (* Total Balance - empty for totals *)
  let total_balance_part = Ui_components.create_text (Printf.sprintf "%15s" "-") in

  (* Current Value total - use green for positive PnL, red for negative *)
  let current_value_str = Printf.sprintf "%.2f" total_current_value in
  let current_value_part = if total_current_value >= 0.0 then Ui_components.create_success_status current_value_str
                          else Ui_components.create_error_status current_value_str in

  (* Accumulated Balance - empty for totals *)
  let accumulated_balance_part = Ui_components.create_text (Printf.sprintf "%15s" "-") in

  (* Accumulated Value total - use green for positive PnL, red for negative *)
  let accumulated_value_str = Printf.sprintf "%.2f" total_accumulated_value in
  let accumulated_value_part = if total_accumulated_value >= 0.0 then Ui_components.create_success_status accumulated_value_str
                              else Ui_components.create_error_status accumulated_value_str in

  hcat [asset_part; Ui_components.create_text " "; total_balance_part; Ui_components.create_text " "; current_value_part; Ui_components.create_text " "; accumulated_balance_part; Ui_components.create_text " "; accumulated_value_part]

(** Format a balance entry for display with USD values and accumulated amounts *)
let format_balance_entry entry pending_amounts =
  let asset_part = Ui_components.create_label (Printf.sprintf "%-8s" entry.asset) in

  (* Total Balance *)
  let total_balance_str = Printf.sprintf "%.8f" entry.total_balance in
  let total_balance_part = Ui_components.create_text (Printf.sprintf "%15s" total_balance_str) in

  (* Current Value (USD equivalent of total balance) *)
  let current_value = match get_usd_price entry.asset with
    | Some price -> entry.total_balance *. price
    | None -> 0.0
  in
  let current_value_str = Printf.sprintf "%.2f" current_value in
  let current_value_part = Ui_components.create_text (Printf.sprintf "%12s" current_value_str) in

  (* Accumulated Balance (total balance minus pending amounts) *)
  (* Exclude USD from accumulated calculations as an edge case *)
  let (accumulated_balance_part, accumulated_value_part) =
    if entry.asset = "USD" then
      (* For USD, show "-" for accumulated fields since they don't apply *)
      (Ui_components.create_text (Printf.sprintf "%15s" "-"), Ui_components.create_text (Printf.sprintf "%12s" "-"))
    else
      let pending_amount = try Hashtbl.find pending_amounts entry.asset with Not_found -> 0.0 in
      let accumulated_balance = entry.total_balance -. pending_amount in
      let accumulated_balance_str = Printf.sprintf "%.8f" accumulated_balance in
      let accumulated_balance_part = Ui_components.create_text (Printf.sprintf "%15s" accumulated_balance_str) in

      (* Accumulated Value (USD equivalent of accumulated balance) *)
      let accumulated_value = match get_usd_price entry.asset with
        | Some price -> accumulated_balance *. price
        | None -> 0.0
      in
      let accumulated_value_str = Printf.sprintf "%.2f" accumulated_value in
      let accumulated_value_part = Ui_components.create_text (Printf.sprintf "%12s" accumulated_value_str) in

      (accumulated_balance_part, accumulated_value_part)
  in

  hcat [asset_part; Ui_components.create_text " "; total_balance_part; Ui_components.create_text " "; current_value_part; Ui_components.create_text " "; accumulated_balance_part; Ui_components.create_text " "; accumulated_value_part]

(** Format an open order entry for display *)
let format_open_order_entry entry =
  let side_part = match entry.side with
    | "buy" -> Ui_components.create_success_status (Printf.sprintf "%-4s" entry.side)
    | "sell" -> Ui_components.create_error_status (Printf.sprintf "%-4s" entry.side)
    | _ -> Ui_components.create_text (Printf.sprintf "%-4s" entry.side)
  in

  let symbol_part = Ui_components.create_text (Printf.sprintf "%-10s" entry.symbol) in

  let qty_part = Ui_components.create_text (Printf.sprintf "%10.4f/%-10.4f" entry.remaining_qty entry.order_qty) in

  let price_str = match entry.limit_price with
    | Some p -> Printf.sprintf "@ %.2f" p
    | None -> "@ market"
  in
  let price_part = Ui_components.create_text (Printf.sprintf "%-12s" price_str) in

  let status_part = match entry.order_status with
    | "new" -> Ui_components.create_success_status (Printf.sprintf "[%s]" entry.order_status)
    | "partially_filled" -> Ui_components.create_warning_status (Printf.sprintf "[%s]" entry.order_status)
    | "filled" -> Ui_components.create_success_status (Printf.sprintf "[%s]" entry.order_status)
    | "canceled" -> Ui_components.create_error_status (Printf.sprintf "[%s]" entry.order_status)
    | _ -> Ui_components.create_text (Printf.sprintf "[%s]" entry.order_status)
  in

  hcat [side_part; symbol_part; qty_part; price_part; status_part]

(** Format aggregated open orders entry for display *)
let format_aggregated_orders_entry (symbol, buy_count, sell_count, buy_cost, sell_value, total_gain, maker_fee_opt, taker_fee_opt) =
  let symbol_part = Ui_components.create_label (Printf.sprintf "%-12s" symbol) in

  (* Buy count *)
  let buy_count_part = Ui_components.create_success_status (Printf.sprintf "%6d" buy_count) in

  (* Buy Cost - show as green for costs *)
  let buy_cost_part = Ui_components.create_success_status (Printf.sprintf "%10.2f" buy_cost) in

  (* Sell count *)
  let sell_count_part = Ui_components.create_error_status (Printf.sprintf "%6d" sell_count) in

  (* Sell Gain - show as red for gains from selling *)
  let sell_value_part = Ui_components.create_error_status (Printf.sprintf "%10.2f" sell_value) in

  (* Total count *)
  let total_count = buy_count + sell_count in
  let total_count_part = Ui_components.create_text (Printf.sprintf "%6d" total_count) in

  (* Total Gain - use green for positive, red for negative *)
  let total_gain_part = if total_gain >= 0.0 then Ui_components.create_success_status (Printf.sprintf "%11.2f" total_gain)
                       else Ui_components.create_error_status (Printf.sprintf "%11.2f" total_gain) in

  (* Maker Fee - display as percentage or "-" if not available *)
  let maker_fee_str = match maker_fee_opt with
    | Some fee -> Printf.sprintf "%.2f" (fee *. 100.0)
    | None -> "-" in
  let maker_fee_part = Ui_components.create_text (Printf.sprintf "%7s" maker_fee_str) in

  (* Taker Fee - display as percentage or "-" if not available *)
  let taker_fee_str = match taker_fee_opt with
    | Some fee -> Printf.sprintf "%.2f" (fee *. 100.0)
    | None -> "-" in
  let taker_fee_part = Ui_components.create_text (Printf.sprintf "%7s" taker_fee_str) in

  hcat [symbol_part; Ui_components.create_text " "; maker_fee_part; Ui_components.create_text " "; taker_fee_part; Ui_components.create_text " "; buy_count_part; Ui_components.create_text " "; buy_cost_part; Ui_components.create_text " "; sell_count_part; Ui_components.create_text " "; sell_value_part; Ui_components.create_text " "; total_count_part; Ui_components.create_text " "; total_gain_part]

(** Aggregate open orders by symbol with counts and financial values *)
let aggregate_orders_by_symbol orders =
  let rec aggregate_orders acc = function
    | [] -> acc
    | order :: rest ->
        let symbol = order.symbol in
        let current = try List.assoc symbol acc with Not_found -> (0, 0, 0.0, 0.0, 0.0, None, None) in
        let (buy_count, sell_count, buy_cost, sell_value, total_gain, maker_fee_opt, taker_fee_opt) = current in

        (* Extract fees from the first order encountered for this symbol *)
        let maker_fee_opt = match maker_fee_opt with Some _ -> maker_fee_opt | None -> order.maker_fee in
        let taker_fee_opt = match taker_fee_opt with Some _ -> taker_fee_opt | None -> order.taker_fee in

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
          | "buy" -> (buy_count + 1, sell_count, buy_cost +. value, sell_value, total_gain -. value, maker_fee_opt, taker_fee_opt)
          | "sell" -> (buy_count, sell_count + 1, buy_cost, sell_value +. value, total_gain +. value, maker_fee_opt, taker_fee_opt)
          | _ -> (buy_count, sell_count, buy_cost, sell_value, total_gain, maker_fee_opt, taker_fee_opt)
        in
        let new_acc = (symbol, new_aggregates) :: List.remove_assoc symbol acc in
        aggregate_orders new_acc rest
  in
  let symbol_aggregates = aggregate_orders [] orders in
  (* Sort by symbol for consistent display *)
  List.sort (fun (s1, _) (s2, _) -> String.compare s1 s2) symbol_aggregates

(** Create balances view widget with size constraints *)
let balances_view balance_snapshot _state ~available_width ~available_height =
  (* Determine screen size for adaptive display *)
  let screen_size = Ui_types.classify_screen_size available_width available_height in

  match screen_size with
  | Mobile ->
      (* Ultra-compact mobile layout: maximum 6 lines *)
      let header_str = if balance_snapshot.timestamp > 0.0 then
        let tm = Unix.localtime balance_snapshot.timestamp in
        Printf.sprintf "Upd: %02d:%02d" tm.Unix.tm_hour tm.Unix.tm_min
      else "Never" in
      let header = string ~attr:Notty.A.(fg white) (Ui_types.truncate_exact (available_width - 4) header_str) in

      (* Calculate pending amounts and filter/sort balances *)
      let pending_amounts = calculate_pending_amounts balance_snapshot.open_orders in
      let filtered_balances = List.filter (fun entry ->
        entry.total_balance <> 0.0 && not (List.mem entry.asset excluded_assets)
      ) balance_snapshot.balances in

      (* Sort by current value (highest first) for mobile *)
      let sorted_balances = List.sort (fun a b ->
        let val_a = match get_usd_price a.asset with Some p -> a.total_balance *. p | None -> 0.0 in
        let val_b = match get_usd_price b.asset with Some p -> b.total_balance *. p | None -> 0.0 in
        compare val_b val_a  (* Descending order *)
      ) filtered_balances in

      (* Take top 4-5 assets only *)
      let top_assets = match available_height with
        | h when h <= 3 -> []  (* No space for assets *)
        | h when h <= 4 -> take_first 2 sorted_balances  (* 2 assets max *)
        | h when h <= 5 -> take_first 3 sorted_balances  (* 3 assets max *)
        | _ -> take_first 4 sorted_balances  (* 4 assets max *)
      in

      (* Format each asset in ultra-compact single-line format *)
      let asset_lines = List.map (fun entry ->
        let pending_amount = try Hashtbl.find pending_amounts entry.asset with Not_found -> 0.0 in
        let available_balance = entry.total_balance -. pending_amount in
        let value = match get_usd_price entry.asset with
          | Some price -> available_balance *. price
          | None -> 0.0
        in
        let asset_name = Ui_types.truncate_tiny entry.asset in
        let balance_str = Ui_types.format_mobile_number entry.total_balance in
        let value_str = Ui_types.format_mobile_currency value in
        let line = Printf.sprintf "%s: %s (%s)" asset_name balance_str value_str in
        string ~attr:Notty.A.(fg white) (Ui_types.truncate_exact (available_width - 4) line)
      ) top_assets in

      (* Combine header and asset lines *)
      let all_lines = header :: asset_lines in
      vcat all_lines

  | Small | Medium | Large ->
      (* Existing layout for larger screens *)
      let show_orders = match screen_size with
        | Ui_types.Small -> false   (* No orders on small screens *)
        | Ui_types.Medium -> true   (* Show orders on tablets *)
        | Ui_types.Large -> true    (* Show orders on large screens *)
        | Mobile -> false (* Won't reach here *)
      in

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
  let separator = string ~attr:Notty.A.(fg (gray 2)) (String.make 66 '-') in  (* Dim gray for gridlines *)

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
    (Printf.sprintf "\n%-12s %7s %7s %6s %10s %6s %10s %6s %11s" "Pair" "Maker%" "Taker%" "Buy" "Buy Cost" "Sell" "Sell Gain" "Total" "Total Gain") in

  let aggregated_orders = aggregate_orders_by_symbol balance_snapshot.open_orders in
  let order_entries = List.map (fun (symbol, (buy_count, sell_count, buy_cost, sell_value, total_gain, maker_fee_opt, taker_fee_opt)) ->
    format_aggregated_orders_entry (symbol, buy_count, sell_count, buy_cost, sell_value, total_gain, maker_fee_opt, taker_fee_opt)
  ) aggregated_orders in

  (* Calculate totals for open orders *)
  let total_buy_count = List.fold_left (fun acc (_, (buy_count, _, _, _, _, _, _)) -> acc + buy_count) 0 aggregated_orders in
  let total_sell_count = List.fold_left (fun acc (_, (_, sell_count, _, _, _, _, _)) -> acc + sell_count) 0 aggregated_orders in
  let total_buy_cost = List.fold_left (fun acc (_, (_, _, buy_cost, _, _, _, _)) -> acc +. buy_cost) 0.0 aggregated_orders in
  let total_sell_value = List.fold_left (fun acc (_, (_, _, _, sell_value, _, _, _)) -> acc +. sell_value) 0.0 aggregated_orders in
  let total_orders_gain = List.fold_left (fun acc (_, (_, _, _, _, total_gain, _, _)) -> acc +. total_gain) 0.0 aggregated_orders in

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

    (* Fees don't aggregate - show "-" for totals *)
    let maker_fee_part = string ~attr:Notty.A.(fg white ++ st bold) (Printf.sprintf "%7s" "-") in
    let taker_fee_part = string ~attr:Notty.A.(fg white ++ st bold) (Printf.sprintf "%7s" "-") in

    hcat [symbol_part; string ~attr:Notty.A.empty " "; maker_fee_part; string ~attr:Notty.A.empty " "; taker_fee_part; string ~attr:Notty.A.empty " "; buy_count_part; string ~attr:Notty.A.empty " "; buy_cost_part; string ~attr:Notty.A.empty " "; sell_count_part; string ~attr:Notty.A.empty " "; sell_value_part; string ~attr:Notty.A.empty " "; total_count_part; string ~attr:Notty.A.empty " "; total_gain_part]
  in

  let orders_total_row = format_orders_total total_buy_count total_sell_count total_buy_cost total_sell_value total_orders_gain in

  (* Add separator and total row to orders list *)
  let orders_separator = string ~attr:Notty.A.(fg (gray 2)) (String.make 73 '-') in
  let orders_list = if order_entries = [] then
    empty
  else
    vcat (order_entries @ [string ~attr:Notty.A.empty ""; orders_separator; orders_total_row]) in

  let orders_section = if balance_snapshot.open_orders = [] then
    vcat [orders_title; orders_header; string ~attr:Notty.A.(fg white) "\n  No open orders"]
  else
    vcat [orders_title; orders_header; orders_list] in

  (* Combine all sections - conditionally include orders based on screen size *)
  let sections = [header; balances_section] in
  let sections = if show_orders then sections @ [orders_section] else sections in
  vcat sections

(** Create reactive balances view *)
let make_balances_view balance_snapshot_var viewport_var =
  let state_var = Lwd.var initial_state in

  (* Reactive UI that updates when data and viewport changes *)
  let ui = Lwd.map2
    (Lwd.map2 (Lwd.get balance_snapshot_var) (Lwd.get state_var) ~f:(fun (snapshot, _) state -> (snapshot, state)))
    (Lwd.get viewport_var)
    ~f:(fun (snapshot, state) (available_width, available_height) ->
      balances_view snapshot state ~available_width ~available_height
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
