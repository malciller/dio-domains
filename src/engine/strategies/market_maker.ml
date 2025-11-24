(**
  Market making strategy indicated by strategy: "MM" in the configuration.

  This strategy implements a market making system that maintains buy and sell orders
  around the current spread, adjusting order placement based on fee levels and
  maintaining balance limits.

  The strategy generates orders and pushes them to a ringbuffer for execution elsewhere.
*)

let section = "market_maker"
let log_interval = 1000000

(** Use common types from Strategy_common module *)
open Strategy_common


(** Utility function to take first n elements from a list *)
let rec take n = function
  | [] -> []
  | x :: xs -> if n <= 0 then [] else x :: take (n - 1) xs

(** Trading configuration type (local definition for strategies) *)
type trading_config = {
  exchange: string;
  symbol: string;
  qty: string;
  min_usd_balance: string option;
  max_exposure: string option;
  strategy: string;
  maker_fee: float option;
  taker_fee: float option;
}

(** Global order ringbuffer - shared across all strategy domains *)
let order_buffer = OrderRingBuffer.create 2048
let order_buffer_mutex = Mutex.create ()

(** Exposed for external access *)
let get_order_buffer () = order_buffer

(** Strategy state per asset *)
type strategy_state = {
  mutable last_buy_order_price: float option;
  mutable last_buy_order_id: string option;
  mutable last_sell_order_price: float option;
  mutable last_sell_order_id: string option;
  mutable pending_orders: (string * order_side * float * float) list;  (* order_id * side * price * timestamp - orders sent but not yet acknowledged *)
  mutable last_cycle: int;
  mutable last_order_time: float;  (* Unix timestamp of last order placement *)
  mutable cancelled_orders: (string * float) list;  (* order_id * timestamp - blacklist of recently cancelled orders *)
  mutable pending_cancellations: (string, float) Hashtbl.t;  (* order_id -> timestamp - track pending cancellation operations *)
  mutable last_cleanup_time: float; (* Last time cleanup was run *)
}

(** Global strategy state store *)
let strategy_states : (string, strategy_state) Hashtbl.t = Hashtbl.create 16
let strategy_states_mutex = Mutex.create ()

(** Get or create strategy state for an asset *)
let get_strategy_state asset_symbol =
  Mutex.lock strategy_states_mutex;
  let state =
    match Hashtbl.find_opt strategy_states asset_symbol with
    | Some state -> state
    | None ->
        let new_state = {
          last_buy_order_price = None;
          last_buy_order_id = None;
          last_sell_order_price = None;
          last_sell_order_id = None;
          pending_orders = [];
          last_cycle = 0;
          last_order_time = 0.0;
          cancelled_orders = [];
          pending_cancellations = Hashtbl.create 16;
          last_cleanup_time = 0.0;
        } in
        Hashtbl.add strategy_states asset_symbol new_state;
        new_state
  in
  Mutex.unlock strategy_states_mutex;
  state

(** Parse configuration values to floats *)
let parse_config_float config value_name default exchange symbol =
  try float_of_string config with
  | Failure _ ->
      Logging.warn_f ~section "Invalid %s value '%s' for %s/%s, using default %.4f"
        value_name config exchange symbol default;
      default

(** Parse configuration values to floats (returns option for optional parameters) *)
let parse_config_float_opt config value_name exchange symbol =
  if config = "" then None
  else
    try Some (float_of_string config) with
    | Failure _ ->
        Logging.warn_f ~section "Invalid %s value '%s' for %s/%s, ignoring parameter"
          value_name config exchange symbol;
        None

(** Round price to appropriate precision for the symbol *)
let round_price price symbol =
  (* Use float-based rounding to avoid string allocation overhead *)
  match Kraken.Kraken_instruments_feed.get_price_increment symbol with
  | Some increment ->
      Float.round (price /. increment) *. increment
  | None ->
      Logging.warn_f ~section "No price increment info for %s, using default rounding" symbol;
      Float.round price  (* Default: 0 decimal places *)

(** Get fee for the asset from the trading config *)
let get_fee_for_asset (asset : trading_config) =
  let maker_fee_str = match asset.maker_fee with Some f -> Printf.sprintf "%.6f" f | None -> "None" in
  let taker_fee_str = match asset.taker_fee with Some f -> Printf.sprintf "%.6f" f | None -> "None" in
  let fee = match asset.maker_fee with
    | Some maker_fee -> maker_fee
    | None -> Option.value asset.taker_fee ~default:0.0026
  in
  Logging.debug_f ~section "Fee config for %s/%s: maker_fee=%s, taker_fee=%s, using_fee=%.6f (post-only orders use maker fee)"
    asset.exchange asset.symbol maker_fee_str taker_fee_str fee;
  fee


(** Check if quantity meets minimum order size for a symbol *)
let meets_min_qty symbol qty =
  match Kraken.Kraken_instruments_feed.get_qty_min symbol with
  | Some min_qty -> qty >= min_qty
  | None -> 
      Logging.warn_f ~section "No qty_min found for %s, assuming qty %.8f is valid" symbol qty;
      true  (* If we can't find min_qty, assume it's valid to avoid blocking *)

(** Create a strategy order for placing a new order *)
let create_place_order asset_symbol side qty price post_only strategy =
  {
    operation = Place;
    order_id = None;
    symbol = asset_symbol;
    side;
    order_type = "limit";
    qty;
    price;
    time_in_force = "GTC";
    post_only;
    userref = Some Strategy_common.strategy_userref_mm;  (* Tag order as MM strategy *)
    strategy;
  }

(** Create a strategy order for amending an existing order *)
let create_amend_order order_id asset_symbol side qty price post_only strategy =
  {
    operation = Amend;
    order_id = Some order_id;
    symbol = asset_symbol;
    side;
    order_type = "limit";
    qty;
    price;
    time_in_force = "GTC";
    post_only;
    userref = None;  (* Amends don't set userref *)
    strategy;
  }

(** Create a strategy order for cancelling an existing order *)
let create_cancel_order order_id asset_symbol strategy =
  {
    operation = Cancel;
    order_id = Some order_id;
    symbol = asset_symbol;
    side = Buy;  (* Not relevant for cancel *)
    order_type = "limit";  (* Not relevant for cancel *)
    qty = 0.0;  (* Not relevant for cancel *)
    price = None;
    time_in_force = "GTC";  (* Not relevant for cancel *)
    post_only = false;
    userref = None;  (* Cancels don't set userref *)
    strategy;
  }

(** Push order to ringbuffer with telemetry *)
let push_order order =
  let operation_str = match order.operation with
    | Place -> "place"
    | Amend -> "amend"
    | Cancel -> "cancel"
  in

  (* Check for duplicate cancellations before pushing *)
  (match order.operation with
   | Cancel ->
       let state = get_strategy_state order.symbol in
       (match order.order_id with
        | Some target_order_id ->
            if Hashtbl.mem state.pending_cancellations target_order_id then begin
              Logging.debug_f ~section "Skipping duplicate cancellation for order %s (already pending)" target_order_id;
              (* Return early without pushing - duplicate cancellation *)
              ()
            end else begin
              (* Not a duplicate, proceed with pushing *)
              Mutex.lock order_buffer_mutex;
              let write_result = OrderRingBuffer.write order_buffer order in
              Mutex.unlock order_buffer_mutex;

              match write_result with
              | Some () ->
                  Logging.debug_f ~section "Pushed %s %s order: %s %.8f @ %s"
                    operation_str
                    (string_of_order_side order.side)
                    order.symbol
                    order.qty
                    (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market");
                  Telemetry.inc_counter (Telemetry.counter "strategy_orders_generated" ()) ();

                  (* Update strategy state *)
                  state.last_order_time <- Unix.time ();
                  (* Add to pending cancellations tracking *)
                  Hashtbl.add state.pending_cancellations target_order_id (Unix.time ());
                  Logging.debug_f ~section "Cancelling order for %s (target: %s)"
                    order.symbol target_order_id
              | None ->
                  Logging.warn_f ~section "Order ringbuffer full, dropped %s %s order for %s"
                    operation_str (string_of_order_side order.side) order.symbol;
                  Telemetry.inc_counter (Telemetry.counter "strategy_orders_dropped" ()) ()
            end
        | None ->
            Logging.warn_f ~section "Cancel operation missing order_id for %s" order.symbol;
            (* Return early without pushing *)
            ())
   | _ ->
       (* For Place and Amend operations, proceed normally *)
       Mutex.lock order_buffer_mutex;
       let write_result = OrderRingBuffer.write order_buffer order in
       Mutex.unlock order_buffer_mutex;

       match write_result with
       | Some () ->
           Logging.debug_f ~section "Pushed %s %s order: %s %.8f @ %s"
             operation_str
             (string_of_order_side order.side)
             order.symbol
             order.qty
             (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market");
           Telemetry.inc_counter (Telemetry.counter "strategy_orders_generated" ()) ();

           (* Update strategy state *)
           let state = get_strategy_state order.symbol in
           state.last_order_time <- Unix.time ();

           (* Handle different operations *)
           (match order.operation with
            | Place ->
                (* Track order as pending - generate temporary ID until we get the real one from exchange *)
                let temp_order_id = Printf.sprintf "pending_%s_%.2f"
                  (string_of_order_side order.side)
                  (Option.value order.price ~default:0.0) in
                let order_price = Option.value order.price ~default:0.0 in
                let timestamp = Unix.time () in
                state.pending_orders <- (temp_order_id, order.side, order_price, timestamp) :: state.pending_orders;
                Logging.debug_f ~section "Added pending order: %s %s @ %.2f for %s"
                  (string_of_order_side order.side) temp_order_id order_price order.symbol;

                (* Update tracked order prices *)
                (match order.side, order.price with
                 | Buy, Some price ->
                     state.last_buy_order_price <- Some price
                 | Sell, Some price ->
                     state.last_sell_order_price <- Some price
                 | _ -> ())
            | Amend ->
                (* For amendments, track as pending but don't add to sell orders yet *)
                let temp_order_id = Printf.sprintf "pending_amend_%s"
                  (Option.value order.order_id ~default:"unknown") in
                let order_price = Option.value order.price ~default:0.0 in
                let timestamp = Unix.time () in
                state.pending_orders <- (temp_order_id, order.side, order_price, timestamp) :: state.pending_orders;
                Logging.debug_f ~section "Added pending amend: %s %s @ %.2f for %s (target: %s)"
                  (string_of_order_side order.side) temp_order_id order_price order.symbol
                  (Option.value order.order_id ~default:"unknown")
            | Cancel -> (* Already handled above *)
                ())
       | None ->
           Logging.warn_f ~section "Order ringbuffer full, dropped %s %s order for %s"
             operation_str (string_of_order_side order.side) order.symbol;
           Telemetry.inc_counter (Telemetry.counter "strategy_orders_dropped" ()) ())

(** Cancel any existing orders at the same price level (duplicate guard) *)
let cancel_duplicate_orders asset_symbol target_price target_side open_orders strategy =
  let duplicates = List.filter (fun (order_id, order_price, qty, side_str, _) ->
    let is_cancelled = List.exists (fun (cancelled_id, _) ->
      cancelled_id = order_id) (get_strategy_state asset_symbol).cancelled_orders in
    not is_cancelled && qty > 0.0 &&
    side_str = (if target_side = Buy then "buy" else "sell") &&
    abs_float (order_price -. target_price) < 0.000001  (* Exact price match *)
  ) open_orders in

  List.iter (fun (order_id, _, _, _, _) ->
    let cancel_order = create_cancel_order order_id asset_symbol strategy in
    push_order cancel_order;
    Logging.debug_f ~section "Cancelling duplicate order %s for %s at price %.8f"
      order_id asset_symbol target_price
  ) duplicates;

  List.length duplicates

(** Main strategy execution function *)
let execute_strategy
    (asset : trading_config)
    (current_price : float option)
    (top_of_book : (float * float * float * float) option)
    (asset_balance : float option)
    (quote_balance : float option)
    (_open_buy_count : int)
    (_open_sell_count : int)
    (open_orders : (string * float * float * string * int option) list)  (* order_id, price, remaining_qty, side, userref *)
    (cycle : int) =

  (* Only log every x iterations to reduce log volume *)
  let should_log = cycle mod log_interval = 0 in

  let state = get_strategy_state asset.symbol in

  let now = Unix.time () in

  (* Efficient cleanup logic - run every cycle but use scan-and-remove to avoid allocation *)
  (* This prevents accumulation while maintaining HFT responsiveness *)
  
  (* Clean up stale pending orders (older than 5 seconds) and enforce hard limit of 50 *)
  let original_count = List.length state.pending_orders in
  state.pending_orders <- List.filter (fun (order_id, _, _, timestamp) ->
    let age = now -. timestamp in
    if age > 5.0 then begin  (* Reduced from 10 to 5 seconds *)
      if should_log then Logging.warn_f ~section "Removing stale pending order %s for %s (age: %.1fs)" order_id asset.symbol age;
      false
    end else
      true
  ) state.pending_orders;

  (* Enforce hard limit of 50 pending orders to prevent memory growth *)
  if List.length state.pending_orders > 50 then begin
    let excess = List.length state.pending_orders - 50 in
    state.pending_orders <- take 50 state.pending_orders;  (* Keep most recent 50 *)
    if should_log then Logging.warn_f ~section "Truncated %d excess pending orders for %s (kept 50)" excess asset.symbol;
  end;

  (* Log if we cleaned up many orders *)
  let cleaned_count = original_count - List.length state.pending_orders in
  if cleaned_count > 0 && should_log then
    Logging.debug_f ~section "Cleaned up %d pending orders for %s" cleaned_count asset.symbol;

  (* Clean up old cancelled orders from blacklist (older than 15 seconds) and enforce hard limit *)
  let original_cancelled_count = List.length state.cancelled_orders in
  state.cancelled_orders <- List.filter (fun (_, timestamp) ->
    now -. timestamp < 15.0  (* Reduced from 30 to 15 seconds *)
  ) state.cancelled_orders;

  (* Enforce hard limit of 20 cancelled orders to prevent memory growth *)
  if List.length state.cancelled_orders > 20 then begin
    let excess = List.length state.cancelled_orders - 20 in
    state.cancelled_orders <- take 20 state.cancelled_orders;  (* Keep most recent 20 *)
    if should_log then Logging.warn_f ~section "Truncated %d excess cancelled orders for %s (kept 20)" excess asset.symbol;
  end;

  (* Clean up old pending cancellations (older than 30 seconds) *)
  (* Optimized scan and remove to avoid Hashtbl copy *)
  let to_remove = ref [] in
  Hashtbl.iter (fun order_id timestamp ->
    if now -. timestamp > 30.0 then
      to_remove := order_id :: !to_remove
  ) state.pending_cancellations;

  List.iter (fun order_id ->
    if should_log then Logging.debug_f ~section "Removing stale pending cancellation %s for %s" order_id asset.symbol;
    Hashtbl.remove state.pending_cancellations order_id
  ) !to_remove;

  (* Log if we cleaned up many cancelled orders *)
  let cleaned_cancelled_count = original_cancelled_count - List.length state.cancelled_orders in
  if cleaned_cancelled_count > 0 && should_log then
    Logging.debug_f ~section "Cleaned up %d cancelled orders for %s" cleaned_cancelled_count asset.symbol;


  match current_price, top_of_book with
  | Some price, Some (bid, _bid_size, ask, _ask_size) ->
      begin
        (* Debug: Log the raw bid/ask values we received *)
        if should_log then Logging.debug_f ~section "DEBUG Asset [%s/%s]: Received bid=%.8f, ask=%.8f, price=%.8f"
          asset.exchange asset.symbol bid ask price;

        (* Parse configuration values *)
        let qty = parse_config_float asset.qty "qty" 0.001 asset.exchange asset.symbol in
        let min_usd_balance_opt = match asset.min_usd_balance with
          | Some balance_str -> parse_config_float_opt balance_str "min_usd_balance" asset.exchange asset.symbol
          | None -> None
        in
        let max_exposure_opt = match asset.max_exposure with
          | Some exposure_str -> parse_config_float_opt exposure_str "max_exposure" asset.exchange asset.symbol
          | None -> None
        in

        (* Get fee for the asset - prefer cache, fallback to config *)
        let fee = match Fee_cache.get_maker_fee ~exchange:asset.exchange ~symbol:asset.symbol with
          | Some cached_fee -> cached_fee
          | None -> get_fee_for_asset asset
        in

        (* Calculate current spread and exposure *)
        let spread = ask -. bid in
        let mid_price = (bid +. ask) /. 2.0 in
        let spread_pct = if mid_price > 0.0 then spread /. mid_price else 0.0 in

        (* Calculate available balances (excluding amounts tied up in open orders) *)
        let available_asset_balance = match asset_balance with
          | Some total_balance ->
              (* Subtract remaining quantities from open sell orders *)
              let locked_in_sells = List.fold_left (fun acc (_, _, remaining_qty, side_str, _) ->
                if side_str = "sell" then acc +. remaining_qty else acc
              ) 0.0 open_orders in
              Some (total_balance -. locked_in_sells)
          | None -> None
        in

        let available_quote_balance = match quote_balance with
          | Some total_balance ->
              (* Subtract remaining value from open buy orders *)
              let locked_in_buys = List.fold_left (fun acc (_, order_price, remaining_qty, side_str, _) ->
                if side_str = "buy" then acc +. (order_price *. remaining_qty) else acc
              ) 0.0 open_orders in
              Some (total_balance -. locked_in_buys)
          | None -> None
        in

        let exposure_value = match available_asset_balance with
          | Some ab -> ab *. mid_price
          | None -> 0.0
        in

        (* Debug: Log the calculated values *)
        if should_log then Logging.debug_f ~section "DEBUG Asset [%s/%s]: spread=%.8f, spread_pct=%.8f, fee=%.8f, exposure=%.2f"
          asset.exchange asset.symbol spread spread_pct fee exposure_value;

        (* Count open buy orders for this asset - needed for balance check *)
        let open_buy_orders = List.filter (fun (order_id, _, qty, side_str, _) ->
          let is_cancelled = List.exists (fun (cancelled_id, _) -> cancelled_id = order_id) state.cancelled_orders in
          side_str = "buy" && not is_cancelled && qty > 0.0
        ) open_orders in

        let open_buy_count = List.length open_buy_orders in

        (* Check balance limits - PAUSE/RESUME LOGIC (only apply constraints if parameters are provided) *)
        (* Consider if we can actually place orders without violating constraints *)
        let usd_balance_ok = match min_usd_balance_opt, available_quote_balance with
          | Some min_balance, Some qb ->
              (* Check if we can place a buy order without going below minimum *)
              (* If we already have a buy order open, don't double-count its cost *)
              let buy_cost = if open_buy_count > 0 then 0.0 else bid *. qty in
              qb -. buy_cost >= min_balance
          | Some _, None -> false  (* If min_balance is set but we have no quote balance data, fail *)
          | None, _ -> true        (* If no min_balance constraint, always pass *)
        in
        let exposure_ok = match max_exposure_opt, available_asset_balance with
          | Some max_exp, Some ab ->
              (* Check if we can place a sell order without exceeding max exposure *)
              (* After selling qty, our exposure would be: (ab - qty) * mid_price <= max_exp *)
              let new_exposure = (ab -. qty) *. mid_price in
              new_exposure <= max_exp
          | Some _, None -> false  (* If max_exp is set but we have no asset balance data, fail *)
          | None, _ -> true        (* If no max_exposure constraint, always pass *)
        in

        (* Calculate projected balances for better logging *)
        let projected_buy_balance = match available_quote_balance with
          | Some qb -> Some (qb -. (bid *. qty))
          | None -> None
        in
        let projected_sell_exposure = match available_asset_balance with
          | Some ab -> Some ((ab -. qty) *. mid_price)
          | None -> None
        in

        if should_log then Logging.info_f ~section "Strategy check for %s: open_buy_count=%d, current_exposure=%.2f, projected_sell_exposure=%s, max_exposure=%s, current_usd=%s, projected_buy_usd=%s, min_usd=%s, exposure_ok=%B, usd_ok=%B"
          asset.symbol open_buy_count exposure_value
          (match projected_sell_exposure with Some pe -> Printf.sprintf "%.2f" pe | None -> "N/A")
          (match max_exposure_opt with Some me -> Printf.sprintf "%.2f" me | None -> "unlimited")
          (match available_quote_balance with Some qb -> string_of_float qb | None -> "None")
          (match projected_buy_balance with Some pb -> Printf.sprintf "%.2f" pb | None -> "N/A")
          (match min_usd_balance_opt with Some mb -> Printf.sprintf "%.2f" mb | None -> "unlimited")
          exposure_ok usd_balance_ok;

        (* EXPOSURE & BALANCE CHECKS (Pause/Protect if Failed) *)
        if not exposure_ok || not usd_balance_ok then begin
          (* Pause strategy: Cancel all open buy orders for this asset *)
          if should_log then Logging.debug_f ~section "Balance/exposure limits exceeded for %s (exposure_ok=%B, usd_ok=%B), pausing strategy"
            asset.symbol exposure_ok usd_balance_ok;

          (* Sync strategy state with actual open orders first *)
          state.last_buy_order_price <- None;
          state.last_buy_order_id <- None;
          state.last_sell_order_price <- None;
          state.last_sell_order_id <- None;

          List.iter (fun (order_id, order_price, qty, side_str, _) ->
            let is_cancelled = List.exists (fun (cancelled_id, _) -> cancelled_id = order_id) state.cancelled_orders in
            if not is_cancelled && qty > 0.0 then
              if side_str = "buy" then
                (state.last_buy_order_price <- Some order_price;
                 state.last_buy_order_id <- Some order_id)
              else
                (state.last_sell_order_price <- Some order_price;
                 state.last_sell_order_id <- Some order_id)
          ) open_orders;

          (* Cancel all open buy orders *)
          (match state.last_buy_order_id with
           | Some buy_id ->
               let cancel_order = create_cancel_order buy_id asset.symbol "MM" in
               push_order cancel_order;
               if should_log then Logging.debug_f ~section "Cancelling buy order due to pause: %s for %s" buy_id asset.symbol
           | None -> ());

          (* Place/ensure sell orders for all available asset_balance at or above best_ask *)
          (match available_asset_balance with
           | Some ab when ab > 0.0 ->
               if meets_min_qty asset.symbol ab then begin
                 let sell_price = round_price ask asset.symbol in
                 let sell_order = create_place_order asset.symbol Sell ab (Some sell_price) true "MM" in
                 push_order sell_order;
                 if should_log then Logging.debug_f ~section "Placed emergency sell order: %.8f @ %.2f for %s" ab sell_price asset.symbol
               end
               (* Exit quietly if below minimum - don't spam logs for dust accumulation *)
           | _ -> ());

          (* Stop execution for this trigger - will re-check on next trigger *)
          state.last_cycle <- cycle

        end else begin
          (* Both checks pass: Proceed to Step 3 - BUY ORDER MANAGEMENT *)

          (* CRITICAL: Check for pending orders before placing new ones - prevents duplicate spam *)
          if state.pending_orders <> [] then begin
            (* Only log every 100,000 iterations to avoid spam *)
            if should_log then begin
              Logging.debug_f ~section "Waiting for %d pending orders before placing new orders for %s: [%s]"
                (List.length state.pending_orders) asset.symbol
                (String.concat "; " (List.map (fun (order_id, side, price, _) ->
                  Printf.sprintf "%s %s@%.2f" (string_of_order_side side) order_id price
                ) state.pending_orders))
            end;
            (* Update cycle counter and exit early *)
            state.last_cycle <- cycle
          end else begin

          (* Sync strategy state with actual open orders *)
          state.last_buy_order_price <- None;
          state.last_buy_order_id <- None;
          state.last_sell_order_price <- None;
          state.last_sell_order_id <- None;

          List.iter (fun (order_id, order_price, qty, side_str, _) ->
            let is_cancelled = List.exists (fun (cancelled_id, _) -> cancelled_id = order_id) state.cancelled_orders in
            if not is_cancelled && qty > 0.0 then
              if side_str = "buy" then
                (state.last_buy_order_price <- Some order_price;
                 state.last_buy_order_id <- Some order_id)
              else
                (state.last_sell_order_price <- Some order_price;
                 state.last_sell_order_id <- Some order_id)
          ) open_orders;

          (* open_buy_count already calculated earlier for balance checks *)
          if should_log then Logging.debug_f ~section "Buy order management for %s: open_buy_count=%d" asset.symbol open_buy_count;

          (* BUY ORDER MANAGEMENT - Aim for Exactly One Active Buy at Profitable Level *)
          if open_buy_count > 1 then begin
            (* If >1 open buy: Cancel all open buys. Stop—next trigger will replace. *)
            if should_log then Logging.debug_f ~section "Cancelling %d excess buy orders for %s" open_buy_count asset.symbol;
            List.iter (fun (order_id, _, _, _, _) ->
              let cancel_order = create_cancel_order order_id asset.symbol "MM" in
              push_order cancel_order
            ) open_buy_orders;
            (* Next trigger will place new orders *)

          end else if open_buy_count = 0 then begin
            (* If 0 open buys: Determine buy price based on fee and place new pair *)
            if should_log then Logging.debug_f ~section "No buy orders for %s, placing new pair" asset.symbol;

            (* Calculate buy price based on fee logic *)
            let buy_price_raw = if fee = 0.0 then bid else ask -. (fee *. 2.0 +. 0.0001) in
            let buy_price = round_price buy_price_raw asset.symbol in

            (* Profitability Guard: Never place/amend if calculated spread < required (2x fee + 0.01%) *)
            (* For zero-fee assets, any spread is profitable, so skip this check *)
            let profitability_ok = if fee = 0.0 then true else begin
              let required_spread = fee *. 2.0 +. 0.0001 in
              let calculated_spread = ask -. buy_price in
              calculated_spread >= required_spread
            end in
            if profitability_ok then begin
              (* Post-Only Safety: Before place: Ensure buy <= best_bid, sell >= best_ask *)
              if buy_price <= bid && ask >= ask then begin  (* ask >= ask is always true, but kept for clarity *)

                (* Balance checks before placing orders *)
                let can_place_buy = match min_usd_balance_opt, available_quote_balance with
                  | Some min_bal, Some qb ->
                      let required_quote = buy_price *. qty in
                      qb >= required_quote && qb -. required_quote >= min_bal
                  | Some _, None -> false  (* Can't check balance if no data *)
                  | None, _ -> true        (* No constraint *)
                in

                let can_place_sell = meets_min_qty asset.symbol qty in

                (* Place SELL order first, then BUY order *)
                if can_place_sell then begin
                  let sell_price = round_price ask asset.symbol in
                  (* Cancel any duplicates at the same price level first *)
                  let _ = cancel_duplicate_orders asset.symbol sell_price Sell open_orders "MM" in
                  let sell_order = create_place_order asset.symbol Sell qty (Some sell_price) true "MM" in
                  push_order sell_order;

                  (* Place buy order only if it meets all criteria *)
                  if can_place_buy then begin
                    (* Cancel any duplicates at the same price level first *)
                    let _ = cancel_duplicate_orders asset.symbol buy_price Buy open_orders "MM" in
                    let buy_order = create_place_order asset.symbol Buy qty (Some buy_price) true "MM" in
                    push_order buy_order;

                    if should_log then Logging.info_f ~section "Placed order pair for %s: buy %.8f @ %.2f, sell %.8f @ %.2f (fee=%.6f)"
                      asset.symbol qty buy_price qty sell_price fee;
                  end else begin
                    if should_log then Logging.info_f ~section "Placed sell-only for %s: sell %.8f @ %.2f (buy balance constraints failed)"
                      asset.symbol qty sell_price;
                  end
                end else begin
                  if should_log then Logging.debug_f ~section "Did not place sell order for %s: qty below minimum"
                    asset.symbol;
                end
              end else begin
                if should_log then Logging.warn_f ~section "Post-only violation for %s: buy_price=%.8f > bid=%.8f or sell_price=%.8f < ask=%.8f"
                  asset.symbol buy_price bid (round_price ask asset.symbol) ask;
              end
            end else begin
              (* This should never happen for zero-fee assets since profitability_ok is always true *)
              if should_log then Logging.warn_f ~section "Profitability guard failed for %s (fee=%.6f)"
                asset.symbol fee;
            end

          end else begin
            (* If exactly 1 open buy: Check if current buy price matches required level *)
            match state.last_buy_order_price, state.last_buy_order_id with
            | Some current_buy_price, Some buy_order_id ->
                (* Re-calculate required buy price as in "0 open buys" *)
                (* Use same fee logic as above *)
                let amendment_fee = match Fee_cache.get_maker_fee ~exchange:asset.exchange ~symbol:asset.symbol with
                  | Some cached_fee -> cached_fee
                  | None -> fee  (* Use the fee already calculated above *)
                in
                let required_buy_price_raw = if amendment_fee = 0.0 then bid else ask -. (amendment_fee *. 2.0 +. 0.0001) in
                let required_buy_price = round_price required_buy_price_raw asset.symbol in

                if should_log then Logging.debug_f ~section "Checking buy order for %s: current=%.8f, required=%.8f, bid=%.8f, ask=%.8f, fee=%.6f"
                  asset.symbol current_buy_price required_buy_price bid ask amendment_fee;

                (* Check if price matches required level *)
                let price_diff = abs_float (required_buy_price -. current_buy_price) in
                let min_move_threshold = match Kraken.Kraken_instruments_feed.get_price_increment asset.symbol with
                  | Some increment -> increment
                  | None -> 0.01
                in

                (* Check if this order is already being amended *)
                let expected_amend_id = "pending_amend_" ^ buy_order_id in
                let is_being_amended = List.exists (fun (id, _, _, _) ->
                  id = expected_amend_id
                ) state.pending_orders in

                if not is_being_amended && price_diff >= min_move_threshold *. 0.9 then begin
                  (* Mismatched: Amend buy to the required price *)
                  (* Profitability and Post-Only checks *)
                  (* For zero-fee assets, any spread is profitable, so skip profitability check *)
                  let profitability_ok = if amendment_fee = 0.0 then true else begin
                    let required_spread = amendment_fee *. 2.0 +. 0.0001 in
                    let calculated_spread = ask -. required_buy_price in
                    calculated_spread >= required_spread
                  end in

                  (* Balance check for amendment *)
                  let amendment_balance_ok = match min_usd_balance_opt, available_quote_balance with
                    | Some min_bal, Some qb ->
                        (* Add back funds locked in the current order being amended *)
                        let current_locked = current_buy_price *. qty in
                        let effective_qb = qb +. current_locked in
                        let required_quote = required_buy_price *. qty in
                        effective_qb >= required_quote && effective_qb -. required_quote >= min_bal
                    | Some _, None -> false  (* Can't check balance if no data *)
                    | None, _ -> true        (* No constraint *)
                  in

                  if should_log then Logging.debug_f ~section "Amendment checks for %s: profit_ok=%B, post_only_ok=%B, balance_ok=%B, price=%.8f, fee=%.6f"
                    asset.symbol profitability_ok (required_buy_price <= bid) amendment_balance_ok required_buy_price amendment_fee;

                  if profitability_ok && required_buy_price <= bid && amendment_balance_ok then begin
                    (* Cancel any duplicates at the new price level first *)
                    let _ = cancel_duplicate_orders asset.symbol required_buy_price Buy open_orders "MM" in
                    let amend_order = create_amend_order buy_order_id asset.symbol Buy qty (Some required_buy_price) true "MM" in
                    push_order amend_order;
                    state.last_buy_order_price <- Some required_buy_price;
                    if should_log then Logging.info_f ~section "Amended buy order for %s: %.8f -> %.8f (spread=%.2f%%, reason=book shift)"
                      asset.symbol current_buy_price required_buy_price (spread_pct *. 100.0);
                  end else begin
                    if Hashtbl.mem state.pending_cancellations buy_order_id then begin
                      if should_log then Logging.debug_f ~section "Buy order %s already pending cancellation, skipping retry" buy_order_id
                    end else begin
                      if should_log then Logging.warn_f ~section "Cannot amend buy order for %s: checks failed (profit=%B, post_only=%B, balance=%B) - CANCELLING"
                        asset.symbol profitability_ok (required_buy_price <= bid) amendment_balance_ok;
                      let cancel_order = create_cancel_order buy_order_id asset.symbol "MM" in
                      push_order cancel_order
                    end
                  end
                end else begin
                  (* Matched: No action needed *)
                  if should_log then Logging.debug_f ~section "Buy order for %s already at correct price: %.8f" asset.symbol current_buy_price;
                end
            | _ -> Logging.warn_f ~section "Buy order tracking inconsistent for %s" asset.symbol
          end;
          end  (* Close pending_orders check *)
        end;  (* Close balance checks *)

        (* Update cycle counter *)
        state.last_cycle <- cycle

      end
  | _ ->
      begin
        Logging.debug_f ~section "No price or top-of-book data available for %s" asset.symbol;
        (* Update cycle counter *)
        state.last_cycle <- cycle
      end

(** Handle order placement success - update pending order status *)
let handle_order_acknowledged asset_symbol order_id side price =
  let state = get_strategy_state asset_symbol in
  (* Remove from pending orders - match by side and approximate price, or by amend order_id *)
  state.pending_orders <- List.filter (fun (pending_id, s, p, _) ->
    (* Match regular orders by side and price, or amend orders by order_id *)
    let matches_side_price = s = side && abs_float (p -. price) < 0.01 in
    let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                       String.sub pending_id 14 (String.length pending_id - 14) = order_id in
    not (matches_side_price || matches_amend)
  ) state.pending_orders;

  (* Update buy/sell order tracking if this is a buy/sell order acknowledgment *)
  (match side with
   | Buy ->
       state.last_buy_order_id <- Some order_id;
       Logging.debug_f ~section "Updated buy order ID tracking: %s @ %.2f for %s" order_id price asset_symbol
   | Sell ->
       state.last_sell_order_id <- Some order_id;
       Logging.debug_f ~section "Updated sell order ID tracking: %s @ %.2f for %s" order_id price asset_symbol);

  Logging.debug_f ~section "Order acknowledged and removed from pending: %s %s @ %.2f for %s"
    (string_of_order_side side) order_id price asset_symbol

(** Handle order placement failure - remove from pending and potentially trigger re-evaluation *)
let handle_order_rejected asset_symbol side price =
  let state = get_strategy_state asset_symbol in
  (* Remove from pending orders *)
  state.pending_orders <- List.filter (fun (_, s, p, _) ->
    not (s = side && abs_float (p -. price) < 0.01)  (* Allow small price difference *)
  ) state.pending_orders;
  Logging.debug_f ~section "Order rejected and removed from pending: %s @ %.2f for %s"
    (string_of_order_side side) price asset_symbol

(** Handle order cancellation - remove from pending and tracked orders *)
let handle_order_cancelled asset_symbol order_id =
  let state = get_strategy_state asset_symbol in

  (* Add to cancelled orders blacklist to prevent re-adding during sync *)
  let now = Unix.time () in
  state.cancelled_orders <- (order_id, now) :: state.cancelled_orders;

  (* Remove from pending orders if it's there *)
  let original_pending_count = List.length state.pending_orders in
  state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
    pending_id <> order_id
  ) state.pending_orders;
  let removed_pending = original_pending_count - List.length state.pending_orders in

  (* If this was a tracked buy/sell order, clear it *)
  (match state.last_buy_order_id with
   | Some buy_id when buy_id = order_id ->
       state.last_buy_order_id <- None;
       state.last_buy_order_price <- None;
       Logging.info_f ~section "Cancelled buy order %s removed from tracking for %s (blacklisted)" order_id asset_symbol
   | _ -> ());
  (match state.last_sell_order_id with
   | Some sell_id when sell_id = order_id ->
       state.last_sell_order_id <- None;
       state.last_sell_order_price <- None;
       Logging.info_f ~section "Cancelled sell order %s removed from tracking for %s (blacklisted)" order_id asset_symbol
   | _ -> ());

  Logging.info_f ~section "Order cancelled and cleaned up: %s for %s (removed %d pending, added to blacklist)"
    order_id asset_symbol removed_pending

(** Clean up pending cancellation tracking for a completed order *)
let cleanup_pending_cancellation asset_symbol order_id =
  let state = get_strategy_state asset_symbol in
  Hashtbl.remove state.pending_cancellations order_id

(** Get pending orders from ringbuffer for processing *)
let get_pending_orders max_orders =
  Mutex.lock order_buffer_mutex;
  let orders =
    Fun.protect
      ~finally:(fun () -> Mutex.unlock order_buffer_mutex)
      (fun () ->
         let orders = ref [] in
         let count = ref 0 in
         while !count < max_orders do
           match OrderRingBuffer.read order_buffer with
           | Some order ->
               orders := order :: !orders;
               incr count
           | None -> count := max_orders  (* Exit loop *)
         done;
         List.rev !orders)
  in
  orders

(** Initialize strategy module *)
let init () =
  Logging.info_f ~section "Market Maker strategy initialized with order buffer size 4096";
  Fee_cache.init ();
  Random.self_init ()

(** Strategy module interface *)
module Strategy = struct
  (** Clean up strategy state for a symbol when domain stops *)
  let cleanup_strategy_state symbol =
    Mutex.lock strategy_states_mutex;
    (match Hashtbl.find_opt strategy_states symbol with
     | Some _ ->
         Hashtbl.remove strategy_states symbol;
         Logging.debug_f ~section "Removed strategy state for %s" symbol
     | None -> ());
    Mutex.unlock strategy_states_mutex

  let execute = execute_strategy
  let get_pending_orders = get_pending_orders
  let handle_order_acknowledged = handle_order_acknowledged
  let handle_order_rejected = handle_order_rejected
  let handle_order_cancelled = handle_order_cancelled
  let cleanup_pending_cancellation = cleanup_pending_cancellation
  let cleanup_strategy_state = cleanup_strategy_state
  let init = init
end