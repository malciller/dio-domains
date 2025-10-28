(*
  Suicide Grid Strategy Implementation

  This strategy implements a grid trading system where:
  - Only 1 open buy order may exist at once
  - Many sell orders may exist at a time
  - Orders are placed at grid intervals from current price
  - Buy orders trail price movements, sell orders are placed above current price

  The strategy generates orders and pushes them to a ringbuffer for execution elsewhere.
*)


let section = "suicide_grid"

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
  grid_interval: string;
  sell_mult: string;
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
  mutable open_sell_orders: (string * float) list;  (* order_id * price *)
  mutable pending_orders: (string * order_side * float) list;  (* order_id * side * price - orders sent but not yet acknowledged *)
  mutable last_cycle: int;
  mutable last_order_time: float;  (* Unix timestamp of last order placement *)
  mutable cancelled_orders: (string * float) list;  (* order_id * timestamp - blacklist of recently cancelled orders *)
  mutable pending_cancellations: (string, float) Hashtbl.t;  (* order_id -> timestamp - track pending cancellation operations *)
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
          open_sell_orders = [];
          pending_orders = [];
          last_cycle = 0;
          last_order_time = 0.0;
          cancelled_orders = [];
          pending_cancellations = Hashtbl.create 16;
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

(** Round price to appropriate precision for the symbol *)
let round_price price symbol =
  (* Fetch price precision from Kraken instruments feed cache *)
  match Kraken.Kraken_instruments_feed.get_precision_info symbol with
  | Some (price_precision, qty_precision) ->
      Logging.debug_f ~section "Rounding price %.8f for %s with precision %d (qty_precision: %d)" price symbol price_precision qty_precision;
      (* Clamp to exact decimal precision by formatting as string and parsing back *)
      (* This ensures we never exceed the allowed decimal places *)
      let clamped_str = Printf.sprintf "%.*f" price_precision price in
      let rounded_price = (try float_of_string clamped_str
       with Failure _ ->
         Logging.warn_f ~section "Failed to parse clamped price '%s' for %s, using original" clamped_str symbol;
         price) in
      Logging.debug_f ~section "Rounded price %.8f -> '%s' -> %.8f for %s" price clamped_str rounded_price symbol;
      rounded_price
  | None ->
      Logging.warn_f ~section "No price precision info for %s, using default rounding" symbol;
      Float.round price  (* Default: 0 decimal places *)

(** Get minimum price increment for the symbol *)
let get_price_increment symbol =
  (* Fetch price increment from Kraken instruments feed cache synchronously *)
  match Kraken.Kraken_instruments_feed.get_price_increment symbol with
  | Some increment -> increment
  | None ->
      Logging.warn_f ~section "No price increment info for %s, using default 0.01" symbol;
      0.01  (* Default fallback *)

(** Calculate grid price based on current price and interval *)
let calculate_grid_price current_price grid_interval_pct is_above symbol =
  let interval = current_price *. (grid_interval_pct /. 100.0) in
  let raw_price = if is_above then current_price +. interval else current_price -. interval in
  round_price raw_price symbol

(** Check if we can place a buy order based on quote balance *)
let can_place_buy_order (_qty : float) quote_balance quote_needed =
  quote_balance >= quote_needed

(** Check if we can place a sell order based on asset balance *)
let can_place_sell_order (_qty : float) (_sell_mult : float) asset_balance asset_needed =
  asset_balance >= asset_needed

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
    userref = Some Strategy_common.strategy_userref_grid;  (* Tag order as Grid strategy *)
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

(** Create a strategy order - backwards compatibility *)
let create_order asset_symbol side qty price post_only =
  create_place_order asset_symbol side qty price post_only "Grid"

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
                state.pending_orders <- (temp_order_id, order.side, order_price) :: state.pending_orders;
                Logging.debug_f ~section "Added pending order: %s %s @ %.2f for %s"
                  (string_of_order_side order.side) temp_order_id order_price order.symbol;

                (* Track sell orders in state for grid logic *)
                (match order.side, order.price with
                 | Sell, Some price ->
                     (* Add sell order to tracking list - use temporary ID for now *)
                     let order_id = temp_order_id in
                     state.open_sell_orders <- (order_id, price) :: state.open_sell_orders;
                     Logging.debug_f ~section "Tracking sell order %s @ %.2f for %s" order_id price order.symbol
                 | _ -> ())
            | Amend ->
                (* For amendments, track as pending but don't add to sell orders yet *)
                let temp_order_id = Printf.sprintf "pending_amend_%s"
                  (Option.value order.order_id ~default:"unknown") in
                let order_price = Option.value order.price ~default:0.0 in
                state.pending_orders <- (temp_order_id, order.side, order_price) :: state.pending_orders;
                Logging.debug_f ~section "Added pending amend: %s %s @ %.2f for %s (target: %s)"
                  (string_of_order_side order.side) temp_order_id order_price order.symbol
                  (Option.value order.order_id ~default:"unknown")
            | Cancel -> (* Already handled above *)
                ())
       | None ->
           Logging.warn_f ~section "Order ringbuffer full, dropped %s %s order for %s"
             operation_str (string_of_order_side order.side) order.symbol;
           Telemetry.inc_counter (Telemetry.counter "strategy_orders_dropped" ()) ())

(** Main strategy execution function *)
let execute_strategy
    (asset : trading_config)
    (current_price : float option)
    (top_of_book : (float * float * float * float) option)
    (asset_balance : float option)
    (quote_balance : float option)
    (open_buy_count : int)
    (_open_sell_count : int)
    (open_orders : (string * float * float * string * int option) list)  (* order_id, price, remaining_qty, side, userref *)
    (cycle : int) =

  (* Only execute strategy periodically to avoid excessive order generation *)
  if cycle mod 100 <> 0 then () else begin

  let state = get_strategy_state asset.symbol in

  (* Throttle order placement - wait at least 1 second between orders *)
  let now = Unix.time () in
  if now -. state.last_order_time < 1.0 then () else begin

  match current_price, top_of_book with
  | None, _ -> ()  (* No price data available yet *)
  | Some price, _ ->

      (* Sync strategy state with actual open orders from exchange *)
      (* Clear existing tracked orders *)
      state.open_sell_orders <- [];
      state.last_buy_order_price <- None;
      state.last_buy_order_id <- None;
      (* Note: We don't clear pending_orders here - they should be managed by order placement responses *)

      (* Clean up old cancelled orders from blacklist (older than 15 seconds) and enforce hard limit *)
      let now = Unix.time () in
      state.cancelled_orders <- List.filter (fun (_, timestamp) ->
        now -. timestamp < 15.0  (* Reduced from 30 to 15 seconds *)
      ) state.cancelled_orders;

      (* Enforce hard limit of 20 cancelled orders to prevent memory growth *)
      if List.length state.cancelled_orders > 20 then begin
        let excess = List.length state.cancelled_orders - 20 in
        state.cancelled_orders <- take 20 state.cancelled_orders;  (* Keep most recent 20 *)
        Logging.warn_f ~section "Truncated %d excess cancelled orders for %s (kept 20)" excess asset.symbol;
      end;

      (* Clean up old pending cancellations (older than 30 seconds) *)
      let pending_cancellations = Hashtbl.fold (fun order_id timestamp acc ->
        if now -. timestamp > 30.0 then begin
          Logging.debug_f ~section "Removing stale pending cancellation %s for %s (age: %.1fs)" order_id asset.symbol (now -. timestamp);
          acc
        end else
          (order_id, timestamp) :: acc
      ) state.pending_cancellations [] in

      (* Clear and repopulate the hashtable with only non-stale entries *)
      Hashtbl.clear state.pending_cancellations;
      List.iter (fun (order_id, timestamp) ->
        Hashtbl.add state.pending_cancellations order_id timestamp
      ) pending_cancellations;

      (* Update with real orders from exchange - open_orders is (order_id, price, qty, side, userref) list *)
      (* Filter out recently cancelled orders to avoid race condition *)
      let buy_orders = ref [] in
      let sell_orders = ref [] in

      List.iter (fun (order_id, order_price, qty, side_str, userref_opt) ->
        (* Skip if this order was recently cancelled *)
        let is_cancelled = List.exists (fun (cancelled_id, _) -> cancelled_id = order_id) state.cancelled_orders in
        if not is_cancelled && qty > 0.0 then (* Only count orders with remaining quantity *)
          (* Use actual order side from exchange, not price-based classification *)
          if side_str = "buy" then
            (* For buy orders, only include those with the grid tag *)
            (match userref_opt with
             | Some userref when userref = Strategy_common.strategy_userref_grid ->
                 buy_orders := (order_id, order_price) :: !buy_orders
             | _ -> ())
          else
            (* For sell orders, include ALL open sell orders regardless of tag *)
            sell_orders := (order_id, order_price) :: !sell_orders
      ) open_orders;

      (* Set the buy order price and ID (take the highest buy order if multiple) *)
      (match !buy_orders with
       | [] -> ()
       | orders ->
           let (best_order_id, best_price) = List.fold_left (fun (acc_id, acc_price) (order_id, price) ->
             if price > acc_price then (order_id, price) else (acc_id, acc_price)
           ) (List.hd orders) (List.tl orders) in
           state.last_buy_order_price <- Some best_price;
           state.last_buy_order_id <- Some best_order_id);

      (* Set sell orders *)
      state.open_sell_orders <- !sell_orders;

      Logging.debug_f ~section "Synced %d open orders for %s: %d buys, %d sells"
        (List.length open_orders) asset.symbol
        (List.length !buy_orders) (List.length !sell_orders);

      (* Parse configuration values *)
      let qty = parse_config_float asset.qty "qty" 0.001 asset.exchange asset.symbol in
      let grid_interval = parse_config_float asset.grid_interval "grid_interval" 1.0 asset.exchange asset.symbol in
      let sell_mult = parse_config_float asset.sell_mult "sell_mult" 1.0 asset.exchange asset.symbol in

      (* Calculate required balances *)
      let quote_needed = price *. qty in
      let asset_needed = qty *. sell_mult in

  (* Log current balance status for debugging *)
  Logging.debug_f ~section "Balance check for %s: asset_balance=%.8f, quote_balance=%.2f, needed asset=%.8f, needed quote=%.2f"
    asset.symbol
    (Option.value asset_balance ~default:0.0)
    (Option.value quote_balance ~default:0.0)
    asset_needed quote_needed;

  (* Check for missing balance data - stale but present balance data is OK *)
  (* Note: Supervisor monitors WebSocket health via heartbeats *)
  let is_stale = asset.exchange = "kraken" && (
    match asset_balance, quote_balance with
    | None, None ->
        (* No balance data at all - can't trade without balance info *)
        if state.last_cycle <> cycle then
          Logging.debug_f ~section "No balance data available for %s - waiting for balance feed" asset.symbol;
        true
    | Some _, None | None, Some _ ->
        (* Partial balance data - may indicate feed issue *)
        if state.last_cycle <> cycle then
          Logging.debug_f ~section "Partial balance data for %s - waiting for complete data" asset.symbol;
        true
    | Some ab, Some qb ->
        (* Have balance data - check if balances are sufficient *)
        if ab = 0.0 && qb < 10.0 then begin
          (* Very low balances - may not be able to trade *)
          if state.last_cycle <> cycle then
            Logging.debug_f ~section "Very low balances for %s - asset: %.8f, quote: %.2f" asset.symbol ab qb;
          false
        end else
          false
  ) in

  (* If balance feed is stale, don't execute strategy to avoid trading with stale data *)
  if is_stale then begin
    state.last_cycle <- cycle;
    ()
  end else begin
    (* Strategy logic based on open orders and balances *)
    (* Log current state for debugging race conditions *)
    if state.last_cycle <> cycle && cycle mod 100000 = 0 then begin
      Logging.debug_f ~section "Strategy state for %s: open_buy_count=%d, pending_orders=%d, buy_price=%s, sell_count=%d"
        asset.symbol open_buy_count (List.length state.pending_orders)
        (match state.last_buy_order_price with Some p -> Printf.sprintf "%.2f" p | None -> "none")
        (List.length state.open_sell_orders)
    end;
    (* Clean up stale pending orders (older than 5 seconds) and enforce hard limit of 50 *)
    let now = Unix.time () in
    let original_pending_count = List.length state.pending_orders in
    state.pending_orders <- List.filter (fun (order_id, _, _) ->
      let age = now -. state.last_order_time in
      if age > 5.0 then begin  (* Reduced from 10 to 5 seconds *)
        Logging.warn_f ~section "Removing stale pending order %s for %s (age: %.1fs)" order_id asset.symbol age;
        false
      end else
        true
    ) state.pending_orders;

    (* Enforce hard limit of 50 pending orders to prevent memory growth *)
    if List.length state.pending_orders > 50 then begin
      let excess = List.length state.pending_orders - 50 in
      state.pending_orders <- take 50 state.pending_orders;  (* Keep most recent 50 *)
      Logging.warn_f ~section "Truncated %d excess pending orders for %s (kept 50)" excess asset.symbol;
    end;

    (* Log cleanup summary occasionally *)
    let cleaned_pending = original_pending_count - List.length state.pending_orders in
    if cleaned_pending > 0 && cycle mod 100000 = 0 then
      Logging.debug_f ~section "Cleaned up %d pending orders for %s" cleaned_pending asset.symbol;

    if state.pending_orders <> [] then begin
        (* Only log every 100,000 iterations to avoid spam *)
        if cycle mod 100000 = 0 then begin
          Logging.debug_f ~section "Waiting for %d pending orders before placing new orders for %s: [%s]"
            (List.length state.pending_orders) asset.symbol
            (String.concat "; " (List.map (fun (order_id, side, price) ->
              Printf.sprintf "%s %s@%.2f" (string_of_order_side side) order_id price
            ) state.pending_orders))
        end;
        (* Update cycle counter *)
        state.last_cycle <- cycle
      end else if open_buy_count > 1 then begin
        (* Case 0: Multiple buy orders exist - cancel all buy orders to maintain single buy order policy *)
        Logging.debug_f ~section "Found %d buy orders for %s, cancelling all buy orders to maintain single buy order policy"
          open_buy_count asset.symbol;

        (* Cancel all buy orders *)
        List.iter (fun (order_id, _) ->
          let cancel_order = create_cancel_order order_id asset.symbol "Grid" in
          push_order cancel_order;
          Logging.debug_f ~section "Cancelling excess buy order: %s for %s" order_id asset.symbol
        ) !buy_orders;

        (* Clear buy order tracking since we're cancelling all *)
        state.last_buy_order_price <- None;
        state.last_buy_order_id <- None;

        (* Update cycle counter *)
        state.last_cycle <- cycle
      end else if open_buy_count = 0 then begin
        (* NO OPEN BUY: Place sell + buy, then enforce 2x grid spacing *)
        let sell_price = calculate_grid_price price grid_interval true asset.symbol in
        let buy_price = calculate_grid_price price grid_interval false asset.symbol in
        
        (* Place sell order - attempt regardless of balance *)
        let sell_order = create_order asset.symbol Sell (qty *. sell_mult) (Some sell_price) true in
        push_order sell_order;
        Logging.debug_f ~section "Placed sell order for %s: %.8f @ %.2f"
          asset.symbol (qty *. sell_mult) sell_price;

        (* Place buy order *)
        (match quote_balance with
         | Some quote_bal when can_place_buy_order qty quote_bal quote_needed ->
             let order = create_order asset.symbol Buy qty (Some buy_price) true in
             push_order order;
             state.last_buy_order_price <- Some buy_price;
             Logging.debug_f ~section "Placed buy order for %s: %.8f @ %.2f"
               asset.symbol qty buy_price;
             
             (* Enforce exactly 2x grid_interval spacing *)
             (* Find closest sell above buy (including the one we just placed) *)
             let all_sells = (("new_sell", sell_price) :: state.open_sell_orders) in
             let closest_sell = List.fold_left (fun acc (_, sp) ->
               if sp > buy_price then
                 match acc with
                 | None -> Some sp
                 | Some best_sp -> if sp < best_sp then Some sp else acc
               else acc
             ) None all_sells in
             
             (match closest_sell with
              | Some cs_price ->
                  let double_grid_interval = price *. (2.0 *. grid_interval /. 100.0) in
                  let target_buy = round_price (cs_price -. double_grid_interval) asset.symbol in
                  let min_move_threshold = get_price_increment asset.symbol in
                  
                  if abs_float (target_buy -. buy_price) > min_move_threshold then
                    Logging.debug_f ~section "Will enforce 2x spacing for %s on next cycle: buy %.2f -> %.2f (from sell@%.2f)"
                      asset.symbol buy_price target_buy cs_price
              | None -> ())
         | Some quote_bal ->
             Logging.warn_f ~section "Insufficient quote balance for %s buy order: need %.2f, have %.2f"
               asset.symbol quote_needed quote_bal
         | None ->
             Logging.warn_f ~section "No quote balance data available for %s buy order"
               asset.symbol
        );
        state.last_cycle <- cycle
      end else if open_buy_count > 0 then begin
        (* Case 2: We have an open buy order - check for sell orders *)
        if state.open_sell_orders <> [] then begin
          (* Find the closest sell order *)
          let closest_sell_order = List.fold_left (fun acc (order_id, sell_price) ->
            match acc with
            | None -> Some (order_id, sell_price)
            | Some (_, best_price) ->
                if sell_price < best_price then Some (order_id, sell_price) else acc
          ) None state.open_sell_orders in

          match closest_sell_order, state.last_buy_order_price, state.last_buy_order_id with
          | Some (_sell_order_id, sell_price), Some current_buy_price, Some buy_order_id ->
              (* Calculate distance from buy to sell as absolute dollar amount *)
              let distance = sell_price -. current_buy_price in
              (* Calculate 2x grid_interval as absolute dollar amount based on current price *)
              let double_grid_interval = price *. (2.0 *. grid_interval /. 100.0) in

              (* Always calculate exact 2x target from sell *)
              let exact_target = round_price (sell_price -. double_grid_interval) asset.symbol in
              
              if distance > double_grid_interval then begin
                (* Distance > 2x: Trail upward ONLY to maintain grid_interval below current price *)
                let proposed_buy_price = calculate_grid_price price grid_interval false asset.symbol in
                
                (* ONLY trail upward - never move buy order down *)
                if proposed_buy_price > current_buy_price then begin
                  let proposed_distance = sell_price -. proposed_buy_price in
                  
                  (* Determine target: try to achieve 2x if possible, otherwise just trail to grid_interval below *)
                  let target_buy_price = 
                    if proposed_distance <= double_grid_interval then
                      (* Proposed position respects 2x - use exact 2x target *)
                      exact_target
                    else
                      (* Proposed position still exceeds 2x - just trail to grid_interval below current price *)
                      (* This is better than staying way behind current price *)
                      proposed_buy_price
                  in
                  
                  let price_diff = abs_float (target_buy_price -. current_buy_price) in
                  let min_move_threshold = get_price_increment asset.symbol in
                  
                  if price_diff > min_move_threshold && target_buy_price <> current_buy_price then begin
                    (match quote_balance with
                     | Some quote_bal when can_place_buy_order qty quote_bal quote_needed ->
                         let order = create_amend_order buy_order_id asset.symbol Buy qty (Some target_buy_price) true "Grid" in
                         push_order order;
                         state.last_buy_order_price <- Some target_buy_price;
                         let target_distance = sell_price -. target_buy_price in
                         Logging.debug_f ~section "Amended buy %s for %s (trailing upward): sell@%.2f, buy@%.2f -> %.2f (dist: %.2f -> %.2f, 2x: %.2f)"
                           buy_order_id asset.symbol sell_price current_buy_price target_buy_price distance target_distance double_grid_interval
                     | Some quote_bal ->
                         Logging.warn_f ~section "Insufficient quote balance to trail %s: need %.2f, have %.2f"
                           asset.symbol quote_needed quote_bal
                     | None ->
                         Logging.warn_f ~section "No quote balance for %s trailing" asset.symbol)
                  end
                end else begin
                  (* Price has fallen - hold buy order steady, don't trail down *)
                  Logging.debug_f ~section "Holding buy %s for %s @ %.2f (price fell, not trailing down)"
                    buy_order_id asset.symbol current_buy_price
                end
              end else begin
                (* Distance <= 2x: Enforce exact 2x spacing *)
                let price_diff = abs_float (exact_target -. current_buy_price) in
                let min_move_threshold = get_price_increment asset.symbol in

                if price_diff > min_move_threshold && exact_target <> current_buy_price then begin
                  (match quote_balance with
                   | Some quote_bal when can_place_buy_order qty quote_bal quote_needed ->
                       let order = create_amend_order buy_order_id asset.symbol Buy qty (Some exact_target) true "Grid" in
                       push_order order;
                       state.last_buy_order_price <- Some exact_target;
                       Logging.debug_f ~section "Amended buy %s for %s (enforcing 2x): sell@%.2f, buy@%.2f -> %.2f (dist: %.2f <= 2x: %.2f)"
                         buy_order_id asset.symbol sell_price current_buy_price exact_target distance double_grid_interval
                   | Some quote_bal ->
                       Logging.warn_f ~section "Insufficient quote balance for %s: need %.2f, have %.2f"
                         asset.symbol quote_needed quote_bal
                   | None ->
                       Logging.warn_f ~section "No quote balance for %s" asset.symbol)
                end else begin
                  Logging.debug_f ~section "Buy %s for %s already at 2x: buy@%.2f, sell@%.2f"
                    buy_order_id asset.symbol current_buy_price sell_price
                end
              end
          | _ ->
              (* No buy order tracked, place sell and buy orders *)
              (* Place sell order grid_interval above current price - attempt regardless of balance *)
              let sell_price = calculate_grid_price price grid_interval true asset.symbol in
              let sell_order = create_order asset.symbol Sell (qty *. sell_mult) (Some sell_price) true in
              push_order sell_order;
              Logging.debug_f ~section "Placed sell order for %s: %.8f @ %.2f (attempt regardless of balance)"
                asset.symbol (qty *. sell_mult) sell_price;

              (* Place buy order grid_interval below current price *)
              let target_buy_price = calculate_grid_price price grid_interval false asset.symbol in
              (match quote_balance with
               | Some quote_bal when can_place_buy_order qty quote_bal quote_needed ->
                   let order = create_place_order asset.symbol Buy qty (Some target_buy_price) true "Grid" in
                   push_order order;
                   state.last_buy_order_price <- Some target_buy_price;
                   Logging.debug_f ~section "Placed buy order at grid_interval below current price: buy@%.2f"
                     target_buy_price
               | Some _ ->
                   Logging.debug_f ~section "Insufficient quote balance for buy order: need %.2f, have %.2f"
                     quote_needed (Option.value quote_balance ~default:0.0)
               | None ->
                   Logging.debug_f ~section "No quote balance data available for buy order decision")
        end else begin
          (* No sell orders: trail current price by grid_interval (upward only) *)
          match state.last_buy_order_price, state.last_buy_order_id with
          | Some current_buy_price, Some buy_order_id ->
              let target_buy_price = calculate_grid_price price grid_interval false asset.symbol in
              
              (* Only trail upward - move buy up if target is higher than current *)
              if target_buy_price > current_buy_price then begin
                let price_diff = target_buy_price -. current_buy_price in
                let min_move_threshold = get_price_increment asset.symbol in

                if price_diff > min_move_threshold then begin
                  (match quote_balance with
                   | Some quote_bal when can_place_buy_order qty quote_bal quote_needed ->
                       let order = create_amend_order buy_order_id asset.symbol Buy qty (Some target_buy_price) true "Grid" in
                       push_order order;
                       state.last_buy_order_price <- Some target_buy_price;
                       Logging.debug_f ~section "Trailing buy %s for %s: %.2f -> %.2f (no sell anchors)"
                         buy_order_id asset.symbol current_buy_price target_buy_price
                   | Some quote_bal ->
                       Logging.warn_f ~section "Insufficient quote balance to trail buy: need %.2f, have %.2f"
                         quote_needed quote_bal
                   | None ->
                       Logging.warn_f ~section "No quote balance for buy trailing")
                end
              end else begin
                Logging.debug_f ~section "Holding buy for %s @ %.2f (price retraced or target unchanged)"
                  asset.symbol current_buy_price
              end
          | _ ->
              Logging.debug_f ~section "Buy order tracking lost for %s, will re-place on next cycle" asset.symbol
        end;
        (* Update cycle counter *)
        state.last_cycle <- cycle
      end else begin
        (* No action needed for other cases *)
        state.last_cycle <- cycle
      end
  end
end
end

(** Handle order placement success - update pending order status *)
let handle_order_acknowledged asset_symbol order_id side price =
  let state = get_strategy_state asset_symbol in
  (* Remove from pending orders - match by side and approximate price since we might not have exact order_id yet *)
  state.pending_orders <- List.filter (fun (_, s, p) ->
    not (s = side && abs_float (p -. price) < 0.01)  (* Allow small price difference *)
  ) state.pending_orders;

  (* Update buy order tracking if this is a buy order acknowledgment *)
  (match side with
   | Buy ->
       state.last_buy_order_id <- Some order_id;
       Logging.debug_f ~section "Updated buy order ID tracking: %s @ %.2f for %s" order_id price asset_symbol
   | Sell -> ());

  Logging.debug_f ~section "Order acknowledged and removed from pending: %s %s @ %.2f for %s"
    (string_of_order_side side) order_id price asset_symbol

(** Handle order placement failure - remove from pending and potentially trigger re-evaluation *)
let handle_order_rejected asset_symbol side price =
  let state = get_strategy_state asset_symbol in
  (* Remove from pending orders *)
  state.pending_orders <- List.filter (fun (_, s, p) ->
    not (s = side && abs_float (p -. price) < 0.01)  (* Allow small price difference *)
  ) state.pending_orders;
  Logging.debug_f ~section "Order rejected and removed from pending: %s @ %.2f for %s"
    (string_of_order_side side) price asset_symbol;
  ()

  (* For buy order rejections, this signals no buy order exists, which is valid for re-evaluation *)
  (* We don't need to do anything special here - the strategy will re-evaluate on next cycle *)

(** Handle order cancellation - remove from pending and tracked orders *)
let handle_order_cancelled asset_symbol order_id =
  let state = get_strategy_state asset_symbol in
  
  (* Add to cancelled orders blacklist to prevent re-adding during sync *)
  let now = Unix.time () in
  state.cancelled_orders <- (order_id, now) :: state.cancelled_orders;
  
  (* Remove from pending orders if it's there *)
  let original_pending_count = List.length state.pending_orders in
  state.pending_orders <- List.filter (fun (pending_id, _, _) ->
    pending_id <> order_id
  ) state.pending_orders;
  let removed_pending = original_pending_count - List.length state.pending_orders in
  
  (* If this was a tracked buy order, clear it *)
  (match state.last_buy_order_id with
   | Some buy_id when buy_id = order_id ->
       state.last_buy_order_id <- None;
       state.last_buy_order_price <- None;
       Logging.debug_f ~section "Cancelled buy order %s removed from tracking for %s (blacklisted)" order_id asset_symbol
   | _ -> ());
  
  (* Remove from sell orders list *)
  let original_sell_count = List.length state.open_sell_orders in
  state.open_sell_orders <- List.filter (fun (sell_id, _) ->
    sell_id <> order_id
  ) state.open_sell_orders;
  let removed_sell = original_sell_count - List.length state.open_sell_orders in
  
  Logging.debug_f ~section "Order cancelled and cleaned up: %s for %s (removed %d pending, %d sell, added to blacklist)"
    order_id asset_symbol removed_pending removed_sell

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
  Logging.debug_f ~section "Suicide Grid strategy initialized with order buffer size 4096";
  Random.self_init ()

(** Strategy module interface *)
module Strategy = struct
  let execute = execute_strategy
  let get_pending_orders = get_pending_orders
  let handle_order_acknowledged = handle_order_acknowledged
  let handle_order_rejected = handle_order_rejected
  let handle_order_cancelled = handle_order_cancelled
  let cleanup_pending_cancellation = cleanup_pending_cancellation
  let init = init
end