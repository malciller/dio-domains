(**
  Market making strategy indicated by strategy: "MM" in the configuration.

  This strategy implements a market making system that maintains buy and sell orders
  around the current spread, adjusting order placement based on fee levels and
  maintaining balance limits.

  The strategy generates orders and pushes them to a ringbuffer for execution elsewhere.
*)

let section = "market_maker"
let log_interval =
  try
    let json = Yojson.Basic.from_file "config.json" in
    let open Yojson.Basic.Util in
    match json |> member "cycle_mod" |> to_int_option with
    | Some i -> i
    | None -> 10000000
  with _ -> 10000000

(** Use common types from Strategy_common module *)

(** Use common types from Strategy_common module *)
open Strategy_common
module Exchange = Dio_exchange.Exchange_intf



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
  mutable open_sell_orders: (string * float * float) list;  (* order_id * price * qty *)
  mutable pending_orders: (string * order_side * float * float) list;  (* order_id * side * price * timestamp - orders sent but not yet acknowledged *)
  mutable last_cycle: int;
  mutable cancelled_orders: (string * float) list;  (* order_id * timestamp - blacklist of recently cancelled orders *)
  mutable pending_cancellations: (string, float) Hashtbl.t;  (* order_id -> timestamp - track pending cancellation operations *)
  mutable last_cleanup_time: float; (* Last time cleanup was run *)
  mutable inflight_buy: bool;         (* true while a buy Place is sent but not yet ack'd/rejected/failed *)
  mutable inflight_sell: bool;        (* true while a sell Place is sent but not yet ack'd/rejected/failed *)
  mutable capital_low: bool;          (* true when quote balance is insufficient; buy+sell paused *)
  mutable asset_low: bool;            (* true when asset balance is insufficient; sell+buy paused *)
  mutable capital_low_logged: bool;   (* suppresses repeat capital_low log spam *)
  mutable last_seen_asset_balance: float;  (* last balance value from feed; used to detect genuine changes *)
  mutex: Mutex.t;  (* Thread safety for modifying strategy state *)
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
          cancelled_orders = [];
          pending_cancellations = Hashtbl.create 16;
          last_cleanup_time = 0.0;
          inflight_buy = false;
          inflight_sell = false;
          capital_low = false;
          asset_low = false;
          capital_low_logged = false;
          last_seen_asset_balance = 0.0;
          mutex = Mutex.create ();
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
let round_price price symbol exchange =
  match Exchange.Registry.get exchange with
  | Some (module Ex : Exchange.S) -> Ex.round_price ~symbol ~price
  | None ->
      Logging.warn_f ~section "No exchange found for %s/%s, using default rounding" exchange symbol;
      Float.round price  (* Default: 0 decimal places *)


(** Round quantity to appropriate precision for the symbol, ensuring we don't exceed max_qty *)
let round_qty qty symbol exchange ~max_qty =
  (* Round DOWN to qty_increment to ensure we don't try to sell more than we have *)
  let increment = match Exchange.Registry.get exchange with
  | Some (module Ex : Exchange.S) -> Ex.get_qty_increment ~symbol
  | None -> None
  in
  match increment with
  | Some inc ->
      let rounded = Float.floor (qty /. inc) *. inc in
      (* Ensure we don't exceed the maximum available quantity *)
      Float.min rounded max_qty
  | None ->
      Logging.warn_f ~section "No qty increment info for %s/%s, using 8 decimal places" exchange symbol;
      let rounded = Float.floor (qty *. 100000000.0) /. 100000000.0 in
      Float.min rounded max_qty

(** Round price DOWN to instrument precision *)
let round_price_down price symbol exchange =
  let increment = match Exchange.Registry.get exchange with
  | Some (module Ex : Exchange.S) -> Ex.get_price_increment ~symbol
  | None -> None
  in
  match increment with
  | Some inc ->
      (* Use a relative epsilon to handle floating-point drift at exact increment boundaries.
         E.g. 70.50 / 0.00001 may produce 7049999.9999... instead of 7050000.0.
         The epsilon nudges it back to the correct integer before floor. *)
      let steps = price /. inc in
      let rounded = Float.floor (steps +. 1e-9) *. inc in
      (* Snap result to eliminate accumulated float error from the multiplication *)
      let decimal_places = Float.round (-.Float.log10 inc) in
      let factor = 10.0 ** decimal_places in
      Float.floor (rounded *. factor +. 0.5) /. factor
  | None ->
      Logging.warn_f ~section "No price increment info for %s/%s, using 2 decimal places" exchange symbol;
      Float.floor (price *. 100.0) /. 100.0

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
let meets_min_qty symbol qty exchange =
  let min_qty_opt = match Exchange.Registry.get exchange with
  | Some (module Ex : Exchange.S) -> Ex.get_qty_min ~symbol
  | None -> None
  in
  match min_qty_opt with
  | Some min_qty -> qty >= min_qty
  | None -> 
      Logging.warn_f ~section "No qty_min found for %s/%s, assuming qty %.8f is valid" exchange symbol qty;
      true  (* If we can't find min_qty, assume it's valid to avoid blocking *)

(** Create a strategy order for placing a new order *)
let generate_side_duplicate_key asset_symbol side =
  Printf.sprintf "%s|%s|mm" asset_symbol (string_of_order_side side)

let create_place_order asset_symbol side qty price post_only strategy exchange =
  {
    operation = Place;
    order_id = None;
    symbol = asset_symbol;
    exchange;
    side;
    order_type = "limit";
    qty;
    price;
    time_in_force = "GTC";
    post_only;
    userref = Some Strategy_common.strategy_userref_mm;  (* Tag order as MM strategy *)
    strategy;
    duplicate_key = generate_side_duplicate_key asset_symbol side;
  }

(** Create a strategy order for amending an existing order *)
let create_amend_order order_id asset_symbol side qty price post_only strategy exchange =
  {
    operation = Amend;
    order_id = Some order_id;
    symbol = asset_symbol;
    exchange;
    side;
    order_type = "limit";
    qty;
    price;
    time_in_force = "GTC";
    post_only;
    userref = None;  (* Amends don't set userref *)
    strategy;
    duplicate_key = ""; (* Not used for amend *)
  }

(** Create a strategy order for cancelling an existing order *)
let create_cancel_order order_id asset_symbol strategy exchange =
  {
    operation = Cancel;
    order_id = Some order_id;
    symbol = asset_symbol;
    exchange;
    side = Buy;  (* Not relevant for cancel *)
    order_type = "limit";  (* Not relevant for cancel *)
    qty = 0.0;  (* Not relevant for cancel *)
    price = None;
    time_in_force = "GTC";  (* Not relevant for cancel *)
    post_only = false;
    userref = None;  (* Cancels don't set userref *)
    strategy;
    duplicate_key = ""; (* Not used for cancel *)
  }

(** Push order to ringbuffer. Returns true if successfully pushed, false if duplicate or full. *)
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
              false
            end else begin
              (* Not a duplicate, proceed with pushing *)
              Mutex.lock order_buffer_mutex;
              let write_result = OrderRingBuffer.write order_buffer order in
              Mutex.unlock order_buffer_mutex;

              match write_result with
              | Some () ->
                  Strategy_common.OrderSignal.broadcast ();
                  Logging.debug_f ~section "Pushed %s %s order: %s %.8f @ %s"
                    operation_str
                    (string_of_order_side order.side)
                    order.symbol
                    order.qty
                    (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market");


                   (* Add to pending cancellations tracking *)
                   Hashtbl.add state.pending_cancellations target_order_id (Unix.time ());
                   Logging.debug_f ~section "Cancelling order for %s (target: %s)"
                     order.symbol target_order_id;
                  true
              | None ->
                  Logging.warn_f ~section "Order ringbuffer full, dropped %s %s order for %s"
                    operation_str (string_of_order_side order.side) order.symbol;
                  false
            end
        | None ->
            Logging.warn_f ~section "Cancel operation missing order_id for %s" order.symbol;
            false)
   | _ ->
       (* Check for duplicates before pushing *)
       let is_duplicate = match order.operation with
         | Place -> not (InFlightOrders.add_in_flight_order order.duplicate_key)
         | Amend -> (match order.order_id with Some oid -> not (InFlightAmendments.add_in_flight_amendment oid) | None -> false)
         | _ -> false
       in

       if is_duplicate then begin
         Logging.debug_f ~section "Duplicate %s detected (strategy): %s" operation_str order.symbol;
         false
       end else begin
         (* For Place and Amend operations, proceed normally *)
         Mutex.lock order_buffer_mutex;
         let write_result = OrderRingBuffer.write order_buffer order in
         Mutex.unlock order_buffer_mutex;

       match write_result with
       | Some () ->
           Strategy_common.OrderSignal.broadcast ();
           Logging.debug_f ~section "Pushed %s %s order: %s %.8f @ %s"
             operation_str
             (string_of_order_side order.side)
             order.symbol
             order.qty
             (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market");

            (* Update strategy state *)
            let state = get_strategy_state order.symbol in

            (* Set inflight flags for Place operations *)
            (match order.operation, order.side with
             | Place, Buy -> state.inflight_buy <- true
             | Place, Sell -> state.inflight_sell <- true
             | _ -> ());

            (* Handle different operations *)
            (match order.operation with
             | Place ->
                (* For Hyperliquid: skip pending tracking for sell orders entirely.
                   Sells are fire-and-forget; tracking them creates ghost entries
                   that cause re-placement loops during cancel-replace amendments.
                   Kraken retains existing pending sell tracking behavior. *)
                let skip_pending = order.exchange = "hyperliquid" && order.side = Sell in
                if not skip_pending then begin
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
                   | Sell, Some price ->
                       (* Add sell order to tracking list - use temporary ID for now *)
                       let order_id = temp_order_id in
                       state.open_sell_orders <- (order_id, price, order.qty) :: state.open_sell_orders
                   | _ -> ())
                end
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
                ());
           true
       | None ->
           (* Remove from tracking if write failed *)
           (match order.operation with
            | Place -> ignore (InFlightOrders.remove_in_flight_order order.duplicate_key)
            | Amend -> (match order.order_id with Some oid -> ignore (InFlightAmendments.remove_in_flight_amendment oid) | None -> ())
            | _ -> ());

           Logging.warn_f ~section "Order ringbuffer full, dropped %s %s order for %s"
             operation_str (string_of_order_side order.side) order.symbol;
           false
       end)

(** Cancel any existing orders at the same price level (duplicate guard) *)
let cancel_duplicate_orders asset_symbol target_price target_side open_orders strategy exchange =
  let duplicates = List.filter (fun (order_id, order_price, qty, side_str, _) ->
    let is_cancelled = List.exists (fun (cancelled_id, _) ->
      cancelled_id = order_id) (get_strategy_state asset_symbol).cancelled_orders in
    not is_cancelled && qty > 0.0 &&
    side_str = (if target_side = Buy then "buy" else "sell") &&
    abs_float (order_price -. target_price) < 0.000001  (* Exact price match *)
  ) open_orders in

  List.iter (fun (order_id, _, _, _, _) ->
    let cancel_order = create_cancel_order order_id asset_symbol strategy exchange in
    ignore (push_order cancel_order);
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

  (* --- Asset-low check: clear flag when asset balance recovers --- *)
  (* For Kraken: only clear when balance has genuinely increased (fill, deposit).
     Kraken's "Insufficient funds" on sells can mean USD collateral is too low,
     not that asset balance is insufficient. The local balance check would
     immediately clear the flag every cycle, causing a tight retry loop.
     For Hyperliquid: clear whenever available balance passes the threshold
     (existing behavior). *)
  (match asset_balance with
   | Some asset_bal ->
       let qty_f = (try float_of_string asset.qty with Failure _ -> 0.001) in
       let balance_actually_changed = asset_bal > state.last_seen_asset_balance in
       let should_clear =
         if asset.exchange = "hyperliquid" then asset_bal >= qty_f
         else asset_bal >= qty_f && balance_actually_changed
       in
       if state.asset_low && should_clear then begin
         state.asset_low <- false;
         state.inflight_sell <- false;
         ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset.symbol Sell));
         Logging.info_f ~section "Asset balance restored for %s (have %.8f, need %.8f) - resuming order placement"
           asset.symbol asset_bal qty_f
       end;
       state.last_seen_asset_balance <- asset_bal
   | None -> ()
  );

  (* --- Capital-low fast path: skip strategy when quote balance is known to be
     insufficient for a buy order. When balance recovers we clear the flag
     and resume normally. --- *)
  (match (quote_balance, current_price) with
   | Some quote_bal, Some price ->
       let qty_f = (try float_of_string asset.qty with Failure _ -> 0.001) in
       let quote_needed_fast = price *. qty_f in
       if state.capital_low && quote_bal < quote_needed_fast then begin
         (* Still low: skip entire strategy body to avoid order spam *)
         ()
       end else if state.capital_low && quote_bal >= quote_needed_fast then begin
         (* Balance has recovered: clear flag and fall through to normal logic *)
         state.capital_low <- false;
         state.capital_low_logged <- false;
         state.inflight_buy <- false;
         ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset.symbol Buy));
         Logging.info_f ~section "Capital restored for %s (have %.2f, need %.2f) - resuming strategy"
           asset.symbol quote_bal quote_needed_fast
       end
       (* capital_low=false: fall through normally *)
   | _ -> () (* No balance/price data yet, fall through *)
  );

  (* Only proceed with main strategy if capital_low flag is clear.
     asset_low is checked below only for sell+buy placement -- order sync,
     cleanup, and cancellation detection must always run. *)
  if not state.capital_low then begin

  let now = Unix.time () in

      (* Clean up stale pending orders (older than 5 seconds) and enforce hard limit of 50 *)
  let original_count = List.length state.pending_orders in
  state.pending_orders <- List.filter (fun (order_id, side, _, timestamp) ->
    let age = now -. timestamp in
    if age > 5.0 then begin  (* Reduced from 10 to 5 seconds *)
      if should_log then Logging.warn_f ~section "Removing stale pending order %s for %s (age: %.1fs)" order_id asset.symbol age;
      
      (* Clean up global trackers to allow immediate re-placement *)
      if String.starts_with ~prefix:"pending_amend_" order_id then begin
        let target_oid = String.sub order_id 14 (String.length order_id - 14) in
        ignore (InFlightAmendments.remove_in_flight_amendment target_oid)
      end else begin
        let duplicate_key = generate_side_duplicate_key asset.symbol side in
        ignore (InFlightOrders.remove_in_flight_order duplicate_key);
        (* Fallback: clear inflight flags for stale placements *)
        (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false)
      end;
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


  (* NOTE: We intentionally do NOT lock state.mutex here.
     The strategy runs in its own OCaml domain and the order processor callbacks
     (handle_order_amended, handle_order_amendment_failed, etc.) are dispatched
     via Lwt.async in the Lwt main domain. Locking the non-recursive Mutex.t
     here would cause EDEADLK ("Resource deadlock avoided") when a callback
     fires while the strategy is executing, because both run in the same thread.
     The handler functions lock state.mutex for inter-domain safety, which is
     correct and sufficient. *)


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
              (* After selling qty, our exposure would be: (ab -. qty) *. mid_price <= max_exp *)
              let new_exposure = (ab -. qty) *. mid_price in
              new_exposure <= max_exp
          | Some _, None -> false  (* If max_exp is set but we have no asset balance data, fail *)
          | None, _ -> true        (* If no max_exposure constraint, always pass *)
        in


        if should_log then Logging.info_f ~section "Strategy check for %s: open_buy_count=%d, bid=%.8f, ask=%.8f, exposure_ok=%B, usd_ok=%B"
          asset.symbol open_buy_count bid ask exposure_ok usd_balance_ok;

        (* EXPOSURE & BALANCE CHECKS (Pause/Protect if Failed) *)
        if not exposure_ok || not usd_balance_ok then begin
          (* Pause strategy: Cancel all open buy orders for this asset *)
          if should_log then Logging.debug_f ~section "Balance/exposure limits exceeded for %s (exposure_ok=%B, usd_ok=%B), pausing strategy"
            asset.symbol exposure_ok usd_balance_ok;

          (* Sync strategy state with actual open orders from exchange *)
          (* Clear existing tracked orders *)
          state.open_sell_orders <- [];
          (* For Hyperliquid: preserve buy tracking across sync.
             webData2 lags behind execution events during cancel-replace amendments,
             so the exchange may briefly report 0 buys while we know one exists.
             The effective_buy_count logic (has_tracked_buy && open_buy_count=0 -> 1)
             handles this, but only if we don't clear the tracking here.
             Genuine cancellations clear tracking via handle_order_cancelled.
             Kraken clears as before since its data feed is authoritative. *)
          if asset.exchange <> "hyperliquid" then begin
            state.last_buy_order_price <- None;
            state.last_buy_order_id <- None
          end;
          (* Note: We don't clear pending_orders here - they should be managed by order placement responses *)

          (* Update with real orders from exchange - open_orders is (order_id, price, qty, side, userref) list *)
          (* Filter out recently cancelled orders to avoid race condition *)
          let buy_orders = ref [] in
          let sell_orders = ref [] in

          List.iter (fun (order_id, order_price, qty, side_str, userref_opt) ->
            (* Skip if this order was recently cancelled *)
            let is_cancelled = List.exists (fun (cancelled_id, _) -> cancelled_id = order_id) state.cancelled_orders in
            
            let is_our_strategy = match userref_opt with
              | Some ref_val -> ref_val = Strategy_common.strategy_userref_mm
              | None -> false
            in

            if not is_cancelled && qty > 0.0 && is_our_strategy then (* Only count orders with remaining quantity *)
              (* Use actual order side from exchange, not price-based classification *)
              if side_str = "buy" then
                (* Treat ALL buy orders as grid buys to enforce single-buy-order policy across all exchanges *)
                buy_orders := (order_id, order_price) :: !buy_orders
              else
                (* For sell orders, include ALL open sell orders regardless of tag *)
                sell_orders := (order_id, order_price, qty) :: !sell_orders
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

          (* Cancel all open buy orders *)
          let buys_to_cancel = 
            let tracked_id = match state.last_buy_order_id with Some id -> [id] | None -> [] in
            let open_ids = List.map (fun (id, _) -> id) !buy_orders in
            List.sort_uniq String.compare (tracked_id @ open_ids) 
          in
          
          List.iter (fun buy_id ->
             let cancel_order = create_cancel_order buy_id asset.symbol "MM" asset.exchange in
             ignore (push_order cancel_order);
             if should_log then Logging.debug_f ~section "Cancelling buy order due to pause: %s for %s" buy_id asset.symbol
          ) buys_to_cancel;

          (* Clear buy order tracking since we cancelled them *)
          state.last_buy_order_price <- None;
          state.last_buy_order_id <- None;

          (* Place/ensure sell orders for all available asset_balance at or above best_ask *)
          (* CRITICAL: Check for pending sell orders first to prevent duplicate spam *)
          (* Place/ensure sell orders for all available asset_balance at or above best_ask *)
          (* CRITICAL: Check for pending sell orders first to prevent duplicate spam *)
                    (* CRITICAL: Check inflight flag to prevent duplicate spam *)
           if state.inflight_sell then begin
              if should_log then Logging.debug_f ~section "Skipping emergency sell for %s: sell order in-flight" asset.symbol
          end else begin
            match available_asset_balance with
            | Some ab when ab > 0.0 ->
                (* Round quantity to instrument precision, ensuring we don't exceed available balance *)
                let rounded_qty = round_qty ab asset.symbol asset.exchange ~max_qty:ab in
                
                (* If we have free balance > min_qty, sell it. Do NOT cancel existing orders. *)
                if meets_min_qty asset.symbol rounded_qty asset.exchange then begin
                   let sell_price = round_price ask asset.symbol asset.exchange in
                   let sell_order = create_place_order asset.symbol Sell rounded_qty (Some sell_price) true "MM" asset.exchange in
                   ignore (push_order sell_order);
                   if should_log then Logging.info_f ~section "Placed emergency sell order for free balance: %.8f @ %.2f for %s" rounded_qty sell_price asset.symbol
                end
            | _ -> ()
          end;

          (* Stop execution for this trigger - will re-check on next trigger *)
          state.last_cycle <- cycle

        end else begin
          (* Both checks pass: Proceed to Step 3 - BUY ORDER MANAGEMENT *)

          (* Clean slate tracking based strictly on open_orders... 
             Except for Hyperliquid where webData2 can lag behind executions,
             so we preserve tracking if it existed. *)
          state.open_sell_orders <- [];
          if asset.exchange <> "hyperliquid" then begin
            state.last_buy_order_price <- None;
            state.last_buy_order_id <- None
          end;

          let buy_orders = ref [] in
          let sell_orders = ref [] in

          List.iter (fun (order_id, order_price, qty, side_str, userref_opt) ->
            let is_cancelled = List.exists (fun (cancelled_id, _) -> cancelled_id = order_id) state.cancelled_orders in
            
            let is_our_strategy = match userref_opt with
              | Some ref_val -> ref_val = Strategy_common.strategy_userref_mm
              | None -> false
            in

            if not is_cancelled && qty > 0.0 && is_our_strategy then
              if side_str = "buy" then
                buy_orders := (order_id, order_price) :: !buy_orders
              else
                sell_orders := (order_id, order_price, qty) :: !sell_orders
          ) open_orders;

          (* Set tracking variables to the actual data from the book *)
          (match !buy_orders with
           | [] -> ()
           | orders ->
               let (best_order_id, best_price) = List.fold_left (fun (acc_id, acc_price) (order_id, price) ->
                 if price > acc_price then (order_id, price) else (acc_id, acc_price)
               ) (List.hd orders) (List.tl orders) in
               state.last_buy_order_price <- Some best_price;
               state.last_buy_order_id <- Some best_order_id);

          state.open_sell_orders <- !sell_orders;

          (* Evaluate exact open buy count, combining internal tracking with webData2 feed *)
          let has_tracked_buy = state.last_buy_order_id <> None in
          let sync_open_buy_count = List.length !buy_orders in
          
          (* For Hyperliquid, if we tracked a buy but webData2 shows 0, trust our tracking. 
             If we cancelled it, tracking would have been cleared by handle_order_cancelled. *)
          let actual_open_buy_count = 
            if asset.exchange = "hyperliquid" && has_tracked_buy && sync_open_buy_count = 0 then 1
            else sync_open_buy_count 
          in

          if should_log then Logging.debug_f ~section "Order management for %s: open_buy_count=%d, open_sell_count=%d" 
            asset.symbol actual_open_buy_count (List.length !sell_orders);

          (* BUY ORDER MANAGEMENT - Aim for Exactly One Active Buy at Profitable Level *)
          if actual_open_buy_count > 1 then begin
            (* If >1 open buy: Cancel all open buys. Stop—next trigger will replace. *)
            if should_log then Logging.debug_f ~section "Cancelling %d excess buy orders for %s" actual_open_buy_count asset.symbol;
            
            List.iter (fun (buy_id, _) ->
              let cancel_order = create_cancel_order buy_id asset.symbol "MM" asset.exchange in
              ignore (push_order cancel_order)
            ) !buy_orders;
            
            state.last_buy_order_price <- None;
            state.last_buy_order_id <- None;
            state.last_cycle <- cycle           end else if actual_open_buy_count = 0 then begin
            (* If 0 open buys: Place new buy+sell PAIR.
               Guard: Don't place if a buy order is still in-flight,
               matching suicide_grid's established pattern. *)
            if state.inflight_buy then begin
              if should_log then Logging.debug_f ~section "Skipping pair placement for %s: buy order in-flight" asset.symbol
            end else begin
            if should_log then Logging.debug_f ~section "No buy orders for %s, placing new pair" asset.symbol;

            (* Determine buy price based on fee:
               If maker_fee == 0: Buy price = best_bid
               If maker_fee > 0: Buy price = best_ask - (best_ask * (2 * maker_fee + 0.0001)) as percentage backoff 
               BUT if the top of the book spread is MORE profitable, we want to capture the bid.
               So we ceiling the buy price at `bid`. *)
               
            let buy_price_raw = 
              if fee = 0.0 then bid
              else min bid (ask *. (1.0 -. (fee *. 2.0 +. 0.0001)))
            in
            
            let buy_price = round_price_down buy_price_raw asset.symbol asset.exchange in
            let sell_price = round_price ask asset.symbol asset.exchange in
            
            (* Maintenance Rules: Profitability Guard *)
            let required_spread = ask *. (fee *. 2.0 +. 0.0001) in
            let final_spread = sell_price -. buy_price in
            let profitability_ok = if fee = 0.0 then true else final_spread >= required_spread in
            
            if profitability_ok then begin
              (* Maintenance Rules: Post-Only Safety *)
              if buy_price <= (bid +. 0.0000001) && sell_price >= (ask -. 0.0000001) then begin
                  let can_place_sell = meets_min_qty asset.symbol qty asset.exchange in
                  let can_place_buy = match min_usd_balance_opt, available_quote_balance with
                    | Some min_bal, Some qb ->
                        let required_quote = buy_price *. qty in
                        qb >= required_quote && qb -. required_quote >= min_bal
                    | Some _, None -> false  
                    | None, _ -> true        
                  in

                  (* Place SELL ORDER first*)
                  if can_place_sell then begin
                    let sell_order = create_place_order asset.symbol Sell qty (Some sell_price) true "MM" asset.exchange in
                    ignore (push_order sell_order);
                    if should_log then Logging.info_f ~section "Placed sell order for %s: %.8f @ %.2f"
                      asset.symbol qty sell_price;
                  end;

                  (* Then place BUY ORDER *)
                  if can_place_buy then begin
                    let _ = cancel_duplicate_orders asset.symbol buy_price Buy open_orders "MM" asset.exchange in
                    let buy_order = create_place_order asset.symbol Buy qty (Some buy_price) true "MM" asset.exchange in
                    if push_order buy_order then begin
                      state.last_buy_order_price <- Some buy_price;
                      if should_log then Logging.info_f ~section "Placed buy order for %s: %.8f @ %.2f (fee=%.6f)"
                        asset.symbol qty buy_price fee;
                    end;
                  end;
              end else begin
                  if should_log then Logging.warn_f ~section "Post-only violation for %s: buy_price=%.8f > bid=%.8f or sell_price=%.8f < ask=%.8f"
                    asset.symbol buy_price bid sell_price ask;
              end
            end else begin
              if should_log then Logging.warn_f ~section "Profitability guard failed for %s (fee=%.6f)" asset.symbol fee;
            end;
            
            end;  (* buy_order_pending guard *)
            
            (* Stop. Re-trigger on next event. *)
            state.last_cycle <- cycle

          end else begin
            (* If exactly 1 open buy: Check if current buy price matches required level *)
            (* Per-order is_being_amended + is_in_flight guards below prevent duplicate
               amendments, matching suicide_grid's established pattern. *)
            match state.last_buy_order_price, state.last_buy_order_id with
            | Some current_buy_price, Some buy_order_id ->
                let required_buy_price_raw = 
                  if fee = 0.0 then bid
                  else min bid (ask *. (1.0 -. (fee *. 2.0 +. 0.0001)))
                in
                let required_buy_price = round_price_down required_buy_price_raw asset.symbol asset.exchange in
                
                let min_move_threshold = match Exchange.Registry.get asset.exchange with
                  | Some (module Ex : Exchange.S) -> Option.value (Ex.get_price_increment ~symbol:asset.symbol) ~default:0.01
                  | None -> 0.01
                in

                let current_buy_price_rounded = round_price current_buy_price asset.symbol asset.exchange in
                let price_diff_rounded = round_price (abs_float (required_buy_price -. current_buy_price_rounded)) asset.symbol asset.exchange in

                (* Check if this order is already being amended (matching suicide_grid pattern) *)
                let is_being_amended = List.exists (fun (id, _, _, _) ->
                  String.starts_with ~prefix:"pending_amend_" id &&
                  String.sub id 14 (String.length id - 14) = buy_order_id
                ) state.pending_orders in
                let is_in_flight = InFlightAmendments.is_in_flight buy_order_id in

                if is_being_amended || is_in_flight then begin
                  if should_log then Logging.debug_f ~section "Skipping amendment for %s: already in-flight or pending" asset.symbol
                end else if price_diff_rounded >= min_move_threshold && required_buy_price <> current_buy_price_rounded then begin
                  (* Mismatched: Amend buy to the required price *)
                  (* Maintenance Rules: Profitability Guard *)
                  let required_spread = ask *. (fee *. 2.0 +. 0.0001) in
                  let current_sell_price = round_price ask asset.symbol asset.exchange in
                  let final_spread = current_sell_price -. required_buy_price in
                  let profitability_ok = if fee = 0.0 then true else final_spread >= required_spread in
                  
                  let amendment_balance_ok = match min_usd_balance_opt, available_quote_balance with
                  | Some min_bal, Some qb ->
                      let current_locked = current_buy_price *. qty in
                      let effective_qb = qb +. current_locked in
                      let required_quote = required_buy_price *. qty in
                      effective_qb >= required_quote && effective_qb -. required_quote >= min_bal
                  | Some _, None -> false
                  | None, _ -> true
                  in

                  (* Post-Only Safety AND Profitability Guard AND Balance Ok *)
                  if profitability_ok && required_buy_price <= (bid +. 0.0000001) && amendment_balance_ok then begin
                    let _ = cancel_duplicate_orders asset.symbol required_buy_price Buy open_orders "MM" asset.exchange in
                    let amend_order = create_amend_order buy_order_id asset.symbol Buy qty (Some required_buy_price) true "MM" asset.exchange in
                    ignore (push_order amend_order);
                    state.last_buy_order_price <- Some required_buy_price;

                    if should_log then Logging.info_f ~section "Amended buy order for %s: %.8f -> %.8f (reason=book shift)"
                      asset.symbol current_buy_price required_buy_price;
                  end else begin
                    let cancel_order = create_cancel_order buy_order_id asset.symbol "MM" asset.exchange in
                    ignore (push_order cancel_order)
                  end
                end else begin
                  (* Matched: No action needed *)
                  if should_log then Logging.debug_f ~section "Buy order for %s already at correct price: %.8f" asset.symbol current_buy_price;
                end
            | _ -> Logging.warn_f ~section "Buy order tracking inconsistent for %s" asset.symbol
          end
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

  end (* capital_low / asset_low guard *)


(* Helper to generate a duplicate key for InFlightOrders based on asset and side *)
(* MUST match the key format used by create_place_order / push_order (line 173) *)
let generate_side_duplicate_key asset_symbol side =
  Printf.sprintf "%s|%s|mm" asset_symbol (string_of_order_side side)

(** Handle order placement success - update pending order status *)
let handle_order_acknowledged asset_symbol order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
  (* Remove from pending orders - match by category: placements, amendments, or fallback for tests *)
  state.pending_orders <- List.filter (fun (pending_id, s, p, _) ->
    (* 1. Match regular placements by side (any pending_buy/pending_sell for that side) *)
    let is_placement_prefix = String.starts_with ~prefix:"pending_buy_" pending_id ||
                              String.starts_with ~prefix:"pending_sell_" pending_id in
    let matches_side_placement = is_placement_prefix && s = side in
    
    (* 2. Match amend orders by order_id *)
    let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                       String.length pending_id > 14 &&
                       String.sub pending_id 14 (String.length pending_id - 14) = order_id in
    
    (* 3. Fallback for tests or legacy IDs that don't use prefixes *)
    let matches_fallback = not (String.starts_with ~prefix:"pending_" pending_id) &&
                          s = side && abs_float (p -. price) < 0.01 in
    
    not (matches_side_placement || matches_amend || matches_fallback)
  ) state.pending_orders;

  (* NOTE: InFlightOrders key is intentionally NOT removed here.
     The guard stays alive while the order is active, preventing duplicate placement.
     It gets cleaned up in handle_order_cancelled/filled or by stale timeout. *)

  (* Update buy/sell order tracking if this is a buy/sell order acknowledgment *)
  (match side with
   | Buy ->
    state.last_buy_order_id <- Some order_id;
    state.last_buy_order_price <- Some price;
    Logging.debug_f ~section "Updated buy order ID and price tracking: %s @ %.2f for %s" order_id price asset_symbol
   | Sell -> ());

  Logging.debug_f ~section "Order acknowledged and removed from pending: %s %s @ %.2f for %s"
    order_id (string_of_order_side side) price asset_symbol
  )

(** Handle order placement failure - clear in-flight trackers so strategy can retry *)
let handle_order_failed asset_symbol side reason =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* Remove from pending orders *)
    state.pending_orders <- List.filter (fun (_, s, _, _) -> s <> side) state.pending_orders;

    (* Clear inflight flags *)
    (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);
    
    (* Clear global in-flight trackers *)
    let duplicate_key = generate_side_duplicate_key asset_symbol side in
    ignore (InFlightOrders.remove_in_flight_order duplicate_key);

    (* Detect insufficient balance errors from the exchange *)
    let lower_reason = String.lowercase_ascii reason in
    let contains_fragment s fragment =
      let sl = String.length s and fl = String.length fragment in
      let rec loop i = i + fl <= sl && (String.sub s i fl = fragment || loop (i + 1)) in
      loop 0
    in
    let is_insufficient_balance = contains_fragment lower_reason "insufficient funds"
      || contains_fragment lower_reason "insufficient spot balance" in

    (* Set balance flags when the exchange reports insufficient balance.
       capital_low (buy side) and asset_low (sell side) halt further placement
       until balance actually recovers (event-driven, no timers). *)
    (match side with
     | Buy when is_insufficient_balance ->
         if not state.capital_low then begin
           state.capital_low <- true;
           state.capital_low_logged <- true;
           Logging.warn_f ~section "Exchange rejected buy for %s with insufficient funds - setting capital_low flag"
             asset_symbol
         end
     | Sell when is_insufficient_balance ->
          (* Sell failures are fire-and-forget on Kraken.  "Insufficient funds"
             on sells reflects USD collateral, not asset balance.  Don't set
             asset_low; the sell simply fails and the strategy continues. *)
          Logging.warn_f ~section "Exchange rejected sell for %s with insufficient balance (ignored, sell is fire-and-forget)"
            asset_symbol
     | _ -> ());
    
    Logging.warn_f ~section "Order failed for %s (%s): %s. Cleared in-flight tracker."
      asset_symbol (string_of_order_side side) reason
  )

(** Handle order placement failure - remove from pending and potentially trigger re-evaluation *)
let handle_order_rejected asset_symbol side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
  (* Remove from pending orders - match by side for regular placements or fallback for tests *)
  state.pending_orders <- List.filter (fun (pending_id, s, p, _) ->
    let is_placement_prefix = String.starts_with ~prefix:"pending_buy_" pending_id ||
                              String.starts_with ~prefix:"pending_sell_" pending_id in
    let matches_side_placement = is_placement_prefix && s = side in
    
    (* Rejections for amends match by ID or price fallback *)
    let matches_amend_prefix = String.starts_with ~prefix:"pending_amend_" pending_id in
    
    let matches_fallback = (not (String.starts_with ~prefix:"pending_" pending_id) || matches_amend_prefix) &&
                          s = side && abs_float (p -. price) < 0.01 in
    
    not (matches_side_placement || matches_fallback)
  ) state.pending_orders;

  (* Clear inflight flags *)
  (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);

  (* Clean up global trackers to allow immediate re-placement after rejection *)
  let duplicate_key = generate_side_duplicate_key asset_symbol side in
  ignore (InFlightOrders.remove_in_flight_order duplicate_key);

  Logging.debug_f ~section "Order rejected and removed from pending/trackers: %s @ %.2f for %s"
    (string_of_order_side side) price asset_symbol
  )
  (* For buy order rejections, this signals no buy order exists, which is valid for re-evaluation *)
  (* We don't need to do anything special here - the strategy will re-evaluate on next cycle *)

(** Handle order fill - fully clear tracking and pending amends *)
let handle_order_filled asset_symbol order_id side ~fill_price:_ =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* 1. Remove any pending amend matching this order ID *)
    state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
      not (String.starts_with ~prefix:"pending_amend_" pending_id &&
           String.length pending_id > 14 &&
           String.sub pending_id 14 (String.length pending_id - 14) = order_id)
    ) state.pending_orders;

    (* 2. Remove from sell orders tracking if it was a sell order *)
    state.open_sell_orders <- List.filter (fun (sell_id, _, _) ->
      sell_id <> order_id
    ) state.open_sell_orders;

    (* 3. Clear buy order tracking if it was the tracked buy order *)
    let was_tracked_buy = match state.last_buy_order_id with
      | Some id when id = order_id -> true
      | _ -> false
    in
    if was_tracked_buy then begin
      state.last_buy_order_id <- None;
      state.last_buy_order_price <- None;
      Logging.debug_f ~section "Filled buy order %s removed from tracking for %s" order_id asset_symbol
    end;

    (* 4. Clear global placement trackers so strategy can replace immediately *)
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol side));

    Logging.debug_f ~section "Order FILLED and cleaned up explicitly: %s for %s"
      order_id asset_symbol
  )

(** Handle order cancellation - remove from pending and tracked orders *)
let handle_order_cancelled asset_symbol order_id side =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
  (* Check if this cancellation is part of a cancel-replace amendment.
     If pending_amend_<order_id> exists, a replacement order is incoming
     and we should NOT remove the InFlightOrders guard or clear buy tracking. *)
  let is_cancel_replace = List.exists (fun (pending_id, _, _, _) ->
    String.starts_with ~prefix:"pending_amend_" pending_id &&
    let target_id = String.sub pending_id 14 (String.length pending_id - 14) in
    target_id = order_id
  ) state.pending_orders in

  if is_cancel_replace then begin
    (* Cancel-replace: clean up the pending_amend entry but keep guards alive *)
    state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
      not (String.starts_with ~prefix:"pending_amend_" pending_id &&
           String.sub pending_id 14 (String.length pending_id - 14) = order_id)
    ) state.pending_orders;
    
    (* Remove the old order from sell orders tracking if present *)
    state.open_sell_orders <- List.filter (fun (sell_id, _, _) ->
      sell_id <> order_id
    ) state.open_sell_orders;
    
    Logging.debug_f ~section "Cancel-replace detected for %s on %s: cleaned up pending_amend, kept InFlightOrders guard"
      order_id asset_symbol
  end else begin
    (* Genuine cancellation: full cleanup *)
    (* Add to cancelled orders blacklist to prevent re-adding during sync *)
    let now = Unix.time () in
    state.cancelled_orders <- (order_id, now) :: state.cancelled_orders;
    
    let cancelled_side = side in

    (* Remove from pending orders if it's there (matches by order_id OR side for ghost placements) *)
    let original_pending_count = List.length state.pending_orders in
    state.pending_orders <- List.filter (fun (pending_id, s, _, _) ->
      let matches_id = pending_id = order_id in
      let is_ghost_placement = (s = cancelled_side) && 
                              (String.starts_with ~prefix:"pending_buy_" pending_id || 
                               String.starts_with ~prefix:"pending_sell_" pending_id) in
      not (matches_id || is_ghost_placement)
    ) state.pending_orders;
    let removed_pending = original_pending_count - List.length state.pending_orders in
    
    (* If this was a tracked buy order, clear it *)
    let was_tracked_buy = match state.last_buy_order_id with
      | Some id when id = order_id -> true
      | _ -> false
    in
    
    if was_tracked_buy then begin
         state.last_buy_order_id <- None;
         state.last_buy_order_price <- None;
         Logging.debug_f ~section "Cancelled buy order %s removed from tracking for %s (blacklisted)" order_id asset_symbol
    end;
    
    (* Remove from sell orders list *)
    let original_sell_count = List.length state.open_sell_orders in
    state.open_sell_orders <- List.filter (fun (sell_id, _, _) ->
      sell_id <> order_id
    ) state.open_sell_orders;
    let removed_sell = original_sell_count - List.length state.open_sell_orders in

    (* Clean up global placement trackers to allow immediate re-placement *)
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol cancelled_side));
    
    Logging.debug_f ~section "Order cancelled and cleaned up: %s for %s (removed %d pending, %d sell, added to blacklist)"
      order_id asset_symbol removed_pending removed_sell
  end
  )

(** Handle Cancel-Replace Order Amendment - swap old ID for new ID *)
let handle_order_amended asset_symbol old_order_id new_order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* 1. Remove the pending amend entry using old_order_id (or new_order_id) *)
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         (String.sub pending_id 14 (String.length pending_id - 14) = old_order_id ||
                          String.sub pending_id 14 (String.length pending_id - 14) = new_order_id) in
      not matches_amend
    ) state.pending_orders;

    (* 1.5 Add old order ID to cancelled orders blacklist to prevent it from acting as a ghost order
       if the exchange's data feed (e.g. Hyperliquid webData2) takes a few seconds to drop the old order.
       CRITICAL: Only blacklist if IDs differ, as Kraken amendments often preserve the original order ID! *)
    if old_order_id <> new_order_id then begin
      let now = Unix.time () in
      state.cancelled_orders <- (old_order_id, now) :: state.cancelled_orders;
    end;

    (* 2. Update the active order tracking - swap old ID to new ID *)
    (match side with
     | Buy ->
         (match state.last_buy_order_id with
          | Some target_id when target_id = old_order_id ->
              state.last_buy_order_id <- Some new_order_id;
              state.last_buy_order_price <- Some price;
              Logging.info_f ~section "Amended buy order ID in tracking: %s -> %s @ %.2f for %s" 
                old_order_id new_order_id price asset_symbol
          | _ -> 
              (* Fallback in case state got wiped or mismatched *)
              state.last_buy_order_id <- Some new_order_id;
              state.last_buy_order_price <- Some price;
              Logging.debug_f ~section "Set buy order ID via amend: %s @ %.2f for %s" 
                new_order_id price asset_symbol)
     | Sell ->
         let original_sell_count = List.length state.open_sell_orders in
         let old_qty = match List.find_opt (fun (id, _, _) -> id = old_order_id) state.open_sell_orders with
           | Some (_, _, q) -> q | None -> 0.0 in
         state.open_sell_orders <- (new_order_id, price, old_qty) :: 
            List.filter (fun (sell_id, _, _) -> sell_id <> old_order_id) state.open_sell_orders;
         if List.length state.open_sell_orders = original_sell_count then
           Logging.info_f ~section "Amended sell order ID in tracking: %s -> %s @ %.2f for %s" 
             old_order_id new_order_id price asset_symbol
         else
           Logging.debug_f ~section "Set sell order ID via amend: %s @ %.2f for %s" 
             new_order_id price asset_symbol);
             
    Logging.debug_f ~section "Order amended and tracking updated: %s %s -> %s @ %.2f for %s"
      (string_of_order_side side) old_order_id new_order_id price asset_symbol
  )

(** Handle Skipped Order Amendment - clear pending lock but don't swap IDs *)
let handle_order_amendment_skipped asset_symbol order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* 1. Remove the pending amend entry using order_id *)
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         String.sub pending_id 14 (String.length pending_id - 14) = order_id in
      not matches_amend
    ) state.pending_orders;
    
    Logging.debug_f ~section "Order amendment skipped for identical price, removed tracking guard: %s %s @ %.2f for %s"
      (string_of_order_side side) order_id price asset_symbol
  )

(** Handle order amendment failure - clear tracking so we can try to place a replacement *)
let handle_order_amendment_failed asset_symbol order_id side reason =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* 1. Remove the pending amend entry *)
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         String.length pending_id > 14 &&
                         String.sub pending_id 14 (String.length pending_id - 14) = order_id in
      not matches_amend
    ) state.pending_orders;

    (* 2. If this was the main tracked order, step down tracking.
       ALWAYS clear tracking on amendment failure — the buy_order_pending guard
       prevents the false-positive scenario of amending before indexing, so if we
       get here the order is genuinely dead or unreachable. Clearing allows recovery. *)
    (match side with
     | Buy ->
         (match state.last_buy_order_id with
          | Some target_id when target_id = order_id ->
              state.last_buy_order_id <- None;
              state.last_buy_order_price <- None;
              let now = Unix.time () in
              state.cancelled_orders <- (order_id, now) :: state.cancelled_orders;
              Logging.info_f ~section "Amendment failed for buy order %s: cleared tracking (%s)" order_id reason
          | _ -> ())
     | Sell ->
         let original = List.length state.open_sell_orders in
         state.open_sell_orders <- List.filter (fun (sell_id, _, _) -> sell_id <> order_id) state.open_sell_orders;
         if List.length state.open_sell_orders < original then begin
           let now = Unix.time () in
           state.cancelled_orders <- (order_id, now) :: state.cancelled_orders;
           Logging.info_f ~section "Amendment failed for sell order %s: cleared tracking (%s)" order_id reason
         end);
           
    (* Clear global in-flight trackers so we don't get stuck *)
    ignore (InFlightAmendments.remove_in_flight_amendment order_id);
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol side));
  )

(** Clean up pending cancellation tracking for a completed order *)
let cleanup_pending_cancellation asset_symbol order_id =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
  Hashtbl.remove state.pending_cancellations order_id
  )

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
  type config = trading_config
  
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
  let handle_order_failed = handle_order_failed
  let handle_order_cancelled = handle_order_cancelled
  let handle_order_filled = handle_order_filled
  let handle_order_amended = handle_order_amended
  let handle_order_amendment_skipped = handle_order_amendment_skipped
  let handle_order_amendment_failed = handle_order_amendment_failed
  let cleanup_pending_cancellation = cleanup_pending_cancellation
  let cleanup_strategy_state = cleanup_strategy_state
  let init = init
end