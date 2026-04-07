(**
   Market making strategy (strategy: MM).

   Maintains a single buy/sell order pair around the top of book. Buy price
   is derived from the ask minus a fee-based backoff, clamped at the best bid.
   Sell price targets the best ask. Orders are written to a shared ring buffer
   for asynchronous execution by the order executor.

   Supports per-symbol balance limits (min_usd_balance, max_exposure) and
   pauses order placement when limits are breached or the exchange rejects
   orders due to insufficient funds.
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

open Strategy_common
module Exchange = Dio_exchange.Exchange_intf



(** Returns the first [n] elements of [list]. *)
let rec take n = function
  | [] -> []
  | x :: xs -> if n <= 0 then [] else x :: take (n - 1) xs

(** Per-asset trading configuration parsed from config.json. *)
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

(** Global order ring buffer shared across all strategy domains. *)
let order_buffer = OrderRingBuffer.create 2048
let order_buffer_mutex = Mutex.create ()

(** Accessor for the shared order ring buffer. *)
let get_order_buffer () = order_buffer

(** Mutable per-symbol strategy state. Tracks orders, balances, and in-flight status. *)
type strategy_state = {
  mutable last_buy_order_price: float option;
  mutable last_buy_order_id: string option;
  mutable open_sell_orders: (string * float * float) list;  (** (order_id, price, qty) *)
  mutable pending_orders: (string * order_side * float * float) list;  (** (order_id, side, price, timestamp) unacknowledged orders *)
  mutable last_cycle: int;
  mutable cancelled_orders: (string * float) list;  (** (order_id, timestamp) blacklist of recently cancelled orders *)
  mutable pending_cancellations: (string, float) Hashtbl.t;  (** order_id -> timestamp for pending cancel operations *)
  mutable last_cleanup_time: float; (** Unix timestamp of last cleanup pass *)
  mutable inflight_buy: bool;         (** true while a buy Place is unacknowledged *)
  mutable inflight_sell: bool;        (** true while a sell Place is unacknowledged *)
  mutable capital_low: bool;          (** true when quote balance is insufficient; pauses placement *)
  mutable asset_low: bool;            (** true when asset balance is insufficient; pauses sell placement *)
  mutable capital_low_logged: bool;   (** suppresses repeated capital_low warnings *)
  mutable last_seen_asset_balance: float;  (** last observed balance; used to detect genuine recovery *)
  mutex: Mutex.t;  (** guards concurrent access from callback handlers *)
}

(** Per-symbol strategy state table. *)
let strategy_states : (string, strategy_state) Hashtbl.t = Hashtbl.create 16
let strategy_states_mutex = Mutex.create ()

(** Returns existing state for [asset_symbol], or creates and registers a new default. *)
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

(** Parses a string config value to float, logging and returning [default] on failure. *)
let parse_config_float config value_name default exchange symbol =
  try float_of_string config with
  | Failure _ ->
      Logging.warn_f ~section "Invalid %s value '%s' for %s/%s, using default %.4f"
        value_name config exchange symbol default;
      default

(** Parses an optional string config value to [float option]. Returns [None] for empty or invalid input. *)
let parse_config_float_opt config value_name exchange symbol =
  if config = "" then None
  else
    try Some (float_of_string config) with
    | Failure _ ->
        Logging.warn_f ~section "Invalid %s value '%s' for %s/%s, ignoring parameter"
          value_name config exchange symbol;
        None

(** Rounds [price] to the instrument tick size via the exchange adapter. *)
let round_price price symbol exchange =
  match Exchange.Registry.get exchange with
  | Some (module Ex : Exchange.S) -> Ex.round_price ~symbol ~price
  | None ->
      Logging.warn_f ~section "No exchange found for %s/%s, using default rounding" exchange symbol;
      Float.round price  (** Fallback: 0 decimal places *)


(** Rounds [qty] down to the instrument lot size, clamped to [max_qty]. *)
let round_qty qty symbol exchange ~max_qty =
  (* Floor to qty_increment; prevents exceeding available balance *)
  let increment = match Exchange.Registry.get exchange with
  | Some (module Ex : Exchange.S) -> Ex.get_qty_increment ~symbol
  | None -> None
  in
  match increment with
  | Some inc ->
      let rounded = Float.floor (qty /. inc) *. inc in
      Float.min rounded max_qty
  | None ->
      Logging.warn_f ~section "No qty increment info for %s/%s, using 8 decimal places" exchange symbol;
      let rounded = Float.floor (qty *. 100000000.0) /. 100000000.0 in
      Float.min rounded max_qty

(** Rounds [price] down (floor) to the instrument tick size. Applies a relative
    epsilon to correct floating-point drift at exact increment boundaries. *)
let round_price_down price symbol exchange =
  let increment = match Exchange.Registry.get exchange with
  | Some (module Ex : Exchange.S) -> Ex.get_price_increment ~symbol
  | None -> None
  in
  match increment with
  | Some inc ->
      (* Epsilon corrects float division drift, e.g., 70.50/0.00001 -> 7049999.999... *)
      let steps = price /. inc in
      let rounded = Float.floor (steps +. 1e-9) *. inc in
      (* Snap to decimal grid to eliminate accumulated float error *)
      let decimal_places = Float.round (-.Float.log10 inc) in
      let factor = 10.0 ** decimal_places in
      Float.floor (rounded *. factor +. 0.5) /. factor
  | None ->
      Logging.warn_f ~section "No price increment info for %s/%s, using 2 decimal places" exchange symbol;
      Float.floor (price *. 100.0) /. 100.0

(** Returns the applicable fee rate from config. Prefers maker_fee; falls back to taker_fee or 0.0026. *)
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


(** Returns true if [qty] meets the exchange's minimum order size for [symbol]. *)
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

(** Generates a side-specific duplicate key for InFlightOrders deduplication. *)
let generate_side_duplicate_key asset_symbol side =
  Printf.sprintf "%s|%s|mm" asset_symbol (string_of_order_side side)

(** Constructs a Place strategy_order. *)
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
    userref = Some Strategy_common.strategy_userref_mm;  (* tags this order as MM strategy *)
    strategy;
    duplicate_key = generate_side_duplicate_key asset_symbol side;
  }

(** Constructs an Amend strategy_order targeting [order_id]. *)
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
    userref = None;
    strategy;
    duplicate_key = "";
  }

(** Constructs a Cancel strategy_order targeting [order_id]. *)
let create_cancel_order order_id asset_symbol strategy exchange =
  {
    operation = Cancel;
    order_id = Some order_id;
    symbol = asset_symbol;
    exchange;
    side = Buy;  (* side field unused for Cancel operations *)
    order_type = "limit";
    qty = 0.0;
    price = None;
    time_in_force = "GTC";
    post_only = false;
    userref = None;
    strategy;
    duplicate_key = "";
  }

(** Writes an order to the ring buffer after deduplication.
    Returns true on success, false if duplicate or buffer full.
    Updates in-flight flags, pending order tracking, and broadcasts OrderSignal. *)
let push_order ?(now = Unix.time ()) order =
  let operation_str = match order.operation with
    | Place -> "place"
    | Amend -> "amend"
    | Cancel -> "cancel"
  in

  (* Reject duplicate cancellations using pending_cancellations table *)
  (match order.operation with
   | Cancel ->
       let state = get_strategy_state order.symbol in
       (match order.order_id with
        | Some target_order_id ->
            if Hashtbl.mem state.pending_cancellations target_order_id then begin
              Logging.debug_f ~section "Skipping duplicate cancellation for order %s (already pending)" target_order_id;
              false
            end else begin
              (* Non-duplicate cancel; write to ring buffer *)
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

                   (* Record cancel to prevent duplicate submissions *)
                   Hashtbl.add state.pending_cancellations target_order_id now;
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
       (* Deduplicate Place and Amend using InFlightOrders/InFlightAmendments *)
       let is_duplicate = match order.operation with
         | Place -> not (InFlightOrders.add_in_flight_order order.duplicate_key)
         | Amend -> (match order.order_id with Some oid -> not (InFlightAmendments.add_in_flight_amendment oid) | None -> false)
         | _ -> false
       in

       if is_duplicate then begin
         Logging.debug_f ~section "Duplicate %s detected (strategy): %s" operation_str order.symbol;
         false
       end else begin
         (* Write Place or Amend to ring buffer *)
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

            (* Update per-symbol strategy state *)
            let state = get_strategy_state order.symbol in

            (* Set inflight flags for Place operations *)
            (match order.operation, order.side with
             | Place, Buy -> state.inflight_buy <- true
             | Place, Sell -> state.inflight_sell <- true
             | _ -> ());

            (* Operation-specific pending order tracking *)
            (match order.operation with
             | Place ->
                (* Hyperliquid: skip pending tracking for sells. Sell orders are
                   fire-and-forget; tracking creates ghost entries that trigger
                   re-placement loops during cancel-replace amendments.
                   Kraken retains pending sell tracking. *)
                let skip_pending = order.exchange = "hyperliquid" && order.side = Sell in
                if not skip_pending then begin
                  (* Assign temporary ID until exchange acknowledgment provides the real one *)
                  let temp_order_id = Printf.sprintf "pending_%s_%.2f"
                    (string_of_order_side order.side)
                    (Option.value order.price ~default:0.0) in
                  let order_price = Option.value order.price ~default:0.0 in
                  let timestamp = now in
                  state.pending_orders <- (temp_order_id, order.side, order_price, timestamp) :: state.pending_orders;
                  Logging.debug_f ~section "Added pending order: %s %s @ %.2f for %s"
                    (string_of_order_side order.side) temp_order_id order_price order.symbol;

                  (* Track sell order price under temporary ID *)
                  (match order.side, order.price with
                   | Sell, Some price ->
                       let order_id = temp_order_id in
                       state.open_sell_orders <- (order_id, price, order.qty) :: state.open_sell_orders
                   | _ -> ())
                end
            | Amend ->
                (* Track amendment as pending; do not add to sell orders until acknowledged *)
                let temp_order_id = Printf.sprintf "pending_amend_%s"
                  (Option.value order.order_id ~default:"unknown") in
                let order_price = Option.value order.price ~default:0.0 in
                let timestamp = now in
                state.pending_orders <- (temp_order_id, order.side, order_price, timestamp) :: state.pending_orders;
                Logging.debug_f ~section "Added pending amend: %s %s @ %.2f for %s (target: %s)"
                  (string_of_order_side order.side) temp_order_id order_price order.symbol
                  (Option.value order.order_id ~default:"unknown")
            | Cancel -> (* handled in Cancel branch above *)
                ());
           true
       | None ->
           (* Rollback in-flight tracking on failed buffer write *)
           (match order.operation with
            | Place -> ignore (InFlightOrders.remove_in_flight_order order.duplicate_key)
            | Amend -> (match order.order_id with Some oid -> ignore (InFlightAmendments.remove_in_flight_amendment oid) | None -> ())
            | _ -> ());

           Logging.warn_f ~section "Order ringbuffer full, dropped %s %s order for %s"
             operation_str (string_of_order_side order.side) order.symbol;
           false
       end)

(** Cancels all open orders at [target_price] for [target_side]. Returns count cancelled. *)
let cancel_duplicate_orders asset_symbol target_price target_side open_orders strategy exchange =
  let duplicates = List.filter (fun (order_id, order_price, qty, side_str, _) ->
    let is_cancelled = List.exists (fun (cancelled_id, _) ->
      cancelled_id = order_id) (get_strategy_state asset_symbol).cancelled_orders in
    not is_cancelled && qty > 0.0 &&
    side_str = (if target_side = Buy then "buy" else "sell") &&
    abs_float (order_price -. target_price) < 0.000001  (* price match within float epsilon *)
  ) open_orders in

  List.iter (fun (order_id, _, _, _, _) ->
    let cancel_order = create_cancel_order order_id asset_symbol strategy exchange in
    ignore (push_order cancel_order);
    Logging.debug_f ~section "Cancelling duplicate order %s for %s at price %.8f"
      order_id asset_symbol target_price
  ) duplicates;

  List.length duplicates

(** Core strategy tick. Synchronizes state with exchange open orders, performs
    pending/cancelled order cleanup, enforces balance and exposure limits, and
    places/amends/cancels buy and sell orders to maintain one active pair. *)
let execute_strategy
    ?cached_state
    (asset : trading_config)
    (current_price : float option)
    (top_of_book : (float * float * float * float) option)
    (asset_balance : float option)
    (quote_balance : float option)
    (_open_buy_count : int)
    (_open_sell_count : int)
    (open_orders : (string * float * float * string * int option) list)  (* (order_id, price, remaining_qty, side, userref) *)
    (cycle : int) =

  (* Throttle logging to every log_interval cycles *)
  let should_log = cycle mod log_interval = 0 in

  let state = match cached_state with Some s -> s | None -> get_strategy_state asset.symbol in

  (* Asset-low recovery: clear flag when asset balance recovers.
     Kraken: only clear when balance has genuinely increased (fill, deposit).
     Kraken "Insufficient funds" on sells may reflect USD collateral, not asset
     balance. Clearing on every cycle would cause a tight retry loop.
     Hyperliquid: clear whenever available balance meets the threshold. *)
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

  (* Capital-low fast path: skip strategy body when quote balance is insufficient
     for a buy order. Clears flag and resumes when balance recovers. *)
  (match (quote_balance, current_price) with
   | Some quote_bal, Some price ->
       let qty_f = (try float_of_string asset.qty with Failure _ -> 0.001) in
       let quote_needed_fast = price *. qty_f in
       if state.capital_low && quote_bal < quote_needed_fast then begin
         (* Still insufficient; skip strategy body to prevent order spam *)
         ()
       end else if state.capital_low && quote_bal >= quote_needed_fast then begin
         (* Recovered; clear flag and proceed to normal logic *)
         state.capital_low <- false;
         state.capital_low_logged <- false;
         state.inflight_buy <- false;
         ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset.symbol Buy));
         Logging.info_f ~section "Capital restored for %s (have %.2f, need %.2f) - resuming strategy"
           asset.symbol quote_bal quote_needed_fast
       end
       (* capital_low=false: proceed normally *)
   | _ -> () (* No balance or price data available; proceed *)
  );

  (* Proceed only when capital_low is clear.
     asset_low is checked separately for sell and buy placement; order sync,
     cleanup, and cancellation detection always execute regardless. *)
  if not state.capital_low then begin

  let now = Unix.time () in

      (* Evict stale pending orders (older than 5s) and cap list at 50 entries *)
  let original_count = List.length state.pending_orders in
  state.pending_orders <- List.filter (fun (order_id, side, _, timestamp) ->
    let age = now -. timestamp in
    if age > 5.0 then begin
      if should_log then Logging.warn_f ~section "Removing stale pending order %s for %s (age: %.1fs)" order_id asset.symbol age;
      
      (* Release global trackers to permit immediate re-placement *)
      if String.starts_with ~prefix:"pending_amend_" order_id then begin
        let target_oid = String.sub order_id 14 (String.length order_id - 14) in
        ignore (InFlightAmendments.remove_in_flight_amendment target_oid)
      end else begin
        let duplicate_key = generate_side_duplicate_key asset.symbol side in
        ignore (InFlightOrders.remove_in_flight_order duplicate_key);
        (* Clear inflight flags for stale placements *)
        (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false)
      end;
      false
    end else
      true
  ) state.pending_orders;

  (* Cap pending orders at 50 to bound memory usage *)
  if List.length state.pending_orders > 50 then begin
    let excess = List.length state.pending_orders - 50 in
    state.pending_orders <- take 50 state.pending_orders;
    if should_log then Logging.warn_f ~section "Truncated %d excess pending orders for %s (kept 50)" excess asset.symbol;
  end;

  (* Log if we cleaned up many orders *)
  let cleaned_count = original_count - List.length state.pending_orders in
  if cleaned_count > 0 && should_log then
    Logging.debug_f ~section "Cleaned up %d pending orders for %s" cleaned_count asset.symbol;

  (* Evict cancelled order blacklist entries older than 15s; cap at 20 *)
  let original_cancelled_count = List.length state.cancelled_orders in
  state.cancelled_orders <- List.filter (fun (_, timestamp) ->
    now -. timestamp < 15.0
  ) state.cancelled_orders;

  (* Cap cancelled orders blacklist at 20 to bound memory usage *)
  if List.length state.cancelled_orders > 20 then begin
    let excess = List.length state.cancelled_orders - 20 in
    state.cancelled_orders <- take 20 state.cancelled_orders;
    if should_log then Logging.warn_f ~section "Truncated %d excess cancelled orders for %s (kept 20)" excess asset.symbol;
  end;

  (* Evict stale pending cancellations (older than 30s) via scan-and-remove *)
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


  (* state.mutex is intentionally not locked in this function.
     The strategy runs in its own OCaml domain; handler callbacks
     (handle_order_amended, etc.) dispatch via Lwt.async in the Lwt domain.
     Locking the non-recursive Mutex.t here would cause EDEADLK on
     concurrent callbacks. Handlers lock state.mutex for inter-domain
     safety, which is correct and sufficient. *)


  match current_price, top_of_book with
  | Some price, Some (bid, _bid_size, ask, _ask_size) ->
      begin
        (* Log raw bid/ask for diagnostics *)
        if should_log then Logging.debug_f ~section "DEBUG Asset [%s/%s]: Received bid=%.8f, ask=%.8f, price=%.8f"
          asset.exchange asset.symbol bid ask price;

        (* Parse config values *)
        let qty = parse_config_float asset.qty "qty" 0.001 asset.exchange asset.symbol in
        let min_usd_balance_opt = match asset.min_usd_balance with
          | Some balance_str -> parse_config_float_opt balance_str "min_usd_balance" asset.exchange asset.symbol
          | None -> None
        in
        let max_exposure_opt = match asset.max_exposure with
          | Some exposure_str -> parse_config_float_opt exposure_str "max_exposure" asset.exchange asset.symbol
          | None -> None
        in

        (* Resolve fee: prefer cached exchange fee, fallback to config *)
        let fee = match Fee_cache.get_maker_fee ~exchange:asset.exchange ~symbol:asset.symbol with
          | Some cached_fee -> cached_fee
          | None -> get_fee_for_asset asset
        in

        (* Compute spread metrics *)
        let spread = ask -. bid in
        let mid_price = (bid +. ask) /. 2.0 in
        let spread_pct = if mid_price > 0.0 then spread /. mid_price else 0.0 in

        (* Compute available balances net of open order commitments *)
        let available_asset_balance = match asset_balance with
          | Some total_balance ->
              (* Deduct qty locked in open sells *)
              let locked_in_sells = List.fold_left (fun acc (_, _, remaining_qty, side_str, _) ->
                if side_str = "sell" then acc +. remaining_qty else acc
              ) 0.0 open_orders in
              Some (total_balance -. locked_in_sells)
          | None -> None
        in

        let available_quote_balance = match quote_balance with
          | Some total_balance ->
              (* Deduct value locked in open buys *)
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

        (* Log computed spread and exposure values *)
        if should_log then Logging.debug_f ~section "DEBUG Asset [%s/%s]: spread=%.8f, spread_pct=%.8f, fee=%.8f, exposure=%.2f"
          asset.exchange asset.symbol spread spread_pct fee exposure_value;

        (* Count non-cancelled open buy orders for balance check *)
        let open_buy_orders = List.filter (fun (order_id, _, qty, side_str, _) ->
          let is_cancelled = List.exists (fun (cancelled_id, _) -> cancelled_id = order_id) state.cancelled_orders in
          side_str = "buy" && not is_cancelled && qty > 0.0
        ) open_orders in

        let open_buy_count = List.length open_buy_orders in

        (* Balance constraints: enforced only when config parameters are set *)
        let usd_balance_ok = match min_usd_balance_opt, available_quote_balance with
          | Some min_balance, Some qb ->
              (* Exclude existing open buy cost to avoid double-counting *)
              let buy_cost = if open_buy_count > 0 then 0.0 else bid *. qty in
              qb -. buy_cost >= min_balance
          | Some _, None -> false  (* no quote balance data; fail-safe *)
          | None, _ -> true        (* no min_balance constraint configured *)
        in
        let exposure_ok = match max_exposure_opt, available_asset_balance with
          | Some max_exp, Some ab ->
              (* Verify post-sell exposure remains within limit *)
              let new_exposure = (ab -. qty) *. mid_price in
              new_exposure <= max_exp
          | Some _, None -> false  (* no asset balance data; fail-safe *)
          | None, _ -> true        (* no max_exposure constraint *)
        in


        if should_log then Logging.info_f ~section "Strategy check for %s: open_buy_count=%d, bid=%.8f, ask=%.8f, exposure_ok=%B, usd_ok=%B"
          asset.symbol open_buy_count bid ask exposure_ok usd_balance_ok;

        (* Exposure or balance limit breach: cancel buys, place emergency sell for free balance *)
        if not exposure_ok || not usd_balance_ok then begin
          (* Pause: cancel all open buy orders for this asset *)
          if should_log then Logging.debug_f ~section "Balance/exposure limits exceeded for %s (exposure_ok=%B, usd_ok=%B), pausing strategy"
            asset.symbol exposure_ok usd_balance_ok;

          (* Sync tracking with actual open orders from exchange *)
          state.open_sell_orders <- [];
          (* Hyperliquid: preserve buy tracking across sync. webData2 lags
             execution events during cancel-replace amendments, so the exchange
             may briefly report 0 buys while one exists internally.
             effective_buy_count logic handles this when tracking is preserved.
             Genuine cancellations clear tracking via handle_order_cancelled.
             Kraken clears tracking since its data feed is authoritative. *)
          if asset.exchange <> "hyperliquid" then begin
            state.last_buy_order_price <- None;
            state.last_buy_order_id <- None
          end;
          (* pending_orders not cleared here; managed by order response handlers *)

          (* Rebuild tracking from exchange open orders, filtering blacklisted cancels *)
          let buy_orders = ref [] in
          let sell_orders = ref [] in

          List.iter (fun (order_id, order_price, qty, side_str, userref_opt) ->
            (* Skip blacklisted (cancelled) orders *)
            let is_cancelled = List.exists (fun (cancelled_id, _) -> cancelled_id = order_id) state.cancelled_orders in
            
            let is_our_strategy = match userref_opt with
              | Some ref_val -> ref_val = Strategy_common.strategy_userref_mm
              | None -> false
            in

            if not is_cancelled && qty > 0.0 && is_our_strategy then
              (* Classify by the exchange-reported side, not by price *)
              if side_str = "buy" then
                (* Enforce single-buy-order policy across all exchanges *)
                buy_orders := (order_id, order_price) :: !buy_orders
              else
                (* Include all open sell orders regardless of tag *)
                sell_orders := (order_id, order_price, qty) :: !sell_orders
          ) open_orders;

          (* Track the highest-priced buy order *)
          (match !buy_orders with
           | [] -> ()
           | orders ->
               let (best_order_id, best_price) = List.fold_left (fun (acc_id, acc_price) (order_id, price) ->
                 if price > acc_price then (order_id, price) else (acc_id, acc_price)
               ) (List.hd orders) (List.tl orders) in
               state.last_buy_order_price <- Some best_price;
               state.last_buy_order_id <- Some best_order_id);

          (* Update sell order tracking *)
          state.open_sell_orders <- !sell_orders;

          (* Cancel all open buy orders; deduplicate tracked and exchange-reported order IDs *)
          let buys_to_cancel = 
            let tracked_id = match state.last_buy_order_id with Some id -> [id] | None -> [] in
            let open_ids = List.map (fun (id, _) -> id) !buy_orders in
            List.sort_uniq String.compare (tracked_id @ open_ids) 
          in
          
          List.iter (fun buy_id ->
             let cancel_order = create_cancel_order buy_id asset.symbol MM asset.exchange in
             ignore (push_order ~now cancel_order);
             if should_log then Logging.debug_f ~section "Cancelling buy order due to pause: %s for %s" buy_id asset.symbol
          ) buys_to_cancel;

          (* Clear buy tracking after cancellation *)
          state.last_buy_order_price <- None;
          state.last_buy_order_id <- None;

          (* Place emergency sell for free asset balance at best ask.
             Skipped if a sell order is already in-flight to prevent duplicates. *)
           if state.inflight_sell then begin
              if should_log then Logging.debug_f ~section "Skipping emergency sell for %s: sell order in-flight" asset.symbol
          end else begin
            match available_asset_balance with
            | Some ab when ab > 0.0 ->
                (* Round qty down to lot size; clamp to available balance *)
                let rounded_qty = round_qty ab asset.symbol asset.exchange ~max_qty:ab in
                
                (* Sell free balance if it meets minimum qty; do not cancel existing orders *)
                if meets_min_qty asset.symbol rounded_qty asset.exchange then begin
                   let sell_price = round_price ask asset.symbol asset.exchange in
                   let sell_order = create_place_order asset.symbol Sell rounded_qty (Some sell_price) true MM asset.exchange in
                   ignore (push_order ~now sell_order);
                   if should_log then Logging.info_f ~section "Placed emergency sell order for free balance: %.8f @ %.2f for %s" rounded_qty sell_price asset.symbol
                end
            | _ -> ()
          end;

          (* Defer further action to next event trigger *)
          state.last_cycle <- cycle

        end else begin
          (* Balance and exposure OK: proceed to buy order management *)

          (* Rebuild tracking from exchange open orders.
             Hyperliquid: preserve buy tracking because webData2 can lag. *)
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

          (* Sync tracking from exchange-reported open orders *)
          (match !buy_orders with
           | [] -> ()
           | orders ->
               let (best_order_id, best_price) = List.fold_left (fun (acc_id, acc_price) (order_id, price) ->
                 if price > acc_price then (order_id, price) else (acc_id, acc_price)
               ) (List.hd orders) (List.tl orders) in
               state.last_buy_order_price <- Some best_price;
               state.last_buy_order_id <- Some best_order_id);

          state.open_sell_orders <- !sell_orders;

          (* Derive effective open buy count by merging internal tracking with exchange data *)
          let has_tracked_buy = state.last_buy_order_id <> None in
          let sync_open_buy_count = List.length !buy_orders in
          
          (* Hyperliquid: if tracking shows a buy but webData2 reports 0, trust tracking.
             Genuine cancellations clear tracking via handle_order_cancelled. *)
          let actual_open_buy_count = 
            if asset.exchange = "hyperliquid" && has_tracked_buy && sync_open_buy_count = 0 then 1
            else sync_open_buy_count 
          in

          if should_log then Logging.debug_f ~section "Order management for %s: open_buy_count=%d, open_sell_count=%d" 
            asset.symbol actual_open_buy_count (List.length !sell_orders);

          (* Buy order management: maintain exactly one active buy at a profitable level *)
          if actual_open_buy_count > 1 then begin
            (* Multiple open buys: cancel all; next trigger re-places a single order *)
            if should_log then Logging.debug_f ~section "Cancelling %d excess buy orders for %s" actual_open_buy_count asset.symbol;
            
            List.iter (fun (buy_id, _) ->
              let cancel_order = create_cancel_order buy_id asset.symbol MM asset.exchange in
              ignore (push_order ~now cancel_order)
            ) !buy_orders;
            
            state.last_buy_order_price <- None;
            state.last_buy_order_id <- None;
            state.last_cycle <- cycle           end else if actual_open_buy_count = 0 then begin
            (* 0 open buys: place new buy+sell pair.
               Guarded: skip if a buy order is already in-flight. *)
            if state.inflight_buy then begin
              if should_log then Logging.debug_f ~section "Skipping pair placement for %s: buy order in-flight" asset.symbol
            end else begin
            if should_log then Logging.debug_f ~section "No buy orders for %s, placing new pair" asset.symbol;

            (* Buy price derivation:
               fee == 0: buy at best bid.
               fee > 0: buy at ask * (1 - (2*fee + 0.0001)), clamped at bid.
               Clamping captures wider spreads when the book is favorable. *)
               
            let buy_price_raw = 
              if fee = 0.0 then bid
              else min bid (ask *. (1.0 -. (fee *. 2.0 +. 0.0001)))
            in
            
            let buy_price = round_price_down buy_price_raw asset.symbol asset.exchange in
            let sell_price = round_price ask asset.symbol asset.exchange in
            
            (* Profitability guard: verify spread covers round-trip fee cost *)
            let required_spread = ask *. (fee *. 2.0 +. 0.0001) in
            let final_spread = sell_price -. buy_price in
            let profitability_ok = if fee = 0.0 then true else final_spread >= required_spread in
            
            if profitability_ok then begin
              (* Post-only safety: verify prices remain within the book *)
              if buy_price <= (bid +. 0.0000001) && sell_price >= (ask -. 0.0000001) then begin
                  let can_place_sell = meets_min_qty asset.symbol qty asset.exchange in
                  let can_place_buy = match min_usd_balance_opt, available_quote_balance with
                    | Some min_bal, Some qb ->
                        let required_quote = buy_price *. qty in
                        qb >= required_quote && qb -. required_quote >= min_bal
                    | Some _, None -> false  
                    | None, _ -> true        
                  in

                  (* Place sell first *)
                  if can_place_sell then begin
                    let sell_order = create_place_order asset.symbol Sell qty (Some sell_price) true MM asset.exchange in
                    ignore (push_order ~now sell_order);
                    if should_log then Logging.info_f ~section "Placed sell order for %s: %.8f @ %.2f"
                      asset.symbol qty sell_price;
                  end;

                  (* Then place buy *)
                  if can_place_buy then begin
                    let _ = cancel_duplicate_orders asset.symbol buy_price Buy open_orders MM asset.exchange in
                    let buy_order = create_place_order asset.symbol Buy qty (Some buy_price) true MM asset.exchange in
                    if push_order ~now buy_order then begin
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
            
            end;  (* inflight_buy guard *)
            
            (* Defer further action to next event trigger *)
            state.last_cycle <- cycle

          end else begin
            (* Exactly 1 open buy: verify price matches required level.
               is_being_amended and is_in_flight guards prevent duplicate amendments. *)
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

                (* Guard: skip if amendment is already pending or in-flight *)
                let is_being_amended = List.exists (fun (id, _, _, _) ->
                  String.starts_with ~prefix:"pending_amend_" id &&
                  String.sub id 14 (String.length id - 14) = buy_order_id
                ) state.pending_orders in
                let is_in_flight = InFlightAmendments.is_in_flight buy_order_id in

                if is_being_amended || is_in_flight then begin
                  if should_log then Logging.debug_f ~section "Skipping amendment for %s: already in-flight or pending" asset.symbol
                end else if price_diff_rounded >= min_move_threshold && required_buy_price <> current_buy_price_rounded then begin
                  (* Price mismatch detected: amend buy to required price *)
                  (* Profitability guard *)
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

                  (* Proceed only if profitable, post-only safe, and balance is sufficient *)
                  if profitability_ok && required_buy_price <= (bid +. 0.0000001) && amendment_balance_ok then begin
                    let _ = cancel_duplicate_orders asset.symbol required_buy_price Buy open_orders MM asset.exchange in
                    let amend_order = create_amend_order buy_order_id asset.symbol Buy qty (Some required_buy_price) true MM asset.exchange in
                    ignore (push_order ~now amend_order);
                    state.last_buy_order_price <- Some required_buy_price;

                    if should_log then Logging.info_f ~section "Amended buy order for %s: %.8f -> %.8f (reason=book shift)"
                      asset.symbol current_buy_price required_buy_price;
                  end else begin
                    let cancel_order = create_cancel_order buy_order_id asset.symbol MM asset.exchange in
                    ignore (push_order ~now cancel_order)
                  end
                end else begin
                  (* Price already correct: no action required *)
                  if should_log then Logging.debug_f ~section "Buy order for %s already at correct price: %.8f" asset.symbol current_buy_price;
                end
            | _ -> Logging.warn_f ~section "Buy order tracking inconsistent for %s" asset.symbol
          end
        end;  (* end balance checks *)

        (* Update cycle counter *)
        state.last_cycle <- cycle

      end
  | _ ->
      begin
        Logging.debug_f ~section "No price or top-of-book data available for %s" asset.symbol;
        state.last_cycle <- cycle
      end

  end (* capital_low guard *)


(* Duplicate key generator for InFlightOrders. Key format must match
   create_place_order and push_order usage. *)
let generate_side_duplicate_key asset_symbol side =
  Printf.sprintf "%s|%s|mm" asset_symbol (string_of_order_side side)

(** Handles a successful order placement acknowledgment.
    Removes matching pending entries and updates buy order tracking. *)
let handle_order_acknowledged asset_symbol order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
  (* Remove from pending orders by matching placement prefix, amend ID, or fallback *)
  state.pending_orders <- List.filter (fun (pending_id, s, p, _) ->
    (* Match regular placements by side *)
    let is_placement_prefix = String.starts_with ~prefix:"pending_buy_" pending_id ||
                              String.starts_with ~prefix:"pending_sell_" pending_id in
    let matches_side_placement = is_placement_prefix && s = side in
    
    (* Match amendments by target order_id *)
    let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                       String.length pending_id > 14 &&
                       String.sub pending_id 14 (String.length pending_id - 14) = order_id in
    
    (* Fallback for test or legacy IDs without prefix *)
    let matches_fallback = not (String.starts_with ~prefix:"pending_" pending_id) &&
                          s = side && abs_float (p -. price) < 0.01 in
    
    not (matches_side_placement || matches_amend || matches_fallback)
  ) state.pending_orders;

  (* InFlightOrders key intentionally retained here. The guard persists while
     the order is active; cleanup occurs in handle_order_cancelled/filled or on timeout. *)

  (* Update buy order tracking on buy acknowledgment *)
  (match side with
   | Buy ->
    state.last_buy_order_id <- Some order_id;
    state.last_buy_order_price <- Some price;
    Logging.debug_f ~section "Updated buy order ID and price tracking: %s @ %.2f for %s" order_id price asset_symbol
   | Sell -> ());

  Logging.debug_f ~section "Order acknowledged and removed from pending: %s %s @ %.2f for %s"
    order_id (string_of_order_side side) price asset_symbol
  )

(** Handles an order placement failure. Clears in-flight trackers and sets
    capital_low or asset_low flags when the exchange reports insufficient funds. *)
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

    (* Detect insufficient balance errors in the exchange response *)
    let lower_reason = String.lowercase_ascii reason in
    let contains_fragment s fragment =
      let sl = String.length s and fl = String.length fragment in
      let rec loop i = i + fl <= sl && (String.sub s i fl = fragment || loop (i + 1)) in
      loop 0
    in
    let is_insufficient_balance = contains_fragment lower_reason "insufficient funds"
      || contains_fragment lower_reason "insufficient spot balance" in

    (* Set balance flags on insufficient funds.
       capital_low (buy) and asset_low (sell) halt further placement
       until balance recovers. Recovery is event-driven, no timers.. *)
    (match side with
     | Buy when is_insufficient_balance ->
         if not state.capital_low then begin
           state.capital_low <- true;
           state.capital_low_logged <- true;
           Logging.warn_f ~section "Exchange rejected buy for %s with insufficient funds - setting capital_low flag"
             asset_symbol
         end
     | Sell when is_insufficient_balance ->
          (* Kraken sell failures are fire-and-forget. "Insufficient funds"
             on sells reflects USD collateral, not asset balance.
             asset_low is not set; the sell fails and strategy continues. *)
          Logging.warn_f ~section "Exchange rejected sell for %s with insufficient balance (ignored, sell is fire-and-forget)"
            asset_symbol
     | _ -> ());
    
    Logging.warn_f ~section "Order failed for %s (%s): %s. Cleared in-flight tracker."
      asset_symbol (string_of_order_side side) reason
  )

(** Handles an order rejection. Removes pending entries and clears in-flight
    trackers to permit immediate re-placement on the next cycle. *)
let handle_order_rejected asset_symbol side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
  (* Remove matching pending entries by side or price fallback *)
  state.pending_orders <- List.filter (fun (pending_id, s, p, _) ->
    let is_placement_prefix = String.starts_with ~prefix:"pending_buy_" pending_id ||
                              String.starts_with ~prefix:"pending_sell_" pending_id in
    let matches_side_placement = is_placement_prefix && s = side in
    
    (* Amend rejections: match by ID or fall back to price *)
    let matches_amend_prefix = String.starts_with ~prefix:"pending_amend_" pending_id in
    
    let matches_fallback = (not (String.starts_with ~prefix:"pending_" pending_id) || matches_amend_prefix) &&
                          s = side && abs_float (p -. price) < 0.01 in
    
    not (matches_side_placement || matches_fallback)
  ) state.pending_orders;

  (* Clear inflight flags *)
  (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);

  (* Release global trackers to permit immediate re-placement *)
  let duplicate_key = generate_side_duplicate_key asset_symbol side in
  ignore (InFlightOrders.remove_in_flight_order duplicate_key);

  Logging.debug_f ~section "Order rejected and removed from pending/trackers: %s @ %.2f for %s"
    (string_of_order_side side) price asset_symbol
  )
  (* Buy rejections leave no active order; strategy re-evaluates on the next cycle *)

(** Handles a full order fill. Clears all tracking (pending amends, sell orders,
    buy order state) and releases in-flight guards. *)
let handle_order_filled asset_symbol order_id side ~fill_price:_ =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* Remove any pending amend referencing this order ID *)
    state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
      not (String.starts_with ~prefix:"pending_amend_" pending_id &&
           String.length pending_id > 14 &&
           String.sub pending_id 14 (String.length pending_id - 14) = order_id)
    ) state.pending_orders;

    (* Remove from sell orders tracking *)
    state.open_sell_orders <- List.filter (fun (sell_id, _, _) ->
      sell_id <> order_id
    ) state.open_sell_orders;

    (* Clear buy order tracking if this was the tracked buy *)
    let was_tracked_buy = match state.last_buy_order_id with
      | Some id when id = order_id -> true
      | _ -> false
    in
    if was_tracked_buy then begin
      state.last_buy_order_id <- None;
      state.last_buy_order_price <- None;
      Logging.debug_f ~section "Filled buy order %s removed from tracking for %s" order_id asset_symbol
    end;

    (* Release global placement trackers to permit immediate re-placement *)
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol side));

    Logging.debug_f ~section "Order FILLED and cleaned up explicitly: %s for %s"
      order_id asset_symbol
  )

(** Handles an order cancellation. Distinguishes cancel-replace (amendment)
    from genuine cancellation. Applies cleanup accordingly. *)
let handle_order_cancelled asset_symbol order_id side =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
  (* Detect cancel-replace amendments: if pending_amend_<order_id> exists,
     a replacement order is incoming. Preserve InFlightOrders guard and buy tracking. *)
  let is_cancel_replace = List.exists (fun (pending_id, _, _, _) ->
    String.starts_with ~prefix:"pending_amend_" pending_id &&
    let target_id = String.sub pending_id 14 (String.length pending_id - 14) in
    target_id = order_id
  ) state.pending_orders in

  if is_cancel_replace then begin
    (* Cancel-replace: remove pending_amend entry; keep in-flight guards active *)
    state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
      not (String.starts_with ~prefix:"pending_amend_" pending_id &&
           String.sub pending_id 14 (String.length pending_id - 14) = order_id)
    ) state.pending_orders;
    
    (* Remove old order from sell tracking *)
    state.open_sell_orders <- List.filter (fun (sell_id, _, _) ->
      sell_id <> order_id
    ) state.open_sell_orders;
    
    Logging.debug_f ~section "Cancel-replace detected for %s on %s: cleaned up pending_amend, kept InFlightOrders guard"
      order_id asset_symbol
  end else begin
    (* Genuine cancellation: full cleanup *)
    (* Blacklist order ID to prevent re-addition during sync *)
    let now = Unix.time () in
    state.cancelled_orders <- (order_id, now) :: state.cancelled_orders;
    
    let cancelled_side = side in

    (* Remove from pending orders by order_id or by side for ghost placements *)
    let original_pending_count = List.length state.pending_orders in
    state.pending_orders <- List.filter (fun (pending_id, s, _, _) ->
      let matches_id = pending_id = order_id in
      let is_ghost_placement = (s = cancelled_side) && 
                              (String.starts_with ~prefix:"pending_buy_" pending_id || 
                               String.starts_with ~prefix:"pending_sell_" pending_id) in
      not (matches_id || is_ghost_placement)
    ) state.pending_orders;
    let removed_pending = original_pending_count - List.length state.pending_orders in
    
    (* Clear buy tracking if this was the tracked buy order *)
    let was_tracked_buy = match state.last_buy_order_id with
      | Some id when id = order_id -> true
      | _ -> false
    in
    
    if was_tracked_buy then begin
         state.last_buy_order_id <- None;
         state.last_buy_order_price <- None;
         Logging.debug_f ~section "Cancelled buy order %s removed from tracking for %s (blacklisted)" order_id asset_symbol
    end;
    
    (* Remove from sell order tracking *)
    let original_sell_count = List.length state.open_sell_orders in
    state.open_sell_orders <- List.filter (fun (sell_id, _, _) ->
      sell_id <> order_id
    ) state.open_sell_orders;
    let removed_sell = original_sell_count - List.length state.open_sell_orders in

    (* Release global placement trackers to permit immediate re-placement *)
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol cancelled_side));
    
    Logging.debug_f ~section "Order cancelled and cleaned up: %s for %s (removed %d pending, %d sell, added to blacklist)"
      order_id asset_symbol removed_pending removed_sell
  end
  )

(** Handles a successful cancel-replace amendment. Swaps old order ID
    for new order ID in tracking. Blacklists old ID when IDs differ. *)
let handle_order_amended asset_symbol old_order_id new_order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* Remove pending amend entries matching old or new order ID *)
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         (String.sub pending_id 14 (String.length pending_id - 14) = old_order_id ||
                          String.sub pending_id 14 (String.length pending_id - 14) = new_order_id) in
      not matches_amend
    ) state.pending_orders;

    (* Blacklist old order ID to prevent ghost orders
       in lagging data feeds (e.g., Hyperliquid webData2).
       Skipped when IDs are identical because Kraken amendments often reuse the original order ID. *)
    if old_order_id <> new_order_id then begin
      let now = Unix.time () in
      state.cancelled_orders <- (old_order_id, now) :: state.cancelled_orders;
    end;

    (* Swap old ID to new ID in active order tracking *)
    (match side with
     | Buy ->
         (match state.last_buy_order_id with
          | Some target_id when target_id = old_order_id ->
              state.last_buy_order_id <- Some new_order_id;
              state.last_buy_order_price <- Some price;
              Logging.info_f ~section "Amended buy order ID in tracking: %s -> %s @ %.2f for %s" 
                old_order_id new_order_id price asset_symbol
          | _ -> 
              (* Fallback: state was wiped or order ID mismatched *)
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

(** Handles a no-op amendment (price unchanged). Removes the pending
    amend entry to release the amendment guard. *)
let handle_order_amendment_skipped asset_symbol order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* Remove pending amend entry for this order_id *)
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         String.sub pending_id 14 (String.length pending_id - 14) = order_id in
      not matches_amend
    ) state.pending_orders;
    
    Logging.debug_f ~section "Order amendment skipped for identical price, removed tracking guard: %s %s @ %.2f for %s"
      (string_of_order_side side) order_id price asset_symbol
  )

(** Handles an amendment failure. Clears tracking for the affected order
    and releases in-flight guards to permit recovery on the next cycle. *)
let handle_order_amendment_failed asset_symbol order_id side reason =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* Remove pending amend entry *)
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         String.length pending_id > 14 &&
                         String.sub pending_id 14 (String.length pending_id - 14) = order_id in
      not matches_amend
    ) state.pending_orders;

    (* Clear tracking on amendment failure. The inflight_buy guard prevents
       false-positive amend-before-index scenarios, so reaching this point
       indicates the order is dead or unreachable, so clearing enables recovery. *)
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
           
    (* Release global in-flight trackers to prevent resource deadlock *)
    ignore (InFlightAmendments.remove_in_flight_amendment order_id);
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol side));
  )

(** Removes a completed order from the pending_cancellations tracking table. *)
let cleanup_pending_cancellation asset_symbol order_id =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
  Hashtbl.remove state.pending_cancellations order_id
  )

(** Drains up to [max_orders] from the ring buffer, returning them in FIFO order. *)
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
           | None -> count := max_orders  (* exit loop *)
         done;
         List.rev !orders)
  in
  orders

(** Initializes the MM strategy module: fee cache and PRNG. *)
let init () =
  Logging.info_f ~section "Market Maker strategy initialized with order buffer size 4096";
  Fee_cache.init ();
  Random.self_init ()

(** Public strategy module interface. *)
module Strategy = struct
  type config = trading_config
  
  (** Removes strategy state for [symbol] during domain shutdown. *)
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