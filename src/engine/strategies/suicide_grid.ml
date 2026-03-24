(*
  Suicide Grid Strategy Implementation

  This strategy implements a grid trading system where:
  - Only 1 open buy order may exist at once
  - Many sell orders may exist at a time
  - Orders are placed at grid intervals from current price
  - Buy orders trail price movements upwards gated by sell orders existing above it,
    sell orders are placed above current price

  The strategy generates orders and pushes them to a ringbuffer for execution elsewhere.
*)


let section = "suicide_grid"

(** Use common types from Strategy_common module *)
open Strategy_common
module Exchange = Dio_exchange.Exchange_intf

(* ── exchange module cache ──────────────────────────────────────────────── *)
(** Memoises Exchange.Registry.get by exchange name.  The registry hashtable
    is walked only on the first call for each exchange; subsequent calls for
    the same exchange string hit this 2-entry cache directly, eliminating the
    repeated Hashtbl.find_opt on every round_price / get_price_increment call. *)
let _exchange_module_cache : (string, (module Exchange.S)) Hashtbl.t =
  Hashtbl.create 4

let get_exchange_module exchange =
  match Hashtbl.find_opt _exchange_module_cache exchange with
  | Some m -> Some m
  | None ->
      (match Exchange.Registry.get exchange with
       | Some m ->
           Hashtbl.replace _exchange_module_cache exchange m;
           Some m
       | None -> None)

(** Per-(symbol,exchange) cache of the resolved round_price closure.
    Key = symbol ^ "|" ^ exchange  (e.g. "BTC\/USD|kraken").
    Value = a direct (float -> float) that bypasses module dispatch on every call.
    The table holds at most 2-3 entries in practice (one per traded pair). *)
let _round_price_fn_cache : (string, float -> float) Hashtbl.t =
  Hashtbl.create 8


(** Utility function to take first n elements from a list *)
let rec take n = function
  | [] -> []
  | x :: xs -> if n <= 0 then [] else x :: take (n - 1) xs

(** Trading configuration type (local definition for strategies) *)
type trading_config = {
  exchange: string;
  symbol: string;
  qty: string;
  grid_interval: float;
  sell_mult: string;
  strategy: string;
  maker_fee: float option;
  taker_fee: float option;
  accumulation_buffer: float;
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
  mutable pending_orders: (string * order_side * float * float) list;  (* order_id * side * price * timestamp - orders sent but not yet acknowledged *)
  mutable last_cycle: int;
  mutable last_order_time: float;  (* Unix timestamp of last order placement *)
  mutable cancelled_orders: (string * float) list;  (* order_id * timestamp - blacklist of recently cancelled orders *)
  mutable pending_cancellations: (string, float) Hashtbl.t;  (* order_id -> timestamp - track pending cancellation operations *)
  mutable amend_cooldowns: (string, float) Hashtbl.t; (* order_id -> timestamp until which amends are blocked *)
  mutable last_cleanup_time: float; (* Last time cleanup was run *)
  mutable inflight_buy: bool;   (* true while a buy Place is sent but not yet ack'd/rejected/failed *)
  mutable inflight_sell: bool;  (* true while a sell Place is sent but not yet ack'd/rejected/failed *)
  mutable asset_low: bool;      (* true when asset balance is insufficient to place next sell; sell+buy paused *)
  mutable capital_low: bool;    (* true when quote balance is insufficient to place next buy; strategy paused *)
  mutable capital_low_logged: bool; (* true once we have logged the capital-low warning to suppress repeat spam *)
  mutable capital_low_at_balance: float; (* quote_bal snapshot when capital_low was set; flag clears only when balance increases past this *)
  mutable reserved_quote: float; (* quote amount locked in the current open buy order for this symbol *)
  mutable accumulated_profit: float;    (* realized PnL from completed buy→sell pairs; Hyperliquid discrete sizing *)
  mutable reserved_base: float;  (* base asset accumulated via sell_mult; excluded from sellable balance *)
  mutable last_buy_fill_price: float option; (* price of the most recent buy fill; used to compute profit on first sell after buy *)
  mutable last_sell_fill_price: float option; (* price of the most recent sell fill; used as cost basis for consecutive sells *)
  mutable grid_qty: float;              (* cached config qty for profit calc in fill handler *)
  mutable maker_fee: float;             (* cached maker fee rate for profit calc in fill handler *)
  mutable exchange_id: string;          (* cached exchange name for persistence decisions *)
  mutable startup_replay: bool;  (* true during startup fill replay; gates profit calculation *)
  mutable last_fill_oid: string option; (* OID of last fill that credited profit; resumption point *)
  mutable highest_startup_oid: string option; (* highest fill OID seen during startup replay; used to bootstrap new strategies *)
  mutable anticipated_base_credit: float;  (* base qty from buy fills not yet reflected in balance feed *)
  mutable last_seen_asset_balance: float;  (* last asset_bal seen; used to detect balance feed catching up *)
  mutex: Mutex.t;  (* Per-asset mutex to prevent concurrent execution for the same symbol *)
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
        (* Load persisted state for this symbol (survives Docker restarts) *)
        let persisted_reserved_base = Hyperliquid.State_persistence.load_reserved_base ~symbol:asset_symbol in
        let persisted_accumulated_profit = Hyperliquid.State_persistence.load_accumulated_profit ~symbol:asset_symbol in
        let persisted_last_fill_oid = Hyperliquid.State_persistence.load_last_fill_oid ~symbol:asset_symbol in
        let persisted_last_buy_fill_price = Hyperliquid.State_persistence.load_last_buy_fill_price ~symbol:asset_symbol in
        let persisted_last_sell_fill_price = Hyperliquid.State_persistence.load_last_sell_fill_price ~symbol:asset_symbol in
        let new_state = {
          last_buy_order_price = None;
          last_buy_order_id = None;
          open_sell_orders = [];
          pending_orders = [];
          last_cycle = 0;
          last_order_time = 0.0;
          cancelled_orders = [];
          pending_cancellations = Hashtbl.create 16;
          amend_cooldowns = Hashtbl.create 16;
          last_cleanup_time = 0.0;
          inflight_buy = false;
          inflight_sell = false;
          asset_low = false;
          capital_low = false;
          capital_low_logged = false;
          capital_low_at_balance = 0.0;
          reserved_quote = 0.0;
          accumulated_profit = persisted_accumulated_profit;
          reserved_base = persisted_reserved_base;
          last_buy_fill_price = persisted_last_buy_fill_price;
          last_sell_fill_price = persisted_last_sell_fill_price;
          grid_qty = 0.0;
          maker_fee = 0.0;
          exchange_id = "";
          startup_replay = true;  (* always gate profit calc during startup; set_startup_replay_done clears *)
          last_fill_oid = persisted_last_fill_oid;
          highest_startup_oid = None;
          anticipated_base_credit = 0.0;
          last_seen_asset_balance = 0.0;
          mutex = Mutex.create ();
        } in
        Hashtbl.replace strategy_states asset_symbol new_state;
        new_state
  in
  Mutex.unlock strategy_states_mutex;
  state

(** Dedicated mutex protecting all reserved_quote reads and writes across domains.
    Lock order: state.mutex -> reservation_mutex -> strategy_states_mutex.
    reservation_mutex is never held while waiting for state.mutex. **)
let reservation_mutex = Mutex.create ()

(** Inner sum - MUST be called with reservation_mutex already held.
    Only sums reserved_quote for states matching the given exchange. *)
let get_total_reserved_quote_locked ~exchange =
  Mutex.lock strategy_states_mutex;
  let total = Hashtbl.fold (fun _key state acc ->
    if state.exchange_id = exchange then acc +. state.reserved_quote
    else acc
  ) strategy_states 0.0 in
  Mutex.unlock strategy_states_mutex;
  total

(** Public read: acquires reservation_mutex then sums same-exchange assets.
    Safe to call from outside state.mutex (capital-low fast-path). *)
let get_total_reserved_quote ~exchange =
  Mutex.lock reservation_mutex;
  let total = get_total_reserved_quote_locked ~exchange in
  Mutex.unlock reservation_mutex;
  total

(** Safely write this asset's reserved_quote.
    May be called from inside state.mutex (lock order: state.mutex -> reservation_mutex). *)
let set_asset_reserved_quote state v =
  Mutex.lock reservation_mutex;
  state.reserved_quote <- v;
  Mutex.unlock reservation_mutex

(** Atomically check available quote balance and reserve for a buy if sufficient.
    Returns (balance_ok, available_quote, total_reserved).
    MUST be called from inside state.mutex (lock order: state.mutex -> reservation_mutex -> strategy_states_mutex).
    If balance_ok, state.reserved_quote is set to reserve_amount before returning. *)
let atomic_check_and_reserve state quote_bal quote_needed reserve_amount =
  Mutex.lock reservation_mutex;
  let total_reserved = get_total_reserved_quote_locked ~exchange:state.exchange_id in
  let available = quote_bal -. total_reserved in
  let ok = available >= quote_needed in
  if ok then state.reserved_quote <- reserve_amount;
  Mutex.unlock reservation_mutex;
  (ok, available, total_reserved)

(** Parse configuration values to floats *)
let parse_config_float config value_name default exchange symbol =
  try float_of_string config with
  | Failure _ ->
      Logging.warn_f ~section "Invalid %s value '%s' for %s/%s, using default %.4f"
        value_name config exchange symbol default;
      default

(** Round price to appropriate precision for the symbol.
    Uses a per-(symbol,exchange) closure cache to avoid module dispatch overhead
    on the hot path inside calculate_grid_price. *)
let round_price price symbol exchange =
  let key = symbol ^ "|" ^ exchange in
  let fn =
    match Hashtbl.find_opt _round_price_fn_cache key with
    | Some f -> f
    | None ->
        let f = match get_exchange_module exchange with
          | Some (module Ex : Exchange.S) -> (fun p -> Ex.round_price ~symbol ~price:p)
          | None ->
              Logging.warn_f ~section "No exchange found for %s/%s, using default rounding" exchange symbol;
              Float.round
        in
        Hashtbl.replace _round_price_fn_cache key f;
        f
  in
  fn price


(** Get minimum price increment for the symbol *)
let get_price_increment symbol exchange =
  (* Fetch price increment from generic Exchange interface *)
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) -> Option.value (Ex.get_price_increment ~symbol) ~default:0.01
  | None ->
      Logging.warn_f ~section "No price increment info for %s/%s, using default 0.01" exchange symbol;
      0.01  (* Default fallback *)

(** Get quantity increment (lot size) for the symbol *)
let get_qty_increment_val symbol exchange =
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) -> Option.value (Ex.get_qty_increment ~symbol) ~default:0.01
  | None -> 0.01

(** Round quantity down to valid lot size for the exchange *)
let round_qty qty symbol exchange =
  let increment = get_qty_increment_val symbol exchange in
  let inv = 1.0 /. increment in
  floor (qty *. inv) /. inv

(** Get minimum price movement required to trigger an amendment *)
let get_min_move_threshold price grid_interval_pct symbol exchange =
  let base_increment = get_price_increment symbol exchange in
  if exchange = "hyperliquid" then
    (* For Hyperliquid, require at least 25% of grid_interval OR 10x base increment to avoid rate limits *)
    let pct_based = price *. (grid_interval_pct *. 0.25 /. 100.0) in
    max (base_increment *. 10.0) pct_based
  else
    base_increment

(** Calculate grid price based on current price and interval *)
let calculate_grid_price current_price grid_interval_pct is_above symbol exchange =
  let interval = current_price *. (grid_interval_pct /. 100.0) in
  let raw_price = if is_above then current_price +. interval else current_price -. interval in
  round_price raw_price symbol exchange

(** Check if we can place a buy order based on quote balance *)
let can_place_buy_order (_qty : float) quote_balance quote_needed =
  quote_balance >= quote_needed

(** Check if we can place a sell order based on asset balance *)
let can_place_sell_order (_qty : float) (_sell_mult : float) asset_balance asset_needed =
  asset_balance >= asset_needed

(** Create a strategy order for placing a new order *)
let generate_side_duplicate_key asset_symbol side =
  (* Allow only one in-flight order per asset+side regardless of price/qty *)
  Printf.sprintf "%s|%s|grid" asset_symbol (string_of_order_side side)

let create_place_order asset_symbol side qty price post_only strategy exchange =
  let tif = if exchange = "hyperliquid" then "Alo" else "GTC" in
  {
    operation = Place;
    order_id = None;
    symbol = asset_symbol;
    exchange;
    side;
    order_type = "limit";
    qty;
    price;
    time_in_force = tif;
    post_only;
    userref = Some Strategy_common.strategy_userref_grid;  (* Tag order as Grid strategy *)
    strategy;
    duplicate_key = generate_side_duplicate_key asset_symbol side;
  }

(** Create a strategy order for amending an existing order *)
let create_amend_order order_id asset_symbol side qty price post_only strategy exchange =
  let tif = if exchange = "hyperliquid" then "Alo" else "GTC" in
  {
    operation = Amend;
    order_id = Some order_id;
    symbol = asset_symbol;
    exchange;
    side;
    order_type = "limit";
    qty;
    price;
    time_in_force = tif;
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

(** Create a strategy order - backwards compatibility *)
let create_order asset_symbol side qty price post_only exchange =
  create_place_order asset_symbol side qty price post_only "Grid" exchange

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

                  (* Update strategy state *)
                  state.last_order_time <- Unix.time ();
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
           state.last_order_time <- Unix.time ();

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

                   (* Track sell orders in state for grid logic (Kraken only) *)
                   (match order.side, order.price with
                    | Sell, Some price ->
                        state.open_sell_orders <- (temp_order_id, price) :: state.open_sell_orders;
                        Logging.debug_f ~section "Tracking sell order %s @ %.2f for %s" temp_order_id price order.symbol
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

  let state = get_strategy_state asset.symbol in

   (* --- Anticipated balance credit decay: clear when balance feed catches up --- *)
   (match asset_balance with
    | Some asset_bal ->
        if asset_bal > state.last_seen_asset_balance && state.anticipated_base_credit > 0.0 then begin
          Logging.debug_f ~section "Balance feed caught up for %s: %.8f -> %.8f, clearing anticipated credit %.8f"
            asset.symbol state.last_seen_asset_balance asset_bal state.anticipated_base_credit;
          state.anticipated_base_credit <- 0.0
        end;
        state.last_seen_asset_balance <- asset_bal
    | None -> ()
   );

   (* --- Asset-low check: clear flag when asset balance recovers --- *)
   (* Subtract reserved_base so accumulated base asset is protected from sells *)
   (* For Kraken: only clear when balance has genuinely increased (fill, deposit).
      Kraken's "Insufficient funds" on sells can mean USD collateral is too low,
      not that asset balance is insufficient. The local balance check would
      immediately clear the flag every cycle, causing a tight retry loop.
      For Hyperliquid: clear whenever available balance passes the threshold
      (existing behavior). *)
   (match asset_balance with
    | Some asset_bal ->
        let qty_f = (try float_of_string asset.qty with Failure _ -> 0.001) in
        let asset_needed_fast =
          if asset.exchange = "hyperliquid" then qty_f  (* 1:1 sells need full qty *)
          else let sell_mult_f = (try float_of_string asset.sell_mult with Failure _ -> 1.0) in
               qty_f *. sell_mult_f
        in
        let available_asset = asset_bal -. state.reserved_base +. state.anticipated_base_credit in
        let balance_actually_changed = asset_bal > state.last_seen_asset_balance in
        let should_clear =
          if asset.exchange = "hyperliquid" then available_asset >= asset_needed_fast
          else available_asset >= asset_needed_fast && balance_actually_changed
        in
        if state.asset_low && should_clear then begin
          state.asset_low <- false;
          state.inflight_sell <- false;
          ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset.symbol Sell));
          Logging.info_f ~section "Asset balance restored for %s (have %.8f, reserved %.8f, anticipated_credit %.8f, available %.8f, need %.8f) - resuming sell+buy placement"
            asset.symbol asset_bal state.reserved_base state.anticipated_base_credit available_asset asset_needed_fast
        end
    | None -> ()
   );

  (* --- Capital-low fast path: skip strategy when quote balance is known to be
     insufficient for a buy order. We still allow the FIRST loop through (capital_low=false)
     so that any accrued sell orders from previously filled buys are placed and can free
     capital. After that first pass, capital_low is set and we short-circuit here. ---
     When balance recovers we clear the flag and resume normally. *)
  (match quote_balance with
   | Some quote_bal ->
       let qty_f = (try float_of_string asset.qty with Failure _ -> 0.001) in
       let quote_needed_fast = (match current_price with Some p -> p *. qty_f | None -> 0.0) in
       (* Available balance = total balance minus ALL domains' reserved quote *)
       let total_reserved = get_total_reserved_quote ~exchange:state.exchange_id in
       let available_quote = quote_bal -. total_reserved in
       if state.capital_low && available_quote < quote_needed_fast then begin
         (* Stamp balance snapshot on first capital_low cycle so recovery requires
            a real balance feed increase, not stale data re-read. *)
         if state.capital_low_at_balance = 0.0 then
           state.capital_low_at_balance <- quote_bal;
         (* Still low: skip entire strategy body to avoid order spam *)
         ()
       end else if state.capital_low && quote_bal > state.capital_low_at_balance then begin
         (* The balance feed has delivered a higher balance than what we saw when capital_low
            was set. This means a real event (a fill crediting USD, or a cancel freeing reserve)
            arrived. Only now is it safe to retry placement. Clearing on stale balance data
            (same quote_bal as at rejection) caused a tight rejection loop. *)
         state.capital_low <- false;
         state.capital_low_logged <- false;
         state.capital_low_at_balance <- 0.0;  (* reset snapshot for next episode *)
         (* Flush the buy-side placement cooldown so the strategy can place immediately.
            Without this, the 2s cooldown set when capital_low was first triggered
            would block the first buy attempt even after balance recovers. *)
         Hashtbl.remove state.amend_cooldowns "place_Buy";
         (* Also clear inflight_buy and evict the InFlightOrders duplicate key. *)
         state.inflight_buy <- false;
         ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset.symbol Buy));
         Logging.info_f ~section "Capital restored for %s (available %.2f, need %.2f, total_reserved %.2f, was_at %.2f) - resuming strategy"
           asset.symbol available_quote quote_needed_fast total_reserved state.capital_low_at_balance
       end
       (* capital_low=false: fall through normally *)
   | None -> () (* No balance data yet, fall through *)
  );

  (* Only proceed with main strategy if capital_low flag is clear.
     asset_low is checked below only for sell+buy placement — order sync,
     cleanup, and cancellation detection must always run. *)
  if not state.capital_low then begin

  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->

  (* Throttle order placement - wait at least 1 second between orders *)
  (* Keep order placement throttle, but allow logic to run every cycle *)
  let now = Unix.time () in
  
  match current_price, top_of_book with
  | None, _ -> ()  (* No price data available yet *)
  | Some price, top_opt ->
      (* Use l2Book top-of-book bid/ask instead of allMids mid price.
         l2Book updates per-block and tracks actual market state, while
         allMids batches across all coins and can diverge from the fill price
         during fast candle movements — causing sell order stacking on Hyperliquid.
         Sell-side calcs use bid_price; buy-side calcs use ask_price.
         Fallback to mid if orderbook data is unavailable. *)
      let (bid_price, ask_price) = match top_opt with
        | Some (bid, _, ask, _) when bid > 0.0 && ask > 0.0 -> (bid, ask)
        | _ -> (price, price)
      in

      (* Efficient cleanup logic - run every cycle but use scan-and-remove to avoid allocation *)
      (* This prevents accumulation while maintaining HFT responsiveness *)
      
      (* Clean up stale pending orders (older than 5 seconds) and enforce hard limit of 50 *)
      let original_pending_count = List.length state.pending_orders in
      state.pending_orders <- List.filter (fun (order_id, side, _, timestamp) ->
        let age = now -. timestamp in
        if age > 5.0 then begin  (* Reduced from 10 to 5 seconds *)
          Logging.warn_f ~section "Removing stale pending order %s for %s (age: %.1fs)" order_id asset.symbol age;
          
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
        Logging.warn_f ~section "Truncated %d excess pending orders for %s (kept 50)" excess asset.symbol;
      end;

      (* Log cleanup summary occasionally *)
      let cleaned_pending = original_pending_count - List.length state.pending_orders in
      if cleaned_pending > 0 && cycle mod 100000 = 0 then
        Logging.debug_f ~section "Cleaned up %d pending orders for %s" cleaned_pending asset.symbol;

      (* Clean up old cancelled orders from blacklist (older than 15 seconds) and enforce hard limit *)
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
      (* Optimized scan and remove to avoid Hashtbl copy *)
      let to_remove = ref [] in
      Hashtbl.iter (fun order_id timestamp ->
        if now -. timestamp > 30.0 then
          to_remove := order_id :: !to_remove
      ) state.pending_cancellations;

      List.iter (fun order_id ->
        Logging.debug_f ~section "Removing stale pending cancellation %s for %s" order_id asset.symbol;
        Hashtbl.remove state.pending_cancellations order_id
      ) !to_remove;

      (* Clean up expired amend cooldowns *)
      let to_remove_cooldowns = ref [] in
      Hashtbl.iter (fun order_id expiry_time ->
        if now > expiry_time then
          to_remove_cooldowns := order_id :: !to_remove_cooldowns
      ) state.amend_cooldowns;

      List.iter (fun order_id ->
        Hashtbl.remove state.amend_cooldowns order_id
      ) !to_remove_cooldowns;

      (* Sync strategy state with actual open orders from exchange *)
      (* Preserve sell orders that were set by handle_order_amended
         or newly placed execution events but may not yet appear in exchange data due to WS lag. *)
      let preserved_sells = state.open_sell_orders in
      state.open_sell_orders <- [];
      (* Preserve buy tracking across sync for all exchanges.
         Exchange open orders feed may lag behind execution events,
         so it may briefly report 0 buys while we know one exists.
         Genuine cancellations clear tracking via handle_order_cancelled. *)
      
      (* Note: We don't clear pending_orders here - they should be managed by order placement responses *)

      (* Update with real orders from exchange - open_orders is (order_id, price, qty, side, userref) list *)
      (* Filter out recently cancelled orders to avoid race condition *)
      let buy_orders = ref [] in
      let sell_orders = ref [] in

      List.iter (fun (order_id, order_price, qty, side_str, userref_opt) ->
        (* Skip if this order was recently cancelled *)
        let is_cancelled = List.exists (fun (cancelled_id, _) -> cancelled_id = order_id) state.cancelled_orders in

        let is_our_strategy = match userref_opt with
          | Some ref_val -> ref_val = Strategy_common.strategy_userref_grid
          | None -> asset.exchange = "hyperliquid"
        in
        
        if not is_cancelled && qty > 0.0 && is_our_strategy then (* Only count orders with remaining quantity *)
          (* Use actual order side from exchange, not price-based classification *)
          if side_str = "buy" then
            (* Treat ALL buy orders as grid buys to enforce single-buy-order policy across all exchanges *)
            buy_orders := (order_id, order_price) :: !buy_orders
          else
            (* For sell orders, include ALL open sell orders regardless of tag *)
            sell_orders := (order_id, order_price) :: !sell_orders
      ) open_orders;

      (* Set the buy order price and ID (take the highest buy order if multiple) *)
      (match !buy_orders with
       | [] ->
           (* No open buy for this symbol - clear this asset's reservation *)
           set_asset_reserved_quote state 0.0
       | orders ->
           let (best_order_id, best_price) = List.fold_left (fun (acc_id, acc_price) (order_id, price) ->
             if price > acc_price then (order_id, price) else (acc_id, acc_price)
           ) (List.hd orders) (List.tl orders) in
           state.last_buy_order_price <- Some best_price;
           state.last_buy_order_id <- Some best_order_id;
           (* Sync per-asset reserved_quote from the actual open buy so the capital_low check
              uses the real exchange-reserved amount even after a restart. *)
           let qty = (try float_of_string asset.qty with Failure _ -> 0.001) in
           set_asset_reserved_quote state (best_price *. qty));

      (* Set sell orders *)
      state.open_sell_orders <- !sell_orders;

      (* For Hyperliquid: merge any preserved sell orders that aren't in the
         exchange-sourced list. This covers cancel-replace amendments where
         the new order hasn't yet appeared in the order update stream. *)
      if asset.exchange = "hyperliquid" then begin
        List.iter (fun (preserved_id, preserved_price) ->
          let already_present = List.exists (fun (id, _) -> id = preserved_id) state.open_sell_orders in
          let is_cancelled = List.exists (fun (cancelled_id, _) -> cancelled_id = preserved_id) state.cancelled_orders in
          if not already_present && not is_cancelled then
            state.open_sell_orders <- (preserved_id, preserved_price) :: state.open_sell_orders
        ) preserved_sells
      end;

      Logging.debug_f ~section "Synced %d open orders for %s: %d buys, %d sells"
        (List.length open_orders) asset.symbol
        (List.length !buy_orders) (List.length !sell_orders);

      (* Parse configuration values *)
      let qty = parse_config_float asset.qty "qty" 0.001 asset.exchange asset.symbol in
      let grid_interval = asset.grid_interval in
      let sell_mult = parse_config_float asset.sell_mult "sell_mult" 1.0 asset.exchange asset.symbol in

      (* Cache config values in state so handle_order_filled can compute real PnL *)
      state.grid_qty <- qty;
      state.maker_fee <- (match asset.maker_fee with
        | Some f -> f
        | None ->
            match Fee_cache.get_maker_fee ~exchange:asset.exchange ~symbol:asset.symbol with
            | Some cached -> cached
            | None -> 0.0);
      state.exchange_id <- asset.exchange;

      (* Calculate required balances *)
      let quote_needed = ask_price *. qty in
      let asset_needed =
        if asset.exchange = "hyperliquid" then qty  (* most sells are 1:1 on Hyperliquid *)
        else qty *. sell_mult
      in

  (* Log current balance status for debugging - throttled to every 10,000 cycles *)
  if cycle mod 10000 = 0 then
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

    let buy_order_pending = List.exists (fun (_, side, _, _) -> side = Buy) state.pending_orders in
    (* Effective buy count: use max of raw count and our own tracking.
       webData2 snapshots can be stale during cancel-replace amendments.
       last_buy_order_id is set immediately by orderUpdates events. *)
    let has_tracked_buy = state.last_buy_order_id <> None in
    let tracked_buy_count = List.length !buy_orders in
    let effective_buy_count = if has_tracked_buy && tracked_buy_count = 0 then 1 else tracked_buy_count in

    if buy_order_pending then begin
        (* Only log every 100,000 iterations to avoid spam *)
        if cycle mod 100000 = 0 then
            Logging.debug_f ~section "Waiting for pending buy order to complete for %s" asset.symbol;
    end else if effective_buy_count > 1 then begin
        (* Case 0: Multiple buy orders exist - cancel all buy orders to maintain single buy order policy *)
        Logging.info_f ~section "Found %d buy orders for %s, cancelling all buy orders to maintain single buy order policy"
          effective_buy_count asset.symbol;

        (* Cancel all buy orders *)
        List.iter (fun (order_id, _) ->
          let cancel_order = create_cancel_order order_id asset.symbol "Grid" asset.exchange in
          ignore (push_order cancel_order);
          Logging.info_f ~section "Cancelling excess buy order: %s for %s" order_id asset.symbol
        ) !buy_orders;

        (* Clear buy order tracking since we're cancelling all *)
        state.last_buy_order_price <- None;
        state.last_buy_order_id <- None;

        (* Blacklist all cancelled buy order IDs so racing amendments
           don't reinstall them via handle_order_amended fallback *)
        List.iter (fun (order_id, _) ->
          state.cancelled_orders <- (order_id, now) :: state.cancelled_orders
        ) !buy_orders;

        (* Apply cooldown to prevent re-entering this branch before exchange acks the cancels *)
        let cooldown_key = "place_Buy" in
        Hashtbl.replace state.amend_cooldowns cooldown_key (now +. 2.0);

        (* Update cycle counter *)
        state.last_cycle <- cycle
      end else if effective_buy_count = 0 && not buy_order_pending then begin
        (* NO OPEN BUY: Place sell + buy.
           When a buy fills, any existing sell's InFlightOrders key is still live,
           which would block the new sell via duplicate detection.
           Force-clear the sell key so a fresh sell is always placed alongside the new buy. *)
        let sell_price = calculate_grid_price bid_price grid_interval true asset.symbol asset.exchange in
        let buy_price = calculate_grid_price ask_price grid_interval false asset.symbol asset.exchange in

        let buy_cooldown_key = "place_Buy" in
        let is_buy_on_cooldown = Hashtbl.mem state.amend_cooldowns buy_cooldown_key in

        (* Place sell order first - sell and buy are always paired.
           asset_low flag blocks entry to this entire branch at the top level.
           Always attempt the sell regardless of local balance — cached balance
           may be stale after a buy fill.  If the exchange rejects, asset_low
           is set and won't clear until (asset_bal - reserved_base) >= needed,
           protecting the accumulated base asset. *)
        (match asset_balance with
         | Some asset_bal when not state.inflight_sell ->
             let sell_qty =
               if asset.exchange = "hyperliquid" then begin
                 let rounded_sell = round_qty (qty *. sell_mult) asset.symbol asset.exchange in
                 let rounding_diff = qty -. rounded_sell in
                 let required_profit = rounding_diff *. sell_price +. asset.accumulation_buffer in
                 if required_profit > 0.0 && state.accumulated_profit >= required_profit then begin
                   (* Profit-gated: enough realized profit to cover the rounding cost *)
                   state.accumulated_profit <- state.accumulated_profit -. required_profit;
                   let base_increment = qty -. rounded_sell in
                   state.reserved_base <- state.reserved_base +. base_increment;
                   (* Persist both values so they survive Docker restarts *)
                   Hyperliquid.State_persistence.save ~symbol:asset.symbol
                     ~reserved_base:state.reserved_base
                     ~accumulated_profit:state.accumulated_profit ();
                   Logging.info_f ~section
                     "Accumulation sell for %s: %.8f (sell_mult, profit %.4f covered cost %.4f, reserved_base now %.8f)"
                     asset.symbol rounded_sell (state.accumulated_profit +. required_profit) required_profit state.reserved_base;
                   rounded_sell
                 end else
                   qty  (* 1:1 sell until profit covers rounding cost *)
               end else
                 qty *. sell_mult
             in
             (* Proactive reserved_base guard (Hyperliquid only): ensure we never sell
                accumulated base asset.  The available balance must exclude:
                1. reserved_base — accumulated base that must never be sold
                2. qty already locked in open sell orders on the exchange
                Uses the open_orders parameter (authoritative exchange data) rather than
                state.open_sell_orders which may be empty after startup/reconnection.
                Kraken does not use reserved_base so the check is skipped. *)
             let balance_ok =
               if asset.exchange = "hyperliquid" then
                 let locked_in_sells = List.fold_left (fun acc (_oid, _price, remaining_qty, side_str, _uref) ->
                   if side_str = "sell" then acc +. remaining_qty else acc
                 ) 0.0 open_orders in
                 let available = asset_bal +. state.anticipated_base_credit -. state.reserved_base -. locked_in_sells in
                 if available >= sell_qty then true
                 else begin
                   Logging.warn_f ~section
                     "Sell order blocked for %s: available %.8f (bal %.8f + anticipated %.8f - reserved %.8f - locked_sells %.8f) < sell_qty %.8f"
                     asset.symbol available asset_bal state.anticipated_base_credit state.reserved_base locked_in_sells sell_qty;
                   false
                 end
               else true
             in
             if balance_ok then begin
               let sell_order = create_order asset.symbol Sell sell_qty (Some sell_price) true asset.exchange in
               if push_order sell_order then
                 Logging.info_f ~section "Placed sell order for %s: %.8f @ %.4f"
                   asset.symbol sell_qty sell_price
             end
         | _ -> ()
        );

        (* Place buy order if we have quote balance *)
        (match quote_balance with
         | Some _ when is_buy_on_cooldown ->
             Logging.debug_f ~section "Skipping buy order placement for %s due to rate limit cooldown" asset.symbol
         | Some _ when state.inflight_buy ->
             Logging.debug_f ~section "Skipping buy order placement for %s: inflight buy=%B" asset.symbol state.inflight_buy
         | Some quote_bal ->
             (* Atomically check balance and reserve - prevents TOCTOU where two domains
                both read sufficient balance simultaneously and both place buys *)
             let (balance_ok, available_quote, total_reserved) =
               atomic_check_and_reserve state quote_bal quote_needed (buy_price *. qty)
             in
             if balance_ok then begin
               let order = create_order asset.symbol Buy qty (Some buy_price) true asset.exchange in
               if push_order order then begin
                 state.last_buy_order_price <- Some buy_price;
                 Logging.info_f ~section "Placed buy order for %s: %.8f @ %.4f (available %.2f, reserved %.2f)"
                   asset.symbol qty buy_price available_quote state.reserved_quote;

                 (* Enforce exactly 2x grid_interval spacing *)
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
                      let double_grid_interval = bid_price *. (2.0 *. grid_interval /. 100.0) in
                      let target_buy = round_price (cs_price -. double_grid_interval) asset.symbol asset.exchange in
                      let min_move_threshold = get_min_move_threshold bid_price grid_interval asset.symbol asset.exchange in
                      if abs_float (target_buy -. buy_price) > min_move_threshold then
                        Logging.info_f ~section "Will enforce 2x spacing for %s on next cycle: buy %.2f -> %.2f (from sell@%.2f)"
                          asset.symbol buy_price target_buy cs_price
                  | None -> ())
               end else begin
                 (* push_order failed (buffer full) - release the reservation we just set *)
                 set_asset_reserved_quote state 0.0
               end
             end else begin
                (* Local balance appears insufficient, but still attempt the buy and let the
                   exchange be the arbiter.  The rejection handler (handle_order_rejected) already
                   sets capital_low when Kraken responds with "EOrder:Insufficient funds".
                   Relying on local calculations alone causes permanent stalls when
                   reservations are stale or balance data lags behind exchange state
                   (e.g. a sell fills and frees capital but total_reserved hasn't caught up).
                   Apply the 2s cooldown to prevent spam while waiting for the ack/rejection. *)
                let cooldown_key = "place_Buy" in
                if not (Hashtbl.mem state.amend_cooldowns cooldown_key) then begin
                  Logging.warn_f ~section "Local balance low for %s buy (need %.2f, available %.2f, total %.2f, total_reserved %.2f) - attempting anyway, exchange will reject if truly insufficient"
                    asset.symbol quote_needed available_quote quote_bal total_reserved;
                  Hashtbl.replace state.amend_cooldowns cooldown_key (now +. 2.0);
                  (* Attempt without a reservation - exchange is the final gatekeeper *)
                  let order = create_order asset.symbol Buy qty (Some buy_price) true asset.exchange in
                  if push_order order then
                    state.last_buy_order_price <- Some buy_price
                end
             end
         | None ->
             Logging.warn_f ~section "No quote balance data available for %s buy order"
               asset.symbol
        );
        state.last_cycle <- cycle
      end else if effective_buy_count > 0 then begin
        (* Case 2: We have an open buy order - check for sell orders *)
        (* Combine active sell orders with pending sell orders for spacing calculation *)
        let all_sell_orders = state.open_sell_orders @ (
          List.filter_map (fun (id, side, price, _) ->
            if side = Sell then Some (id, price) else None
          ) state.pending_orders
        ) in
        
        if all_sell_orders <> [] then begin
          (* Find the closest sell order *)
          let closest_sell_order = List.fold_left (fun acc (order_id, sell_price) ->
            match acc with
            | None -> Some (order_id, sell_price)
            | Some (_, best_price) ->
                if sell_price < best_price then Some (order_id, sell_price) else acc
          ) None all_sell_orders in

          match closest_sell_order, state.last_buy_order_price, state.last_buy_order_id with
          | Some (_sell_order_id, sell_price), Some current_buy_price, Some buy_order_id ->
              (* Calculate distance from buy to sell as absolute dollar amount *)
              let distance = sell_price -. current_buy_price in
              (* Calculate 2x grid_interval as absolute dollar amount based on current price *)
              let double_grid_interval = bid_price *. (2.0 *. grid_interval /. 100.0) in

              (* Always calculate exact 2x target from sell *)
              let exact_target = round_price (sell_price -. double_grid_interval) asset.symbol asset.exchange in
              
              if distance > double_grid_interval then begin
                (* Distance > 2x: Trail upward ONLY to maintain grid_interval below current price *)
                let proposed_buy_price = calculate_grid_price ask_price grid_interval false asset.symbol asset.exchange in
                
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
                  
                  let min_move_threshold = get_min_move_threshold bid_price grid_interval asset.symbol asset.exchange in
                  let current_buy_price_rounded = round_price current_buy_price asset.symbol asset.exchange in
                  let price_diff_rounded = round_price (abs_float (target_buy_price -. current_buy_price_rounded)) asset.symbol asset.exchange in
                  
                  (* Check if this order is already being amended *)
                  let is_being_amended = List.exists (fun (id, _, _, _) ->
                    String.starts_with ~prefix:"pending_amend_" id &&
                    String.sub id 14 (String.length id - 14) = buy_order_id
                  ) state.pending_orders in
                  
                  let is_in_flight = InFlightAmendments.is_in_flight buy_order_id in
                  let is_on_cooldown = Hashtbl.mem state.amend_cooldowns buy_order_id in

                  if not is_being_amended && not is_in_flight && not is_on_cooldown && price_diff_rounded >= min_move_threshold && target_buy_price <> current_buy_price_rounded then begin
                    (match quote_balance with
                     | Some quote_bal when can_place_buy_order qty quote_bal quote_needed ->
                         let order = create_amend_order buy_order_id asset.symbol Buy qty (Some target_buy_price) true "Grid" asset.exchange in
                         ignore (push_order order);
                         state.last_buy_order_price <- Some target_buy_price;
                         let target_distance = sell_price -. target_buy_price in
                         Logging.debug_f ~section "Amended buy %s for %s (trailing upward): sell@%.4f, buy@%.4f -> %.4f (dist: %.4f -> %.4f, 2x: %.4f)"
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
                let exact_target_rounded = exact_target in
                let current_buy_price_rounded = round_price current_buy_price asset.symbol asset.exchange in
                let price_diff_rounded = round_price (abs_float (exact_target_rounded -. current_buy_price_rounded)) asset.symbol asset.exchange in
                let min_move_threshold = get_min_move_threshold bid_price grid_interval asset.symbol asset.exchange in

                (* Check if this order is already being amended *)
                let is_being_amended = List.exists (fun (id, _, _, _) ->
                  String.starts_with ~prefix:"pending_amend_" id &&
                  String.sub id 14 (String.length id - 14) = buy_order_id
                ) state.pending_orders in

                let is_in_flight = InFlightAmendments.is_in_flight buy_order_id in
                let is_on_cooldown = Hashtbl.mem state.amend_cooldowns buy_order_id in

                if not is_being_amended && not is_in_flight && not is_on_cooldown && price_diff_rounded >= min_move_threshold && exact_target_rounded <> current_buy_price_rounded then begin
                  (match quote_balance with
                   | Some quote_bal when can_place_buy_order qty quote_bal quote_needed ->
                       let order = create_amend_order buy_order_id asset.symbol Buy qty (Some exact_target) true "Grid" asset.exchange in
                       ignore (push_order order);
                       state.last_buy_order_price <- Some exact_target;
                       Logging.debug_f ~section "Amended buy %s for %s (enforcing 2x): sell@%.4f, buy@%.4f -> %.4f (dist: %.4f <= 2x: %.4f)"
                         buy_order_id asset.symbol sell_price current_buy_price exact_target distance double_grid_interval
                   | Some quote_bal ->
                       Logging.warn_f ~section "Insufficient quote balance for %s: need %.2f, have %.2f"
                         asset.symbol quote_needed quote_bal
                   | None ->
                       Logging.warn_f ~section "No quote balance for %s" asset.symbol)
                end else begin
                  Logging.debug_f ~section "Buy %s for %s already at 2x: buy@%.4f, sell@%.4f"
                    buy_order_id asset.symbol current_buy_price sell_price
                end
              end
          | _ ->
              Logging.debug_f ~section "Buy order tracking lost for %s, will re-place on next cycle" asset.symbol
        end else begin
          (* No sell orders exist but we have an active buy.
             Trail the buy without sell anchors - sells are only placed
             in the effective_buy_count=0 branch after a buy fills. *)
           match state.last_buy_order_price, state.last_buy_order_id with
           | Some current_buy_price, Some buy_order_id ->
               let target_buy_price = calculate_grid_price ask_price grid_interval false asset.symbol asset.exchange in
                            (* Only trail upward - move buy up if target is higher than current *)
               if target_buy_price > current_buy_price then begin
                 let min_move_threshold = get_min_move_threshold ask_price grid_interval asset.symbol asset.exchange in
                 let current_buy_price_rounded = round_price current_buy_price asset.symbol asset.exchange in
                 let price_diff_rounded = round_price (abs_float (target_buy_price -. current_buy_price_rounded)) asset.symbol asset.exchange in

                (* Check if this order is already being amended *)
                let is_being_amended = List.exists (fun (id, _, _, _) ->
                  String.starts_with ~prefix:"pending_amend_" id &&
                  String.sub id 14 (String.length id - 14) = buy_order_id
                ) state.pending_orders in

                let is_in_flight = InFlightAmendments.is_in_flight buy_order_id in
                let is_on_cooldown = Hashtbl.mem state.amend_cooldowns buy_order_id in

                if not is_being_amended && not is_in_flight && not is_on_cooldown && price_diff_rounded >= min_move_threshold then begin
                  (match quote_balance with
                   | Some quote_bal when can_place_buy_order qty quote_bal quote_needed ->
                       let order = create_amend_order buy_order_id asset.symbol Buy qty (Some target_buy_price) true "Grid" asset.exchange in
                       ignore (push_order order);
                       state.last_buy_order_price <- Some target_buy_price;
                       Logging.debug_f ~section "Trailing buy %s for %s: %.4f -> %.4f (no sell anchors)"
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
  end);
  ()
end


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

  (* Update buy order tracking if this is a buy order acknowledgment *)
  (match side with
   | Buy ->
    state.last_buy_order_id <- Some order_id;
    state.last_buy_order_price <- Some price;
    state.inflight_buy <- false;
    Logging.debug_f ~section "Updated buy order ID and price tracking: %s @ %.2f for %s" order_id price asset_symbol
   | Sell ->
    state.inflight_sell <- false);

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

    (* Release this asset's quote reservation on buy failures *)
    (match side with Buy -> set_asset_reserved_quote state 0.0 | Sell -> ());

    (* Clear global in-flight trackers *)
    let duplicate_key = generate_side_duplicate_key asset_symbol side in
    ignore (InFlightOrders.remove_in_flight_order duplicate_key);
    
    let lower_reason = String.lowercase_ascii reason in
    let contains_fragment s fragment =
      let sl = String.length s and fl = String.length fragment in
      let rec loop i = i + fl <= sl && (String.sub s i fl = fragment || loop (i + 1)) in
      loop 0
    in
    let is_rate_limit = contains_fragment lower_reason "too many cumulative requests" || contains_fragment lower_reason "rate limit" in
    let is_insufficient_balance = contains_fragment lower_reason "insufficient funds"
      || contains_fragment lower_reason "insufficient spot balance" in
    let cooldown = if is_rate_limit then 10.0 else 2.0 in

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
     | Sell when is_insufficient_balance && state.exchange_id = "hyperliquid" ->
          if not state.asset_low then begin
            state.asset_low <- true;
            Logging.warn_f ~section "Exchange rejected sell for %s with insufficient balance - setting asset_low flag"
              asset_symbol
          end
     | Sell when is_insufficient_balance ->
          (* Kraken: sell failures are fire-and-forget. Don't set asset_low;
             the sell simply fails and the strategy continues placing the buy. *)
          Logging.warn_f ~section "Exchange rejected sell for %s with insufficient balance (ignored, Kraken sell is fire-and-forget)"
            asset_symbol
     | _ -> ());

    (* Apply timer cooldown only for buy-side rate limits; sell side uses asset_low flag *)
    let now = Unix.time () in
    (match side with
     | Buy ->
         Hashtbl.replace state.amend_cooldowns "place_Buy" (now +. cooldown)
     | Sell -> ());

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

  (* Apply short cooldown for buy side only; sell side uses asset_low flag.
     BUT: don't overwrite a longer cooldown already set by handle_order_failed. *)
  (match side with
   | Buy ->
       let now = Unix.time () in
       let new_expiry = now +. 2.0 in
       let existing_expiry = match Hashtbl.find_opt state.amend_cooldowns "place_Buy" with
         | Some t -> t
         | None -> 0.0
       in
       if new_expiry > existing_expiry then
         Hashtbl.replace state.amend_cooldowns "place_Buy" new_expiry
   | Sell -> ());

  Logging.debug_f ~section "Order rejected and removed from pending/trackers: %s @ %.2f for %s"
    (string_of_order_side side) price asset_symbol
  )

  (* For buy order rejections, this signals no buy order exists, which is valid for re-evaluation *)
  (* We don't need to do anything special here - the strategy will re-evaluate on next cycle *)

(** Handle order fill - fully clear tracking and pending amends *)
let handle_order_filled asset_symbol order_id side ~fill_price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* 1. Remove any pending amend matching this order ID *)
    state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
      not (String.starts_with ~prefix:"pending_amend_" pending_id &&
           String.length pending_id > 14 &&
           String.sub pending_id 14 (String.length pending_id - 14) = order_id)
    ) state.pending_orders;

    (* 2. Look up sell price BEFORE filtering — needed for profit calculation.
       Use exec event fill_price as primary source; fall back to open_sell_orders
       for backward compat. This fixes Hyperliquid amends where webData2 never
       surfaces the replacement sell order. *)
    let sell_fill_price = match List.assoc_opt order_id state.open_sell_orders with
      | Some p -> p
      | None -> fill_price  (* exec event fill price — always available *)
    in

    (* 3. Remove from sell orders tracking if it was a sell order *)
    state.open_sell_orders <- List.filter (fun (sell_id, _) ->
      sell_id <> order_id
    ) state.open_sell_orders;

    (* 4. Clear buy order tracking only on buy fills.
       A sell fill must NEVER clear last_buy_order_id — the buy is still live
       and just needs spacing adjustment, not a fresh placement cycle.
       Failing to gate on [side = Buy] here caused sell fills to spuriously
       clear buy tracking, making effective_buy_count=0 on the next cycle and
       triggering a redundant buy placement alongside the still-open original. *)
    let was_tracked_buy = match side, state.last_buy_order_id with
      | Buy, Some id when id = order_id -> true
      | _ -> false
    in
    if was_tracked_buy then begin
      (* Record actual buy fill price for profit calculation on matching sell fill *)
      state.last_buy_fill_price <- Some fill_price;
      (* Clear last_sell_fill_price: next sell is the first after this buy *)
      state.last_sell_fill_price <- None;
      state.last_buy_order_id <- None;
      state.last_buy_order_price <- None;
      (* Persist buy fill price so it survives Docker restarts.
         Without this, sell fills after restart have no buy_price reference
         and silently skip profit calculation. *)
      if state.exchange_id = "hyperliquid" then
        Hyperliquid.State_persistence.save ~symbol:asset_symbol
          ~reserved_base:state.reserved_base
          ~accumulated_profit:state.accumulated_profit
          ~last_buy_fill_price:fill_price
          ~last_sell_fill_price:0.0 ();
      (* Credit anticipated base so the sell guard sees the incoming asset before
         the balance feed catches up (race: exec feed is faster than balance feed) *)
      if state.grid_qty > 0.0 then begin
        state.anticipated_base_credit <- state.anticipated_base_credit +. state.grid_qty;
        Logging.info_f ~section "Anticipated base credit for %s: +%.8f (total: %.8f) from buy fill %s"
          asset_symbol state.grid_qty state.anticipated_base_credit order_id
      end;
      Logging.debug_f ~section "Filled buy order %s removed from tracking for %s" order_id asset_symbol
    end;

    (* 5. Compute realized net profit on sell fills (Hyperliquid discrete sizing accumulator).
       profit = (sell_price - buy_price) * qty - maker fees on both legs
       During startup_replay, skip profit calculation for fills already persisted
       (OID <= last_fill_oid). Tracking state (buy/sell IDs, inflight flags) is
       still updated above so the strategy enters the correct operational state.
       Also track the highest OID seen during startup for new-strategy bootstrapping. *)
    (* Track highest OID during startup replay for new-strategy bootstrapping *)
    if state.startup_replay then begin
      let dominated = match state.highest_startup_oid with
        | None -> true
        | Some prev ->
            (try Int64.compare (Int64.of_string order_id) (Int64.of_string prev) > 0
             with _ -> false)
      in
      if dominated then
        state.highest_startup_oid <- Some order_id
    end;
    let skip_profit_calc =
      state.startup_replay &&
      (match state.last_fill_oid with
       | Some persisted_oid ->
           (* OIDs are monotonically increasing integers on Hyperliquid *)
           (try Int64.compare (Int64.of_string order_id) (Int64.of_string persisted_oid) <= 0
            with _ -> false)
       | None -> true  (* New strategy: no persisted OID — skip ALL fills during startup replay *))
    in
    if skip_profit_calc then
      Logging.debug_f ~section "Skipping startup replay fill %s for %s (already persisted, last_fill_oid=%s)"
        order_id asset_symbol (Option.value state.last_fill_oid ~default:"none")
    else
    (match side with
     | Sell ->
          (* Determine cost basis: use last_sell_fill_price for consecutive sells
             (ladder steps), otherwise use last_buy_fill_price (first sell after buy) *)
          let cost_basis = match state.last_sell_fill_price with
            | Some prev_sell when prev_sell > 0.0 -> Some prev_sell
            | _ -> state.last_buy_fill_price
          in
          (match cost_basis with
           | Some base_price when sell_fill_price > base_price ->
              let qty = state.grid_qty in
              let gross = (sell_fill_price -. base_price) *. qty in
              let fees = (sell_fill_price *. qty *. state.maker_fee)
                       +. (base_price *. qty *. state.maker_fee) in
              let net_profit = gross -. fees in
              if net_profit > 0.0 then begin
                state.accumulated_profit <- state.accumulated_profit +. net_profit;
                (* Only advance last_fill_oid forward — never regress to a lower OID.
                   Old sells (low OIDs placed earlier) can fill after newer sells;
                   regressing would cause double-counting on restart replay. *)
                let should_update_oid = match state.last_fill_oid with
                  | Some prev_oid ->
                      (try Int64.compare (Int64.of_string order_id) (Int64.of_string prev_oid) > 0
                       with _ -> true)
                  | None -> true
                in
                if should_update_oid then
                  state.last_fill_oid <- Some order_id;
                (* Persist so profit progress survives Docker restarts.
                   Also persist current last_buy_fill_price so the next
                   sell after a restart still has a buy reference. *)
                if state.exchange_id = "hyperliquid" then
                  Hyperliquid.State_persistence.save ~symbol:asset_symbol
                    ~reserved_base:state.reserved_base
                    ~accumulated_profit:state.accumulated_profit
                    ~last_fill_oid:(Option.get state.last_fill_oid)
                    ?last_buy_fill_price:state.last_buy_fill_price
                    ~last_sell_fill_price:sell_fill_price ();
                Logging.debug_f ~section "Realized profit for %s: %.6f (gross %.6f - fees %.6f, sell@%.4f base@%.4f x %.8f), accumulated: %.6f"
                  asset_symbol net_profit gross fees sell_fill_price base_price qty state.accumulated_profit
              end;
              (* Always record sell fill price for consecutive sell detection *)
              state.last_sell_fill_price <- Some sell_fill_price
          | _ ->
              (* Sell at or below cost basis — still record for consecutive sell tracking *)
              state.last_sell_fill_price <- Some sell_fill_price)
     | Buy -> ());

    (* 4. Clear inflight flags, reservation, and global placement trackers so strategy can replace immediately *)
    (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);
    (* Release this asset's quote reservation when buy fills (funds are now in the asset, not quote) *)
    (match side with Buy -> set_asset_reserved_quote state 0.0 | Sell -> ());
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol side));

    (* 5. When a buy fills, also clear the Sell side's InFlightOrders guard.
       The strategy places buy+sell together; the previous sell's guard would
       block the new sell via duplicate detection. *)
    if side = Buy then begin
      ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol Sell));
      state.inflight_sell <- false
    end;

    Logging.debug_f ~section "Order FILLED and cleaned up explicitly: %s for %s"
      order_id asset_symbol
  )

(** Handle order cancellation - remove from pending and tracked orders *)
let handle_order_cancelled asset_symbol order_id side =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* Genuine cancellation: full cleanup *)
    (* Add to cancelled orders blacklist to prevent re-adding during sync *)
    (* 1. Remove any pending operations for this order *)
    let original_pending_count = List.length state.pending_orders in
    state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
      (* True if the pending_id matches order_id, OR if it's a pending amend matching the order_id *)
      let matches = pending_id = order_id || 
                   (String.starts_with ~prefix:"pending_amend_" pending_id && 
                    String.length pending_id > 14 && 
                    String.sub pending_id 14 (String.length pending_id - 14) = order_id) in
      not matches
    ) state.pending_orders;
    let removed_pending = original_pending_count - List.length state.pending_orders in
    
    (* Clear the pending cancellations tracking *)
    Hashtbl.remove state.pending_cancellations order_id;

    (* 1.5 Add cancelled order ID to blacklist immediately.
       This prevents race conditions where an old websocket update
       re-adds the order to tracking before the exchange fully drops it *)
    let now = Unix.time () in
    state.cancelled_orders <- (order_id, now) :: state.cancelled_orders;

    (* Clear amend cooldowns if any exist *)
    Hashtbl.remove state.amend_cooldowns order_id;
    ignore (InFlightAmendments.remove_in_flight_amendment order_id);

    (* 2. Clear tracking conditionally depending on whether it's Buy or Sell *)
    let cancelled_side = side in
    
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
    state.open_sell_orders <- List.filter (fun (sell_id, _) ->
      sell_id <> order_id
    ) state.open_sell_orders;
    let removed_sell = original_sell_count - List.length state.open_sell_orders in

    (* Clear inflight flags and global placement trackers to allow immediate re-placement *)
    (match cancelled_side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol cancelled_side));
    
    Logging.debug_f ~section "Order cancelled and cleaned up: %s for %s (removed %d pending, %d sell, added to blacklist)"
      order_id asset_symbol removed_pending removed_sell
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
          (* Check if old order was explicitly cancelled (race with multi-buy cleanup) *)
          let is_blacklisted = List.exists (fun (cid, _) -> cid = old_order_id) state.cancelled_orders in
          if is_blacklisted then
            Logging.info_f ~section "Ignoring amendment for blacklisted buy order %s -> %s for %s"
              old_order_id new_order_id asset_symbol
          else
          (match state.last_buy_order_id with
           | Some target_id when target_id = old_order_id ->
               state.last_buy_order_id <- Some new_order_id;
               state.last_buy_order_price <- Some price;
               Logging.info_f ~section "Amended buy order ID in tracking: %s -> %s @ %.2f for %s" 
                 old_order_id new_order_id price asset_symbol
             | _ -> 
                (* Fallback removed. If we didn't track the old order, we MUST NOT install the new order 
                   as the tracked buy, otherwise we resurrect ghost orders from stale orderUpdate websocket events 
                   which causes duplicate orders/ghost order detection when combined with fresh placements. *)
                Logging.info_f ~section "Ignoring amendment for untracked buy order %s -> %s for %s"
                  old_order_id new_order_id asset_symbol)
     | Sell ->
         let original_sell_count = List.length state.open_sell_orders in
         state.open_sell_orders <- (new_order_id, price) :: 
            List.filter (fun (sell_id, _) -> sell_id <> old_order_id) state.open_sell_orders;
         if List.length state.open_sell_orders = original_sell_count then
           Logging.info_f ~section "Amended sell order ID in tracking: %s -> %s @ %.2f for %s" 
             old_order_id new_order_id price asset_symbol
         else
           Logging.debug_f ~section "Set sell order ID via amend: %s @ %.2f for %s" 
             new_order_id price asset_symbol);
             
    (* Apply a short cooldown to avoid amending the newly amended order
       before the exchange's WebSocket open-orders cache catches up.
       This prevents race-condition duplicate amends on Hyperliquid. *)
    let now = Unix.time () in
    (* In-place modify (same ID): short cooldown as re-amendment throttle only.
       Cancel-replace (different IDs): longer cooldown to cover WS data lag. *)
    let cooldown = if old_order_id = new_order_id then 2.0 else 10.0 in
    Hashtbl.replace state.amend_cooldowns old_order_id (now +. cooldown);
    Hashtbl.replace state.amend_cooldowns new_order_id (now +. cooldown);

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

    (* 2. Determine whether to clear buy tracking.
       For Hyperliquid, the REST Cancel-Replace structure means the open order
       was untouched on the book even if the amend request failed. We apply a cooldown
       instead of dropping tracking, which would induce a spamming duplicate placement loop. *)
    let lower_reason = String.lowercase_ascii reason in
    let contains_fragment s fragment =
      let sl = String.length s and fl = String.length fragment in
      let rec loop i = i + fl <= sl && (String.sub s i fl = fragment || loop (i + 1)) in
      loop 0
    in
    
    let is_cache_miss = contains_fragment lower_reason "order not found" || contains_fragment lower_reason "not found for amendment" in
    let is_cannot_modify = contains_fragment lower_reason "cannot modify canceled or filled" in
    let is_margin_error = contains_fragment lower_reason "insufficient" || contains_fragment lower_reason "margin" in
    let is_rate_limit = contains_fragment lower_reason "too many cumulative requests" || contains_fragment lower_reason "rate limit" in

    let cooldown_duration = 
      if is_rate_limit then 10.0
      else if is_cache_miss || is_cannot_modify || is_margin_error then 0.5
      else 2.0
    in

    (* Apply cooldown to prevent spamming failed amends *)
    let now = Unix.time () in
    Hashtbl.replace state.amend_cooldowns order_id (now +. cooldown_duration);

    (* If the order is definitely gone (canceled or filled), we MUST clear tracking 
       so the grid can replace it. Otherwise we get stuck trying to amend it forever. *)
    let is_order_gone = is_cache_miss || is_cannot_modify || is_margin_error in
    
    if is_order_gone then begin
      (* Explicitly send a cancel order just in case it's wedged in the exchange's cache *)
      let cancel_order = create_cancel_order order_id asset_symbol "Grid" state.exchange_id in
      ignore (push_order cancel_order);

      (* Blacklist the gone order ID so racing amendment events
         don't reinstall it via handle_order_amended fallback.
         We set the timestamp 10 years in the future so it never expires from the 15s cleanup. *)
      let far_future = now +. 315360000.0 in
      state.cancelled_orders <- (order_id, far_future) :: state.cancelled_orders;
      (match side with
       | Buy ->
           (match state.last_buy_order_id with
            | Some target_id when target_id = order_id ->
                state.last_buy_order_id <- None;
                state.last_buy_order_price <- None;
                (* Clear place_Buy cooldown so strategy can place a new buy immediately
                   instead of waiting for a stale cooldown from the multi-buy cancel branch *)
                Hashtbl.remove state.amend_cooldowns "place_Buy";
                Logging.info_f ~section "Amendment failed for buy order %s: %s. Order is gone, cleared tracking." order_id reason
            | _ ->
                (* Order ID doesn't match tracking — already cleared by a previous call.
                   Still clear cooldowns to unblock placement. *)
                Hashtbl.remove state.amend_cooldowns "place_Buy";
                Logging.debug_f ~section "Amendment failed for untracked buy order %s: %s. Cleared place_Buy cooldown." order_id reason)
       | Sell ->
           let original_sell_count = List.length state.open_sell_orders in
           state.open_sell_orders <- List.filter (fun (sell_id, _) -> sell_id <> order_id) state.open_sell_orders;
           if List.length state.open_sell_orders < original_sell_count then
             Logging.info_f ~section "Amendment failed for sell order %s: %s. Order is gone, cleared tracking." order_id reason)
    end else begin
      (* Log exactly what tracking we are taking action on. We no longer 
         auto-clear tracking on other amendment failures to prevent replacement loops *)
      (match side with
       | Buy ->
           (match state.last_buy_order_id with
            | Some target_id when target_id = order_id ->
                Logging.info_f ~section "Amendment failed for buy order %s: %s. Applying %.1fs cooldown (keeping tracking)." order_id reason cooldown_duration
            | _ -> ())
       | Sell ->
           Logging.info_f ~section "Amendment failed for sell order %s: %s. Applying %.1fs cooldown (keeping tracking)." order_id reason cooldown_duration);
    end;
           
    (* Clear global in-flight trackers so we don't get stuck *)
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol side));
  )

(** Clean up pending cancellation tracking for a completed order *)
let cleanup_pending_cancellation asset_symbol order_id =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
  Hashtbl.remove state.pending_cancellations order_id;
  (* Safety net: if cancelled order is the tracked buy, clear tracking.
     Handles ghost orders from rapid amendments where the cancel response
     arrives but handle_order_cancelled never fires (order didn't exist). *)
  (match state.last_buy_order_id with
   | Some id when id = order_id ->
       state.last_buy_order_id <- None;
       state.last_buy_order_price <- None;
       set_asset_reserved_quote state 0.0;
       state.inflight_buy <- false;
       ignore (InFlightOrders.remove_in_flight_order
         (generate_side_duplicate_key asset_symbol Buy));
       Hashtbl.remove state.amend_cooldowns "place_Buy";
       Logging.info_f ~section
         "Cleared ghost buy tracking for %s after cancel cleanup of %s"
         asset_symbol order_id
   | _ -> ())
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
  Logging.debug_f ~section "Suicide Grid strategy initialized with order buffer size 4096";
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
  let handle_order_cancelled = handle_order_cancelled
  let handle_order_filled = handle_order_filled
  let handle_order_amended = handle_order_amended
  let handle_order_amendment_skipped = handle_order_amendment_skipped
  let handle_order_amendment_failed = handle_order_amendment_failed
  let handle_order_failed = handle_order_failed
  let cleanup_pending_cancellation = cleanup_pending_cancellation
  let cleanup_strategy_state = cleanup_strategy_state
  let init = init

  (** Clear the startup_replay flag so all subsequent fills are processed normally.
      Called by domain_spawner once the first exec event batch has been consumed. *)
  let set_startup_replay_done symbol =
    let state = get_strategy_state symbol in
    Mutex.lock state.mutex;
    if state.startup_replay then begin
      state.startup_replay <- false;
      Logging.info_f ~section "Startup replay complete for %s (last_fill_oid=%s, accumulated_profit=%.6f)"
        symbol
        (Option.value state.last_fill_oid ~default:"none")
        state.accumulated_profit;
      (* Bootstrap persistent state for new Hyperliquid suicide_grid strategies.
         If we have never persisted state for this symbol (last_fill_oid=None)
         but saw fill events during startup, establish an anchor point so the
         next restart can resume from a known position. *)
      if state.last_fill_oid = None
         && state.highest_startup_oid <> None
         && state.exchange_id = "hyperliquid" then begin
        state.last_fill_oid <- state.highest_startup_oid;
        Hyperliquid.State_persistence.save ~symbol
          ~reserved_base:state.reserved_base
          ~accumulated_profit:state.accumulated_profit
          ~last_fill_oid:(Option.get state.highest_startup_oid) ();
        Logging.info_f ~section
          "Bootstrapped initial state for %s (last_fill_oid=%s, reserved_base=%.8f, accumulated_profit=%.6f)"
          symbol
          (Option.get state.highest_startup_oid)
          state.reserved_base
          state.accumulated_profit
      end
    end;
    Mutex.unlock state.mutex
end