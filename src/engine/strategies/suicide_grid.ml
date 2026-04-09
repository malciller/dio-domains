(* Suicide Grid Strategy

   Grid trading system with a single-buy, multi-sell order model.
   Buy orders are placed one grid interval below price and trail upward,
   gated by sell orders above. Sell orders are placed one grid interval
   above price. Generated orders are pushed to a ringbuffer for
   consumption by the order executor. *)


let section = "suicide_grid"

open Strategy_common
module Exchange = Dio_exchange.Exchange_intf

(* Memoization cache for Exchange.Registry.get lookups by exchange name.
   Avoids repeated Hashtbl.find_opt on each round_price / get_price_increment call. *)
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

(* Per-(symbol,exchange) cache of resolved round_price closures.
   Key format: "symbol|exchange". Bypasses module dispatch on the hot path. *)
let _round_price_fn_cache : (string, float -> float) Hashtbl.t =
  Hashtbl.create 8


(** Returns the first [n] elements of a list. Tail-recursive. *)
let take n lst =
  let rec aux acc n = function
    | [] -> List.rev acc
    | _ when n <= 0 -> List.rev acc
    | x :: xs -> aux (x :: acc) (n - 1) xs
  in
  aux [] n lst

(** Case-sensitive substring search. Returns true if [s] contains [fragment]. *)
let contains_fragment s fragment =
  let sl = String.length s and fl = String.length fragment in
  let rec loop i = i + fl <= sl && (String.sub s i fl = fragment || loop (i + 1)) in
  loop 0

(** Exchange-specific behavioral configuration.
    Encodes per-exchange differences in order handling, balance checks, and
    sell logic. Persistence routing uses explicit exchange_id checks. *)
type exchange_config = {
  time_in_force: string;                      (** TIF value for limit orders ("Alo" or "GTC") *)
  track_pending_sells: bool;                  (** Add sell orders to pending_orders tracking *)
  use_accumulation_sells: bool;               (** Enable profit-gated accumulation sell path *)
  sell_uses_mult: bool;                       (** true: sell qty = qty * sell_mult; false: 1:1 sells *)
  sell_failure_sets_asset_low: bool;           (** Set asset_low on sell insufficient-balance rejection *)
  use_reserved_base_guard: bool;              (** Check reserved_base + locked_in_sells before selling *)
  asset_low_requires_balance_change: bool;    (** true: clear asset_low only on balance increase *)
  merge_preserved_sells: bool;                (** Merge recently_injected_sells into open_sell_orders *)
  check_stale_balance: bool;                  (** Block strategy execution when balance data is missing *)
}

let kraken_config = {
  time_in_force = "GTC";
  track_pending_sells = true;
  use_accumulation_sells = false;
  sell_uses_mult = true;
  sell_failure_sets_asset_low = false;
  use_reserved_base_guard = false;
  asset_low_requires_balance_change = true;
  merge_preserved_sells = false;
  check_stale_balance = true;
}

let hyperliquid_config = {
  time_in_force = "Alo";
  track_pending_sells = false;
  use_accumulation_sells = true;
  sell_uses_mult = false;
  sell_failure_sets_asset_low = true;
  use_reserved_base_guard = true;
  asset_low_requires_balance_change = false;
  merge_preserved_sells = true;
  check_stale_balance = false;
}

let ibkr_config = {
  time_in_force = "GTC";
  track_pending_sells = true;
  use_accumulation_sells = true;     (* accumulate whole shares via profit *)
  sell_uses_mult = false;             (* 1:1 sells for equity *)
  sell_failure_sets_asset_low = true;  (* Exchange rejection as safety net *)
  use_reserved_base_guard = true;     (* Pre-check position before selling — prevents short-selling on margin *)
  asset_low_requires_balance_change = false;
  merge_preserved_sells = false;
  check_stale_balance = true;
}

let get_exchange_config exchange =
  if exchange = "hyperliquid" then hyperliquid_config
  else if exchange = "ibkr" then ibkr_config
  else kraken_config

(** Per-asset trading configuration. *)
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

(** Shared order ringbuffer across all strategy domains. *)
let order_buffer = OrderRingBuffer.create 2048
let order_buffer_mutex = Mutex.create ()

(** Accessor for the shared order ringbuffer. *)
let get_order_buffer () = order_buffer



(** Mutable per-symbol strategy state. *)
type strategy_state = {
  mutable last_buy_order_price: float option;
  mutable last_buy_order_id: string option;
  mutable open_sell_orders: (string * float * float) list;  (* (order_id, price, qty) *)
  mutable recently_injected_sells: (string * float * float) list; (* (order_id, price, timestamp) *)
  mutable pending_orders: (string * order_side * float * float) list;  (* (order_id, side, price, timestamp) *)
  mutable last_cycle: int;
  mutable last_order_time: float;  (* Unix timestamp of most recent order submission *)
  mutable inflight_cancel_buy: bool;  (* true while buy cancel is pending confirmation via order channel *)
  mutable inflight_amend_buy: bool;   (* true while buy amend is in-flight; gates duplicate-buy cancel during inject/remove window *)
  mutable amend_cooldowns: (string, float) Hashtbl.t; (* order_id -> expiry Unix timestamp *)
  mutable last_cleanup_time: float;
  mutable inflight_buy: bool;   (* true while buy Place is pending ack or reject *)
  mutable inflight_sell: bool;  (* true while sell Place is pending ack or reject *)
  mutable asset_low: bool;      (* set when asset balance is insufficient for next sell; pauses sell and buy *)
  mutable capital_low: bool;    (* set when quote balance is insufficient for next buy; pauses strategy *)
  mutable capital_low_logged: bool; (* suppresses repeated capital-low log warnings *)
  mutable capital_low_at_balance: float; (* quote_bal snapshot when capital_low was set; clears only on balance increase *)
  mutable reserved_quote: float; (* quote amount reserved by current open buy for this symbol *)
  mutable accumulated_profit: float;    (* realized PnL from buy/sell cycles; gates accumulation sell placement *)
  mutable reserved_base: float;  (* base asset accumulated via sell_mult; excluded from sellable balance *)
  mutable last_buy_fill_price: float option; (* fill price of most recent buy; cost basis for sell profit calc *)
  mutable last_sell_fill_price: float option; (* fill price of most recent sell; cost basis for consecutive sells *)
  mutable grid_qty: float;              (* cached config qty; used by fill handler for profit calc *)
  mutable cached_sell_mult: float;      (* cached parsed sell_mult; avoids float_of_string per cycle *)
  mutable cached_ecfg: exchange_config; (* cached exchange_config; avoids string comparison per cycle *)
  mutable maker_fee: float;             (* cached maker fee rate; used by fill handler for profit calc *)
  mutable exchange_id: string;          (* cached exchange name; used for persistence routing *)
  mutable startup_replay: bool;  (* true during startup fill replay; suppresses profit calculation *)
  mutable last_fill_oid: string option; (* OID of last profit-credited fill; replay resumption point *)
  mutable highest_startup_oid: string option; (* highest fill OID observed during startup; bootstraps new strategies *)
  mutable anticipated_base_credit: float;  (* base qty from buy fills not yet reflected in balance feed *)
  mutable last_seen_asset_balance: float;  (* previous asset_bal value; used to detect balance feed updates *)
  mutable persistence_dirty: bool;  (* true when accumulation state changed; flushed by caller outside hotloop *)
  mutable last_cycle_orders_hash: int;  (* tracks exchange state for 0-alloc diffing *)
  mutable last_cycle_buy_count: int;
  mutex: Mutex.t;  (* per-symbol mutex; prevents concurrent strategy execution *)
}

(** Global registry of per-symbol strategy states. *)
let strategy_states : (string, strategy_state) Hashtbl.t = Hashtbl.create 16
let strategy_states_mutex = Mutex.create ()

(** Retrieves or lazily initializes the strategy state for [asset_symbol].
    On first access, loads persisted state (reserved_base, accumulated_profit,
    last_fill_oid, fill prices) from Hyperliquid.State_persistence. *)
let get_strategy_state asset_symbol =
  Mutex.lock strategy_states_mutex;
  let state =
    match Hashtbl.find_opt strategy_states asset_symbol with
    | Some state -> state
    | None ->
        (* Load persisted state for this symbol (survives container restarts). *)
        let persisted_reserved_base = Hyperliquid.State_persistence.load_reserved_base ~symbol:asset_symbol in
        let persisted_accumulated_profit = Hyperliquid.State_persistence.load_accumulated_profit ~symbol:asset_symbol in
        let persisted_last_fill_oid = Hyperliquid.State_persistence.load_last_fill_oid ~symbol:asset_symbol in
        let persisted_last_buy_fill_price = Hyperliquid.State_persistence.load_last_buy_fill_price ~symbol:asset_symbol in
        let persisted_last_sell_fill_price = Hyperliquid.State_persistence.load_last_sell_fill_price ~symbol:asset_symbol in
        let new_state = {
          last_buy_order_price = None;
          last_buy_order_id = None;
          open_sell_orders = [];
          recently_injected_sells = [];
          pending_orders = [];
          last_cycle = 0;
          last_order_time = 0.0;
          inflight_cancel_buy = false;
          inflight_amend_buy = false;
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
          cached_sell_mult = 1.0;
          cached_ecfg = kraken_config;
          maker_fee = 0.0;
          exchange_id = "";
          startup_replay = true;  (* gates profit calc until set_startup_replay_done *)
          last_fill_oid = persisted_last_fill_oid;
          highest_startup_oid = None;
          anticipated_base_credit = 0.0;
          last_seen_asset_balance = 0.0;
          persistence_dirty = false;
          last_cycle_orders_hash = 0;
          last_cycle_buy_count = 0;
          mutex = Mutex.create ();
        } in
        Hashtbl.replace strategy_states asset_symbol new_state;
        new_state
  in
  Mutex.unlock strategy_states_mutex;
  state

(** Mutex protecting all reserved_quote reads and writes across domains.
    Lock order: state.mutex -> reservation_mutex.
    reservation_mutex is never held while waiting for state.mutex. *)
let reservation_mutex = Mutex.create ()

(** Tracking total reserved quote per exchange to avoid O(N) strategy_states_mutex locking. *)
let total_reserved_by_exchange : (string, float) Hashtbl.t = Hashtbl.create 4

(** Gets the cached total reserved quote for [exchange]. Caller must hold reservation_mutex. *)
let get_total_reserved_quote_locked ~exchange =
  match Hashtbl.find_opt total_reserved_by_exchange exchange with
  | Some total -> total
  | None -> 0.0

(** Acquires reservation_mutex and returns total reserved_quote for [exchange].
    Safe to call outside state.mutex. *)
let get_total_reserved_quote ~exchange =
  Mutex.lock reservation_mutex;
  let total = get_total_reserved_quote_locked ~exchange in
  Mutex.unlock reservation_mutex;
  total

(** Sets this asset's reserved_quote under reservation_mutex.
    May be called from inside state.mutex (respects lock order). *)
let set_asset_reserved_quote state v =
  Mutex.lock reservation_mutex;
  let diff = v -. state.reserved_quote in
  state.reserved_quote <- v;
  if state.exchange_id <> "" then begin
    let current_total = get_total_reserved_quote_locked ~exchange:state.exchange_id in
    Hashtbl.replace total_reserved_by_exchange state.exchange_id (current_total +. diff)
  end;
  Mutex.unlock reservation_mutex

(** Atomically checks available quote balance and reserves for a buy if sufficient.
    Returns (balance_ok, available_quote, total_reserved).
    Must be called from inside state.mutex. On success, sets state.reserved_quote. *)
let atomic_check_and_reserve state quote_bal quote_needed reserve_amount =
  Mutex.lock reservation_mutex;
  let total_reserved = get_total_reserved_quote_locked ~exchange:state.exchange_id in
  let available = quote_bal -. total_reserved in
  let ok = available >= quote_needed in
  if ok then begin
    let diff = reserve_amount -. state.reserved_quote in
    state.reserved_quote <- reserve_amount;
    if state.exchange_id <> "" then begin
      Hashtbl.replace total_reserved_by_exchange state.exchange_id (total_reserved +. diff)
    end
  end;
  Mutex.unlock reservation_mutex;
  (ok, available, total_reserved)

(** Parses a config string to float with fallback default and warning. *)
let parse_config_float config value_name default exchange symbol =
  try float_of_string config with
  | Failure _ ->
      Logging.warn_f ~section "Invalid %s value '%s' for %s/%s, using default %.4f"
        value_name config exchange symbol default;
      default

(** Rounds price to exchange-mandated precision for the given symbol.
    Uses a per-(symbol,exchange) closure cache to skip module dispatch. *)
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

(** Returns the minimum price increment (tick size) for the symbol. *)
let get_price_increment symbol exchange =
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) -> Option.value (Ex.get_price_increment ~symbol) ~default:0.01
  | None ->
      Logging.warn_f ~section "No price increment info for %s/%s, using default 0.01" exchange symbol;
      0.01

(** Returns the quantity increment (lot size) for the symbol. *)
let get_qty_increment_val symbol exchange =
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) -> Option.value (Ex.get_qty_increment ~symbol) ~default:0.01
  | None -> 0.01

(** Rounds quantity down to the nearest valid lot size. *)
let round_qty qty symbol exchange =
  let increment = get_qty_increment_val symbol exchange in
  let inv = 1.0 /. increment in
  floor (qty *. inv) /. inv

(** Returns the minimum price delta required to trigger an amendment.
    max(10 * tick_size, 5% of one grid interval). Prevents amendment spam. *)
let get_min_move_threshold price grid_interval_pct symbol exchange =
  let base_increment = get_price_increment symbol exchange in
  (* Require at least 5% of one grid interval or 10x base increment. *)
  let pct_based = price *. (grid_interval_pct *. 0.05 /. 100.0) in
  max (base_increment *. 10.0) pct_based

(** Computes a grid price one interval above or below [current_price]. *)
let calculate_grid_price current_price grid_interval_pct is_above symbol exchange =
  let interval = current_price *. (grid_interval_pct /. 100.0) in
  let raw_price = if is_above then current_price +. interval else current_price -. interval in
  round_price raw_price symbol exchange

(** Returns true if an amendment is permitted for [order_id].
    Checks: no pending amend, not in InFlightAmendments, no active cooldown,
    price_diff >= min_move_threshold, and target differs from current. *)
let amend_allowed ~state ~order_id ~target_price ~current_price_rounded
    ~price_diff ~min_move_threshold =
  let is_being_amended = List.exists (fun (id, _, _, _) ->
    String.starts_with ~prefix:"pending_amend_" id &&
    String.sub id 14 (String.length id - 14) = order_id
  ) state.pending_orders in
  let is_in_flight = InFlightAmendments.is_in_flight order_id in
  let is_on_cooldown = Hashtbl.mem state.amend_cooldowns order_id in
  not is_being_amended && not is_in_flight && not is_on_cooldown
  && price_diff >= min_move_threshold
  && target_price <> current_price_rounded

(** Returns true if quote_balance >= quote_needed. *)
let can_place_buy_order (_qty : float) quote_balance quote_needed =
  quote_balance >= quote_needed

(** Returns true if asset_balance >= asset_needed. *)
let can_place_sell_order (_qty : float) (_sell_mult : float) asset_balance asset_needed =
  asset_balance >= asset_needed

(** Generates a dedup key for in-flight order tracking. One key per (asset, side) pair. *)
let generate_side_duplicate_key asset_symbol side =
  Printf.sprintf "%s|%s|grid" asset_symbol (string_of_order_side side)

let create_place_order asset_symbol side qty price post_only strategy exchange =
  let ecfg = get_exchange_config exchange in
  {
    operation = Place;
    order_id = None;
    symbol = asset_symbol;
    exchange;
    side;
    order_type = "limit";
    qty;
    price;
    time_in_force = ecfg.time_in_force;
    post_only;
    userref = Some Strategy_common.strategy_userref_grid;
    strategy;
    duplicate_key = generate_side_duplicate_key asset_symbol side;
  }

(** Constructs an Amend strategy_order targeting [order_id]. *)
let create_amend_order order_id asset_symbol side qty price post_only strategy exchange =
  let ecfg = get_exchange_config exchange in
  {
    operation = Amend;
    order_id = Some order_id;
    symbol = asset_symbol;
    exchange;
    side;
    order_type = "limit";
    qty;
    price;
    time_in_force = ecfg.time_in_force;
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
    side = Buy;
    order_type = "limit";
    qty = 0.0;
    price = None;
    time_in_force = "GTC";
    post_only = false;
    userref = None;
    strategy;
    duplicate_key = "";
  }

(** Backwards-compatible order constructor. Delegates to create_place_order with Grid strategy. *)
let create_order asset_symbol side qty price post_only exchange =
  create_place_order asset_symbol side qty price post_only Grid exchange

(** Pushes an order to the ringbuffer. Returns true on success, false on duplicate or full buffer.
    [now] is a pre-computed Unix.time() timestamp to avoid redundant syscalls. *)
let push_order ~now order =
  let operation_str = match order.operation with
    | Place -> "place"
    | Amend -> "amend"
    | Cancel -> "cancel"
  in

  (* For Cancel operations, set inflight_cancel_buy when cancelling a buy *)
  (match order.operation with
   | Cancel ->
       let state = get_strategy_state order.symbol in
       (match order.order_id with
        | Some target_order_id ->
            Mutex.lock order_buffer_mutex;
            let write_result = OrderRingBuffer.write order_buffer order in
            Mutex.unlock order_buffer_mutex;

            (match write_result with
            | Some () ->
                Strategy_common.OrderSignal.broadcast ();
                Logging.debug_f ~section "Pushed %s %s order: %s %.8f @ %s"
                  operation_str
                  (string_of_order_side order.side)
                  order.symbol
                  order.qty
                  (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market");
                state.last_order_time <- now;
                (* Gate open_orders sync until order channel confirms the cancel. *)
                if order.side = Buy then
                  state.inflight_cancel_buy <- true;
                Logging.debug_f ~section "Cancelling order for %s (target: %s)"
                  order.symbol target_order_id;
                true
            | None ->
                Logging.warn_f ~section "Order ringbuffer full, dropped %s %s order for %s"
                  operation_str (string_of_order_side order.side) order.symbol;
                false)
        | None ->
            Logging.warn_f ~section "Cancel operation missing order_id for %s" order.symbol;
            false)
   | _ ->
       (* Reject duplicate in-flight orders before writing to the buffer. *)
       let is_duplicate = match order.operation with
         | Place -> not (InFlightOrders.add_in_flight_order order.duplicate_key)
         | Amend -> (match order.order_id with Some oid -> not (InFlightAmendments.add_in_flight_amendment oid) | None -> false)
         | _ -> false
       in

       if is_duplicate then begin
         Logging.debug_f ~section "Duplicate %s detected (strategy): %s" operation_str order.symbol;
         false
       end else begin
         (* Write Place or Amend to ringbuffer. *)
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

           (* Update strategy state. *)
           let state = get_strategy_state order.symbol in
           state.last_order_time <- now;

            (* Set inflight flags for Place operations. *)
            (match order.operation, order.side with
             | Place, Buy -> state.inflight_buy <- true
             | Place, Sell -> state.inflight_sell <- true
             | _ -> ());

            (* Route per-operation tracking. *)
            (match order.operation with
             | Place ->
                 (* Skip pending tracking for sell orders on exchanges that do not
                    use it. Prevents ghost entries that cause re-placement loops. *)
                 let order_ecfg = get_exchange_config order.exchange in
                 let skip_pending = not order_ecfg.track_pending_sells && order.side = Sell in
                 if not skip_pending then begin
                   (* Track as pending with temp ID until exchange ack. *)
                   let temp_order_id = Printf.sprintf "pending_%s_%.2f"
                     (string_of_order_side order.side)
                     (Option.value order.price ~default:0.0) in
                   let order_price = Option.value order.price ~default:0.0 in
                   let timestamp = now in
                   state.pending_orders <- (temp_order_id, order.side, order_price, timestamp) :: state.pending_orders;
                   Logging.debug_f ~section "Added pending order: %s %s @ %.2f for %s"
                     (string_of_order_side order.side) temp_order_id order_price order.symbol;

                   (* Track sell orders in state for grid spacing logic. *)
                   (match order.side, order.price with
                    | Sell, Some price ->
                        state.open_sell_orders <- (temp_order_id, price, order.qty) :: state.open_sell_orders;
                        Logging.debug_f ~section "Tracking sell order %s @ %.2f for %s" temp_order_id price order.symbol
                    | _ -> ())
                 end
            | Amend ->
                (* Track pending amend; do not add to open_sell_orders. *)
                let temp_order_id = Printf.sprintf "pending_amend_%s"
                  (Option.value order.order_id ~default:"unknown") in
                let order_price = Option.value order.price ~default:0.0 in
                let timestamp = now in
                state.pending_orders <- (temp_order_id, order.side, order_price, timestamp) :: state.pending_orders;
                Logging.debug_f ~section "Added pending amend: %s %s @ %.2f for %s (target: %s)"
                  (string_of_order_side order.side) temp_order_id order_price order.symbol
                  (Option.value order.order_id ~default:"unknown");
                (* Gate duplicate-buy cancel for the inject->remove window. *)
                if order.side = Buy then
                  state.inflight_amend_buy <- true
            | Cancel -> (* Handled in the Cancel branch above. *)
                ());
           true
       | None ->
           (* Remove from in-flight tracking if ringbuffer write failed. *)
           (match order.operation with
            | Place -> ignore (InFlightOrders.remove_in_flight_order order.duplicate_key)
            | Amend -> (match order.order_id with Some oid -> ignore (InFlightAmendments.remove_in_flight_amendment oid) | None -> ())
            | _ -> ());

           Logging.warn_f ~section "Order ringbuffer full, dropped %s %s order for %s"
             operation_str (string_of_order_side order.side) order.symbol;
           false
       end)

(** Main strategy execution loop.
    [~now] is a pre-computed [Unix.gettimeofday ()] timestamp passed by the
    caller to avoid redundant syscalls on every cycle. *)
let execute_strategy
    ?cached_state
    ~now
    (asset : trading_config)
    (current_price : float option)
    (top_of_book : (float * float * float * float) option)
    (asset_balance : float option)
    (quote_balance : float option)
    (_open_buy_count : int)
    (_open_sell_count : int)
    (iter_open_orders : ((string -> float -> float -> string -> int option -> unit) -> unit))
    (cycle : int) =

  let state = match cached_state with Some s -> s | None -> get_strategy_state asset.symbol in
  if state.exchange_id = "" then state.exchange_id <- asset.exchange;
  let ecfg = state.cached_ecfg in

  (* Only proceed with main strategy if capital_low flag is clear.
     asset_low is checked below only for sell+buy placement; order sync,
     cleanup, and cancellation detection must always run.
     NOTE: All state mutations below run under state.mutex to prevent
     races when concurrent callers target the same symbol. *)
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->

   (* Anticipated balance credit decay: clear when balance feed catches up. *)
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

   (* Asset-low check: clear flag when asset balance recovers.
      Subtract reserved_base so accumulated base asset is protected from sells. *)
   (match asset_balance with
    | Some asset_bal ->
        let qty_f = state.grid_qty in
        let asset_needed_fast =
          if ecfg.sell_uses_mult then
            qty_f *. state.cached_sell_mult
          else qty_f  (* 1:1 sells require full qty *)
        in
        let available_asset = asset_bal -. state.reserved_base +. state.anticipated_base_credit in
        let balance_actually_changed = asset_bal > state.last_seen_asset_balance in
        let should_clear =
          if ecfg.asset_low_requires_balance_change then
            available_asset >= asset_needed_fast && balance_actually_changed
          else
            available_asset >= asset_needed_fast
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

  (* Capital-low fast path: skip strategy when quote balance is known to be
     insufficient for a buy order. The FIRST loop (capital_low=false) is allowed
     so that any accrued sell orders from previously filled buys can be placed
     to free capital. After that first pass, capital_low is set and the strategy
     short-circuits here. When balance recovers, the flag is cleared. *)
  (match quote_balance with
   | Some quote_bal ->
       let qty_f = state.grid_qty in
       let quote_needed_fast = (match current_price with Some p -> p *. qty_f | None -> 0.0) in
       (* Available balance = total balance minus all domains' reserved quote. *)
       let total_reserved = get_total_reserved_quote ~exchange:state.exchange_id in
       let available_quote = quote_bal -. total_reserved in
       if state.capital_low && available_quote < quote_needed_fast then begin
         (* Stamp balance snapshot on first capital_low cycle so recovery
            requires a real balance feed increase, not stale data re-read. *)
         if state.capital_low_at_balance = 0.0 then
           state.capital_low_at_balance <- quote_bal;
         (* Still low: skip entire strategy body to avoid order spam. *)
         ()
       end else if state.capital_low && quote_bal > state.capital_low_at_balance then begin
         (* Balance feed delivered a higher balance than the snapshot taken when
            capital_low was set. A real event (fill crediting USD or cancel freeing
            reserve) has occurred. Only now is it safe to retry placement. Clearing
            on stale balance data (same quote_bal as at rejection) caused tight
            rejection loops. *)
         state.capital_low <- false;
         state.capital_low_logged <- false;
         state.capital_low_at_balance <- 0.0;  (* reset snapshot for next recovery episode *)
         (* Flush buy-side placement cooldown so the strategy can place immediately.
            Without this, the 2s cooldown set when capital_low was first triggered
            blocks the first buy attempt even after balance recovers. *)
         Hashtbl.remove state.amend_cooldowns "place_Buy";
         (* Clear inflight_buy and evict the InFlightOrders duplicate key. *)
         state.inflight_buy <- false;
         ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset.symbol Buy));
         Logging.info_f ~section "Capital restored for %s (available %.2f, need %.2f, total_reserved %.2f, was_at %.2f) - resuming strategy"
           asset.symbol available_quote quote_needed_fast total_reserved state.capital_low_at_balance
       end
       (* capital_low=false: fall through normally *)
   | None -> () (* No balance data yet, fall through *)
  );

  if not state.capital_low then begin

  (* [now] is provided by the caller to avoid per-cycle syscalls. *)
  
  match current_price, top_of_book with
  | None, _ -> ()  (* No price data available yet *)
  | Some price, top_opt ->
      (* Use l2Book top-of-book bid/ask instead of allMids mid price.
         l2Book updates per-block and tracks actual market state, while
         allMids batches across all coins and can diverge from fill price
         during fast candle movements, causing sell order stacking on Hyperliquid.
         Sell-side calcs use bid_price; buy-side calcs use ask_price.
         Falls back to mid when orderbook data is unavailable. *)
      let (bid_price, ask_price) = match top_opt with
        | Some (bid, _, ask, _) when bid > 0.0 && ask > 0.0 -> (bid, ask)
        | _ -> (price, price)
      in

      (* Clean up stale pending orders and enforce a hard limit in a single pass.
         Combines List.length + List.filter + List.length + take into one
         List.fold_left that counts kept/removed and truncates at 50. *)
      let needs_pending_cleanup =
        let rec check_stale count = function
          | [] -> count > 50
          | (_, _, _, ts) :: rest ->
              if now -. ts > 5.0 then true else check_stale (count + 1) rest
        in check_stale 0 state.pending_orders
      in
      if needs_pending_cleanup then begin
        let (kept_rev, kept_count, removed_count) =
          List.fold_left (fun (acc, kept, removed) ((order_id, side, _, timestamp) as entry) ->
            let age = now -. timestamp in
            if age > 5.0 then begin
              Logging.warn_f ~section "Removing stale pending order %s for %s (age: %.1fs)" order_id asset.symbol age;
              if String.starts_with ~prefix:"pending_amend_" order_id then begin
                let target_oid = String.sub order_id 14 (String.length order_id - 14) in
                ignore (InFlightAmendments.remove_in_flight_amendment target_oid)
              end else begin
                let duplicate_key = generate_side_duplicate_key asset.symbol side in
                ignore (InFlightOrders.remove_in_flight_order duplicate_key);
                (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false)
              end;
              (acc, kept, removed + 1)
            end else if kept >= 50 then
              (acc, kept, removed + 1)  (* silently drop excess beyond hard limit *)
            else
              (entry :: acc, kept + 1, removed)
          ) ([], 0, 0) state.pending_orders
        in
        state.pending_orders <- List.rev kept_rev;
        if removed_count > 0 && cycle mod 100000 = 0 then
          Logging.debug_f ~section "Cleaned up %d pending orders for %s (kept %d)" removed_count asset.symbol kept_count;
      end;


      (* Clean up expired amend cooldowns. *)
      if Hashtbl.length state.amend_cooldowns > 0 then begin
        let has_expired = ref false in
        Hashtbl.iter (fun _ expiry_time -> if now > expiry_time then has_expired := true) state.amend_cooldowns;
        if !has_expired then begin
          let to_remove_cooldowns = ref [] in
          Hashtbl.iter (fun order_id expiry_time ->
            if now > expiry_time then
              to_remove_cooldowns := order_id :: !to_remove_cooldowns
          ) state.amend_cooldowns;
          List.iter (fun order_id ->
            Hashtbl.remove state.amend_cooldowns order_id
          ) !to_remove_cooldowns;
        end;
        (* Hard cap: prevent unbounded growth if cooldowns accumulate faster than they expire. *)
        if Hashtbl.length state.amend_cooldowns > 100 then begin
          Hashtbl.reset state.amend_cooldowns;
          Logging.warn_f ~section "amend_cooldowns exceeded 100 entries for %s, reset" asset.symbol
        end
      end;

      (* Sync strategy state with actual open orders from exchange.
         Preserve sell orders set by handle_order_amended or newly placed
         execution events that may not yet appear in exchange data due to WS lag. *)
      let now_time = now in
      let needs_sells_cleanup =
        let rec check_injected count = function
          | [] -> count > 20
          | (_, _, ts) :: rest -> 
              if now_time -. ts >= 10.0 then true else check_injected (count + 1) rest
        in check_injected 0 state.recently_injected_sells
      in
      if needs_sells_cleanup then begin
        state.recently_injected_sells <- List.filter (fun (_, _, ts) -> 
          now_time -. ts < 10.0
        ) state.recently_injected_sells;
        (* Cap to 20 entries to prevent O(n^2) scans during sell storms. *)
        if List.length state.recently_injected_sells > 20 then
          state.recently_injected_sells <- take 20 state.recently_injected_sells
      end;
      let preserved_sells = state.recently_injected_sells in
      state.open_sell_orders <- [];
      (* Preserve buy tracking across sync for all exchanges.
         Exchange open orders feed may lag behind execution events,
         so it may briefly report 0 buys while we know one exists.
         Genuine cancellations clear tracking via handle_order_cancelled. *)
      
      (* pending_orders are managed by order placement responses; not cleared here. *)

      (* Rebuild buy and sell lists from exchange open_orders data in a single pass. *)
          let open_buy_count_local = ref 0 in
          let buy_orders = ref [] in
          let sell_orders = ref [] in
          
          iter_open_orders (fun order_id order_price qty side_str userref_opt ->
            let is_our_strategy = match userref_opt with
              | Some ref_val -> ref_val <> Strategy_common.strategy_userref_mm
              | None -> true
            in
            if qty > 0.0 && is_our_strategy then begin
              if side_str = "buy" then begin
                incr open_buy_count_local;
                buy_orders := (order_id, order_price) :: !buy_orders
              end else begin
                sell_orders := (order_id, order_price, qty) :: !sell_orders
              end
            end
          );

      (* Set buy order price and ID (select highest buy if multiple exist).
         Gate: if a cancel is in flight, the open_orders snapshot is not trusted
         because the order channel has not yet confirmed the cancel, so open_orders
         may still report the order as open. Only the order channel
         (handle_order_cancelled) clears inflight_cancel_buy. *)
      (match !buy_orders with
       | [] when not state.inflight_cancel_buy && not state.inflight_buy ->
           (* No open buy and no in-flight op; clear reservation. *)
           set_asset_reserved_quote state 0.0
       | orders when not state.inflight_cancel_buy && not state.inflight_buy ->
           (* Safe to sync: no in-flight operation. *)
           let (best_order_id, best_price) = List.fold_left (fun (acc_id, acc_price) (order_id, price) ->
             if price > acc_price then (order_id, price) else (acc_id, acc_price)
           ) (List.hd orders) (List.tl orders) in
           state.last_buy_order_price <- Some best_price;
           state.last_buy_order_id <- Some best_order_id;
           (* Sync per-asset reserved_quote from the actual open buy so the
              capital_low check uses the real exchange-reserved amount after restart. *)
           let qty = state.grid_qty in
           set_asset_reserved_quote state (best_price *. qty)
       | _ ->
           (* In-flight cancel or place: open_orders may be stale.
              Retain current state; order channel will resolve. *)
           ());

      (* Apply exchange sell orders. *)
      state.open_sell_orders <- !sell_orders;

      (* Merge any preserved sell orders that aren't in the exchange-sourced list.
         This covers cancel-replace amendments where the new order hasn't yet
         appeared in the order update stream. *)
      if ecfg.merge_preserved_sells then begin
        List.iter (fun (preserved_id, preserved_price, _) ->
          let already_present = List.exists (fun (id, _, _) -> id = preserved_id) state.open_sell_orders in
          if not already_present then
            state.open_sell_orders <- (preserved_id, preserved_price, state.grid_qty) :: state.open_sell_orders
        ) preserved_sells
      end;

      Logging.debug_f ~section "Synced open orders for %s: %d buys, %d sells"
        asset.symbol (List.length !buy_orders) (List.length !sell_orders);

      (* Use cached parsed config values; updated once at domain startup
         and whenever the config ref changes (Fear & Greed re-evaluation). *)
      let qty = state.grid_qty in
      let grid_interval = asset.grid_interval in
      let sell_mult = state.cached_sell_mult in

      (* Refresh maker_fee from live cache (exchange may push updated fees). *)
      state.maker_fee <- (match asset.maker_fee with
        | Some f -> f
        | None ->
            match Fee_cache.get_maker_fee ~exchange:asset.exchange ~symbol:asset.symbol with
            | Some cached -> cached
            | None -> 0.0);

      (* Calculate required balances. *)
      let quote_needed = ask_price *. qty in
      let asset_needed =
        if ecfg.sell_uses_mult then qty *. sell_mult
        else qty  (* 1:1 sells *)
      in

  (* Log current balance status; throttled to every 10,000 cycles. *)
  if cycle mod 10000 = 0 then
    Logging.debug_f ~section "Balance check for %s: asset_balance=%.8f, quote_balance=%.2f, needed asset=%.8f, needed quote=%.2f"
      asset.symbol
      (Option.value asset_balance ~default:0.0)
      (Option.value quote_balance ~default:0.0)
      asset_needed quote_needed;

  (* Check for missing balance data. Stale but present balance data is acceptable.
     Supervisor monitors WebSocket health via heartbeats. *)
  let is_stale = ecfg.check_stale_balance && (
    match asset_balance, quote_balance with
    | None, None ->
        (* No balance data at all; cannot trade without balance info. *)
        if state.last_cycle <> cycle then
          Logging.debug_f ~section "No balance data available for %s - waiting for balance feed" asset.symbol;
        true
    | Some _, None | None, Some _ ->
        (* Partial balance data; may indicate feed issue. *)
        if state.last_cycle <> cycle then
          Logging.debug_f ~section "Partial balance data for %s - waiting for complete data" asset.symbol;
        true
    | Some ab, Some qb ->
        (* Have balance data; check if balances are sufficient. *)
        if ab = 0.0 && qb < 10.0 then begin
          (* Very low balances; may not be able to trade. *)
          if state.last_cycle <> cycle then
            Logging.debug_f ~section "Very low balances for %s - asset: %.8f, quote: %.2f" asset.symbol ab qb;
          false
        end else
          false
  ) in

  (* If balance feed is stale, skip strategy to avoid trading with stale data. *)
  if is_stale then begin
    state.last_cycle <- cycle;
    ()
  end else begin
    (* Core strategy logic based on open orders and balances.
       Log state periodically for race condition debugging. *)
    if state.last_cycle <> cycle && cycle mod 100000 = 0 then begin
      Logging.debug_f ~section "Strategy state for %s: pending_orders=%d, buy_price=%s, sell_count=%d"
        asset.symbol (List.length state.pending_orders)
        (match state.last_buy_order_price with Some p -> Printf.sprintf "%.2f" p | None -> "none")
        (List.length state.open_sell_orders)
    end;

    let buy_order_pending = List.exists (fun (_, side, _, _) -> side = Buy) state.pending_orders in
    (* Effective buy count: use max of raw count and our own tracking.
       webData2 snapshots can be stale during cancel-replace amendments.
       last_buy_order_id is set immediately by orderUpdates events. *)
    let has_tracked_buy = state.last_buy_order_id <> None in
    (* Count non-cancelled open buy orders for balance check *)
    let open_buy_count = _open_buy_count in
    let effective_buy_count = if has_tracked_buy && open_buy_count = 0 then 1 else open_buy_count in

    if buy_order_pending then begin
        (* Log every 100,000 iterations to avoid spam. *)
        if cycle mod 100000 = 0 then
            Logging.debug_f ~section "Waiting for pending buy order to complete for %s" asset.symbol;
    end else if effective_buy_count > 1 && not state.inflight_cancel_buy && not state.inflight_amend_buy then begin
        (* Case 0: Multiple buy orders exist. Cancel all to enforce single buy
           order policy. Fires at startup reconciliation when last_buy_order_id=None
           and the order channel shows multiple buys. inflight_cancel_buy gates
           sync until handle_order_cancelled confirms each cancel. *)
        Logging.info_f ~section "Found %d buy orders for %s, cancelling all buy orders to maintain single buy order policy"
          effective_buy_count asset.symbol;

        (* Cancel all buy orders. *)
        List.iter (fun (order_id, _) ->
          let cancel_order = create_cancel_order order_id asset.symbol Grid asset.exchange in
          ignore (push_order ~now cancel_order);
          Logging.info_f ~section "Cancelling excess buy order: %s for %s" order_id asset.symbol
        ) !buy_orders;

        (* Clear buy order tracking; all buys are being cancelled. *)
        state.last_buy_order_id <- None;
        state.last_buy_order_price <- None;
        (* inflight_cancel_buy is set by push_order for each cancel. *)


        (* Update cycle counter. *)
        state.last_cycle <- cycle
      end else if effective_buy_count = 0 && not buy_order_pending then begin
        (* No open buy: place sell + buy pair.
           On buy fill, the existing sell's InFlightOrders key is still live,
           which would block the new sell via duplicate detection.
           Force-clear the sell key so a fresh sell is always placed alongside the new buy. *)
        let sell_price = calculate_grid_price bid_price grid_interval true asset.symbol asset.exchange in
        let buy_price = calculate_grid_price ask_price grid_interval false asset.symbol asset.exchange in

        let buy_cooldown_key = "place_Buy" in
        let is_buy_on_cooldown = Hashtbl.mem state.amend_cooldowns buy_cooldown_key in

        (* Place sell order first; sell and buy are always paired.
           asset_low blocks entry to this entire branch at the top level.
           Always attempt the sell regardless of local balance, as cached balance
           may be stale after a buy fill. If the exchange rejects, asset_low
           is set and will not clear until (asset_bal - reserved_base) >= needed,
           protecting accumulated base asset. *)
        (match asset_balance with
         | Some asset_bal when not state.inflight_sell ->
             let (sell_qty, is_accumulation_sell) =
               if ecfg.use_accumulation_sells then begin
                 let rounded_sell = round_qty (qty *. sell_mult) asset.symbol asset.exchange in
                 let rounding_diff = qty -. rounded_sell in
                 let required_profit = rounding_diff *. sell_price +. asset.accumulation_buffer in
                 if required_profit > 0.0 && state.accumulated_profit >= required_profit then
                   (rounded_sell, true)
                 else
                   (qty, false)  (* 1:1 sell until profit covers rounding cost. *)
               end else if ecfg.sell_uses_mult then
                 (qty *. sell_mult, false)
               else
                 (qty, false)
             in
             (* Proactive reserved_base guard: prevent selling accumulated base.
                Available balance must exclude:
                1. reserved_base: accumulated base that must not be sold.
                2. qty locked in open sell orders on the exchange. *)
             let balance_ok =
               if ecfg.use_reserved_base_guard then
                 let locked_in_sells = List.fold_left (fun acc (_, _, qty) -> acc +. qty) 0.0 !sell_orders in
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
             if sell_qty = 0.0 && is_accumulation_sell then begin
                (* Whole-share retention: profit covers the cost of a full share.
                   Skip the sell order and retain the share as reserved_base.
                   The reserved_base guard will prevent this share from being sold. *)
                let required_profit = qty *. sell_price +. asset.accumulation_buffer in
                state.accumulated_profit <- state.accumulated_profit -. required_profit;
                state.reserved_base <- state.reserved_base +. qty;
                state.persistence_dirty <- true;
                Logging.info_f ~section
                  "Retained full share of %s (profit %.4f covered cost %.4f, reserved_base now %.0f)"
                  asset.symbol (state.accumulated_profit +. required_profit) required_profit state.reserved_base
              end else if balance_ok then begin
                let sell_order = create_order asset.symbol Sell sell_qty (Some sell_price) true asset.exchange in
                if push_order ~now sell_order then begin
                   (* Commit accumulation state after push_order succeeds.
                      Persistence is deferred: set dirty flag for the caller
                      (domain_spawner) to flush outside the hotloop. *)
                   if is_accumulation_sell then begin
                     let rounded_sell = sell_qty in
                     let rounding_diff = qty -. rounded_sell in
                     let required_profit = rounding_diff *. sell_price +. asset.accumulation_buffer in
                     state.accumulated_profit <- state.accumulated_profit -. required_profit;
                     let base_increment = qty -. rounded_sell in
                     state.reserved_base <- state.reserved_base +. base_increment;
                     state.persistence_dirty <- true;
                     Logging.info_f ~section
                       "Accumulation sell for %s: %.8f (sell_mult, profit %.4f covered cost %.4f, reserved_base now %.8f)"
                       asset.symbol rounded_sell (state.accumulated_profit +. required_profit) required_profit state.reserved_base
                   end;
                  Logging.info_f ~section "Placed sell order for %s: %.8f @ %.4f"
                    asset.symbol sell_qty sell_price
                end
              end
         | _ -> ()
        );

        (* Place buy order if quote balance is available. *)
        (match quote_balance with
         | Some _ when is_buy_on_cooldown ->
             Logging.debug_f ~section "Skipping buy order placement for %s due to rate limit cooldown" asset.symbol
         | Some _ when state.inflight_buy ->
             Logging.debug_f ~section "Skipping buy order placement for %s: inflight buy=%B" asset.symbol state.inflight_buy
         | Some _quote_bal ->
             (* Compute available balances net of open order commitments *)
             let available_quote_balance = match quote_balance with
               | Some total_balance ->
                   (* Deduct value locked in open buys *)
                   let locked_in_buys = List.fold_left (fun acc (_, price) -> acc +. (price *. state.grid_qty)) 0.0 !buy_orders in
                   Some (total_balance -. locked_in_buys)
               | None -> None
             in
             let balance_ok = match available_quote_balance with
               | Some avail -> avail >= (buy_price *. qty)
               | None -> true
             in
             if balance_ok then begin
               let order = create_order asset.symbol Buy qty (Some buy_price) true asset.exchange in
               if push_order ~now order then begin
                 state.last_buy_order_price <- Some buy_price;
                 Logging.info_f ~section "Placed buy order for %s: %.8f @ %.4f"
                   asset.symbol qty buy_price;

                 (* Enforce exactly 2x grid_interval spacing. *)
                 let all_sells = (("new_sell", sell_price, 0.0) :: state.open_sell_orders) in
                 let closest_sell = List.fold_left (fun acc (_, sp, _) ->
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
               end
             end else begin
                (* Local balance appears insufficient but still attempt the buy,
                   letting the exchange be the arbiter. The rejection handler
                   (handle_order_rejected) sets capital_low when the exchange
                   responds with "EOrder:Insufficient funds". Relying solely on
                   local calculations causes permanent stalls when reservations
                   are stale or balance data lags behind exchange state (e.g. a
                   sell fills and frees capital but total_reserved has not caught
                   up). Apply 2s cooldown to prevent spam while waiting for the
                   ack or rejection. *)
                let cooldown_key = "place_Buy" in
                if not (Hashtbl.mem state.amend_cooldowns cooldown_key) then begin
                  Logging.warn_f ~section "Local balance low for %s buy (need %.2f, available %.2f) - attempting anyway, exchange will reject if truly insufficient"
                    asset.symbol quote_needed (Option.value available_quote_balance ~default:0.0);
                  Hashtbl.replace state.amend_cooldowns cooldown_key (now +. 2.0);
                  (* Attempt without a reservation; exchange is the final gatekeeper. *)
                  let order = create_order asset.symbol Buy qty (Some buy_price) true asset.exchange in
                  if push_order ~now order then
                    state.last_buy_order_price <- Some buy_price
                end
             end
         | None ->
             Logging.warn_f ~section "No quote balance data available for %s buy order"
               asset.symbol
        );
        state.last_cycle <- cycle
      end else if effective_buy_count > 0 then begin
        (* Case 2: open buy exists; check sell orders for spacing enforcement. *)
        (* Combine active and pending sell orders for spacing calculation. *)
        let closest_sell_order = ref None in
        let update_closest oid price =
          match !closest_sell_order with
          | None -> closest_sell_order := Some (oid, price)
          | Some (_, best_price) ->
              if price < best_price then closest_sell_order := Some (oid, price)
        in
        
        List.iter (fun (oid, price, _) -> update_closest oid price) state.open_sell_orders;
        List.iter (fun (oid, side, price, _) -> 
          if side = Sell then update_closest oid price
        ) state.pending_orders;
        
        let closest_sell_order_val = !closest_sell_order in
        if closest_sell_order_val <> None then begin
          match closest_sell_order_val, state.last_buy_order_price, state.last_buy_order_id with
          | Some (_sell_order_id, sell_price), Some current_buy_price, Some buy_order_id ->
              (* Calculate distance from buy to sell as absolute dollar amount. *)
              let distance = sell_price -. current_buy_price in
              (* Calculate 2x grid_interval as absolute dollar amount from current price. *)
              let double_grid_interval = bid_price *. (2.0 *. grid_interval /. 100.0) in

              (* Compute exact 2x target from closest sell price. *)
              let exact_target = round_price (sell_price -. double_grid_interval) asset.symbol asset.exchange in
              
              if distance > double_grid_interval then begin
                (* Distance > 2x: trail buy upward to maintain grid_interval below price. *)
                let proposed_buy_price = calculate_grid_price ask_price grid_interval false asset.symbol asset.exchange in
                
                (* Only trail upward; never move buy order down. *)
                if proposed_buy_price > current_buy_price then begin
                  let proposed_distance = sell_price -. proposed_buy_price in
                  
                  (* Determine target: achieve 2x if possible, otherwise trail to grid_interval below. *)
                  let target_buy_price = 
                    if proposed_distance <= double_grid_interval then
                      (* Proposed position respects 2x; use exact 2x target. *)
                      exact_target
                    else
                      (* Proposed position still exceeds 2x; trail to grid_interval below price. *)
                      proposed_buy_price
                  in
                  
                  let min_move_threshold = get_min_move_threshold bid_price grid_interval asset.symbol asset.exchange in
                  let current_buy_price_rounded = round_price current_buy_price asset.symbol asset.exchange in
                  let price_diff_rounded = round_price (abs_float (target_buy_price -. current_buy_price_rounded)) asset.symbol asset.exchange in

                  if amend_allowed ~state ~order_id:buy_order_id ~target_price:target_buy_price
                       ~current_price_rounded:current_buy_price_rounded
                       ~price_diff:price_diff_rounded ~min_move_threshold then begin
                    (match quote_balance with
                     | Some quote_bal when can_place_buy_order qty quote_bal quote_needed ->
                         let order = create_amend_order buy_order_id asset.symbol Buy qty (Some target_buy_price) true Grid asset.exchange in
                         ignore (push_order ~now order);
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
                  (* Price has fallen; hold buy order steady, do not trail down. *)
                  Logging.debug_f ~section "Holding buy %s for %s @ %.2f (price fell, not trailing down)"
                    buy_order_id asset.symbol current_buy_price
                end
              end else begin
                (* Distance <= 2x: enforce exact 2x spacing. *)
                let exact_target_rounded = exact_target in
                let current_buy_price_rounded = round_price current_buy_price asset.symbol asset.exchange in
                let price_diff_rounded = round_price (abs_float (exact_target_rounded -. current_buy_price_rounded)) asset.symbol asset.exchange in
                let min_move_threshold = get_min_move_threshold bid_price grid_interval asset.symbol asset.exchange in

                if amend_allowed ~state ~order_id:buy_order_id ~target_price:exact_target_rounded
                     ~current_price_rounded:current_buy_price_rounded
                     ~price_diff:price_diff_rounded ~min_move_threshold then begin
                  (match quote_balance with
                   | Some quote_bal when can_place_buy_order qty quote_bal quote_needed ->
                       let order = create_amend_order buy_order_id asset.symbol Buy qty (Some exact_target) true Grid asset.exchange in
                       ignore (push_order ~now order);
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
          (* No sell orders exist but buy is active.
             Trail buy without sell anchors; sells are only placed
             in the effective_buy_count=0 branch after a buy fills. *)
           match state.last_buy_order_price, state.last_buy_order_id with
           | Some current_buy_price, Some buy_order_id ->
               let target_buy_price = calculate_grid_price ask_price grid_interval false asset.symbol asset.exchange in
                            (* Only trail upward; amend buy if target is higher than current. *)
               if target_buy_price > current_buy_price then begin
                 let min_move_threshold = get_min_move_threshold ask_price grid_interval asset.symbol asset.exchange in
                 let current_buy_price_rounded = round_price current_buy_price asset.symbol asset.exchange in
                 let price_diff_rounded = round_price (abs_float (target_buy_price -. current_buy_price_rounded)) asset.symbol asset.exchange in

                if amend_allowed ~state ~order_id:buy_order_id ~target_price:target_buy_price
                     ~current_price_rounded:current_buy_price_rounded
                     ~price_diff:price_diff_rounded ~min_move_threshold then begin
                  (match quote_balance with
                   | Some quote_bal when can_place_buy_order qty quote_bal quote_needed ->
                       let order = create_amend_order buy_order_id asset.symbol Buy qty (Some target_buy_price) true Grid asset.exchange in
                       ignore (push_order ~now order);
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
        (* Update cycle counter. *)
        state.last_cycle <- cycle
      end else begin
        (* No action required for remaining cases. *)
        state.last_cycle <- cycle
      end
  end  (* end: is_stale else begin *)
   end) (* end: if not capital_low then begin; close Fun.protect *)


(** Flushes deferred accumulation state to disk when the dirty flag is set.
    Called by the domain_spawner after execute_strategy returns, keeping
    blocking file I/O out of the strategy hotloop. Acquires state.mutex
    to read and clear the flag atomically with the save. *)
let flush_persistence asset_symbol =
  let state = get_strategy_state asset_symbol in
  (* Fast non-locking check to skip mutex acquisition on non-dirty cycles. *)
  if state.persistence_dirty then begin
    let snapshot_to_save =
      Mutex.lock state.mutex;
      Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
        if state.persistence_dirty then begin
          state.persistence_dirty <- false;
          Some (
            state.reserved_base,
            state.accumulated_profit,
            state.last_fill_oid,
            state.last_buy_fill_price,
            state.last_sell_fill_price
          )
        end else None)
    in
    match snapshot_to_save with
    | Some (reserved_base, accumulated_profit, last_fill_oid, last_buy_fill_price, last_sell_fill_price) ->
        Hyperliquid.State_persistence.save_async ~symbol:asset_symbol
          ~reserved_base
          ~accumulated_profit
          ?last_fill_oid
          ?last_buy_fill_price
          ?last_sell_fill_price ()
    | None -> ()
  end


(** Handles order placement acknowledgment. Updates pending and tracking state. *)
let handle_order_acknowledged asset_symbol order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
  (* Remove from pending orders. Only remove placement-prefix entries for this side.
     pending_amend_* entries are owned by the amend lifecycle
     (handle_order_amended / handle_order_amendment_skipped / handle_order_amendment_failed)
     and must not be evicted here. On Hyperliquid (in-place modify, same order ID returned
     after an amend), the placed order ID can collide with an in-flight amend's target ID;
     removing that amend entry prematurely causes handle_order_amended to miss the pending
     record and log "Ignoring amendment for untracked buy order". *)
  state.pending_orders <- List.filter (fun (pending_id, s, p, _) ->
    (* 1. Match regular placements by side (any pending_buy/pending_sell for that side). *)
    let is_placement_prefix = String.starts_with ~prefix:"pending_buy_" pending_id ||
                              String.starts_with ~prefix:"pending_sell_" pending_id in
    let matches_side_placement = is_placement_prefix && s = side in
    
    (* 2. Fallback for tests or legacy IDs that do not use prefixes.
          Explicitly exclude pending_amend_* so they are never touched here. *)
    let matches_fallback = not (String.starts_with ~prefix:"pending_" pending_id) &&
                          s = side && abs_float (p -. price) < 0.01 in
    
    not (matches_side_placement || matches_fallback)
  ) state.pending_orders;

  (* InFlightOrders key is intentionally not removed here.
     The guard stays alive while the order is active, preventing duplicate placement.
     Cleaned up in handle_order_cancelled/filled or by stale timeout. *)

  (* Update buy order tracking on buy acknowledgment. *)
  (match side with
   | Buy ->
    state.last_buy_order_id <- Some order_id;
    state.last_buy_order_price <- Some price;
    state.inflight_buy <- false;
    (* Amendment window is over; exchange has acknowledged the new order. *)
    state.inflight_amend_buy <- false;
    Logging.debug_f ~section "Updated buy order ID and price tracking: %s @ %.2f for %s" order_id price asset_symbol
   | Sell ->
    state.inflight_sell <- false;
    state.recently_injected_sells <- (order_id, price, Unix.gettimeofday ()) :: state.recently_injected_sells);

  Logging.debug_f ~section "Order acknowledged and removed from pending: %s %s @ %.2f for %s"
    order_id (string_of_order_side side) price asset_symbol
  )

(** Handles order placement failure. Clears in-flight trackers so strategy can retry. *)
let handle_order_failed asset_symbol side reason =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* Remove from pending orders. *)
    state.pending_orders <- List.filter (fun (_, s, _, _) -> s <> side) state.pending_orders;
    
    (* Clear inflight flags. *)
    (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);

    (* Release this asset's quote reservation on buy failures. *)
    (match side with Buy -> set_asset_reserved_quote state 0.0 | Sell -> ());

    (* Clear global in-flight trackers. *)
    let duplicate_key = generate_side_duplicate_key asset_symbol side in
    ignore (InFlightOrders.remove_in_flight_order duplicate_key);
    
    let lower_reason = String.lowercase_ascii reason in

    let is_rate_limit = contains_fragment lower_reason "too many cumulative requests" || contains_fragment lower_reason "rate limit" in
    let is_insufficient_balance = contains_fragment lower_reason "insufficient funds"
      || contains_fragment lower_reason "insufficient spot balance" in
    let cooldown = if is_rate_limit then 10.0 else 2.0 in

    (* Set balance flags when exchange reports insufficient balance.
       capital_low (buy side) and asset_low (sell side) halt further placement
       until balance recovers (event-driven, no timers). *)
    (match side with
     | Buy when is_insufficient_balance ->
         if not state.capital_low then begin
           state.capital_low <- true;
           state.capital_low_logged <- true;
           Logging.warn_f ~section "Exchange rejected buy for %s with insufficient funds - setting capital_low flag"
             asset_symbol
         end
     | Sell when is_insufficient_balance ->
          let ecfg = get_exchange_config state.exchange_id in
          if ecfg.sell_failure_sets_asset_low then begin
            if not state.asset_low then begin
              state.asset_low <- true;
              Logging.warn_f ~section "Exchange rejected sell for %s with insufficient balance - setting asset_low flag"
                asset_symbol
            end
          end else
            (* Sell failures are fire-and-forget. Do not set asset_low;
               the sell fails and the strategy continues placing the buy. *)
            Logging.warn_f ~section "Exchange rejected sell for %s with insufficient balance (ignored, sell is fire-and-forget)"
              asset_symbol
     | _ -> ());

    (* Apply timer cooldown for buy-side rate limits only; sell side uses asset_low flag. *)
    let now = Unix.time () in
    (match side with
     | Buy ->
         Hashtbl.replace state.amend_cooldowns "place_Buy" (now +. cooldown)
     | Sell -> ());

    Logging.warn_f ~section "Order failed for %s (%s): %s. Cleared in-flight tracker."
      asset_symbol (string_of_order_side side) reason
  )

(** Handles order rejection. Removes from pending and clears trackers for re-placement. *)
let handle_order_rejected asset_symbol side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
  (* Remove from pending orders. Match by side for regular placements or fallback for tests. *)
  state.pending_orders <- List.filter (fun (pending_id, s, p, _) ->
    let is_placement_prefix = String.starts_with ~prefix:"pending_buy_" pending_id ||
                              String.starts_with ~prefix:"pending_sell_" pending_id in
    let matches_side_placement = is_placement_prefix && s = side in
    
    (* Rejections for amends match by ID or price fallback. *)
    let matches_amend_prefix = String.starts_with ~prefix:"pending_amend_" pending_id in
    
    let matches_fallback = (not (String.starts_with ~prefix:"pending_" pending_id) || matches_amend_prefix) &&
                          s = side && abs_float (p -. price) < 0.01 in
    
    not (matches_side_placement || matches_fallback)
  ) state.pending_orders;

  (* Clear inflight flags. *)
  (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);

  (* Clean up global trackers to allow immediate re-placement after rejection. *)
  let duplicate_key = generate_side_duplicate_key asset_symbol side in
  ignore (InFlightOrders.remove_in_flight_order duplicate_key);

  (* Apply short cooldown for buy side only; sell side uses asset_low flag.
     Do not overwrite a longer cooldown already set by handle_order_failed. *)
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

  (* Buy rejection signals no buy order exists; strategy re-evaluates on next cycle. *)

(** Handles order fill. Fully clears tracking, pending amends, and computes profit. *)
let handle_order_filled asset_symbol order_id side ~fill_price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* 1. Remove any pending amend matching this order ID. *)
    state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
      not (String.starts_with ~prefix:"pending_amend_" pending_id &&
           String.length pending_id > 14 &&
           String.sub pending_id 14 (String.length pending_id - 14) = order_id)
    ) state.pending_orders;

    (* 2. Look up sell price before filtering; needed for profit calculation.
       Use exec event fill_price as primary source; fall back to open_sell_orders
       for backward compat. Fixes Hyperliquid amends where webData2 never
       surfaces the replacement sell order. *)
    let sell_fill_price = match List.find_opt (fun (id, _, _) -> id = order_id) state.open_sell_orders with
      | Some (_, p, _) -> p
      | None -> fill_price  (* exec event fill price; always available *)
    in

    (* 3. Remove from sell orders tracking if this was a sell order. *)
    state.open_sell_orders <- List.filter (fun (sell_id, _, _) ->
      sell_id <> order_id
    ) state.open_sell_orders;

    (* 4. Clear buy order tracking only on buy fills.
       A sell fill must not clear last_buy_order_id because the buy is still
       live and only needs spacing adjustment, not a fresh placement cycle.
       Failing to gate on [side = Buy] caused sell fills to spuriously clear
       buy tracking, setting effective_buy_count=0 on the next cycle and
       triggering a redundant buy placement alongside the still-open original. *)
    let was_tracked_buy = match side, state.last_buy_order_id with
      | Buy, Some id when id = order_id -> true
      | _ -> false
    in
    if was_tracked_buy then begin
      (* Record actual buy fill price for profit calculation on matching sell fill. *)
      state.last_buy_fill_price <- Some fill_price;
      (* Clear last_sell_fill_price; next sell is the first after this buy. *)
      state.last_sell_fill_price <- None;
      state.last_buy_order_id <- None;
      state.last_buy_order_price <- None;
      (* Mark dirty so flush_persistence writes buy fill price to disk.
         Without this, sell fills after restart have no buy_price reference
         and silently skip profit calculation. *)
      if state.exchange_id = "hyperliquid" then
        state.persistence_dirty <- true;
      (* Credit anticipated base so the sell guard sees incoming asset before
         the balance feed catches up (exec feed is faster than balance feed). *)
      if state.grid_qty > 0.0 then begin
        state.anticipated_base_credit <- state.anticipated_base_credit +. state.grid_qty;
        Logging.info_f ~section "Anticipated base credit for %s: +%.8f (total: %.8f) from buy fill %s"
          asset_symbol state.grid_qty state.anticipated_base_credit order_id
      end;
      Logging.debug_f ~section "Filled buy order %s removed from tracking for %s" order_id asset_symbol
    end;

    (* 5. Compute realized net profit on sell fills (discrete sizing accumulator).
       profit = (sell_price - buy_price) * qty - maker fees on both legs.
       During startup_replay, skip profit calculation for fills already persisted
       (OID <= last_fill_oid). Tracking state (buy/sell IDs, inflight flags) is
       still updated above so the strategy enters the correct operational state.
       Track highest OID seen during startup for new-strategy bootstrapping. *)
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
           (* OIDs are monotonically increasing integers on Hyperliquid. *)
           (try Int64.compare (Int64.of_string order_id) (Int64.of_string persisted_oid) <= 0
            with _ -> false)
       | None -> true  (* New strategy: no persisted OID; skip all fills during startup replay. *))
    in
    if skip_profit_calc then
      Logging.debug_f ~section "Skipping startup replay fill %s for %s (already persisted, last_fill_oid=%s)"
        order_id asset_symbol (Option.value state.last_fill_oid ~default:"none")
    else
    (match side with
     | Sell ->
          (* Determine cost basis: use last_sell_fill_price for consecutive sells
             (ladder steps), otherwise use last_buy_fill_price (first sell after buy). *)
          let cost_basis = match state.last_sell_fill_price with
            | Some prev_sell when prev_sell > 0.0 -> Some prev_sell
            | _ -> state.last_buy_fill_price
          in
          (match cost_basis with
           | Some base_price when sell_fill_price > base_price ->
              let qty = state.grid_qty in
              let gross = (sell_fill_price -. base_price) *. qty in
              (* Hyperliquid spot: buy fee is deducted from received base asset
                 (no quote impact); sell fee is deducted from received quote.
                 Only the sell-side fee affects USDC balance growth.
                 Kraken: both fees are deducted from quote currency. *)
              let fees =
                if state.exchange_id = "hyperliquid" then
                  sell_fill_price *. qty *. state.maker_fee
                else
                  (sell_fill_price *. qty *. state.maker_fee)
                  +. (base_price *. qty *. state.maker_fee)
              in
              let net_profit = gross -. fees in
              if net_profit > 0.0 then begin
                state.accumulated_profit <- state.accumulated_profit +. net_profit;
                (* Only advance last_fill_oid forward; never regress to a lower OID.
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
                (* Persist so profit survives container restarts.
                   Also persist last_buy_fill_price so the next
                   sell after restart still has a buy reference. *)
                if state.exchange_id = "hyperliquid" then
                  state.persistence_dirty <- true;
                Logging.debug_f ~section "Realized profit for %s: %.6f (gross %.6f - fees %.6f, sell@%.4f base@%.4f x %.8f), accumulated: %.6f"
                  asset_symbol net_profit gross fees sell_fill_price base_price qty state.accumulated_profit
              end;
              (* Always record sell fill price for consecutive sell detection. *)
              state.last_sell_fill_price <- Some sell_fill_price
          | _ ->
              (* Sell at or below cost basis; still record for consecutive sell tracking. *)
              state.last_sell_fill_price <- Some sell_fill_price)
     | Buy -> ());

    (* 6. Clear inflight flags, reservation, and global placement trackers for immediate re-placement. *)
    (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);
    (* Release quote reservation when buy fills (funds transferred to base asset). *)
    (match side with Buy -> set_asset_reserved_quote state 0.0 | Sell -> ());
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol side));

    (* 7. On buy fill, clear the Sell side's InFlightOrders guard.
       The strategy places buy+sell together; the previous sell's guard
       would block the new sell via duplicate detection. *)
    if side = Buy then begin
      ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol Sell));
      state.inflight_sell <- false
    end;

    Logging.debug_f ~section "Order FILLED and cleaned up explicitly: %s for %s"
      order_id asset_symbol
  )

(** Handles order cancellation. Removes from pending and tracked orders. *)
let handle_order_cancelled asset_symbol order_id side =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* Order channel confirmed cancellation (authoritative source of truth).
       1. Remove any pending operations for this order. *)
    let original_pending_count = List.length state.pending_orders in
    state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
      (* Match pending_id equal to order_id, or pending_amend_ entries targeting order_id. *)
      let matches = pending_id = order_id || 
                   (String.starts_with ~prefix:"pending_amend_" pending_id && 
                    String.length pending_id > 14 && 
                    String.sub pending_id 14 (String.length pending_id - 14) = order_id) in
      not matches
    ) state.pending_orders;
    let removed_pending = original_pending_count - List.length state.pending_orders in

    (* Clear amend cooldowns if any exist. *)
    Hashtbl.remove state.amend_cooldowns order_id;
    ignore (InFlightAmendments.remove_in_flight_amendment order_id);

    (* 2. Clear tracking conditionally based on side. *)
    let cancelled_side = side in
    
    let was_tracked_buy = match state.last_buy_order_id with
      | Some id when id = order_id -> true
      | _ -> false
    in
    
    if was_tracked_buy then begin
         state.last_buy_order_id <- None;
         state.last_buy_order_price <- None;
         Logging.debug_f ~section "Cancelled buy order %s removed from tracking for %s" order_id asset_symbol
    end;

    (* Clear inflight_cancel_buy now that the order channel has confirmed; unblocks sync. *)
    if cancelled_side = Buy then begin
      state.inflight_cancel_buy <- false;
      (* Also clear amend gate if the order was cancelled mid-amendment. *)
      state.inflight_amend_buy <- false;
      (* Flush buy-side placement cooldown so strategy can place immediately. *)
      Hashtbl.remove state.amend_cooldowns "place_Buy"
    end;
    
    (* Remove from sell orders list. *)
    let original_sell_count = List.length state.open_sell_orders in
    state.open_sell_orders <- List.filter (fun (sell_id, _, _) ->
      sell_id <> order_id
    ) state.open_sell_orders;
    let removed_sell = original_sell_count - List.length state.open_sell_orders in

    (* Clear inflight flags and global placement trackers for immediate re-placement. *)
    (match cancelled_side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol cancelled_side));
    
    Logging.debug_f ~section "Order cancelled and cleaned up: %s for %s (removed %d pending, %d sell)"
      order_id asset_symbol removed_pending removed_sell
  )

(** Handles cancel-replace order amendment. Swaps old ID for new ID in tracking. *)
let handle_order_amended asset_symbol old_order_id new_order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* 1. Remove the pending amend entry using old_order_id or new_order_id. *)
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         (String.sub pending_id 14 (String.length pending_id - 14) = old_order_id ||
                          String.sub pending_id 14 (String.length pending_id - 14) = new_order_id) in
      not matches_amend
    ) state.pending_orders;

    (* 2. Update the active order tracking; swap old ID to new ID. *)
    (match side with
     | Buy ->
          (match state.last_buy_order_id with
           | Some target_id when target_id = old_order_id ->
               state.last_buy_order_id <- Some new_order_id;
               state.last_buy_order_price <- Some price;
               if old_order_id = new_order_id then
                 (* In-place amendment (Kraken): same order ID, price updated only. *)
                 Logging.debug_f ~section "Updated buy price in tracking: %s @ %.2f for %s"
                   old_order_id price asset_symbol
               else
                 (* Cancel-replace amendment (Hyperliquid): new order ID assigned. *)
                 Logging.info_f ~section "Amended buy order ID in tracking: %s -> %s @ %.2f for %s"
                   old_order_id new_order_id price asset_symbol
             | _ -> 
                (* If the old order was not tracked, do not install the new order
                   as the tracked buy. Doing so resurrects ghost orders from stale
                   orderUpdate websocket events, causing duplicate/ghost order
                   detection when combined with fresh placements. *)
                Logging.debug_f ~section "Ignoring amendment for untracked buy order %s -> %s for %s"
                  old_order_id new_order_id asset_symbol)
     | Sell ->
         let original_sell_count = List.length state.open_sell_orders in
         let old_qty = match List.find_opt (fun (id, _, _) -> id = old_order_id) state.open_sell_orders with
           | Some (_, _, q) -> q | None -> state.grid_qty in
         state.open_sell_orders <- (new_order_id, price, old_qty) :: 
            List.filter (fun (sell_id, _, _) -> sell_id <> old_order_id) state.open_sell_orders;
         state.recently_injected_sells <- (new_order_id, price, Unix.gettimeofday ()) :: state.recently_injected_sells;
         if List.length state.open_sell_orders = original_sell_count then
           Logging.info_f ~section "Amended sell order ID in tracking: %s -> %s @ %.2f for %s" 
             old_order_id new_order_id price asset_symbol
         else
           Logging.debug_f ~section "Set sell order ID via amend: %s @ %.2f for %s" 
             new_order_id price asset_symbol);
             
    (* Apply a short cooldown to prevent amending the newly amended order
       before the exchange's WebSocket open-orders cache catches up.
       Prevents race-condition duplicate amends on Hyperliquid. *)
    let now = Unix.time () in
    (* In-place modify (same ID): short cooldown as re-amendment throttle.
       Cancel-replace (different IDs): longer cooldown to cover WS data lag. *)
    let cooldown = if old_order_id = new_order_id then 2.0 else 10.0 in
    Hashtbl.replace state.amend_cooldowns old_order_id (now +. cooldown);
    Hashtbl.replace state.amend_cooldowns new_order_id (now +. cooldown);

    (* Amendment REST round-trip complete; clear the inflight gate.
       handle_order_acknowledged also clears it when the WS ack arrives,
       but clearing here lifts the gate as soon as the REST response is
       processed, since the inject->remove window is already over. *)
    if side = Buy then
      state.inflight_amend_buy <- false;

    Logging.debug_f ~section "Order amended and tracking updated: %s %s -> %s @ %.2f for %s"
      (string_of_order_side side) old_order_id new_order_id price asset_symbol
  )

(** Handles skipped order amendment. Clears pending lock without swapping IDs. *)
let handle_order_amendment_skipped asset_symbol order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* 1. Remove the pending amend entry using order_id. *)
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         String.sub pending_id 14 (String.length pending_id - 14) = order_id in
      not matches_amend
    ) state.pending_orders;
    
    Logging.debug_f ~section "Order amendment skipped for identical price, removed tracking guard: %s %s @ %.2f for %s"
      (string_of_order_side side) order_id price asset_symbol
  )

(** Handles order amendment failure. Clears tracking so replacement can be placed. *)
let handle_order_amendment_failed asset_symbol order_id side reason =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* 1. Remove the pending amend entry. *)
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         String.length pending_id > 14 &&
                         String.sub pending_id 14 (String.length pending_id - 14) = order_id in
      not matches_amend
    ) state.pending_orders;

    (* 2. Determine whether to clear buy tracking.
       For Hyperliquid, the REST cancel-replace structure means the open order
       was untouched on book even if the amend request failed. Apply a cooldown
       instead of dropping tracking, which would induce a duplicate placement loop. *)
    let lower_reason = String.lowercase_ascii reason in

    
    let is_cache_miss = contains_fragment lower_reason "order not found" || contains_fragment lower_reason "not found for amendment" || contains_fragment lower_reason "unknown order" in
    let is_cannot_modify = contains_fragment lower_reason "cannot modify canceled or filled" in
    let is_margin_error = contains_fragment lower_reason "insufficient" || contains_fragment lower_reason "margin" in
    let is_rate_limit = contains_fragment lower_reason "too many cumulative requests" || contains_fragment lower_reason "rate limit" in

    let cooldown_duration = 
      if is_rate_limit then 10.0
      else if is_cache_miss || is_cannot_modify || is_margin_error then 0.5
      else 2.0
    in

    (* Apply cooldown to prevent spamming failed amends. *)
    let now = Unix.time () in
    Hashtbl.replace state.amend_cooldowns order_id (now +. cooldown_duration);

    (* If the order is definitively gone (canceled, filled, or margin error),
       clear tracking so the grid can place a replacement. *)
    let is_order_gone = is_cache_miss || is_cannot_modify || is_margin_error in
    
    if is_order_gone then begin
      (* Send explicit cancel in case the order is wedged in the exchange's cache. *)
      let cancel_order = create_cancel_order order_id asset_symbol Grid state.exchange_id in
      ignore (push_order ~now:(Unix.time ()) cancel_order);

      (* Mark cancel in flight so the sync block does not reinstall this order. *)
      if side = Buy then
        state.inflight_cancel_buy <- true;
      (match side with
       | Buy ->
           (match state.last_buy_order_id with
            | Some target_id when target_id = order_id ->
                state.last_buy_order_id <- None;
                state.last_buy_order_price <- None;
                (* Clear place_Buy cooldown so strategy can place a new buy immediately
                   instead of waiting for a stale cooldown from the multi-buy cancel branch. *)
                Hashtbl.remove state.amend_cooldowns "place_Buy";
                Logging.info_f ~section "Amendment failed for buy order %s: %s. Order is gone, cleared tracking." order_id reason
            | _ ->
                (* Order ID does not match tracking; already cleared by a previous call.
                   Clear cooldowns to unblock placement. *)
                Hashtbl.remove state.amend_cooldowns "place_Buy";
                Logging.debug_f ~section "Amendment failed for untracked buy order %s: %s. Cleared place_Buy cooldown." order_id reason)
       | Sell ->
           let original_sell_count = List.length state.open_sell_orders in
           state.open_sell_orders <- List.filter (fun (sell_id, _, _) -> sell_id <> order_id) state.open_sell_orders;
           if List.length state.open_sell_orders < original_sell_count then
             Logging.info_f ~section "Amendment failed for sell order %s: %s. Order is gone, cleared tracking." order_id reason)
    end else begin
      (* Log tracking action taken. Do not auto-clear tracking on other
         amendment failures to prevent replacement loops. *)
      (match side with
       | Buy ->
           (match state.last_buy_order_id with
            | Some target_id when target_id = order_id ->
                Logging.info_f ~section "Amendment failed for buy order %s: %s. Applying %.1fs cooldown (keeping tracking)." order_id reason cooldown_duration
            | _ -> ())
       | Sell ->
           Logging.info_f ~section "Amendment failed for sell order %s: %s. Applying %.1fs cooldown (keeping tracking)." order_id reason cooldown_duration);
    end;
           
    (* Clear global in-flight trackers to prevent deadlock. *)
    ignore (InFlightOrders.remove_in_flight_order (generate_side_duplicate_key asset_symbol side));
  )

(** No-op shim: pending_cancellations removed; cancel state is now tracked via
    inflight_cancel_buy. The supervisor still calls this after every cancel
    response, so we keep the signature to satisfy the Strategy interface. *)
let cleanup_pending_cancellation _asset_symbol _order_id = ()


(** Reads up to [max_orders] orders from the ringbuffer for processing. *)
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
           | None -> count := max_orders  (* Exit loop when buffer is empty. *)
         done;
         List.rev !orders)
  in
  orders

(** Initializes the strategy module. *)
let init () =
  Logging.debug_f ~section "Suicide Grid strategy initialized with order buffer size 4096";
  Random.self_init ()

(** Strategy module interface. *)
module Strategy = struct
  type config = trading_config

  (** Cleans up strategy state for a symbol when domain stops. *)
  let cleanup_strategy_state symbol =
    Mutex.lock strategy_states_mutex;
    (match Hashtbl.find_opt strategy_states symbol with
     | Some _ ->
         Hashtbl.remove strategy_states symbol;
         Logging.debug_f ~section "Removed strategy state for %s" symbol
     | None -> ());
    Mutex.unlock strategy_states_mutex

  let execute = execute_strategy
  let flush_persistence = flush_persistence
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

  (** Clears the startup_replay flag so subsequent fills are processed normally.
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
         If state has never been persisted for this symbol (last_fill_oid=None)
         but fill events were seen during startup, establish an anchor point so
         the next restart can resume from a known position. *)
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