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
  merge_preserved_sells = true;
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
  merge_preserved_sells = true;
  check_stale_balance = true;
}

let lighter_config = {
  time_in_force = "GTC";
  track_pending_sells = false;
  use_accumulation_sells = true;
  sell_uses_mult = false;             (* 1:1 sells; accumulation path handles rounding *)
  sell_failure_sets_asset_low = true;
  use_reserved_base_guard = true;     (* Prevent selling accumulated base *)
  asset_low_requires_balance_change = false;
  merge_preserved_sells = true;
  check_stale_balance = false;        (* Balance feed may lag; don't block strategy *)
}

let get_exchange_config exchange =
  if exchange = "hyperliquid" then hyperliquid_config
  else if exchange = "lighter" then lighter_config
  else if exchange = "ibkr" then ibkr_config
  else kraken_config

(** Exchanges that persist accumulation (buy fill price, profit, last_fill_oid) on fills.
    IBKR was missing from this path and never set [persistence_dirty] on buy/sell fills. *)
let[@inline always] persistence_accumulation_exchange id =
  id = "hyperliquid" || id = "lighter" || id = "ibkr"

(** Spot venues: sell-leg maker fee only in realized PnL (not IBKR equities). *)
let[@inline always] hl_like_spot_fee_exchange id =
  id = "hyperliquid" || id = "lighter"

(** IBKR Pro Tiered commission: $0.0035/share, min $0.35/order, max 1% of trade value.
    Returns the USD commission for a single order leg. *)
let ibkr_commission ~qty ~price =
  let per_share_rate = 0.0035 in
  let raw = qty *. per_share_rate in
  let min_fee = 0.35 in
  let max_fee = 0.01 *. qty *. price in
  Float.max min_fee (Float.min raw max_fee)

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
let order_buffer = Strategy_common.LockFreeQueue.create ()

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
  mutable duplicate_key_buy: string;
  mutable duplicate_key_sell: string;
  mutable cached_round_price: float -> float;
  mutable cached_price_increment: float;
  mutable cached_qty_increment: float;
  mutable cached_qty_min: float;
  mutable exchange_reserved_atomic: float Atomic.t option;
  mutex: Mutex.t;  (* per-symbol mutex; prevents concurrent strategy execution *)
}

(** Global registry of per-symbol strategy states. *)
let strategy_states = Atomic.make Strategy_common.StringMap.empty

(** Retrieves or lazily initializes the strategy state for [asset_symbol].
    On first access, loads persisted state (reserved_base, accumulated_profit,
    last_fill_oid, fill prices) from Dio_persistence.State_persistence. *)
let rec get_strategy_state asset_symbol =
  let map = Atomic.get strategy_states in
  match Strategy_common.StringMap.find_opt asset_symbol map with
  | Some state -> state
  | None ->
      (* Load persisted state for this symbol (survives container restarts). *)
      let persisted_reserved_base = Dio_persistence.State_persistence.load_reserved_base ~symbol:asset_symbol in
      let persisted_accumulated_profit = Dio_persistence.State_persistence.load_accumulated_profit ~symbol:asset_symbol in
      let persisted_last_fill_oid = Dio_persistence.State_persistence.load_last_fill_oid ~symbol:asset_symbol in
      let persisted_last_buy_fill_price = Dio_persistence.State_persistence.load_last_buy_fill_price ~symbol:asset_symbol in
      let persisted_last_sell_fill_price = Dio_persistence.State_persistence.load_last_sell_fill_price ~symbol:asset_symbol in
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
        duplicate_key_buy = Printf.sprintf "%s|buy|grid" asset_symbol;
        duplicate_key_sell = Printf.sprintf "%s|sell|grid" asset_symbol;
        cached_round_price = Float.round;
        cached_price_increment = 0.01;
        cached_qty_increment = 0.01;
        cached_qty_min = 1.0;
        exchange_reserved_atomic = None;
        mutex = Mutex.create ();
      } in
      if Atomic.compare_and_set strategy_states map (Strategy_common.StringMap.add asset_symbol new_state map) then
        new_state
      else
        get_strategy_state asset_symbol

(** Tracking total reserved quote per exchange to avoid O(N) strategy_states locking. *)
let total_reserved_by_exchange = Atomic.make (
  List.fold_left (fun acc ex -> Strategy_common.StringMap.add ex (Atomic.make 0.0) acc)
    Strategy_common.StringMap.empty ["kraken"; "hyperliquid"; "lighter"; "ibkr"]
)

(** Gets the cached total reserved quote atomic for [exchange]. *)
let rec get_exchange_reserved_atomic exchange =
  let map = Atomic.get total_reserved_by_exchange in
  match Strategy_common.StringMap.find_opt exchange map with
  | Some a -> a
  | None ->
      let a3 = Atomic.make 0.0 in
      let new_map = Strategy_common.StringMap.add exchange a3 map in
      if Atomic.compare_and_set total_reserved_by_exchange map new_map then
        a3
      else
        get_exchange_reserved_atomic exchange

let get_total_reserved_quote state =
  let a = match state.exchange_reserved_atomic with
    | Some a -> a
    | None -> let atm = get_exchange_reserved_atomic state.exchange_id in state.exchange_reserved_atomic <- Some atm; atm
  in
  Atomic.get a

let rec atomic_add a diff =
  let old_val = Atomic.get a in
  if not (Atomic.compare_and_set a old_val (old_val +. diff)) then
    atomic_add a diff

(** Sets this asset's reserved_quote safely. *)
let set_asset_reserved_quote state v =
  let diff = v -. state.reserved_quote in
  state.reserved_quote <- v;
  if state.exchange_id <> "" then begin
    let a = match state.exchange_reserved_atomic with
      | Some a -> a
      | None -> let atm = get_exchange_reserved_atomic state.exchange_id in state.exchange_reserved_atomic <- Some atm; atm
    in
    atomic_add a diff
  end

(** Atomically checks available quote balance and reserves for a buy if sufficient.
    Returns (balance_ok, available_quote, total_reserved). *)
let atomic_check_and_reserve state quote_bal quote_needed reserve_amount =
  let a = match state.exchange_reserved_atomic with
    | Some a -> a
    | None -> let atm = get_exchange_reserved_atomic state.exchange_id in state.exchange_reserved_atomic <- Some atm; atm
  in
  let diff = reserve_amount -. state.reserved_quote in
  let rec attempt () =
    let total_reserved = Atomic.get a in
    let available = quote_bal -. total_reserved in
    if available >= quote_needed then begin
      if state.exchange_id <> "" then begin
        if Atomic.compare_and_set a total_reserved (total_reserved +. diff) then begin
          state.reserved_quote <- reserve_amount;
          (true, available, total_reserved)
        end else
          attempt ()
      end else begin
        state.reserved_quote <- reserve_amount;
        (true, available, total_reserved)
      end
    end else
      (false, available, total_reserved)
  in
  attempt ()

(** Parses a config string to float with fallback default and warning. *)
let parse_config_float config value_name default exchange symbol =
  try float_of_string config with
  | Failure _ ->
      Logging.warn_f ~section "Invalid %s value '%s' for %s/%s, using default %.4f"
        value_name config exchange symbol default;
      default

(** Rounds price to exchange-mandated precision for the given symbol.
    Uses a per-(symbol,exchange) closure cache to skip module dispatch. *)
let get_round_price_fn symbol exchange =
  let key = symbol ^ "|" ^ exchange in
  match Hashtbl.find_opt _round_price_fn_cache key with
  | Some f -> f
  | None ->
      let f = match get_exchange_module exchange with
        | Some (module Ex : Exchange.S) -> (fun p -> Ex.round_price ~symbol ~price:p)
        | None -> Float.round
      in
      Hashtbl.replace _round_price_fn_cache key f;
      f

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

let venue_lot_qty grid_qty exchange state =
  match exchange with
  | "ibkr" ->
      if grid_qty <= 0.0 then 0.0
      else
        let q = let inv = 1.0 /. state.cached_qty_increment in floor (grid_qty *. inv) /. inv in
        if q > 0.0 then q else state.cached_qty_min
  | "lighter" ->
      if grid_qty <= 0.0 then 0.0
      else
        let q = let inv = 1.0 /. state.cached_qty_increment in floor (grid_qty *. inv) /. inv in
        if q > 0.0 then q else state.cached_qty_min
  | _ -> grid_qty

(** Returns the minimum price delta required to trigger an amendment.
    max(10 * tick_size, 5% of one grid interval). Prevents amendment spam. *)
let get_min_move_threshold price grid_interval_pct state =
  let base_increment = state.cached_price_increment in
  let pct_based = price *. (grid_interval_pct *. 0.05 /. 100.0) in
  max (base_increment *. 10.0) pct_based

(** Computes a grid price one interval above or below [current_price]. *)
let calculate_grid_price current_price grid_interval_pct is_above state =
  let interval = current_price *. (grid_interval_pct /. 100.0) in
  let raw_price = if is_above then current_price +. interval else current_price -. interval in
  state.cached_round_price raw_price

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


let create_place_order dup_key asset_symbol side qty price post_only strategy exchange =
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
    duplicate_key = dup_key;
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
let create_order dup_key asset_symbol side qty price post_only exchange =
  create_place_order dup_key asset_symbol side qty price post_only Grid exchange

(** Pushes an order to the ringbuffer. Returns true on success, false on duplicate or full buffer.
    [now] is a pre-computed Unix.time() timestamp to avoid redundant syscalls.
    [?state] allows callers that already hold the strategy state to pass it
    directly, avoiding a redundant [get_strategy_state] mutex acquisition. *)
let push_order ~now ?state order =
  let operation_str = match order.operation with
    | Place -> "place"
    | Amend -> "amend"
    | Cancel -> "cancel"
  in

  (* For Cancel operations, set inflight_cancel_buy when cancelling a buy *)
  (match order.operation with
   | Cancel ->
       let state = match state with Some s -> s | None -> get_strategy_state order.symbol in
       (match order.order_id with
        | Some _ ->
            let write_result = Strategy_common.LockFreeQueue.write order_buffer order in

            (match write_result with
            | Some () ->
                Strategy_common.OrderSignal.broadcast ();

                state.last_order_time <- now;
                (* Gate open_orders sync until order channel confirms the cancel. *)
                if order.side = Buy then
                  state.inflight_cancel_buy <- true;

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

         false
       end else begin
         (* Write Place or Amend to ringbuffer. *)
         let write_result = Strategy_common.LockFreeQueue.write order_buffer order in

       match write_result with
       | Some () ->
           Strategy_common.OrderSignal.broadcast ();


           (* Update strategy state. *)
           let state = match state with Some s -> s | None -> get_strategy_state order.symbol in
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


                   (* Track sell orders in state for grid spacing logic. *)
                   (match order.side, order.price with
                    | Sell, Some price ->
                        state.open_sell_orders <- (temp_order_id, price, order.qty) :: state.open_sell_orders;
                        ()
                    | _ -> ())
                 end
            | Amend ->
                (* Track pending amend; do not add to open_sell_orders. *)
                let temp_order_id = Printf.sprintf "pending_amend_%s"
                  (Option.value order.order_id ~default:"unknown") in
                let order_price = Option.value order.price ~default:0.0 in
                let timestamp = now in
                state.pending_orders <- (temp_order_id, order.side, order_price, timestamp) :: state.pending_orders;

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
    ?precounted_orders
    ~now
    (asset : trading_config)
    (current_price : float)
    (top_bid : float)
    (top_ask : float)
    (asset_balance : float)
    (quote_balance : float)
    (_open_buy_count : int)
    (_open_sell_count : int)
    (iter_open_orders : ((string -> float -> float -> string -> int option -> unit) -> unit))
    (cycle : int) =

  let state = match cached_state with Some s -> s | None -> get_strategy_state asset.symbol in
  if state.exchange_id = "" then begin
    state.exchange_id <- asset.exchange;
    state.cached_round_price <- get_round_price_fn asset.symbol asset.exchange;
    state.cached_price_increment <- get_price_increment asset.symbol asset.exchange;
    state.cached_qty_increment <- get_qty_increment_val asset.symbol asset.exchange;
    state.cached_qty_min <- 
      (match get_exchange_module asset.exchange with
       | Some (module Ex : Exchange.S) -> Option.value (Ex.get_qty_min ~symbol:asset.symbol) ~default:1.0
       | None -> 1.0);
    state.exchange_reserved_atomic <- Some (get_exchange_reserved_atomic asset.exchange);
  end;
  let ecfg = state.cached_ecfg in

  (* Only proceed with main strategy if capital_low flag is clear.
     asset_low is checked below only for sell+buy placement; order sync,
     cleanup, and cancellation detection must always run.
     NOTE: All state mutations below run under state.mutex to prevent
     races when concurrent callers target the same symbol. *)
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
   let lot_qty = venue_lot_qty state.grid_qty asset.exchange state in

   (* Anticipated balance credit decay: clear when balance feed catches up. *)
   if not (Float.is_nan asset_balance) then begin
     let asset_bal = asset_balance in
     if asset_bal > state.last_seen_asset_balance && state.anticipated_base_credit > 0.0 then begin
       state.anticipated_base_credit <- 0.0
     end;
     state.last_seen_asset_balance <- asset_bal;

     (* Asset-low check: clear flag when asset balance recovers.
        Subtract reserved_base and locked_in_sells so the clearing condition
        matches the blocking guard in the sell-placement path.  Without
        subtracting locked_in_sells here, `available_asset` appears large
        enough to clear asset_low, but the sell path immediately re-sets it,
        causing a hot loop (~50 ms). *)
     let qty_f = lot_qty in
     let asset_needed_fast =
       if ecfg.sell_uses_mult then
         qty_f *. state.cached_sell_mult
       else qty_f  (* 1:1 sells require full qty *)
     in
     let locked_in_sells =
       if ecfg.use_reserved_base_guard then
         List.fold_left (fun acc (_, _, qty) -> acc +. qty) 0.0 state.open_sell_orders
       else 0.0
     in
     let available_asset = asset_bal -. state.reserved_base +. state.anticipated_base_credit -. locked_in_sells in
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
       ignore (InFlightOrders.remove_in_flight_order (state.duplicate_key_sell));
       Logging.info_f ~section "Asset balance restored for %s (have %.8f, reserved %.8f, anticipated_credit %.8f, locked_sells %.8f, available %.8f, need %.8f) - resuming sell+buy placement"
         asset.symbol asset_bal state.reserved_base state.anticipated_base_credit locked_in_sells available_asset asset_needed_fast
     end
   end;

  (* Capital-low fast path: skip strategy when quote balance is known to be
     insufficient for a buy order. The FIRST loop (capital_low=false) is allowed
     so that any accrued sell orders from previously filled buys can be placed
     to free capital. After that first pass, capital_low is set and the strategy
     short-circuits here. When balance recovers, the flag is cleared. *)
  if not (Float.is_nan quote_balance) then begin
    let quote_bal = quote_balance in
    let qty_f = lot_qty in
    let quote_needed_fast = if not (Float.is_nan current_price) then current_price *. qty_f else 0.0 in
    (* Available balance = total balance minus all domains' reserved quote. *)
    let total_reserved = get_total_reserved_quote state in
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
      ignore (InFlightOrders.remove_in_flight_order (state.duplicate_key_buy));
      Logging.info_f ~section "Capital restored for %s (available %.2f, need %.2f, total_reserved %.2f, was_at %.2f) - resuming strategy"
        asset.symbol available_quote quote_needed_fast total_reserved state.capital_low_at_balance
    end
  end;

  if not state.capital_low then begin

  (* [now] is provided by the caller to avoid per-cycle syscalls. *)
  
  if Float.is_nan current_price then begin
    if state.last_cycle <> cycle then
      Logging.info_f ~section "Waiting for price data for %s (no ticker received yet)" asset.symbol
  end else begin
      (* Use l2Book top-of-book bid/ask instead of allMids mid price.
         l2Book updates per-block and tracks actual market state, while
         allMids batches across all coins and can diverge from fill price
         during fast candle movements, causing sell order stacking on Hyperliquid.
         Sell-side calcs use bid_price; buy-side calcs use ask_price.
         Falls back to mid when orderbook data is unavailable. *)
      let (bid_price, ask_price) =
        if not (Float.is_nan top_bid) && top_bid > 0.0 && not (Float.is_nan top_ask) && top_ask > 0.0 then
          (top_bid, top_ask)
        else
          (current_price, current_price)
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
        let (kept_rev, _, _) =
          List.fold_left (fun (acc, kept, removed) ((order_id, side, _, timestamp) as entry) ->
            let age = now -. timestamp in
            if age > 5.0 then begin
              Logging.warn_f ~section "Removing stale pending order %s for %s (age: %.1fs)" order_id asset.symbol age;
              if String.starts_with ~prefix:"pending_amend_" order_id then begin
                let target_oid = String.sub order_id 14 (String.length order_id - 14) in
                ignore (InFlightAmendments.remove_in_flight_amendment target_oid)
              end else begin
                let duplicate_key = (match side with Buy -> state.duplicate_key_buy | Sell -> state.duplicate_key_sell) in
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

      end;


      (* Clean up expired amend cooldowns in a single pass. *)
      if Hashtbl.length state.amend_cooldowns > 0 then begin
        let to_remove = ref [] in
        Hashtbl.iter (fun k v -> if now > v then to_remove := k :: !to_remove) state.amend_cooldowns;
        List.iter (Hashtbl.remove state.amend_cooldowns) !to_remove;
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

      (* Rebuild buy and sell lists from pre-computed data or exchange scan.
         When precounted_orders is provided by the caller, skip the redundant
         iter_open_orders call that would re-acquire orders_mutex and re-iterate
         the same hashtable the caller already scanned. *)
          let (buy_orders, sell_orders, open_buy_count_from_scan) =
            match precounted_orders with
            | Some (buys, sells, bc) ->
                (ref buys, ref sells, bc)
            | None ->
                let obc = ref 0 in
                let buys = ref [] in
                let sells = ref [] in
                iter_open_orders (fun order_id order_price qty side_str userref_opt ->
                  let is_our_strategy = match userref_opt with
                    | Some ref_val -> ref_val <> Strategy_common.strategy_userref_mm
                    | None -> true
                  in
                  if qty > 0.0 && is_our_strategy then begin
                    if side_str = "buy" then begin
                      incr obc;
                      buys := (order_id, order_price) :: !buys
                    end else begin
                      sells := (order_id, order_price, qty) :: !sells;
                      ()
                    end
                  end
                );
                (buys, sells, !obc)
          in

      (* Set buy order price and ID (select highest buy if multiple exist).
         Gate: if a cancel is in flight, the open_orders snapshot is not trusted
         because the order channel has not yet confirmed the cancel, so open_orders
         may still report the order as open. Only the order channel
         (handle_order_cancelled) clears inflight_cancel_buy. *)
      (match !buy_orders with
       | [] when not state.inflight_cancel_buy && not state.inflight_buy ->
           (* No open buy and no in-flight op; clear reservation and buy tracking.
              This handles orders cancelled by the exchange during a WS disconnect
              where the cancellation event was never received (ghost orders).
              Without this, last_buy_order_id would persist indefinitely,
              effective_buy_count would stay at 1, and no new buy could be placed. *)
           set_asset_reserved_quote state 0.0;
           (match state.last_buy_order_id with
            | Some ghost_id ->
                Logging.info_f ~section "Clearing ghost buy order %s for %s (exchange reports 0 open buys)"
                  ghost_id asset.symbol;
                state.last_buy_order_id <- None;
                state.last_buy_order_price <- None
            | None -> ())
       | orders when not state.inflight_cancel_buy && not state.inflight_buy ->
           (* Safe to sync: no in-flight operation. *)
           let valid_orders = List.filter (fun (_, p) -> p > 0.0) orders in
           (match valid_orders with
            | [] -> () (* Maintain prior state if price is temporarily unavailable *)
            | hd :: tl ->
               let (best_order_id, best_price) = List.fold_left (fun (acc_id, acc_price) (order_id, price) ->
                 if price > acc_price then (order_id, price) else (acc_id, acc_price)
               ) hd tl in
               state.last_buy_order_price <- Some best_price;
               state.last_buy_order_id <- Some best_order_id;
               (* Sync per-asset reserved_quote from the actual open buy so the
                  capital_low check uses the real exchange-reserved amount after restart. *)
               set_asset_reserved_quote state (best_price *. lot_qty))
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
          if not already_present then begin
            state.open_sell_orders <- (preserved_id, preserved_price, lot_qty) :: state.open_sell_orders;
            ()
          end
        ) preserved_sells
      end;





      (* Use cached parsed config values; updated once at domain startup
         and whenever the config ref changes (Fear & Greed re-evaluation). *)
      let qty = lot_qty in
      let grid_interval = asset.grid_interval in
      let sell_mult = state.cached_sell_mult in

      (* Refresh maker_fee from live cache (exchange may push updated fees). *)
      state.maker_fee <- (match asset.maker_fee with
        | Some f -> f
        | None ->
            match Fee_cache.get_maker_fee ~exchange:asset.exchange ~symbol:asset.symbol with
            | Some cached -> cached
            | None -> 0.0);

      let quote_needed = ask_price *. qty in

  (* Check for missing balance data. Stale but present balance data is acceptable.
     Supervisor monitors WebSocket health via heartbeats. *)
  let is_stale = ecfg.check_stale_balance && (Float.is_nan asset_balance || Float.is_nan quote_balance) in

  (* If balance feed is stale, skip strategy to avoid trading with stale data. *)
  if is_stale then begin
    state.last_cycle <- cycle;
    ()
  end else begin
    (* Core strategy logic based on open orders and balances.
       Log state periodically for race condition debugging. *)

    let buy_order_pending = List.exists (fun (_, side, _, _) -> side = Buy) state.pending_orders in
    (* Effective buy count: use max of raw count and our own tracking.
       webData2 snapshots can be stale during cancel-replace amendments.
       last_buy_order_id is set immediately by orderUpdates events. *)
    let has_tracked_buy = state.last_buy_order_id <> None in
    (* Use the count from our own scan (precounted or internal), not the
       external parameter which may include orders from other strategies. *)
    let open_buy_count = open_buy_count_from_scan in
    let effective_buy_count = if has_tracked_buy && open_buy_count = 0 then 1 else open_buy_count in

    if buy_order_pending then begin
        (* Log every 100,000 iterations to avoid spam. *)
        ();
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
          ignore (push_order ~now ~state cancel_order);
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
        let sell_price = calculate_grid_price bid_price grid_interval true state in
        let buy_price = calculate_grid_price ask_price grid_interval false state in

        let buy_cooldown_key = "place_Buy" in
        let is_buy_on_cooldown = Hashtbl.mem state.amend_cooldowns buy_cooldown_key in

        (* Place sell order first; sell and buy are always paired.
           asset_low blocks entry to this entire branch at the top level.
           Always attempt the sell regardless of local balance, as cached balance
           may be stale after a buy fill. If the exchange rejects, asset_low
           is set and will not clear until (asset_bal - reserved_base) >= needed,
           protecting accumulated base asset. *)
        if not (Float.is_nan asset_balance) && not state.inflight_sell && not state.asset_low then begin
             let asset_bal = asset_balance in
             let (sell_qty, is_accumulation_sell) =
               if ecfg.use_accumulation_sells then begin
                 let rounded_sell =
                   if asset.exchange = "ibkr" then
                     max 0.0 (qty -. 1.0)
                   else
                     round_qty (qty *. sell_mult) asset.symbol asset.exchange
                 in
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
                   (* Set asset_low to prevent tight-looping on every cycle.
                      Clears event-driven when balance recovers. Without this,
                      the strategy re-enters the effective_buy_count=0 branch,
                      retries the sell, and spams this warning every ~50ms. *)
                   if not state.asset_low then begin
                     state.asset_low <- true;
                     Logging.info_f ~section "Setting asset_low for %s (sell blocked by reserved_base guard)" asset.symbol
                   end;
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
                let sell_order = create_order state.duplicate_key_sell asset.symbol Sell sell_qty (Some sell_price) true asset.exchange in
                if push_order ~now ~state sell_order then begin
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
        end;

        (* Place buy order if quote balance is available. *)
        if not (Float.is_nan quote_balance) then begin
         if is_buy_on_cooldown || state.inflight_buy then ()
         else begin
             let quote_bal = quote_balance in
             (* Compute available balances net of open order commitments *)
             let locked_in_buys = List.fold_left (fun acc (_, price) -> acc +. (price *. lot_qty)) 0.0 !buy_orders in
             let available_quote_balance = quote_bal -. locked_in_buys in
             let balance_ok = available_quote_balance >= (buy_price *. qty) in
             if balance_ok then begin
               let order = create_order state.duplicate_key_buy asset.symbol Buy qty (Some buy_price) true asset.exchange in
               if push_order ~now ~state order then begin
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
                      let target_buy = state.cached_round_price (cs_price -. double_grid_interval) in
                      let min_move_threshold = get_min_move_threshold bid_price grid_interval state in
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
                    asset.symbol quote_needed available_quote_balance;
                  Hashtbl.replace state.amend_cooldowns cooldown_key (now +. 2.0);
                  (* Attempt without a reservation; exchange is the final gatekeeper. *)
                  let order = create_order state.duplicate_key_buy asset.symbol Buy qty (Some buy_price) true asset.exchange in
                  if push_order ~now ~state order then
                    state.last_buy_order_price <- Some buy_price
                end
             end
          end
        end else begin
             Logging.warn_f ~section "No quote balance data available for %s buy order"
               asset.symbol
        end;
        state.last_cycle <- cycle
      end else if effective_buy_count > 0 then begin
        (* Case 2: open buy exists; check sell orders for spacing enforcement. *)
        (* Combine active and pending sell orders for spacing calculation. *)
        let closest_sell_order = ref None in
        let update_closest oid price =
          if price > 0.0 then begin
            match !closest_sell_order with
            | None -> closest_sell_order := Some (oid, price)
            | Some (_, best_price) ->
                if price < best_price then closest_sell_order := Some (oid, price)
          end
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
              let exact_target = state.cached_round_price (sell_price -. double_grid_interval) in
              
              if distance > double_grid_interval then begin
                (* Distance > 2x: trail buy upward to maintain grid_interval below price. *)
                let proposed_buy_price = calculate_grid_price ask_price grid_interval false state in
                
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
                  
                  let min_move_threshold = get_min_move_threshold bid_price grid_interval state in
                  let current_buy_price_rounded = state.cached_round_price current_buy_price in
                  let price_diff_rounded = state.cached_round_price (abs_float (target_buy_price -. current_buy_price_rounded)) in

                  if amend_allowed ~state ~order_id:buy_order_id ~target_price:target_buy_price
                       ~current_price_rounded:current_buy_price_rounded
                       ~price_diff:price_diff_rounded ~min_move_threshold then begin
                    let quote_bal = quote_balance in
                    if not (Float.is_nan quote_balance) && can_place_buy_order qty quote_bal quote_needed then begin
                         let order = create_amend_order buy_order_id asset.symbol Buy qty (Some target_buy_price) true Grid asset.exchange in
                         ignore (push_order ~now ~state order);
                         state.last_buy_order_price <- Some target_buy_price;
                         ()
                    end else if not (Float.is_nan quote_balance) then begin
                         Logging.warn_f ~section "Insufficient quote balance to trail %s: need %.2f, have %.2f"
                           asset.symbol quote_needed quote_bal
                    end else begin
                         Logging.warn_f ~section "No quote balance for %s trailing" asset.symbol
                    end
                  end
                end else begin
                  (* Price has fallen; hold buy order steady, do not trail down. *)
                   ()
                end
              end else begin
                (* Distance <= 2x: enforce exact 2x spacing. *)
                let exact_target_rounded = exact_target in
                let current_buy_price_rounded = state.cached_round_price current_buy_price in
                let price_diff_rounded = state.cached_round_price (abs_float (exact_target_rounded -. current_buy_price_rounded)) in
                let min_move_threshold = get_min_move_threshold bid_price grid_interval state in

                if amend_allowed ~state ~order_id:buy_order_id ~target_price:exact_target_rounded
                     ~current_price_rounded:current_buy_price_rounded
                     ~price_diff:price_diff_rounded ~min_move_threshold then begin
                  let quote_bal = quote_balance in
                  if not (Float.is_nan quote_balance) && can_place_buy_order qty quote_bal quote_needed then begin
                       let order = create_amend_order buy_order_id asset.symbol Buy qty (Some exact_target) true Grid asset.exchange in
                       ignore (push_order ~now ~state order);
                       state.last_buy_order_price <- Some exact_target;
                       ()
                  end else if not (Float.is_nan quote_balance) then begin
                       Logging.warn_f ~section "Insufficient quote balance for %s: need %.2f, have %.2f"
                         asset.symbol quote_needed quote_bal
                  end else begin
                       Logging.warn_f ~section "No quote balance for %s" asset.symbol
                  end
                end else begin
                  ()
                end
              end
          | _ -> ()
        end else begin
          (* No sell orders exist but buy is active.
             Trail buy without sell anchors; sells are only placed
             in the effective_buy_count=0 branch after a buy fills. *)
           match state.last_buy_order_price, state.last_buy_order_id with
           | Some current_buy_price, Some buy_order_id ->
               let target_buy_price = calculate_grid_price ask_price grid_interval false state in
                            (* Only trail upward; amend buy if target is higher than current. *)
               if target_buy_price > current_buy_price then begin
                 let min_move_threshold = get_min_move_threshold ask_price grid_interval state in
                 let current_buy_price_rounded = state.cached_round_price current_buy_price in
                 let price_diff_rounded = state.cached_round_price (abs_float (target_buy_price -. current_buy_price_rounded)) in

                if amend_allowed ~state ~order_id:buy_order_id ~target_price:target_buy_price
                     ~current_price_rounded:current_buy_price_rounded
                     ~price_diff:price_diff_rounded ~min_move_threshold then begin
                  let quote_bal = quote_balance in
                  if not (Float.is_nan quote_balance) && can_place_buy_order qty quote_bal quote_needed then begin
                       let order = create_amend_order buy_order_id asset.symbol Buy qty (Some target_buy_price) true Grid asset.exchange in
                       ignore (push_order ~now ~state order);
                       state.last_buy_order_price <- Some target_buy_price;
                       ()
                  end else if not (Float.is_nan quote_balance) then begin
                       Logging.warn_f ~section "Insufficient quote balance to trail buy: need %.2f, have %.2f"
                         quote_needed quote_bal
                  end else begin
                       Logging.warn_f ~section "No quote balance for buy trailing"
                  end
                end
              end else begin
                ()
              end
          | _ ->
              ()
        end;
        (* Update cycle counter. *)
        state.last_cycle <- cycle
      end else begin
        (* No action required for remaining cases. *)
        state.last_cycle <- cycle
      end
  end  (* end of: if Float.is_nan current_price then ... else begin *)
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
        Dio_persistence.State_persistence.save_async ~symbol:asset_symbol
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
    ()
   | Sell ->
    state.inflight_sell <- false;
    state.recently_injected_sells <- (order_id, price, Unix.gettimeofday ()) :: state.recently_injected_sells;
    ());

  ()
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
    let duplicate_key = (match side with Buy -> state.duplicate_key_buy | Sell -> state.duplicate_key_sell) in
    ignore (InFlightOrders.remove_in_flight_order duplicate_key);
    
    let lower_reason = String.lowercase_ascii reason in

    let is_rate_limit = contains_fragment lower_reason "too many cumulative requests" || contains_fragment lower_reason "rate limit" in
    let is_insufficient_balance = contains_fragment lower_reason "insufficient funds"
      || contains_fragment lower_reason "insufficient spot balance"
      || contains_fragment lower_reason "not enough asset balance" in
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
  let duplicate_key = (match side with Buy -> state.duplicate_key_buy | Sell -> state.duplicate_key_sell) in
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

  ()
  )

  (* Buy rejection signals no buy order exists; strategy re-evaluates on next cycle. *)

(** True when [tracked] is the strategy's buy id and the execution report matches
    either the exchange [order_id] or an optional client id (Lighter/Hyperliquid). *)
let buy_tracking_matches_exchange_event tracked order_id cl_ord_id =
  tracked = order_id
  || match cl_ord_id with Some c -> tracked = c | None -> false

(** Handles order fill. Fully clears tracking, pending amends, and computes profit. *)
let handle_order_filled asset_symbol order_id side ~fill_price cl_ord_id =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    let acc_qty = venue_lot_qty state.grid_qty state.exchange_id state in
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
      | Buy, Some id when buy_tracking_matches_exchange_event id order_id cl_ord_id -> true
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
      if persistence_accumulation_exchange state.exchange_id then
        state.persistence_dirty <- true;
      (* Credit anticipated base so the sell guard sees incoming asset before
         the balance feed catches up (exec feed is faster than balance feed). *)
      if acc_qty > 0.0 then begin
        state.anticipated_base_credit <- state.anticipated_base_credit +. acc_qty;
        Logging.info_f ~section "Anticipated base credit for %s: +%.8f (total: %.8f) from buy fill %s"
          asset_symbol acc_qty state.anticipated_base_credit order_id
      end;
      ()
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
           (* OIDs are monotonically increasing on Hyperliquid and Lighter. *)
           (try Int64.compare (Int64.of_string order_id) (Int64.of_string persisted_oid) <= 0
            with _ -> false)
       | None -> true  (* New strategy: no persisted OID; skip all fills during startup replay. *))
    in
    if skip_profit_calc then
      ()
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
              let qty = acc_qty in
              let gross = (sell_fill_price -. base_price) *. qty in
              (* Hyperliquid / Lighter spot: buy fee from base; sell fee from quote.
                 Only the sell-side fee affects quote (e.g. USDC) balance growth.
                 Kraken: both legs charged against quote. *)
              let fees =
                if state.exchange_id = "ibkr" then
                  (* IBKR Pro Tiered: per-share with min/max, charged per leg *)
                  ibkr_commission ~qty ~price:base_price
                  +. ibkr_commission ~qty ~price:sell_fill_price
                else if hl_like_spot_fee_exchange state.exchange_id then
                  sell_fill_price *. qty *. state.maker_fee
                else
                  (sell_fill_price *. qty *. state.maker_fee)
                  +. (base_price *. qty *. state.maker_fee)
              in
              let net_profit = gross -. fees in
              if net_profit > 0.0 then begin
                state.accumulated_profit <- state.accumulated_profit +. net_profit;
                Logging.debug_f ~section "Realized profit for %s: %.6f (gross %.6f - fees %.6f, sell@%.4f base@%.4f x %.8f), accumulated: %.6f"
                  asset_symbol net_profit gross fees sell_fill_price base_price qty state.accumulated_profit
              end;
              (* Always record sell fill price for consecutive sell detection. *)
              state.last_sell_fill_price <- Some sell_fill_price
          | _ ->
              (* Sell at or below cost basis; still record for consecutive sell tracking. *)
              state.last_sell_fill_price <- Some sell_fill_price)
     | Buy -> ());

    (* 5b. Advance last_fill_oid on EVERY fill (buy or sell, profitable or not).
       This ensures startup replay always has a valid watermark to resume from.
       Without this, symbols that never had a profitable sell would have
       last_fill_oid=None, causing all fills to be skipped on every restart
       and preventing profit accumulation permanently. *)
    let should_update_oid = match state.last_fill_oid with
      | Some prev_oid ->
          (try Int64.compare (Int64.of_string order_id) (Int64.of_string prev_oid) > 0
           with _ -> true)
      | None -> true
    in
    if should_update_oid then
      state.last_fill_oid <- Some order_id;
    (* Persist state on every fill so last_fill_oid, fill prices, and profit
       all survive container restarts. *)
    if persistence_accumulation_exchange state.exchange_id then
      state.persistence_dirty <- true;

    (* 6. Clear inflight flags, reservation, and global placement trackers for immediate re-placement. *)
    (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);
    (* Release quote reservation when buy fills (funds transferred to base asset). *)
    (match side with Buy -> set_asset_reserved_quote state 0.0 | Sell -> ());
    ignore (InFlightOrders.remove_in_flight_order ((match side with Buy -> state.duplicate_key_buy | Sell -> state.duplicate_key_sell)));

    (* 7. On buy fill, clear the Sell side's InFlightOrders guard.
       The strategy places buy+sell together; the previous sell's guard
       would block the new sell via duplicate detection. *)
    if side = Buy then begin
      ignore (InFlightOrders.remove_in_flight_order (state.duplicate_key_sell));
      state.inflight_sell <- false
    end;

    ()
  )

(** Handles order cancellation. Removes from pending and tracked orders. *)
let handle_order_cancelled asset_symbol order_id side cl_ord_id =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* Order channel confirmed cancellation (authoritative source of truth).
       1. Remove any pending operations for this order. *)

    state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
      (* Match pending_id equal to order_id, or pending_amend_ entries targeting order_id. *)
      let matches = pending_id = order_id || 
                   (String.starts_with ~prefix:"pending_amend_" pending_id && 
                    String.length pending_id > 14 && 
                    String.sub pending_id 14 (String.length pending_id - 14) = order_id) in
      not matches
    ) state.pending_orders;


    (* Clear amend cooldowns if any exist. *)
    Hashtbl.remove state.amend_cooldowns order_id;
    ignore (InFlightAmendments.remove_in_flight_amendment order_id);

    (* 2. Clear tracking conditionally based on side. *)
    let cancelled_side = side in
    
    let was_tracked_buy = match state.last_buy_order_id with
      | Some id when buy_tracking_matches_exchange_event id order_id cl_ord_id -> true
      | _ -> false
    in
    
    if was_tracked_buy then begin
         state.last_buy_order_id <- None;
         state.last_buy_order_price <- None;
         ()
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

    state.open_sell_orders <- List.filter (fun (sell_id, _, _) ->
      sell_id <> order_id
    ) state.open_sell_orders;


    (* Clear inflight flags and global placement trackers for immediate re-placement. *)
    (match cancelled_side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);
    ignore (InFlightOrders.remove_in_flight_order ((match cancelled_side with Buy -> state.duplicate_key_buy | Sell -> state.duplicate_key_sell)));
    
    ()
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
                 ()
               else
                 (* Cancel-replace amendment (Hyperliquid): new order ID assigned. *)
                 Logging.info_f ~section "Amended buy order ID in tracking: %s -> %s @ %.2f for %s"
                   old_order_id new_order_id price asset_symbol
             | _ -> 
                (* If the old order was not tracked, do not install the new order
                   as the tracked buy. Doing so resurrects ghost orders from stale
                   orderUpdate websocket events, causing duplicate/ghost order
                   detection when combined with fresh placements. *)
                ())
     | Sell ->
         let original_sell_count = List.length state.open_sell_orders in
         let old_entry = List.find_opt (fun (id, _, _) -> id = old_order_id) state.open_sell_orders in
         let old_qty = match old_entry with
           | Some (_, _, q) -> q
           | None -> venue_lot_qty state.grid_qty state.exchange_id state in
         Logging.info_f ~section "SELL_AMEND [%s] %s -> %s @ %.2f: old_entry=%s old_qty=%.8f sells_before=%d"
           asset_symbol old_order_id new_order_id price
           (match old_entry with Some (_, p, q) -> Printf.sprintf "%.2f/%.8f" p q | None -> "NOT_FOUND")
           old_qty original_sell_count;
         state.open_sell_orders <- (new_order_id, price, old_qty) ::
            List.filter (fun (sell_id, _, _) -> sell_id <> old_order_id) state.open_sell_orders;
         state.recently_injected_sells <- (new_order_id, price, Unix.gettimeofday ()) :: state.recently_injected_sells;
         Logging.info_f ~section "SELL_AMEND [%s] result: sells_after=%d"
           asset_symbol (List.length state.open_sell_orders));
             
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

    ()
  )

(** Handles skipped order amendment. Clears pending lock without swapping IDs. *)
let handle_order_amendment_skipped asset_symbol order_id _ _ =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    (* 1. Remove the pending amend entry using order_id. *)
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         String.sub pending_id 14 (String.length pending_id - 14) = order_id in
      not matches_amend
    ) state.pending_orders;
    
    ()
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
      ignore (push_order ~now:(Unix.time ()) ~state cancel_order);

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
                ())
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
    ignore (InFlightOrders.remove_in_flight_order ((match side with Buy -> state.duplicate_key_buy | Sell -> state.duplicate_key_sell)));
  )

(** No-op shim: pending_cancellations removed; cancel state is now tracked via
    inflight_cancel_buy. The supervisor still calls this after every cancel
    response, so we keep the signature to satisfy the Strategy interface. *)
let cleanup_pending_cancellation _asset_symbol _order_id = ()


(** Reads up to [max_orders] orders from the ringbuffer for processing. *)
let get_pending_orders max_orders =
  Strategy_common.LockFreeQueue.read_batch order_buffer max_orders

(** Initializes the strategy module. *)
let init () =

  Random.self_init ()

(** Strategy module interface. *)
module Strategy = struct
  type config = trading_config

  (** Cleans up strategy state for a symbol when domain stops. *)
  let rec cleanup_strategy_state symbol =
    let map = Atomic.get strategy_states in
    if Strategy_common.StringMap.mem symbol map then
      let new_map = Strategy_common.StringMap.remove symbol map in
      if not (Atomic.compare_and_set strategy_states map new_map) then
        cleanup_strategy_state symbol

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
      (* Bootstrap persistent state for new suicide_grid strategies on venues that
         persist fill OIDs (Hyperliquid, Lighter, IBKR). If state has never been
         persisted (last_fill_oid=None) but fill events were seen during startup,
         establish an anchor so the next restart can resume from a known position. *)
      if state.last_fill_oid = None
         && state.highest_startup_oid <> None
         && persistence_accumulation_exchange state.exchange_id then begin
        state.last_fill_oid <- state.highest_startup_oid;
        Dio_persistence.State_persistence.save ~symbol
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