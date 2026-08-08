(* Suicide Grid Strategy

   Grid trading system with a single-buy, multi-sell order model.
   Delegates to modular sub-components in suicide_grid/ for:
   - Types & state management (Suicide_grid_types)
   - Exchange configuration & precision helpers (Suicide_grid_config)
   - Reservation & accumulation (Suicide_grid_reservation)
   - Order construction & buffer dispatch (Suicide_grid_orders)
   - Strategy execution loop (Suicide_grid_execution)
   - Lifecycle event handlers (Suicide_grid_events) *)

open Strategy_common

(* Re-exported Types *)
type exchange_config = Suicide_grid_types.exchange_config =
  { time_in_force : string
  ; track_pending_sells : bool
  ; use_accumulation_sells : bool
  ; sell_uses_mult : bool
  ; sell_failure_sets_asset_low : bool
  ; use_reserved_base_guard : bool
  ; asset_low_requires_balance_change : bool
  ; merge_preserved_sells : bool
  ; check_stale_balance : bool
  ; remaintain_expired_sells : bool
  }

type trading_config = Suicide_grid_types.trading_config =
  { exchange : string
  ; symbol : string
  ; qty : string
  ; grid_interval : float
  ; sell_mult : string
  ; strategy : string
  ; maker_fee : float option
  ; taker_fee : float option
  ; accumulation_buffer : float
  }

type strategy_state = Suicide_grid_types.strategy_state

(* Re-exported Values & Functions *)
let section = Suicide_grid_types.section
let take = Suicide_grid_types.take
let contains_fragment = Suicide_grid_types.contains_fragment
let kraken_config = Suicide_grid_config.kraken_config
let hyperliquid_config = Suicide_grid_config.hyperliquid_config
let ibkr_config = Suicide_grid_config.ibkr_config
let lighter_config = Suicide_grid_config.lighter_config
let get_exchange_config = Suicide_grid_config.get_exchange_config

let persistence_accumulation_exchange =
  Suicide_grid_config.persistence_accumulation_exchange
;;

let hl_like_spot_fee_exchange = Suicide_grid_config.hl_like_spot_fee_exchange
let ibkr_commission = Suicide_grid_config.ibkr_commission
let get_exchange_module = Suicide_grid_config.get_exchange_module
let get_round_price_fn = Suicide_grid_config.get_round_price_fn
let get_price_increment = Suicide_grid_config.get_price_increment
let get_qty_increment_val = Suicide_grid_config.get_qty_increment_val
let round_qty = Suicide_grid_config.round_qty
let venue_lot_qty = Suicide_grid_config.venue_lot_qty
let parse_config_float = Suicide_grid_config.parse_config_float
let get_min_move_threshold = Suicide_grid_config.get_min_move_threshold
let calculate_grid_price = Suicide_grid_config.calculate_grid_price
let get_strategy_state = Suicide_grid_types.get_strategy_state
let total_reserved_by_exchange = Suicide_grid_reservation.total_reserved_by_exchange
let get_exchange_reserved_atomic = Suicide_grid_reservation.get_exchange_reserved_atomic
let get_total_reserved_quote = Suicide_grid_reservation.get_total_reserved_quote
let set_asset_reserved_quote = Suicide_grid_reservation.set_asset_reserved_quote
let atomic_check_and_reserve = Suicide_grid_reservation.atomic_check_and_reserve
let can_place_buy_order = Suicide_grid_reservation.can_place_buy_order
let can_place_sell_order = Suicide_grid_reservation.can_place_sell_order
let amend_allowed = Suicide_grid_reservation.amend_allowed
let compute_sell_qty = Suicide_grid_reservation.compute_sell_qty

let accumulation_sell_allowed_on_recovery =
  Suicide_grid_reservation.accumulation_sell_allowed_on_recovery
;;

let has_active_sell = Suicide_grid_reservation.has_active_sell
let order_buffer = Suicide_grid_orders.order_buffer
let get_order_buffer = Suicide_grid_orders.get_order_buffer
let create_place_order = Suicide_grid_orders.create_place_order
let create_amend_order = Suicide_grid_orders.create_amend_order
let create_cancel_order = Suicide_grid_orders.create_cancel_order
let create_order = Suicide_grid_orders.create_order
let push_order = Suicide_grid_orders.push_order
let sync_open_orders = Suicide_grid_execution.sync_open_orders
let evaluate_sell_leg = Suicide_grid_execution.evaluate_sell_leg
let execute_strategy = Suicide_grid_execution.execute_strategy
let flush_persistence = Suicide_grid_events.flush_persistence
let handle_order_acknowledged = Suicide_grid_events.handle_order_acknowledged
let handle_order_failed = Suicide_grid_events.handle_order_failed
let handle_order_rejected = Suicide_grid_events.handle_order_rejected
let handle_order_filled = Suicide_grid_events.handle_order_filled
let handle_order_cancelled = Suicide_grid_events.handle_order_cancelled
let handle_order_amended = Suicide_grid_events.handle_order_amended
let handle_order_amendment_skipped = Suicide_grid_events.handle_order_amendment_skipped
let handle_order_amendment_failed = Suicide_grid_events.handle_order_amendment_failed
let cleanup_pending_cancellation = Suicide_grid_events.cleanup_pending_cancellation

(** Reads up to [max_orders] orders from the ringbuffer for processing. *)
let get_pending_orders max_orders = LockFreeQueue.read_batch order_buffer max_orders

(** Initializes the strategy module. *)
let init () = Random.self_init ()

(** Strategy module interface. *)
module Strategy = struct
  type config = trading_config

  (** Cleans up strategy state for a symbol when domain stops. *)
  let rec cleanup_strategy_state symbol =
    let map = Atomic.get Suicide_grid_types.strategy_states in
    if StringMap.mem symbol map
    then (
      let new_map = StringMap.remove symbol map in
      if not (Atomic.compare_and_set Suicide_grid_types.strategy_states map new_map)
      then cleanup_strategy_state symbol)
  ;;

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

  (** Clears the startup_replay flag so subsequent fills are processed normally. *)
  let set_startup_replay_done symbol =
    let state = get_strategy_state symbol in
    Mutex.lock state.mutex;
    if state.startup_replay
    then (
      state.startup_replay <- false;
      Logging.debug_f
        ~section
        "Startup replay complete for %s (last_fill_oid=%s, accumulated_profit=%.6f)"
        symbol
        (Option.value state.last_fill_oid ~default:"none")
        state.accumulated_profit;
      if
        state.last_fill_oid = None
        && state.highest_startup_oid <> None
        && Suicide_grid_config.persistence_accumulation_exchange state.exchange_id
      then (
        state.last_fill_oid <- state.highest_startup_oid;
        Dio_persistence.State_persistence.save
          ~symbol
          ~reserved_base:state.reserved_base
          ~accumulated_profit:state.accumulated_profit
          ~last_fill_oid:state.last_fill_oid
          ~last_buy_fill_price:state.last_buy_fill_price
          ~last_sell_fill_price:state.last_sell_fill_price
          ();
        Logging.info_f
          ~section
          "Bootstrapped initial state for %s (last_fill_oid=%s, reserved_base=%.8f, \
           accumulated_profit=%.6f)"
          symbol
          (Option.get state.highest_startup_oid)
          state.reserved_base
          state.accumulated_profit);
      state.inflight_sell <- false;
      state.inflight_buy <- false;
      state.recently_injected_sells <- [];
      ignore (InFlightOrders.remove_in_flight_order state.duplicate_key_sell);
      ignore (InFlightOrders.remove_in_flight_order state.duplicate_key_buy));
    Mutex.unlock state.mutex
  ;;
end
