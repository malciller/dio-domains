(* Jacobs Ladder Strategy

   Grid trading system with a single-buy, multi-sell order model.
   Delegates to modular sub-components in jacobs_ladder/ for:
   - Types & state management (Jacobs_ladder_types)
   - Exchange configuration & precision helpers (Jacobs_ladder_config)
   - Reservation & accumulation (Jacobs_ladder_reservation)
   - Order construction & buffer dispatch (Jacobs_ladder_orders)
   - Strategy execution loop (Jacobs_ladder_execution)
   - Lifecycle event handlers (Jacobs_ladder_events) *)

open Strategy_common

(* Re-exported Types *)
type exchange_config = Jacobs_ladder_types.exchange_config =
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

type trading_config = Jacobs_ladder_types.trading_config =
  { exchange : string
  ; symbol : string
  ; qty : string
  ; grid_interval : float
  ; sell_mult : string
  ; strategy : string
  ; maker_fee : float option
  ; taker_fee : float option
  ; accumulation_buffer : float
  ; base_accumulation : bool
  ; sell_levels_persistence : bool
  }

type strategy_state = Jacobs_ladder_types.strategy_state

(* Re-exported Values & Functions *)
let section = Jacobs_ladder_types.section
let take = Jacobs_ladder_types.take
let contains_fragment = Jacobs_ladder_types.contains_fragment
let kraken_config = Jacobs_ladder_config.kraken_config
let hyperliquid_config = Jacobs_ladder_config.hyperliquid_config
let ibkr_config = Jacobs_ladder_config.ibkr_config
let lighter_config = Jacobs_ladder_config.lighter_config
let get_exchange_config = Jacobs_ladder_config.get_exchange_config

let hl_like_spot_fee_exchange = Jacobs_ladder_config.hl_like_spot_fee_exchange
let ibkr_commission = Jacobs_ladder_config.ibkr_commission
let get_exchange_module = Jacobs_ladder_config.get_exchange_module
let get_round_price_fn = Jacobs_ladder_config.get_round_price_fn
let get_price_increment = Jacobs_ladder_config.get_price_increment
let get_qty_increment_val = Jacobs_ladder_config.get_qty_increment_val
let round_qty = Jacobs_ladder_config.round_qty
let venue_lot_qty = Jacobs_ladder_config.venue_lot_qty
let parse_config_float = Jacobs_ladder_config.parse_config_float
let get_min_move_threshold = Jacobs_ladder_config.get_min_move_threshold
let calculate_grid_price = Jacobs_ladder_config.calculate_grid_price
let get_strategy_state = Jacobs_ladder_types.get_strategy_state
let total_reserved_by_exchange = Jacobs_ladder_reservation.total_reserved_by_exchange
let get_exchange_reserved_atomic = Jacobs_ladder_reservation.get_exchange_reserved_atomic
let get_total_reserved_quote = Jacobs_ladder_reservation.get_total_reserved_quote
let set_asset_reserved_quote = Jacobs_ladder_reservation.set_asset_reserved_quote
let atomic_check_and_reserve = Jacobs_ladder_reservation.atomic_check_and_reserve
let can_place_buy_order = Jacobs_ladder_reservation.can_place_buy_order
let can_place_sell_order = Jacobs_ladder_reservation.can_place_sell_order
let amend_allowed = Jacobs_ladder_reservation.amend_allowed
let compute_sell_qty = Jacobs_ladder_reservation.compute_sell_qty

let accumulation_sell_allowed_on_recovery =
  Jacobs_ladder_reservation.accumulation_sell_allowed_on_recovery
;;

let has_active_sell = Jacobs_ladder_reservation.has_active_sell
let order_buffer = Jacobs_ladder_orders.order_buffer
let get_order_buffer = Jacobs_ladder_orders.get_order_buffer
let create_place_order = Jacobs_ladder_orders.create_place_order
let create_amend_order = Jacobs_ladder_orders.create_amend_order
let create_cancel_order = Jacobs_ladder_orders.create_cancel_order
let create_order = Jacobs_ladder_orders.create_order
let push_order = Jacobs_ladder_orders.push_order
let sync_open_orders = Jacobs_ladder_execution.sync_open_orders

let reconcile_persisted_sell_levels =
  Jacobs_ladder_execution.reconcile_persisted_sell_levels
;;

let evaluate_sell_leg = Jacobs_ladder_execution.evaluate_sell_leg
let execute_strategy = Jacobs_ladder_execution.execute_strategy

(* ------------------------------------------------------------------ *)
(* Priority-reclamation step (pure decision).                          *)
(*                                                                     *)
(* The capital oracle's reclamation pass asks a domain to cancel its    *)
(* resting buy(s) (decision.reclaim_capital) so the committed capital  *)
(* returns to the account pool for a higher-priority asset. The domain *)
(* issues the cancel through the normal order pipeline. A cancel is a  *)
(* one-shot network op that can fail silently (dispatch dropped while  *)
(* the exchange connection flapped, the exchange rejected it, the ring *)
(* buffer was full): the cancellation is latched here so it is NOT     *)
(* re-issued every cycle, but it MUST be retried while the reclaim     *)
(* decision persists and eligible buys still sit in the store -        *)
(* otherwise a single failed attempt leaves the account permanently    *)
(* stuck: the reclaimed asset stays paused (the decision only clears   *)
(* once the store's committed value drops to zero) and the priority    *)
(* asset never resumes on capital that was never actually released.    *)
(* ------------------------------------------------------------------ *)

(** The domain's per-cycle reclaim action, decided purely from the latch
    state and the exchange store's buy orders:
    - [Reclaim_rearm]: no buy at all remains in the store - the cancel landed
      (or never needed). The domain re-arms its latch so a later reclaim
      decision re-triggers cleanly, and the capital oracle is woken to
      re-size with the released capital.
    - [Reclaim_cancel n]: [n] cancellable buys remain and the cancel may be
      issued (none is in flight, or the retry interval elapsed after a failed
      attempt). The domain pushes cancels for every eligible buy and re-arms
      the latch.
    - [Reclaim_deferred]: a cancel is already in flight (issued within the
      retry interval) or the only remaining buys are mid-amendment (the
      exchange rejects canceling an order being amended) - wait, do not spam
      the exchange. *)
type reclaim_step =
  | Reclaim_rearm
  | Reclaim_cancel of int
  | Reclaim_deferred

let reclaim_step
      ~(now : float)
      ~(retry_seconds : float)
      ~(issued : bool)
      ~(issued_at : float)
      ~(eligible : int)
      ~(any_buy : bool)
  : reclaim_step
  =
  if eligible > 0
  then
    if (not issued) || now -. issued_at > retry_seconds
    then Reclaim_cancel eligible
    else Reclaim_deferred
  else if any_buy
  then Reclaim_deferred (* only mid-amendment buys remain: wait for the amend *)
  else Reclaim_rearm
;;

let flush_persistence = Jacobs_ladder_events.flush_persistence
let handle_order_acknowledged = Jacobs_ladder_events.handle_order_acknowledged
let handle_order_failed = Jacobs_ladder_events.handle_order_failed
let handle_order_rejected = Jacobs_ladder_events.handle_order_rejected
let handle_order_filled = Jacobs_ladder_events.handle_order_filled
let handle_order_cancelled = Jacobs_ladder_events.handle_order_cancelled
let handle_order_amended = Jacobs_ladder_events.handle_order_amended
let handle_order_amendment_skipped = Jacobs_ladder_events.handle_order_amendment_skipped
let handle_order_amendment_failed = Jacobs_ladder_events.handle_order_amendment_failed
let cleanup_pending_cancellation = Jacobs_ladder_events.cleanup_pending_cancellation
let enqueue_event = Jacobs_ladder_events.enqueue_event
let drain_events = Jacobs_ladder_events.drain_events

(** Reads up to [max_orders] orders from the ringbuffer for processing. *)
let get_pending_orders max_orders = LockFreeQueue.read_batch order_buffer max_orders

(** Initializes the strategy module. *)
let init () = Random.self_init ()

(** Strategy module interface. *)
module Strategy = struct
  type config = trading_config

  (** Cleans up strategy state for a symbol when domain stops. *)
  let rec cleanup_strategy_state symbol =
    let map = Atomic.get Jacobs_ladder_types.strategy_states in
    if StringMap.mem symbol map
    then (
      let new_map = StringMap.remove symbol map in
      if not (Atomic.compare_and_set Jacobs_ladder_types.strategy_states map new_map)
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
  let enqueue_event = enqueue_event
  let drain_events = drain_events

  (** Supervisor REST callbacks enqueue lifecycle events of this type. *)
  type lifecycle_event = Jacobs_ladder_events.lifecycle_event =
    | Ack of
        { now : float
        ; order_id : string
        ; side : order_side
        ; price : float
        }
    | Failed of
        { now : float
        ; side : order_side
        ; reason : string
        }
    | Rejected of
        { now : float
        ; side : order_side
        ; price : float
        }
    | Amended of
        { now : float
        ; old_id : string
        ; new_id : string
        ; side : order_side
        ; price : float
        }
    | Amendment_skipped of
        { now : float
        ; order_id : string
        ; side : order_side
        ; price : float
        }
    | Amendment_failed of
        { now : float
        ; order_id : string
        ; side : order_side
        ; reason : string
        }

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
        && state.base_accumulation_enabled
      then (
        state.last_fill_oid <- state.highest_startup_oid;
        let key =
          match state.persistence_key with
          | Some k -> k
          | None -> "migrated:" ^ symbol
        in
        if state.base_accumulation_enabled
        then
          Dio_persistence.Base_accumulation_store.save
            ~key
            { Dio_persistence.Base_accumulation_store.reserved_base = state.reserved_base
            ; accumulated_profit = state.accumulated_profit
            ; last_fill_oid = state.last_fill_oid
            ; last_buy_fill_price = state.last_buy_fill_price
            ; last_buy_fill_qty = state.last_buy_fill_qty
            ; last_sell_fill_price = state.last_sell_fill_price
            ; last_sell_fill_qty = state.last_sell_fill_qty
            };
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
