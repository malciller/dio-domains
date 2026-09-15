(* Strategy API: strategy-agnostic aggregation surface.

   Re-exports the generic state (Strategy_state), venue configuration and precision
   (Strategy_venue), reservation/accumulation (Strategy_reservation), order construction
   and dispatch (Strategy_orders), lifecycle/accounting actions (Strategy_lifecycle),
   decision procedures (Strategy_decision) and lifecycle event handlers (Strategy_events),
   plus the capital-reclamation step and the [Strategy] seam used by the offline reference
   replay (to be retired with the harness scaffolding). *)

open Strategy_common

(* Re-exported Types *)
type exchange_config = Strategy_state.exchange_config =
  { time_in_force : string
  ; track_pending_sells : bool
  ; use_accumulation_sells : bool
  ; sell_failure_sets_asset_low : bool
  ; use_reserved_base_guard : bool
  ; use_unnetted_sell_hold : bool
  ; balance_nets_open_order_holds : bool
  ; hold_netted_from_venue_state : bool
  ; asset_low_requires_balance_change : bool
  ; merge_preserved_sells : bool
  ; check_stale_balance : bool
  ; remaintain_expired_sells : bool
  }

type trading_config = Strategy_state.trading_config =
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

type strategy_state = Strategy_state.strategy_state

type sell_commitment = Strategy_state.sell_commitment =
  { mutable sc_price : float
  ; mutable sc_qty : float
  ; mutable sc_seen : bool
  ; mutable sc_acked : bool
  ; mutable sc_listed : bool
  ; sc_armed : float
  }

(* Re-exported Values & Functions *)
let section = Strategy_state.section
let take = Strategy_state.take
let contains_fragment = Strategy_state.contains_fragment
let kraken_config = Strategy_venue.kraken_config
let hyperliquid_config = Strategy_venue.hyperliquid_config
let ibkr_config = Strategy_venue.ibkr_config
let lighter_config = Strategy_venue.lighter_config
let get_exchange_config = Strategy_venue.get_exchange_config
let hl_like_spot_fee_exchange = Strategy_venue.hl_like_spot_fee_exchange
let ibkr_commission = Strategy_venue.ibkr_commission
let get_exchange_module = Strategy_venue.get_exchange_module
let get_round_price_fn = Strategy_venue.get_round_price_fn
let get_price_increment = Strategy_venue.get_price_increment
let get_qty_increment_val = Strategy_venue.get_qty_increment_val
let get_min_notional_val = Strategy_venue.get_min_notional_val
let round_qty = Strategy_venue.round_qty
let venue_lot_qty = Strategy_venue.venue_lot_qty
let parse_config_float = Strategy_venue.parse_config_float
let get_min_move_threshold = Strategy_venue.get_min_move_threshold
let calculate_grid_price = Strategy_venue.calculate_grid_price
let grid_price = Strategy_venue.grid_price
let get_strategy_state = Strategy_state.get_strategy_state
let total_reserved_by_exchange = Strategy_reservation.total_reserved_by_exchange
let get_exchange_reserved_atomic = Strategy_reservation.get_exchange_reserved_atomic
let get_total_reserved_quote = Strategy_reservation.get_total_reserved_quote
let set_asset_reserved_quote = Strategy_reservation.set_asset_reserved_quote
let atomic_check_and_reserve = Strategy_reservation.atomic_check_and_reserve
let can_place_buy_order = Strategy_reservation.can_place_buy_order
let can_place_sell_order = Strategy_reservation.can_place_sell_order
let amend_allowed = Strategy_reservation.amend_allowed
let has_active_sell = Strategy_reservation.has_active_sell

let sell_commitment_in_flight_timeout_s =
  Strategy_reservation.sell_commitment_in_flight_timeout_s
;;

let upsert_sell_commitment = Strategy_reservation.upsert_sell_commitment
let arm_sell_commitment = Strategy_reservation.arm_sell_commitment
let rekey_sell_commitment = Strategy_reservation.rekey_sell_commitment
let remove_sell_commitment = Strategy_reservation.remove_sell_commitment
let remove_pending_sell_commitments = Strategy_reservation.remove_pending_sell_commitments
let committed_sell_base = Strategy_reservation.committed_sell_base
let effective_committed_sell_base = Strategy_reservation.effective_committed_sell_base
let order_buffer = Strategy_orders.order_buffer
let get_order_buffer = Strategy_orders.get_order_buffer
let create_place_order = Strategy_orders.create_place_order
let create_amend_order = Strategy_orders.create_amend_order
let create_cancel_order = Strategy_orders.create_cancel_order
let create_order = Strategy_orders.create_order
let push_order = Strategy_orders.push_order
let sync_open_orders = Strategy_lifecycle.sync_open_orders
let reconcile_persisted_sell_levels = Strategy_lifecycle.reconcile_persisted_sell_levels
let evaluate_sell_leg = Strategy_decision.evaluate_sell_leg

type sell_pre = Strategy_decision.sell_pre

let sell_leg_prepare = Strategy_decision.sell_leg_prepare
let sell_leg_place = Strategy_decision.sell_leg_place
let sell_leg_finalize = Strategy_decision.sell_leg_finalize
let sell_leg_finalize_latch = Strategy_decision.sell_leg_finalize_latch
let sell_excess_sweep_phase = Strategy_decision.sell_excess_sweep_phase
let sell_finalize_end = Strategy_decision.sell_finalize_end
let evaluate_buy_leg = Strategy_decision.evaluate_buy_leg
let buy_leg_facts = Strategy_decision.buy_leg_facts
let buy_cancel_excess = Strategy_decision.buy_cancel_excess
let buy_place_initial = Strategy_decision.buy_place_initial
let buy_amend = Strategy_decision.buy_amend
let buy_amend_has_sell = Strategy_decision.buy_amend_has_sell
let buy_amend_with_sell = Strategy_decision.buy_amend_with_sell
let buy_amend_no_sell = Strategy_decision.buy_amend_no_sell

type buy_plan = Strategy_decision.buy_plan

let buy_place_plan = Strategy_decision.buy_place_plan
let buy_place_send = Strategy_decision.buy_place_send
let buy_place_send_insufficient = Strategy_decision.buy_place_send_insufficient
let buy_place_latch_capital_low = Strategy_decision.buy_place_latch_capital_low
let buy_place_warn_quote = Strategy_decision.buy_place_warn_quote
let cleanup_pending_and_cooldowns = Strategy_lifecycle.cleanup_pending_and_cooldowns
let expire_amend_cooldowns = Strategy_lifecycle.expire_amend_cooldowns
let evict_ghost_orders = Strategy_lifecycle.evict_ghost_orders
let reconcile_position = Strategy_lifecycle.reconcile_position
let evaluate_asset_low_recovery = Strategy_lifecycle.evaluate_asset_low_recovery
let evaluate_capital_low_recovery = Strategy_lifecycle.evaluate_capital_low_recovery
let unnetted_sell_hold = Strategy_lifecycle.unnetted_sell_hold
let execute_strategy = Strategy_decision.execute_strategy
let compute_buy_ref_price = Strategy_lifecycle.compute_buy_ref_price

(* ------------------------------------------------------------------ *)
(* Priority-reclamation step (pure decision). *)
(* *)
(* The capital oracle's reclamation pass asks a domain to cancel its *)
(* resting buys (decision.reclaim_capital) so committed capital returns *)
(* to the account pool. A cancel is a one-shot network op that can fail *)
(* silently (dropped dispatch, exchange reject, full ring buffer), so *)
(* the cancellation is latched to avoid re-issuing it every cycle, but *)
(* it must be retried while the reclaim decision persists and eligible *)
(* buys remain in the store - otherwise one failed attempt leaves the *)
(* asset paused permanently and the priority asset never resumes on *)
(* capital that was never released. *)
(* ------------------------------------------------------------------ *)

(** Per-cycle reclaim action, decided from the latch state and the exchange store's buy
    orders:
    - [Reclaim_rearm]: no buy remains in the store; the cancel landed (or was
      unnecessary). The domain re-arms its latch and wakes the capital oracle to re-size
      on the released capital.
    - [Reclaim_cancel n]: [n] cancellable buys remain and a cancel may be issued (none in
      flight, or the retry interval elapsed after a failed attempt). The domain pushes
      cancels for every eligible buy and re-arms the latch.
    - [Reclaim_deferred]: a cancel is already in flight (issued within the retry interval)
      or only mid-amendment buys remain (the exchange rejects canceling an order being
      amended). Wait; do not spam the exchange. *)
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

let flush_persistence = Strategy_events.flush_persistence
let handle_order_acknowledged = Strategy_events.handle_order_acknowledged
let record_exec_event = Strategy_events.record_exec_event
let apply_event = Strategy_events.apply_event
let handle_order_failed = Strategy_events.handle_order_failed
let handle_order_rejected = Strategy_events.handle_order_rejected
let handle_order_filled = Strategy_events.handle_order_filled
let handle_order_cancelled = Strategy_events.handle_order_cancelled
let handle_order_amended = Strategy_events.handle_order_amended
let handle_order_amendment_skipped = Strategy_events.handle_order_amendment_skipped
let handle_order_amendment_failed = Strategy_events.handle_order_amendment_failed
let cleanup_pending_cancellation = Strategy_events.cleanup_pending_cancellation
let enqueue_event = Strategy_events.enqueue_event
let drain_events = Strategy_events.drain_events
let drain_events_with = Strategy_events.drain_events_with
let runtime_event_of_lifecycle = Strategy_events.runtime_event_of_lifecycle

(** Reads up to [max_orders] orders from the ringbuffer. *)
let get_pending_orders max_orders = LockFreeQueue.read_batch order_buffer max_orders

(** Seeds the process RNG. *)
let init () = Random.self_init ()

(** Strategy module interface. *)
module Strategy = struct
  type config = trading_config

  (** Cleans up strategy state for a symbol when domain stops. *)
  let rec cleanup_strategy_state symbol =
    let map = Atomic.get Strategy_state.strategy_states in
    if StringMap.mem symbol map
    then (
      let new_map = StringMap.remove symbol map in
      if not (Atomic.compare_and_set Strategy_state.strategy_states map new_map)
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
  let record_exec_event = record_exec_event
  let apply_event = apply_event
  let cleanup_pending_cancellation = cleanup_pending_cancellation
  let cleanup_strategy_state = cleanup_strategy_state
  let init = init
  let enqueue_event = enqueue_event
  let drain_events = drain_events
  let drain_events_with = drain_events_with

  (** Supervisor REST callbacks enqueue lifecycle events of this type. *)
  type lifecycle_event = Strategy_events.lifecycle_event =
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
    | Cancel_cleanup of { order_id : string }

  (** Clears the startup_replay flag so subsequent fills are processed normally. *)
  let set_startup_replay_done symbol =
    let state = get_strategy_state symbol in
    Mutex.lock state.mutex;
    if state.startup_replay
    then (
      state.startup_replay <- false;
      (* Startup replay skips history fills by design, and every skip bumps
         skipped_fill_streak. Reset it so the replayed count does not leak into live
         trading: a strategy that replayed >= 50 historical fills would otherwise boot
         with the streak at the self-heal threshold, and the first post-replay duplicate
         would trip the CRITICAL self-heal and wipe the last_fill_oid high-water mark. *)
      state.skipped_fill_streak <- 0;
      Logging.debug_f
        ~section
        "Startup replay complete for %s (last_fill_oid=%s, accumulated_profit=%.6f)"
        symbol
        (Option.value state.last_fill_oid ~default:"none")
        state.accumulated_profit;
      if state.last_fill_oid = None
         && state.highest_startup_oid <> None
         && state.base_accumulation_enabled
      then (
        (* last_fill_oid was None (fresh strategy or absent state file): the first-batch
           fills were all treated as pre-restart history and are not accounted. Surface
           this loudly - a fill that genuinely happened after restart (order placed
           pre-restart, filled in the down window) is silently excluded from
           inventory/P&L. *)
        Logging.warn_f
          ~section
          "Startup replay for %s had no persisted last_fill_oid; bootstrapping to \
           highest_startup_oid=%s. %d fill(s) in the first batch were treated as \
           pre-restart history and NOT accounted (state file absent or fresh?)."
          symbol
          (Option.value state.highest_startup_oid ~default:"none")
          state.skipped_fills_total;
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
