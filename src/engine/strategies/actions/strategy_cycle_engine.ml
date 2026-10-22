(** Config-driven grid engine context (milestone 3; fine-orchestration decomposition).

    Holds the per-cycle strategy inputs and implements the reference actions the shipped
    strategy file composes ([prepare]/[cleanup]/[sync]/[refresh_fee]/[guard]/[buy_*]/
    [sell_*]), each by calling the reference [execute_strategy] sub-functions with these
    inputs, so a config-driven grid replicates by construction. Enabled whenever an entry
    is bound to a strategy file; the mutable fields are updated in place each cycle (no
    per-cycle allocation). Shared by the domain loop and the offline candidate replay, so
    the candidate interpreter is exercised on exactly the wiring the live loop uses. The
    caller holds [state.mutex] for the whole interpreter cycle (see [with_lock]); the
    sub-functions do not lock. *)

module Jac = Strategy_api
module Types = Strategy_state
module Slots = Strategy_fact_slots

let section = "strategy_cycle_engine"

type ctx =
  { mutable cg_asset : Types.trading_config option
  ; mutable cg_state : Types.strategy_state option
  ; mutable cg_carry : Dio_persistence.Carry_state_store.t option
  ; mutable cg_min_liq_reserve : float
  (** Aggregate hedgeable inventory across every leg of this controller. [< 0] means "not
      an overlay controller"; [market_facts] then falls back to the local venue figure. *)
  ; mutable cg_overlay_accumulated : float
  (** True when the controller has a Hyperliquid spot leg, so the recycle base buy is
      active. When false the recycle stays 100% quote. *)
  ; mutable cg_overlay_has_spot_leg : bool
  (** Symbol of the Hyperliquid spot leg whose book the recycle buy targets. *)
  ; mutable cg_overlay_spot_symbol : string
  ; mutable cg_last_leverage : float
  (** Venue time of the last carry entry (perp sell) fill, or 0.0. A fresh entry fill can
      arrive before the position snapshot reflects it; [order_place] holds a new entry
      for a short grace window so the cycle cannot re-short the full target. *)
  ; mutable cg_last_entry_fill_at : float
  ; mutable cg_price : float
  ; mutable cg_bid : float
  ; mutable cg_ask : float
  ; mutable cg_abal : float
  ; mutable cg_qbal : float
  ; mutable cg_now : float
  ; mutable cg_cycle : int
  ; mutable cg_quote_stale : bool
  ; mutable cg_oracle_halted : bool
  ; mutable cg_base_age : float option
  ; mutable cg_gen : int
  ; mutable cg_iter : (string -> float -> float -> string -> int option -> unit) -> unit
  ; mutable cg_drain :
      symbol:string
      -> (string * (float option * float * string * int option) option) list * bool
  ; mutable cg_ecfg : Types.exchange_config option
  ; mutable cg_lot_qty : float
  ; mutable cg_bid_r : float
  ; mutable cg_ask_r : float
  ; mutable cg_continue : bool
  ; mutable cg_open_buy_count : int
  ; mutable cg_has_recent_amend_buy : bool
  ; mutable cg_locked_in_buys : float
  ; mutable cg_locked_in_sells : float
  ; mutable cg_closest_sell_order : (string * float) option
  ; mutable cg_open_persisted : (float * float) list
  ; mutable cg_missing_persisted : (float * float) list
  ; mutable cg_buy_attempted : bool
  ; mutable cg_buy_active : bool
  ; mutable cg_buy_pending : bool
  ; mutable cg_buy_effective_count : int
  ; mutable cg_buy_should_cancel : bool
  ; mutable cg_buy_plan : Jac.buy_plan option
  ; mutable cg_profile : bool
  ; mutable cg_sell_pre : Jac.sell_pre option
  ; mutable cg_symbol : string
  }

let create () =
  { cg_asset = None
  ; cg_state = None
  ; cg_carry = None
  ; cg_min_liq_reserve = 0.0
  ; cg_overlay_accumulated = -1.0
  ; cg_overlay_has_spot_leg = false
  ; cg_overlay_spot_symbol = ""
  ; cg_last_leverage = 0.0
  ; cg_last_entry_fill_at = 0.0
  ; cg_price = nan
  ; cg_bid = nan
  ; cg_ask = nan
  ; cg_abal = nan
  ; cg_qbal = nan
  ; cg_now = 0.0
  ; cg_cycle = 0
  ; cg_quote_stale = false
  ; cg_oracle_halted = false
  ; cg_base_age = None
  ; cg_gen = -1
  ; cg_iter = (fun _ -> ())
  ; cg_drain = (fun ~symbol:_ -> [], true)
  ; cg_ecfg = None
  ; cg_lot_qty = nan
  ; cg_bid_r = nan
  ; cg_ask_r = nan
  ; cg_continue = false
  ; cg_open_buy_count = 0
  ; cg_has_recent_amend_buy = false
  ; cg_locked_in_buys = 0.0
  ; cg_locked_in_sells = 0.0
  ; cg_closest_sell_order = None
  ; cg_open_persisted = []
  ; cg_missing_persisted = []
  ; cg_buy_attempted = false
  ; cg_buy_active = false
  ; cg_buy_pending = false
  ; cg_buy_effective_count = 0
  ; cg_buy_should_cancel = false
  ; cg_buy_plan = None
  ; cg_profile = false
  ; cg_sell_pre = None
  ; cg_symbol = ""
  }
;;

(** Per-cycle phase attribution. Cheap on the hot path: when profiling is off it just runs
    [f]; when on it reads the domain-local minor-word counter and monotonic clock and
    accumulates into the strategy-state scratch fields the dashboard reads. *)
let[@inline] measure c (phase : Strategy_actions_cycle.phase) (local_ (f : unit -> unit)) =
  if not c.cg_profile
  then f ()
  else (
    let a0 = Monotonic_clock.minor_words () in
    let t0 = Monotonic_clock.now_ns () in
    f ();
    match c.cg_state with
    | None -> ()
    | Some st ->
      let da = Monotonic_clock.minor_words () - a0 in
      let dt = Monotonic_clock.now_ns () - t0 in
      (match phase with
       | Strategy_actions_cycle.Preamble ->
         st.alloc_preamble_words <- st.alloc_preamble_words + da;
         st.time_preamble_ns <- st.time_preamble_ns + dt
       | Strategy_actions_cycle.Facts ->
         st.alloc_facts_words <- st.alloc_facts_words + da;
         st.time_facts_ns <- st.time_facts_ns + dt
       | Strategy_actions_cycle.Cleanup ->
         st.alloc_cleanup_words <- st.alloc_cleanup_words + da;
         st.time_cleanup_ns <- st.time_cleanup_ns + dt
       | Strategy_actions_cycle.Sync ->
         st.alloc_sync_words <- st.alloc_sync_words + da;
         st.time_sync_ns <- st.time_sync_ns + dt
       | Strategy_actions_cycle.BuyPlan ->
         st.alloc_buy_plan_words <- st.alloc_buy_plan_words + da;
         st.time_buy_plan_ns <- st.time_buy_plan_ns + dt
       | Strategy_actions_cycle.BuyAmend ->
         st.alloc_buy_amend_words <- st.alloc_buy_amend_words + da;
         st.time_buy_amend_ns <- st.time_buy_amend_ns + dt
       | Strategy_actions_cycle.SellPlan ->
         st.alloc_sell_plan_words <- st.alloc_sell_plan_words + da;
         st.time_sell_plan_ns <- st.time_sell_plan_ns + dt
       | Strategy_actions_cycle.SellPlace ->
         st.alloc_sell_place_words <- st.alloc_sell_place_words + da;
         st.time_sell_place_ns <- st.time_sell_place_ns + dt
       | Strategy_actions_cycle.SfinLatch ->
         st.alloc_sfin_latch_words <- st.alloc_sfin_latch_words + da;
         st.time_sfin_latch_ns <- st.time_sfin_latch_ns + dt
       | Strategy_actions_cycle.SfinSweep ->
         st.alloc_sfin_sweep_words <- st.alloc_sfin_sweep_words + da;
         st.time_sfin_sweep_ns <- st.time_sfin_sweep_ns + dt
       | Strategy_actions_cycle.SfinEnd ->
         st.alloc_sfin_end_words <- st.alloc_sfin_end_words + da;
         st.time_sfin_end_ns <- st.time_sfin_end_ns + dt
       | Strategy_actions_cycle.SellFinalize ->
         st.alloc_sell_finalize_words <- st.alloc_sell_finalize_words + da;
         st.time_sell_finalize_ns <- st.time_sell_finalize_ns + dt
       | Strategy_actions_cycle.Buy ->
         st.alloc_buy_words <- st.alloc_buy_words + da;
         st.time_buy_ns <- st.time_buy_ns + dt
       | Strategy_actions_cycle.Sell ->
         st.alloc_sell_words <- st.alloc_sell_words + da;
         st.time_sell_ns <- st.time_sell_ns + dt))
;;

(** Run [f] while holding the strategy-state mutex (no-op when state is absent). *)
let with_lock ctx f =
  match ctx.cg_state with
  | Some state ->
    Mutex.lock state.mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) f
  | None -> f ()
;;

(** Fine path step 1a: one-time state init (venue config/precision caches) and the
    per-cycle accumulation-buffer refresh. *)
let prepare_init c =
  match c.cg_asset, c.cg_state with
  | Some asset, Some state ->
    if String.equal state.exchange_id ""
    then (
      state.exchange_id <- asset.exchange;
      state.persistence_key
      <- Some
           (Dio_persistence.Base_accumulation_store.key_of
              ~strategy:asset.strategy
              ~symbol:asset.symbol
              ~venue:asset.exchange);
      state.base_accumulation_enabled <- asset.base_accumulation;
      state.sell_levels_enabled <- asset.sell_levels_persistence;
      state.cached_ecfg <- Jac.get_exchange_config asset.exchange;
      state.cached_round_price <- Jac.get_round_price_fn asset.symbol asset.exchange;
      state.cached_price_increment <- Jac.get_price_increment asset.symbol asset.exchange;
      state.cached_qty_increment <- Jac.get_qty_increment_val asset.symbol asset.exchange;
      state.cached_venue_min_qty
      <- (match Jac.get_exchange_module asset.exchange with
          | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
            Option.value (Ex.get_qty_min ~symbol:asset.symbol) ~default:1.0
          | None -> 1.0);
      state.cached_venue_min_notional
      <- Jac.get_min_notional_val asset.symbol asset.exchange;
      state.exchange_reserved_atomic
      <- Some (Jac.get_exchange_reserved_atomic asset.exchange));
    if state.cached_venue_min_notional <= 0.0
    then
      state.cached_venue_min_notional
      <- Jac.get_min_notional_val asset.symbol asset.exchange;
    c.cg_ecfg <- Some state.cached_ecfg;
    state.accumulation_buffer <- asset.accumulation_buffer
  | _ -> ()
;;

(** Fine path step 1c: resolve the effective bid/ask (fall back to the last price).
    Returns whether the cycle should continue. *)
let resolve_book c =
  if Float.is_nan c.cg_price
  then (
    c.cg_continue <- false;
    false)
  else (
    let bid_price, ask_price =
      if (not (Float.is_nan c.cg_bid))
         && c.cg_bid > 0.0
         && (not (Float.is_nan c.cg_ask))
         && c.cg_ask > 0.0
      then c.cg_bid, c.cg_ask
      else c.cg_price, c.cg_price
    in
    c.cg_bid_r <- bid_price;
    c.cg_ask_r <- ask_price;
    c.cg_continue <- true;
    true)
;;

(** Fine path step 1b: low-flag recovery and lot sizing. *)
let prepare_recovery c =
  match c.cg_asset, c.cg_state with
  | Some asset, Some state ->
    let ecfg =
      match c.cg_ecfg with
      | Some e -> e
      | None -> state.cached_ecfg
    in
    c.cg_ecfg <- Some ecfg;
    let lot_qty = Jac.venue_lot_qty state.grid_qty asset.exchange state in
    c.cg_lot_qty <- lot_qty;
    let unnetted_hold =
      Jac.unnetted_sell_hold ~state ~ecfg ~now:c.cg_now ~base_balance_age:c.cg_base_age
    in
    Jac.evaluate_asset_low_recovery
      ~state
      ~now:c.cg_now
      ~base_balance_age:c.cg_base_age
      ~ecfg
      ~asset
      ~asset_balance:c.cg_abal
      ~lot_qty
      ~unnetted_hold;
    Jac.evaluate_capital_low_recovery
      ~state
      ~asset
      ~quote_balance:c.cg_qbal
      ~current_price:c.cg_price
      ~lot_qty
  | _ -> c.cg_continue <- false
;;

(** Fine path step 1 (combined): init, recovery and book resolution. *)
let prepare c =
  prepare_init c;
  prepare_recovery c;
  ignore (resolve_book c)
;;

(** Fine path step 2a: expire stale amend cooldowns. *)
let expire_amend_cooldowns c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset -> Jac.expire_amend_cooldowns ~state ~now:c.cg_now ~asset
  | _ -> ()
;;

(** Fine path step 2b: evict expired ghost-order markers. *)
let evict_ghost_orders c =
  match c.cg_state with
  | Some state -> Jac.evict_ghost_orders ~state ~now:c.cg_now
  | None -> ()
;;

(** Fine path step 2: expire stale cooldowns/ghost markers. *)
let cleanup c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    Jac.cleanup_pending_and_cooldowns ~state ~now:c.cg_now ~asset
  | _ -> ()
;;

(** Fine path step 3: reconcile the open-order feed and publish the scan results. *)
let sync c =
  match c.cg_state, c.cg_asset, c.cg_ecfg with
  | Some state, Some asset, Some ecfg ->
    let ( open_buy_count
        , has_recent_amend_buy
        , locked_in_buys
        , locked_in_sells
        , closest
        , open_p
        , missing_p )
      =
      Jac.sync_open_orders
        ~state
        ~now:c.cg_now
        ~asset
        ~bid_price:c.cg_bid_r
        ~lot_qty:c.cg_lot_qty
        ~iter_open_orders:c.cg_iter
        ~get_open_orders_generation:(fun () -> c.cg_gen)
        ~drain_open_order_changes:c.cg_drain
        ~ecfg
    in
    c.cg_open_buy_count <- open_buy_count;
    c.cg_has_recent_amend_buy <- has_recent_amend_buy;
    c.cg_locked_in_buys <- locked_in_buys;
    c.cg_locked_in_sells <- locked_in_sells;
    c.cg_closest_sell_order <- closest;
    c.cg_open_persisted <- open_p;
    c.cg_missing_persisted <- missing_p
  | _ -> ()
;;

(** Fine path step 4: refresh the maker fee (the refresh cadence is decided by the file;
    this only resolves the current value). *)
let refresh_fee c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    state.maker_fee
    <- (match asset.maker_fee with
        | Some f -> f
        | None ->
          (match
             Fee_cache.get_maker_fee ~exchange:asset.exchange ~symbol:asset.symbol
           with
           | Some cached -> cached
           | None -> 0.0))
  | _ -> ()
;;

(** Fine path step 5: stale-balance guard (mirrors [execute_strategy]'s [is_stale]). *)
let guard c =
  (match c.cg_asset, c.cg_state, c.cg_ecfg with
   | Some _, Some state, Some ecfg ->
     let is_stale =
       ecfg.check_stale_balance && (Float.is_nan c.cg_abal || Float.is_nan c.cg_qbal)
     in
     if is_stale
     then (
       state.last_cycle <- c.cg_cycle;
       c.cg_continue <- false)
     else c.cg_continue <- true
   | _ -> c.cg_continue <- false);
  c.cg_continue
;;

(** Fine path step 6a: TIF-recovery bookkeeping and the oracle-halt buy gate. Returns
    whether the buy branches should run (false halts buy placement but the sell leg still
    runs). Resets [buy_attempted] for the cycle. *)
let buy_gate c =
  c.cg_buy_attempted <- false;
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    let recovery_expired =
      state.tif_recovery_pending && c.cg_now -. state.tif_recovery_since >= 900.0
    in
    if recovery_expired
    then (
      state.tif_recovery_pending <- false;
      Logging.info_f
        ~section:"strategy_cycle_engine"
        "TIF recovery window expired for %s - resuming normal oracle-gated buying"
        asset.symbol);
    let tif_recovery_active =
      state.tif_recovery_pending && c.cg_now -. state.tif_recovery_since < 900.0
    in
    let active = not (c.cg_oracle_halted && not tif_recovery_active) in
    c.cg_buy_active <- active;
    active
  | _ ->
    c.cg_buy_active <- false;
    false
;;

(** Fine path step 6a': reset the per-cycle buy attempt latch and expire the TIF-recovery
    window (the expiry side effect is state maintenance; the buy gate policy itself now
    lives in the strategy file, computed from the published cycle facts). *)
let expire_tif_recovery c =
  c.cg_buy_attempted <- false;
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    if state.tif_recovery_pending && c.cg_now -. state.tif_recovery_since >= 900.0
    then (
      state.tif_recovery_pending <- false;
      Logging.info_f
        ~section:"strategy_cycle_engine"
        "TIF recovery window expired for %s - resuming normal oracle-gated buying"
        asset.symbol)
  | _ -> ()
;;

(** Early path facts, published before the order-feed scan: the book/balance flags that
    [skip_nan_price], [mark_stale], the fee step and the [cycle_ok] gate need. The
    scan-dependent facts follow in [cycle_facts] after [sync]. *)
let early_facts c (t : Strategy_runtime.t) =
  let check_stale_balance =
    match c.cg_ecfg with
    | Some ecfg -> ecfg.check_stale_balance
    | None -> false
  in
  let maker_fee_set =
    match c.cg_state with
    | Some s -> s.maker_fee > 0.0
    | None -> false
  in
  Strategy_runtime.set_platform_bool t Slots.price_nan (Float.is_nan c.cg_price);
  Strategy_runtime.set_platform_bool t Slots.check_stale_balance check_stale_balance;
  Strategy_runtime.set_platform_bool t Slots.asset_balance_nan (Float.is_nan c.cg_abal);
  Strategy_runtime.set_platform_bool t Slots.quote_balance_nan (Float.is_nan c.cg_qbal);
  Strategy_runtime.set_platform_bool t Slots.maker_fee_set maker_fee_set;
  Strategy_runtime.set_platform_bool t Slots.fee_refresh_due (c.cg_cycle land 0x3ff = 0)
;;

(** Fine path step 6a'': publish the raw gate facts the strategy file combines into the
    buy-active condition: the oracle-halt latch and the TIF-recovery latch/timestamp. *)
let cycle_facts c (t : Strategy_runtime.t) =
  let state = c.cg_state in
  let pending, since =
    match state with
    | Some s -> s.tif_recovery_pending, s.tif_recovery_since
    | None -> false, 0.0
  in
  let has_pending_buy =
    match state with
    | Some s ->
      List.exists (fun (_, side, _, _) -> side = Strategy_common.Buy) s.pending_orders
    | None -> false
  in
  let has_tracked_buy, inflight_cancel_buy, inflight_amend_buy, maker_fee_set =
    match state with
    | Some s ->
      ( s.last_buy_order_id <> None
      , s.inflight_cancel_buy
      , s.inflight_amend_buy
      , s.maker_fee > 0.0 )
    | None -> false, false, false, false
  in
  let check_stale_balance =
    match c.cg_ecfg with
    | Some ecfg -> ecfg.check_stale_balance
    | None -> false
  in
  Strategy_runtime.set_platform_bool t Slots.oracle_halted c.cg_oracle_halted;
  Strategy_runtime.set_platform_bool t Slots.tif_recovery_pending pending;
  Strategy_runtime.set_platform_float t Slots.tif_recovery_since since;
  Strategy_runtime.set_platform_bool t Slots.price_nan (Float.is_nan c.cg_price);
  Strategy_runtime.set_platform_bool t Slots.maker_fee_set maker_fee_set;
  Strategy_runtime.set_platform_bool t Slots.fee_refresh_due (c.cg_cycle land 0x3ff = 0);
  Strategy_runtime.set_platform_bool t Slots.check_stale_balance check_stale_balance;
  Strategy_runtime.set_platform_bool t Slots.asset_balance_nan (Float.is_nan c.cg_abal);
  Strategy_runtime.set_platform_bool t Slots.quote_balance_nan (Float.is_nan c.cg_qbal);
  Strategy_runtime.set_platform_bool t Slots.has_pending_buy has_pending_buy;
  Strategy_runtime.set_platform_bool t Slots.has_tracked_buy has_tracked_buy;
  Strategy_runtime.set_platform_bool t Slots.inflight_cancel_buy inflight_cancel_buy;
  Strategy_runtime.set_platform_bool t Slots.inflight_amend_buy inflight_amend_buy;
  Strategy_runtime.set_platform_int t Slots.open_buy_count c.cg_open_buy_count;
  Strategy_runtime.set_platform_bool
    t
    Slots.has_recent_amend_buy
    c.cg_has_recent_amend_buy
;;

(** Generic execution context for overlays. Published on demand by the [market_facts]
    action; it deliberately has no grid-state dependency. Missing feeds publish zero (and
    [funding_valid]/[perp_valid] false) so a consumer can gate on validity instead of
    acting on a zero that looks real. *)
let market_facts c (t : Strategy_runtime.t) =
  let asset = c.cg_asset in
  let symbol =
    match asset with
    | Some a -> a.symbol
    | None -> c.cg_symbol
  in
  let exchange =
    match asset with
    | Some a -> a.exchange
    | None -> ""
  in
  let base =
    match String.split_on_char '/' symbol with
    | b :: _ -> b
    | [] -> symbol
  in
  let setf slot v = Strategy_runtime.set_platform_float t slot v in
  let sets slot v =
    Strategy_runtime.set_platform_slot t slot (Strategy_expr.V_string v)
  in
  let setb slot v = Strategy_runtime.set_platform_bool t slot v in
  let seti slot v = Strategy_runtime.set_platform_int t slot v in
  let fresh ts =
    let age = c.cg_now -. ts in
    age >= 0.0 && age <= 60.0
  in
  let mc = Dio_exchange.Exchange_intf.Registry.get_market_context exchange in
  let funding =
    match mc with
    | Some (module Mc : Dio_exchange.Exchange_intf.Market_context.S) ->
      Mc.get_funding ~coin:base
    | None -> None
  in
  let positions_updated_at =
    match mc with
    | Some (module Mc : Dio_exchange.Exchange_intf.Market_context.S) ->
      Mc.get_positions_updated_at ()
    | None -> None
  in
  let position =
    match mc with
    | Some (module Mc : Dio_exchange.Exchange_intf.Market_context.S) ->
      Mc.get_position ~coin:base
    | None -> None
  in
  let margin =
    match mc with
    | Some (module Mc : Dio_exchange.Exchange_intf.Market_context.S) -> Mc.get_margin ()
    | None -> None
  in
  let reserved =
    match Dio_exchange.Exchange_intf.Registry.get_capital_reservation exchange with
    | Some (module Cr : Dio_exchange.Exchange_intf.Capital_reservation.S) ->
      Cr.get_total ()
    | None -> 0.0
  in
  sets Slots.symbol symbol;
  sets Slots.base_coin base;
  setf Slots.now c.cg_now;
  (match funding with
   | Some f when fresh f.updated_at ->
     setb Slots.funding_valid true;
     setf Slots.funding_apr (f.funding *. 24.0 *. 365.0);
     setf Slots.premium f.premium;
     setf Slots.mark f.mark;
     setf Slots.oracle f.oracle
   | _ ->
     setb Slots.funding_valid false;
     setf Slots.funding_apr 0.0;
     setf Slots.premium 0.0;
     setf Slots.mark 0.0;
     setf Slots.oracle 0.0);
  setb
    Slots.perp_valid
    (match positions_updated_at with
     | Some ts -> fresh ts
     | None -> false);
  (match position with
   | Some p ->
     setf Slots.perp_qty p.qty;
     setf Slots.perp_entry p.entry;
     setf Slots.perp_margin p.margin_used;
     setf Slots.perp_liq (Option.value p.liquidation ~default:0.0);
     (match p.leverage with
      | Some l when l > 0.0 ->
        if Float.abs (l -. c.cg_last_leverage) > 1e-9 && Option.is_some c.cg_carry
        then
          Logging.info_f
            ~section
            "carry leverage %s: %.2fx (position snapshot)"
            c.cg_symbol
            l;
        c.cg_last_leverage <- l
      | _ -> ())
   | None ->
     setf Slots.perp_qty 0.0;
     setf Slots.perp_entry 0.0;
     setf Slots.perp_margin 0.0;
     setf Slots.perp_liq 0.0);
  setf Slots.perp_leverage c.cg_last_leverage;
  (* A position snapshot newer than the entry fill supersedes the provisional fill facts.
     Keep them while the snapshot still has no usable position margin, so the fill-based
     TP can cover the gap. *)
  (match positions_updated_at with
   | Some ts ->
     let fill_at =
       match Strategy_runtime.get_platform t "carry_entry_fill_at" with
       | Strategy_expr.V_float f -> f
       | _ -> 0.0
     in
     let superseded =
       ts > fill_at
       &&
       match position with
       | None -> true
       | Some p -> p.margin_used > 0.0
     in
     if superseded
     then (
       setf Slots.carry_entry_fill_price 0.0;
       setf Slots.carry_entry_fill_qty 0.0;
       setf Slots.carry_entry_fill_at 0.0)
   | None -> ());
  (match margin with
   | Some m when fresh m.updated_at ->
     setf Slots.perp_account_value m.account_value;
     setf Slots.perp_maintenance m.maintenance_margin;
     setf
       Slots.margin_ratio
       (if m.maintenance_margin > 0.0
        then m.account_value /. m.maintenance_margin
        else 0.0)
   | _ ->
     setf Slots.perp_account_value 0.0;
     setf Slots.perp_maintenance 0.0;
     setf Slots.margin_ratio 0.0);
  setf Slots.carry_reserved reserved;
  setf Slots.min_notional (Strategy_venue.get_min_notional_val symbol exchange);
  setf Slots.min_liq_reserve c.cg_min_liq_reserve;
  (match c.cg_carry with
   | Some cs ->
     let close_price, close_kind, close_realized, close_qty =
       match cs.Dio_persistence.Carry_state_store.last_close with
       | Some f -> f.price, f.kind, f.realized, f.qty
       | None -> 0.0, "", 0.0, 0.0
     in
     let spot_price, spot_qty =
       match cs.Dio_persistence.Carry_state_store.last_spot with
       | Some f -> f.price, f.qty
       | None -> 0.0, 0.0
     in
     setf Slots.carry_close_price close_price;
     setf Slots.carry_close_qty close_qty;
     sets Slots.carry_close_kind close_kind;
     setb
       Slots.carry_close_is_recycle
       (String.equal close_kind "carry:recycle:close|ro"
        || String.equal close_kind "carry:drawdown|ro");
     setf Slots.carry_close_realized close_realized;
     setf
       Slots.carry_close_realized_total
       cs.Dio_persistence.Carry_state_store.close_realized_total;
     seti Slots.carry_close_seq cs.Dio_persistence.Carry_state_store.close_seq;
     setf
       Slots.carry_recycle_realized_total
       cs.Dio_persistence.Carry_state_store.recycle_realized_total;
     seti Slots.carry_recycle_seq cs.Dio_persistence.Carry_state_store.recycle_seq;
     setf Slots.carry_spot_price spot_price;
     setf Slots.carry_spot_qty spot_qty;
     seti Slots.carry_spot_seq cs.Dio_persistence.Carry_state_store.spot_seq
   | None ->
     setf Slots.carry_close_price 0.0;
     setf Slots.carry_close_qty 0.0;
     sets Slots.carry_close_kind "";
     setb Slots.carry_close_is_recycle false;
     setf Slots.carry_close_realized 0.0;
     setf Slots.carry_close_realized_total 0.0;
     seti Slots.carry_close_seq 0;
     setf Slots.carry_recycle_realized_total 0.0;
     seti Slots.carry_recycle_seq 0;
     setf Slots.carry_spot_price 0.0;
     setf Slots.carry_spot_qty 0.0;
     seti Slots.carry_spot_seq 0);
  match Jac.get_exchange_module exchange with
  | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
    let staked =
      try Ex.get_staked_balance ~asset:base with
      | _ -> 0.0
    in
    let reserved =
      match c.cg_state with
      | Some s -> s.reserved_base
      | None -> 0.0
    in
    let spot_total =
      try Ex.get_total_balance ~asset:base with
      | _ -> 0.0
    in
    let ledger_base = reserved +. staked in
    let live_base = spot_total +. staked in
    let hedge_base =
      if live_base > 0.0 && live_base < ledger_base then live_base else ledger_base
    in
    (* A carry controller supplies the aggregate across all inventory legs. Without a
       controller (plain grid domain) keep the local-venue figure. *)
    setf
      Slots.accumulated_balance
      (if c.cg_overlay_accumulated >= 0.0 then c.cg_overlay_accumulated else hedge_base);
    setf Slots.spot_base_total spot_total;
    setf
      Slots.usdc_free
      (try Ex.get_tradeable_balance ~asset:"USDC" with
       | _ -> 0.0);
    (* A perp l2Book can be keyed by the bare coin (the usual case) or, for a
       cross-venue controller whose symbol has no matching spot pair, by the controller's
       pair: on Hyperliquid the l2Book subscription for an ADA/XMR domain is the bare
       coin, but the frame is filed under the domain pair, so the coin store stays empty.
       Try the coin, then the domain symbol. For an overlay the domain symbol is
       frequently a *spot* pair (ETH/USDC, SOL/USDC, HYPE/USDC), where the spot book is
       the wrong side of the perp touch by the spot-perp premium; those already resolve
       on the coin store, so the pair fallback only fires for the pair-keyed perp books.
       If neither exists, quote the perp mark so the hedge is still priced off the perp
       rather than left with no quote. *)
    let overlay_controller = c.cg_overlay_accumulated >= 0.0 in
    let fallback_mark =
      match funding with
      | Some f when fresh f.updated_at -> f.mark
      | _ -> 0.0
    in
    (match Ex.get_top_of_book ~symbol:base with
     | Some (bid, _, ask, _) ->
       setf Slots.bid bid;
       setf Slots.ask ask
     | None when overlay_controller && fallback_mark > 0.0 ->
       (* The perp book store is not keyed by the coin for this domain: use the perp
          mark. It is a genuine perp quote, unlike the pair store which can be a spot
          book. *)
       setf Slots.bid fallback_mark;
       setf Slots.ask fallback_mark
     | None ->
       (match Ex.get_top_of_book ~symbol with
        | Some (bid, _, ask, _) ->
          setf Slots.bid bid;
          setf Slots.ask ask
        | None ->
          setf Slots.bid 0.0;
          setf Slots.ask 0.0));
    let spot_enabled = (not overlay_controller) || c.cg_overlay_has_spot_leg in
    let spot_symbol =
      if overlay_controller && c.cg_overlay_spot_symbol <> ""
      then c.cg_overlay_spot_symbol
      else symbol
    in
    if spot_enabled
    then (
      match Ex.get_top_of_book ~symbol:spot_symbol with
      | Some (bid, _, ask, _) ->
        setf Slots.spot_bid bid;
        setf Slots.spot_ask ask
      | None ->
        setf Slots.spot_bid 0.0;
        setf Slots.spot_ask 0.0)
    else (
      setf Slots.spot_bid 0.0;
      setf Slots.spot_ask 0.0)
  | None ->
    setf Slots.accumulated_balance 0.0;
    setf Slots.spot_base_total 0.0;
    setf Slots.usdc_free 0.0;
    setf Slots.bid 0.0;
    setf Slots.ask 0.0;
    setf Slots.spot_bid 0.0;
    setf Slots.spot_ask 0.0
;;

(** Perp coin for a spot pair: [BTC/USDC] -> [BTC]. *)
let perp_coin symbol =
  match String.split_on_char '/' symbol with
  | base :: _ -> base
  | [] -> symbol
;;

(** Generic overlay shadow switch. Set [DIO_OVERLAY_DRY_RUN=1] to log intended overlay
    orders/reservations without pushing or mutating state. The primary strategy is
    unaffected.

    Resolved once at module load, not in a [lazy]: every asset domain calls [dry_run] on
    its own Domain, and OCaml 5's [Lazy.force] is not domain-safe. Concurrent forcing of
    one shared lazy raised [CamlinternalLazy.Undefined] and crashed the overlay domains. *)
let overlay_dry_run =
  match Sys.getenv_opt "DIO_OVERLAY_DRY_RUN" with
  | Some v ->
    let v = String.lowercase_ascii (String.trim v) in
    String.equal v "1" || String.equal v "true" || String.equal v "yes"
  | None -> false
;;

let dry_run () = overlay_dry_run

(** In-flight dedupe key for an overlay placement.

    The logical kind (["carry:entry"], ["carry:recycle:close|ro"], ...) stays the value
    the carry registry uses to match a resting order. The in-flight registry needs a key
    that is unique per venue and instrument: a bare ["carry:entry"] is shared by every
    symbol, so a placement that is still in flight - or whose key was never released -
    suppresses the placement for every other symbol on every venue. *)
let overlay_inflight_key ~dedup_key ~side ~qty ~price ~exchange ~instrument ~reduce_only =
  let base =
    if String.equal dedup_key ""
    then Strategy_common.generate_duplicate_key instrument side qty price
    else dedup_key
  in
  base ^ "|" ^ exchange ^ "|" ^ instrument ^ if reduce_only then "|ro" else ""
;;

(** Generic order placement for overlays. The engine knows nothing about carry policy: the
    strategy file chooses the market, side, size, price, TIF, and reduce-only flag. *)
let order_place
  c
  ~symbol
  ~market
  ~side
  ~qty
  ~price
  ~tif
  ~reduce_only
  ~post_only
  ~dedup_key
  =
  match c.cg_asset with
  | None -> ()
  | Some asset ->
    let instrument =
      match symbol with
      | Some s -> if String.equal market "perp" then perp_coin s else s
      | None ->
        if String.equal market "perp" then perp_coin asset.symbol else asset.symbol
    in
    let min_notional = Strategy_venue.get_min_notional_val instrument asset.exchange in
    (* The venue rounds size DOWN to the instrument's lot, so a quantity sized at exactly
       the floor can arrive below it (SOL perp lot 0.01 vs spot 0.001). Only when that
       floor would fall under the venue minimum do we bump the request up to the next lot;
       otherwise the requested size is left untouched so the venue's own rounding is what
       the strategy asked for. A reduce-only close is left exact: Hyperliquid exempts an
       exact close from the minimum, and bumping it could cross into the opposite side. *)
    let qty =
      if reduce_only
      then qty
      else (
        let floor_qty = Jac.round_qty qty instrument asset.exchange in
        match price with
        | Some p when Float.is_finite p && p > 0.0 && min_notional > 0.0
                      && floor_qty *. p < min_notional ->
          Jac.round_qty_up qty instrument asset.exchange
        | _ -> qty)
    in
    (* A fresh carry entry fill can land before the position snapshot reflects it. The
       strategy would then see a flat position and re-short the full target, doubling the
       position. Hold new entry placements for a short grace window after the fill; the
       position snapshot (or the entry-fill fact) settles within it. *)
    let entry_grace =
      String.equal dedup_key "carry:entry"
      && (not reduce_only)
      && c.cg_last_entry_fill_at > 0.0
      && c.cg_now -. c.cg_last_entry_fill_at <= 2.0
    in
    if entry_grace
    then
      Logging.debug_f
        ~section
        "Overlay entry held %.2fs for the position snapshot after the entry fill (%s)"
        (c.cg_now -. c.cg_last_entry_fill_at)
        instrument
    else if (not (Float.is_finite qty)) || qty <= 0.0
    then
      Logging.warn_f
        ~section
        "Overlay order rejected: invalid qty %.8g for %s"
        qty
        instrument
    else (
      let side =
        if String.equal (String.lowercase_ascii side) "sell"
        then Strategy_common.Sell
        else Strategy_common.Buy
      in
      (* Hyperliquid's $10 floor applies to perps as well as spot. `qty` above is the size
         the venue will accept: untouched when its lot rounding already clears the floor,
         bumped one lot when it would not. A reduce-only close is exempt by the venue's
         exact-close rule. The strategy's ledger/accounting action is a separate step and
         is intentionally retained when the venue placement is skipped: a later, larger
         spot buy can realise the reserved base. *)
      let requested_notional =
        match price with
        | Some p when Float.is_finite p -> qty *. p
        | _ -> 0.0
      in
      if (not reduce_only)
         && min_notional > 0.0
         && requested_notional > 0.0
         && requested_notional < min_notional
      then
        Logging.info_f
          ~section
          "Overlay order below venue minimum notional: %s %s %.8g @ %.8g = %.4f < %.4f; \
           skipping venue placement (ledger/accounting update retained)"
          (Strategy_common.string_of_order_side side)
          instrument
          qty
          (match price with
           | Some p -> p
           | None -> 0.0)
          requested_notional
          min_notional
      else (
        let tif, post_only =
          match String.uppercase_ascii tif with
          | "ALO" -> "GTC", true
          | "GTC" -> "GTC", post_only
          | _ -> "IOC", post_only
        in
        let order_type =
          match price with
          | Some _ -> "limit"
          | None -> "market"
        in
        (* The carry registry matches a resting order to its logical kind
           (["carry:entry"], ["carry:recycle:close|ro"], ...), so [kind_key] stays
           symbol-agnostic. The in-flight dedupe key must not, or one symbol's pending
           placement suppresses every other symbol's until the 60s TTL sweep. *)
        let kind_key = dedup_key ^ if reduce_only then "|ro" else "" in
        let duplicate_key =
          overlay_inflight_key
            ~dedup_key
            ~side:(Strategy_common.string_of_order_side side)
            ~qty
            ~price
            ~exchange:asset.exchange
            ~instrument
            ~reduce_only
        in
        if Strategy_common.InFlightOrders.is_in_flight duplicate_key
        then
          (* The previous attempt for this kind/symbol/venue is still awaiting the venue
             ack. Retrying now can only create a duplicate; the first cycle after the ack
             sees the resting order and amends it. Book updates burst far faster than a
             REST round-trip, so this path is expected - keep it quiet. A key held long
             enough to be a leak is reported by the periodic in-flight sweep instead. *)
          Logging.debug_f ~section "Overlay order already in flight: %s" duplicate_key
        else (
          (* Track every overlay placement, entry sells included. On ack the venue id is
             bound to the kind so [order_replace] can see a tracked order while the
             venue's open-orders scan is still catching up, exactly like the grid's
             tracked buys/sells. *)
          Strategy_common.register_pending_carry_order
            ~dedup_key:duplicate_key
            ~market:
              (if String.equal market "perp"
               then Strategy_common.Carry_perp
               else Strategy_common.Carry_spot)
            ~maker:post_only
            ~kind:kind_key;
          let order =
            Jac.create_place_order
              ~reduce_only
              ~time_in_force:tif
              ~order_type
              ~userref:Strategy_common.strategy_userref_carry
              duplicate_key
              instrument
              side
              qty
              price
              post_only
              Strategy_common.Carry
              asset.exchange
          in
          let describe () =
            Printf.sprintf
              "%s %s %.8g @ %s tif=%s reduce_only=%b key=%s"
              (Strategy_common.string_of_order_side side)
              instrument
              qty
              (match price with
               | Some p -> Printf.sprintf "%.8g" p
               | None -> "market")
              tif
              reduce_only
              duplicate_key
          in
          if dry_run ()
          then Logging.info_f ~section "Overlay DRY RUN: %s" (describe ())
          else (
            let pushed = Jac.push_order ~now:c.cg_now ~manage_state:false order in
            if pushed
            then Logging.info_f ~section "Overlay order: %s" (describe ())
            else
              Logging.debug_f
                ~section
                "Overlay order suppressed (in-flight duplicate or full buffer): %s"
                duplicate_key))))
;;

let order_cancel c ~market ~order_id =
  match c.cg_asset with
  | None -> ()
  | Some asset ->
    let instrument =
      if String.equal market "perp" then perp_coin asset.symbol else asset.symbol
    in
    if dry_run ()
    then Logging.info_f ~section "Overlay DRY RUN: cancel %s %s" instrument order_id
    else (
      let order =
        Jac.create_cancel_order order_id instrument Strategy_common.Carry asset.exchange
      in
      ignore (Jac.push_order ~now:c.cg_now ~manage_state:false order : bool))
;;

(** Place or amend the overlay's resting order to the requested price/size. The engine
    consults the venue's open orders, so the strategy file never has to track an order id
    or reissue on a timer: every book event calls this, and it no-ops when the resting
    order already matches. *)
let order_replace
  c
  ~symbol
  ~market
  ~side
  ~qty
  ~price
  ~tif
  ~reduce_only
  ~post_only
  ~dedup_key
  =
  match c.cg_asset, price with
  | Some asset, Some px ->
    let instrument =
      match symbol with
      | Some s -> if String.equal market "perp" then perp_coin s else s
      | None ->
        if String.equal market "perp" then perp_coin asset.symbol else asset.symbol
    in
    let min_qty = Strategy_venue.get_qty_min_val instrument asset.exchange in
    if (not (Float.is_finite qty)) || qty < 0.0 || (not (Float.is_finite px)) || px <= 0.0
    then
      Logging.debug_f
        ~section
        "Overlay replace skipped: qty=%.8g (min %.8g) price=%.8g for %s"
        qty
        min_qty
        px
        instrument
    else (
      let order_side =
        if String.equal (String.lowercase_ascii side) "sell"
        then Strategy_common.Sell
        else Strategy_common.Buy
      in
      let ex_side =
        match order_side with
        | Strategy_common.Sell -> Dio_exchange.Exchange_intf.Types.Sell
        | Strategy_common.Buy -> Dio_exchange.Exchange_intf.Types.Buy
      in
      (* Buys share the same userref: the spot recycle buy and the perp take-profit are
         both carry buys. Match an existing order only when its booked kind is the same;
         otherwise a spot replace could amend/cancel the take-profit (or vice versa). *)
      let kind_key = dedup_key ^ if reduce_only then "|ro" else "" in
      let tif, post_only =
        match String.uppercase_ascii tif with
        | "ALO" -> "GTC", true
        | "GTC" -> "GTC", post_only
        | _ -> "IOC", post_only
      in
      let ex = Jac.get_exchange_module asset.exchange in
      let rounded =
        match ex with
        | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
          (try Ex.round_price ~symbol:instrument ~price:px with
           | _ -> px)
        | None -> px
      in
      let matching =
        match ex with
        | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
          (* Open orders can be keyed by the spot pair (orderUpdates maps the perp coin
             back to the registered pair) or by the perp coin (the placement injection
             uses the order symbol). Query both and dedupe by order id. *)
          let spot_orders =
            try Ex.get_open_orders ~symbol:asset.symbol with
            | _ -> []
          in
          let perp_orders =
            if String.equal instrument asset.symbol
            then []
            else (
              try Ex.get_open_orders ~symbol:instrument with
              | _ -> [])
          in
          let by_id = Hashtbl.create 8 in
          List.iter
            (fun (o : Dio_exchange.Exchange_intf.Types.open_order) ->
              Hashtbl.replace by_id o.order_id o)
            (spot_orders @ perp_orders);
          Hashtbl.fold (fun _ o acc -> o :: acc) by_id []
          |> List.filter (fun (o : Dio_exchange.Exchange_intf.Types.open_order) ->
            o.user_ref = Some Strategy_common.strategy_userref_carry
            && o.side = ex_side
            && (o.status = Dio_exchange.Exchange_intf.Types.New
                || o.status = Dio_exchange.Exchange_intf.Types.PartiallyFilled)
            &&
            match order_side with
            | Strategy_common.Sell -> true
            | Strategy_common.Buy ->
              (match Strategy_common.lookup_carry_order ~order_id:o.order_id with
               | Some meta -> String.equal meta.com_kind kind_key
               | None -> false))
        | None -> []
      in
      (* The venue floor applies to the order's total size, not the remaining size on an
         amend. `amend_total` bumps that total up one lot only when the venue's own
         round-down would fall under the $10 floor; otherwise the requested total is
         unchanged. A reduce-only close stays exact (venue exact-close exemption). *)
      let min_notional = Strategy_venue.get_min_notional_val instrument asset.exchange in
      let amend_total (o : Dio_exchange.Exchange_intf.Types.open_order) =
        let total = o.cum_qty +. qty in
        if reduce_only
        then total
        else (
          let floor_total = Jac.round_qty total instrument asset.exchange in
          if min_notional > 0.0 && floor_total *. rounded < min_notional
          then Jac.round_qty_up total instrument asset.exchange
          else total)
      in
      let below_min_on_amend (o : Dio_exchange.Exchange_intf.Types.open_order) =
        (not reduce_only) && min_notional > 0.0 && amend_total o *. rounded < min_notional
      in
      let below_min_qty_on_amend (o : Dio_exchange.Exchange_intf.Types.open_order) =
        min_qty > 0.0 && amend_total o < min_qty
      in
      let same_price (o : Dio_exchange.Exchange_intf.Types.open_order) =
        match o.limit_price with
        | Some p -> Float.abs (p -. rounded) <= 1e-9 *. Float.max 1.0 (Float.abs rounded)
        | None -> false
      in
      let same_qty (o : Dio_exchange.Exchange_intf.Types.open_order) =
        Float.abs (o.remaining_qty -. Float.max 0.0 (amend_total o -. o.cum_qty)) <= 1e-9
      in
      let cancel_one
        ?(reason = "target reached")
        (o : Dio_exchange.Exchange_intf.Types.open_order)
        =
        if dry_run ()
        then
          Logging.info_f
            ~section
            "Overlay DRY RUN: cancel %s %s (%s)"
            instrument
            o.order_id
            reason
        else (
          let order =
            Jac.create_cancel_order
              o.order_id
              instrument
              Strategy_common.Carry
              asset.exchange
          in
          ignore (Jac.push_order ~now:c.cg_now ~manage_state:false order : bool))
      in
      (* One live order per overlay kind. A process restart or a cancel+replace race can
         leave a duplicate; keep the first and cancel the rest so two take-profits cannot
         both target the same position (the chart then shows one close, not a doubled
         one). *)
      (match matching with
       | _ :: (_ :: _ as extras) when qty > 0.0 ->
         List.iter (fun o -> cancel_one ~reason:"duplicate" o) extras
       | _ -> ());
      if qty <= 0.0
      then List.iter (fun o -> cancel_one o) matching
      else (
        match matching with
        | o :: _ when same_price o && same_qty o -> ()
        | o :: _ when below_min_on_amend o ->
          Logging.info_f
            ~section
            "Overlay amend below venue minimum notional: %s %s total %.8g @ %.8g = %.4f \
             < %.4f; skipping venue placement (ledger/accounting update retained)"
            (Strategy_common.string_of_order_side order_side)
            instrument
            (amend_total o)
            rounded
            (amend_total o *. rounded)
            min_notional
        | o :: _ when below_min_qty_on_amend o ->
          Logging.info_f
            ~section
            "Overlay amend below venue minimum quantity: %s %s total %.8g < %.8g; \
             skipping venue placement (ledger/accounting update retained)"
            (Strategy_common.string_of_order_side order_side)
            instrument
            (amend_total o)
            min_qty
        | o :: _ ->
          (* Amend the total size, preserving the already-filled part. The caller passes
             the desired remaining quantity. *)
          let total_qty = amend_total o in
          if dry_run ()
          then
            Logging.info_f
              ~section
              "Overlay DRY RUN: amend %s %s to %.8g @ %.8g"
              instrument
              o.order_id
              total_qty
              rounded
          else (
            let order =
              Jac.create_amend_order
                ~dup_key:kind_key
                ~reduce_only
                ~time_in_force:tif
                o.order_id
                instrument
                order_side
                total_qty
                (Some px)
                post_only
                Strategy_common.Carry
                asset.exchange
            in
            ignore (Jac.push_order ~now:c.cg_now ~manage_state:false order : bool))
        | [] ->
          (* No open order matched the venue scan. If the order is already tracked (acked
             but not yet visible), do not place a second one: the grid's
             [has.tracked.buy] equivalent. The next cycle amends once the scan sees it. *)
          let tracked_key =
            overlay_inflight_key
              ~dedup_key
              ~side
              ~qty
              ~price:(Some px)
              ~exchange:asset.exchange
              ~instrument
              ~reduce_only
          in
          if Strategy_common.has_tracked_carry_order ~key:tracked_key
          then
            Logging.debug_f
              ~section
              "Overlay placement held: %s already tracked"
              tracked_key
          else
            order_place
              c
              ~symbol
              ~market
              ~side
              ~qty
              ~price:(Some px)
              ~tif
              ~reduce_only
              ~post_only
              ~dedup_key))
  | _ ->
    order_place
      c
      ~symbol
      ~market
      ~side
      ~qty
      ~price
      ~tif
      ~reduce_only
      ~post_only
      ~dedup_key
;;

let quote_reserve c ~amount =
  match c.cg_asset with
  | None -> ()
  | Some asset ->
    if dry_run ()
    then
      Logging.info_f
        ~section
        "Overlay DRY RUN: reserve %.4f %s quote for %s"
        amount
        asset.exchange
        asset.symbol
    else (
      match
        Dio_exchange.Exchange_intf.Registry.get_capital_reservation asset.exchange
      with
      | Some (module Cr : Dio_exchange.Exchange_intf.Capital_reservation.S) ->
        Cr.set ~owner:("strategy:" ^ asset.symbol) ~amount
      | None -> ())
;;

(** Moves base into the reserved ledger. The caller holds the strategy-state mutex; only
    mark the state dirty here and let the domain's normal flush persist it. *)
let reserve_base c ~qty =
  match c.cg_asset, c.cg_state with
  | Some asset, Some state when Float.is_finite qty && qty <> 0.0 ->
    if dry_run ()
    then
      Logging.info_f ~section "Overlay DRY RUN: reserve %.8f base for %s" qty asset.symbol
    else (
      state.reserved_base <- Float.max 0.0 (state.reserved_base +. qty);
      state.persistence_dirty <- true)
  | _ -> ()
;;

(** Fine path: the stale-balance side effect (record the cycle on the state). The file
    decides when this applies; this only performs the latch. *)
let mark_stale c =
  match c.cg_state with
  | Some state -> state.last_cycle <- c.cg_cycle
  | None -> ()
;;

(** Fine path step 6b: publish the buy-leg branch facts. Returns
    [(pending, effective_count, should_cancel)]. *)
let buy_facts c =
  match c.cg_state with
  | Some state ->
    let pending, effective, should_cancel =
      Jac.buy_leg_facts
        ~state
        ~open_buy_count_from_scan:c.cg_open_buy_count
        ~has_recent_amend_buy:c.cg_has_recent_amend_buy
    in
    c.cg_buy_pending <- pending;
    c.cg_buy_effective_count <- effective;
    c.cg_buy_should_cancel <- should_cancel;
    pending, effective, should_cancel
  | None ->
    c.cg_buy_pending <- false;
    c.cg_buy_effective_count <- 0;
    c.cg_buy_should_cancel <- false;
    false, 0, false
;;

(** Fine path branch: cancel every resting buy (single-buy policy). *)
let buy_cancel c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    let effective_buy_count =
      if state.last_buy_order_id <> None && c.cg_open_buy_count = 0
      then 1
      else c.cg_open_buy_count
    in
    c.cg_buy_effective_count <- effective_buy_count;
    Jac.buy_cancel_excess
      ~state
      ~now:c.cg_now
      ~asset
      ~iter_open_orders:c.cg_iter
      ~cycle:c.cg_cycle
      ~effective_buy_count
  | _ -> ()
;;

(** Fine path branch: compute the fresh-buy plan and publish its branch facts. *)
let buy_place_plan c (t : Strategy_runtime.t) =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    let p =
      Jac.buy_place_plan
        ~state
        ~now:c.cg_now
        ~asset
        ~bid_price:c.cg_bid_r
        ~ask_price:c.cg_ask_r
        ~quote_balance:c.cg_qbal
        ~oracle_halted:c.cg_oracle_halted
        ~cycle:c.cg_cycle
        ~locked_in_buys:c.cg_locked_in_buys
        ~closest_sell_order_initial:c.cg_closest_sell_order
    in
    c.cg_buy_plan <- Some p;
    Strategy_runtime.set_platform_float t Slots.buy_price p.bp_price;
    Strategy_runtime.set_platform_float t Slots.buy_qty p.bp_qty;
    Strategy_runtime.set_platform_float t Slots.buy_quote_needed p.bp_quote_needed;
    Strategy_runtime.set_platform_float t Slots.buy_available p.bp_available;
    Strategy_runtime.set_platform_bool t Slots.buy_balance_ok p.bp_balance_ok;
    Strategy_runtime.set_platform_bool t Slots.buy_capital_low p.bp_capital_low;
    Strategy_runtime.set_platform_bool t Slots.buy_crossing p.bp_crossing;
    Strategy_runtime.set_platform_bool t Slots.buy_quote_nan p.bp_quote_nan;
    Strategy_runtime.set_platform_bool t Slots.buy_cooldown p.bp_cooldown;
    Strategy_runtime.set_platform_bool t Slots.buy_inflight p.bp_inflight
  | _ -> c.cg_buy_plan <- None
;;

let buy_plan_exn c =
  match c.cg_buy_plan with
  | Some p -> p
  | None -> failwith "strategy_cycle_engine: buy branch action without a plan"
;;

(** Fine path branch: send the balanced fresh buy. *)
let buy_place_send c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    let p = buy_plan_exn c in
    c.cg_buy_attempted
    <- Jac.buy_place_send ~state ~now:c.cg_now ~asset ~qty:p.bp_qty ~buy_price:p.bp_price
  | _ -> ()
;;

(** Fine path branch: stale-balance attempt anyway. *)
let buy_place_send_insufficient c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    let p = buy_plan_exn c in
    c.cg_buy_attempted
    <- Jac.buy_place_send_insufficient
         ~state
         ~now:c.cg_now
         ~asset
         ~qty:p.bp_qty
         ~buy_price:p.bp_price
         ~quote_needed:p.bp_quote_needed
         ~available_quote_balance:p.bp_available
  | _ -> ()
;;

(** Fine path branch: latch capital_low. *)
let buy_place_latch_capital_low c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    let p = buy_plan_exn c in
    Jac.buy_place_latch_capital_low
      ~state
      ~now:c.cg_now
      ~asset
      ~quote_needed:p.bp_quote_needed
      ~available_quote_balance:p.bp_available
  | _ -> ()
;;

(** Fine path branch: warn on missing quote balance. *)
let buy_place_warn_quote c =
  match c.cg_asset with
  | Some asset -> Jac.buy_place_warn_quote ~asset
  | None -> ()
;;

(** Fine path branch: place the initial buy; sets [buy_attempted]. *)
let buy_place c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    c.cg_buy_attempted
    <- Jac.buy_place_initial
         ~state
         ~now:c.cg_now
         ~asset
         ~bid_price:c.cg_bid_r
         ~ask_price:c.cg_ask_r
         ~quote_balance:c.cg_qbal
         ~quote_balance_stale:c.cg_quote_stale
         ~oracle_halted:c.cg_oracle_halted
         ~cycle:c.cg_cycle
         ~locked_in_buys:c.cg_locked_in_buys
         ~closest_sell_order_initial:c.cg_closest_sell_order
  | _ -> c.cg_buy_attempted <- false
;;

(** Fine path branch: publish whether a resting/pending sell is tracked. *)
let buy_amend_has_sell c =
  match c.cg_state with
  | Some state ->
    Jac.buy_amend_has_sell ~state ~closest_sell_order_initial:c.cg_closest_sell_order
  | None -> false
;;

(** Fine path branch: with-sell buy amend. *)
let buy_amend_with_sell c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    Jac.buy_amend_with_sell
      ~state
      ~now:c.cg_now
      ~asset
      ~bid_price:c.cg_bid_r
      ~ask_price:c.cg_ask_r
      ~quote_balance:c.cg_qbal
      ~cycle:c.cg_cycle
      ~locked_in_buys:c.cg_locked_in_buys
      ~closest_sell_order_initial:c.cg_closest_sell_order
  | _ -> ()
;;

(** Fine path branch: no-sell buy amend. *)
let buy_amend_no_sell c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    Jac.buy_amend_no_sell
      ~state
      ~now:c.cg_now
      ~asset
      ~bid_price:c.cg_bid_r
      ~ask_price:c.cg_ask_r
      ~quote_balance:c.cg_qbal
      ~cycle:c.cg_cycle
      ~locked_in_buys:c.cg_locked_in_buys
      ~closest_sell_order_initial:c.cg_closest_sell_order
  | _ -> ()
;;

(** Fine path branch: trail/amend the single resting buy. *)
let buy_amend c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    Jac.buy_amend
      ~state
      ~now:c.cg_now
      ~asset
      ~bid_price:c.cg_bid_r
      ~ask_price:c.cg_ask_r
      ~quote_balance:c.cg_qbal
      ~cycle:c.cg_cycle
      ~locked_in_buys:c.cg_locked_in_buys
      ~closest_sell_order_initial:c.cg_closest_sell_order
  | _ -> ()
;;

(** Fine path sell phase 1: derive the sell facts and reconcile the persisted ladder. *)
let sell_prepare c =
  match c.cg_state, c.cg_asset, c.cg_ecfg with
  | Some state, Some asset, Some ecfg ->
    c.cg_sell_pre
    <- Some
         (Jac.sell_leg_prepare
            ~persisted_reconcile:(c.cg_open_persisted, c.cg_missing_persisted)
            ~state
            ~now:c.cg_now
            ~asset
            ~bid_price:c.cg_bid_r
            ~ask_price:c.cg_ask_r
            ~asset_balance:c.cg_abal
            ~buy_attempted:c.cg_buy_attempted
            ~oracle_halted:c.cg_oracle_halted
            ~ecfg
            ~locked_in_sells:c.cg_locked_in_sells
            ~base_balance_age:c.cg_base_age)
  | _ -> c.cg_sell_pre <- None
;;

(** Fine path sell phase 2a: whether a fresh sell is placeable this cycle. *)
let sell_place_should c =
  match c.cg_state, c.cg_sell_pre with
  | Some state, Some pre -> Jac.sell_place_should ~state ~asset_balance:c.cg_abal ~pre
  | _ -> false
;;

(** Fine path sell phase 2b: place the owed/restored sell. *)
let sell_place_body c =
  match c.cg_state, c.cg_asset, c.cg_ecfg, c.cg_sell_pre with
  | Some state, Some asset, Some ecfg, Some pre ->
    Jac.sell_place_body
      ~state
      ~now:c.cg_now
      ~asset
      ~bid_price:c.cg_bid_r
      ~ask_price:c.cg_ask_r
      ~buy_attempted:c.cg_buy_attempted
      ~ecfg
      ~pre
  | _ -> ()
;;

(** Fine path sell phase 2: the gated placement block. *)
let sell_place c =
  match c.cg_state, c.cg_asset, c.cg_ecfg, c.cg_sell_pre with
  | Some state, Some asset, Some ecfg, Some pre ->
    Jac.sell_leg_place
      ~state
      ~now:c.cg_now
      ~asset
      ~bid_price:c.cg_bid_r
      ~ask_price:c.cg_ask_r
      ~asset_balance:c.cg_abal
      ~buy_attempted:c.cg_buy_attempted
      ~ecfg
      ~pre
  | _ -> ()
;;

(** Fine path sell phase 3 facts: publish the excess-sweep gate. *)
let sell_finalize_facts c (t : Strategy_runtime.t) =
  match c.cg_sell_pre with
  | Some pre ->
    let remaintain =
      match c.cg_ecfg with
      | Some e -> e.remaintain_expired_sells
      | None -> false
    in
    let just_filled, resuming, active_sell =
      match c.cg_state with
      | Some st ->
        st.just_filled_buy, st.resuming_after_balance_flag, Jac.has_active_sell st
      | None -> false, false, false
    in
    let balance_fresh =
      match c.cg_base_age with
      | Some age -> age <= Platform_accounting.sweep_max_balance_age_s
      | None -> true
    in
    Strategy_runtime.set_platform_bool t Slots.remaintain_expired_sells remaintain;
    Strategy_runtime.set_platform_bool
      t
      Slots.sell_missing_empty
      (!(pre.sp_missing_after_reconcile) = []);
    Strategy_runtime.set_platform_bool t Slots.just_filled_buy just_filled;
    Strategy_runtime.set_platform_bool t Slots.resuming_after_balance resuming;
    Strategy_runtime.set_platform_bool t Slots.buy_attempted c.cg_buy_attempted;
    Strategy_runtime.set_platform_bool t Slots.sell_pushed !(pre.sp_sell_pushed);
    Strategy_runtime.set_platform_bool t Slots.has_active_sell active_sell;
    Strategy_runtime.set_platform_bool t Slots.balance_fresh balance_fresh
  | None -> ()
;;

(** Fine path sell phase 3a: retry-latch bookkeeping. *)
let sell_finalize_latch c =
  match c.cg_state, c.cg_asset, c.cg_ecfg, c.cg_sell_pre with
  | Some state, Some asset, Some ecfg, Some pre ->
    Jac.sell_leg_finalize_latch
      ~state
      ~now:c.cg_now
      ~asset
      ~asset_balance:c.cg_abal
      ~buy_attempted:c.cg_buy_attempted
      ~ecfg
      ~pre
  | _ -> ()
;;

(** Fine path sell phase 3b: excess sweep (the file gates when it runs). *)
let sell_excess_sweep_phase c =
  match c.cg_state, c.cg_asset, c.cg_ecfg, c.cg_sell_pre with
  | Some state, Some asset, Some ecfg, Some pre ->
    Jac.sell_excess_sweep_phase ~state ~now:c.cg_now ~asset ~pre ~ecfg
  | _ -> ()
;;

(** Fine path sell phase 3c: clear the resume marker. *)
let sell_finalize_end c =
  match c.cg_state with
  | Some state -> Jac.sell_finalize_end ~state
  | None -> ()
;;

(** Fine path sell phase 3: retry-latch bookkeeping, consumption, excess sweep. *)
let sell_finalize c =
  match c.cg_state, c.cg_asset, c.cg_ecfg, c.cg_sell_pre with
  | Some state, Some asset, Some ecfg, Some pre ->
    Jac.sell_leg_finalize
      ~state
      ~now:c.cg_now
      ~asset
      ~asset_balance:c.cg_abal
      ~buy_attempted:c.cg_buy_attempted
      ~ecfg
      ~base_balance_age:c.cg_base_age
      ~pre
  | _ -> ()
;;

let value_string = function
  | Strategy_expr.V_string s -> s
  | V_int i -> string_of_int i
  | V_float f -> string_of_float f
  | _ -> ""
;;

let value_float = function
  | Strategy_expr.V_float f -> f
  | V_int i -> float_of_int i
  | _ -> 0.0
;;

let value_string_opt = function
  | Strategy_expr.V_string s -> Some s
  | _ -> None
;;

(** Fine path: dispatch an order-lifecycle event to the reference handlers. The
    interpreter routes fill/cancel/ack/amend (and the REST-path variants) through this
    when the entry is file-bound, so the strategy file owns the event surface. The
    reference handlers remain the bodies; they lock [state.mutex] themselves, so this must
    run OUTSIDE [with_lock]. *)
let on_event c (ev : Strategy_runtime.event) =
  match c.cg_state with
  | Some _ ->
    let f k = List.assoc_opt k ev.Strategy_runtime.ev_fields in
    let obs =
      { Strategy_trace.ev_kind = ev.Strategy_runtime.ev_kind
      ; ev_now =
          (match f "now" with
           | Some v -> value_float v
           | None -> c.cg_now)
      ; ev_order_id =
          (match f "order_id" with
           | Some v -> value_string v
           | None -> "")
      ; ev_new_order_id =
          (match f "new_order_id" with
           | Some v -> value_string v
           | None -> "")
      ; ev_side =
          (match f "side" with
           | Some v -> value_string v
           | None -> "")
      ; ev_price =
          (match f "price" with
           | Some v -> value_float v
           | None -> 0.0)
      ; ev_qty =
          (match f "qty" with
           | Some v -> value_float v
           | None -> 0.0)
      ; ev_cl_ord_id =
          (match f "cl_ord_id" with
           | Some v -> value_string_opt v
           | None -> None)
      ; ev_reason =
          (match f "reason" with
           | Some v -> value_string v
           | None -> "")
      }
    in
    Jac.apply_event c.cg_symbol obs
  | None -> ()
;;
