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

type ctx =
  { mutable cg_asset : Types.trading_config option
  ; mutable cg_state : Types.strategy_state option
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
let measure c (phase : Strategy_actions_cycle.phase) f =
  if not c.cg_profile
  then f ()
  else (
    let a0 = int_of_float (Gc.minor_words ()) in
    let t0 = Monotonic_clock.now_ns () in
    f ();
    match c.cg_state with
    | None -> ()
    | Some st ->
      let da = int_of_float (Gc.minor_words ()) - a0 in
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
let early_facts c sink =
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
  sink "price_nan" (Strategy_expr.V_bool (Float.is_nan c.cg_price));
  sink "check_stale_balance" (Strategy_expr.V_bool check_stale_balance);
  sink "asset_balance_nan" (Strategy_expr.V_bool (Float.is_nan c.cg_abal));
  sink "quote_balance_nan" (Strategy_expr.V_bool (Float.is_nan c.cg_qbal));
  sink "maker_fee_set" (Strategy_expr.V_bool maker_fee_set);
  sink "fee_refresh_due" (Strategy_expr.V_bool (c.cg_cycle land 0x3ff = 0))
;;

(** Fine path step 6a'': publish the raw gate facts the strategy file combines into the
    buy-active condition: the oracle-halt latch and the TIF-recovery latch/timestamp. *)
let cycle_facts c sink =
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
  sink "oracle_halted" (Strategy_expr.V_bool c.cg_oracle_halted);
  sink "tif_recovery_pending" (Strategy_expr.V_bool pending);
  sink "tif_recovery_since" (Strategy_expr.V_float since);
  sink "price_nan" (Strategy_expr.V_bool (Float.is_nan c.cg_price));
  sink "maker_fee_set" (Strategy_expr.V_bool maker_fee_set);
  sink "fee_refresh_due" (Strategy_expr.V_bool (c.cg_cycle land 0x3ff = 0));
  sink "check_stale_balance" (Strategy_expr.V_bool check_stale_balance);
  sink "asset_balance_nan" (Strategy_expr.V_bool (Float.is_nan c.cg_abal));
  sink "quote_balance_nan" (Strategy_expr.V_bool (Float.is_nan c.cg_qbal));
  sink "has_pending_buy" (Strategy_expr.V_bool has_pending_buy);
  sink "has_tracked_buy" (Strategy_expr.V_bool has_tracked_buy);
  sink "inflight_cancel_buy" (Strategy_expr.V_bool inflight_cancel_buy);
  sink "inflight_amend_buy" (Strategy_expr.V_bool inflight_amend_buy);
  sink "open_buy_count" (Strategy_expr.V_int c.cg_open_buy_count);
  sink "has_recent_amend_buy" (Strategy_expr.V_bool c.cg_has_recent_amend_buy)
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
let buy_place_plan c =
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
    [ "buy_price", Strategy_expr.V_float p.bp_price
    ; "buy_qty", Strategy_expr.V_float p.bp_qty
    ; "buy_quote_needed", Strategy_expr.V_float p.bp_quote_needed
    ; "buy_available", Strategy_expr.V_float p.bp_available
    ; "buy_balance_ok", Strategy_expr.V_bool p.bp_balance_ok
    ; "buy_capital_low", Strategy_expr.V_bool p.bp_capital_low
    ; "buy_crossing", Strategy_expr.V_bool p.bp_crossing
    ; "buy_quote_nan", Strategy_expr.V_bool p.bp_quote_nan
    ; "buy_cooldown", Strategy_expr.V_bool p.bp_cooldown
    ; "buy_inflight", Strategy_expr.V_bool p.bp_inflight
    ]
  | _ ->
    c.cg_buy_plan <- None;
    []
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
let sell_finalize_facts c =
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
    [ "remaintain_expired_sells", Strategy_expr.V_bool remaintain
    ; "sell_missing_empty", Strategy_expr.V_bool (!(pre.sp_missing_after_reconcile) = [])
    ; "just_filled_buy", Strategy_expr.V_bool just_filled
    ; "resuming_after_balance", Strategy_expr.V_bool resuming
    ; "buy_attempted", Strategy_expr.V_bool c.cg_buy_attempted
    ; "sell_pushed", Strategy_expr.V_bool !(pre.sp_sell_pushed)
    ; "has_active_sell", Strategy_expr.V_bool active_sell
    ; "balance_fresh", Strategy_expr.V_bool balance_fresh
    ]
  | None -> []
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
