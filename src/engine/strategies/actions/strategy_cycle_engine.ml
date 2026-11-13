(** Config-driven grid engine context (milestone 3; fine-orchestration decomposition).

    Holds the per-cycle strategy inputs and implements the reference actions the shipped
    strategy file composes ([prepare]/[cleanup]/[sync]/[refresh_fee]/[guard]/[buy_*]/
    [sell_*]), each by calling the reference [execute_strategy] sub-functions with these
    inputs, so a config-driven grid replicates by construction. Enabled whenever an entry
    is bound to a strategy file; the mutable fields are updated in place each cycle (no
    per-cycle allocation). Shared by the domain loop and the offline candidate replay, so
    the candidate interpreter is exercised on exactly the wiring the live loop uses. The
    caller runs on the symbol's asset domain, which is the sole writer of this state
    (order callbacks are queued as lifecycle events and drained there); the sub-functions
    do not lock. *)

module Jac = Strategy_api
module Types = Strategy_state
module Slots = Strategy_fact_slots

let section = "strategy_cycle_engine"

type ctx =
  { mutable cg_asset : Types.trading_config option
  ; mutable cg_state : Types.strategy_state option
  ; mutable cg_carry : Dio_persistence.Overlay_state_store.t option
  ; mutable cg_reserve_share : float
  (** Aggregate hedgeable inventory across every leg of this controller. [< 0] means "not
      an overlay controller"; [market_facts] then falls back to the local venue figure. *)
  ; mutable cg_overlay_accumulated : float
  (** Accumulated inventory for a generic overlay instance: its owner leg plus any
      declared [inventory] legs, measured with the same [Overlay_inventory] rule a carry
      controller uses. [-1] means unset. *)
  ; mutable cg_inventory_override : float
  (** Oracle publish generation [cg_parent_*] were read at. The parent decision is a
      lock-free hashtable read but hashing the (exchange, symbol) key allocates, so the
      overlay refreshes at most once per oracle pass instead of once per cycle. *)
  ; mutable cg_parent_gen : int
  (** Parent grid cannot place its next buy (capital oracle INACTIVE). *)
  ; mutable cg_parent_unfunded : bool
  (** Parent's next buy size in base units, inherited verbatim by the outlier fallback. *)
  ; mutable cg_parent_order_qty : float
  (** True when the controller has a Hyperliquid spot leg, so the recycle base buy is
      active. When false the recycle stays 100% quote. *)
  ; mutable cg_overlay_has_spot_leg : bool
  (** Symbol of the Hyperliquid spot leg whose book the recycle buy targets. *)
  ; mutable cg_overlay_spot_symbol : string
  ; mutable cg_last_leverage : float
  (** Venue time of the last carry entry (perp sell) fill, or 0.0. A fresh entry fill can
      arrive before the position snapshot reflects it; [order_place] holds a new entry for
      a short grace window so the cycle cannot re-short the full target. *)
  ; mutable cg_last_entry_fill_at : float
  (** Arrival time of the last perp execution whose effect the venue position snapshot has
      not yet absorbed, or 0.0. While set, [order_place]/[order_replace] hold reduce-only
      orders: the facts are still computed from the pre-fill position, so a take-profit
      that just closed the short would be re-sent at full size into a flat position (the
      venue rejects it with "Reduce only order would increase position"). The hold lifts
      as soon as a position snapshot newer than the fill is published - never by a clock. *)
  ; mutable cg_perp_fill_at : float
  ; mutable cg_price : float
  ; mutable cg_bid : float
  ; mutable cg_ask : float
  ; mutable cg_abal : float
  ; mutable cg_qbal : float
  ; mutable cg_now : float
  ; mutable cg_cycle : int
  (** Controller key ("venue|symbol") whose [accumulated_profit] this domain drains. Empty
      when the domain is not the grid leg of a carry controller. *)
  ; mutable cg_credit_key : string
  (** Persisted accumulation entry that receives this overlay's recycled profit (the parent
      grid's "{strategy}:{symbol}:{venue}" key). Empty when there is no parent grid. *)
  ; mutable cg_credit_store_key : string
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
  ; cg_sell_pre : Jac.sell_pre
  ; mutable cg_sell_pre_valid : bool
  ; mutable cg_symbol : string
  ; mutable cg_base_symbol : string
  ; mutable cg_exchange_module : (module Dio_exchange.Exchange_intf.S) option
  ; mutable cg_adopt_untagged_orders : bool
  (** True for an overlay whose market no other strategy trades on a venue that drops the
      userref. A same-side order with a matching venue reduce-only flag is then this
      book's own resting order, adopted rather than duplicated after a restart. *)
  (* Section timestamps written by [market_facts] when the owner asks for a break-down:
     the cutover path times the fact refresh because its composition is not obvious from
     the outside. Off by default, so the interpreter path pays one branch. *)
  ; mutable cg_instrument : bool
  ; cg_marks : int array
  ; cg_alloc_marks : int array
  (* Venue capabilities resolved once by the owner. Registry lookups return a first-class
     module, which allocates a block per call, and [market_facts] needs three of them per
     tick. [None] means "not resolved here": the registry is used instead. *)
  ; mutable cg_mc : (module Dio_exchange.Exchange_intf.Market_context.S) option
  ; mutable cg_capital : (module Dio_exchange.Exchange_intf.Capital_reservation.S) option
  ; mutable cg_key_coin : string
  (** Prebuilt [exchange|coin] key for the funding and position stores, and the coin it was
      built for. Both tables are keyed by that string, so a per-event reader that rebuilds
      it allocates on every tick. Empty means "not resolved": build from [cg_base_symbol]. *)
  ; mutable cg_key_coin_for : string
  ; mutable cg_min_notional : float
  ; mutable cg_min_notional_for : string
  (** Venue minimum notional per instrument. It is static metadata, but the accessor is a
      registry lookup returning a first-class module, and [market_facts] needs it on every
      event. *)
  ; mutable cg_risk : Dio_exchange.Exchange_intf.Market_context.risk option
  ; mutable cg_risk_for : string
  (** Per-coin risk metadata (maintenance fraction, max leverage). Venue metadata, not live
      state: it only changes when the venue relists the instrument. *)
  ; cg_tob : float array
  (** Reused top-of-book buffer for {!get_top_of_book_into}: the accessor that returns an
      option tuple allocates one [Some] per read, and the cycle reads the book per event. *)
  ; mutable cg_venue : string
  (** Exchange the two resolved capabilities belong to; a different one invalidates
      them. *)
  ; mutable cg_venue_cached : bool
  ; mutable cg_quote_free : (unit -> float) option
  (** Generation-cached tradeable quote figure for the book's quote asset, resolved once
      by the owner. [market.facts] reads it instead of the raw venue figure, whose
      open-buy sweep walks every quoting symbol's order list on every tick. *)
  }

let create () =
  { cg_asset = None
  ; cg_state = None
  ; cg_carry = None
  ; cg_reserve_share = 0.0
  ; cg_instrument = false
  ; cg_marks = Array.make 8 0
  ; cg_alloc_marks = Array.make 8 0
  ; cg_mc = None
  ; cg_capital = None
  ; cg_key_coin = ""
  ; cg_key_coin_for = ""
  ; cg_min_notional = nan
  ; cg_min_notional_for = ""
  ; cg_risk = None
  ; cg_risk_for = ""
  ; cg_tob = Array.make 4 0.0
  ; cg_venue = ""
  ; cg_venue_cached = false
  ; cg_quote_free = None
  ; cg_overlay_accumulated = -1.0
  ; cg_inventory_override = -1.0
  ; cg_parent_gen = -1
  ; cg_parent_unfunded = false
  ; cg_parent_order_qty = 0.0
  ; cg_overlay_has_spot_leg = false
  ; cg_overlay_spot_symbol = ""
  ; cg_last_leverage = 0.0
  ; cg_last_entry_fill_at = 0.0
  ; cg_perp_fill_at = 0.0
  ; cg_price = nan
  ; cg_bid = nan
  ; cg_ask = nan
  ; cg_abal = nan
  ; cg_qbal = nan
  ; cg_now = 0.0
  ; cg_cycle = 0
  ; cg_credit_key = ""
  ; cg_credit_store_key = ""
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
  ; cg_sell_pre = Jac.fresh_sell_pre ()
  ; cg_sell_pre_valid = false
  ; cg_symbol = ""
  ; cg_base_symbol = ""
  ; cg_exchange_module = None
  ; cg_adopt_untagged_orders = false
  }
;;

(** Coarse phases the cycle engine attributes time and allocation to (the dashboard's
    STRATEGY breakdown). *)
type phase =
  | Preamble
  | Facts
  | Cleanup
  | Sync
  | Buy
  | BuyPlan
  | BuyAmend
  | Sell
  | SellPlan
  | SellPlace
  | SellFinalize
  | SfinLatch
  | SfinSweep
  | SfinEnd
;;

(** Per-cycle phase attribution. Cheap on the hot path: when profiling is off it just runs
    [f]; when on it reads the domain-local minor-word counter and monotonic clock and
    accumulates into the strategy-state scratch fields the dashboard reads. *)
let[@inline] measure c (phase : phase) (local_ (f : unit -> unit)) =
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
       | Preamble ->
         st.alloc_preamble_words <- st.alloc_preamble_words + da;
         st.time_preamble_ns <- st.time_preamble_ns + dt
       | Facts ->
         st.alloc_facts_words <- st.alloc_facts_words + da;
         st.time_facts_ns <- st.time_facts_ns + dt
       | Cleanup ->
         st.alloc_cleanup_words <- st.alloc_cleanup_words + da;
         st.time_cleanup_ns <- st.time_cleanup_ns + dt
       | Sync ->
         st.alloc_sync_words <- st.alloc_sync_words + da;
         st.time_sync_ns <- st.time_sync_ns + dt
       | BuyPlan ->
         st.alloc_buy_plan_words <- st.alloc_buy_plan_words + da;
         st.time_buy_plan_ns <- st.time_buy_plan_ns + dt
       | BuyAmend ->
         st.alloc_buy_amend_words <- st.alloc_buy_amend_words + da;
         st.time_buy_amend_ns <- st.time_buy_amend_ns + dt
       | SellPlan ->
         st.alloc_sell_plan_words <- st.alloc_sell_plan_words + da;
         st.time_sell_plan_ns <- st.time_sell_plan_ns + dt
       | SellPlace ->
         st.alloc_sell_place_words <- st.alloc_sell_place_words + da;
         st.time_sell_place_ns <- st.time_sell_place_ns + dt
       | SfinLatch ->
         st.alloc_sfin_latch_words <- st.alloc_sfin_latch_words + da;
         st.time_sfin_latch_ns <- st.time_sfin_latch_ns + dt
       | SfinSweep ->
         st.alloc_sfin_sweep_words <- st.alloc_sfin_sweep_words + da;
         st.time_sfin_sweep_ns <- st.time_sfin_sweep_ns + dt
       | SfinEnd ->
         st.alloc_sfin_end_words <- st.alloc_sfin_end_words + da;
         st.time_sfin_end_ns <- st.time_sfin_end_ns + dt
       | SellFinalize ->
         st.alloc_sell_finalize_words <- st.alloc_sell_finalize_words + da;
         st.time_sell_finalize_ns <- st.time_sell_finalize_ns + dt
       | Buy ->
         st.alloc_buy_words <- st.alloc_buy_words + da;
         st.time_buy_ns <- st.time_buy_ns + dt
       | Sell ->
         st.alloc_sell_words <- st.alloc_sell_words + da;
         st.time_sell_ns <- st.time_sell_ns + dt))
;;

(** The asset domain is the sole writer for its symbol, so the cycle needs no lock. *)
let with_lock _ctx f = f ()

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
      c.cg_base_symbol
      <- (match String.index_opt asset.symbol '/' with
          | Some i -> String.sub asset.symbol 0 i
          | None -> asset.symbol);
      c.cg_exchange_module <- Jac.get_exchange_module asset.exchange;
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
    (match c.cg_ecfg with
     | Some e when e == state.cached_ecfg -> ()
     | _ -> c.cg_ecfg <- Some state.cached_ecfg);
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
    (match c.cg_ecfg with
     | Some e when e == ecfg -> ()
     | _ -> c.cg_ecfg <- Some ecfg);
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
      ~now:c.cg_now
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
    <- Strategy_venue.resolve_fee_rate
         ~configured:asset.maker_fee
         ~cached:(Fee_cache.get_maker_fee ~exchange:asset.exchange ~symbol:asset.symbol)
         ~current:state.maker_fee;
    state.taker_fee
    <- Strategy_venue.resolve_fee_rate
         ~configured:asset.taker_fee
         ~cached:(Fee_cache.get_taker_fee ~exchange:asset.exchange ~symbol:asset.symbol)
         ~current:state.taker_fee
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

(** Applies drained credits in order. A credit at or below [carry_credit_seq] was already
    applied (restart replay) and is dropped. Tail-recursive and allocation-free: this runs
    from [market_facts] on every overlay tick. *)
let rec apply_credits (s : Types.strategy_state) applied = function
  | [] -> applied
  | (seq, qty) :: rest ->
    if seq <= 0 || seq > s.carry_credit_seq
    then (
      s.accumulated_profit <- s.accumulated_profit +. qty;
      if seq > s.carry_credit_seq then s.carry_credit_seq <- seq;
      apply_credits s true rest)
    else apply_credits s applied rest
;;

(** Moves pending overlay credits into the grid's state. [Accum_credit.drain] is
    destructive, so [cycle_facts] and [market_facts] cannot double-count. *)
let drain_credit c =
  match c.cg_state with
  | Some s when c.cg_credit_key <> "" ->
    if apply_credits s false (Accum_credit.drain ~key:c.cg_credit_key)
    then s.persistence_dirty <- true
  | _ -> ()
;;

(** Fine path step 6a'': publish the raw gate facts the strategy file combines into the
    buy-active condition: the oracle-halt latch and the TIF-recovery latch/timestamp. *)
let rec has_pending_buy = function
  | [] -> false
  | (_, side, _, _) :: rest -> side = Strategy_common.Buy || has_pending_buy rest
;;

let cycle_facts c (t : Strategy_runtime.t) =
  drain_credit c;
  let state = c.cg_state in
  (* The grid's reserve subtraction is [reserved_base - staked_base], so staked base must
     be refreshed on the grid path too. It used to be set only by [market_facts] (overlay
     path), leaving the grid with staked=0 and treating an over-reserved book as fully
     unsellable even when the venue's staked + tradeable holdings covered the reserve. *)
  (match c.cg_asset, state with
   | Some asset, Some s ->
     (* Cache the base symbol and venue module on first use. [prepare_init] normally does
        this; direct callers (tests, replay) may call [cycle_facts] without it. *)
     let base, module_opt =
       if String.equal c.cg_base_symbol ""
       then (
         let base =
           match String.index_opt asset.symbol '/' with
           | Some i -> String.sub asset.symbol 0 i
           | None -> asset.symbol
         in
         let module_opt = Jac.get_exchange_module asset.exchange in
         c.cg_base_symbol <- base;
         c.cg_exchange_module <- module_opt;
         base, module_opt)
       else c.cg_base_symbol, c.cg_exchange_module
     in
     let staked =
       match module_opt with
       | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
         (try Ex.get_staked_balance ~asset:base with
          | _ -> 0.0)
       | None -> 0.0
     in
     s.staked_base <- Float.max 0.0 staked
   | _ -> ());
  let pending, since =
    match state with
    | Some s -> s.tif_recovery_pending, s.tif_recovery_since
    | None -> false, 0.0
  in
  let has_pending_buy =
    match state with
    | Some s -> has_pending_buy s.pending_orders
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
let market_facts_io (io : Strategy_runtime.facts_io) c =
  drain_credit c;
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
    if String.equal c.cg_base_symbol ""
    then (
      (* One [String.sub]; the overlay cycle calls this on every tick and
         [String.split_on_char] builds a list plus a substring per call. *)
      let base =
        match String.index_opt symbol '/' with
        | Some i -> String.sub symbol 0 i
        | None -> symbol
      in
      c.cg_base_symbol <- base;
      base)
    else c.cg_base_symbol
  in
  let setf = io.Strategy_runtime.io_f in
  let setstr = io.io_s in
  let setb = io.io_b in
  let seti = io.io_i in
  let fresh ts =
    let age = c.cg_now -. ts in
    age >= 0.0 && age <= 60.0
  in
  let mark i =
    if c.cg_instrument
    then (
      c.cg_marks.(i) <- Monotonic_clock.now_ns ();
      c.cg_alloc_marks.(i) <- Monotonic_clock.minor_words ())
  in
  mark 0;
  let mc =
    if c.cg_venue_cached && String.equal c.cg_venue exchange
    then c.cg_mc
    else (
      let mc = Dio_exchange.Exchange_intf.Registry.get_market_context exchange in
      let cap = Dio_exchange.Exchange_intf.Registry.get_capital_reservation exchange in
      c.cg_mc <- mc;
      c.cg_capital <- cap;
      c.cg_exchange_module <- Jac.get_exchange_module exchange;
      c.cg_venue <- exchange;
      c.cg_venue_cached <- true;
      mc)
  in
  (* The store is keyed by "exchange|coin". Rebuilding that string per read allocated five
     times per event; keep it, keyed by the coin it was built for. *)
  let key_coin =
    if String.equal c.cg_key_coin_for base
    then c.cg_key_coin
    else (
      let k = Dio_exchange.Market_context.key_of exchange base in
      c.cg_key_coin <- k;
      c.cg_key_coin_for <- base;
      k)
  in
  let funding =
    match mc with
    | Some _ -> Dio_exchange.Market_context.get_funding_key key_coin
    | None -> None
  in
  let positions_updated_at =
    match mc with
    | Some (module Mc : Dio_exchange.Exchange_intf.Market_context.S) ->
      (* Prefer the coin's own snapshot time: a coin published by a builder-dex poller must
         not inherit the main dex's freshness, and vice versa. *)
      (match Dio_exchange.Market_context.get_position_updated_at ~exchange ~coin:base with
       | Some ts -> Some ts
       | None -> Mc.get_positions_updated_at ())
    | None -> None
  in
  let position =
    match mc with
    | Some _ -> Dio_exchange.Market_context.get_position_key key_coin
    | None -> None
  in
  let margin =
    match mc with
    | Some (module Mc : Dio_exchange.Exchange_intf.Market_context.S) -> Mc.get_margin ()
    | None -> None
  in
  let risk =
    match mc with
    | None -> None
    | Some (module Mc : Dio_exchange.Exchange_intf.Market_context.S) ->
      if String.equal c.cg_risk_for base
      then c.cg_risk
      else (
        let r = Mc.get_risk ~coin:base in
        c.cg_risk_for <- base;
        c.cg_risk <- r;
        r)
  in
  mark 1;
  let reserved =
    match c.cg_capital with
    | Some (module Cr : Dio_exchange.Exchange_intf.Capital_reservation.S) ->
      Cr.get_total ()
    | None ->
    match Dio_exchange.Exchange_intf.Registry.get_capital_reservation exchange with
    | Some (module Cr : Dio_exchange.Exchange_intf.Capital_reservation.S) ->
      Cr.get_total ()
    | None -> 0.0
  in
  mark 2;
  setstr Slots.symbol symbol;
  setstr Slots.base_coin base;
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
  mark 6;
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
     let fill_at = io.Strategy_runtime.io_get_f Slots.carry_entry_fill_at in
     let superseded =
       ts > fill_at
       &&
       match position with
       | None ->
         (* A flat snapshot received shortly after the entry fill can predate the fill's
            effect (the feed timestamps arrival, not venue state). Keep the provisional
            fill facts until the next snapshot interval has passed; otherwise the overlay
            sees a flat position again and re-shorts the target. *)
         fill_at <= 0.0 || ts -. fill_at > 5.0
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
     setf Slots.perp_withdrawable m.withdrawable;
     setf
       Slots.margin_ratio
       (if m.maintenance_margin > 0.0
        then m.account_value /. m.maintenance_margin
        else 0.0)
   | _ ->
     setf Slots.perp_account_value 0.0;
     setf Slots.perp_maintenance 0.0;
     setf Slots.perp_withdrawable 0.0;
     setf Slots.margin_ratio 0.0);
  (match risk with
   | Some r ->
     setf Slots.perp_maintenance_frac r.maintenance_frac;
     (match r.max_leverage with
      | Some l -> setf Slots.perp_max_leverage l
      | None -> setf Slots.perp_max_leverage 0.0)
   | None ->
     setf Slots.perp_maintenance_frac 0.0;
     setf Slots.perp_max_leverage 0.0);
  setf Slots.overlay_reserved reserved;
  setf
    Slots.min_notional
    (if String.equal c.cg_min_notional_for symbol && Float.is_finite c.cg_min_notional
     then c.cg_min_notional
     else (
       let v = Strategy_venue.get_min_notional_val symbol exchange in
       c.cg_min_notional <- v;
       c.cg_min_notional_for <- symbol;
       v));
  mark 7;
  setf Slots.reserve_share c.cg_reserve_share;
  setb Slots.parent_unfunded c.cg_parent_unfunded;
  setf Slots.parent_order_qty c.cg_parent_order_qty;
  mark 3;
  (match c.cg_carry with
   | Some cs ->
     let close_price, close_kind, close_realized, close_qty =
       match cs.Dio_persistence.Overlay_state_store.last_close with
       | Some f -> f.price, f.kind, f.realized, f.qty
       | None -> 0.0, "", 0.0, 0.0
     in
     let spot_price, spot_qty =
       match cs.Dio_persistence.Overlay_state_store.last_spot with
       | Some f -> f.price, f.qty
       | None -> 0.0, 0.0
     in
     setf Slots.carry_close_price close_price;
     setf Slots.carry_close_qty close_qty;
     setstr Slots.carry_close_kind close_kind;
     setb
       Slots.carry_close_is_recycle
       (String.equal close_kind "carry:recycle:close|ro"
        || String.equal close_kind "carry:drawdown|ro");
     setf Slots.carry_close_realized close_realized;
     setf
       Slots.carry_close_realized_total
       cs.Dio_persistence.Overlay_state_store.close_realized_total;
     seti Slots.carry_close_seq cs.Dio_persistence.Overlay_state_store.close_seq;
     setf
       Slots.carry_recycle_realized_total
       cs.Dio_persistence.Overlay_state_store.recycle_realized_total;
     seti Slots.carry_recycle_seq cs.Dio_persistence.Overlay_state_store.recycle_seq;
     setf Slots.carry_spot_price spot_price;
     setf Slots.carry_spot_qty spot_qty;
     seti Slots.carry_spot_seq cs.Dio_persistence.Overlay_state_store.spot_seq
   | None ->
     setf Slots.carry_close_price 0.0;
     setf Slots.carry_close_qty 0.0;
     setstr Slots.carry_close_kind "";
     setb Slots.carry_close_is_recycle false;
     setf Slots.carry_close_realized 0.0;
     setf Slots.carry_close_realized_total 0.0;
     seti Slots.carry_close_seq 0;
     setf Slots.carry_recycle_realized_total 0.0;
     seti Slots.carry_recycle_seq 0;
     setf Slots.carry_spot_price 0.0;
     setf Slots.carry_spot_qty 0.0;
     seti Slots.carry_spot_seq 0);
  let ex =
    if c.cg_venue_cached && String.equal c.cg_venue exchange
    then c.cg_exchange_module
    else (
      let m = Jac.get_exchange_module exchange in
      c.cg_exchange_module <- m;
      c.cg_venue <- exchange;
      c.cg_venue_cached <- true;
      m)
  in
  mark 4;
  (match ex with
  | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
    let staked =
      try Ex.get_staked_balance ~asset:base with
      | _ -> 0.0
    in
    let reserved =
      match c.cg_state with
      | Some s ->
        s.staked_base <- Float.max 0.0 staked;
        s.reserved_base
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
    (* A carry controller supplies its aggregate; a generic overlay instance supplies the
       same rule over its owner and inventory legs. A plain grid keeps the local figure. *)
    setf
      Slots.accumulated_balance
      (if c.cg_overlay_accumulated >= 0.0
       then c.cg_overlay_accumulated
       else if c.cg_inventory_override >= 0.0
       then c.cg_inventory_override
       else hedge_base);
    setf Slots.spot_base_total spot_total;
    setf
      Slots.usdc_free
      (match c.cg_quote_free with
       | Some f ->
         (try f () with
          | _ -> 0.0)
       | None ->
         (try Ex.get_tradeable_balance ~asset:"USDC" with
          | _ -> 0.0));
    (* A perp l2Book can be keyed by the bare coin (the usual case) or, for a cross-venue
       controller whose symbol has no matching spot pair, by the controller's pair: on
       Hyperliquid the l2Book subscription for an ADA/XMR domain is the bare coin, but the
       frame is filed under the domain pair, so the coin store stays empty. Try the coin,
       then the domain symbol. For an overlay the domain symbol is frequently a *spot*
       pair (ETH/USDC, SOL/USDC, HYPE/USDC), where the spot book is the wrong side of the
       perp touch by the spot-perp premium; those already resolve on the coin store, so
       the pair fallback only fires for the pair-keyed perp books. If neither exists,
       quote the perp mark so the hedge is still priced off the perp rather than left with
       no quote. *)
    let overlay_controller = c.cg_overlay_accumulated >= 0.0 in
    let fallback_mark =
      match funding with
      | Some f when fresh f.updated_at -> f.mark
      | _ -> 0.0
    in
    let tob = c.cg_tob in
    (if Ex.get_top_of_book_into ~symbol:base tob
     then (
       setf Slots.bid tob.(0);
       setf Slots.ask tob.(2))
     else if overlay_controller && fallback_mark > 0.0
     then (
       (* The perp book store is not keyed by the coin for this domain: use the perp mark.
          It is a genuine perp quote, unlike the pair store which can be a spot book. *)
       setf Slots.bid fallback_mark;
       setf Slots.ask fallback_mark)
     else if Ex.get_top_of_book_into ~symbol tob
     then (
       (* Second attempt keyed by the domain symbol: a perp l2Book can be filed under the
          pair rather than the bare coin. *)
       setf Slots.bid tob.(0);
       setf Slots.ask tob.(2))
     else (
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
      if Ex.get_top_of_book_into ~symbol:spot_symbol tob
      then (
        setf Slots.spot_bid tob.(0);
        setf Slots.spot_ask tob.(2))
      else (
        setf Slots.spot_bid 0.0;
        setf Slots.spot_ask 0.0))
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
    setf Slots.spot_ask 0.0);
  mark 5
;;

(** Publish the facts into the interpreter runtime's platform table. *)
let market_facts c (t : Strategy_runtime.t) =
  market_facts_io (Strategy_runtime.rt_facts_io t) c
;;

(** Perp coin for a spot pair: [BTC/USDC] -> [BTC]. *)
let perp_coin symbol =
  match String.index_opt symbol '/' with
  | Some i -> String.sub symbol 0 i
  | None -> symbol
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

(** In-flight dedupe key shared with restart adoption (see
    {!Strategy_common.overlay_inflight_key}). *)
let overlay_inflight_key = Strategy_common.overlay_inflight_key

(** Whether the venue's position snapshot still predates the last perp fill. Event-driven:
    the hold lifts as soon as a snapshot newer than the fill is published, so no clock is
    involved. While it holds, a reduce-only order would be sized from the pre-fill
    position and, for a close that just filled, would be re-sent into a flat book. *)
let position_behind_fill c ~exchange =
  c.cg_perp_fill_at > 0.0
  &&
  match Dio_exchange.Market_context.get_positions_updated_at ~exchange with
  | Some ts -> ts <= c.cg_perp_fill_at
  | None -> true
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
    let is_perp = String.equal market "perp" in
    let market_class = if is_perp then Strategy_common.Perp else Strategy_common.Spot in
    let instrument =
      match symbol with
      | Some s -> if is_perp then perp_coin s else s
      | None -> if is_perp then perp_coin asset.symbol else asset.symbol
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
        | Some p
          when Float.is_finite p
               && p > 0.0
               && min_notional > 0.0
               && floor_qty *. p < min_notional ->
          Jac.round_qty_up qty instrument asset.exchange
        | _ -> qty)
    in
    let order_side =
      if String.equal (String.lowercase_ascii side) "sell"
      then Strategy_common.Sell
      else Strategy_common.Buy
    in
    (* A reduce-only order may only ever shrink the position it names. Clamp to the live
       position so a stale strategy input or a duplicated close can never open the
       opposite side. *)
    let qty =
      if reduce_only && is_perp
      then (
        let position =
          match
            Dio_exchange.Market_context.get_position
              ~exchange:asset.exchange
              ~coin:instrument
          with
          | Some (p : Dio_exchange.Market_context.position) -> Some p.qty
          | None -> None
        in
        Strategy_common.reduce_only_room ~position ~side:order_side ~qty)
      else qty
    in
    (* A fresh entry fill can precede the position snapshot; without a grace window the
       overlay sees flat and re-shorts the full target. Hold new entries until the next
       webData2 snapshot (~5s). The fill is already booked. *)
    let entry_grace =
      String.equal dedup_key "carry:entry"
      && (not reduce_only)
      && c.cg_last_entry_fill_at > 0.0
      && c.cg_now -. c.cg_last_entry_fill_at <= 6.0
    in
    if reduce_only && is_perp && position_behind_fill c ~exchange:asset.exchange
    then
      Logging.debug_f
        ~section
        "Overlay reduce-only order held until the position snapshot reflects the last \
         fill: %s (%s)"
        instrument
        dedup_key
    else if entry_grace
    then
      Logging.debug_f
        ~section
        "Overlay entry held %.2fs for the position snapshot after the entry fill (%s)"
        (c.cg_now -. c.cg_last_entry_fill_at)
        instrument
    else if reduce_only && is_perp && qty <= 0.0
    then
      Logging.debug_f
        ~section
        "Overlay reduce-only order skipped: nothing to reduce on %s (%s)"
        instrument
        dedup_key
    else if (not (Float.is_finite qty)) || qty <= 0.0
    then
      Logging.warn_f
        ~section
        "Overlay order rejected: invalid qty %.8g for %s"
        qty
        instrument
    else (
      let side = order_side in
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
        if Strategy_common.Placement_backoff.remaining ~key:duplicate_key () > 0.0
        then
          Logging.debug_f
            ~section
            "Overlay placement backed off after a venue failure: %s"
            duplicate_key
        else if Strategy_common.InFlightOrders.is_in_flight duplicate_key
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
              (if is_perp then Strategy_common.Carry_perp else Strategy_common.Carry_spot)
            ~maker:post_only
            ~kind:kind_key;
          let order =
            Jac.create_place_order
              ~reduce_only
              ~time_in_force:tif
              ~order_type
              ~userref:Strategy_common.strategy_userref_carry
              ~market:market_class
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
              "%s [%s] %s %.8g @ %s tif=%s reduce_only=%b key=%s"
              (Strategy_common.string_of_order_side side)
              (Strategy_common.string_of_market market_class)
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
    let is_perp = String.equal market "perp" in
    let market_class = if is_perp then Strategy_common.Perp else Strategy_common.Spot in
    let instrument = if is_perp then perp_coin asset.symbol else asset.symbol in
    if dry_run ()
    then
      Logging.info_f
        ~section
        "Overlay DRY RUN: cancel [%s] %s %s"
        (Strategy_common.string_of_market market_class)
        instrument
        order_id
    else (
      let order =
        Jac.create_cancel_order
          ~market:market_class
          order_id
          instrument
          Strategy_common.Carry
          asset.exchange
      in
      ignore (Jac.push_order ~now:c.cg_now ~manage_state:false order : bool))
;;

(* Open-order scans keyed by the venue's open-orders generation. The carry overlay runs
   several replaces per book tick and each replace queries both order-book keys; on a perp
   symbol that read walks the venue's whole order index under two process-wide mutexes.
   The scan is a pure function of the venue snapshot, so it is cached against the same
   generation the other read caches use. *)
type order_row =
  { oor_id : string
  ; oor_price : float
  ; oor_remaining : float
  ; oor_side : string
  ; oor_userref : int option
  }

type order_scan =
  { osn_gen : int
  ; osn_rows : order_row array
  }

let order_scans : (string * string, order_scan) Ds.Cow_table.t =
  Ds.Cow_table.create ~shard_count:32 ()
;;

(* Reusable dedupe set for [order_replace]'s two-phase carry scan. [order_replace] runs
   synchronously within one strategy cycle, and each asset cycles on its own domain, so
   the buffer is per-domain (DLS) rather than module-global: two assets replacing
   concurrently must not share it. Cleared at entry; removes one hashtable allocation per
   book tick. *)
[@@@alert "-unsafe_multidomain"]

let replace_seen_key = Domain.DLS.new_key (fun () -> Hashtbl.create 8)
let replace_seen () = Domain.DLS.get replace_seen_key

(* Restore the alert beyond the two bindings above. *)
[@@@alert "+unsafe_multidomain"]

let open_orders_snapshot (module Ex : Dio_exchange.Exchange_intf.S) ~exchange ~symbol =
  let gen =
    try Ex.get_open_orders_generation ~symbol with
    | _ -> -1
  in
  match Ds.Cow_table.find_opt order_scans (exchange, symbol) with
  | Some s when gen >= 0 && s.osn_gen = gen -> s.osn_rows
  | _ ->
    let acc = ref [] in
    (try
       Ex.iter_open_orders_fast ~symbol (fun id price remaining side userref ->
         acc
         := { oor_id = id
            ; oor_price = price
            ; oor_remaining = remaining
            ; oor_side = side
            ; oor_userref = userref
            }
            :: !acc)
     with
     | _ -> ());
    let rows = Array.of_list (List.rev !acc) in
    (* No generation means the venue cannot report a change, so caching would be
       unverifiable: skip the publish rather than republishing an unvalidated row set. *)
    if gen >= 0
    then
      Ds.Cow_table.set order_scans (exchange, symbol) { osn_gen = gen; osn_rows = rows };
    rows
;;

(** Place or amend the overlay's resting order to the requested price/size. The engine
    consults the venue's open orders, so the strategy file never has to track an order id
    or reissue on a timer: every book event calls this, and it no-ops when the resting
    order already matches. *)
let order_replace_body
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
    let is_perp = String.equal market "perp" in
    let want_market = if is_perp then Strategy_common.Perp else Strategy_common.Spot in
    let instrument =
      match symbol with
      | Some s -> if is_perp then perp_coin s else s
      | None -> if is_perp then perp_coin asset.symbol else asset.symbol
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
      (* A reduce-only order may only ever shrink the position it names. Clamp to the live
         position so a repriced or duplicated close can never open the opposite side; a
         flat position leaves any resting close alone (see [protect_close] below). *)
      let qty =
        if reduce_only && is_perp
        then (
          let position =
            match
              Dio_exchange.Market_context.get_position
                ~exchange:asset.exchange
                ~coin:instrument
            with
            | Some (p : Dio_exchange.Market_context.position) -> Some p.qty
            | None -> None
          in
          Strategy_common.reduce_only_room ~position ~side:order_side ~qty)
        else qty
      in
      (* A reduce-only perp close is never cancelled by a zero-size request. That request
         can only come from a position read that says flat, and position snapshots flap
         (resyncs, partially-delivered account frames). A resting reduce-only order cannot
         increase exposure, so cancelling it buys nothing and costs the maker slot - and
         because a cancel retires the carry binding, the next tick re-places it: a
         cancel-and-replace loop that never leaves a close resting. The venue drops its
         own close once the position is gone, and the next long amends this one in place. *)
      let protect_close = reduce_only && is_perp in
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
      let backoff_key =
        overlay_inflight_key
          ~dedup_key
          ~side:(Strategy_common.string_of_order_side order_side)
          ~qty
          ~price:(Some rounded)
          ~exchange:asset.exchange
          ~instrument
          ~reduce_only
      in
      let backoff =
        if qty <= 0.0
        then 0.0
        else Strategy_common.Placement_backoff.remaining ~key:backoff_key ()
      in
      let matching =
        match ex with
        | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
          (* Open orders can be keyed by the spot pair (orderUpdates maps the perp coin
             back to the registered pair) or by the perp coin (the placement injection
             uses the order symbol). Query both.

             Two-phase scan: the fast iterator yields primitives, so only an order that
             matches this carry kind is materialized (usually one, often zero). Building
             the full [Types.open_order] list for every resting order on every book tick
             was the overlay's dominant allocation (100+ orders per pair, several replaces
             per cycle). *)
          let seen = replace_seen () in
          Hashtbl.clear seen;
          let out = ref [] in
          let want_perp = is_perp in
          let want_side =
            match order_side with
            | Strategy_common.Sell -> "sell"
            | Strategy_common.Buy -> "buy"
          in
          (* Same iterator shape as the venue accessor it replaces, so a failed read is
             still swallowed here rather than aborting the cycle. *)
          let iter_cached ~symbol f =
            try
              Array.iter
                (fun (r : order_row) ->
                  f r.oor_id r.oor_price r.oor_remaining r.oor_side r.oor_userref)
                (open_orders_snapshot (module Ex) ~exchange:asset.exchange ~symbol)
            with
            | _ -> ()
          in
          let drops_userref = Strategy_venue.drops_userref ~exchange:asset.exchange in
          let consider ~is_perp symbol =
            iter_cached ~symbol (fun oid price _remaining side_str userref ->
              if String.equal side_str want_side && not (Hashtbl.mem seen oid)
              then (
                (* Only the engine's own carry orders need the registry; untagged orders
                   on a venue that drops the userref need it to identify themselves.
                   Looking every grid order up here was the scan's dominant cost. *)
                let wants_registry =
                  match userref with
                  | Some 0 | None -> drops_userref
                  | Some u -> u = Strategy_common.strategy_userref_carry
                in
                if wants_registry
                then (
                  let meta = Strategy_common.lookup_carry_order ~order_id:oid in
                  let price_hits_target =
                    price > 0.0
                    && Float.abs (price -. rounded)
                       <= 1e-9 *. Float.max 1.0 (Float.abs rounded)
                  in
                  (* Venues that drop the engine's userref (Lighter) return [None]/[0].
                     The id registry is the strongest identity; with none (a restart) fall
                     back to the exact target price, or - where this overlay owns the
                     market - to the venue's reduce-only flag. *)
                  let userref_ok =
                    match userref with
                    | Some 0 | None ->
                      (* Only a venue that drops the tag can be adopted this way; other
                         venues' untagged orders are external. *)
                      (match meta with
                       | Some m -> String.equal m.Strategy_common.com_kind kind_key
                       | None -> price_hits_target || c.cg_adopt_untagged_orders)
                    | Some u -> u = Strategy_common.strategy_userref_carry
                  in
                  if userref_ok
                  then (
                    (* Both books share one userref. Match instrument class and logical
                       kind so a spot recycle is never amended as a perp take-profit. The
                       registry check covers a view that lists an unclassified order. *)
                    let market_ok =
                      match meta with
                      | Some m ->
                        m.Strategy_common.com_market
                        = Strategy_common.Carry_perp
                        = want_perp
                      | None ->
                        (* No registry entry: the materialized order's own market label
                           decides, not the store key. *)
                        true
                    in
                    let kind_ok =
                      (* No registry entry: the carry order's market+side identifies its
                         kind. Ignoring it strands a live order that the next placement
                         would duplicate. *)
                      match order_side, meta with
                      | Strategy_common.Buy, Some m ->
                        String.equal m.Strategy_common.com_kind kind_key
                      | Strategy_common.Buy, None -> true
                      | Strategy_common.Sell, Some m ->
                        String.equal m.Strategy_common.com_kind kind_key
                      | Strategy_common.Sell, None -> true
                    in
                    if market_ok && kind_ok
                    then (
                      (* The id index may point at the spot pair even when the scan was
                         keyed by the bare coin. Try the scanned store, then the other
                         store; querying only the scanned key duplicates every resting
                         perp order. *)
                      let materialize store_symbol =
                        match Ex.get_open_order ~symbol:store_symbol ~order_id:oid with
                        | Some _ as o -> o
                        | None -> None
                      in
                      let other = if is_perp then asset.symbol else instrument in
                      let order =
                        match materialize symbol with
                        | Some _ as o -> o
                        | None when not (String.equal other symbol) -> materialize other
                        | None -> None
                      in
                      match order with
                      | Some (o : Dio_exchange.Exchange_intf.Types.open_order)
                        when (o.market = want_market
                              && o.status = Dio_exchange.Exchange_intf.Types.New)
                             || o.status
                                = Dio_exchange.Exchange_intf.Types.PartiallyFilled ->
                        (* An echoed userref, or a registry entry, is already proof of
                           ownership; only an untagged, unregistered order needs the venue
                           flag to be adopted. *)
                        let identity_known =
                          match userref with
                          | Some 0 | None -> Option.is_some meta
                          | Some _ -> true
                        in
                        let adopted = (not identity_known) && not price_hits_target in
                        let reduce_only_ok =
                          identity_known
                          || Strategy_common.untagged_order_is_own_kind
                               ~price_hits_target
                               ~adopt_by_flags:c.cg_adopt_untagged_orders
                               ~want_reduce_only:reduce_only
                               ~venue_reduce_only:o.reduce_only
                        in
                        if reduce_only_ok
                        then (
                          (* Bind the id now so fills classify and the next tick amends
                             rather than layers another close. *)
                          if adopted
                          then (
                            Strategy_common.register_carry_order_id
                              ~order_id:oid
                              ~market:
                                (if is_perp
                                 then Strategy_common.Carry_perp
                                 else Strategy_common.Carry_spot)
                              ~maker:post_only
                              ~kind:kind_key
                              ~key:
                                (overlay_inflight_key
                                   ~dedup_key
                                   ~side:(Strategy_common.string_of_order_side order_side)
                                   ~qty
                                   ~price:(Some rounded)
                                   ~exchange:asset.exchange
                                   ~instrument
                                   ~reduce_only);
                            Logging.info_f
                              ~section
                              "Overlay adopt: %s %s %s (kind %s, reduce_only=%b)"
                              instrument
                              oid
                              (Strategy_common.string_of_order_side order_side)
                              kind_key
                              reduce_only);
                          Hashtbl.replace seen oid ();
                          out := o :: !out)
                      | _ -> ())))))
          in
          if want_perp
          then
            (* The perp view resolves orders stored under either the bare coin or the
               registered spot pair through the order index, so one scan covers both. *)
            consider ~is_perp:true instrument
          else (
            consider ~is_perp:false asset.symbol;
            if not (String.equal instrument asset.symbol)
            then consider ~is_perp:false instrument);
          !out
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
          (* One cancel request per order. [order_replace] runs every book tick and a
             canceled order can linger in the open-orders cache. The supervisor clears the
             key on the cancel ack/failure. *)
          let cancel_key = "overlay:cancel|" ^ o.order_id in
          if Strategy_common.InFlightOrders.is_in_flight cancel_key
          then ()
          else (
            ignore (Strategy_common.InFlightOrders.add_in_flight_order cancel_key);
            (* A cancel/replace loop is otherwise indistinguishable from progress. *)
            Logging.info_f
              ~section
              "Overlay cancel: %s %s (%s)"
              instrument
              o.order_id
              reason;
            let order =
              Jac.create_cancel_order
                o.order_id
                instrument
                Strategy_common.Carry
                asset.exchange
            in
            let pushed = Jac.push_order ~now:c.cg_now ~manage_state:false order in
            if not pushed
            then ignore (Strategy_common.InFlightOrders.remove_in_flight_order cancel_key)))
      in
      (* One live order per overlay kind. Keep the copy nearest the target so sweeping the
         rest does not leave the keeper needing an immediate amend. *)
      let matching =
        match matching with
        | [] | [ _ ] -> matching
        | first :: rest ->
          let distance (o : Dio_exchange.Exchange_intf.Types.open_order) =
            match o.limit_price with
            | Some p -> Float.abs (p -. rounded)
            | None -> Float.infinity
          in
          let best =
            List.fold_left
              (fun a o -> if distance o < distance a then o else a)
              first
              rest
          in
          best
          :: List.filter
               (fun (o : Dio_exchange.Exchange_intf.Types.open_order) ->
                 not (String.equal o.order_id best.order_id))
               matching
      in
      if qty <= 0.0
      then
        if protect_close
        then (
          (* Never cancel the keeper: a cancelled close retires its binding and the next
             tick re-places it. *)
          match matching with
          | _ :: (_ :: _ as extras) ->
            List.iter (fun o -> cancel_one ~reason:"duplicate" o) extras
          | _ -> ())
        else List.iter (fun o -> cancel_one o) matching
      else if backoff > 0.0
      then
        Logging.debug_f
          ~section
          "Overlay placement backed off %.1fs after a venue failure: %s"
          backoff
          backoff_key
      else (
        (match matching with
         | _ :: (_ :: _ as extras) ->
           List.iter (fun o -> cancel_one ~reason:"duplicate" o) extras
         | _ -> ());
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
                ~market:want_market
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
          (* No open order matched the venue scan. A registered order of this kind that
             has not been retired is still live: the venue's view drops a filled order
             before the domain drains its terminal event, so placing now would re-send a
             close into a position the venue already flattened. *)
          if qty <= 0.0
          then
            (* A zero-size request with nothing resting is a cancel that has nothing to
               cancel - the common case on every tick of a file whose entry is not up.
               Falling through to [order_place] reported it as an invalid quantity, which
               made a healthy no-op look like a rejected order. *)
            Logging.debug_f
              ~section
              "Overlay cancel: no resting %s order on %s" kind_key instrument
          else if Strategy_common.has_live_carry_order
               ~kind:kind_key
               ~exchange:asset.exchange
               ~instrument
               ~reduce_only
               ()
          then
            Logging.debug_f
              ~section
              "Overlay placement held: the live %s order on %s is not retired yet"
              kind_key
              instrument
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
    (* No price (or no asset): a zero-size request is a cancel with nothing to size, so
       there is nothing to place. Reaching [order_place] with it reported an invalid
       quantity for a file that was simply standing down its entry. *)
    if qty <= 0.0
    then
      Logging.debug_f
        ~section
        "Overlay cancel: no price for %s, nothing to place" dedup_key
    else
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

(** {!order_replace_body}, held while the venue position snapshot still predates the last
    perp fill (see [cg_perp_fill_at]). The close that would be re-placed or resized here
    is the one that just filled, so the only thing a stale position can produce is a
    reduce-only order sized against a book the venue no longer has. The snapshot-driven
    cycle re-derives it from the live position. *)
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
  let behind =
    match c.cg_asset with
    | Some asset -> position_behind_fill c ~exchange:asset.exchange
    | None -> false
  in
  if reduce_only && String.equal market "perp" && behind
  then
    Logging.debug_f
      ~section
      "Overlay reduce-only replace held until the position snapshot reflects the last \
       fill: %s (%s)"
      c.cg_symbol
      dedup_key
  else
    order_replace_body
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

(** Credit realized carry profit to the parent grid's [accumulated_profit].

    The durable write goes straight to the parent's accumulation entry so a recycle
    survives a restart before the parent's next cycle. When the overlay shares the
    parent's state (co-located grid), the live state is updated too, so no handoff is
    queued and the domain flush cannot double-count. Otherwise the credit is also queued
    in [Accum_credit] for the parent domain to drain on its next cycle. A controller with
    no parent grid has no ledger to credit, so the credit is dropped. *)
let credit_profit c ~qty ~seq =
  if Float.is_finite qty && qty <> 0.0
  then (
    match c.cg_asset with
    | Some asset ->
      let store_key = c.cg_credit_store_key in
      if String.equal store_key ""
      then ()
      else (
        let applied =
          Dio_persistence.Base_accumulation_store.credit_profit ~key:store_key ~seq ~qty
        in
        if applied
        then (
          match c.cg_state with
          | Some s when s.persistence_key = Some store_key ->
            if seq <= 0 || seq > s.carry_credit_seq
            then (
              s.accumulated_profit <- s.accumulated_profit +. qty;
              if seq > s.carry_credit_seq then s.carry_credit_seq <- seq;
              s.persistence_dirty <- true)
          | _ ->
            Accum_credit.add
              ~key:(Accum_credit.key ~exchange:asset.exchange ~symbol:asset.symbol)
              ~seq
              qty))
    | None -> ())
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

(** Compiled guards run before the pass executes any phase, so the facts they read are
    the previous cycle's. The plan and the state flags are fresh; decide on those. *)
let buy_gate_live c (p : Jac.buy_plan) =
  match c.cg_state with
  | None -> false
  | Some state ->
    p.bp_balance_ok
    && (not state.capital_low)
    && (not state.inflight_buy)
    && state.last_buy_order_id = None
    && (not (Hashtbl.mem state.amend_cooldowns "place_Buy"))
    && not p.bp_crossing
;;

let buy_place_send c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    let p = buy_plan_exn c in
    if not (buy_gate_live c p)
    then
      Logging.info_f
        ~section
        "buy send suppressed for %s: the compiled guard passed on a stale fact (funded=%b \
         capital_low=%b inflight=%b tracked=%s cooldown=%b crossing=%b)"
        asset.symbol
        p.bp_balance_ok
        state.capital_low
        state.inflight_buy
        (match state.last_buy_order_id with
         | Some id -> id
         | None -> "none")
        (Hashtbl.mem state.amend_cooldowns "place_Buy")
        p.bp_crossing
    else (
      Logging.info_f
        ~section
        "buy send %s: cycle=%d price=%.4f open_scan=%d"
        asset.symbol
        c.cg_cycle
        p.bp_price
        c.cg_open_buy_count;
      c.cg_buy_attempted
      <- Jac.buy_place_send ~state ~now:c.cg_now ~asset ~qty:p.bp_qty ~buy_price:p.bp_price)
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

(** Fine path branch: latch capital_low. Same stale-guard hazard as [buy_place_send]:
    latching on it pauses the grid until an unrelated event clears it. *)
let buy_place_latch_capital_low ?rt c =
  match c.cg_state, c.cg_asset with
  | Some state, Some asset ->
    let p = buy_plan_exn c in
    (match rt with
     | Some t ->
       Logging.info_f
         ~section
         "latch %s: plan_balance_ok=%b published=%s (quote=%.4f need=%.4f)"
         asset.symbol
         p.bp_balance_ok
         (Strategy_expr.string_of_value
            (Strategy_runtime.get_platform_slot t Slots.buy_balance_ok))
         c.cg_qbal
         p.bp_quote_needed
     | None -> ());
    if p.bp_balance_ok
    then
      Logging.info_f
        ~section
        "latch suppressed for %s: the compiled guard saw a stale balance verdict, the \
         fresh plan is funded (available %.2f >= %.2f)"
        asset.symbol
        p.bp_available
        (p.bp_price *. p.bp_qty)
    else
      Jac.buy_place_latch_capital_low
        ~state
        ~now:c.cg_now
        ~asset
        ~quote_needed:p.bp_quote_needed
        ~available_quote_balance:p.bp_available
        ~quote_balance:c.cg_qbal
        ~locked_in_buys:c.cg_locked_in_buys
        ~buy_price:p.bp_price
        ~qty:p.bp_qty
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
    Jac.sell_leg_prepare_into
      ~pre:c.cg_sell_pre
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
      ~base_balance_age:c.cg_base_age;
    c.cg_sell_pre_valid <- true
  | _ -> c.cg_sell_pre_valid <- false
;;

(** Fine path sell phase 2a: whether a fresh sell is placeable this cycle. *)
let sell_place_should c =
  match c.cg_state with
  | Some state when c.cg_sell_pre_valid ->
    let pre = c.cg_sell_pre in
    Jac.sell_place_should ~state ~asset_balance:c.cg_abal ~pre
  | _ -> false
;;

(** Fine path sell phase 2b: place the owed/restored sell. *)
let sell_place_body c =
  match c.cg_state, c.cg_asset, c.cg_ecfg with
  | Some state, Some asset, Some ecfg when c.cg_sell_pre_valid ->
    let pre = c.cg_sell_pre in
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
  match c.cg_state, c.cg_asset, c.cg_ecfg with
  | Some state, Some asset, Some ecfg when c.cg_sell_pre_valid ->
    let pre = c.cg_sell_pre in
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
  match c.cg_sell_pre_valid with
  | true ->
    let pre = c.cg_sell_pre in
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
  | false -> ()
;;

(** Fine path sell phase 3a: retry-latch bookkeeping. *)
let sell_finalize_latch c =
  match c.cg_state, c.cg_asset, c.cg_ecfg with
  | Some state, Some asset, Some ecfg when c.cg_sell_pre_valid ->
    let pre = c.cg_sell_pre in
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
  match c.cg_state, c.cg_asset, c.cg_ecfg with
  | Some state, Some asset, Some ecfg when c.cg_sell_pre_valid ->
    let pre = c.cg_sell_pre in
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
  match c.cg_state, c.cg_asset, c.cg_ecfg with
  | Some state, Some asset, Some ecfg when c.cg_sell_pre_valid ->
    let pre = c.cg_sell_pre in
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
    reference handlers remain the bodies; they mutate the symbol's state directly, so this
    must run on the owning asset domain. *)
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
