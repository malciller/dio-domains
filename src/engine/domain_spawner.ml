(* [Domain.spawn] is marked [do_not_spawn_domains] because unbounded domains degrade GC.
   This module spawns a bounded, supervised set of long-lived per-asset domains; the alert
   is acknowledged here. *)
[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

open Config
module Fear_and_greed = Cmc.Fear_and_greed

(* Capital-oracle runtime; explicit alias avoids opening Dio_oracle. *)
module Oracle_runtime = Dio_oracle.Oracle_runtime
module Oracle_types = Dio_oracle.Oracle_types
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

let section = "domain_spawner"

(** Sampling mask for the per-cycle [Gc.quick_stat] capture, which allocates ~24 words and
    costs ~0.3us. [0] samples every measured cycle so the window-max cycle always carries
    its GC cause; at single-digit cycles/s per domain the added allocation is negligible. *)
let gc_sample_mask = 0

(** Quote-balance age, in seconds, above which a snapshot is not authoritative. Stale
    snapshot: an under-funded buy is still attempted (the exchange's verdict rules). Fresh
    snapshot: an unfunded buy is skipped. Unknown age (no freshness tracking) is treated
    as stale. *)
let stale_balance_age_seconds = 60.0

(** True for crypto exchanges, mirroring the capital oracle's rule
    (Oracle_tasks.calendar_kind_of_exchange). Crypto assets blend Fear & Greed into
    sizing; equities are sized by the oracle alone and never consume F&G values. *)
let is_crypto_exchange = function
  | "hyperliquid" | "kraken" -> true
  | _ -> false
;;

(** Construct a unique registry key from exchange and symbol. *)
let domain_key asset = Printf.sprintf "%s/%s" asset.exchange asset.symbol

(** Mutable state tracked per supervised domain. *)
type domain_state =
  { asset : trading_config
  ; domain_handle : unit Domain.t option Atomic.t
  ; last_restart : float Atomic.t
  ; restart_count : int Atomic.t
  ; is_running : bool Atomic.t
  ; mutex : Mutex.t
  }

(** Global registry mapping domain keys to their supervisor state. *)
let domain_registry : (string, domain_state) Hashtbl.t = Hashtbl.create 32

let registry_mutex = Mutex.create ()

(** Atomic flag set to true when graceful shutdown is requested. *)
let shutdown_requested = Atomic.make false

(** Per-symbol latency profiler cache. Persists across domain restarts so ~800KB profiler
    objects are allocated once per symbol, not per asset_domain_worker invocation. *)
type domain_profilers =
  { prof_ob : Latency_profiler.t
  ; prof_exec : Latency_profiler.t
  ; prof_prep : Latency_profiler.t
  ; prof_strategy : Latency_profiler.t
  ; prof_cycle : Latency_profiler.t
  }

let domain_profiler_cache : (string, domain_profilers) Hashtbl.t = Hashtbl.create 8
let profiler_cache_mutex = Mutex.create ()

let get_domain_profilers symbol =
  Mutex.lock profiler_cache_mutex;
  let profs =
    match Hashtbl.find_opt domain_profiler_cache symbol with
    | Some p -> p
    | None ->
      let p =
        { prof_ob = Latency_profiler.create (symbol ^ ":ob")
        ; prof_exec =
            Latency_profiler.create ~bucket_us:10 ~max_latency_us:20_000 (symbol ^ ":exec")
        ; prof_prep = Latency_profiler.create (symbol ^ ":prep")
        ; prof_strategy = Latency_profiler.create (symbol ^ ":strategy")
        ; prof_cycle =
            Latency_profiler.create
              ~bucket_us:10
              ~max_latency_us:20_000
              (symbol ^ ":cycle")
        }
      in
      Hashtbl.replace domain_profiler_cache symbol p;
      p
  in
  Mutex.unlock profiler_cache_mutex;
  profs
;;

(** Emits at most one INFO line per completed internal latency window, and only when a
    stage recorded a sample at or above [threshold_us]. [cycle]'s cause is appended to the
    [Latency_profiler.spike_message] output as the worst-cycle continuation line. *)
let log_latency_window ~key ~window_seconds ~threshold_us ~ob ~exec ~prep ~strategy ~cycle
  =
  match
    Latency_profiler.spike_message
      ~key
      ~window_seconds
      ~threshold_us
      [ "BOOK", ob; "EVENTS", exec; "PREP", prep; "STRATEGY", strategy; "TOTAL", cycle ]
  with
  | None -> ()
  | Some msg -> Logging.info_f ~section "%s" msg
;;

(** Core worker function executed by each OCaml domain for a trading asset. Runs the
    event-driven loop: consumes ring buffer events, executes strategy, and blocks on
    Exchange_wakeup between cycles. *)
let asset_domain_worker
  (config : config)
  (fee_fetcher : trading_config -> trading_config)
  (asset : trading_config)
  =
  Random.self_init ();
  (* Fetch exchange fee schedule at domain startup *)
  let asset_with_fees = fee_fetcher asset in
  (* Behavioral-equivalence tracing (default off): when enabled, record an observable
     trace per busy cycle and persist it periodically. *)
  let trace_recorder =
    if config.strategy_trace
    then Some (Dio_strategies.Strategy_event_recorder.create ())
    else None
  in
  let trace_path =
    let sanitize s = String.map (fun c -> if Char.equal c '/' then '_' else c) s in
    Printf.sprintf
      "data/strategy_trace_%s_%s.json"
      (sanitize asset_with_fees.exchange)
      (sanitize asset_with_fees.symbol)
  in
  let trace_cycles = ref 0 in
  (* Resolves accumulation_buffer from Fear & Greed on every venue (Kraken runs the same
     reserved_base accrual; see jacobs_ladder_config.kraken_config). Only a live F&G
     reading resolves it; without one the grid places no orders. *)
  let fng_accumulation_buffer () =
    let exch_id =
      Dio_exchange.Exchange_intf.Types.exchange_of_string asset_with_fees.exchange
    in
    let is_accumulation_exch =
      match exch_id with
      | Hyperliquid | Ibkr | Lighter | Alpaca | Kraken -> true
      | Custom _ -> false
    in
    if is_accumulation_exch
       && (asset_with_fees.strategy = "jacobs_ladder"
           || asset_with_fees.strategy = "Ladder")
    then (
      match Fear_and_greed.get_cached () with
      | None -> None
      | Some fng ->
        let resolved =
          Fear_and_greed.grid_value_for_fng
            ~grid_interval:asset_with_fees.accumulation_buffer
            ~fear_and_greed:fng
        in
        let lo, hi = asset_with_fees.accumulation_buffer in
        Logging.debug_f
          ~section
          "Resolved accumulation_buffer for %s/%s: %.4f (F&G=%.2f, range %.4f-%.4f)"
          asset_with_fees.exchange
          asset_with_fees.symbol
          resolved
          fng
          lo
          hi;
        Some resolved)
    else None
  in
  let resolved_accumulation_buffer = fng_accumulation_buffer () in
  match Exchange.Registry.get asset_with_fees.exchange with
  | None ->
    Logging.error_f
      ~section
      "Unknown exchange '%s' for asset %s, aborting domain"
      asset_with_fees.exchange
      asset_with_fees.symbol
  | Some (module Ex) ->
    let exec_read_pos = ref 0 in
    let orderbook_read_pos = ref 0 in
    (* Latest market data derived from consumed ring buffer events *)
    let current_price = ref nan in
    let tob_bid = ref nan in
    let tob_ask = ref nan in
    let tob_bsize = ref nan in
    let tob_asize = ref nan in
    (* Event-driven flag: true when new data warrants a strategy execution *)
    let should_execute_strategy = ref true in
    (* Startup gate: blocks strategy execution until the initial snapshot's execution
       events have been consumed, so handle_order_acknowledged restores order state
       (last_buy_order_id, etc.) before new placements. Applies to all exchanges. *)
    let exec_ready = ref false in
    (* Set after the first exec position check; fallback that opens the exec_ready gate
       for assets with no open orders (empty snapshot). *)
    let exec_checked = ref false in
    let latency_active = ref false in
    let exec_ready_cycle = ref 0 in
    let open_orders_dirty = ref true in
    (* Per-cycle oracle decision lookup is cached against the publish generation, so idle
       cycles do no decision work. *)
    let oracle_gen_cached = ref (-1) in
    let oracle_decision_cached = ref None in
    (* Strategy config refs are initialized per strategy type. The capital-oracle runtime
       publishes a per-asset decision (qty, blended grid_interval, active) to a lock-free
       snapshot; while a decision exists, the oracle's blended qty/gi win. F&G enters that
       blend inside the oracle (parameter_components), never re-derived here. *)
    let baseline_price = ref None in
    (* [None] until a real F&G value is seen: a missing index means no live F&G signal;
       the per-cycle re-evaluation is skipped, not neutralized. *)
    let last_known_fng = ref None in
    let oracle_decision_at_startup =
      Oracle_runtime.decision_for
        ~exchange:asset_with_fees.exchange
        ~symbol:asset_with_fees.symbol
    in
    (* Last applied oracle halt state, so the per-cycle block logs only on
       active<->inactive transitions. Initialized from the startup decision. *)
    let oracle_halted_prev =
      ref
        (match oracle_decision_at_startup with
         | Some d -> not d.active
         | None -> false)
    in
    (* Cascade cancel state: a decision with [cancel_resting_buys] asks this domain to
       cancel its resting buys so committed capital returns to the venue pool for a
       higher-priority strategy. The cancel is a network op that can fail silently
       (dropped dispatch, exchange rejection, full ring buffer), so it is latched by
       [reclaim_cancel_issued]/[reclaim_cancel_at] rather than issued every cycle, and
       retried while the decision persists and eligible buys remain. The latch re-arms
       when the store shows no eligible buy (cancel landed) or the decision stops being a
       cascade; see [Dio_strategies.Jacobs_ladder.reclaim_step]. Without the retry a
       single failed cancel leaves the pool permanently short. *)
    let reclaim_cancel_issued = ref false in
    let reclaim_cancel_at = ref 0.0 in
    let reclaim_retry_seconds = 15.0 in
    (* One-shot warning: startup-window-elapsed withhold messages fire at most once per
       domain (see the closed-gate branch below). *)
    let no_signal_warned = ref false in
    (* At startup the grid asset is pre-materialized only from an ACTIVE capital-oracle
       decision. With none (no decision or an INACTIVE one) the ref starts [None] and the
       startup gate stays closed: no fallback sizing exists, so the BUY leg places nothing
       until the oracle publishes. The loop's decision handler materializes an INACTIVE
       asset too, so its sell leg runs under halt. *)
    let grid_asset_of
      ?(qty = asset_with_fees.qty)
      ?(accumulation_buffer = resolved_accumulation_buffer)
      ~(grid_interval : float)
      ()
      =
      { Dio_strategies.Jacobs_ladder.exchange = asset_with_fees.exchange
      ; symbol = asset_with_fees.symbol
      ; qty
      ; grid_interval
      ; sell_mult = asset_with_fees.sell_mult
      ; strategy = asset_with_fees.strategy
      ; maker_fee = asset_with_fees.maker_fee
      ; taker_fee = asset_with_fees.taker_fee
      ; accumulation_buffer = Option.value accumulation_buffer ~default:0.0
      ; base_accumulation = asset_with_fees.base_accumulation
      ; sell_levels_persistence = asset_with_fees.sell_levels
      }
    in
    (* The strategy materializes from the FIRST capital-oracle decision, ACTIVE or
       INACTIVE. No F&G or config fallback sizing exists: an ACTIVE startup decision sizes
       it here; an INACTIVE one is materialized by the decision handler below so the sell
       leg can run under halt. *)
    (* TODO(milestone-3): temporary bridge. Strategy names are user-defined and opaque;
       this hardcoded name dispatch is replaced by compiled-instance dispatch once the
       interpreter is wired. See docs/strategy-engine-design.md §9.5. Do not extend. *)
    let grid_strategy_asset_ref =
      if asset_with_fees.strategy = "jacobs_ladder" || asset_with_fees.strategy = "Ladder"
      then (
        match oracle_decision_at_startup with
        | Some d when d.active ->
          ref
            (Some
               (grid_asset_of
                  ~qty:(Printf.sprintf "%.8g" d.buy_qty)
                  ~grid_interval:d.grid_interval
                  ()))
        | _ -> ref None)
      else ref None
    in
    (* Oracle startup gate: grid strategies withhold execution until the capital oracle
       publishes its first decision for this asset. The gate opens on any decision, ACTIVE
       or INACTIVE. There is no F&G or config fallback sizing path; with no decision,
       orders are withheld and a one-shot warning fires once the grace period elapses.
       While gated the domain clears its execute flag and blocks on
       [Exchange_wakeup.wait_since]. [oracle_gate_deadline] bounds when the warning may
       fire; it is checked on wakeups, never polled. *)
    let is_grid_strategy =
      asset_with_fees.strategy = "jacobs_ladder" || asset_with_fees.strategy = "Ladder"
    in
    let oracle_tracks_asset =
      Oracle_runtime.tracks_asset
        ~exchange:asset_with_fees.exchange
        ~symbol:asset_with_fees.symbol
    in
    (* Startup gate window: lets the oracle's first history refresh and pass publish
       before this domain starts trading. *)
    let oracle_startup_wait = 120.0 in
    let oracle_gate_open = ref (not is_grid_strategy) in
    let oracle_gate_deadline = ref (Unix.gettimeofday () +. oracle_startup_wait) in
    (* TODO(milestone-3): temporary bridge (see above / §9.5). *)
    let is_mm_strategy =
      asset_with_fees.strategy = "market_maker" || asset_with_fees.strategy = "MM"
    in
    let mm_strategy_asset_ref =
      if is_mm_strategy then ref (Some asset_with_fees) else ref None
    in
    (* Pre-populate strategy state (exchange_id, grid_qty, maker_fee) so fill handlers
       invoked during exec-event consumption see correct values before the first
       execute_strategy call; otherwise profit calculations and persistence writes use
       zero defaults. *)
    if is_grid_strategy
    then (
      let st = Dio_strategies.Jacobs_ladder.get_strategy_state asset_with_fees.symbol in
      st.exchange_id <- asset_with_fees.exchange;
      let initial_qty =
        match oracle_decision_at_startup with
        | Some d when d.active && d.buy_qty > 0.0 -> d.buy_qty
        | _ ->
          (try float_of_string asset_with_fees.qty with
           | Failure _ -> 0.001)
      in
      st.grid_qty <- initial_qty;
      st.cached_sell_mult
      <- (try float_of_string asset_with_fees.sell_mult with
          | Failure _ -> 1.0);
      st.cached_ecfg
      <- Dio_strategies.Jacobs_ladder.get_exchange_config asset_with_fees.exchange;
      st.cached_round_price
      <- (fun p -> Ex.round_price ~symbol:asset_with_fees.symbol ~price:p);
      st.cached_price_increment
      <- Option.value
           (Ex.get_price_increment ~symbol:asset_with_fees.symbol)
           ~default:0.01;
      st.cached_qty_increment
      <- Option.value (Ex.get_qty_increment ~symbol:asset_with_fees.symbol) ~default:0.01;
      st.cached_venue_min_qty
      <- Option.value (Ex.get_qty_min ~symbol:asset_with_fees.symbol) ~default:0.01;
      st.cached_venue_min_notional
      <- Dio_strategies.Jacobs_ladder.get_min_notional_val
           asset_with_fees.symbol
           asset_with_fees.exchange;
      st.persistence_dirty <- false;
      st.persistence_key
      <- Some
           (Dio_persistence.Base_accumulation_store.key_of
              ~strategy:asset_with_fees.strategy
              ~symbol:asset_with_fees.symbol
              ~venue:asset_with_fees.exchange);
      st.base_accumulation_enabled <- asset_with_fees.base_accumulation;
      st.sell_levels_enabled <- asset_with_fees.sell_levels;
      st.maker_fee
      <- (match asset_with_fees.maker_fee with
          | Some f -> f
          | None ->
            (match
               Dio_strategies.Fee_cache.get_maker_fee
                 ~exchange:asset_with_fees.exchange
                 ~symbol:asset_with_fees.symbol
             with
             | Some cached -> cached
             | None -> 0.0));
      ());
    (* Exec read position starts at 0 for all exchanges so snapshot events replay through
       handle_order_acknowledged, restoring last_buy_order_id and open sell tracking
       before the first strategy cycle. *)
    Logging.debug_f
      ~section
      "About to get execution feed position for %s"
      asset_with_fees.symbol;
    exec_read_pos := 0;
    (* Waits for the execution snapshot before entering the loop; otherwise the first
       cycle can see zero open orders and place duplicates. 15s timeout. *)
    let deadline = Unix.gettimeofday () +. 15.0 in
    while
      (not (Ex.has_execution_data ~symbol:asset_with_fees.symbol))
      && Unix.gettimeofday () < deadline
    do
      Thread.delay 0.05
    done;
    if not (Ex.has_execution_data ~symbol:asset_with_fees.symbol)
    then
      Logging.warn_f
        ~section
        "Execution data not ready for %s/%s after 15s, proceeding anyway"
        asset_with_fees.exchange
        asset_with_fees.symbol
    else
      Logging.debug_f
        ~section
        "Execution data confirmed ready for %s/%s"
        asset_with_fees.exchange
        asset_with_fees.symbol;
    Logging.debug_f
      ~section
      "Domain for %s/%s starting consumption from exec position 0 (full replay)"
      asset_with_fees.exchange
      asset_with_fees.symbol;
    (* Orderbook position starts at the current write position, skipping stale ring-buffer
       data. Starting at 0 would replay up to 128 historical entries per symbol on each
       restart. *)
    orderbook_read_pos := Ex.get_orderbook_position ~symbol:asset_with_fees.symbol;
    (* Seed current_price and top_of_book from the exchange live cache so the first cycle
       can execute immediately rather than waiting for the next incoming update. *)
    (match Ex.get_top_of_book ~symbol:asset_with_fees.symbol with
     | Some (bid_price, bid_size, ask_price, ask_size) ->
       tob_bid := bid_price;
       tob_ask := ask_price;
       tob_bsize := bid_size;
       tob_asize := ask_size;
       current_price := (bid_price +. ask_price) /. 2.0;
       Logging.debug_f
         ~section
         "Seeded initial price for %s from cache: %.4f"
         asset_with_fees.symbol
         !current_price
     | None -> ());
    Logging.debug_f
      ~section
      "Domain initialized for asset: %s/%s (Strategy: %s)"
      asset_with_fees.exchange
      asset_with_fees.symbol
      asset_with_fees.strategy;
    let key = domain_key asset_with_fees in
    let state = Hashtbl.find domain_registry key in
    Logging.debug_f
      ~section
      "Entering domain loop for %s. is_running=%B"
      key
      (Atomic.get state.is_running);
    (* Parse base/quote currency pair from the symbol *)
    let base_asset, quote_currency =
      if String.contains asset_with_fees.symbol '/'
      then (
        let parts = String.split_on_char '/' asset_with_fees.symbol in
        List.nth parts 0, List.nth parts 1)
      else asset_with_fees.symbol, "USD"
    in
    (* Allocation-free cached balance closures. *)
    let base_balance_fn = Ex.get_tradeable_balance_fast ~asset:base_asset in
    let quote_balance_fn = Ex.get_tradeable_balance_fast ~asset:quote_currency in
    (* Base-balance snapshot age. The sell-hold guard releases placed-sell holds once a
       balance message newer than the placement arrives (venue hold-netting is then
       included). *)
    let base_balance_age_fn = Ex.get_balance_age_fast ~asset:base_asset in
    (* Cached closures for latency-sensitive feed access in the hot loop *)
    let get_ob_pos_fn = Ex.get_orderbook_position_fast ~symbol:asset_with_fees.symbol in
    let get_tob_fn = Ex.get_top_of_book_fast ~symbol:asset_with_fees.symbol in
    let get_exec_pos_fn =
      Ex.get_execution_feed_position_fast ~symbol:asset_with_fees.symbol
    in
    let has_exec_fn = Ex.has_execution_data_fast ~symbol:asset_with_fees.symbol in
    let cycle_count = ref 0 in
    let { prof_ob; prof_exec; prof_prep; prof_strategy; prof_cycle } =
      get_domain_profilers asset_with_fees.symbol
    in
    (* Rolling latency window: publish and reset each profiler every
       [latency_window_seconds] so the dashboard reads fresh percentiles. Publishing swaps
       an immutable snapshot into an Atomic cell, so the dashboard never scans a histogram
       being mutated by this domain. *)
    let latency_window_seconds = config.latency_window_seconds in
    let latency_spike_threshold_us = config.latency_spike_threshold_us in
    let latency_spike_report_seconds = config.latency_spike_report_seconds in
    let last_window_time = ref (Unix.gettimeofday ()) in
    (* Wall-clock throttle for internal spike INFO lines. Percentile windows still publish
       every [latency_window_seconds]; this only limits a busy domain to one reported
       window per interval. *)
    let last_spike_report_time = ref (Unix.gettimeofday ()) in
    (* Per-cycle GC attribution is captured inline in the loop (two [Gc.quick_stat] reads,
       ~0.3us each; see [gc_monitor]); the window publisher below does not sample GC. *)
    let publish_windows () =
      let ob =
        Latency_profiler.snapshot_and_reset
          ~spike_threshold_us:latency_spike_threshold_us
          prof_ob
      in
      let exec =
        Latency_profiler.snapshot_and_reset
          ~spike_threshold_us:latency_spike_threshold_us
          prof_exec
      in
      (* Snapshot+reset this symbol's place/amend/cancel profilers on the window cadence;
         their per-op [report] calls no longer run on the order hot path. *)
      Order_executor.snapshot_symbol_profilers asset_with_fees.symbol;
      (* The strategy window's execution count is the number of order actions pushed in
         the window (place/amend/cancel), so the dashboard's STRAT/S reports real order
         activity, not strategy-invocation cycles. *)
      Latency_profiler.set_executions
        prof_strategy
        (Dio_strategies.Strategy_common.Order_actions.snapshot_and_reset
           asset_with_fees.symbol);
      let prep =
        Latency_profiler.snapshot_and_reset
          ~spike_threshold_us:latency_spike_threshold_us
          prof_prep
      in
      let strategy =
        Latency_profiler.snapshot_and_reset
          ~spike_threshold_us:latency_spike_threshold_us
          prof_strategy
      in
      let cycle =
        Latency_profiler.snapshot_and_reset
          ~spike_threshold_us:latency_spike_threshold_us
          prof_cycle
      in
      if reports_internal config.latency_spike_report
      then (
        let now = Unix.gettimeofday () in
        if now -. !last_spike_report_time >= latency_spike_report_seconds
        then (
          last_spike_report_time := now;
          log_latency_window
            ~key
            ~window_seconds:latency_window_seconds
            ~threshold_us:latency_spike_threshold_us
            ~ob
            ~exec
            ~prep
            ~strategy
            ~cycle))
    in
    (* Publish an initial empty window so the dashboard renders this domain as idle
       immediately, and clear any stale snapshot from a previous incarnation. *)
    publish_windows ();
    last_window_time := Unix.gettimeofday ();
    (* Caches the equity market-hours evaluation, which does gmtime+mktime+DST math per
       call (alpaca_market_hours.ml:11-105). Session boundaries are minute-granular, so a
       30s TTL is correct. *)
    let mh_cache_seconds = 30.0 in
    let mh_cache = ref (None : (float * bool) option) in
    (* Cached strategy state refs, avoiding repeated mutex acquisition on the hot path.
       References are stable while is_running is true. *)
    let cached_grid_state =
      if is_grid_strategy
      then Some (Dio_strategies.Jacobs_ladder.get_strategy_state asset_with_fees.symbol)
      else None
    in
    let cached_mm_state =
      if is_mm_strategy
      then Some (Dio_strategies.Market_maker.get_strategy_state asset_with_fees.symbol)
      else None
    in
    let cached_fng_check_threshold = config.fng_check_threshold in
    let wakeup_sync =
      Concurrency.Exchange_wakeup.get_sync_handle asset_with_fees.symbol
    in
    while Atomic.get state.is_running do
      let latency_this_cycle = !latency_active in
      if !cycle_count = 0 then Logging.debug_f ~section "First cycle for %s" key;
      incr cycle_count;
      (* Capture the wakeup generation before reading producer state. Any signal from here
         to the [wait_since] at the loop bottom bumps the generation past this baseline,
         so the wait returns immediately instead of parking through data that landed
         mid-cycle. *)
      let wake_baseline = Concurrency.Exchange_wakeup.get_generation_fast wakeup_sync in
      let cycle_events = ref 0 in
      let lifecycle_events = ref 0 in
      (* Latency safepoint. The EVENTS drain below runs synchronously on this domain
         thread and allocates; a minor collection triggered inside a multi-event batch
         would be charged to every event in it. Forcing the domain's pending minor
         collection here, before any measured span, starts the drain with a free minor
         heap. The exec position is sampled once and reused by the drain so gate and
         iteration see the same producer position. *)
      let current_pos = get_exec_pos_fn () in
      let did_exec = current_pos <> !exec_read_pos in
      if did_exec then Gc.minor ();
      (* Per-cycle GC counters, captured at cycle start and at the cause site to attribute
         spikes to minor/major collections. The start capture is sampled per
         [gc_sample_mask]; taken before the stage markers so it is not charged to the ob
         bracket. *)
      let gc_sampled = latency_this_cycle && !cycle_count land gc_sample_mask = 0 in
      let stats_start = if gc_sampled then Gc_monitor.get_stats () else Gc_monitor.zero in
      (* Stage timing uses non-allocating [Monotonic_clock] rather than
         [Mtime_clock.now_ns] (3 boxed words per read), so the profiler's clock does not
         pollute the allocation counts it measures. *)
      let t1 = if latency_this_cycle then Monotonic_clock.now_ns () else 0 in
      let alloc_start =
        if latency_this_cycle then int_of_float (Gc.minor_words ()) else 0
      in
      (* Drain lifecycle events queued by the supervisor REST path. All handlers (REST-
         and WS-sourced) run on THIS domain thread at cycle top, so the strategy mutex is
         never contended across threads. Runs unconditionally; the queue is empty on the
         common cycle. *)
      if is_grid_strategy
      then
        lifecycle_events
        := !lifecycle_events
           + Dio_strategies.Jacobs_ladder.Strategy.drain_events asset_with_fees.symbol;
      (* Drain MM lifecycle events queued by the supervisor REST path. Same discipline as
         the grid queue: handlers run on THIS domain thread, so MM state is never mutated
         cross-thread against execute_strategy. *)
      if is_mm_strategy
      then
        lifecycle_events
        := !lifecycle_events
           + Dio_strategies.Market_maker.Strategy.drain_events asset_with_fees.symbol;
      (* === ORDERBOOK HOT PATH === *)
      let ob_pos = get_ob_pos_fn () in
      let did_ob =
        ob_pos <> !orderbook_read_pos || (!orderbook_read_pos = 0 && ob_pos > 0)
      in
      if did_ob
      then (
        orderbook_read_pos := ob_pos;
        match get_tob_fn () with
        | Some (bid_price, bid_size, ask_price, ask_size) ->
          let changed = bid_price <> !tob_bid || ask_price <> !tob_ask in
          tob_bid := bid_price;
          tob_ask := ask_price;
          tob_bsize := bid_size;
          tob_asize := ask_size;
          current_price := (bid_price +. ask_price) /. 2.0;
          if changed then should_execute_strategy := true
        | None -> ());
      let t2 = if latency_this_cycle then Monotonic_clock.now_ns () else 0 in
      let alloc_at_t2 =
        if latency_this_cycle then int_of_float (Gc.minor_words ()) else 0
      in
      if did_ob && latency_this_cycle then Latency_profiler.record_ns prof_ob (t2 - t1);
      let was_exec_ready = !exec_ready in
      let event_count = ref 0 in
      if did_exec
      then (
        open_orders_dirty := true;
        let now_exec = Unix.gettimeofday () in
        let new_pos =
          Ex.iter_execution_events
            ~symbol:asset_with_fees.symbol
            ~start_pos:!exec_read_pos
            (fun (event : Types.execution_event) ->
               incr event_count;
               incr cycle_events;
               match event.order_status with
               | Types.Canceled | Types.Rejected | Types.Expired ->
                 should_execute_strategy := true;
                 (* A canceled/rejected/expired order changes the live pool: notify the
                    capital oracle with released committed capital (a canceled BUY returns
                    remaining_qty x limit price) so it re-sizes in-process without a
                    network wait. *)
                 Oracle_runtime.notify_order_cancel
                   ~exchange:asset_with_fees.exchange
                   ~symbol:asset_with_fees.symbol
                   ~testnet:asset_with_fees.testnet
                   ~side:event.side
                   ~value:
                     (event.remaining_qty *. Option.value event.limit_price ~default:0.0);
                 let side =
                   match event.side with
                   | Types.Buy -> Dio_strategies.Strategy_common.Buy
                   | Types.Sell -> Dio_strategies.Strategy_common.Sell
                 in
                 if is_grid_strategy
                 then
                   Dio_strategies.Jacobs_ladder.Strategy.handle_order_cancelled
                     ~now:now_exec
                     asset_with_fees.symbol
                     event.order_id
                     side
                     event.cl_ord_id;
                 if is_mm_strategy
                 then
                   Dio_strategies.Market_maker.Strategy.handle_order_cancelled
                     ~now:now_exec
                     asset_with_fees.symbol
                     event.order_id
                     side
                     event.cl_ord_id
               | Types.Filled ->
                 should_execute_strategy := true;
                 (* A fill consumes/returns quote: notify the capital oracle with the pool
                    delta so it re-sizes the asset and the account's priority order
                    in-process, without a network balance refresh (lock-free, microsecond
                    wake). *)
                 Oracle_runtime.notify_fill
                   ~exchange:asset_with_fees.exchange
                   ~symbol:asset_with_fees.symbol
                   ~testnet:asset_with_fees.testnet
                   ~side:event.side
                   ~filled_qty:event.filled_qty
                   ~avg_price:event.avg_price
                   ~fee:
                     (Option.value asset_with_fees.maker_fee ~default:0.001
                      *. event.filled_qty
                      *. event.avg_price);
                 let side =
                   match event.side with
                   | Types.Buy -> Dio_strategies.Strategy_common.Buy
                   | Types.Sell -> Dio_strategies.Strategy_common.Sell
                 in
                 if is_grid_strategy
                 then
                   Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
                     ~now:now_exec
                     asset_with_fees.symbol
                     event.order_id
                     side
                     ~fill_price:event.avg_price
                     ~fill_qty:event.filled_qty
                     event.cl_ord_id;
                 if is_mm_strategy
                 then
                   Dio_strategies.Market_maker.Strategy.handle_order_filled
                     ~now:now_exec
                     asset_with_fees.symbol
                     event.order_id
                     side
                     ~fill_price:event.avg_price
                     ~fill_qty:event.filled_qty
                     event.cl_ord_id;
                 (* Trigger Auto-Hedging module *)
                 if asset_with_fees.hedge
                 then (
                   let hedge_symbol =
                     String.split_on_char '/' asset_with_fees.symbol |> List.hd
                   in
                   let perp_tob = Ex.get_top_of_book ~symbol:hedge_symbol in
                   Dio_strategies.Auto_hedger.handle_order_filled
                     asset_with_fees.testnet
                     asset_with_fees.exchange
                     hedge_symbol
                     side
                     event.filled_qty
                     event.avg_price
                     perp_tob)
               | Types.New | Types.PartiallyFilled ->
                 should_execute_strategy := true;
                 (* Skip handle_order_acknowledged for in-place amendment confirmations
                    (Kraken exec_type=amended, status=new). The amendment lifecycle runs
                    through the supervisor's handle_order_amended callback on the REST
                    path; routing these here causes a dual-update race that corrupts
                    open_sell_orders tracking. *)
                 if event.is_amended
                 then (
                   Logging.debug_f
                     ~section
                     "AMENDED_WS_EVENT %s [%s] status=%s (updating strategy tracker)"
                     event.order_id
                     asset_with_fees.symbol
                     (match event.order_status with
                      | Types.New -> "New"
                      | Types.PartiallyFilled -> "PartiallyFilled"
                      | _ -> "Other");
                   match event.limit_price with
                   | Some price when price > 0.0 ->
                     let side =
                       match event.side with
                       | Types.Buy -> Dio_strategies.Strategy_common.Buy
                       | Types.Sell -> Dio_strategies.Strategy_common.Sell
                     in
                     if is_grid_strategy
                     then
                       Dio_strategies.Jacobs_ladder.Strategy.handle_order_amended
                         ~now:now_exec
                         asset_with_fees.symbol
                         event.order_id
                         event.order_id
                         side
                         price;
                     if is_mm_strategy
                     then
                       Dio_strategies.Market_maker.Strategy.handle_order_amended
                         ~now:now_exec
                         asset_with_fees.symbol
                         event.order_id
                         event.order_id
                         side
                         price
                   | _ -> ())
                 else (
                   match event.limit_price with
                   | Some price when price > 0.0 ->
                     let side =
                       match event.side with
                       | Types.Buy -> Dio_strategies.Strategy_common.Buy
                       | Types.Sell -> Dio_strategies.Strategy_common.Sell
                     in
                     if is_grid_strategy
                     then
                       Dio_strategies.Jacobs_ladder.Strategy.handle_order_acknowledged
                         ~now:now_exec
                         asset_with_fees.symbol
                         event.order_id
                         side
                         price;
                     if is_mm_strategy
                     then
                       Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
                         ~now:now_exec
                         asset_with_fees.symbol
                         event.order_id
                         side
                         price
                   | Some _ -> ()
                   | None -> ())
               | _ -> ())
        in
        if !event_count > 0
        then
          (* First exec batch received: open the startup gate for ALL exchanges *)
          if not !exec_ready
          then (
            exec_ready := true;
            exec_ready_cycle := !cycle_count;
            if is_grid_strategy
            then
              Dio_strategies.Jacobs_ladder.Strategy.set_startup_replay_done
                asset_with_fees.symbol;
            if is_mm_strategy
            then
              Dio_strategies.Market_maker.Strategy.set_startup_replay_done
                asset_with_fees.symbol;
            Logging.debug_f
              ~section
              "[%s/%s] First exec event batch received, strategy now active"
              asset_with_fees.exchange
              asset_with_fees.symbol);
        exec_read_pos := new_pos;
        exec_checked := true);
      let t3 = if latency_this_cycle then Monotonic_clock.now_ns () else 0 in
      let alloc_at_t3 =
        if latency_this_cycle then int_of_float (Gc.minor_words ()) else 0
      in
      (* Per-event exec histogram writes are deferred until after [t4] so they are not
         charged to STRAT/CYCLE; with N events they would self-inflate the exec-heavy
         cycles that define the tail. *)
      (* [-1] means "no exec events this cycle" (sentinel, no option boxing). *)
      let exec_per_event_ns =
        if did_exec && latency_this_cycle && was_exec_ready && !event_count > 0
        then (
          let elapsed_ns = t3 - t2 in
          if !event_count > 1 then elapsed_ns / !event_count else elapsed_ns)
        else -1
      in
      (* Fallback gate for domains with no open orders: if no exec events arrived and the
         snapshot is ingested, open the gate so the strategy can place its initial order. *)
      if (not !exec_ready) && (not !exec_checked) && has_exec_fn ()
      then (
        let current_pos_now = get_exec_pos_fn () in
        if current_pos_now = !exec_read_pos
        then (
          exec_checked := true;
          (* No exec events and feed ready: fetch snapshot orders and inject them to
             restore strategy tracking state. *)
          (* Timestamp hoisted outside the per-order callback: one fewer gettimeofday
             syscall per open order. *)
          let now_inject = Unix.gettimeofday () in
          Ex.iter_open_orders_fast
            ~symbol:asset_with_fees.symbol
            (fun oid price _qty side_str userref_opt ->
               let is_mm =
                 match userref_opt with
                 | Some uref ->
                   Dio_strategies.Strategy_common.is_strategy_order
                     Dio_strategies.Strategy_common.strategy_userref_mm
                     uref
                 | None -> false
               in
               let order_side =
                 if side_str = "buy"
                 then Dio_strategies.Strategy_common.Buy
                 else Dio_strategies.Strategy_common.Sell
               in
               if is_mm
               then (
                 if is_mm_strategy
                 then
                   Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
                     ~now:now_inject
                     asset_with_fees.symbol
                     oid
                     order_side
                     price)
               else if is_grid_strategy
               then
                 Dio_strategies.Jacobs_ladder.Strategy.handle_order_acknowledged
                   ~now:now_inject
                   asset_with_fees.symbol
                   oid
                   order_side
                   price);
          exec_ready := true;
          exec_ready_cycle := !cycle_count;
          (* Mark startup replay complete to ungate profit calculation *)
          if is_grid_strategy
          then
            Dio_strategies.Jacobs_ladder.Strategy.set_startup_replay_done
              asset_with_fees.symbol;
          if is_mm_strategy
          then
            Dio_strategies.Market_maker.Strategy.set_startup_replay_done
              asset_with_fees.symbol;
          Logging.debug_f
            ~section
            "[%s/%s] Snapshot done, injected open orders - strategy now active"
            asset_with_fees.exchange
            asset_with_fees.symbol));
      (* Execute strategy if new events have been consumed and feed is ready (event-driven
         gate) *)
      (* Equity market-hours gate: suppress strategy execution when the US equity market
         is closed. Otherwise the strategy amends against stale delayed data; the gateway
         rejects with error 354 (no market data) while in-memory state records the amend
         as successful, causing an infinite amend loop. *)
      let equity_market_closed =
        match asset_with_fees.exchange with
        | "ibkr" | "alpaca" ->
          let now_mh = Unix.gettimeofday () in
          (match !mh_cache with
           | Some (t, closed) when now_mh -. t < mh_cache_seconds -> closed
           | _ ->
             let closed =
               (asset_with_fees.exchange = "ibkr"
                && not (Ibkr.Market_hours.is_market_open ()))
               || (asset_with_fees.exchange = "alpaca"
                   && not (Alpaca.Market_hours.is_market_open ()))
             in
             mh_cache := Some (now_mh, closed);
             closed)
        | _ -> false
      in
      (* Capital-oracle decision application. Read every cycle (lock-free Atomic.get of an
         immutable snapshot) so a halted asset can re-activate as soon as the runtime
         publishes, not only on market events. Runs OUTSIDE the should_execute gate: an
         inactive asset never enters the execution block, so its re-activation must not
         depend on it. The lookup is cached per publish generation; [decision_for] is
         re-invoked only when a new pass is published. *)
      let oracle_decision =
        if !oracle_gen_cached <> Oracle_runtime.get_publish_generation ()
        then (
          oracle_gen_cached := Oracle_runtime.get_publish_generation ();
          oracle_decision_cached
          := Oracle_runtime.decision_for
               ~exchange:asset_with_fees.exchange
               ~symbol:asset_with_fees.symbol;
          !oracle_decision_cached)
        else !oracle_decision_cached
      in
      let oracle_halted =
        match oracle_decision with
        | Some d when d.cancel_resting_buys -> true
        | Some d when not d.active ->
          (* Allocation-free scan: [iter_open_orders_fast] yields primitives, no
             [Types.open_order] record per order per idle cycle. *)
          let has_open_buy = ref false in
          Ex.iter_open_orders_fast
            ~symbol:asset_with_fees.symbol
            (fun _oid _price qty side_str _userref ->
               if qty > 0.0 && side_str = "buy" then has_open_buy := true);
          not !has_open_buy
        | _ -> false
      in
      (match oracle_decision, !grid_strategy_asset_ref with
       | Some d, None ->
         (* Materialize the grid strategy on the FIRST oracle decision, ACTIVE or
            INACTIVE. A halted asset must still run its sell leg (adopt resting inventory,
            track fills/cancels, place inventory sells); buys are withheld by
            [oracle_halted] inside execute_strategy, not by skipping execution. An
            INACTIVE decision may carry a zero/placeholder buy size, so fall back to the
            configured qty. *)
         let qty_str =
           if d.buy_qty > 0.0
           then Printf.sprintf "%.8g" d.buy_qty
           else asset_with_fees.qty
         in
         grid_strategy_asset_ref
         := Some (grid_asset_of ~qty:qty_str ~grid_interval:d.grid_interval ());
         let st =
           Dio_strategies.Jacobs_ladder.get_strategy_state asset_with_fees.symbol
         in
         (try st.grid_qty <- float_of_string qty_str with
          | Failure _ -> ());
         (* Re-check any resting buy against the decision's spacing: it amends DOWN only
            when the resting price violates a ladder constraint (inside a sell's 2*gi
            restricted zone); an order within one grid interval of market is left to trail
            up. Armed only by an ACTIVE decision; under halt the buy leg places/re-anchors
            nothing. *)
         if d.active then st.force_buy_reanchor <- true;
         should_execute_strategy := true;
         Logging.debug_f
           ~section
           "[%s/%s] Capital oracle decision materialized (%s): qty %s gi %.4f%% (D_surv \
            %.1f%%)"
           asset_with_fees.exchange
           asset_with_fees.symbol
           (if d.active then "ACTIVE" else "INACTIVE")
           qty_str
           d.grid_interval
           (d.d_surv *. 100.0)
       | Some d, Some asset when d.active ->
         (* The oracle re-derives qty from the live pool every pass, so successive passes
            publish micro-different values (e.g. QQQ 0.03877239 -> 0.03877509). Exact
            string comparison would trip [qty_changed] every pass, forcing a buy re-anchor
            and an Alpaca amend (cancel+create) loop. Judge numerically with a 0.1%
            relative deadband so only a material re-size re-anchors. *)
         let qty_changed =
           let current_qty =
             try float_of_string asset.qty with
             | Failure _ -> 0.0
           in
           abs_float (d.buy_qty -. current_qty) > max (current_qty *. 0.001) 1e-9
         in
         let gi_changed = abs_float (d.grid_interval -. asset.grid_interval) > 1e-12 in
         if qty_changed || gi_changed
         then (
           let qty_str = Printf.sprintf "%.8g" d.buy_qty in
           let new_asset =
             { asset with qty = qty_str; grid_interval = d.grid_interval }
           in
           grid_strategy_asset_ref := Some new_asset;
           let st = Dio_strategies.Jacobs_ladder.get_strategy_state asset.symbol in
           (try st.grid_qty <- float_of_string qty_str with
            | Failure _ -> ());
           (* A qty-only change is adopted without forcing a buy re-anchor: the grid's
              qty-mismatch amend (Alpaca) fixes size at the same price (buys only trail
              up), and other venues take the new size on the next placement. Forcing a
              price re-anchor on qty drift caused grid/oracle cancel+create churn. Only a
              grid-interval change re-checks the resting buy; the amend-down itself stays
              gated on a sell-spacing violation. *)
           if gi_changed then st.force_buy_reanchor <- true;
           should_execute_strategy := true;
           Logging.debug_f
             ~section
             "[%s/%s] Capital oracle updated sizing: qty %.8g gi %.4f%% (D_surv %.1f%%)"
             asset.exchange
             asset.symbol
             d.buy_qty
             d.grid_interval
             (d.d_surv *. 100.0))
       | _ -> ());
      (* Log only on active<->inactive transitions, not every cycle. *)
      if oracle_halted <> !oracle_halted_prev
      then (
        oracle_halted_prev := oracle_halted;
        match oracle_decision with
        | Some d when d.active ->
          Logging.info_f
            ~section
            "[%s/%s] Capital oracle re-activated (qty %.8g gi %.4f%%); resuming orders"
            asset_with_fees.exchange
            asset_with_fees.symbol
            d.buy_qty
            d.grid_interval;
          if is_grid_strategy
          then (
            let st =
              Dio_strategies.Jacobs_ladder.get_strategy_state asset_with_fees.symbol
            in
            st.capital_low <- false;
            st.capital_low_logged <- false;
            st.capital_low_at_balance <- 0.0)
        | Some d ->
          Logging.warn_f
            ~section
            "[%s/%s] Capital oracle INACTIVE: %s (D_surv %.1f%%, qty %.8g, gi %.4f%%); \
             new orders suspended, fills still tracked"
            asset_with_fees.exchange
            asset_with_fees.symbol
            (if d.reason = "" then "capital reallocated" else d.reason)
            (d.d_surv *. 100.0)
            d.buy_qty
            d.grid_interval
        | None ->
          Logging.info_f
            ~section
            "[%s/%s] No capital-oracle decision; sizing falls back to Fear & Greed alone \
             (orders withheld if no live F&G reading exists)"
            asset_with_fees.exchange
            asset_with_fees.symbol);
      (* Priority reclamation: an INACTIVE-with-reclaim decision asks this domain to
         cancel its resting buys so committed capital returns to the account pool for a
         higher-priority asset. Runs OUTSIDE the execution gate: a halted asset must still
         release capital. The cancel is pushed through the grid's order buffer into the
         supervisor pipeline (supervisor_orders dispatch_cancel -> Order_executor ->
         dashboard/order tracking), guarded like the grid's own excess-buy cancel
         (strategy mutex held, mid-amendment buys skipped because Hyperliquid rejects
         canceling an order being amended). SELF-HEALING: the cancel is latched, not
         re-issued every cycle, but retried while the decision persists and eligible buys
         remain; the latch re-arms when the store shows no eligible buy or the decision
         stops being a reclaim. Without the retry, a single failed cancel leaves the asset
         paused permanently. Wakes the oracle ([request_pass]) so released capital is
         recognized even if the exchange WS cancel event is missed. *)
      (match oracle_decision with
       | Some d when d.cancel_resting_buys ->
         let now = Unix.gettimeofday () in
         let st =
           Dio_strategies.Jacobs_ladder.get_strategy_state asset_with_fees.symbol
         in
         Mutex.lock st.mutex;
         Fun.protect
           ~finally:(fun () -> Mutex.unlock st.mutex)
           (fun () ->
             (* Eligible = cancellable resting buys (not mid-amendment). [any_buy]
                distinguishes "store clean" from "only buys stuck mid-amendment": a
                mid-amend buy cannot be cancelled and is expected to resolve into a
                cancellable replacement. *)
             let eligible = ref 0 in
             let any_buy = ref false in
             (* Allocation-free: primitives only, no [Types.open_order] record. *)
             Ex.iter_open_orders_fast
               ~symbol:asset_with_fees.symbol
               (fun oid _price qty side_str _userref ->
                  if qty > 0.0 && side_str = "buy"
                  then (
                    any_buy := true;
                    if not
                         (Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight
                            oid)
                    then incr eligible));
             match
               Dio_strategies.Jacobs_ladder.reclaim_step
                 ~now
                 ~retry_seconds:reclaim_retry_seconds
                 ~issued:!reclaim_cancel_issued
                 ~issued_at:!reclaim_cancel_at
                 ~eligible:!eligible
                 ~any_buy:!any_buy
             with
             | Dio_strategies.Jacobs_ladder.Reclaim_rearm ->
               (* No buy remains: the cancel(s) landed or were never needed. Re-arm the
                  latch so a later reclaim re-triggers cleanly, and wake the oracle so it
                  re-sizes with the released capital. *)
               reclaim_cancel_issued := false;
               reclaim_cancel_at := 0.0;
               Oracle_runtime.request_pass ()
             | Dio_strategies.Jacobs_ladder.Reclaim_cancel _ ->
               let n = ref 0 in
               Ex.iter_open_orders_fast
                 ~symbol:asset_with_fees.symbol
                 (fun oid _price qty side_str _userref ->
                    if qty > 0.0
                       && side_str = "buy"
                       && not
                            (Dio_strategies.Strategy_common.InFlightAmendments
                             .is_in_flight
                               oid)
                    then (
                      let cancel =
                        Dio_strategies.Jacobs_ladder.create_cancel_order
                          oid
                          asset_with_fees.symbol
                          Dio_strategies.Strategy_common.Ladder
                          asset_with_fees.exchange
                      in
                      ignore (Dio_strategies.Jacobs_ladder.push_order ~now cancel);
                      incr n));
               reclaim_cancel_issued := true;
               reclaim_cancel_at := now;
               (* Wake the capital oracle so it re-sizes as soon as the release lands; the
                  reclaim cycle does not depend on the exchange WS cancel event. *)
               Oracle_runtime.request_pass ();
               Logging.warn_f
                 ~section
                 "[%s/%s] Capital oracle cancellation cascade: canceling %d resting \
                  buy(s) to free quote for a higher-priority strategy"
                 asset_with_fees.exchange
                 asset_with_fees.symbol
                 !n
             | Dio_strategies.Jacobs_ladder.Reclaim_deferred -> ())
       | _ ->
         reclaim_cancel_issued := false;
         reclaim_cancel_at := 0.0);
      (* Oracle startup gate. Opens once, monotonically, on the first capital-oracle
         decision for this asset (ACTIVE or INACTIVE; an INACTIVE decision halts new
         orders through oracle_halted above). There is no F&G or config fallback sizing
         path: with no decision the execute flag stays cleared, the domain falls through
         to the per-symbol wakeup wait, and a one-shot warning fires once the grace period
         elapses. *)
      if not !oracle_gate_open
      then (
        (* Gate opens ONLY on a capital-oracle decision; no F&G/config fallback sizing
           exists. Until a decision arrives the strategy places nothing. One-shot warnings
           distinguish: cold start (first history refresh running), analysis failed (pass
           finished with no decision), startup elapsed (no pass completed), unmodeled (no
           capital-survival adapter). *)
        match oracle_decision with
        | Some d ->
          oracle_gate_open := true;
          should_execute_strategy := true;
          Logging.debug_f
            ~section
            "[%s/%s] Capital oracle first decision received (%s, qty %.8g gi %.4f%%, \
             D_surv %.1f%%); grid gate open"
            asset_with_fees.exchange
            asset_with_fees.symbol
            (if d.active then "ACTIVE" else "INACTIVE")
            d.buy_qty
            d.grid_interval
            (d.d_surv *. 100.0)
        | None ->
          should_execute_strategy := false;
          let startup_window_elapsed =
            Oracle_runtime.first_pass_attempt_done ()
            || Unix.gettimeofday () >= !oracle_gate_deadline
          in
          if startup_window_elapsed && not !no_signal_warned
          then (
            no_signal_warned := true;
            if not oracle_tracks_asset
            then
              Logging.info_f
                ~section
                "[%s/%s] Asset not modeled by the capital oracle; orders withheld"
                asset_with_fees.exchange
                asset_with_fees.symbol
            else (
              match Oracle_runtime.materialized () with
              | None ->
                Logging.warn_f
                  ~section
                  "[%s/%s] Capital-oracle first history refresh still in progress; \
                   orders withheld (no fallback sizing exists)"
                  asset_with_fees.exchange
                  asset_with_fees.symbol
              | Some _ when Oracle_runtime.first_pass_attempt_done () ->
                Logging.warn_f
                  ~section
                  "[%s/%s] Capital-oracle produced no decision for this asset (analysis \
                   failed); orders withheld"
                  asset_with_fees.exchange
                  asset_with_fees.symbol
              | Some _ ->
                Logging.warn_f
                  ~section
                  "[%s/%s] Capital-oracle never completed a pass; orders withheld"
                  asset_with_fees.exchange
                  asset_with_fees.symbol)))
      else ();
      (* The oracle halt gates only BUY placement (the [~oracle_halted] flag to
         execute_strategy); the SELL leg still runs. A sell is the capital- recovery path
         (needs inventory, not quote), so the sell for a just-filled buy is placed even
         under halt; otherwise the last fill's inventory stays unreclaimable. *)
      let should_execute =
        !exec_ready
        && !should_execute_strategy
        && has_exec_fn ()
        && (not equity_market_closed)
        && !oracle_gate_open
      in
      (* PREP/STRATEGY boundary. [t3] ends the exec phase; the block below up to the
         strategy call (oracle apply, halt/reclaim, startup gate, balance reads, F&G
         re-evaluation) is PREP. [t3_strategy] is seeded here so idle cycles still report
         true prep cost. The [should_execute] branch below re-stamps it after the
         balance/F&G block, so STRAT is the strategy call alone. *)
      let t3_strategy =
        ref (if latency_this_cycle then Monotonic_clock.now_ns () else t3)
      in
      let alloc_at_t3s =
        ref (if latency_this_cycle then int_of_float (Gc.minor_words ()) else alloc_at_t3)
      in
      if should_execute
      then (
        should_execute_strategy := false;
        (* Single-pass open-order scan: counts by strategy and collects grid buy/sell
           lists, eliminating a second iter_open_orders plus orders_mutex acquisition
           inside the grid strategy. *)
        let iter_orders f = Ex.iter_open_orders_fast ~symbol:asset_with_fees.symbol f in
        (* Pass the iter_orders closure down directly, avoiding intermediate
           order-tracking list allocations (2-3ms STW pause) on the event path. *)
        (* Fast-path balance access without hashtable locks. *)
        let asset_bal_val =
          match base_balance_fn () with
          | bal -> bal
          | exception _ -> nan
        in
        let quote_bal_val =
          match quote_balance_fn () with
          | bal -> bal
          | exception _ -> nan
        in
        (* Balance-snapshot staleness: a fresh quote balance is authoritative, so an
           under-funded buy is skipped; a stale snapshot may be wrong, so the grid
           attempts and lets the exchange decide. Unknown age (None) is treated as stale. *)
        let quote_balance_stale =
          match Ex.get_balance_age_fast ~asset:quote_currency () with
          | Some age -> age > stale_balance_age_seconds
          | None -> true
        in
        (* Trigger async Fear & Greed refresh on significant price movement *)
        if not (Float.is_nan !current_price)
        then (
          let cp = !current_price in
          match !baseline_price with
          | None -> baseline_price := Some cp
          | Some base ->
            let diff_pct = abs_float ((cp -. base) /. base) *. 100.0 in
            if diff_pct >= cached_fng_check_threshold
            then (
              Logging.info_f
                ~section
                "[%s/%s] Price moved by %.2f%% from baseline $%.2f to $%.2f. Triggering \
                 dynamic Fear & Greed check."
                asset_with_fees.exchange
                asset_with_fees.symbol
                diff_pct
                base
                cp;
              baseline_price := Some cp;
              Fear_and_greed.force_fetch_async ()));
        (* Applies an updated Fear & Greed value if changed. A missing index (get_cached
           () = None) means no live signal: re-evaluation is skipped, never neutralized
           to 50. *)
        let current_fng_opt = Fear_and_greed.get_cached () in
        if current_fng_opt <> !last_known_fng
        then (
          last_known_fng := current_fng_opt;
          match current_fng_opt with
          | None -> ()
          | Some current_fng when not (is_crypto_exchange asset_with_fees.exchange) ->
            (* Equities are pure oracle: F&G never enters sizing. Log so the ignored
               signal is visible; last_known_fng bookkeeping still updates so a later
               change re-checks. *)
            Logging.debug_f
              ~section
              "[%s/%s] Fear & Greed updated to %.2f but ignored: equity asset sizes from \
               the capital oracle only (pure oracle)"
              asset_with_fees.exchange
              asset_with_fees.symbol
              current_fng
          | Some current_fng ->
            (* The capital oracle computes the crypto grid interval as a weighted blend of
               the F&G side, the per-asset range side, and the survival-constrained
               parameter. The decision is the sole sizing source; F&G does not size
               grid_interval. F&G still manages accumulation_buffer, which the oracle does
               not size. *)
            let update_accumulation_buffer () =
              let exch_id =
                Dio_exchange.Exchange_intf.Types.exchange_of_string
                  asset_with_fees.exchange
              in
              let is_accumulation_exch =
                match exch_id with
                | Hyperliquid | Ibkr | Lighter | Alpaca | Kraken -> true
                | Custom _ -> false
              in
              if is_accumulation_exch
              then (
                let ab_lo, ab_hi = asset_with_fees.accumulation_buffer in
                let new_ab =
                  Fear_and_greed.grid_value_for_fng
                    ~grid_interval:asset_with_fees.accumulation_buffer
                    ~fear_and_greed:current_fng
                in
                Logging.debug_f
                  ~section
                  "[%s/%s] Re-evaluated accumulation_buffer to %.4f (range %.4f-%.4f)"
                  asset_with_fees.exchange
                  asset_with_fees.symbol
                  new_ab
                  ab_lo
                  ab_hi;
                match !grid_strategy_asset_ref with
                | Some asset ->
                  let new_asset =
                    { asset with
                      Dio_strategies.Jacobs_ladder.accumulation_buffer = new_ab
                    }
                  in
                  grid_strategy_asset_ref := Some new_asset
                | None -> ())
            in
            (match oracle_decision with
             | Some d when d.active ->
               (* Oracle owns sizing: log its published gi/qty/D_surv and touch only the
                  accumulation buffer. *)
               Logging.info_f
                 ~section
                 "[%s/%s] Fear & Greed updated to %.2f: oracle sizing gi %.4f%% · qty \
                  %.6g · D_surv %.1f%%"
                 asset_with_fees.exchange
                 asset_with_fees.symbol
                 current_fng
                 d.grid_interval
                 d.buy_qty
                 (d.d_surv *. 100.0);
               update_accumulation_buffer ()
             | Some _ ->
               (* Oracle decision exists but is INACTIVE: it owns sizing and orders are
                  withheld; no competing F&G value is applied. *)
               Logging.debug_f
                 ~section
                 "[%s/%s] F&G gi re-evaluation skipped: capital-oracle decision INACTIVE \
                  (orders withheld)"
                 asset_with_fees.exchange
                 asset_with_fees.symbol;
               update_accumulation_buffer ()
             | None ->
               (* No oracle decision yet: no config/F&G fallback sizing, so the strategy
                  places nothing. This only refreshes the F&G-resolved accumulation buffer
                  reference. *)
               Logging.debug_f
                 ~section
                 "[%s/%s] No capital-oracle decision yet; no fallback sizing (strategy \
                  stays quiet until the oracle publishes)"
                 asset_with_fees.exchange
                 asset_with_fees.symbol;
               update_accumulation_buffer ()));
        (* Wall-clock timestamp computed once per cycle for strategy use, eliminating
           gettimeofday syscalls inside the strategy. *)
        let now = Unix.gettimeofday () in
        (* Activity tick so the dashboard reports executions/sec and last-execution time
           even with zero latency samples this window. *)
        Latency_profiler.tick_exec prof_strategy ~now;
        (* PREP/STRATEGY split: work above is charged to [prof_prep]; only the strategy
           call is STRAT. *)
        t3_strategy := if latency_this_cycle then Monotonic_clock.now_ns () else 0;
        alloc_at_t3s := if latency_this_cycle then int_of_float (Gc.minor_words ()) else 0;
        (match !grid_strategy_asset_ref, cached_grid_state with
         | Some asset, Some cs ->
           Dio_strategies.Jacobs_ladder.Strategy.execute
             ~cached_state:cs
             ~quote_balance_stale
             ~oracle_halted
             ~get_open_orders_generation:(fun () ->
               Ex.get_open_orders_generation ~symbol:asset_with_fees.symbol)
             ~base_balance_age:(base_balance_age_fn ())
             ~now
             asset
             !current_price
             !tob_bid
             !tob_ask
             asset_bal_val
             quote_bal_val
             0
             0
             iter_orders
             !cycle_count
         | _ -> ());
        match !mm_strategy_asset_ref, cached_mm_state with
        | Some asset, Some cs when not oracle_halted ->
          let mm_cp = if Float.is_nan !current_price then None else Some !current_price in
          let mm_tob =
            if Float.is_nan !tob_bid
            then None
            else Some (!tob_bid, !tob_bsize, !tob_ask, !tob_asize)
          in
          let mm_abal = if Float.is_nan asset_bal_val then None else Some asset_bal_val in
          let mm_qbal = if Float.is_nan quote_bal_val then None else Some quote_bal_val in
          Dio_strategies.Market_maker.Strategy.execute
            ~cached_state:cs
            asset
            mm_cp
            mm_tob
            mm_abal
            mm_qbal
            0
            0
            iter_orders
            !cycle_count
        | _ -> ());
      (match trace_recorder with
       | Some r when did_ob || did_exec || should_execute ->
         List.iter
           (fun (o : Types.open_order) ->
             let open Dio_strategies.Strategy_trace in
             Dio_strategies.Strategy_event_recorder.record_order_intent
               r
               { oi_symbol = asset_with_fees.symbol
               ; oi_side =
                   (match o.side with
                    | Types.Buy -> "buy"
                    | Types.Sell -> "sell")
               ; oi_qty = o.qty
               ; oi_price =
                   (match o.limit_price with
                    | Some p -> p
                    | None -> nan)
               ; oi_post_only = false
               ; oi_reduce_only = false
               ; oi_tif = None
               })
           (Ex.get_open_orders ~symbol:asset_with_fees.symbol);
         Dio_strategies.Strategy_event_recorder.record_state
           r
           [ "price", Dio_strategies.Strategy_expr.V_float !current_price ];
         Dio_strategies.Strategy_event_recorder.end_cycle r;
         incr trace_cycles;
         if !trace_cycles mod 50 = 0
         then
           Dio_strategies.Strategy_trace.save
             trace_path
             (Dio_strategies.Strategy_event_recorder.snapshot r)
       | _ -> ());
      let t4 = if latency_this_cycle then Monotonic_clock.now_ns () else 0 in
      let alloc_at_t4 =
        if latency_this_cycle then int_of_float (Gc.minor_words ()) else 0
      in
      (* PREP is recorded on every measured cycle: oracle apply / halt / reclaim / gate
         work runs on idle cycles too, and folding it into CYCLE made those cycles
         unattributable. STRAT is the strategy call alone. *)
      if latency_this_cycle
      then (
        Latency_profiler.record_ns prof_prep (!t3_strategy - t3);
        if should_execute then Latency_profiler.record_ns prof_strategy (t4 - !t3_strategy));
      (* Exec histogram writes deferred from [t3] (see above): now outside both the STRAT
         and CYCLE measured spans. *)
      if exec_per_event_ns >= 0
      then
        for _ = 1 to !event_count do
          Latency_profiler.record_ns prof_exec exec_per_event_ns
        done;
      (* Flush deferred accumulation persistence outside the strategy hot path; file I/O
         only when the dirty flag was set during execute_strategy. *)
      if should_execute
      then
        if is_grid_strategy
        then
          Dio_strategies.Jacobs_ladder.Strategy.flush_persistence asset_with_fees.symbol;
      (* Records active cycle work time before blocking, excluding
         Exchange_wakeup.wait_since sleep. Only busy cycles are recorded; idle wakeups
         would pin cycle p50/p99 at 0us. *)
      let cycle_busy = did_ob || did_exec || should_execute in
      if latency_this_cycle && cycle_busy
      then
        if (* Cause string is built only for a new window maximum, avoiding a per-cycle
              closure and [alloc_start] box. *)
           Latency_profiler.record_max_ns prof_cycle (t4 - t1)
        then (
          let alloc_diff = int_of_float (Gc.minor_words ()) - alloc_start in
          let gc_str =
            if gc_sampled
            then Gc_monitor.diff_to_string stats_start (Gc_monitor.get_stats ())
            else ""
          in
          (* Grid STRAT sub-phase attribution, filled by [execute_strategy] via
             strategy-state scratch fields; meaningful only when the grid ran. *)
          let phase_str =
            match cached_grid_state with
            | Some cs when should_execute ->
              Printf.sprintf
                " strat[pre=%dus sync=%dw/%dus(scan %dus rec %dus n %d) ledger=%d \
                 buy=%dw/%dus sell=%dw/%dus cln=%dw/%dus]"
                (cs.time_preamble_ns / 1000)
                cs.alloc_sync_words
                (cs.time_sync_ns / 1000)
                (cs.time_sync_scan_ns / 1000)
                (cs.time_sync_rec_ns / 1000)
                cs.sync_orders_seen
                (Hashtbl.length cs.sell_commitments)
                cs.alloc_buy_words
                (cs.time_buy_ns / 1000)
                cs.alloc_sell_words
                (cs.time_sell_ns / 1000)
                cs.alloc_cleanup_words
                (cs.time_cleanup_ns / 1000)
            | _ -> ""
          in
          Latency_profiler.set_cause
            prof_cycle
            (Printf.sprintf
               "ob:%B ex:%d lev:%d st:%B al:%dw[ob:%d ex:%d prep:%d strat:%d]%s%s"
               did_ob
               !cycle_events
               !lifecycle_events
               should_execute
               alloc_diff
               (alloc_at_t2 - alloc_start)
               (alloc_at_t3 - alloc_at_t2)
               (!alloc_at_t3s - alloc_at_t3)
               (alloc_at_t4 - !alloc_at_t3s)
               gc_str
               phase_str));
      (* Roll the latency window on a fixed time cadence, not a cycle count: the old
         cycle_mod gate (10000 cycles) accumulated minutes before a wipe. *)
      let now_flush = Unix.gettimeofday () in
      if now_flush -. !last_window_time >= latency_window_seconds
      then (
        last_window_time := now_flush;
        publish_windows ());
      (* Blocks until a producer signals new data or data is ready. Uses the cached
         has_exec_fn closure to avoid a Hashtbl lookup. [wait_since] returns immediately
         if a producer signalled while this cycle ran, so a racing signal is not lost to
         the park. *)
      (* Parks whenever this cycle produced no executable work. The execute flag stays
         set, so the next event-driven wake re-evaluates it; every condition that can
         unblock it (new book/exec frame, oracle publish, first data at session open) is a
         signal. No polling sleep. *)
      if not should_execute
      then Concurrency.Exchange_wakeup.wait_since_fast wakeup_sync ~since:wake_baseline;
      if !exec_ready && (not !latency_active) && !cycle_count - !exec_ready_cycle >= 10
      then (
        latency_active := true;
        Logging.debug_f
          ~section
          "[%s/%s] Startup warmup complete (10 cycles post-ready). Latency measurements \
           active."
          asset_with_fees.exchange
          asset_with_fees.symbol);
      ()
    done
;;

(** Create a new domain_state and register it in the global domain_registry. *)
let register_domain asset =
  let key = domain_key asset in
  let state =
    { asset
    ; domain_handle = Atomic.make None
    ; last_restart = Atomic.make (Unix.time ())
    ; restart_count = Atomic.make 0
    ; is_running = Atomic.make false
    ; mutex = Mutex.create ()
    }
  in
  Mutex.lock registry_mutex;
  Hashtbl.replace domain_registry key state;
  Mutex.unlock registry_mutex;
  state
;;

(** Condition variable signalled on domain exit (crash or normal). Allows supervisor_loop
    to react immediately rather than waiting the full 5s tick. *)
let domain_died_mutex = Mutex.create ()

let domain_died_cond = Condition.create ()

(** Signal domain_died_cond to wake the supervisor after a domain exits. *)
let notify_domain_died () =
  Mutex.lock domain_died_mutex;
  Condition.signal domain_died_cond;
  Mutex.unlock domain_died_mutex
;;

(** Spawn a new OCaml domain for the given state, guarded by its mutex. Joins any previous
    domain handle before spawning. Returns false if the domain is already running. *)
let start_domain config state fee_fetcher =
  let asset = state.asset in
  let key = domain_key asset in
  Mutex.lock state.mutex;
  if Atomic.get state.is_running
  then (
    Mutex.unlock state.mutex;
    Logging.warn_f ~section "Domain %s is already running" key;
    false)
  else (
    Atomic.set state.last_restart (Unix.time ());
    Atomic.set state.restart_count (Atomic.get state.restart_count + 1);
    Atomic.set state.is_running true;
    (* Join the previous domain handle synchronously before spawning *)
    (match Atomic.get state.domain_handle with
     | Some old_handle ->
       (try Domain.join old_handle with
        | exn ->
          Logging.warn_f
            ~section
            "Exception joining old domain %s: %s"
            key
            (Printexc.to_string exn))
     | None -> ());
    let domain_handle =
      Domain.spawn (fun () ->
        (* Catch all exceptions including from apply_gc_config: a
           CamlinternalLazy.Undefined from concurrent Lazy.force on the shared
           cached_gc_config would otherwise silently kill the domain. *)
        try
          Config.apply_gc_config ();
          Logging.debug_f
            ~section
            "Domain for %s/%s started (restart #%d)"
            asset.exchange
            asset.symbol
            (Atomic.get state.restart_count);
          asset_domain_worker config fee_fetcher asset;
          Logging.info_f
            ~section
            "Domain for %s/%s completed normally"
            asset.exchange
            asset.symbol
        with
        | exn ->
          Logging.critical_f
            ~section
            "Domain for %s/%s crashed (CAUGHT IN SPAWNER): %s"
            asset.exchange
            asset.symbol
            (Printexc.to_string exn);
          (* Mark stopped and notify the supervisor; domain_handle is preserved for join
             on the next start_domain call. *)
          Atomic.set state.is_running false;
          notify_domain_died ();
          ())
    in
    Atomic.set state.domain_handle (Some domain_handle);
    Mutex.unlock state.mutex;
    Logging.info_f ~section "Domain %s started successfully" key;
    true)
;;

(** Stop a running domain: set is_running to false, clean up strategy state, signal
    blocked workers via Exchange_wakeup, and join the domain handle. *)
let stop_domain state =
  let key = domain_key state.asset in
  Mutex.lock state.mutex;
  Atomic.set state.is_running false;
  (* Release strategy state for this symbol *)
  let symbol = state.asset.symbol in
  (* TODO(milestone-3): temporary bridge — dispatch on the compiled instance, not the
     user's strategy name. See docs/strategy-engine-design.md §9.5. *)
  (match state.asset.strategy with
   | "Ladder" | "jacobs_ladder" ->
     Dio_strategies.Jacobs_ladder.Strategy.cleanup_strategy_state symbol
   | "MM" -> Dio_strategies.Market_maker.Strategy.cleanup_strategy_state symbol
   | _ -> ());
  (* Unblock workers in Exchange_wakeup.wait_since so they observe is_running=false and
     exit the main loop. *)
  Concurrency.Exchange_wakeup.signal_all ();
  (match Atomic.get state.domain_handle with
   | Some handle ->
     Logging.info_f ~section "Stopping domain %s..." key;
     (* Join synchronously; domain exits promptly after is_running is cleared *)
     (try Domain.join handle with
      | exn ->
        Logging.warn_f
          ~section
          "Exception joining domain %s: %s"
          key
          (Printexc.to_string exn));
     Atomic.set state.domain_handle None
   | None -> ());
  Mutex.unlock state.mutex
;;

(** Returns true if the domain is stopped and no shutdown has been requested. *)
let domain_needs_restart state =
  Mutex.lock state.mutex;
  (* Suppress restart when shutdown is in progress *)
  let needs_restart =
    (not (Atomic.get state.is_running)) && not (Atomic.get shutdown_requested)
  in
  Mutex.unlock state.mutex;
  needs_restart
;;

(** Persistent waker thread: signals domain_died_cond every 5s so the supervisor loop
    wakes on a regular cadence even when no domain crashes. Allocated once at module load. *)
let _supervisor_waker_thread : Thread.t =
  Thread.create
    (fun () ->
      while not (Atomic.get shutdown_requested) do
        Thread.delay 5.0;
        Mutex.lock domain_died_mutex;
        Condition.signal domain_died_cond;
        Mutex.unlock domain_died_mutex
      done)
    ()
;;

(** Supervisor monitoring loop. Blocks on domain_died_cond, then iterates the registry and
    restarts any stopped domains with exponential backoff. *)
let supervisor_loop config fee_fetcher =
  let section = "domain_supervisor" in
  Logging.info ~section "Domain supervisor started";
  while not (Atomic.get shutdown_requested) do
    try
      (* Block until domain_died_cond is signalled by a crashed domain or the periodic 5s
         tick from _supervisor_waker_thread. *)
      Mutex.lock domain_died_mutex;
      Condition.wait domain_died_cond domain_died_mutex;
      Mutex.unlock domain_died_mutex;
      (* Re-check shutdown flag after waking *)
      if Atomic.get shutdown_requested then raise Exit;
      Mutex.lock registry_mutex;
      let domains = Hashtbl.to_seq_values domain_registry |> List.of_seq in
      Mutex.unlock registry_mutex;
      List.iter
        (fun state ->
          (* Early exit if shutdown was requested during iteration *)
          if Atomic.get shutdown_requested then raise Exit;
          if domain_needs_restart state
          then (
            let key = domain_key state.asset in
            let last_restart = Atomic.get state.last_restart in
            let restart_count = Atomic.get state.restart_count in
            let time_since_restart = Unix.time () -. last_restart in
            (* Exponential backoff: 1s, 2s, 4s, 8s, ... capped at 30s *)
            let backoff_delay = min 30.0 (2.0 ** float_of_int (restart_count - 1)) in
            if time_since_restart >= backoff_delay
            then (
              Logging.warn_f
                ~section
                "Restarting crashed domain %s (attempt #%d, backoff %.1fs)"
                key
                restart_count
                backoff_delay;
              ignore (start_domain config state fee_fetcher))))
        domains
    with
    | exn ->
      (match exn with
       | Exit -> () (* Clean exit on shutdown *)
       | _ ->
         Logging.error_f
           ~section
           "Exception in domain supervisor: %s"
           (Printexc.to_string exn))
  done
;;

(** Initialize strategies, register all assets, start their domains, and launch the
    supervisor thread. Returns the supervisor Thread.t handle. *)
let spawn_supervised_domains_for_assets
  (config : config)
  (fee_fetcher : trading_config -> trading_config)
  (assets : trading_config list)
  : Thread.t
  =
  (* Initialize strategy module state *)
  Dio_strategies.Jacobs_ladder.Strategy.init ();
  Dio_strategies.Market_maker.Strategy.init ();
  (* Register each asset in the domain registry *)
  List.iter (fun asset -> ignore (register_domain asset)) assets;
  (* Pre-force the shared cached_gc_config Lazy before spawning domains: OCaml 5 domains
     concurrently forcing the same value race, and if the computing domain fails the
     others get CamlinternalLazy.Undefined. Forcing in the main domain eliminates the
     race. *)
  Config.apply_gc_config ();
  (* Spawn the initial domain for each registered asset *)
  Mutex.lock registry_mutex;
  let all_states = Hashtbl.to_seq_values domain_registry |> List.of_seq in
  Mutex.unlock registry_mutex;
  List.iter (fun state -> ignore (start_domain config state fee_fetcher)) all_states;
  (* Launch the supervisor monitoring thread *)
  let supervisor_thread = Thread.create (supervisor_loop config) fee_fetcher in
  Logging.info ~section "Domain supervisor thread started";
  supervisor_thread
;;

(** Return a snapshot of all domain states for external monitoring. *)
let get_domain_status () =
  Mutex.lock registry_mutex;
  let status =
    Hashtbl.fold
      (fun key state acc ->
        let running = Atomic.get state.is_running in
        let restart_count = Atomic.get state.restart_count in
        let last_restart = Atomic.get state.last_restart in
        (key, (running, restart_count, last_restart)) :: acc)
      domain_registry
      []
  in
  Mutex.unlock registry_mutex;
  status
;;

(** Clear the domain registry. Intended for test teardown only. *)
let clear_domain_registry () =
  Mutex.lock registry_mutex;
  Hashtbl.clear domain_registry;
  Mutex.unlock registry_mutex
;;

(** Returns latency profiler snapshots for all domains from their most recently completed
    windows. Result type: (symbol, [(label, snapshot option)]) list.

    Reads immutable snapshots published via [Latency_profiler.published_snapshot]
    (lock-free [Atomic.get]); no percentile scan touches a histogram being mutated by a
    domain thread. Safe to call from the dashboard. *)
let get_domain_profiler_snapshots () =
  Mutex.lock profiler_cache_mutex;
  let profiler_refs =
    Hashtbl.fold (fun symbol profs acc -> (symbol, profs) :: acc) domain_profiler_cache []
  in
  Mutex.unlock profiler_cache_mutex;
  List.map
    (fun (symbol, profs) ->
      let snaps =
        [ "orderbook", Latency_profiler.published_snapshot profs.prof_ob
        ; "execution", Latency_profiler.published_snapshot profs.prof_exec
        ; "prep", Latency_profiler.published_snapshot profs.prof_prep
        ; "strategy", Latency_profiler.published_snapshot profs.prof_strategy
        ; "cycle", Latency_profiler.published_snapshot profs.prof_cycle
        ]
      in
      symbol, snaps)
    profiler_refs
;;

(** Initiate graceful shutdown: signal supervisor, stop each domain, and wait up to 10s
    for all domains to terminate. *)
let stop_all_domains () =
  Logging.info ~section "Stopping all supervised domains...";
  (* Set shutdown flag to prevent supervisor from restarting domains *)
  Atomic.set shutdown_requested true;
  Mutex.lock registry_mutex;
  let all_states = Hashtbl.to_seq_values domain_registry |> List.of_seq in
  Mutex.unlock registry_mutex;
  List.iter stop_domain all_states;
  (* Poll until all domains have stopped or timeout expires *)
  let rec wait_for_stop max_wait =
    if max_wait <= 0.0
    then Logging.warn ~section "Timeout waiting for domains to stop"
    else (
      let all_stopped =
        List.for_all (fun state -> not (Atomic.get state.is_running)) all_states
      in
      if all_stopped
      then Logging.info ~section "All domains stopped successfully"
      else (
        Thread.delay 0.1;
        wait_for_stop (max_wait -. 0.1)))
  in
  wait_for_stop 10.0;
  (* Final persistence flush. [flush_persistence] only runs inside the domain cycle, so
     state dirtied in the last cycles before shutdown (accumulation P&L, last_fill_oid,
     sell levels) would never reach the save queue; at_exit [flush_all] drains only what
     is queued. Domains are stopped, so this cannot race a cycle. *)
  List.iter
    (fun state ->
      let strategy = state.asset.strategy in
      if strategy = "jacobs_ladder" || strategy = "Ladder"
      then Dio_strategies.Jacobs_ladder.Strategy.flush_persistence state.asset.symbol)
    all_states
;;
