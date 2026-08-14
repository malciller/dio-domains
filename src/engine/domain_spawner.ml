open Config
module Fear_and_greed = Cmc.Fear_and_greed

(* Capital-oracle runtime (wrapped library: explicit alias avoids opening the
   whole Dio_oracle namespace). *)
module Oracle_runtime = Dio_oracle.Oracle_runtime
module Oracle_types = Dio_oracle.Oracle_types

(* Exchange interface and types *)
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

let section = "domain_spawner"

(** A quote-balance snapshot older than this is not authoritative: buy
    placement against an under-funded stale snapshot is still attempted (the
    exchange's verdict is the truth); a FRESH snapshot that cannot fund the
    buy is skipped outright. Exchanges without freshness tracking report
    unknown age, which is treated as stale (previous behavior). *)
let stale_balance_age_seconds = 60.0

(** Pure startup-gate decision for grid domains: the gate opens when the
    capital oracle published a decision for this asset, or when a live Fear &
    Greed reading exists AND the startup window has been given to both
    signals (the oracle's first pass attempt finished, or the startup
    deadline elapsed). Startup always attempts BOTH signals - event-driven
    wakeups carry the oracle pass completion and the per-cycle check picks up
    an F&G fetch - and only after one connects and the other times out does
    the gate proceed on the single live one. It never opens on fabricated
    config defaults: with neither signal the grid cannot profitably and
    accurately create orders, so it does not. *)
let grid_gate_should_open
      ~(oracle_decision : bool)
      ~(fng_available : bool)
      ~(gate_waiver : bool)
  =
  oracle_decision || (fng_available && gate_waiver)
;;

(** Crypto vs equity for F&G blending, mirroring the capital oracle's own
    rule (Oracle_tasks.calendar_kind_of_exchange): crypto assets blend the
    Fear & Greed signal into their sizing; equities are sized by the capital
    oracle alone (pure oracle) and never take F&G values - neither for the
    grid interval/qty nor as a startup signal. *)
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

(** Per-symbol latency profiler cache. Persists across domain restarts so
    profiler objects (each ~800KB) are allocated once per symbol rather than
    on every asset_domain_worker invocation. *)
type domain_profilers =
  { prof_ob : Latency_profiler.t
  ; prof_exec : Latency_profiler.t
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
        ; prof_exec = Latency_profiler.create (symbol ^ ":exec")
        ; prof_strategy = Latency_profiler.create (symbol ^ ":strategy")
        ; prof_cycle =
            Latency_profiler.create
              ~bucket_us:10
              ~max_latency_us:1_000_000
              (symbol ^ ":cycle")
        }
      in
      Hashtbl.replace domain_profiler_cache symbol p;
      p
  in
  Mutex.unlock profiler_cache_mutex;
  profs
;;

(** Core worker function executed by each OCaml domain for a trading asset.
    Runs the event-driven loop: consumes ring buffer events, executes strategy,
    and blocks on Exchange_wakeup between cycles. *)
let asset_domain_worker
      (config : config)
      (fee_fetcher : trading_config -> trading_config)
      (asset : trading_config)
  =
  Random.self_init ();
  (* Fetch exchange fee schedule at domain startup *)
  let asset_with_fees = fee_fetcher asset in
  (* Resolve grid_interval from the cached Fear & Greed index when the
     capital oracle has no decision yet (fallback sizing). There is no
     midpoint default: the config range [lo, hi] is a constraint, not a
     fallback. F&G only applies when a REAL index value was fetched (the F&G
     cache holds genuinely fetched values only); when neither the capital
     oracle nor a live F&G reading can size the asset, the grid withholds
     orders entirely - the startup gate below never opens. *)
  let fng_grid_interval () =
    match Fear_and_greed.get_cached () with
    | None -> None
    | Some fng ->
      let resolved =
        Fear_and_greed.grid_value_for_fng
          ~grid_interval:asset_with_fees.grid_interval
          ~fear_and_greed:fng
      in
      let lo, hi = asset_with_fees.grid_interval in
      Logging.debug_f
        ~section
        "Resolved F&G fallback grid_interval for %s/%s: %.4f (F&G=%.2f, range %.4f-%.4f)"
        asset_with_fees.exchange
        asset_with_fees.symbol
        resolved
        fng
        lo
        hi;
      Some resolved
  in
  let resolved_grid_interval = fng_grid_interval () in
  (* Resolve accumulation_buffer via Fear & Greed (Hyperliquid, Lighter, IBKR
     and Alpaca only). Same no-default rule: only a live F&G reading resolves
     it; without one the grid does not place orders anyway. *)
  let fng_accumulation_buffer () =
    let exch_id =
      Dio_exchange.Exchange_intf.Types.exchange_of_string asset_with_fees.exchange
    in
    let is_accumulation_exch =
      match exch_id with
      | Hyperliquid | Ibkr | Lighter | Alpaca -> true
      | _ -> false
    in
    if
      is_accumulation_exch
      && (asset_with_fees.strategy = "suicide_grid" || asset_with_fees.strategy = "Grid")
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
    (* Startup gate: blocks strategy execution until execution events from
         the initial snapshot have been consumed via the ring buffer, ensuring
         handle_order_acknowledged restores order state (last_buy_order_id, etc.)
         before any new orders are placed. Applies to ALL exchanges. *)
    let exec_ready = ref false in
    (* Set after the first exec position check. Acts as a fallback to open the
         exec_ready gate for assets with no open orders (empty snapshot). *)
    let exec_checked = ref false in
    let latency_active = ref false in
    let exec_ready_cycle = ref 0 in
    let open_orders_dirty = ref true in
    (* H5: the per-cycle oracle decision lookup is cached against the publish
       generation, so idle cycles (no new pass) do zero decision work. *)
    let oracle_gen_cached = ref (-1) in
    let oracle_decision_cached = ref None in
    (* Initialize strategy configuration refs based on strategy type.
       The capital-oracle runtime publishes a per-asset decision (qty, the
       blended grid_interval, active) to a lock-free snapshot; while a
       decision exists the oracle's blended qty/gi win - F&G enters that blend
       inside the oracle (parameter_components), it is never re-derived as a
       competing value here. *)
    let baseline_price = ref None in
    (* None until a real F&G value is seen: a missing index means "no live F&G
       signal" and the per-cycle re-evaluation is skipped, not neutralized. *)
    let last_known_fng = ref None in
    let oracle_decision_at_startup =
      Oracle_runtime.decision_for
        ~exchange:asset_with_fees.exchange
        ~symbol:asset_with_fees.symbol
    in
    (* Tracks the last applied oracle halt state so the per-cycle block only
         logs on active<->inactive transitions. Initialized from the startup
         decision (an asset born inactive starts quiet). *)
    let oracle_halted_prev =
      ref
        (match oracle_decision_at_startup with
         | Some d -> not d.active
         | None -> false)
    in
    (* Reclaim cancel state (priority reclamation): a decision with
       [reclaim_capital] asks this domain to cancel its resting buy(s) so the
       committed capital returns to the account pool for a higher-priority
       asset. The cancel is a network op that can fail silently (dispatch
       dropped on a connection flap, exchange rejection, ring-buffer full), so
       it is NOT issued every cycle - [reclaim_cancel_issued] latches it with
       the timestamp [reclaim_cancel_at] - but it MUST be retried while the
       reclaim decision persists and eligible buys still sit in the store.
       The latch re-arms the moment the store no longer shows an eligible buy
       (the cancel landed) OR the decision stops being a reclaim; see
       [Dio_strategies.Suicide_grid.reclaim_step]. Without the retry, a single
       failed cancel leaves the account permanently stuck: the reclaimed
       asset stays paused (the oracle's plan only clears once the store's
       committed value drops to zero) and the priority asset never resumes on
       capital that was never actually released. *)
    let reclaim_cancel_issued = ref false in
    let reclaim_cancel_at = ref 0.0 in
    let reclaim_retry_seconds = 15.0 in
    (* One-shot warnings: the "only one sizing source active" cases (oracle
       decision without F&G, F&G without an oracle decision) and the "no
       signal at all" withhold case each fire once per domain. *)
    let fng_unavailable_warned = ref false in
    let no_signal_warned = ref false in
    (* One-shot info: F&G is live but the startup window still gives the
       oracle's first pass its chance - the gate waits instead of jumping on
       F&G alone. *)
    let fng_ready_wait_warned = ref false in
    (* The grid strategy asset is materialized only when a sizing signal
       exists - the capital oracle's decision (qty/gi win) or a live F&G
       reading. With neither, the ref stays None and the startup gate below
       stays closed: the grid cannot profitably and accurately create orders
       without real sizing information, so it does not. *)
    let grid_asset_of
          ?(qty = asset_with_fees.qty)
          ?(accumulation_buffer = resolved_accumulation_buffer)
          ~(grid_interval : float)
          ()
      =
      { Dio_strategies.Suicide_grid.exchange = asset_with_fees.exchange
      ; symbol = asset_with_fees.symbol
      ; qty
      ; grid_interval
      ; sell_mult = asset_with_fees.sell_mult
      ; strategy = asset_with_fees.strategy
      ; maker_fee = asset_with_fees.maker_fee
      ; taker_fee = asset_with_fees.taker_fee
      ; accumulation_buffer = Option.value accumulation_buffer ~default:0.0
      }
    in
    let grid_strategy_asset_ref =
      if asset_with_fees.strategy = "suicide_grid" || asset_with_fees.strategy = "Grid"
      then (
        match oracle_decision_at_startup, resolved_grid_interval with
        | Some d, _ when d.active ->
          ref
            (Some
               (grid_asset_of
                  ~qty:(Printf.sprintf "%.8g" d.qty)
                  ~grid_interval:d.grid_interval
                  ()))
        | _, Some gi -> ref (Some (grid_asset_of ~grid_interval:gi ()))
        | _ -> ref None)
      else ref None
    in
    (* Oracle/signal startup gate: grid strategies withhold strategy
       execution until the startup window has given BOTH sizing sources their
       chance - the capital-oracle's first pass attempt (event-driven: its
       on_publish hook wakes the domains via Exchange_wakeup.signal_all) and
       the Fear & Greed fetch (startup fetch in main.ml, retried on every
       oracle pass, plus async refresh on price moves). The gate opens on the
       oracle's first decision for this asset at any time; otherwise, once
       the first pass attempt has finished or the startup deadline elapsed,
       a live F&G reading alone is enough to proceed (one real signal
       suffices after both had their chance). It never opens on fabricated
       config defaults: with neither signal the grid cannot profitably and
       accurately create orders, so it does not (orders are withheld, and a
       one-shot warning fires once the grace period elapses). When only one
       source is active and the other failed, a one-shot warning names the
       failed one. While gated the domain clears its execute flag and blocks
       on the normal per-symbol [Exchange_wakeup.wait].
       [oracle_gate_deadline] is the startup window: how long both signals
       get before the gate proceeds on whichever single one is live; it is
       checked on wakeups, never polled. *)
    let is_grid_strategy =
      asset_with_fees.strategy = "suicide_grid" || asset_with_fees.strategy = "Grid"
    in
    let oracle_tracks_asset =
      Oracle_runtime.tracks_asset
        ~exchange:asset_with_fees.exchange
        ~symbol:asset_with_fees.symbol
    in
    let oracle_startup_wait =
      match config.oracle with
      | Some o -> o.startup_wait_seconds
      | None -> (Oracle_runtime.default_config ()).startup_wait_seconds
    in
    let oracle_gate_open = ref (not is_grid_strategy) in
    let oracle_gate_deadline = ref (Unix.gettimeofday () +. oracle_startup_wait) in
    let mm_strategy_asset_ref =
      if asset_with_fees.strategy = "MM" then ref (Some asset_with_fees) else ref None
    in
    (* Pre-populate strategy state fields (exchange_id, grid_qty, maker_fee)
         so that fill handlers invoked during exec event consumption have
         correct values before the first execute_strategy call. Without this,
         profit calculations and persistence writes use zero defaults. *)
    (match !grid_strategy_asset_ref with
     | Some asset ->
       let st = Dio_strategies.Suicide_grid.get_strategy_state asset.symbol in
       st.exchange_id <- asset.exchange;
       st.grid_qty
       <- (try float_of_string asset.qty with
           | Failure _ -> 0.001);
       st.cached_sell_mult
       <- (try float_of_string asset.Dio_strategies.Suicide_grid.sell_mult with
           | Failure _ -> 1.0);
       st.cached_ecfg <- Dio_strategies.Suicide_grid.get_exchange_config asset.exchange;
       st.maker_fee
       <- (match asset.maker_fee with
           | Some f -> f
           | None ->
             (match
                Dio_strategies.Fee_cache.get_maker_fee
                  ~exchange:asset.exchange
                  ~symbol:asset.symbol
              with
              | Some cached -> cached
              | None -> 0.0));
       ()
     | None -> ());
    (* Initialize exec read position: ALL exchanges start from position 0
         to replay snapshot events through handle_order_acknowledged, restoring
         last_buy_order_id and open sell tracking. This unifies the startup
         path across Kraken, Hyperliquid, and IBKR; previously only
         Hyperliquid replayed from 0, causing a race condition where Kraken
         domains could execute their first strategy cycle before the snapshot
         populated the open_orders Hashtbl. *)
    Logging.debug_f
      ~section
      "About to get execution feed position for %s"
      asset_with_fees.symbol;
    exec_read_pos := 0;
    (* Wait for execution snapshot to be ingested before entering the loop.
         Without this, the first cycle may see zero open orders and place
         duplicates. Timeout after 15s to avoid blocking indefinitely. *)
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
    (* Set orderbook positions to current write position, skipping
         stale ring buffer data. Starting at 0 would replay up to 128 historical
         entries per symbol on every restart, causing excessive allocations. *)
    orderbook_read_pos := Ex.get_orderbook_position ~symbol:asset_with_fees.symbol;
    (* Seed current_price and top_of_book from the exchange live cache so
         the first cycle can execute immediately rather than waiting for the
         next incoming update. *)
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
    (* Cached closures for highly efficient, allocation-free balance reporting *)
    let base_balance_fn = Ex.get_tradeable_balance_fast ~asset:base_asset in
    let quote_balance_fn = Ex.get_tradeable_balance_fast ~asset:quote_currency in
    (* Cached closures for latency-sensitive feed access in the hot loop *)
    let get_ob_pos_fn = Ex.get_orderbook_position_fast ~symbol:asset_with_fees.symbol in
    let get_tob_fn = Ex.get_top_of_book_fast ~symbol:asset_with_fees.symbol in
    let get_exec_pos_fn =
      Ex.get_execution_feed_position_fast ~symbol:asset_with_fees.symbol
    in
    let has_exec_fn = Ex.has_execution_data_fast ~symbol:asset_with_fees.symbol in
    let cycle_count = ref 0 in
    let { prof_ob; prof_exec; prof_strategy; prof_cycle } =
      get_domain_profilers asset_with_fees.symbol
    in
    (* Rolling latency window: publish + reset each profiler every
       [latency_window_seconds] so the dashboard reads fresh, moving
       percentiles instead of multi-minute accumulations with abrupt wipes
       (F1/F4). Publishing swaps an immutable snapshot into an Atomic cell,
       so the dashboard never scans a histogram being mutated by this domain. *)
    let latency_window_seconds = config.latency_window_seconds in
    let last_window_time = ref (Unix.gettimeofday ()) in
    (* M8: GC stats are sampled once per latency window (inside
       [publish_windows]) instead of twice per busy cycle. [Gc.quick_stat]
       costs ~2µs; on the publish cadence it is negligible, per-cycle it
       would tax every busy domain cycle once profiling warms up. *)
    let gc_start =
      ref { Gc_monitor.minor_collections = 0; major_collections = 0; compactions = 0 }
    in
    let gc_end = ref !gc_start in
    let publish_windows () =
      ignore (Latency_profiler.snapshot_and_reset prof_ob);
      ignore (Latency_profiler.snapshot_and_reset prof_exec);
      (* The strategy window's execution count is set to the number of order
         actions this domain's strategy actually pushed in the window (place/
         amend/cancel), so the dashboard's STRAT/S column reports real
         executions per second instead of raw strategy-invocation cycles
         (which for a fast feed are far higher than actual order activity). *)
      Latency_profiler.set_executions
        prof_strategy
        (Dio_strategies.Strategy_common.Order_actions.snapshot_and_reset
           asset_with_fees.symbol);
      ignore (Latency_profiler.snapshot_and_reset prof_strategy);
      ignore (Latency_profiler.snapshot_and_reset prof_cycle);
      (* Refresh the window-scoped GC sampling pair once per window. *)
      gc_start := Gc_monitor.get_stats ();
      gc_end := !gc_start
    in
    (* Publish an initial empty window so the dashboard renders this domain as
       idle immediately rather than after the first window elapses, and clears
       any stale snapshot left by a previous domain incarnation. *)
    publish_windows ();
    last_window_time := Unix.gettimeofday ();
    (* Cache the equity market-hours evaluation (~1s TTL). The underlying check
       does gmtime+mktime+DST math per call (alpaca_market_hours.ml:11-105);
       evaluating it on every hot ibkr/alpaca cycle inflated the cycle latency
       profile (F7). *)
    let mh_cache = ref (None : (float * bool) option) in
    (* Cache strategy state references to avoid repeated mutex acquisition
         on the hot path. References are stable while is_running is true. *)
    let cached_grid_state =
      match !grid_strategy_asset_ref with
      | Some _ ->
        let st = Dio_strategies.Suicide_grid.get_strategy_state asset_with_fees.symbol in
        st.cached_round_price
        <- (fun p -> Ex.round_price ~symbol:asset_with_fees.symbol ~price:p);
        st.cached_price_increment
        <- Option.value
             (Ex.get_price_increment ~symbol:asset_with_fees.symbol)
             ~default:0.01;
        st.cached_qty_increment
        <- Option.value
             (Ex.get_qty_increment ~symbol:asset_with_fees.symbol)
             ~default:0.01;
        st.cached_qty_min
        <- Option.value (Ex.get_qty_min ~symbol:asset_with_fees.symbol) ~default:0.01;
        Some st
      | None -> None
    in
    let cached_mm_state =
      match !mm_strategy_asset_ref with
      | Some _ ->
        Some (Dio_strategies.Market_maker.get_strategy_state asset_with_fees.symbol)
      | None -> None
    in
    let cached_fng_check_threshold = config.fng_check_threshold in
    while Atomic.get state.is_running do
      let latency_this_cycle = !latency_active in
      if !cycle_count = 0 then Logging.debug_f ~section "First cycle for %s" key;
      incr cycle_count;
      let cycle_events = ref 0 in
      let t1 = if latency_this_cycle then Mtime_clock.now_ns () else 0L in
      let alloc_start = if latency_this_cycle then Gc.minor_words () else 0.0 in
      (* M8: GC stats are window-scoped (sampled in [publish_windows]); the
         per-cycle cause string reads the last window pair. No [Gc.quick_stat]
         on the per-tick path. *)
      (* H3: drain lifecycle events queued by the supervisor REST path. All
         handler invocations (REST- and WS-sourced) now execute on THIS domain
         thread at the top of the cycle, so the strategy mutex is never
         contended across threads. Runs unconditionally; the queue is empty
         on the common cycle and the read is a lock-free CAS. *)
      (match !grid_strategy_asset_ref with
       | Some _ ->
         Dio_strategies.Suicide_grid.Strategy.drain_events asset_with_fees.symbol
       | None -> ());
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
      let t2 = if latency_this_cycle then Mtime_clock.now_ns () else 0L in
      if did_ob && latency_this_cycle
      then Latency_profiler.record prof_ob (Mtime.Span.of_uint64_ns (Int64.sub t2 t1));
      let current_pos = get_exec_pos_fn () in
      let did_exec = current_pos <> !exec_read_pos in
      if did_exec
      then (
        open_orders_dirty := true;
        let event_count = ref 0 in
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
                 (* A canceled/rejected/expired order changes the live pool and
                    the strategy's open-order state: wake the capital oracle so
                    it re-sizes (lock-free, ~50ms latency). *)
                 Oracle_runtime.request_pass ();
                 let side =
                   match event.side with
                   | Types.Buy -> Dio_strategies.Strategy_common.Buy
                   | Types.Sell -> Dio_strategies.Strategy_common.Sell
                 in
                 (match !grid_strategy_asset_ref with
                  | Some _ ->
                    Dio_strategies.Suicide_grid.Strategy.handle_order_cancelled
                      ~now:now_exec
                      asset_with_fees.symbol
                      event.order_id
                      side
                      event.cl_ord_id;
                    ()
                  | None -> ());
                 (match !mm_strategy_asset_ref with
                  | Some _ ->
                    Dio_strategies.Market_maker.Strategy.handle_order_cancelled
                      ~now:now_exec
                      asset_with_fees.symbol
                      event.order_id
                      side
                      event.cl_ord_id;
                    ()
                  | None -> ())
               | Types.Filled ->
                 should_execute_strategy := true;
                 (* A fill returns/consumes quote: wake the capital oracle so
                    it re-sizes the asset (and the rest of the account's
                    priority order) as soon as the pool changes (lock-free,
                    ~50ms latency). *)
                 Oracle_runtime.request_pass ();
                 let side =
                   match event.side with
                   | Types.Buy -> Dio_strategies.Strategy_common.Buy
                   | Types.Sell -> Dio_strategies.Strategy_common.Sell
                 in
                 (match !grid_strategy_asset_ref with
                  | Some _ ->
                    Dio_strategies.Suicide_grid.Strategy.handle_order_filled
                      ~now:now_exec
                      asset_with_fees.symbol
                      event.order_id
                      side
                      ~fill_price:event.avg_price
                      ~fill_qty:event.filled_qty
                      event.cl_ord_id;
                    ()
                  | None -> ());
                 (match !mm_strategy_asset_ref with
                  | Some _ ->
                    Dio_strategies.Market_maker.Strategy.handle_order_filled
                      ~now:now_exec
                      asset_with_fees.symbol
                      event.order_id
                      side
                      ~fill_price:event.avg_price
                      ~fill_qty:event.filled_qty
                      event.cl_ord_id;
                    ()
                  | None -> ());
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
                 (* Guard: skip handle_order_acknowledged for in-place amendment
                     confirmations (Kraken exec_type=amended with status=new).
                     The amendment lifecycle is handled by the supervisor's
                     handle_order_amended callback on the REST response path.
                     Routing these through handle_order_acknowledged causes a
                     dual-update race that corrupts open_sell_orders tracking. *)
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
                     (match !grid_strategy_asset_ref with
                      | Some _ ->
                        Dio_strategies.Suicide_grid.Strategy.handle_order_amended
                          ~now:now_exec
                          asset_with_fees.symbol
                          event.order_id
                          event.order_id
                          side
                          price;
                        ()
                      | None -> ());
                     (match !mm_strategy_asset_ref with
                      | Some _ ->
                        Dio_strategies.Market_maker.Strategy.handle_order_amended
                          ~now:now_exec
                          asset_with_fees.symbol
                          event.order_id
                          event.order_id
                          side
                          price;
                        ()
                      | None -> ())
                   | _ -> ())
                 else (
                   match event.limit_price with
                   | Some price when price > 0.0 ->
                     let side =
                       match event.side with
                       | Types.Buy -> Dio_strategies.Strategy_common.Buy
                       | Types.Sell -> Dio_strategies.Strategy_common.Sell
                     in
                     (match !grid_strategy_asset_ref with
                      | Some _ ->
                        Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
                          ~now:now_exec
                          asset_with_fees.symbol
                          event.order_id
                          side
                          price;
                        ()
                      | None -> ());
                     (match !mm_strategy_asset_ref with
                      | Some _ ->
                        Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
                          ~now:now_exec
                          asset_with_fees.symbol
                          event.order_id
                          side
                          price;
                        ()
                      | None -> ())
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
            (match !grid_strategy_asset_ref with
             | Some _ ->
               Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done
                 asset_with_fees.symbol
             | None -> ());
            (match !mm_strategy_asset_ref with
             | Some _ ->
               Dio_strategies.Market_maker.Strategy.set_startup_replay_done
                 asset_with_fees.symbol
             | None -> ());
            Logging.debug_f
              ~section
              "[%s/%s] First exec event batch received, strategy now active"
              asset_with_fees.exchange
              asset_with_fees.symbol);
        exec_read_pos := new_pos;
        exec_checked := true);
      let t3 = if latency_this_cycle then Mtime_clock.now_ns () else 0L in
      if did_exec && latency_this_cycle
      then Latency_profiler.record prof_exec (Mtime.Span.of_uint64_ns (Int64.sub t3 t2));
      (* Fallback gate for domains with no open orders: if no exec events
           arrived and the execution data is ready (snapshot ingested), open
           the gate so the strategy can place its initial order. *)
      if (not !exec_ready) && (not !exec_checked) && has_exec_fn ()
      then (
        let current_pos_now = get_exec_pos_fn () in
        if current_pos_now = !exec_read_pos
        then (
          exec_checked := true;
          (* No exec events arrived and feed is ready: fetch snapshot orders 
               and inject them into the strategies to restore tracking state. *)
          (* Hoist timestamp outside the per-order callback: eliminates
               one gettimeofday syscall per open order during injection. *)
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
                 match !mm_strategy_asset_ref with
                 | Some _ ->
                   Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
                     ~now:now_inject
                     asset_with_fees.symbol
                     oid
                     order_side
                     price;
                   ()
                 | None -> ())
               else (
                 match !grid_strategy_asset_ref with
                 | Some _ ->
                   Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
                     ~now:now_inject
                     asset_with_fees.symbol
                     oid
                     order_side
                     price;
                   ()
                 | None -> ()));
          exec_ready := true;
          exec_ready_cycle := !cycle_count;
          (* Mark startup replay complete to ungate profit calculation *)
          (match !grid_strategy_asset_ref with
           | Some _ ->
             Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done
               asset_with_fees.symbol
           | None -> ());
          (match !mm_strategy_asset_ref with
           | Some _ ->
             Dio_strategies.Market_maker.Strategy.set_startup_replay_done
               asset_with_fees.symbol
           | None -> ());
          Logging.debug_f
            ~section
            "[%s/%s] Snapshot done, injected open orders - strategy now active"
            asset_with_fees.exchange
            asset_with_fees.symbol));
      (* Execute strategy if new events have been consumed and feed is ready (event-driven gate) *)
      (* IBKR market hours gate: suppress strategy execution entirely when
           the US equity market is closed. Without this, the strategy emits
           amendments against stale delayed data, the gateway rejects with
           error 354 (no market data), but our in-memory state already recorded
           the amend as successful, causing an infinite amend spam loop. *)
      let equity_market_closed =
        match asset_with_fees.exchange with
        | "ibkr" | "alpaca" ->
          let now_mh = Unix.gettimeofday () in
          (match !mh_cache with
           | Some (t, closed) when now_mh -. t < 1.0 -> closed
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
      (* Capital-oracle decision application. Read every cycle (a lock-free
            Atomic.get of an immutable snapshot), so a halted asset can be
            re-activated and a changed qty/gi adopted as soon as the runtime
            publishes - not only when market events trigger a cycle. Runs
            OUTSIDE the should_execute gate on purpose: an inactive asset
            never enters the execution block, so its re-activation must not
            depend on it. The oracle's qty/gi win over the F&G re-evaluation
            above (the oracle owns the sizing while it has a decision).
            H5: the lookup is cached per generation; [decision_for] is only
            re-invoked (with its lowercase+hashtable cost) when the runtime
            published a new pass, so idle cycles do zero decision work. *)
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
        | Some d -> not d.active
        | None -> false
      in
      (match oracle_decision, !grid_strategy_asset_ref with
       | Some d, None when d.active ->
         (* First oracle decision after a no-signal startup (no F&G was
            available): materialize the grid strategy from the decision. *)
         let qty_str = Printf.sprintf "%.8g" d.qty in
         grid_strategy_asset_ref
         := Some (grid_asset_of ~qty:qty_str ~grid_interval:d.grid_interval ());
         let st = Dio_strategies.Suicide_grid.get_strategy_state asset_with_fees.symbol in
         (try st.grid_qty <- float_of_string qty_str with
          | Failure _ -> ());
         (* Re-anchor any already-resting buy to the decision's spacing: an
            order injected from the exchange (or placed under an earlier
            sizing) can sit closer to the market than the oracle's gi allows,
            and the buy-trailing leg never moves a buy DOWN on its own. *)
         st.force_buy_reanchor <- true;
         should_execute_strategy := true;
         Logging.info_f
           ~section
           "[%s/%s] Capital oracle decision materialized: qty %.8g gi %.4f%% (D_surv \
            %.1f%%)"
           asset_with_fees.exchange
           asset_with_fees.symbol
           d.qty
           d.grid_interval
           (d.d_surv *. 100.0)
       | Some d, Some asset when d.active ->
         (* The oracle re-derives the qty from the live pool every pass,
             and the pool drifts with every balance/price update, so
             successive passes publish micro-different qtys (e.g. QQQ
             0.03877239 -> 0.03877509 -> 0.03876709). An exact string
             comparison trips [qty_changed] on EVERY pass, which forces a
             buy re-anchor -> an Alpaca amend (cancel+create) on every
             pass -> the infinite amend loop (the grid and oracle fight
             over the resting order's size). Judge the change numerically
             with a relative deadband (0.1%) so only a material re-size
             re-anchors; micro pool drift leaves the book untouched. *)
         let qty_changed =
           let current_qty =
             try float_of_string asset.qty with
             | Failure _ -> 0.0
           in
           abs_float (d.qty -. current_qty) > max (current_qty *. 0.001) 1e-9
         in
         let gi_changed = abs_float (d.grid_interval -. asset.grid_interval) > 1e-12 in
         if qty_changed || gi_changed
         then (
           let qty_str = Printf.sprintf "%.8g" d.qty in
           let new_asset =
             { asset with qty = qty_str; grid_interval = d.grid_interval }
           in
           grid_strategy_asset_ref := Some new_asset;
           let st = Dio_strategies.Suicide_grid.get_strategy_state asset.symbol in
           (try st.grid_qty <- float_of_string qty_str with
            | Failure _ -> ());
           (* The sizing changed: force the resting buy to re-anchor to the
              new qty/gi spacing (amend down included). Without this, a
              widened grid interval leaves the buy - and the whole book - at
              the old tighter spacing until the market happens to rise. *)
           st.force_buy_reanchor <- true;
           should_execute_strategy := true;
           Logging.info_f
             ~section
             "[%s/%s] Capital oracle updated sizing: qty %.8g gi %.4f%% (D_surv %.1f%%)"
             asset.exchange
             asset.symbol
             d.qty
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
            asset.exchange
            asset.symbol
            d.qty
            d.grid_interval
        | Some d ->
          Logging.warn_f
            ~section
            "[%s/%s] Capital oracle INACTIVE: %s (D_surv %.1f%%, qty %.8g, gi %.4f%%); \
             new orders suspended, fills still tracked"
            asset.exchange
            asset.symbol
            (if d.reason = "" then "capital reallocated" else d.reason)
            (d.d_surv *. 100.0)
            d.qty
            d.grid_interval
        | None ->
          Logging.info_f
            ~section
            "[%s/%s] No capital-oracle decision; sizing falls back to Fear & Greed alone \
             (orders withheld if no live F&G reading exists)"
            asset.exchange
            asset.symbol);
      (* Priority reclamation: an INACTIVE-with-reclaim decision asks this
         domain to cancel its own resting buy(s) so the committed capital
         returns to the account pool for a higher-priority asset. Runs OUTSIDE
         the execution gate on purpose: a halted asset must still release
         capital. The cancel is a first-class Grid strategy order pushed
         through the grid's order buffer into the established supervisor
         pipeline (supervisor_orders dispatch_cancel -> Order_executor ->
         dashboard/order tracking), guarded like the grid's own excess-buy
         cancel (strategy mutex held, mid-amendment buys skipped - Hyperliquid
         rejects canceling an order being amended). SELF-HEALING: the cancel
         is latched (not re-issued every cycle) but it is RETRIED while the
         reclaim decision persists and eligible buys still sit in the store,
         and the latch re-arms the moment the store shows no eligible buy (the
         cancel landed) or the decision stops being a reclaim. Without the
         retry, a single failed cancel (dispatch dropped on a connection flap,
         exchange rejection, ring-buffer full) would leave the account
         permanently stuck: the reclaimed asset stays paused (the oracle's
         plan only clears once the store's committed value drops to zero), the
         priority asset never resumes on capital that was never released, and
         the dashboard keeps showing the resting buy. Wakes the capital oracle
         ([request_pass]) so released capital is recognized promptly even if
         the exchange's WS cancel event is missed. *)
      (match oracle_decision with
       | Some d when d.reclaim_capital ->
         let now = Unix.gettimeofday () in
         let st = Dio_strategies.Suicide_grid.get_strategy_state asset_with_fees.symbol in
         Mutex.lock st.mutex;
         Fun.protect
           ~finally:(fun () -> Mutex.unlock st.mutex)
           (fun () ->
              (* Eligible = cancellable resting buys (not mid-amendment);
                 [any_buy] distinguishes "store is clean" from "only buys
                 stuck mid-amendment" - a mid-amend buy cannot be cancelled
                 (the exchange rejects it) and is expected to resolve into a
                 cancellable replacement on its own. *)
              let eligible = ref 0 in
              let any_buy = ref false in
              Ex.fold_open_orders
                ~symbol:asset_with_fees.symbol
                ~init:()
                ~f:(fun () (o : Types.open_order) ->
                  if o.side = Types.Buy && o.remaining_qty > 0.0
                  then (
                    any_buy := true;
                    if
                      not
                        (Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight
                           o.order_id)
                    then incr eligible))
              |> ignore;
              match
                Dio_strategies.Suicide_grid.reclaim_step
                  ~now
                  ~retry_seconds:reclaim_retry_seconds
                  ~issued:!reclaim_cancel_issued
                  ~issued_at:!reclaim_cancel_at
                  ~eligible:!eligible
                  ~any_buy:!any_buy
              with
              | Dio_strategies.Suicide_grid.Reclaim_rearm ->
                (* The store no longer holds any buy: the cancel(s) landed (or
                   never needed). Re-arm the latch so a later reclaim decision
                   re-triggers cleanly, and wake the capital oracle so it
                   re-sizes with the released capital - a release the oracle
                   does not yet know about is exactly the stall that leaves
                   the priority asset parked. *)
                reclaim_cancel_issued := false;
                reclaim_cancel_at := 0.0;
                Oracle_runtime.request_pass ()
              | Dio_strategies.Suicide_grid.Reclaim_cancel _ ->
                let n = ref 0 in
                Ex.fold_open_orders
                  ~symbol:asset_with_fees.symbol
                  ~init:0
                  ~f:(fun acc (o : Types.open_order) ->
                    if
                      o.side = Types.Buy
                      && o.remaining_qty > 0.0
                      && not
                           (Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight
                              o.order_id)
                    then (
                      let cancel =
                        Dio_strategies.Suicide_grid.create_cancel_order
                          o.order_id
                          asset_with_fees.symbol
                          Dio_strategies.Strategy_common.Grid
                          asset_with_fees.exchange
                      in
                      ignore (Dio_strategies.Suicide_grid.push_order ~now cancel);
                      incr n;
                      acc + 1)
                    else acc)
                |> ignore;
                reclaim_cancel_issued := true;
                reclaim_cancel_at := now;
                (* Wake the capital oracle so it re-sizes against the released
                   capital as soon as it lands - the reclaim cycle is
                   self-driving and does not depend on the exchange's WS
                   cancel event reaching this domain. *)
                Oracle_runtime.request_pass ();
                Logging.warn_f
                  ~section
                  "[%s/%s] Capital oracle reclaim: canceling %d resting buy(s) to return \
                   capital to %s"
                  asset_with_fees.exchange
                  asset_with_fees.symbol
                  !n
                  d.reclaim_target
              | Dio_strategies.Suicide_grid.Reclaim_deferred -> ())
       | _ ->
         reclaim_cancel_issued := false;
         reclaim_cancel_at := 0.0);
      (* Oracle/signal startup gate (see the gate state initialized above).
         Opens - once, monotonically - when the startup window has given BOTH
         signals their chance: the first capital-oracle decision for this
         asset (active or INACTIVE: an INACTIVE one halts new orders through
         oracle_halted above) opens it at any time; otherwise a live Fear &
         Greed reading opens it once the oracle's first pass attempt has
         finished or the startup deadline has elapsed (one real signal
         suffices after both had their chance). It never opens on fabricated
         config defaults - with neither signal the grid cannot profitably and
         accurately create orders, so it does not (the execute flag stays
         cleared and a one-shot warning fires once the grace period elapses).
         When only one source is active, a one-shot warning names the failed
         one. While closed, the execute flag is cleared so the domain falls
         through to the per-symbol wakeup wait below instead of busy-
         spinning. *)
      if not !oracle_gate_open
      then (
        let fng_available =
          match Fear_and_greed.get_cached () with
          | Some _ -> true
          | None -> false
        in
        (* Equities are pure oracle: F&G is never a valid sizing signal or
           gate opener for them (only the capital oracle can open an equity
           grid domain). *)
        let is_crypto = is_crypto_exchange asset_with_fees.exchange in
        let gate_waiver =
          Oracle_runtime.first_pass_attempt_done ()
          || Unix.gettimeofday () >= !oracle_gate_deadline
        in
        match oracle_decision with
        | Some d ->
          oracle_gate_open := true;
          should_execute_strategy := true;
          (* Only the oracle is active: warn once if F&G is unavailable. *)
          if (not fng_available) && not !fng_unavailable_warned
          then (
            fng_unavailable_warned := true;
            Logging.warn_f
              ~section
              "[%s/%s] Fear & Greed unavailable (fetch failed); sizing from the capital \
               oracle only"
              asset_with_fees.exchange
              asset_with_fees.symbol);
          Logging.info_f
            ~section
            "[%s/%s] Capital oracle first decision received (%s, qty %.8g gi %.4f%%, \
             D_surv %.1f%%); grid gate open"
            asset_with_fees.exchange
            asset_with_fees.symbol
            (if d.active then "ACTIVE" else "INACTIVE")
            d.qty
            d.grid_interval
            (d.d_surv *. 100.0)
        | None
          when fng_available
               && is_crypto
               && grid_gate_should_open
                    ~oracle_decision:false
                    ~fng_available:true
                    ~gate_waiver ->
          oracle_gate_open := true;
          should_execute_strategy := true;
          (* Materialize the grid strategy from F&G when it started with no
             sizing signal at all (ref is None). *)
          (match !grid_strategy_asset_ref with
           | Some _ -> ()
           | None ->
             (match fng_grid_interval () with
              | Some gi ->
                grid_strategy_asset_ref := Some (grid_asset_of ~grid_interval:gi ())
              | None -> ()));
          (* Only F&G is active: warn once if the oracle failed to produce a
             decision for an asset it models (pass finished without one, or
             the startup window elapsed); plain info otherwise. The messages
             distinguish the three real causes:
             - cold start: the gate opened on the startup deadline while the
               oracle's first history refresh was still running (no
               materialized state yet) - NOT an analysis failure;
             - analysis failed: at least one real pass finished and this
               asset still has no decision;
             - startup window elapsed: no pass ever completed. *)
          if oracle_tracks_asset
          then (
            match Oracle_runtime.materialized () with
            | None ->
              Logging.warn_f
                ~section
                "[%s/%s] Capital-oracle first history refresh still in progress; sizing \
                 from Fear & Greed only (startup deadline elapsed)"
                asset_with_fees.exchange
                asset_with_fees.symbol
            | Some _ when Oracle_runtime.first_pass_attempt_done () ->
              Logging.warn_f
                ~section
                "[%s/%s] Capital-oracle produced no decision for this asset (analysis \
                 failed); sizing from Fear & Greed only"
                asset_with_fees.exchange
                asset_with_fees.symbol
            | Some _ ->
              Logging.warn_f
                ~section
                "[%s/%s] Capital-oracle never completed a pass (startup window elapsed); \
                 sizing from Fear & Greed only"
                asset_with_fees.exchange
                asset_with_fees.symbol)
          else
            Logging.info_f
              ~section
              "[%s/%s] Asset not modeled by the capital oracle; sizing from Fear & Greed"
              asset_with_fees.exchange
              asset_with_fees.symbol
        | None when fng_available ->
          (* F&G is live but the startup window has not closed (the oracle's
             first pass attempt has not finished and the deadline has not
             elapsed): keep the gate closed so BOTH signals get their chance
             at startup. The gate opens on the oracle's first decision, or on
             F&G alone once the pass attempt finishes / the deadline elapses -
             whichever comes first (crypto only; an equity opens on the oracle
             decision alone - pure oracle). *)
          should_execute_strategy := false;
          if not !fng_ready_wait_warned
          then (
            fng_ready_wait_warned := true;
            Logging.info_f
              ~section
              "[%s/%s] %s; waiting for the capital-oracle first pass before sizing"
              asset_with_fees.exchange
              asset_with_fees.symbol
              (if is_crypto
               then "Fear & Greed live"
               else "equity asset sizes from the capital oracle only (pure oracle)"))
        | None ->
          (* Neither source can open the gate (no oracle decision, and either
             no F&G reading or an equity asset that never takes F&G): withhold
             orders. The execute flag stays cleared; the gate re-checks every
             cycle, so the first oracle decision opens it. Warn once after the
             grace period. *)
          should_execute_strategy := false;
          if gate_waiver && not !no_signal_warned
          then (
            no_signal_warned := true;
            if is_crypto
            then
              Logging.warn_f
                ~section
                "[%s/%s] No capital-oracle decision and no Fear & Greed signal; orders \
                 withheld until the oracle publishes a decision or a live F&G reading \
                 exists"
                asset_with_fees.exchange
                asset_with_fees.symbol
            else
              Logging.warn_f
                ~section
                "[%s/%s] No capital-oracle decision; equity sizing is pure oracle, so \
                 orders are withheld until the oracle publishes one"
                asset_with_fees.exchange
                asset_with_fees.symbol))
      else ();
      (* The oracle-halt no longer gates the whole execution block: an
         INACTIVE decision halts BUY placement inside the strategy (the
         [~oracle_halted] flag passed to execute_strategy) but the SELL leg
         still runs - a sell is the account's capital-recovery path (it needs
         only inventory, not quote), so the sell for a just-filled buy is
         placed even when capital is exhausted and the asset is halted.
         Without this the last fill's inventory sits unreclaimable and the
         pool never recovers. *)
      let should_execute =
        !exec_ready
        && !should_execute_strategy
        && has_exec_fn ()
        && (not equity_market_closed)
        && !oracle_gate_open
      in
      if should_execute
      then (
        should_execute_strategy := false;
        (* Single-pass open order scan: count by strategy AND collect
             grid buy/sell order lists. Eliminates a second iter_open_orders
             + orders_mutex acquisition inside the grid strategy. *)
        let iter_orders f = Ex.iter_open_orders_fast ~symbol:asset_with_fees.symbol f in
        (* Pass iter_orders closure directly down. This removes the 2-3ms STW GC pause 
             caused by allocating intermediate Order tracking lists exactly on the event hotpath. *)
        (* Fast-path tick perfect balance access without hashtable locks *)
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
        (* Balance snapshot staleness: a fresh quote balance is authoritative
           for the placement guard (an under-funded buy is skipped, not sent
           to be rejected); a stale snapshot may be wrong, so the grid still
           attempts and lets the exchange decide. Exchanges without
           freshness tracking report None -> unknown -> treated as stale
           (previous behavior). *)
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
        (* Apply updated Fear & Greed value to strategy config if changed. A
           missing index (get_cached () = None) means no live F&G signal: the
           re-evaluation is skipped entirely - never neutralized to 50 - and a
           domain still waiting on the gate keeps withholding orders. *)
        let current_fng_opt = Fear_and_greed.get_cached () in
        if current_fng_opt <> !last_known_fng
        then (
          last_known_fng := current_fng_opt;
          match current_fng_opt with
          | None -> ()
          | Some current_fng when not (is_crypto_exchange asset_with_fees.exchange) ->
            (* Equities are pure oracle: F&G never enters the sizing (the
               capital oracle owns the equity grid entirely). Log once so the
               operator knows the signal was deliberately ignored, then keep
               last_known_fng bookkeeping so a later change re-checks. *)
            Logging.debug_f
              ~section
              "[%s/%s] Fear & Greed updated to %.2f but ignored: equity asset sizes from \
               the capital oracle only (pure oracle)"
              asset_with_fees.exchange
              asset_with_fees.symbol
              current_fng
          | Some current_fng ->
            (* One blend, one owner. The capital oracle computes the crypto
               grid interval as a weighted blend of the F&G side, the
               per-asset range side and the survival-constrained parameter,
               and publishes the composition in the decision's
               [parameter_components]. While the oracle holds a decision for
               this asset (active or INACTIVE) that blended gi is the sizing:
               re-evaluating a pure F&G value over the config range here
               would fight the oracle's value every cycle (the oracle
               re-applies its gi and the grid flickers between the two
               systems). F&G still manages accumulation_buffer, which the
               oracle does not size. Only when the oracle has NO decision for
               this asset (not modeled, or analysis failed) does F&G-alone
               sizing apply here - the fng side of the blend without the
               survival/range constraint, explicitly labeled as the fallback
               it is. *)
            let lo, hi = asset_with_fees.grid_interval in
            let fng_interval =
              Fear_and_greed.grid_value_for_fng
                ~grid_interval:asset_with_fees.grid_interval
                ~fear_and_greed:current_fng
            in
            (* F&G owns accumulation_buffer in every crypto case: the oracle
               does not size it. *)
            let update_accumulation_buffer () =
              let exch_id =
                Dio_exchange.Exchange_intf.Types.exchange_of_string
                  asset_with_fees.exchange
              in
              let is_accumulation_exch =
                match exch_id with
                | Hyperliquid | Ibkr | Lighter -> true
                | _ -> false
              in
              if is_accumulation_exch
              then (
                let ab_lo, ab_hi = asset_with_fees.accumulation_buffer in
                let new_ab =
                  Fear_and_greed.grid_value_for_fng
                    ~grid_interval:asset_with_fees.accumulation_buffer
                    ~fear_and_greed:current_fng
                in
                Logging.info_f
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
                      Dio_strategies.Suicide_grid.accumulation_buffer = new_ab
                    }
                  in
                  grid_strategy_asset_ref := Some new_asset
                | None -> ())
            in
            (match oracle_decision with
             | Some d when d.active ->
               (* Oracle owns the sizing: log the sizing it actually published
                  (how the gi/qty were chosen - the survival-driven reasons
                  carried by the decision - and the replayed D_surv) instead
                  of an F&G-only value, and touch only the accumulation
                  buffer. *)
               Logging.info_f
                 ~section
                 "[%s/%s] Fear & Greed updated to %.2f: oracle sizing gi %.4f%% (%s) · \
                  qty %.6g (%s) · D_surv %.1f%%"
                 asset_with_fees.exchange
                 asset_with_fees.symbol
                 current_fng
                 d.grid_interval
                 d.gi_reason
                 d.qty
                 d.qty_reason
                 (d.d_surv *. 100.0);
               update_accumulation_buffer ()
             | Some _ ->
               (* Oracle decision exists but INACTIVE: the oracle owns the
                  sizing and orders are withheld; no competing F&G value is
                  applied (it would be adopted on re-activation only to be
                  immediately replaced by the oracle's own gi). *)
               Logging.debug_f
                 ~section
                 "[%s/%s] F&G gi re-evaluation skipped: capital-oracle decision INACTIVE \
                  (orders withheld)"
                 asset_with_fees.exchange
                 asset_with_fees.symbol;
               update_accumulation_buffer ()
             | None ->
               (* No capital-oracle decision: F&G-alone is the designed
                  fallback. This is exactly the fng side the oracle would
                  blend (same mapping over the same config range) minus the
                  survival/range constraint only the oracle's analysis can
                  compute - labeled as fallback so the two signals never read
                  as fighting. *)
               Logging.info_f
                 ~section
                 "[%s/%s] No capital-oracle decision; sizing grid_interval from Fear & \
                  Greed only (fallback): %.4f%% (range %.4f-%.4f)"
                 asset_with_fees.exchange
                 asset_with_fees.symbol
                 fng_interval
                 lo
                 hi;
               update_accumulation_buffer ();
               (match !grid_strategy_asset_ref with
                | Some asset ->
                  let new_asset =
                    { asset with
                      Dio_strategies.Suicide_grid.grid_interval = fng_interval
                    }
                  in
                  grid_strategy_asset_ref := Some new_asset
                | None -> ())));
        (* Compute wall-clock timestamp once per cycle for strategy use,
             eliminating Unix.time/gettimeofday syscalls inside the strategy. *)
        let now = Unix.gettimeofday () in
        (* Count this strategy invocation as an activity tick so the dashboard
             can report executions/sec and last-execution time even when the
             window's latency sample count is zero (S2). *)
        Latency_profiler.tick_exec prof_strategy ~now;
        (match !grid_strategy_asset_ref, cached_grid_state with
         | Some asset, Some cs ->
           Dio_strategies.Suicide_grid.Strategy.execute
             ~cached_state:cs
             ~quote_balance_stale
             ~oracle_halted
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
      let t4 = if latency_this_cycle then Mtime_clock.now_ns () else 0L in
      if should_execute && latency_this_cycle
      then
        Latency_profiler.record prof_strategy (Mtime.Span.of_uint64_ns (Int64.sub t4 t3));
      (* Flush deferred accumulation persistence outside the strategy hotloop.
           Only performs file I/O when the dirty flag was set during execute_strategy. *)
      if should_execute
      then (
        match !grid_strategy_asset_ref with
        | Some _ ->
          Dio_strategies.Suicide_grid.Strategy.flush_persistence asset_with_fees.symbol
        | None -> ());
      (* Record cycle work time before blocking. Captures active processing
           latency only, excluding sleep time in Exchange_wakeup.wait.
           Only busy cycles (real book/exec/strategy work) are recorded: idle
           wakeups would otherwise pin cycle p50/p99 at 0us (F2). *)
      let cycle_busy = did_ob || did_exec || should_execute in
      let cycle_span = Mtime.Span.of_uint64_ns (Int64.sub t4 t1) in
      if latency_this_cycle && cycle_busy
      then (
        let cause_thunk () =
          let alloc_diff = Gc.minor_words () -. alloc_start in
          (* M8: GC stats are window-scoped; the cause string uses the last
             [publish_windows] pair, never a per-cycle [Gc.quick_stat]. *)
          let gc_str = Gc_monitor.diff_to_string !gc_start !gc_end in
          Printf.sprintf
            "ob:%B ex:%d st:%B al:%.0fw%s"
            did_ob
            !cycle_events
            should_execute
            alloc_diff
            gc_str
        in
        Latency_profiler.record_with_cause prof_cycle cycle_span cause_thunk);
      (* Roll the latency window on a fixed time cadence rather than a cycle
           count: at typical domain cycle rates the old cycle_mod gate (10000
           cycles) accumulated minutes of samples before an abrupt wipe (F1). *)
      let now_flush = Unix.gettimeofday () in
      if now_flush -. !last_window_time >= latency_window_seconds
      then (
        last_window_time := now_flush;
        publish_windows ());
      (* Block until the next websocket frame signals new data or until data is ready.
           Use cached has_exec_fn closure instead of Ex.has_execution_data to
           avoid Hashtbl lookup on the hot blocking path. *)
      if (not !should_execute_strategy) || not (has_exec_fn ())
      then Concurrency.Exchange_wakeup.wait ~symbol:asset_with_fees.symbol;
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

(** Condition variable signalled on domain exit (crash or normal). Allows
    supervisor_loop to react immediately rather than waiting the full 5s tick. *)
let domain_died_mutex = Mutex.create ()

let domain_died_cond = Condition.create ()

(** Signal domain_died_cond to wake the supervisor after a domain exits. *)
let notify_domain_died () =
  Mutex.lock domain_died_mutex;
  Condition.signal domain_died_cond;
  Mutex.unlock domain_died_mutex
;;

(** Spawn a new OCaml domain for the given state, guarded by its mutex.
    Joins any previous domain handle before spawning. Returns false if
    the domain is already running. *)
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
        (* Catch ALL exceptions including those from apply_gc_config.
         Previously apply_gc_config was outside the try/with, so a
         CamlinternalLazy.Undefined from concurrent Lazy.force on the
         shared cached_gc_config would silently kill the domain. *)
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
          (* Mark domain as stopped; notify supervisor for potential restart.
           domain_handle is preserved for join on next start_domain call. *)
          Atomic.set state.is_running false;
          notify_domain_died ();
          ())
    in
    Atomic.set state.domain_handle (Some domain_handle);
    Mutex.unlock state.mutex;
    Logging.info_f ~section "Domain %s started successfully" key;
    true)
;;

(** Stop a running domain: set is_running to false, clean up strategy state,
    signal blocked workers via Exchange_wakeup, and join the domain handle. *)
let stop_domain state =
  let key = domain_key state.asset in
  Mutex.lock state.mutex;
  Atomic.set state.is_running false;
  (* Release strategy state for this symbol *)
  let symbol = state.asset.symbol in
  (match state.asset.strategy with
   | "Grid" | "suicide_grid" ->
     Dio_strategies.Suicide_grid.Strategy.cleanup_strategy_state symbol
   | "MM" -> Dio_strategies.Market_maker.Strategy.cleanup_strategy_state symbol
   | _ -> ());
  (* Unblock workers in Exchange_wakeup.wait so they observe is_running=false
     and exit the main loop. *)
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

(** Persistent waker thread: signals domain_died_cond every 5s so the
    supervisor loop wakes on a regular cadence even when no domain crashes.
    Allocated once at module load to avoid per-iteration thread leaks. *)
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

(** Supervisor monitoring loop. Blocks on domain_died_cond, then iterates
    the registry and restarts any stopped domains with exponential backoff. *)
let supervisor_loop config fee_fetcher =
  let section = "domain_supervisor" in
  Logging.info ~section "Domain supervisor started";
  while not (Atomic.get shutdown_requested) do
    try
      (* Block until domain_died_cond is signalled by a crashed domain or
         the periodic 5s tick from _supervisor_waker_thread. *)
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

(** Initialize strategies, register all assets, start their domains, and
    launch the supervisor thread. Returns the supervisor Thread.t handle. *)
let spawn_supervised_domains_for_assets
      (config : config)
      (fee_fetcher : trading_config -> trading_config)
      (assets : trading_config list)
  : Thread.t
  =
  (* Initialize strategy module state *)
  Dio_strategies.Suicide_grid.Strategy.init ();
  Dio_strategies.Market_maker.Strategy.init ();
  (* Register each asset in the domain registry *)
  List.iter (fun asset -> ignore (register_domain asset)) assets;
  (* Pre-force the shared cached_gc_config Lazy before spawning domains.
     OCaml 5 domains that concurrently Lazy.force the same value race:
     the first domain computes while others block, but if the computing
     domain fails, blocked domains get CamlinternalLazy.Undefined.
     Forcing here in the main domain eliminates the race entirely.
     (Same pattern as the Conduit context pre-force in main.ml.) *)
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

(** Return latency profiler snapshots for all domains from their most
    recently completed windows.
    Result type: (symbol, [(label, snapshot option)]) list.
    Safe to call from the dashboard; does not touch live profiler state.

    Reads the immutable snapshots published by each domain's rolling window
    via [Latency_profiler.published_snapshot], a lock-free [Atomic.get].
    No percentile scan runs against a histogram that the domain thread is
    concurrently mutating, which eliminates the torn-read race between the
    dashboard and the domain writer (F4). *)
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
         ; "strategy", Latency_profiler.published_snapshot profs.prof_strategy
         ; "cycle", Latency_profiler.published_snapshot profs.prof_cycle
         ]
       in
       symbol, snaps)
    profiler_refs
;;

(** Initiate graceful shutdown: signal supervisor, stop each domain,
    and wait up to 10s for all domains to terminate. *)
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
  wait_for_stop 10.0
;;
