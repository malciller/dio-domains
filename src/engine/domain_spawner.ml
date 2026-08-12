open Config
module Fear_and_greed = Cmc.Fear_and_greed

(* Capital-oracle runtime (wrapped library: explicit alias avoids opening the
   whole Dio_oracle namespace). *)
module Oracle_runtime = Dio_oracle.Oracle_runtime

(* Exchange interface and types *)
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

let section = "domain_spawner"

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
  (* Seed PRNG for this domain *)

  (* Fetch exchange fee schedule at domain startup *)
  let asset_with_fees = fee_fetcher asset in
  (* Resolve grid_interval once using the cached Fear & Greed index *)
  let resolved_grid_interval =
    if asset_with_fees.strategy = "suicide_grid" || asset_with_fees.strategy = "Grid"
    then (
      let fallback =
        let lo, hi = asset_with_fees.grid_interval in
        (lo +. hi) /. 2.0
      in
      let fng = Fear_and_greed.fetch_value ~fallback () in
      let resolved =
        Fear_and_greed.grid_value_for_fng
          ~grid_interval:asset_with_fees.grid_interval
          ~fear_and_greed:fng
      in
      let lo, hi = asset_with_fees.grid_interval in
      Logging.debug_f
        ~section
        "Resolved grid_interval for %s/%s: %.4f (F&G=%.2f, range %.4f-%.4f)"
        asset_with_fees.exchange
        asset_with_fees.symbol
        resolved
        fng
        lo
        hi;
      Some resolved)
    else None
  in
  (* Resolve accumulation_buffer once via Fear & Greed. Hyperliquid and IBKR. *)
  let resolved_accumulation_buffer =
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
      let fallback =
        let lo, hi = asset_with_fees.accumulation_buffer in
        (lo +. hi) /. 2.0
      in
      let fng = Fear_and_greed.fetch_value ~fallback () in
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
  (* Resolve exchange module from the registry *)
  match Exchange.Registry.get asset_with_fees.exchange with
  | None ->
    Logging.error_f
      ~section
      "Unknown exchange '%s' for asset %s, aborting domain"
      asset_with_fees.exchange
      asset_with_fees.symbol
  | Some (module Ex) ->
    (* Ring buffer read positions for this domain *)
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
    (* Initialize strategy configuration refs based on strategy type.
       The capital-oracle runtime publishes a per-asset decision (qty,
       grid_interval, active) to a lock-free snapshot; when a decision exists
       and the asset is active, its qty/gi win over the F&G-resolved values. *)
    let baseline_price = ref None in
    let last_known_fng = ref (Fear_and_greed.fetch_value ()) in
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
    let grid_strategy_asset_ref =
      if asset_with_fees.strategy = "suicide_grid" || asset_with_fees.strategy = "Grid"
      then (
        let grid_interval =
          match oracle_decision_at_startup with
          | Some d when d.active -> d.grid_interval
          | _ ->
            (match resolved_grid_interval with
             | Some g -> g
             | None ->
               let lo, hi = asset_with_fees.grid_interval in
               (lo +. hi) /. 2.0)
        in
        let qty =
          match oracle_decision_at_startup with
          | Some d when d.active -> Printf.sprintf "%.8g" d.qty
          | _ -> asset_with_fees.qty
        in
        let accumulation_buffer =
          match resolved_accumulation_buffer with
          | Some ab -> ab
          | None ->
            (* Non-Hyperliquid: use midpoint of the configured range *)
            let lo, hi = asset_with_fees.accumulation_buffer in
            (lo +. hi) /. 2.0
        in
        ref
          (Some
             { Dio_strategies.Suicide_grid.exchange = asset_with_fees.exchange
             ; symbol = asset_with_fees.symbol
             ; qty
             ; grid_interval
             ; sell_mult = asset_with_fees.sell_mult
             ; strategy = asset_with_fees.strategy
             ; maker_fee = asset_with_fees.maker_fee
             ; taker_fee = asset_with_fees.taker_fee
             ; accumulation_buffer
             }))
      else ref None
    in
    (* Oracle startup gate: for grid strategies on assets the capital oracle
       models (kraken / hyperliquid / alpaca), withhold strategy execution
       until the runtime's first decision for this asset arrives, so the grid
       never places or amends orders at config/F&G default sizing that the
       first decision would immediately overwrite (the startup qty bounce).
       Event-driven, matching the rest of the startup sequencing: while gated
       the domain clears its execute flag and blocks on the normal per-symbol
       [Exchange_wakeup.wait]; the oracle runtime's on_publish hook
       (Exchange_wakeup.signal_all wired in bin/main.ml) wakes it ~one cycle
       after a decision - or after the first pass attempt - is published.
       [oracle_gate_deadline] is only a watchdog for a runtime that never
       completes a pass at all; it is checked on wakeups, never polled. *)
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
    let oracle_gate_open = ref (not (is_grid_strategy && oracle_tracks_asset)) in
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
         path across Kraken, Hyperliquid, and IBKR — previously only
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
    let publish_windows () =
      ignore (Latency_profiler.snapshot_and_reset prof_ob);
      ignore (Latency_profiler.snapshot_and_reset prof_exec);
      ignore (Latency_profiler.snapshot_and_reset prof_strategy);
      ignore (Latency_profiler.snapshot_and_reset prof_cycle)
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
      let gc_start =
        if latency_this_cycle
        then Gc_monitor.get_stats ()
        else { minor_collections = 0; major_collections = 0; compactions = 0 }
      in
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
      (* Consume pending execution events from the ring buffer *)
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
           the amend as successful — causing an infinite amend spam loop. *)
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
            above (the oracle owns the sizing while it has a decision). *)
      let oracle_decision =
        Oracle_runtime.decision_for
          ~exchange:asset_with_fees.exchange
          ~symbol:asset_with_fees.symbol
      in
      let oracle_halted =
        match oracle_decision with
        | Some d -> not d.active
        | None -> false
      in
      (match oracle_decision, !grid_strategy_asset_ref with
       | Some d, Some asset when d.active ->
         let qty_str = Printf.sprintf "%.8g" d.qty in
         let qty_changed = qty_str <> asset.qty in
         let gi_changed = abs_float (d.grid_interval -. asset.grid_interval) > 1e-12 in
         if qty_changed || gi_changed
         then (
           let new_asset =
             { asset with qty = qty_str; grid_interval = d.grid_interval }
           in
           grid_strategy_asset_ref := Some new_asset;
           let st = Dio_strategies.Suicide_grid.get_strategy_state asset.symbol in
           (try st.grid_qty <- float_of_string qty_str with
            | Failure _ -> ());
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
            "[%s/%s] No capital-oracle decision; resuming config/F&G behavior"
            asset.exchange
            asset.symbol);
      (* Oracle startup gate (see the gate state initialized above). Opens -
         once, monotonically - when the first decision for this asset is
         published (active or INACTIVE: an INACTIVE one halts new orders
         through oracle_halted above), or when the first pass attempt has
         finished without a decision for this asset (analysis failed / the
         runtime could not complete a pass: last-known-good is empty at fresh
         startup, so fall back to config/F&G instead of stalling), or when the
         watchdog deadline elapses. While closed, the execute flag is cleared
         so the domain falls through to the per-symbol wakeup wait below
         instead of busy-spinning. *)
      if not !oracle_gate_open
      then (
        match oracle_decision with
        | Some d ->
          oracle_gate_open := true;
          should_execute_strategy := true;
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
        | None ->
          if
            Oracle_runtime.first_pass_attempt_done ()
            || Unix.gettimeofday () >= !oracle_gate_deadline
          then (
            oracle_gate_open := true;
            should_execute_strategy := true;
            Logging.warn_f
              ~section
              "[%s/%s] No capital-oracle decision for this asset yet (first pass done or \
               watchdog elapsed); grid gate open on config/F&G sizing"
              asset_with_fees.exchange
              asset_with_fees.symbol)
          else should_execute_strategy := false)
      else ();
      let should_execute =
        !exec_ready
        && !should_execute_strategy
        && has_exec_fn ()
        && (not equity_market_closed)
        && (not oracle_halted)
        && !oracle_gate_open
      in
      if should_execute
      then (
        should_execute_strategy := false;
        (* Clear event-driven trigger *)

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
        (* Apply updated Fear & Greed value to strategy config if changed *)
        let current_fng =
          match Fear_and_greed.get_cached () with
          | Some v -> v
          | None -> 50.0
        in
        if current_fng <> !last_known_fng
        then (
          last_known_fng := current_fng;
          (* The capital oracle owns the grid interval while it has an active
             decision; the F&G re-evaluation must not clobber it (the oracle
             re-applies its gi every cycle and would fight the F&G value,
             flickering the grid). F&G still manages accumulation_buffer,
             which the oracle does not size. *)
          let oracle_governing =
            match oracle_decision with
            | Some d when d.active -> true
            | _ -> false
          in
          let lo, hi = asset_with_fees.grid_interval in
          let new_interval =
            Fear_and_greed.grid_value_for_fng
              ~grid_interval:asset_with_fees.grid_interval
              ~fear_and_greed:current_fng
          in
          Logging.info_f
            ~section
            "[%s/%s] Fear & Greed updated to %.2f. Re-evaluated grid_interval to %.4f \
             (range %.4f-%.4f)"
            asset_with_fees.exchange
            asset_with_fees.symbol
            current_fng
            new_interval
            lo
            hi;
          (* Update accumulation_buffer for exchanges that use it *)
          let exch_id =
            Dio_exchange.Exchange_intf.Types.exchange_of_string asset_with_fees.exchange
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
            | Some asset when oracle_governing ->
              (* Oracle owns the grid interval; refresh only the buffer. *)
              let new_asset =
                { asset with Dio_strategies.Suicide_grid.accumulation_buffer = new_ab }
              in
              grid_strategy_asset_ref := Some new_asset
            | Some asset ->
              let new_asset =
                { asset with
                  Dio_strategies.Suicide_grid.grid_interval = new_interval
                ; Dio_strategies.Suicide_grid.accumulation_buffer = new_ab
                }
              in
              grid_strategy_asset_ref := Some new_asset
            | None -> ())
          else (
            match !grid_strategy_asset_ref with
            | Some _ when oracle_governing ->
              Logging.debug_f
                ~section
                "[%s/%s] F&G grid_interval re-evaluation skipped: capital oracle owns \
                 sizing"
                asset_with_fees.exchange
                asset_with_fees.symbol
            | Some asset ->
              let new_asset =
                { asset with Dio_strategies.Suicide_grid.grid_interval = new_interval }
              in
              grid_strategy_asset_ref := Some new_asset
            | None -> ()));
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
        | Some asset, Some cs ->
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
        let gc_end = Gc_monitor.get_stats () in
        let cause_thunk () =
          let alloc_diff = Gc.minor_words () -. alloc_start in
          let gc_str = Gc_monitor.diff_to_string gc_start gc_end in
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
    via [Latency_profiler.published_snapshot] — a lock-free [Atomic.get].
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
