open Config
module Fear_and_greed = Cmc.Fear_and_greed

(* Exchange interface and types *)
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

let section = "domain_spawner"

(** Construct a unique registry key from exchange and symbol. *)
let domain_key asset = Printf.sprintf "%s/%s" asset.exchange asset.symbol

(** Mutable state tracked per supervised domain. *)
type domain_state = {
  asset: trading_config;
  domain_handle: unit Domain.t option Atomic.t;
  last_restart: float Atomic.t;
  restart_count: int Atomic.t;
  is_running: bool Atomic.t;
  mutex: Mutex.t;
}

(** Global registry mapping domain keys to their supervisor state. *)
let domain_registry : (string, domain_state) Hashtbl.t = Hashtbl.create 32
let registry_mutex = Mutex.create ()

(** Atomic flag set to true when graceful shutdown is requested. *)
let shutdown_requested = Atomic.make false

(** Per-symbol latency profiler cache. Persists across domain restarts so
    profiler objects (each ~800KB) are allocated once per symbol rather than
    on every asset_domain_worker invocation. *)
type domain_profilers = {
  prof_ob:       Latency_profiler.t;
  prof_exec:     Latency_profiler.t;
  prof_strategy: Latency_profiler.t;
  prof_cycle:    Latency_profiler.t;
}
let domain_profiler_cache : (string, domain_profilers) Hashtbl.t = Hashtbl.create 8
let profiler_cache_mutex = Mutex.create ()

let get_domain_profilers symbol =
  Mutex.lock profiler_cache_mutex;
  let profs = match Hashtbl.find_opt domain_profiler_cache symbol with
    | Some p -> p
    | None ->
        let p = {
          prof_ob       = Latency_profiler.create (symbol ^ ":ob");
          prof_exec     = Latency_profiler.create (symbol ^ ":exec");
          prof_strategy = Latency_profiler.create (symbol ^ ":strategy");
          prof_cycle    = Latency_profiler.create ~bucket_us:10 ~max_latency_us:1_000_000 (symbol ^ ":cycle");
        } in
        Hashtbl.replace domain_profiler_cache symbol p;
        p
  in
  Mutex.unlock profiler_cache_mutex;
  profs


(** Core worker function executed by each OCaml domain for a trading asset.
    Runs the event-driven loop: consumes ring buffer events, executes strategy,
    and blocks on Exchange_wakeup between cycles. *)
let asset_domain_worker (config : config) (fee_fetcher : trading_config -> trading_config) (asset : trading_config) =
  Random.self_init ();  (* Seed PRNG for this domain *)
  
  (* Fetch exchange fee schedule at domain startup *)
  let asset_with_fees = fee_fetcher asset in

  (* Resolve grid_interval once using the cached Fear & Greed index *)
  let resolved_grid_interval =
    if asset_with_fees.strategy = "suicide_grid" || asset_with_fees.strategy = "Grid" then
      let fallback = let (lo, hi) = asset_with_fees.grid_interval in (lo +. hi) /. 2.0 in
      let fng = Fear_and_greed.fetch_value ~fallback () in
      let resolved = Fear_and_greed.grid_value_for_fng ~grid_interval:asset_with_fees.grid_interval ~fear_and_greed:fng in
      let (lo, hi) = asset_with_fees.grid_interval in
      Logging.info_f ~section "Resolved grid_interval for %s/%s: %.4f (F&G=%.2f, range %.4f-%.4f)"
        asset_with_fees.exchange asset_with_fees.symbol resolved fng lo hi;
      Some resolved
    else
      None
  in

  (* Resolve accumulation_buffer once via Fear & Greed. Hyperliquid and IBKR. *)
  let resolved_accumulation_buffer =
    if (asset_with_fees.exchange = "hyperliquid" || asset_with_fees.exchange = "ibkr" || asset_with_fees.exchange = "lighter")
       && (asset_with_fees.strategy = "suicide_grid" || asset_with_fees.strategy = "Grid") then
      let fallback = let (lo, hi) = asset_with_fees.accumulation_buffer in (lo +. hi) /. 2.0 in
      let fng = Fear_and_greed.fetch_value ~fallback () in
      let resolved = Fear_and_greed.grid_value_for_fng ~grid_interval:asset_with_fees.accumulation_buffer ~fear_and_greed:fng in
      let (lo, hi) = asset_with_fees.accumulation_buffer in
      Logging.info_f ~section "Resolved accumulation_buffer for %s/%s: %.4f (F&G=%.2f, range %.4f-%.4f)"
        asset_with_fees.exchange asset_with_fees.symbol resolved fng lo hi;
      Some resolved
    else
      None
  in
  



  (* Resolve exchange module from the registry *)

  match Exchange.Registry.get asset_with_fees.exchange with
  | None ->
      Logging.error_f ~section "Unknown exchange '%s' for asset %s, aborting domain"
        asset_with_fees.exchange asset_with_fees.symbol
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
      let cached_grid_buy_orders = ref [] in
      let cached_grid_sell_orders = ref [] in
      let cached_grid_open_buy_count = ref 0 in
      let cached_grid_open_sell_count = ref 0 in
      let cached_mm_open_buy_count = ref 0 in
      let cached_mm_open_sell_count = ref 0 in
      let cached_mm_open_orders = ref [] in
      let cached_global_locked_buys = ref 0.0 in
      let cached_global_locked_sells = ref 0.0 in

      (* Initialize strategy configuration refs based on strategy type *)
      let baseline_price = ref None in
      let last_known_fng = ref (Fear_and_greed.fetch_value ()) in

       let grid_strategy_asset_ref =
        if asset_with_fees.strategy = "suicide_grid" || asset_with_fees.strategy = "Grid" then
          let grid_interval =
            match resolved_grid_interval with
            | Some g -> g
            | None ->
                let (lo, hi) = asset_with_fees.grid_interval in
                (lo +. hi) /. 2.0
          in
          let accumulation_buffer =
            match resolved_accumulation_buffer with
            | Some ab -> ab
            | None ->
                (* Non-Hyperliquid: use midpoint of the configured range *)
                let (lo, hi) = asset_with_fees.accumulation_buffer in
                (lo +. hi) /. 2.0
          in
          ref (Some {
            Dio_strategies.Suicide_grid.exchange = asset_with_fees.exchange;
            symbol = asset_with_fees.symbol;
            qty = asset_with_fees.qty;
            grid_interval;
            sell_mult = asset_with_fees.sell_mult;
            strategy = asset_with_fees.strategy;
            maker_fee = asset_with_fees.maker_fee;
            taker_fee = asset_with_fees.taker_fee;
            accumulation_buffer;
          })
        else ref None
      in
      let mm_strategy_asset_ref =
        if asset_with_fees.strategy = "MM" then
          ref (Some {
            Dio_strategies.Market_maker.exchange = asset_with_fees.exchange;
            symbol = asset_with_fees.symbol;
            qty = asset_with_fees.qty;
            min_usd_balance = asset_with_fees.min_usd_balance;
            max_exposure = asset_with_fees.max_exposure;
            strategy = asset_with_fees.strategy;
            maker_fee = asset_with_fees.maker_fee;
            taker_fee = asset_with_fees.taker_fee;
          })
        else ref None
      in

      (* Pre-populate strategy state fields (exchange_id, grid_qty, maker_fee)
         so that fill handlers invoked during exec event consumption have
         correct values before the first execute_strategy call. Without this,
         profit calculations and persistence writes use zero defaults. *)
      (match !grid_strategy_asset_ref with
       | Some asset ->
           let st = Dio_strategies.Suicide_grid.get_strategy_state asset.symbol in
           st.exchange_id <- asset.exchange;
           st.grid_qty <- (try float_of_string asset.qty with Failure _ -> 0.001);
           st.cached_sell_mult <- (try float_of_string asset.Dio_strategies.Suicide_grid.sell_mult with Failure _ -> 1.0);
           st.cached_ecfg <- Dio_strategies.Suicide_grid.get_exchange_config asset.exchange;
           st.maker_fee <- (match asset.maker_fee with
             | Some f -> f
             | None ->
                 match Dio_strategies.Fee_cache.get_maker_fee ~exchange:asset.exchange ~symbol:asset.symbol with
                 | Some cached -> cached
                 | None -> 0.0);
            ()
       | None -> ());

      (* Initialize exec read position: ALL exchanges start from position 0
         to replay snapshot events through handle_order_acknowledged, restoring
         last_buy_order_id and open sell tracking. This unifies the startup
         path across Kraken, Hyperliquid, and IBKR — previously only
         Hyperliquid replayed from 0, causing a race condition where Kraken
         domains could execute their first strategy cycle before the snapshot
         populated the open_orders Hashtbl. *)
      Logging.info_f ~section "About to get execution feed position for %s" asset_with_fees.symbol;
      exec_read_pos := 0;

      (* Wait for execution snapshot to be ingested before entering the loop.
         Without this, the first cycle may see zero open orders and place
         duplicates. Timeout after 15s to avoid blocking indefinitely. *)
      let deadline = Unix.gettimeofday () +. 15.0 in
      while not (Ex.has_execution_data ~symbol:asset_with_fees.symbol)
            && Unix.gettimeofday () < deadline do
        Thread.delay 0.05
      done;
      if not (Ex.has_execution_data ~symbol:asset_with_fees.symbol) then
        Logging.warn_f ~section "Execution data not ready for %s/%s after 15s, proceeding anyway"
          asset_with_fees.exchange asset_with_fees.symbol
      else
        Logging.info_f ~section "Execution data confirmed ready for %s/%s"
          asset_with_fees.exchange asset_with_fees.symbol;

      Logging.info_f ~section "Domain for %s/%s starting consumption from exec position 0 (full replay)"
        asset_with_fees.exchange asset_with_fees.symbol;
      
      (* Set orderbook positions to current write position, skipping
         stale ring buffer data. Starting at 0 would replay up to 128 historical
         entries per symbol on every restart, causing excessive allocations. *)
      orderbook_read_pos := Ex.get_orderbook_position ~symbol:asset_with_fees.symbol;
      (* Seed current_price and top_of_book from the exchange live cache so
         the first cycle can execute immediately rather than waiting for the
         next incoming update. *)
      (match Ex.get_top_of_book ~symbol:asset_with_fees.symbol with
       | Some (bid_price, bid_size, ask_price, ask_size) ->
           tob_bid := bid_price; tob_ask := ask_price;
           tob_bsize := bid_size; tob_asize := ask_size;
           current_price := (bid_price +. ask_price) /. 2.0;
           Logging.info_f ~section "Seeded initial price for %s from cache: %.4f"
             asset_with_fees.symbol !current_price
       | None -> ());
      
      Logging.info_f ~section "Domain initialized for asset: %s/%s (Strategy: %s)"
        asset_with_fees.exchange asset_with_fees.symbol asset_with_fees.strategy;


      let key = domain_key asset_with_fees in
      let state = Hashtbl.find domain_registry key in

      Logging.info_f ~section "Entering domain loop for %s. is_running=%B" key (Atomic.get state.is_running);
      
      (* Parse base/quote currency pair from the symbol *)
      let (base_asset, quote_currency) =
        if String.contains asset_with_fees.symbol '/' then
          let parts = String.split_on_char '/' asset_with_fees.symbol in
          (List.nth parts 0, List.nth parts 1)
        else
          (asset_with_fees.symbol, "USD")
      in

      (* Cached closures for highly efficient, allocation-free balance reporting *)
      let base_balance_fn = Ex.get_balance_fast ~asset:base_asset in
      let quote_balance_fn = Ex.get_balance_fast ~asset:quote_currency in
      
      (* Cached closures for latency-sensitive feed access in the hot loop *)
      let get_ob_pos_fn = Ex.get_orderbook_position_fast ~symbol:asset_with_fees.symbol in
      let get_tob_fn = Ex.get_top_of_book_fast ~symbol:asset_with_fees.symbol in
      let get_exec_pos_fn = Ex.get_execution_feed_position_fast ~symbol:asset_with_fees.symbol in
      let has_exec_fn = Ex.has_execution_data_fast ~symbol:asset_with_fees.symbol in
      
      let last_buy_count = ref 0 in
      let last_sell_count = ref 0 in

      let cycle_count = ref 0 in

      let { prof_ob; prof_exec; prof_strategy; prof_cycle } =
        get_domain_profilers asset_with_fees.symbol in

      (* Cache strategy state references to avoid repeated mutex acquisition
         on the hot path. References are stable while is_running is true. *)
      let cached_grid_state = match !grid_strategy_asset_ref with
        | Some _ ->
            let st = Dio_strategies.Suicide_grid.get_strategy_state asset_with_fees.symbol in
            st.cached_round_price <- (fun p -> Ex.round_price ~symbol:asset_with_fees.symbol ~price:p);
            st.cached_price_increment <- Option.value (Ex.get_price_increment ~symbol:asset_with_fees.symbol) ~default:0.01;
            st.cached_qty_increment <- Option.value (Ex.get_qty_increment ~symbol:asset_with_fees.symbol) ~default:0.01;
            st.cached_qty_min <- Option.value (Ex.get_qty_min ~symbol:asset_with_fees.symbol) ~default:0.01;
            Some st
        | None -> None in
      let cached_mm_state = match !mm_strategy_asset_ref with
        | Some _ -> Some (Dio_strategies.Market_maker.get_strategy_state asset_with_fees.symbol)
        | None -> None in

      let cached_fng_check_threshold = config.fng_check_threshold in

      while Atomic.get state.is_running do
        let latency_this_cycle = !latency_active in
        if !cycle_count = 0 then Logging.info_f ~section "First cycle for %s" key;
        incr cycle_count;
        

        
        let t1 = if latency_this_cycle then Mtime_clock.now_ns () else 0L in
        (* === ORDERBOOK HOT PATH === *)
        let ob_pos = get_ob_pos_fn () in
        let did_ob = ob_pos <> !orderbook_read_pos || (!orderbook_read_pos = 0 && ob_pos > 0) in
        if did_ob then begin
          orderbook_read_pos := ob_pos;
          (match get_tob_fn () with
           | Some (bid_price, bid_size, ask_price, ask_size) ->
               let changed = bid_price <> !tob_bid || ask_price <> !tob_ask in
               tob_bid := bid_price; tob_ask := ask_price;
               tob_bsize := bid_size; tob_asize := ask_size;
               current_price := (bid_price +. ask_price) /. 2.0;
               if changed then should_execute_strategy := true
           | None -> ());
        end;
        let t2 = if latency_this_cycle then Mtime_clock.now_ns () else 0L in
        if did_ob && latency_this_cycle then Latency_profiler.record prof_ob (Mtime.Span.of_uint64_ns (Int64.sub t2 t1));
        
        (* Consume pending execution events from the ring buffer *)
        let current_pos = get_exec_pos_fn () in
        let did_exec = current_pos <> !exec_read_pos in
        if did_exec then begin
          open_orders_dirty := true;
          let event_count = ref 0 in
          let now_exec = Unix.gettimeofday () in
          let new_pos = Ex.iter_execution_events ~symbol:asset_with_fees.symbol ~start_pos:!exec_read_pos (fun (event : Types.execution_event) ->
            incr event_count;
            match event.order_status with
              | Types.Canceled | Types.Rejected | Types.Expired ->
                  should_execute_strategy := true;

                  let side = match event.side with
                    | Types.Buy -> Dio_strategies.Strategy_common.Buy
                    | Types.Sell -> Dio_strategies.Strategy_common.Sell
                  in
                  (match !grid_strategy_asset_ref with
                   | Some _ ->
                       Dio_strategies.Suicide_grid.Strategy.handle_order_cancelled
                         ~now:now_exec asset_with_fees.symbol event.order_id side event.cl_ord_id;
                       ()
                   | None -> ());
                  (match !mm_strategy_asset_ref with
                   | Some _ ->
                       Dio_strategies.Market_maker.Strategy.handle_order_cancelled
                         ~now:now_exec asset_with_fees.symbol event.order_id side event.cl_ord_id;
                       ()
                   | None -> ())
              | Types.Filled ->
                  should_execute_strategy := true;
                  let side = match event.side with
                    | Types.Buy -> Dio_strategies.Strategy_common.Buy
                    | Types.Sell -> Dio_strategies.Strategy_common.Sell
                  in
                  (match !grid_strategy_asset_ref with
                   | Some _ ->
                       Dio_strategies.Suicide_grid.Strategy.handle_order_filled
                         ~now:now_exec asset_with_fees.symbol event.order_id side ~fill_price:event.avg_price
                         event.cl_ord_id;
                       ()
                   | None -> ());
                  (match !mm_strategy_asset_ref with
                   | Some _ ->
                       Dio_strategies.Market_maker.Strategy.handle_order_filled
                         ~now:now_exec asset_with_fees.symbol event.order_id side ~fill_price:event.avg_price
                         event.cl_ord_id;
                       ()
                   | None -> ());
                  
                  (* Trigger Auto-Hedging module *)
                  if asset_with_fees.hedge then begin
                    let hedge_symbol = String.split_on_char '/' asset_with_fees.symbol |> List.hd in
                    let perp_tob = Ex.get_top_of_book ~symbol:hedge_symbol in
                    Dio_strategies.Auto_hedger.handle_order_filled
                      asset_with_fees.testnet asset_with_fees.exchange hedge_symbol side event.filled_qty event.avg_price perp_tob
                  end
              | Types.New | Types.PartiallyFilled ->
                  should_execute_strategy := true;
                  (* Guard: skip handle_order_acknowledged for in-place amendment
                     confirmations (Kraken exec_type=amended with status=new).
                     The amendment lifecycle is handled by the supervisor's
                     handle_order_amended callback on the REST response path.
                     Routing these through handle_order_acknowledged causes a
                     dual-update race that corrupts open_sell_orders tracking. *)
                  if event.is_amended then begin
                    Logging.debug_f ~section "AMENDED_WS_EVENT %s [%s] status=%s (updating strategy tracker)"
                      event.order_id asset_with_fees.symbol
                      (match event.order_status with Types.New -> "New" | Types.PartiallyFilled -> "PartiallyFilled" | _ -> "Other");
                    (match event.limit_price with
                     | Some price when price > 0.0 ->
                         let side = match event.side with
                           | Types.Buy -> Dio_strategies.Strategy_common.Buy
                           | Types.Sell -> Dio_strategies.Strategy_common.Sell
                         in
                         (match !grid_strategy_asset_ref with
                          | Some _ ->
                              Dio_strategies.Suicide_grid.Strategy.handle_order_amended
                                ~now:now_exec asset_with_fees.symbol event.order_id event.order_id side price;
                              ()
                          | None -> ());
                         (match !mm_strategy_asset_ref with
                          | Some _ ->
                              Dio_strategies.Market_maker.Strategy.handle_order_amended
                                ~now:now_exec asset_with_fees.symbol event.order_id event.order_id side price;
                              ()
                          | None -> ())
                     | _ -> ())
                  end else
                  (match event.limit_price with
                   | Some price when price > 0.0 ->
                       let side = match event.side with
                         | Types.Buy -> Dio_strategies.Strategy_common.Buy
                         | Types.Sell -> Dio_strategies.Strategy_common.Sell
                       in
                       (match !grid_strategy_asset_ref with
                        | Some _ ->
                            Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
                              ~now:now_exec asset_with_fees.symbol event.order_id side price;
                            ()
                        | None -> ());
                       (match !mm_strategy_asset_ref with
                        | Some _ ->
                            Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
                              ~now:now_exec asset_with_fees.symbol event.order_id side price;
                            ()
                        | None -> ())
                   | Some _ ->
                       ()
                   | None -> ())
              | _ -> ()
          ) in
          if !event_count > 0 then begin
            (* First exec batch received: open the startup gate for ALL exchanges *)
            if not !exec_ready then begin
              exec_ready := true;
              exec_ready_cycle := !cycle_count;
              (match !grid_strategy_asset_ref with
               | Some _ ->
                   Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done asset_with_fees.symbol
               | None -> ());
              (match !mm_strategy_asset_ref with
               | Some _ ->
                   Dio_strategies.Market_maker.Strategy.set_startup_replay_done asset_with_fees.symbol
               | None -> ());
              Logging.info_f ~section "[%s/%s] First exec event batch received, strategy now active"
                asset_with_fees.exchange asset_with_fees.symbol
            end
          end;
          exec_read_pos := new_pos;
          exec_checked := true;
        end;
        let t3 = if latency_this_cycle then Mtime_clock.now_ns () else 0L in
        if did_exec && latency_this_cycle then Latency_profiler.record prof_exec (Mtime.Span.of_uint64_ns (Int64.sub t3 t2));
        (* Fallback gate for domains with no open orders: if no exec events
           arrived and the execution data is ready (snapshot ingested), open
           the gate so the strategy can place its initial order. *)
        if not !exec_ready && not !exec_checked
           && has_exec_fn () then begin
          let current_pos_now = get_exec_pos_fn () in
          if current_pos_now = !exec_read_pos then begin
            exec_checked := true;
            (* No exec events arrived and feed is ready: fetch snapshot orders 
               and inject them into the strategies to restore tracking state. *)
            Ex.iter_open_orders_fast ~symbol:asset_with_fees.symbol (fun oid price _qty side_str userref_opt ->
                let is_mm = match userref_opt with
                  | Some uref -> Dio_strategies.Strategy_common.is_strategy_order Dio_strategies.Strategy_common.strategy_userref_mm uref
                  | None -> false
                in
                let order_side = if side_str = "buy" then Dio_strategies.Strategy_common.Buy else Dio_strategies.Strategy_common.Sell in
                if is_mm then begin
                  (match !mm_strategy_asset_ref with
                   | Some _ ->
                       Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
                         ~now:(Unix.gettimeofday ()) asset_with_fees.symbol oid order_side price;
                       ()
                   | None -> ())
                end else begin
                  (match !grid_strategy_asset_ref with
                   | Some _ ->
                       Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
                         ~now:(Unix.gettimeofday ()) asset_with_fees.symbol oid order_side price;
                       ()
                   | None -> ())
                end
            );
            exec_ready := true;
            exec_ready_cycle := !cycle_count;
            (* Mark startup replay complete to ungate profit calculation *)
            (match !grid_strategy_asset_ref with
             | Some _ ->
                 Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done asset_with_fees.symbol
             | None -> ());
            (match !mm_strategy_asset_ref with
             | Some _ ->
                 Dio_strategies.Market_maker.Strategy.set_startup_replay_done asset_with_fees.symbol
             | None -> ());
            Logging.info_f ~section "[%s/%s] Snapshot done, injected open orders - strategy now active"
              asset_with_fees.exchange asset_with_fees.symbol
          end
        end;

        (* Execute strategy if new events have been consumed and feed is ready (event-driven gate) *)
        let should_execute = !exec_ready && !should_execute_strategy && 
                             has_exec_fn () in
        if should_execute then begin
          should_execute_strategy := false;  (* Clear event-driven trigger *)

          (* Single-pass open order scan: count by strategy AND collect
             grid buy/sell order lists. Eliminates a second iter_open_orders
             + orders_mutex acquisition inside the grid strategy. *)


          let iter_orders f =
            Ex.iter_open_orders_fast ~symbol:asset_with_fees.symbol f
          in

          if !open_orders_dirty then begin
            open_orders_dirty := false;
            let current_grid_buy_orders = ref [] in
            let current_grid_sell_orders = ref [] in
            let current_grid_open_buy_count = ref 0 in
            let current_grid_open_sell_count = ref 0 in
            let current_mm_open_buy_count = ref 0 in
            let current_mm_open_sell_count = ref 0 in
            let current_mm_open_orders = ref [] in
            let current_global_locked_buys = ref 0.0 in
            let current_global_locked_sells = ref 0.0 in

            iter_orders (fun oid price qty side_str userref_opt ->
               if qty > 0.0 then begin
                 if side_str = "buy" then current_global_locked_buys := !current_global_locked_buys +. (price *. qty)
                 else if side_str = "sell" then current_global_locked_sells := !current_global_locked_sells +. qty;
                 
                 let is_mm = match userref_opt with
                   | Some uref -> Dio_strategies.Strategy_common.is_strategy_order Dio_strategies.Strategy_common.strategy_userref_mm uref
                   | None -> false
                 in
                 if is_mm then begin
                   current_mm_open_orders := (oid, price, qty, side_str) :: !current_mm_open_orders;
                   if side_str = "buy" then incr current_mm_open_buy_count else incr current_mm_open_sell_count
                 end else begin
                   if side_str = "buy" then begin
                     incr current_grid_open_buy_count;
                     current_grid_buy_orders := (oid, price) :: !current_grid_buy_orders
                   end else begin
                     incr current_grid_open_sell_count;
                     current_grid_sell_orders := (oid, price, qty) :: !current_grid_sell_orders
                   end
                 end
               end
            );
            cached_grid_buy_orders := !current_grid_buy_orders;
            cached_grid_sell_orders := !current_grid_sell_orders;
            cached_grid_open_buy_count := !current_grid_open_buy_count;
            cached_grid_open_sell_count := !current_grid_open_sell_count;
            cached_mm_open_buy_count := !current_mm_open_buy_count;
            cached_mm_open_sell_count := !current_mm_open_sell_count;
            cached_mm_open_orders := !current_mm_open_orders;
            cached_global_locked_buys := !current_global_locked_buys;
            cached_global_locked_sells := !current_global_locked_sells;
          end;

          let grid_buy_orders = !cached_grid_buy_orders in
          let grid_sell_orders = !cached_grid_sell_orders in
          let grid_open_buy_count = ref !cached_grid_open_buy_count in
          let grid_open_sell_count = ref !cached_grid_open_sell_count in
          let mm_open_buy_count = ref !cached_mm_open_buy_count in
          let mm_open_sell_count = ref !cached_mm_open_sell_count in
          let mm_open_orders = !cached_mm_open_orders in
          let global_locked_buys = !cached_global_locked_buys in
          let global_locked_sells = !cached_global_locked_sells in

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
          
          last_buy_count := !grid_open_buy_count + !mm_open_buy_count;
          last_sell_count := !grid_open_sell_count + !mm_open_sell_count;

          (* Trigger async Fear & Greed refresh on significant price movement *)
          if not (Float.is_nan !current_price) then begin
            let cp = !current_price in
            (match !baseline_price with
             | None -> baseline_price := Some cp
             | Some base ->
                 let diff_pct = abs_float ((cp -. base) /. base) *. 100.0 in
                 if diff_pct >= cached_fng_check_threshold then begin
                   Logging.info_f ~section "[%s/%s] Price moved by %.2f%% from baseline $%.2f to $%.2f. Triggering dynamic Fear & Greed check."
                     asset_with_fees.exchange asset_with_fees.symbol diff_pct base cp;
                   baseline_price := Some cp;
                   Fear_and_greed.force_fetch_async ()
                 end)
          end;

          (* Apply updated Fear & Greed value to strategy config if changed *)
          let current_fng = match Fear_and_greed.get_cached () with Some v -> v | None -> 50.0 in
          if current_fng <> !last_known_fng then begin
            last_known_fng := current_fng;
            let (lo, hi) = asset_with_fees.grid_interval in
            let new_interval = Fear_and_greed.grid_value_for_fng ~grid_interval:asset_with_fees.grid_interval ~fear_and_greed:current_fng in
            Logging.info_f ~section "[%s/%s] Fear & Greed updated to %.2f. Re-evaluated grid_interval to %.4f (range %.4f-%.4f)"
              asset_with_fees.exchange asset_with_fees.symbol current_fng new_interval lo hi;
            
            (* Update accumulation_buffer for exchanges that use it *)
            if asset_with_fees.exchange = "hyperliquid" || asset_with_fees.exchange = "ibkr" || asset_with_fees.exchange = "lighter" then begin
              let (ab_lo, ab_hi) = asset_with_fees.accumulation_buffer in
              let new_ab = Fear_and_greed.grid_value_for_fng ~grid_interval:asset_with_fees.accumulation_buffer ~fear_and_greed:current_fng in
              Logging.info_f ~section "[%s/%s] Re-evaluated accumulation_buffer to %.4f (range %.4f-%.4f)"
                asset_with_fees.exchange asset_with_fees.symbol new_ab ab_lo ab_hi;
              (match !grid_strategy_asset_ref with
               | Some asset ->
                   let new_asset = { asset with Dio_strategies.Suicide_grid.grid_interval = new_interval;
                                                Dio_strategies.Suicide_grid.accumulation_buffer = new_ab } in
                   grid_strategy_asset_ref := Some new_asset
               | None -> ())
            end else begin
              (match !grid_strategy_asset_ref with
               | Some asset ->
                   let new_asset = { asset with Dio_strategies.Suicide_grid.grid_interval = new_interval } in
                   grid_strategy_asset_ref := Some new_asset
               | None -> ())
            end
          end;

          (* Compute wall-clock timestamp once per cycle for strategy use,
             eliminating Unix.time/gettimeofday syscalls inside the strategy. *)
          let now = Unix.gettimeofday () in

          (match !grid_strategy_asset_ref, cached_grid_state with
           | Some asset, Some cs ->
               Dio_strategies.Suicide_grid.Strategy.execute ~cached_state:cs ~now
                 ~precounted_orders:(grid_buy_orders, grid_sell_orders, !grid_open_buy_count)
                 asset !current_price !tob_bid !tob_ask asset_bal_val quote_bal_val !grid_open_buy_count !grid_open_sell_count iter_orders !cycle_count
           | _ -> ());
          (match !mm_strategy_asset_ref, cached_mm_state with
           | Some asset, Some cs ->
               let mm_cp = if Float.is_nan !current_price then None else Some !current_price in
               let mm_tob = if Float.is_nan !tob_bid then None else Some (!tob_bid, !tob_bsize, !tob_ask, !tob_asize) in
               let mm_abal = if Float.is_nan asset_bal_val then None else Some asset_bal_val in
               let mm_qbal = if Float.is_nan quote_bal_val then None else Some quote_bal_val in
               Dio_strategies.Market_maker.Strategy.execute ~cached_state:cs ~precounted_orders:(mm_open_orders, global_locked_buys, global_locked_sells) asset mm_cp mm_tob mm_abal mm_qbal !mm_open_buy_count !mm_open_sell_count iter_orders !cycle_count
           | _ -> ());
        end;
        let t4 = if latency_this_cycle then Mtime_clock.now_ns () else 0L in
        if should_execute && latency_this_cycle then Latency_profiler.record prof_strategy (Mtime.Span.of_uint64_ns (Int64.sub t4 t3));

        (* Flush deferred accumulation persistence outside the strategy hotloop.
           Only performs file I/O when the dirty flag was set during execute_strategy. *)
        if should_execute then begin
          (match !grid_strategy_asset_ref with
           | Some _ ->
               Dio_strategies.Suicide_grid.Strategy.flush_persistence asset_with_fees.symbol
           | None -> ())
        end;
        
        (* Periodic cycle statistics (gated by cycle_mod) *)
        if !cycle_count mod config.cycle_mod = 0 then begin
          if not (Float.is_nan !current_price) && not (Float.is_nan !tob_bid) then
            ()
        end;
        
        (* Record cycle work time before blocking. Captures active processing
           latency only, excluding sleep time in Exchange_wakeup.wait. *)
        let cycle_span = Mtime.Span.of_uint64_ns (Int64.sub t4 t1) in
        if latency_this_cycle then Latency_profiler.record prof_cycle cycle_span;

        (* Flush latency reports periodically, gated by cycle_mod to avoid
           5 threshold checks per cycle on the hot path. *)
        if !cycle_count mod config.cycle_mod = 0 then begin

          Latency_profiler.report ~sample_threshold:1000000 prof_ob;
          Latency_profiler.report ~sample_threshold:100000 prof_exec;
          Latency_profiler.report ~sample_threshold:1000000 prof_strategy;
          Latency_profiler.report ~sample_threshold:1000000 prof_cycle;
        end;

        (* Block until the next websocket frame signals new data or until data is ready *)
        if not !should_execute_strategy || not (Ex.has_execution_data ~symbol:asset_with_fees.symbol) then
          Concurrency.Exchange_wakeup.wait ~symbol:asset_with_fees.symbol;

        if !exec_ready && not !latency_active && (!cycle_count - !exec_ready_cycle >= 100) then begin
          latency_active := true;
          Logging.info_f ~section "[%s/%s] Startup warmup complete (100 cycles post-ready). Latency measurements active." asset_with_fees.exchange asset_with_fees.symbol
        end;

        ()
      done

(** Create a new domain_state and register it in the global domain_registry. *)
let register_domain asset =
  let key = domain_key asset in
  let state = {
    asset;
    domain_handle = Atomic.make None;
    last_restart = Atomic.make (Unix.time ());
    restart_count = Atomic.make 0;
    is_running = Atomic.make false;
    mutex = Mutex.create ();
  } in
  Mutex.lock registry_mutex;
  Hashtbl.replace domain_registry key state;
  Mutex.unlock registry_mutex;

  state

(** Condition variable signalled on domain exit (crash or normal). Allows
    supervisor_loop to react immediately rather than waiting the full 5s tick. *)
let domain_died_mutex = Mutex.create ()
let domain_died_cond = Condition.create ()

(** Signal domain_died_cond to wake the supervisor after a domain exits. *)
let notify_domain_died () =
  Mutex.lock domain_died_mutex;
  Condition.signal domain_died_cond;
  Mutex.unlock domain_died_mutex

(** Spawn a new OCaml domain for the given state, guarded by its mutex.
    Joins any previous domain handle before spawning. Returns false if
    the domain is already running. *)
let start_domain config state fee_fetcher =
  let asset = state.asset in
  let key = domain_key asset in

  Mutex.lock state.mutex;
  if Atomic.get state.is_running then (
    Mutex.unlock state.mutex;
    Logging.warn_f ~section "Domain %s is already running" key;
    false
  ) else (
    Atomic.set state.last_restart (Unix.time ());
    Atomic.set state.restart_count (Atomic.get state.restart_count + 1);
    Atomic.set state.is_running true;

    (* Join the previous domain handle synchronously before spawning *)
    (match Atomic.get state.domain_handle with
     | Some old_handle ->

         (try Domain.join old_handle
          with exn -> Logging.warn_f ~section "Exception joining old domain %s: %s" 
                        key (Printexc.to_string exn))
     | None -> ());

    let domain_handle = Domain.spawn (fun () ->
      (* Catch ALL exceptions including those from apply_gc_config.
         Previously apply_gc_config was outside the try/with, so a
         CamlinternalLazy.Undefined from concurrent Lazy.force on the
         shared cached_gc_config would silently kill the domain. *)
      try
        Config.apply_gc_config ();
        Logging.info_f ~section "Domain for %s/%s started (restart #%d)"
          asset.exchange asset.symbol (Atomic.get state.restart_count);

        asset_domain_worker config fee_fetcher asset;
        Logging.info_f ~section "Domain for %s/%s completed normally" asset.exchange asset.symbol
      with exn ->
        Logging.critical_f ~section "Domain for %s/%s crashed (CAUGHT IN SPAWNER): %s"
          asset.exchange asset.symbol (Printexc.to_string exn);

        (* Mark domain as stopped; notify supervisor for potential restart.
           domain_handle is preserved for join on next start_domain call. *)
        Atomic.set state.is_running false;
        notify_domain_died ();
        ()
    ) in

    Atomic.set state.domain_handle (Some domain_handle);
    Mutex.unlock state.mutex;
    Logging.info_f ~section "Domain %s started successfully" key;
    true
  )

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
   | "MM" ->
       Dio_strategies.Market_maker.Strategy.cleanup_strategy_state symbol
   | _ -> ());

  (* Unblock workers in Exchange_wakeup.wait so they observe is_running=false
     and exit the main loop. *)
  Concurrency.Exchange_wakeup.signal_all ();


  (match Atomic.get state.domain_handle with
   | Some handle ->
       Logging.info_f ~section "Stopping domain %s..." key;
       (* Join synchronously; domain exits promptly after is_running is cleared *)
       (try Domain.join handle
        with exn -> Logging.warn_f ~section "Exception joining domain %s: %s"
                      key (Printexc.to_string exn));
       Atomic.set state.domain_handle None
   | None -> ());
  Mutex.unlock state.mutex

(** Returns true if the domain is stopped and no shutdown has been requested. *)
let domain_needs_restart state =
  Mutex.lock state.mutex;
  (* Suppress restart when shutdown is in progress *)
  let needs_restart = not (Atomic.get state.is_running) && not (Atomic.get shutdown_requested) in
  Mutex.unlock state.mutex;
  needs_restart

(** Persistent waker thread: signals domain_died_cond every 5s so the
    supervisor loop wakes on a regular cadence even when no domain crashes.
    Allocated once at module load to avoid per-iteration thread leaks. *)
let _supervisor_waker_thread : Thread.t =
  Thread.create (fun () ->
    while not (Atomic.get shutdown_requested) do
      Thread.delay 5.0;
      Mutex.lock domain_died_mutex;
      Condition.signal domain_died_cond;
      Mutex.unlock domain_died_mutex
    done
  ) ()

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

      List.iter (fun state ->
        (* Early exit if shutdown was requested during iteration *)
        if Atomic.get shutdown_requested then raise Exit;

        if domain_needs_restart state then (
          let key = domain_key state.asset in
          let last_restart = Atomic.get state.last_restart in
          let restart_count = Atomic.get state.restart_count in
          let time_since_restart = Unix.time () -. last_restart in

          (* Exponential backoff: 1s, 2s, 4s, 8s, ... capped at 30s *)
          let backoff_delay = min 30.0 (2.0 ** float_of_int (restart_count - 1)) in

          if time_since_restart >= backoff_delay then (
            Logging.warn_f ~section "Restarting crashed domain %s (attempt #%d, backoff %.1fs)"
              key restart_count backoff_delay;
            ignore (start_domain config state fee_fetcher)
          )
        )
      ) domains

    with exn ->
      match exn with
      | Exit -> ()  (* Clean exit on shutdown *)
      | _ -> Logging.error_f ~section "Exception in domain supervisor: %s" (Printexc.to_string exn)
  done

(** Initialize strategies, register all assets, start their domains, and
    launch the supervisor thread. Returns the supervisor Thread.t handle. *)
let spawn_supervised_domains_for_assets (config : config) (fee_fetcher : trading_config -> trading_config) (assets : trading_config list) : Thread.t =


  (* Initialize strategy module state *)
  Dio_strategies.Suicide_grid.Strategy.init ();
  Dio_strategies.Market_maker.Strategy.init ();

  (* Register each asset in the domain registry *)
  List.iter (fun asset ->
    ignore (register_domain asset)
  ) assets;

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

  List.iter (fun state ->
    ignore (start_domain config state fee_fetcher)
  ) all_states;
  (* Launch the supervisor monitoring thread *)
  let supervisor_thread = Thread.create (supervisor_loop config) fee_fetcher in
  Logging.info ~section "Domain supervisor thread started";
  supervisor_thread

(** Return a snapshot of all domain states for external monitoring. *)
let get_domain_status () =
  Mutex.lock registry_mutex;
  let status = Hashtbl.fold (fun key state acc ->
    let running = Atomic.get state.is_running in
    let restart_count = Atomic.get state.restart_count in
    let last_restart = Atomic.get state.last_restart in
    (key, (running, restart_count, last_restart)) :: acc
  ) domain_registry [] in
  Mutex.unlock registry_mutex;
  status

(** Clear the domain registry. Intended for test teardown only. *)
let clear_domain_registry () =
  Mutex.lock registry_mutex;
  Hashtbl.clear domain_registry;
  Mutex.unlock registry_mutex

(** Return non-destructive latency profiler snapshots for all domains.
    Result type: (symbol, [(label, snapshot option)]) list.
    Safe to call from the dashboard; does not reset profiler data.

    Implementation: snapshots profiler references under profiler_cache_mutex
    in O(N) then computes percentiles outside the lock. This prevents the
    dashboard from blocking domain workers (which acquire the same mutex
    in get_domain_profilers) during the O(bucket_count) percentile scans. *)
let get_domain_profiler_snapshots () =
  (* Phase 1: snapshot profiler references under mutex (fast, O(N) pointer copies). *)
  Mutex.lock profiler_cache_mutex;
  let profiler_refs = Hashtbl.fold (fun symbol profs acc ->
    (symbol, profs) :: acc
  ) domain_profiler_cache [] in
  Mutex.unlock profiler_cache_mutex;
  (* Phase 2: compute percentiles outside mutex (slow, O(bucket_count) per percentile).
     Profiler.snapshot reads only immutable-ish fields (samples, buckets array)
     that are monotonically updated by a single domain writer. Reading stale
     counts is harmless — the next snapshot will pick up the latest values. *)
  List.map (fun (symbol, profs) ->
    let snaps = [

      "orderbook", Latency_profiler.snapshot profs.prof_ob;
      "execution", Latency_profiler.snapshot profs.prof_exec;
      "strategy",  Latency_profiler.snapshot profs.prof_strategy;
      "cycle",     Latency_profiler.snapshot profs.prof_cycle;
    ] in
    (symbol, snaps)
  ) profiler_refs

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
    if max_wait <= 0.0 then
      Logging.warn ~section "Timeout waiting for domains to stop"
    else
      let all_stopped = List.for_all (fun state -> not (Atomic.get state.is_running)) all_states in
      if all_stopped then
        Logging.info ~section "All domains stopped successfully"
      else (
        Thread.delay 0.1;
        wait_for_stop (max_wait -. 0.1)
      )
  in
  wait_for_stop 10.0


