open Config

(** Import memory tracing for tracked data structures *)
let () = try ignore (Sys.getenv "DIO_MEMORY_TRACING") with Not_found -> ()

let section = "domain_spawner"

(** Memory tracing hooks - conditionally available *)
let memory_tracing_available =
  try
    ignore (Sys.getenv "DIO_MEMORY_TRACING");
    true
  with Not_found -> false

(** Get domain key for registry *)
let domain_key asset = Printf.sprintf "%s/%s" asset.exchange asset.symbol

(** Import startup coordinator for phase coordination *)
open Concurrency.Startup_coordinator

(** Domain supervisor state *)
type domain_state = {
  asset: trading_config;
  domain_handle: unit Domain.t option Atomic.t;
  last_restart: float Atomic.t;
  restart_count: int Atomic.t;
  is_running: bool Atomic.t;
  is_initialized: bool Atomic.t;  (* Track if domain has completed initialization *)
  mutex: Mutex.t;
}

(** Global domain registry *)
let domain_registry : (string, domain_state) Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.t =
  Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.create 32
let registry_mutex = Mutex.create ()

(** Global shutdown flag for domain supervisor *)
let shutdown_requested = Atomic.make false


(** The worker function executed by each domain for a trading asset *)
let asset_domain_worker state (fee_fetcher : trading_config -> trading_config) (asset : trading_config) =
  Random.self_init ();  (* Initialize random state for this domain *)
  
  (* Fetch fees at domain startup using the provided fetcher *)
  let asset_with_fees = fee_fetcher asset in
  
  (* Create telemetry metrics for this asset *)
  let asset_label = asset_with_fees.exchange ^ "/" ^ asset_with_fees.symbol in
  let cycles_counter = Telemetry.asset_sliding_counter "domain_cycles" asset_label ~window_size:2000 ~track_rate:true ~rate_window:30.0 () in
  let cycle_duration_hist = Telemetry.asset_histogram "domain_cycle_duration_seconds" asset_label () in
  let open_buy_orders_gauge = Telemetry.asset_gauge "open_buy_orders" asset_label in
  let open_sell_orders_gauge = Telemetry.asset_gauge "open_sell_orders" asset_label in
  let execution_events_consumed_counter = Telemetry.asset_counter "execution_events_consumed" asset_label () in

  let format_distance_info asset_symbol current_price strategy_type =
    match strategy_type with
    | "MM" ->
        let state = Dio_strategies.Market_maker.get_strategy_state asset_symbol in
        (match current_price with
         | None -> " | Strategy: no price data"
         | Some _ ->
             let buy_info = match state.last_buy_order_price with
               | Some buy_price -> Printf.sprintf "Buy@%.2f" buy_price
               | None -> "Buy:none"
             in
             let sell_info = match state.last_sell_order_price with
               | Some sell_price -> Printf.sprintf "Sell@%.2f" sell_price
               | None -> "Sell:none"
             in
             Printf.sprintf " | Strategy: %s %s" buy_info sell_info)
    | _ ->
        (* Default to grid strategy info *)
        let state = Dio_strategies.Suicide_grid.get_strategy_state asset_symbol in
        match current_price with
        | None -> " | Strategy: no price data"
        | Some price ->
            let buy_info =
              match state.last_buy_order_price with
              | Some buy_price ->
                  let buy_distance_pct = ((buy_price -. price) /. price) *. 100.0 in
                  Printf.sprintf "Buy@%.2f(%.2f%c)" buy_price buy_distance_pct '%'
              | None -> "Buy:none"
            in
            let sell_info =
              if Dio_memory_tracing.Memory_tracing.Tracked.List.is_empty state.open_sell_orders then
                "Sell:none"
              else
                let closest_sell = List.fold_left (fun acc (_, sell_price) ->
                  match acc with
                  | None -> Some sell_price
                  | Some current_closest ->
                      if abs_float (sell_price -. price) < abs_float (current_closest -. price)
                      then Some sell_price
                      else Some current_closest
                ) None (Dio_memory_tracing.Memory_tracing.Tracked.List.to_list state.open_sell_orders) in
                match closest_sell with
                | Some sell_price ->
                    let sell_distance_pct = ((sell_price -. price) /. price) *. 100.0 in
                    Printf.sprintf "Sell@%.2f(%.2f%c,%d)" sell_price sell_distance_pct '%' (Dio_memory_tracing.Memory_tracing.Tracked.List.length state.open_sell_orders)
                | None -> "Sell:none"
            in
            Printf.sprintf " | Strategy: %s %s" buy_info sell_info
  in

  (* Track ring buffer positions for this domain *)
  let exec_read_pos = ref 0 in
  let ticker_read_pos = ref 0 in
  let orderbook_read_pos = ref 0 in

  (* Track latest market data from consumed events *)
  let current_price = ref None in
  let top_of_book = ref None in

  (* Track when to execute strategies (event-driven) *)
  let should_execute_strategy = ref true in  (* Start with true to execute on first cycle *)
  let last_balance_check = ref 0.0 in

  (* Initialize strategy for this asset based on strategy type *)
  let (grid_strategy_asset, mm_strategy_asset) =
    if asset_with_fees.strategy = "suicide_grid" || asset_with_fees.strategy = "Grid" then
      (Some {
        Dio_strategies.Suicide_grid.exchange = asset_with_fees.exchange;
        symbol = asset_with_fees.symbol;
        qty = asset_with_fees.qty;
        grid_interval = asset_with_fees.grid_interval;
        sell_mult = asset_with_fees.sell_mult;
        strategy = asset_with_fees.strategy;
        maker_fee = asset_with_fees.maker_fee;
        taker_fee = asset_with_fees.taker_fee;
      }, None)
    else if asset_with_fees.strategy = "MM" then
      (None, Some {
        Dio_strategies.Market_maker.exchange = asset_with_fees.exchange;
        symbol = asset_with_fees.symbol;
        qty = asset_with_fees.qty;
        min_usd_balance = asset_with_fees.min_usd_balance;
        max_exposure = asset_with_fees.max_exposure;
        strategy = asset_with_fees.strategy;
        maker_fee = asset_with_fees.maker_fee;
        taker_fee = asset_with_fees.taker_fee;
      })
    else
      (None, None)
  in
  
  (* Set exec position to current to skip any snapshot events that occurred before this domain started *)
  if asset_with_fees.exchange = "kraken" then begin
    exec_read_pos := Kraken.Kraken_executions_feed.get_current_position asset_with_fees.symbol;
    Logging.debug_f ~section "Domain for %s/%s starting consumption from exec position %d"
      asset_with_fees.exchange asset_with_fees.symbol !exec_read_pos
  end;
  
  (* Initialize ticker and orderbook positions to 0 to catch all data from start *)
  (* Unlike executions, ticker/orderbook don't have pre-existing state to skip *)
  ticker_read_pos := 0;
  orderbook_read_pos := 0;
  
  Logging.info_f ~section "Domain initialized for asset: %s/%s (Strategy: %s)"
    asset_with_fees.exchange asset_with_fees.symbol asset_with_fees.strategy;

  (* Signal that this domain is ready for operation *)
  (* This helps prevent race conditions during startup by ensuring domains are fully initialized before others start *)
  Atomic.set state.is_initialized true;
  (* Note: Using synchronous signaling since this is called from within the domain thread *)
  ();

  let cycle_count = ref 0 in
  let telemetry_batch = ref 0 in
  let cycle_sample_counter = ref 0 in
  while Atomic.get state.is_running do
    let cycle_start = Telemetry.start_timer_v2 () in
    incr cycle_count;
    incr telemetry_batch;

    (* For testing: exit quickly if shutdown requested *)
    if Atomic.get shutdown_requested then raise Exit;
    
    (* Minimal logging in hot loop *)
    if !cycle_count mod 1000000 = 0 then
      Logging.debug_f ~section "Asset [%s/%s] cycle #%d"
        asset_with_fees.exchange asset_with_fees.symbol !cycle_count;
    
    (* Consume ticker events from ring buffer - lock-free, wait-free reads *)
    if asset_with_fees.exchange = "kraken" then begin
      let ticker_pos = Kraken.Kraken_ticker_feed.get_current_position asset_with_fees.symbol in
      (* Always consume if position changed OR if we haven't consumed yet (both at 0) *)
      if ticker_pos <> !ticker_read_pos || (!ticker_read_pos = 0 && ticker_pos > 0) then begin
        let new_tickers = Kraken.Kraken_ticker_feed.read_ticker_events asset_with_fees.symbol !ticker_read_pos in
        ticker_read_pos := ticker_pos;
        
        (* Process ticker events - keep latest price *)
        List.iter (fun (ticker : Kraken.Kraken_ticker_feed.ticker) ->
          current_price := Some ((ticker.bid +. ticker.ask) /. 2.0);
          (* Trigger strategy execution on price updates *)
          should_execute_strategy := true;
          Logging.debug_f ~section "Asset [%s/%s]: Consumed ticker event - price=$%.2f"
            asset_with_fees.exchange asset_with_fees.symbol ((ticker.bid +. ticker.ask) /. 2.0)
        ) new_tickers
      end
    end;
    
    (* Consume orderbook events from ring buffer - lock-free, wait-free reads *)
    if asset_with_fees.exchange = "kraken" then begin
      let ob_pos = Kraken.Kraken_orderbook_feed.get_current_position asset_with_fees.symbol in
      (* Always consume if position changed OR if we haven't consumed yet (both at 0) *)
      if ob_pos <> !orderbook_read_pos || (!orderbook_read_pos = 0 && ob_pos > 0) then begin
        let new_orderbooks = Kraken.Kraken_orderbook_feed.read_orderbook_events asset_with_fees.symbol !orderbook_read_pos in
        orderbook_read_pos := ob_pos;
        
        (* Process orderbook events - keep latest best bid/ask *)
        List.iter (fun (ob : Kraken.Kraken_orderbook_feed.orderbook) ->
          if Array.length ob.bids > 0 && Array.length ob.asks > 0 then begin
            let bid = ob.bids.(0) in
            let ask = ob.asks.(0) in
            (try
              let bid_price, bid_size = float_of_string bid.price, float_of_string bid.size in
              let ask_price, ask_size = float_of_string ask.price, float_of_string ask.size in
              top_of_book := Some (bid_price, bid_size, ask_price, ask_size);
              (* Trigger strategy execution on orderbook updates *)
              should_execute_strategy := true;
              Logging.debug_f ~section "Asset [%s/%s]: Consumed orderbook event - bid=$%.2f x %.4f, ask=$%.2f x %.4f"
                asset_with_fees.exchange asset_with_fees.symbol
                bid_price bid_size ask_price ask_size
            with _ ->
              Logging.warn_f ~section "Failed to parse orderbook level for %s" asset_with_fees.symbol)
          end
        ) new_orderbooks
      end
    end;
    
    (* Get balances for asset and quote currency *)
    let (base_asset, quote_currency) =
      if String.contains asset_with_fees.symbol '/' then
        let parts = String.split_on_char '/' asset_with_fees.symbol in
        (List.nth parts 0, List.nth parts 1)
      else
        (asset_with_fees.symbol, "USD")
    in
    
    (* CRITICAL: Consume execution events FIRST, before reading order counts!
     * This ensures cancellations and fills are reflected in the counts.
     * The actual state management of open orders is handled in the executions_feed module.
     * Here, we process events and notify strategies about external cancellations. *)
    if asset_with_fees.exchange = "kraken" then begin
      let current_pos = Kraken.Kraken_executions_feed.get_current_position asset_with_fees.symbol in
      if current_pos <> !exec_read_pos then begin
        let new_events = Kraken.Kraken_executions_feed.read_execution_events asset_with_fees.symbol !exec_read_pos in
        let event_count = List.length new_events in
        if event_count > 0 then begin
          Telemetry.inc_counter execution_events_consumed_counter ~value:event_count ();
          
          (* Process events to detect new orders and cancellations, notify strategies *)
          List.iter (fun (event : Kraken.Kraken_executions_feed.execution_event) ->
            match event.order_status with
            | CanceledStatus ->
                (* Trigger strategy execution on order cancellations *)
                should_execute_strategy := true;
                (* Notify strategies about external order cancellations *)
                (match grid_strategy_asset with
                 | Some _ ->
                     Dio_strategies.Suicide_grid.Strategy.handle_order_cancelled
                       asset_with_fees.symbol event.order_id;
                     Logging.debug_f ~section "Notified Grid strategy about cancelled order %s for %s"
                       event.order_id asset_with_fees.symbol
                 | None -> ());
                (match mm_strategy_asset with
                 | Some _ ->
                     Dio_strategies.Market_maker.Strategy.handle_order_cancelled
                       asset_with_fees.symbol event.order_id;
                     Logging.debug_f ~section "Notified MM strategy about cancelled order %s for %s"
                       event.order_id asset_with_fees.symbol
                 | None -> ())
            | NewStatus | PartiallyFilledStatus ->
                (* Trigger strategy execution on order fills/acknowledgments *)
                should_execute_strategy := true;
                (* Notify strategies about new or updated orders to clear pending state *)
                (match event.limit_price with
                 | Some price ->
                     let side = match event.side with
                       | Kraken.Kraken_executions_feed.Buy -> Dio_strategies.Strategy_common.Buy
                       | Kraken.Kraken_executions_feed.Sell -> Dio_strategies.Strategy_common.Sell
                     in
                     (match grid_strategy_asset with
                      | Some _ ->
                          Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
                            asset_with_fees.symbol event.order_id side price;
                          Logging.debug_f ~section "Notified Grid strategy about acknowledged order %s for %s"
                            event.order_id asset_with_fees.symbol
                      | None -> ());
                     (match mm_strategy_asset with
                      | Some _ ->
                          Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
                            asset_with_fees.symbol event.order_id side price;
                          Logging.debug_f ~section "Notified MM strategy about acknowledged order %s for %s"
                            event.order_id asset_with_fees.symbol
                      | None -> ())
                 | None -> ())
            | _ -> ()  (* Ignore other status events *)
          ) new_events
        end;
        exec_read_pos := current_pos;
      end
    end;

    (* Get open orders for strategy sync - include side and cl_ord_id for strategy filtering *)
    (* IMPORTANT: Get orders first, then calculate counts from the SAME snapshot to avoid race conditions *)
    let all_open_orders =
      if asset_with_fees.exchange = "kraken" then begin
        let orders = Kraken.Kraken_executions_feed.get_open_orders asset_with_fees.symbol in
        List.filter_map (fun (order : Kraken.Kraken_executions_feed.open_order) ->
          match order.limit_price with
          | Some price -> 
              let side_str = match order.side with
                | Kraken.Kraken_executions_feed.Buy -> "buy"
                | Kraken.Kraken_executions_feed.Sell -> "sell"
              in
              Some (order.order_id, price, order.remaining_qty, side_str, order.order_userref)
          | None -> None
        ) orders
      end else
        []
    in

    (* Filter orders by strategy based on userref *)
    let filter_orders_by_strategy strategy_name strategy_userref =
      (* Always log for first few cycles to debug filtering *)
      let should_log = !cycle_count < 10 || !cycle_count mod 100000 = 0 in
      
      if should_log && List.length all_open_orders > 0 then
        Logging.debug_f ~section "Filtering %d total orders on %s for %s strategy (userref=%d)"
          (List.length all_open_orders) asset_with_fees.symbol strategy_name strategy_userref;
      
      let filtered = List.filter (fun (order_id, _, _, _, userref_opt) ->
        match userref_opt with
        | Some userref -> 
            let matches = Dio_strategies.Strategy_common.is_strategy_order strategy_userref userref in
            if should_log then
              Logging.debug_f ~section "Order %s: userref=%d, matches=%B"
                order_id userref matches;
            matches
        | None -> 
            if should_log then
              Logging.debug_f ~section "Order %s has no userref, ignoring for strategy filtering" order_id;
            false  (* Orders without userref are ignored - likely manual orders *)
      ) all_open_orders in
      
      if should_log then
        Logging.debug_f ~section "Filtered %d orders for %s strategy from %d total orders on %s"
          (List.length filtered) strategy_name (List.length all_open_orders) asset_with_fees.symbol;
      filtered
    in

    (* Get strategy-specific open orders *)

    (* TODO: currently passing all open sell orders since most don't have tags. fresh run would use this instead of all_open_orders, will revert
    once all orders cycle if I remember  *)
    (* let grid_open_orders = filter_orders_by_strategy "Grid" Dio_strategies.Strategy_common.strategy_userref_grid in *)
    let mm_open_orders = filter_orders_by_strategy "MM" Dio_strategies.Strategy_common.strategy_userref_mm in

    (* Calculate buy/sell counts from grid strategy orders *)
    (* For Grid strategy: only count grid-tagged BUY orders, but count ALL sell orders for positioning *)
    let (grid_open_buy_count, grid_open_sell_count) =
      List.fold_left (fun (buys, sells) (_, _, _, side_str, userref_opt) ->
        if side_str = "buy" then
          (* Only count buy orders with grid tag *)
          (match userref_opt with
           | Some userref when Dio_strategies.Strategy_common.is_strategy_order Dio_strategies.Strategy_common.strategy_userref_grid userref -> (buys + 1, sells)
           | _ -> (buys, sells))
        else
          (* Count ALL sell orders regardless of tag *)
          (buys, sells + 1)
      ) (0, 0) all_open_orders
    in

    (* Calculate buy/sell counts from MM strategy orders *)
    let (mm_open_buy_count, mm_open_sell_count) =
      List.fold_left (fun (buys, sells) (_, _, _, side_str, _) ->
        if side_str = "buy" then (buys + 1, sells) else (buys, sells + 1)
      ) (0, 0) mm_open_orders
    in

    (* Lock-free balance queries *)
    let asset_balance =
      if asset_with_fees.exchange = "kraken" then
        try Some (Kraken.Kraken_balances_feed.get_balance base_asset)
        with _ -> None
      else None
    in
    let quote_balance =
      if asset_with_fees.exchange = "kraken" then
        try Some (Kraken.Kraken_balances_feed.get_balance quote_currency)
        with _ -> None
      else None
    in

    (* Check for balance changes every 10 cycles (~2 seconds at 200k/sec) *)
    let now = Unix.time () in
    if now -. !last_balance_check > 2.0 then begin
      last_balance_check := now;
      (* Balance changes would trigger strategy execution, but we don't have old balances to compare *)
      (* For now, we'll trigger periodically. Could be enhanced to track balance changes *)
    end;

    (* Execute strategy based on type - only when triggered by events (event-driven) *)
    (* Add fallback: execute every 10000 cycles (~2 seconds at 200k/sec) to prevent getting stuck *)
    let should_execute = !should_execute_strategy || (!cycle_count mod 10000 = 0) in
    if should_execute then begin
      should_execute_strategy := false;  (* Reset flag *)

      (match grid_strategy_asset with
       | Some asset ->
           (try
             (* Pass ALL open orders to Grid strategy so it can see all sell orders for positioning *)
             Dio_strategies.Suicide_grid.Strategy.execute asset !current_price !top_of_book asset_balance quote_balance grid_open_buy_count grid_open_sell_count all_open_orders !cycle_count
           with exn ->
             Logging.error_f ~section "Exception in Grid strategy execution for %s: %s"
               asset_with_fees.symbol (Printexc.to_string exn))
       | None -> ());
      (match mm_strategy_asset with
       | Some asset ->
           (try
             Dio_strategies.Market_maker.Strategy.execute asset !current_price !top_of_book asset_balance quote_balance mm_open_buy_count mm_open_sell_count mm_open_orders !cycle_count
           with exn ->
             Logging.error_f ~section "Exception in MM strategy execution for %s: %s"
               asset_with_fees.symbol (Printexc.to_string exn))
       | None -> ());
    end;
    
    (* Calculate total order counts across all strategies for logging *)
    let total_open_buy_count = grid_open_buy_count + mm_open_buy_count in
    let total_open_sell_count = grid_open_sell_count + mm_open_sell_count in
    
    (* Simulate some trading logic variations *)
    let cycle = !cycle_count in
    (* Only log status every 1000000 cycles at HFT speeds *)
    if cycle mod 1000000 = 0 then begin
      (match !current_price, !top_of_book, asset_balance, quote_balance with
      | Some price, Some (bid_price, _bid_size, ask_price, _ask_size), Some asset_bal, Some quote_bal ->
          Logging.info_f ~section "[%s/%s] C#%d - $%.2f | bid=$%.8f | ask=$%.8f | %s: %.8f | %s: %.2f | %d buy / %d sell%s"
            asset_with_fees.exchange asset_with_fees.symbol cycle price
            bid_price ask_price base_asset asset_bal quote_currency quote_bal
            total_open_buy_count total_open_sell_count (format_distance_info asset_with_fees.symbol !current_price asset_with_fees.strategy)
      | Some price, Some (bid_price, _bid_size, ask_price, _ask_size), _, _ ->
          Logging.info_f ~section "[%s/%s] C#%d - $%.2f | bid=$%.8f | ask=$%.8f | %d buy / %d sell%s"
            asset_with_fees.exchange asset_with_fees.symbol cycle price
            bid_price ask_price total_open_buy_count total_open_sell_count
            (format_distance_info asset_with_fees.symbol !current_price asset_with_fees.strategy)
      | Some price, None, _, _ ->
          Logging.info_f ~section "[%s/%s] C#%d - $%.2f (orderbook loading...) | %d buy / %d sell%s"
            asset_with_fees.exchange asset_with_fees.symbol cycle price total_open_buy_count total_open_sell_count
            (format_distance_info asset_with_fees.symbol !current_price asset_with_fees.strategy)
      | None, _, _, _ ->
          Logging.info_f ~section "[%s/%s] C#%d - no price/orderbook data yet | %d buy / %d sell%s"
            asset_with_fees.exchange asset_with_fees.symbol cycle total_open_buy_count total_open_sell_count
            (format_distance_info asset_with_fees.symbol !current_price asset_with_fees.strategy))
    end;
    (match asset_with_fees.maker_fee, asset_with_fees.taker_fee with
     | Some m, Some t -> Logging.debug_f ~section "Asset [%s/%s]: Cached fees maker=%.4f%% taker=%.4f%%" 
         asset_with_fees.exchange asset_with_fees.symbol (m *. 100.) (t *. 100.)
     | Some m, None -> Logging.debug_f ~section "Asset [%s/%s]: Cached maker fee %.4f%% (taker unknown)" 
         asset_with_fees.exchange asset_with_fees.symbol (m *. 100.)
     | None, Some t -> Logging.debug_f ~section "Asset [%s/%s]: Cached taker fee %.4f%% (maker unknown)" 
         asset_with_fees.exchange asset_with_fees.symbol (t *. 100.)
     | None, None -> ());

    (* Record individual cycle duration for accurate timing analysis *)
    (* Sample only 1 in 100 cycles to manage memory at 200k/sec rate *)
    cycle_sample_counter := (!cycle_sample_counter + 1) mod 100;
    if !cycle_sample_counter = 0 then
      Telemetry.record_duration_v2 cycle_duration_hist cycle_start |> ignore;

    (* Batch telemetry updates every 1000 cycles to minimize overhead *)
    if !telemetry_batch >= 1000 then begin
      let (open_buy_count, open_sell_count) =
        if asset_with_fees.exchange = "kraken" then
          Kraken.Kraken_executions_feed.count_open_orders_by_side asset_with_fees.symbol
        else
          (0, 0)
      in
      Telemetry.inc_sliding_counter cycles_counter ~value:1000 ();
      Telemetry.set_gauge open_buy_orders_gauge (float_of_int open_buy_count);
      Telemetry.set_gauge open_sell_orders_gauge (float_of_int open_sell_count);
      telemetry_batch := 0
    end;

    (* Yield to allow other threads (websockets) to run *)
    Domain.cpu_relax ();

    (* Debug: Log completion of cycle for USDG to help diagnose hangs *)
    if asset_with_fees.symbol = "USDG/USD" && !cycle_count mod 100 = 0 then
      Logging.debug_f ~section "Domain %s completed cycle %d" asset_with_fees.symbol !cycle_count;

    ()
  done

(** Register a domain for supervision *)
let register_domain asset =
  let key = domain_key asset in
  let state = {
    asset;
    domain_handle = Atomic.make None;
    last_restart = Atomic.make (Unix.time ());
    restart_count = Atomic.make 0;
    is_running = Atomic.make false;
    is_initialized = Atomic.make false;
    mutex = Mutex.create ();
  } in
  (* Lock registry for entire operation to prevent race conditions *)
  Mutex.lock registry_mutex;
  let already_exists = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.mem domain_registry key in
  if not already_exists then
    Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.replace domain_registry key state;
  Mutex.unlock registry_mutex;

  Logging.debug_f ~section "Registered domain for supervision: %s" key;
  state

(** Start a supervised domain *)
let start_domain state fee_fetcher =
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

    let domain_handle = Domain.spawn (fun () ->
      Logging.info_f ~section "Domain for %s/%s started (restart #%d)"
        asset.exchange asset.symbol (Atomic.get state.restart_count);

      (* Exception handling for robustness *)
      try
        asset_domain_worker state fee_fetcher asset;
        Logging.info_f ~section "Domain for %s/%s completed normally" asset.exchange asset.symbol
      with exn ->
        match exn with
        | Exit -> 
            (* Clean shutdown requested *)
            Logging.debug_f ~section "Domain for %s/%s shutting down cleanly" asset.exchange asset.symbol
        | _ ->
            Logging.critical_f ~section "Domain for %s/%s crashed: %s"
              asset.exchange asset.symbol (Printexc.to_string exn);

        (* Mark domain as stopped and allow restart *)
        Atomic.set state.is_running false;
        Atomic.set state.domain_handle None;

        (* Don't re-raise - domain should be restarted by supervisor *)
        ()
    ) in

    Atomic.set state.domain_handle (Some domain_handle);
    Mutex.unlock state.mutex;
    Logging.info_f ~section "Domain %s started successfully" key;
    true
  )

(** Stop a domain *)
let stop_domain state =
  let key = domain_key state.asset in
  Mutex.lock state.mutex;
  Atomic.set state.is_running false;
  (match Atomic.get state.domain_handle with
   | Some _handle ->
       Logging.info_f ~section "Stopping domain %s..." key;
       (* Note: OCaml doesn't provide a way to forcibly stop domains *)
       (* The domain will stop when it detects is_running = false *)
       (* For testing, we need to ensure domains exit quickly *)
       Atomic.set state.domain_handle None
   | None -> ());
  Mutex.unlock state.mutex

(** Check if domain needs restart *)
let domain_needs_restart state =
  Mutex.lock state.mutex;
  (* Don't restart if shutdown has been requested, even if domain is not running *)
  let needs_restart = not (Atomic.get state.is_running) && not (Atomic.get shutdown_requested) in
  Mutex.unlock state.mutex;
  needs_restart

(** Domain supervisor monitoring loop *)
let supervisor_loop fee_fetcher =
  let section = "domain_supervisor" in
  Logging.info ~section "Domain supervisor started";

  while not (Atomic.get shutdown_requested) do
    try
      (* Use shorter delay for testing to allow quick shutdown *)
      let delay = if Sys.getenv_opt "DIO_TESTING" = Some "1" then 0.1 else 5.0 in
      Thread.delay delay;

      (* Check for shutdown again after the delay *)
      if Atomic.get shutdown_requested then raise Exit;

      Mutex.lock registry_mutex;
      let domains = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.to_seq_values domain_registry |> List.of_seq in
      Mutex.unlock registry_mutex;

      List.iter (fun state ->
        (* Check for shutdown during iteration to exit immediately *)
        if Atomic.get shutdown_requested then raise Exit;

        if domain_needs_restart state then (
          let key = domain_key state.asset in
          let last_restart = Atomic.get state.last_restart in
          let restart_count = Atomic.get state.restart_count in
          let time_since_restart = Unix.time () -. last_restart in

          (* Implement exponential backoff: 1s, 2s, 4s, 8s, max 30s *)
          let backoff_delay = min 30.0 (2.0 ** float_of_int (restart_count - 1)) in

          if time_since_restart >= backoff_delay then (
            Logging.warn_f ~section "Restarting crashed domain %s (attempt #%d, backoff %.1fs)"
              key restart_count backoff_delay;
            ignore (start_domain state fee_fetcher)
          )
        )
      ) domains

    with exn ->
      match exn with
      | Exit -> ()  (* Exit cleanly on shutdown *)
      | _ -> Logging.error_f ~section "Exception in domain supervisor: %s" (Printexc.to_string exn)
  done

(** Spawn supervised domains for assets *)
let spawn_supervised_domains_for_assets (fee_fetcher : trading_config -> trading_config) (assets : trading_config list) : unit Lwt.t =
  Logging.debug_f ~section "Spawning supervised domains for %d assets..." (List.length assets);

  (* Initialize strategy modules *)
  Dio_strategies.Suicide_grid.Strategy.init ();
  Dio_strategies.Market_maker.Strategy.init ();

  (* Register all domains *)
  List.iter (fun asset ->
    ignore (register_domain asset)
  ) assets;

  (* Start initial domains with coordination to prevent startup race conditions *)
  Mutex.lock registry_mutex;
  let all_states = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.to_seq_values domain_registry |> List.of_seq in
  Mutex.unlock registry_mutex;

  (* Start domains sequentially and wait for each to initialize to prevent startup race conditions *)
  let%lwt () = Lwt_list.iter_s (fun state ->
    let started = start_domain state fee_fetcher in
    if started then begin
      (* Wait for domain to signal that it's fully initialized by polling the atomic flag *)
      let rec wait_for_init () =
        if Atomic.get state.is_initialized then
          Lwt.return_unit
        else begin
          let%lwt () = Lwt_unix.sleep 0.001 in  (* Small delay to avoid busy waiting *)
          wait_for_init ()
        end
      in
      wait_for_init ()
    end else begin
      Lwt.return_unit
    end
  ) all_states in

  (* Start supervisor thread *)
  ignore (Thread.create supervisor_loop fee_fetcher);
  Logging.info ~section "Domain supervisor thread started";

  (* Signal that all domains have been initialized and are ready *)
  signal_phase_complete DomainsInit;

  Lwt.return_unit

(** Get domain status for monitoring *)
let get_domain_status () =
  Mutex.lock registry_mutex;
  let status = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.fold (fun key state acc ->
    let running = Atomic.get state.is_running in
    let restart_count = Atomic.get state.restart_count in
    let last_restart = Atomic.get state.last_restart in
    (key, (running, restart_count, last_restart)) :: acc
  ) domain_registry [] in
  Mutex.unlock registry_mutex;
  status

(** Clear domain registry (for testing) *)
let clear_domain_registry () =
  Mutex.lock registry_mutex;
  Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.clear domain_registry;
  Mutex.unlock registry_mutex

(** Get the number of domains currently registered *)
let get_domain_registry_count () =
  Mutex.lock registry_mutex;
  let count = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.length domain_registry in
  Mutex.unlock registry_mutex;
  count

(** Signal all domains to shutdown (synchronous) *)
let signal_domain_shutdown () =
  Logging.info ~section "Signaling all supervised domains to shutdown...";
  (* Signal supervisor to stop restarting domains *)
  Atomic.set shutdown_requested true;
  Mutex.lock registry_mutex;
  let all_states = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.to_seq_values domain_registry |> List.of_seq in
  Mutex.unlock registry_mutex;

  List.iter stop_domain all_states

(** Stop all domains gracefully *)
let stop_all_domains () : unit Lwt.t =
  Logging.info ~section "Stopping all supervised domains...";
  (* Signal shutdown to all domains *)
  signal_domain_shutdown ();

  (* Get the list of states to monitor for shutdown *)
  Mutex.lock registry_mutex;
  let all_states = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.to_seq_values domain_registry |> List.of_seq in
  Mutex.unlock registry_mutex;

  (* Wait for domains to stop asynchronously *)
  let rec wait_for_stop max_wait =
    if max_wait <= 0.0 then
      Lwt.return (Logging.warn ~section "Timeout waiting for domains to stop")
    else
      let all_stopped = List.for_all (fun state -> not (Atomic.get state.is_running)) all_states in
      if all_stopped then
        Lwt.return (Logging.info ~section "All domains stopped successfully")
      else (
        let%lwt () = Lwt_unix.sleep 0.1 in
        wait_for_stop (max_wait -. 0.1)
      )
  in
  wait_for_stop 10.0

(** Legacy function for backward compatibility - returns empty list since domains are now supervised *)
let spawn_domains_for_assets (fee_fetcher : trading_config -> trading_config) (assets : trading_config list) : unit Domain.t list =
  Logging.warn ~section "spawn_domains_for_assets is deprecated, use spawn_supervised_domains_for_assets instead";
  ignore (spawn_supervised_domains_for_assets fee_fetcher assets);

  (* Return the list of domain handles for compatibility *)
  Mutex.lock registry_mutex;
  let domains = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.to_seq_values domain_registry |> List.of_seq
                |> List.filter_map (fun state -> Atomic.get state.domain_handle) in
  Mutex.unlock registry_mutex;
  domains

(* Entrypoint to spawn config domains; call in main program as appropriate *)
let spawn_config_domains (fee_fetcher : trading_config -> trading_config) () : unit Domain.t list =
  let configs = (read_config ()).trading in
  Logging.debug_f ~section "Preparing to spawn domains for %d assets..." (List.length configs);
  spawn_domains_for_assets fee_fetcher configs

