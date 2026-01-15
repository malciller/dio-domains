open Config
module Fear_and_greed = Cmc.Fear_and_greed

(* Open Generic Exchange Interface *)
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

let section = "domain_spawner"

(** Get domain key for registry *)
let domain_key asset = Printf.sprintf "%s/%s" asset.exchange asset.symbol

(** Domain supervisor state *)
type domain_state = {
  asset: trading_config;
  domain_handle: unit Domain.t option Atomic.t;
  last_restart: float Atomic.t;
  restart_count: int Atomic.t;
  is_running: bool Atomic.t;
  mutex: Mutex.t;
}

(** Global domain registry *)
let domain_registry : (string, domain_state) Hashtbl.t = Hashtbl.create 32
let registry_mutex = Mutex.create ()

(** Global shutdown flag for domain supervisor *)
let shutdown_requested = Atomic.make false


(** The worker function executed by each domain for a trading asset *)
let asset_domain_worker (fee_fetcher : trading_config -> trading_config) (asset : trading_config) =
  Random.self_init ();  (* Initialize random state for this domain *)
  
  (* Fetch fees at domain startup using the provided fetcher *)
  let asset_with_fees = fee_fetcher asset in

  (* Resolve grid interval once using Fear & Greed (cached) *)
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
             let sell_info =
               if state.open_sell_orders = [] then
                 "Sell:none"
               else
                 let best_price = match current_price with
                   | Some price ->
                       List.fold_left (fun acc (_, sell_price) ->
                         match acc with
                         | None -> Some sell_price
                         | Some best -> if abs_float (sell_price -. price) < abs_float (best -. price) then Some sell_price else Some best
                       ) None state.open_sell_orders
                   | None -> 
                       match state.open_sell_orders with (_, p)::_ -> Some p | [] -> None
                 in
                 match best_price with
                 | Some p -> Printf.sprintf "Sell@%.2f" p
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
              if state.open_sell_orders = [] then
                "Sell:none"
              else
                let closest_sell = List.fold_left (fun acc (_, sell_price) ->
                  match acc with
                  | None -> Some sell_price
                  | Some current_closest ->
                      if abs_float (sell_price -. price) < abs_float (current_closest -. price)
                      then Some sell_price
                      else Some current_closest
                ) None state.open_sell_orders in
                match closest_sell with
                | Some sell_price ->
                    let sell_distance_pct = ((sell_price -. price) /. price) *. 100.0 in
                    Printf.sprintf "Sell@%.2f(%.2f%c,%d)" sell_price sell_distance_pct '%' (List.length state.open_sell_orders)
                | None -> "Sell:none"
            in
            Printf.sprintf " | Strategy: %s %s" buy_info sell_info
  in

  (* Look up exchange implementation *)
  Logging.info_f ~section "Looking up exchange %s" asset_with_fees.exchange;
  match Exchange.Registry.get asset_with_fees.exchange with
  | None ->
      Logging.error_f ~section "Unknown exchange '%s' for asset %s, aborting domain"
        asset_with_fees.exchange asset_with_fees.symbol
  | Some (module Ex) ->
      Logging.info_f ~section "Found exchange module for %s" asset_with_fees.exchange;

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
          let grid_interval =
            match resolved_grid_interval with
            | Some g -> g
            | None ->
                let (lo, hi) = asset_with_fees.grid_interval in
                (lo +. hi) /. 2.0
          in
          (Some {
            Dio_strategies.Suicide_grid.exchange = asset_with_fees.exchange;
            symbol = asset_with_fees.symbol;
            qty = asset_with_fees.qty;
            grid_interval;
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
      Logging.info_f ~section "About to get execution feed position for %s" asset_with_fees.symbol;
      begin try
        exec_read_pos := Ex.get_execution_feed_position ~symbol:asset_with_fees.symbol;
        Logging.info_f ~section "Got execution feed position %d" !exec_read_pos;
        Logging.debug_f ~section "Domain for %s/%s starting consumption from exec position %d"
          asset_with_fees.exchange asset_with_fees.symbol !exec_read_pos
      with exn ->
        Logging.info_f ~section "Caught exception in get_execution_feed_position: %s" (Printexc.to_string exn);
        Logging.error_f ~section "Failed to get execution position for %s/%s: %s (starting from 0)"
          asset_with_fees.exchange asset_with_fees.symbol (Printexc.to_string exn);
        exec_read_pos := 0
      end;
      
      (* Initialize ticker and orderbook positions to 0 - but better to get current to avoid old data *)
      (* existing logic initialized to 0. We'll use get_xxx_position call if not 0? *)
      (* Original code: ticker_read_pos := 0; orderbook_read_pos := 0; *)
      ticker_read_pos := 0;
      orderbook_read_pos := 0;
      
      Logging.info_f ~section "Domain initialized for asset: %s/%s (Strategy: %s)"
        asset_with_fees.exchange asset_with_fees.symbol asset_with_fees.strategy;


      let key = domain_key asset_with_fees in
      let state = Hashtbl.find domain_registry key in
      
      Logging.info_f ~section "Entering domain loop for %s. is_running=%B" key (Atomic.get state.is_running);

      let cycle_count = ref 0 in
      while Atomic.get state.is_running do
        if !cycle_count = 0 then Logging.info_f ~section "First cycle for %s" key;
        incr cycle_count;
        
        (* Minimal logging in hot loop *)
        if !cycle_count mod 1000000 = 0 then
          Logging.debug_f ~section "Asset [%s/%s] cycle #%d"
            asset_with_fees.exchange asset_with_fees.symbol !cycle_count;
        
        (* Consume ticker events *)
        let ticker_pos = Ex.get_ticker_position ~symbol:asset_with_fees.symbol in
        if ticker_pos <> !ticker_read_pos || (!ticker_read_pos = 0 && ticker_pos > 0) then begin
          let new_tickers = Ex.read_ticker_events ~symbol:asset_with_fees.symbol ~start_pos:!ticker_read_pos in
          ticker_read_pos := ticker_pos;
          
          List.iter (fun (ticker : Types.ticker_event) ->
            current_price := Some ((ticker.bid +. ticker.ask) /. 2.0);
            should_execute_strategy := true;
            Logging.debug_f ~section "Asset [%s/%s]: Consumed ticker event - price=$%.2f"
              asset_with_fees.exchange asset_with_fees.symbol ((ticker.bid +. ticker.ask) /. 2.0)
          ) new_tickers
        end;
        
        (* Consume orderbook events *)
        let ob_pos = Ex.get_orderbook_position ~symbol:asset_with_fees.symbol in
        if ob_pos <> !orderbook_read_pos || (!orderbook_read_pos = 0 && ob_pos > 0) then begin
          let new_orderbooks = Ex.read_orderbook_events ~symbol:asset_with_fees.symbol ~start_pos:!orderbook_read_pos in
          orderbook_read_pos := ob_pos;
          
          List.iter (fun (ob : Types.orderbook_event) ->
            if Array.length ob.bids > 0 && Array.length ob.asks > 0 then begin
              let (bid_price, bid_size) = ob.bids.(0) in
              let (ask_price, ask_size) = ob.asks.(0) in
              
              top_of_book := Some (bid_price, bid_size, ask_price, ask_size);
              should_execute_strategy := true;
              Logging.debug_f ~section "Asset [%s/%s]: Consumed orderbook event - bid=$%.2f x %.4f, ask=$%.2f x %.4f"
                asset_with_fees.exchange asset_with_fees.symbol
                bid_price bid_size ask_price ask_size
            end
          ) new_orderbooks
        end;
        
        (* Get balances for asset and quote currency *)
        let (base_asset, quote_currency) =
          if String.contains asset_with_fees.symbol '/' then
            let parts = String.split_on_char '/' asset_with_fees.symbol in
            (List.nth parts 0, List.nth parts 1)
          else
            (asset_with_fees.symbol, "USD")
        in
        
        (* Consume execution events *)
        let current_pos = Ex.get_execution_feed_position ~symbol:asset_with_fees.symbol in
        if current_pos <> !exec_read_pos then begin
          let new_events = Ex.read_execution_events ~symbol:asset_with_fees.symbol ~start_pos:!exec_read_pos in
          let event_count = List.length new_events in
          if event_count > 0 then begin
            
            List.iter (fun (event : Types.execution_event) ->
              match event.order_status with
              | Types.Canceled | Types.Filled ->
                  should_execute_strategy := true;
                  let status_desc = match event.order_status with
                    | Types.Canceled -> "cancelled"
                    | Types.Filled -> "filled"
                    | _ -> "terminated"
                  in
                  (match grid_strategy_asset with
                   | Some _ ->
                       Dio_strategies.Suicide_grid.Strategy.handle_order_cancelled
                         asset_with_fees.symbol event.order_id;
                       Logging.debug_f ~section "Notified Grid strategy about %s order %s for %s"
                         status_desc event.order_id asset_with_fees.symbol
                   | None -> ());
                  (match mm_strategy_asset with
                   | Some _ ->
                       Dio_strategies.Market_maker.Strategy.handle_order_cancelled
                         asset_with_fees.symbol event.order_id;
                       Logging.debug_f ~section "Notified MM strategy about %s order %s for %s"
                         status_desc event.order_id asset_with_fees.symbol
                   | None -> ())
              | Types.New | Types.PartiallyFilled ->
                  should_execute_strategy := true;
                  (match event.limit_price with
                   | Some price ->
                       let side = match event.side with
                         | Types.Buy -> Dio_strategies.Strategy_common.Buy
                         | Types.Sell -> Dio_strategies.Strategy_common.Sell
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
              | _ -> ()
            ) new_events
          end;
          exec_read_pos := current_pos;
        end;

        (* Get open orders for strategy sync *)
        let orders = Ex.get_open_orders ~symbol:asset_with_fees.symbol in
        (* Map to internal format used by existing strategies:
           (order_id, price, remaining_qty, side_str, order_userref) 
        *)
        let all_open_orders = List.filter_map (fun (order : Types.open_order) ->
          match order.limit_price with
          | Some price -> 
              let side_str = match order.side with
                | Types.Buy -> "buy"
                | Types.Sell -> "sell"
              in
              Some (order.order_id, price, order.remaining_qty, side_str, order.user_ref)
          | None -> None
        ) orders in

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
        (* let grid_open_orders = filter_orders_by_strategy "Grid" Dio_strategies.Strategy_common.strategy_userref_grid in *)
        let mm_open_orders = filter_orders_by_strategy "MM" Dio_strategies.Strategy_common.strategy_userref_mm in

        (* Calculate buy/sell counts from grid strategy orders *)
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
          try Some (Ex.get_balance ~asset:base_asset)
          with _ -> None
        in
        let quote_balance =
          try Some (Ex.get_balance ~asset:quote_currency)
          with _ -> None
        in

        (* Check for balance changes every 10 cycles (~2 seconds at 200k/sec) *)
        let now = Unix.time () in
        if now -. !last_balance_check > 2.0 then begin
          last_balance_check := now;
        end;

        (* Execute strategy based on type - only when triggered by events (event-driven) *)
        (* Add fallback: execute every 10000 cycles (~2 seconds at 200k/sec) to prevent getting stuck *)
        let should_execute = !should_execute_strategy || (!cycle_count mod 10000 = 0) in
        if should_execute then begin
          should_execute_strategy := false;  (* Reset flag *)

          (match grid_strategy_asset with
           | Some asset ->
               (* Pass ALL open orders to Grid strategy so it can see all sell orders for positioning *)
               Dio_strategies.Suicide_grid.Strategy.execute asset !current_price !top_of_book asset_balance quote_balance grid_open_buy_count grid_open_sell_count all_open_orders !cycle_count
           | None -> ());
          (match mm_strategy_asset with
           | Some asset ->
               Dio_strategies.Market_maker.Strategy.execute asset !current_price !top_of_book asset_balance quote_balance mm_open_buy_count mm_open_sell_count mm_open_orders !cycle_count
           | None -> ());
        end;
        
        (* Calculate total order counts across all strategies for logging *)
        let total_open_buy_count = grid_open_buy_count + mm_open_buy_count in
        let total_open_sell_count = grid_open_sell_count + mm_open_sell_count in
        
        (* Log cycle stats *)
        let cycle = !cycle_count in
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



        (* Yield to allow other threads (websockets) to run *)
        Domain.cpu_relax ();
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
    mutex = Mutex.create ();
  } in
  Mutex.lock registry_mutex;
  Hashtbl.replace domain_registry key state;
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

    (* Join old domain synchronously before starting new one *)
    (match Atomic.get state.domain_handle with
     | Some old_handle ->
         Logging.debug_f ~section "Joining old domain %s before restart" key;
         (try Domain.join old_handle
          with exn -> Logging.warn_f ~section "Exception joining old domain %s: %s" 
                        key (Printexc.to_string exn))
     | None -> ());

    let domain_handle = Domain.spawn (fun () ->
      Logging.info_f ~section "Domain for %s/%s started (restart #%d)"
        asset.exchange asset.symbol (Atomic.get state.restart_count);

      (* Exception handling for robustness *)
      try
        asset_domain_worker fee_fetcher asset;
        Logging.info_f ~section "Domain for %s/%s completed normally" asset.exchange asset.symbol
      with exn ->
        Logging.critical_f ~section "Domain for %s/%s crashed (CAUGHT IN SPAWNER): %s"
          asset.exchange asset.symbol (Printexc.to_string exn);

        (* Mark domain as stopped and allow restart *)
        Atomic.set state.is_running false;
        (* Do NOT clear domain_handle here - we need it to join the domain later *)
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

  (* Clean up strategy state when domain stops *)
  let symbol = state.asset.symbol in
  (match state.asset.strategy with
   | "Grid" ->
       Dio_strategies.Suicide_grid.Strategy.cleanup_strategy_state symbol;
       Logging.debug_f ~section "Cleaned up Grid strategy state for %s" symbol
   | "MM" ->
       Dio_strategies.Market_maker.Strategy.cleanup_strategy_state symbol;
       Logging.debug_f ~section "Cleaned up MM strategy state for %s" symbol
   | _ ->
       Logging.debug_f ~section "No cleanup needed for strategy %s" state.asset.strategy);

  (match Atomic.get state.domain_handle with
   | Some handle ->
       Logging.info_f ~section "Stopping domain %s..." key;
       (* Join synchronously - domain will exit quickly when is_running=false *)
       (try Domain.join handle
        with exn -> Logging.warn_f ~section "Exception joining domain %s: %s"
                      key (Printexc.to_string exn));
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
      Thread.delay 5.0;  (* Check every 5 seconds *)

      (* Check for shutdown again after the delay *)
      if Atomic.get shutdown_requested then raise Exit;

      Mutex.lock registry_mutex;
      let domains = Hashtbl.to_seq_values domain_registry |> List.of_seq in
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
let spawn_supervised_domains_for_assets (fee_fetcher : trading_config -> trading_config) (assets : trading_config list) : Thread.t =
  Logging.debug_f ~section "Spawning supervised domains for %d assets..." (List.length assets);

  (* Initialize strategy modules *)
  Dio_strategies.Suicide_grid.Strategy.init ();
  Dio_strategies.Market_maker.Strategy.init ();

  (* Register all domains *)
  List.iter (fun asset ->
    ignore (register_domain asset)
  ) assets;

  (* Start initial domains *)
  Mutex.lock registry_mutex;
  let all_states = Hashtbl.to_seq_values domain_registry |> List.of_seq in
  Mutex.unlock registry_mutex;

  List.iter (fun state ->
    ignore (start_domain state fee_fetcher)
  ) all_states;



  (* Start supervisor thread *)
  let supervisor_thread = Thread.create supervisor_loop fee_fetcher in
  Logging.info ~section "Domain supervisor thread started";
  supervisor_thread

(** Get domain status for monitoring *)
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

(** Clear domain registry (for testing) *)
let clear_domain_registry () =
  Mutex.lock registry_mutex;
  Hashtbl.clear domain_registry;
  Mutex.unlock registry_mutex

(** Stop all domains gracefully *)
let stop_all_domains () =
  Logging.info ~section "Stopping all supervised domains...";
  (* Signal supervisor to stop restarting domains *)
  Atomic.set shutdown_requested true;
  Mutex.lock registry_mutex;
  let all_states = Hashtbl.to_seq_values domain_registry |> List.of_seq in
  Mutex.unlock registry_mutex;

  List.iter stop_domain all_states;

  (* Wait for domains to stop *)
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

(** Legacy function for backward compatibility - returns empty list since domains are now supervised *)
let spawn_domains_for_assets (fee_fetcher : trading_config -> trading_config) (assets : trading_config list) : unit Domain.t list =
  Logging.warn ~section "spawn_domains_for_assets is deprecated, use spawn_supervised_domains_for_assets instead";
  ignore (spawn_supervised_domains_for_assets fee_fetcher assets);

  (* Return the list of domain handles for compatibility *)
  Mutex.lock registry_mutex;
  let domains = Hashtbl.to_seq_values domain_registry |> List.of_seq
                |> List.filter_map (fun state -> Atomic.get state.domain_handle) in
  Mutex.unlock registry_mutex;
  domains

(* Entrypoint to spawn config domains; call in main program as appropriate *)
let spawn_config_domains (fee_fetcher : trading_config -> trading_config) () : unit Domain.t list =
  let configs = (read_config ()).trading in
  Logging.debug_f ~section "Preparing to spawn domains for %d assets..." (List.length configs);
  spawn_domains_for_assets fee_fetcher configs
