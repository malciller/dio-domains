open Config

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

(** The worker function executed by each domain for a trading asset *)
let asset_domain_worker (fee_fetcher : trading_config -> trading_config) (asset : trading_config) =
  Random.self_init ();  (* Initialize random state for this domain *)
  
  (* Fetch fees at domain startup using the provided fetcher *)
  let asset_with_fees = fee_fetcher asset in
  
  (* Create telemetry metrics for this asset *)
  let asset_label = asset_with_fees.exchange ^ "/" ^ asset_with_fees.symbol in
  let cycles_counter = Telemetry.asset_counter "domain_cycles" asset_label ~track_rate:true ~rate_window:10.0 () in
  let cycle_duration_hist = Telemetry.asset_histogram "domain_cycle_duration_seconds" asset_label in
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

  (* Track ring buffer positions for this domain *)
  let exec_read_pos = ref 0 in
  let ticker_read_pos = ref 0 in
  let orderbook_read_pos = ref 0 in

  (* Track latest market data from consumed events *)
  let current_price = ref None in
  let top_of_book = ref None in

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

  let key = domain_key asset_with_fees in
  let state = Hashtbl.find domain_registry key in

  let cycle_count = ref 0 in
  let telemetry_batch = ref 0 in
  while Atomic.get state.is_running do
    let cycle_start = Telemetry.start_timer () in
    incr cycle_count;
    incr telemetry_batch;
    
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

    (* Get open orders for strategy sync - include side to avoid price-based classification *)
    (* IMPORTANT: Get orders first, then calculate counts from the SAME snapshot to avoid race conditions *)
    let open_orders =
      if asset_with_fees.exchange = "kraken" then begin
        let orders = Kraken.Kraken_executions_feed.get_open_orders asset_with_fees.symbol in
        List.filter_map (fun (order : Kraken.Kraken_executions_feed.open_order) ->
          match order.limit_price with
          | Some price -> 
              let side_str = match order.side with
                | Kraken.Kraken_executions_feed.Buy -> "buy"
                | Kraken.Kraken_executions_feed.Sell -> "sell"
              in
              Some (order.order_id, price, order.remaining_qty, side_str)
          | None -> None
        ) orders
      end else
        []
    in

    (* Calculate buy/sell counts from the synchronized open_orders list to avoid race conditions *)
    let (open_buy_count, open_sell_count) =
      List.fold_left (fun (buys, sells) (_, _, _, side_str) ->
        if side_str = "buy" then (buys + 1, sells) else (buys, sells + 1)
      ) (0, 0) open_orders
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

    (* Execute strategy based on type *)
    (match grid_strategy_asset with
     | Some asset ->
         Dio_strategies.Suicide_grid.Strategy.execute asset !current_price !top_of_book asset_balance quote_balance open_buy_count open_sell_count open_orders !cycle_count
     | None -> ());
    (match mm_strategy_asset with
     | Some asset ->
         Dio_strategies.Market_maker.Strategy.execute asset !current_price !top_of_book asset_balance quote_balance open_buy_count open_sell_count open_orders !cycle_count
     | None -> ());
    
    (* Simulate some trading logic variations *)
    let cycle = !cycle_count in
    (* Only log status every 1000000 cycles at HFT speeds *)
    if cycle mod 1000000 = 0 then begin
      (match !current_price, !top_of_book, asset_balance, quote_balance with
      | Some price, Some (bid_price, _bid_size, ask_price, _ask_size), Some asset_bal, Some quote_bal ->
          Logging.info_f ~section "[%s/%s] C#%d - $%.2f | bid=$%.8f | ask=$%.8f | %s: %.8f | %s: %.2f | %d buy / %d sell%s"
            asset_with_fees.exchange asset_with_fees.symbol cycle price
            bid_price ask_price base_asset asset_bal quote_currency quote_bal
            open_buy_count open_sell_count (format_distance_info asset_with_fees.symbol !current_price asset_with_fees.strategy)
      | Some price, Some (bid_price, _bid_size, ask_price, _ask_size), _, _ ->
          Logging.info_f ~section "[%s/%s] C#%d - $%.2f | bid=$%.8f | ask=$%.8f | %d buy / %d sell%s"
            asset_with_fees.exchange asset_with_fees.symbol cycle price
            bid_price ask_price open_buy_count open_sell_count
            (format_distance_info asset_with_fees.symbol !current_price asset_with_fees.strategy)
      | Some price, None, _, _ ->
          Logging.info_f ~section "[%s/%s] C#%d - $%.2f (orderbook loading...) | %d buy / %d sell%s"
            asset_with_fees.exchange asset_with_fees.symbol cycle price open_buy_count open_sell_count
            (format_distance_info asset_with_fees.symbol !current_price asset_with_fees.strategy)
      | None, _, _, _ ->
          Logging.info_f ~section "[%s/%s] C#%d - no price/orderbook data yet | %d buy / %d sell%s"
            asset_with_fees.exchange asset_with_fees.symbol cycle open_buy_count open_sell_count
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
    
    (* Batch telemetry updates every 1000 cycles to minimize overhead *)
    if !telemetry_batch >= 1000 then begin
      let (open_buy_count, open_sell_count) = 
        if asset_with_fees.exchange = "kraken" then
          Kraken.Kraken_executions_feed.count_open_orders_by_side asset_with_fees.symbol
        else
          (0, 0)
      in
      Telemetry.inc_counter cycles_counter ~value:1000 ();
      Telemetry.set_gauge open_buy_orders_gauge (float_of_int open_buy_count);
      Telemetry.set_gauge open_sell_orders_gauge (float_of_int open_sell_count);
      let _ = Telemetry.record_duration cycle_duration_hist cycle_start in
      telemetry_batch := 0
    end;

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

    let domain_handle = Domain.spawn (fun () ->
      Logging.info_f ~section "Domain for %s/%s started (restart #%d)"
        asset.exchange asset.symbol (Atomic.get state.restart_count);

      (* Exception handling for robustness *)
      try
        asset_domain_worker fee_fetcher asset;
        Logging.info_f ~section "Domain for %s/%s completed normally" asset.exchange asset.symbol
      with exn ->
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
       Atomic.set state.domain_handle None
   | None -> ());
  Mutex.unlock state.mutex

(** Check if domain needs restart *)
let domain_needs_restart state =
  Mutex.lock state.mutex;
  let needs_restart = not (Atomic.get state.is_running) in
  Mutex.unlock state.mutex;
  needs_restart

(** Domain supervisor monitoring loop *)
let supervisor_loop fee_fetcher =
  let section = "domain_supervisor" in
  Logging.info ~section "Domain supervisor started";

  while true do
    try
      Thread.delay 5.0;  (* Check every 5 seconds *)

      Mutex.lock registry_mutex;
      let domains = Hashtbl.to_seq_values domain_registry |> List.of_seq in
      Mutex.unlock registry_mutex;

      List.iter (fun state ->
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
      Logging.error_f ~section "Exception in domain supervisor: %s" (Printexc.to_string exn)
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

