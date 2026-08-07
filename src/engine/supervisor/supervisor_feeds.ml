
(** Feed initialization. Reads trading configs, partitions symbols by
    exchange, initializes all store subsystems, registers and starts
    supervised WebSocket connections, gates on initial data readiness,
    and fetches trading fees. *)

open Lwt.Infix

open Supervisor_types
open Supervisor_connection

let section = "supervisor"

(** Performs the full WebSocket feed initialization sequence:
    1. Reads trading configs and partitions symbols by exchange
    2. Initializes ticker, instrument, orderbook, balance, and execution stores
    3. Registers and starts supervised WebSocket connections
    4. Waits for initial market data readiness with timeouts
    5. Fetches and caches trading fees per symbol
    Returns (configs_with_fees, auth_token). *)
let initialize_feeds () : ((Dio_engine.Config.trading_config list * string) Lwt.t) =
  Logging.info ~section "Initializing websocket feeds...";

  (* Load trading configurations *)
  let config = Dio_engine.Config.read_config () in
  let app_configs = config.trading in
  Logging.info_f ~section "Loaded %d trading configuration(s)" (List.length app_configs);

  (* Partition symbols by exchange *)
  let kraken_symbols = app_configs
                      |> List.filter (fun cfg -> Dio_exchange.Exchange_intf.Types.exchange_of_string cfg.Dio_engine.Config.exchange = Kraken)
                      |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol) in

  (* Extract Hyperliquid symbols; include base asset of spot pairs for perp hedge pricing *)
  let hyperliquid_symbols = app_configs
                           |> List.filter (fun cfg -> Dio_exchange.Exchange_intf.Types.exchange_of_string cfg.Dio_engine.Config.exchange = Hyperliquid)
                           |> List.fold_left (fun acc cfg ->
                                let sym = cfg.Dio_engine.Config.symbol in
                                if String.contains sym '/' then
                                  let base_asset = String.split_on_char '/' sym |> List.hd in
                                  base_asset :: sym :: acc
                                else
                                  sym :: acc
                              ) []
                           |> List.sort_uniq String.compare in
  let has_hyperliquid = List.length hyperliquid_symbols > 0 in
  let has_kraken = List.length kraken_symbols > 0 in
  let hyperliquid_testnet =
    match app_configs |> List.find_opt (fun (cfg : Dio_engine.Config.trading_config) -> Dio_exchange.Exchange_intf.Types.exchange_of_string cfg.exchange = Hyperliquid) with
    | Some cfg -> cfg.testnet
    | None -> false in

  (* Extract IBKR symbols *)
  let ibkr_symbols = app_configs
                     |> List.filter (fun cfg -> Dio_exchange.Exchange_intf.Types.exchange_of_string cfg.Dio_engine.Config.exchange = Ibkr)
                     |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol) in
  let has_ibkr = List.length ibkr_symbols > 0 in
  let ibkr_testnet =
    match app_configs |> List.find_opt (fun (cfg : Dio_engine.Config.trading_config) -> Dio_exchange.Exchange_intf.Types.exchange_of_string cfg.exchange = Ibkr) with
    | Some cfg -> cfg.testnet
    | None -> true in

  (* Extract Lighter symbols *)
  let lighter_symbols = app_configs
                        |> List.filter (fun cfg -> Dio_exchange.Exchange_intf.Types.exchange_of_string cfg.Dio_engine.Config.exchange = Lighter)
                        |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol) in
  let has_lighter = List.length lighter_symbols > 0 in

  (* Extract Alpaca symbols *)
  let alpaca_symbols = app_configs
                       |> List.filter (fun cfg -> Dio_exchange.Exchange_intf.Types.exchange_of_string cfg.Dio_engine.Config.exchange = Alpaca)
                       |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol) in
  let has_alpaca = List.length alpaca_symbols > 0 in
  let alpaca_testnet =
    match app_configs |> List.find_opt (fun (cfg : Dio_engine.Config.trading_config) -> Dio_exchange.Exchange_intf.Types.exchange_of_string cfg.exchange = Alpaca) with
    | Some cfg -> cfg.testnet
    | None -> true in
  let alpaca_data_feed =
    match app_configs |> List.find_opt (fun (cfg : Dio_engine.Config.trading_config) -> Dio_exchange.Exchange_intf.Types.exchange_of_string cfg.exchange = Alpaca) with
    | Some cfg -> Option.value cfg.data_feed ~default:"iex"
    | None -> "iex" in

  (* Apply testnet flag to Hyperliquid module *)
  if has_hyperliquid then
    Hyperliquid.Module.Hyperliquid_impl.set_testnet hyperliquid_testnet;

  (* Apply testnet flag to IBKR module — must happen before gateway connection *)
  if has_ibkr then
    Ibkr.Module.Config.set_testnet ibkr_testnet;

  if has_alpaca then begin
    Alpaca.Module.Config.set_testnet alpaca_testnet;
    Alpaca.Module.Config.set_data_feed alpaca_data_feed;
  end;

  Logging.info_f ~section "Connecting to %d Kraken websockets..." (List.length kraken_symbols);
  if has_hyperliquid then
    Logging.info_f ~section "Connecting to %d Hyperliquid websockets..." (List.length hyperliquid_symbols);
  if has_ibkr then
    Logging.info_f ~section "Connecting to IBKR gateway for %d symbols..." (List.length ibkr_symbols);
  if has_lighter then
    Logging.info_f ~section "Connecting to Lighter L2 for %d symbols..." (List.length lighter_symbols);
  if has_alpaca then
    Logging.info_f ~section "Connecting to Alpaca WS for %d symbols (%s feed)..." (List.length alpaca_symbols) alpaca_data_feed;

  (* Begin sequential initialization steps *)

  let all_hyperliquid_symbols = hyperliquid_symbols |> List.sort_uniq String.compare in



  Logging.info ~section "Step 1.5: Starting Hyperliquid websocket connection early...";
  if has_hyperliquid then begin
     let hl_ws_conn = register ~name:"hyperliquid_ws" ~connect_fn:None in
     let hl_ws_connect_fn () =
       Lwt.catch (fun () ->
         let on_failure reason =
           set_state hl_ws_conn (Failed reason);
            (* Immediately schedule reconnection; avoids waiting for
               monitor loop backoff (mirrors Kraken auth WS pattern). *)           Lwt.async (fun () ->
             Lwt.catch (fun () ->
               Lwt.pause () >>= fun () ->
               start_async hl_ws_conn;
               Lwt.return_unit
             ) (fun exn ->
               Logging.warn_f ~section "[%s] Exception during emergency reconnection: %s" hl_ws_conn.name (Printexc.to_string exn);
               Lwt.return_unit
             )
           )
         in
         let on_heartbeat () = update_data_heartbeat hl_ws_conn in
         let on_connected () =
           set_state hl_ws_conn Connected;
           let wallet = Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.value ~default:"" in
           Lwt.async (fun () -> 
             Hyperliquid.Instruments_feed.wait_until_ready () >>= fun () ->
             Hyperliquid.Ws.subscribe_to_feeds ~symbols:hyperliquid_symbols ~wallet >>= fun () ->
             (* Clear out logic replaced by reconciliation in inject_open_orders to emit proper status events *)
             Hyperliquid.Module.fetch_open_orders_ws ())
         in
         Hyperliquid.Ws.connect_and_monitor 
           ~testnet:hyperliquid_testnet 
           ~on_failure ~on_connected ~on_heartbeat
       ) (fun exn ->
         let error_msg = Printexc.to_string exn in
         Logging.error_f ~section "[%s] Connection failed: %s" hl_ws_conn.name error_msg;
         set_state hl_ws_conn (Failed error_msg);
         Lwt.return_unit
       )
     in
     set_connect_fn hl_ws_conn (Some hl_ws_connect_fn);
     start_async hl_ws_conn;
  end;

  (* Start Lighter WS connection and signer initialization *)
  let%lwt () = (if has_lighter then begin
    Logging.info ~section "Initializing Lighter signer and WebSocket...";
    let%lwt () = Lighter.Module.initialize_signer () in
    let lt_ws_conn = register ~name:"lighter_ws" ~connect_fn:None in
    let lt_ws_connect_fn () =
      Lwt.catch (fun () ->
        let on_failure reason =
          set_state lt_ws_conn (Failed reason);
          (* Do NOT call start_async here — connect_and_monitor has
             self-healing reconnect loops that never exit. The failure
             callback is only invoked when both sides are simultaneously
             down; the internal loops will recover automatically.
             Calling start_async would spawn a duplicate instance. *)
        in
        let on_heartbeat () = update_data_heartbeat lt_ws_conn in
        let on_connected () =
          set_state lt_ws_conn Connected;
          let account_index = match Sys.getenv_opt "LIGHTER_ACCOUNT_INDEX" |> Option.map String.trim with
            | Some s -> (try int_of_string s with _ -> 0)
            | None -> 0
          in
          let auth_token = Lighter.Signer.get_auth_token () in
          Lwt.async (fun () ->
            Lighter.Instruments_feed.wait_until_ready () >>= fun () ->
            Logging.info_f ~section "Lighter WS reconnected — resubscribing and rebuilding open-order state";
            Lighter.Ws.subscribe_to_feeds ~symbols:lighter_symbols ~account_index ~auth_token >>= fun () ->
            (* Rebuild order state when both sides come up together.
               Individual side reconnects handle their own resubscription
               internally via the per-side reconnect callbacks.
               We explicitly do NOT call clear_all_open_orders() here so that
               Lighter.Module.fetch_open_orders() can perform reconciliation
               and emit terminal events for missing orders. *)
            Lighter.Module.fetch_open_orders ())
        in
        Lighter.Ws.connect_and_monitor
          ~on_failure ~on_connected ~on_heartbeat
      ) (fun exn ->
        let error_msg = Printexc.to_string exn in
        Logging.error_f ~section "[%s] Connection failed: %s" lt_ws_conn.name error_msg;
        set_state lt_ws_conn (Failed error_msg);
        Lwt.return_unit
      )
    in
    Lwt.async lt_ws_connect_fn;
    Lwt.return_unit
  end else Lwt.return_unit) in

  Logging.info ~section "Step 2: Initializing instruments feed stores...";
  let%lwt () = Kraken.Kraken_instruments_feed.initialize_symbols kraken_symbols in
  let%lwt () = 
    if has_hyperliquid then Hyperliquid.Module.initialize_instruments_ws ()
    else Lwt.return_unit
  in
  let%lwt () =
    if has_lighter then Lighter.Module.initialize_instruments ~symbols:lighter_symbols
    else Lwt.return_unit
  in

  Logging.info ~section "Step 3: Initializing orderbook feed stores...";
  let%lwt () = Kraken.Kraken_orderbook_feed.initialize kraken_symbols in
  if has_hyperliquid then Hyperliquid.Orderbook_feed.initialize all_hyperliquid_symbols;
  if has_ibkr then Ibkr.Orderbook_feed.initialize ibkr_symbols;
  if has_lighter then Lighter.Orderbook_feed.initialize lighter_symbols;
  if has_alpaca then ignore (Alpaca.Orderbook.subscribe_symbols alpaca_symbols);

  Logging.info ~section "Step 4: Getting authentication token...";
  let%lwt auth_token = 
    if has_kraken then Kraken.Kraken_generate_auth_token.get_token ()
    else Lwt.return "temp_token_for_hyperliquid_only"
  in
  Logging.info ~section "Authentication token obtained";

  (* Store token globally for order executor reuse *)
  Token_store.set (Some auth_token);

  Logging.info ~section "Step 5: Initializing balances feed stores...";
  (* Derive unique base asset list from trading symbols *)
  let all_assets = app_configs
                  |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol)
                  |> List.map (fun symbol -> 
                      if String.contains symbol '/' then String.split_on_char '/' symbol |> List.hd
                      else symbol)
                  |> List.sort_uniq String.compare
                  |> fun assets -> "USD" :: assets in  (* Include USD as quote currency *)
  let all_assets = if has_hyperliquid then "USDC" :: all_assets else all_assets in
  let all_assets = if has_lighter then "USDC" :: all_assets else all_assets in
  let all_assets = List.sort_uniq String.compare all_assets in
  
  let lighter_assets = lighter_symbols
                      |> List.map (fun symbol -> 
                          if String.contains symbol '/' then String.split_on_char '/' symbol |> List.hd
                          else symbol)
                      |> List.sort_uniq String.compare
                      |> fun assets -> "USDC" :: assets in
  let lighter_assets = List.sort_uniq String.compare lighter_assets in
  
  let () = try
    Kraken.Kraken_balances_feed.initialize all_assets;
    if has_hyperliquid then begin
      Hyperliquid.Balances.initialize ~testnet:hyperliquid_testnet all_assets;
      Lwt.async (fun () -> Hyperliquid.Module.fetch_spot_balances_ws ())
    end;
    if has_ibkr then Ibkr.Balances.initialize ();
    if has_lighter then begin
      Lighter.Balances.initialize lighter_assets;
      Lwt.async (fun () -> Lighter.Module.fetch_balances ())
    end;
    if has_alpaca then Alpaca.Balances.initialize ();
    Logging.info ~section "Balances feed stores initialized";
  with exn ->
    Logging.error_f ~section "Failed to initialize balances feed stores: %s" (Printexc.to_string exn)
  in

  Logging.info ~section "Step 6: Initializing executions feed stores...";
  Kraken.Kraken_executions_feed.initialize kraken_symbols;
  if has_hyperliquid then Hyperliquid.Executions_feed.initialize all_hyperliquid_symbols;
  if has_ibkr then Ibkr.Executions_feed.initialize ibkr_symbols;
  if has_lighter then Lighter.Executions_feed.initialize lighter_symbols;
  if has_alpaca then Alpaca.Executions.initialize alpaca_symbols;

  (* Synchronously fetch open orders before domains start to prevent duplicate placements *)
  let%lwt () =
    if has_hyperliquid then Hyperliquid.Module.fetch_open_orders_ws ()
    else Lwt.return_unit
  in

  (* Step 7: Register and start remaining supervised WebSocket connections *)
  Logging.info ~section "Step 7: Starting Kraken websocket connections...";

  (* Kraken orderbook feed *)
  if has_kraken then begin
    let orderbook_conn = register ~name:"kraken_orderbook_ws" ~connect_fn:None in
    let orderbook_connect_fn () =
      (* Reset orderbook stores to ensure clean snapshot state *)
      Kraken.Kraken_orderbook_feed.clear_all_stores ();
      (* Exception boundary for connection establishment *)
      Lwt.catch (fun () ->
        let on_failure reason = set_state orderbook_conn (Failed reason) in
        let on_heartbeat () = update_data_heartbeat orderbook_conn in
        let on_connected () = set_state orderbook_conn Connected in
        Kraken.Kraken_orderbook_feed.connect_and_subscribe kraken_symbols ~on_failure ~on_heartbeat ~on_connected >>= fun () ->
        (* Unexpected early return from WebSocket connect_fn *)
        Lwt.return_unit
      ) (fun exn ->
        let error_msg = Printexc.to_string exn in
        Logging.error_f ~section "[%s] Connection failed during establishment: %s" orderbook_conn.name error_msg;
        set_state orderbook_conn (Failed error_msg);
        Lwt.return_unit
      )
    in
    set_connect_fn orderbook_conn (Some orderbook_connect_fn);
    start_async orderbook_conn;

    (* Unified authenticated WebSocket for trading, balances, and executions *)
    let auth_ws_conn = register ~name:"kraken_auth_ws" ~connect_fn:None in
    let subscriptions_registered = ref false in
    let auth_ws_connect_fn () =
      (* Exception boundary for connection establishment *)
      Lwt.catch (fun () ->
        let on_failure reason =
          set_state auth_ws_conn (Failed reason);
          (* Schedule immediate reconnection; bypass monitor loop backoff *)
          Lwt.async (fun () ->
            Lwt.catch (fun () ->
              Lwt.pause () >>= fun () ->  (* Cooperative yield to prevent same-turn re-entry *)
              start_async auth_ws_conn;
              Lwt.return_unit
            ) (fun exn ->
              Logging.warn_f ~section "[%s] Exception during emergency reconnection: %s" auth_ws_conn.name (Printexc.to_string exn);
              Lwt.return_unit
            )
          )
        in
        let on_heartbeat () = update_data_heartbeat auth_ws_conn in
        let on_connected () =
          set_state auth_ws_conn Connected;
          (* Subscribe balance and execution feeds on the unified connection once.
             Subsequent reconnections automatically replay registered subscriptions via Kraken_trading_client. *)
          if not !subscriptions_registered then begin
            subscriptions_registered := true;
            Lwt.async (fun () ->
              Lwt.join [
                Kraken.Kraken_balances_feed.connect_and_subscribe auth_token ~on_failure ~on_heartbeat ~on_connected:(fun () -> ());
                Kraken.Kraken_executions_feed.connect_and_subscribe auth_token ~on_failure ~on_heartbeat ~on_connected:(fun () -> ());
              ]
            )
          end
        in
        Kraken.Kraken_trading_client.connect_and_monitor auth_token ~on_failure ~on_connected >>= fun () ->
        (* Unexpected early return from WebSocket connect_fn *)
        Lwt.return_unit
      ) (fun exn ->
        let error_msg = Printexc.to_string exn in
        Logging.error_f ~section "[%s] Connection failed during establishment: %s" auth_ws_conn.name error_msg;
        set_state auth_ws_conn (Failed error_msg);
        Lwt.return_unit
      )
    in
    set_connect_fn auth_ws_conn (Some auth_ws_connect_fn);
    start_async auth_ws_conn;
  end;

  (* Alpaca WebSocket connections *)
  if has_alpaca then begin
    let alpaca_data_conn = register ~name:"alpaca_data_ws" ~connect_fn:None in
    let alpaca_data_connect_fn () =
      Lwt.catch (fun () ->
        let on_failure reason = set_state alpaca_data_conn (Failed reason) in
        let on_heartbeat () = update_data_heartbeat alpaca_data_conn in
        let on_connected () = set_state alpaca_data_conn Connected in
        Alpaca.Orderbook.connect_and_monitor ~on_failure ~on_connected ~on_heartbeat
      ) (fun exn ->
        let msg = Printexc.to_string exn in
        set_state alpaca_data_conn (Failed msg);
        Lwt.return_unit
      )
    in
    set_connect_fn alpaca_data_conn (Some alpaca_data_connect_fn);
    start_async alpaca_data_conn;

    let alpaca_trading_conn = register ~name:"alpaca_trading_ws" ~connect_fn:None in
    let alpaca_trading_connect_fn () =
      Lwt.catch (fun () ->
        let on_failure reason = set_state alpaca_trading_conn (Failed reason) in
        let on_heartbeat () = update_data_heartbeat alpaca_trading_conn in
        let on_connected () = set_state alpaca_trading_conn Connected in
        Alpaca.Executions.connect_and_monitor ~on_failure ~on_connected ~on_heartbeat
      ) (fun exn ->
        let msg = Printexc.to_string exn in
        set_state alpaca_trading_conn (Failed msg);
        Lwt.return_unit
      )
    in
    set_connect_fn alpaca_trading_conn (Some alpaca_trading_connect_fn);
    start_async alpaca_trading_conn;
  end;

  (* IBKR Gateway TCP connection *)
  if has_ibkr then begin
    let ibkr_conn_sup = register ~name:"ibkr_gateway" ~connect_fn:None in
    (* Register feed handler hooks so they survive dispatcher reset() on
       every connect/reconnect. These closures are called from
       Ibkr.Dispatcher.initialize after core handlers are registered. *)
    Ibkr.Dispatcher.on_initialize_hooks := [
      Ibkr.Orderbook_feed.register_handlers;
      Ibkr.Executions_feed.register_handlers;
      Ibkr.Balances.register_handlers;
    ];
    let ibkr_connect_fn () =
      Lwt.catch (fun () ->
        (* Gate on US equity market hours: if the market is closed,
           sleep until the next extended-hours open instead of burning
           reconnect attempts against a gateway that will reject
           contract resolution. *)
        let%lwt () =
          if not (Ibkr.Market_hours.is_market_open ()) then begin
            let sleep_secs = Ibkr.Market_hours.seconds_until_next_open () in
            Ibkr.Market_hours.log_market_status ();
            Logging.info_f ~section "[ibkr_gateway] Sleeping %.0fs (%.1f hours) until market opens"
              sleep_secs (sleep_secs /. 3600.0);
            set_state ibkr_conn_sup (Failed "Market closed");
            Lwt_unix.sleep sleep_secs
          end else
            Lwt.return_unit
        in
        (* Clean up previous connection state to prevent leaks on reconnection.
           Old req_id mappings, handler closures, and IO channels would otherwise
           accumulate across reconnect cycles. *)
        let%lwt () = (match !(Ibkr.Module.connection) with
         | Some old_conn ->
             Logging.info ~section "[ibkr_gateway] Disconnecting old connection before reconnect";
             Ibkr.Module.connection := None;
             Ibkr.Connection.disconnect old_conn
         | None -> Lwt.return_unit) in
        Ibkr.Dispatcher.reset ();
        Ibkr.Orderbook_feed.clear_req_ids ();
        let conn = Ibkr.Connection.create
          ~host:Ibkr.Module.Config.gateway_host
          ~port:!(Ibkr.Module.Config.gateway_port)
          ~client_id:Ibkr.Module.Config.client_id
        in
        Ibkr.Module.connection := Some conn;
        Ibkr.Connection.connect_with_retry conn ~max_attempts:5 >>= fun () ->
        Ibkr.Dispatcher.initialize conn;
        (* Register callback so openOrderEnd marks execution stores as ready.
           Must be set after initialize (which clears state) and before
           request_open_orders fires — avoids dependency cycle in the lib. *)
        Ibkr.Dispatcher.on_open_orders_end := Some Ibkr.Executions_feed.mark_ready_all;
        Ibkr.Connection.start_reader conn
          ~on_message:Ibkr.Dispatcher.dispatch
          ~on_disconnect:(fun reason ->
            let is_current = match !(Ibkr.Module.connection) with
              | Some c -> c == conn
              | None -> false
            in
            if not is_current then
              Logging.debug_f ~section "[ibkr_gateway] Superseded connection closed: %s (ignoring reconnect)" reason
            else begin
              set_state ibkr_conn_sup (Failed reason);
              update_circuit_breaker ibkr_conn_sup false;
              (* Gate on market hours: don't pile up reconnect attempts
                 against a closed gateway. The monitor loop's Failed handler
                 will defer reconnection to the next market open. *)
              if Ibkr.Market_hours.is_market_open () then
                Lwt.async (fun () ->
                  Lwt.catch (fun () ->
                    Lwt.pause () >>= fun () ->
                    start_async ibkr_conn_sup;
                    Lwt.return_unit
                  ) (fun exn ->
                    Logging.warn_f ~section "[ibkr_gateway] Reconnect exception: %s" (Printexc.to_string exn);
                    Lwt.return_unit
                  )
                )
              else
                Logging.info_f ~section "[ibkr_gateway] Market closed, deferring reconnection"
            end
          );
        (* Do NOT set Connected yet — defer until contract resolution succeeds.
           Setting Connected here would reset reconnect_attempts to 0, defeating
           the backoff and circuit breaker when contract resolution keeps failing. *)
        Logging.info ~section "IBKR Gateway TCP connected, resolving contracts...";
        update_data_heartbeat ibkr_conn_sup;
        (* Wait for nextValidId before subscribing *)
        Lwt_unix.sleep 1.0 >>= fun () ->
        (* Subscribe to account updates *)
        let account_id = match Ibkr.Module.Config.account_id with
          | Some id -> id
          | None -> Ibkr.Connection.get_account_id conn
        in
        Ibkr.Balances.subscribe conn ~account_id >>= fun () ->
        (* Request open orders snapshot *)
        Ibkr.Executions_feed.request_open_orders conn >>= fun () ->

        let is_paper = !(Ibkr.Module.Config.trading_mode) = "paper" in

        (* Phase 1: Snapshot — seed an initial price immediately.
           Paper: type 4 (delayed-frozen) — free, no live subscription needed.
           Live:  type 2 (frozen) — last close from live subscription. *)
        let snapshot_type = if is_paper then "4" else "2" in
        Logging.info_f ~section "IBKR Phase 1: Requesting %s snapshot for initial price seed"
          (if is_paper then "delayed-frozen" else "frozen");
        Ibkr.Connection.send conn [
          string_of_int Ibkr.Types.msg_req_market_data_type;
          "1";    (* version *)
          snapshot_type;
        ] >>= fun () ->
        (* Resolve contracts once; reuse for both snapshot and streaming.
           Catch contract resolution failures gracefully — IB Gateway may
           reject symbol lookups when the market data farm is disconnected.
           Return normally with Failed state instead of re-raising to avoid
           resetting backoff and circuit breaker. *)
        Lwt.catch (fun () ->
          let%lwt contracts = Lwt_list.map_s (fun symbol ->
            Ibkr.Contracts.resolve conn ~symbol >>= fun contract ->
            Lwt.return (symbol, contract)
          ) ibkr_symbols in
          let%lwt () = Lwt_list.iter_s (fun (_symbol, contract) ->
            Ibkr.Orderbook_feed.request_snapshot conn ~contract
          ) contracts in
          (* Brief pause to let the gateway deliver snapshot ticks *)
          Lwt_unix.sleep 2.0 >>= fun () ->

          (* Phase 2: Streaming — ongoing market data.
             Paper: type 4 (delayed-frozen) — 15-min delayed during hours,
                    last known quote when closed. Never touches live data.
             Live:  type 1 (live) — real-time streaming. *)
          let stream_type = if is_paper then "4" else "1" in
          Logging.info_f ~section "IBKR Phase 2: Switching to %s streaming"
            (if is_paper then "delayed-frozen" else "live");
          Ibkr.Connection.send conn [
            string_of_int Ibkr.Types.msg_req_market_data_type;
            "1";    (* version *)
            stream_type;
          ] >>= fun () ->
          let%lwt () = Lwt_list.iter_s (fun (_symbol, contract) ->
            Ibkr.Orderbook_feed.subscribe conn ~contract
          ) contracts in
          Logging.info_f ~section "IBKR subscribed to %d symbols (snapshot + streaming)" (List.length ibkr_symbols);
          (* Contract resolution succeeded — NOW mark as Connected.
             This is the correct place: reconnect_attempts resets to 0,
             circuit breaker resets, and backoff is cleared. *)
          set_state ibkr_conn_sup Connected;
          update_circuit_breaker ibkr_conn_sup true;
          Logging.info ~section "✓ IBKR Gateway fully connected";
          (* Block forever — reader loop runs in background *)
          let wait_p, _wait_u = Lwt.wait () in
          wait_p
        ) (fun exn ->
          (* Contract resolution failed (e.g., error 200: no security definition).
             This typically means the IB market data farm is down or the market
             is closed. Disconnect cleanly and handle based on market hours. *)
          let error_msg = Printexc.to_string exn in
          let%lwt () = Ibkr.Connection.disconnect conn in
          Ibkr.Module.connection := None;
          if not (Ibkr.Market_hours.is_market_open ()) then begin
            (* Market is closed — don't escalate the circuit breaker.
               Schedule a deferred reconnect at the next market open. *)
            let sleep_secs = Ibkr.Market_hours.seconds_until_next_open () in
            Logging.info_f ~section "[ibkr_gateway] Contract resolution failed (market closed): %s" error_msg;
            Logging.info_f ~section "[ibkr_gateway] Sleeping %.0fs (%.1f hours) until market opens"
              sleep_secs (sleep_secs /. 3600.0);
            set_state ibkr_conn_sup (Failed "Market closed");
            (* Sleep until market open, then let the connect_fn return normally
               so start_async is triggered by the supervisor's on_disconnect handler. *)
            let%lwt () = Lwt_unix.sleep sleep_secs in
            Lwt.return_unit
          end else begin
            (* Market is open but contract resolution still failed — genuine error.
               Escalate via circuit breaker as before. *)
            Logging.error_f ~section "[ibkr_gateway] Contract resolution failed: %s" error_msg;
            update_circuit_breaker ibkr_conn_sup false;
            set_state ibkr_conn_sup (Failed error_msg);
            Lwt.return_unit
          end
        )
      ) (fun exn ->
        let error_msg = Printexc.to_string exn in
        Logging.error_f ~section "[ibkr_gateway] Connection failed: %s" error_msg;
        update_circuit_breaker ibkr_conn_sup false;
        Lwt.fail exn
      )
    in
    set_connect_fn ibkr_conn_sup (Some ibkr_connect_fn);
    start_async ibkr_conn_sup;
  end;

  (* Block until trading client WebSocket is connected to prevent
     strategies from issuing orders on a dead connection. *)
  let%lwt () = if has_kraken then begin
    Logging.info ~section "Waiting for trading client to be ready...";
    let%lwt trading_client_ready = 
      let timeout = 10.0 in
      let start_time = Unix.gettimeofday () in
      let rec wait_loop () =
        let elapsed = Unix.gettimeofday () -. start_time in
        if elapsed >= timeout then
          Lwt.return false
        else if Kraken.Kraken_trading_client.is_connected () then
          Lwt.return true
        else
          Lwt_unix.sleep 0.1 >>= fun () ->
          wait_loop ()
      in
      wait_loop ()
    in
    if not trading_client_ready then
      Logging.warn ~section "Timeout waiting for trading client connection, continuing anyway..."
    else
      Logging.info ~section "✓ Trading client connected and ready";
      
    (* Await executions feed before strategies start to avoid stale-state race *)
    Logging.info ~section "Waiting for executions feed to be ready...";
    let%lwt executions_ready = Kraken.Kraken_executions_feed.wait_for_execution_data kraken_symbols 10.0 in
    if not executions_ready then
      Logging.warn ~section "Timeout waiting for executions data, continuing anyway..."
    else
      Logging.info ~section "✓ Executions feed ready";
      
    Lwt.return_unit
  end else Lwt.return_unit in

  (* Await initial data from each market data feed *)
  Logging.info ~section "Waiting for initial market data from all feeds...";



  (* Orderbook readiness gate *)
  let%lwt () = if has_kraken then begin
    let%lwt orderbook_ready = Kraken.Kraken_orderbook_feed.wait_for_orderbook_data kraken_symbols 10.0 in
    if not orderbook_ready then
      Logging.warn ~section "Timeout waiting for orderbook data, continuing anyway..."
    else
      Logging.info ~section "✓ Orderbook feed ready";
    Lwt.return_unit
  end else Lwt.return_unit in

  (* Executions readiness gate (both exchanges) *)
  let%lwt hl_executions_ready = 
    if has_hyperliquid then Hyperliquid.Executions_feed.wait_for_execution_data all_hyperliquid_symbols 10.0
    else Lwt.return_true
  in
  let%lwt executions_ready = 
    if has_kraken then Kraken.Kraken_executions_feed.wait_for_execution_data kraken_symbols 10.0 
    else Lwt.return_true
  in
  if not (executions_ready && hl_executions_ready) then
    Logging.warn ~section "Timeout waiting for executions data, continuing anyway..."
  else
    Logging.info ~section "✓ Executions feed ready";

  (* Balance readiness gate — run all exchanges in parallel since
     subscriptions are already in-flight. Sequential waits would
     accumulate timeouts and delay Lighter by 10-20s unnecessarily. *)
  let%lwt balances_ready = 
    let kraken_p = 
      if has_kraken then Kraken.Kraken_balances_feed.wait_for_balance_data all_assets 10.0
      else Lwt.return_true
    in
    let hl_p = 
      if has_hyperliquid then
        Lwt.pick [
          Hyperliquid.Balances.wait_until_ready ();
          (Lwt_unix.sleep 10.0 >|= fun () -> false)
        ]
      else Lwt.return_true
    in
    let lighter_p =
      if has_lighter then
        Lwt.pick [
          Lighter.Balances.wait_until_ready ();
          (Lwt_unix.sleep 10.0 >|= fun () -> false)
        ]
      else Lwt.return_true
    in
    let alpaca_p =
      if has_alpaca then
        Lwt.pick [
          Alpaca.Balances.wait_until_ready ();
          (Lwt_unix.sleep 10.0 >|= fun () -> false)
        ]
      else Lwt.return_true
    in
    let%lwt kraken_ready = kraken_p
    and hl_ready = hl_p
    and lighter_ready = lighter_p
    and alpaca_ready = alpaca_p in
    Lwt.return (kraken_ready && hl_ready && lighter_ready && alpaca_ready)
  in
  if not balances_ready then
    Logging.warn ~section "Timeout waiting for balance data, continuing anyway..."
  else
    Logging.info ~section "✓ Balances feed ready";

  Logging.info ~section "All feeds initialized with market data!";

  (* Start Lighter TIF renewal background monitor to keep GTT orders alive *)
  if has_lighter then begin
    Logging.info ~section "Starting Lighter TIF renewal monitor...";
    Lwt.async (fun () -> Lighter.Tif_renewal.start ~symbols:lighter_symbols)
  end;

  (* Step 8: Fetch and cache trading fees per symbol *)
  Logging.info ~section "Step 8: Fetching trading fees for all assets...";

  let%lwt global_hl_fees = 
    if has_hyperliquid then begin
      Logging.info ~section "Fetching global Hyperliquid fees...";
      let%lwt fee_opt = Hyperliquid.Get_fee.get_fee_info ~testnet:hyperliquid_testnet () in
      match fee_opt with
      | Some fees -> Lwt.return_some fees
      | None ->
          Logging.error ~section "Fatal: Failed to fetch global Hyperliquid fees on startup. Exiting...";
          exit 1
    end else Lwt.return_none
  in

  (* Sequentially fetch fees per config; results enrich trading_config with fee fields *)
  let%lwt configs_with_fees = Lwt_list.map_s (fun asset ->
    try
      match Dio_exchange.Exchange_intf.Types.exchange_of_string asset.Dio_engine.Config.exchange with
      | Kraken -> begin
        let%lwt fee_info_opt = Kraken.Kraken_get_fee.get_fee_info asset.Dio_engine.Config.symbol in
        let%lwt result = match fee_info_opt with
        | Some fee_info ->

            (* Populate Fee_cache for dashboard access *)
            (match fee_info.Kraken.Kraken_get_fee.maker_fee, fee_info.Kraken.Kraken_get_fee.taker_fee with
             | Some maker, Some taker ->
                 Dio_strategies.Fee_cache.store_fees 
                   ~exchange:asset.Dio_engine.Config.exchange 
                   ~symbol:asset.Dio_engine.Config.symbol 
                   ~maker_fee:maker 
                   ~taker_fee:taker 
                   ~ttl_seconds:600.0
             | Some maker, None ->
                 (* Fallback: use maker fee as taker when taker is absent *)
                 Dio_strategies.Fee_cache.store_fees 
                   ~exchange:asset.Dio_engine.Config.exchange 
                   ~symbol:asset.Dio_engine.Config.symbol 
                   ~maker_fee:maker 
                   ~taker_fee:maker 
                   ~ttl_seconds:600.0
             | _ -> ());
            Lwt.return { asset with
              Dio_engine.Config.maker_fee = fee_info.Kraken.Kraken_get_fee.maker_fee;
              Dio_engine.Config.taker_fee = fee_info.Kraken.Kraken_get_fee.taker_fee }
        | None ->
            Logging.error_f ~section "Fatal: Failed to fetch fees for %s. Exiting." asset.Dio_engine.Config.symbol;
            exit 1 in
        (* Sequential Lwt_list.map_s guarantees >10ms between HTTP requests,
           so nonce/timestamp collisions are not possible. *)
        Lwt.return result
      end
      | Hyperliquid -> begin
        let is_spot = String.contains asset.Dio_engine.Config.symbol '/' in
        let%lwt result = match global_hl_fees with
        | Some fee_info ->
            let maker = 
              if is_spot then Option.value fee_info.spot_maker_fee ~default:0.0
              else Option.value fee_info.maker_fee ~default:0.0002 
            in
            let taker = 
              if is_spot then Option.value fee_info.spot_taker_fee ~default:0.001
              else Option.value fee_info.taker_fee ~default:0.0005 
            in

            Dio_strategies.Fee_cache.store_fees ~exchange:"hyperliquid" ~symbol:asset.Dio_engine.Config.symbol ~maker_fee:maker ~taker_fee:taker ~ttl_seconds:600.0;
            Lwt.return { asset with Dio_engine.Config.maker_fee = Some maker; Dio_engine.Config.taker_fee = Some taker }
        | None ->
            Logging.error_f ~section "Fatal: No global HL fees available for %s. Exiting." asset.Dio_engine.Config.symbol;
            exit 1
        in
        Lwt.return result
      end
      | Ibkr -> begin
        (* IBKR uses fixed per-share commissions, not maker/taker %.
           US equities Fixed plan: $0.005/share all-in is a conservative estimate.
           Express as fraction of trade value for Fee_cache compatibility. *)
        let maker = 0.0005 in  (* 0.05% — conservative estimate for ETFs *)
        let taker = 0.0005 in

        Dio_strategies.Fee_cache.store_fees
          ~exchange:"ibkr"
          ~symbol:asset.Dio_engine.Config.symbol
          ~maker_fee:maker
          ~taker_fee:taker
          ~ttl_seconds:86400.0;  (* Fees don't change often for IBKR *)
        Lwt.return { asset with
          Dio_engine.Config.maker_fee = Some maker;
          Dio_engine.Config.taker_fee = Some taker }
      end
      | Lighter -> begin
        (* Lighter fees are embedded in orderBookDetails and already cached
           in the instruments feed — no separate fee endpoint needed. *)
        let fees = Lighter.Instruments_feed.lookup_info asset.Dio_engine.Config.symbol in
        let maker = match fees with Some i -> i.Lighter.Types.maker_fee | None -> 0.0 in
        let taker = match fees with Some i -> i.Lighter.Types.taker_fee | None -> 0.0 in

        Dio_strategies.Fee_cache.store_fees
          ~exchange:"lighter"
          ~symbol:asset.Dio_engine.Config.symbol
          ~maker_fee:maker
          ~taker_fee:taker
          ~ttl_seconds:86400.0;
        Lwt.return { asset with
          Dio_engine.Config.maker_fee = Some maker;
          Dio_engine.Config.taker_fee = Some taker }
      end
      | Alpaca -> begin
        (* Alpaca is commission-free for US equities/ETFs *)
        let maker = 0.0 in
        let taker = 0.0 in

        Dio_strategies.Fee_cache.store_fees
          ~exchange:"alpaca"
          ~symbol:asset.Dio_engine.Config.symbol
          ~maker_fee:maker
          ~taker_fee:taker
          ~ttl_seconds:86400.0;
        Lwt.return { asset with
          Dio_engine.Config.maker_fee = Some maker;
          Dio_engine.Config.taker_fee = Some taker }
      end
      | Custom _ -> begin
        Logging.warn_f ~section "Fee fetching not implemented for exchange: %s, using defaults" asset.Dio_engine.Config.exchange;
        (* Cache default fees for unsupported exchanges *)
        Dio_strategies.Fee_cache.store_fees 
          ~exchange:asset.Dio_engine.Config.exchange 
          ~symbol:asset.Dio_engine.Config.symbol 
          ~maker_fee:0.0016 
          ~taker_fee:0.0026 
          ~ttl_seconds:600.0;
        (* Apply default fee values *)
        Lwt.return { asset with
          Dio_engine.Config.maker_fee = Some 0.0016;  (* 0.16% maker fee default *)
          Dio_engine.Config.taker_fee = Some 0.0026 } (* 0.26% taker fee default *)
      end
    with exn ->
      Logging.error_f ~section "Fatal: Exception during fee fetching for %s: %s. Exiting."
        asset.Dio_engine.Config.symbol (Printexc.to_string exn);
      exit 1
  ) app_configs in

  Lwt.return (configs_with_fees, auth_token)
