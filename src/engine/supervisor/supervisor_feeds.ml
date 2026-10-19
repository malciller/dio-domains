[@@@ocaml.text
  " Feed initialization: reads trading configs, partitions symbols by exchange,\n    initializes all store subsystems, registers and starts supervised WebSocket\n    connections, gates on initial data readiness, and fetches trading fees. "]
open Lwt.Infix
open Supervisor_types
open Supervisor_connection
let section = "supervisor"
let initialize_feeds ()  :
  (Dio_engine.Config.trading_config list * string) Lwt.t =
  Logging.info ~section "Initializing websocket feeds...";
  (let config = Dio_engine.Config.read_config () in
   let app_configs = config.trading in
   Logging.info_f ~section "Loaded %d trading configuration(s)"
     (List.length app_configs);
   (let kraken_symbols =
      (app_configs |>
         (List.filter
            (fun cfg ->
               (Dio_exchange.Exchange_intf.Types.exchange_of_string
                  cfg.Dio_engine.Config.exchange)
                 = Kraken)))
        |> (List.map (fun cfg -> cfg.Dio_engine.Config.symbol)) in
    let hyperliquid_symbols =
      ((app_configs |>
          (List.filter
             (fun cfg ->
                (Dio_exchange.Exchange_intf.Types.exchange_of_string
                   cfg.Dio_engine.Config.exchange)
                  = Hyperliquid)))
         |>
         (List.fold_left
            (fun acc cfg ->
               let sym = cfg.Dio_engine.Config.symbol in
               if String.contains sym '/'
               then
                 let base_asset = (String.split_on_char '/' sym) |> List.hd in
                 base_asset :: sym :: acc
               else sym :: acc) []))
        |> (List.sort_uniq String.compare) in
    let has_hyperliquid = (List.length hyperliquid_symbols) > 0 in
    let has_kraken = (List.length kraken_symbols) > 0 in
    let hyperliquid_testnet =
      match app_configs |>
              (List.find_opt
                 (fun (cfg : Dio_engine.Config.trading_config) ->
                    (Dio_exchange.Exchange_intf.Types.exchange_of_string
                       cfg.exchange)
                      = Hyperliquid))
      with
      | Some cfg -> cfg.testnet
      | None -> false in
    let ibkr_symbols =
      (app_configs |>
         (List.filter
            (fun cfg ->
               (Dio_exchange.Exchange_intf.Types.exchange_of_string
                  cfg.Dio_engine.Config.exchange)
                 = Ibkr)))
        |> (List.map (fun cfg -> cfg.Dio_engine.Config.symbol)) in
    let has_ibkr = (List.length ibkr_symbols) > 0 in
    let ibkr_testnet =
      match app_configs |>
              (List.find_opt
                 (fun (cfg : Dio_engine.Config.trading_config) ->
                    (Dio_exchange.Exchange_intf.Types.exchange_of_string
                       cfg.exchange)
                      = Ibkr))
      with
      | Some cfg -> cfg.testnet
      | None -> true in
    let lighter_symbols =
      (app_configs |>
         (List.filter
            (fun cfg ->
               (Dio_exchange.Exchange_intf.Types.exchange_of_string
                  cfg.Dio_engine.Config.exchange)
                 = Lighter)))
        |> (List.map (fun cfg -> cfg.Dio_engine.Config.symbol)) in
    let has_lighter = (List.length lighter_symbols) > 0 in
    let alpaca_symbols =
      (app_configs |>
         (List.filter
            (fun cfg ->
               (Dio_exchange.Exchange_intf.Types.exchange_of_string
                  cfg.Dio_engine.Config.exchange)
                 = Alpaca)))
        |> (List.map (fun cfg -> cfg.Dio_engine.Config.symbol)) in
    let has_alpaca = (List.length alpaca_symbols) > 0 in
    let alpaca_configs =
      app_configs |>
        (List.filter
           (fun (cfg : Dio_engine.Config.trading_config) ->
              (Dio_exchange.Exchange_intf.Types.exchange_of_string
                 cfg.exchange)
                = Alpaca)) in
    let alpaca_testnet_values =
      (alpaca_configs |>
         (List.map (fun cfg -> cfg.Dio_engine.Config.testnet)))
        |> (List.sort_uniq Bool.compare) in
    (match alpaca_testnet_values with
     | [] | _::[] -> ()
     | _ ->
         (Logging.critical_f ~section
            "Alpaca configs must share the same 'testnet' value (paper vs live is a module-global setting). Found mixed values: %s. Exiting."
            (String.concat ", "
               (List.map string_of_bool alpaca_testnet_values));
          exit 1));
    (let alpaca_testnet =
       match app_configs |>
               (List.find_opt
                  (fun (cfg : Dio_engine.Config.trading_config) ->
                     (Dio_exchange.Exchange_intf.Types.exchange_of_string
                        cfg.exchange)
                       = Alpaca))
       with
       | Some cfg -> cfg.testnet
       | None -> true in
     let alpaca_data_feed =
       match app_configs |>
               (List.find_opt
                  (fun (cfg : Dio_engine.Config.trading_config) ->
                     (Dio_exchange.Exchange_intf.Types.exchange_of_string
                        cfg.exchange)
                       = Alpaca))
       with
       | Some cfg -> Option.value cfg.data_feed ~default:"iex"
       | None -> "iex" in
     if has_hyperliquid
     then Hyperliquid.Module.Hyperliquid_impl.set_testnet hyperliquid_testnet;
     if has_ibkr then Ibkr.Module.Config.set_testnet ibkr_testnet;
     if has_alpaca
     then
       (Alpaca.Module.Config.set_testnet alpaca_testnet;
        Alpaca.Module.Config.set_data_feed alpaca_data_feed);
     Logging.info_f ~section "Connecting to %d Kraken websockets..."
       (List.length kraken_symbols);
     if has_hyperliquid
     then
       Logging.info_f ~section "Connecting to %d Hyperliquid websockets..."
         (List.length hyperliquid_symbols);
     if has_ibkr
     then
       Logging.info_f ~section "Connecting to IBKR gateway for %d symbols..."
         (List.length ibkr_symbols);
     if has_lighter
     then
       Logging.info_f ~section "Connecting to Lighter L2 for %d symbols..."
         (List.length lighter_symbols);
     if has_alpaca
     then
       Logging.info_f ~section
         "Connecting to Alpaca WS for %d symbols (%s feed)..."
         (List.length alpaca_symbols) alpaca_data_feed;
     (let all_hyperliquid_symbols =
        hyperliquid_symbols |> (List.sort_uniq String.compare) in
      Logging.info ~section
        "Step 1.5: Starting Hyperliquid websocket connection early...";
      if has_hyperliquid
      then
        (let hl_ws_conn = register ~name:"hyperliquid_ws" ~connect_fn:None in
         let hl_ws_connect_fn () =
           Lwt.catch
             (fun () ->
                let on_failure reason =
                  set_state hl_ws_conn (Failed reason);
                  Lwt.async
                    (fun () ->
                       Lwt.catch
                         (fun () ->
                            (Lwt.pause ()) >>=
                              (fun () ->
                                 start_async hl_ws_conn; Lwt.return_unit))
                         (fun exn ->
                            Logging.warn_f ~section
                              "[%s] Exception during emergency reconnection: %s"
                              hl_ws_conn.name (Printexc.to_string exn);
                            Lwt.return_unit)) in
                let on_heartbeat () = update_data_heartbeat hl_ws_conn in
                let on_connected () =
                  set_state hl_ws_conn Connected;
                  (let wallet =
                     (Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS") |>
                       (Option.value ~default:"") in
                   Lwt.async
                     (fun () ->
                        (Hyperliquid.Instruments_feed.wait_until_ready ())
                          >>=
                          (fun () ->
                             (Hyperliquid.Ws.subscribe_to_feeds
                                ~symbols:hyperliquid_symbols ~wallet)
                               >>=
                               (fun () ->
                                  Hyperliquid.Module.fetch_open_orders_ws ())))) in
                Hyperliquid.Ws.connect_and_monitor
                  ~testnet:hyperliquid_testnet ~on_failure ~on_connected
                  ~on_heartbeat)
             (fun exn ->
                let error_msg = Printexc.to_string exn in
                Logging.error_f ~section "[%s] Connection failed: %s"
                  hl_ws_conn.name error_msg;
                set_state hl_ws_conn (Failed error_msg);
                Lwt.return_unit) in
         set_connect_fn hl_ws_conn (Some hl_ws_connect_fn);
         start_async hl_ws_conn);
      (let __ppx_lwt_0 =
         if has_lighter
         then
           (Logging.info ~section
              "Initializing Lighter signer and WebSocket...";
            (let __ppx_lwt_0 = Lighter.Module.initialize_signer () in
             Lwt.backtrace_bind "src/engine/supervisor/supervisor_feeds.ml"
               224 (fun exn -> try Lwt.reraise exn with | exn -> exn)
               __ppx_lwt_0
               (fun () ->
                  let lt_ws_conn =
                    register ~name:"lighter_ws" ~connect_fn:None in
                  let lt_ws_connect_fn () =
                    Lwt.catch
                      (fun () ->
                         let on_failure reason =
                           set_state lt_ws_conn (Failed reason) in
                         let on_heartbeat () =
                           update_data_heartbeat lt_ws_conn in
                         let on_connected () =
                           set_state lt_ws_conn Connected;
                           (let account_index =
                              match (Sys.getenv_opt "LIGHTER_ACCOUNT_INDEX")
                                      |> (Option.map String.trim)
                              with
                              | Some s -> (try int_of_string s with | _ -> 0)
                              | None -> 0 in
                            let auth_token = Lighter.Signer.get_auth_token () in
                            Lwt.async
                              (fun () ->
                                 (Lighter.Instruments_feed.wait_until_ready
                                    ())
                                   >>=
                                   (fun () ->
                                      Logging.info_f ~section
                                        "Lighter WS reconnected \226\128\148 resubscribing and rebuilding open-order state";
                                      (Lighter.Ws.subscribe_to_feeds
                                         ~symbols:lighter_symbols
                                         ~account_index ~auth_token)
                                        >>=
                                        ((fun () ->
                                            Lighter.Module.fetch_open_orders
                                              ()))))) in
                         Lighter.Ws.connect_and_monitor ~on_failure
                           ~on_connected ~on_heartbeat)
                      (fun exn ->
                         let error_msg = Printexc.to_string exn in
                         Logging.error_f ~section
                           "[%s] Connection failed: %s" lt_ws_conn.name
                           error_msg;
                         set_state lt_ws_conn (Failed error_msg);
                         Lwt.return_unit) in
                  Lwt.async lt_ws_connect_fn; Lwt.return_unit)))
         else Lwt.return_unit in
       Lwt.backtrace_bind "src/engine/supervisor/supervisor_feeds.ml" 220
         (fun exn -> try Lwt.reraise exn with | exn -> exn) __ppx_lwt_0
         (fun () ->
            Logging.info ~section
              "Step 2: Initializing instruments feed stores...";
            (let __ppx_lwt_0 =
               Kraken.Kraken_instruments_feed.initialize_symbols
                 kraken_symbols in
             Lwt.backtrace_bind "src/engine/supervisor/supervisor_feeds.ml"
               283 (fun exn -> try Lwt.reraise exn with | exn -> exn)
               __ppx_lwt_0
               (fun () ->
                  let __ppx_lwt_0 =
                    if has_hyperliquid
                    then Hyperliquid.Module.initialize_instruments_ws ()
                    else Lwt.return_unit in
                  Lwt.backtrace_bind
                    "src/engine/supervisor/supervisor_feeds.ml" 284
                    (fun exn -> try Lwt.reraise exn with | exn -> exn)
                    __ppx_lwt_0
                    (fun () ->
                       let __ppx_lwt_0 =
                         if has_lighter
                         then
                           Lighter.Module.initialize_instruments
                             ~symbols:lighter_symbols
                         else Lwt.return_unit in
                       Lwt.backtrace_bind
                         "src/engine/supervisor/supervisor_feeds.ml" 289
                         (fun exn -> try Lwt.reraise exn with | exn -> exn)
                         __ppx_lwt_0
                         (fun () ->
                            Logging.info ~section
                              "Step 3: Initializing orderbook feed stores...";
                            (let __ppx_lwt_0 =
                               Kraken.Kraken_orderbook_feed.initialize
                                 kraken_symbols in
                             Lwt.backtrace_bind
                               "src/engine/supervisor/supervisor_feeds.ml"
                               295
                               (fun exn ->
                                  try Lwt.reraise exn with | exn -> exn)
                               __ppx_lwt_0
                               (fun () ->
                                  if has_hyperliquid
                                  then
                                    Hyperliquid.Orderbook_feed.initialize
                                      all_hyperliquid_symbols;
                                  if has_ibkr
                                  then
                                    Ibkr.Orderbook_feed.initialize
                                      ibkr_symbols;
                                  if has_lighter
                                  then
                                    Lighter.Orderbook_feed.initialize
                                      lighter_symbols;
                                  if has_alpaca
                                  then
                                    ignore
                                      (Alpaca.Orderbook.subscribe_symbols
                                         alpaca_symbols);
                                  Logging.info ~section
                                    "Step 4: Getting authentication token...";
                                  (let __ppx_lwt_0 =
                                     if has_kraken
                                     then
                                       Kraken.Kraken_generate_auth_token.get_token
                                         ()
                                     else
                                       Lwt.return
                                         "temp_token_for_hyperliquid_only" in
                                   Lwt.backtrace_bind
                                     "src/engine/supervisor/supervisor_feeds.ml"
                                     301
                                     (fun exn ->
                                        try Lwt.reraise exn with | exn -> exn)
                                     __ppx_lwt_0
                                     (fun auth_token ->
                                        Logging.debug ~section
                                          "Authentication token obtained";
                                        Token_store.set (Some auth_token);
                                        Logging.info ~section
                                          "Step 5: Initializing balances feed stores...";
                                        (let all_assets =
                                           (((app_configs |>
                                                (List.map
                                                   (fun cfg ->
                                                      cfg.Dio_engine.Config.symbol)))
                                               |>
                                               (List.map
                                                  (fun symbol ->
                                                     if
                                                       String.contains symbol
                                                         '/'
                                                     then
                                                       (String.split_on_char
                                                          '/' symbol)
                                                         |> List.hd
                                                     else symbol)))
                                              |>
                                              (List.sort_uniq String.compare))
                                             |>
                                             (fun assets -> "USD" :: assets) in
                                         let all_assets =
                                           if has_hyperliquid
                                           then "USDC" :: all_assets
                                           else all_assets in
                                         let all_assets =
                                           if has_lighter
                                           then "USDC" :: all_assets
                                           else all_assets in
                                         let all_assets =
                                           List.sort_uniq String.compare
                                             all_assets in
                                         let lighter_assets =
                                           ((lighter_symbols |>
                                               (List.map
                                                  (fun symbol ->
                                                     if
                                                       String.contains symbol
                                                         '/'
                                                     then
                                                       (String.split_on_char
                                                          '/' symbol)
                                                         |> List.hd
                                                     else symbol)))
                                              |>
                                              (List.sort_uniq String.compare))
                                             |>
                                             (fun assets -> "USDC" :: assets) in
                                         let lighter_assets =
                                           List.sort_uniq String.compare
                                             lighter_assets in
                                         let () =
                                           try
                                             Kraken.Kraken_balances_feed.initialize
                                               all_assets;
                                             if has_hyperliquid
                                             then
                                               (Hyperliquid.Balances.initialize
                                                  ~testnet:hyperliquid_testnet
                                                  all_assets;
                                                Hyperliquid.Module.start_staking_balance_poller
                                                  ();
                                                Lwt.async
                                                  (fun () ->
                                                     Hyperliquid.Module.fetch_spot_balances_ws
                                                       ()));
                                             if has_ibkr
                                             then Ibkr.Balances.initialize ();
                                             if has_lighter
                                             then
                                               (Lighter.Balances.initialize
                                                  lighter_assets;
                                                Lwt.async
                                                  (fun () ->
                                                     Lighter.Module.fetch_balances
                                                       ()));
                                             if has_alpaca
                                             then
                                               Alpaca.Balances.initialize ();
                                             Logging.debug ~section
                                               "Balances feed stores initialized"
                                           with
                                           | exn ->
                                               Logging.error_f ~section
                                                 "Failed to initialize balances feed stores: %s"
                                                 (Printexc.to_string exn) in
                                         Logging.info ~section
                                           "Step 6: Initializing executions feed stores...";
                                         Kraken.Kraken_executions_feed.initialize
                                           kraken_symbols;
                                         if has_hyperliquid
                                         then
                                           Hyperliquid.Executions_feed.initialize
                                             all_hyperliquid_symbols;
                                         if has_ibkr
                                         then
                                           Ibkr.Executions_feed.initialize
                                             ibkr_symbols;
                                         if has_lighter
                                         then
                                           Lighter.Executions_feed.initialize
                                             lighter_symbols;
                                         if has_alpaca
                                         then
                                           Alpaca.Executions.initialize
                                             alpaca_symbols;
                                         (let __ppx_lwt_0 =
                                            let open_orders_p =
                                              if has_hyperliquid
                                              then
                                                Lwt.pick
                                                  [(Hyperliquid.Module.fetch_open_orders_ws
                                                      ())
                                                     >|= ((fun () -> `Ok));
                                                  (Lwt_unix.sleep 10.0) >|=
                                                    ((fun () -> `Timed_out))]
                                              else Lwt.return `Ok in
                                            Lwt.backtrace_try_bind
                                              "src/engine/supervisor/supervisor_feeds.ml"
                                              375
                                              (fun exn ->
                                                 try Lwt.reraise exn
                                                 with | exn -> exn)
                                              (fun () -> open_orders_p)
                                              (function
                                               | `Ok -> Lwt.return_unit
                                               | `Timed_out ->
                                                   (Logging.warn_f ~section
                                                      "Hyperliquid open-orders fetch timed out after 10s";
                                                    Lwt.return_unit))
                                              (function
                                               | exn -> Lwt.reraise exn) in
                                          Lwt.backtrace_bind
                                            "src/engine/supervisor/supervisor_feeds.ml"
                                            365
                                            (fun exn ->
                                               try Lwt.reraise exn
                                               with | exn -> exn) __ppx_lwt_0
                                            (fun () ->
                                               let __ppx_lwt_0 =
                                                 let open_orders_p =
                                                   if has_alpaca
                                                   then
                                                     Lwt.pick
                                                       [(Alpaca.Executions.bootstrap_open_orders
                                                           ())
                                                          >|=
                                                          ((fun () -> `Ok));
                                                       (Lwt_unix.sleep 10.0)
                                                         >|=
                                                         ((fun () ->
                                                             `Timed_out))]
                                                   else Lwt.return `Ok in
                                                 Lwt.backtrace_try_bind
                                                   "src/engine/supervisor/supervisor_feeds.ml"
                                                   391
                                                   (fun exn ->
                                                      try Lwt.reraise exn
                                                      with | exn -> exn)
                                                   (fun () -> open_orders_p)
                                                   (function
                                                    | `Ok -> Lwt.return_unit
                                                    | `Timed_out ->
                                                        (Logging.warn_f
                                                           ~section
                                                           "Alpaca open-orders bootstrap timed out after 10s";
                                                         Lwt.return_unit))
                                                   (function
                                                    | exn -> Lwt.reraise exn) in
                                               Lwt.backtrace_bind
                                                 "src/engine/supervisor/supervisor_feeds.ml"
                                                 381
                                                 (fun exn ->
                                                    try Lwt.reraise exn
                                                    with | exn -> exn)
                                                 __ppx_lwt_0
                                                 (fun () ->
                                                    if has_kraken
                                                    then
                                                      Kraken.Kraken_executions_feed.set_on_snapshot_hook
                                                        (fun () ->
                                                           Lwt.async
                                                             (fun () ->
                                                                Lwt.catch
                                                                  (fun () ->
                                                                    (Kraken.Kraken_open_orders.get_open_orders
                                                                    ~symbols:kraken_symbols
                                                                    ()) >|=
                                                                    Kraken.Kraken_executions_feed.inject_open_orders)
                                                                  (fun exn ->
                                                                    Logging.warn_f
                                                                    ~section
                                                                    "Kraken open-orders bootstrap after snapshot failed: %s"
                                                                    (Printexc.to_string
                                                                    exn);
                                                                    Lwt.return_unit)));
                                                    (let __ppx_lwt_0 =
                                                       let open_orders_p =
                                                         if has_kraken
                                                         then
                                                           Lwt.pick
                                                             [(Kraken.Kraken_open_orders.get_open_orders
                                                                 ~symbols:kraken_symbols
                                                                 ())
                                                                >|=
                                                                ((fun events
                                                                    ->
                                                                    Kraken.Kraken_executions_feed.inject_open_orders
                                                                    events;
                                                                    `Ok));
                                                             (Lwt_unix.sleep
                                                                10.0)
                                                               >|=
                                                               ((fun () ->
                                                                   `Timed_out))]
                                                         else Lwt.return `Ok in
                                                       Lwt.backtrace_try_bind
                                                         "src/engine/supervisor/supervisor_feeds.ml"
                                                         429
                                                         (fun exn ->
                                                            try
                                                              Lwt.reraise exn
                                                            with | exn -> exn)
                                                         (fun () ->
                                                            open_orders_p)
                                                         (function
                                                          | `Ok ->
                                                              Lwt.return_unit
                                                          | `Timed_out ->
                                                              (Logging.warn_f
                                                                 ~section
                                                                 "Kraken open-orders bootstrap timed out after 10s";
                                                               Lwt.return_unit))
                                                         (function
                                                          | exn ->
                                                              Lwt.reraise exn) in
                                                     Lwt.backtrace_bind
                                                       "src/engine/supervisor/supervisor_feeds.ml"
                                                       416
                                                       (fun exn ->
                                                          try Lwt.reraise exn
                                                          with | exn -> exn)
                                                       __ppx_lwt_0
                                                       (fun () ->
                                                          Logging.info
                                                            ~section
                                                            "Step 7: Starting Kraken websocket connections...";
                                                          if has_kraken
                                                          then
                                                            (let orderbook_conn
                                                               =
                                                               register
                                                                 ~name:"kraken_orderbook_ws"
                                                                 ~connect_fn:None in
                                                             let orderbook_connect_fn
                                                               () =
                                                               Kraken.Kraken_orderbook_feed.clear_all_stores
                                                                 ();
                                                               Lwt.catch
                                                                 (fun () ->
                                                                    let on_failure
                                                                    reason =
                                                                    set_state
                                                                    orderbook_conn
                                                                    (Failed
                                                                    reason) in
                                                                    let on_heartbeat
                                                                    () =
                                                                    update_data_heartbeat
                                                                    orderbook_conn in
                                                                    let on_connected
                                                                    () =
                                                                    set_state
                                                                    orderbook_conn
                                                                    Connected in
                                                                    (Kraken.Kraken_orderbook_feed.connect_and_subscribe
                                                                    kraken_symbols
                                                                    ~on_failure
                                                                    ~on_heartbeat
                                                                    ~on_connected)
                                                                    >>=
                                                                    (fun ()
                                                                    ->
                                                                    Lwt.return_unit))
                                                                 (fun exn ->
                                                                    let error_msg
                                                                    =
                                                                    Printexc.to_string
                                                                    exn in
                                                                    Logging.error_f
                                                                    ~section
                                                                    "[%s] Connection failed during establishment: %s"
                                                                    orderbook_conn.name
                                                                    error_msg;
                                                                    set_state
                                                                    orderbook_conn
                                                                    (Failed
                                                                    error_msg);
                                                                    Lwt.return_unit) in
                                                             set_connect_fn
                                                               orderbook_conn
                                                               (Some
                                                                  orderbook_connect_fn);
                                                             start_async
                                                               orderbook_conn;
                                                             (let auth_ws_conn
                                                                =
                                                                register
                                                                  ~name:"kraken_auth_ws"
                                                                  ~connect_fn:None in
                                                              let subscriptions_registered
                                                                = ref false in
                                                              let auth_ws_connect_fn
                                                                () =
                                                                Lwt.catch
                                                                  (fun () ->
                                                                    let on_failure
                                                                    reason =
                                                                    set_state
                                                                    auth_ws_conn
                                                                    (Failed
                                                                    reason);
                                                                    Lwt.async
                                                                    (fun ()
                                                                    ->
                                                                    Lwt.catch
                                                                    (fun ()
                                                                    ->
                                                                    (Lwt.pause
                                                                    ()) >>=
                                                                    (fun ()
                                                                    ->
                                                                    start_async
                                                                    auth_ws_conn;
                                                                    Lwt.return_unit))
                                                                    (fun exn
                                                                    ->
                                                                    Logging.warn_f
                                                                    ~section
                                                                    "[%s] Exception during emergency reconnection: %s"
                                                                    auth_ws_conn.name
                                                                    (Printexc.to_string
                                                                    exn);
                                                                    Lwt.return_unit)) in
                                                                    let on_heartbeat
                                                                    () =
                                                                    update_data_heartbeat
                                                                    auth_ws_conn in
                                                                    let on_connected
                                                                    () =
                                                                    set_state
                                                                    auth_ws_conn
                                                                    Connected;
                                                                    if
                                                                    not
                                                                    (!subscriptions_registered)
                                                                    then
                                                                    (subscriptions_registered
                                                                    := true;
                                                                    Lwt.async
                                                                    (fun ()
                                                                    ->
                                                                    Lwt.join
                                                                    [
                                                                    Kraken.Kraken_balances_feed.connect_and_subscribe
                                                                    auth_token
                                                                    ~on_failure
                                                                    ~on_heartbeat
                                                                    ~on_connected:(
                                                                    fun () ->
                                                                    ());
                                                                    Kraken.Kraken_executions_feed.connect_and_subscribe
                                                                    auth_token
                                                                    ~on_failure
                                                                    ~on_heartbeat
                                                                    ~on_connected:(
                                                                    fun () ->
                                                                    ())])) in
                                                                    (Kraken.Kraken_trading_client.connect_and_monitor
                                                                    auth_token
                                                                    ~on_failure
                                                                    ~on_connected)
                                                                    >>=
                                                                    (fun ()
                                                                    ->
                                                                    Lwt.return_unit))
                                                                  (fun exn ->
                                                                    let error_msg
                                                                    =
                                                                    Printexc.to_string
                                                                    exn in
                                                                    Logging.error_f
                                                                    ~section
                                                                    "[%s] Connection failed during establishment: %s"
                                                                    auth_ws_conn.name
                                                                    error_msg;
                                                                    set_state
                                                                    auth_ws_conn
                                                                    (Failed
                                                                    error_msg);
                                                                    Lwt.return_unit) in
                                                              set_connect_fn
                                                                auth_ws_conn
                                                                (Some
                                                                   auth_ws_connect_fn);
                                                              start_async
                                                                auth_ws_conn));
                                                          if has_alpaca
                                                          then
                                                            (let alpaca_data_conn
                                                               =
                                                               register
                                                                 ~name:"alpaca_data_ws"
                                                                 ~connect_fn:None in
                                                             let alpaca_data_connect_fn
                                                               () =
                                                               Lwt.catch
                                                                 (fun () ->
                                                                    let on_failure
                                                                    reason =
                                                                    set_state
                                                                    alpaca_data_conn
                                                                    (Failed
                                                                    reason) in
                                                                    let on_heartbeat
                                                                    () =
                                                                    update_data_heartbeat
                                                                    alpaca_data_conn in
                                                                    let on_connected
                                                                    () =
                                                                    set_state
                                                                    alpaca_data_conn
                                                                    Connected in
                                                                    Alpaca.Orderbook.connect_and_monitor
                                                                    ~on_failure
                                                                    ~on_connected
                                                                    ~on_heartbeat)
                                                                 (fun exn ->
                                                                    let msg =
                                                                    Printexc.to_string
                                                                    exn in
                                                                    set_state
                                                                    alpaca_data_conn
                                                                    (Failed
                                                                    msg);
                                                                    Lwt.return_unit) in
                                                             set_connect_fn
                                                               alpaca_data_conn
                                                               (Some
                                                                  alpaca_data_connect_fn);
                                                             start_async
                                                               alpaca_data_conn;
                                                             (let alpaca_trading_conn
                                                                =
                                                                register
                                                                  ~name:"alpaca_trading_ws"
                                                                  ~connect_fn:None in
                                                              let alpaca_trading_connect_fn
                                                                () =
                                                                Lwt.catch
                                                                  (fun () ->
                                                                    let on_failure
                                                                    reason =
                                                                    set_state
                                                                    alpaca_trading_conn
                                                                    (Failed
                                                                    reason) in
                                                                    let on_heartbeat
                                                                    () =
                                                                    update_data_heartbeat
                                                                    alpaca_trading_conn in
                                                                    let on_connected
                                                                    () =
                                                                    set_state
                                                                    alpaca_trading_conn
                                                                    Connected in
                                                                    Alpaca.Executions.connect_and_monitor
                                                                    ~on_failure
                                                                    ~on_connected
                                                                    ~on_heartbeat)
                                                                  (fun exn ->
                                                                    let msg =
                                                                    Printexc.to_string
                                                                    exn in
                                                                    set_state
                                                                    alpaca_trading_conn
                                                                    (Failed
                                                                    msg);
                                                                    Lwt.return_unit) in
                                                              set_connect_fn
                                                                alpaca_trading_conn
                                                                (Some
                                                                   alpaca_trading_connect_fn);
                                                              start_async
                                                                alpaca_trading_conn));
                                                          if has_ibkr
                                                          then
                                                            (let ibkr_conn_sup
                                                               =
                                                               register
                                                                 ~name:"ibkr_gateway"
                                                                 ~connect_fn:None in
                                                             Ibkr.Dispatcher.on_initialize_hooks
                                                               :=
                                                               [Ibkr.Orderbook_feed.register_handlers;
                                                               Ibkr.Executions_feed.register_handlers;
                                                               Ibkr.Balances.register_handlers];
                                                             (let ibkr_connect_fn
                                                                () =
                                                                Lwt.catch
                                                                  (fun () ->
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    if
                                                                    not
                                                                    (Ibkr.Market_hours.is_market_open
                                                                    ())
                                                                    then
                                                                    let sleep_secs
                                                                    =
                                                                    Ibkr.Market_hours.seconds_until_next_open
                                                                    () in
                                                                    (Ibkr.Market_hours.log_market_status
                                                                    ();
                                                                    Logging.info_f
                                                                    ~section
                                                                    "[ibkr_gateway] Sleeping %.0fs (%.1f hours) until market opens"
                                                                    sleep_secs
                                                                    (sleep_secs
                                                                    /. 3600.0);
                                                                    set_state
                                                                    ibkr_conn_sup
                                                                    (Failed
                                                                    "Market closed");
                                                                    Lwt_unix.sleep
                                                                    sleep_secs)
                                                                    else
                                                                    Lwt.return_unit in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    581
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun ()
                                                                    ->
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    match
                                                                    Atomic.get
                                                                    Ibkr.Module.connection
                                                                    with
                                                                    |
                                                                    Some
                                                                    old_conn
                                                                    ->
                                                                    (Logging.info
                                                                    ~section
                                                                    "[ibkr_gateway] Disconnecting old connection before reconnect";
                                                                    Atomic.set
                                                                    Ibkr.Module.connection
                                                                    None;
                                                                    Ibkr.Connection.disconnect
                                                                    old_conn)
                                                                    |
                                                                    None ->
                                                                    Lwt.return_unit in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    598
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun ()
                                                                    ->
                                                                    Ibkr.Dispatcher.reset
                                                                    ();
                                                                    Ibkr.Orderbook_feed.clear_req_ids
                                                                    ();
                                                                    (
                                                                    let conn
                                                                    =
                                                                    Ibkr.Connection.create
                                                                    ~host:Ibkr.Module.Config.gateway_host
                                                                    ~port:(
                                                                    !Ibkr.Module.Config.gateway_port)
                                                                    ~client_id:Ibkr.Module.Config.client_id in
                                                                    (Ibkr.Connection.connect_with_retry
                                                                    conn
                                                                    ~max_attempts:5)
                                                                    >>=
                                                                    (fun ()
                                                                    ->
                                                                    Ibkr.Dispatcher.initialize
                                                                    conn;
                                                                    Atomic.set
                                                                    Ibkr.Module.connection
                                                                    (Some
                                                                    conn);
                                                                    Ibkr.Dispatcher.on_open_orders_end
                                                                    :=
                                                                    (Some
                                                                    Ibkr.Executions_feed.mark_ready_all);
                                                                    Ibkr.Connection.start_reader
                                                                    conn
                                                                    ~on_message:Ibkr.Dispatcher.dispatch
                                                                    ~on_disconnect:(
                                                                    fun
                                                                    reason ->
                                                                    let is_current
                                                                    =
                                                                    match
                                                                    Atomic.get
                                                                    Ibkr.Module.connection
                                                                    with
                                                                    |
                                                                    Some c ->
                                                                    c == conn
                                                                    |
                                                                    None ->
                                                                    false in
                                                                    if
                                                                    not
                                                                    is_current
                                                                    then
                                                                    Logging.debug_f
                                                                    ~section
                                                                    "[ibkr_gateway] Superseded connection closed: %s (ignoring reconnect)"
                                                                    reason
                                                                    else
                                                                    (set_state
                                                                    ibkr_conn_sup
                                                                    (Failed
                                                                    reason);
                                                                    update_circuit_breaker
                                                                    ibkr_conn_sup
                                                                    false;
                                                                    if
                                                                    Ibkr.Market_hours.is_market_open
                                                                    ()
                                                                    then
                                                                    Lwt.async
                                                                    (fun ()
                                                                    ->
                                                                    Lwt.catch
                                                                    (fun ()
                                                                    ->
                                                                    (Lwt.pause
                                                                    ()) >>=
                                                                    (fun ()
                                                                    ->
                                                                    start_async
                                                                    ibkr_conn_sup;
                                                                    Lwt.return_unit))
                                                                    (fun exn
                                                                    ->
                                                                    Logging.warn_f
                                                                    ~section
                                                                    "[ibkr_gateway] Reconnect exception: %s"
                                                                    (Printexc.to_string
                                                                    exn);
                                                                    Lwt.return_unit))
                                                                    else
                                                                    Logging.info_f
                                                                    ~section
                                                                    "[ibkr_gateway] Market closed, deferring reconnection"));
                                                                    Logging.info
                                                                    ~section
                                                                    "IBKR Gateway TCP connected, resolving contracts...";
                                                                    update_data_heartbeat
                                                                    ibkr_conn_sup;
                                                                    (Lwt_unix.sleep
                                                                    1.0) >>=
                                                                    ((fun ()
                                                                    ->
                                                                    let account_id
                                                                    =
                                                                    match Ibkr.Module.Config.account_id
                                                                    with
                                                                    |
                                                                    Some id
                                                                    -> id
                                                                    |
                                                                    None ->
                                                                    Ibkr.Connection.get_account_id
                                                                    conn in
                                                                    (Ibkr.Balances.subscribe
                                                                    conn
                                                                    ~account_id)
                                                                    >>=
                                                                    (fun ()
                                                                    ->
                                                                    (Ibkr.Executions_feed.request_open_orders
                                                                    conn) >>=
                                                                    (fun ()
                                                                    ->
                                                                    let is_paper
                                                                    =
                                                                    (!Ibkr.Module.Config.trading_mode)
                                                                    = "paper" in
                                                                    let snapshot_type
                                                                    =
                                                                    if
                                                                    is_paper
                                                                    then "4"
                                                                    else "2" in
                                                                    Logging.info_f
                                                                    ~section
                                                                    "IBKR Phase 1: Requesting %s snapshot for initial price seed"
                                                                    (if
                                                                    is_paper
                                                                    then
                                                                    "delayed-frozen"
                                                                    else
                                                                    "frozen");
                                                                    (Ibkr.Connection.send
                                                                    conn
                                                                    [
                                                                    string_of_int
                                                                    Ibkr.Types.msg_req_market_data_type;
                                                                    "1";
                                                                    snapshot_type])
                                                                    >>=
                                                                    ((fun ()
                                                                    ->
                                                                    Lwt.catch
                                                                    (fun ()
                                                                    ->
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    Lwt_list.map_s
                                                                    (fun
                                                                    symbol ->
                                                                    (Ibkr.Contracts.resolve
                                                                    conn
                                                                    ~symbol)
                                                                    >>=
                                                                    (fun
                                                                    contract
                                                                    ->
                                                                    Lwt.return
                                                                    (symbol,
                                                                    contract)))
                                                                    ibkr_symbols in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    708
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    contracts
                                                                    ->
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    Lwt_list.iter_s
                                                                    (fun
                                                                    (_symbol,
                                                                    contract)
                                                                    ->
                                                                    Ibkr.Orderbook_feed.request_snapshot
                                                                    conn
                                                                    ~contract)
                                                                    contracts in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    715
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun ()
                                                                    ->
                                                                    (Lwt_unix.sleep
                                                                    2.0) >>=
                                                                    (fun ()
                                                                    ->
                                                                    let stream_type
                                                                    =
                                                                    if
                                                                    is_paper
                                                                    then "4"
                                                                    else "1" in
                                                                    Logging.info_f
                                                                    ~section
                                                                    "IBKR Phase 2: Switching to %s streaming"
                                                                    (if
                                                                    is_paper
                                                                    then
                                                                    "delayed-frozen"
                                                                    else
                                                                    "live");
                                                                    (Ibkr.Connection.send
                                                                    conn
                                                                    [
                                                                    string_of_int
                                                                    Ibkr.Types.msg_req_market_data_type;
                                                                    "1";
                                                                    stream_type])
                                                                    >>=
                                                                    ((fun ()
                                                                    ->
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    Lwt_list.iter_s
                                                                    (fun
                                                                    (_symbol,
                                                                    contract)
                                                                    ->
                                                                    Ibkr.Orderbook_feed.subscribe
                                                                    conn
                                                                    ~contract)
                                                                    contracts in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    740
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun ()
                                                                    ->
                                                                    Logging.info_f
                                                                    ~section
                                                                    "IBKR subscribed to %d symbols (snapshot + streaming)"
                                                                    (List.length
                                                                    ibkr_symbols);
                                                                    set_state
                                                                    ibkr_conn_sup
                                                                    Connected;
                                                                    update_circuit_breaker
                                                                    ibkr_conn_sup
                                                                    true;
                                                                    Logging.info
                                                                    ~section
                                                                    "\226\156\147 IBKR Gateway fully connected";
                                                                    (
                                                                    let
                                                                    (wait_p,
                                                                    _wait_u)
                                                                    =
                                                                    Lwt.wait
                                                                    () in
                                                                    wait_p))))))))
                                                                    (fun exn
                                                                    ->
                                                                    let error_msg
                                                                    =
                                                                    Printexc.to_string
                                                                    exn in
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    Ibkr.Connection.disconnect
                                                                    conn in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    763
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun ()
                                                                    ->
                                                                    Atomic.set
                                                                    Ibkr.Module.connection
                                                                    None;
                                                                    if
                                                                    not
                                                                    (Ibkr.Market_hours.is_market_open
                                                                    ())
                                                                    then
                                                                    (let sleep_secs
                                                                    =
                                                                    Ibkr.Market_hours.seconds_until_next_open
                                                                    () in
                                                                    Logging.info_f
                                                                    ~section
                                                                    "[ibkr_gateway] Contract resolution failed (market closed): %s"
                                                                    error_msg;
                                                                    Logging.info_f
                                                                    ~section
                                                                    "[ibkr_gateway] Sleeping %.0fs (%.1f hours) until market opens"
                                                                    sleep_secs
                                                                    (sleep_secs
                                                                    /. 3600.0);
                                                                    set_state
                                                                    ibkr_conn_sup
                                                                    (Failed
                                                                    "Market closed");
                                                                    (
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    Lwt_unix.sleep
                                                                    sleep_secs in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    782
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun ()
                                                                    ->
                                                                    Lwt.return_unit)))
                                                                    else
                                                                    (Logging.error_f
                                                                    ~section
                                                                    "[ibkr_gateway] Contract resolution failed: %s"
                                                                    error_msg;
                                                                    update_circuit_breaker
                                                                    ibkr_conn_sup
                                                                    false;
                                                                    set_state
                                                                    ibkr_conn_sup
                                                                    (Failed
                                                                    error_msg);
                                                                    Lwt.return_unit))))))))))))))
                                                                  (fun exn ->
                                                                    let error_msg
                                                                    =
                                                                    Printexc.to_string
                                                                    exn in
                                                                    Logging.error_f
                                                                    ~section
                                                                    "[ibkr_gateway] Connection failed: %s"
                                                                    error_msg;
                                                                    update_circuit_breaker
                                                                    ibkr_conn_sup
                                                                    false;
                                                                    Lwt.fail
                                                                    exn) in
                                                              set_connect_fn
                                                                ibkr_conn_sup
                                                                (Some
                                                                   ibkr_connect_fn);
                                                              start_async
                                                                ibkr_conn_sup));
                                                          (let __ppx_lwt_0 =
                                                             if has_kraken
                                                             then
                                                               (Logging.info
                                                                  ~section
                                                                  "Waiting for trading client to be ready...";
                                                                (let __ppx_lwt_0
                                                                   =
                                                                   let timeout
                                                                    = 10.0 in
                                                                   let start_time
                                                                    =
                                                                    Unix.gettimeofday
                                                                    () in
                                                                   let rec wait_loop
                                                                    () =
                                                                    let elapsed
                                                                    =
                                                                    (Unix.gettimeofday
                                                                    ()) -.
                                                                    start_time in
                                                                    if
                                                                    elapsed
                                                                    >=
                                                                    timeout
                                                                    then
                                                                    Lwt.return
                                                                    false
                                                                    else
                                                                    if
                                                                    Kraken.Kraken_trading_client.is_connected
                                                                    ()
                                                                    then
                                                                    Lwt.return
                                                                    true
                                                                    else
                                                                    (Lwt_unix.sleep
                                                                    0.1) >>=
                                                                    ((fun ()
                                                                    ->
                                                                    wait_loop
                                                                    ())) in
                                                                   wait_loop
                                                                    () in
                                                                 Lwt.backtrace_bind
                                                                   "src/engine/supervisor/supervisor_feeds.ml"
                                                                   808
                                                                   (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                   __ppx_lwt_0
                                                                   (fun
                                                                    trading_client_ready
                                                                    ->
                                                                    if
                                                                    not
                                                                    trading_client_ready
                                                                    then
                                                                    Logging.warn
                                                                    ~section
                                                                    "Timeout waiting for trading client connection, continuing anyway..."
                                                                    else
                                                                    Logging.info
                                                                    ~section
                                                                    "\226\156\147 Trading client connected and ready";
                                                                    Logging.info
                                                                    ~section
                                                                    "Waiting for executions feed to be ready...";
                                                                    (
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    Kraken.Kraken_executions_feed.wait_for_execution_data
                                                                    kraken_symbols
                                                                    10.0 in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    829
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    executions_ready
                                                                    ->
                                                                    if
                                                                    not
                                                                    executions_ready
                                                                    then
                                                                    Logging.warn
                                                                    ~section
                                                                    "Timeout waiting for executions data, continuing anyway..."
                                                                    else
                                                                    Logging.info
                                                                    ~section
                                                                    "\226\156\147 Executions feed ready";
                                                                    Lwt.return_unit)))))
                                                             else
                                                               Lwt.return_unit in
                                                           Lwt.backtrace_bind
                                                             "src/engine/supervisor/supervisor_feeds.ml"
                                                             804
                                                             (fun exn ->
                                                                try
                                                                  Lwt.reraise
                                                                    exn
                                                                with
                                                                | exn -> exn)
                                                             __ppx_lwt_0
                                                             (fun () ->
                                                                Logging.info
                                                                  ~section
                                                                  "Waiting for initial market data from all feeds...";
                                                                (let __ppx_lwt_0
                                                                   =
                                                                   if
                                                                    has_kraken
                                                                   then
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    Kraken.Kraken_orderbook_feed.wait_for_orderbook_data
                                                                    kraken_symbols
                                                                    10.0 in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    843
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    orderbook_ready
                                                                    ->
                                                                    if
                                                                    not
                                                                    orderbook_ready
                                                                    then
                                                                    Logging.warn
                                                                    ~section
                                                                    "Timeout waiting for orderbook data, continuing anyway..."
                                                                    else
                                                                    Logging.info
                                                                    ~section
                                                                    "\226\156\147 Orderbook feed ready";
                                                                    Lwt.return_unit)
                                                                   else
                                                                    Lwt.return_unit in
                                                                 Lwt.backtrace_bind
                                                                   "src/engine/supervisor/supervisor_feeds.ml"
                                                                   840
                                                                   (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                   __ppx_lwt_0
                                                                   (fun () ->
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    if
                                                                    has_hyperliquid
                                                                    then
                                                                    Hyperliquid.Executions_feed.wait_for_execution_data
                                                                    all_hyperliquid_symbols
                                                                    10.0
                                                                    else
                                                                    Lwt.return_true in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    853
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    hl_executions_ready
                                                                    ->
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    if
                                                                    has_kraken
                                                                    then
                                                                    Kraken.Kraken_executions_feed.wait_for_execution_data
                                                                    kraken_symbols
                                                                    10.0
                                                                    else
                                                                    Lwt.return_true in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    858
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    executions_ready
                                                                    ->
                                                                    if
                                                                    not
                                                                    (executions_ready
                                                                    &&
                                                                    hl_executions_ready)
                                                                    then
                                                                    Logging.warn
                                                                    ~section
                                                                    "Timeout waiting for executions data, continuing anyway..."
                                                                    else
                                                                    Logging.info
                                                                    ~section
                                                                    "\226\156\147 All executions feeds ready (kraken + hyperliquid)";
                                                                    (
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    let kraken_p
                                                                    =
                                                                    if
                                                                    has_kraken
                                                                    then
                                                                    Kraken.Kraken_balances_feed.wait_for_balance_data
                                                                    all_assets
                                                                    10.0
                                                                    else
                                                                    Lwt.return_true in
                                                                    let hl_p
                                                                    =
                                                                    if
                                                                    has_hyperliquid
                                                                    then
                                                                    Lwt.pick
                                                                    [
                                                                    Hyperliquid.Balances.wait_until_ready
                                                                    ();
                                                                    (Lwt_unix.sleep
                                                                    10.0) >|=
                                                                    ((fun ()
                                                                    -> false))]
                                                                    else
                                                                    Lwt.return_true in
                                                                    let lighter_p
                                                                    =
                                                                    if
                                                                    has_lighter
                                                                    then
                                                                    Lwt.pick
                                                                    [
                                                                    Lighter.Balances.wait_until_ready
                                                                    ();
                                                                    (Lwt_unix.sleep
                                                                    10.0) >|=
                                                                    ((fun ()
                                                                    -> false))]
                                                                    else
                                                                    Lwt.return_true in
                                                                    let alpaca_p
                                                                    =
                                                                    if
                                                                    has_alpaca
                                                                    then
                                                                    Lwt.pick
                                                                    [
                                                                    Alpaca.Balances.wait_until_ready
                                                                    ();
                                                                    (Lwt_unix.sleep
                                                                    10.0) >|=
                                                                    ((fun ()
                                                                    -> false))]
                                                                    else
                                                                    Lwt.return_true in
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    kraken_p
                                                                    and __ppx_lwt_1
                                                                    = hl_p
                                                                    and __ppx_lwt_2
                                                                    =
                                                                    lighter_p
                                                                    and __ppx_lwt_3
                                                                    =
                                                                    alpaca_p in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    902
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    kraken_ready
                                                                    ->
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    902
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_1
                                                                    (fun
                                                                    hl_ready
                                                                    ->
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    902
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_2
                                                                    (fun
                                                                    lighter_ready
                                                                    ->
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    902
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_3
                                                                    (fun
                                                                    alpaca_ready
                                                                    ->
                                                                    Lwt.return
                                                                    (kraken_ready
                                                                    &&
                                                                    (hl_ready
                                                                    &&
                                                                    (lighter_ready
                                                                    &&
                                                                    alpaca_ready))))))) in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    869
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    balances_ready
                                                                    ->
                                                                    if
                                                                    not
                                                                    balances_ready
                                                                    then
                                                                    Logging.warn
                                                                    ~section
                                                                    "Timeout waiting for balance data, continuing anyway..."
                                                                    else
                                                                    Logging.info
                                                                    ~section
                                                                    "\226\156\147 Balances feed ready";
                                                                    Logging.info
                                                                    ~section
                                                                    "All feeds initialized with market data!";
                                                                    if
                                                                    has_lighter
                                                                    then
                                                                    (Logging.info
                                                                    ~section
                                                                    "Starting Lighter TIF renewal monitor...";
                                                                    Lwt.async
                                                                    (fun ()
                                                                    ->
                                                                    Lighter.Tif_renewal.start
                                                                    ~symbols:lighter_symbols));
                                                                    Logging.info
                                                                    ~section
                                                                    "Step 8: Fetching trading fees for all assets...";
                                                                    (
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    if
                                                                    has_hyperliquid
                                                                    then
                                                                    (Logging.info
                                                                    ~section
                                                                    "Fetching global Hyperliquid fees...";
                                                                    (let __ppx_lwt_0
                                                                    =
                                                                    Hyperliquid.Get_fee.get_fee_info
                                                                    ~testnet:hyperliquid_testnet
                                                                    () in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    922
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    fee_opt
                                                                    ->
                                                                    match fee_opt
                                                                    with
                                                                    |
                                                                    Some fees
                                                                    ->
                                                                    Lwt.return_some
                                                                    fees
                                                                    |
                                                                    None ->
                                                                    (Logging.error
                                                                    ~section
                                                                    "Fatal: Failed to fetch global Hyperliquid fees on startup. Exiting...";
                                                                    exit 1))))
                                                                    else
                                                                    Lwt.return_none in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    918
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    global_hl_fees
                                                                    ->
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    Lwt_list.map_s
                                                                    (fun
                                                                    asset ->
                                                                    try
                                                                    match
                                                                    Dio_exchange.Exchange_intf.Types.exchange_of_string
                                                                    asset.Dio_engine.Config.exchange
                                                                    with
                                                                    |
                                                                    Kraken ->
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    Kraken.Kraken_get_fee.get_fee_info
                                                                    asset.Dio_engine.Config.symbol in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    944
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    fee_info_opt
                                                                    ->
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    match fee_info_opt
                                                                    with
                                                                    |
                                                                    Some
                                                                    fee_info
                                                                    ->
                                                                    ((
                                                                    match
                                                                    (fee_info.Kraken.Kraken_get_fee.maker_fee,
                                                                    fee_info.Kraken.Kraken_get_fee.taker_fee)
                                                                    with
                                                                    |
                                                                    (Some
                                                                    maker,
                                                                    Some
                                                                    taker) ->
                                                                    Dio_strategies.Fee_cache.store_fees
                                                                    ~exchange:(
                                                                    asset.Dio_engine.Config.exchange)
                                                                    ~symbol:(
                                                                    asset.Dio_engine.Config.symbol)
                                                                    ~maker_fee:maker
                                                                    ~taker_fee:taker
                                                                    ~ttl_seconds:600.0
                                                                    |
                                                                    (Some
                                                                    maker,
                                                                    None) ->
                                                                    Dio_strategies.Fee_cache.store_fees
                                                                    ~exchange:(
                                                                    asset.Dio_engine.Config.exchange)
                                                                    ~symbol:(
                                                                    asset.Dio_engine.Config.symbol)
                                                                    ~maker_fee:maker
                                                                    ~taker_fee:maker
                                                                    ~ttl_seconds:600.0
                                                                    |
                                                                    _ -> ());
                                                                    Lwt.return
                                                                    {
                                                                    asset with
                                                                    Dio_engine.Config.maker_fee
                                                                    =
                                                                    (fee_info.Kraken.Kraken_get_fee.maker_fee);
                                                                    Dio_engine.Config.taker_fee
                                                                    =
                                                                    (fee_info.Kraken.Kraken_get_fee.taker_fee)
                                                                    })
                                                                    |
                                                                    None ->
                                                                    (Logging.error_f
                                                                    ~section
                                                                    "Fatal: Failed to fetch fees for %s. Exiting."
                                                                    asset.Dio_engine.Config.symbol;
                                                                    exit 1) in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    947
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    result ->
                                                                    Lwt.return
                                                                    result))
                                                                    |
                                                                    Hyperliquid
                                                                    ->
                                                                    let is_spot
                                                                    =
                                                                    String.contains
                                                                    asset.Dio_engine.Config.symbol
                                                                    '/' in
                                                                    let __ppx_lwt_0
                                                                    =
                                                                    match global_hl_fees
                                                                    with
                                                                    |
                                                                    Some
                                                                    fee_info
                                                                    ->
                                                                    let maker
                                                                    =
                                                                    if
                                                                    is_spot
                                                                    then
                                                                    Option.value
                                                                    fee_info.spot_maker_fee
                                                                    ~default:0.0
                                                                    else
                                                                    Option.value
                                                                    fee_info.maker_fee
                                                                    ~default:0.0002 in
                                                                    let taker
                                                                    =
                                                                    if
                                                                    is_spot
                                                                    then
                                                                    Option.value
                                                                    fee_info.spot_taker_fee
                                                                    ~default:0.001
                                                                    else
                                                                    Option.value
                                                                    fee_info.taker_fee
                                                                    ~default:0.0005 in
                                                                    (Dio_strategies.Fee_cache.store_fees
                                                                    ~exchange:"hyperliquid"
                                                                    ~symbol:(
                                                                    asset.Dio_engine.Config.symbol)
                                                                    ~maker_fee:maker
                                                                    ~taker_fee:taker
                                                                    ~ttl_seconds:600.0;
                                                                    Lwt.return
                                                                    {
                                                                    asset with
                                                                    Dio_engine.Config.maker_fee
                                                                    =
                                                                    (Some
                                                                    maker);
                                                                    Dio_engine.Config.taker_fee
                                                                    =
                                                                    (Some
                                                                    taker)
                                                                    })
                                                                    |
                                                                    None ->
                                                                    (Logging.error_f
                                                                    ~section
                                                                    "Fatal: No global HL fees available for %s. Exiting."
                                                                    asset.Dio_engine.Config.symbol;
                                                                    exit 1) in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    987
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    result ->
                                                                    Lwt.return
                                                                    result)
                                                                    |
                                                                    Ibkr ->
                                                                    let maker
                                                                    = 0.0005 in
                                                                    let taker
                                                                    = 0.0005 in
                                                                    (Dio_strategies.Fee_cache.store_fees
                                                                    ~exchange:"ibkr"
                                                                    ~symbol:(
                                                                    asset.Dio_engine.Config.symbol)
                                                                    ~maker_fee:maker
                                                                    ~taker_fee:taker
                                                                    ~ttl_seconds:86400.0;
                                                                    Lwt.return
                                                                    {
                                                                    asset with
                                                                    Dio_engine.Config.maker_fee
                                                                    =
                                                                    (Some
                                                                    maker);
                                                                    Dio_engine.Config.taker_fee
                                                                    =
                                                                    (Some
                                                                    taker)
                                                                    })
                                                                    |
                                                                    Lighter
                                                                    ->
                                                                    let fees
                                                                    =
                                                                    Lighter.Instruments_feed.lookup_info
                                                                    asset.Dio_engine.Config.symbol in
                                                                    let maker
                                                                    =
                                                                    match fees
                                                                    with
                                                                    |
                                                                    Some i ->
                                                                    i.Lighter.Types.maker_fee
                                                                    |
                                                                    None ->
                                                                    0.0 in
                                                                    let taker
                                                                    =
                                                                    match fees
                                                                    with
                                                                    |
                                                                    Some i ->
                                                                    i.Lighter.Types.taker_fee
                                                                    |
                                                                    None ->
                                                                    0.0 in
                                                                    (Dio_strategies.Fee_cache.store_fees
                                                                    ~exchange:"lighter"
                                                                    ~symbol:(
                                                                    asset.Dio_engine.Config.symbol)
                                                                    ~maker_fee:maker
                                                                    ~taker_fee:taker
                                                                    ~ttl_seconds:86400.0;
                                                                    Lwt.return
                                                                    {
                                                                    asset with
                                                                    Dio_engine.Config.maker_fee
                                                                    =
                                                                    (Some
                                                                    maker);
                                                                    Dio_engine.Config.taker_fee
                                                                    =
                                                                    (Some
                                                                    taker)
                                                                    })
                                                                    |
                                                                    Alpaca ->
                                                                    let maker
                                                                    = 0.0 in
                                                                    let taker
                                                                    = 0.0 in
                                                                    (Dio_strategies.Fee_cache.store_fees
                                                                    ~exchange:"alpaca"
                                                                    ~symbol:(
                                                                    asset.Dio_engine.Config.symbol)
                                                                    ~maker_fee:maker
                                                                    ~taker_fee:taker
                                                                    ~ttl_seconds:86400.0;
                                                                    Lwt.return
                                                                    {
                                                                    asset with
                                                                    Dio_engine.Config.maker_fee
                                                                    =
                                                                    (Some
                                                                    maker);
                                                                    Dio_engine.Config.taker_fee
                                                                    =
                                                                    (Some
                                                                    taker)
                                                                    })
                                                                    |
                                                                    Custom _
                                                                    ->
                                                                    (Logging.warn_f
                                                                    ~section
                                                                    "Fee fetching not implemented for exchange: %s, using defaults"
                                                                    asset.Dio_engine.Config.exchange;
                                                                    Dio_strategies.Fee_cache.store_fees
                                                                    ~exchange:(
                                                                    asset.Dio_engine.Config.exchange)
                                                                    ~symbol:(
                                                                    asset.Dio_engine.Config.symbol)
                                                                    ~maker_fee:0.0016
                                                                    ~taker_fee:0.0026
                                                                    ~ttl_seconds:600.0;
                                                                    Lwt.return
                                                                    {
                                                                    asset with
                                                                    Dio_engine.Config.maker_fee
                                                                    =
                                                                    (Some
                                                                    0.0016);
                                                                    Dio_engine.Config.taker_fee
                                                                    =
                                                                    (Some
                                                                    0.0026)
                                                                    })
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    (Logging.error_f
                                                                    ~section
                                                                    "Fatal: Exception during fee fetching for %s: %s. Exiting."
                                                                    asset.Dio_engine.Config.symbol
                                                                    (Printexc.to_string
                                                                    exn);
                                                                    exit 1))
                                                                    app_configs in
                                                                    Lwt.backtrace_bind
                                                                    "src/engine/supervisor/supervisor_feeds.ml"
                                                                    935
                                                                    (fun exn
                                                                    ->
                                                                    try
                                                                    Lwt.reraise
                                                                    exn
                                                                    with
                                                                    |
                                                                    exn ->
                                                                    exn)
                                                                    __ppx_lwt_0
                                                                    (fun
                                                                    configs_with_fees
                                                                    ->
                                                                    Lwt.return
                                                                    (configs_with_fees,
                                                                    auth_token))))))))))))))))))))))))))))))))
  [@@ocaml.doc
    " Full WebSocket feed initialization sequence:\n    1. Reads trading configs and partitions symbols by exchange\n    2. Initializes ticker, instrument, orderbook, balance, and execution stores\n    3. Registers and starts supervised WebSocket connections\n    4. Waits for initial market data readiness with timeouts\n    5. Fetches and caches trading fees per symbol Returns\n       [(configs_with_fees, auth_token)]. "]
