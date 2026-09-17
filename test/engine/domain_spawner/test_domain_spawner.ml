(* Fee fetcher stub: returns the asset with maker/taker fees set. *)
let mock_fee_fetcher (asset : Dio_engine.Config.trading_config)
  : Dio_engine.Config.trading_config
  =
  { asset with maker_fee = Some 0.001; taker_fee = Some 0.002 }
;;

let test_spawn_domains_basic () =
  let assets =
    [ { Dio_engine.Config.exchange = "kraken"
      ; symbol = "BTC/USD"
      ; qty = "0.001"
      ; grid_interval = 1.0, 1.0
      ; sell_mult = "1.0"
      ; min_usd_balance = None
      ; max_exposure = None
      ; strategy = "jacobs_ladder"
      ; maker_fee = None
      ; taker_fee = None
      ; testnet = false
      ; hedge = false
      ; accumulation_buffer = 0.01, 0.01
      ; data_feed = None
      ; base_accumulation = true
      ; sell_levels = true
      ; cpu_priority = 0
      }
    ; { Dio_engine.Config.exchange = "kraken"
      ; symbol = "ETH/USD"
      ; qty = "0.01"
      ; grid_interval = 0.5, 0.5
      ; sell_mult = "1.1"
      ; min_usd_balance = Some "100.0"
      ; max_exposure = Some "500.0"
      ; strategy = "jacobs_ladder"
      ; maker_fee = None
      ; taker_fee = None
      ; testnet = false
      ; hedge = false
      ; accumulation_buffer = 0.01, 0.01
      ; data_feed = None
      ; base_accumulation = true
      ; sell_levels = true
      ; cpu_priority = 0
      }
    ]
  in
  let config =
    { Dio_engine.Config.cycle_mod = 10000
    ; logging = { level = Logging.INFO; sections = []; width = None }
    ; gc = None
    ; oracle = None
    ; trading = assets
    ; latency_window_seconds = 5.0
    ; latency_spike_threshold_us = 10.0
    ; latency_spike_report = Dio_engine.Config.Spike_report_internal
    ; latency_spike_report_seconds = 30.0
    ; latency_network_spike_threshold_us = 20_000.0
    ; fng_check_threshold = 1.5
    ; theme = None
    ; strategy_trace = false
    }
  in
  let _supervisor_thread =
    Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets
      config
      mock_fee_fetcher
      assets
  in
  let status = Dio_engine.Domain_spawner.get_domain_status () in
  Alcotest.(check int)
    "correct number of domains"
    (List.length assets)
    (List.length status)
;;

let test_spawn_domains_empty () =
  (* Reset registry state from prior tests. *)
  Dio_engine.Domain_spawner.clear_domain_registry ();
  let config =
    { Dio_engine.Config.cycle_mod = 10000
    ; logging = { level = Logging.INFO; sections = []; width = None }
    ; gc = None
    ; oracle = None
    ; trading = []
    ; latency_window_seconds = 5.0
    ; latency_spike_threshold_us = 10.0
    ; latency_spike_report = Dio_engine.Config.Spike_report_internal
    ; latency_spike_report_seconds = 30.0
    ; latency_network_spike_threshold_us = 20_000.0
    ; fng_check_threshold = 1.5
    ; theme = None
    ; strategy_trace = false
    }
  in
  let _supervisor_thread =
    Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets
      config
      mock_fee_fetcher
      []
  in
  let status = Dio_engine.Domain_spawner.get_domain_status () in
  Alcotest.(check int) "empty domains list length" 0 (List.length status)
;;

let test_fee_fetcher_integration () =
  let asset =
    { Dio_engine.Config.exchange = "kraken"
    ; symbol = "LTC/USD"
    ; qty = "0.1"
    ; grid_interval = 2.0, 2.0
    ; sell_mult = "1.05"
    ; min_usd_balance = None
    ; max_exposure = None
    ; strategy = "jacobs_ladder"
    ; maker_fee = None
    ; taker_fee = None
    ; testnet = false
    ; hedge = false
    ; accumulation_buffer = 0.01, 0.01
    ; data_feed = None
    ; base_accumulation = true
    ; sell_levels = true
    ; cpu_priority = 0
    }
  in
  let asset_with_fees = mock_fee_fetcher asset in
  Alcotest.(check (option (float 0.001)))
    "maker_fee added"
    (Some 0.001)
    asset_with_fees.maker_fee;
  Alcotest.(check (option (float 0.001)))
    "taker_fee added"
    (Some 0.002)
    asset_with_fees.taker_fee
;;

let test_strategy_initialization () =
  Alcotest.(check unit)
    "jacobs_ladder init"
    ()
    (Dio_strategies.Strategy_api.Strategy.init ())
;;

let test_domain_error_handling () =
  (* Reset registry state from prior tests. *)
  Dio_engine.Domain_spawner.clear_domain_registry ();
  let failing_asset =
    { Dio_engine.Config.exchange = "invalid_exchange"
    ; symbol = "TEST/USD"
    ; qty = "0.001"
    ; grid_interval = 1.0, 1.0
    ; sell_mult = "1.0"
    ; min_usd_balance = None
    ; max_exposure = None
    ; strategy = "invalid_strategy"
    ; maker_fee = None
    ; taker_fee = None
    ; testnet = false
    ; hedge = false
    ; accumulation_buffer = 0.01, 0.01
    ; data_feed = None
    ; base_accumulation = true
    ; sell_levels = true
    ; cpu_priority = 0
    }
  in
  (* Domains handle errors internally; the runner must not crash. *)
  let config =
    { Dio_engine.Config.cycle_mod = 10000
    ; logging = { level = Logging.INFO; sections = []; width = None }
    ; gc = None
    ; oracle = None
    ; trading = [ failing_asset ]
    ; latency_window_seconds = 5.0
    ; latency_spike_threshold_us = 10.0
    ; latency_spike_report = Dio_engine.Config.Spike_report_internal
    ; latency_spike_report_seconds = 30.0
    ; latency_network_spike_threshold_us = 20_000.0
    ; fng_check_threshold = 1.5
    ; theme = None
    ; strategy_trace = false
    }
  in
  let _supervisor_thread =
    Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets
      config
      mock_fee_fetcher
      [ failing_asset ]
  in
  (* Allow domains time to fail. *)
  Unix.sleepf 0.1;
  let status = Dio_engine.Domain_spawner.get_domain_status () in
  Alcotest.(check int) "domain created for failing asset" 1 (List.length status)
;;

let mk_asset symbol cpu_priority : Dio_engine.Config.trading_config =
  { Dio_engine.Config.exchange = "kraken"
  ; symbol
  ; qty = "1.0"
  ; grid_interval = 1.0, 1.0
  ; sell_mult = "1.0"
  ; min_usd_balance = None
  ; max_exposure = None
  ; strategy = "jacobs_ladder"
  ; maker_fee = None
  ; taker_fee = None
  ; testnet = false
  ; hedge = false
  ; accumulation_buffer = 0.01, 0.01
  ; data_feed = None
  ; base_accumulation = true
  ; sell_levels = true
  ; cpu_priority
  }
;;

let test_order_by_cpu_priority () =
  let symbols assets =
    List.map (fun (a : Dio_engine.Config.trading_config) -> a.symbol) assets
  in
  Alcotest.(check (list string))
    "descending priority, ties keep config order"
    [ "D"; "B"; "A"; "C" ]
    (symbols
       (Dio_engine.Domain_spawner.order_by_cpu_priority
          [ mk_asset "A" 0; mk_asset "B" 5; mk_asset "C" 0; mk_asset "D" 10 ]));
  Alcotest.(check (list string))
    "equal priorities preserve config order"
    [ "A"; "C" ]
    (symbols
       (Dio_engine.Domain_spawner.order_by_cpu_priority
          [ mk_asset "A" 0; mk_asset "C" 0 ]))
;;

let () =
  Alcotest.run
    "Domain Spawner"
    [ ( "spawning"
      , [ Alcotest.test_case "basic spawning" `Quick test_spawn_domains_basic
        ; Alcotest.test_case "empty list" `Quick test_spawn_domains_empty
        ; Alcotest.test_case "error handling" `Quick test_domain_error_handling
        ] )
    ; ( "affinity"
      , [ Alcotest.test_case "cpu priority ordering" `Quick test_order_by_cpu_priority ] )
    ; ( "integration"
      , [ Alcotest.test_case "fee fetcher" `Quick test_fee_fetcher_integration
        ; Alcotest.test_case "strategy init" `Quick test_strategy_initialization
        ] )
    ]
;;
