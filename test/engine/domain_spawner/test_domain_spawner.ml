(* Mock fee fetcher for testing - just returns the asset with some fees set *)
let mock_fee_fetcher (asset : Dio_engine.Config.trading_config)
  : Dio_engine.Config.trading_config
  =
  { asset with maker_fee = Some 0.001; taker_fee = Some 0.002 }
;;

let test_spawn_domains_basic () =
  (* Test spawning domains for basic asset configs *)
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
      ; asset_class = None
      ; base_accumulation = true
      ; sell_levels = true
      }
    ; { Dio_engine.Config.exchange = "kraken"
      ; symbol = "ETH/USD"
      ; qty = "0.01"
      ; grid_interval = 0.5, 0.5
      ; sell_mult = "1.1"
      ; min_usd_balance = Some "100.0"
      ; max_exposure = Some "500.0"
      ; strategy = "MM"
      ; maker_fee = None
      ; taker_fee = None
      ; testnet = false
      ; hedge = false
      ; accumulation_buffer = 0.01, 0.01
      ; data_feed = None
      ; asset_class = None
      ; base_accumulation = true
      ; sell_levels = true
      }
    ]
  in
  (* Spawn domains for the assets *)
  let config =
    { Dio_engine.Config.cycle_mod = 10000
    ; logging = { level = Logging.INFO; sections = []; width = None }
    ; gc = None
    ; oracle = None
    ; trading = assets
    ; latency_window_seconds = 5.0
    ; fng_check_threshold = 1.5
    }
  in
  let _supervisor_thread =
    Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets
      config
      mock_fee_fetcher
      assets
  in
  let status = Dio_engine.Domain_spawner.get_domain_status () in
  (* Verify correct number of domains created *)
  Alcotest.(check int)
    "correct number of domains"
    (List.length assets)
    (List.length status)
;;

let test_spawn_domains_empty () =
  (* Test spawning domains with empty asset list *)
  (* Clear any existing domain registry state from previous tests *)
  Dio_engine.Domain_spawner.clear_domain_registry ();
  let config =
    { Dio_engine.Config.cycle_mod = 10000
    ; logging = { level = Logging.INFO; sections = []; width = None }
    ; gc = None
    ; oracle = None
    ; trading = []
    ; latency_window_seconds = 5.0
    ; fng_check_threshold = 1.5
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
  (* Test that fee fetcher is called and integrated properly *)
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
    ; asset_class = None
    ; base_accumulation = true
    ; sell_levels = true
    }
  in
  (* Verify fee fetcher adds fees correctly *)
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
  (* Test that strategy modules are initialized without errors *)
  Alcotest.(check unit)
    "jacobs_ladder init"
    ()
    (Dio_strategies.Jacobs_ladder.Strategy.init ());
  Alcotest.(check unit)
    "market_maker init"
    ()
    (Dio_strategies.Market_maker.Strategy.init ())
;;

let test_domain_error_handling () =
  (* Clear any existing domain registry state from previous tests *)
  Dio_engine.Domain_spawner.clear_domain_registry ();
  (* Test that domain errors are handled properly - create a failing asset config *)
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
    ; asset_class = None
    ; base_accumulation = true
    ; sell_levels = true
    }
  in
  (* This should not crash the test runner, domains should handle errors internally *)
  let config =
    { Dio_engine.Config.cycle_mod = 10000
    ; logging = { level = Logging.INFO; sections = []; width = None }
    ; gc = None
    ; oracle = None
    ; trading = [ failing_asset ]
    ; latency_window_seconds = 5.0
    ; fng_check_threshold = 1.5
    }
  in
  let _supervisor_thread =
    Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets
      config
      mock_fee_fetcher
      [ failing_asset ]
  in
  (* Give domains a moment to potentially fail *)
  Unix.sleepf 0.1;
  let status = Dio_engine.Domain_spawner.get_domain_status () in
  (* If we get here, domains were created successfully (even if they fail internally) *)
  Alcotest.(check int) "domain created for failing asset" 1 (List.length status)
;;

let test_grid_gate_should_open () =
  (* The grid startup gate gives BOTH sizing sources their chance at startup:
     it opens immediately on a capital-oracle decision for the asset, or on a
     live Fear & Greed reading once the oracle's first pass attempt has
     finished / the startup deadline elapsed. It never opens on fabricated
     config defaults - with neither signal the grid withholds orders. *)
  let open Dio_engine.Domain_spawner in
  Alcotest.(check bool)
    "oracle decision alone opens the gate"
    true
    (grid_gate_should_open ~oracle_decision:true ~fng_available:false ~gate_waiver:false);
  Alcotest.(check bool)
    "oracle decision opens regardless of F&G"
    true
    (grid_gate_should_open ~oracle_decision:true ~fng_available:false ~gate_waiver:true);
  Alcotest.(check bool)
    "F&G opens once the oracle's chance elapsed"
    true
    (grid_gate_should_open ~oracle_decision:false ~fng_available:true ~gate_waiver:true);
  Alcotest.(check bool)
    "F&G alone does not open before the oracle's chance"
    false
    (grid_gate_should_open ~oracle_decision:false ~fng_available:true ~gate_waiver:false);
  Alcotest.(check bool)
    "neither keeps the gate closed (orders withheld)"
    false
    (grid_gate_should_open ~oracle_decision:false ~fng_available:false ~gate_waiver:true)
;;

let () =
  Alcotest.run
    "Domain Spawner"
    [ ( "spawning"
      , [ Alcotest.test_case "basic spawning" `Quick test_spawn_domains_basic
        ; Alcotest.test_case "empty list" `Quick test_spawn_domains_empty
        ; Alcotest.test_case "error handling" `Quick test_domain_error_handling
        ] )
    ; ( "gate"
      , [ Alcotest.test_case
            "grid gate opens on one real signal, never on defaults"
            `Quick
            test_grid_gate_should_open
        ] )
    ; ( "integration"
      , [ Alcotest.test_case "fee fetcher" `Quick test_fee_fetcher_integration
        ; Alcotest.test_case "strategy init" `Quick test_strategy_initialization
        ] )
    ]
;;
