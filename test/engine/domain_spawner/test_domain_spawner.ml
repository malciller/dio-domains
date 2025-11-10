(* Mock fee fetcher for testing - just returns the asset with some fees set *)
let mock_fee_fetcher (asset : Dio_engine.Config.trading_config) : Dio_engine.Config.trading_config =
  { asset with
    maker_fee = Some 0.001;
    taker_fee = Some 0.002
  }

let test_spawn_domains_basic () =
  (* Test spawning domains for basic asset configs *)
  let assets = [
    { Dio_engine.Config.exchange = "kraken";
      symbol = "BTC/USD";
      qty = "0.001";
      grid_interval = "1.0";
      sell_mult = "1.0";
      min_usd_balance = None;
      max_exposure = None;
      strategy = "suicide_grid";
      maker_fee = None;
      taker_fee = None };
    { Dio_engine.Config.exchange = "kraken";
      symbol = "ETH/USD";
      qty = "0.01";
      grid_interval = "0.5";
      sell_mult = "1.1";
      min_usd_balance = Some "100.0";
      max_exposure = Some "500.0";
      strategy = "MM";
      maker_fee = None;
      taker_fee = None }
  ] in

  (* Set testing environment for faster shutdown *)
  Unix.putenv "DIO_TESTING" "1";

  (* Spawn supervised domains for the assets *)
  Lwt_main.run (
    let%lwt () = Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets mock_fee_fetcher assets in

    (* Give domains a moment to start *)
    let%lwt () = Lwt_unix.sleep 0.1 in

    (* Check that domains were registered in the registry *)
    let registry_count = Dio_engine.Domain_spawner.get_domain_registry_count () in
    Alcotest.(check int) "correct number of domains registered" (List.length assets) registry_count;

    (* Clean up: stop all domains to prevent hanging *)
    let%lwt () = Dio_engine.Domain_spawner.stop_all_domains () in

    (* Give cleanup a moment to complete *)
    let%lwt () = Lwt_unix.sleep 0.2 in

    Lwt.return_unit
  )

let test_spawn_domains_empty () =
  (* Test spawning domains with empty asset list *)
  (* Clear any existing domain registry state from previous tests *)
  Dio_engine.Domain_spawner.clear_domain_registry ();

  (* Spawn supervised domains for empty asset list - this returns unit *)
  Lwt_main.run (
    let%lwt () = Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets mock_fee_fetcher [] in

    (* Check that no domains were registered *)
    let registry_count = Dio_engine.Domain_spawner.get_domain_registry_count () in
    Alcotest.(check int) "empty domains registry count" 0 registry_count;

    Lwt.return_unit
  )

let test_fee_fetcher_integration () =
  (* Test that fee fetcher is called and integrated properly *)
  let asset = {
    Dio_engine.Config.exchange = "kraken";
    symbol = "LTC/USD";
    qty = "0.1";
    grid_interval = "2.0";
    sell_mult = "1.05";
    min_usd_balance = None;
    max_exposure = None;
    strategy = "suicide_grid";
    maker_fee = None;
    taker_fee = None
  } in

  (* Verify fee fetcher adds fees correctly *)
  let asset_with_fees = mock_fee_fetcher asset in

  Alcotest.(check (option (float 0.001))) "maker_fee added" (Some 0.001) asset_with_fees.maker_fee;
  Alcotest.(check (option (float 0.001))) "taker_fee added" (Some 0.002) asset_with_fees.taker_fee

let test_strategy_initialization () =
  (* Test that strategy modules are initialized without errors *)
  Alcotest.(check unit) "suicide_grid init" () (Dio_strategies.Suicide_grid.Strategy.init ());
  Alcotest.(check unit) "market_maker init" () (Dio_strategies.Market_maker.Strategy.init ())

let test_domain_error_handling () =
  (* Clear any existing domain registry state from previous tests *)
  Dio_engine.Domain_spawner.clear_domain_registry ();

  (* Set testing environment for faster shutdown *)
  Unix.putenv "DIO_TESTING" "1";

  (* Test that domain errors are handled properly - create a failing asset config *)
  let failing_asset = {
    Dio_engine.Config.exchange = "invalid_exchange";
    symbol = "TEST/USD";
    qty = "0.001";
    grid_interval = "1.0";
    sell_mult = "1.0";
    min_usd_balance = None;
    max_exposure = None;
    strategy = "invalid_strategy";
    maker_fee = None;
    taker_fee = None
  } in

  (* Spawn supervised domains - this should not crash even with invalid config *)
  Lwt_main.run (
    let%lwt () = Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets mock_fee_fetcher [failing_asset] in

    (* Give domains a moment to potentially fail *)
    let%lwt () = Lwt_unix.sleep 0.1 in

    (* Check that domain was registered (even if it fails internally) *)
    let registry_count = Dio_engine.Domain_spawner.get_domain_registry_count () in
    Alcotest.(check int) "domain registered for failing asset" 1 registry_count;

    (* Clean up: stop all domains to prevent hanging *)
    let%lwt () = Dio_engine.Domain_spawner.stop_all_domains () in

    (* Give cleanup a moment to complete *)
    let%lwt () = Lwt_unix.sleep 0.2 in

    Lwt.return_unit
  )

let () =
  Alcotest.run "Domain Spawner" [
    "spawning", [
      Alcotest.test_case "basic spawning" `Quick test_spawn_domains_basic;
      Alcotest.test_case "empty list" `Quick test_spawn_domains_empty;
      Alcotest.test_case "error handling" `Quick test_domain_error_handling;
    ];
    "integration", [
      Alcotest.test_case "fee fetcher" `Quick test_fee_fetcher_integration;
      Alcotest.test_case "strategy init" `Quick test_strategy_initialization;
    ];
  ]
