let test_get_balance_unknown () =
  let bal = Lighter.Balances.get_balance "TOTALLY_UNKNOWN_LIGHTER" in
  Alcotest.(check (float 0.000001)) "unknown is 0" 0.0 bal

let test_has_balance_data_initially_false () =
  let has = Lighter.Balances.has_balance_data "BALCHECK_LIGHTER" in
  Alcotest.(check bool) "no data initially" false has

let test_process_asset_balances () =
  (* Lighter account_all_assets format: assets is an assoc of asset_id -> balance_obj *)
  let json = `Assoc [
    ("type", `String "update/account_all_assets");
    ("account_all", `Assoc [
      ("assets", `Assoc [
        ("0", `Assoc [("symbol", `String "ETH"); ("balance", `String "5.25")]);
        ("1", `Assoc [("symbol", `String "WBTC"); ("balance", `String "0.1")])
      ])
    ])
  ] in

  Lighter.Balances.process_market_data json;

  let eth_bal = Lighter.Balances.get_balance "ETH" in
  Alcotest.(check (float 0.000001)) "ETH balance" 5.25 eth_bal;
  let wbtc_bal = Lighter.Balances.get_balance "WBTC" in
  Alcotest.(check (float 0.000001)) "WBTC balance" 0.1 wbtc_bal

let test_usdc_skipped_in_asset_balances () =
  (* USDC in account_all_assets should be skipped (only from user_stats) *)
  let json = `Assoc [
    ("type", `String "update/account_all_assets");
    ("account_all", `Assoc [
      ("assets", `Assoc [
        ("0", `Assoc [("symbol", `String "USDC"); ("balance", `String "9999.0")])
      ])
    ])
  ] in

  (* Reset USDC to known state *)
  let usdc_before = Lighter.Balances.get_balance "USDC" in
  Lighter.Balances.process_market_data json;
  let usdc_after = Lighter.Balances.get_balance "USDC" in
  (* USDC should remain unchanged (not updated from account_all_assets) *)
  Alcotest.(check (float 0.000001)) "USDC unchanged" usdc_before usdc_after

let test_process_user_stats_usdc () =
  let json = `Assoc [
    ("type", `String "subscribed/user_stats");
    ("stats", `Assoc [
      ("collateral", `String "1500.75")
    ])
  ] in

  Lighter.Balances.process_market_data json;

  let usdc_bal = Lighter.Balances.get_balance "USDC" in
  Alcotest.(check (float 0.000001)) "USDC via user_stats" 1500.75 usdc_bal

let test_get_all_balances () =
  (* After the above tests, we should have at least ETH, WBTC, USDC *)
  let balances = Lighter.Balances.get_all_balances () in
  let has_eth = List.exists (fun (asset, _) -> asset = "ETH") balances in
  Alcotest.(check bool) "has ETH" true has_eth

let test_user_stats_null_stats () =
  (* Should not crash when stats field is null *)
  let json = `Assoc [
    ("type", `String "update/user_stats");
    ("stats", `Null)
  ] in
  Lighter.Balances.process_market_data json;
  (* No crash = pass *)
  Alcotest.(check bool) "no crash" true true

let () =
  Alcotest.run "Lighter Balances" [
    "initial_state", [
      Alcotest.test_case "unknown balance is 0" `Quick test_get_balance_unknown;
      Alcotest.test_case "has_balance_data initially false" `Quick test_has_balance_data_initially_false;
    ];
    "processing", [
      Alcotest.test_case "process_asset_balances" `Quick test_process_asset_balances;
      Alcotest.test_case "USDC skipped in asset_balances" `Quick test_usdc_skipped_in_asset_balances;
      Alcotest.test_case "process_user_stats USDC" `Quick test_process_user_stats_usdc;
      Alcotest.test_case "user_stats null stats" `Quick test_user_stats_null_stats;
    ];
    "aggregation", [
      Alcotest.test_case "get_all_balances" `Quick test_get_all_balances;
    ]
  ]
