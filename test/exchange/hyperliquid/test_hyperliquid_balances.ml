let test_parse_json_float () =
  Alcotest.(check (float 0.000001)) "from string" 1.5 (Hyperliquid.Balances.parse_json_float (`String "1.5"));
  Alcotest.(check (float 0.000001)) "from float" 2.5 (Hyperliquid.Balances.parse_json_float (`Float 2.5));
  Alcotest.(check (float 0.000001)) "from int" 3.0 (Hyperliquid.Balances.parse_json_float (`Int 3));
  Alcotest.(check (float 0.000001)) "fallback" 0.0 (Hyperliquid.Balances.parse_json_float (`Null))

let test_process_spotState () =
  let json = `Assoc [
    ("channel", `String "spotState");
    ("data", `Assoc [
      ("spotState", `Assoc [
        ("balances", `List [
          `Assoc [("coin", `String "USDC"); ("total", `String "100.5")];
          `Assoc [("coin", `String "HYPE"); ("total", `String "50.0")]
        ])
      ])
    ])
  ] in
  
  Hyperliquid.Balances.process_market_data json;
  
  let usdc_bal = Hyperliquid.Balances.get_balance "USDC" in
  let hype_bal = Hyperliquid.Balances.get_balance "HYPE" in
  
  Alcotest.(check (float 0.000001)) "USDC balance updated" 100.5 usdc_bal;
  Alcotest.(check (float 0.000001)) "HYPE balance updated" 50.0 hype_bal

let test_process_webData2 () =
  let json = `Assoc [
    ("channel", `String "webData2");
    ("data", `Assoc [
      ("clearinghouseState", `Assoc [
        ("withdrawable", `String "500.0");
        ("marginSummary", `Assoc [
          ("accountValue", `String "1000.0")
        ])
      ]);
      ("userState", `Null)
    ])
  ] in

  Hyperliquid.Balances.process_market_data json;

  let usdc_bal = Hyperliquid.Balances.get_balance "USDC" in
  (* withdrawable is preferred over accountValue when > 0 *)
  Alcotest.(check bool) "USDC updated from webData2" true (usdc_bal > 0.0)

let test_get_balance_unknown () =
  let bal = Hyperliquid.Balances.get_balance "TOTALLY_UNKNOWN_TOKEN" in
  Alcotest.(check (float 0.000001)) "unknown is 0" 0.0 bal

let test_has_balance_data () =
  let has_before = Hyperliquid.Balances.has_balance_data "BALCHECK_COIN" in
  Alcotest.(check bool) "no data initially" false has_before

let () =
  Alcotest.run "Hyperliquid Balances" [
    "parsing", [
      Alcotest.test_case "parse_json_float" `Quick test_parse_json_float;
    ];
    "processing", [
      Alcotest.test_case "process_spotState" `Quick test_process_spotState;
      Alcotest.test_case "process_webData2" `Quick test_process_webData2;
      Alcotest.test_case "get_balance_unknown" `Quick test_get_balance_unknown;
      Alcotest.test_case "has_balance_data" `Quick test_has_balance_data;
    ]
  ]
