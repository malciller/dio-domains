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

let () =
  Alcotest.run "Hyperliquid Balances" [
    "parsing", [
      Alcotest.test_case "parse_json_float" `Quick test_parse_json_float;
    ];
    "processing", [
      Alcotest.test_case "process_spotState" `Quick test_process_spotState;
    ]
  ]
