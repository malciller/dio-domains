open Hyperliquid

let test_process_market_data () =
  (* Test webData2 with the common userState nesting *)
  let json = `Assoc [
    ("channel", `String "webData2");
    ("data", `Assoc [
      ("userState", `Assoc [
        ("clearinghouseState", `Assoc [
          ("marginSummary", `Assoc [
            ("accountValue", `String "100.50")
          ])
        ]);
        ("spotState", `Assoc [
          ("balances", `List [
            `Assoc [("coin", `String "HYPE"); ("total", `Float 50.25)];
            `Assoc [("coin", `String "USDC"); ("total", `Int 75)]
          ])
        ])
      ])
    ])
  ] in
  
  Hyperliquid_balances_feed.process_market_data json;
  
  (* 100.50 (perp) + 75.0 (spot) = 175.5. *)
  Alcotest.(check (float 1e-9)) "USDC should handle nested userState and aggregation" 
    175.5 (Hyperliquid_balances_feed.get_balance "USDC");
  Alcotest.(check (float 1e-9)) "HYPE should handle nested userState" 
    50.25 (Hyperliquid_balances_feed.get_balance "HYPE");
  
  (* Test webData2 with direct fields (no userState) to ensure robustness *)
  let json2 = `Assoc [
    ("channel", `String "webData2");
    ("data", `Assoc [
      ("clearinghouseState", `Assoc [
        ("marginSummary", `Assoc [
          ("accountValue", `Int 200)
        ])
      ])
    ])
  ] in
  
  Hyperliquid_balances_feed.process_market_data json2;
  Alcotest.(check (float 1e-9)) "USDC should handle direct clearinghouseState" 
    200.0 (Hyperliquid_balances_feed.get_balance "USDC")

let () =
  Alcotest.run "Hyperliquid Balances Feed" [
    "operations", [
      Alcotest.test_case "process_market_data" `Quick test_process_market_data;
    ];
  ]
