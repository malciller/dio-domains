let test_get_balance () =
  (* Currently a stub in the implementation, but we should test the API contract *)
  let balance = Hyperliquid.Hyperliquid_balances_feed.get_balance "USDC" in
  Alcotest.(check (float 0.01)) "stub returns 0.0" 0.0 balance

let () =
  Alcotest.run "Hyperliquid Balances Feed" [
    "operations", [
      Alcotest.test_case "get_balance" `Quick test_get_balance;
    ];
  ]
