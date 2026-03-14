let test_get_price_increment () =
  let inc = Hyperliquid.Hyperliquid_instruments_feed.get_price_increment "BTC" in
  Alcotest.(check bool) "stub returns none" true (inc = None)

let test_get_qty_increment () =
  let inc = Hyperliquid.Hyperliquid_instruments_feed.get_qty_increment "BTC" in
  Alcotest.(check bool) "stub returns none" true (inc = None)

let test_get_qty_min () =
  let inc = Hyperliquid.Hyperliquid_instruments_feed.get_qty_min "BTC" in
  Alcotest.(check bool) "stub returns none" true (inc = None)

let () =
  Alcotest.run "Hyperliquid Instruments Feed" [
    "operations", [
      Alcotest.test_case "get_price_increment" `Quick test_get_price_increment;
      Alcotest.test_case "get_qty_increment" `Quick test_get_qty_increment;
      Alcotest.test_case "get_qty_min" `Quick test_get_qty_min;
    ];
  ]
