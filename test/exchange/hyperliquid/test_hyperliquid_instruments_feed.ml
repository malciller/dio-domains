let test_get_price_increment () =
  (* We just initialize to test but price_increment is static *)
  let inc = Hyperliquid.Hyperliquid_instruments_feed.get_price_increment "BTC" in
  Alcotest.(check bool) "price increment is fetched" true (Option.is_some inc)

let test_get_qty_increment () =
  Lwt_main.run (Hyperliquid.Hyperliquid_instruments_feed.initialize_symbols ~testnet:true ["BTC"]);
  let inc = Hyperliquid.Hyperliquid_instruments_feed.get_qty_increment "BTC" in
  Alcotest.(check bool) "qty increment is fetched and not none" true (Option.is_some inc);
  Alcotest.(check bool) "qty increment matches expected size decimal (0.00001)" true
    (match inc with Some v -> Float.abs (v -. 0.00001) < 1e-9 | None -> false)

let test_get_qty_min () =
  (* Initialization should be cached from above *)
  let inc = Hyperliquid.Hyperliquid_instruments_feed.get_qty_min "BTC" in
  Alcotest.(check bool) "qty min is fetched and not none" true (Option.is_some inc)

let () =
  Alcotest.run "Hyperliquid Instruments Feed" [
    "operations", [
      Alcotest.test_case "get_price_increment" `Quick test_get_price_increment;
      Alcotest.test_case "get_qty_increment" `Quick test_get_qty_increment;
      Alcotest.test_case "get_qty_min" `Quick test_get_qty_min;
    ];
  ]
