let test_round_price_to_tick () =
  (* Since we test without a fully loaded pair cache, it should fallback to 5 sig figs / 6 dec max *)
  Alcotest.(check (float 0.000001)) "round up" 123.46 (Hyperliquid.Instruments_feed.round_price_to_tick 123.456);
  Alcotest.(check (float 0.000001)) "round down" 123.45 (Hyperliquid.Instruments_feed.round_price_to_tick 123.454);
  Alcotest.(check (float 0.000001)) "small number" 0.0012345 (Hyperliquid.Instruments_feed.round_price_to_tick 0.00123449)

let test_round_qty_to_lot () =
  (* Fallback (unknown symbol) doesn't round *)
  Alcotest.(check (float 0.000001)) "no round for unknown" 1.23456 (Hyperliquid.Instruments_feed.round_qty_to_lot "UNKNOWN" 1.23456)

let () =
  Alcotest.run "Hyperliquid Instruments Feed" [
    "rounding", [
      Alcotest.test_case "round_price_to_tick" `Quick test_round_price_to_tick;
      Alcotest.test_case "round_qty_to_lot" `Quick test_round_qty_to_lot;
    ]
  ]
