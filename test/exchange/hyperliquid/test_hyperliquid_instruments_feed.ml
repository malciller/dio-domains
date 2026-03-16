let test_round_price_to_tick () =
  (* Since we test without a fully loaded pair cache, it should fallback to 5 sig figs / 6 dec max *)
  Alcotest.(check (float 0.000001)) "round up" 123.46 (Hyperliquid.Instruments_feed.round_price_to_tick 123.456);
  Alcotest.(check (float 0.000001)) "round down" 123.45 (Hyperliquid.Instruments_feed.round_price_to_tick 123.454);
  Alcotest.(check (float 0.000001)) "small number" 0.0012345 (Hyperliquid.Instruments_feed.round_price_to_tick 0.00123449);
  
  (* Test the identical rounding edge case from order_executor loop bug *)
  let stored_price = 70.4999900000 in
  let requested_price = 70.5000000000 in
  let tick = 0.0000100000 in
  
  (* The diff is precisely the tick size in this edge case, so a 5 sig fig round might not catch the problem if precision rules differ *)
  (* For HYPE/USDC mock rules where szDecimals is 5, price decimal cap is 5 *)
  
  let rounded_stored = Hyperliquid.Instruments_feed.round_price_to_tick stored_price in
  let rounded_requested = Hyperliquid.Instruments_feed.round_price_to_tick requested_price in
  
  Alcotest.(check (float 0.000001)) "70.5 sig figs match" 70.500 rounded_requested;
  
  (* We assert that for this specific case, rounding the stored vs requested price produces values that are closer together than the tick size, 
     which is what `diff < 0.000000001` checks effectively for equality *)
  Alcotest.(check bool) "diff is correctly minimized" true (abs_float (rounded_requested -. rounded_stored) < tick);
  
  (* In the actual feed, round_price_to_tick_for_symbol rounds to max(szDecimals - 1, MIN_DECIMALS) *)
  (* Ensure that applying the same function twice yields no further difference *)
  let r1 = Hyperliquid.Instruments_feed.round_price_to_tick 123.456 in
  let r2 = Hyperliquid.Instruments_feed.round_price_to_tick r1 in
  Alcotest.(check (float 0.000000001)) "idempotency" r1 r2

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
