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

let test_get_subscription_coin () =
  (* Perp symbol - should return base name *)
  Hyperliquid.Instruments_feed.initialize ["HYPE"];
  let coin = Hyperliquid.Instruments_feed.get_subscription_coin "HYPE" in
  Alcotest.(check string) "perp coin" "HYPE" coin

let test_get_asset_index () =
  Hyperliquid.Instruments_feed.initialize ["TESTIDX"];
  let idx = Hyperliquid.Instruments_feed.get_asset_index "TESTIDX" in
  Alcotest.(check bool) "known symbol" true (Option.is_some idx);
  let idx_none = Hyperliquid.Instruments_feed.get_asset_index "NOSYMBOL" in
  Alcotest.(check bool) "unknown symbol" true (Option.is_none idx_none)

let test_lookup_info () =
  Hyperliquid.Instruments_feed.initialize ["BTC"];
  (* Direct match *)
  let direct = Hyperliquid.Instruments_feed.lookup_info "BTC" in
  Alcotest.(check bool) "direct match" true (Option.is_some direct);
  (* BASE/QUOTE stripping to find perp *)
  let stripped = Hyperliquid.Instruments_feed.lookup_info "BTC/USDC" in
  Alcotest.(check bool) "stripped match" true (Option.is_some stripped);
  (* Miss *)
  let miss = Hyperliquid.Instruments_feed.lookup_info "NOSUCH" in
  Alcotest.(check bool) "miss" true (Option.is_none miss)

let test_get_qty_increment () =
  Hyperliquid.Instruments_feed.initialize ["QTYTEST"];
  let incr = Hyperliquid.Instruments_feed.get_qty_increment "QTYTEST" in
  Alcotest.(check bool) "known" true (Option.is_some incr);
  (match incr with
   | Some v -> Alcotest.(check bool) "positive" true (v > 0.0)
   | None -> Alcotest.fail "unreachable");
  let none = Hyperliquid.Instruments_feed.get_qty_increment "NOPE" in
  Alcotest.(check bool) "unknown" true (Option.is_none none)

let test_get_price_increment () =
  let incr = Hyperliquid.Instruments_feed.get_price_increment "any" in
  Alcotest.(check bool) "always Some" true (Option.is_some incr);
  Alcotest.(check (option (float 0.0000001))) "value" (Some 0.00001) incr

let test_round_price_for_symbol () =
  Hyperliquid.Instruments_feed.initialize ["RNDTEST"];
  let rounded = Hyperliquid.Instruments_feed.round_price_to_tick_for_symbol "RNDTEST" 123.456789 in
  (* Should be rounded to some precision *)
  Alcotest.(check bool) "rounded differs or equals" true (rounded > 0.0);
  (* Idempotency *)
  let r2 = Hyperliquid.Instruments_feed.round_price_to_tick_for_symbol "RNDTEST" rounded in
  Alcotest.(check (float 0.000000001)) "idempotent" rounded r2;
  (* Zero/negative passthrough *)
  let zero = Hyperliquid.Instruments_feed.round_price_to_tick_for_symbol "RNDTEST" 0.0 in
  Alcotest.(check (float 0.000001)) "zero" 0.0 zero;
  let neg = Hyperliquid.Instruments_feed.round_price_to_tick_for_symbol "RNDTEST" (-5.0) in
  Alcotest.(check (float 0.000001)) "negative" (-5.0) neg

let () =
  Alcotest.run "Hyperliquid Instruments Feed" [
    "rounding", [
      Alcotest.test_case "round_price_to_tick" `Quick test_round_price_to_tick;
      Alcotest.test_case "round_qty_to_lot" `Quick test_round_qty_to_lot;
      Alcotest.test_case "round_price_for_symbol" `Quick test_round_price_for_symbol;
    ];
    "lookup", [
      Alcotest.test_case "get_subscription_coin" `Quick test_get_subscription_coin;
      Alcotest.test_case "get_asset_index" `Quick test_get_asset_index;
      Alcotest.test_case "lookup_info" `Quick test_lookup_info;
      Alcotest.test_case "get_qty_increment" `Quick test_get_qty_increment;
      Alcotest.test_case "get_price_increment" `Quick test_get_price_increment;
    ]
  ]
