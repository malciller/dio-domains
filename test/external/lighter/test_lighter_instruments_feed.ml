let test_initialize_and_lookup () =
  Lighter.Instruments_feed.initialize ["ETH_INST"; "BTC_INST"];

  let eth = Lighter.Instruments_feed.lookup_info "ETH_INST" in
  Alcotest.(check bool) "ETH found" true (Option.is_some eth);
  (match eth with
   | Some info ->
       Alcotest.(check string) "symbol" "ETH_INST" info.symbol;
       (* Mock defaults from initialize *)
       Alcotest.(check int) "price_decimals" 2 info.supported_price_decimals;
       Alcotest.(check int) "size_decimals" 4 info.supported_size_decimals
   | None -> Alcotest.fail "unreachable");

  let btc = Lighter.Instruments_feed.lookup_info "BTC_INST" in
  Alcotest.(check bool) "BTC found" true (Option.is_some btc);

  let miss = Lighter.Instruments_feed.lookup_info "NOSUCH_INST" in
  Alcotest.(check bool) "miss" true (Option.is_none miss)

let test_get_market_index () =
  Lighter.Instruments_feed.initialize ["MITEST"];
  let idx = Lighter.Instruments_feed.get_market_index ~symbol:"MITEST" in
  Alcotest.(check bool) "found" true (Option.is_some idx);
  let miss = Lighter.Instruments_feed.get_market_index ~symbol:"NOMATCH" in
  Alcotest.(check bool) "miss" true (Option.is_none miss)

let test_get_symbol () =
  (* Mock initialize assigns sequential market_index values starting at 0 *)
  Lighter.Instruments_feed.initialize ["SYM_BACK"];
  let sym = Lighter.Instruments_feed.get_symbol ~market_index:0 in
  Alcotest.(check (option string)) "reverse lookup" (Some "SYM_BACK") sym

let test_get_price_increment () =
  Lighter.Instruments_feed.initialize ["PINC"];
  let incr = Lighter.Instruments_feed.get_price_increment "PINC" in
  Alcotest.(check bool) "found" true (Option.is_some incr);
  (* Mock: supported_price_decimals=2, so increment = 0.01 *)
  Alcotest.(check (option (float 0.000001))) "value" (Some 0.01) incr;
  let miss = Lighter.Instruments_feed.get_price_increment "NOPE_P" in
  Alcotest.(check bool) "miss" true (Option.is_none miss)

let test_get_qty_increment () =
  Lighter.Instruments_feed.initialize ["QINC"];
  let incr = Lighter.Instruments_feed.get_qty_increment "QINC" in
  Alcotest.(check bool) "found" true (Option.is_some incr);
  (* Mock: supported_size_decimals=4, so increment = 0.0001 *)
  Alcotest.(check (option (float 0.000001))) "value" (Some 0.0001) incr;
  let miss = Lighter.Instruments_feed.get_qty_increment "NOPE_Q" in
  Alcotest.(check bool) "miss" true (Option.is_none miss)

let test_get_qty_min () =
  Lighter.Instruments_feed.initialize ["QMIN"];
  let min = Lighter.Instruments_feed.get_qty_min "QMIN" in
  Alcotest.(check bool) "found" true (Option.is_some min);
  (* Mock: min_base_amount = 0.001 *)
  Alcotest.(check (option (float 0.000001))) "value" (Some 0.001) min

let test_round_price_for_symbol () =
  Lighter.Instruments_feed.initialize ["RNDP"];
  (* Mock: supported_price_decimals=2 *)
  let rounded = Lighter.Instruments_feed.round_price_for_symbol "RNDP" 123.456 in
  Alcotest.(check (float 0.000001)) "rounded" 123.46 rounded;
  (* Idempotency *)
  let r2 = Lighter.Instruments_feed.round_price_for_symbol "RNDP" rounded in
  Alcotest.(check (float 0.000000001)) "idempotent" rounded r2;
  (* Unknown symbol — passthrough *)
  let pass = Lighter.Instruments_feed.round_price_for_symbol "UNKNOWN_RND" 123.456 in
  Alcotest.(check (float 0.000001)) "passthrough" 123.456 pass

let test_round_qty_for_symbol () =
  Lighter.Instruments_feed.initialize ["RNDQ"];
  (* Mock: supported_size_decimals=4 → floor to 4 decimals *)
  let rounded = Lighter.Instruments_feed.round_qty_for_symbol "RNDQ" 1.23456789 in
  Alcotest.(check (float 0.000001)) "rounded" 1.2345 rounded;
  (* Idempotency *)
  let r2 = Lighter.Instruments_feed.round_qty_for_symbol "RNDQ" rounded in
  Alcotest.(check (float 0.000000001)) "idempotent" rounded r2

let () =
  Alcotest.run "Lighter Instruments Feed" [
    "lookup", [
      Alcotest.test_case "initialize_and_lookup" `Quick test_initialize_and_lookup;
      Alcotest.test_case "get_market_index" `Quick test_get_market_index;
      Alcotest.test_case "get_symbol" `Quick test_get_symbol;
      Alcotest.test_case "get_price_increment" `Quick test_get_price_increment;
      Alcotest.test_case "get_qty_increment" `Quick test_get_qty_increment;
      Alcotest.test_case "get_qty_min" `Quick test_get_qty_min;
    ];
    "rounding", [
      Alcotest.test_case "round_price_for_symbol" `Quick test_round_price_for_symbol;
      Alcotest.test_case "round_qty_for_symbol" `Quick test_round_qty_for_symbol;
    ]
  ]
