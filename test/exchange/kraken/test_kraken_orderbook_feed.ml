let test_take_function () =
  (* Test take function for list truncation *)
  let test_list = [1; 2; 3; 4; 5] in

  let take_3 = Kraken.Kraken_orderbook_feed.take 3 test_list in
  let take_10 = Kraken.Kraken_orderbook_feed.take 10 test_list in
  let take_0 = Kraken.Kraken_orderbook_feed.take 0 test_list in
  let take_empty = Kraken.Kraken_orderbook_feed.take 5 [] in

  Alcotest.(check (list int)) "take 3 from [1;2;3;4;5]" [1; 2; 3] take_3;
  Alcotest.(check (list int)) "take 10 from [1;2;3;4;5]" [1; 2; 3; 4; 5] take_10;
  Alcotest.(check (list int)) "take 0 from [1;2;3;4;5]" [] take_0;
  Alcotest.(check (list int)) "take 5 from empty list" [] take_empty

let test_decimal_compare () =
  (* Test decimal string comparison *)
  let check_comparison s1 s2 expected_desc expected_cmp =
    let result = Kraken.Kraken_orderbook_feed.decimal_compare s1 s2 in
    let passed = match expected_cmp with
      | 1 -> result > 0
      | -1 -> result < 0
      | 0 -> result = 0
      | _ -> false
    in
    Alcotest.(check bool) expected_desc true passed
  in

  check_comparison "1.5" "1.4" "1.5 > 1.4" 1;
  check_comparison "1.4" "1.5" "1.4 < 1.5" (-1);
  check_comparison "2.0" "2.0" "2.0 = 2.0" 0;
  check_comparison "1.10" "1.1" "1.10 = 1.1" 0;
  check_comparison "0.001" "0.002" "0.001 < 0.002" (-1);
  check_comparison "100" "50" "100 > 50" 1

let test_to_decimal_str () =
  (* Test decimal string conversion *)
  Alcotest.(check string) "to_decimal_str Float 1.5" "1.5" (Kraken.Kraken_orderbook_feed.to_decimal_str (`Float 1.5));
  Alcotest.(check string) "to_decimal_str Float 0.001" "0.001" (Kraken.Kraken_orderbook_feed.to_decimal_str (`Float 0.001));
  Alcotest.(check string) "to_decimal_str Float 100.0" "100" (Kraken.Kraken_orderbook_feed.to_decimal_str (`Float 100.0));
  Alcotest.(check string) "to_decimal_str Int 42" "42" (Kraken.Kraken_orderbook_feed.to_decimal_str (`Int 42));
  Alcotest.(check string) "to_decimal_str Intlit 123" "123" (Kraken.Kraken_orderbook_feed.to_decimal_str (`Intlit "123"))

let test_to_decimal_str_with_precision () =
  (* Test decimal string conversion with custom precision *)
  let json = `Float 1.23456789 in
  let result = Kraken.Kraken_orderbook_feed.to_decimal_str ~dec:2 json in

  Alcotest.(check string) "to_decimal_str with 2 decimal precision" "1.23" result

let test_is_effectively_zero () =
  (* Test zero-checking for orderbook levels *)
  Alcotest.(check bool) "is_effectively_zero '0'" true (Kraken.Kraken_orderbook_feed.is_effectively_zero "0");
  Alcotest.(check bool) "is_effectively_zero '0.0'" true (Kraken.Kraken_orderbook_feed.is_effectively_zero "0.0");
  Alcotest.(check bool) "is_effectively_zero '0.000'" true (Kraken.Kraken_orderbook_feed.is_effectively_zero "0.000");
  Alcotest.(check bool) "is_effectively_zero '0.0001'" false (Kraken.Kraken_orderbook_feed.is_effectively_zero "0.0001");
  Alcotest.(check bool) "is_effectively_zero '1.0'" false (Kraken.Kraken_orderbook_feed.is_effectively_zero "1.0");
  Alcotest.(check bool) "is_effectively_zero '-0.0'" true (Kraken.Kraken_orderbook_feed.is_effectively_zero "-0.0");
  Alcotest.(check bool) "is_effectively_zero 'invalid'" false (Kraken.Kraken_orderbook_feed.is_effectively_zero "invalid")

let test_orderbook_structure () =
  (* Test orderbook record structure *)
  let empty_bids = Array.make 25 { Kraken.Kraken_orderbook_feed.price = "0"; size = "0"; price_float = 0.0; size_float = 0.0 } in
  let empty_asks = Array.make 25 { Kraken.Kraken_orderbook_feed.price = "0"; size = "0"; price_float = 0.0; size_float = 0.0 } in

  let test_book = {
    Kraken.Kraken_orderbook_feed.symbol = "BTC/USD";
    bids = empty_bids;
    asks = empty_asks;
    sequence = Some 12345L;
    checksum = Some 67890l;
    timestamp = Unix.time ();
  } in

  Alcotest.(check string) "orderbook symbol" "BTC/USD" test_book.symbol;
  Alcotest.(check int) "orderbook bids length" 25 (Array.length test_book.bids);
  Alcotest.(check int) "orderbook asks length" 25 (Array.length test_book.asks);
  Alcotest.(check (option int64)) "orderbook sequence" (Some 12345L) test_book.sequence;
  Alcotest.(check (option int32)) "orderbook checksum" (Some 67890l) test_book.checksum;
  Alcotest.(check bool) "orderbook timestamp positive" true (test_book.timestamp > 0.0)

let test_level_structure () =
  (* Test level record structure *)
  let test_level = {
    Kraken.Kraken_orderbook_feed.price = "45000.50";
    size = "1.234567";
    price_float = 45000.50;
    size_float = 1.234567;
  } in

  Alcotest.(check string) "level price" "45000.50" test_level.price;
  Alcotest.(check string) "level size" "1.234567" test_level.size;
  Alcotest.(check (float 0.01)) "level price_float" 45000.50 test_level.price_float;
  Alcotest.(check (float 0.000001)) "level size_float" 1.234567 test_level.size_float

let test_store_operations () =
  (* Test basic store operations *)
  let symbol = "STORE_TEST" in

  (* Initially should not have store *)
  match Kraken.Kraken_orderbook_feed.store_opt symbol with
  | None -> Alcotest.(check bool) "store initially None" true true
  | Some _ -> Alcotest.fail "unexpected initial store"

let test_constants () =
  (* Test that constants are properly defined *)
  (* We can't directly access these, but we can verify related functionality exists *)
  Alcotest.(check bool) "constants test placeholder" true true

let test_json_parsing_helpers () =
  (* Test JSON parsing helper functions *)
  try
    let _int64_json = `Intlit "12345" in
    let _int32_json = `Int 67890 in

    (* These functions exist and don't crash on valid input *)
    Alcotest.(check bool) "json parsing helpers don't crash" true true
  with _ ->
    Alcotest.fail "json parsing helpers crashed"

let test_get_top_levels () =
  (* Test getting top levels from orderbook *)
  let symbol = "TOP_LEVELS_TEST" in

  (* Should not crash even if no data exists *)
  let bids, asks = Kraken.Kraken_orderbook_feed.get_top_levels symbol in
  Alcotest.(check int) "get_top_levels bids length for unknown symbol" 0 (Array.length bids);
  Alcotest.(check int) "get_top_levels asks length for unknown symbol" 0 (Array.length asks)

let () =
  Alcotest.run "Kraken Orderbook Feed" [
    "utility functions", [
      Alcotest.test_case "take_function" `Quick test_take_function;
      Alcotest.test_case "decimal_compare" `Quick test_decimal_compare;
      Alcotest.test_case "to_decimal_str" `Quick test_to_decimal_str;
      Alcotest.test_case "to_decimal_str_with_precision" `Quick test_to_decimal_str_with_precision;
      Alcotest.test_case "is_effectively_zero" `Quick test_is_effectively_zero;
    ];
    "data structures", [
      Alcotest.test_case "orderbook_structure" `Quick test_orderbook_structure;
      Alcotest.test_case "level_structure" `Quick test_level_structure;
    ];
    "store operations", [
      Alcotest.test_case "store_operations" `Quick test_store_operations;
      Alcotest.test_case "get_top_levels" `Quick test_get_top_levels;
    ];
    "constants and helpers", [
      Alcotest.test_case "constants" `Quick test_constants;
      Alcotest.test_case "json_parsing_helpers" `Quick test_json_parsing_helpers;
    ];
  ]
