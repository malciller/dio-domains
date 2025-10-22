let test_pair_status_conversions () =
  (* Test pair status string conversions *)
  let test_cases = [
    (Kraken.Kraken_instruments_feed.Online, "online");
    (Kraken.Kraken_instruments_feed.CancelOnly, "cancel_only");
    (Kraken.Kraken_instruments_feed.Delisted, "delisted");
    (Kraken.Kraken_instruments_feed.LimitOnly, "limit_only");
    (Kraken.Kraken_instruments_feed.Maintenance, "maintenance");
    (Kraken.Kraken_instruments_feed.PostOnly, "post_only");
    (Kraken.Kraken_instruments_feed.ReduceOnly, "reduce_only");
    (Kraken.Kraken_instruments_feed.WorkInProgress, "work_in_progress");
    (Kraken.Kraken_instruments_feed.Unknown, "unknown");
  ] in

  List.iter (fun (status, expected_str) ->
    let str = Kraken.Kraken_instruments_feed.status_to_string status in
    let back_to_status = Kraken.Kraken_instruments_feed.status_of_string str in
    Alcotest.(check string) ("status_to_string " ^ expected_str) expected_str str;
    Alcotest.(check bool) ("status_of_string roundtrip for " ^ expected_str) true (back_to_status = status)
  ) test_cases

let test_is_tradeable () =
  (* Test tradeability checking *)
  Alcotest.(check bool) "Online is tradeable" true (Kraken.Kraken_instruments_feed.is_tradeable Kraken.Kraken_instruments_feed.Online);
  Alcotest.(check bool) "CancelOnly is not tradeable" false (Kraken.Kraken_instruments_feed.is_tradeable Kraken.Kraken_instruments_feed.CancelOnly);
  Alcotest.(check bool) "Delisted is not tradeable" false (Kraken.Kraken_instruments_feed.is_tradeable Kraken.Kraken_instruments_feed.Delisted);
  Alcotest.(check bool) "LimitOnly is not tradeable" false (Kraken.Kraken_instruments_feed.is_tradeable Kraken.Kraken_instruments_feed.LimitOnly);
  Alcotest.(check bool) "Maintenance is not tradeable" false (Kraken.Kraken_instruments_feed.is_tradeable Kraken.Kraken_instruments_feed.Maintenance);
  Alcotest.(check bool) "PostOnly is not tradeable" false (Kraken.Kraken_instruments_feed.is_tradeable Kraken.Kraken_instruments_feed.PostOnly);
  Alcotest.(check bool) "ReduceOnly is not tradeable" false (Kraken.Kraken_instruments_feed.is_tradeable Kraken.Kraken_instruments_feed.ReduceOnly);
  Alcotest.(check bool) "WorkInProgress is not tradeable" false (Kraken.Kraken_instruments_feed.is_tradeable Kraken.Kraken_instruments_feed.WorkInProgress);
  Alcotest.(check bool) "Unknown is not tradeable" false (Kraken.Kraken_instruments_feed.is_tradeable Kraken.Kraken_instruments_feed.Unknown)

let test_parse_pair_info () =
  (* Test parsing pair info from JSON *)
  let json_str = {|
    {
      "symbol": "BTC/USD",
      "base": "BTC",
      "quote": "USD",
      "status": "online",
      "qty_precision": 8,
      "qty_increment": 0.00000001,
      "qty_min": 0.0001,
      "price_precision": 1,
      "price_increment": 0.1,
      "cost_precision": 8,
      "cost_min": 0.5,
      "marginable": true,
      "has_index": true
    }
  |} in

  try
    let json = Yojson.Safe.from_string json_str in
    match Kraken.Kraken_instruments_feed.parse_pair_info json with
    | Some info ->
        Alcotest.(check string) "parsed symbol" "BTC/USD" info.symbol;
        Alcotest.(check string) "parsed base" "BTC" info.base;
        Alcotest.(check string) "parsed quote" "USD" info.quote;
        Alcotest.(check bool) "parsed status is Online" true (info.status = Kraken.Kraken_instruments_feed.Online);
        Alcotest.(check int) "parsed qty_precision" 8 info.qty_precision;
        Alcotest.(check (float 0.00000001)) "parsed qty_increment" 0.00000001 info.qty_increment;
        Alcotest.(check (float 0.0001)) "parsed qty_min" 0.0001 info.qty_min;
        Alcotest.(check int) "parsed price_precision" 1 info.price_precision;
        Alcotest.(check (float 0.1)) "parsed price_increment" 0.1 info.price_increment;
        Alcotest.(check int) "parsed cost_precision" 8 info.cost_precision;
        Alcotest.(check (float 0.5)) "parsed cost_min" 0.5 info.cost_min;
        Alcotest.(check bool) "parsed marginable" true info.marginable;
        Alcotest.(check bool) "parsed has_index" true info.has_index;
        Alcotest.(check bool) "parsed last_updated positive" true (info.last_updated > 0.0)
    | None -> Alcotest.fail "parsing returned None"
  with _ ->
    Alcotest.fail "exception during parsing"

let test_parse_pair_info_invalid () =
  (* Test parsing with invalid JSON *)
  let invalid_json_str = {|
    {
      "symbol": "INVALID",
      "missing_required_field": "test"
    }
  |} in

  try
    let json = Yojson.Safe.from_string invalid_json_str in
    match Kraken.Kraken_instruments_feed.parse_pair_info json with
    | None -> Alcotest.(check bool) "invalid JSON parsing returns None" true true
    | Some _ -> Alcotest.fail "should return None for invalid JSON"
  with _ ->
    Alcotest.fail "exception during invalid JSON parsing"

let test_round_to_increment () =
  (* Test rounding to increment *)
  let check_rounding value increment expected desc =
    let result = Kraken.Kraken_instruments_feed.round_to_increment value increment in
    Alcotest.(check (float 0.000001)) desc expected result
  in

  check_rounding 1.23456 0.01 1.23 "round 1.23456 to 0.01 increment";
  check_rounding 1.23999 0.01 1.24 "round 1.23999 to 0.01 increment";
  check_rounding 0.00012345 0.0001 0.0001 "round 0.00012345 to 0.0001 increment";
  check_rounding 10.5 1.0 11.0 "round 10.5 to 1.0 increment";
  check_rounding 0.0 0.1 0.0 "round 0.0 to 0.1 increment"

let test_get_precision_info () =
  (* Test precision info retrieval *)
  (* This function depends on the cache being populated, so we'll test the interface *)
  let symbol = "TEST_SYMBOL" in

  (* Initially should return None since cache is empty *)
  match Kraken.Kraken_instruments_feed.get_precision_info symbol with
  | None -> Alcotest.(check bool) "get_precision_info returns None initially" true true
  | Some _ -> Alcotest.fail "should return None initially"

let test_cache_operations () =
  (* Test basic cache operations *)
  let test_symbol = "CACHE_TEST" in

  (* Initially should not have info *)
  let result = Lwt_main.run (
    Kraken.Kraken_instruments_feed.get_pair_info test_symbol
  ) in
  match result with
  | None -> Alcotest.(check bool) "cache initially empty" true true
  | Some _ -> Alcotest.fail "unexpected initial data in cache"

let test_is_pair_tradeable () =
  (* Test pair tradeability checking *)
  let test_symbol = "TRADEABLE_TEST" in

  (* Initially should not be tradeable since no data *)
  let result = Lwt_main.run (
    Kraken.Kraken_instruments_feed.is_pair_tradeable test_symbol
  ) in
  Alcotest.(check bool) "pair not tradeable initially" false result

let test_pair_info_structure () =
  (* Test pair_info record structure *)
  let test_info = {
    Kraken.Kraken_instruments_feed.symbol = "TEST/USD";
    base = "TEST";
    quote = "USD";
    status = Kraken.Kraken_instruments_feed.Online;
    qty_precision = 8;
    qty_increment = 0.00000001;
    qty_min = 0.0001;
    price_precision = 2;
    price_increment = 0.01;
    cost_precision = 8;
    cost_min = 1.0;
    marginable = true;
    has_index = false;
    last_updated = Unix.time ();
  } in

  Alcotest.(check string) "pair_info symbol" "TEST/USD" test_info.symbol;
  Alcotest.(check string) "pair_info base" "TEST" test_info.base;
  Alcotest.(check string) "pair_info quote" "USD" test_info.quote;
  Alcotest.(check bool) "pair_info status is Online" true (test_info.status = Kraken.Kraken_instruments_feed.Online);
  Alcotest.(check int) "pair_info qty_precision" 8 test_info.qty_precision;
  Alcotest.(check (float 0.00000001)) "pair_info qty_increment" 0.00000001 test_info.qty_increment;
  Alcotest.(check (float 0.0001)) "pair_info qty_min" 0.0001 test_info.qty_min;
  Alcotest.(check int) "pair_info price_precision" 2 test_info.price_precision;
  Alcotest.(check (float 0.01)) "pair_info price_increment" 0.01 test_info.price_increment;
  Alcotest.(check int) "pair_info cost_precision" 8 test_info.cost_precision;
  Alcotest.(check (float 1.0)) "pair_info cost_min" 1.0 test_info.cost_min;
  Alcotest.(check bool) "pair_info marginable" true test_info.marginable;
  Alcotest.(check bool) "pair_info has_index" false test_info.has_index;
  Alcotest.(check bool) "pair_info last_updated positive" true (test_info.last_updated > 0.0)

let () =
  Alcotest.run "Kraken Instruments Feed" [
    "status handling", [
      Alcotest.test_case "pair_status_conversions" `Quick test_pair_status_conversions;
      Alcotest.test_case "is_tradeable" `Quick test_is_tradeable;
    ];
    "json parsing", [
      Alcotest.test_case "parse_pair_info" `Quick test_parse_pair_info;
      Alcotest.test_case "parse_pair_info_invalid" `Quick test_parse_pair_info_invalid;
    ];
    "utilities", [
      Alcotest.test_case "round_to_increment" `Quick test_round_to_increment;
    ];
    "data access", [
      Alcotest.test_case "get_precision_info" `Quick test_get_precision_info;
      Alcotest.test_case "cache_operations" `Quick test_cache_operations;
      Alcotest.test_case "is_pair_tradeable" `Quick test_is_pair_tradeable;
    ];
    "data structures", [
      Alcotest.test_case "pair_info_structure" `Quick test_pair_info_structure;
    ];
  ]
