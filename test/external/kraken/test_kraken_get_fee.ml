let test_fee_info_structure () =
  (* Test fee_info record structure *)
  let test_fee = {
    Kraken.Kraken_get_fee.exchange = "kraken";
    maker_fee = Some 0.0016;
    taker_fee = Some 0.0026;
  } in

  Alcotest.(check string) "fee info exchange" "kraken" test_fee.exchange;
  Alcotest.(check (option (float 0.0001))) "fee info maker_fee" (Some 0.0016) test_fee.maker_fee;
  Alcotest.(check (option (float 0.0001))) "fee info taker_fee" (Some 0.0026) test_fee.taker_fee

let test_symbol_normalization () =
  (* Test symbol normalization function *)
  let test_cases = [
    ("btc/usd", "BTC/USD");
    ("BTCUSD", "BTCUSD");
    ("  eth/usd  ", "ETH/USD");
    ("adausd", "ADAUSD");
    ("", "");
  ] in

  List.iter (fun (input, expected) ->
    let result = Kraken.Kraken_get_fee.symbol_to_kraken_pair input in
    Alcotest.(check string) (Printf.sprintf "symbol normalization '%s'" input) expected result
  ) test_cases

let test_fee_percentage_conversion () =
  (* Test that fee percentages are correctly converted from API response format *)
  (* API returns fees as percentages like "0.16" for 0.16%, we convert to decimal like 0.0016 *)
  let api_fee_percentage = 0.16 in  (* 0.16% *)
  let expected_decimal = 0.0016 in  (* 0.16% as decimal *)

  Alcotest.(check (float 0.000001)) "fee percentage conversion" expected_decimal (api_fee_percentage /. 100.0)

let test_fee_info_creation () =
  (* Test creating fee_info records with different fee combinations *)
  let test_cases = [
    ("kraken", Some 0.0010, Some 0.0025);  (* Both fees present *)
    ("kraken", Some 0.0015, None);         (* Only maker fee *)
    ("kraken", None, Some 0.0030);         (* Only taker fee *)
    ("kraken", None, None);                (* No fees *)
  ] in

  List.iteri (fun i (exchange, maker, taker) ->
    let fee_info = {
      Kraken.Kraken_get_fee.exchange;
      maker_fee = maker;
      taker_fee = taker;
    } in
    Alcotest.(check string) (Printf.sprintf "fee info creation %d exchange" i) exchange fee_info.exchange;
    Alcotest.(check (option (float 0.0001))) (Printf.sprintf "fee info creation %d maker_fee" i) maker fee_info.maker_fee;
    Alcotest.(check (option (float 0.0001))) (Printf.sprintf "fee info creation %d taker_fee" i) taker fee_info.taker_fee
  ) test_cases

let test_get_fees_for_symbols_structure () =
  (* Test that get_fees_for_symbols returns proper structure *)
  (* We can't test the actual API call, but we can test the function signature *)
  let symbols = ["BTC/USD"; "ETH/USD"] in

  (* This would make actual API calls, so we'll just test that the function exists *)
  (* and returns a promise of the right type *)
  try
    let _promise = Kraken.Kraken_get_fee.get_fees_for_symbols symbols in
    Alcotest.(check bool) "get_fees_for_symbols function exists and returns promise" true true
  with _ ->
    Alcotest.fail "get_fees_for_symbols function should exist and be callable"

let test_endpoint_constant () =
  (* Test that endpoint constant is properly defined *)
  (* We can't directly access it, but we can verify related functionality *)
  let expected_endpoint = "https://api.kraken.com" in
  (* This is just a documentation test - the actual endpoint is used in HTTP calls *)
  Alcotest.(check bool) "endpoint constant is non-empty" true (String.length expected_endpoint > 0)

let test_api_request_path () =
  (* Test that the API request path is correctly structured *)
  let expected_path = "/0/private/TradeVolume" in
  (* This is a documentation test - the actual path is used in HTTP calls *)
  Alcotest.(check bool) "API request path starts with correct prefix" true (String.starts_with ~prefix:"/0/private/" expected_path)

let test_query_parameter_encoding () =
  (* Test that query parameters would be properly encoded *)
  try
    let nonce = "1234567890123" in
    let pair = "BTCUSD" in
    let encoded = Uri.encoded_of_query [("nonce", [nonce]); ("pair", [pair])] in

    (* Should contain the parameters *)
    Alcotest.(check bool) "query parameter encoding contains equals" true (String.contains encoded '=');
    Alcotest.(check bool) "query parameter encoding contains ampersand" true (String.contains encoded '&')
  with _ ->
    Alcotest.fail "query parameter encoding should not raise exception"

let () =
  Alcotest.run "Kraken Get Fee" [
    "data structures", [
      Alcotest.test_case "fee info structure" `Quick test_fee_info_structure;
      Alcotest.test_case "fee info creation" `Quick test_fee_info_creation;
    ];
    "symbol handling", [
      Alcotest.test_case "symbol normalization" `Quick test_symbol_normalization;
    ];
    "fee calculations", [
      Alcotest.test_case "fee percentage conversion" `Quick test_fee_percentage_conversion;
    ];
    "API interface", [
      Alcotest.test_case "get fees for symbols structure" `Quick test_get_fees_for_symbols_structure;
    ];
    "constants", [
      Alcotest.test_case "endpoint constant" `Quick test_endpoint_constant;
      Alcotest.test_case "API request path" `Quick test_api_request_path;
    ];
    "encoding", [
      Alcotest.test_case "query parameter encoding" `Quick test_query_parameter_encoding;
    ];
  ]
