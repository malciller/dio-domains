let test_parse_l2book_for_ticker () =
  let valid_json_str = "{
    \"channel\": \"l2Book\",
    \"data\": {
      \"coin\": \"BTC\",
      \"time\": 1670000000000.0,
      \"levels\": [
        [{\"px\": \"50000.0\", \"sz\": \"1.0\", \"n\": 1}],
        [{\"px\": \"50005.0\", \"sz\": \"2.0\", \"n\": 1}]
      ]
    }
  }" in
  let valid_json = Yojson.Safe.from_string valid_json_str in
  
  match Hyperliquid.Hyperliquid_ticker_feed.parse_l2book_for_ticker valid_json with
  | Some (symbol, ticker) ->
      Alcotest.(check string) "symbol parsed" "BTC" symbol;
      Alcotest.(check (float 0.01)) "bid parsed" 50000.0 ticker.bid;
      Alcotest.(check (float 0.01)) "ask parsed" 50005.0 ticker.ask;
      Alcotest.(check (float 0.01)) "time scaled to seconds" 1670000000.0 ticker.timestamp
  | None -> Alcotest.fail "Failed to parse valid l2Book JSON"

let test_parse_invalid_l2book () =
  let invalid_json_str = "{
    \"channel\": \"l2Book\",
    \"data\": {
      \"coin\": \"BTC\",
      \"time\": 1670000000000,
      \"levels\": [
        [], []
      ]
    }
  }" in
  let invalid_json = Yojson.Safe.from_string invalid_json_str in
  
  match Hyperliquid.Hyperliquid_ticker_feed.parse_l2book_for_ticker invalid_json with
  | Some _ -> Alcotest.fail "Should not extract ticker when levels are empty"
  | None -> Alcotest.(check bool) "Gracefully rejected empty levels" true true

let test_ignore_other_channels () =
  let other_msg = Yojson.Safe.from_string "{ \"channel\": \"trades\" }" in
  match Hyperliquid.Hyperliquid_ticker_feed.parse_l2book_for_ticker other_msg with
  | Some _ -> Alcotest.fail "Should ignore non-l2Book channels"
  | None -> Alcotest.(check bool) "Ignored other channel correctly" true true

let () =
  Alcotest.run "Hyperliquid Ticker Feed" [
    "parsing", [
      Alcotest.test_case "valid_l2book" `Quick test_parse_l2book_for_ticker;
      Alcotest.test_case "invalid_l2book" `Quick test_parse_invalid_l2book;
      Alcotest.test_case "ignore_other_channels" `Quick test_ignore_other_channels;
    ];
  ]
