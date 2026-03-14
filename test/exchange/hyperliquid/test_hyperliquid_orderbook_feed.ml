let test_parse_l2book () =
  let valid_json_str = "{
    \"channel\": \"l2Book\",
    \"data\": {
      \"coin\": \"BTC\",
      \"time\": 1670000000000.0,
      \"levels\": [
        [{\"px\": \"50000.0\", \"sz\": \"1.0\", \"n\": 1}],
        [{\"px\": \"50005.0\", \"sz\": \"2.0\", \"n\": 1}, {\"px\": \"50010.0\", \"sz\": \"3.0\", \"n\": 1}]
      ]
    }
  }" in
  let valid_json = Yojson.Safe.from_string valid_json_str in
  
  match Hyperliquid.Hyperliquid_orderbook_feed.parse_l2book valid_json with
  | Some (symbol, book) ->
      Alcotest.(check string) "symbol parsed" "BTC" symbol;
      Alcotest.(check int) "bids length" 1 (Array.length book.bids);
      Alcotest.(check int) "asks length" 2 (Array.length book.asks);
      
      Alcotest.(check string) "best bid px" "50000.0" book.bids.(0).price;
      Alcotest.(check string) "best bid sz" "1.0" book.bids.(0).size;
      
      Alcotest.(check string) "best ask px" "50005.0" book.asks.(0).price;
      Alcotest.(check string) "best ask sz" "2.0" book.asks.(0).size;
      
      Alcotest.(check (float 0.01)) "timestamp" 1670000000.0 book.timestamp
  | None -> Alcotest.fail "Failed to parse valid full l2Book JSON"

let test_parse_invalid_l2book () =
  let invalid_json_str = "{
    \"channel\": \"l2Book\",
    \"data\": {
      \"coin\": \"BTC\",
      \"time\": 1670000000000,
      \"levels\": [ [] ]
    }
  }" in
  let invalid_json = Yojson.Safe.from_string invalid_json_str in
  
  match Hyperliquid.Hyperliquid_orderbook_feed.parse_l2book invalid_json with
  | Some _ -> Alcotest.fail "Should not parse malformed levels array"
  | None -> Alcotest.(check bool) "Gracefully rejected missing level component" true true

let test_ignore_other_channels () =
  let other_msg = Yojson.Safe.from_string "{ \"channel\": \"pong\" }" in
  match Hyperliquid.Hyperliquid_orderbook_feed.parse_l2book other_msg with
  | Some _ -> Alcotest.fail "Should ignore non-l2Book channels"
  | None -> Alcotest.(check bool) "Ignored other channel correctly" true true

let () =
  Alcotest.run "Hyperliquid Orderbook Feed" [
    "parsing", [
      Alcotest.test_case "valid_l2book" `Quick test_parse_l2book;
      Alcotest.test_case "invalid_l2book" `Quick test_parse_invalid_l2book;
      Alcotest.test_case "ignore_other_channels" `Quick test_ignore_other_channels;
    ];
  ]
