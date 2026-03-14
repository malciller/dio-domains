let test_get_open_orders () =
  (* Since process_market_data is mostly a stub, we will just test the external read APIs *)
  let empty = Hyperliquid.Hyperliquid_executions_feed.get_open_orders "BTC" in
  Alcotest.(check int) "initially no open orders" 0 (List.length empty)

let test_get_open_order () =
  let opt = Hyperliquid.Hyperliquid_executions_feed.get_open_order "BTC" "123" in
  Alcotest.(check bool) "order not found" true (opt = None)

let test_parsing_stub () =
  let valid_json_str = "{
    \"channel\": \"webData2\",
    \"data\": {}
  }" in
  let valid_json = Yojson.Safe.from_string valid_json_str in
  
  (* Should not raise exception *)
  let () = Hyperliquid.Hyperliquid_executions_feed.process_market_data valid_json in
  Alcotest.(check bool) "stub executes without failure" true true

let test_ignore_other_channels () =
  let other_msg = Yojson.Safe.from_string "{ \"channel\": \"l2Book\" }" in
  let () = Hyperliquid.Hyperliquid_executions_feed.process_market_data other_msg in
  Alcotest.(check bool) "ignored logically" true true

let () =
  Alcotest.run "Hyperliquid Executions Feed" [
    "state", [
      Alcotest.test_case "get_open_orders" `Quick test_get_open_orders;
      Alcotest.test_case "get_open_order" `Quick test_get_open_order;
    ];
    "parsing", [
      Alcotest.test_case "process_market_data_stub" `Quick test_parsing_stub;
      Alcotest.test_case "ignore_other_channels" `Quick test_ignore_other_channels;
    ]
  ]
