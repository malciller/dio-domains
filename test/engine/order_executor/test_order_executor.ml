module Test = Dio_engine.Order_executor.Test

let test_generate_duplicate_key () =
  let key1 = Test.generate_duplicate_key "BTC/USD" "buy" 0.001 (Some 50000.0) in
  let key2 = Test.generate_duplicate_key "BTC/USD" "buy" 0.001 (Some 50000.0) in
  let key3 = Test.generate_duplicate_key "BTC/USD" "sell" 0.001 (Some 50000.0) in
  let key6 = Test.generate_duplicate_key "BTC/USD" "buy" 0.001 None in

  Alcotest.(check string) "identical orders generate same key" key1 key2;
  Alcotest.(check string) "limit order key format correct" "BTC/USD|buy|0.00100000|50000.00000000" key1;
  Alcotest.(check string) "market order key format correct" "BTC/USD|buy|0.00100000|market" key6;
  Alcotest.(check bool) "different sides generate different keys" true (key1 <> key3)

let test_in_flight_orders () =
  let key1 = "test_key_1" in
  let key2 = "test_key_2" in

  Alcotest.(check bool) "can add new order to cache" true (Test.InFlightOrders.add_in_flight_order key1);
  Alcotest.(check bool) "duplicate order rejected" false (Test.InFlightOrders.add_in_flight_order key1);
  Alcotest.(check bool) "different order key accepted" true (Test.InFlightOrders.add_in_flight_order key2);
  Alcotest.(check bool) "order successfully removed" true (Test.InFlightOrders.remove_in_flight_order key1);
  Alcotest.(check bool) "order can be added again after removal" true (Test.InFlightOrders.add_in_flight_order key1);

  (* Clean up *)
  ignore (Test.InFlightOrders.remove_in_flight_order key1);
  ignore (Test.InFlightOrders.remove_in_flight_order key2)

let test_validate_order_request () =
  (* Test valid order *)
  let valid_request = {
    Dio_engine.Order_executor.order_type = "limit";
    side = "buy";
    quantity = 0.001;
    symbol = "BTC/USD";
    limit_price = Some 50000.0;
    time_in_force = None;
    post_only = None;
    margin = None;
    reduce_only = None;
    order_userref = None;
    cl_ord_id = None;
    trigger_price = None;
    trigger_price_type = None;
    display_qty = None;
    fee_preference = None;
    duplicate_key = "test_key";
  } in
  Alcotest.(check (result unit string)) "valid order accepted" (Ok ()) (Dio_engine.Order_executor.validate_order_request valid_request);

  (* Test invalid order - zero quantity *)
  let invalid_request = { valid_request with quantity = 0.0 } in
  match Dio_engine.Order_executor.validate_order_request invalid_request with
  | Error _ -> Alcotest.(check bool) "zero quantity correctly rejected" true true
  | Ok _ -> Alcotest.fail "zero quantity should be rejected"

let () =
  Alcotest.run "Order Executor" [
    "duplicate_key", [
      Alcotest.test_case "generation" `Quick test_generate_duplicate_key;
    ];
    "in_flight_orders", [
      Alcotest.test_case "cache operations" `Quick test_in_flight_orders;
    ];
    "validation", [
      Alcotest.test_case "order request validation" `Quick test_validate_order_request;
    ];
  ]
