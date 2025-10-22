module OrderExecutor = Dio_engine.Order_executor
module Test = OrderExecutor.Test

(** Test duplicate key generation *)
let test_generate_duplicate_key () =
  let key1 = Test.generate_duplicate_key "BTC/USD" "buy" 0.001 (Some 50000.0) in
  let key2 = Test.generate_duplicate_key "BTC/USD" "buy" 0.001 (Some 50000.0) in
  let key3 = Test.generate_duplicate_key "BTC/USD" "sell" 0.001 (Some 50000.0) in
  let key6 = Test.generate_duplicate_key "BTC/USD" "buy" 0.001 None in

  Alcotest.(check string) "identical orders generate same key" key1 key2;
  Alcotest.(check string) "limit order key format correct" "BTC/USD|buy|0.00100000|50000.00000000" key1;
  Alcotest.(check string) "market order key format correct" "BTC/USD|buy|0.00100000|market" key6;
  Alcotest.(check bool) "different sides generate different keys" true (key1 <> key3)

(** Test in-flight order cache *)
let test_in_flight_orders () =
  let key1 = "test_key_1" in
  let key2 = "test_key_2" in

  (* Initially empty - can add new order *)
  Alcotest.(check bool) "can add new order to cache" true (Test.InFlightOrders.add_in_flight_order key1);

  (* Duplicate order should be rejected *)
  Alcotest.(check bool) "duplicate order rejected" false (Test.InFlightOrders.add_in_flight_order key1);

  (* Different key should work *)
  Alcotest.(check bool) "different order key accepted" true (Test.InFlightOrders.add_in_flight_order key2);

  (* Remove first order *)
  Alcotest.(check bool) "order successfully removed" true (Test.InFlightOrders.remove_in_flight_order key1);

  (* Should be able to add again after removal *)
  Alcotest.(check bool) "order can be added again after removal" true (Test.InFlightOrders.add_in_flight_order key1);

  (* Clean up *)
  ignore (Test.InFlightOrders.remove_in_flight_order key1);
  ignore (Test.InFlightOrders.remove_in_flight_order key2)

let () =
  Alcotest.run "Order Cache" [
    "duplicate_key", [
      Alcotest.test_case "generation" `Quick test_generate_duplicate_key;
    ];
    "in_flight_orders", [
      Alcotest.test_case "cache operations" `Quick test_in_flight_orders;
    ];
  ]
