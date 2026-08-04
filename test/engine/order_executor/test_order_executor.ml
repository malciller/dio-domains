open Alcotest

let test_generate_duplicate_key () =
  let key1 = Dio_engine.Order_executor.generate_duplicate_key "BTC/USD" "buy" 0.001 (Some 50000.0) in
  check string "limit order key" "BTC/USD|buy|0.001|50000." key1;

  let key2 = Dio_engine.Order_executor.generate_duplicate_key "ETH/USD" "sell" 1.5 None in
  check string "market order key" "ETH/USD|sell|1.5|market" key2

let test_validate_order_request () =
  let valid_request : Dio_engine.Order_executor.order_request = {
    exchange = "kraken";
    order_type = "limit";
    side = "buy";
    quantity = 0.01;
    symbol = "BTC/USD";
    limit_price = Some 50000.0;
    time_in_force = Some "GTC";
    post_only = Some true;
    margin = None;
    reduce_only = None;
    order_userref = Some 2;
    cl_ord_id = None;
    trigger_price = None;
    trigger_price_type = None;
    display_qty = None;
    fee_preference = None;
    duplicate_key = "BTC/USD|buy|0.01|50000.";
  } in
  check (result unit string) "valid order request" (Ok ()) (Dio_engine.Order_executor.validate_order_request valid_request);

  let empty_exchange = { valid_request with exchange = "" } in
  check bool "empty exchange invalid" true (Result.is_error (Dio_engine.Order_executor.validate_order_request empty_exchange));

  let zero_qty = { valid_request with quantity = 0.0 } in
  check bool "zero qty invalid" true (Result.is_error (Dio_engine.Order_executor.validate_order_request zero_qty));

  let missing_price = { valid_request with limit_price = None } in
  check bool "limit order without price invalid" true (Result.is_error (Dio_engine.Order_executor.validate_order_request missing_price))

let test_validate_cancel_request () =
  let valid_cancel : Dio_engine.Order_executor.cancel_request = {
    exchange = "kraken";
    order_ids = Some ["order123"];
    cl_ord_ids = None;
    order_userrefs = None;
    symbol = Some "BTC/USD";
  } in
  check (result unit string) "valid cancel request" (Ok ()) (Dio_engine.Order_executor.validate_cancel_request valid_cancel);

  let no_ids : Dio_engine.Order_executor.cancel_request = {
    exchange = "kraken";
    order_ids = None;
    cl_ord_ids = None;
    order_userrefs = None;
    symbol = Some "BTC/USD";
  } in
  check bool "no identifiers invalid" true (Result.is_error (Dio_engine.Order_executor.validate_cancel_request no_ids))

let test_duplicate_detection () =
  let key = "TEST_DUP|buy|0.1|1000." in
  (* Ensure clean slate *)
  ignore (Dio_engine.Order_executor.InFlightOrders.remove_in_flight_order key);

  let added = Dio_engine.Order_executor.InFlightOrders.add_in_flight_order key in
  check bool "first add returns true" true added;

  let is_inflight = Dio_engine.Order_executor.InFlightOrders.is_in_flight key in
  check bool "is_in_flight returns true" true is_inflight;

  let added_again = Dio_engine.Order_executor.InFlightOrders.add_in_flight_order key in
  check bool "duplicate add returns false" false added_again;

  let removed = Dio_engine.Order_executor.InFlightOrders.remove_in_flight_order key in
  check bool "remove returns true" true removed;

  let is_inflight_after = Dio_engine.Order_executor.InFlightOrders.is_in_flight key in
  check bool "is_in_flight returns false after remove" false is_inflight_after

let () =
  run "Order Executor" [
    "duplicate_key", [
      test_case "generate key" `Quick test_generate_duplicate_key;
    ];
    "validation", [
      test_case "validate order request" `Quick test_validate_order_request;
      test_case "validate cancel request" `Quick test_validate_cancel_request;
    ];
    "in_flight_orders", [
      test_case "duplicate detection tracking" `Quick test_duplicate_detection;
    ];
  ]
