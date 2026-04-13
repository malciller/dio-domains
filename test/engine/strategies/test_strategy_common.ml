(* Create sample strategy orders for testing *)
let create_test_order ?(operation=Dio_strategies.Strategy_common.Place)
    ?(order_id=None) ?(symbol="BTC/USD") ?(side=Dio_strategies.Strategy_common.Buy)
    ?(order_type="limit") ?(qty=0.001) ?(price=Some 50000.0)
    ?(time_in_force="GTC") ?(post_only=true) ?(userref=Some 2) ?(strategy=Dio_strategies.Strategy_common.MM) 
    ?duplicate_key () =
  let duplicate_key = match duplicate_key with
    | Some k -> k
    | None -> Dio_strategies.Strategy_common.generate_duplicate_key symbol (Dio_strategies.Strategy_common.string_of_order_side side) qty price
  in
  {
    Dio_strategies.Strategy_common.operation;
    order_id;
    symbol;
    side;
    order_type;
    qty;
    price;
    time_in_force;
    post_only;
    userref;
    strategy;
    duplicate_key;
    exchange = "kraken";
  }

let test_order_side_conversion () =
  (* Test order_side to string conversion *)
  Alcotest.(check string) "buy side conversion" "buy"
    (Dio_strategies.Strategy_common.string_of_order_side Dio_strategies.Strategy_common.Buy);
  Alcotest.(check string) "sell side conversion" "sell"
    (Dio_strategies.Strategy_common.string_of_order_side Dio_strategies.Strategy_common.Sell)

let test_operation_type_conversion () =
  (* Test operation_type to string conversion *)
  Alcotest.(check string) "place operation conversion" "place"
    (Dio_strategies.Strategy_common.string_of_operation_type Dio_strategies.Strategy_common.Place);
  Alcotest.(check string) "amend operation conversion" "amend"
    (Dio_strategies.Strategy_common.string_of_operation_type Dio_strategies.Strategy_common.Amend);
  Alcotest.(check string) "cancel operation conversion" "cancel"
    (Dio_strategies.Strategy_common.string_of_operation_type Dio_strategies.Strategy_common.Cancel)

let test_strategy_order_creation () =
  (* Test strategy order record creation *)
  let order = create_test_order ~userref:(Some 12345) () in

  Alcotest.(check bool) "order operation" true (order.operation = Dio_strategies.Strategy_common.Place);
  Alcotest.(check (option string)) "order id" None order.order_id;
  Alcotest.(check string) "order symbol" "BTC/USD" order.symbol;
  Alcotest.(check bool) "order side" true (order.side = Dio_strategies.Strategy_common.Buy);
  Alcotest.(check string) "order type" "limit" order.order_type;
  Alcotest.(check (float 0.001)) "order qty" 0.001 order.qty;
  Alcotest.(check (option (float 0.001))) "order price" (Some 50000.0) order.price;
  Alcotest.(check string) "time in force" "GTC" order.time_in_force;
  Alcotest.(check bool) "post only" true order.post_only;
  Alcotest.(check (option int)) "userref" (Some 12345) order.userref;
  Alcotest.(check bool) "strategy" true (order.strategy = Dio_strategies.Strategy_common.MM)

let test_strategy_order_variants () =
  (* Test different strategy order variants *)
  let place_order = create_test_order ~operation:Dio_strategies.Strategy_common.Place () in
  let amend_order = create_test_order ~operation:Dio_strategies.Strategy_common.Amend
    ~order_id:(Some "order123") ~price:(Some 51000.0) () in
  let cancel_order = create_test_order ~operation:Dio_strategies.Strategy_common.Cancel
    ~order_id:(Some "order456") ~qty:0.0 ~price:None () in

  Alcotest.(check bool) "place order operation" true (place_order.operation = Dio_strategies.Strategy_common.Place);
  Alcotest.(check bool) "amend order operation" true (amend_order.operation = Dio_strategies.Strategy_common.Amend);
  Alcotest.(check (option string)) "amend order id" (Some "order123") amend_order.order_id;
  Alcotest.(check bool) "cancel order operation" true (cancel_order.operation = Dio_strategies.Strategy_common.Cancel);
  Alcotest.(check (option string)) "cancel order id" (Some "order456") cancel_order.order_id;
  Alcotest.(check (option (float 0.001))) "cancel order price" None cancel_order.price

let test_queue_creation () =
  (* Test LockFreeQueue creation *)
  let _ = Dio_strategies.Strategy_common.LockFreeQueue.create () in
  Alcotest.(check bool) "queue created" true true

let test_queue_basic_write_read () =
  (* Test basic write/read operations *)
  let buffer = Dio_strategies.Strategy_common.LockFreeQueue.create () in
  let order = create_test_order () in

  (* Write an order *)
  let write_result = Dio_strategies.Strategy_common.LockFreeQueue.write buffer order in

  (* Should succeed *)
  Alcotest.(check (option unit)) "write success" (Some ()) write_result;

  (* Read the order back *)
  let read_result = Dio_strategies.Strategy_common.LockFreeQueue.read buffer in
  Alcotest.(check bool) "read order success" true (Option.is_some read_result);
  let read_order = Option.get read_result in
  Alcotest.(check bool) "read order operation" true (read_order.operation = order.operation);
  Alcotest.(check string) "read order symbol" order.symbol read_order.symbol

let test_queue_empty_read () =
  (* Test reading from empty queue *)
  let buffer = Dio_strategies.Strategy_common.LockFreeQueue.create () in
  let read_result = Dio_strategies.Strategy_common.LockFreeQueue.read buffer in
  Alcotest.(check bool) "empty read" true (Option.is_none read_result)

let test_queue_batch_read () =
  (* Test batch reading *)
  let buffer = Dio_strategies.Strategy_common.LockFreeQueue.create () in
  let order1 = create_test_order ~symbol:"BTC/USD" () in
  let order2 = create_test_order ~symbol:"ETH/USD" () in
  let order3 = create_test_order ~symbol:"LTC/USD" () in

  (* Write three orders *)
  let _ = Dio_strategies.Strategy_common.LockFreeQueue.write buffer order1 in
  let _ = Dio_strategies.Strategy_common.LockFreeQueue.write buffer order2 in
  let _ = Dio_strategies.Strategy_common.LockFreeQueue.write buffer order3 in

  (* Batch read with limit of 2 *)
  let batch = Dio_strategies.Strategy_common.LockFreeQueue.read_batch buffer 2 in

  Alcotest.(check int) "batch size" 2 (List.length batch)

let test_queue_batch_read_empty () =
  (* Test batch reading from empty queue *)
  let buffer = Dio_strategies.Strategy_common.LockFreeQueue.create () in
  let batch = Dio_strategies.Strategy_common.LockFreeQueue.read_batch buffer 10 in
  Alcotest.(check bool) "empty batch" true (batch = [])

let () =
  Alcotest.run "Strategy Common" [
    "conversions", [
      Alcotest.test_case "order side conversion" `Quick test_order_side_conversion;
      Alcotest.test_case "operation type conversion" `Quick test_operation_type_conversion;
    ];
    "orders", [
      Alcotest.test_case "strategy order creation" `Quick test_strategy_order_creation;
      Alcotest.test_case "strategy order variants" `Quick test_strategy_order_variants;
    ];
    "lockfreequeue", [
      Alcotest.test_case "queue creation" `Quick test_queue_creation;
      Alcotest.test_case "basic write/read" `Quick test_queue_basic_write_read;
      Alcotest.test_case "empty read" `Quick test_queue_empty_read;
      Alcotest.test_case "batch read" `Quick test_queue_batch_read;
      Alcotest.test_case "batch read empty" `Quick test_queue_batch_read_empty;
    ];
  ]