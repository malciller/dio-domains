(* Create sample strategy orders for testing *)
let create_test_order ?(operation=Dio_strategies.Strategy_common.Place)
    ?(order_id=None) ?(symbol="BTC/USD") ?(side=Dio_strategies.Strategy_common.Buy)
    ?(order_type="limit") ?(qty=0.001) ?(price=Some 50000.0)
    ?(time_in_force="GTC") ?(post_only=true) ?(userref=Some 2) ?(strategy="MM") () =
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
  Alcotest.(check string) "strategy" "MM" order.strategy

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

let test_ringbuffer_creation () =
  (* Test OrderRingBuffer creation *)
  let buffer = Dio_strategies.Strategy_common.OrderRingBuffer.create 10 in

  (* Check that buffer was created with correct size *)
  Alcotest.(check int) "ringbuffer initial size" 0 (Dio_strategies.Strategy_common.OrderRingBuffer.size buffer)

let test_ringbuffer_basic_write_read () =
  (* Test basic write/read operations *)
  let buffer = Dio_strategies.Strategy_common.OrderRingBuffer.create 5 in
  let order = create_test_order () in

  (* Write an order *)
  let write_result = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order in

  (* Should succeed *)
  Alcotest.(check (option unit)) "write success" (Some ()) write_result;

  (* Read the order back *)
  let read_result = Dio_strategies.Strategy_common.OrderRingBuffer.read buffer in
  Alcotest.(check bool) "read order success" true (Option.is_some read_result);
  let read_order = Option.get read_result in
  Alcotest.(check bool) "read order operation" true (read_order.operation = order.operation);
  Alcotest.(check string) "read order symbol" order.symbol read_order.symbol

let test_ringbuffer_empty_read () =
  (* Test reading from empty buffer *)
  let buffer = Dio_strategies.Strategy_common.OrderRingBuffer.create 5 in

  let read_result = Dio_strategies.Strategy_common.OrderRingBuffer.read buffer in

  Alcotest.(check bool) "empty read" true (Option.is_none read_result)

let test_ringbuffer_full_write () =
  (* Test writing to full buffer *)
  let buffer = Dio_strategies.Strategy_common.OrderRingBuffer.create 3 in
  let order = create_test_order () in

  (* Fill the buffer *)
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order in
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order in
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order in

  (* Try to write to full buffer *)
  let write_result = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order in

  Alcotest.(check (option unit)) "full buffer write" None write_result

let test_ringbuffer_size () =
  (* Test size calculation *)
  let buffer = Dio_strategies.Strategy_common.OrderRingBuffer.create 5 in
  let order = create_test_order () in

  (* Initially empty *)
  Alcotest.(check int) "initial size" 0 (Dio_strategies.Strategy_common.OrderRingBuffer.size buffer);

  (* Add one item *)
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order in
  Alcotest.(check int) "size after write" 1 (Dio_strategies.Strategy_common.OrderRingBuffer.size buffer);

  (* Read it back *)
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.read buffer in
  Alcotest.(check int) "size after read" 0 (Dio_strategies.Strategy_common.OrderRingBuffer.size buffer)

let test_ringbuffer_batch_read () =
  (* Test batch reading *)
  let buffer = Dio_strategies.Strategy_common.OrderRingBuffer.create 5 in
  let order1 = create_test_order ~symbol:"BTC/USD" () in
  let order2 = create_test_order ~symbol:"ETH/USD" () in
  let order3 = create_test_order ~symbol:"LTC/USD" () in

  (* Write three orders *)
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order1 in
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order2 in
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order3 in

  (* Batch read with limit of 2 *)
  let batch = Dio_strategies.Strategy_common.OrderRingBuffer.read_batch buffer 2 in

  Alcotest.(check int) "batch size" 2 (List.length batch);
  (* Should have read 2 items, leaving 1 in buffer *)
  Alcotest.(check int) "remaining size" 1 (Dio_strategies.Strategy_common.OrderRingBuffer.size buffer)

let test_ringbuffer_batch_read_empty () =
  (* Test batch reading from empty buffer *)
  let buffer = Dio_strategies.Strategy_common.OrderRingBuffer.create 5 in

  let batch = Dio_strategies.Strategy_common.OrderRingBuffer.read_batch buffer 10 in

  Alcotest.(check bool) "empty batch" true (batch = [])

let test_ringbuffer_wraparound () =
  (* Test buffer wraparound behavior *)
  let buffer = Dio_strategies.Strategy_common.OrderRingBuffer.create 3 in
  let order = create_test_order () in

  (* Fill buffer *)
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order in
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order in
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order in

  (* Read 2 items, leaving space for 2 more *)
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.read buffer in
  let _ = Dio_strategies.Strategy_common.OrderRingBuffer.read buffer in

  (* Write 2 more items (should wrap around) *)
  let write1 = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order in
  let write2 = Dio_strategies.Strategy_common.OrderRingBuffer.write buffer order in

  (* Read all remaining items *)
  let read1 = Dio_strategies.Strategy_common.OrderRingBuffer.read buffer in
  let read2 = Dio_strategies.Strategy_common.OrderRingBuffer.read buffer in

  (* Try to read one more time - should fail (buffer empty) *)
  let read3 = Dio_strategies.Strategy_common.OrderRingBuffer.read buffer in

  Alcotest.(check (option unit)) "wraparound write1" (Some ()) write1;
  Alcotest.(check (option unit)) "wraparound write2" (Some ()) write2;
  Alcotest.(check bool) "wraparound read1 success" true (Option.is_some read1);
  Alcotest.(check bool) "wraparound read2 success" true (Option.is_some read2);
  Alcotest.(check bool) "wraparound read3 empty" true (Option.is_none read3)

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
    "ringbuffer", [
      Alcotest.test_case "ringbuffer creation" `Quick test_ringbuffer_creation;
      Alcotest.test_case "basic write/read" `Quick test_ringbuffer_basic_write_read;
      Alcotest.test_case "empty read" `Quick test_ringbuffer_empty_read;
      Alcotest.test_case "full write" `Quick test_ringbuffer_full_write;
      Alcotest.test_case "size calculation" `Quick test_ringbuffer_size;
      Alcotest.test_case "batch read" `Quick test_ringbuffer_batch_read;
      Alcotest.test_case "batch read empty" `Quick test_ringbuffer_batch_read_empty;
      Alcotest.test_case "wraparound" `Quick test_ringbuffer_wraparound;
    ];
  ]