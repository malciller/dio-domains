let test_exec_type_conversions () =
  (* Test exec_type string conversions *)
  let test_cases = [
    (Kraken.Kraken_executions_feed.PendingNew, "pending_new");
    (Kraken.Kraken_executions_feed.New, "new");
    (Kraken.Kraken_executions_feed.Trade, "trade");
    (Kraken.Kraken_executions_feed.PartiallyFilled, "partially_filled");
    (Kraken.Kraken_executions_feed.Filled, "filled");
    (Kraken.Kraken_executions_feed.Canceled, "canceled");
    (Kraken.Kraken_executions_feed.Expired, "expired");
    (Kraken.Kraken_executions_feed.Amended, "amended");
    (Kraken.Kraken_executions_feed.Restated, "restated");
    (Kraken.Kraken_executions_feed.Rejected, "rejected");
    (Kraken.Kraken_executions_feed.Unknown "custom", "custom");
  ] in

  List.iter (fun (exec_type, expected_str) ->
    let str = Kraken.Kraken_executions_feed.string_of_exec_type exec_type in
    let back_to_type = Kraken.Kraken_executions_feed.exec_type_of_string str in
    Alcotest.(check string) (Printf.sprintf "exec_type to string: %s" expected_str) expected_str str;
    Alcotest.(check bool) (Printf.sprintf "exec_type round-trip: %s" expected_str) true (back_to_type = exec_type)
  ) test_cases

let test_side_conversions () =
  (* Test side string conversions *)
  let buy_str = Kraken.Kraken_executions_feed.string_of_side Kraken.Kraken_executions_feed.Buy in
  let sell_str = Kraken.Kraken_executions_feed.string_of_side Kraken.Kraken_executions_feed.Sell in
  let buy_back = Kraken.Kraken_executions_feed.side_of_string "buy" in
  let sell_back = Kraken.Kraken_executions_feed.side_of_string "sell" in
  let invalid_side = Kraken.Kraken_executions_feed.side_of_string "invalid" in

  Alcotest.(check string) "buy side to string" "buy" buy_str;
  Alcotest.(check string) "sell side to string" "sell" sell_str;
  Alcotest.(check bool) "buy string to side" true (buy_back = Kraken.Kraken_executions_feed.Buy);
  Alcotest.(check bool) "sell string to side" true (sell_back = Kraken.Kraken_executions_feed.Sell);
  Alcotest.(check bool) "invalid side defaults to buy" true (invalid_side = Kraken.Kraken_executions_feed.Buy)

let test_order_status_conversions () =
  (* Test order_status string conversions *)
  let test_cases = [
    (Kraken.Kraken_executions_feed.PendingNewStatus, "pending_new");
    (Kraken.Kraken_executions_feed.NewStatus, "new");
    (Kraken.Kraken_executions_feed.PartiallyFilledStatus, "partially_filled");
    (Kraken.Kraken_executions_feed.FilledStatus, "filled");
    (Kraken.Kraken_executions_feed.CanceledStatus, "canceled");
    (Kraken.Kraken_executions_feed.ExpiredStatus, "expired");
    (Kraken.Kraken_executions_feed.RejectedStatus, "rejected");
    (Kraken.Kraken_executions_feed.UnknownStatus "custom", "custom");
  ] in

  List.iter (fun (status, expected_str) ->
    let str = Kraken.Kraken_executions_feed.string_of_order_status status in
    let back_to_status = Kraken.Kraken_executions_feed.order_status_of_string str in
    Alcotest.(check string) (Printf.sprintf "order_status to string: %s" expected_str) expected_str str;
    Alcotest.(check bool) (Printf.sprintf "order_status round-trip: %s" expected_str) true (back_to_status = status)
  ) test_cases

let test_ring_buffer_operations () =
  (* Test RingBuffer create, write, and read operations *)
  let buffer = Kraken.Kraken_executions_feed.RingBuffer.create 4 in

  (* Initially should have no data *)
  let initial_data = Kraken.Kraken_executions_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "initial buffer state is empty" None initial_data;

  (* Write some test data *)
  Kraken.Kraken_executions_feed.RingBuffer.write buffer 42;
  Kraken.Kraken_executions_feed.RingBuffer.write buffer 123;
  Kraken.Kraken_executions_feed.RingBuffer.write buffer 999;

  let data_after_writes = Kraken.Kraken_executions_feed.RingBuffer.read_all buffer in
  Alcotest.(check int) "buffer length after 3 writes" 3 (List.length data_after_writes);
  Alcotest.(check bool) "buffer contains 42" true (List.mem 42 data_after_writes);
  Alcotest.(check bool) "buffer contains 123" true (List.mem 123 data_after_writes);
  Alcotest.(check bool) "buffer contains 999" true (List.mem 999 data_after_writes);

  (* Test buffer wrap-around *)
  Kraken.Kraken_executions_feed.RingBuffer.write buffer 555;
  Kraken.Kraken_executions_feed.RingBuffer.write buffer 777;  (* This should overwrite oldest data *)

  let data_after_wrap = Kraken.Kraken_executions_feed.RingBuffer.read_all buffer in
  Alcotest.(check int) "buffer length after wrap-around" 4 (List.length data_after_wrap);
  Alcotest.(check bool) "buffer still contains 123 after wrap" true (List.mem 123 data_after_wrap);
  Alcotest.(check bool) "buffer still contains 999 after wrap" true (List.mem 999 data_after_wrap);
  Alcotest.(check bool) "buffer contains new 555" true (List.mem 555 data_after_wrap);
  Alcotest.(check bool) "buffer contains new 777" true (List.mem 777 data_after_wrap);
  Alcotest.(check bool) "buffer no longer contains overwritten 42" false (List.mem 42 data_after_wrap)

let test_execution_event_structure () =
  (* Test execution_event record structure *)
  let test_event = {
    Kraken.Kraken_executions_feed.order_id = "order123";
    symbol = "BTC/USD";
    exec_type = Kraken.Kraken_executions_feed.New;
    order_status = Kraken.Kraken_executions_feed.NewStatus;
    side = Kraken.Kraken_executions_feed.Buy;
    order_qty = 1.5;
    cum_qty = 0.0;
    cum_cost = 0.0;
    avg_price = 0.0;
    limit_price = Some 45000.0;
    last_qty = None;
    last_price = None;
    fee = None;
    trade_id = None;
    order_userref = Some 12345;
    timestamp = Unix.time ();
  } in

  Alcotest.(check string) "execution event order_id" "order123" test_event.order_id;
  Alcotest.(check string) "execution event symbol" "BTC/USD" test_event.symbol;
  Alcotest.(check bool) "execution event exec_type" true (test_event.exec_type = Kraken.Kraken_executions_feed.New);
  Alcotest.(check bool) "execution event order_status" true (test_event.order_status = Kraken.Kraken_executions_feed.NewStatus);
  Alcotest.(check bool) "execution event side" true (test_event.side = Kraken.Kraken_executions_feed.Buy);
  Alcotest.(check (float 0.1)) "execution event order_qty" 1.5 test_event.order_qty;
  Alcotest.(check (float 0.1)) "execution event cum_qty" 0.0 test_event.cum_qty;
  Alcotest.(check (float 0.1)) "execution event cum_cost" 0.0 test_event.cum_cost;
  Alcotest.(check (float 0.1)) "execution event avg_price" 0.0 test_event.avg_price;
  Alcotest.(check (option (float 0.1))) "execution event limit_price" (Some 45000.0) test_event.limit_price;
  Alcotest.(check bool) "execution event last_qty is none" true (Option.is_none test_event.last_qty);
  Alcotest.(check bool) "execution event last_price is none" true (Option.is_none test_event.last_price);
  Alcotest.(check bool) "execution event fee is none" true (Option.is_none test_event.fee);
  Alcotest.(check bool) "execution event trade_id is none" true (Option.is_none test_event.trade_id);
  Alcotest.(check (option int)) "execution event order_userref" (Some 12345) test_event.order_userref;
  Alcotest.(check bool) "execution event timestamp is positive" true (test_event.timestamp > 0.0)

let test_open_order_structure () =
  (* Test open_order record structure *)
  let test_order = {
    Kraken.Kraken_executions_feed.order_id = "open123";
    symbol = "ETH/USD";
    side = Kraken.Kraken_executions_feed.Sell;
    order_qty = 10.0;
    cum_qty = 3.0;
    remaining_qty = 7.0;
    limit_price = Some 3000.0;
    avg_price = 2995.0;
    cum_cost = 8985.0;
    order_status = Kraken.Kraken_executions_feed.PartiallyFilledStatus;
    order_userref = Some 67890;
    last_updated = Unix.time ();
  } in

  Alcotest.(check string) "open order order_id" "open123" test_order.order_id;
  Alcotest.(check string) "open order symbol" "ETH/USD" test_order.symbol;
  Alcotest.(check bool) "open order side" true (test_order.side = Kraken.Kraken_executions_feed.Sell);
  Alcotest.(check (float 0.1)) "open order order_qty" 10.0 test_order.order_qty;
  Alcotest.(check (float 0.1)) "open order cum_qty" 3.0 test_order.cum_qty;
  Alcotest.(check (float 0.1)) "open order remaining_qty" 7.0 test_order.remaining_qty;
  Alcotest.(check (option (float 0.1))) "open order limit_price" (Some 3000.0) test_order.limit_price;
  Alcotest.(check (float 0.1)) "open order avg_price" 2995.0 test_order.avg_price;
  Alcotest.(check (float 0.1)) "open order cum_cost" 8985.0 test_order.cum_cost;
  Alcotest.(check bool) "open order order_status" true (test_order.order_status = Kraken.Kraken_executions_feed.PartiallyFilledStatus);
  Alcotest.(check (option int)) "open order order_userref" (Some 67890) test_order.order_userref;
  Alcotest.(check bool) "open order last_updated is positive" true (test_order.last_updated > 0.0)

let test_symbol_store_management () =
  (* Test symbol store creation and basic operations *)
  let symbol = "TEST_SYMBOL" in

  (* Initially should not have execution data *)
  Alcotest.(check bool) "initially no execution data" false (Kraken.Kraken_executions_feed.has_execution_data symbol);

  (* Get symbol store (creates it if needed) *)
  let _store = Kraken.Kraken_executions_feed.get_symbol_store symbol in

  (* Should still not have data since we haven't added any *)
  Alcotest.(check bool) "no data after store creation" false (Kraken.Kraken_executions_feed.has_execution_data symbol)

let test_wait_for_execution_data () =
  (* Test waiting for execution data (with timeout) *)
  let symbols = ["EXEC_TEST1"; "EXEC_TEST2"] in

  (* Initially should not have data *)
  Alcotest.(check bool) "initially no execution data for symbols" false (List.for_all Kraken.Kraken_executions_feed.has_execution_data symbols);

  (* Start waiting with very short timeout *)
  let result = Lwt_main.run (Kraken.Kraken_executions_feed.wait_for_execution_data symbols 0.001) in

  (* Should timeout since no data is available *)
  Alcotest.(check bool) "wait times out with no data" false result

let test_ring_buffer_capacity () =
  (* Test that ring buffer respects capacity limits *)
  let buffer = Kraken.Kraken_executions_feed.RingBuffer.create 3 in

  (* Fill buffer to capacity *)
  Kraken.Kraken_executions_feed.RingBuffer.write buffer 1;
  Kraken.Kraken_executions_feed.RingBuffer.write buffer 2;
  Kraken.Kraken_executions_feed.RingBuffer.write buffer 3;

  let data_at_capacity = Kraken.Kraken_executions_feed.RingBuffer.read_all buffer in
  Alcotest.(check int) "buffer length at capacity" 3 (List.length data_at_capacity);

  (* Add one more - should maintain capacity *)
  Kraken.Kraken_executions_feed.RingBuffer.write buffer 4;

  let data_after_overflow = Kraken.Kraken_executions_feed.RingBuffer.read_all buffer in
  Alcotest.(check int) "buffer length after overflow" 3 (List.length data_after_overflow);
  Alcotest.(check bool) "buffer contains new element 4" true (List.mem 4 data_after_overflow);
  Alcotest.(check bool) "buffer no longer contains oldest element 1" false (List.mem 1 data_after_overflow)

let () =
  Alcotest.run "Kraken Executions Feed" [
    "type conversions", [
      Alcotest.test_case "exec type conversions" `Quick test_exec_type_conversions;
      Alcotest.test_case "side conversions" `Quick test_side_conversions;
      Alcotest.test_case "order status conversions" `Quick test_order_status_conversions;
    ];
    "data structures", [
      Alcotest.test_case "execution event structure" `Quick test_execution_event_structure;
      Alcotest.test_case "open order structure" `Quick test_open_order_structure;
    ];
    "ring buffer", [
      Alcotest.test_case "ring buffer operations" `Quick test_ring_buffer_operations;
      Alcotest.test_case "ring buffer capacity" `Quick test_ring_buffer_capacity;
    ];
    "symbol management", [
      Alcotest.test_case "symbol store management" `Quick test_symbol_store_management;
    ];
    "async operations", [
      Alcotest.test_case "wait for execution data" `Quick test_wait_for_execution_data;
    ];
  ]
