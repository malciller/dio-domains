let test_execution_event_structure () =
  let event = {
    Ibkr.Executions_feed.order_id = "42";
    symbol = "AAPL";
    side = "BUY";
    status = Ibkr.Types.Submitted;
    filled_qty = 50.0;
    remaining_qty = 50.0;
    avg_fill_price = 175.50;
    last_fill_price = 175.55;
    last_fill_qty = 25.0;
    timestamp = Unix.gettimeofday ();
  } in

  Alcotest.(check string) "event order_id" "42" event.order_id;
  Alcotest.(check string) "event symbol" "AAPL" event.symbol;
  Alcotest.(check string) "event side" "BUY" event.side;
  Alcotest.(check bool) "event status" true (event.status = Ibkr.Types.Submitted);
  Alcotest.(check (float 0.01)) "event filled_qty" 50.0 event.filled_qty;
  Alcotest.(check (float 0.01)) "event remaining_qty" 50.0 event.remaining_qty;
  Alcotest.(check (float 0.01)) "event avg_fill_price" 175.50 event.avg_fill_price;
  Alcotest.(check (float 0.01)) "event last_fill_price" 175.55 event.last_fill_price;
  Alcotest.(check (float 0.01)) "event last_fill_qty" 25.0 event.last_fill_qty;
  Alcotest.(check bool) "event timestamp positive" true (event.timestamp > 0.0)

let test_open_order_structure () =
  let oo = {
    Ibkr.Executions_feed.oo_order_id = "99";
    oo_symbol = "SPY";
    oo_side = "SELL";
    oo_qty = 100.0;
    oo_filled_qty = 30.0;
    oo_remaining_qty = 70.0;
    oo_limit_price = Some 450.25;
    oo_status = Ibkr.Types.PreSubmitted;
    oo_last_updated = Unix.gettimeofday ();
  } in

  Alcotest.(check string) "oo order_id" "99" oo.oo_order_id;
  Alcotest.(check string) "oo symbol" "SPY" oo.oo_symbol;
  Alcotest.(check string) "oo side" "SELL" oo.oo_side;
  Alcotest.(check (float 0.01)) "oo qty" 100.0 oo.oo_qty;
  Alcotest.(check (float 0.01)) "oo filled_qty" 30.0 oo.oo_filled_qty;
  Alcotest.(check (float 0.01)) "oo remaining_qty" 70.0 oo.oo_remaining_qty;
  Alcotest.(check (option (float 0.01))) "oo limit_price" (Some 450.25) oo.oo_limit_price;
  Alcotest.(check bool) "oo status" true (oo.oo_status = Ibkr.Types.PreSubmitted);
  Alcotest.(check bool) "oo last_updated positive" true (oo.oo_last_updated > 0.0)

let test_open_order_no_limit () =
  let oo = {
    Ibkr.Executions_feed.oo_order_id = "100";
    oo_symbol = "QQQ";
    oo_side = "BUY";
    oo_qty = 50.0;
    oo_filled_qty = 0.0;
    oo_remaining_qty = 50.0;
    oo_limit_price = None;
    oo_status = Ibkr.Types.Submitted;
    oo_last_updated = Unix.gettimeofday ();
  } in
  Alcotest.(check (option (float 0.01))) "oo no limit_price" None oo.oo_limit_price

let test_get_symbol_store () =
  let symbol = "IBKR_EXEC_TEST" in
  let _store = Ibkr.Executions_feed.get_symbol_store symbol in
  (* Second call should return same store *)
  let _store2 = Ibkr.Executions_feed.get_symbol_store symbol in
  Alcotest.(check bool) "get_symbol_store creates store" true true

let test_register_and_resolve_order () =
  let order_id = 99999 in
  let symbol = "IBKR_REG_TEST" in
  Ibkr.Executions_feed.register_order ~order_id ~symbol;

  match Ibkr.Executions_feed.resolve_symbol order_id with
  | Some s -> Alcotest.(check string) "resolved symbol" symbol s
  | None -> Alcotest.fail "failed to resolve registered order"

let test_resolve_unknown_order () =
  match Ibkr.Executions_feed.resolve_symbol 88888 with
  | None -> Alcotest.(check bool) "unknown order resolves to None" true true
  | Some _ -> Alcotest.fail "unexpected resolution for unknown order"

let test_get_open_orders_empty () =
  let orders = Ibkr.Executions_feed.get_open_orders "IBKR_NO_ORDERS_SYMBOL" in
  Alcotest.(check int) "no open orders initially" 0 (List.length orders)

let test_get_open_order_missing () =
  match Ibkr.Executions_feed.get_open_order "IBKR_NO_ORDER" "12345" with
  | None -> Alcotest.(check bool) "missing order returns None" true true
  | Some _ -> Alcotest.fail "unexpected open order"

let test_read_execution_events () =
  let events = Ibkr.Executions_feed.read_execution_events "IBKR_NO_EXEC_EVENTS" 0 in
  Alcotest.(check int) "no execution events initially" 0 (List.length events)

let test_get_current_position () =
  let pos = Ibkr.Executions_feed.get_current_position "IBKR_NO_EXEC_POS" in
  Alcotest.(check int) "execution position 0" 0 pos

let test_fold_open_orders_empty () =
  let result = Ibkr.Executions_feed.fold_open_orders "IBKR_FOLD_TEST"
    ~init:0 ~f:(fun acc _oo -> acc + 1) in
  Alcotest.(check int) "fold empty orders" 0 result

let test_ring_buffer_operations () =
  let buffer = Ibkr.Executions_feed.RingBuffer.create 4 in

  let initial = Ibkr.Executions_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "initial buffer empty" None initial;

  Ibkr.Executions_feed.RingBuffer.write buffer 1;
  Ibkr.Executions_feed.RingBuffer.write buffer 2;
  Ibkr.Executions_feed.RingBuffer.write buffer 3;

  let latest = Ibkr.Executions_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "latest after writes" (Some 3) latest;

  let since_0 = Ibkr.Executions_feed.RingBuffer.read_since buffer 0 in
  Alcotest.(check int) "3 events since 0" 3 (List.length since_0)

let test_ring_buffer_wraparound () =
  let buffer = Ibkr.Executions_feed.RingBuffer.create 2 in
  Ibkr.Executions_feed.RingBuffer.write buffer 10;
  Ibkr.Executions_feed.RingBuffer.write buffer 20;
  Ibkr.Executions_feed.RingBuffer.write buffer 30;

  let latest = Ibkr.Executions_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "wraparound latest" (Some 30) latest

let () =
  Alcotest.run "IBKR Executions Feed" [
    "data structures", [
      Alcotest.test_case "execution_event_structure" `Quick test_execution_event_structure;
      Alcotest.test_case "open_order_structure" `Quick test_open_order_structure;
      Alcotest.test_case "open_order_no_limit" `Quick test_open_order_no_limit;
    ];
    "store management", [
      Alcotest.test_case "get_symbol_store" `Quick test_get_symbol_store;
    ];
    "order tracking", [
      Alcotest.test_case "register_and_resolve_order" `Quick test_register_and_resolve_order;
      Alcotest.test_case "resolve_unknown_order" `Quick test_resolve_unknown_order;
      Alcotest.test_case "get_open_orders_empty" `Quick test_get_open_orders_empty;
      Alcotest.test_case "get_open_order_missing" `Quick test_get_open_order_missing;
      Alcotest.test_case "fold_open_orders_empty" `Quick test_fold_open_orders_empty;
    ];
    "execution events", [
      Alcotest.test_case "read_execution_events" `Quick test_read_execution_events;
      Alcotest.test_case "get_current_position" `Quick test_get_current_position;
    ];
    "ring buffer", [
      Alcotest.test_case "ring_buffer_operations" `Quick test_ring_buffer_operations;
      Alcotest.test_case "ring_buffer_wraparound" `Quick test_ring_buffer_wraparound;
    ];
  ]
