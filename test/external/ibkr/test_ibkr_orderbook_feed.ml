let test_level_structure () =
  let level = {
    Ibkr.Orderbook_feed.price = 175.50;
    size = 1000.0;
  } in
  Alcotest.(check (float 0.01)) "level price" 175.50 level.price;
  Alcotest.(check (float 0.01)) "level size" 1000.0 level.size

let test_orderbook_structure () =
  let bids = Array.make 5 { Ibkr.Orderbook_feed.price = 0.0; size = 0.0 } in
  let asks = Array.make 5 { Ibkr.Orderbook_feed.price = 0.0; size = 0.0 } in
  bids.(0) <- { price = 175.50; size = 100.0 };
  bids.(1) <- { price = 175.49; size = 200.0 };
  asks.(0) <- { price = 175.55; size = 150.0 };
  asks.(1) <- { price = 175.56; size = 250.0 };

  let ob = {
    Ibkr.Orderbook_feed.bids;
    asks;
    timestamp = Unix.gettimeofday ();
  } in

  Alcotest.(check int) "bids length" 5 (Array.length ob.bids);
  Alcotest.(check int) "asks length" 5 (Array.length ob.asks);
  Alcotest.(check (float 0.01)) "top bid price" 175.50 ob.bids.(0).price;
  Alcotest.(check (float 0.01)) "top bid size" 100.0 ob.bids.(0).size;
  Alcotest.(check (float 0.01)) "top ask price" 175.55 ob.asks.(0).price;
  Alcotest.(check (float 0.01)) "top ask size" 150.0 ob.asks.(0).size;
  Alcotest.(check bool) "timestamp positive" true (ob.timestamp > 0.0)

let test_ensure_store () =
  let symbol = "IBKR_OB_TEST" in
  (match Ibkr.Orderbook_feed.store_opt symbol with
   | None -> Alcotest.(check bool) "no initial store" true true
   | Some _ -> Alcotest.fail "unexpected initial store");

  let _store = Ibkr.Orderbook_feed.ensure_store symbol in
  (match Ibkr.Orderbook_feed.store_opt symbol with
   | Some _ -> Alcotest.(check bool) "store exists after ensure" true true
   | None -> Alcotest.fail "store not found after ensure")

let test_store_depth () =
  let symbol = "IBKR_OB_DEPTH_TEST" in
  let store = Ibkr.Orderbook_feed.ensure_store symbol in
  Alcotest.(check int) "store depth matches default" Ibkr.Types.default_orderbook_depth store.depth;
  Alcotest.(check int) "bids array length" Ibkr.Types.default_orderbook_depth (Array.length store.bids);
  Alcotest.(check int) "asks array length" Ibkr.Types.default_orderbook_depth (Array.length store.asks)

let test_read_events_unknown () =
  let events = Ibkr.Orderbook_feed.read_orderbook_events "IBKR_OB_UNKNOWN" 0 in
  Alcotest.(check (list int)) "no events for unknown symbol" [] (List.map (fun _ -> 1) events)

let test_get_position_unknown () =
  Alcotest.(check int) "position 0 for unknown" 0
    (Ibkr.Orderbook_feed.get_current_position "IBKR_OB_NO_POS")

let test_ring_buffer_operations () =
  let buffer = Ibkr.Orderbook_feed.RingBuffer.create 4 in

  let initial = Ibkr.Orderbook_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "initial read None" None initial;

  Ibkr.Orderbook_feed.RingBuffer.write buffer 10;
  Ibkr.Orderbook_feed.RingBuffer.write buffer 20;

  let latest = Ibkr.Orderbook_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "latest is 20" (Some 20) latest

let test_ring_buffer_wraparound () =
  let buffer = Ibkr.Orderbook_feed.RingBuffer.create 2 in
  Ibkr.Orderbook_feed.RingBuffer.write buffer 1;
  Ibkr.Orderbook_feed.RingBuffer.write buffer 2;
  Ibkr.Orderbook_feed.RingBuffer.write buffer 3;

  let latest = Ibkr.Orderbook_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "wraparound latest is 3" (Some 3) latest

let () =
  Alcotest.run "IBKR Orderbook Feed" [
    "data structures", [
      Alcotest.test_case "level_structure" `Quick test_level_structure;
      Alcotest.test_case "orderbook_structure" `Quick test_orderbook_structure;
    ];
    "store operations", [
      Alcotest.test_case "ensure_store" `Quick test_ensure_store;
      Alcotest.test_case "store_depth" `Quick test_store_depth;
    ];
    "data access", [
      Alcotest.test_case "read_events_unknown" `Quick test_read_events_unknown;
      Alcotest.test_case "get_position_unknown" `Quick test_get_position_unknown;
    ];
    "ring buffer", [
      Alcotest.test_case "ring_buffer_operations" `Quick test_ring_buffer_operations;
      Alcotest.test_case "ring_buffer_wraparound" `Quick test_ring_buffer_wraparound;
    ];
  ]
