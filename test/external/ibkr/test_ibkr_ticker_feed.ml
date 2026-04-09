let test_ticker_structure () =
  let ticker = {
    Ibkr.Ticker_feed.symbol = "AAPL";
    bid = 175.50;
    ask = 175.55;
    last = 175.52;
    bid_size = 100.0;
    ask_size = 200.0;
    volume = 50000.0;
    timestamp = Unix.gettimeofday ();
  } in

  Alcotest.(check string) "ticker symbol" "AAPL" ticker.symbol;
  Alcotest.(check (float 0.01)) "ticker bid" 175.50 ticker.bid;
  Alcotest.(check (float 0.01)) "ticker ask" 175.55 ticker.ask;
  Alcotest.(check (float 0.01)) "ticker last" 175.52 ticker.last;
  Alcotest.(check (float 0.01)) "ticker bid_size" 100.0 ticker.bid_size;
  Alcotest.(check (float 0.01)) "ticker ask_size" 200.0 ticker.ask_size;
  Alcotest.(check (float 0.01)) "ticker volume" 50000.0 ticker.volume;
  Alcotest.(check bool) "ticker timestamp positive" true (ticker.timestamp > 0.0)

let test_ensure_store () =
  let symbol = "IBKR_TICKER_TEST" in

  (* Initially should not have store *)
  (match Ibkr.Ticker_feed.store_opt symbol with
   | None -> Alcotest.(check bool) "no initial store" true true
   | Some _ -> Alcotest.fail "unexpected initial store");

  (* Ensure store creates it *)
  let _store = Ibkr.Ticker_feed.ensure_store symbol in
  (match Ibkr.Ticker_feed.store_opt symbol with
   | Some _ -> Alcotest.(check bool) "store exists after ensure" true true
   | None -> Alcotest.fail "store not found after ensure")

let test_ensure_store_idempotent () =
  let symbol = "IBKR_TICKER_IDEM_TEST" in
  let store1 = Ibkr.Ticker_feed.ensure_store symbol in
  let store2 = Ibkr.Ticker_feed.ensure_store symbol in
  (* Both should reference the same store *)
  Alcotest.(check bool) "ensure_store idempotent" true
    (Ibkr.Ticker_feed.RingBuffer.get_position store1.buffer =
     Ibkr.Ticker_feed.RingBuffer.get_position store2.buffer)

let test_get_latest_ticker_unknown () =
  match Ibkr.Ticker_feed.get_latest_ticker "IBKR_UNKNOWN_TICKER" with
  | None -> Alcotest.(check bool) "no ticker for unknown symbol" true true
  | Some _ -> Alcotest.fail "unexpected ticker for unknown symbol"

let test_has_price_data () =
  Alcotest.(check bool) "no price data initially" false
    (Ibkr.Ticker_feed.has_price_data "IBKR_NO_PRICE_DATA")

let test_read_ticker_events_unknown () =
  let events = Ibkr.Ticker_feed.read_ticker_events "IBKR_NO_EVENTS" 0 in
  Alcotest.(check (list int)) "no events for unknown symbol" [] (List.map (fun _ -> 1) events)

let test_get_current_position_unknown () =
  let pos = Ibkr.Ticker_feed.get_current_position "IBKR_NO_POS" in
  Alcotest.(check int) "position is 0 for unknown symbol" 0 pos

let test_ring_buffer_operations () =
  let buffer = Ibkr.Ticker_feed.RingBuffer.create 4 in

  (* Initially empty *)
  let initial = Ibkr.Ticker_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "initial read None" None initial;

  (* Write and read *)
  Ibkr.Ticker_feed.RingBuffer.write buffer 10;
  Ibkr.Ticker_feed.RingBuffer.write buffer 20;
  Ibkr.Ticker_feed.RingBuffer.write buffer 30;

  let latest = Ibkr.Ticker_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "latest is 30" (Some 30) latest;

  let since_0 = Ibkr.Ticker_feed.RingBuffer.read_since buffer 0 in
  Alcotest.(check int) "3 events since 0" 3 (List.length since_0)

let test_ring_buffer_wraparound () =
  let buffer = Ibkr.Ticker_feed.RingBuffer.create 3 in
  Ibkr.Ticker_feed.RingBuffer.write buffer 1;
  Ibkr.Ticker_feed.RingBuffer.write buffer 2;
  Ibkr.Ticker_feed.RingBuffer.write buffer 3;
  Ibkr.Ticker_feed.RingBuffer.write buffer 4;

  let latest = Ibkr.Ticker_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "wraparound latest is 4" (Some 4) latest

let test_concurrent_ring_buffer () =
  let buffer = Ibkr.Ticker_feed.RingBuffer.create 10 in

  let write_thread value delay =
    Thread.create (fun () ->
      Thread.delay delay;
      Ibkr.Ticker_feed.RingBuffer.write buffer value
    ) ()
  in

  let t1 = write_thread 100 0.01 in
  let t2 = write_thread 200 0.02 in
  let t3 = write_thread 300 0.03 in

  Thread.join t1;
  Thread.join t2;
  Thread.join t3;

  let latest = Ibkr.Ticker_feed.RingBuffer.read_latest buffer in
  let is_valid = match latest with
    | Some 100 | Some 200 | Some 300 -> true
    | _ -> false
  in
  Alcotest.(check bool) "concurrent writes produce valid latest" true is_valid

let () =
  Alcotest.run "IBKR Ticker Feed" [
    "ticker structure", [
      Alcotest.test_case "ticker_structure" `Quick test_ticker_structure;
    ];
    "store operations", [
      Alcotest.test_case "ensure_store" `Quick test_ensure_store;
      Alcotest.test_case "ensure_store_idempotent" `Quick test_ensure_store_idempotent;
    ];
    "data access", [
      Alcotest.test_case "get_latest_ticker_unknown" `Quick test_get_latest_ticker_unknown;
      Alcotest.test_case "has_price_data" `Quick test_has_price_data;
      Alcotest.test_case "read_ticker_events_unknown" `Quick test_read_ticker_events_unknown;
      Alcotest.test_case "get_current_position_unknown" `Quick test_get_current_position_unknown;
    ];
    "ring buffer", [
      Alcotest.test_case "ring_buffer_operations" `Quick test_ring_buffer_operations;
      Alcotest.test_case "ring_buffer_wraparound" `Quick test_ring_buffer_wraparound;
      Alcotest.test_case "concurrent_ring_buffer" `Quick test_concurrent_ring_buffer;
    ];
  ]
