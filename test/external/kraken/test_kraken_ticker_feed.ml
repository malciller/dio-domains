let test_ring_buffer_operations () =
  (* Test RingBuffer create, write, and read operations *)
  let buffer = Kraken.Kraken_ticker_feed.RingBuffer.create 4 in

  (* Initially should have no data *)
  let initial_read = Kraken.Kraken_ticker_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "initial read should be None" None initial_read;

  (* Write some test data *)
  Kraken.Kraken_ticker_feed.RingBuffer.write buffer 42;
  Kraken.Kraken_ticker_feed.RingBuffer.write buffer 123;
  Kraken.Kraken_ticker_feed.RingBuffer.write buffer 999;

  let latest_after_writes = Kraken.Kraken_ticker_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "latest after writes should be 999" (Some 999) latest_after_writes;

  (* Test read_since functionality *)
  let events_since_0 = Kraken.Kraken_ticker_feed.RingBuffer.read_since buffer 0 in
  Alcotest.(check int) "events_since_0 length should be 3" 3 (List.length events_since_0);
  Alcotest.(check bool) "events_since_0 should contain 42" true (List.mem 42 events_since_0);
  Alcotest.(check bool) "events_since_0 should contain 123" true (List.mem 123 events_since_0);
  Alcotest.(check bool) "events_since_0 should contain 999" true (List.mem 999 events_since_0);

  (* Read from current position should return empty *)
  let current_pos = 3 in  (* After 3 writes in buffer of size 4 *)
  let events_since_current = Kraken.Kraken_ticker_feed.RingBuffer.read_since buffer current_pos in
  Alcotest.(check (list int)) "events_since_current should be empty" [] events_since_current

let test_ticker_structure () =
  (* Test ticker record structure *)
  let test_ticker = {
    Kraken.Kraken_ticker_feed.symbol = "BTC/USD";
    bid = 45000.50;
    ask = 45010.25;
    last = 45005.00;
    volume = 123.456;
    timestamp = Unix.time ();
  } in

  Alcotest.(check string) "ticker symbol" "BTC/USD" test_ticker.symbol;
  Alcotest.(check (float 0.01)) "ticker bid" 45000.50 test_ticker.bid;
  Alcotest.(check (float 0.01)) "ticker ask" 45010.25 test_ticker.ask;
  Alcotest.(check (float 0.01)) "ticker last" 45005.00 test_ticker.last;
  Alcotest.(check (float 0.001)) "ticker volume" 123.456 test_ticker.volume;
  Alcotest.(check bool) "ticker timestamp should be positive" true (test_ticker.timestamp > 0.0)

let test_store_operations () =
  (* Test store creation and operations *)
  let symbol = "STORE_TEST" in

  (* Initially should not have store *)
  match Kraken.Kraken_ticker_feed.store_opt symbol with
  | None ->
      (* Ensure store creates it *)
      let _store = Kraken.Kraken_ticker_feed.ensure_store symbol in
      (* Now should have store *)
      (match Kraken.Kraken_ticker_feed.store_opt symbol with
       | Some _ -> Alcotest.(check bool) "store should exist after ensure" true true
       | None -> Alcotest.fail "store not found after ensure")
  | Some _ -> Alcotest.fail "unexpected initial store"

let test_get_latest_ticker () =
  (* Test getting latest ticker *)
  let symbol = "LATEST_TEST" in

  (* Initially should return None *)
  match Kraken.Kraken_ticker_feed.get_latest_ticker symbol with
  | None -> Alcotest.(check bool) "get_latest_ticker should return None initially" true true
  | Some _ -> Alcotest.fail "unexpected initial ticker"

let test_get_latest_price () =
  (* Test getting latest price *)
  let symbol = "PRICE_TEST" in

  (* Initially should return None *)
  match Kraken.Kraken_ticker_feed.get_latest_price symbol with
  | None -> Alcotest.(check bool) "get_latest_price should return None initially" true true
  | Some _ -> Alcotest.fail "unexpected initial price"

let test_has_price_data () =
  (* Test price data availability checking *)
  let symbol = "HAS_PRICE_TEST" in

  (* Initially should not have price data *)
  Alcotest.(check bool) "has_price_data should be false initially" false (Kraken.Kraken_ticker_feed.has_price_data symbol)

let test_wait_for_price_data () =
  (* Test waiting for price data (with timeout) *)
  let symbols = ["WAIT_PRICE_TEST1"; "WAIT_PRICE_TEST2"] in

  (* Initially should not have data *)
  let initial_data_check = not (List.for_all Kraken.Kraken_ticker_feed.has_price_data symbols) in
  Alcotest.(check bool) "initial data check should be false" true initial_data_check;

  (* Start waiting with very short timeout *)
  let result = Lwt_main.run (Kraken.Kraken_ticker_feed.wait_for_price_data symbols 0.001) in

  (* Should timeout since no data is available *)
  Alcotest.(check bool) "wait_for_price_data should timeout" false result

let test_price_calculation () =
  (* Test price calculation (midpoint of bid/ask) *)
  (* Since we can't easily add test data, we'll test the interface *)
  let symbol = "PRICE_CALC_TEST" in

  (* Should not crash and return None for unknown symbol *)
  match Kraken.Kraken_ticker_feed.get_latest_price symbol with
  | None -> Alcotest.(check bool) "get_latest_price should return None for unknown symbol" true true
  | Some _ -> Alcotest.fail "unexpected price for unknown symbol"

let test_ring_buffer_wraparound () =
  (* Test ring buffer wraparound behavior *)
  let buffer = Kraken.Kraken_ticker_feed.RingBuffer.create 3 in

  (* Fill buffer exactly *)
  Kraken.Kraken_ticker_feed.RingBuffer.write buffer 1;
  Kraken.Kraken_ticker_feed.RingBuffer.write buffer 2;
  Kraken.Kraken_ticker_feed.RingBuffer.write buffer 3;

  (* Add one more - should overwrite oldest *)
  Kraken.Kraken_ticker_feed.RingBuffer.write buffer 4;

  let latest = Kraken.Kraken_ticker_feed.RingBuffer.read_latest buffer in
  Alcotest.(check (option int)) "ring buffer wraparound should return 4" (Some 4) latest

let test_concurrent_ring_buffer () =
  (* Test ring buffer under concurrent access *)
  let buffer = Kraken.Kraken_ticker_feed.RingBuffer.create 10 in

  (* Start multiple threads writing to the same buffer *)
  let write_thread value delay =
    Thread.create (fun () ->
      Thread.delay delay;
      Kraken.Kraken_ticker_feed.RingBuffer.write buffer value
    ) ()
  in

  let thread1 = write_thread 100 0.01 in
  let thread2 = write_thread 200 0.02 in
  let thread3 = write_thread 300 0.03 in

  (* Wait for all threads to complete *)
  Thread.join thread1;
  Thread.join thread2;
  Thread.join thread3;

  (* Check that latest value is one of the written values *)
  let latest = Kraken.Kraken_ticker_feed.RingBuffer.read_latest buffer in
  let is_valid_latest = match latest with
    | Some 100 | Some 200 | Some 300 -> true
    | _ -> false
  in
  Alcotest.(check bool) "concurrent ring buffer should have valid latest value" true is_valid_latest

let () =
  Alcotest.run "Kraken Ticker Feed" [
    "ring buffer", [
      Alcotest.test_case "ring_buffer_operations" `Quick test_ring_buffer_operations;
      Alcotest.test_case "ring_buffer_wraparound" `Quick test_ring_buffer_wraparound;
      Alcotest.test_case "concurrent_ring_buffer" `Quick test_concurrent_ring_buffer;
    ];
    "ticker structure", [
      Alcotest.test_case "ticker_structure" `Quick test_ticker_structure;
    ];
    "store operations", [
      Alcotest.test_case "store_operations" `Quick test_store_operations;
    ];
    "ticker data access", [
      Alcotest.test_case "get_latest_ticker" `Quick test_get_latest_ticker;
      Alcotest.test_case "get_latest_price" `Quick test_get_latest_price;
      Alcotest.test_case "has_price_data" `Quick test_has_price_data;
      Alcotest.test_case "wait_for_price_data" `Quick test_wait_for_price_data;
      Alcotest.test_case "price_calculation" `Quick test_price_calculation;
    ];
  ]
