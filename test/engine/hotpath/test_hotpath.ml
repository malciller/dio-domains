let frame =
  "{\"channel\":\"l2Book\",\"data\":{\"coin\":\"BTC\",\"levels\":[[{\"px\":\"72000.5\",\"sz\":\"1.25\"}],[{\"px\":\"72001.5\",\"sz\":\"2.5\"}]]}}"
;;

let check_float = Alcotest.(check (float 0.0001))

let test_coin_filter () =
  Alcotest.(check bool) "target coin" true (Hotpath_feed.coin_matches ~target:"BTC" frame);
  Alcotest.(check bool) "other coin" false (Hotpath_feed.coin_matches ~target:"ETH" frame)
;;

let test_tob () =
  let p = Hotpath_feed.create_parser () in
  let t = Hotpath_feed.create_tob () in
  Hotpath_feed.parse_tob_into p t frame;
  Alcotest.(check bool) "parsed" true t.Hotpath_feed.ok;
  check_float "bid px" 72000.5 t.Hotpath_feed.bid_px;
  check_float "bid sz" 1.25 t.Hotpath_feed.bid_sz;
  check_float "ask px" 72001.5 t.Hotpath_feed.ask_px;
  check_float "ask sz" 2.5 t.Hotpath_feed.ask_sz
;;

let test_ring () =
  let r = Hotpath_feed.create_ring 4 in
  Alcotest.(check bool) "push 1" true (Hotpath_feed.push r ~payload:"a" ~recv_ns:11);
  Alcotest.(check bool) "push 2" true (Hotpath_feed.push r ~payload:"b" ~recv_ns:22);
  Alcotest.(check bool) "pop 1" true (Hotpath_feed.try_pop r);
  Alcotest.(check string) "payload 1" "a" r.Hotpath_feed.current_payload;
  Alcotest.(check int) "recv 1" 11 r.Hotpath_feed.current_recv;
  Alcotest.(check bool) "pop 2" true (Hotpath_feed.try_pop r);
  Alcotest.(check string) "payload 2" "b" r.Hotpath_feed.current_payload;
  Alcotest.(check bool) "empty" false (Hotpath_feed.try_pop r)
;;

let test_zero_alloc () =
  let p = Hotpath_feed.create_parser () in
  let t = Hotpath_feed.create_tob () in
  (* Warm any lazy runtime state before measuring. *)
  Hotpath_feed.parse_tob_into p t frame;
  let before = Monotonic_clock.minor_words () in
  for _ = 1 to 100 do
    Hotpath_feed.parse_tob_into p t frame
  done;
  let after = Monotonic_clock.minor_words () in
  Alcotest.(check int) "parse allocates zero words" 0 (after - before)
;;

let () =
  Alcotest.run
    "hotpath"
    [ ( "feed"
      , [ Alcotest.test_case "coin filter" `Quick test_coin_filter
        ; Alcotest.test_case "tob parse" `Quick test_tob
        ; Alcotest.test_case "spsc ring" `Quick test_ring
        ; Alcotest.test_case "zero allocation" `Quick test_zero_alloc
        ] )
    ]
;;
