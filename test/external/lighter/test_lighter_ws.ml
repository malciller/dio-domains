let test_connection_state_initially_false () =
  Alcotest.(check bool) "initially disconnected" false (Lighter.Ws.is_connected ())

let test_subscribe_market_data () =
  let sub = Lighter.Ws.subscribe_market_data () in
  (* The stream should exist and be initially empty *)
  let items = Lwt_stream.get_available sub.stream in
  Alcotest.(check int) "initially empty" 0 (List.length items);
  sub.close ()

let test_broadcast_message () =
  let sub = Lighter.Ws.subscribe_market_data () in
  let test_json = `Assoc [("test", `String "value")] in
  Lighter.Ws.broadcast_message test_json;
  let items = Lwt_stream.get_available sub.stream in
  Alcotest.(check int) "received 1 message" 1 (List.length items);
  sub.close ()

let test_broadcast_multiple_subscribers () =
  let sub1 = Lighter.Ws.subscribe_market_data () in
  let sub2 = Lighter.Ws.subscribe_market_data () in
  let test_json = `Assoc [("key", `String "val")] in
  Lighter.Ws.broadcast_message test_json;
  let items1 = Lwt_stream.get_available sub1.stream in
  let items2 = Lwt_stream.get_available sub2.stream in
  Alcotest.(check int) "sub1 received" 1 (List.length items1);
  Alcotest.(check int) "sub2 received" 1 (List.length items2);
  sub1.close ();
  sub2.close ()

let test_close_unsubscribes () =
  let sub = Lighter.Ws.subscribe_market_data () in
  sub.close ();
  (* After close, broadcast should not deliver *)
  Lighter.Ws.broadcast_message (`Assoc [("after", `String "close")]);
  let items = Lwt_stream.get_available sub.stream in
  Alcotest.(check int) "no messages after close" 0 (List.length items)

let test_ping_failure_counter () =
  Lighter.Ws.reset_ping_failures ();
  Alcotest.(check int) "starts at 0" 0 (Lighter.Ws.get_ping_failures ());
  Lighter.Ws.incr_ping_failures ();
  Alcotest.(check int) "after incr" 1 (Lighter.Ws.get_ping_failures ());
  Lighter.Ws.incr_ping_failures ();
  Alcotest.(check int) "after 2 incr" 2 (Lighter.Ws.get_ping_failures ());
  Lighter.Ws.reset_ping_failures ();
  Alcotest.(check int) "after reset" 0 (Lighter.Ws.get_ping_failures ())

let () =
  Alcotest.run "Lighter WebSocket" [
    "state", [
      Alcotest.test_case "initial_state" `Quick test_connection_state_initially_false;
    ];
    "pubsub", [
      Alcotest.test_case "subscribe_market_data" `Quick test_subscribe_market_data;
      Alcotest.test_case "broadcast_message" `Quick test_broadcast_message;
      Alcotest.test_case "broadcast_multiple_subscribers" `Quick test_broadcast_multiple_subscribers;
      Alcotest.test_case "close_unsubscribes" `Quick test_close_unsubscribes;
    ];
    "ping", [
      Alcotest.test_case "ping_failure_counter" `Quick test_ping_failure_counter;
    ]
  ]
