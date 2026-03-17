
let test_connection_state_initially_false () =
  Alcotest.(check bool) "initially disconnected" false (Hyperliquid.Ws.is_connected ())

let test_subscribe_market_data () =
  let sub = Hyperliquid.Ws.subscribe_market_data () in
  (* The stream should exist and be initially empty *)
  let item = Lwt_stream.get_available sub.stream in
  Alcotest.(check int) "initially empty" 0 (List.length item);
  sub.close ()

let test_broadcast_message () =
  let sub = Hyperliquid.Ws.subscribe_market_data () in
  let test_json = `Assoc [("test", `String "value")] in
  Hyperliquid.Ws.broadcast_message test_json;
  let items = Lwt_stream.get_available sub.stream in
  Alcotest.(check int) "received 1 message" 1 (List.length items);
  sub.close ()

let () =
  Alcotest.run "Hyperliquid WebSocket" [
    "state", [
      Alcotest.test_case "initial_state" `Quick test_connection_state_initially_false;
    ];
    "pubsub", [
      Alcotest.test_case "subscribe_market_data" `Quick test_subscribe_market_data;
      Alcotest.test_case "broadcast_message" `Quick test_broadcast_message;
    ]
  ]
