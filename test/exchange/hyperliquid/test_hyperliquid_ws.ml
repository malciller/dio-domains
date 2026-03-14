let test_is_connected () =
  (* Since this is a unit test, we just check the initial state *)
  Alcotest.(check bool) "initially not connected" false (Hyperliquid.Hyperliquid_ws.is_connected ())

let test_parse_message_routing () =
  (* We just want to ensure that it doesn't crash on various payloads. *)
  
  let ping_msg_str = "{\"channel\": \"pong\"}" in
  let ping_frame = Websocket.Frame.create ~content:ping_msg_str () in
  let _ = Lwt_main.run (Hyperliquid.Hyperliquid_ws.handle_frame ping_frame) in
  
  let l2_book_msg_str = "{
    \"channel\": \"l2Book\",
    \"data\": {
      \"coin\": \"BTC\",
      \"levels\": [
        [{\"px\": \"50000\", \"sz\": \"1\", \"n\": 1}],
        [{\"px\": \"50001\", \"sz\": \"2\", \"n\": 1}]
      ]
    }
  }" in
  let l2_frame = Websocket.Frame.create ~content:l2_book_msg_str () in
  let _ = Lwt_main.run (Hyperliquid.Hyperliquid_ws.handle_frame l2_frame) in
  
  let web_data2_msg_str = "{
    \"channel\": \"webData2\",
    \"data\": {
      \"clearinghouseState\": {
        \"marginSummary\": {
          \"accountValue\": \"1000.0\"
        }
      }
    }
  }" in
  let web_data2_frame = Websocket.Frame.create ~content:web_data2_msg_str () in
  let _ = Lwt_main.run (Hyperliquid.Hyperliquid_ws.handle_frame web_data2_frame) in

  Alcotest.(check bool) "dispatch didn't crash" true true

let () =
  Alcotest.run "Hyperliquid WebSocket" [
    "state", [
      Alcotest.test_case "is_connected" `Quick test_is_connected;
    ];
    "routing", [
      Alcotest.test_case "dispatch_message" `Quick test_parse_message_routing;
    ];
  ]
