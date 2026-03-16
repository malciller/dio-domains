
let test_connection_state_initially_false () =
  Alcotest.(check bool) "initially disconnected" false (Hyperliquid.Ws.is_connected ())

let () =
  Alcotest.run "Hyperliquid WebSocket" [
    "state", [
      Alcotest.test_case "initial_state" `Quick test_connection_state_initially_false;
    ]
  ]
