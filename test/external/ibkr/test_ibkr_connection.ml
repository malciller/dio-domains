let test_connection_create () =
  let conn = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4002 ~client_id:0 in
  Alcotest.(check bool) "not connected initially" false (Ibkr.Connection.is_connected conn);
  Alcotest.(check string) "account_id empty" "" (Ibkr.Connection.get_account_id conn);
  Alcotest.(check int) "server_version 0" 0 (Ibkr.Connection.get_server_version conn)

let test_get_next_order_id () =
  let conn = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4002 ~client_id:1 in
  let id1 = Ibkr.Connection.get_next_order_id conn in
  let id2 = Ibkr.Connection.get_next_order_id conn in
  let id3 = Ibkr.Connection.get_next_order_id conn in
  Alcotest.(check bool) "order IDs increment" true (id2 = id1 + 1 && id3 = id1 + 2)

let test_disconnect_when_not_connected () =
  let conn = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4002 ~client_id:2 in
  (* Disconnecting when not connected should be safe *)
  let result = Lwt_main.run (Ibkr.Connection.disconnect conn) in
  Alcotest.(check unit) "disconnect ok when not connected" () result;
  Alcotest.(check bool) "still not connected" false (Ibkr.Connection.is_connected conn)

let test_multiple_connections () =
  let conn1 = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4001 ~client_id:10 in
  let conn2 = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4002 ~client_id:20 in
  (* Should be independent *)
  let _id1 = Ibkr.Connection.get_next_order_id conn1 in
  let _id2 = Ibkr.Connection.get_next_order_id conn1 in
  let id_from_conn2 = Ibkr.Connection.get_next_order_id conn2 in
  (* conn2 should still start at 0 *)
  Alcotest.(check int) "conn2 independent order ID" 0 id_from_conn2

let () =
  Alcotest.run "IBKR Connection" [
    "creation", [
      Alcotest.test_case "connection_create" `Quick test_connection_create;
    ];
    "order IDs", [
      Alcotest.test_case "get_next_order_id" `Quick test_get_next_order_id;
      Alcotest.test_case "multiple_connections" `Quick test_multiple_connections;
    ];
    "lifecycle", [
      Alcotest.test_case "disconnect_when_not_connected" `Quick test_disconnect_when_not_connected;
    ];
  ]
