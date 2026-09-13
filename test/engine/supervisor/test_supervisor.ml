let test_connection_registration () =
  let connect_called = ref false in
  let connect_fn () =
    connect_called := true;
    Lwt.return_unit
  in
  let conn = Supervisor.register ~name:"test_conn" ~connect_fn:(Some connect_fn) in
  Alcotest.(check string) "connection name" "test_conn" conn.name;
  Alcotest.(check bool) "has connect function" true (Option.is_some conn.connect_fn);
  Alcotest.(check bool)
    "initial state disconnected"
    true
    (conn.state = Supervisor_types.Disconnected)
;;

let test_connection_registration_monitoring () =
  let conn = Supervisor.register_for_monitoring ~name:"monitor_conn" in
  Alcotest.(check string) "monitoring connection name" "monitor_conn" conn.name;
  Alcotest.(check bool) "no connect function" true (Option.is_none conn.connect_fn);
  Alcotest.(check bool)
    "monitoring state connected"
    true
    (conn.state = Supervisor_types.Connected)
;;

let test_state_management () =
  let conn = Supervisor.register_for_monitoring ~name:"state_test" in
  Supervisor.set_state conn Supervisor_types.Disconnected;
  Alcotest.(check bool)
    "disconnected state"
    true
    (Supervisor.get_state conn = Supervisor_types.Disconnected);
  Supervisor.set_state conn Supervisor_types.Connected;
  Alcotest.(check bool)
    "connected state"
    true
    (Supervisor.get_state conn = Supervisor_types.Connected);
  Supervisor.set_state conn (Supervisor_types.Failed "test error");
  match Supervisor.get_state conn with
  | Supervisor_types.Failed "test error" -> Alcotest.(check bool) "failed state" true true
  | _ -> Alcotest.fail "wrong failed state"
;;

let test_circuit_breaker_initial () =
  let conn = Supervisor.register_for_monitoring ~name:"cb_test" in
  Alcotest.(check bool)
    "circuit breaker closed"
    true
    (conn.circuit_breaker = Supervisor_types.Closed);
  Alcotest.(check int) "zero failures" 0 conn.circuit_breaker_failures
;;

let test_circuit_breaker_failure_counting () =
  let conn = Supervisor.register_for_monitoring ~name:"cb_fail_test" in
  Supervisor.update_circuit_breaker conn false;
  Supervisor.update_circuit_breaker conn false;
  Supervisor.update_circuit_breaker conn false;
  Supervisor.update_circuit_breaker conn false;
  (* Fifth failure opens breaker. *)
  Supervisor.update_circuit_breaker conn false;
  Alcotest.(check int) "five failures" 5 conn.circuit_breaker_failures;
  Alcotest.(check bool)
    "circuit breaker open"
    true
    (conn.circuit_breaker = Supervisor_types.Open)
;;

let test_circuit_breaker_recovery () =
  let conn = Supervisor.register_for_monitoring ~name:"cb_recover_test" in
  conn.circuit_breaker <- Supervisor_types.Open;
  conn.circuit_breaker_failures <- 5;
  conn.circuit_breaker_last_failure <- Some (Unix.time () -. 400.0);
  (* 400 s ago, past the recovery timeout. *)
  Alcotest.(check bool)
    "allows connection after timeout"
    true
    (Supervisor.circuit_breaker_allows_connection conn);
  Supervisor.update_circuit_breaker conn true;
  Alcotest.(check bool)
    "circuit breaker closed after success"
    true
    (conn.circuit_breaker = Supervisor_types.Closed);
  Alcotest.(check int) "zero failures after success" 0 conn.circuit_breaker_failures
;;

let test_token_store () =
  Supervisor.Token_store.set None;
  let test_token = "test_auth_token_123" in
  Supervisor.Token_store.set (Some test_token);
  Alcotest.(check (option string))
    "token retrieved"
    (Some test_token)
    (Supervisor.Token_store.get ());
  Supervisor.Token_store.set None;
  Alcotest.(check (option string)) "token cleared" None (Supervisor.Token_store.get ())
;;

let test_connection_queries () =
  (* Reset connections. *)
  Supervisor.stop_all ();
  let _ = Supervisor.register_for_monitoring ~name:"query_test1" in
  let _ = Supervisor.register_for_monitoring ~name:"query_test2" in
  let conn1 = Supervisor.get_connection "query_test1" in
  Alcotest.(check string) "get connection name" "query_test1" conn1.name;
  let all_conns = Supervisor.get_all_connections () in
  Alcotest.(check bool) "at least 2 connections" true (List.length all_conns >= 2);
  let names =
    List.map (fun (c : Supervisor_types.supervised_connection) -> c.name) all_conns
  in
  Alcotest.(check bool) "contains query_test1" true (List.mem "query_test1" names);
  Alcotest.(check bool) "contains query_test2" true (List.mem "query_test2" names)
;;

let test_connection_uptime () =
  let conn = Supervisor.register ~name:"uptime_test" ~connect_fn:None in
  (* Disconnected connection has no uptime. *)
  Alcotest.(check bool)
    "no uptime when disconnected"
    true
    (Option.is_none (Supervisor.get_uptime conn));
  Supervisor.set_state conn Supervisor_types.Connected;
  Alcotest.(check bool)
    "has uptime when connected"
    true
    (Option.is_some (Supervisor.get_uptime conn))
;;

let test_data_heartbeat () =
  let conn = Supervisor.register_for_monitoring ~name:"heartbeat_test" in
  Supervisor.update_data_heartbeat conn;
  Alcotest.(check bool) "heartbeat updated" true (Option.is_some conn.last_data_received)
;;

let test_connection_metrics () =
  let conn = Supervisor.register_for_monitoring ~name:"metrics_test" in
  let initial_attempts = conn.reconnect_attempts in
  let initial_total = conn.total_connections in
  (* Connected transition increments total_connections. *)
  Supervisor.set_state conn Supervisor_types.Connected;
  Alcotest.(check int)
    "reconnect attempts unchanged"
    initial_attempts
    conn.reconnect_attempts;
  Alcotest.(check int)
    "total connections incremented"
    (initial_total + 1)
    conn.total_connections
;;

let () =
  Alcotest.run
    "Supervisor"
    [ ( "registration"
      , [ Alcotest.test_case "connection registration" `Quick test_connection_registration
        ; Alcotest.test_case
            "monitoring registration"
            `Quick
            test_connection_registration_monitoring
        ] )
    ; ( "state_management"
      , [ Alcotest.test_case "state transitions" `Quick test_state_management ] )
    ; ( "circuit_breaker"
      , [ Alcotest.test_case "initial state" `Quick test_circuit_breaker_initial
        ; Alcotest.test_case
            "failure counting"
            `Quick
            test_circuit_breaker_failure_counting
        ; Alcotest.test_case "recovery" `Quick test_circuit_breaker_recovery
        ] )
    ; "token_store", [ Alcotest.test_case "token operations" `Quick test_token_store ]
    ; ( "queries"
      , [ Alcotest.test_case "connection queries" `Quick test_connection_queries ] )
    ; ( "monitoring"
      , [ Alcotest.test_case "uptime" `Quick test_connection_uptime
        ; Alcotest.test_case "heartbeat" `Quick test_data_heartbeat
        ; Alcotest.test_case "metrics" `Quick test_connection_metrics
        ] )
    ]
;;
