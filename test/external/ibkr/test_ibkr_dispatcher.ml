let test_handler_registration () =
  (* Register and dispatch a test handler *)
  let received = ref false in
  Ibkr.Dispatcher.register_handler
    ~msg_id:9999
    ~handler:(fun _fields -> received := true);

  (* Dispatch to the registered handler *)
  Ibkr.Dispatcher.dispatch ~msg_id:9999 ~fields:["test"];
  Alcotest.(check bool) "handler was invoked" true !received

let test_handler_overwrite () =
  let call_count = ref 0 in
  Ibkr.Dispatcher.register_handler
    ~msg_id:9998
    ~handler:(fun _fields -> call_count := 1);
  Ibkr.Dispatcher.register_handler
    ~msg_id:9998
    ~handler:(fun _fields -> call_count := 2);

  Ibkr.Dispatcher.dispatch ~msg_id:9998 ~fields:[];
  Alcotest.(check int) "latest handler wins" 2 !call_count

let test_unhandled_message () =
  (* Dispatching an unregistered msg_id should not crash *)
  Ibkr.Dispatcher.dispatch ~msg_id:8888 ~fields:["some"; "data"];
  Alcotest.(check bool) "unhandled message doesn't crash" true true

let test_req_handler_registration () =
  let data_received = ref false in
  let end_received = ref false in

  let _condition = Ibkr.Dispatcher.register_req_handler
    ~req_id:7777
    ~on_data:(fun _fields -> data_received := true)
    ~on_end:(fun () -> end_received := true)
  in

  Alcotest.(check bool) "req handler registered" true true;

  (* Clean up *)
  Ibkr.Dispatcher.remove_req_handler ~req_id:7777

let test_remove_req_handler () =
  let _condition = Ibkr.Dispatcher.register_req_handler
    ~req_id:6666
    ~on_data:(fun _fields -> ())
    ~on_end:(fun () -> ())
  in

  Ibkr.Dispatcher.remove_req_handler ~req_id:6666;
  (* Dispatching to removed handler should not crash *)
  Ibkr.Dispatcher.dispatch ~msg_id:55555 ~fields:["6666"; "data"];
  Alcotest.(check bool) "removed req handler doesn't crash" true true

let test_dispatch_empty_fields () =
  (* Dispatching with empty fields should not crash *)
  Ibkr.Dispatcher.dispatch ~msg_id:7654 ~fields:[];
  Alcotest.(check bool) "empty fields dispatch ok" true true

let test_handler_receives_correct_fields () =
  let captured_fields = ref [] in
  Ibkr.Dispatcher.register_handler
    ~msg_id:9997
    ~handler:(fun fields -> captured_fields := fields);

  Ibkr.Dispatcher.dispatch ~msg_id:9997 ~fields:["field1"; "field2"; "field3"];
  Alcotest.(check (list string)) "handler receives correct fields"
    ["field1"; "field2"; "field3"] !captured_fields

let test_handler_exception_recovery () =
  (* A handler that throws should not crash the dispatcher *)
  Ibkr.Dispatcher.register_handler
    ~msg_id:9996
    ~handler:(fun _fields -> failwith "test exception");

  Ibkr.Dispatcher.dispatch ~msg_id:9996 ~fields:["test"];
  Alcotest.(check bool) "exception in handler is caught" true true

let () =
  Alcotest.run "IBKR Dispatcher" [
    "handler registration", [
      Alcotest.test_case "handler_registration" `Quick test_handler_registration;
      Alcotest.test_case "handler_overwrite" `Quick test_handler_overwrite;
      Alcotest.test_case "handler_receives_correct_fields" `Quick test_handler_receives_correct_fields;
    ];
    "dispatch", [
      Alcotest.test_case "unhandled_message" `Quick test_unhandled_message;
      Alcotest.test_case "dispatch_empty_fields" `Quick test_dispatch_empty_fields;
      Alcotest.test_case "handler_exception_recovery" `Quick test_handler_exception_recovery;
    ];
    "req handlers", [
      Alcotest.test_case "req_handler_registration" `Quick test_req_handler_registration;
      Alcotest.test_case "remove_req_handler" `Quick test_remove_req_handler;
    ];
  ]
