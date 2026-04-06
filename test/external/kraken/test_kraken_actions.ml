let test_string_contains () =
  (* Test string substring matching *)
  Alcotest.(check bool) "contains 'world'" true (Kraken.Kraken_actions.string_contains "hello world" "world");
  Alcotest.(check bool) "contains 'hello'" true (Kraken.Kraken_actions.string_contains "hello world" "hello");
  Alcotest.(check bool) "does not contain 'goodbye'" false (Kraken.Kraken_actions.string_contains "hello world" "goodbye");
  Alcotest.(check bool) "does not contain longer substring" false (Kraken.Kraken_actions.string_contains "hi" "hello");
  Alcotest.(check bool) "contains single char" true (Kraken.Kraken_actions.string_contains "aaa" "a");
  Alcotest.(check bool) "empty string does not contain char" false (Kraken.Kraken_actions.string_contains "" "a")

let test_json_to_string_precise () =
  (* Test JSON encoding without symbol precision (should use default 8) *)
  let json = `Assoc [("key", `Float 1.23456789)] in
  let result = Kraken.Kraken_actions.json_to_string_precise None json in
  Alcotest.(check string) "basic JSON encoding" "{\"key\":1.23456789}" result;

  (* Test with string escaping *)
  let json_with_strings = `Assoc [("msg", `String "hello\"world\\test")] in
  let result2 = Kraken.Kraken_actions.json_to_string_precise None json_with_strings in
  Alcotest.(check string) "string escaping" "{\"msg\":\"hello\\\"world\\\\test\"}" result2;

  (* Test nested structures *)
  let nested = `Assoc [("data", `List [`Int 1; `Int 2; `Int 3])] in
  let result3 = Kraken.Kraken_actions.json_to_string_precise None nested in
  Alcotest.(check string) "nested structures" "{\"data\":[1,2,3]}" result3

let test_next_req_id () =
  (* Test request ID generation *)
  let id1 = Kraken.Kraken_actions.next_req_id () in
  let id2 = Kraken.Kraken_actions.next_req_id () in
  let id3 = Kraken.Kraken_actions.next_req_id () in

  Alcotest.(check bool) "ID increments correctly" true (id2 = id1 + 1 && id3 = id1 + 2)

let test_is_retriable_error () =
  (* Test error classification for retry logic *)
  Alcotest.(check bool) "Connection timeout is retriable" true (Kraken.Kraken_actions.is_retriable_error "Connection timeout");
  Alcotest.(check bool) "Network error 500 is retriable" true (Kraken.Kraken_actions.is_retriable_error "Network error 500");
  Alcotest.(check bool) "WebSocket connection reset is retriable" true (Kraken.Kraken_actions.is_retriable_error "WebSocket connection reset");
  Alcotest.(check bool) "Invalid order parameters is not retriable" false (Kraken.Kraken_actions.is_retriable_error "Invalid order parameters");
  Alcotest.(check bool) "Insufficient funds is not retriable" false (Kraken.Kraken_actions.is_retriable_error "Insufficient funds")

let test_retry_config_defaults () =
  (* Test default retry configuration *)
  let config = Kraken.Kraken_actions.default_retry_config in
  Alcotest.(check int) "max_attempts default" 3 config.max_attempts;
  Alcotest.(check (float 0.001)) "base_delay_ms default" 1000.0 config.base_delay_ms;
  Alcotest.(check (float 0.001)) "max_delay_ms default" 30000.0 config.max_delay_ms;
  Alcotest.(check (float 0.001)) "backoff_factor default" 2.0 config.backoff_factor

let test_add_to_assoc () =
  (* Test adding key-value pairs to JSON assoc *)
  let json = `Assoc [("existing", `String "value")] in
  let result = Kraken.Kraken_actions.add_to_assoc json ("new_key", `Int 42) in
  match result with
  | `Assoc pairs ->
      let has_existing = List.exists (fun (k, v) -> k = "existing" && v = `String "value") pairs in
      let has_new = List.exists (fun (k, v) -> k = "new_key" && v = `Int 42) pairs in
      Alcotest.(check bool) "contains existing key" true has_existing;
      Alcotest.(check bool) "contains new key" true has_new
  | _ -> Alcotest.fail "Expected Assoc type"

let test_truncate_price_to_precision () =
  (* Test price truncation - this depends on Kraken_instruments_feed.get_precision_info *)
  (* Since we can't easily mock that, we'll test the case where no precision info is available *)
  let price = 123.456789 in
  let result = Kraken.Kraken_actions.truncate_price_to_precision price "UNKNOWN_SYMBOL" in
  Alcotest.(check (float 0.000001)) "unknown symbol returns original price" price result

let test_retry_with_backoff_logic () =
  (* Test retry logic without actually calling external functions *)
  let call_count = ref 0 in
  let test_function () =
    incr call_count;
    if !call_count < 3 then
      Lwt.return (Error "Temporary failure")
    else
      Lwt.return (Ok "Success")
  in

  let config = { Kraken.Kraken_actions.max_attempts = 5; base_delay_ms = 1.0; max_delay_ms = 100.0; backoff_factor = 1.5 } in

  let result = Lwt_main.run (
    Kraken.Kraken_actions.retry_with_backoff
      ~config
      ~f:test_function
      ~is_retriable:(fun _ -> true)
  ) in

  match result with
  | Ok "Success" ->
      Alcotest.(check int) "function called correct number of times" 3 !call_count
  | _ -> Alcotest.fail "Expected success after retries"

let test_max_retry_attempts () =
  (* Test that retry stops after max attempts *)
  let call_count = ref 0 in
  let failing_function () =
    incr call_count;
    Lwt.return (Error "Persistent failure")
  in

  let config = { Kraken.Kraken_actions.max_attempts = 2; base_delay_ms = 1.0; max_delay_ms = 10.0; backoff_factor = 1.0 } in

  let result = Lwt_main.run (
    Kraken.Kraken_actions.retry_with_backoff
      ~config
      ~f:failing_function
      ~is_retriable:(fun _ -> true)
  ) in

  match result with
  | Error _ ->
      Alcotest.(check int) "function called max attempts times" 2 !call_count
  | _ -> Alcotest.fail "Expected failure after max retries"

let () =
  Alcotest.run "Kraken Actions" [
    "string utilities", [
      Alcotest.test_case "string_contains" `Quick test_string_contains;
    ];
    "json handling", [
      Alcotest.test_case "json_to_string_precise" `Quick test_json_to_string_precise;
      Alcotest.test_case "add_to_assoc" `Quick test_add_to_assoc;
    ];
    "request handling", [
      Alcotest.test_case "next_req_id" `Quick test_next_req_id;
    ];
    "error handling", [
      Alcotest.test_case "is_retriable_error" `Quick test_is_retriable_error;
    ];
    "configuration", [
      Alcotest.test_case "retry_config_defaults" `Quick test_retry_config_defaults;
      Alcotest.test_case "truncate_price_to_precision" `Quick test_truncate_price_to_precision;
    ];
    "retry logic", [
      Alcotest.test_case "retry_with_backoff_logic" `Quick test_retry_with_backoff_logic;
      Alcotest.test_case "max_retry_attempts" `Quick test_max_retry_attempts;
    ];
  ]
