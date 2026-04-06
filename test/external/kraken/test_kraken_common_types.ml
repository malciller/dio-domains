let test_nonce_generation () =
  (* Test nonce generation *)
  let nonce1 = Kraken.Kraken_common_types.nonce () in
  let nonce2 = Kraken.Kraken_common_types.nonce () in

  (* Nonces should be numeric strings and increasing *)
  Alcotest.(check bool) "nonce1 not empty" true (nonce1 <> "");
  Alcotest.(check bool) "nonce2 not empty" true (nonce2 <> "");

  (* Check they are numeric *)
  let n1 = Int64.of_string nonce1 in
  let n2 = Int64.of_string nonce2 in
  Alcotest.(check bool) "nonces are non-decreasing" true (n2 >= n1)

let test_normalize_base64_secret () =
  (* Test base64 secret normalization *)
  let test_cases = [
    ("SGVsbG8gV29ybGQ=", "SGVsbG8gV29ybGQ=");  (* Already valid *)
    ("SGVsbG8gV29ybGQ", "SGVsbG8gV29ybGQ=");   (* Missing padding *)
    ("SGVsbG8gV29ybGQ-", "SGVsbG8gV29ybGQ+");  (* URL-safe dash *)
    ("SGVsbG8gV29ybGQ_", "SGVsbG8gV29ybGQ/");   (* URL-safe underscore *)
    ("  SGVsbG8gV29ybGQ=  ", "SGVsbG8gV29ybGQ="); (* With whitespace *)
    ("SGVs\nbG8g\rV29ybGQ=\t", "SGVsbG8gV29ybGQ="); (* With newlines/tabs *)
  ] in

  List.iter (fun (input, expected) ->
    let result = Kraken.Kraken_common_types.normalize_base64_secret input in
    Alcotest.(check string) ("normalize " ^ input) expected result
  ) test_cases

let test_decode_secret_base64 () =
  (* Test base64 secret decoding *)
  let secret = "SGVsbG8gV29ybGQ=" in  (* "Hello World" *)
  let decoded = Kraken.Kraken_common_types.decode_secret_base64 secret in
  Alcotest.(check string) "decode base64 secret" "Hello World" decoded

let test_invalid_secret_handling () =
  (* Test handling of invalid secrets *)
  let test_case secret expected_prefix =
    try
      ignore (Kraken.Kraken_common_types.decode_secret_base64 secret);
      Alcotest.fail ("Expected '" ^ secret ^ "' to raise an exception")
    with Failure msg ->
      if String.starts_with ~prefix:expected_prefix msg then
        () (* Success *)
      else
        Alcotest.fail (Printf.sprintf "Expected error starting with '%s', got '%s'" expected_prefix msg)
  in
  test_case "" "Kraken API secret is empty";
  test_case "@@@" "Invalid Kraken API secret (base64 decode failed)";
  test_case "!!!" "Invalid Kraken API secret (base64 decode failed)";
  test_case "not_base64_at_all!!!" "Invalid Kraken API secret (base64 decode failed)"

let test_sign_function () =
  (* Test HMAC-SHA512 signing *)
  let secret = "SGVsbG8gV29ybGQ=" in  (* "Hello World" *)
  let path = "/api/test" in
  let body = "test=data" in
  let nonce = "1234567890" in

  let signature = Kraken.Kraken_common_types.sign ~secret ~path ~body ~nonce in

  (* Signature should be a non-empty base64 string *)
  Alcotest.(check bool) "signature not empty" true (signature <> "");

  (* Try to decode it to ensure it's valid base64 *)
  match Base64.decode signature with
  | Ok decoded ->
      Alcotest.(check int) "signature length" 64 (String.length decoded)
  | Error _ -> Alcotest.fail "signature should be valid base64"

let test_ws_response_structure () =
  (* Test ws_response record structure *)
  let test_response = {
    Kraken.Kraken_common_types.method_ = "test_method";
    success = true;
    req_id = Some 123;
    time_in = "2023-01-01T00:00:00.000Z";
    time_out = "2023-01-01T00:00:00.001Z";
    result = Some (`String "test_result");
    error = None;
    warnings = Some ["test_warning"];
  } in

  Alcotest.(check string) "method_" "test_method" test_response.method_;
  Alcotest.(check bool) "success" true test_response.success;
  Alcotest.(check (option int)) "req_id" (Some 123) test_response.req_id;
  Alcotest.(check string) "time_in" "2023-01-01T00:00:00.000Z" test_response.time_in;
  Alcotest.(check string) "time_out" "2023-01-01T00:00:00.001Z" test_response.time_out;
  Alcotest.(check bool) "result is some" true (Option.is_some test_response.result);
  Alcotest.(check bool) "error is none" true (Option.is_none test_response.error);
  Alcotest.(check (option (list string))) "warnings" (Some ["test_warning"]) test_response.warnings

let test_order_result_structures () =
  (* Test order result record structures *)
  let add_result = {
    Kraken.Kraken_common_types.order_id = "order123";
    cl_ord_id = Some "client123";
    order_userref = Some 456;
  } in

  let amend_result = {
    Kraken.Kraken_common_types.amend_id = "amend123";
    order_id = "order123";
    cl_ord_id = Some "client123";
  } in

  let cancel_result = {
    Kraken.Kraken_common_types.order_id = "order123";
    cl_ord_id = Some "client123";
  } in

  Alcotest.(check string) "add order_id" "order123" add_result.order_id;
  Alcotest.(check (option string)) "add cl_ord_id" (Some "client123") add_result.cl_ord_id;
  Alcotest.(check (option int)) "add order_userref" (Some 456) add_result.order_userref;
  Alcotest.(check string) "amend amend_id" "amend123" amend_result.amend_id;
  Alcotest.(check string) "amend order_id" "order123" amend_result.order_id;
  Alcotest.(check (option string)) "amend cl_ord_id" (Some "client123") amend_result.cl_ord_id;
  Alcotest.(check string) "cancel order_id" "order123" cancel_result.order_id;
  Alcotest.(check (option string)) "cancel cl_ord_id" (Some "client123") cancel_result.cl_ord_id

let test_sign_consistency () =
  (* Test that signing the same data produces the same result *)
  let secret = "dGVzdCBzZWNyZXQ=" in  (* "test secret" *)
  let path = "/api/v1/test" in
  let body = "param1=value1&param2=value2" in
  let nonce = "9876543210" in

  let sig1 = Kraken.Kraken_common_types.sign ~secret ~path ~body ~nonce in
  let sig2 = Kraken.Kraken_common_types.sign ~secret ~path ~body ~nonce in

  Alcotest.(check bool) "signature not empty" true (sig1 <> "");
  Alcotest.(check string) "signatures are identical" sig1 sig2

let () =
  Alcotest.run "Kraken Common Types" [
    "nonce", [
      Alcotest.test_case "generation" `Quick test_nonce_generation;
    ];
    "base64", [
      Alcotest.test_case "normalization" `Quick test_normalize_base64_secret;
      Alcotest.test_case "decoding" `Quick test_decode_secret_base64;
      Alcotest.test_case "invalid handling" `Quick test_invalid_secret_handling;
    ];
    "signing", [
      Alcotest.test_case "function" `Quick test_sign_function;
      Alcotest.test_case "consistency" `Quick test_sign_consistency;
    ];
    "structures", [
      Alcotest.test_case "ws response" `Quick test_ws_response_structure;
      Alcotest.test_case "order results" `Quick test_order_result_structures;
    ];
  ]
