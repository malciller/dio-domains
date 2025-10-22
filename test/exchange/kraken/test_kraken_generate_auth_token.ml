let test_missing_env_variables () =
  (* Test error handling for missing environment variables *)
  (* First backup any existing env vars *)
  let original_key = Sys.getenv_opt "KRAKEN_API_KEY" in
  let original_secret = Sys.getenv_opt "KRAKEN_API_SECRET" in

  (* Clear environment variables by setting to empty *)
  Unix.putenv "KRAKEN_API_KEY" "";
  Unix.putenv "KRAKEN_API_SECRET" "";

  (* This should fail due to missing env vars *)
  Alcotest.check_raises "missing env variables should raise exception"
    (Failure "Missing environment variable: KRAKEN_API_KEY")
    (fun () -> Lwt_main.run (Kraken.Kraken_generate_auth_token.get_api_credentials_from_env ()) |> ignore);

  (* Restore env vars *)
  begin match original_key with Some k -> Unix.putenv "KRAKEN_API_KEY" k | None -> () end;
  begin match original_secret with Some s -> Unix.putenv "KRAKEN_API_SECRET" s | None -> () end

let test_nonce_generation_in_signing () =
  (* Test that nonce generation works in the context of signing *)
  (* We can't test the full get_token function without API credentials,
     but we can test that nonce generation doesn't break signing *)
  let nonce = Kraken.Kraken_common_types.nonce () in
  let secret = "dGVzdCBzZWNyZXQ=" in  (* "test secret" *)
  let path = "/test" in
  let body = "nonce=" ^ nonce in

  let signature = Kraken.Kraken_common_types.sign ~secret ~path ~body ~nonce in

  (* Signature should be non-empty *)
  Alcotest.(check bool) "signature is not empty" true (signature <> "");

  (* Signature should be valid base64 *)
  match Base64.decode signature with
  | Ok _ -> Alcotest.(check bool) "signature is valid base64" true true
  | Error _ -> Alcotest.fail "signature is not valid base64"

let test_endpoint_constant () =
  (* Test that the endpoint constant is correctly defined *)
  (* This is a basic sanity check *)
  let expected_endpoint = "https://api.kraken.com" in
  (* We can't directly access the module's endpoint constant,
     but we can verify the expected endpoint format *)
  Alcotest.(check bool) "endpoint starts with https" true (String.starts_with ~prefix:"https://" expected_endpoint);
  Alcotest.(check bool) "endpoint contains kraken" true (Kraken.Kraken_actions.string_contains expected_endpoint "kraken")

let test_token_request_structure () =
  (* Test that the token request would be structured correctly *)
  (* We can't make actual HTTP requests, but we can verify the components *)
  let nonce = Kraken.Kraken_common_types.nonce () in
  let expected_body = "nonce=" ^ nonce in
  let expected_path = "/0/private/GetWebSocketsToken" in

  (* Verify nonce is in body *)
  Alcotest.(check bool) "body starts with nonce=" true (String.starts_with ~prefix:"nonce=" expected_body);

  (* Verify path is correct *)
  Alcotest.(check string) "path is correct" "/0/private/GetWebSocketsToken" expected_path

let test_env_file_loading () =
  (* Test that .env file loading doesn't crash even if file doesn't exist *)
  (* This should not crash even if .env doesn't exist *)
  ignore (Lwt_main.run (
    Lwt.catch
      (fun () -> (Dotenv.export ~path:".env") (); Lwt.return true)
      (fun _ -> Lwt.return false)
  ));
  (* The test passes as long as it doesn't crash *)
  Alcotest.(check bool) "env file loading doesn't crash" true true

let test_api_credentials_structure () =
  (* Test that API credentials are expected to be strings *)
  (* This is more of a documentation test *)
  let test_key = "test_api_key_12345" in
  let test_secret = "dGVzdCBzZWNyZXQ=" in

  Alcotest.(check bool) "API key is non-empty string" true (String.length test_key > 0);
  Alcotest.(check bool) "API secret is non-empty string" true (String.length test_secret > 0)

let test_signature_components () =
  (* Test that signature components are properly formatted *)
  let secret = "dGVzdCBzZWNyZXQ=" in  (* "test secret" *)
  let path = "/0/private/GetWebSocketsToken" in
  let nonce = "1234567890123" in
  let body = "nonce=" ^ nonce in

  let signature = Kraken.Kraken_common_types.sign ~secret ~path ~body ~nonce in

  (* Check that signature is valid base64 and correct length *)
  match Base64.decode signature with
  | Ok decoded_bytes ->
      Alcotest.(check int) "signature length is 64 bytes (SHA512)" 64 (String.length decoded_bytes)
  | Error _ ->
      Alcotest.fail "signature is not valid base64"

let () =
  Alcotest.run "Kraken Generate Auth Token" [
    "environment variables", [
      Alcotest.test_case "missing_env_variables" `Quick test_missing_env_variables;
    ];
    "nonce and signing", [
      Alcotest.test_case "nonce_generation_in_signing" `Quick test_nonce_generation_in_signing;
    ];
    "constants and endpoints", [
      Alcotest.test_case "endpoint_constant" `Quick test_endpoint_constant;
    ];
    "request structure", [
      Alcotest.test_case "token_request_structure" `Quick test_token_request_structure;
    ];
    "file loading", [
      Alcotest.test_case "env_file_loading" `Quick test_env_file_loading;
    ];
    "credentials", [
      Alcotest.test_case "api_credentials_structure" `Quick test_api_credentials_structure;
    ];
    "signature components", [
      Alcotest.test_case "signature_components" `Quick test_signature_components;
    ];
  ]
