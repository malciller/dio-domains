let test_parse_success_order () =
  let json_str = {|
    {
      "channel": "post",
      "data": {
        "id": 123,
        "response": {
          "type": "action",
          "payload": {
            "status": "ok",
            "response": {
              "type": "order",
              "data": {
                "statuses": [
                  {
                    "resting": {
                      "oid": 12345
                    }
                  }
                ]
              }
            }
          }
        }
      }
    }
  |} in
  let json = Yojson.Safe.from_string json_str in
  let result = Hyperliquid.Hyperliquid_module.parse_order_response "BTC/USDC" 100L "cloid-1" 0.1 (Some 50000.0) json in
  match result with
  | Ok (oid, cloid) ->
      Alcotest.(check string) "order id" "12345" oid;
      Alcotest.(check string) "cloid" "cloid-1" cloid
  | Error e -> Alcotest.fail ("Should have succeeded but failed with: " ^ e)

let test_parse_error_outer () =
  let json_str = {|
    {
      "channel": "post",
      "data": {
        "id": 123,
        "response": {
          "type": "error",
          "payload": "Invalid request format"
        }
      }
    }
  |} in
  let json = Yojson.Safe.from_string json_str in
  let result = Hyperliquid.Hyperliquid_module.parse_order_response "BTC/USDC" 100L "cloid-1" 0.1 (Some 50000.0) json in
  match result with
  | Error e -> Alcotest.(check string) "error message" "Invalid request format" e
  | Ok _ -> Alcotest.fail "Should have failed but succeeded"

let test_parse_error_payload () =
  let json_str = {|
    {
      "channel": "post",
      "data": {
        "id": 123,
        "response": {
          "type": "action",
          "payload": {
            "status": "err",
            "response": "Insufficient margin"
          }
        }
      }
    }
  |} in
  let json = Yojson.Safe.from_string json_str in
  let result = Hyperliquid.Hyperliquid_module.parse_order_response "BTC/USDC" 100L "cloid-1" 0.1 (Some 50000.0) json in
  match result with
  | Error e -> Alcotest.(check string) "error message" "Insufficient margin" e
  | Ok _ -> Alcotest.fail "Should have failed but succeeded"

let test_parse_action_success_empty_statuses () =
  let json_str = {|
    {
      "channel": "post",
      "data": {
        "id": 123,
        "response": {
          "type": "action",
          "payload": {
            "status": "ok",
            "response": {
              "type": "order",
              "data": {
                "statuses": []
              }
            }
          }
        }
      }
    }
  |} in
  let json = Yojson.Safe.from_string json_str in
  let result = Hyperliquid.Hyperliquid_module.parse_order_response "BTC/USDC" 100L "cloid-1" 0.1 (Some 50000.0) json in
  match result with
  | Ok (oid, _) ->
      Alcotest.(check bool) "mock order id generated" true (String.starts_with ~prefix:"hl-" oid)
  | Error e -> Alcotest.fail ("Should have succeeded with mock ID but failed with: " ^ e)


let test_parse_modify_response () =
  let json_str = {|
    {
      "channel": "post",
      "data": {
        "id": 123,
        "response": {
          "type": "action",
          "payload": {
            "status": "ok",
            "response": {
              "type": "modify",
              "data": {
                "statuses": [
                  {
                    "resting": {
                      "oid": 67890
                    }
                  }
                ]
              }
            }
          }
        }
      }
    }
  |} in
  let json = Yojson.Safe.from_string json_str in
  let result = Hyperliquid.Hyperliquid_module.parse_order_response "BTC/USDC" 100L "cloid-modify" 0.1 (Some 50000.0) json in
  match result with
  | Ok (oid, cloid) ->
      Alcotest.(check string) "new order id" "67890" oid;
      Alcotest.(check string) "cloid" "cloid-modify" cloid
  | Error e -> Alcotest.fail ("Should have succeeded but failed with: " ^ e)

let test_parse_batch_modify_response () =
  let json_str = {|
    {
      "channel": "post",
      "data": {
        "id": 123,
        "response": {
          "type": "action",
          "payload": {
            "status": "ok",
            "response": {
              "type": "batchModify",
              "data": {
                "statuses": [
                  {
                    "resting": {
                      "oid": 54321
                    }
                  }
                ]
              }
            }
          }
        }
      }
    }
  |} in
  let json = Yojson.Safe.from_string json_str in
  let result = Hyperliquid.Hyperliquid_module.parse_order_response "BTC/USDC" 100L "cloid-batch" 0.1 (Some 50000.0) json in
  match result with
  | Ok (oid, cloid) ->
      Alcotest.(check string) "batch modify order id" "54321" oid;
      Alcotest.(check string) "cloid" "cloid-batch" cloid
  | Error e -> Alcotest.fail ("Should have succeeded but failed with: " ^ e)

let () =
  Alcotest.run "Hyperliquid Parsing" [
    "order response", [
      Alcotest.test_case "success resting order" `Quick test_parse_success_order;
      Alcotest.test_case "outer error response" `Quick test_parse_error_outer;
      Alcotest.test_case "payload error response" `Quick test_parse_error_payload;
      Alcotest.test_case "action success empty statuses" `Quick test_parse_action_success_empty_statuses;
      Alcotest.test_case "modify response parsing" `Quick test_parse_modify_response;
      Alcotest.test_case "batchModify response parsing" `Quick test_parse_batch_modify_response;
    ];
  ]
