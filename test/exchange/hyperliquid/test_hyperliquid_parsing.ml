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

let test_pack_batch_modify_action () =
  let order : Hyperliquid.Hyperliquid_types.order_wire = {
    a = 1;
    b = true;
    p = "50000.0";
    s = "0.1";
    r = false;
    t = Limit { tif = Gtc };
    c = Some "cloid-123";
  } in
  let msgpck = Hyperliquid.Hyperliquid_types.pack_batch_modify_action 
    ~modifies:[(12345L, order)] 
    ~grouping:"na" in
  match msgpck with
  | Msgpck.Map pairs ->
      let get_val k = List.assoc (Msgpck.String k) pairs in
      Alcotest.(check string) "type" "batchModify" (match get_val "type" with String s -> s | _ -> "");
      Alcotest.(check string) "grouping" "na" (match get_val "grouping" with String s -> s | _ -> "");
      let modifies_list = match get_val "modifies" with List l -> l | _ -> [] in
      Alcotest.(check int) "modifies length" 1 (List.length modifies_list);
      let mod_map = match List.hd modifies_list with Map m -> m | _ -> [] in
      let get_mod_val k = List.assoc (Msgpck.String k) mod_map in
      Alcotest.(check int64) "oid" 12345L (match get_mod_val "oid" with Int64 i -> i | _ -> 0L);
      let order_map = match get_mod_val "order" with Map m -> m | _ -> [] in
      let get_order_val k = List.assoc (Msgpck.String k) order_map in
      Alcotest.(check string) "order p" "50000.0" (match get_order_val "p" with String s -> s | _ -> "")
  | _ -> Alcotest.fail "Should have returned a Map"

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
    "action packing", [
      Alcotest.test_case "pack batch modify action" `Quick test_pack_batch_modify_action;
    ];
  ]
