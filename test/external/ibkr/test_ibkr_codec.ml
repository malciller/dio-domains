let test_encode_fields_single () =
  let result = Ibkr.Codec.encode_fields ["hello"] in
  (* payload = "hello\x00" = 6 bytes, total = 4 + 6 = 10 *)
  Alcotest.(check int) "encoded length" 10 (Bytes.length result);
  (* Check length prefix is big-endian 6 *)
  Alcotest.(check int) "length byte 0" 0 (Bytes.get_uint8 result 0);
  Alcotest.(check int) "length byte 1" 0 (Bytes.get_uint8 result 1);
  Alcotest.(check int) "length byte 2" 0 (Bytes.get_uint8 result 2);
  Alcotest.(check int) "length byte 3" 6 (Bytes.get_uint8 result 3)

let test_encode_fields_multiple () =
  let result = Ibkr.Codec.encode_fields ["a"; "bb"; "ccc"] in
  (* payload = "a\x00bb\x00ccc\x00" = 9 bytes *)
  let expected_payload = "a\x00bb\x00ccc\x00" in
  let payload = Bytes.sub_string result 4 (Bytes.length result - 4) in
  Alcotest.(check string) "payload content" expected_payload payload;
  (* Length prefix should be 9 *)
  let len = (Bytes.get_uint8 result 0 lsl 24) lor
            (Bytes.get_uint8 result 1 lsl 16) lor
            (Bytes.get_uint8 result 2 lsl 8) lor
            (Bytes.get_uint8 result 3) in
  Alcotest.(check int) "length prefix" 9 len

let test_encode_fields_empty_list () =
  let result = Ibkr.Codec.encode_fields [] in
  (* payload = "\x00" = 1 byte *)
  Alcotest.(check int) "empty list encoded length" 5 (Bytes.length result);
  Alcotest.(check int) "empty list length prefix" 1
    (Bytes.get_uint8 result 3)

let test_encode_fields_empty_strings () =
  let result = Ibkr.Codec.encode_fields [""; ""; ""] in
  (* payload = "\x00\x00\x00" = 3 bytes *)
  let len = Bytes.get_uint8 result 3 in
  Alcotest.(check int) "empty strings length" 3 len

let test_decode_fields_basic () =
  let payload = "hello\x00world\x00" in
  let result = Ibkr.Codec.decode_fields payload in
  Alcotest.(check (list string)) "decode basic" ["hello"; "world"] result

let test_decode_fields_single () =
  let payload = "onlyone\x00" in
  let result = Ibkr.Codec.decode_fields payload in
  Alcotest.(check (list string)) "decode single" ["onlyone"] result

let test_decode_fields_empty_payload () =
  let result = Ibkr.Codec.decode_fields "" in
  Alcotest.(check (list string)) "decode empty" [] result

let test_decode_fields_empty_fields () =
  let payload = "\x00\x00\x00" in
  let result = Ibkr.Codec.decode_fields payload in
  Alcotest.(check (list string)) "decode empty fields" [""; ""; ""] result

let test_encode_decode_roundtrip () =
  let original = ["71"; "2"; "0"; ""] in
  let encoded = Ibkr.Codec.encode_fields original in
  (* Skip 4-byte length prefix, decode payload *)
  let payload = Bytes.sub_string encoded 4 (Bytes.length encoded - 4) in
  let decoded = Ibkr.Codec.decode_fields payload in
  Alcotest.(check (list string)) "roundtrip" original decoded

let test_encode_handshake () =
  let result = Ibkr.Codec.encode_handshake ~min_ver:100 ~max_ver:176 in
  (* Should start with "API\x00" *)
  Alcotest.(check char) "API byte 0" 'A' (Bytes.get result 0);
  Alcotest.(check char) "API byte 1" 'P' (Bytes.get result 1);
  Alcotest.(check char) "API byte 2" 'I' (Bytes.get result 2);
  Alcotest.(check int) "API null byte" 0 (Bytes.get_uint8 result 3);
  (* After "API\x00" (4 bytes) comes 4-byte length prefix then "v100..176" *)
  let version_str = "v100..176" in
  let version_len = String.length version_str in
  let len_from_prefix =
    (Bytes.get_uint8 result 4 lsl 24) lor
    (Bytes.get_uint8 result 5 lsl 16) lor
    (Bytes.get_uint8 result 6 lsl 8) lor
    (Bytes.get_uint8 result 7) in
  Alcotest.(check int) "version length prefix" version_len len_from_prefix;
  let version_payload = Bytes.sub_string result 8 version_len in
  Alcotest.(check string) "version string" version_str version_payload

let test_read_string () =
  let v, rest = Ibkr.Codec.read_string ["hello"; "world"] in
  Alcotest.(check string) "read_string value" "hello" v;
  Alcotest.(check (list string)) "read_string rest" ["world"] rest

let test_read_string_empty () =
  let v, rest = Ibkr.Codec.read_string [] in
  Alcotest.(check string) "read_string empty value" "" v;
  Alcotest.(check (list string)) "read_string empty rest" [] rest

let test_read_int () =
  let v, rest = Ibkr.Codec.read_int ["42"; "next"] in
  Alcotest.(check int) "read_int value" 42 v;
  Alcotest.(check (list string)) "read_int rest" ["next"] rest

let test_read_int_invalid () =
  let v, rest = Ibkr.Codec.read_int ["not_a_number"; "next"] in
  Alcotest.(check int) "read_int invalid defaults to 0" 0 v;
  Alcotest.(check (list string)) "read_int invalid rest" ["next"] rest

let test_read_int_empty () =
  let v, rest = Ibkr.Codec.read_int [] in
  Alcotest.(check int) "read_int empty defaults to 0" 0 v;
  Alcotest.(check (list string)) "read_int empty rest" [] rest

let test_read_float () =
  let v, rest = Ibkr.Codec.read_float ["3.14"; "next"] in
  Alcotest.(check (float 0.001)) "read_float value" 3.14 v;
  Alcotest.(check (list string)) "read_float rest" ["next"] rest

let test_read_float_max_value () =
  (* TWS Double.MAX_VALUE sentinel *)
  let v, _ = Ibkr.Codec.read_float ["1.7976931348623157E308"] in
  Alcotest.(check (float 0.001)) "read_float MAX_VALUE becomes 0.0" 0.0 v

let test_read_float_empty_string () =
  let v, _ = Ibkr.Codec.read_float [""] in
  Alcotest.(check (float 0.001)) "read_float empty string becomes 0.0" 0.0 v

let test_read_float_invalid () =
  let v, _ = Ibkr.Codec.read_float ["not_a_float"] in
  Alcotest.(check (float 0.001)) "read_float invalid defaults to 0.0" 0.0 v

let test_read_float_empty_list () =
  let v, rest = Ibkr.Codec.read_float [] in
  Alcotest.(check (float 0.001)) "read_float empty list" 0.0 v;
  Alcotest.(check (list string)) "read_float empty list rest" [] rest

let test_read_bool_true () =
  let v1, _ = Ibkr.Codec.read_bool ["1"] in
  let v2, _ = Ibkr.Codec.read_bool ["true"] in
  Alcotest.(check bool) "read_bool 1" true v1;
  Alcotest.(check bool) "read_bool true" true v2

let test_read_bool_false () =
  let v1, _ = Ibkr.Codec.read_bool ["0"] in
  let v2, _ = Ibkr.Codec.read_bool ["false"] in
  let v3, _ = Ibkr.Codec.read_bool [""] in
  Alcotest.(check bool) "read_bool 0" false v1;
  Alcotest.(check bool) "read_bool false" false v2;
  Alcotest.(check bool) "read_bool empty" false v3

let test_read_bool_empty_list () =
  let v, rest = Ibkr.Codec.read_bool [] in
  Alcotest.(check bool) "read_bool empty list" false v;
  Alcotest.(check (list string)) "read_bool empty rest" [] rest

let test_skip () =
  let fields = ["a"; "b"; "c"; "d"; "e"] in
  let result = Ibkr.Codec.skip 3 fields in
  Alcotest.(check (list string)) "skip 3" ["d"; "e"] result

let test_skip_zero () =
  let fields = ["a"; "b"] in
  let result = Ibkr.Codec.skip 0 fields in
  Alcotest.(check (list string)) "skip 0" ["a"; "b"] result

let test_skip_more_than_available () =
  let fields = ["a"; "b"] in
  let result = Ibkr.Codec.skip 10 fields in
  Alcotest.(check (list string)) "skip overflow" [] result

let test_read_int_max () =
  let v1, _ = Ibkr.Codec.read_int_max [""] in
  let v2, _ = Ibkr.Codec.read_int_max ["2147483647"] in
  let v3, _ = Ibkr.Codec.read_int_max ["42"] in
  Alcotest.(check int) "read_int_max empty" max_int v1;
  Alcotest.(check int) "read_int_max MAX_VALUE" max_int v2;
  Alcotest.(check int) "read_int_max normal" 42 v3

let test_read_int_max_empty_list () =
  let v, rest = Ibkr.Codec.read_int_max [] in
  Alcotest.(check int) "read_int_max empty list" max_int v;
  Alcotest.(check (list string)) "read_int_max empty rest" [] rest

let test_encode_contract () =
  let contract = {
    Ibkr.Types.con_id = 265598;
    symbol = "AAPL";
    sec_type = "STK";
    exchange = "SMART";
    currency = "USD";
    local_symbol = "AAPL";
    trading_class = "AAPL";
    min_tick = 0.01;
    multiplier = "";
  } in
  let fields = Ibkr.Codec.encode_contract contract in
  (* Should be 13 fields *)
  Alcotest.(check int) "encode_contract field count" 13 (List.length fields);
  Alcotest.(check string) "encode_contract con_id" "265598" (List.nth fields 0);
  Alcotest.(check string) "encode_contract symbol" "AAPL" (List.nth fields 1);
  Alcotest.(check string) "encode_contract sec_type" "STK" (List.nth fields 2);
  Alcotest.(check string) "encode_contract exchange" "SMART" (List.nth fields 7);
  Alcotest.(check string) "encode_contract currency" "USD" (List.nth fields 9);
  Alcotest.(check string) "encode_contract includeExpired" "0" (List.nth fields 12)

let test_encode_contract_short () =
  let contract = Ibkr.Types.make_stk_contract ~symbol:"SPY" in
  let fields = Ibkr.Codec.encode_contract_short contract in
  (* Should be 12 fields (no includeExpired) *)
  Alcotest.(check int) "encode_contract_short field count" 12 (List.length fields);
  Alcotest.(check string) "encode_contract_short symbol" "SPY" (List.nth fields 1);
  Alcotest.(check string) "encode_contract_short sec_type" "STK" (List.nth fields 2)

let test_encode_order () =
  let order = Ibkr.Types.make_limit_order ~order_id:1 ~action:"BUY" ~qty:10.0 ~price:150.0 in
  let fields = Ibkr.Codec.encode_order order in
  (* Should be 19 fields *)
  Alcotest.(check int) "encode_order field count" 19 (List.length fields);
  Alcotest.(check string) "encode_order action" "BUY" (List.nth fields 0);
  Alcotest.(check string) "encode_order order_type" "LMT" (List.nth fields 2);
  Alcotest.(check string) "encode_order tif" "GTC" (List.nth fields 5)

let test_encode_order_market () =
  let order = Ibkr.Types.make_market_order ~order_id:1 ~action:"SELL" ~qty:5.0 in
  let fields = Ibkr.Codec.encode_order order in
  Alcotest.(check string) "encode_order market action" "SELL" (List.nth fields 0);
  Alcotest.(check string) "encode_order market type" "MKT" (List.nth fields 2);
  (* lmt_price should be empty for market orders *)
  Alcotest.(check string) "encode_order market lmt_price" "" (List.nth fields 3)

let test_encode_order_tail () =
  let fields = Ibkr.Codec.encode_order_tail () in
  (* Should be a long list of defaults *)
  Alcotest.(check bool) "encode_order_tail has many fields" true (List.length fields > 50)

let () =
  Alcotest.run "IBKR Codec" [
    "encoding", [
      Alcotest.test_case "encode_fields_single" `Quick test_encode_fields_single;
      Alcotest.test_case "encode_fields_multiple" `Quick test_encode_fields_multiple;
      Alcotest.test_case "encode_fields_empty_list" `Quick test_encode_fields_empty_list;
      Alcotest.test_case "encode_fields_empty_strings" `Quick test_encode_fields_empty_strings;
      Alcotest.test_case "encode_handshake" `Quick test_encode_handshake;
    ];
    "decoding", [
      Alcotest.test_case "decode_fields_basic" `Quick test_decode_fields_basic;
      Alcotest.test_case "decode_fields_single" `Quick test_decode_fields_single;
      Alcotest.test_case "decode_fields_empty_payload" `Quick test_decode_fields_empty_payload;
      Alcotest.test_case "decode_fields_empty_fields" `Quick test_decode_fields_empty_fields;
    ];
    "roundtrip", [
      Alcotest.test_case "encode_decode_roundtrip" `Quick test_encode_decode_roundtrip;
    ];
    "field readers", [
      Alcotest.test_case "read_string" `Quick test_read_string;
      Alcotest.test_case "read_string_empty" `Quick test_read_string_empty;
      Alcotest.test_case "read_int" `Quick test_read_int;
      Alcotest.test_case "read_int_invalid" `Quick test_read_int_invalid;
      Alcotest.test_case "read_int_empty" `Quick test_read_int_empty;
      Alcotest.test_case "read_float" `Quick test_read_float;
      Alcotest.test_case "read_float_max_value" `Quick test_read_float_max_value;
      Alcotest.test_case "read_float_empty_string" `Quick test_read_float_empty_string;
      Alcotest.test_case "read_float_invalid" `Quick test_read_float_invalid;
      Alcotest.test_case "read_float_empty_list" `Quick test_read_float_empty_list;
      Alcotest.test_case "read_bool_true" `Quick test_read_bool_true;
      Alcotest.test_case "read_bool_false" `Quick test_read_bool_false;
      Alcotest.test_case "read_bool_empty_list" `Quick test_read_bool_empty_list;
      Alcotest.test_case "read_int_max" `Quick test_read_int_max;
      Alcotest.test_case "read_int_max_empty_list" `Quick test_read_int_max_empty_list;
    ];
    "skip", [
      Alcotest.test_case "skip" `Quick test_skip;
      Alcotest.test_case "skip_zero" `Quick test_skip_zero;
      Alcotest.test_case "skip_more_than_available" `Quick test_skip_more_than_available;
    ];
    "contract encoding", [
      Alcotest.test_case "encode_contract" `Quick test_encode_contract;
      Alcotest.test_case "encode_contract_short" `Quick test_encode_contract_short;
    ];
    "order encoding", [
      Alcotest.test_case "encode_order" `Quick test_encode_order;
      Alcotest.test_case "encode_order_market" `Quick test_encode_order_market;
      Alcotest.test_case "encode_order_tail" `Quick test_encode_order_tail;
    ];
  ]
