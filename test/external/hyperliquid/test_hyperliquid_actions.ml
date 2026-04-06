module ExTypes = Dio_exchange.Exchange_intf.Types

let test_hl_order_type () =
  let hl_type1 = Hyperliquid.Actions.hl_order_type ExTypes.Limit (Some ExTypes.GTC) in
  (match hl_type1 with
  | Hyperliquid.Types.Limit { tif } -> Alcotest.(check string) "Limit GTC" "Alo" (Hyperliquid.Types.tif_to_string tif) (* The code maps Limit to Alo always, tif arg is ignored *)
  | _ -> Alcotest.fail "Expected Limit");
  
  let hl_type2 = Hyperliquid.Actions.hl_order_type ExTypes.Market None in
  (match hl_type2 with
  | Hyperliquid.Types.Limit { tif } -> Alcotest.(check string) "Market IOC" "Ioc" (Hyperliquid.Types.tif_to_string tif)
  | _ -> Alcotest.fail "Expected Limit")

let test_format_number () =
  Alcotest.(check string) "integer" "1" (Hyperliquid.Actions.format_number 1.0);
  Alcotest.(check string) "decimals" "1.234" (Hyperliquid.Actions.format_number 1.234);
  Alcotest.(check string) "trailing zeros stripped" "1.234" (Hyperliquid.Actions.format_number 1.23400000);
  Alcotest.(check string) "zero" "0" (Hyperliquid.Actions.format_number 0.0);
  Alcotest.(check string) "negative zero" "0" (Hyperliquid.Actions.format_number (-0.0));
  Alcotest.(check string) "small number" "0.0001" (Hyperliquid.Actions.format_number 0.0001)

let test_to_hl_order_wire () =
  let wire = Hyperliquid.Actions.to_hl_order_wire
    ~qty:1.5
    ~symbol:"TEST"
    ~asset_index:5
    ~ot:(Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Alo })
    ~side:ExTypes.Buy
    ~limit_price:(Some 123.45)
    ~reduce_only:None
    ~cl_ord_id:(Some "client_id")
  in
  Alcotest.(check int) "a" 5 wire.a;
  Alcotest.(check bool) "b" true wire.b;
  Alcotest.(check string) "p" "123.45" wire.p;
  Alcotest.(check string) "s" "1.5" wire.s;
  Alcotest.(check bool) "r" false wire.r;
  Alcotest.(check (option string)) "c" (Some "client_id") wire.c

let test_string_contains () =
  Alcotest.(check bool) "present" true (Hyperliquid.Actions.string_contains "hello world" "world");
  Alcotest.(check bool) "absent" false (Hyperliquid.Actions.string_contains "hello world" "xyz");
  Alcotest.(check bool) "empty substr" true (Hyperliquid.Actions.string_contains "hello" "");
  Alcotest.(check bool) "empty string" false (Hyperliquid.Actions.string_contains "" "a");
  Alcotest.(check bool) "both empty" true (Hyperliquid.Actions.string_contains "" "");
  Alcotest.(check bool) "at start" true (Hyperliquid.Actions.string_contains "hello" "hel");
  Alcotest.(check bool) "at end" true (Hyperliquid.Actions.string_contains "hello" "llo");
  Alcotest.(check bool) "exact" true (Hyperliquid.Actions.string_contains "hello" "hello");
  Alcotest.(check bool) "longer substr" false (Hyperliquid.Actions.string_contains "hi" "hello")

let test_is_retriable_error () =
  (* Network errors *)
  Alcotest.(check bool) "timeout" true (Hyperliquid.Actions.is_retriable_error "Connection timeout");
  Alcotest.(check bool) "connection" true (Hyperliquid.Actions.is_retriable_error "connection refused");
  Alcotest.(check bool) "network" true (Hyperliquid.Actions.is_retriable_error "network unreachable");
  Alcotest.(check bool) "reset" true (Hyperliquid.Actions.is_retriable_error "connection reset by peer");
  Alcotest.(check bool) "broken pipe" true (Hyperliquid.Actions.is_retriable_error "Broken pipe");
  (* Server errors *)
  Alcotest.(check bool) "500" true (Hyperliquid.Actions.is_retriable_error "HTTP Error 500");
  Alcotest.(check bool) "502" true (Hyperliquid.Actions.is_retriable_error "HTTP Error 502: Bad Gateway");
  Alcotest.(check bool) "503" true (Hyperliquid.Actions.is_retriable_error "503 Service Unavailable");
  Alcotest.(check bool) "504" true (Hyperliquid.Actions.is_retriable_error "504 Gateway Timeout");
  (* Rate limiting *)
  Alcotest.(check bool) "rate limit" true (Hyperliquid.Actions.is_retriable_error "Rate limit exceeded");
  Alcotest.(check bool) "too many requests" true (Hyperliquid.Actions.is_retriable_error "Too many requests");
  Alcotest.(check bool) "too many cumulative" true (Hyperliquid.Actions.is_retriable_error "Too many cumulative requests");
  (* Non-retriable *)
  Alcotest.(check bool) "rejected" false (Hyperliquid.Actions.is_retriable_error "Order rejected: insufficient balance");
  Alcotest.(check bool) "unknown symbol" false (Hyperliquid.Actions.is_retriable_error "Unknown symbol: XYZ");
  Alcotest.(check bool) "empty" false (Hyperliquid.Actions.is_retriable_error "")

let test_get_next_nonce () =
  let n1 = Hyperliquid.Actions.get_next_nonce () in
  let n2 = Hyperliquid.Actions.get_next_nonce () in
  let n3 = Hyperliquid.Actions.get_next_nonce () in
  Alcotest.(check bool) "n2 > n1" true (Int64.compare n2 n1 > 0);
  Alcotest.(check bool) "n3 > n2" true (Int64.compare n3 n2 > 0);
  Alcotest.(check bool) "n1 > 0" true (Int64.compare n1 0L > 0)

let test_to_int64_opt () =
  Alcotest.(check (option int64)) "Int" (Some 42L) (Hyperliquid.Actions.to_int64_opt (`Int 42));
  Alcotest.(check (option int64)) "Intlit" (Some 123456789012345L) (Hyperliquid.Actions.to_int64_opt (`Intlit "123456789012345"));
  Alcotest.(check (option int64)) "String" (Some 99L) (Hyperliquid.Actions.to_int64_opt (`String "99"));
  Alcotest.(check (option int64)) "bad String" None (Hyperliquid.Actions.to_int64_opt (`String "abc"));
  Alcotest.(check (option int64)) "Null" None (Hyperliquid.Actions.to_int64_opt `Null);
  Alcotest.(check (option int64)) "Float" None (Hyperliquid.Actions.to_int64_opt (`Float 1.5))

let () =
  Alcotest.run "Hyperliquid Actions" [
    "conversion", [
      Alcotest.test_case "hl_order_type" `Quick test_hl_order_type;
      Alcotest.test_case "format_number" `Quick test_format_number;
      Alcotest.test_case "to_hl_order_wire" `Quick test_to_hl_order_wire;
    ];
    "utils", [
      Alcotest.test_case "string_contains" `Quick test_string_contains;
      Alcotest.test_case "is_retriable_error" `Quick test_is_retriable_error;
      Alcotest.test_case "get_next_nonce" `Quick test_get_next_nonce;
      Alcotest.test_case "to_int64_opt" `Quick test_to_int64_opt;
    ]
  ]
