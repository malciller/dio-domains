let test_find_registered_symbol () =
  (* Initialize instruments feed so resolve_symbol works (mocking it via initialize or just testing the fallback) *)
  (* For testing the fallback, we can inject a store directly *)
  Hyperliquid.Executions_feed.initialize ["HYPE"; "BTC"];
  
  (* In the test environment, without actual instruments feed populated, it will rely on the fallback matching *)
  let result1 = Hyperliquid.Executions_feed.find_registered_symbol "HYPE" in
  Alcotest.(check (option string)) "Direct match" (Some "HYPE") result1;
  
  let result2 = Hyperliquid.Executions_feed.find_registered_symbol "BTC" in
  Alcotest.(check (option string)) "Direct match" (Some "BTC") result2;
  
  let result3 = Hyperliquid.Executions_feed.find_registered_symbol "UNKNOWN" in
  Alcotest.(check (option string)) "No match" None result3

let test_inject_order () =
  Hyperliquid.Executions_feed.initialize ["HYPE"];
  
  Hyperliquid.Executions_feed.inject_order
    ~symbol:"HYPE"
    ~order_id:"order123"
    ~side:Hyperliquid.Executions_feed.Buy
    ~qty:1.5
    ~price:100.0
    ~user_ref:42
    ~cl_ord_id:"client123"
    ();
    
  let order_opt = Hyperliquid.Executions_feed.get_open_order "HYPE" "order123" in
  match order_opt with
  | Some o ->
      Alcotest.(check string) "symbol" "HYPE" o.symbol;
      Alcotest.(check (float 0.000001)) "qty" 1.5 o.order_qty;
      Alcotest.(check (option (float 0.000001))) "price" (Some 100.0) o.limit_price;
      Alcotest.(check (option int)) "user_ref" (Some 42) o.order_userref;
      Alcotest.(check (option string)) "cl_ord_id" (Some "client123") o.cl_ord_id
  | None -> Alcotest.fail "Order not injected"

let test_process_order_updates () =
  Hyperliquid.Executions_feed.initialize ["HYPE"];
  
  let json = `List [
    `Assoc [
      ("order", `Assoc [
        ("coin", `String "HYPE");
        ("oid", `Int 999);
        ("status", `String "open");
        ("side", `String "B");
        ("limitPx", `String "150.0");
        ("sz", `String "2.0");
        ("cloid", `String "client999");
      ]);
      ("status", `String "open")
    ]
  ] in
  
  Hyperliquid.Executions_feed.process_order_updates json;
  
  let order_opt = Hyperliquid.Executions_feed.get_open_order "HYPE" "999" in
  match order_opt with
  | Some o ->
      Alcotest.(check string) "symbol" "HYPE" o.symbol;
      Alcotest.(check (float 0.000001)) "qty" 2.0 o.order_qty;
      Alcotest.(check (option string)) "cl_ord_id" (Some "client999") o.cl_ord_id
  | None -> Alcotest.fail "Order update not processed"

let test_get_open_orders () =
  Hyperliquid.Executions_feed.initialize ["TESTORDERS"];

  Hyperliquid.Executions_feed.inject_order
    ~symbol:"TESTORDERS" ~order_id:"ord_a" ~side:Hyperliquid.Executions_feed.Buy
    ~qty:1.0 ~price:100.0 ~user_ref:1 ~cl_ord_id:"cl_a" ();

  Hyperliquid.Executions_feed.inject_order
    ~symbol:"TESTORDERS" ~order_id:"ord_b" ~side:Hyperliquid.Executions_feed.Sell
    ~qty:2.0 ~price:200.0 ~user_ref:2 ~cl_ord_id:"cl_b" ();

  let orders = Hyperliquid.Executions_feed.get_open_orders "TESTORDERS" in
  Alcotest.(check bool) "at least 2 orders" true (List.length orders >= 2);
  let has_a = List.exists (fun (o : Hyperliquid.Executions_feed.open_order) -> o.order_id = "ord_a") orders in
  let has_b = List.exists (fun (o : Hyperliquid.Executions_feed.open_order) -> o.order_id = "ord_b") orders in
  Alcotest.(check bool) "has ord_a" true has_a;
  Alcotest.(check bool) "has ord_b" true has_b

let test_process_order_terminal_status () =
  Hyperliquid.Executions_feed.initialize ["TESTTERM"];

  (* Create an order first *)
  let json_open = `List [
    `Assoc [
      ("order", `Assoc [
        ("coin", `String "TESTTERM");
        ("oid", `Int 5555);
        ("status", `String "open");
        ("side", `String "B");
        ("limitPx", `String "100.0");
        ("sz", `String "1.0");
        ("cloid", `Null);
      ]);
      ("status", `String "open")
    ]
  ] in
  Hyperliquid.Executions_feed.process_order_updates json_open;

  let order_open = Hyperliquid.Executions_feed.get_open_order "TESTTERM" "5555" in
  Alcotest.(check bool) "order exists" true (Option.is_some order_open);

  (* Cancel it *)
  let json_cancel = `List [
    `Assoc [
      ("order", `Assoc [
        ("coin", `String "TESTTERM");
        ("oid", `Int 5555);
        ("status", `String "canceled");
        ("side", `String "B");
        ("limitPx", `String "100.0");
        ("sz", `String "1.0");
        ("cloid", `Null);
      ]);
      ("status", `String "canceled")
    ]
  ] in
  Hyperliquid.Executions_feed.process_order_updates json_cancel;

  let order_gone = Hyperliquid.Executions_feed.get_open_order "TESTTERM" "5555" in
  Alcotest.(check bool) "order removed" true (Option.is_none order_gone)

let test_find_order_everywhere () =
  Hyperliquid.Executions_feed.initialize ["TESTFIND"];

  Hyperliquid.Executions_feed.inject_order
    ~symbol:"TESTFIND" ~order_id:"findme_123" ~side:Hyperliquid.Executions_feed.Buy
    ~qty:0.5 ~price:50.0 ~user_ref:99 ~cl_ord_id:"cl_find" ();

  let result = Hyperliquid.Executions_feed.find_order_everywhere "findme_123" in
  Alcotest.(check bool) "found" true (Option.is_some result);
  (match result with
   | Some o -> Alcotest.(check string) "symbol" "TESTFIND" o.symbol
   | None -> Alcotest.fail "unreachable");

  let missing = Hyperliquid.Executions_feed.find_order_everywhere "nonexistent_999" in
  Alcotest.(check bool) "not found" true (Option.is_none missing)

let test_get_all_symbols () =
  Hyperliquid.Executions_feed.initialize ["SYM_A"; "SYM_B"];
  let symbols = Hyperliquid.Executions_feed.get_all_symbols () in
  let has_a = List.mem "SYM_A" symbols in
  let has_b = List.mem "SYM_B" symbols in
  Alcotest.(check bool) "has SYM_A" true has_a;
  Alcotest.(check bool) "has SYM_B" true has_b

let () =
  Alcotest.run "Hyperliquid Executions Feed" [
    "symbols", [
      Alcotest.test_case "find_registered_symbol" `Quick test_find_registered_symbol;
      Alcotest.test_case "get_all_symbols" `Quick test_get_all_symbols;
    ];
    "orders", [
      Alcotest.test_case "inject_order" `Quick test_inject_order;
      Alcotest.test_case "process_order_updates" `Quick test_process_order_updates;
      Alcotest.test_case "get_open_orders" `Quick test_get_open_orders;
      Alcotest.test_case "terminal status removes order" `Quick test_process_order_terminal_status;
      Alcotest.test_case "find_order_everywhere" `Quick test_find_order_everywhere;
    ]
  ]
