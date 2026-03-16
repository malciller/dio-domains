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

let () =
  Alcotest.run "Hyperliquid Executions Feed" [
    "symbols", [
      Alcotest.test_case "find_registered_symbol" `Quick test_find_registered_symbol;
    ];
    "orders", [
      Alcotest.test_case "inject_order" `Quick test_inject_order;
      Alcotest.test_case "process_order_updates" `Quick test_process_order_updates;
    ]
  ]
