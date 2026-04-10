let test_inject_order () =
  Lighter.Executions_feed.initialize ["EXETH"];

  Lighter.Executions_feed.inject_order
    ~symbol:"EXETH"
    ~order_id:"order_001"
    ~side:Lighter.Executions_feed.Buy
    ~qty:1.5
    ~price:2180.00
    ~cl_ord_id:"cl_001"
    ();

  let order_opt = Lighter.Executions_feed.get_open_order "EXETH" "order_001" in
  match order_opt with
  | Some o ->
      Alcotest.(check string) "symbol" "EXETH" o.symbol;
      Alcotest.(check string) "order_id" "order_001" o.order_id;
      Alcotest.(check (float 0.000001)) "qty" 1.5 o.order_qty;
      Alcotest.(check (option (float 0.000001))) "price" (Some 2180.00) o.limit_price;
      Alcotest.(check (option string)) "cl_ord_id" (Some "cl_001") o.cl_ord_id
  | None -> Alcotest.fail "Order not injected"

let test_get_open_orders () =
  Lighter.Executions_feed.initialize ["EXMULTI"];

  Lighter.Executions_feed.inject_order
    ~symbol:"EXMULTI" ~order_id:"m_1" ~side:Lighter.Executions_feed.Buy
    ~qty:1.0 ~price:100.0 ();

  Lighter.Executions_feed.inject_order
    ~symbol:"EXMULTI" ~order_id:"m_2" ~side:Lighter.Executions_feed.Sell
    ~qty:2.0 ~price:200.0 ();

  let orders = Lighter.Executions_feed.get_open_orders "EXMULTI" in
  Alcotest.(check bool) "at least 2" true (List.length orders >= 2);
  let has_1 = List.exists (fun (o : Lighter.Executions_feed.open_order) -> o.order_id = "m_1") orders in
  let has_2 = List.exists (fun (o : Lighter.Executions_feed.open_order) -> o.order_id = "m_2") orders in
  Alcotest.(check bool) "has m_1" true has_1;
  Alcotest.(check bool) "has m_2" true has_2

let test_find_order_everywhere () =
  Lighter.Executions_feed.initialize ["EXFIND"];

  Lighter.Executions_feed.inject_order
    ~symbol:"EXFIND" ~order_id:"findme_42" ~side:Lighter.Executions_feed.Buy
    ~qty:0.5 ~price:50.0 ~cl_ord_id:"cl_find" ();

  let result = Lighter.Executions_feed.find_order_everywhere "findme_42" in
  Alcotest.(check bool) "found" true (Option.is_some result);
  (match result with
   | Some o -> Alcotest.(check string) "symbol" "EXFIND" o.symbol
   | None -> Alcotest.fail "unreachable");

  let missing = Lighter.Executions_feed.find_order_everywhere "nonexistent_999" in
  Alcotest.(check bool) "not found" true (Option.is_none missing)

let test_terminal_status_removes_order () =
  Lighter.Executions_feed.initialize ["EXTERM"];

  Lighter.Executions_feed.inject_order
    ~symbol:"EXTERM" ~order_id:"term_1" ~side:Lighter.Executions_feed.Buy
    ~qty:1.0 ~price:100.0 ();

  let before = Lighter.Executions_feed.get_open_order "EXTERM" "term_1" in
  Alcotest.(check bool) "order exists" true (Option.is_some before);

  (* Simulate a filled event via process_account_orders_update.
     Must provide correct Lighter JSON structure. *)
  Lighter.Instruments_feed.initialize ["EXTERM"];
  let json = `Assoc [
    ("type", `String "update/account_all_orders");
    ("account_all_orders", `Assoc [
      ("0", `Assoc [
        ("order_index", `String "term_1");
        ("market_index", `Int 0);
        ("status", `Int 3);  (* Filled *)
        ("is_ask", `Bool false);
        ("price", `String "100.0");
        ("initial_base_amount", `String "1.0");
        ("filled_base_amount", `String "1.0");
        ("avg_fill_price", `String "100.0")
      ])
    ])
  ] in
  Lighter.Executions_feed.process_account_orders_update json;

  let after = Lighter.Executions_feed.get_open_order "EXTERM" "term_1" in
  Alcotest.(check bool) "order removed after fill" true (Option.is_none after)

let test_startup_snapshot_done () =
  (* is_startup_snapshot_done should return true after set_startup_snapshot_done *)
  Lighter.Executions_feed.initialize ["EXSNAP"];
  Lighter.Executions_feed.set_startup_snapshot_done ();
  Alcotest.(check bool) "snapshot done" true (Lighter.Executions_feed.is_startup_snapshot_done ())

let test_has_execution_data () =
  Lighter.Executions_feed.initialize ["EXHAS"];
  (* inject_order triggers notify_ready on the store *)
  Lighter.Executions_feed.inject_order
    ~symbol:"EXHAS" ~order_id:"has_1" ~side:Lighter.Executions_feed.Buy
    ~qty:1.0 ~price:100.0 ();
  Alcotest.(check bool) "has data after inject" true
    (Lighter.Executions_feed.has_execution_data "EXHAS")

let test_execution_events_ring_buffer () =
  Lighter.Executions_feed.initialize ["EXRING"];

  let pos_before = Lighter.Executions_feed.get_current_position "EXRING" in

  Lighter.Executions_feed.inject_order
    ~symbol:"EXRING" ~order_id:"ring_1" ~side:Lighter.Executions_feed.Buy
    ~qty:1.0 ~price:100.0 ();

  let pos_after = Lighter.Executions_feed.get_current_position "EXRING" in
  Alcotest.(check bool) "position advanced" true (pos_after > pos_before);

  let events = Lighter.Executions_feed.read_execution_events "EXRING" pos_before in
  Alcotest.(check bool) "has events" true (List.length events > 0);
  let first = List.hd events in
  Alcotest.(check string) "event order_id" "ring_1" first.order_id

let () =
  Alcotest.run "Lighter Executions Feed" [
    "orders", [
      Alcotest.test_case "inject_order" `Quick test_inject_order;
      Alcotest.test_case "get_open_orders" `Quick test_get_open_orders;
      Alcotest.test_case "find_order_everywhere" `Quick test_find_order_everywhere;
      Alcotest.test_case "terminal status removes order" `Quick test_terminal_status_removes_order;
    ];
    "state", [
      Alcotest.test_case "startup_snapshot_done" `Quick test_startup_snapshot_done;
      Alcotest.test_case "has_execution_data" `Quick test_has_execution_data;
    ];
    "ring_buffer", [
      Alcotest.test_case "execution events" `Quick test_execution_events_ring_buffer;
    ]
  ]
