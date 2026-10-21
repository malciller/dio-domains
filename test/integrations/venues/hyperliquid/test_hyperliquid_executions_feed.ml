let test_find_registered_symbol () =
  (* No instruments feed is loaded; registration feeds the fallback matcher. *)
  Hyperliquid.Executions_feed.initialize [ "HYPE"; "BTC" ];
  let result1 = Hyperliquid.Executions_feed.find_registered_symbol "HYPE" in
  Alcotest.(check (option string)) "Direct match" (Some "HYPE") result1;
  let result2 = Hyperliquid.Executions_feed.find_registered_symbol "BTC" in
  Alcotest.(check (option string)) "Direct match" (Some "BTC") result2;
  let result3 = Hyperliquid.Executions_feed.find_registered_symbol "UNKNOWN" in
  Alcotest.(check (option string)) "No match" None result3
;;

let test_inject_order () =
  Hyperliquid.Executions_feed.initialize [ "HYPE" ];
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
;;

let test_process_order_updates () =
  Hyperliquid.Executions_feed.initialize [ "HYPE" ];
  let json =
    `List
      [ `Assoc
          [ ( "order"
            , `Assoc
                [ "coin", `String "HYPE"
                ; "oid", `Int 999
                ; "status", `String "open"
                ; "side", `String "B"
                ; "limitPx", `String "150.0"
                ; "sz", `String "2.0"
                ; "cloid", `String "client999"
                ] )
          ; "status", `String "open"
          ]
      ]
  in
  Hyperliquid.Executions_feed.process_order_updates json;
  let order_opt = Hyperliquid.Executions_feed.get_open_order "HYPE" "999" in
  match order_opt with
  | Some o ->
    Alcotest.(check string) "symbol" "HYPE" o.symbol;
    Alcotest.(check (float 0.000001)) "qty" 2.0 o.order_qty;
    Alcotest.(check (option string)) "cl_ord_id" (Some "client999") o.cl_ord_id
  | None -> Alcotest.fail "Order update not processed"
;;

let test_get_open_orders () =
  Hyperliquid.Executions_feed.initialize [ "TESTORDERS" ];
  Hyperliquid.Executions_feed.inject_order
    ~symbol:"TESTORDERS"
    ~order_id:"ord_a"
    ~side:Hyperliquid.Executions_feed.Buy
    ~qty:1.0
    ~price:100.0
    ~user_ref:1
    ~cl_ord_id:"cl_a"
    ();
  Hyperliquid.Executions_feed.inject_order
    ~symbol:"TESTORDERS"
    ~order_id:"ord_b"
    ~side:Hyperliquid.Executions_feed.Sell
    ~qty:2.0
    ~price:200.0
    ~user_ref:2
    ~cl_ord_id:"cl_b"
    ();
  let orders = Hyperliquid.Executions_feed.get_open_orders "TESTORDERS" in
  Alcotest.(check bool) "at least 2 orders" true (List.length orders >= 2);
  let has_a =
    List.exists
      (fun (o : Hyperliquid.Executions_feed.open_order) -> o.order_id = "ord_a")
      orders
  in
  let has_b =
    List.exists
      (fun (o : Hyperliquid.Executions_feed.open_order) -> o.order_id = "ord_b")
      orders
  in
  Alcotest.(check bool) "has ord_a" true has_a;
  Alcotest.(check bool) "has ord_b" true has_b
;;

let test_process_order_terminal_status () =
  Hyperliquid.Executions_feed.initialize [ "TESTTERM" ];
  let json_open =
    `List
      [ `Assoc
          [ ( "order"
            , `Assoc
                [ "coin", `String "TESTTERM"
                ; "oid", `Int 5555
                ; "status", `String "open"
                ; "side", `String "B"
                ; "limitPx", `String "100.0"
                ; "sz", `String "1.0"
                ; "cloid", `Null
                ] )
          ; "status", `String "open"
          ]
      ]
  in
  Hyperliquid.Executions_feed.process_order_updates json_open;
  let order_open = Hyperliquid.Executions_feed.get_open_order "TESTTERM" "5555" in
  Alcotest.(check bool) "order exists" true (Option.is_some order_open);
  let json_cancel =
    `List
      [ `Assoc
          [ ( "order"
            , `Assoc
                [ "coin", `String "TESTTERM"
                ; "oid", `Int 5555
                ; "status", `String "canceled"
                ; "side", `String "B"
                ; "limitPx", `String "100.0"
                ; "sz", `String "1.0"
                ; "cloid", `Null
                ] )
          ; "status", `String "canceled"
          ]
      ]
  in
  Hyperliquid.Executions_feed.process_order_updates json_cancel;
  let order_gone = Hyperliquid.Executions_feed.get_open_order "TESTTERM" "5555" in
  Alcotest.(check bool) "order removed" true (Option.is_none order_gone)
;;

let test_find_order_everywhere () =
  Hyperliquid.Executions_feed.initialize [ "TESTFIND" ];
  Hyperliquid.Executions_feed.inject_order
    ~symbol:"TESTFIND"
    ~order_id:"findme_123"
    ~side:Hyperliquid.Executions_feed.Buy
    ~qty:0.5
    ~price:50.0
    ~user_ref:99
    ~cl_ord_id:"cl_find"
    ();
  let result = Hyperliquid.Executions_feed.find_order_everywhere "findme_123" in
  Alcotest.(check bool) "found" true (Option.is_some result);
  (match result with
   | Some o -> Alcotest.(check string) "symbol" "TESTFIND" o.symbol
   | None -> Alcotest.fail "unreachable");
  let missing = Hyperliquid.Executions_feed.find_order_everywhere "nonexistent_999" in
  Alcotest.(check bool) "not found" true (Option.is_none missing)
;;

let test_get_all_symbols () =
  Hyperliquid.Executions_feed.initialize [ "SYM_A"; "SYM_B" ];
  let symbols = Hyperliquid.Executions_feed.get_all_symbols () in
  let has_a = List.mem "SYM_A" symbols in
  let has_b = List.mem "SYM_B" symbols in
  Alcotest.(check bool) "has SYM_A" true has_a;
  Alcotest.(check bool) "has SYM_B" true has_b
;;

let test_perp_orders_classified () =
  Hyperliquid.Executions_feed.initialize [ "PERPT"; "PERPT/USDC" ];
  (* A perp placement is injected under the bare coin. *)
  Hyperliquid.Executions_feed.inject_order
    ~symbol:"PERPT"
    ~order_id:"perp_live"
    ~side:Hyperliquid.Executions_feed.Buy
    ~qty:1.0
    ~price:95.0
    ();
  (* Spot orders on the same asset must not be reported as perp. *)
  Hyperliquid.Executions_feed.inject_order
    ~symbol:"PERPT/USDC"
    ~order_id:"spot_live"
    ~side:Hyperliquid.Executions_feed.Sell
    ~qty:1.0
    ~price:120.0
    ();
  (* The startup snapshot keys perp orders by the registered spot pair; the venue coin is
     still what classifies them. It also sweeps orders it did not see, so the injected
     checks run first. *)
  let ids_of orders =
    List.map (fun (o : Hyperliquid.Executions_feed.open_order) -> o.order_id) orders
  in
  let perp_before = Hyperliquid.Executions_feed.get_perp_open_orders ~coin:"PERPT" in
  Alcotest.(check bool)
    "injected perp order present"
    true
    (List.mem "perp_live" (ids_of perp_before));
  Alcotest.(check bool)
    "spot order excluded"
    false
    (List.mem "spot_live" (ids_of perp_before));
  Hyperliquid.Executions_feed.inject_open_orders
    (`List
      [ `Assoc
          [ "coin", `String "PERPT"
          ; "oid", `Int 4242
          ; "side", `String "B"
          ; "limitPx", `String "90.0"
          ; "origSz", `String "2.0"
          ; "timestamp", `Int 1_000_000
          ]
      ]);
  let perp = Hyperliquid.Executions_feed.get_perp_open_orders ~coin:"PERPT" in
  let ids = ids_of perp in
  Alcotest.(check int) "snapshot perp order returned" 1 (List.length perp);
  Alcotest.(check bool) "snapshot perp order present" true (List.mem "4242" ids);
  Alcotest.(check bool)
    "spot order excluded after snapshot"
    false
    (List.mem "spot_live" ids);
  (* The venue stores this perp order under the spot pair. The raw pair view sees it; the
     spot-only view used by the grid must not. This is the duplicate-buy regression. *)
  let raw_pair = ids_of (Hyperliquid.Executions_feed.get_open_orders "PERPT/USDC") in
  Alcotest.(check bool) "raw pair sees the perp order" true (List.mem "4242" raw_pair);
  let spot_pair =
    ids_of (Hyperliquid.Executions_feed.get_spot_open_orders "PERPT/USDC")
  in
  Alcotest.(check bool)
    "spot pair excludes the perp order"
    false
    (List.mem "4242" spot_pair);
  let folded = ref [] in
  Hyperliquid.Executions_feed.fold_spot_open_orders
    "PERPT/USDC"
    ~init:()
    ~f:(fun () (o : Hyperliquid.Executions_feed.open_order) ->
      folded := o.order_id :: !folded);
  Alcotest.(check bool)
    "spot fold excludes the perp order"
    false
    (List.mem "4242" !folded);
  let changes, _ =
    Hyperliquid.Executions_feed.drain_open_order_changes ~symbol:"PERPT/USDC"
  in
  Alcotest.(check bool)
    "spot delta excludes the perp order"
    false
    (List.exists (fun (id, _) -> String.equal id "4242") changes)
;;

let () =
  Alcotest.run
    "Hyperliquid Executions Feed"
    [ ( "symbols"
      , [ Alcotest.test_case "find_registered_symbol" `Quick test_find_registered_symbol
        ; Alcotest.test_case "get_all_symbols" `Quick test_get_all_symbols
        ] )
    ; ( "orders"
      , [ Alcotest.test_case "inject_order" `Quick test_inject_order
        ; Alcotest.test_case "process_order_updates" `Quick test_process_order_updates
        ; Alcotest.test_case "get_open_orders" `Quick test_get_open_orders
        ; Alcotest.test_case
            "terminal status removes order"
            `Quick
            test_process_order_terminal_status
        ; Alcotest.test_case "find_order_everywhere" `Quick test_find_order_everywhere
        ; Alcotest.test_case "perp orders classified" `Quick test_perp_orders_classified
        ] )
    ]
;;
