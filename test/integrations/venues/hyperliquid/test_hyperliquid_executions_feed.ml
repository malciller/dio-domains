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

(* The spot view reads a published membership snapshot, not the classification index
   itself. Make sure a reclassification (forget) is reflected: a stale snapshot would keep
   hiding a real spot order after the perp position retired it. *)
let test_perp_classification_snapshot_updates () =
  Hyperliquid.Executions_feed.initialize [ "SNAPX"; "SNAPX/USDC" ];
  Hyperliquid.Executions_feed.inject_order
    ~symbol:"SNAPX/USDC"
    ~order_id:"snap-1"
    ~side:Hyperliquid.Executions_feed.Sell
    ~qty:1.0
    ~price:100.0
    ();
  let spot_ids () =
    List.map
      (fun (o : Hyperliquid.Executions_feed.open_order) -> o.order_id)
      (Hyperliquid.Executions_feed.get_spot_open_orders "SNAPX/USDC")
  in
  Alcotest.(check bool) "initially a spot order" true (List.mem "snap-1" (spot_ids ()));
  Hyperliquid.Executions_feed.note_perp_order ~order_id:"snap-1" ~coin:"SNAPX";
  Alcotest.(check bool)
    "classified perp is hidden from spot"
    false
    (List.mem "snap-1" (spot_ids ()));
  Hyperliquid.Executions_feed.forget_perp_order "snap-1";
  Alcotest.(check bool)
    "retired classification restores spot view"
    true
    (List.mem "snap-1" (spot_ids ()))
;;

let test_amend_replacement_visible () =
  Hyperliquid.Executions_feed.initialize [ "AMENDX/USDC" ];
  let ids_of orders =
    List.map (fun (o : Hyperliquid.Executions_feed.open_order) -> o.order_id) orders
  in
  (* A resting take-profit is injected with its perp coin, as [place_order] does on ack.
     It must be filed under the registered spot pair, the store the owning domain reads.
     Filing it under the bare coin hides every fill from the overlay. *)
  Hyperliquid.Executions_feed.inject_order
    ~symbol:"AMENDX"
    ~order_id:"tp_old"
    ~side:Hyperliquid.Executions_feed.Buy
    ~qty:2.0
    ~price:100.0
    ~user_ref:3
    ();
  Alcotest.(check (option string))
    "perp order is filed under the registered pair"
    (Some "AMENDX/USDC")
    (Hyperliquid.Executions_feed.order_store_symbol ~order_id:"tp_old");
  Alcotest.(check bool)
    "old order visible before the amend"
    true
    (List.mem
       "tp_old"
       (ids_of (Hyperliquid.Executions_feed.get_perp_open_orders ~coin:"AMENDX")));
  (* Hyperliquid's modify retires the old oid and creates a new one. [amend_order] must
     retire the old oid and inject the replacement, otherwise [order.replace]'s
     open-orders scan comes back empty and places a duplicate reduce-only order. *)
  Hyperliquid.Executions_feed.remove_open_order ~symbol:"AMENDX/USDC" ~order_id:"tp_old";
  Hyperliquid.Executions_feed.inject_order
    ~symbol:"AMENDX"
    ~order_id:"tp_new"
    ~side:Hyperliquid.Executions_feed.Buy
    ~qty:2.0
    ~price:105.0
    ~user_ref:3
    ();
  let perp = Hyperliquid.Executions_feed.get_perp_open_orders ~coin:"AMENDX" in
  let ids = ids_of perp in
  Alcotest.(check int) "exactly one live perp order" 1 (List.length perp);
  Alcotest.(check bool) "replacement visible" true (List.mem "tp_new" ids);
  Alcotest.(check bool) "retired order gone" false (List.mem "tp_old" ids);
  match Hyperliquid.Executions_feed.get_open_order "AMENDX/USDC" "tp_new" with
  | Some (o : Hyperliquid.Executions_feed.open_order) ->
    Alcotest.(check (float 0.000001))
      "replacement carries the new price"
      105.0
      (Option.value o.limit_price ~default:0.0)
  | None -> Alcotest.fail "replacement order missing from the pair store"
;;

let test_fill_intent_close_short () =
  Hyperliquid.Executions_feed.initialize [ "HYPE/USDC" ];
  let tid = 987_654_321 in
  let now_ms = int_of_float (Unix.gettimeofday () *. 1000.0) in
  let json =
    `Assoc
      [ "channel", `String "userFills"
      ; ( "data"
        , `Assoc
            [ ( "fills"
              , `List
                  [ `Assoc
                      [ "coin", `String "HYPE"
                      ; "oid", `Int 999_001
                      ; "px", `String "77.83"
                      ; "sz", `String "0.52"
                      ; "side", `String "B"
                      ; "time", `Int now_ms
                      ; "dir", `String "Close Short"
                      ; "closedPnl", `String "0.42"
                      ; "fee", `String "0.01"
                      ; "tid", `Int tid
                      ] ] )
            ] )
      ]
  in
  Hyperliquid.Executions_feed.process_market_data json;
  let fills = Concurrency.Fill_event_bus.get_recent_fills () in
  match
    List.find_opt
      (fun (f : Concurrency.Fill_event_bus.fill_event) -> f.order_id = "999001")
      fills
  with
  | None -> Alcotest.fail "fill event not published"
  | Some f ->
    Alcotest.(check string) "market is perp" "perp" f.market;
    Alcotest.(check string) "intent is close_short" "close_short" f.intent;
    Alcotest.(check string) "strategy is carry" "carry" f.strategy;
    (match f.realized_pnl with
     | Some p -> Alcotest.(check (float 0.000001)) "closed PnL" 0.42 p
     | None -> Alcotest.fail "expected realized PnL");
    (match f.note with
     | Some n -> Alcotest.(check string) "direction note" "Close Short" n
     | None -> Alcotest.fail "expected direction note")
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
        ; Alcotest.test_case
            "perp classification snapshot updates"
            `Quick
            test_perp_classification_snapshot_updates
        ; Alcotest.test_case
            "amend replacement is visible"
            `Quick
            test_amend_replacement_visible
        ; Alcotest.test_case
            "fill intent: close short"
            `Quick
            test_fill_intent_close_short
        ] )
    ]
;;
