let test_inject_order () =
  Lighter.Executions_feed.initialize [ "EXETH" ];
  Lighter.Executions_feed.inject_order
    ~symbol:"EXETH"
    ~order_id:"order_001"
    ~side:Lighter.Executions_feed.Buy
    ~qty:1.5
    ~price:2180.00
    ~cl_ord_id:"cl_001"
    ~order_userref:42
    ();
  let order_opt = Lighter.Executions_feed.get_open_order "EXETH" "order_001" in
  match order_opt with
  | Some o ->
    Alcotest.(check string) "symbol" "EXETH" o.symbol;
    Alcotest.(check string) "order_id" "order_001" o.order_id;
    Alcotest.(check (float 0.000001)) "qty" 1.5 o.order_qty;
    Alcotest.(check (option (float 0.000001))) "price" (Some 2180.00) o.limit_price;
    Alcotest.(check (option string)) "cl_ord_id" (Some "cl_001") o.cl_ord_id;
    Alcotest.(check (option int)) "order_userref" (Some 42) o.order_userref
  | None -> Alcotest.fail "Order not injected"
;;

let test_get_open_orders () =
  Lighter.Executions_feed.initialize [ "EXMULTI" ];
  Lighter.Executions_feed.inject_order
    ~symbol:"EXMULTI"
    ~order_id:"m_1"
    ~side:Lighter.Executions_feed.Buy
    ~qty:1.0
    ~price:100.0
    ();
  Lighter.Executions_feed.inject_order
    ~symbol:"EXMULTI"
    ~order_id:"m_2"
    ~side:Lighter.Executions_feed.Sell
    ~qty:2.0
    ~price:200.0
    ();
  let orders = Lighter.Executions_feed.get_open_orders "EXMULTI" in
  Alcotest.(check bool) "at least 2" true (List.length orders >= 2);
  let has_1 =
    List.exists
      (fun (o : Lighter.Executions_feed.open_order) -> o.order_id = "m_1")
      orders
  in
  let has_2 =
    List.exists
      (fun (o : Lighter.Executions_feed.open_order) -> o.order_id = "m_2")
      orders
  in
  Alcotest.(check bool) "has m_1" true has_1;
  Alcotest.(check bool) "has m_2" true has_2
;;

let test_find_order_everywhere () =
  Lighter.Executions_feed.initialize [ "EXFIND" ];
  Lighter.Executions_feed.inject_order
    ~symbol:"EXFIND"
    ~order_id:"findme_42"
    ~side:Lighter.Executions_feed.Buy
    ~qty:0.5
    ~price:50.0
    ~cl_ord_id:"cl_find"
    ();
  let result = Lighter.Executions_feed.find_order_everywhere "findme_42" in
  Alcotest.(check bool) "found" true (Option.is_some result);
  (match result with
   | Some o -> Alcotest.(check string) "symbol" "EXFIND" o.symbol
   | None -> Alcotest.fail "unreachable");
  let missing = Lighter.Executions_feed.find_order_everywhere "nonexistent_999" in
  Alcotest.(check bool) "not found" true (Option.is_none missing)
;;

let test_terminal_status_removes_order () =
  Lighter.Executions_feed.initialize [ "EXTERM" ];
  Lighter.Executions_feed.inject_order
    ~symbol:"EXTERM"
    ~order_id:"term_1"
    ~side:Lighter.Executions_feed.Buy
    ~qty:1.0
    ~price:100.0
    ();
  let before = Lighter.Executions_feed.get_open_order "EXTERM" "term_1" in
  Alcotest.(check bool) "order exists" true (Option.is_some before);
  (* Filled event (status 3) must remove the open order. *)
  Lighter.Instruments_feed.initialize [ "EXTERM" ];
  let json =
    `Assoc
      [ "type", `String "update/account_all_orders"
      ; ( "account_all_orders"
        , `Assoc
            [ ( "0"
              , `Assoc
                  [ "order_index", `String "term_1"
                  ; "market_index", `Int 0
                  ; "status", `Int 3
                  ; (* Filled *)
                    "is_ask", `Bool false
                  ; "price", `String "100.0"
                  ; "initial_base_amount", `String "1.0"
                  ; "filled_base_amount", `String "1.0"
                  ; "avg_fill_price", `String "100.0"
                  ] )
            ] )
      ]
  in
  Lighter.Executions_feed.process_account_orders_update json;
  let after = Lighter.Executions_feed.get_open_order "EXTERM" "term_1" in
  Alcotest.(check bool) "order removed after fill" true (Option.is_none after)
;;

let test_userref_lookup () =
  Lighter.Instruments_feed.initialize [ "EXREF" ];
  Lighter.Executions_feed.initialize [ "EXREF" ];
  Lighter.Executions_feed.register_userref ~client_order_id:"cl_ref" ~userref:7;
  let json =
    `Assoc
      [ "type", `String "update/account_all_orders"
      ; ( "account_all_orders"
        , `Assoc
            [ ( "0"
              , `Assoc
                  [ "order_index", `String "ref_1"
                  ; "client_order_id", `String "cl_ref"
                  ; "market_index", `Int 0
                  ; "status", `Int 2
                  ; "is_ask", `Bool true
                  ; "price", `String "100.0"
                  ; "initial_base_amount", `String "1.0"
                  ; "filled_base_amount", `String "0.0"
                  ; "avg_fill_price", `String "0.0"
                  ] )
            ] )
      ]
  in
  Lighter.Executions_feed.process_account_orders_update json;
  match Lighter.Executions_feed.get_open_order "EXREF" "ref_1" with
  | Some o ->
    Alcotest.(check (option int))
      "tag resolved from client order id"
      (Some 7)
      o.order_userref
  | None -> Alcotest.fail "order not parsed"
;;

let test_startup_snapshot_done () =
  Lighter.Executions_feed.initialize [ "EXSNAP" ];
  Lighter.Executions_feed.set_startup_snapshot_done ();
  Alcotest.(check bool)
    "snapshot done"
    true
    (Lighter.Executions_feed.is_startup_snapshot_done ())
;;

let test_has_execution_data () =
  Lighter.Executions_feed.initialize [ "EXHAS" ];
  (* inject_order triggers notify_ready on the store. *)
  Lighter.Executions_feed.inject_order
    ~symbol:"EXHAS"
    ~order_id:"has_1"
    ~side:Lighter.Executions_feed.Buy
    ~qty:1.0
    ~price:100.0
    ();
  Alcotest.(check bool)
    "has data after inject"
    true
    (Lighter.Executions_feed.has_execution_data "EXHAS")
;;

let test_execution_events_ring_buffer () =
  Lighter.Executions_feed.initialize [ "EXRING" ];
  let pos_before = Lighter.Executions_feed.get_current_position "EXRING" in
  Lighter.Executions_feed.inject_order
    ~symbol:"EXRING"
    ~order_id:"ring_1"
    ~side:Lighter.Executions_feed.Buy
    ~qty:1.0
    ~price:100.0
    ();
  let pos_after = Lighter.Executions_feed.get_current_position "EXRING" in
  Alcotest.(check bool) "position advanced" true (pos_after > pos_before);
  let events = Lighter.Executions_feed.read_execution_events "EXRING" pos_before in
  Alcotest.(check bool) "has events" true (List.length events > 0);
  let first = List.hd events in
  Alcotest.(check string) "event order_id" "ring_1" first.order_id
;;

let test_clear_all_open_orders_resets_reconnect_state () =
  Lighter.Executions_feed.initialize [ "EXRESET" ];
  Lighter.Executions_feed.inject_order
    ~symbol:"EXRESET"
    ~order_id:"reset_1"
    ~side:Lighter.Executions_feed.Buy
    ~qty:1.0
    ~price:100.0
    ();
  Lighter.Executions_feed.set_startup_snapshot_done ();
  Alcotest.(check bool)
    "ready before clear"
    true
    (Lighter.Executions_feed.has_execution_data "EXRESET");
  Alcotest.(check bool)
    "snapshot done before clear"
    true
    (Lighter.Executions_feed.is_startup_snapshot_done ());
  Lighter.Executions_feed.clear_all_open_orders ();
  Alcotest.(check bool)
    "order removed after clear"
    true
    (Option.is_none (Lighter.Executions_feed.get_open_order "EXRESET" "reset_1"));
  Alcotest.(check bool)
    "ready reset after clear"
    false
    (Lighter.Executions_feed.has_execution_data "EXRESET");
  Alcotest.(check bool)
    "startup snapshot reset after clear"
    false
    (Lighter.Executions_feed.is_startup_snapshot_done ())
;;

let test_fill_intent () =
  let check label expected ~is_ask ~reduce_only =
    Alcotest.(check string)
      label
      expected
      (Lighter.Executions_feed.fill_intent ~symbol:"BTC" ~is_ask ~reduce_only)
  in
  check "buy opens long" "open_long" ~is_ask:false ~reduce_only:(Some false);
  check "sell opens short" "open_short" ~is_ask:true ~reduce_only:(Some false);
  check "buy closes short" "close_short" ~is_ask:false ~reduce_only:(Some true);
  check "sell closes long" "close_long" ~is_ask:true ~reduce_only:(Some true)
;;

let test_parse_reduce_only () =
  let check label expected json =
    Alcotest.(check (option bool))
      label
      expected
      (Lighter.Executions_feed.parse_reduce_only json)
  in
  check "snake bool" (Some true) (`Assoc [ "reduce_only", `Bool true ]);
  check "camel bool" (Some false) (`Assoc [ "reduceOnly", `Bool false ]);
  check "int" (Some true) (`Assoc [ "reduce_only", `Int 1 ]);
  check "string" (Some false) (`Assoc [ "reduce_only", `String "false" ]);
  check "absent" None (`Assoc [ "is_ask", `Bool true ])
;;

let test_fill_intent_position_fallback () =
  let position qty : Dio_exchange.Market_context.position =
    { coin = "EXFALL"
    ; qty
    ; entry = 100.0
    ; position_value = 100.0
    ; unrealized_pnl = 0.0
    ; margin_used = 0.0
    ; liquidation = None
    ; leverage = None
    ; updated_at = 1.0
    }
  in
  let check label expected ~is_ask qty =
    Dio_exchange.Market_context.replace_positions
      ~exchange:"lighter"
      ~updated_at:1.0
      [ position qty ];
    Alcotest.(check string)
      label
      expected
      (Lighter.Executions_feed.fill_intent ~symbol:"EXFALL" ~is_ask ~reduce_only:None)
  in
  check "sell against long closes" "close_long" ~is_ask:true 2.0;
  check "buy with long adds" "open_long" ~is_ask:false 2.0;
  check "buy against short closes" "close_short" ~is_ask:false (-2.0);
  check "sell with short adds" "open_short" ~is_ask:true (-2.0);
  check "sell when flat opens short" "open_short" ~is_ask:true 0.0;
  check "buy when flat opens long" "open_long" ~is_ask:false 0.0
;;

let test_close_fill_realized_pnl () =
  (* Lighter publishes no per-fill PnL, but its position carries the venue's entry basis.
     A reduce-only close must be priced off that basis rather than reported blank. *)
  Lighter.Instruments_feed.initialize [ "EXCLOSE" ];
  Lighter.Executions_feed.initialize [ "EXCLOSE" ];
  ignore
    (Lighter.Positions.process_account_all
       (`Assoc
         [ "type", `String "update/account_all"
         ; ( "account_all"
           , `Assoc
               [ ( "positions"
                 , `Assoc
                     [ ( "0"
                       , `Assoc
                           [ "market_id", `Int 0
                           ; "symbol", `String "EXCLOSE-USD"
                           ; "sign", `Int 1
                           ; "position", `String "1.0"
                           ; "avg_entry_price", `String "100.0"
                           ; "position_value", `String "100.0"
                           ; "unrealized_pnl", `String "0.0"
                           ] )
                     ] )
               ] )
         ]));
  let json =
    `Assoc
      [ "type", `String "update/account_all_orders"
      ; ( "account_all_orders"
        , `Assoc
            [ ( "0"
              , `Assoc
                  [ "order_index", `String "close_1"
                  ; "market_index", `Int 0
                  ; "status", `Int 3
                  ; "is_ask", `Bool true
                  ; "reduce_only", `Bool true
                  ; "price", `String "110.0"
                  ; "initial_base_amount", `String "1.0"
                  ; "filled_base_amount", `String "1.0"
                  ; "avg_fill_price", `String "110.0"
                  ] )
            ] )
      ]
  in
  Lighter.Executions_feed.process_account_orders_update json;
  let fills = Concurrency.Fill_event_bus.get_recent_fills () in
  match
    List.find_opt
      (fun (f : Concurrency.Fill_event_bus.fill_event) -> f.order_id = "close_1")
      fills
  with
  | None -> Alcotest.fail "close fill not published"
  | Some f ->
    Alcotest.(check string) "closing the long" "close_long" f.intent;
    Alcotest.(check (option (float 0.000001)))
      "realized PnL off the venue entry basis"
      (Some 10.0)
      f.realized_pnl
;;

let test_open_fill_has_no_realized_pnl () =
  Lighter.Instruments_feed.initialize [ "EXOPEN" ];
  Lighter.Executions_feed.initialize [ "EXOPEN" ];
  let json =
    `Assoc
      [ "type", `String "update/account_all_orders"
      ; ( "account_all_orders"
        , `Assoc
            [ ( "0"
              , `Assoc
                  [ "order_index", `String "open_1"
                  ; "market_index", `Int 0
                  ; "status", `Int 3
                  ; "is_ask", `Bool true
                  ; "reduce_only", `Bool false
                  ; "price", `String "100.0"
                  ; "initial_base_amount", `String "1.0"
                  ; "filled_base_amount", `String "1.0"
                  ; "avg_fill_price", `String "100.0"
                  ] )
            ] )
      ]
  in
  Lighter.Executions_feed.process_account_orders_update json;
  let fills = Concurrency.Fill_event_bus.get_recent_fills () in
  match
    List.find_opt
      (fun (f : Concurrency.Fill_event_bus.fill_event) -> f.order_id = "open_1")
      fills
  with
  | None -> Alcotest.fail "open fill not published"
  | Some f ->
    Alcotest.(check bool) "no PnL on an opening fill" true (f.realized_pnl = None)
;;

let test_snapshot_missing_order_keeps_tracking () =
  Lighter.Instruments_feed.initialize [ "EXKEEP" ];
  Lighter.Executions_feed.initialize [ "EXKEEP" ];
  Lighter.Executions_feed.inject_order
    ~symbol:"EXKEEP"
    ~order_id:"keep_1"
    ~side:Lighter.Executions_feed.Buy
    ~qty:1.0
    ~price:100.0
    ();
  Unix.sleepf 0.01;
  let since = Unix.gettimeofday () in
  let empty_snapshot = `Assoc [ "code", `Int 200; "orders", `List [] ] in
  Lighter.Executions_feed.handle_snapshot ~since empty_snapshot;
  Alcotest.(check bool)
    "one miss does not cancel"
    true
    (Option.is_some (Lighter.Executions_feed.get_open_order "EXKEEP" "keep_1"));
  Lighter.Executions_feed.handle_snapshot ~since empty_snapshot;
  Alcotest.(check bool)
    "unconfirmed second miss does not cancel"
    true
    (Option.is_some (Lighter.Executions_feed.get_open_order "EXKEEP" "keep_1"));
  Lighter.Executions_feed.handle_snapshot
    ~since
    (`Assoc [ "code", `Int 200; "message", `String "no orders field" ]);
  Alcotest.(check bool)
    "snapshot without an orders list does not cancel"
    true
    (Option.is_some (Lighter.Executions_feed.get_open_order "EXKEEP" "keep_1"))
;;

let test_reject_order_retires_phantom () =
  let symbol = "EXREJECT" in
  Lighter.Instruments_feed.initialize [ symbol ];
  Lighter.Executions_feed.initialize [ symbol ];
  let pos_before = Lighter.Executions_feed.get_current_position symbol in
  Lighter.Executions_feed.inject_order
    ~symbol
    ~order_id:"rej_1"
    ~side:Lighter.Executions_feed.Sell
    ~qty:0.44
    ~price:86.64
    ();
  Alcotest.(check bool)
    "tracked after injection"
    true
    (Option.is_some (Lighter.Executions_feed.get_open_order symbol "rej_1"));
  Lighter.Executions_feed.reject_order ~order_id:"rej_1" ~reason:"code=21107";
  Alcotest.(check bool)
    "dropped after rejection"
    true
    (Option.is_none (Lighter.Executions_feed.get_open_order symbol "rej_1"));
  let rejected =
    Lighter.Executions_feed.read_execution_events symbol pos_before
    |> List.filter (fun (e : Lighter.Executions_feed.execution_event) ->
      String.equal e.order_id "rej_1"
      && e.order_status = Lighter.Executions_feed.RejectedStatus)
  in
  Alcotest.(check int) "rejection event emitted" 1 (List.length rejected)
;;

let test_snapshot_confirmed_stale_cancels_at_venue () =
  let symbol = "EXSTALE" in
  Lighter.Instruments_feed.initialize [ symbol ];
  Lighter.Executions_feed.initialize [ symbol ];
  let cancelled = ref [] in
  Lighter.Executions_feed.set_venue_cancel_hook (fun sym order_id ->
    cancelled := (sym, order_id) :: !cancelled);
  Lighter.Executions_feed.inject_order
    ~symbol
    ~order_id:"stale_1"
    ~side:Lighter.Executions_feed.Sell
    ~qty:0.44
    ~price:86.64
    ();
  let placed_at = Unix.gettimeofday () in
  Unix.sleepf 0.01;
  let since = Unix.gettimeofday () in
  let empty_snapshot = `Assoc [ "code", `Int 200; "orders", `List [] ] in
  Lighter.Executions_feed.handle_snapshot ~since empty_snapshot;
  Alcotest.(check bool)
    "first miss only arms"
    true
    (Option.is_some (Lighter.Executions_feed.get_open_order symbol "stale_1"));
  Alcotest.(check int) "no venue cancel yet" 0 (List.length !cancelled);
  Lighter.Executions_feed.handle_snapshot ~now:(placed_at +. 60.0) ~since empty_snapshot;
  Alcotest.(check bool)
    "confirmed miss drops tracking"
    true
    (Option.is_none (Lighter.Executions_feed.get_open_order symbol "stale_1"));
  Alcotest.(check (list (pair string string)))
    "confirmed miss cancels at the venue"
    [ symbol, "stale_1" ]
    (List.filter (fun (sym, _) -> String.equal sym symbol) (List.rev !cancelled));
  Lighter.Executions_feed.set_venue_cancel_hook (fun _ _ -> ())
;;

let test_snapshot_present_order_stays () =
  let symbol = "EXPRESENT" in
  Lighter.Instruments_feed.initialize [ symbol ];
  Lighter.Executions_feed.initialize [ symbol ];
  let cancelled = ref 0 in
  Lighter.Executions_feed.set_venue_cancel_hook (fun _ _ -> incr cancelled);
  Lighter.Executions_feed.inject_order
    ~symbol
    ~order_id:"present_1"
    ~side:Lighter.Executions_feed.Sell
    ~qty:0.44
    ~price:86.64
    ();
  let placed_at = Unix.gettimeofday () in
  Unix.sleepf 0.01;
  let since = Unix.gettimeofday () in
  let snapshot =
    `Assoc
      [ "code", `Int 200
      ; ( "orders"
        , `List
            [ `Assoc
                [ "order_index", `String "present_1"
                ; "price", `String "86.64"
                ; "initial_base_amount", `String "0.44"
                ; "filled_base_amount", `String "0.0"
                ; "is_ask", `Bool true
                ]
            ] )
      ]
  in
  Lighter.Executions_feed.handle_snapshot ~now:(placed_at +. 60.0) ~since snapshot;
  Alcotest.(check bool)
    "reported order kept"
    true
    (Option.is_some (Lighter.Executions_feed.get_open_order symbol "present_1"));
  Alcotest.(check int) "no venue cancel for a reported order" 0 !cancelled;
  Lighter.Executions_feed.set_venue_cancel_hook (fun _ _ -> ())
;;

let test_first_update_adopts_optimistic_copy () =
  let symbol = "EXADOPT" in
  Lighter.Instruments_feed.initialize [ symbol ];
  Lighter.Executions_feed.initialize [ symbol ];
  Lighter.Executions_feed.inject_order
    ~symbol
    ~order_id:"client_1"
    ~side:Lighter.Executions_feed.Sell
    ~qty:0.45
    ~price:88.26
    ~cl_ord_id:"client_1"
    ();
  (* Lighter's first update for a fresh order sometimes omits [client_order_id]. The
     optimistic copy must not survive beside the venue id, or the engine sees two closes
     for one kind and cancels one of them. *)
  let json =
    `Assoc
      [ "type", `String "update/account_all_orders"
      ; ( "account_all_orders"
        , `Assoc
            [ ( "0"
              , `Assoc
                  [ "order_index", `String "venue_1"
                  ; "market_index", `Int 0
                  ; "status", `String "Active"
                  ; "is_ask", `Bool true
                  ; "price", `String "88.26"
                  ; "initial_base_amount", `String "0.45"
                  ; "filled_base_amount", `String "0.0"
                  ; "avg_fill_price", `String "0.0"
                  ] )
            ] )
      ]
  in
  Lighter.Executions_feed.process_account_orders_update json;
  Alcotest.(check bool)
    "optimistic copy replaced"
    true
    (Option.is_none (Lighter.Executions_feed.get_open_order symbol "client_1"));
  Alcotest.(check bool)
    "venue order tracked"
    true
    (Option.is_some (Lighter.Executions_feed.get_open_order symbol "venue_1"))
;;

let test_ambiguous_adoption_is_skipped () =
  let symbol = "EXAMBIG" in
  Lighter.Instruments_feed.initialize [ symbol ];
  Lighter.Executions_feed.initialize [ symbol ];
  List.iter
    (fun id ->
      Lighter.Executions_feed.inject_order
        ~symbol
        ~order_id:id
        ~side:Lighter.Executions_feed.Sell
        ~qty:0.45
        ~price:88.26
        ~cl_ord_id:id
        ())
    [ "client_a"; "client_b" ];
  let json =
    `Assoc
      [ "type", `String "update/account_all_orders"
      ; ( "account_all_orders"
        , `Assoc
            [ ( "0"
              , `Assoc
                  [ "order_index", `String "venue_2"
                  ; "market_index", `Int 0
                  ; "status", `String "Active"
                  ; "is_ask", `Bool true
                  ; "price", `String "88.26"
                  ; "initial_base_amount", `String "0.45"
                  ; "filled_base_amount", `String "0.0"
                  ; "avg_fill_price", `String "0.0"
                  ] )
            ] )
      ]
  in
  Lighter.Executions_feed.process_account_orders_update json;
  Alcotest.(check bool)
    "first candidate kept"
    true
    (Option.is_some (Lighter.Executions_feed.get_open_order symbol "client_a"));
  Alcotest.(check bool)
    "second candidate kept"
    true
    (Option.is_some (Lighter.Executions_feed.get_open_order symbol "client_b"))
;;

let () =
  Alcotest.run
    "Lighter Executions Feed"
    [ ( "orders"
      , [ Alcotest.test_case "inject_order" `Quick test_inject_order
        ; Alcotest.test_case "get_open_orders" `Quick test_get_open_orders
        ; Alcotest.test_case "find_order_everywhere" `Quick test_find_order_everywhere
        ; Alcotest.test_case "userref lookup" `Quick test_userref_lookup
        ; Alcotest.test_case
            "terminal status removes order"
            `Quick
            test_terminal_status_removes_order
        ; Alcotest.test_case
            "clear resets reconnect state"
            `Quick
            test_clear_all_open_orders_resets_reconnect_state
        ; Alcotest.test_case
            "snapshot miss keeps tracking"
            `Quick
            test_snapshot_missing_order_keeps_tracking
        ; Alcotest.test_case
            "snapshot confirmed miss cancels"
            `Quick
            test_snapshot_confirmed_stale_cancels_at_venue
        ; Alcotest.test_case
            "snapshot reported order stays"
            `Quick
            test_snapshot_present_order_stays
        ; Alcotest.test_case
            "venue rejection retires order"
            `Quick
            test_reject_order_retires_phantom
        ; Alcotest.test_case
            "first update adopts optimistic copy"
            `Quick
            test_first_update_adopts_optimistic_copy
        ; Alcotest.test_case
            "ambiguous adoption is skipped"
            `Quick
            test_ambiguous_adoption_is_skipped
        ; Alcotest.test_case
            "close fill reports venue PnL"
            `Quick
            test_close_fill_realized_pnl
        ; Alcotest.test_case
            "open fill has no PnL"
            `Quick
            test_open_fill_has_no_realized_pnl
        ] )
    ; ( "intent"
      , [ Alcotest.test_case "reduce-only mapping" `Quick test_fill_intent
        ; Alcotest.test_case "parse reduce_only" `Quick test_parse_reduce_only
        ; Alcotest.test_case "position fallback" `Quick test_fill_intent_position_fallback
        ] )
    ; ( "state"
      , [ Alcotest.test_case "startup_snapshot_done" `Quick test_startup_snapshot_done
        ; Alcotest.test_case "has_execution_data" `Quick test_has_execution_data
        ] )
    ; ( "ring_buffer"
      , [ Alcotest.test_case "execution events" `Quick test_execution_events_ring_buffer ]
      )
    ]
;;
