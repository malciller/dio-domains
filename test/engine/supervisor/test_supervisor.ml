let test_connection_registration () =
  let connect_called = ref false in
  let connect_fn () =
    connect_called := true;
    Lwt.return_unit
  in
  let conn = Supervisor.register ~name:"test_conn" ~connect_fn:(Some connect_fn) in
  Alcotest.(check string) "connection name" "test_conn" conn.name;
  Alcotest.(check bool) "has connect function" true (Option.is_some conn.connect_fn);
  Alcotest.(check bool)
    "initial state disconnected"
    true
    (conn.state = Supervisor_types.Disconnected)
;;

let test_connection_registration_monitoring () =
  let conn = Supervisor.register_for_monitoring ~name:"monitor_conn" in
  Alcotest.(check string) "monitoring connection name" "monitor_conn" conn.name;
  Alcotest.(check bool) "no connect function" true (Option.is_none conn.connect_fn);
  Alcotest.(check bool)
    "monitoring state connected"
    true
    (conn.state = Supervisor_types.Connected)
;;

let test_state_management () =
  let conn = Supervisor.register_for_monitoring ~name:"state_test" in
  Supervisor.set_state conn Supervisor_types.Disconnected;
  Alcotest.(check bool)
    "disconnected state"
    true
    (Supervisor.get_state conn = Supervisor_types.Disconnected);
  Supervisor.set_state conn Supervisor_types.Connected;
  Alcotest.(check bool)
    "connected state"
    true
    (Supervisor.get_state conn = Supervisor_types.Connected);
  Supervisor.set_state conn (Supervisor_types.Failed "test error");
  match Supervisor.get_state conn with
  | Supervisor_types.Failed "test error" -> Alcotest.(check bool) "failed state" true true
  | _ -> Alcotest.fail "wrong failed state"
;;

let test_circuit_breaker_initial () =
  let conn = Supervisor.register_for_monitoring ~name:"cb_test" in
  Alcotest.(check bool)
    "circuit breaker closed"
    true
    (conn.circuit_breaker = Supervisor_types.Closed);
  Alcotest.(check int) "zero failures" 0 conn.circuit_breaker_failures
;;

let test_circuit_breaker_failure_counting () =
  let conn = Supervisor.register_for_monitoring ~name:"cb_fail_test" in
  Supervisor.update_circuit_breaker conn false;
  Supervisor.update_circuit_breaker conn false;
  Supervisor.update_circuit_breaker conn false;
  Supervisor.update_circuit_breaker conn false;
  (* Fifth failure opens breaker. *)
  Supervisor.update_circuit_breaker conn false;
  Alcotest.(check int) "five failures" 5 conn.circuit_breaker_failures;
  Alcotest.(check bool)
    "circuit breaker open"
    true
    (conn.circuit_breaker = Supervisor_types.Open)
;;

let test_circuit_breaker_recovery () =
  let conn = Supervisor.register_for_monitoring ~name:"cb_recover_test" in
  conn.circuit_breaker <- Supervisor_types.Open;
  conn.circuit_breaker_failures <- 5;
  conn.circuit_breaker_last_failure <- Some (Unix.time () -. 400.0);
  (* 400 s ago, past the recovery timeout. *)
  Alcotest.(check bool)
    "allows connection after timeout"
    true
    (Supervisor.circuit_breaker_allows_connection conn);
  Supervisor.update_circuit_breaker conn true;
  Alcotest.(check bool)
    "circuit breaker closed after success"
    true
    (conn.circuit_breaker = Supervisor_types.Closed);
  Alcotest.(check int) "zero failures after success" 0 conn.circuit_breaker_failures
;;

let test_token_store () =
  Supervisor.Token_store.set None;
  let test_token = "test_auth_token_123" in
  Supervisor.Token_store.set (Some test_token);
  Alcotest.(check (option string))
    "token retrieved"
    (Some test_token)
    (Supervisor.Token_store.get ());
  Supervisor.Token_store.set None;
  Alcotest.(check (option string)) "token cleared" None (Supervisor.Token_store.get ())
;;

let test_connection_queries () =
  (* Reset connections. *)
  Supervisor.stop_all ();
  let _ = Supervisor.register_for_monitoring ~name:"query_test1" in
  let _ = Supervisor.register_for_monitoring ~name:"query_test2" in
  let conn1 = Supervisor.get_connection "query_test1" in
  Alcotest.(check string) "get connection name" "query_test1" conn1.name;
  let all_conns = Supervisor.get_all_connections () in
  Alcotest.(check bool) "at least 2 connections" true (List.length all_conns >= 2);
  let names =
    List.map (fun (c : Supervisor_types.supervised_connection) -> c.name) all_conns
  in
  Alcotest.(check bool) "contains query_test1" true (List.mem "query_test1" names);
  Alcotest.(check bool) "contains query_test2" true (List.mem "query_test2" names)
;;

let test_connection_uptime () =
  let conn = Supervisor.register ~name:"uptime_test" ~connect_fn:None in
  (* Disconnected connection has no uptime. *)
  Alcotest.(check bool)
    "no uptime when disconnected"
    true
    (Option.is_none (Supervisor.get_uptime conn));
  Supervisor.set_state conn Supervisor_types.Connected;
  Alcotest.(check bool)
    "has uptime when connected"
    true
    (Option.is_some (Supervisor.get_uptime conn))
;;

let test_data_heartbeat () =
  let conn = Supervisor.register_for_monitoring ~name:"heartbeat_test" in
  Supervisor.update_data_heartbeat conn;
  Alcotest.(check bool) "heartbeat updated" true (Option.is_some conn.last_data_received)
;;

let test_connection_metrics () =
  let conn = Supervisor.register_for_monitoring ~name:"metrics_test" in
  let initial_attempts = conn.reconnect_attempts in
  let initial_total = conn.total_connections in
  (* Connected transition increments total_connections. *)
  Supervisor.set_state conn Supervisor_types.Connected;
  Alcotest.(check int)
    "reconnect attempts unchanged"
    initial_attempts
    conn.reconnect_attempts;
  Alcotest.(check int)
    "total connections incremented"
    (initial_total + 1)
    conn.total_connections
;;

let test_post_only_rejection_reason () =
  Alcotest.(check bool)
    "HL post-only reason"
    true
    (Supervisor_orders.is_post_only_rejection
       "HL Order Rejected: Post only order would have immediately matched, bbo was \
        80.488@80.492");
  Alcotest.(check bool)
    "hyphenated post-only reason"
    true
    (Supervisor_orders.is_post_only_rejection "post-only order would cross");
  Alcotest.(check bool)
    "inventory rejection is not post-only"
    false
    (Supervisor_orders.is_post_only_rejection "insufficient funds")
;;

let test_rejection_bbo () =
  let err =
    "HL Amendment Rejected: Error placing new order during modify: Post only order would \
     have immediately matched, bbo was 80.892@80.893."
  in
  match Supervisor_orders.rejection_bbo err with
  | None -> Alcotest.fail "rejection BBO should parse"
  | Some (bid, ask) ->
    Alcotest.(check (float 1e-9)) "bid" 80.892 bid;
    Alcotest.(check (float 1e-9)) "ask" 80.893 ask;
    Alcotest.(check (option (pair (float 1e-9) (float 1e-9))))
      "unrelated rejection has no bbo"
      None
      (Supervisor_orders.rejection_bbo "insufficient funds")
;;

let test_maker_retry_price () =
  let bbo = 80.892, 80.893 in
  Alcotest.(check (float 1e-9))
    "sell retries above the ask"
    80.894
    (Supervisor_orders.maker_retry_price
       ~side:Dio_strategies.Strategy_common.Sell
       ~price:80.89
       ~bbo);
  Alcotest.(check (float 1e-9))
    "buy retries below the bid"
    80.891
    (Supervisor_orders.maker_retry_price
       ~side:Dio_strategies.Strategy_common.Buy
       ~price:80.89
       ~bbo)
;;

let test_amend_request_preserves_flags () =
  let order =
    { Dio_strategies.Strategy_common.operation = Dio_strategies.Strategy_common.Amend
    ; order_id = Some "oid-old"
    ; symbol = "TEST"
    ; market = Dio_strategies.Strategy_common.Perp
    ; side = Dio_strategies.Strategy_common.Buy
    ; order_type = "limit"
    ; qty = 1.0
    ; price = Some 10.0
    ; time_in_force = "GTC"
    ; post_only = false
    ; reduce_only = true
    ; userref = None
    ; strategy = Dio_strategies.Strategy_common.Carry
    ; exchange = "hyperliquid"
    ; duplicate_key = "carry:recycle:close|ro"
    }
  in
  let req = Supervisor_orders.amend_request_of_order order "oid-old" in
  Alcotest.(check (option bool))
    "reduce only"
    (Some true)
    req.Dio_engine.Order_executor.reduce_only;
  Alcotest.(check (option string))
    "tif"
    (Some "GTC")
    req.Dio_engine.Order_executor.time_in_force
;;

let carry_place_order duplicate_key =
  { Dio_strategies.Strategy_common.operation = Dio_strategies.Strategy_common.Place
  ; order_id = None
  ; symbol = "BTC"
  ; market = Dio_strategies.Strategy_common.Perp
  ; side = Dio_strategies.Strategy_common.Sell
  ; order_type = "limit"
  ; qty = 0.001
  ; price = Some 84_000.0
  ; time_in_force = "GTC"
  ; post_only = true
  ; reduce_only = false
  ; userref = None
  ; strategy = Dio_strategies.Strategy_common.Carry
  ; exchange = "hyperliquid"
  ; duplicate_key
  }
;;

let carry_amend_order ~symbol ~side ~reduce_only ~duplicate_key =
  { Dio_strategies.Strategy_common.operation = Dio_strategies.Strategy_common.Amend
  ; order_id = Some "old-id"
  ; symbol
  ; market = Dio_strategies.Strategy_common.Perp
  ; side
  ; order_type = "limit"
  ; qty = 0.001
  ; price = Some 84_000.0
  ; time_in_force = "GTC"
  ; post_only = false
  ; reduce_only
  ; userref = Some Dio_strategies.Strategy_common.strategy_userref_carry
  ; strategy = Dio_strategies.Strategy_common.Carry
  ; exchange = "hyperliquid"
  ; duplicate_key
  }
;;

let test_carry_amend_retires_old_and_tracks_new () =
  (* Venue amend = new order id. The old mapping must be retired (or it suppresses every
     later replace for the kind) and the new id must inherit the correct kind/market so
     its fill is booked. *)
  (* entry (sell, not reduce-only) *)
  let key = "carry:entry|hyperliquid|BTC" in
  Dio_strategies.Strategy_common.register_pending_carry_order
    ~dedup_key:key
    ~market:Dio_strategies.Strategy_common.Carry_perp
    ~maker:false
    ~kind:"carry:entry";
  Dio_strategies.Strategy_common.bind_carry_order ~dedup_key:key ~order_id:"btc-entry-old";
  let entry =
    carry_amend_order
      ~symbol:"BTC"
      ~side:Dio_strategies.Strategy_common.Sell
      ~reduce_only:false
      ~duplicate_key:"carry:entry"
  in
  Supervisor_orders.hedger_callbacks.on_amend_ok entry "btc-entry-old" "btc-entry-new";
  (match Dio_strategies.Strategy_common.lookup_carry_order ~order_id:"btc-entry-new" with
   | Some meta ->
     Alcotest.(check string) "new entry kind" "carry:entry" meta.com_kind;
     Alcotest.(check string) "new entry tracked key" key meta.com_key;
     Alcotest.(check bool)
       "new entry market"
       true
       (meta.com_market = Dio_strategies.Strategy_common.Carry_perp)
   | None -> Alcotest.fail "amended entry id is not tracked");
  Alcotest.(check bool)
    "old entry id retired"
    true
    (Dio_strategies.Strategy_common.lookup_carry_order ~order_id:"btc-entry-old" = None);
  (* take-profit (buy, reduce-only): the full key keeps the |ro suffix *)
  let ro = "carry:recycle:close|hyperliquid|BTC|ro" in
  Dio_strategies.Strategy_common.register_pending_carry_order
    ~dedup_key:ro
    ~market:Dio_strategies.Strategy_common.Carry_perp
    ~maker:false
    ~kind:"carry:recycle:close|ro";
  Dio_strategies.Strategy_common.bind_carry_order ~dedup_key:ro ~order_id:"tp-old";
  let tp =
    carry_amend_order
      ~symbol:"BTC"
      ~side:Dio_strategies.Strategy_common.Buy
      ~reduce_only:true
      ~duplicate_key:"carry:recycle:close|ro"
  in
  Supervisor_orders.hedger_callbacks.on_amend_ok tp "tp-old" "tp-new";
  (match Dio_strategies.Strategy_common.lookup_carry_order ~order_id:"tp-new" with
   | Some meta ->
     Alcotest.(check string) "new tp kind" "carry:recycle:close|ro" meta.com_kind;
     Alcotest.(check string) "new tp tracked key" ro meta.com_key
   | None -> Alcotest.fail "amended take-profit id is not tracked");
  Alcotest.(check bool)
    "old tp id retired"
    true
    (Dio_strategies.Strategy_common.lookup_carry_order ~order_id:"tp-old" = None);
  Dio_strategies.Strategy_common.retire_carry_order ~order_id:"btc-entry-new";
  Dio_strategies.Strategy_common.retire_carry_order ~order_id:"tp-new"
;;

let test_overlay_inflight_key_is_namespaced () =
  let key ~instrument ~exchange ~reduce_only =
    Dio_strategies.Strategy_cycle_engine.overlay_inflight_key
      ~dedup_key:"carry:entry"
      ~side:"sell"
      ~qty:0.001
      ~price:(Some 84_000.0)
      ~exchange
      ~instrument
      ~reduce_only
  in
  let btc = key ~instrument:"BTC" ~exchange:"hyperliquid" ~reduce_only:false in
  Alcotest.(check bool)
    "same kind on another instrument does not collide"
    false
    (String.equal btc (key ~instrument:"HYPE" ~exchange:"hyperliquid" ~reduce_only:false));
  Alcotest.(check bool)
    "same instrument on another venue does not collide"
    false
    (String.equal btc (key ~instrument:"BTC" ~exchange:"kraken" ~reduce_only:false));
  Alcotest.(check bool)
    "reduce-only kind does not collide with the entry kind"
    false
    (String.equal btc (key ~instrument:"BTC" ~exchange:"hyperliquid" ~reduce_only:true))
;;

let test_carry_place_ok_releases_inflight_key () =
  let key = "carry:entry|hyperliquid|BTC" in
  let order = carry_place_order key in
  Alcotest.(check bool)
    "key inserted"
    true
    (Dio_strategies.Strategy_common.InFlightOrders.add_in_flight_order key);
  Supervisor_orders.hedger_callbacks.on_place_ok order "oid-test";
  (* Ack releases the placement key; the id -> kind registry remains the duplicate guard
     until the terminal event. *)
  Alcotest.(check bool)
    "key released on ack"
    false
    (Dio_strategies.Strategy_common.InFlightOrders.is_in_flight key)
;;

let test_carry_place_fail_releases_inflight_key () =
  let key = "carry:entry|hyperliquid|BTC" in
  let order = carry_place_order key in
  Alcotest.(check bool)
    "key inserted"
    true
    (Dio_strategies.Strategy_common.InFlightOrders.add_in_flight_order key);
  Supervisor_orders.hedger_callbacks.on_place_fail order "post only";
  Alcotest.(check bool)
    "key released on terminal failure"
    false
    (Dio_strategies.Strategy_common.InFlightOrders.is_in_flight key)
;;

let test_overlay_place_skips_while_in_flight () =
  Dio_strategies.Strategy_actions_builtin.register_all ();
  ignore (Dio_strategies.Strategy_api.get_pending_orders 32 : _ list);
  let asset : Dio_strategies.Strategy_state.trading_config =
    { exchange = "hyperliquid"
    ; symbol = "TEST/USDC"
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "carry_overlay"
    ; maker_fee = None
    ; taker_fee = None
    ; accumulation_buffer = 0.01
    ; base_accumulation = false
    ; sell_levels_persistence = false
    }
  in
  let ctx = Dio_strategies.Strategy_cycle_engine.create () in
  ctx.cg_asset <- Some asset;
  ctx.cg_symbol <- asset.symbol;
  ctx.cg_now <- 1.0;
  let key = "test:skip|hyperliquid|TEST" in
  let place () =
    Dio_strategies.Strategy_cycle_engine.order_place
      ctx
      ~symbol:None
      ~market:"perp"
      ~side:"sell"
      ~qty:0.1
      ~price:(Some 100.0)
      ~tif:"IOC"
      ~reduce_only:false
      ~post_only:false
      ~dedup_key:"test:skip"
  in
  ignore (Dio_strategies.Strategy_common.InFlightOrders.add_in_flight_order key);
  place ();
  Alcotest.(check int)
    "no order pushed while the key is in flight"
    0
    (List.length (Dio_strategies.Strategy_api.get_pending_orders 32));
  ignore (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order key);
  place ();
  Alcotest.(check int)
    "order pushed once the key is released"
    1
    (List.length (Dio_strategies.Strategy_api.get_pending_orders 32));
  ignore (Dio_strategies.Strategy_api.get_pending_orders 32 : _ list);
  ignore (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order key)
;;

let test_inflight_stale_detection () =
  let key = "stale:test|hyperliquid|BTC" in
  ignore (Dio_strategies.Strategy_common.InFlightOrders.add_in_flight_order key);
  let fresh, _ = Dio_strategies.Strategy_common.InFlightOrders.stale ~age:3600.0 () in
  Alcotest.(check int) "fresh key is not stale" 0 fresh;
  let count, oldest =
    Dio_strategies.Strategy_common.InFlightOrders.stale ~age:(-1.0) ()
  in
  Alcotest.(check int) "inserted key is stale at a negative threshold" 1 count;
  (match oldest with
   | Some (k, _) -> Alcotest.(check string) "oldest key reported" key k
   | None -> Alcotest.fail "expected an oldest held entry");
  ignore (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order key);
  let after, _ = Dio_strategies.Strategy_common.InFlightOrders.stale ~age:(-1.0) () in
  Alcotest.(check int) "removed key is no longer reported" 0 after
;;

let () =
  Alcotest.run
    "Supervisor"
    [ ( "registration"
      , [ Alcotest.test_case "connection registration" `Quick test_connection_registration
        ; Alcotest.test_case
            "monitoring registration"
            `Quick
            test_connection_registration_monitoring
        ] )
    ; ( "state_management"
      , [ Alcotest.test_case "state transitions" `Quick test_state_management ] )
    ; ( "circuit_breaker"
      , [ Alcotest.test_case "initial state" `Quick test_circuit_breaker_initial
        ; Alcotest.test_case
            "failure counting"
            `Quick
            test_circuit_breaker_failure_counting
        ; Alcotest.test_case "recovery" `Quick test_circuit_breaker_recovery
        ] )
    ; "token_store", [ Alcotest.test_case "token operations" `Quick test_token_store ]
    ; ( "queries"
      , [ Alcotest.test_case "connection queries" `Quick test_connection_queries ] )
    ; ( "monitoring"
      , [ Alcotest.test_case "uptime" `Quick test_connection_uptime
        ; Alcotest.test_case "heartbeat" `Quick test_data_heartbeat
        ; Alcotest.test_case "metrics" `Quick test_connection_metrics
        ] )
    ; ( "order_retry"
      , [ Alcotest.test_case
            "post-only rejection detection"
            `Quick
            test_post_only_rejection_reason
        ; Alcotest.test_case "rejection bbo parses" `Quick test_rejection_bbo
        ; Alcotest.test_case "maker retry price" `Quick test_maker_retry_price
        ; Alcotest.test_case
            "amend request preserves flags"
            `Quick
            test_amend_request_preserves_flags
        ] )
    ; ( "carry_inflight"
      , [ Alcotest.test_case
            "overlay in-flight key is namespaced"
            `Quick
            test_overlay_inflight_key_is_namespaced
        ; Alcotest.test_case
            "carry place ack releases the key"
            `Quick
            test_carry_place_ok_releases_inflight_key
        ; Alcotest.test_case
            "carry place failure releases the key"
            `Quick
            test_carry_place_fail_releases_inflight_key
        ; Alcotest.test_case
            "overlay place retries are skipped while in flight"
            `Quick
            test_overlay_place_skips_while_in_flight
        ; Alcotest.test_case
            "carry amend retires old id and tracks new"
            `Quick
            test_carry_amend_retires_old_and_tracks_new
        ; Alcotest.test_case
            "stale in-flight keys are detected"
            `Quick
            test_inflight_stale_detection
        ] )
    ]
;;
