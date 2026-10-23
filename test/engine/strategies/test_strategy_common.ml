(* Sample strategy-order fixture. *)
let create_test_order
  ?(operation = Dio_strategies.Strategy_common.Place)
  ?(order_id = None)
  ?(symbol = "BTC/USD")
  ?(side = Dio_strategies.Strategy_common.Buy)
  ?(order_type = "limit")
  ?(qty = 0.001)
  ?(price = Some 50000.0)
  ?(time_in_force = "GTC")
  ?(post_only = true)
  ?(userref = Some 2)
  ?(strategy = Dio_strategies.Strategy_common.MM)
  ?duplicate_key
  ()
  =
  let duplicate_key =
    match duplicate_key with
    | Some k -> k
    | None ->
      Dio_strategies.Strategy_common.generate_duplicate_key
        symbol
        (Dio_strategies.Strategy_common.string_of_order_side side)
        qty
        price
  in
  { Dio_strategies.Strategy_common.operation
  ; order_id
  ; symbol
  ; market = Dio_strategies.Strategy_common.Spot
  ; side
  ; order_type
  ; qty
  ; price
  ; time_in_force
  ; post_only
  ; reduce_only = false
  ; userref
  ; strategy
  ; duplicate_key
  ; exchange = "kraken"
  }
;;

let test_order_side_conversion () =
  Alcotest.(check string)
    "buy side conversion"
    "buy"
    (Dio_strategies.Strategy_common.string_of_order_side
       Dio_strategies.Strategy_common.Buy);
  Alcotest.(check string)
    "sell side conversion"
    "sell"
    (Dio_strategies.Strategy_common.string_of_order_side
       Dio_strategies.Strategy_common.Sell)
;;

let test_operation_type_conversion () =
  Alcotest.(check string)
    "place operation conversion"
    "place"
    (Dio_strategies.Strategy_common.string_of_operation_type
       Dio_strategies.Strategy_common.Place);
  Alcotest.(check string)
    "amend operation conversion"
    "amend"
    (Dio_strategies.Strategy_common.string_of_operation_type
       Dio_strategies.Strategy_common.Amend);
  Alcotest.(check string)
    "cancel operation conversion"
    "cancel"
    (Dio_strategies.Strategy_common.string_of_operation_type
       Dio_strategies.Strategy_common.Cancel)
;;

let test_strategy_order_creation () =
  let order = create_test_order ~userref:(Some 12345) () in
  Alcotest.(check bool)
    "order operation"
    true
    (order.operation = Dio_strategies.Strategy_common.Place);
  Alcotest.(check (option string)) "order id" None order.order_id;
  Alcotest.(check string) "order symbol" "BTC/USD" order.symbol;
  Alcotest.(check bool) "order side" true (order.side = Dio_strategies.Strategy_common.Buy);
  Alcotest.(check string) "order type" "limit" order.order_type;
  Alcotest.(check (float 0.001)) "order qty" 0.001 order.qty;
  Alcotest.(check (option (float 0.001))) "order price" (Some 50000.0) order.price;
  Alcotest.(check string) "time in force" "GTC" order.time_in_force;
  Alcotest.(check bool) "post only" true order.post_only;
  Alcotest.(check (option int)) "userref" (Some 12345) order.userref;
  Alcotest.(check bool)
    "strategy"
    true
    (order.strategy = Dio_strategies.Strategy_common.MM)
;;

let test_strategy_order_variants () =
  let place_order =
    create_test_order ~operation:Dio_strategies.Strategy_common.Place ()
  in
  let amend_order =
    create_test_order
      ~operation:Dio_strategies.Strategy_common.Amend
      ~order_id:(Some "order123")
      ~price:(Some 51000.0)
      ()
  in
  let cancel_order =
    create_test_order
      ~operation:Dio_strategies.Strategy_common.Cancel
      ~order_id:(Some "order456")
      ~qty:0.0
      ~price:None
      ()
  in
  Alcotest.(check bool)
    "place order operation"
    true
    (place_order.operation = Dio_strategies.Strategy_common.Place);
  Alcotest.(check bool)
    "amend order operation"
    true
    (amend_order.operation = Dio_strategies.Strategy_common.Amend);
  Alcotest.(check (option string)) "amend order id" (Some "order123") amend_order.order_id;
  Alcotest.(check bool)
    "cancel order operation"
    true
    (cancel_order.operation = Dio_strategies.Strategy_common.Cancel);
  Alcotest.(check (option string))
    "cancel order id"
    (Some "order456")
    cancel_order.order_id;
  Alcotest.(check (option (float 0.001))) "cancel order price" None cancel_order.price
;;

let test_queue_creation () =
  let _ = Dio_strategies.Strategy_common.LockFreeQueue.create () in
  Alcotest.(check bool) "queue created" true true
;;

let test_queue_basic_write_read () =
  let buffer = Dio_strategies.Strategy_common.LockFreeQueue.create () in
  let order = create_test_order () in
  let write_result = Dio_strategies.Strategy_common.LockFreeQueue.write buffer order in
  Alcotest.(check (option unit)) "write success" (Some ()) write_result;
  let read_result = Dio_strategies.Strategy_common.LockFreeQueue.read buffer in
  Alcotest.(check bool) "read order success" true (Option.is_some read_result);
  let read_order = Option.get read_result in
  Alcotest.(check bool)
    "read order operation"
    true
    (read_order.operation = order.operation);
  Alcotest.(check string) "read order symbol" order.symbol read_order.symbol
;;

let test_queue_empty_read () =
  let buffer = Dio_strategies.Strategy_common.LockFreeQueue.create () in
  let read_result = Dio_strategies.Strategy_common.LockFreeQueue.read buffer in
  Alcotest.(check bool) "empty read" true (Option.is_none read_result)
;;

let test_queue_batch_read () =
  let buffer = Dio_strategies.Strategy_common.LockFreeQueue.create () in
  let order1 = create_test_order ~symbol:"BTC/USD" () in
  let order2 = create_test_order ~symbol:"ETH/USD" () in
  let order3 = create_test_order ~symbol:"LTC/USD" () in
  let _ = Dio_strategies.Strategy_common.LockFreeQueue.write buffer order1 in
  let _ = Dio_strategies.Strategy_common.LockFreeQueue.write buffer order2 in
  let _ = Dio_strategies.Strategy_common.LockFreeQueue.write buffer order3 in
  let batch = Dio_strategies.Strategy_common.LockFreeQueue.read_batch buffer 2 in
  Alcotest.(check int) "batch size" 2 (List.length batch)
;;

let test_queue_batch_read_empty () =
  let buffer = Dio_strategies.Strategy_common.LockFreeQueue.create () in
  let batch = Dio_strategies.Strategy_common.LockFreeQueue.read_batch buffer 10 in
  Alcotest.(check bool) "empty batch" true (batch = [])
;;

let test_create_amend_order_preserves_flags () =
  let order =
    Dio_strategies.Strategy_api.create_amend_order
      ~reduce_only:true
      ~time_in_force:"IOC"
      "oid-1"
      "TEST/USDC"
      Dio_strategies.Strategy_common.Buy
      1.0
      (Some 10.0)
      false
      Dio_strategies.Strategy_common.Carry
      "kraken"
  in
  Alcotest.(check bool) "reduce only preserved" true order.reduce_only;
  Alcotest.(check string) "tif preserved" "IOC" order.time_in_force
;;

let test_overlay_inflight_key () =
  let module C = Dio_strategies.Strategy_common in
  Alcotest.(check string)
    "entry key is venue + instrument scoped"
    "carry:entry|hyperliquid|XMR"
    (C.overlay_inflight_key
       ~dedup_key:"carry:entry"
       ~side:"sell"
       ~qty:0.0
       ~price:None
       ~exchange:"hyperliquid"
       ~instrument:"XMR"
       ~reduce_only:false);
  Alcotest.(check string)
    "reduce-only key keeps the suffix at the end"
    "carry:recycle:close|hyperliquid|XMR|ro"
    (C.overlay_inflight_key
       ~dedup_key:"carry:recycle:close"
       ~side:"buy"
       ~qty:0.0
       ~price:None
       ~exchange:"hyperliquid"
       ~instrument:"XMR"
       ~reduce_only:true)
;;

let test_carry_order_terminal () =
  let open Dio_exchange.Exchange_intf.Types in
  let check label expected status =
    Alcotest.(check bool)
      label
      expected
      (Dio_strategies.Strategy_common.carry_order_terminal status)
  in
  (* A placement ack is not terminal: [New]/[PartiallyFilled] must stay matchable, or
     [order_replace] duplicates the resting take-profit. *)
  check "New keeps the mapping" false New;
  check "PartiallyFilled keeps the mapping" false PartiallyFilled;
  check "Pending keeps the mapping" false Pending;
  check "Filled retires the mapping" true Filled;
  check "Canceled retires the mapping" true Canceled;
  check "Rejected retires the mapping" true Rejected;
  check "Expired retires the mapping" true Expired;
  check "Unknown keeps the mapping" false (Unknown "resting")
;;

let test_grid_order_ownership () =
  let module C = Dio_strategies.Strategy_common in
  Alcotest.(check bool) "grid tag" true (C.is_grid_order (Some C.strategy_userref_grid));
  Alcotest.(check bool)
    "carry tag is not a grid order"
    false
    (C.is_grid_order (Some C.strategy_userref_carry));
  Alcotest.(check bool)
    "mm tag is not a grid order"
    false
    (C.is_grid_order (Some C.strategy_userref_mm));
  Alcotest.(check bool) "untagged order counts as grid" true (C.is_grid_order None);
  Alcotest.(check bool)
    "userref 0 counts as untagged grid"
    true
    (C.is_grid_order (Some 0));
  Alcotest.(check string)
    "owner: grid"
    "grid"
    (C.order_owner (Some C.strategy_userref_grid));
  Alcotest.(check string)
    "owner: carry"
    "carry"
    (C.order_owner (Some C.strategy_userref_carry));
  Alcotest.(check string) "owner: mm" "mm" (C.order_owner (Some C.strategy_userref_mm));
  Alcotest.(check string) "owner: untagged" "grid" (C.order_owner None);
  Alcotest.(check string) "owner: userref 0" "grid" (C.order_owner (Some 0));
  Alcotest.(check string) "owner: unknown tag" "other" (C.order_owner (Some 99))
;;

let test_tracked_carry_order_has_no_age_bound () =
  let module C = Dio_strategies.Strategy_common in
  let key = "test:tracked|hyperliquid|TEST" in
  let order_id = "oid-tracked-test" in
  C.register_pending_carry_order
    ~dedup_key:key
    ~market:C.Carry_perp
    ~maker:false
    ~kind:"carry:entry";
  C.bind_carry_order ~dedup_key:key ~order_id;
  Alcotest.(check bool) "acked order is tracked" true (C.has_tracked_carry_order ~key ());
  (* Age the mapping well past 5 s. A resting take-profit can sit for minutes; the
     tracked-order guard must still suppress a duplicate placement. *)
  Mutex.lock C.carry_registry_mutex;
  (match Hashtbl.find_opt C.carry_orders order_id with
   | Some meta ->
     Hashtbl.replace
       C.carry_orders
       order_id
       { meta with C.com_at = Unix.gettimeofday () -. 3600.0 }
   | None -> ());
  Mutex.unlock C.carry_registry_mutex;
  Alcotest.(check bool)
    "tracked guard does not age out"
    true
    (C.has_tracked_carry_order ~key ());
  C.retire_carry_order ~order_id;
  Alcotest.(check bool)
    "terminal retirement clears the guard"
    false
    (C.has_tracked_carry_order ~key ())
;;

let test_resolve_fee_rate () =
  (* Fee resolution never invents a literal rate: explicit config, then the live cache,
     then the caller's last known value (which the domain seeded from the venue default). *)
  let open Dio_strategies.Strategy_venue in
  Alcotest.(check (float 1e-12))
    "config wins"
    0.002
    (resolve_fee_rate ~configured:(Some 0.002) ~cached:(Some 0.003) ~current:0.004);
  Alcotest.(check (float 1e-12))
    "cache next"
    0.003
    (resolve_fee_rate ~configured:None ~cached:(Some 0.003) ~current:0.004);
  Alcotest.(check (float 1e-12))
    "last known value next"
    0.004
    (resolve_fee_rate ~configured:None ~cached:None ~current:0.004);
  Alcotest.(check (float 1e-12))
    "non-finite config falls through"
    0.003
    (resolve_fee_rate ~configured:(Some Float.nan) ~cached:(Some 0.003) ~current:0.004)
;;

let () =
  Alcotest.run
    "Strategy Common"
    [ ( "conversions"
      , [ Alcotest.test_case "order side conversion" `Quick test_order_side_conversion
        ; Alcotest.test_case
            "operation type conversion"
            `Quick
            test_operation_type_conversion
        ] )
    ; ( "orders"
      , [ Alcotest.test_case "strategy order creation" `Quick test_strategy_order_creation
        ; Alcotest.test_case "strategy order variants" `Quick test_strategy_order_variants
        ; Alcotest.test_case
            "amend preserves reduce-only and tif"
            `Quick
            test_create_amend_order_preserves_flags
        ; Alcotest.test_case
            "overlay in-flight key is venue and instrument scoped"
            `Quick
            test_overlay_inflight_key
        ; Alcotest.test_case
            "carry mapping retires only on terminal status"
            `Quick
            test_carry_order_terminal
        ; Alcotest.test_case
            "tracked carry order guard has no age bound"
            `Quick
            test_tracked_carry_order_has_no_age_bound
        ; Alcotest.test_case
            "only grid-tagged orders are grid-owned"
            `Quick
            test_grid_order_ownership
        ] )
    ; ( "fees"
      , [ Alcotest.test_case
            "fee resolution never invents a rate"
            `Quick
            test_resolve_fee_rate
        ] )
    ; ( "lockfreequeue"
      , [ Alcotest.test_case "queue creation" `Quick test_queue_creation
        ; Alcotest.test_case "basic write/read" `Quick test_queue_basic_write_read
        ; Alcotest.test_case "empty read" `Quick test_queue_empty_read
        ; Alcotest.test_case "batch read" `Quick test_queue_batch_read
        ; Alcotest.test_case "batch read empty" `Quick test_queue_batch_read_empty
        ] )
    ]
;;
