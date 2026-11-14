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

let test_live_carry_order_holds_until_terminal () =
  let module C = Dio_strategies.Strategy_common in
  let exchange = "hyperliquid" in
  let instrument = "TEST" in
  let kind = "test:tp|ro" in
  let key = "test:tp|hyperliquid|TEST|ro" in
  let order_id = "oid-live-test" in
  Alcotest.(check string)
    "scope: perp close"
    "|hyperliquid|TEST|ro"
    (C.carry_scope ~exchange ~instrument ~reduce_only:true);
  Alcotest.(check string)
    "scope: perp entry"
    "|hyperliquid|TEST"
    (C.carry_scope ~exchange ~instrument ~reduce_only:false);
  C.register_carry_order_id ~order_id ~market:C.Carry_perp ~maker:false ~kind ~key;
  let live () = C.has_live_carry_order ~kind ~exchange ~instrument ~reduce_only:true () in
  Alcotest.(check bool) "acked close is live" true (live ());
  Alcotest.(check bool)
    "another instrument is unaffected"
    false
    (C.has_live_carry_order ~kind ~exchange ~instrument:"OTHER" ~reduce_only:true ());
  Alcotest.(check bool)
    "another kind is unaffected"
    false
    (C.has_live_carry_order ~kind:"test:entry" ~exchange ~instrument ~reduce_only:true ());
  (* A mapping the venue never retires must stop suppressing placement. *)
  let shard = C.carry_shard_of order_id in
  Mutex.lock C.carry_order_mutexes.(shard);
  (match Concurrency.Fifo_index.find_opt C.carry_order_shards.(shard) order_id with
   | Some meta ->
     Concurrency.Fifo_index.set
       C.carry_order_shards.(shard)
       ~key:order_id
       ~value:{ meta with C.com_at = Unix.gettimeofday () -. 3600.0 }
   | None -> Alcotest.fail "live close mapping was not registered");
  Mutex.unlock C.carry_order_mutexes.(shard);
  Alcotest.(check bool) "an unreleased mapping stops suppressing" false (live ());
  C.retire_carry_order ~order_id;
  Alcotest.(check bool) "terminal retirement re-arms placement" false (live ())
;;

(** The trading domain defers its order ring to a cycle-bottom flush, so the self-pipe write
    stops landing inside the measured STRATEGY span. Every other producer must keep the
    immediate write: it has no flush of its own. *)
let test_order_signal_deferral () =
  let module C = Dio_strategies.Strategy_common in
  let module S = C.OrderSignal in
  let ready () =
    let r, _, _ = Unix.select [ S.read_fd ] [] [] 0.0 in
    r <> []
  in
  (* The consumer clears [pending] before draining; this test plays the consumer. *)
  S.drain_pipe ();
  Atomic.set S.pending false;
  S.set_deferred true;
  S.broadcast ();
  Alcotest.(check bool) "deferred ring writes no byte" false (ready ());
  S.flush ();
  Alcotest.(check bool) "flush writes the byte" true (ready ());
  S.drain_pipe ();
  Atomic.set S.pending false;
  S.set_deferred false;
  S.broadcast ();
  Alcotest.(check bool) "default producer writes immediately" true (ready ());
  S.drain_pipe ();
  Atomic.set S.pending false;
  S.flush ();
  Alcotest.(check bool) "flush without a pending ring writes nothing" false (ready ())
;;

let test_carry_registry_stays_bounded () =
  let module C = Dio_strategies.Strategy_common in
  let module F = Concurrency.Fifo_index in
  let key = "test:bounded|hyperliquid|TEST" in
  (* Amend acks retire the old id and register a new one. Retiring cannot remove the FIFO
     admission record in O(1), so without the periodic trim the queue would retain one
     record per order ever seen while the map stayed at one entry. *)
  for i = 1 to 5_000 do
    let order_id = Printf.sprintf "oid-bounded-%d" i in
    C.register_carry_order_id
      ~order_id
      ~market:C.Carry_perp
      ~maker:false
      ~kind:"carry:entry"
      ~key;
    C.retire_carry_order ~order_id
  done;
  (* The index is sharded by order id, so its statistics are summed across shards. *)
  let stats () =
    Array.fold_left
      (fun (entries, pending) idx ->
        let s = F.stats idx in
        entries + s.F.entries, pending + s.F.pending_queue)
      (0, 0)
      C.carry_order_shards
  in
  let entries_before, pending_before = stats () in
  Alcotest.(check int) "retired ids leave the map empty" 0 entries_before;
  Alcotest.(check bool)
    "superseded queue records are retained until the sweep"
    true
    (pending_before >= 5_000);
  ignore (C.trim_carry_orders ());
  let entries_after, pending_after = stats () in
  Alcotest.(check int) "sweep compacts the admission queue" 0 pending_after;
  Alcotest.(check bool)
    "map never exceeds its declared cap"
    true
    (entries_after <= C.carry_order_capacity)
;;

let test_unanswered_amendment_is_age_bounded () =
  let module C = Dio_strategies.Strategy_common in
  let module A = C.InFlightAmendments in
  let order_id = "oid-amend-stale" in
  ignore (A.add_in_flight_amendment order_id);
  let shard = A.shard_of order_id in
  Mutex.lock A.shard_mutexes.(shard);
  (match Hashtbl.find_opt A.registries.(shard) order_id with
   | Some entry -> entry.last <- Unix.gettimeofday () -. (A.pending_max_age_s +. 1.0)
   | None -> ());
  Mutex.unlock A.shard_mutexes.(shard);
  (* [cleanup] only runs once a second; make the throttle window already elapsed. *)
  Atomic.set A.last_cleanup 0.0;
  ignore (A.cleanup ());
  Mutex.lock A.shard_mutexes.(shard);
  let still_present = Hashtbl.mem A.registries.(shard) order_id in
  Mutex.unlock A.shard_mutexes.(shard);
  Alcotest.(check bool)
    "an amendment older than any possible REST round-trip is evicted"
    false
    still_present
;;

let test_carry_pending_is_age_bounded () =
  let module C = Dio_strategies.Strategy_common in
  let key = "test:pending|hyperliquid|TEST" in
  C.register_pending_carry_order
    ~dedup_key:key
    ~market:C.Carry_perp
    ~maker:false
    ~kind:"carry:entry";
  Alcotest.(check bool)
    "a fresh placement waits for its ack"
    true
    (Hashtbl.mem C.carry_pending key);
  ignore (C.trim_carry_orders ~max_pending_age:(-1.0) ());
  Alcotest.(check bool)
    "a lost ack cannot retain the entry for the process lifetime"
    false
    (Hashtbl.mem C.carry_pending key)
;;

let test_reduce_only_room_clamps_to_position () =
  let module C = Dio_strategies.Strategy_common in
  let room ~position side qty = C.reduce_only_room ~position ~side ~qty in
  Alcotest.(check (float 1e-9))
    "close long is capped at the long"
    (room ~position:(Some 2.0) C.Sell 5.0)
    2.0;
  Alcotest.(check (float 1e-9))
    "close long may be smaller than the long"
    (room ~position:(Some 2.0) C.Sell 0.5)
    0.5;
  Alcotest.(check (float 1e-9))
    "a long cannot be closed with a buy"
    (room ~position:(Some 2.0) C.Buy 5.0)
    0.0;
  Alcotest.(check (float 1e-9))
    "close short is capped at the short"
    (room ~position:(Some (-2.0)) C.Buy 5.0)
    2.0;
  Alcotest.(check (float 1e-9))
    "a short cannot be closed with a sell"
    (room ~position:(Some (-2.0)) C.Sell 5.0)
    0.0;
  Alcotest.(check (float 1e-9))
    "flat has no room"
    (room ~position:(Some 0.0) C.Sell 5.0)
    0.0;
  Alcotest.(check (float 1e-9))
    "unknown position passes through to the venue flag"
    (room ~position:None C.Sell 5.0)
    5.0
;;

let test_untagged_order_adoption () =
  let module C = Dio_strategies.Strategy_common in
  let owns ~price_hits ~adopt ~want ~venue =
    C.untagged_order_is_own_kind
      ~price_hits_target:price_hits
      ~adopt_by_flags:adopt
      ~want_reduce_only:want
      ~venue_reduce_only:venue
  in
  (* The old exact-price fallback must keep working even when the venue does not report
     the flag. *)
  Alcotest.(check bool)
    "target price always matches"
    true
    (owns ~price_hits:true ~adopt:false ~want:true ~venue:None);
  Alcotest.(check bool)
    "an adopted close needs the venue reduce-only flag"
    true
    (owns ~price_hits:false ~adopt:true ~want:true ~venue:(Some true));
  Alcotest.(check bool)
    "a non-reduce-only sell is never adopted as a take-profit"
    false
    (owns ~price_hits:false ~adopt:true ~want:true ~venue:(Some false));
  Alcotest.(check bool)
    "a stray non-reduce-only sell at the target price is not adopted"
    false
    (owns ~price_hits:true ~adopt:true ~want:true ~venue:(Some false));
  Alcotest.(check bool)
    "an adopted entry needs a non-reduce-only order"
    true
    (owns ~price_hits:false ~adopt:true ~want:false ~venue:(Some false));
  Alcotest.(check bool)
    "a reduce-only buy is not adopted as an entry"
    false
    (owns ~price_hits:false ~adopt:true ~want:false ~venue:(Some true));
  Alcotest.(check bool)
    "a venue that hides the flag cannot be adopted"
    false
    (owns ~price_hits:false ~adopt:true ~want:true ~venue:None);
  Alcotest.(check bool)
    "a shared market is not adopted"
    false
    (owns ~price_hits:false ~adopt:false ~want:true ~venue:(Some true))
;;

let test_placement_backoff_is_bounded () =
  let module C = Dio_strategies.Strategy_common in
  let key = "test:backoff|hyperliquid|TEST" in
  C.Placement_backoff.note_failure ~err:"Insufficient margin" ~key ();
  Alcotest.(check bool)
    "rejection holds the key"
    true
    (C.Placement_backoff.remaining ~key () > 0.0);
  C.Placement_backoff.clear ~key;
  Alcotest.(check bool)
    "success releases the key"
    true
    (C.Placement_backoff.remaining ~key () = 0.0);
  C.Placement_backoff.note_failure
    ~err:"Failed to parse HL response (Raw: \"429 Too Many Requests\")"
    ~key
    ();
  Alcotest.(check bool)
    "rate limit backs off longer than a plain rejection"
    true
    (C.Placement_backoff.remaining ~key () > C.Placement_backoff.default_s);
  C.Placement_backoff.clear ~key;
  C.Placement_backoff.note_unresolved_amend ~key ();
  Alcotest.(check bool)
    "unresolved amend holds briefly"
    true
    (C.Placement_backoff.remaining ~key () > 0.0);
  C.Placement_backoff.clear ~key
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
            "live carry order holds placement until its terminal event"
            `Quick
            test_live_carry_order_holds_until_terminal
        ; Alcotest.test_case
            "carry registry queue is compacted and capped"
            `Quick
            test_carry_registry_stays_bounded
        ; Alcotest.test_case
            "order ring defers to the cycle-bottom flush"
            `Quick
            test_order_signal_deferral
        ; Alcotest.test_case
            "unanswered amendment is age bounded"
            `Quick
            test_unanswered_amendment_is_age_bounded
        ; Alcotest.test_case
            "placement backoff is bounded"
            `Quick
            test_placement_backoff_is_bounded
        ; Alcotest.test_case
            "carry pending placements are age bounded"
            `Quick
            test_carry_pending_is_age_bounded
        ; Alcotest.test_case
            "reduce-only size is capped at the live position"
            `Quick
            test_reduce_only_room_clamps_to_position
        ; Alcotest.test_case
            "untagged orders adopt only by venue flags"
            `Quick
            test_untagged_order_adoption
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
