open Dio_strategies

let oi ?(side = "buy") ?(qty = 1.0) ?(price = 100.0) () =
  { Strategy_trace.oi_symbol = "BTC/USDC"
  ; oi_side = side
  ; oi_qty = qty
  ; oi_price = price
  ; oi_post_only = true
  ; oi_reduce_only = false
  ; oi_tif = None
  }
;;

let record_buy_run r =
  Strategy_event_recorder.record_order_intent r (oi ());
  Strategy_event_recorder.record_state
    r
    [ "tracked_buy", Strategy_expr.V_bool true; "reserved", Strategy_expr.V_float 0.0 ];
  Strategy_event_recorder.end_cycle r;
  Strategy_event_recorder.record_order_intent r (oi ~side:"sell" ~price:110.0 ());
  Strategy_event_recorder.record_persistence r "accumulation" "{\"reserved\":0}";
  Strategy_event_recorder.end_cycle r
;;

let test_identity () =
  let ref_trace = Strategy_equivalence.capture record_buy_run in
  let cand_trace = Strategy_equivalence.capture record_buy_run in
  Alcotest.(check bool)
    "ref vs ref equivalent"
    true
    (Strategy_equivalence.is_equiv
       (Strategy_equivalence.compare_traces ~reference:ref_trace ~candidate:cand_trace))
;;

let test_state_order_insensitive () =
  (* state entries compared as a set, not positionally *)
  let a =
    Strategy_equivalence.capture (fun r ->
      Strategy_event_recorder.record_state
        r
        [ "x", Strategy_expr.V_float 1.0; "y", Strategy_expr.V_float 2.0 ];
      Strategy_event_recorder.end_cycle r)
  in
  let b =
    Strategy_equivalence.capture (fun r ->
      Strategy_event_recorder.record_state
        r
        [ "y", Strategy_expr.V_float 2.0; "x", Strategy_expr.V_float 1.0 ];
      Strategy_event_recorder.end_cycle r)
  in
  Alcotest.(check bool)
    "state set equal"
    true
    (Strategy_equivalence.is_equiv
       (Strategy_equivalence.compare_traces ~reference:a ~candidate:b))
;;

let test_divergence_order () =
  let reference = Strategy_equivalence.capture record_buy_run in
  let diverging r =
    (* wrong price on the first order *)
    Strategy_event_recorder.record_order_intent r (oi ~price:101.0 ());
    Strategy_event_recorder.record_state
      r
      [ "tracked_buy", Strategy_expr.V_bool true; "reserved", Strategy_expr.V_float 0.0 ];
    Strategy_event_recorder.end_cycle r;
    Strategy_event_recorder.record_order_intent r (oi ~side:"sell" ~price:110.0 ());
    Strategy_event_recorder.record_persistence r "accumulation" "{\"reserved\":0}";
    Strategy_event_recorder.end_cycle r
  in
  match Strategy_equivalence.check ~reference diverging with
  | Equiv -> Alcotest.fail "expected divergence"
  | Divergence _ -> ()
;;

let test_divergence_cycle_count () =
  let reference = Strategy_equivalence.capture record_buy_run in
  let short r =
    Strategy_event_recorder.record_order_intent r (oi ());
    Strategy_event_recorder.record_state
      r
      [ "tracked_buy", Strategy_expr.V_bool true; "reserved", Strategy_expr.V_float 0.0 ];
    Strategy_event_recorder.end_cycle r
  in
  match Strategy_equivalence.check ~reference short with
  | Equiv -> Alcotest.fail "expected divergence"
  | Divergence m ->
    Alcotest.(check bool)
      "mentions extra cycles"
      true
      (let needle = "extra cycles" in
       let n = String.length needle
       and s = String.length m in
       let rec loop i = i + n <= s && (String.sub m i n = needle || loop (i + 1)) in
       loop 0)
;;

let () =
  Alcotest.run
    "strategy_harness"
    [ ( "equivalence"
      , [ Alcotest.test_case "identity (ref vs ref)" `Quick test_identity
        ; Alcotest.test_case "state order-insensitive" `Quick test_state_order_insensitive
        ; Alcotest.test_case "divergence: order field" `Quick test_divergence_order
        ; Alcotest.test_case "divergence: cycle count" `Quick test_divergence_cycle_count
        ] )
    ]
;;
