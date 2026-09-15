open Dio_strategies

let oi ?(side = "buy") ?(qty = 1.0) ?(price = 100.0) () =
  { Strategy_trace.oi_symbol = "BTC/USDC"
  ; oi_side = side
  ; oi_qty = qty
  ; oi_price = price
  ; oi_post_only = true
  ; oi_reduce_only = false
  ; oi_tif = None
  ; oi_order_id = None
  ; oi_userref = None
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

let test_roundtrip () =
  let t = Strategy_equivalence.capture record_buy_run in
  let path = Filename.temp_file "strategy_trace" ".json" in
  Strategy_trace.save path t;
  let loaded = Strategy_trace.load path in
  Alcotest.(check bool) "roundtrip equal" true (Strategy_trace.equal t loaded)
;;

let sample_event () =
  { Strategy_trace.ev_kind = "filled"
  ; ev_now = 12.5
  ; ev_order_id = "o1"
  ; ev_new_order_id = ""
  ; ev_side = "buy"
  ; ev_price = 100.0
  ; ev_qty = 2.0
  ; ev_cl_ord_id = Some "cl1"
  ; ev_reason = ""
  }
;;

let record_event_run r =
  Strategy_event_recorder.record_event r (sample_event ());
  Strategy_event_recorder.record_state r [ "after_event", Strategy_expr.V_bool true ];
  Strategy_event_recorder.end_cycle r
;;

let test_event_roundtrip () =
  let t = Strategy_equivalence.capture record_event_run in
  let path = Filename.temp_file "strategy_trace_event" ".json" in
  Strategy_trace.save path t;
  let loaded = Strategy_trace.load path in
  Alcotest.(check bool) "event round-trip equal" true (Strategy_trace.equal t loaded)
;;

let test_event_observed () =
  let t = Strategy_equivalence.capture record_event_run in
  match t with
  | [ { Strategy_trace.c_obs; _ } ] ->
    Alcotest.(check bool)
      "event captured in first cycle"
      true
      (List.exists
         (function
           | Strategy_trace.Event _ -> true
           | _ -> false)
         c_obs)
  | _ -> Alcotest.fail "expected one cycle"
;;

let replay_strategy =
  {|{"name":"r","version":1,"triggers":["book_update"],"steps":[
     {"id":"s","when":{"event":"book_update"},"then":[
       {"action":"track_buy","args":{"token":"t","price":"100.0"}}]}]}|}
;;

let replay_inputs =
  [ { Strategy_equivalence.ri_price = 100.0
    ; ri_now = 0.0
    ; ri_kind = "book_update"
    ; ri_fields = []
    }
  ; { Strategy_equivalence.ri_price = 101.0
    ; ri_now = 1.0
    ; ri_kind = "book_update"
    ; ri_fields = []
    }
  ]
;;

let mk_rt json () =
  match Strategy_file.parse_string json with
  | Error e -> failwith e
  | Ok f -> Strategy_runtime.create ~handlers:Strategy_actions_grid.handler f
;;

let replay_step rt (input : Strategy_equivalence.input) r =
  let calls =
    Strategy_runtime.run_cycle
      rt
      ~price:input.ri_price
      ~now:input.ri_now
      ~event:(Strategy_runtime.make_event input.ri_kind input.ri_fields)
  in
  Strategy_event_recorder.record_state
    r
    [ ( "actions"
      , Strategy_expr.V_string
          (String.concat
             ","
             (List.map (fun (c : Strategy_runtime.action_call) -> c.ac_action) calls)) )
    ];
  Strategy_event_recorder.end_cycle r
;;

let test_replay_equiv () =
  let reference =
    Strategy_equivalence.replay
      ~inputs:replay_inputs
      ~setup:(mk_rt replay_strategy)
      ~step:replay_step
  in
  let candidate =
    Strategy_equivalence.replay
      ~inputs:replay_inputs
      ~setup:(mk_rt replay_strategy)
      ~step:replay_step
  in
  Alcotest.(check bool)
    "same inputs, same run -> equivalent"
    true
    (Strategy_equivalence.is_equiv
       (Strategy_equivalence.compare_traces ~reference ~candidate))
;;

let test_replay_divergence () =
  let reference =
    Strategy_equivalence.replay
      ~inputs:replay_inputs
      ~setup:(mk_rt replay_strategy)
      ~step:replay_step
  in
  let empty = {|{"name":"r","version":1,"triggers":["book_update"],"steps":[]}|} in
  let result =
    Strategy_equivalence.check_replay
      ~reference
      ~inputs:replay_inputs
      ~setup:(mk_rt empty)
      ~step:replay_step
  in
  Alcotest.(check bool)
    "different run -> divergence"
    false
    (Strategy_equivalence.is_equiv result)
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
    ; "persistence", [ Alcotest.test_case "json round-trip" `Quick test_roundtrip ]
    ; ( "events"
      , [ Alcotest.test_case "event json round-trip" `Quick test_event_roundtrip
        ; Alcotest.test_case "event observed" `Quick test_event_observed
        ] )
    ; ( "replay"
      , [ Alcotest.test_case "same inputs equivalent" `Quick test_replay_equiv
        ; Alcotest.test_case "different run divergence" `Quick test_replay_divergence
        ] )
    ]
;;
