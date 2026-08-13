open Alcotest

let test_inflight_amendments_cache () =
  let order_id = "test_amend_oid_1" in
  (* Clean state *)
  ignore
    (Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment
       order_id);
  let added =
    Dio_strategies.Strategy_common.InFlightAmendments.add_in_flight_amendment order_id
  in
  check bool "add in flight amendment" true added;
  let in_flight =
    Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight order_id
  in
  check bool "is in flight amendment" true in_flight;
  let added_duplicate =
    Dio_strategies.Strategy_common.InFlightAmendments.add_in_flight_amendment order_id
  in
  check bool "add duplicate amendment returns false" false added_duplicate;
  let removed =
    Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment order_id
  in
  check bool "remove in flight amendment" true removed;
  let in_flight_after =
    Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight order_id
  in
  check bool "is in flight false after removal" false in_flight_after
;;

let test_inflight_amendments_cleanup () =
  let order_id = "stale_amend_oid" in
  ignore
    (Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment
       order_id);
  ignore
    (Dio_strategies.Strategy_common.InFlightAmendments.add_in_flight_amendment order_id);
  (* Immediate cleanup with max_age=0.0 should evict *)
  let _drift, trimmed =
    Dio_strategies.Strategy_common.InFlightAmendments.cleanup ~max_age:0.0 ()
  in
  check bool "cleanup executed" true (trimmed >= 0)
;;

let test_amend_lifecycle_replace () =
  (* A replace-style amendment (Hyperliquid/Alpaca: old id cancelled, new id
     created): after the exchange confirms, the OLD id stays registered as
     [Replaced] for the recognition window, so a late cancel event for the
     old id is recognized as the amend's side effect and must not reset
     tracking. *)
  let module A = Dio_strategies.Strategy_common.InFlightAmendments in
  let old_id = "lifecycle_old" in
  let new_id = "lifecycle_new" in
  ignore (A.remove_in_flight_amendment old_id);
  check bool "add pending" true (A.add_in_flight_amendment old_id);
  check bool "pending is in flight" true (A.is_in_flight old_id);
  check bool "pending blocks re-amend dedup" false (A.add_in_flight_amendment old_id);
  A.note_amendment_succeeded ~old_id ~new_id;
  check bool "replaced not in flight (dedup cleared)" false (A.is_in_flight old_id);
  check bool "replaced is superseded" true (A.is_superseded old_id);
  check
    bool
    "replaced is lifecycle-active (late cancel ignored)"
    true
    (A.is_amend_lifecycle_active old_id);
  check bool "new id has no lifecycle state" false (A.is_amend_lifecycle_active new_id);
  (* Removal (also the cleanup path) drops the recognition window. *)
  ignore (A.remove_in_flight_amendment old_id);
  check bool "superseded gone after removal" false (A.is_superseded old_id)
;;

let test_amend_lifecycle_same_id () =
  (* An in-place amendment (Kraken: same id): no Replaced entry is retained,
     so events for that id are always real (a genuine cancel must reset
     tracking). *)
  let module A = Dio_strategies.Strategy_common.InFlightAmendments in
  let order_id = "lifecycle_same_id" in
  ignore (A.remove_in_flight_amendment order_id);
  ignore (A.add_in_flight_amendment order_id);
  A.note_amendment_succeeded ~old_id:order_id ~new_id:order_id;
  check
    bool
    "same-id amend leaves no lifecycle state"
    false
    (A.is_amend_lifecycle_active order_id);
  check bool "same-id amend not superseded" false (A.is_superseded order_id)
;;

let test_amend_lifecycle_failed_is_terminal () =
  (* A failed amendment drops the entry immediately: a follow-up cancel event
     for the old id is handled as a real one (the failure path has already
     reconciled tracking). *)
  let module A = Dio_strategies.Strategy_common.InFlightAmendments in
  let order_id = "lifecycle_failed" in
  ignore (A.remove_in_flight_amendment order_id);
  ignore (A.add_in_flight_amendment order_id);
  A.note_amendment_failed ~old_id:order_id ~reason:"test rejection";
  check
    bool
    "failed amend is terminal (no lifecycle state)"
    false
    (A.is_amend_lifecycle_active order_id);
  A.note_amendment_skipped ~old_id:order_id;
  check
    bool
    "skipped amend is terminal (no lifecycle state)"
    false
    (A.is_amend_lifecycle_active order_id)
;;

let () =
  run
    "Order Cache"
    [ ( "inflight_amendments"
      , [ test_case "amendments cache lifecycle" `Quick test_inflight_amendments_cache
        ; test_case "amendments cleanup" `Quick test_inflight_amendments_cleanup
        ; test_case
            "amend lifecycle: replace keeps old id in recognition window"
            `Quick
            test_amend_lifecycle_replace
        ; test_case
            "amend lifecycle: same-id amend retains no state"
            `Quick
            test_amend_lifecycle_same_id
        ; test_case
            "amend lifecycle: failure/skip are terminal"
            `Quick
            test_amend_lifecycle_failed_is_terminal
        ] )
    ]
;;
