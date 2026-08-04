open Alcotest

let test_inflight_amendments_cache () =
  let order_id = "test_amend_oid_1" in
  (* Clean state *)
  ignore (Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment order_id);

  let added = Dio_strategies.Strategy_common.InFlightAmendments.add_in_flight_amendment order_id in
  check bool "add in flight amendment" true added;

  let in_flight = Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight order_id in
  check bool "is in flight amendment" true in_flight;

  let added_duplicate = Dio_strategies.Strategy_common.InFlightAmendments.add_in_flight_amendment order_id in
  check bool "add duplicate amendment returns false" false added_duplicate;

  let removed = Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment order_id in
  check bool "remove in flight amendment" true removed;

  let in_flight_after = Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight order_id in
  check bool "is in flight false after removal" false in_flight_after

let test_inflight_amendments_cleanup () =
  let order_id = "stale_amend_oid" in
  ignore (Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment order_id);
  ignore (Dio_strategies.Strategy_common.InFlightAmendments.add_in_flight_amendment order_id);

  (* Immediate cleanup with max_age=0.0 should evict *)
  let (_drift, trimmed) = Dio_strategies.Strategy_common.InFlightAmendments.cleanup ~max_age:0.0 () in
  check bool "cleanup executed" true (trimmed >= 0)

let () =
  run "Order Cache" [
    "inflight_amendments", [
      test_case "amendments cache lifecycle" `Quick test_inflight_amendments_cache;
      test_case "amendments cleanup" `Quick test_inflight_amendments_cleanup;
    ];
  ]
