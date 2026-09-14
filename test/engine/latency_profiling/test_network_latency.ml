(* Tests for the feed-latency clock-offset correction and the RFC3339 parser
   used to extract server event timestamps from feed messages. *)

let check_float = Alcotest.(check (float 0.001))
let check_us = Alcotest.(check (float 200.0))

let test_unix_of_rfc3339_fractional () =
  match Network_latency.unix_of_rfc3339 "2025-01-14T16:05:51.872012Z" with
  | Some t -> check_float "epoch seconds" 1736870751.872012 t
  | None -> Alcotest.fail "expected a parsed timestamp"
;;

let test_unix_of_rfc3339_no_fraction () =
  match Network_latency.unix_of_rfc3339 "2024-01-01T00:00:00Z" with
  | Some t -> check_float "epoch seconds" 1704067200.0 t
  | None -> Alcotest.fail "expected a parsed timestamp"
;;

let test_unix_of_rfc3339_offset () =
  (* 12:00:00+02:00 == 10:00:00Z. *)
  match Network_latency.unix_of_rfc3339 "2024-01-01T12:00:00+02:00" with
  | Some t -> check_float "timezone offset applied" 1704103200.0 t
  | None -> Alcotest.fail "expected a parsed timestamp"
;;

let test_unix_of_rfc3339_invalid () =
  Alcotest.(check bool)
    "garbage is None"
    true
    (Network_latency.unix_of_rfc3339 "not-a-time" = None)
;;

(* Offset correction: with min one-way (recv - event) = 2.0s and min ping RTT
   = 100ms, the estimated clock offset is 2.0 - 0.05 = 1.95s. A 2.0s raw
   difference then corrects to the 50ms network floor, and a 2.1s raw
   difference to 150ms. *)
let assert_feed_floor_and_excess venue =
  Network_latency.publish_all ();
  match List.assoc_opt "ws_feed" (Network_latency.snapshots venue) with
  | Some (Some snap) ->
    Alcotest.(check int) "two samples" 2 snap.Latency_profiler.samples;
    check_us "p50 is the 50ms floor" 50_000.0 snap.Latency_profiler.p50;
    check_us "p99 is 150ms" 150_000.0 snap.Latency_profiler.p99
  | _ -> Alcotest.fail "missing ws_feed snapshot"
;;

let test_offset_correction_seconds () =
  let venue = "offset-test" in
  Network_latency.record_ping_s venue 0.100;
  Network_latency.record_feed_event_s venue ~event:98.0 ~recv:100.0 ();
  Network_latency.record_feed_event_s venue ~event:97.9 ~recv:100.0 ();
  assert_feed_floor_and_excess venue
;;

let test_offset_correction_milliseconds () =
  let venue = "offset-ms-test" in
  Network_latency.record_ping_s venue 0.100;
  Network_latency.record_feed_event_ms venue ~event_ms:98_000.0 ~recv:100.0 ();
  Network_latency.record_feed_event_ms venue ~event_ms:97_900.0 ~recv:100.0 ();
  assert_feed_floor_and_excess venue
;;

let get_some = function
  | Some v -> v
  | None -> Alcotest.fail "expected a latency sample"
;;

(* A single anomalously low sample must not bias the offset forever: after the
   sliding window (300s) passes it ages out and the estimate re-anchors. *)
let test_window_eviction_adapts () =
  let venue = "window-test" in
  Network_latency.observe_rtt ~now:0.0 venue 0.100;
  (* t=0: torn-low d=0.0 -> offset -0.05, latency = 50ms floor. *)
  let l0 =
    get_some (Network_latency.corrected_one_way ~now:0.0 venue ~recv:0.0 ~event:0.0)
  in
  check_float "outlier reads the floor" 0.05 l0;
  (* t=400 > window: the t=0 outlier is evicted, min_d re-anchors to 1.0. *)
  let l1 =
    get_some
      (Network_latency.corrected_one_way ~now:400.0 venue ~recv:400.0 ~event:399.0)
  in
  check_float "stale outlier evicted, reads floor" 0.05 l1;
  Alcotest.(check bool)
    "not biased by the stale minimum"
    true
    (abs_float (l1 -. 1.05) > 0.5)
;;

(* A feed that keeps re-sending the same event timestamp while local time
   advances is stale, not enormously late: the freshness gate drops it instead
   of pinning the metric at the profiler ceiling. *)
let test_stale_feed_rejected () =
  let venue = "stale-test" in
  Network_latency.observe_rtt ~now:0.0 venue 0.100;
  let rec feed k =
    if k <= 8
    then (
      ignore
        (Network_latency.corrected_one_way
           ~now:(float_of_int k *. 10.0)
           venue
           ~recv:(float_of_int k *. 10.0)
           ~event:0.0);
      feed (k + 1))
  in
  feed 0;
  Alcotest.(check bool)
    "stale feed produces no sample"
    true
    (Network_latency.corrected_one_way ~now:90.0 venue ~recv:90.0 ~event:0.0 = None)
;;

(* Advancing server timestamps are accepted. *)
let test_live_feed_accepted () =
  let venue = "live-test" in
  Network_latency.observe_rtt ~now:0.0 venue 0.100;
  let rec feed k =
    if k <= 8
    then (
      let t = float_of_int k *. 10.0 in
      ignore (Network_latency.corrected_one_way ~now:t venue ~recv:t ~event:(t -. 0.05));
      feed (k + 1))
  in
  feed 0;
  Alcotest.(check bool)
    "live feed accepted"
    true
    (Option.is_some
       (Network_latency.corrected_one_way ~now:90.0 venue ~recv:90.0 ~event:89.95))
;;

(* The latency profilers must not saturate: a 5s sample is preserved, not
   collapsed to the old 2s ceiling. *)
let test_large_latency_not_clamped () =
  let venue = "cap-test" in
  Network_latency.record_ping_s venue 5.0;
  Network_latency.publish_all ();
  match List.assoc_opt "ws_ping" (Network_latency.snapshots venue) with
  | Some (Some snap) ->
    check_us "5s ping preserved" 5_000_000.0 snap.Latency_profiler.p99;
    Alcotest.(check int) "no overflow" 0 snap.Latency_profiler.overflow
  | _ -> Alcotest.fail "missing ws_ping snapshot"
;;

let () =
  Alcotest.run
    "network_latency"
    [ ( "rfc3339"
      , [ Alcotest.test_case "fractional seconds" `Quick test_unix_of_rfc3339_fractional
        ; Alcotest.test_case "no fraction" `Quick test_unix_of_rfc3339_no_fraction
        ; Alcotest.test_case "timezone offset" `Quick test_unix_of_rfc3339_offset
        ; Alcotest.test_case "invalid input" `Quick test_unix_of_rfc3339_invalid
        ] )
    ; ( "clock_offset"
      , [ Alcotest.test_case
            "seconds offset correction"
            `Quick
            test_offset_correction_seconds
        ; Alcotest.test_case
            "milliseconds offset correction"
            `Quick
            test_offset_correction_milliseconds
        ; Alcotest.test_case
            "sliding window evicts stale minimum"
            `Quick
            test_window_eviction_adapts
        ; Alcotest.test_case
            "stale feed rejected by freshness gate"
            `Quick
            test_stale_feed_rejected
        ; Alcotest.test_case
            "live feed accepted"
            `Quick
            test_live_feed_accepted
        ; Alcotest.test_case
            "large latency not clamped"
            `Quick
            test_large_latency_not_clamped
        ] )
    ]
;;
