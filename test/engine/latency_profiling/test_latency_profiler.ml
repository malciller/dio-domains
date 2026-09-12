open Alcotest
module LP = Latency_profiler

let test_basic () =
  let t = LP.create "test" in
  (* Record 100 samples of 10us *)
  for _ = 1 to 100 do
    LP.record t (Mtime.Span.of_uint64_ns 10000L)
  done;
  let p50 = LP.percentile t 0.50 in
  let p90 = LP.percentile t 0.90 in
  let p99 = LP.percentile t 0.99 in
  Alcotest.(check (float 0.1)) "p50 is 10" 10.0 p50;
  Alcotest.(check (float 0.1)) "p90 is 10" 10.0 p90;
  Alcotest.(check (float 0.1)) "p99 is 10" 10.0 p99
;;

let test_distribution () =
  let t = LP.create "dist" in
  (* 50 samples of 10us, 50 samples of 20us *)
  for _ = 1 to 50 do
    LP.record t (Mtime.Span.of_uint64_ns 10000L)
  done;
  for _ = 1 to 50 do
    LP.record t (Mtime.Span.of_uint64_ns 20000L)
  done;
  let p50 = LP.percentile t 0.50 in
  let p90 = LP.percentile t 0.90 in
  Alcotest.(check (float 0.1)) "p50 is 10" 10.0 p50;
  Alcotest.(check (float 0.1)) "p90 is 20" 20.0 p90
;;

let test_percentile_accuracy () =
  let t = LP.create "accuracy" in
  (* 90 samples at 5us, 10 samples at 100us *)
  for _ = 1 to 90 do
    LP.record t (Mtime.Span.of_uint64_ns 5000L)
  done;
  for _ = 1 to 10 do
    LP.record t (Mtime.Span.of_uint64_ns 100000L)
  done;
  let p50 = LP.percentile t 0.50 in
  let p90 = LP.percentile t 0.90 in
  let p95 = LP.percentile t 0.95 in
  (* p50 = ceil(100*0.50) = 50th sample → bucket 5 *)
  Alcotest.(check (float 0.1)) "p50 is 5" 5.0 p50;
  (* p90 = ceil(100*0.90) = 90th sample → bucket 5 (last of the 90) *)
  Alcotest.(check (float 0.1)) "p90 is 5" 5.0 p90;
  (* p95 = ceil(100*0.95) = 95th sample → bucket 100 (in the tail) *)
  Alcotest.(check (float 0.1)) "p95 captures tail at 100" 100.0 p95
;;

let test_overflow () =
  let t = LP.create "overflow" in
  (* Record a sample well above max_latency_us (10ms = 10000us) *)
  LP.record t (Mtime.Span.of_uint64_ns 50_000_000L);
  (* 50ms *)
  LP.record t (Mtime.Span.of_uint64_ns 5000L);
  (* 5us normal *)
  Alcotest.(check int) "overflow count is 1" 1 t.overflow;
  Alcotest.(check int) "total samples is 2" 2 t.samples
;;

let test_sub_microsecond_ns () =
  let t = LP.create "subus" in
  (* 100 samples of 500ns: previously truncated to 0us, now captured in the
     nanosecond tier (500ns = 0.5us). *)
  for _ = 1 to 100 do
    LP.record t (Mtime.Span.of_uint64_ns 500L)
  done;
  let p50 = LP.percentile t 0.50 in
  let p90 = LP.percentile t 0.90 in
  let p999 = LP.percentile t 0.999 in
  Alcotest.(check (float 0.0001)) "p50 is 500ns (0.5us)" 0.5 p50;
  Alcotest.(check (float 0.0001)) "p90 is 500ns (0.5us)" 0.5 p90;
  Alcotest.(check (float 0.0001)) "p999 is 500ns (0.5us)" 0.5 p999;
  Alcotest.(check int) "sub-us samples counted" 100 t.sub_us_samples
;;

let test_sub_us_mixed_with_us () =
  let t = LP.create "mixed" in
  (* 50 samples of 500ns, 50 samples of 10us: p50 lands in the ns tier,
     p90/p95 in the us tier. *)
  for _ = 1 to 50 do
    LP.record t (Mtime.Span.of_uint64_ns 500L)
  done;
  for _ = 1 to 50 do
    LP.record t (Mtime.Span.of_uint64_ns 10000L)
  done;
  let p50 = LP.percentile t 0.50 in
  let p90 = LP.percentile t 0.90 in
  let p95 = LP.percentile t 0.95 in
  Alcotest.(check (float 0.0001)) "p50 is 500ns (0.5us)" 0.5 p50;
  Alcotest.(check (float 0.0001)) "p90 is 10us" 10.0 p90;
  Alcotest.(check (float 0.0001)) "p95 is 10us" 10.0 p95
;;

let test_sub_us_spanning_boundary () =
  let t = LP.create "boundary" in
  (* 40 at 500ns, 30 at 999ns, 30 at 2us: p50 lands at 999ns (last ns
     bucket), p90/p95 cross into the us tier at 2us. *)
  for _ = 1 to 40 do
    LP.record t (Mtime.Span.of_uint64_ns 500L)
  done;
  for _ = 1 to 30 do
    LP.record t (Mtime.Span.of_uint64_ns 999L)
  done;
  for _ = 1 to 30 do
    LP.record t (Mtime.Span.of_uint64_ns 2000L)
  done;
  let p50 = LP.percentile t 0.50 in
  let p90 = LP.percentile t 0.90 in
  let p95 = LP.percentile t 0.95 in
  Alcotest.(check (float 0.0001)) "p50 is 999ns (0.999us)" 0.999 p50;
  Alcotest.(check (float 0.0001)) "p90 is 2us" 2.0 p90;
  Alcotest.(check (float 0.0001)) "p95 is 2us" 2.0 p95
;;

let test_us_boundary_exact () =
  let t = LP.create "boundary-exact" in
  (* 1000ns is exactly 1us: routes to the microsecond tier, bucket 1. *)
  for _ = 1 to 100 do
    LP.record t (Mtime.Span.of_uint64_ns 1000L)
  done;
  Alcotest.(check (float 0.0001)) "p50 is 1us" 1.0 (LP.percentile t 0.50);
  Alcotest.(check int) "sub-us samples is 0" 0 t.sub_us_samples
;;

let test_ns_bucket_routing () =
  let t = LP.create "routing" in
  LP.record t (Mtime.Span.of_uint64_ns 300L);
  LP.record t (Mtime.Span.of_uint64_ns 700L);
  Alcotest.(check int) "300ns lands in ns bucket 300" 1 t.ns_buckets.(300);
  Alcotest.(check int) "700ns lands in ns bucket 700" 1 t.ns_buckets.(700);
  Alcotest.(check int) "sub-us samples is 2" 2 t.sub_us_samples;
  Alcotest.(check int) "us bucket 0 untouched" 0 t.buckets.(0);
  Alcotest.(check int) "total samples is 2" 2 t.samples
;;

let test_zero_ns_percentile () =
  let t = LP.create "zero" in
  (* All samples are exactly 0ns. Percentiles must stay 0.0 rather than
     drifting to the next ns bucket boundary (sentinel regression). *)
  for _ = 1 to 100 do
    LP.record t (Mtime.Span.of_uint64_ns 0L)
  done;
  let p50 = LP.percentile t 0.50 in
  let p99 = LP.percentile t 0.99 in
  Alcotest.(check (float 0.0001)) "p50 is 0" 0.0 p50;
  Alcotest.(check (float 0.0001)) "p99 is 0" 0.0 p99;
  let snap = LP.snapshot_and_reset t in
  Alcotest.(check (float 0.0001)) "snapshot p999 is 0" 0.0 snap.p999
;;

let test_record_ns () =
  let t = LP.create "ns_direct" in
  (* record_ns bypasses Mtime.Span boxing: 500ns routes to the ns tier, 2.5us
     to the coarse tier at bucket_us=1, and a backwards clock (negative) is
     clamped to 0 rather than indexing below the histogram. *)
  LP.record_ns t 500;
  LP.record_ns t 2500;
  LP.record_ns t 30000;
  LP.record_ns t (-5);
  Alcotest.(check int) "samples" 4 t.samples;
  Alcotest.(check int) "sub-us samples" 2 t.sub_us_samples;
  Alcotest.(check int) "max stays at 30us" 30000 t.max_latency_ns;
  Alcotest.(check (float 0.0001)) "p50 is the 500ns sample" 0.5 (LP.percentile t 0.50);
  Alcotest.(check (float 0.0001)) "p99 is 30us" 30.0 (LP.percentile t 0.99)
;;

let test_record_ns_matches_record () =
  let a = LP.create "span" in
  let b = LP.create "ns" in
  for i = 1 to 100 do
    let ns = i * 137 in
    LP.record a (Mtime.Span.of_uint64_ns (Int64.of_int ns));
    LP.record_ns b ns
  done;
  Alcotest.(check int) "same samples" a.samples b.samples;
  Alcotest.(check int) "same max" a.max_latency_ns b.max_latency_ns;
  Alcotest.(check (float 0.0001)) "same p99" (LP.percentile a 0.99) (LP.percentile b 0.99)
;;

let test_canary_clock_nonalloc () =
  (* The canary's whole purpose depends on a clock that does not allocate:
     an allocating clock would make the detector trigger its own minor GCs.
     Read 1000 times and assert the minor-word counter does not move. *)
  let before = Gc.minor_words () in
  let last = ref 0 in
  let backwards = ref false in
  for _ = 1 to 1000 do
    let t = Monotonic_clock.now_ns () in
    if t < !last then backwards := true;
    last := t
  done;
  let allocated = Gc.minor_words () -. before in
  Alcotest.(check bool) "monotonic never goes backwards" false !backwards;
  Alcotest.(check (float 0.001)) "clock allocates no minor words" 0.0 allocated
;;

let test_snapshot_sub_us () =
  let t = LP.create "snap" in
  (* 10 samples of 250ns + 5 samples of 4us: the published snapshot must
     carry nanosecond-resolution percentiles for the sub-us majority. *)
  for _ = 1 to 10 do
    LP.record t (Mtime.Span.of_uint64_ns 250L)
  done;
  for _ = 1 to 5 do
    LP.record t (Mtime.Span.of_uint64_ns 4000L)
  done;
  let snap = LP.snapshot_and_reset t in
  Alcotest.(check (float 0.0001)) "snapshot p50 is 250ns (0.25us)" 0.25 snap.p50;
  Alcotest.(check (float 0.0001)) "snapshot p90 is 4us" 4.0 snap.p90;
  Alcotest.(check (float 0.0001)) "snapshot p999 is 4us" 4.0 snap.p999;
  Alcotest.(check int) "snapshot sub-us samples" 10 snap.sub_us_samples;
  Alcotest.(check int) "snapshot samples" 15 snap.samples;
  (* Window reset: the ns tier must be cleared for the next window. *)
  Alcotest.(check int) "ns tier cleared after reset" 0 t.ns_buckets.(250);
  Alcotest.(check int) "sub-us counter cleared after reset" 0 t.sub_us_samples
;;

let test_coarse_bucket_low_range () =
  (* The oracle profilers use bucket_us=1000 (1ms-wide coarse buckets) to
     bound memory. Latencies in [1us, 1000us) must STILL be reported at
     microsecond resolution via the fine tier, not collapsed into coarse
     bucket 0 (which reported 0us before the fine tier existed). *)
  let t = LP.create ~bucket_us:1000 ~max_latency_us:60_000_000 "oracle-style" in
  (* 100 cache-hit analyses at 2.5us + 1 slow recompute at 5ms. *)
  for _ = 1 to 100 do
    LP.record t (Mtime.Span.of_uint64_ns 2500L)
  done;
  LP.record t (Mtime.Span.of_uint64_ns 5_000_000L);
  let p50 = LP.percentile t 0.50 in
  let p90 = LP.percentile t 0.90 in
  Alcotest.(check (float 0.0001)) "p50 is 2us (fine tier)" 2.0 p50;
  Alcotest.(check (float 0.0001)) "p90 is 2us (fine tier)" 2.0 p90;
  (* The slow sample lands in the coarse tier (5ms = 5000us >= 1000). *)
  let p999 = LP.percentile t 0.999 in
  Alcotest.(check (float 0.0001)) "p999 is 5ms" 5000.0 p999;
  let snap = LP.snapshot_and_reset t in
  Alcotest.(check (float 0.0001)) "snapshot p50 is 2us" 2.0 snap.p50;
  Alcotest.(check (float 0.0001)) "snapshot p999 is 5ms" 5000.0 snap.p999;
  Alcotest.(check int) "snapshot samples" 101 snap.samples;
  Alcotest.(check int) "snapshot sub-us samples" 0 snap.sub_us_samples
;;

let test_fine_tier_routing () =
  (* bucket_us=10: samples 1-9us route to the fine 1us tier; 10us+ to coarse. *)
  let t = LP.create ~bucket_us:10 ~max_latency_us:1000 "fine" in
  LP.record t (Mtime.Span.of_uint64_ns 5000L);
  (* 5us -> fine bucket 4 *)
  LP.record t (Mtime.Span.of_uint64_ns 9_900L);
  (* 9us -> fine bucket 8 *)
  LP.record t (Mtime.Span.of_uint64_ns 10_000L);
  (* 10us -> coarse bucket 1 *)
  LP.record t (Mtime.Span.of_uint64_ns 1000L);
  (* 1us -> fine bucket 0 *)
  Alcotest.(check int) "1us in fine bucket 0" 1 t.us_buckets.(0);
  Alcotest.(check int) "5us in fine bucket 4" 1 t.us_buckets.(4);
  Alcotest.(check int) "9us in fine bucket 8" 1 t.us_buckets.(8);
  Alcotest.(check int) "10us in coarse bucket 1" 1 t.buckets.(1);
  Alcotest.(check int) "coarse bucket 0 untouched" 0 t.buckets.(0);
  Alcotest.(check int) "total samples" 4 t.samples;
  (* p50 = ceil(4*0.5) = 2nd sample = 5us *)
  Alcotest.(check (float 0.0001)) "p50 is 5us" 5.0 (LP.percentile t 0.50);
  (* p90 = ceil(4*0.9) = 4th sample = 10us (coarse) *)
  Alcotest.(check (float 0.0001)) "p90 is 10us" 10.0 (LP.percentile t 0.90)
;;

let test_mixed_three_tiers () =
  (* One sample in each tier with bucket_us=10: ns (500ns), fine (5us),
     coarse (100us). Percentiles must resolve across all three. *)
  let t = LP.create ~bucket_us:10 ~max_latency_us:1000 "three-tier" in
  for _ = 1 to 100 do
    LP.record t (Mtime.Span.of_uint64_ns 500L)
  done;
  for _ = 1 to 100 do
    LP.record t (Mtime.Span.of_uint64_ns 5000L)
  done;
  for _ = 1 to 100 do
    LP.record t (Mtime.Span.of_uint64_ns 100_000L)
  done;
  (* 300 samples: p50 target=150 -> 5us; p99 target=297 -> 100us. *)
  Alcotest.(check (float 0.0001)) "p50 is 5us" 5.0 (LP.percentile t 0.50);
  Alcotest.(check (float 0.0001)) "p90 is 100us" 100.0 (LP.percentile t 0.90);
  Alcotest.(check (float 0.0001)) "p99 is 100us" 100.0 (LP.percentile t 0.99);
  let snap = LP.snapshot_and_reset t in
  Alcotest.(check (float 0.0001)) "snap p50 is 5us" 5.0 snap.p50;
  Alcotest.(check (float 0.0001)) "snap p999 is 100us" 100.0 snap.p999;
  Alcotest.(check int) "snap sub-us samples" 100 snap.sub_us_samples
;;

let test_spike_tracking () =
  let t = LP.create ~bucket_us:1 ~max_latency_us:10_000 "spikes" in
  (* 90 samples at 5us (under), 8 at 20us and 2 at 50us (over a 10us ceiling). *)
  for _ = 1 to 90 do
    LP.record t (Mtime.Span.of_uint64_ns 5000L)
  done;
  for _ = 1 to 8 do
    LP.record t (Mtime.Span.of_uint64_ns 20000L)
  done;
  for _ = 1 to 2 do
    LP.record t (Mtime.Span.of_uint64_ns 50000L)
  done;
  Alcotest.(check int) "count at/above 10us is 10" 10 (LP.count_above t 10.0);
  Alcotest.(check int) "count at/above 50us is 2" 2 (LP.count_above t 50.0);
  Alcotest.(check int) "count at/above 51us is 0" 0 (LP.count_above t 51.0);
  let snap = LP.snapshot_and_reset ~spike_threshold_us:10.0 t in
  Alcotest.(check int) "snapshot spike count" 10 snap.over_threshold;
  Alcotest.(check (float 0.001)) "snapshot max_us" 50.0 snap.max_us
;;

let test_spike_count_coarse_bucket () =
  (* A coarse-bucket profiler (the cycle/exec shape) must still count
     sub-bucket spikes via its fine microsecond tier. *)
  let t = LP.create ~bucket_us:1000 ~max_latency_us:2_000_000 "coarse-spikes" in
  LP.record t (Mtime.Span.of_uint64_ns 5000L);
  (* 5us, fine tier *)
  LP.record t (Mtime.Span.of_uint64_ns 500_000L);
  (* 500us, fine tier *)
  LP.record t (Mtime.Span.of_uint64_ns 1_500_000L);
  (* 1.5ms, coarse *)
  Alcotest.(check int) "three samples above 1us" 3 (LP.count_above t 1.0);
  Alcotest.(check int) "two samples above 10us" 2 (LP.count_above t 10.0);
  let snap = LP.snapshot_and_reset ~spike_threshold_us:1.0 t in
  Alcotest.(check int) "snapshot coarse spike count" 3 snap.over_threshold;
  Alcotest.(check (float 1.0)) "snapshot coarse max_us" 1500.0 snap.max_us
;;

let test_count_above_fine_and_coarse_split () =
  (* bucket_us=10: 1-9us live in the fine microsecond tier, 10us+ in the coarse
     tier. A threshold at the boundary must count only the coarse sample, and a
     threshold inside the fine tier must count the fine samples at/above it -
     this locks the start-index optimization in [count_above]. *)
  let t = LP.create ~bucket_us:10 ~max_latency_us:1000 "split" in
  LP.record t (Mtime.Span.of_uint64_ns 5_000L);
  (* 5us -> fine bucket 4 *)
  LP.record t (Mtime.Span.of_uint64_ns 9_000L);
  (* 9us -> fine bucket 8 *)
  LP.record t (Mtime.Span.of_uint64_ns 15_000L);
  (* 15us -> coarse bucket 1 *)
  Alcotest.(check int) "5us+ counts all three" 3 (LP.count_above t 5.0);
  Alcotest.(check int) "6us+ excludes the 5us sample" 2 (LP.count_above t 6.0);
  Alcotest.(check int)
    "10us boundary counts only the coarse sample"
    1
    (LP.count_above t 10.0);
  Alcotest.(check int) "16us counts nothing" 0 (LP.count_above t 16.0);
  Alcotest.(check int) "non-finite threshold counts nothing" 0 (LP.count_above t infinity)
;;

let test_snapshot_no_threshold () =
  let t = LP.create "no-threshold" in
  LP.record t (Mtime.Span.of_uint64_ns 50_000L);
  let snap = LP.snapshot_and_reset t in
  Alcotest.(check int) "no threshold means no spike count" 0 snap.over_threshold;
  Alcotest.(check (float 0.001)) "max still recorded" 50.0 snap.max_us
;;

(* Minimal snapshot builder for the spike-message tests: only the fields the
   formatter reads are interesting; the rest are fixed sentinels. *)
let snap
      ?(samples = 0)
      ?(over_threshold = 0)
      ?(max_us = 0.0)
      ?(p99 = 0.0)
      ?(p50 = 0.0)
      ?max_cause
      name
  =
  ({ LP.name
   ; p50
   ; p90 = 0.0
   ; p95 = 0.0
   ; p99
   ; p999 = 0.0
   ; samples
   ; sub_us_samples = 0
   ; overflow = 0
   ; max_us
   ; over_threshold
   ; max_cause
   ; executions = 0
   ; last_exec_time = 0.0
   ; window_start = 0.0
   ; window_end = 5.0
   }
   : LP.snapshot)
;;

let test_spike_message_silent_when_healthy () =
  let healthy = snap ~samples:1000 "healthy" in
  Alcotest.(check bool)
    "healthy window emits nothing"
    true
    (LP.spike_message
       ~key:"k"
       ~window_seconds:5.0
       ~threshold_us:10.0
       [ "STRAT", healthy; "CYCLE", healthy ]
     = None)
;;

let test_spike_message_reports_breaches () =
  let clean = snap ~samples:1000 "clean" in
  let strategy =
    snap ~samples:980 ~over_threshold:3 ~max_us:310.0 ~p99:90.0 ~p50:12.0 "STRAT"
  in
  let cycle =
    snap
      ~samples:980
      ~over_threshold:2
      ~max_us:1200.0
      ~p99:250.0
      ~p50:40.0
      ~max_cause:"ob:true ex:3"
      "CYCLE"
  in
  Alcotest.(check (option string))
    "breaches named with max, p99, p50, count and cause; clean stages omitted"
    (Some
       "[kraken/BTC/USD] latency spikes >=10.0us over 5s: STRAT max=310.0us p99=90.0us \
        p50=12.0us spikes=3/980  CYCLE max=1.20ms p99=250.0us p50=40.0us spikes=2/980\n\
       \ worst-cycle cause: ob:true ex:3")
    (LP.spike_message
       ~key:"kraken/BTC/USD"
       ~window_seconds:5.0
       ~threshold_us:10.0
       [ "OB", clean; "STRAT", strategy; "CYCLE", cycle ])
;;

let test_spike_message_without_cause () =
  let cycle = snap ~samples:10 ~over_threshold:1 ~max_us:25.0 ~p99:25.0 "CYCLE" in
  Alcotest.(check bool)
    "no cause omits the continuation line"
    true
    (match
       LP.spike_message ~key:"k" ~window_seconds:5.0 ~threshold_us:10.0 [ "CYCLE", cycle ]
     with
     | Some m -> not (String.contains m '\n')
     | None -> false)
;;

let () =
  run
    "Latency Profiler"
    [ ( "basic"
      , [ test_case "basic recording" `Quick test_basic
        ; test_case "distribution" `Quick test_distribution
        ; test_case "percentile accuracy" `Quick test_percentile_accuracy
        ; test_case "overflow tracking" `Quick test_overflow
        ] )
    ; ( "nanosecond resolution"
      , [ test_case "sub-us capture at ns level" `Quick test_sub_microsecond_ns
        ; test_case "mixed sub-us and us distribution" `Quick test_sub_us_mixed_with_us
        ; test_case
            "percentile spanning ns/us boundary"
            `Quick
            test_sub_us_spanning_boundary
        ; test_case "exact 1us routes to us tier" `Quick test_us_boundary_exact
        ; test_case "ns bucket routing" `Quick test_ns_bucket_routing
        ; test_case "zero-ns percentiles stay 0" `Quick test_zero_ns_percentile
        ; test_case "snapshot carries sub-us percentiles" `Quick test_snapshot_sub_us
        ; test_case "direct nanosecond record" `Quick test_record_ns
        ; test_case "record_ns matches record" `Quick test_record_ns_matches_record
        ] )
    ; ( "stop-the-world canary"
      , [ test_case
            "clock is monotonic and non-allocating"
            `Quick
            test_canary_clock_nonalloc
        ] )
    ; ( "fine tier for coarse-bucket profilers"
      , [ test_case
            "coarse bucket_us reports low-range us precisely"
            `Quick
            test_coarse_bucket_low_range
        ; test_case "fine tier routing" `Quick test_fine_tier_routing
        ; test_case "percentiles span all three tiers" `Quick test_mixed_three_tiers
        ] )
    ; ( "spike tracking"
      , [ test_case "threshold spike count and max" `Quick test_spike_tracking
        ; test_case "coarse-bucket spike count" `Quick test_spike_count_coarse_bucket
        ; test_case
            "fine/coarse threshold split"
            `Quick
            test_count_above_fine_and_coarse_split
        ; test_case "no threshold leaves count at zero" `Quick test_snapshot_no_threshold
        ] )
    ; ( "spike messages"
      , [ test_case "silent when healthy" `Quick test_spike_message_silent_when_healthy
        ; test_case "reports breaching stages" `Quick test_spike_message_reports_breaches
        ; test_case "omits cause line when absent" `Quick test_spike_message_without_cause
        ] )
    ]
;;
