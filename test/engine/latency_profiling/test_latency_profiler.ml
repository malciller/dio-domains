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

let test_overflow () =
  let t = LP.create "overflow" in
  (* Record a sample well above max_latency_us (10ms = 10000us) *)
  LP.record t (Mtime.Span.of_uint64_ns 50_000_000L);  (* 50ms *)
  LP.record t (Mtime.Span.of_uint64_ns 5000L);         (* 5us normal *)

  Alcotest.(check int) "overflow count is 1" 1 t.overflow;
  Alcotest.(check int) "total samples is 2" 2 t.samples

let () =
  run "Latency Profiler" [
    "basic", [
      test_case "basic recording" `Quick test_basic;
      test_case "distribution" `Quick test_distribution;
      test_case "percentile accuracy" `Quick test_percentile_accuracy;
      test_case "overflow tracking" `Quick test_overflow;
    ];
  ]
