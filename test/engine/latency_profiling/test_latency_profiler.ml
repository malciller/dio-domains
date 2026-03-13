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

let () =
  run "Latency Profiler" [
    "basic", [
      test_case "basic recording" `Quick test_basic;
      test_case "distribution" `Quick test_distribution;
    ];
  ]
