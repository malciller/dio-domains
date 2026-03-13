open Mtime

let section = "latency_profiler"

(* bucket resolution in microseconds *)
let bucket_us = 1

(* maximum latency tracked: 10ms *)
let max_latency_us = 10_000

let bucket_count = max_latency_us / bucket_us

type t = {
  name : string;
  buckets : int array;
  mutable samples : int;
  mutable last_report_ns : int64;
}

let create name = 
  {
    name;
    buckets = Array.make bucket_count 0;
    samples = 0;
    last_report_ns = Mtime_clock.now_ns ();
  }

let record t span =
  let us = Int64.to_int (Int64.div (Span.to_uint64_ns span) 1000L) in
  let bucket =
    if us >= bucket_count then bucket_count - 1 else us
  in
  t.buckets.(bucket) <- t.buckets.(bucket) + 1;
  t.samples <- t.samples + 1

let percentile t p =
  if t.samples = 0 then 0.0
  else
    let target = int_of_float (float t.samples *. p) in
    let cumulative = ref 0 in
    let result = ref 0 in

    for i = 0 to bucket_count - 1 do
      cumulative := !cumulative + t.buckets.(i);
      if !cumulative >= target && !result = 0 then
        result := i
    done;

    float !result

let reset t =
  Array.fill t.buckets 0 bucket_count 0;
  t.samples <- 0

let report t =
  let now = Mtime_clock.now_ns () in
  let elapsed_ns = Int64.sub now t.last_report_ns in
  let elapsed_s = Int64.to_float elapsed_ns /. 1_000_000_000. in
  
  if elapsed_s > 5.0 && t.samples > 100 then (
    t.last_report_ns <- now;

    let p50 = percentile t 0.50 in
    let p90 = percentile t 0.90 in
    let p95 = percentile t 0.95 in
    let p99 = percentile t 0.99 in
    let p999 = percentile t 0.999 in

    Logging.info_f ~section "[%s] latency (us): p50=%.2f p90=%.2f p95=%.2f p99=%.2f p999=%.2f samples=%d"
      t.name p50 p90 p95 p99 p999 t.samples;

    reset t
  )

(* Helper for timing a function *)
let time_it t f =
  let start = Mtime_clock.now_ns () in
  let res = f () in
  let stop = Mtime_clock.now_ns () in
  let span = Span.of_uint64_ns (Int64.sub stop start) in
  record t span;
  res
