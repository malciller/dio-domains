open Mtime

let section = "latency_profiler"

(* bucket resolution in microseconds *)
let bucket_us = 1

(* maximum latency tracked: 10ms *)
let max_latency_us = 100_000

type t = {
  name : string;
  buckets : int array;
  bucket_us : int;
  bucket_count : int;
  mutable samples : int;
  mutable overflow : int;
}

let create ?(bucket_us=1) ?(max_latency_us=10_000) name = 
  let count = max_latency_us / bucket_us in
  {
    name;
    buckets = Array.make count 0;
    bucket_us;
    bucket_count = count;
    samples = 0;
    overflow = 0;
  }

let record t span =
  let us = Int64.to_int (Int64.div (Span.to_uint64_ns span) 1000L) in
  let bucket_idx = us / t.bucket_us in
  if bucket_idx >= t.bucket_count then begin
    t.buckets.(t.bucket_count - 1) <- t.buckets.(t.bucket_count - 1) + 1;
    t.overflow <- t.overflow + 1
  end else begin
    t.buckets.(bucket_idx) <- t.buckets.(bucket_idx) + 1
  end;
  t.samples <- t.samples + 1

let percentile t p =
  if t.samples = 0 then 0.0
  else
    let target = int_of_float (ceil (float t.samples *. p)) in
    let cumulative = ref 0 in
    let result = ref 0 in

    for i = 0 to t.bucket_count - 1 do
      cumulative := !cumulative + t.buckets.(i);
      if !cumulative >= target && !result = 0 then
        result := i
    done;

    float (!result * t.bucket_us)

let reset t =
  Array.fill t.buckets 0 t.bucket_count 0;
  t.samples <- 0;
  t.overflow <- 0

let report ?(sample_threshold=1) t =
  if t.samples >= sample_threshold then (

    let p50 = percentile t 0.50 in
    let p90 = percentile t 0.90 in
    let p95 = percentile t 0.95 in
    let p99 = percentile t 0.99 in
    let p999 = percentile t 0.999 in

    if t.overflow > 0 then
      Logging.debug_f ~section "[%s] latency (us): p50=%.2f p90=%.2f p95=%.2f p99=%.2f p999=%.2f samples=%d overflow=%d"
        t.name p50 p90 p95 p99 p999 t.samples t.overflow
    else
      Logging.debug_f ~section "[%s] latency (us): p50=%.2f p90=%.2f p95=%.2f p99=%.2f p999=%.2f samples=%d"
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

(** Non-destructive snapshot of current latency percentiles.
    Safe to call from the dashboard without interfering with the
    engine's periodic report-and-reset cycle. Returns None if
    no samples have been recorded yet. *)
type snapshot = {
  name: string;
  p50: float;
  p90: float;
  p95: float;
  p99: float;
  p999: float;
  samples: int;
  overflow: int;
}

let snapshot (prof : t) : snapshot option =
  if prof.samples = 0 then None
  else
    let p50 = percentile prof 0.50 in
    let p90 = percentile prof 0.90 in
    let p95 = percentile prof 0.95 in
    let p99 = percentile prof 0.99 in
    let p999 = percentile prof 0.999 in
    Some { name = prof.name; p50; p90; p95; p99; p999;
           samples = prof.samples; overflow = prof.overflow }

(** Get profiler name *)
let name t = t.name

