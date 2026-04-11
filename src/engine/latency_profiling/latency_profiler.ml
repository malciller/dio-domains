(** Histogram-based latency profiler.
    Records latency samples into fixed-width buckets and computes
    percentile distributions (p50, p90, p95, p99, p999).
    All latency values are in microseconds. *)

open Mtime

let section = "latency_profiler"

(* Default bucket width in microseconds. *)
let bucket_us = 1

(* Upper bound of tracked latency range in microseconds. *)
let max_latency_us = 100_000

(** Profiler state. Contains a fixed-size histogram array and
    running counters for total samples and overflow events. *)
type t = {
  name : string;          (* Identifier for this profiler instance. *)
  buckets : int array;    (* Histogram bin counts. *)
  bucket_us : int;        (* Width of each bucket in microseconds. *)
  bucket_count : int;     (* Total number of histogram buckets. *)
  mutable samples : int;  (* Total recorded samples. *)
  mutable overflow : int; (* Samples exceeding the histogram range. *)
}

(** [create ?bucket_us ?max_latency_us name] allocates a profiler with
    [max_latency_us / bucket_us] histogram buckets, all initialized to zero. *)
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

let one_k_l = 1000L

(** [record t span] converts [span] from nanoseconds to microseconds,
    maps it to the corresponding histogram bucket, and increments both
    the bucket count and total sample count. Samples that exceed the
    histogram range are clamped to the last bucket and counted as overflow. *)
let[@inline] record t span =
  let us = Int64.to_int (Int64.div (Span.to_uint64_ns span) one_k_l) in
  let bucket_idx = us / t.bucket_us in
  if bucket_idx >= t.bucket_count then begin
    t.buckets.(t.bucket_count - 1) <- t.buckets.(t.bucket_count - 1) + 1;
    t.overflow <- t.overflow + 1
  end else begin
    t.buckets.(bucket_idx) <- t.buckets.(bucket_idx) + 1
  end;
  t.samples <- t.samples + 1

(** [percentile t p] computes the p-th percentile (0.0 to 1.0) from the
    histogram by performing a cumulative scan over buckets. Returns the
    bucket boundary in microseconds. Returns 0.0 when no samples exist.
    Uses early exit to avoid scanning the full bucket array once the
    target cumulative count is reached — critical for large histograms
    (e.g. the cycle profiler with 100,000 buckets). *)
let percentile t p =
  if t.samples = 0 then 0.0
  else
    let target = int_of_float (ceil (float t.samples *. p)) in
    let cumulative = ref 0 in
    let i = ref 0 in
    while !i < t.bucket_count && !cumulative < target do
      cumulative := !cumulative + t.buckets.(!i);
      if !cumulative < target then incr i
    done;
    float (!i * t.bucket_us)

(** [reset t] zeroes all histogram buckets and resets sample/overflow counters. *)
let reset t =
  Array.fill t.buckets 0 t.bucket_count 0;
  t.samples <- 0;
  t.overflow <- 0

(** [report ?sample_threshold t] logs the current percentile distribution
    if at least [sample_threshold] samples have been collected. Includes
    overflow count in the log output when overflow is nonzero. Resets the
    profiler state after reporting. *)
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

(** [time_it t f] measures the wall-clock execution time of [f ()],
    records the resulting span in the profiler, and returns the result of [f]. *)
let time_it t f =
  let start = Mtime_clock.now_ns () in
  let res = f () in
  let stop = Mtime_clock.now_ns () in
  let span = Span.of_uint64_ns (Int64.sub stop start) in
  record t span;
  res

(** Read-only snapshot of percentile data. Computed without mutating
    the profiler state, so it does not interfere with the periodic
    report-and-reset cycle. *)
type snapshot = {
  name: string;     (* Profiler instance name. *)
  p50: float;       (* 50th percentile in microseconds. *)
  p90: float;       (* 90th percentile in microseconds. *)
  p95: float;       (* 95th percentile in microseconds. *)
  p99: float;       (* 99th percentile in microseconds. *)
  p999: float;      (* 99.9th percentile in microseconds. *)
  samples: int;     (* Total samples at snapshot time. *)
  overflow: int;    (* Overflow count at snapshot time. *)
}

(** [snapshot prof] returns [Some snapshot] with current percentile values,
    or [None] if no samples have been recorded. Does not reset the profiler. *)
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

(** [name t] returns the profiler instance identifier. *)
let name t = t.name

