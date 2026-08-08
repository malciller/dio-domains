(** Histogram-based latency profiler.
    Records latency samples into fixed-width buckets and computes
    percentile distributions (p50, p90, p95, p99, p999).
    All latency values are in microseconds.

    Metrics are accumulated in a window and published atomically via
    [snapshot_and_reset] (typically once per window from the recording
    domain). Readers consume the immutable published snapshot through
    [published_snapshot] — a lock-free [Atomic.get] — which removes the
    torn-read race between the histogram writer and the dashboard reader. *)

open Mtime

let section = "latency_profiler"

(* Default bucket width in microseconds. *)
let bucket_us = 1

(* Upper bound of tracked latency range in microseconds. *)
let max_latency_us = 100_000

(** Read-only snapshot of a completed measurement window. Immutable once
    published; readers never observe a partially-updated histogram. *)
type snapshot =
  { name : string (* Identifier for this profiler instance. *)
  ; p50 : float (* 50th percentile in microseconds. *)
  ; p90 : float (* 90th percentile in microseconds. *)
  ; p95 : float (* 95th percentile in microseconds. *)
  ; p99 : float (* 99th percentile in microseconds. *)
  ; p999 : float (* 99.9th percentile in microseconds. *)
  ; samples : int (* Total samples in this window. *)
  ; overflow : int (* Overflow count in this window. *)
  ; max_cause : string option (* Cause of the max latency in this window. *)
  ; executions : int (* Activity ticks recorded in this window. *)
  ; last_exec_time : float (* Unix time of the last activity tick. *)
  ; window_start : float (* Unix time the window began. *)
  ; window_end : float (* Unix time the window was published. *)
  }

(** Profiler state. Contains a fixed-size histogram array, running counters
    for total samples and overflow events, per-window activity counters, and
    the immutable snapshot of the most recently completed window. *)
type t =
  { name : string (* Identifier for this profiler instance. *)
  ; buckets : int array (* Histogram bin counts. *)
  ; bucket_us : int (* Width of each bucket in microseconds. *)
  ; bucket_count : int (* Total number of histogram buckets. *)
  ; mutable samples : int (* Total recorded samples. *)
  ; mutable overflow : int (* Samples exceeding the histogram range. *)
  ; mutable max_latency_us : int
  ; mutable max_cause : string option
  ; mutable executions : int (* Activity ticks in the current window. *)
  ; mutable last_exec_time : float (* Unix time of the last activity tick. *)
  ; mutable window_start : float (* Unix time the current window began. *)
  ; published : snapshot option Atomic.t (* Last completed window snapshot. *)
  ; mutex : Mutex.t (* Guards snapshot_and_reset against reset races. *)
  }

(** [create ?bucket_us ?max_latency_us name] allocates a profiler with
    [max_latency_us / bucket_us] histogram buckets, all initialized to zero. *)
let create ?(bucket_us = 1) ?(max_latency_us = 10_000) name =
  let count = max_latency_us / bucket_us in
  { name
  ; buckets = Array.make count 0
  ; bucket_us
  ; bucket_count = count
  ; samples = 0
  ; overflow = 0
  ; max_latency_us = 0
  ; max_cause = None
  ; executions = 0
  ; last_exec_time = 0.0
  ; window_start = Unix.gettimeofday ()
  ; published = Atomic.make None
  ; mutex = Mutex.create ()
  }
;;

let one_k_l = 1000L

(** [record t span] converts [span] from nanoseconds to microseconds,
    maps it to the corresponding histogram bucket, and increments both
    the bucket count and total sample count. Samples that exceed the
    histogram range are clamped to the last bucket and counted as overflow. *)
let[@inline] record t span =
  let us = Int64.to_int (Int64.div (Span.to_uint64_ns span) one_k_l) in
  let bucket_idx = us / t.bucket_us in
  if bucket_idx >= t.bucket_count
  then (
    t.buckets.(t.bucket_count - 1) <- t.buckets.(t.bucket_count - 1) + 1;
    t.overflow <- t.overflow + 1)
  else t.buckets.(bucket_idx) <- t.buckets.(bucket_idx) + 1;
  t.samples <- t.samples + 1;
  if us > t.max_latency_us
  then (
    t.max_latency_us <- us;
    t.max_cause <- None)
;;

(** [record_with_cause t span cause_thunk] is like [record] but if the span
    establishes a new maximum latency, it evaluates [cause_thunk ()] and
    records the result as the cause. *)
let[@inline] record_with_cause t span cause_thunk =
  let us = Int64.to_int (Int64.div (Span.to_uint64_ns span) one_k_l) in
  let bucket_idx = us / t.bucket_us in
  if bucket_idx >= t.bucket_count
  then (
    t.buckets.(t.bucket_count - 1) <- t.buckets.(t.bucket_count - 1) + 1;
    t.overflow <- t.overflow + 1)
  else t.buckets.(bucket_idx) <- t.buckets.(bucket_idx) + 1;
  t.samples <- t.samples + 1;
  if us > t.max_latency_us
  then (
    t.max_latency_us <- us;
    t.max_cause <- Some (cause_thunk ()))
;;

(** [tick_exec t ~now] records one activity event (e.g. a strategy execution)
    at Unix time [now]. The count and timestamp appear in the next published
    snapshot so consumers can derive an executions-per-second rate and the
    time of last activity even when a window has zero latency samples. *)
let tick_exec t ~now =
  t.executions <- t.executions + 1;
  t.last_exec_time <- now
;;

(** [percentile t p] computes the p-th percentile (0.0 to 1.0) from the
    histogram by performing a cumulative scan over buckets. Returns the
    bucket boundary in microseconds. Returns 0.0 when no samples exist.
    Uses early exit to avoid scanning the full bucket array once the
    target cumulative count is reached — critical for large histograms
    (e.g. the cycle profiler with 100,000 buckets). *)
let percentile t p =
  if t.samples = 0
  then 0.0
  else (
    let target = int_of_float (ceil (float t.samples *. p)) in
    let cumulative = ref 0 in
    let i = ref 0 in
    while !i < t.bucket_count && !cumulative < target do
      cumulative := !cumulative + t.buckets.(!i);
      if !cumulative < target then incr i
    done;
    float (!i * t.bucket_us))
;;

(** [reset t] zeroes all histogram buckets and resets sample/overflow and
    activity counters. Does not touch [window_start]; callers that advance
    the window must set it explicitly. *)
let reset t =
  Array.fill t.buckets 0 t.bucket_count 0;
  t.samples <- 0;
  t.overflow <- 0;
  t.max_latency_us <- 0;
  t.max_cause <- None;
  t.executions <- 0;
  t.last_exec_time <- 0.0
;;

(** [snapshot_and_reset t] computes the percentile distribution of the
    current window, publishes it as an immutable snapshot (replacing the
    previous window's snapshot in the Atomic cell), zeroes the histogram,
    and starts a new window. Always publishes a snapshot, even when the
    window had zero samples, so consumers can distinguish "idle" from
    "no data". Returns the published snapshot. *)
let snapshot_and_reset t =
  let now = Unix.gettimeofday () in
  Mutex.lock t.mutex;
  let window_start = t.window_start in
  let snap =
    if t.samples = 0
    then
      { name = t.name
      ; p50 = 0.0
      ; p90 = 0.0
      ; p95 = 0.0
      ; p99 = 0.0
      ; p999 = 0.0
      ; samples = 0
      ; overflow = 0
      ; max_cause = None
      ; executions = t.executions
      ; last_exec_time = t.last_exec_time
      ; window_start
      ; window_end = now
      }
    else
      { name = t.name
      ; p50 = percentile t 0.50
      ; p90 = percentile t 0.90
      ; p95 = percentile t 0.95
      ; p99 = percentile t 0.99
      ; p999 = percentile t 0.999
      ; samples = t.samples
      ; overflow = t.overflow
      ; max_cause = t.max_cause
      ; executions = t.executions
      ; last_exec_time = t.last_exec_time
      ; window_start
      ; window_end = now
      }
  in
  Atomic.set t.published (Some snap);
  reset t;
  t.window_start <- now;
  Mutex.unlock t.mutex;
  snap
;;

(** [published_snapshot t] returns the snapshot of the most recently completed
    window. [None] before the first window is published. Lock-free: reads the
    Atomic cell only, never the live histogram. *)
let published_snapshot t = Atomic.get t.published

(** [report ?sample_threshold t] logs the current window's percentile
    distribution if at least [sample_threshold] samples have been collected,
    then advances the window (publishing + resetting). *)
let report ?(sample_threshold = 1) t =
  if t.samples >= sample_threshold
  then (
    let snap = snapshot_and_reset t in
    Logging.info_f
      ~section
      "Latency report [%s]: samples=%d p50=%.1fus p90=%.1fus p95=%.1fus p99=%.1fus \
       p999=%.1fus overflow=%d execs=%d"
      snap.name
      snap.samples
      snap.p50
      snap.p90
      snap.p95
      snap.p99
      snap.p999
      snap.overflow
      snap.executions)
;;

(** [time_it t f] measures the wall-clock execution time of [f ()],
    records the resulting span in the profiler, and returns the result of [f]. *)
let time_it t f =
  let start = Mtime_clock.now_ns () in
  let res = f () in
  let stop = Mtime_clock.now_ns () in
  let span = Span.of_uint64_ns (Int64.sub stop start) in
  record t span;
  res
;;

(** [snapshot prof] returns [Some snapshot] with the live percentile values,
    or [None] if no samples have been recorded. Does not reset the profiler.
    Intended for non-windowed consumers that need a quick read of the
    currently accumulating histogram. *)
let snapshot (prof : t) : snapshot option =
  if prof.samples = 0
  then None
  else (
    let p50 = percentile prof 0.50 in
    let p90 = percentile prof 0.90 in
    let p95 = percentile prof 0.95 in
    let p99 = percentile prof 0.99 in
    let p999 = percentile prof 0.999 in
    Some
      { name = prof.name
      ; p50
      ; p90
      ; p95
      ; p99
      ; p999
      ; samples = prof.samples
      ; overflow = prof.overflow
      ; max_cause = prof.max_cause
      ; executions = prof.executions
      ; last_exec_time = prof.last_exec_time
      ; window_start = prof.window_start
      ; window_end = Unix.gettimeofday ()
      })
;;

(** [name t] returns the profiler instance identifier. *)
let name t = t.name
