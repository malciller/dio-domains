(** Histogram-based latency profiler.
    Records latency samples into fixed-width buckets and computes
    percentile distributions (p50, p90, p95, p99, p999).

    Three resolution tiers, so low-end precision is INDEPENDENT of the
    (memory-bounded) coarse bucket width:
    - samples below 1us are captured in a nanosecond tier (1ns buckets);
    - samples from 1us up to the coarse bucket width are captured in a fine
      microsecond tier (1us buckets);
    - samples at/above the coarse bucket width use the wide buckets as before.
    Without the fine tier, a profiler configured with wide buckets (e.g.
    bucket_us=1000 for the oracle) would collapse every latency between 1us
    and 999us into bucket 0 and report 0us. Reported percentile values are
    in microseconds as floats — a sub-microsecond percentile is a fraction
    (e.g. 0.5 = 500ns); consumers that need nanosecond display scale by 1000.

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

(* Nanosecond tier for sub-microsecond samples: one bucket per nanosecond.
   The sub-us range is inherently 0..999ns, so a fixed 1000-bucket tier fully
   covers it with 1ns resolution (no division needed on the hot path). *)
let ns_bucket_count = 1000

(** Read-only snapshot of a completed measurement window. Immutable once
    published; readers never observe a partially-updated histogram. *)
type snapshot =
  { name : string (* Identifier for this profiler instance. *)
  ; p50 : float (* 50th percentile in microseconds (fractional when < 1us). *)
  ; p90 : float (* 90th percentile in microseconds. *)
  ; p95 : float (* 95th percentile in microseconds. *)
  ; p99 : float (* 99th percentile in microseconds. *)
  ; p999 : float (* 99.9th percentile in microseconds. *)
  ; samples : int (* Total samples in this window. *)
  ; sub_us_samples : int (* Samples below 1us, captured at ns resolution. *)
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
  ; buckets : int array (* Coarse histogram bins (>= bucket_us us). *)
  ; us_buckets : int array (* Fine bins: 1us each, for [1, bucket_us) us. *)
  ; ns_buckets : int array (* Sub-us histogram bin counts (1ns buckets). *)
  ; bucket_us : int (* Width of each coarse bucket in microseconds. *)
  ; bucket_count : int (* Total number of coarse histogram buckets. *)
  ; us_bucket_count : int (* Fine microsecond bucket count. *)
  ; mutable samples : int (* Total recorded samples. *)
  ; mutable sub_us_samples : int (* Recorded sub-microsecond samples. *)
  ; mutable overflow : int (* Samples exceeding the histogram range. *)
  ; mutable max_latency_ns : int (* Largest recorded latency, in nanoseconds. *)
  ; mutable max_cause : string option
  ; mutable executions : int (* Activity ticks in the current window. *)
  ; mutable last_exec_time : float (* Unix time of the last activity tick. *)
  ; mutable window_start : float (* Unix time the current window began. *)
  ; published : snapshot option Atomic.t (* Last completed window snapshot. *)
  ; mutex : Mutex.t (* Guards snapshot_and_reset against reset races. *)
  }

(** [create ?bucket_us ?max_latency_us name] allocates a profiler with
    [max_latency_us / bucket_us] coarse buckets plus the fine microsecond
    tier ([bucket_us - 1] one-us buckets) and the nanosecond tier for
    sub-microsecond samples, all initialized to zero. *)
let create ?(bucket_us = 1) ?(max_latency_us = 10_000) name =
  let count = max_latency_us / bucket_us in
  let us_count = max 0 (bucket_us - 1) in
  { name
  ; buckets = Array.make count 0
  ; us_buckets = Array.make us_count 0
  ; ns_buckets = Array.make ns_bucket_count 0
  ; bucket_us
  ; bucket_count = count
  ; us_bucket_count = us_count
  ; samples = 0
  ; sub_us_samples = 0
  ; overflow = 0
  ; max_latency_ns = 0
  ; max_cause = None
  ; executions = 0
  ; last_exec_time = 0.0
  ; window_start = Unix.gettimeofday ()
  ; published = Atomic.make None
  ; mutex = Mutex.create ()
  }
;;

(** [record t span] converts [span] to nanoseconds, maps it to the
    corresponding histogram bucket, and increments both the bucket count and
    total sample count. Sub-microsecond samples are bucketed in the dedicated
    nanosecond tier (1ns resolution); samples in [1us, bucket_us) use the
    fine one-microsecond tier; samples at/above [bucket_us] use the coarse
    buckets as before. Samples that exceed the histogram range are clamped to
    the last coarse bucket and counted as overflow. *)
let[@inline] record t span =
  let ns = Int64.to_int (Span.to_uint64_ns span) in
  if ns < 1000
  then (
    (* Sub-microsecond: capture at nanosecond level (bucket index = ns). *)
    t.ns_buckets.(ns) <- t.ns_buckets.(ns) + 1;
    t.sub_us_samples <- t.sub_us_samples + 1)
  else (
    let us = ns / 1000 in
    if us < t.bucket_us
    then
      (* Fine tier: exact microsecond resolution (bucket i holds (i+1)us),
         independent of the coarse bucket width. *)
      t.us_buckets.(us - 1) <- t.us_buckets.(us - 1) + 1
    else (
      let bucket_idx = us / t.bucket_us in
      if bucket_idx >= t.bucket_count
      then (
        t.buckets.(t.bucket_count - 1) <- t.buckets.(t.bucket_count - 1) + 1;
        t.overflow <- t.overflow + 1)
      else t.buckets.(bucket_idx) <- t.buckets.(bucket_idx) + 1));
  t.samples <- t.samples + 1;
  if ns > t.max_latency_ns
  then (
    t.max_latency_ns <- ns;
    t.max_cause <- None)
;;

(** [record_with_cause t span cause_thunk] is like [record] but if the span
    establishes a new maximum latency, it evaluates [cause_thunk ()] and
    records the result as the cause. *)
let[@inline] record_with_cause t span cause_thunk =
  let ns = Int64.to_int (Span.to_uint64_ns span) in
  if ns < 1000
  then (
    t.ns_buckets.(ns) <- t.ns_buckets.(ns) + 1;
    t.sub_us_samples <- t.sub_us_samples + 1)
  else (
    let us = ns / 1000 in
    if us < t.bucket_us
    then t.us_buckets.(us - 1) <- t.us_buckets.(us - 1) + 1
    else (
      let bucket_idx = us / t.bucket_us in
      if bucket_idx >= t.bucket_count
      then (
        t.buckets.(t.bucket_count - 1) <- t.buckets.(t.bucket_count - 1) + 1;
        t.overflow <- t.overflow + 1)
      else t.buckets.(bucket_idx) <- t.buckets.(bucket_idx) + 1));
  t.samples <- t.samples + 1;
  if ns > t.max_latency_ns
  then (
    t.max_latency_ns <- ns;
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
    histogram by performing a cumulative scan over the buckets. Returns the
    bucket boundary in microseconds — a sub-microsecond result is a fraction
    (e.g. 0.5 = 500ns). Returns 0.0 when no samples exist.
    The sub-microsecond nanosecond tier is scanned first (0..999ns), then the
    fine one-microsecond tier ([1, bucket_us) us), then the coarse buckets,
    so percentiles below the coarse bucket width keep microsecond/nanosecond
    resolution regardless of [bucket_us]. Uses early exit to avoid scanning
    the full bucket array once the target cumulative count is reached —
    critical for large histograms (e.g. the cycle profiler with 100,000
    buckets). *)
let percentile t p =
  if t.samples = 0
  then 0.0
  else (
    let target = int_of_float (ceil (float t.samples *. p)) in
    let cumulative = ref 0 in
    (* Nanosecond tier: buckets 0..999 cover 0..999ns. *)
    let i = ref 0 in
    while !i < ns_bucket_count && !cumulative < target do
      cumulative := !cumulative + t.ns_buckets.(!i);
      if !cumulative < target then incr i
    done;
    if !i < ns_bucket_count
    then float !i /. 1000.0
    else (
      (* Fine microsecond tier: bucket j holds (j+1)us. *)
      let j = ref 0 in
      while !j < t.us_bucket_count && !cumulative < target do
        cumulative := !cumulative + t.us_buckets.(!j);
        if !cumulative < target then incr j
      done;
      if !j < t.us_bucket_count
      then float (!j + 1)
      else (
        (* Coarse microsecond tier. *)
        let k = ref 0 in
        while !k < t.bucket_count && !cumulative < target do
          cumulative := !cumulative + t.buckets.(!k);
          if !cumulative < target then incr k
        done;
        float (!k * t.bucket_us))))
;;

(** [percentiles5 t] computes p50/p90/p95/p99/p999 in a SINGLE cumulative pass
    over the histogram instead of five independent scans (M7: the oracle
    profilers are 60k–100k buckets, so five scans cost ~0.5–5ms per window).
    The sub-microsecond nanosecond tier is scanned first, then the fine
    one-microsecond tier, then the coarse buckets; each target is captured
    the moment its cumulative count is crossed. Sub-microsecond percentiles
    are fractions of a microsecond (e.g. 0.5 = 500ns). *)
let percentiles5 t =
  if t.samples = 0
  then 0.0, 0.0, 0.0, 0.0, 0.0
  else (
    let fracs = [| 0.50; 0.90; 0.95; 0.99; 0.999 |] in
    (* -1.0 marks "not yet captured": a real percentile can legitimately be
       0.0 (all samples sub-microsecond), which must not re-trigger capture. *)
    let vals = [| -1.0; -1.0; -1.0; -1.0; -1.0 |] in
    let cumulative = ref 0 in
    let remaining = ref 5 in
    let capture v =
      if !remaining > 0
      then
        for k = 0 to 4 do
          if vals.(k) < 0.0 && float !cumulative >= ceil (float t.samples *. fracs.(k))
          then (
            vals.(k) <- v;
            decr remaining)
        done
    in
    (* Nanosecond tier (0..999ns → 0.0..0.999 microseconds). *)
    let i = ref 0 in
    while !i < ns_bucket_count && !remaining > 0 do
      cumulative := !cumulative + t.ns_buckets.(!i);
      capture (float !i /. 1000.0);
      incr i
    done;
    (* Fine microsecond tier ((j+1)us, 1us resolution). *)
    let j = ref 0 in
    while !j < t.us_bucket_count && !remaining > 0 do
      cumulative := !cumulative + t.us_buckets.(!j);
      capture (float (!j + 1));
      incr j
    done;
    (* Coarse microsecond tier. *)
    let k = ref 0 in
    while !k < t.bucket_count && !remaining > 0 do
      cumulative := !cumulative + t.buckets.(!k);
      capture (float (!k * t.bucket_us));
      incr k
    done;
    vals.(0), vals.(1), vals.(2), vals.(3), vals.(4))
;;

(** [reset t] zeroes all histogram buckets (all three tiers) and resets
    sample/overflow and activity counters. Does not touch [window_start];
    callers that advance the window must set it explicitly. *)
let reset t =
  Array.fill t.buckets 0 t.bucket_count 0;
  Array.fill t.us_buckets 0 t.us_bucket_count 0;
  Array.fill t.ns_buckets 0 ns_bucket_count 0;
  t.samples <- 0;
  t.sub_us_samples <- 0;
  t.overflow <- 0;
  t.max_latency_ns <- 0;
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
      ; sub_us_samples = 0
      ; overflow = 0
      ; max_cause = None
      ; executions = t.executions
      ; last_exec_time = t.last_exec_time
      ; window_start
      ; window_end = now
      }
    else (
      let p50, p90, p95, p99, p999 = percentiles5 t in
      { name = t.name
      ; p50
      ; p90
      ; p95
      ; p99
      ; p999
      ; samples = t.samples
      ; sub_us_samples = t.sub_us_samples
      ; overflow = t.overflow
      ; max_cause = t.max_cause
      ; executions = t.executions
      ; last_exec_time = t.last_exec_time
      ; window_start
      ; window_end = now
      })
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

(** [format_us f] renders a microsecond value, switching to nanoseconds when
    the value is sub-microsecond so log output preserves nanosecond
    resolution (e.g. 500ns instead of 0.5us). *)
let format_us f =
  if f < 1.0 then Printf.sprintf "%.0fns" (f *. 1000.0) else Printf.sprintf "%.1fus" f
;;

(** [report ?sample_threshold t] logs the current window's percentile
    distribution if at least [sample_threshold] samples have been collected,
    then advances the window (publishing + resetting). *)
let report ?(sample_threshold = 1) t =
  if t.samples >= sample_threshold
  then (
    let snap = snapshot_and_reset t in
    Logging.info_f
      ~section
      "Latency report [%s]: samples=%d (sub-us=%d) p50=%s p90=%s p95=%s p99=%s p999=%s \
       overflow=%d execs=%d"
      snap.name
      snap.samples
      snap.sub_us_samples
      (format_us snap.p50)
      (format_us snap.p90)
      (format_us snap.p95)
      (format_us snap.p99)
      (format_us snap.p999)
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
      ; sub_us_samples = prof.sub_us_samples
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
