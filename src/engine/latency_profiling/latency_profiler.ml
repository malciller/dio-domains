(** Histogram-based latency profiler.
    Records latency samples into fixed-width buckets and computes percentile
    distributions (p50, p90, p95, p99, p999).

    Three resolution tiers keep low-end precision independent of the
    memory-bounded coarse bucket width:
    - below 1us: nanosecond tier (1ns buckets);
    - [1us, bucket_us): fine microsecond tier (1us buckets);
    - at/above [bucket_us]: coarse buckets.
    Without the fine tier a wide-bucket profiler (e.g. bucket_us=1000 for the
    oracle) would collapse every latency in [1us, 999us] into bucket 0 and
    report 0us. Percentiles are microseconds as floats; a sub-microsecond value
    is a fraction (e.g. 0.5 = 500ns); nanosecond display scales by 1000.

    Metrics accumulate in a window and are published atomically via
    [snapshot_and_reset] (typically once per window, from the recording domain).
    Readers consume the immutable snapshot via [published_snapshot], a lock-free
    [Atomic.get], which removes the torn-read race between histogram writer and
    dashboard reader. *)

open Mtime

let section = "latency_profiler"

(* Default bucket width in microseconds. *)
let bucket_us = 1

(* Upper bound of tracked latency range in microseconds. *)
let max_latency_us = 100_000

(* Nanosecond tier for sub-microsecond samples: one bucket per nanosecond. The sub-us
   range is 0..999ns, so a fixed 1000-bucket tier covers it at 1ns resolution with no
   division on the hot path. *)
let ns_bucket_count = 1000

(** Read-only snapshot of a completed measurement window. Immutable once published;
    readers never observe a partially-updated histogram. *)
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
  ; max_us : float (* Largest recorded latency in this window, microseconds. *)
  ; over_threshold : int
      (* Samples at or above the threshold passed to [snapshot_and_reset]; 0 when no
         threshold was supplied. *)
  ; max_cause : string option (* Cause of the max latency in this window. *)
  ; executions : int (* Activity ticks recorded in this window. *)
  ; last_exec_time : float (* Unix time of the last activity tick. *)
  ; window_start : float (* Unix time the window began. *)
  ; window_end : float (* Unix time the window was published. *)
  }

(** Profiler state: fixed-size histogram arrays, running sample/overflow counters,
    per-window activity counters, and the last completed snapshot. *)
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
  ; rolling : int
      (* Number of recent windows retained for the published aggregate; 0 disables rolling
         and restores per-window behavior. *)
  ; ring_buckets : int array array (* Per-window copies of the coarse tier. *)
  ; ring_us : int array array
  ; ring_ns : int array array
  ; ring_samples : int array
  ; ring_sub_us : int array
  ; ring_overflow : int array
  ; ring_max_ns : int array
  ; ring_cause : string option array
  ; mutable ring_idx : int (* Next ring slot to overwrite. *)
  ; mutable ring_filled : int (* Number of valid ring slots (<= rolling). *)
  ; (* Running union of the ring, updated incrementally (evict oldest, add current) so a
       snapshot is O(buckets) and allocation-free instead of summing all N windows. *)
    mutable sum_samples : int
  ; mutable sum_sub_us : int
  ; mutable sum_overflow : int
  ; sum_buckets : int array
  ; sum_us : int array
  ; sum_ns : int array
  }

(** [create ?bucket_us ?max_latency_us ?rolling_windows name] allocates a profiler with
    [max_latency_us / bucket_us] coarse buckets, a fine tier of [bucket_us - 1] one-us
    buckets, and a 1000-bucket nanosecond tier, all zeroed. [rolling_windows] > 1 keeps
    the last N completed windows and publishes percentiles over their union, so a low-rate
    symbol's p50/p99 does not swing on a handful of samples; the default 0 publishes each
    window on its own. *)
let create ?(bucket_us = 1) ?(max_latency_us = 10_000) ?(rolling_windows = 0) name =
  let count = max_latency_us / bucket_us in
  let us_count = max 0 (bucket_us - 1) in
  let rolling = max 0 rolling_windows in
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
  ; rolling
  ; ring_buckets = Array.init rolling (fun _ -> Array.make count 0)
  ; ring_us = Array.init rolling (fun _ -> Array.make us_count 0)
  ; ring_ns = Array.init rolling (fun _ -> Array.make ns_bucket_count 0)
  ; ring_samples = Array.make rolling 0
  ; ring_sub_us = Array.make rolling 0
  ; ring_overflow = Array.make rolling 0
  ; ring_max_ns = Array.make rolling 0
  ; ring_cause = Array.make rolling None
  ; ring_idx = 0
  ; ring_filled = 0
  ; sum_samples = 0
  ; sum_sub_us = 0
  ; sum_overflow = 0
  ; sum_buckets = Array.make count 0
  ; sum_us = Array.make us_count 0
  ; sum_ns = Array.make ns_bucket_count 0
  }
;;

(** [record_ns t ns] records a latency in nanoseconds, bypassing the
    [Mtime.Span] argument and its [int64] boxing in [record]. Used by callers
    holding a non-allocating nanosecond timestamp, notably the stop-the-world
    canary's C-stub clock. Negative values are clamped to zero. Sub-microsecond
    samples use the nanosecond tier (1ns resolution); [1us, bucket_us) uses the
    fine tier; at/above [bucket_us] uses the coarse buckets. Samples above the
    histogram range are clamped to the last coarse bucket and counted as
    overflow. *)
let[@inline] record_ns t ns =
  let ns = if ns < 0 then 0 else ns in
  if ns < 1000
  then (
    (* Sub-microsecond: capture at nanosecond level (bucket index = ns). *)
    t.ns_buckets.(ns) <- t.ns_buckets.(ns) + 1;
    t.sub_us_samples <- t.sub_us_samples + 1)
  else (
    let us = ns / 1000 in
    if us < t.bucket_us
    then
      (* Fine tier: exact microsecond resolution; bucket i holds (i+1)us, independent of
         the coarse bucket width. *)
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

(** [record t span] converts [span] to nanoseconds and records it via [record_ns]. *)
let[@inline] record t span = record_ns t (Int64.to_int (Span.to_uint64_ns span))

(** [record_max_ns t ns] behaves as [record_ns] and returns [true] when [ns] set a new
    window maximum, so the caller builds an expensive cause string only on that rare path
    instead of allocating a cause closure per cycle. *)
let[@inline] record_max_ns t ns =
  let ns = if ns < 0 then 0 else ns in
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
    t.max_cause <- None;
    true)
  else false
;;

(** [record_max t span] is [record_max_ns] on a [Mtime.Span]. *)
let[@inline] record_max t span = record_max_ns t (Int64.to_int (Span.to_uint64_ns span))

(** [set_cause t cause] attaches a cause string to the current window's maximum sample.
    Only meaningful immediately after [record_max] returned [true]. *)
let set_cause t cause = t.max_cause <- Some cause

(** [record_with_cause t span cause_thunk] behaves as [record], but if [span] sets a new
    maximum latency it evaluates [cause_thunk ()] and records the result as the cause. *)
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

(** [tick_exec t ~now] records one activity event (e.g. a strategy execution) at Unix time
    [now]. The count and timestamp appear in the next snapshot so consumers can derive an
    executions-per-second rate and last-activity time even when a window has zero latency
    samples. *)
let tick_exec t ~now =
  t.executions <- t.executions + 1;
  t.last_exec_time <- now
;;

(** [set_executions t count] overwrites the current window's execution count (e.g. order
    actions actually pushed, not raw strategy-invocation cycles) and refreshes
    [last_exec_time] when [count > 0]. Call before [snapshot_and_reset] so the published
    snapshot carries the real count. *)
let set_executions t count =
  t.executions <- count;
  if count > 0 then t.last_exec_time <- Unix.gettimeofday ()
;;

(** [percentile t p] returns the p-th percentile ([0.0, 1.0]) by a cumulative
    scan over the histogram. Returns the bucket boundary in microseconds; a
    sub-microsecond result is a fraction (e.g. 0.5 = 500ns); 0.0 when no samples
    exist. Scans the nanosecond tier (0..999ns), then the fine microsecond tier
    ([1, bucket_us) us), then the coarse buckets, so percentiles below the coarse
    bucket width keep their resolution regardless of [bucket_us]. Early-exits
    once the target cumulative count is reached, critical for large histograms
    (e.g. the cycle profiler with 100,000 buckets). *)
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

(** [percentiles5_arrays ~bucket_us ~ns ~us ~coarse ~samples] computes p50/p90/p95/p99/
    p999 in a single cumulative pass over three tier arrays. Shared by the live window and
    the rolling union so both report identical statistics. *)
let percentiles5_arrays ~bucket_us ~ns ~us ~coarse ~samples =
  if samples = 0
  then 0.0, 0.0, 0.0, 0.0, 0.0
  else (
    let fracs = [| 0.50; 0.90; 0.95; 0.99; 0.999 |] in
    (* -1.0 marks "not yet captured": a real percentile can be 0.0 (all samples
       sub-microsecond), which must not re-trigger capture. *)
    let vals = [| -1.0; -1.0; -1.0; -1.0; -1.0 |] in
    let cumulative = ref 0 in
    let remaining = ref 5 in
    let capture v =
      if !remaining > 0
      then
        for k = 0 to 4 do
          if vals.(k) < 0.0 && float !cumulative >= ceil (float samples *. fracs.(k))
          then (
            vals.(k) <- v;
            decr remaining)
        done
    in
    (* Nanosecond tier (0..999ns → 0.0..0.999 microseconds). *)
    let i = ref 0 in
    while !i < Array.length ns && !remaining > 0 do
      cumulative := !cumulative + ns.(!i);
      capture (float !i /. 1000.0);
      incr i
    done;
    (* Fine microsecond tier ((j+1)us, 1us resolution). *)
    let j = ref 0 in
    while !j < Array.length us && !remaining > 0 do
      cumulative := !cumulative + us.(!j);
      capture (float (!j + 1));
      incr j
    done;
    (* Coarse microsecond tier. *)
    let k = ref 0 in
    while !k < Array.length coarse && !remaining > 0 do
      cumulative := !cumulative + coarse.(!k);
      capture (float (!k * bucket_us));
      incr k
    done;
    vals.(0), vals.(1), vals.(2), vals.(3), vals.(4))
;;

(** [percentiles5 t] computes the live window's percentiles. *)
let percentiles5 t =
  percentiles5_arrays
    ~bucket_us:t.bucket_us
    ~ns:t.ns_buckets
    ~us:t.us_buckets
    ~coarse:t.buckets
    ~samples:t.samples
;;

(** [reset t] zeroes all three histogram tiers and the sample/overflow and activity
    counters. Does not touch [window_start]; callers advancing the window must set it
    explicitly. *)
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

(** [count_above_arrays ~bucket_us ~ns ~us ~coarse threshold_us] returns the number of
    samples at or above [threshold_us] across three tier arrays. The nanosecond tier is
    exact; the fine tier is exact to its microsecond bucket; a coarse bucket counts when
    its lower edge reaches the threshold, so a spike at the ceiling is not missed to
    bucket rounding. Non-finite thresholds return 0. *)
let count_above_arrays ~bucket_us ~ns ~us ~coarse threshold_us =
  if not (Float.is_finite threshold_us)
  then 0
  else (
    let count = ref 0 in
    (* Skip buckets whose lower edge is below the threshold: the nanosecond and fine tiers
       are fully below a multi-us threshold, and the coarse tier starts at the first edge
       >= threshold, turning a full 20k-bucket scan into a tail scan. *)
    let start_ns =
      if threshold_us <= 0.0
      then 0
      else max 0 (int_of_float (ceil (threshold_us *. 1000.0)))
    in
    for i = start_ns to Array.length ns - 1 do
      count := !count + ns.(i)
    done;
    let start_us =
      if threshold_us <= 1.0 then 0 else max 0 (int_of_float (ceil threshold_us) - 1)
    in
    for j = start_us to Array.length us - 1 do
      count := !count + us.(j)
    done;
    let start_k =
      if threshold_us <= 0.0
      then 0
      else max 0 (int_of_float (ceil (threshold_us /. float bucket_us)))
    in
    for k = start_k to Array.length coarse - 1 do
      count := !count + coarse.(k)
    done;
    !count)
;;

(** [count_above t threshold_us] counts live-window samples at or above [threshold_us]. *)
let count_above t threshold_us =
  count_above_arrays
    ~bucket_us:t.bucket_us
    ~ns:t.ns_buckets
    ~us:t.us_buckets
    ~coarse:t.buckets
    threshold_us
;;

(** [snapshot_and_reset ?spike_threshold_us t] computes the current window's percentiles,
    publishes them as an immutable snapshot (replacing the previous one in the Atomic
    cell), zeroes the histogram, and starts a new window. Always publishes, even for a
    zero-sample window, so consumers can distinguish "idle" from "no data".
    [spike_threshold_us] sets [over_threshold] to the count of samples at or above it.
    Locked against concurrent resets.
    @return the published snapshot. *)
let snapshot_and_reset ?(spike_threshold_us = infinity) t =
  let now = Unix.gettimeofday () in
  Mutex.lock t.mutex;
  let window_start = t.window_start in
  (* Rolling: fold the just-finished window into the ring and report the union of the
     retained windows, so a low-rate symbol's percentiles rest on more than a handful of
     samples. Non-rolling: report the window alone. *)
  let samples, sub_us_samples, overflow, max_latency_ns, max_cause, over_threshold, pcts =
    if t.rolling > 0
    then (
      let idx = t.ring_idx in
      (* Update the running union incrementally: for each bucket subtract the evicted
         window's value, overwrite the ring slot with the current window, then add it
         back. O(buckets) and allocation-free, versus re-summing all N windows into fresh
         arrays every snapshot. *)
      let rb = t.ring_buckets.(idx) in
      for i = 0 to t.bucket_count - 1 do
        let old = rb.(i) in
        let cur = t.buckets.(i) in
        rb.(i) <- cur;
        t.sum_buckets.(i) <- t.sum_buckets.(i) - old + cur
      done;
      let ru = t.ring_us.(idx) in
      for i = 0 to t.us_bucket_count - 1 do
        let old = ru.(i) in
        let cur = t.us_buckets.(i) in
        ru.(i) <- cur;
        t.sum_us.(i) <- t.sum_us.(i) - old + cur
      done;
      let rn = t.ring_ns.(idx) in
      for i = 0 to ns_bucket_count - 1 do
        let old = rn.(i) in
        let cur = t.ns_buckets.(i) in
        rn.(i) <- cur;
        t.sum_ns.(i) <- t.sum_ns.(i) - old + cur
      done;
      t.sum_samples <- t.sum_samples - t.ring_samples.(idx) + t.samples;
      t.sum_sub_us <- t.sum_sub_us - t.ring_sub_us.(idx) + t.sub_us_samples;
      t.sum_overflow <- t.sum_overflow - t.ring_overflow.(idx) + t.overflow;
      t.ring_samples.(idx) <- t.samples;
      t.ring_sub_us.(idx) <- t.sub_us_samples;
      t.ring_overflow.(idx) <- t.overflow;
      t.ring_max_ns.(idx) <- t.max_latency_ns;
      t.ring_cause.(idx) <- t.max_cause;
      let filled = min t.rolling (t.ring_filled + 1) in
      t.ring_filled <- filled;
      t.ring_idx <- (idx + 1) mod t.rolling;
      (* Max/cause cannot be maintained incrementally under eviction, so scan the (small)
         ring for the current maximum. *)
      let maxns = ref 0
      and cause = ref None in
      for k = 0 to filled - 1 do
        if t.ring_max_ns.(k) > !maxns
        then (
          maxns := t.ring_max_ns.(k);
          cause := t.ring_cause.(k))
      done;
      (* Spike-breaker counts and [samples] stay per-window so a single spike alarms once
         (not for the K windows it remains in the rolling union); percentiles report the
         union. *)
      let over_threshold =
        if Float.is_finite spike_threshold_us then count_above t spike_threshold_us else 0
      in
      ( t.samples
      , t.sub_us_samples
      , t.overflow
      , !maxns
      , !cause
      , over_threshold
      , percentiles5_arrays
          ~bucket_us:t.bucket_us
          ~ns:t.sum_ns
          ~us:t.sum_us
          ~coarse:t.sum_buckets
          ~samples:t.sum_samples ))
    else (
      let over_threshold =
        if Float.is_finite spike_threshold_us then count_above t spike_threshold_us else 0
      in
      ( t.samples
      , t.sub_us_samples
      , t.overflow
      , t.max_latency_ns
      , t.max_cause
      , over_threshold
      , percentiles5 t ))
  in
  let p50, p90, p95, p99, p999 = pcts in
  let max_us = float max_latency_ns /. 1000.0 in
  let snap =
    { name = t.name
    ; p50
    ; p90
    ; p95
    ; p99
    ; p999
    ; samples
    ; sub_us_samples
    ; overflow
    ; max_us
    ; over_threshold
    ; max_cause
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

(** [published_snapshot t] returns the most recently completed window's snapshot, or
    [None] before the first publication. Lock-free: reads only the Atomic cell, never the
    live histogram. *)
let published_snapshot t = Atomic.get t.published

(** [format_us f] renders a microsecond value: nanoseconds below 1us (e.g. "500ns"),
    milliseconds at or above 1ms (e.g. "1.20ms"), else microseconds. *)
let format_us f =
  if f < 1.0
  then Printf.sprintf "%.0fns" (f *. 1000.0)
  else if f < 1000.0
  then Printf.sprintf "%.1fus" f
  else Printf.sprintf "%.2fms" (f /. 1000.0)
;;

(** [spike_message ~key ~window_seconds ~threshold_us stages] renders a one-line spike
    report for each stage in [stages] with non-zero [over_threshold], naming the stage,
    worst spike, breach count over sample total, and p99. Appends the first non-empty
    [max_cause] among the stages as a continuation line. Returns [None] when no stage
    breached. Pure and unit-testable. *)
let spike_message ~key ~window_seconds ~threshold_us stages =
  let breached = List.filter (fun (_, (s : snapshot)) -> s.over_threshold > 0) stages in
  if breached = []
  then None
  else (
    let stage_str (label, (s : snapshot)) =
      Printf.sprintf
        "%s max=%s p99=%s p50=%s spikes=%d/%d"
        label
        (format_us s.max_us)
        (format_us s.p99)
        (format_us s.p50)
        s.over_threshold
        s.samples
    in
    let base =
      Printf.sprintf
        "[%s] latency spikes >=%s over %.0fs: %s"
        key
        (format_us threshold_us)
        window_seconds
        (String.concat "  " (List.map stage_str breached))
    in
    let cause =
      List.find_map
        (fun (_, (s : snapshot)) ->
          match s.max_cause with
          | Some c when c <> "" -> Some c
          | _ -> None)
        stages
    in
    Some
      (match cause with
       | None -> base
       | Some c -> base ^ "\n worst-cycle cause: " ^ c))
;;

(** [report ?sample_threshold t] logs the current window's percentiles when at least
    [sample_threshold] samples were collected, then advances the window (publish + reset). *)
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

(** [time_it t f] records the wall-clock execution time of [f ()] in the profiler and
    returns the result of [f]. *)
let time_it t f =
  let start = Mtime_clock.now_ns () in
  let res = f () in
  let stop = Mtime_clock.now_ns () in
  let span = Span.of_uint64_ns (Int64.sub stop start) in
  record t span;
  res
;;

(** [snapshot prof] returns [Some snapshot] of the live percentiles, or [None] if no
    samples were recorded. Does not reset. For non-windowed consumers reading the
    accumulating histogram. *)
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
      ; max_us = float prof.max_latency_ns /. 1000.0
      ; over_threshold = 0
      ; max_cause = prof.max_cause
      ; executions = prof.executions
      ; last_exec_time = prof.last_exec_time
      ; window_start = prof.window_start
      ; window_end = Unix.gettimeofday ()
      })
;;

(** [name t] returns the profiler instance identifier. *)
let name t = t.name
