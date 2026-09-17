(** Non-allocating monotonic clock, in nanoseconds.

    Every OCaml stdlib clock allocates because the C FFI returns boxed scalars:
    [Mtime_clock.now_ns] costs 3 words and [Unix.gettimeofday] 2 (measured). On the
    per-cycle latency path that allocation is charged to the stages being measured, so
    stage timing uses this immediate-[int] clock instead (see [monotonic_clock_stubs.c]).
    The value is nanoseconds since an unspecified monotonic epoch; only differences are
    meaningful. *)

external now_ns : unit -> int = "dio_monotonic_ns" [@@noalloc]

(** Per-thread CPU time in nanoseconds ([CLOCK_THREAD_CPUTIME_ID]). Comparing a cycle's
    wall span against its CPU span distinguishes real work from a stall: a large
    wall-minus -CPU gap means the thread was descheduled or stopped-the-world (major GC),
    not busy. Non-allocating for the same reason as {!now_ns}. *)
external thread_cpu_ns : unit -> int = "dio_thread_cpu_ns"
[@@noalloc]

(** Domain-local minor-heap word count, non-allocating. [Gc.minor_words] returns a boxed
    float, so a per-phase allocation delta built from it would itself allocate; this reads
    the domain's stat counter directly and returns an immediate int. Deltas are monotonic
    and only ever compared, so wraparound/truncation at 2^62 words is irrelevant. *)
external minor_words : unit -> int = "dio_minor_words"
[@@noalloc]

(** Calibrate the cost of one [now_ns] call and sanity-check [thread_cpu_ns]. The busy
    loop runs on a single thread, so its thread-CPU delta must not exceed its wall delta.
    If it does (by a lot), the container is reporting process CPU (or isn't virtualizing
    [CLOCK_THREAD_CPUTIME_ID]), which would make every [cpu]/[stall] number meaningless.
    Returns (ns_per_now_ns_call, busy_wall_ns, busy_cpu_ns). *)
let calibration_summary () =
  let n = 100_000 in
  let acc = ref 0 in
  let t0 = now_ns () in
  for _ = 1 to n do
    acc := !acc + now_ns ()
  done;
  let t1 = now_ns () in
  ignore !acc;
  let per_call = float (t1 - t0) /. float n in
  let w0 = now_ns () in
  let c0 = thread_cpu_ns () in
  let x = ref 0 in
  for i = 1 to 5_000_000 do
    x := !x + (i land 1)
  done;
  ignore !x;
  let w1 = now_ns () in
  let c1 = thread_cpu_ns () in
  per_call, w1 - w0, c1 - c0
;;
