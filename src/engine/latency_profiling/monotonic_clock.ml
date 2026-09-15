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
