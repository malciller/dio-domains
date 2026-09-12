(** Non-allocating monotonic clock, in nanoseconds.

    Every OCaml stdlib clock allocates because the C FFI can only return boxed
    scalars: [Mtime_clock.now_ns] costs 3 words and [Unix.gettimeofday] 2
    (measured). On the per-cycle latency path that allocation is charged to the
    very stages being measured, so stage timing uses this immediate-[int] clock
    instead (see [monotonic_clock_stubs.c]). The returned value is nanoseconds
    since an unspecified monotonic epoch; only differences are meaningful. *)

external now_ns : unit -> int = "dio_monotonic_ns" [@@noalloc]
