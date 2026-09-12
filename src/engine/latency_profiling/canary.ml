(** Canary / stop-the-world detector.

    A trading domain paused by another domain's major collection observes
    neither a minor nor a major collection delta in its own [Gc.quick_stat]
    counters, so the per-cycle [Gc_monitor] cause string stays blank while the
    cycle latency balloons. This module runs a dedicated busy-spin domain that
    reads a non-allocating monotonic clock and records the gap between
    consecutive reads. A gap far above the spin loop's natural iteration time is
    a process-wide runtime pause (major-GC stop-the-world) or a scheduler
    descheduling, reported on the same window cadence as the per-domain latency
    profiles so the two can be correlated by timestamp.

    The clock is {!Monotonic_clock.now_ns}, an immediate-[int] C stub: every
    stdlib clock ([Mtime_clock.now_ns], [Unix.gettimeofday], [Gc.quick_stat])
    allocates, and a canary that allocates would trigger its own minor
    collections and end up measuring itself rather than the runtime. *)

let section = "canary"

(** [enabled ()] honors the [DIO_CANARY] kill switch; the canary busy-spins a
    full core, so it must be easy to disable outside a diagnostic window. *)
let enabled () =
  match Sys.getenv_opt "DIO_CANARY" with
  | Some ("0" | "false" | "off" | "no") -> false
  | _ -> true
;;

let env_float name default =
  match Sys.getenv_opt name with
  | None -> default
  | Some s ->
    (match float_of_string_opt s with
     | Some f -> f
     | None -> default)
;;

(** [run ~threshold_us ~window_seconds] never returns. It records the gap
    between consecutive clock reads into a latency histogram and logs a summary
    every [window_seconds]. Reporting on the same cadence as the per-domain
    windows lets a global pause be matched to the domain cycles that spiked. *)
let run ~threshold_us ~window_seconds =
  let prof = Latency_profiler.create ~bucket_us:1 ~max_latency_us:10_000 "canary" in
  let window_ns = int_of_float (window_seconds *. 1_000_000_000.) in
  let window_start = ref (Monotonic_clock.now_ns ()) in
  let prev = ref (Monotonic_clock.now_ns ()) in
  while true do
    let now = Monotonic_clock.now_ns () in
    let gap = now - !prev in
    prev := now;
    Latency_profiler.record_ns prof gap;
    if now - !window_start >= window_ns
    then (
      window_start := now;
      let snap =
        Latency_profiler.snapshot_and_reset ~spike_threshold_us:threshold_us prof
      in
      Logging.info_f
        ~section
        "STW canary p50=%s p99=%s max=%s spikes=%d/%d (threshold %s)"
        (Latency_profiler.format_us snap.p50)
        (Latency_profiler.format_us snap.p99)
        (Latency_profiler.format_us snap.max_us)
        snap.over_threshold
        snap.samples
        (Latency_profiler.format_us threshold_us));
    Domain.cpu_relax ()
  done
;;

(** [start ()] spawns the detector domain unless disabled by [DIO_CANARY].
    [DIO_CANARY_THRESHOLD_US] (default 10) and [DIO_CANARY_WINDOW_S] (default 5)
    tune the reported spike threshold and the log window. Detached: it runs for
    the life of the process and is never joined. *)
let start () =
  if enabled ()
  then (
    let threshold_us = env_float "DIO_CANARY_THRESHOLD_US" 10.0 in
    let window_seconds = env_float "DIO_CANARY_WINDOW_S" 5.0 in
    Logging.info_f
      ~section
      "Stop-the-world canary started (threshold %s, window %.0fs)"
      (Latency_profiler.format_us threshold_us)
      window_seconds;
    ignore (Domain.spawn (fun () -> run ~threshold_us ~window_seconds)))
;;
