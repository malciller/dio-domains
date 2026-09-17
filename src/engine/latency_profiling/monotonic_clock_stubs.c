/* Non-allocating monotonic clock shared by the stop-the-world canary and the
 * per-cycle latency profiler.
 *
 * Every OCaml stdlib clock (Mtime_clock.now_ns, Unix.gettimeofday,
 * Gc.quick_stat) allocates: the C FFI can only return boxed scalars, so using
 * one on the hot path would charge that allocation to the stages being
 * measured (and, for the canary, would make the detector trigger its own minor
 * collections and measure itself). Returning an immediate OCaml int keeps the
 * hot loop at zero allocation. */

#define CAML_INTERNALS
#include <caml/mlvalues.h>
#include <caml/minor_gc.h>
#include <stdint.h>
#include <time.h>

CAMLprim value dio_monotonic_ns(value unit) {
  (void)unit;
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return Val_long((intnat)ts.tv_sec * 1000000000L + (intnat)ts.tv_nsec);
}

/* Domain-local minor-heap allocation counter, returned as an immediate int.
 * The stdlib [Gc.minor_words] returns a boxed float (two words per call), so a
 * per-phase allocation attribution built from it would charge its own
 * allocation to the phase it measures. [caml_minor_words_allocated] reads the
 * domain's stat counter + young pointer directly. */
CAMLprim value dio_minor_words(value unit) {
  (void)unit;
  return Val_long((intnat)caml_minor_words_allocated());
}

/* Per-thread CPU time. wall - cpu over a span isolates stalls (deschedule or
 * stop-the-world pause) from genuine CPU work, with the same zero-allocation
 * immediate-int return. */
CAMLprim value dio_thread_cpu_ns(value unit) {
  (void)unit;
  struct timespec ts;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  return Val_long((intnat)ts.tv_sec * 1000000000L + (intnat)ts.tv_nsec);
}
