/* Non-allocating monotonic clock shared by the stop-the-world canary and the
 * per-cycle latency profiler.
 *
 * Every OCaml stdlib clock (Mtime_clock.now_ns, Unix.gettimeofday,
 * Gc.quick_stat) allocates: the C FFI can only return boxed scalars, so using
 * one on the hot path would charge that allocation to the stages being
 * measured (and, for the canary, would make the detector trigger its own minor
 * collections and measure itself). Returning an immediate OCaml int keeps the
 * hot loop at zero allocation. */

#include <caml/mlvalues.h>
#include <stdint.h>
#include <time.h>

CAMLprim value dio_monotonic_ns(value unit) {
  (void)unit;
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return Val_long((intnat)ts.tv_sec * 1000000000L + (intnat)ts.tv_nsec);
}
