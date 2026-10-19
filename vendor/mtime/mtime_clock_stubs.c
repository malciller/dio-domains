/* In-tree monotonic clock stubs.
 *
 * CLOCK_MONOTONIC is available on macOS (10.12+) and Linux and is not
 * affected by wall-clock adjustments. elapsed_ns is relative to the first
 * call in this process. */

#include <caml/alloc.h>
#include <caml/mlvalues.h>

#include <stdint.h>
#include <time.h>

static int64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ((int64_t)ts.tv_sec * 1000000000LL) + (int64_t)ts.tv_nsec;
}

CAMLprim value dio_mtime_now_ns(value unit) {
  (void)unit;
  return caml_copy_int64(now_ns());
}

CAMLprim value dio_mtime_elapsed_ns(value unit) {
  static int64_t start = 0;
  (void)unit;
  if (start == 0) {
    start = now_ns();
  }
  return caml_copy_int64(now_ns() - start);
}
