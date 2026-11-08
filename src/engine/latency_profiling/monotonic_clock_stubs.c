/* Non-allocating monotonic clock shared by the stop-the-world canary and the
 * per-cycle latency profiler.
 *
 * Every OCaml stdlib clock (Mtime_clock.now_ns, Unix.gettimeofday,
 * Gc.quick_stat) allocates: the C FFI can only return boxed scalars, so using
 * one on the hot path would charge that allocation to the stages being
 * measured (and, for the canary, would make the detector trigger its own minor
 * collections and measure itself). Returning an immediate OCaml int keeps the
 * hot loop at zero allocation.
 *
 * Clock source, chosen once:
 *   - x86_64 Linux: clock_gettime(CLOCK_MONOTONIC), a vDSO call. rdtsc is
 *     opt-in (DIO_CLOCK=tsc) because invariant TSC guarantees a constant rate,
 *     not a shared offset across cores or packages; a migrated thread can
 *     inflate a span by an arbitrary offset. Calibration falls back if CPUID
 *     reports no invariant TSC or rdtsc is slower (some hypervisors trap it).
 *   - Apple: mach_absolute_time (a hardware counter).
 *   - Everything else: clock_gettime(CLOCK_MONOTONIC).
 * Only differences between values are meaningful; epochs differ per source. */

#define CAML_INTERNALS
#include <caml/mlvalues.h>
#include <caml/minor_gc.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(__APPLE__)
#include <mach/mach_time.h>

static mach_timebase_info_data_t dio_timebase;
static pthread_once_t dio_timebase_once = PTHREAD_ONCE_INIT;
static void dio_timebase_init(void) { mach_timebase_info(&dio_timebase); }
#endif

#if !defined(__APPLE__)
static uint64_t dio_clock_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
#endif

#if defined(__linux__) && defined(__x86_64__)

/* Inline asm keeps the calibration independent of libc's cpuid/rdtsc headers. */
static inline uint64_t dio_rdtsc(void) {
  unsigned int lo, hi;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)hi << 32) | (uint64_t)lo;
}

static inline void dio_cpuid(unsigned int leaf, unsigned int *a, unsigned int *b,
                             unsigned int *c, unsigned int *d) {
  __asm__ __volatile__("cpuid" : "=a"(*a), "=b"(*b), "=c"(*c), "=d"(*d) : "a"(leaf));
}

/* 0 = uninitialised, 1 = ready, -1 = use clock_gettime. */
static int dio_tsc_state = 0;
/* Nanoseconds per TSC tick in 32.32 fixed point. */
static uint64_t dio_tsc_scale = 0;
/* Several domains call the clock before the first cycle, so calibration runs once
 * under pthread_once rather than racing on [dio_tsc_state]. */
static pthread_once_t dio_tsc_once = PTHREAD_ONCE_INIT;

/* Cost check: a trapped rdtsc (some hypervisors) exits to the host and is
 * slower than the vDSO clock, so measure both before committing. */
static uint64_t dio_time_rdtsc(void) {
  uint64_t acc = 0;
  uint64_t t0 = dio_clock_ns();
  for (int i = 0; i < 100000; i++) {
    acc += dio_rdtsc();
  }
  uint64_t t1 = dio_clock_ns();
  return (acc == 0xdeadbeefull) ? UINT64_MAX : t1 - t0;
}

static uint64_t dio_time_clock(void) {
  uint64_t acc = 0;
  uint64_t t0 = dio_clock_ns();
  for (int i = 0; i < 100000; i++) {
    acc += dio_clock_ns();
  }
  uint64_t t1 = dio_clock_ns();
  return (acc == 0xdeadbeefull) ? UINT64_MAX : t1 - t0;
}

static void dio_tsc_init(void) {
  const char *mode = getenv("DIO_CLOCK");
  if (mode == NULL || strcmp(mode, "tsc") != 0) {
    dio_tsc_state = -1;
    return;
  }
  unsigned int eax, ebx, ecx, edx;
  dio_cpuid(0x80000000u, &eax, &ebx, &ecx, &edx);
  if (eax < 0x80000007u) {
    dio_tsc_state = -1;
    return;
  }
  dio_cpuid(0x80000007u, &eax, &ebx, &ecx, &edx);
  if ((edx & (1u << 8)) == 0) { /* invariant TSC */
    dio_tsc_state = -1;
    return;
  }
  uint64_t n0 = dio_clock_ns();
  uint64_t t0 = dio_rdtsc();
  struct timespec req = {0, 10000000}; /* 10 ms */
  nanosleep(&req, NULL);
  uint64_t t1 = dio_rdtsc();
  uint64_t n1 = dio_clock_ns();
  if (t1 <= t0 || n1 <= n0) {
    dio_tsc_state = -1;
    return;
  }
  long double ns_per_tick = (long double)(n1 - n0) / (long double)(t1 - t0);
  dio_tsc_scale = (uint64_t)(ns_per_tick * 4294967296.0L);
  if (dio_tsc_scale == 0 || dio_time_rdtsc() >= dio_time_clock()) {
    dio_tsc_state = -1;
    return;
  }
  dio_tsc_state = 1;
}

#endif

CAMLprim value dio_monotonic_ns(value unit) {
  (void)unit;
#if defined(__APPLE__)
  pthread_once(&dio_timebase_once, dio_timebase_init);
  uint64_t ticks = mach_absolute_time();
  return Val_long(
      (intnat)(uint64_t)(((__uint128_t)ticks * dio_timebase.numer) / dio_timebase.denom));
#elif defined(__linux__) && defined(__x86_64__)
  pthread_once(&dio_tsc_once, dio_tsc_init);
  if (dio_tsc_state == 1) {
    uint64_t ticks = dio_rdtsc();
    return Val_long((intnat)(uint64_t)(((__uint128_t)ticks * dio_tsc_scale) >> 32));
  }
  return Val_long((intnat)dio_clock_ns());
#else
  return Val_long((intnat)dio_clock_ns());
#endif
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
