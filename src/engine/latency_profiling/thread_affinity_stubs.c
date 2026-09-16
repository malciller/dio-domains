/* CPU affinity for the trading domains.
 *
 * The host is a hybrid CPU (P-cores + E-cores). Left to CFS, a busy event-driven
 * domain migrates between core types and preempts against other runnable
 * threads, which shows up as multi-hundred-microsecond p999 stalls with wall >>
 * cpu and stw = 0. Pinning each domain to a chosen CPU removes the migration and
 * lets the operator keep the P-cores for trading and push everything else onto
 * the E-cores. [sched_setaffinity] on the caller's own thread needs no
 * privilege, so this works in the unprivileged container.
 *
 * Linux-only: macOS has no [pthread_setaffinity_np] / [cpu_set_t], so the calls
 * below degrade to no-ops returning false and the OCaml layer leaves affinity
 * alone (dev builds/tests run on macOS; production runs linux/amd64). */

#include <caml/mlvalues.h>

#ifdef __linux__

#define _GNU_SOURCE
#include <pthread.h>
#include <sched.h>

/* Pin the calling thread (the current OCaml domain) to [cpu]. Returns true on
 * success. */
CAMLprim value dio_pin_current_thread(value vcpu) {
  int cpu = Int_val(vcpu);
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
  return Val_bool(rc == 0);
}

/* Pin the calling thread to the closed range [lo, hi]. Returns true on success. */
CAMLprim value dio_pin_current_thread_range(value vlo, value vhi) {
  int lo = Int_val(vlo);
  int hi = Int_val(vhi);
  cpu_set_t set;
  CPU_ZERO(&set);
  for (int c = lo; c <= hi; c++) {
    CPU_SET(c, &set);
  }
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
  return Val_bool(rc == 0);
}

/* Number of CPUs currently allowed for this process (affinity mask size, not the
 * host's online count), so a cpuset-restricted container reports its own CPUs. */
CAMLprim value dio_allowed_cpu_count(value unit) {
  (void)unit;
  cpu_set_t set;
  if (sched_getaffinity(0, sizeof(set), &set) != 0) {
    return Val_int(0);
  }
  return Val_int(CPU_COUNT(&set));
}

/* Drop the calling thread to SCHED_IDLE so it only runs when no normal thread is
 * runnable. Unprivileged (own thread only) and the right policy for a pure
 * busy-spin keep-warm loop: it must never preempt a trading cycle. Returns true on
 * success. */
CAMLprim value dio_set_current_idle(value unit) {
  (void)unit;
  struct sched_param p;
  p.sched_priority = 0;
  int rc = sched_setscheduler(0, SCHED_IDLE, &p);
  return Val_bool(rc == 0);
}

#else

CAMLprim value dio_pin_current_thread(value vcpu) {
  (void)vcpu;
  return Val_false;
}

CAMLprim value dio_pin_current_thread_range(value vlo, value vhi) {
  (void)vlo;
  (void)vhi;
  return Val_false;
}

CAMLprim value dio_allowed_cpu_count(value unit) {
  (void)unit;
  return Val_int(0);
}

CAMLprim value dio_set_current_idle(value unit) {
  (void)unit;
  return Val_false;
}

#endif
