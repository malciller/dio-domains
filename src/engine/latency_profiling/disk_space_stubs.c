/* Free-space probe for the latency journal's disk floor.
 *
 * The journal must stop before the shared data directory fills, because that directory
 * also holds position state: telemetry yields to capital state. This toolchain's stdlib
 * has no [Unix.statvfs], and shelling out to [df] from a diagnostic thread would fork a
 * process and parse text to answer a question the kernel already knows.
 *
 * Returns the bytes available to an unprivileged writer ([f_bavail]), or -1 when the
 * query fails (missing path, unsupported filesystem). Callers treat -1 as "unknown" and
 * fall through to their size caps rather than stopping. */

#include <caml/mlvalues.h>
#include <stdint.h>
#include <sys/statvfs.h>

CAMLprim value dio_disk_free_bytes(value path) {
  struct statvfs s;
  if (statvfs(String_val(path), &s) != 0) {
    return Val_long(-1);
  }
  uint64_t bytes = (uint64_t)s.f_bavail * (uint64_t)s.f_frsize;
  /* Keep the immediate-int representation valid; 2^62 bytes is far beyond any real
   * volume, so clamping is safe and never observed. */
  if (bytes > (uint64_t)((1ull << 62) - 1)) {
    bytes = (uint64_t)((1ull << 62) - 1);
  }
  return Val_long((intnat)bytes);
}
