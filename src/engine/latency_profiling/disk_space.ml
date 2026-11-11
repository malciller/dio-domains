(** Free space on the filesystem holding a path, for the latency journal's disk floor.

    The journal must stop before the shared data directory fills, because that directory
    also holds position state. The stdlib in this toolchain has no [statvfs], hence the
    stub. Unlike the hot-path clocks in this library, this is called at most once per
    journal interval, so allocation and blocking behaviour do not matter. *)

(** Bytes available to an unprivileged writer, or -1 when the query fails (missing path,
    unsupported filesystem). Treat -1 as "unknown" and fall through to the size caps. *)
external free_bytes : string -> int = "dio_disk_free_bytes"
