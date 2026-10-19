(** In-tree monotonic clock. *)

external now_ns : unit -> int64 = "dio_mtime_now_ns"
external elapsed_ns : unit -> int64 = "dio_mtime_elapsed_ns"
