(** Minimal select-based event loop for the in-tree promise runtime.

    Internal to [lwt.unix]: timers, file-descriptor readiness, and a self-pipe used to
    wake the loop from other system threads. *)

val on_timer : float -> (unit -> unit) -> int
val on_readable : Unix.file_descr -> (unit -> unit) -> int
val on_writable : Unix.file_descr -> (unit -> unit) -> int
val stop_event : int -> unit

(** Run pending cross-thread jobs, then one polling iteration. [block] is false when the
    caller has paused promises to resolve immediately. *)
val iter : bool -> unit

(** Enqueue a closure to run on the thread that owns the loop, and wake it. *)
val enqueue_job : (unit -> unit) -> unit

val wake : unit -> unit
