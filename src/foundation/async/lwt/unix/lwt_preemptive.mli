(** Run a promise on the thread that owns the event loop. *)

val run_in_main : (unit -> 'a Lwt.t) -> 'a
val run_in_main_dont_wait : (unit -> unit Lwt.t) -> (exn -> unit) -> unit
