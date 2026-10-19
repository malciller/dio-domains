(** Run a promise to completion on the calling thread. *)

val run : 'a Lwt.t -> 'a
