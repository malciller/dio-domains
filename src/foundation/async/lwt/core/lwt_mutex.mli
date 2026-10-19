(** Cooperative mutexes. *)

type t

val create : unit -> t
val with_lock : t -> (unit -> 'a Lwt.t) -> 'a Lwt.t
