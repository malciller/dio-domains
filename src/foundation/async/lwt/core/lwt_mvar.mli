(** Single-slot mailbox variables. *)

type 'a t

val create_empty : unit -> 'a t
val put : 'a t -> 'a -> unit Lwt.t
val take : 'a t -> 'a Lwt.t
val is_empty : 'a t -> bool
