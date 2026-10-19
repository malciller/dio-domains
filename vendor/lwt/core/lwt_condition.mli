(** Condition variables for the in-tree promise runtime. *)

type 'a t

val create : unit -> 'a t
val wait : 'a t -> 'a Lwt.t
val signal : 'a t -> 'a -> unit
val broadcast : 'a t -> 'a -> unit
