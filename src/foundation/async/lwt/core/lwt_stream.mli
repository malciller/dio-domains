(** Push/pull streams. *)

type 'a t

exception Closed
exception Full
exception Empty

class type ['a] bounded_push = object
  method size : int
  method resize : int -> unit
  method push : 'a -> unit Lwt.t
  method close : unit
  method count : int
end

val create : unit -> 'a t * ('a option -> unit)
val create_bounded : int -> 'a t * 'a bounded_push
val from : (unit -> 'a option Lwt.t) -> 'a t
val of_list : 'a list -> 'a t
val get : 'a t -> 'a option Lwt.t
val get_available : 'a t -> 'a list
val closed : 'a t -> unit Lwt.t
val iter : ('a -> unit) -> 'a t -> unit Lwt.t
val iter_s : ('a -> unit Lwt.t) -> 'a t -> unit Lwt.t
