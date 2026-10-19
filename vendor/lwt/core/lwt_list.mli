(** Sequential and concurrent list helpers. *)

val iter : ('a -> unit) -> 'a list -> unit Lwt.t
val iter_s : ('a -> unit Lwt.t) -> 'a list -> unit Lwt.t
val iter_p : ('a -> unit Lwt.t) -> 'a list -> unit Lwt.t
val map_s : ('a -> 'b Lwt.t) -> 'a list -> 'b list Lwt.t
val map_p : ('a -> 'b Lwt.t) -> 'a list -> 'b list Lwt.t
val mapi_s : (int -> 'a -> 'b Lwt.t) -> 'a list -> 'b list Lwt.t
val fold_left_s : ('a -> 'b -> 'a Lwt.t) -> 'a -> 'b list -> 'a Lwt.t
val filter_map_s : ('a -> 'b option Lwt.t) -> 'a list -> 'b list Lwt.t
val filter_map_p : ('a -> 'b option Lwt.t) -> 'a list -> 'b list Lwt.t
