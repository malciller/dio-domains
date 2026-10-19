type t = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

val create : int -> t
val length : t -> int
val unsafe_blit : t -> int -> t -> int -> int -> unit
val read : Lwt_unix.file_descr -> t -> int -> int -> int Lwt.t
val write : Lwt_unix.file_descr -> t -> int -> int -> int Lwt.t
