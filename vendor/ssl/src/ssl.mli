type context
type socket

type protocol =
  | TLSv1_2
  | TLSv1_3

type context_type =
  | Client_context
  | Server_context
  | Both_context

type verify_mode =
  | Verify_none
  | Verify_peer

type x509_check_flag = No_partial_wildcards

type ssl_error =
  | Error_none
  | Error_ssl
  | Error_want_read
  | Error_want_write
  | Error_syscall
  | Error_zero_return

exception Connection_error of ssl_error
exception Accept_error of ssl_error
exception Read_error of ssl_error
exception Write_error of ssl_error

type bigarray = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

val init : unit -> unit
val create_context : protocol -> context_type -> context
val set_min_protocol_version : context -> protocol -> unit
val set_max_protocol_version : context -> protocol -> unit
val set_default_verify_paths : context -> bool
val set_verify : context -> verify_mode list -> (bool -> unit) option -> unit
val embed_socket : Unix.file_descr -> context -> socket
val set_client_SNI_hostname : socket -> string -> unit
val set_hostflags : socket -> x509_check_flag list -> unit
val set_host : socket -> string -> unit
val connect : socket -> unit
val accept : socket -> unit
val read : socket -> bytes -> int -> int -> int
val read_into_bigarray : socket -> bigarray -> int -> int -> int
val write : socket -> bytes -> int -> int -> int
val write_bigarray : socket -> bigarray -> int -> int -> int
val close_notify : socket -> bool
val shutdown : socket -> unit
val file_descr_of_socket : socket -> Unix.file_descr
val get_error_string : unit -> string
