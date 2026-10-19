(** In-tree OpenSSL binding for the TLS client.

    OpenSSL remains the native TLS implementation. This module owns the OCaml surface used
    by [Lwt_ssl] and the conduit layer. *)

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

external init_raw : unit -> unit = "dio_ssl_init"

external create_context_raw
  :  protocol
  -> context_type
  -> context
  = "dio_ssl_create_context"

external set_min_protocol_version_raw
  :  context
  -> protocol
  -> unit
  = "dio_ssl_set_min_protocol_version"

external set_max_protocol_version_raw
  :  context
  -> protocol
  -> unit
  = "dio_ssl_set_max_protocol_version"

external set_default_verify_paths_raw
  :  context
  -> bool
  = "dio_ssl_set_default_verify_paths"

external set_verify_raw
  :  context
  -> verify_mode list
  -> (bool -> unit) option
  -> unit
  = "dio_ssl_set_verify"

external embed_socket_raw : Unix.file_descr -> context -> socket = "dio_ssl_embed_socket"

external set_client_sni_hostname_raw
  :  socket
  -> string
  -> unit
  = "dio_ssl_set_client_sni_hostname"

external set_hostflags_raw
  :  socket
  -> x509_check_flag list
  -> unit
  = "dio_ssl_set_hostflags"

external set_host_raw : socket -> string -> unit = "dio_ssl_set_host"
external connect_raw : socket -> int = "dio_ssl_connect_raw"
external accept_raw : socket -> int = "dio_ssl_accept_raw"
external read_raw : socket -> bytes -> int -> int -> int = "dio_ssl_read"
external write_raw : socket -> bytes -> int -> int -> int = "dio_ssl_write"

external read_into_bigarray_raw
  :  socket
  -> bigarray
  -> int
  -> int
  -> int
  = "dio_ssl_read_into_bigarray"

external write_bigarray_raw
  :  socket
  -> bigarray
  -> int
  -> int
  -> int
  = "dio_ssl_write_bigarray"

external close_notify_raw : socket -> bool = "dio_ssl_close_notify"
external shutdown_raw : socket -> unit = "dio_ssl_shutdown"

external file_descr_of_socket_raw
  :  socket
  -> Unix.file_descr
  = "dio_ssl_file_descr_of_socket"

external get_error_string : unit -> string = "dio_ssl_get_error_string"

let init () = init_raw ()
let create_context protocol type_ = create_context_raw protocol type_
let set_min_protocol_version ctx protocol = set_min_protocol_version_raw ctx protocol
let set_max_protocol_version ctx protocol = set_max_protocol_version_raw ctx protocol
let set_default_verify_paths ctx = set_default_verify_paths_raw ctx
let set_verify ctx modes callback = set_verify_raw ctx modes callback
let embed_socket fd ctx = embed_socket_raw fd ctx
let set_client_SNI_hostname socket host = set_client_sni_hostname_raw socket host
let set_hostflags socket flags = set_hostflags_raw socket flags
let set_host socket host = set_host_raw socket host
let file_descr_of_socket socket = file_descr_of_socket_raw socket

let error_of_code = function
  | 1 -> Error_want_read
  | 2 -> Error_want_write
  | 3 -> Error_zero_return
  | 4 -> Error_syscall
  | _ -> Error_ssl
;;

let check_connect code = if code <> 0 then raise (Connection_error (error_of_code code))
let check_accept code = if code <> 0 then raise (Accept_error (error_of_code code))
let check_io wrap code = if code >= 0 then code else raise (wrap (error_of_code (-code)))
let connect socket = check_connect (connect_raw socket)
let accept socket = check_accept (accept_raw socket)

let read socket buf pos len =
  check_io (fun e -> Read_error e) (read_raw socket buf pos len)
;;

let read_into_bigarray socket buf pos len =
  check_io (fun e -> Read_error e) (read_into_bigarray_raw socket buf pos len)
;;

let write socket buf pos len =
  check_io (fun e -> Write_error e) (write_raw socket buf pos len)
;;

let write_bigarray socket buf pos len =
  check_io (fun e -> Write_error e) (write_bigarray_raw socket buf pos len)
;;

let close_notify socket = close_notify_raw socket
let shutdown socket = shutdown_raw socket
