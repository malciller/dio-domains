type socket
type uninitialized_socket

val ssl_socket : socket -> Ssl.socket option
val ssl_socket_of_uninitialized_socket : uninitialized_socket -> Ssl.socket
val is_ssl : socket -> bool
val plain : Lwt_unix.file_descr -> socket
val embed_socket : Lwt_unix.file_descr -> Ssl.context -> socket

val embed_uninitialized_socket
  :  Lwt_unix.file_descr
  -> Ssl.context
  -> uninitialized_socket

val ssl_accept : Lwt_unix.file_descr -> Ssl.context -> socket Lwt.t
val ssl_connect : Lwt_unix.file_descr -> Ssl.context -> socket Lwt.t
val ssl_accept_handshake : uninitialized_socket -> socket Lwt.t
val ssl_perform_handshake : uninitialized_socket -> socket Lwt.t
val read : socket -> bytes -> int -> int -> int Lwt.t
val read_bytes : socket -> Lwt_bytes.t -> int -> int -> int Lwt.t
val write : socket -> bytes -> int -> int -> int Lwt.t
val write_bytes : socket -> Lwt_bytes.t -> int -> int -> int Lwt.t
val wait_read : socket -> unit Lwt.t
val wait_write : socket -> unit Lwt.t
val shutdown : socket -> Unix.shutdown_command -> unit
val ssl_shutdown : socket -> unit Lwt.t
val close : socket -> unit Lwt.t
val close_notify : socket -> bool Lwt.t
val abort : socket -> exn -> unit
val shutdown_and_close : socket -> unit Lwt.t
val in_channel_of_descr : ?buffer:Lwt_bytes.t -> socket -> Lwt_io.input_channel
val out_channel_of_descr : ?buffer:Lwt_bytes.t -> socket -> Lwt_io.output_channel
val get_fd : socket -> Lwt_unix.file_descr
val get_unix_fd : socket -> Unix.file_descr
val getsockname : socket -> Unix.sockaddr
val getpeername : socket -> Unix.sockaddr
