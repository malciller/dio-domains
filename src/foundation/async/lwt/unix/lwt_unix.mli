(** Unix event loop and I/O for the in-tree promise runtime. *)

type file_descr

exception Timeout
exception Retry_read
exception Retry_write

type io_event =
  | Read
  | Write

type sockaddr = Unix.sockaddr =
  | ADDR_UNIX of string
  | ADDR_INET of Unix.inet_addr * int

type socket_bool_option = Unix.socket_bool_option =
  | SO_DEBUG
  | SO_BROADCAST
  | SO_REUSEADDR
  | SO_KEEPALIVE
  | SO_DONTROUTE
  | SO_OOBINLINE
  | SO_ACCEPTCONN
  | TCP_NODELAY
  | IPV6_ONLY
  | SO_REUSEPORT

(* File descriptors *)
val of_unix_file_descr
  :  ?blocking:bool
  -> ?set_flags:bool
  -> Unix.file_descr
  -> file_descr

val unix_file_descr : file_descr -> Unix.file_descr
val socket : ?cloexec:bool -> Unix.socket_domain -> Unix.socket_type -> int -> file_descr
val bind : file_descr -> Unix.sockaddr -> unit Lwt.t
val listen : file_descr -> int -> unit
val accept : ?cloexec:bool -> file_descr -> (file_descr * Unix.sockaddr) Lwt.t
val connect : file_descr -> Unix.sockaddr -> unit Lwt.t
val getsockname : file_descr -> Unix.sockaddr
val setsockopt : file_descr -> socket_bool_option -> bool -> unit
val shutdown : file_descr -> Unix.shutdown_command -> unit
val close : file_descr -> unit Lwt.t
val abort : file_descr -> exn -> unit
val blocking : file_descr -> bool Lwt.t

(* I/O *)
val read : file_descr -> bytes -> int -> int -> int Lwt.t
val write : file_descr -> bytes -> int -> int -> int Lwt.t
val wait_read : file_descr -> unit Lwt.t
val wait_write : file_descr -> unit Lwt.t
val register_action : io_event -> file_descr -> (unit -> 'a) -> 'a Lwt.t

(* Time *)
val sleep : float -> unit Lwt.t
val timeout : float -> 'a Lwt.t
val with_timeout : float -> (unit -> 'a Lwt.t) -> 'a Lwt.t

(* Name resolution *)
val getaddrinfo
  :  string
  -> string
  -> Unix.getaddrinfo_option list
  -> Unix.addr_info list Lwt.t
