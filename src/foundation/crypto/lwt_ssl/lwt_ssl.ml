(** In-tree Lwt wrapper over the OpenSSL binding. *)

open Lwt.Infix

type t =
  | Plain
  | SSL of Ssl.socket

type socket = Lwt_unix.file_descr * t
type uninitialized_socket = Lwt_unix.file_descr * Ssl.socket

let ssl_socket (_, kind) =
  match kind with
  | Plain -> None
  | SSL socket -> Some socket
;;

let ssl_socket_of_uninitialized_socket (_, socket) = socket

let is_ssl (_, kind) =
  match kind with
  | Plain -> false
  | SSL _ -> true
;;

let map_retry = function
  | Ssl.Connection_error e | Ssl.Accept_error e | Ssl.Read_error e | Ssl.Write_error e ->
    (match e with
     | Ssl.Error_want_read -> raise Lwt_unix.Retry_read
     | Ssl.Error_want_write -> raise Lwt_unix.Retry_write
     | Ssl.Error_none | Ssl.Error_ssl | Ssl.Error_syscall | Ssl.Error_zero_return ->
       raise (Ssl.Connection_error e))
  | exn -> raise exn
;;

let wrap_call f () =
  try f () with
  | (Ssl.Connection_error _ | Ssl.Accept_error _ | Ssl.Read_error _ | Ssl.Write_error _)
    as exn -> map_retry exn
;;

let repeat_call fd f =
  Lwt_unix.blocking fd
  >>= fun _ ->
  try Lwt.return (wrap_call f ()) with
  | Lwt_unix.Retry_read -> Lwt_unix.register_action Lwt_unix.Read fd (wrap_call f)
  | Lwt_unix.Retry_write -> Lwt_unix.register_action Lwt_unix.Write fd (wrap_call f)
  | exn -> Lwt.fail exn
;;

let plain fd = fd, Plain
let embed_socket fd ctx = fd, SSL (Ssl.embed_socket (Lwt_unix.unix_file_descr fd) ctx)

let embed_uninitialized_socket fd ctx =
  fd, Ssl.embed_socket (Lwt_unix.unix_file_descr fd) ctx
;;

let ssl_accept fd ctx =
  let socket = Ssl.embed_socket (Lwt_unix.unix_file_descr fd) ctx in
  repeat_call fd (fun () -> Ssl.accept socket) >|= fun () -> fd, SSL socket
;;

let ssl_connect fd ctx =
  let socket = Ssl.embed_socket (Lwt_unix.unix_file_descr fd) ctx in
  repeat_call fd (fun () -> Ssl.connect socket) >|= fun () -> fd, SSL socket
;;

let ssl_accept_handshake (fd, socket) =
  repeat_call fd (fun () -> Ssl.accept socket) >|= fun () -> fd, SSL socket
;;

let ssl_perform_handshake (fd, socket) =
  repeat_call fd (fun () -> Ssl.connect socket) >|= fun () -> fd, SSL socket
;;

let read (fd, kind) buf pos len =
  match kind with
  | Plain -> Lwt_unix.read fd buf pos len
  | SSL socket ->
    if len = 0
    then Lwt.return 0
    else
      repeat_call fd (fun () ->
        try Ssl.read socket buf pos len with
        | Ssl.Read_error Ssl.Error_zero_return -> 0)
;;

let read_bytes (fd, kind) buf pos len =
  match kind with
  | Plain -> Lwt_bytes.read fd buf pos len
  | SSL socket ->
    if len = 0
    then Lwt.return 0
    else
      repeat_call fd (fun () ->
        try Ssl.read_into_bigarray socket buf pos len with
        | Ssl.Read_error Ssl.Error_zero_return -> 0)
;;

let write (fd, kind) buf pos len =
  match kind with
  | Plain -> Lwt_unix.write fd buf pos len
  | SSL socket ->
    if len = 0
    then Lwt.return 0
    else repeat_call fd (fun () -> Ssl.write socket buf pos len)
;;

let write_bytes (fd, kind) buf pos len =
  match kind with
  | Plain -> Lwt_bytes.write fd buf pos len
  | SSL socket ->
    if len = 0
    then Lwt.return 0
    else repeat_call fd (fun () -> Ssl.write_bigarray socket buf pos len)
;;

let wait_read (fd, kind) =
  match kind with
  | Plain -> Lwt_unix.wait_read fd
  | SSL _ -> Lwt.pause ()
;;

let wait_write (fd, kind) =
  match kind with
  | Plain -> Lwt_unix.wait_write fd
  | SSL _ -> Lwt.pause ()
;;

let ssl_shutdown (fd, kind) =
  match kind with
  | Plain -> Lwt.return_unit
  | SSL socket -> repeat_call fd (fun () -> Ssl.shutdown socket)
;;

let shutdown (fd, _) cmd = Lwt_unix.shutdown fd cmd

let close_notify = function
  | (_, Plain) as s ->
    shutdown s Unix.SHUTDOWN_SEND;
    Lwt.return_true
  | fd, SSL socket -> repeat_call fd (fun () -> Ssl.close_notify socket)
;;

let close (fd, _) = Lwt_unix.close fd
let abort (fd, _) = Lwt_unix.abort fd

let shutdown_and_close s =
  ssl_shutdown s
  >>= fun () -> Lwt.wrap2 shutdown s Unix.SHUTDOWN_ALL >>= fun () -> close s
;;

let out_channel_of_descr ?buffer s =
  Lwt_io.make
    ?buffer
    ~mode:Lwt_io.output
    ~close:(fun () -> shutdown_and_close s)
    (fun buf pos len -> write_bytes s buf pos len)
;;

let in_channel_of_descr ?buffer s =
  Lwt_io.make
    ?buffer
    ~mode:Lwt_io.input
    ~close:(fun () -> shutdown_and_close s)
    (fun buf pos len -> read_bytes s buf pos len)
;;

let get_fd (fd, _) = fd

let get_unix_fd (fd, kind) =
  match kind with
  | Plain -> Lwt_unix.unix_file_descr fd
  | SSL socket -> Ssl.file_descr_of_socket socket
;;

let getsockname s = Unix.getsockname (get_unix_fd s)
let getpeername s = Unix.getpeername (get_unix_fd s)
