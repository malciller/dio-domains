open Lwt.Infix

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

exception Timeout
exception Retry_read
exception Retry_write

type fd_state =
  | Opened
  | Closed
  | Aborted of exn

type file_descr =
  { fd : Unix.file_descr
  ; mutable state : fd_state
  ; blocking : bool
  ; mutable waiters : (int * (exn -> unit)) list
  }

let check_descriptor fd =
  match fd.state with
  | Opened -> ()
  | Closed -> raise (Unix.Unix_error (Unix.EBADF, "Lwt_unix", ""))
  | Aborted exn -> raise exn
;;

let of_unix_file_descr ?(blocking = true) ?(set_flags = true) fd =
  if set_flags
  then (
    try Unix.set_nonblock fd with
    | _ -> ());
  { fd; state = Opened; blocking; waiters = [] }
;;

let unix_file_descr fd = fd.fd

let socket ?(cloexec = false) domain kind proto =
  let fd = Unix.socket domain kind proto in
  if cloexec
  then (
    try Unix.set_close_on_exec fd with
    | _ -> ());
  (try Unix.set_nonblock fd with
   | _ -> ());
  { fd; state = Opened; blocking = false; waiters = [] }
;;

let readable fd =
  try
    let ready, _, _ = Unix.select [ fd.fd ] [] [] 0.0 in
    ready <> []
  with
  | _ -> false
;;

let writable fd =
  try
    let _, ready, _ = Unix.select [] [ fd.fd ] [] 0.0 in
    ready <> []
  with
  | _ -> false
;;

let stop_waiter fd id =
  Lwt_engine.stop_event id;
  fd.waiters <- List.filter (fun (event_id, _) -> event_id <> id) fd.waiters
;;

let register_action event fd action =
  check_descriptor fd;
  let promise, resolver = Lwt.task () in
  let current = ref None in
  let remove () =
    match !current with
    | Some id ->
      stop_waiter fd id;
      current := None
    | None -> ()
  in
  let rec register direction =
    let id =
      match direction with
      | Read -> Lwt_engine.on_readable fd.fd run
      | Write -> Lwt_engine.on_writable fd.fd run
    in
    current := Some id;
    fd.waiters <- (id, fun exn -> Lwt.wakeup_exn resolver exn) :: fd.waiters
  and run () =
    remove ();
    match
      try Ok (action ()) with
      | exn -> Error exn
    with
    | Ok value -> Lwt.wakeup resolver value
    | Error Retry_read -> register Read
    | Error Retry_write -> register Write
    | Error exn -> Lwt.wakeup_exn resolver exn
  in
  register
    (match event with
     | Read -> Read
     | Write -> Write);
  Lwt.on_cancel promise remove;
  promise
;;

let wrap_syscall direction fd action =
  check_descriptor fd;
  try
    if (direction = Read && readable fd) || (direction = Write && writable fd)
    then Lwt.return (action ())
    else register_action direction fd action
  with
  | Retry_read -> register_action Read fd action
  | Retry_write -> register_action Write fd action
  | Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) ->
    register_action direction fd action
  | Sys_blocked_io -> register_action direction fd action
;;

let read fd buffer pos len =
  if pos < 0 || len < 0 || pos > Bytes.length buffer - len
  then invalid_arg "Lwt_unix.read"
  else wrap_syscall Read fd (fun () -> Unix.read fd.fd buffer pos len)
;;

let write fd buffer pos len =
  if pos < 0 || len < 0 || pos > Bytes.length buffer - len
  then invalid_arg "Lwt_unix.write"
  else wrap_syscall Write fd (fun () -> Unix.write fd.fd buffer pos len)
;;

let wait_read fd =
  check_descriptor fd;
  if readable fd then Lwt.return_unit else register_action Read fd ignore
;;

let wait_write fd =
  check_descriptor fd;
  if writable fd then Lwt.return_unit else register_action Write fd ignore
;;

let sleep delay =
  let promise, resolver = Lwt.task () in
  let id = Lwt_engine.on_timer delay (fun () -> Lwt.wakeup resolver ()) in
  Lwt.on_cancel promise (fun () -> Lwt_engine.stop_event id);
  promise
;;

let timeout delay = sleep delay >>= fun () -> raise Timeout
let with_timeout delay f = Lwt.pick [ timeout delay; Lwt.apply f () ]

let bind fd addr =
  check_descriptor fd;
  try
    Unix.bind fd.fd addr;
    Lwt.return_unit
  with
  | exn -> Lwt.fail exn
;;

let listen fd backlog =
  check_descriptor fd;
  Unix.listen fd.fd backlog
;;

let accept ?(cloexec = false) fd =
  check_descriptor fd;
  wrap_syscall Read fd (fun () ->
    let client, addr = Unix.accept fd.fd in
    if cloexec
    then (
      try Unix.set_close_on_exec client with
      | _ -> ());
    (try Unix.set_nonblock client with
     | _ -> ());
    of_unix_file_descr ~blocking:false ~set_flags:false client, addr)
;;

let connect fd addr =
  check_descriptor fd;
  try
    Unix.connect fd.fd addr;
    Lwt.return_unit
  with
  | Unix.Unix_error ((Unix.EINPROGRESS | Unix.EWOULDBLOCK | Unix.EAGAIN), _, _) ->
    wait_write fd
    >>= fun () ->
    (match Unix.getsockopt_error fd.fd with
     | None -> Lwt.return_unit
     | Some err -> Lwt.fail (Unix.Unix_error (err, "connect", "")))
  | Unix.Unix_error (Unix.EISCONN, _, _) -> Lwt.return_unit
;;

let getsockname fd =
  check_descriptor fd;
  Unix.getsockname fd.fd
;;

let setsockopt fd option value =
  check_descriptor fd;
  Unix.setsockopt fd.fd option value
;;

let shutdown fd command =
  check_descriptor fd;
  Unix.shutdown fd.fd command
;;

let close fd =
  (match fd.state with
   | Opened ->
     fd.state <- Closed;
     List.iter
       (fun (id, fail) ->
         Lwt_engine.stop_event id;
         fail (Unix.Unix_error (Unix.EBADF, "Lwt_unix.close", "")))
       fd.waiters;
     fd.waiters <- [];
     (try Unix.close fd.fd with
      | _ -> ())
   | _ -> ());
  Lwt.return_unit
;;

let abort fd exn =
  match fd.state with
  | Opened ->
    fd.state <- Aborted exn;
    List.iter
      (fun (id, fail) ->
        Lwt_engine.stop_event id;
        fail exn)
      fd.waiters;
    fd.waiters <- [];
    (try Unix.close fd.fd with
     | _ -> ())
  | _ -> ()
;;

let blocking fd = Lwt.return fd.blocking

let getaddrinfo host service options =
  (* Resolve on a helper thread so a slow resolver cannot stall every other fiber; hand
     the result back to the loop thread through the job queue. *)
  let promise, resolver = Lwt.task () in
  ignore
    (Thread.create
       (fun () ->
         let outcome =
           try Ok (Unix.getaddrinfo host service options) with
           | exn -> Error exn
         in
         Lwt_engine.enqueue_job (fun () ->
           match outcome with
           | Ok addresses -> Lwt.wakeup resolver addresses
           | Error exn -> Lwt.wakeup_exn resolver exn))
       ());
  promise
;;
