(** Domain-safe one-bit doorbell.

    A producer on any domain rings it; a single waiter on the Lwt domain wakes as soon as
    the pipe becomes readable. The byte carries no payload: a ring means "do the work
    now", and repeated rings coalesce while one byte is pending. A ring that lands during
    the work leaves a byte pending, so the waiter runs again immediately afterwards.

    [Lwt_condition] is not safe off the Lwt domain, so a self-pipe is used instead: a
    non-blocking byte write from a parse/trading domain wakes [Lwt_unix.wait_read] without
    touching Lwt internals. One doorbell has one waiter. *)

open Lwt.Infix

type t =
  { read_fd : Unix.file_descr
  ; write_fd : Unix.file_descr
  ; lwt_read_fd : Lwt_unix.file_descr
  ; pending : bool Atomic.t
  ; byte : Bytes.t
  }

let create () =
  let read_fd, write_fd = Unix.pipe ~cloexec:true () in
  Unix.set_nonblock read_fd;
  Unix.set_nonblock write_fd;
  { read_fd
  ; write_fd
  ; lwt_read_fd = Lwt_unix.of_unix_file_descr ~blocking:false ~set_flags:false read_fd
  ; pending = Atomic.make false
  ; byte = Bytes.make 1 '\x00'
  }
;;

(** Ring from any domain. Coalesces: while a byte is still pending, later rings only set
    the caller's predicate. Never allocates. *)
let ring t =
  if not (Atomic.exchange t.pending true)
  then (
    try ignore (Unix.write t.write_fd t.byte 0 1) with
    | Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK), _, _) -> ()
    | _ -> ())
;;

let drain t =
  let buf = Bytes.create 64 in
  let rec go () =
    try
      let n = Unix.read t.read_fd buf 0 64 in
      if n > 0 then go ()
    with
    | Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK), _, _) -> ()
    | _ -> ()
  in
  go ()
;;

(** Block until the next ring. [pending] is cleared before draining so a producer that
    rings in the window between the wake and the read observes [false], writes its own
    byte, and is either drained here or caught by the caller's predicate re-check. *)
let wait t =
  Lwt_unix.wait_read t.lwt_read_fd
  >>= fun () ->
  Atomic.set t.pending false;
  drain t;
  Lwt.return_unit
;;
