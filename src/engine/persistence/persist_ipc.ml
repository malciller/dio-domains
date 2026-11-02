(** Out-of-process persistence transport.

    The engine renders a store file and hands the bytes to [dio-persist], which owns the
    fsync and the rename. Only that disk work leaves the process: the JSON is still built
    by the store, because the helper has no schema for it. What moves out is the fsync and
    rename latency, plus the helper's own allocation, which otherwise lands in the same
    OCaml 5 runtime — and a GC pause stops every domain in its runtime.

    Liveness contract — the engine never blocks on the helper:

    - At [init], if no helper socket is reachable, this module stays disabled and
      {!is_enabled} reports [false]; the caller then uses the in-process writer, which is
      exactly the previous behaviour. Dev boxes and tests therefore need no daemon.
    - Once a helper was reachable, a later failure drops the save and increments
      {!dropped_save_count}. Every store is latest-wins, so a dropped intermediate save is
      superseded; durability is degraded, never the trading loop.
    - Only [flush] (shutdown path) waits, and only for a bounded time.

    Wire format, one record per message, all integers big-endian:

    {v
      uint32 body_len
      uint8  kind            (0 = write, 1 = flush)
      kind 0: uint32 name_len, name bytes, contents bytes
      kind 1: (empty)
    v }

    [contents] carries no trailing newline: the helper terminates the file, so the engine
    does not build a second copy of the document just to append one character. The helper
    answers a flush with one byte so an orderly restart cannot lose the last save. *)

let default_socket = "/tmp/dio-persist.sock"

(* Test hook: overrides the environment lookup so a test does not have to mutate the
   process environment (which is unsafe under OCaml 5 multi-domain). *)
let socket_override : string option ref = ref None
let set_socket_path path = socket_override := Some path

let socket_path () =
  match !socket_override with
  | Some path -> path
  | None ->
    (match Sys.getenv_opt "DIO_PERSIST_SOCK" with
     | Some path when path <> "" -> path
     | _ -> default_socket)
;;

type client =
  { fd : Unix.file_descr
  ; mutable alive : bool
  }

let client : client option ref = ref None
let client_mutex = Mutex.create ()

(* Read from any store domain without taking the client lock, so it must be atomic rather
   than a plain ref. *)
let enabled = Atomic.make false
let dropped_saves = Atomic.make 0

(** Cumulative saves dropped because the helper was gone or its socket full. *)
let dropped_save_count () = Atomic.get dropped_saves

let is_enabled () = Atomic.get enabled

let close_client c =
  c.alive <- false;
  try Unix.close c.fd with
  | _ -> ()
;;

let connect_nb () =
  let fd = Unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  try
    Unix.connect fd (Unix.ADDR_UNIX (socket_path ()));
    Unix.set_nonblock fd;
    Unix.set_close_on_exec fd;
    Some { fd; alive = true }
  with
  | _ ->
    (try Unix.close fd with
     | _ -> ());
    None
;;

(** Writes [frame] completely. A partial write leaves the stream unparseable, so the
    caller must close the connection: there is no resynchronisation point inside a record. *)
let write_frame c frame =
  let len = Bytes.length frame in
  let rec loop off =
    if off >= len
    then true
    else (
      match Unix.write c.fd frame off (len - off) with
      | 0 -> false
      | n -> loop (off + n)
      | exception Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK), _, _) -> false
      | exception _ -> false)
  in
  loop 0
;;

let drop_save reason =
  let n = Atomic.fetch_and_add dropped_saves 1 + 1 in
  (* Power-of-two rate limit: a dead helper fails every save, and an unthrottled WARN
     would turn degraded durability into a log storm on the trading process. *)
  if n land (n - 1) = 0
  then
    Logging.warn_f
      ~section:"persist_ipc"
      "%d state save(s) dropped (%s); durability is degraded until dio-persist returns"
      n
      reason
;;

let ensure_client () =
  match !client with
  | Some c when c.alive -> Some c
  | _ ->
    (match connect_nb () with
     | Some c ->
       client := Some c;
       Atomic.set enabled true;
       Some c
     | None ->
       client := None;
       None)
;;

(** Hand one store file to the helper. The helper owns the file terminator, so [contents]
    carries no trailing newline. A helper that was never attached, or a dropped save
    (counted in {!dropped_save_count}), is logged and otherwise ignored: the caller does
    not fall back to an in-process write once attached, because that would put back the
    fsync this path exists to remove. *)
let send ~filename ~contents =
  Mutex.lock client_mutex;
  if Atomic.get enabled
  then (
    match ensure_client () with
    | None -> drop_save "helper unreachable"
    | Some c ->
      let name_len = String.length filename in
      let body_len = 5 + name_len + String.length contents in
      let frame = Bytes.create (4 + body_len) in
      Bytes.set_int32_be frame 0 (Int32.of_int body_len);
      Bytes.set frame 4 '\000';
      Bytes.set_int32_be frame 5 (Int32.of_int name_len);
      Bytes.blit_string filename 0 frame 9 name_len;
      Bytes.blit_string contents 0 frame (9 + name_len) (String.length contents);
      if not (write_frame c frame)
      then (
        close_client c;
        client := None;
        drop_save "socket full or closed"));
  Mutex.unlock client_mutex
;;

(** Attempt to attach to a running helper. Call once at startup. *)
let init () =
  Mutex.lock client_mutex;
  if not (Atomic.get enabled)
  then (
    match connect_nb () with
    | Some c ->
      client := Some c;
      Atomic.set enabled true
    | None -> ());
  Mutex.unlock client_mutex
;;

(** Shutdown path: flush and stop the helper. Bounded — a dead helper costs at most the
    socket timeout and never blocks shutdown indefinitely. *)
let flush () =
  Mutex.lock client_mutex;
  (match !client with
   | Some c when c.alive ->
     let frame = Bytes.create 5 in
     Bytes.set_int32_be frame 0 1l;
     Bytes.set frame 4 '\001';
     if write_frame c frame
     then (
       try
         Unix.clear_nonblock c.fd;
         Unix.setsockopt_float c.fd Unix.SO_RCVTIMEO 2.0;
         let ack = Bytes.create 1 in
         ignore (Unix.read c.fd ack 0 1)
       with
       | _ -> ())
   | _ -> ());
  (match !client with
   | Some c -> close_client c
   | None -> ());
  client := None;
  Atomic.set enabled false;
  Mutex.unlock client_mutex
;;

(* ---- helper side --------------------------------------------------------- *)

let rec read_exactly fd buf off len =
  if len = 0
  then true
  else (
    match Unix.read fd buf off len with
    | 0 -> false
    | n -> read_exactly fd buf (off + n) (len - n)
    | exception Unix.Unix_error (Unix.EINTR, _, _) -> read_exactly fd buf off len
    | exception _ -> false)
;;

(** Atomic write: temp file -> flush -> fsync -> rename, byte-identical to what the engine
    used to do in process, including the newline terminator the engine no longer sends. *)
let write_atomic path contents =
  let tmp = path ^ ".tmp" in
  let oc = open_out_bin tmp in
  Fun.protect
    ~finally:(fun () -> close_out_noerr oc)
    (fun () ->
      output_string oc contents;
      output_char oc '\n';
      Stdlib.flush oc;
      try Unix.fsync (Unix.descr_of_out_channel oc) with
      | _ -> ());
  Sys.rename tmp path
;;

let handle_connection fd =
  let len_buf = Bytes.create 4 in
  let rec loop () =
    if not (read_exactly fd len_buf 0 4)
    then ()
    else (
      let body_len = Int32.to_int (Bytes.get_int32_be len_buf 0) in
      (* Reject absurd lengths instead of allocating on a corrupt stream. *)
      if body_len <= 0 || body_len > 256 * 1024 * 1024
      then ()
      else (
        let body = Bytes.create body_len in
        if not (read_exactly fd body 0 body_len)
        then ()
        else (
          (match Bytes.get body 0 with
           | '\000' ->
             let name_len = Int32.to_int (Bytes.get_int32_be body 1) in
             if name_len > 0 && name_len <= body_len - 5
             then (
               let name = Bytes.sub_string body 5 name_len in
               let contents =
                 Bytes.sub_string body (5 + name_len) (body_len - 5 - name_len)
               in
               try write_atomic name contents with
               | exn ->
                 prerr_endline
                   (Printf.sprintf
                      "dio-persist: failed to write %s: %s"
                      name
                      (Printexc.to_string exn)))
           | '\001' ->
             (* Flush ack. *)
             (try ignore (Unix.write fd (Bytes.make 1 '\001') 0 1) with
              | _ -> ())
           | _ -> ());
          loop ())))
  in
  loop ()
;;

(** Runs the helper forever. One client at a time (the engine), reconnect-safe. *)
let serve () =
  let path = socket_path () in
  (try Unix.unlink path with
   | _ -> ());
  let sock = Unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.bind sock (Unix.ADDR_UNIX path);
  Unix.listen sock 8;
  prerr_endline (Printf.sprintf "dio-persist: listening on %s" path);
  let rec accept_loop () =
    match Unix.accept sock with
    | fd, _ ->
      (try handle_connection fd with
       | _ -> ());
      (try Unix.close fd with
       | _ -> ());
      accept_loop ()
    | exception Unix.Unix_error (Unix.EINTR, _, _) -> accept_loop ()
    | exception _ -> accept_loop ()
  in
  accept_loop ()
;;
