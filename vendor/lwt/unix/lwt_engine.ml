type event =
  | Timer of float * (unit -> unit)
  | Readable of Unix.file_descr * (unit -> unit)
  | Writable of Unix.file_descr * (unit -> unit)

let events : (int, event) Hashtbl.t = Hashtbl.create 64
let next_id = ref 0

let fresh_id () =
  incr next_id;
  !next_id
;;

let on_timer delay action =
  let id = fresh_id () in
  Hashtbl.replace events id (Timer (Unix.gettimeofday () +. max 0.0 delay, action));
  id
;;

let on_readable fd action =
  let id = fresh_id () in
  Hashtbl.replace events id (Readable (fd, action));
  id
;;

let on_writable fd action =
  let id = fresh_id () in
  Hashtbl.replace events id (Writable (fd, action));
  id
;;

let stop_event id = Hashtbl.remove events id

(* Self-pipe used to wake the loop from other threads. *)
let wake_read, wake_write = Unix.pipe ~cloexec:true ()
let () = Unix.set_nonblock wake_read
let () = Unix.set_nonblock wake_write

let wake () =
  let byte = Bytes.make 1 '\000' in
  try ignore (Unix.write wake_write byte 0 1) with
  | Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK), _, _) -> ()
  | Unix.Unix_error _ -> ()
;;

let drain_wake () =
  let buffer = Bytes.create 64 in
  let rec loop () =
    match Unix.read wake_read buffer 0 64 with
    | 0 -> ()
    | _ -> loop ()
    | exception Unix.Unix_error _ -> ()
  in
  loop ()
;;

(* Closures queued from other system threads. *)
let foreign_mutex = Mutex.create ()
let foreign_jobs : (unit -> unit) Queue.t = Queue.create ()

let enqueue_job job =
  Mutex.lock foreign_mutex;
  Queue.push job foreign_jobs;
  Mutex.unlock foreign_mutex;
  wake ()
;;

let run_foreign_jobs () =
  let jobs =
    Mutex.lock foreign_mutex;
    let jobs = Queue.fold (fun acc job -> job :: acc) [] foreign_jobs in
    Queue.clear foreign_jobs;
    Mutex.unlock foreign_mutex;
    List.rev jobs
  in
  List.iter
    (fun job ->
      try job () with
      | exn -> !Lwt.async_exception_hook exn)
    jobs
;;

let collect_ready read_fds write_fds =
  let ready = ref [] in
  Hashtbl.iter
    (fun id event ->
      match event with
      | Timer _ -> ()
      | Readable (fd, action) ->
        if List.mem fd read_fds then ready := (id, action) :: !ready
      | Writable (fd, action) ->
        if List.mem fd write_fds then ready := (id, action) :: !ready)
    events;
  List.rev !ready
;;

let expired_timers now =
  let ready = ref [] in
  Hashtbl.iter
    (fun id event ->
      match event with
      | Timer (at, action) when at <= now -> ready := (id, action) :: !ready
      | _ -> ())
    events;
  List.rev !ready
;;

let next_timer_time () =
  Hashtbl.fold
    (fun _ event acc ->
      match event with
      | Timer (at, _) ->
        (match acc with
         | Some t -> Some (min t at)
         | None -> Some at)
      | _ -> acc)
    events
    None
;;

let iter block =
  run_foreign_jobs ();
  let read_fds = ref []
  and write_fds = ref [] in
  Hashtbl.iter
    (fun _ event ->
      match event with
      | Timer _ -> ()
      | Readable (fd, _) ->
        if not (List.mem fd !read_fds) then read_fds := fd :: !read_fds
      | Writable (fd, _) ->
        if not (List.mem fd !write_fds) then write_fds := fd :: !write_fds)
    events;
  let now = Unix.gettimeofday () in
  let timeout =
    if not block
    then 0.0
    else (
      match next_timer_time () with
      | None -> -1.0
      | Some at -> max 0.0 (at -. now))
  in
  let read_set = wake_read :: !read_fds in
  let ready_read, ready_write, _ =
    try Unix.select read_set !write_fds [] timeout with
    | Unix.Unix_error (Unix.EINTR, _, _) -> [], [], []
  in
  if List.mem wake_read ready_read then drain_wake ();
  let ready_read = List.filter (fun fd -> fd <> wake_read) ready_read in
  let ready = collect_ready ready_read ready_write in
  let expired = expired_timers (Unix.gettimeofday ()) in
  List.iter (fun (id, _) -> Hashtbl.remove events id) ready;
  List.iter (fun (id, _) -> Hashtbl.remove events id) expired;
  List.iter
    (fun (_, action) ->
      try action () with
      | exn -> !Lwt.async_exception_hook exn)
    ready;
  List.iter
    (fun (_, action) ->
      try action () with
      | exn -> !Lwt.async_exception_hook exn)
    expired;
  run_foreign_jobs ()
;;
