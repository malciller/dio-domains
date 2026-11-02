(* Event loop for the in-tree promise runtime.
 *
 * Responsibility split:
 *   - this module owns the watcher registry: [id -> event], per-fd watcher lists,
 *     and a timer min-heap.
 *   - a poller owns only "which fds are ready right now".
 *
 * [DIO_IO_BACKEND=select|epoll] picks the poller. [select] is the reference
 * implementation, the rollback path, and the only backend on non-Linux hosts;
 * [epoll] removes the per-iteration fd-list rebuild, the O(n·m) readiness
 * matching, and the FD_SETSIZE ceiling.
 *
 * Both pollers share every line of registry, heap and dispatch logic below, so
 * only the readiness syscall differs between them. *)

type event =
  | Timer of float * (unit -> unit)
  | Readable of Unix.file_descr * (unit -> unit)
  | Writable of Unix.file_descr * (unit -> unit)

(* Authoritative id -> event map. Timers also use it for lazy heap deletion: a cancelled
   timer stays in the heap and is discarded when it surfaces. *)
let events : (int, event) Hashtbl.t = Hashtbl.create 64
let next_id = ref 0

let fresh_id () =
  incr next_id;
  !next_id
;;

(* ---- timer min-heap ------------------------------------------------------ *)

type timer =
  { t_at : float
  ; t_id : int
  ; t_action : unit -> unit
  }

let dummy_timer = { t_at = 0.0; t_id = -1; t_action = (fun () -> ()) }
let heap : timer array ref = ref (Array.make 32 dummy_timer)
let heap_len = ref 0

let heap_grow () =
  let next = Array.make (Array.length !heap * 2) dummy_timer in
  Array.blit !heap 0 next 0 !heap_len;
  heap := next
;;

let heap_push tm =
  if !heap_len = Array.length !heap then heap_grow ();
  let h = !heap in
  let rec up i =
    if i = 0
    then h.(0) <- tm
    else (
      let p = (i - 1) / 2 in
      if h.(p).t_at > tm.t_at
      then (
        h.(i) <- h.(p);
        up p)
      else h.(i) <- tm)
  in
  up !heap_len;
  incr heap_len
;;

let heap_pop () =
  let n = !heap_len in
  if n = 0
  then None
  else (
    let h = !heap in
    let top = h.(0) in
    let count = n - 1 in
    heap_len := count;
    if count > 0
    then (
      let last = h.(count) in
      let rec down i =
        let l = (2 * i) + 1 in
        if l >= count
        then h.(i) <- last
        else (
          let r = l + 1 in
          let c = if r < count && h.(r).t_at < h.(l).t_at then r else l in
          if h.(c).t_at < last.t_at
          then (
            h.(i) <- h.(c);
            down c)
          else h.(i) <- last)
      in
      down 0);
    Some top)
;;

let heap_peek () = if !heap_len = 0 then None else Some !heap.(0)

(* Skips entries whose timer was cancelled while it sat in the heap. Because the heap is
   ordered by [t_at] over live *and* dead entries, a dead entry at the top is always safe
   to drop: it is the smallest pending deadline. *)
let rec next_timer_time () =
  match heap_peek () with
  | None -> None
  | Some tm ->
    if Hashtbl.mem events tm.t_id
    then Some tm.t_at
    else (
      ignore (heap_pop ());
      next_timer_time ())
;;

let pop_expired now =
  let rec loop acc =
    match heap_peek () with
    | Some tm when tm.t_at <= now ->
      ignore (heap_pop ());
      if Hashtbl.mem events tm.t_id
      then (
        Hashtbl.remove events tm.t_id;
        loop ((tm.t_id, tm.t_action) :: acc))
      else loop acc
    | _ -> List.rev acc
  in
  loop []
;;

(* ---- fd watcher indexes -------------------------------------------------- *)

type watcher =
  { w_id : int
  ; w_action : unit -> unit
  }

let read_index : (Unix.file_descr, watcher list ref) Hashtbl.t = Hashtbl.create 64
let write_index : (Unix.file_descr, watcher list ref) Hashtbl.t = Hashtbl.create 64

let has_readers fd =
  match Hashtbl.find_opt read_index fd with
  | Some cell -> !cell <> []
  | None -> false
;;

let has_writers fd =
  match Hashtbl.find_opt write_index fd with
  | Some cell -> !cell <> []
  | None -> false
;;

(* 1 = readable interest, 2 = writable interest. Mirrors epoll_stubs.c. *)
let mask_of_fd fd = (if has_readers fd then 1 else 0) lor if has_writers fd then 2 else 0

(* ---- self-pipe ----------------------------------------------------------- *)

let wake_read, wake_write = Unix.pipe ~cloexec:true ()
let () = Unix.set_nonblock wake_read
let () = Unix.set_nonblock wake_write
let wake_byte = Bytes.make 1 '\000'

let wake () =
  try ignore (Unix.write wake_write wake_byte 0 1) with
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

(* ---- pollers ------------------------------------------------------------- *)

module type POLLER = sig
  val name : string

  (* Re-derive this fd's interest from the watcher indexes. *)
  val sync : Unix.file_descr -> unit

  (* Fds ready within [timeout] seconds; negative means block indefinitely. *)
  val wait : timeout:float -> (Unix.file_descr * int) array

  (* Cheap startup probe: true only if this backend can actually do ADD/MOD/DEL on this
     kernel. *)
  val self_test : unit -> bool
end

module Select_poller : POLLER = struct
  let name = "select"

  (* select is stateless here: the watcher indexes are the source of truth and get
     converted to fd sets per call. *)
  let sync _ = ()
  let self_test () = true

  let wait ~timeout =
    let rfds = Hashtbl.fold (fun fd _ acc -> fd :: acc) read_index [] in
    let wfds = Hashtbl.fold (fun fd _ acc -> fd :: acc) write_index [] in
    let ready_r, ready_w, _ =
      try Unix.select (wake_read :: rfds) wfds [] timeout with
      | Unix.Unix_error (Unix.EINTR, _, _) -> [], [], []
    in
    let out = ref [] in
    List.iter (fun fd -> out := (fd, 1) :: !out) ready_r;
    List.iter (fun fd -> out := (fd, 2) :: !out) ready_w;
    Array.of_list !out
  ;;
end

module Epoll_poller : POLLER = struct
  let name = "epoll"

  (* Unix.file_descr is an immediate int on Unix; the stubs take it as such. *)
  let fd_int (fd : Unix.file_descr) : int = Obj.magic fd
  let fd_of_int (i : int) : Unix.file_descr = Obj.magic i
  let epfd = lazy (Epoll_stubs.create ())
  let registered : (Unix.file_descr, int) Hashtbl.t = Hashtbl.create 64

  (* Startup probe. The re-ADD is the important step: a DEL/MOD mix-up is otherwise
     silent, because MOD with an empty mask is accepted and leaves the fd registered,
     which turns every later ADD on that fd into EEXIST. *)
  let self_test () =
    if not (Epoll_stubs.supported ())
    then false
    else (
      try
        let e = Epoll_stubs.create () in
        let a, b = Unix.socketpair Unix.PF_UNIX Unix.SOCK_STREAM 0 in
        Fun.protect
          ~finally:(fun () ->
            (try Unix.close a with
             | _ -> ());
            (try Unix.close b with
             | _ -> ());
            try Unix.close (fd_of_int e) with
            | _ -> ())
          (fun () ->
            Epoll_stubs.ctl Epoll_stubs.ctl_add e (fd_int a) 1;
            Epoll_stubs.ctl Epoll_stubs.ctl_mod e (fd_int a) 2;
            Epoll_stubs.ctl Epoll_stubs.ctl_del e (fd_int a) 0;
            Epoll_stubs.ctl Epoll_stubs.ctl_add e (fd_int a) 1;
            Epoll_stubs.ctl Epoll_stubs.ctl_del e (fd_int a) 0;
            true)
      with
      | _ -> false)
  ;;

  let del e fd =
    try Epoll_stubs.ctl Epoll_stubs.ctl_del e (fd_int fd) 0 with
    | Unix.Unix_error (Unix.ENOENT, _, _) -> ()
  ;;

  (* Never fatal. A ctl failure means the kernel and [registered] disagree; the recovery
     below converges them instead of wedging a live connection, which is how one wrong op
     code turned into a permanent reconnect loop. *)
  let apply fd mask =
    let e = Lazy.force epfd in
    match Hashtbl.find_opt registered fd with
    | Some old when old = mask -> ()
    | Some _ ->
      if mask = 0
      then (
        del e fd;
        Hashtbl.remove registered fd)
      else (
        (try Epoll_stubs.ctl Epoll_stubs.ctl_mod e (fd_int fd) mask with
         | Unix.Unix_error (Unix.ENOENT, _, _) ->
           Epoll_stubs.ctl Epoll_stubs.ctl_add e (fd_int fd) mask);
        Hashtbl.replace registered fd mask)
    | None ->
      if mask <> 0
      then (
        (try Epoll_stubs.ctl Epoll_stubs.ctl_add e (fd_int fd) mask with
         | Unix.Unix_error (Unix.EEXIST, _, _) ->
           Epoll_stubs.ctl Epoll_stubs.ctl_mod e (fd_int fd) mask);
        Hashtbl.replace registered fd mask)
  ;;

  (* The wake pipe is registered permanently: it is the only way the loop can be unblocked
     from another thread, so it must not depend on user watchers. [sync wake_read] arms it
     rather than deriving a zero interest from the indexes. *)
  let sync fd = if fd = wake_read then apply wake_read 1 else apply fd (mask_of_fd fd)

  let wait ~timeout =
    let e = Lazy.force epfd in
    let ms =
      if timeout < 0.0
      then -1
      else if timeout = 0.0
      then 0
      else max 0 (int_of_float (timeout *. 1000.0))
    in
    let flat = Epoll_stubs.wait e 256 ms in
    Array.init
      (Array.length flat / 2)
      (fun i -> fd_of_int flat.(2 * i), flat.((2 * i) + 1))
  ;;
end

let epoll_ok = lazy (Epoll_poller.self_test ())

let backend_name =
  match Sys.getenv_opt "DIO_IO_BACKEND" with
  | Some s ->
    (match String.lowercase_ascii s with
     | "select" -> "select"
     | "epoll" ->
       if Lazy.force epoll_ok
       then "epoll"
       else (
         prerr_endline
           "dio: DIO_IO_BACKEND=epoll requested but the epoll self-test failed; using \
            the select backend";
         "select")
     | _ -> if Lazy.force epoll_ok then "epoll" else "select")
  | None -> if Lazy.force epoll_ok then "epoll" else "select"
;;

module P : POLLER =
  (val if String.equal backend_name "epoll"
       then (module Epoll_poller : POLLER)
       else (module Select_poller : POLLER))

let backend () = P.name
let () = if String.equal backend_name "epoll" then P.sync wake_read

(* ---- registration -------------------------------------------------------- *)

let on_timer delay action =
  let id = fresh_id () in
  let at = Unix.gettimeofday () +. max 0.0 delay in
  Hashtbl.replace events id (Timer (at, action));
  heap_push { t_at = at; t_id = id; t_action = action };
  id
;;

let add_watcher index fd id action =
  let cell =
    match Hashtbl.find_opt index fd with
    | Some c -> c
    | None ->
      let c = ref [] in
      Hashtbl.replace index fd c;
      c
  in
  cell := { w_id = id; w_action = action } :: !cell
;;

let on_readable fd action =
  let id = fresh_id () in
  Hashtbl.replace events id (Readable (fd, action));
  add_watcher read_index fd id action;
  P.sync fd;
  id
;;

let on_writable fd action =
  let id = fresh_id () in
  Hashtbl.replace events id (Writable (fd, action));
  add_watcher write_index fd id action;
  P.sync fd;
  id
;;

let remove_watcher index fd id =
  match Hashtbl.find_opt index fd with
  | None -> ()
  | Some cell ->
    cell := List.filter (fun w -> w.w_id <> id) !cell;
    if !cell = [] then Hashtbl.remove index fd
;;

let stop_event id =
  match Hashtbl.find_opt events id with
  | None -> ()
  | Some (Timer _) -> Hashtbl.remove events id
  | Some (Readable (fd, _)) ->
    Hashtbl.remove events id;
    remove_watcher read_index fd id;
    P.sync fd
  | Some (Writable (fd, _)) ->
    Hashtbl.remove events id;
    remove_watcher write_index fd id;
    P.sync fd
;;

(* ---- cross-thread jobs --------------------------------------------------- *)

(* Bounded in practice: only [lwt_unix.detach]/[lwt_preemptive] enqueue here, and each job
   corresponds to an in-flight operation that the caller is already waiting on. Not
   bounded deliberately — dropping a job would strand a promise, which is worse than the
   transient allocation. *)
let foreign_mutex = Mutex.create ()
let foreign_jobs : (unit -> unit) Queue.t = Queue.create ()

let enqueue_job job =
  Mutex.lock foreign_mutex;
  Queue.push job foreign_jobs;
  Mutex.unlock foreign_mutex;
  wake ()
;;

let run_foreign_jobs () =
  (* Drain under the lock, run outside it: a job may enqueue another. *)
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

(* ---- iteration ----------------------------------------------------------- *)

let dispatch index fd =
  match Hashtbl.find_opt index fd with
  | None -> ()
  | Some cell ->
    (* Snapshot: a continuation may stop itself or a sibling (lwt_unix re-registers on
       Retry_read), which mutates [cell] mid-iteration. *)
    let ws = List.rev !cell in
    List.iter
      (fun w ->
        try w.w_action () with
        | exn -> !Lwt.async_exception_hook exn)
      ws
;;

let iter block =
  run_foreign_jobs ();
  let timeout =
    if not block
    then 0.0
    else (
      match next_timer_time () with
      | None -> -1.0
      | Some at -> max 0.0 (at -. Unix.gettimeofday ()))
  in
  let ready = P.wait ~timeout in
  let wake_ready = ref false in
  Array.iter (fun (fd, _) -> if fd = wake_read then wake_ready := true) ready;
  if !wake_ready then drain_wake ();
  Array.iter
    (fun (fd, mask) ->
      if fd <> wake_read
      then (
        if mask land 1 <> 0 then dispatch read_index fd;
        if mask land 2 <> 0 then dispatch write_index fd))
    ready;
  (* Clock read is skipped entirely when no timer is outstanding. *)
  if !heap_len > 0
  then (
    let now = Unix.gettimeofday () in
    List.iter
      (fun (_, action) ->
        try action () with
        | exn -> !Lwt.async_exception_hook exn)
      (pop_expired now));
  run_foreign_jobs ()
;;
