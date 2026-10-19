(** In-tree promise runtime. See [lwt.mli].

    The public promise type is covariant and its element type is erased from the mutable
    cell. That keeps [Lwt.return_none] polymorphic despite the value restriction, exactly
    as upstream Lwt achieves with an abstract public type. Element values are re-typed at
    the few points where they cross the cell. *)

type 'a resolved = ('a, exn) result

type cancel_action =
  | Not_cancelable
  | Cancel_self
  | Propagate of (unit -> unit) list

type +'a t =
  { mutable state : Obj.t option
  ; mutable callbacks : (Obj.t -> unit) list
  ; mutable cancel_action : cancel_action
  ; mutable cancel_hooks : (unit -> unit) list
  }

type 'a u = { promise : 'a t }

exception Canceled

let async_exception_hook =
  ref (fun exn ->
    prerr_string "Fatal error: exception ";
    prerr_string (Printexc.to_string exn);
    prerr_char '\n';
    flush stderr)
;;

let callback_depth = ref 0
let max_callback_depth = 42
let deferred : (unit -> unit) Queue.t = Queue.create ()

let rec run_callback f =
  incr callback_depth;
  (try f () with
   | exn -> !async_exception_hook exn);
  decr callback_depth;
  if !callback_depth = 0 then drain_deferred ()

and drain_deferred () =
  if Queue.is_empty deferred
  then ()
  else (
    let f = Queue.pop deferred in
    run_callback f)

and call_callback_or_defer ?(max_depth = max_callback_depth) f =
  if !callback_depth < max_depth then run_callback f else Queue.push f deferred
;;

let state_of (p : 'a t) : 'a resolved option =
  match p.state with
  | None -> None
  | Some packed -> Some (Obj.obj packed)
;;

let add_callback p (cb : 'a resolved -> unit) =
  match p.state with
  | Some packed -> call_callback_or_defer (fun () -> cb (Obj.obj packed))
  | None -> p.callbacks <- (fun packed -> cb (Obj.obj packed)) :: p.callbacks
;;

let resolve p res =
  match p.state with
  | Some _ -> ()
  | None ->
    let packed = Obj.repr res in
    p.state <- Some packed;
    let callbacks = List.rev p.callbacks in
    p.callbacks <- [];
    List.iter (fun cb -> call_callback_or_defer (fun () -> cb packed)) callbacks
;;

let run_hooks hooks =
  List.iter
    (fun f ->
      try f () with
      | exn -> !async_exception_hook exn)
    (List.rev hooks)
;;

let rec cancel p =
  match p.state with
  | Some _ -> ()
  | None ->
    let hooks = p.cancel_hooks in
    p.cancel_hooks <- [];
    (match p.cancel_action with
     | Not_cancelable -> ()
     | Cancel_self ->
       resolve p (Error Canceled);
       run_hooks hooks
     | Propagate thunks ->
       resolve p (Error Canceled);
       run_hooks hooks;
       List.iter
         (fun f ->
           try f () with
           | exn -> !async_exception_hook exn)
         thunks)

and add_cancel_hook p f =
  match state_of p with
  | Some (Error Canceled) ->
    (try f () with
     | exn -> !async_exception_hook exn)
  | Some _ -> ()
  | None -> p.cancel_hooks <- f :: p.cancel_hooks

and link q r =
  if q.state <> None
  then ()
  else (
    match state_of r with
    | Some res -> resolve q res
    | None ->
      add_cancel_hook q (fun () -> cancel r);
      add_callback r (fun res -> resolve q res))
;;

let new_promise () =
  { state = None; callbacks = []; cancel_action = Not_cancelable; cancel_hooks = [] }
;;

let new_task () =
  { state = None; callbacks = []; cancel_action = Cancel_self; cancel_hooks = [] }
;;

let resolved_result res =
  { state = Some (Obj.repr res)
  ; callbacks = []
  ; cancel_action = Not_cancelable
  ; cancel_hooks = []
  }
;;

let return v = resolved_result (Ok v)
let return_unit = return ()
let return_none = return None
let return_some v = return (Some v)
let return_true = return true
let return_false = return false
let fail exn = resolved_result (Error exn)
let fail_with msg = fail (Failure msg)

let bind p f =
  let q = new_promise () in
  q.cancel_action <- Propagate [ (fun () -> cancel p) ];
  let complete x =
    if q.state = None
    then (
      try link q (f x) with
      | exn -> resolve q (Error exn))
  in
  (match state_of p with
   | Some (Ok x) -> call_callback_or_defer (fun () -> complete x)
   | Some (Error exn) -> resolve q (Error exn)
   | None ->
     add_callback p (function
       | Ok x -> complete x
       | Error exn -> resolve q (Error exn)));
  q
;;

let map f p = bind p (fun x -> return (f x))

let try_bind thunk on_ok on_error =
  let q = new_promise () in
  let apply_handler handler arg =
    if q.state = None
    then (
      try link q (handler arg) with
      | exn -> resolve q (Error exn))
  in
  (try
     let p = thunk () in
     q.cancel_action <- Propagate [ (fun () -> cancel p) ];
     match state_of p with
     | Some (Ok x) -> call_callback_or_defer (fun () -> apply_handler on_ok x)
     | Some (Error exn) -> call_callback_or_defer (fun () -> apply_handler on_error exn)
     | None ->
       add_callback p (function
         | Ok x -> apply_handler on_ok x
         | Error exn -> apply_handler on_error exn)
   with
   | exn -> call_callback_or_defer (fun () -> apply_handler on_error exn));
  q
;;

let catch thunk handler = try_bind thunk return handler

let finalize thunk cleanup =
  let q = new_promise () in
  let run_cleanup result =
    let after_cleanup cleanup_result =
      match cleanup_result with
      | Ok () -> resolve q result
      | Error exn -> resolve q (Error exn)
    in
    try
      let cp = cleanup () in
      q.cancel_action <- Propagate [ (fun () -> cancel cp) ];
      match state_of cp with
      | Some r -> call_callback_or_defer (fun () -> after_cleanup r)
      | None -> add_callback cp after_cleanup
    with
    | exn -> resolve q (Error exn)
  in
  (try
     let p = thunk () in
     q.cancel_action <- Propagate [ (fun () -> cancel p) ];
     match state_of p with
     | Some r -> call_callback_or_defer (fun () -> run_cleanup r)
     | None -> add_callback p run_cleanup
   with
   | exn -> call_callback_or_defer (fun () -> run_cleanup (Error exn)));
  q
;;

let wrap2 f a b =
  try return (f a b) with
  | exn -> fail exn
;;

let apply f x =
  try f x with
  | exn -> fail exn
;;

let reraise exn = raise exn

let backtrace_bind _ _ rethrow p f =
  try_bind
    (fun () -> p)
    f
    (fun exn ->
      try fail (rethrow exn) with
      | exn -> fail exn)
;;

let backtrace_try_bind _ _ rethrow thunk on_ok on_error =
  try_bind thunk on_ok (fun exn ->
    try on_error (rethrow exn) with
    | exn -> fail exn)
;;

let wait () =
  let p = new_promise () in
  p, { promise = p }
;;

let task () =
  let p = new_task () in
  p, { promise = p }
;;

let wakeup_general api r res =
  let p = r.promise in
  match state_of p with
  | Some (Error Canceled) -> ()
  | Some _ -> invalid_arg api
  | None -> resolve p res
;;

let wakeup r v = wakeup_general "Lwt.wakeup" r (Ok v)
let wakeup_later r v = wakeup_general "Lwt.wakeup_later" r (Ok v)
let wakeup_exn r exn = wakeup_general "Lwt.wakeup_exn" r (Error exn)
let wakeup_later_exn r exn = wakeup_general "Lwt.wakeup_later_exn" r (Error exn)
let is_sleeping p = p.state = None

let both p1 p2 =
  let q = new_promise () in
  q.cancel_action <- Propagate [ (fun () -> cancel p1); (fun () -> cancel p2) ];
  let v1 = ref None
  and v2 = ref None
  and done1 = ref false
  and done2 = ref false
  and first_error = ref None in
  let finish () =
    if !done1 && !done2
    then (
      match !first_error with
      | Some exn -> resolve q (Error exn)
      | None -> resolve q (Ok (Option.get !v1, Option.get !v2)))
  in
  let on1 = function
    | Ok v ->
      v1 := Some v;
      done1 := true;
      finish ()
    | Error exn ->
      (match !first_error with
       | Some _ -> ()
       | None -> first_error := Some exn);
      done1 := true;
      finish ()
  in
  let on2 = function
    | Ok v ->
      v2 := Some v;
      done2 := true;
      finish ()
    | Error exn ->
      (match !first_error with
       | Some _ -> ()
       | None -> first_error := Some exn);
      done2 := true;
      finish ()
  in
  (match state_of p1 with
   | Some r -> call_callback_or_defer (fun () -> on1 r)
   | None -> add_callback p1 on1);
  (match state_of p2 with
   | Some r -> call_callback_or_defer (fun () -> on2 r)
   | None -> add_callback p2 on2);
  q
;;

let join ps =
  let q = new_promise () in
  q.cancel_action <- Propagate (List.map (fun p () -> cancel p) ps);
  if ps = []
  then resolve q (Ok ())
  else (
    let remaining = ref (List.length ps)
    and first_error = ref None in
    let finish () =
      if !remaining = 0
      then (
        match !first_error with
        | Some exn -> resolve q (Error exn)
        | None -> resolve q (Ok ()))
    in
    List.iter
      (fun p ->
        let on = function
          | Ok () ->
            decr remaining;
            finish ()
          | Error exn ->
            (match !first_error with
             | Some _ -> ()
             | None -> first_error := Some exn);
            decr remaining;
            finish ()
        in
        match state_of p with
        | Some r -> call_callback_or_defer (fun () -> on r)
        | None -> add_callback p on)
      ps);
  q
;;

let choose ps =
  match ps with
  | [] -> invalid_arg "Lwt.choose"
  | _ ->
    let q = new_promise () in
    q.cancel_action <- Propagate (List.map (fun p () -> cancel p) ps);
    let already_resolved = List.filter_map (fun p -> state_of p) ps in
    (match
       List.find_opt
         (function
           | Error _ -> true
           | Ok _ -> false)
         already_resolved
     with
     | Some res -> resolve q res
     | None ->
       (match already_resolved with
        | res :: _ -> resolve q res
        | [] ->
          List.iter
            (fun p -> add_callback p (fun res -> if q.state = None then resolve q res))
            ps));
    q
;;

let pick ps =
  let q = choose ps in
  add_callback q (fun _ -> List.iter (fun p -> if is_sleeping p then cancel p) ps);
  q
;;

let on_any p on_ok on_error =
  match state_of p with
  | Some (Ok v) -> call_callback_or_defer (fun () -> on_ok v)
  | Some (Error exn) -> call_callback_or_defer (fun () -> on_error exn)
  | None ->
    add_callback p (function
      | Ok v -> on_ok v
      | Error exn -> on_error exn)
;;

let on_cancel p f =
  match state_of p with
  | Some (Error Canceled) ->
    (try f () with
     | exn -> !async_exception_hook exn)
  | Some _ -> ()
  | None -> p.cancel_hooks <- f :: p.cancel_hooks
;;

let async thunk =
  match
    try Ok (thunk ()) with
    | exn -> Error exn
  with
  | Error exn -> !async_exception_hook exn
  | Ok p -> on_any p (fun _ -> ()) (fun exn -> !async_exception_hook exn)
;;

let dont_wait thunk handler =
  match
    try Ok (thunk ()) with
    | exn -> Error exn
  with
  | Error exn -> handler exn
  | Ok p -> on_any p (fun _ -> ()) handler
;;

(* Paused promises are resolved once per engine iteration. *)
let paused : unit t list ref = ref []

let pause () =
  let p = new_task () in
  paused := p :: !paused;
  add_cancel_hook p (fun () -> paused := List.filter (fun q -> q != p) !paused);
  p
;;

let wakeup_paused () =
  let ps = List.rev !paused in
  paused := [];
  List.iter (fun p -> resolve p (Ok ())) ps
;;

let paused_count () = List.length !paused
let abandon_paused () = paused := []

let poll p =
  match state_of p with
  | Some (Ok v) -> Some v
  | Some (Error exn) -> raise exn
  | None -> None
;;

module Infix = struct
  let ( >>= ) = bind
  let ( >|= ) p f = map f p
end
