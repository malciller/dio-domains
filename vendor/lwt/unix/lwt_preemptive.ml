(* Cross-thread hand-off: [enqueue_job] schedules work on the thread running the event
   loop; the condition variable releases the caller once the promise resolves. *)

let run_in_main thunk =
  let mutex = Mutex.create () in
  let condition = Condition.create () in
  let result = ref None in
  let publish value =
    Mutex.lock mutex;
    result := Some value;
    Condition.signal condition;
    Mutex.unlock mutex
  in
  Lwt_engine.enqueue_job (fun () ->
    match
      try Ok (thunk ()) with
      | exn -> Error exn
    with
    | Error exn -> publish (Error exn)
    | Ok promise ->
      Lwt.on_any
        promise
        (fun value -> publish (Ok value))
        (fun exn -> publish (Error exn)));
  Mutex.lock mutex;
  while !result = None do
    Condition.wait condition mutex
  done;
  let outcome = Option.get !result in
  Mutex.unlock mutex;
  match outcome with
  | Ok value -> value
  | Error exn -> raise exn
;;

let run_in_main_dont_wait thunk handler =
  Lwt_engine.enqueue_job (fun () ->
    match
      try Ok (thunk ()) with
      | exn -> Error exn
    with
    | Error exn -> handler exn
    | Ok promise -> Lwt.on_any promise (fun _ -> ()) handler)
;;
