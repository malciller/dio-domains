type t =
  { mutable locked : bool
  ; mutable waiters : unit Lwt.u list
  }

let create () = { locked = false; waiters = [] }

let acquire mutex =
  if mutex.locked
  then (
    let promise, resolver = Lwt.task () in
    mutex.waiters <- mutex.waiters @ [ resolver ];
    Lwt.on_cancel promise (fun () ->
      mutex.waiters <- List.filter (fun waiter -> waiter != resolver) mutex.waiters);
    promise)
  else (
    mutex.locked <- true;
    Lwt.return_unit)
;;

let release mutex =
  match mutex.waiters with
  | [] ->
    mutex.locked <- false;
    Lwt.return_unit
  | waiter :: rest ->
    mutex.waiters <- rest;
    Lwt.wakeup waiter ();
    Lwt.return_unit
;;

let with_lock mutex f =
  Lwt.bind (acquire mutex) (fun () -> Lwt.finalize f (fun () -> release mutex))
;;
