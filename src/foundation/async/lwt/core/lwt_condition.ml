type 'a t = { mutable waiters : 'a Lwt.u list }

let create () = { waiters = [] }

let wait condition =
  let promise, resolver = Lwt.task () in
  condition.waiters <- resolver :: condition.waiters;
  Lwt.on_cancel promise (fun () ->
    condition.waiters <- List.filter (fun waiter -> waiter != resolver) condition.waiters);
  promise
;;

let signal condition value =
  match condition.waiters with
  | [] -> ()
  | waiter :: rest ->
    condition.waiters <- rest;
    Lwt.wakeup_later waiter value
;;

let broadcast condition value =
  let waiters = List.rev condition.waiters in
  condition.waiters <- [];
  List.iter (fun waiter -> Lwt.wakeup_later waiter value) waiters
;;
