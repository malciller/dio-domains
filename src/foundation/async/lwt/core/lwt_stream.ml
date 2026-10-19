open Lwt.Infix

exception Closed
exception Full
exception Empty

type 'a t =
  { queue : 'a Queue.t
  ; mutable closed_flag : bool
  ; close_promise : unit Lwt.t
  ; close_resolver : unit Lwt.u
  ; mutable getters : 'a option Lwt.u list
  ; mutable putters : ('a * unit Lwt.u) list
  ; mutable capacity : int option
  ; source : (unit -> 'a option Lwt.t) option
  }

let new_stream ?capacity ?source () =
  let close_promise, close_resolver = Lwt.wait () in
  { queue = Queue.create ()
  ; closed_flag = false
  ; close_promise
  ; close_resolver
  ; getters = []
  ; putters = []
  ; capacity
  ; source
  }
;;

let closed stream = stream.close_promise

let close stream =
  if not stream.closed_flag
  then (
    stream.closed_flag <- true;
    let getters = List.rev stream.getters in
    stream.getters <- [];
    List.iter (fun resolver -> Lwt.wakeup_later resolver None) getters;
    let putters = List.rev stream.putters in
    stream.putters <- [];
    List.iter (fun (_, resolver) -> Lwt.wakeup_later_exn resolver Closed) putters;
    Lwt.wakeup_later stream.close_resolver ())
;;

let push_value stream value =
  if stream.closed_flag
  then false
  else (
    match stream.getters with
    | resolver :: rest ->
      stream.getters <- rest;
      Lwt.wakeup_later resolver (Some value);
      true
    | [] ->
      Queue.push value stream.queue;
      true)
;;

let admit_putters stream =
  let rec loop () =
    match stream.capacity with
    | Some capacity when Queue.length stream.queue < capacity ->
      (match stream.putters with
       | (value, resolver) :: rest ->
         stream.putters <- rest;
         Queue.push value stream.queue;
         Lwt.wakeup_later resolver ();
         loop ()
       | [] -> ())
    | _ -> ()
  in
  loop ()
;;

let get stream =
  if not (Queue.is_empty stream.queue)
  then (
    let value = Queue.pop stream.queue in
    admit_putters stream;
    Lwt.return_some value)
  else (
    match stream.source with
    | Some source ->
      (try source () with
       | exn ->
         close stream;
         Lwt.fail exn)
      >>= (function
       | None ->
         close stream;
         Lwt.return_none
       | Some value -> Lwt.return_some value)
    | None ->
      if stream.closed_flag
      then Lwt.return_none
      else (
        let promise, resolver = Lwt.task () in
        stream.getters <- stream.getters @ [ resolver ];
        Lwt.on_cancel promise (fun () ->
          stream.getters <- List.filter (fun waiter -> waiter != resolver) stream.getters);
        promise))
;;

let get_available stream =
  let acc = ref [] in
  let drain () =
    while not (Queue.is_empty stream.queue) do
      acc := Queue.pop stream.queue :: !acc
    done
  in
  drain ();
  admit_putters stream;
  drain ();
  List.rev !acc
;;

let create () =
  let stream = new_stream () in
  let push = function
    | Some value -> ignore (push_value stream value)
    | None -> close stream
  in
  stream, push
;;

class ['a] bounded_push_impl (stream : 'a t) =
  object
    method size =
      match stream.capacity with
      | Some size -> size
      | None -> 0

    method count = Queue.length stream.queue

    method resize size =
      if size < 0 then invalid_arg "Lwt_stream.bounded_push#resize";
      stream.capacity <- Some size;
      admit_putters stream

    method push value =
      if stream.closed_flag
      then Lwt.fail Closed
      else (
        match stream.getters with
        | resolver :: rest ->
          stream.getters <- rest;
          Lwt.wakeup_later resolver (Some value);
          Lwt.return_unit
        | [] ->
          (match stream.capacity with
           | Some capacity when Queue.length stream.queue >= capacity ->
             let promise, resolver = Lwt.task () in
             stream.putters <- stream.putters @ [ value, resolver ];
             Lwt.on_cancel promise (fun () ->
               stream.putters <- List.filter (fun (_, r) -> r != resolver) stream.putters);
             promise
           | _ ->
             Queue.push value stream.queue;
             Lwt.return_unit))

    method close = close stream
  end

class type ['a] bounded_push = object
  method size : int
  method resize : int -> unit
  method push : 'a -> unit Lwt.t
  method close : unit
  method count : int
end

let create_bounded size =
  if size < 0 then invalid_arg "Lwt_stream.create_bounded";
  let stream = new_stream ~capacity:size () in
  let push_source = new bounded_push_impl stream in
  stream, (push_source :> 'a bounded_push)
;;

let from source = new_stream ~source ()

let of_list values =
  let stream = new_stream () in
  List.iter (fun value -> ignore (push_value stream value)) values;
  close stream;
  stream
;;

let rec iter f stream =
  get stream
  >>= function
  | None -> Lwt.return_unit
  | Some value ->
    f value;
    iter f stream
;;

let rec iter_s f stream =
  get stream
  >>= function
  | None -> Lwt.return_unit
  | Some value -> f value >>= fun () -> iter_s f stream
;;
