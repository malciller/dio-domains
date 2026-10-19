type 'a t =
  { mutable value : 'a option
  ; mutable takers : 'a Lwt.u list
  ; mutable putters : ('a * unit Lwt.u) list
  }

let create_empty () = { value = None; takers = []; putters = [] }
let is_empty mvar = mvar.value = None

let take mvar =
  match mvar.value with
  | Some value ->
    mvar.value <- None;
    (match mvar.putters with
     | (next, resolver) :: rest ->
       mvar.putters <- rest;
       mvar.value <- Some next;
       Lwt.wakeup resolver ()
     | [] -> ());
    Lwt.return value
  | None ->
    (match mvar.putters with
     | (value, resolver) :: rest ->
       mvar.putters <- rest;
       Lwt.wakeup resolver ();
       Lwt.return value
     | [] ->
       let promise, resolver = Lwt.task () in
       mvar.takers <- mvar.takers @ [ resolver ];
       Lwt.on_cancel promise (fun () ->
         mvar.takers <- List.filter (fun taker -> taker != resolver) mvar.takers);
       promise)
;;

let put mvar value =
  match mvar.takers with
  | taker :: rest ->
    mvar.takers <- rest;
    Lwt.wakeup taker value;
    Lwt.return_unit
  | [] ->
    if mvar.value = None
    then (
      mvar.value <- Some value;
      Lwt.return_unit)
    else (
      let promise, resolver = Lwt.task () in
      mvar.putters <- mvar.putters @ [ value, resolver ];
      Lwt.on_cancel promise (fun () ->
        mvar.putters <- List.filter (fun (_, r) -> r != resolver) mvar.putters);
      promise)
;;
