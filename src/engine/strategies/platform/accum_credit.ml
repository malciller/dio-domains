(** Carry profit credited to a parent grid's [accumulated_profit] across domains: the
    overlay and the grid it funds may run in different domains, so they meet on a
    controller-keyed table. One entry per controller; a CAS loop beats a mutex on the
    strategy path. *)

let key ~exchange ~symbol = exchange ^ "|" ^ String.uppercase_ascii symbol

let table : (string * float) list Atomic.t = Atomic.make []

let add ~key qty =
  if Float.is_finite qty && qty > 0.0
  then (
    let rec loop () =
      let old = Atomic.get table in
      let prev = try List.assoc key old with Not_found -> 0.0 in
      let next = (key, prev +. qty) :: List.remove_assoc key old in
      if not (Atomic.compare_and_set table old next) then loop ()
    in
    loop ())
;;

(** Returns the pending credit for [key] and clears it. *)
let drain ~key =
  let rec loop () =
    let old = Atomic.get table in
    match List.assoc_opt key old with
    | None -> 0.0
    | Some v ->
      if Atomic.compare_and_set table old (List.remove_assoc key old) then v else loop ()
  in
  loop ()
;;
