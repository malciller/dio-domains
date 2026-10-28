(** Carry profit credited to a parent grid's [accumulated_profit] across domains: the
    overlay and the grid it funds may run in different domains, so they meet on a
    controller-keyed table. One entry per controller; a CAS loop beats a mutex on the
    strategy path.

    Each credit carries the recycle sequence it belongs to. The parent keeps the highest
    sequence it has applied, so a restart that replays a close (the carry state can be
    saved before the credit and reloaded after) is dropped by the same guard the persisted
    store uses. *)

let key ~exchange ~symbol = exchange ^ "|" ^ String.uppercase_ascii symbol

(** key -> [(seq, qty)], newest last. At most a handful of entries: the parent drains
    every cycle. *)
let table : (string * (int * float) list) list Atomic.t = Atomic.make []

let add ~key ~seq qty =
  if Float.is_finite qty && qty <> 0.0
  then (
    let rec loop () =
      let old = Atomic.get table in
      let prev =
        try List.assoc key old with
        | Not_found -> []
      in
      let next = (key, prev @ [ seq, qty ]) :: List.remove_assoc key old in
      if not (Atomic.compare_and_set table old next) then loop ()
    in
    loop ())
;;

(** Returns the pending credits for [key], oldest first, and clears them. *)
let drain ~key =
  let rec loop () =
    let old = Atomic.get table in
    match List.assoc_opt key old with
    | None -> []
    | Some v ->
      if Atomic.compare_and_set table old (List.remove_assoc key old) then v else loop ()
  in
  loop ()
;;
