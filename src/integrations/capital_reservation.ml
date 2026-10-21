(** Venue-agnostic quote-capital reservations.

    An overlay that needs quote capital for margin can reserve it here. The grid reads the
    total reservation for its exchange and sizes new buys from the remainder. This is a
    dynamic senior claim, not a static split: the owner replaces its amount every cycle
    and releases it when the claim is gone.

    Keys are [exchange ^ "|" ^ owner]. Copy-on-write keeps reads lock-free. *)

let reservations : (string, float) Hashtbl.t Atomic.t = Atomic.make (Hashtbl.create 8)
let key exchange owner = exchange ^ "|" ^ owner

let update f =
  let rec loop () =
    let old = Atomic.get reservations in
    let copy = Hashtbl.copy old in
    f copy;
    if Atomic.compare_and_set reservations old copy then () else loop ()
  in
  loop ()
;;

(** Sets (or replaces) [owner]'s reservation. A non-positive amount removes the claim. *)
let set ~exchange ~owner ~amount =
  update (fun t ->
    if amount > 0.0
    then Hashtbl.replace t (key exchange owner) amount
    else Hashtbl.remove t (key exchange owner))
;;

let release ~exchange ~owner = set ~exchange ~owner ~amount:0.0

(** Total reserved quote for [exchange]. *)
let get_total ~exchange =
  Hashtbl.fold
    (fun k v acc ->
      match String.index_opt k '|' with
      | Some i when String.sub k 0 i = exchange -> acc +. v
      | _ -> acc)
    (Atomic.get reservations)
    0.0
;;

(** Available quote after the senior reservations, clamped at zero. *)
let available ~exchange ~quote = Float.max 0.0 (quote -. get_total ~exchange)
