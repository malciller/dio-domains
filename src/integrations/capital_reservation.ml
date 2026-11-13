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

(** True when [k] is a [exchange|...] key. Compared in place: the table is keyed by
    [exchange ^ "|" ^ owner], and slicing the exchange out allocated once per reservation on
    every read - the fact refresh reads this on every event. *)
let key_belongs_to ~exchange k =
  let n = String.length exchange in
  String.length k > n
  && Char.equal (String.unsafe_get k n) '|'
  && String.starts_with ~prefix:exchange k
;;

(** Total reserved quote for [exchange]. *)
let get_total ~exchange =
  Hashtbl.fold
    (fun k v acc -> if key_belongs_to ~exchange k then acc +. v else acc)
    (Atomic.get reservations)
    0.0
;;

(** Available quote after the senior reservations, clamped at zero. *)
let available ~exchange ~quote = Float.max 0.0 (quote -. get_total ~exchange)
