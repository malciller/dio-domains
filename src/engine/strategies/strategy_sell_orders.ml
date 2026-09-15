(** Abstract, array-backed set of live open sell orders (strategy-agnostic): a growable,
    reusable parallel-array store of [(order_id, price, remaining_qty)].

    The previous representation was a fresh [(string * float * float) list] rebuilt every
    strategy cycle. That allocated per order not only the cons cell and the tuple block
    but also a boxed [float] for each of price and qty (a non-float-only tuple boxes its
    floats), plus a boxed float on every [ref] accumulator update. A single scan of an
    N-order feed therefore touched the minor heap ~18 words per order. Storing ids in a
    [string array] and price/qty in [float array]s keeps the doubles unboxed and reuses
    one backing allocation across cycles, so a steady-state scan is allocation-free.

    Semantics mirror the list operations it replaces:
    - [push] appends; iteration is in insertion order (callers did not depend on list
      order - first-match reads assume at most one order per id/price).
    - [remove_by_id] and [replace_first] preserve the relative order of the
      surviving/following elements.
    - [to_list]/[of_list] bridge to the old shape for persistence snapshots, tests, and
      cold paths. *)

type t =
  { mutable ids : string array
  ; mutable prices : float array
  ; mutable qtys : float array
  ; mutable len : int
  }

let create capacity =
  let capacity = max 1 capacity in
  { ids = Array.make capacity ""
  ; prices = Array.make capacity 0.0
  ; qtys = Array.make capacity 0.0
  ; len = 0
  }
;;

let clear t = t.len <- 0
let[@inline] length t = t.len
let[@inline] is_empty t = t.len = 0
let[@inline] get_id t i = t.ids.(i)
let[@inline] get_price t i = t.prices.(i)
let[@inline] get_qty t i = t.qtys.(i)

(** Grow the backing arrays to hold at least [n] elements, preserving the live prefix.
    Doubles capacity so a full feed grows amortised-O(1). *)
let ensure_capacity t n =
  let cap = Array.length t.ids in
  if n > cap
  then (
    let cap' = max n (2 * cap) in
    let ids = Array.make cap' "" in
    let prices = Array.make cap' 0.0 in
    let qtys = Array.make cap' 0.0 in
    Array.blit t.ids 0 ids 0 t.len;
    Array.blit t.prices 0 prices 0 t.len;
    Array.blit t.qtys 0 qtys 0 t.len;
    t.ids <- ids;
    t.prices <- prices;
    t.qtys <- qtys)
;;

let push t id price qty =
  ensure_capacity t (t.len + 1);
  t.ids.(t.len) <- id;
  t.prices.(t.len) <- price;
  t.qtys.(t.len) <- qty;
  t.len <- t.len + 1
;;

let iter t f =
  for i = 0 to t.len - 1 do
    f t.ids.(i) t.prices.(i) t.qtys.(i)
  done
;;

(** Copy [src] over [dst] in place, reusing [dst]'s backing arrays (growing only if
    needed). Used to snapshot/restore the live scan between generations without rebuilding
    a list. Allocates nothing once [dst] has capacity. *)
let blit ~src ~dst =
  ensure_capacity dst src.len;
  Array.blit src.ids 0 dst.ids 0 src.len;
  Array.blit src.prices 0 dst.prices 0 src.len;
  Array.blit src.qtys 0 dst.qtys 0 src.len;
  dst.len <- src.len
;;

let fold t init f =
  let acc = ref init in
  for i = 0 to t.len - 1 do
    acc := f !acc t.ids.(i) t.prices.(i) t.qtys.(i)
  done;
  !acc
;;

let exists_id t id =
  let rec go i = i < t.len && (t.ids.(i) = id || go (i + 1)) in
  go 0
;;

(** Any element whose price satisfies [p]. The price predicate is passed the price only
    (the common "is a live order resting at/below X" test). *)
let exists_price t p =
  let rec go i = i < t.len && (p t.prices.(i) || go (i + 1)) in
  go 0
;;

(** Any element whose price is at or below [x]. Closure-free counterpart to [exists_price]
    for the threshold test (avoids boxing the float into the predicate closure on every
    element). *)
let exists_price_leq t x =
  let rec go i = i < t.len && (t.prices.(i) <= x || go (i + 1)) in
  go 0
;;

let sum_qty t =
  let s = ref 0.0 in
  for i = 0 to t.len - 1 do
    s := !s +. t.qtys.(i)
  done;
  !s
;;

(** First element matching [pred], or [None]. Allocates the result option/tuple; only used
    on cold paths (excess sweep, amend handlers). *)
let find_first t pred =
  let rec go i =
    if i >= t.len
    then None
    else if pred t.ids.(i) t.prices.(i) t.qtys.(i)
    then Some (t.ids.(i), t.prices.(i), t.qtys.(i))
    else go (i + 1)
  in
  go 0
;;

(** Replace the first element matching [pred] with [f] applied to it, leaving the rest
    untouched. Mirrors the old ["List.map" + first-match flag] used to re-key a pending
    sell id on ack. *)
let replace_first t pred f =
  let rec go i =
    if i < t.len
    then
      if pred t.ids.(i) t.prices.(i) t.qtys.(i)
      then (
        let id, price, qty = f t.ids.(i) t.prices.(i) t.qtys.(i) in
        t.ids.(i) <- id;
        t.prices.(i) <- price;
        t.qtys.(i) <- qty)
      else go (i + 1)
  in
  go 0
;;

(** Remove every element whose id equals [id] (ids are unique in practice), preserving the
    order of the survivors. Returns whether anything was removed. In place; no allocation. *)
let remove_by_id t id =
  let w = ref 0 in
  for i = 0 to t.len - 1 do
    if t.ids.(i) <> id
    then (
      if !w <> i
      then (
        t.ids.(!w) <- t.ids.(i);
        t.prices.(!w) <- t.prices.(i);
        t.qtys.(!w) <- t.qtys.(i));
      incr w)
  done;
  let removed = !w < t.len in
  t.len <- !w;
  removed
;;

(** Remove every element whose id starts with [prefix] (temporary "pending_sell_"
    placeholders). In place; no allocation and no closure. *)
let remove_prefix t prefix =
  let w = ref 0 in
  for i = 0 to t.len - 1 do
    if not (String.starts_with ~prefix t.ids.(i))
    then (
      if !w <> i
      then (
        t.ids.(!w) <- t.ids.(i);
        t.prices.(!w) <- t.prices.(i);
        t.qtys.(!w) <- t.qtys.(i));
      incr w)
  done;
  t.len <- !w
;;

let of_list l =
  let t = create (List.length l) in
  List.iter (fun (id, price, qty) -> push t id price qty) l;
  t
;;

let to_list t = List.init t.len (fun i -> t.ids.(i), t.prices.(i), t.qtys.(i))
