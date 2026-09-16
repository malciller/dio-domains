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
  ; mutable sum_q : float
  (** Running sum of [qtys.(0..len-1)], maintained on every mutation so [sum_qty] is an
      O(1) read on the hot path instead of a per-cycle fold. *)
  ; mutable min_p : float
  (** Running minimum of [prices.(0..len-1)] ([Float.infinity] when empty), so
      [exists_price_leq] is an O(1) comparison instead of a per-cycle scan. Recomputed
      O(n) only when the current minimum is removed. *)
  }

let create capacity =
  let capacity = max 1 capacity in
  { ids = Array.make capacity ""
  ; prices = Array.make capacity 0.0
  ; qtys = Array.make capacity 0.0
  ; len = 0
  ; sum_q = 0.0
  ; min_p = Float.infinity
  }
;;

(** Recompute the O(n) aggregates from scratch. Only used on the cold mutators (prefix
    removal, first-match replace) and when the minimum leaves. Accumulates in a
    [float array] (unboxed cells) rather than [float ref]s or a recursive float-threaded
    loop: a boxed accumulator would allocate ~2 words per element and turn this walk into
    a minor -GC tax every time the closest sell is amended. *)
let recompute_aggregates t =
  let buf = [| 0.0; Float.infinity |] in
  for i = 0 to t.len - 1 do
    buf.(0) <- buf.(0) +. t.qtys.(i);
    if t.prices.(i) < buf.(1) then buf.(1) <- t.prices.(i)
  done;
  t.sum_q <- buf.(0);
  t.min_p <- buf.(1)
;;

let clear t =
  t.len <- 0;
  t.sum_q <- 0.0;
  t.min_p <- Float.infinity
;;

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
  t.len <- t.len + 1;
  t.sum_q <- t.sum_q +. qty;
  if price < t.min_p then t.min_p <- price
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
  dst.len <- src.len;
  dst.sum_q <- src.sum_q;
  dst.min_p <- src.min_p
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

(** Any element whose price is at or below [x]. O(1) via the maintained [min_p]. *)
let exists_price_leq t x = t.min_p <= x

let sum_qty t = t.sum_q

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
        t.qtys.(i) <- qty;
        recompute_aggregates t)
      else go (i + 1)
  in
  go 0
;;

(** Remove every element whose id equals [id] (ids are unique in practice), preserving the
    order of the survivors. Returns whether anything was removed. In place; no allocation. *)
let remove_by_id t id =
  let removed_qty = ref 0.0 in
  let removed_min = ref false in
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
    else (
      removed_qty := !removed_qty +. t.qtys.(i);
      (* [min_p] is the minimum, so a removed price <= it *is* the minimum: recompute. *)
      if t.prices.(i) <= t.min_p then removed_min := true)
  done;
  let removed = !w < t.len in
  t.len <- !w;
  if removed
  then (
    t.sum_q <- t.sum_q -. !removed_qty;
    if !removed_min then recompute_aggregates t);
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
  t.len <- !w;
  recompute_aggregates t
;;

let of_list l =
  let t = create (List.length l) in
  List.iter (fun (id, price, qty) -> push t id price qty) l;
  t
;;

let to_list t = List.init t.len (fun i -> t.ids.(i), t.prices.(i), t.qtys.(i))
