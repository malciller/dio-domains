(** Observable traces for behavioral equivalence.

    A trace is a cycle-indexed sequence of observations — order intents (compared on
    semantic fields, not wire bytes), post-cycle state, and persistence writes. The
    differential harness compares a reference trace against a candidate trace. *)

type order_intent =
  { oi_symbol : string
  ; oi_side : string
  ; oi_qty : float
  ; oi_price : float
  ; oi_post_only : bool
  ; oi_reduce_only : bool
  ; oi_tif : string option
  }

type obs =
  | Order_intent of order_intent
  | State of (string * Strategy_expr.value) list
  | Persistence of string * string

type cycle =
  { c_index : int
  ; c_obs : obs list
  }

type t = cycle list

let order_intent_eq a b =
  String.equal a.oi_symbol b.oi_symbol
  && String.equal a.oi_side b.oi_side
  && Float.equal a.oi_qty b.oi_qty
  && Float.equal a.oi_price b.oi_price
  && Bool.equal a.oi_post_only b.oi_post_only
  && Bool.equal a.oi_reduce_only b.oi_reduce_only
  && a.oi_tif = b.oi_tif
;;

let string_of_order_intent o =
  Printf.sprintf
    "order(symbol=%s side=%s qty=%.8g price=%.8g post_only=%b reduce_only=%b tif=%s)"
    o.oi_symbol
    o.oi_side
    o.oi_qty
    o.oi_price
    o.oi_post_only
    o.oi_reduce_only
    (match o.oi_tif with
     | Some t -> t
     | None -> "-")
;;

let state_eq a b =
  let sort = List.sort (fun (x, _) (y, _) -> String.compare x y) in
  let a = sort a
  and b = sort b in
  List.length a = List.length b
  && List.for_all2
       (fun (k1, v1) (k2, v2) -> String.equal k1 k2 && Strategy_expr.value_eq v1 v2)
       a
       b
;;

let obs_eq a b =
  match a, b with
  | Order_intent x, Order_intent y -> order_intent_eq x y
  | State x, State y -> state_eq x y
  | Persistence (k1, v1), Persistence (k2, v2) -> String.equal k1 k2 && String.equal v1 v2
  | _ -> false
;;

let string_of_obs = function
  | Order_intent o -> string_of_order_intent o
  | State entries -> "state(" ^ String.concat "," (List.map fst entries) ^ ")"
  | Persistence (k, _) -> "persistence(" ^ k ^ ")"
;;

(** [None] when equal, else a human-readable first-divergence description. *)
let compare (a : t) (b : t) : string option =
  let compare_cycles ca cb =
    let rec obs_loop j = function
      | [], [] -> None
      | [], _ ->
        Some
          (Printf.sprintf
             "cycle %d obs %d: candidate has extra observations"
             ca.c_index
             j)
      | _, [] ->
        Some
          (Printf.sprintf
             "cycle %d obs %d: reference has extra observations"
             ca.c_index
             j)
      | oa :: ra, ob :: rb ->
        if obs_eq oa ob
        then obs_loop (j + 1) (ra, rb)
        else
          Some
            (Printf.sprintf
               "cycle %d obs %d: %s vs %s"
               ca.c_index
               j
               (string_of_obs oa)
               (string_of_obs ob))
    in
    obs_loop 0 (ca.c_obs, cb.c_obs)
  in
  let rec loop i = function
    | [], [] -> None
    | [], _ -> Some (Printf.sprintf "cycle %d: candidate has extra cycles" i)
    | _, [] -> Some (Printf.sprintf "cycle %d: reference has extra cycles" i)
    | ca :: ta, cb :: tb ->
      (match compare_cycles ca cb with
       | Some msg -> Some msg
       | None -> loop (i + 1) (ta, tb))
  in
  loop 0 (a, b)
;;

let equal a b = compare a b = None
