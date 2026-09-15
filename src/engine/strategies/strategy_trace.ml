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

let value_to_json (v : Strategy_expr.value) : Yojson.Basic.t =
  match v with
  | V_none -> `Null
  | V_float f -> `Float f
  | V_int i -> `Int i
  | V_bool b -> `Bool b
  | V_string s -> `String s
;;

let value_of_json (j : Yojson.Basic.t) : Strategy_expr.value =
  match j with
  | `Null -> V_none
  | `Float f -> V_float f
  | `Int i -> V_int i
  | `Bool b -> V_bool b
  | `String s -> V_string s
  | _ -> V_none
;;

let order_intent_to_json o =
  `Assoc
    [ "symbol", `String o.oi_symbol
    ; "side", `String o.oi_side
    ; "qty", `Float o.oi_qty
    ; "price", `Float o.oi_price
    ; "post_only", `Bool o.oi_post_only
    ; "reduce_only", `Bool o.oi_reduce_only
    ; ( "tif"
      , match o.oi_tif with
        | Some t -> `String t
        | None -> `Null )
    ]
;;

let order_intent_of_json j =
  let open Yojson.Basic.Util in
  let opt_str = function
    | `String s -> Some s
    | _ -> None
  in
  { oi_symbol = j |> member "symbol" |> to_string
  ; oi_side = j |> member "side" |> to_string
  ; oi_qty = j |> member "qty" |> to_float
  ; oi_price = j |> member "price" |> to_float
  ; oi_post_only = j |> member "post_only" |> to_bool
  ; oi_reduce_only = j |> member "reduce_only" |> to_bool
  ; oi_tif = opt_str (j |> member "tif")
  }
;;

let obs_to_json = function
  | Order_intent o -> `Assoc [ "kind", `String "order"; "order", order_intent_to_json o ]
  | State entries ->
    `Assoc
      [ "kind", `String "state"
      ; "entries", `Assoc (List.map (fun (k, v) -> k, value_to_json v) entries)
      ]
  | Persistence (k, v) ->
    `Assoc [ "kind", `String "persistence"; "key", `String k; "value", `String v ]
;;

let obs_of_json j =
  let open Yojson.Basic.Util in
  match j |> member "kind" |> to_string with
  | "order" -> Order_intent (order_intent_of_json (member "order" j))
  | "state" ->
    State (member "entries" j |> to_assoc |> List.map (fun (k, v) -> k, value_of_json v))
  | "persistence" ->
    Persistence (member "key" j |> to_string, member "value" j |> to_string)
  | other -> failwith ("unknown observation kind: " ^ other)
;;

let to_json (t : t) : Yojson.Basic.t =
  `List
    (List.map
       (fun c ->
         `Assoc [ "index", `Int c.c_index; "obs", `List (List.map obs_to_json c.c_obs) ])
       t)
;;

let of_json (j : Yojson.Basic.t) : t =
  let open Yojson.Basic.Util in
  j
  |> to_list
  |> List.map (fun c ->
    { c_index = c |> member "index" |> to_int
    ; c_obs = c |> member "obs" |> to_list |> List.map obs_of_json
    })
;;

let save path t = Yojson.Basic.to_file path (to_json t)
let load path = of_json (Yojson.Basic.from_file path)
