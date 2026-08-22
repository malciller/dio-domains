(* Sell levels persistence store.

   Per-strategy opt-in persistence of pending sell orders so they survive
   restarts on venues that drop orders due to time constraints. The store
   only loads/persists levels; reconciliation (fulfilled-vs-unfulfilled
   verification, replacement under contention, adoption of unknown exchange
   orders) stays in the strategy/execution layer (jacobs_ladder_execution.ml).

   Compact [p; q] JSON list form is kept: it keeps the file small and the
   per-save serialization fast for large sell grids. Levels are stored sorted
   price-descending. *)

let section = "sell_levels_store"

type level =
  { price : float
  ; qty : float
  }

type t = level list (* sorted price desc *)
type key = string

let key_of ~strategy ~symbol ~venue = Printf.sprintf "%s:%s:%s" strategy symbol venue
let sort_levels levels = List.sort (fun a b -> Float.compare b.price a.price) levels

(* -- Serialization ------------------------------------------------------ *)

let level_of_json json =
  let open Yojson.Basic.Util in
  (* Compact form: [price, qty]. *)
  try
    match json with
    | `List [ p; q ] -> Some { price = to_float p; qty = to_float q }
    | _ ->
      (* Legacy verbose form: {"price": p, "qty": q}. *)
      Some
        { price = json |> member "price" |> to_float
        ; qty = json |> member "qty" |> to_float
        }
  with
  | _ -> None
;;

let of_json json =
  let open Yojson.Basic.Util in
  try json |> to_list |> List.filter_map level_of_json |> sort_levels with
  | _ -> []
;;

let to_json t = `List (List.map (fun l -> `List [ `Float l.price; `Float l.qty ]) t)

let orchestrator =
  Persistence_orchestrator.create
    ~filename:"sell_levels_state.json"
    ~parse:(fun tree ->
      let open Yojson.Basic.Util in
      try tree |> to_assoc |> List.map (fun (k, v) -> k, of_json v) with
      | _ -> [])
    ~serialize:to_json
;;

(* -- Persistence -------------------------------------------------------- *)

(** Loads the levels for [key]. Returns [] when absent or disabled. *)
let load ~key =
  match Persistence_orchestrator.load orchestrator ~key with
  | Some t -> t
  | None -> []
;;

(** Resolves the unique store key whose symbol segment is [symbol]; logs
    loudly when ambiguous. *)
let resolve_key_for_symbol ~symbol =
  let matches_symbol k =
    match String.split_on_char ':' k with
    | [ _strategy; sym; _venue ] -> sym = symbol
    | _ -> false
  in
  let matches = List.filter matches_symbol (Persistence_orchestrator.keys orchestrator) in
  match matches with
  | [] -> None
  | [ k ] -> Some k
  | k :: _ ->
    Logging.warn_f
      ~section
      "Ambiguous persistence key for symbol %s (%d matches); using %s"
      symbol
      (List.length matches)
      k;
    Some k
;;

let save_async ~key t = Persistence_orchestrator.put_async orchestrator ~key (sort_levels t)

(** Adopts an order that exists on the exchange but not in memory (also covers
    the async-save crash window). Deduplicates by price-tolerance match. *)
let adopt_exchange_order ~key level =
  let current = load ~key in
  let tol = level.price *. 0.0001 in
  let is_dup l =
    abs_float (l.price -. level.price) <= tol
    || abs_float (l.price -. level.price) <= 1e-4
  in
  if List.for_all (fun l -> not (is_dup l)) current
  then save_async ~key (sort_levels (level :: current))
;;

let price_qty_match l r =
  abs_float (l.price -. r.price) <= max (l.price *. 0.0001) 1e-4
  && abs_float (l.qty -. r.qty) <= max (l.qty *. 0.0001) 1e-4
;;

(** Removes the given levels from the store (verified filled or removed on the
    exchange). Furthest-out orders first is a caller decision. *)
let remove_levels ~key ~(levels : t) =
  let remaining =
    List.filter (fun l -> not (List.exists (price_qty_match l) levels)) (load ~key)
  in
  save_async ~key remaining
;;

(** Replaces a set of old levels with new ones in one write. *)
let replace_levels ~key ~(old : t) ~(new_ : t) =
  let remaining =
    List.filter (fun l -> not (List.exists (price_qty_match l) old)) (load ~key)
  in
  save_async ~key (sort_levels (List.rev_append new_ remaining))
;;

let save_async ~key t = Persistence_orchestrator.put_async orchestrator ~key (sort_levels t)

(* -- Legacy migration ----------------------------------------------------- *)

(** Imports the legacy entry's "sell_levels" field under a full strategy key
    when exactly one configured strategy matches the symbol, else under
    "migrated:{symbol}". *)
let migrate_entry symbol json =
  let open Yojson.Basic.Util in
  match json |> member "sell_levels" with
  | `Null -> ()
  | levels_json ->
    let strategy_key =
      match Persistence_orchestrator.unique_configured_strategy_for_symbol symbol with
      | Some (strategy, venue) -> key_of ~strategy ~symbol ~venue
      | None -> "migrated:" ^ symbol
    in
    save_async ~key:strategy_key (of_json levels_json);
    Logging.info_f ~section "Migrated sell levels for %s -> %s" symbol strategy_key
;;

let () = Persistence_orchestrator.register_migrate_hook migrate_entry
