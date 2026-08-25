(* Base accumulation persistence store.

   Per-strategy opt-in tracking of accumulated base assets and local
   buy/sell cycle profitability. One entry per strategy key
   ("{strategy_name}:{symbol}:{venue}", synthesized from config.json's
   strategy field).

   Persisted fields (data/accumulation_state.json, keyed by strategy key):
   - reserved_base: base asset accumulated via sell_mult; excluded from sellable balance
   - accumulated_profit: realized local PnL denominated in quote
   - last_fill_oid / last_buy_fill_* / last_sell_fill_*: most recent fill references

   Crash-window semantics: async saves may lose the most recent coalesced
   write, so accumulation may under-count profit after a crash. Accepted.

   Opt-in: callers consult the per-strategy config flag before invoking
   save/save_async; disabled means zero I/O. *)

let section = "base_accumulation_store"

type t =
  { reserved_base : float
  ; accumulated_profit : float
  ; last_fill_oid : string option
  ; last_buy_fill_price : float option
  ; last_buy_fill_qty : float option
  ; last_sell_fill_price : float option
  ; last_sell_fill_qty : float option
  }

type key = string

(** Synthesizes "{strategy_name}:{symbol}:{venue}" from config.json fields. *)
let key_of ~strategy ~symbol ~venue = Printf.sprintf "%s:%s:%s" strategy symbol venue

let default =
  { reserved_base = 0.0
  ; accumulated_profit = 0.0
  ; last_fill_oid = None
  ; last_buy_fill_price = None
  ; last_buy_fill_qty = None
  ; last_sell_fill_price = None
  ; last_sell_fill_qty = None
  }
;;

(* -- Serialization --------------------------------------------------- *)

let get_float_opt json field =
  let open Yojson.Basic.Util in
  try Some (json |> member field |> to_float) with
  | _ -> None
;;

let get_string_opt json field =
  let open Yojson.Basic.Util in
  try Some (json |> member field |> to_string) with
  | _ -> None
;;

let opt_field name = function
  | Some v -> [ name, `Float v ]
  | None -> []
;;

let of_json json =
  let f name default =
    match get_float_opt json name with
    | Some v -> v
    | None -> default
  in
  { reserved_base = f "reserved_base" 0.0
  ; accumulated_profit = f "accumulated_profit" 0.0
  ; last_fill_oid = get_string_opt json "last_fill_oid"
  ; last_buy_fill_price = get_float_opt json "last_buy_fill_price"
  ; last_buy_fill_qty = get_float_opt json "last_buy_fill_qty"
  ; last_sell_fill_price = get_float_opt json "last_sell_fill_price"
  ; last_sell_fill_qty = get_float_opt json "last_sell_fill_qty"
  }
;;

let to_json t =
  `Assoc
    ([ "reserved_base", `Float t.reserved_base
     ; "accumulated_profit", `Float t.accumulated_profit
     ]
     @ (match t.last_fill_oid with
        | Some oid -> [ "last_fill_oid", `String oid ]
        | None -> [])
     @ opt_field "last_buy_fill_price" t.last_buy_fill_price
     @ opt_field "last_buy_fill_qty" t.last_buy_fill_qty
     @ opt_field "last_sell_fill_price" t.last_sell_fill_price
     @ opt_field "last_sell_fill_qty" t.last_sell_fill_qty)
;;

let orchestrator =
  Persistence_orchestrator.create
    ~filename:"accumulation_state.json"
    ~parse:(fun tree ->
      let open Yojson.Basic.Util in
      try tree |> to_assoc |> List.map (fun (k, v) -> k, of_json v) with
      | _ -> [])
    ~serialize:to_json
;;

(* Pure decision logic: no I/O, unit-testable *)

(** Buy fill: update the last-buy reference info for the next sell's
    profitability check. [oid] is accepted per the store contract; OID
    sequencing itself is owned by the execution layer. *)
let apply_buy_fill t ~price ~qty ~oid =
  let _ = oid in
  { t with last_buy_fill_price = Some price; last_buy_fill_qty = Some qty }
;;

(** Sell fill: compare against the last buy fill for profitability.
    profit = (sell_price - last_buy_price) * paired qty; the optional [fees]
    (all-inclusive: both legs) are subtracted; if net profit > 0 it is added
    to accumulated_profit. When accumulated_profit covers the acquisition
    cost of the reserved base plus [buffer] (realtime, fear-and-greed driven):
    reserved_base += oracle_qty * (1 - sell_mult) and accumulated_profit is
    debited by the base cost (accumulated_profit <- accumulated_profit - base_cost),
    preserving the buffer and surplus profit in the quote ledger.
    [fees] defaults to 0.0 so the pure spec formula holds exactly. *)
let apply_sell_fill t ~price ~qty ~oid ~buffer ~sell_mult ~oracle_qty ?(fees = 0.0) () =
  let t =
    { t with
      last_sell_fill_price = Some price
    ; last_sell_fill_qty = Some qty
    ; last_fill_oid = Some oid
    }
  in
  match t.last_buy_fill_price with
  | Some buy_price when buy_price > 0.0 && buy_price < price ->
    let paired_qty =
      match t.last_buy_fill_qty with
      | Some q when q > 0.0 -> Float.min q qty
      | _ -> qty
    in
    let net_profit = ((price -. buy_price) *. paired_qty) -. fees in
    if net_profit > 0.0
    then (
      let accumulated_profit = t.accumulated_profit +. net_profit in
      let accrued_base = Float.max 0.0 (oracle_qty *. (1.0 -. sell_mult)) in
      let base_cost = accrued_base *. buy_price in
      if accrued_base > 0.0 && accumulated_profit >= base_cost +. buffer
      then (
        { t with
          reserved_base = t.reserved_base +. accrued_base
        ; accumulated_profit = accumulated_profit -. base_cost
        })
      else { t with accumulated_profit })
    else t
  | _ -> t
;;

(* -- Persistence ------------------------------------------------------ *)

(** Loads the entry for [key]. Returns defaults when absent or disabled. *)
let load ~key =
  match Persistence_orchestrator.load orchestrator ~key with
  | Some t -> t
  | None -> default
;;

(** Resolves the unique store key whose symbol segment is [symbol]
    ("{strategy}:{symbol}:{venue}"). Used during hydration before the strategy
    name is known; logs loudly when ambiguous. *)
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

let save ~key t = Persistence_orchestrator.put orchestrator ~key t
let save_async ~key t = Persistence_orchestrator.put_async orchestrator ~key t

(* -- Legacy migration -------------------------------------------------- *)

(** Imports one legacy flat entry: accumulation fields go under a full
    strategy key when exactly one configured strategy matches the symbol,
    else under "migrated:{symbol}" (logged loudly either way). Sell levels in
    the legacy entry are ignored here - sell_levels_store has its own hook. *)
let migrate_entry symbol json =
  let open Yojson.Basic.Util in
  let has field = json |> member field <> `Null in
  if has "reserved_base" || has "accumulated_profit"
  then (
    let strategy_key =
      match Persistence_orchestrator.unique_configured_strategy_for_symbol symbol with
      | Some (strategy, venue) -> key_of ~strategy ~symbol ~venue
      | None ->
        Logging.warn_f
          ~section
          "No unique configured strategy matches legacy symbol %s; migrating under \
           'migrated:%s'"
          symbol
          symbol;
        "migrated:" ^ symbol
    in
    save ~key:strategy_key (of_json json);
    Logging.info_f ~section "Migrated accumulation state for %s -> %s" symbol strategy_key)
;;

let () = Persistence_orchestrator.register_migrate_hook migrate_entry
