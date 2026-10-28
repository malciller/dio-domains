(* Base accumulation persistence store.

   Per-strategy opt-in tracking of accumulated base assets and local buy/sell
   cycle profitability. One entry per strategy key
   ("{strategy_name}:{symbol}:{venue}", from config.json's strategy field).

   Persisted fields (data/accumulation_state.json, keyed by strategy key):
   - reserved_base: base accumulated via sell_mult; excluded from sellable balance
   - accumulated_profit: realized local PnL in quote
   - carry_credit_seq: newest carry recycle already credited (restart dedup)
   - last_fill_oid / last_buy_fill_* / last_sell_fill_*: most recent fill refs

   Crash window: an async save may lose the most recent coalesced write, so
   accumulation may under-count profit after a crash. Accepted.

   Opt-in: callers check the per-strategy config flag before save/save_async;
   disabled means zero I/O. *)

let section = "base_accumulation_store"

type t =
  { reserved_base : float
  ; accumulated_profit : float
  ; carry_credit_seq : int
  (** Highest carry recycle sequence already credited to [accumulated_profit]. Rides in
      the same entry as the money so a restart cannot re-credit a close. *)
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
  ; carry_credit_seq = 0
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

let get_int_opt json field =
  let open Yojson.Basic.Util in
  try Some (json |> member field |> to_int) with
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
  ; carry_credit_seq = Option.value (get_int_opt json "carry_credit_seq") ~default:0
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
     ; "carry_credit_seq", `Int t.carry_credit_seq
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

(** Buy fill: update the last-buy reference for the next sell's profitability check. [oid]
    is accepted per the store contract; OID sequencing is owned by the execution layer. *)
let apply_buy_fill t ~price ~qty ~oid =
  let _ = oid in
  { t with last_buy_fill_price = Some price; last_buy_fill_qty = Some qty }
;;

(** Sell fill: profit = (sell_price - last_buy_price) * paired_qty - [fees] (all-in, both
    legs); positive net profit accrues to [accumulated_profit].

    [sell_uses_mult] venues (Kraken) withheld the [(1 - sell_mult)] slice inside the
    order, so [oracle_qty * (1 - sell_mult)] may move into [reserved_base] once profit
    covers its cost. On 1:1 venues (Hyperliquid/Alpaca) the base was sold, not withheld;
    accruing it would reserve base that does not exist. Those keep profit in quote and
    skim a whole lot separately (see [Strategy_events]). [fees] is required: the real
    all-in fee. *)
let apply_sell_fill
  t
  ~price
  ~qty
  ~oid
  ~buffer
  ~sell_mult
  ~oracle_qty
  ~fees
  ?(sell_uses_mult = true)
  ()
  =
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
      if not sell_uses_mult
      then { t with accumulated_profit }
      else (
        let accrued_base = Float.max 0.0 (oracle_qty *. (1.0 -. sell_mult)) in
        let base_cost = accrued_base *. buy_price in
        if accrued_base > 0.0 && accumulated_profit >= base_cost +. buffer
        then
          { t with
            reserved_base = t.reserved_base +. accrued_base
          ; accumulated_profit = accumulated_profit -. base_cost
          }
        else { t with accumulated_profit }))
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

(** Resolve the store key whose symbol segment is [symbol]
    ("{strategy}:{symbol}:{venue}"). When exactly one configured strategy matches the
    symbol, return its canonical key directly (same key [state_key_of_symbol] uses), so
    stale keys left by a strategy rename never shadow the live entry. Otherwise fall back
    to a symbol-segment scan; logs a warning and picks the first when that is ambiguous. *)
let resolve_key_for_symbol ~symbol =
  match Persistence_orchestrator.unique_configured_strategy_for_symbol symbol with
  | Some (strategy, venue) -> Some (key_of ~strategy ~symbol ~venue)
  | None ->
    let matches_symbol k =
      match String.split_on_char ':' k with
      | [ _strategy; sym; _venue ] -> sym = symbol
      | _ -> false
    in
    let matches =
      List.filter matches_symbol (Persistence_orchestrator.keys orchestrator)
    in
    (match matches with
     | [] -> None
     | [ k ] -> Some k
     | k :: _ ->
       Logging.warn_f
         ~section
         "Ambiguous persistence key for symbol %s (%d matches); using %s"
         symbol
         (List.length matches)
         k;
       Some k)
;;

let save ~key t = Persistence_orchestrator.put orchestrator ~key t
let save_async ~key t = Persistence_orchestrator.put_async orchestrator ~key t

(** Adds signed carry PnL to a strategy's persisted [accumulated_profit]. [seq] is the
    recycle sequence: a credit at or below [carry_credit_seq] is a restart replay and is
    dropped. Returns whether it was applied, so the caller can gate its live state on the
    same decision. The strategy's own flush writes the same total, so this cannot
    double-count. *)
let credit_profit ~key ~seq ~qty =
  if Float.is_finite qty && qty <> 0.0
  then
    Persistence_orchestrator.update_async orchestrator ~key (fun current ->
      let t = Option.value current ~default in
      if seq > 0 && seq <= t.carry_credit_seq
      then t, false
      else
        ( { t with
            accumulated_profit = t.accumulated_profit +. qty
          ; carry_credit_seq =
              (if seq > t.carry_credit_seq then seq else t.carry_credit_seq)
          }
        , true ))
  else false
;;

(* -- Legacy migration -------------------------------------------------- *)

(** Import one legacy flat entry: accumulation fields go under a full strategy key when
    exactly one configured strategy matches the symbol, else under "migrated:[{symbol}]"
    (logged either way). Legacy sell levels are ignored here; sell_levels_store has its
    own hook. *)
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
