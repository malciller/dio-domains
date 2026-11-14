(* Base accumulation persistence store.

   Per-strategy opt-in tracking of accumulated base assets and local buy/sell
   cycle profitability. One entry per strategy key
   ("{strategy_name}:{symbol}:{venue}", from config.json's strategy field).

   Persisted fields (data/accumulation_state.json, keyed by strategy key):
   - reserved_base: base accumulated via sell_mult; excluded from sellable balance
   - accumulated_profit: realized local PnL in quote
   - carry_credit_seq: newest carry recycle already credited (restart dedup)
   - buy_lots: unspent buy fills, newest first, consumed one per credited sell
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
  ; buy_lots : (float * float) list
  (** Unspent buy fills, newest first as [(price, qty)], consumed one per credited sell.
      Sharing one last-buy anchor across a run of buys credits the ladder step once per
      sell too many. *)
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
  ; buy_lots = []
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

(** Reads [[price, qty], ...]. Malformed rows are dropped rather than failing the entry. *)
let get_lots json field =
  let open Yojson.Basic.Util in
  try
    json
    |> member field
    |> to_list
    |> List.filter_map (fun lot ->
      try
        match lot |> to_list with
        | p :: q :: _ ->
          let p = to_float p
          and q = to_float q in
          if Float.is_finite p && Float.is_finite q && p > 0.0 && q > 0.0
          then Some (p, q)
          else None
        | _ -> None
      with
      | _ -> None)
  with
  | _ -> []
;;

(** True when [field] is present and not JSON null. [member] raises on a non-assoc entry,
    and the caller's parse would then drop every entry in the file. *)
let has_field json field =
  try json |> Yojson.Basic.Util.member field <> `Null with
  | _ -> false
;;

let lots_json lots = `List (List.map (fun (p, q) -> `List [ `Float p; `Float q ]) lots)

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
  ; buy_lots =
      (if has_field json "buy_lots"
       then get_lots json "buy_lots"
       else (
         (* Pre-queue entry: resume with the last buy fill as the only open lot. An absent
            field must not be read as "no unspent buys", or the anchor is spent twice. *)
         match
           ( get_float_opt json "last_buy_fill_price"
           , get_float_opt json "last_buy_fill_qty" )
         with
         | Some p, Some q when p > 0.0 && q > 0.0 -> [ p, q ]
         | _ -> []))
  }
;;

let to_json t =
  `Assoc
    ([ "reserved_base", `Float t.reserved_base
     ; "accumulated_profit", `Float t.accumulated_profit
     ; "carry_credit_seq", `Int t.carry_credit_seq
     ; "buy_lots", lots_json t.buy_lots
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

(** Open buy lots kept per strategy; the tail drops first, being the least likely
    companion of a future sell. *)
let max_buy_lots = 32

(** Keeps the [n] newest lots, dropping the tail. *)
let rec keep_lots n = function
  | [] -> []
  | _ :: _ when n <= 0 -> []
  | lot :: rest -> lot :: keep_lots (n - 1) rest
;;

(** Buy fill: records an unspent lot for the next sell's profitability check and updates
    the last-buy reference, which prices the follow-up sell. [oid] is accepted per the
    store contract; OID sequencing is owned by the execution layer. *)
let apply_buy_fill t ~price ~qty ~oid =
  let _ = oid in
  let buy_lots =
    if Float.is_finite price && Float.is_finite qty && price > 0.0 && qty > 0.0
    then keep_lots max_buy_lots ((price, qty) :: t.buy_lots)
    else t.buy_lots
  in
  { t with last_buy_fill_price = Some price; last_buy_fill_qty = Some qty; buy_lots }
;;

(** Consumes unspent lots newest-first down to [qty]. Returns the matched spread, the qty
    left unmatched, and the surviving lots. Newest-first because the ladder places one
    companion sell per buy, so the most recent buy funds the next sell to fill. *)
let rec consume_lots ~price ~remaining acc = function
  | [] -> acc, remaining, []
  | (lot_price, lot_qty) :: rest ->
    if remaining <= 0.0
    then acc, 0.0, (lot_price, lot_qty) :: rest
    else (
      let matched = Float.min lot_qty remaining in
      let acc = acc +. ((price -. lot_price) *. matched) in
      let remaining = remaining -. matched in
      let left = lot_qty -. matched in
      (* Relative cutoff: a rounded fill qty rarely drains a lot exactly, and a rounding
         residual must not fund a later cycle. *)
      if left > 1e-9 *. lot_qty
      then acc, remaining, (lot_price, left) :: rest
      else consume_lots ~price ~remaining acc rest)
;;

(** Removes [remaining] base from the newest lots without pricing it. Used for base that
    left the sellable pool without being sold (the slice a [sell_uses_mult] venue withheld
    inside the order), so it cannot fund a second cycle. *)
let rec drop_lots ~remaining = function
  | [] -> []
  | (lot_price, lot_qty) :: rest ->
    if remaining <= 0.0
    then (lot_price, lot_qty) :: rest
    else (
      let matched = Float.min lot_qty remaining in
      let remaining = remaining -. matched in
      let left = lot_qty -. matched in
      if left > 1e-9 *. lot_qty
      then (lot_price, left) :: rest
      else drop_lots ~remaining rest)
;;

(** Sell fill: profit = the spread over the buy lots the sell consumes, minus [fees]
    (all-in, both legs); positive net profit accrues to [accumulated_profit].

    The consumed lots are dropped even when the cycle does not net a profit: that base
    left the account, so it cannot fund another cycle.

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
  let matched_profit, _unmatched, lots_after_sell =
    consume_lots ~price ~remaining:qty 0.0 t.buy_lots
  in
  (* The reserved slice is part of the base just sold, so its cost is that base's own buy
     price: the newest consumed lot, else the last-buy anchor. *)
  let basis_price =
    match t.buy_lots with
    | (p, _) :: _ -> p
    | [] -> Option.value t.last_buy_fill_price ~default:price
  in
  let net_profit = matched_profit -. fees in
  if net_profit <= 0.0
  then { t with buy_lots = lots_after_sell }
  else (
    let accumulated_profit = t.accumulated_profit +. net_profit in
    if not sell_uses_mult
    then { t with accumulated_profit; buy_lots = lots_after_sell }
    else (
      let accrued_base = Float.max 0.0 (oracle_qty *. (1.0 -. sell_mult)) in
      let base_cost = accrued_base *. basis_price in
      if accrued_base > 0.0 && accumulated_profit >= base_cost +. buffer
      then
        { t with
          reserved_base = t.reserved_base +. accrued_base
        ; accumulated_profit = accumulated_profit -. base_cost
        ; (* Withheld, not sold. *)
          buy_lots = drop_lots ~remaining:accrued_base lots_after_sell
        }
      else { t with accumulated_profit; buy_lots = lots_after_sell }))
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
