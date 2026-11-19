(* Base accumulation persistence store.

   Per-strategy opt-in tracking of accumulated base assets and local buy/sell
   cycle profitability. One entry per strategy key
   ("{strategy_name}:{symbol}:{venue}", from config.json's strategy field).

   Persisted fields (data/accumulation_state.json, keyed by strategy key):
   - reserved_base: base accumulated by the buy-fill skim; excluded from sellable balance
   - accumulated_profit: realized local PnL in quote
   - carry_credit_seq: newest carry recycle already credited (restart dedup)
   - buy_lots: unspent buy fills, newest first, consumed one per credited sell
   - applied_fill_ids: fills already booked, newest first (restart replay dedup)
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
  (** Full received fill; the skim is a profit debit, not a smaller buy. *)
  ; last_sell_fill_price : float option
  ; last_sell_fill_qty : float option
  ; buy_lots : (float * float) list
  (** Unspent buy fills, newest first as [(price, qty)], consumed one per credited sell.
      Sharing one last-buy anchor across a run of buys credits the ladder step once per
      sell too many. *)
  ; applied_fill_ids : string list
  (** Newest-first ids of booked fills, bounded. [last_fill_oid] alone cannot dedup a
      replayed exec ring on non-monotonic ids (Alpaca UUIDs). *)
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
  ; applied_fill_ids = []
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

(** Reads a JSON array of non-empty strings. Malformed entries are dropped. *)
let get_strings json field =
  let open Yojson.Basic.Util in
  try
    json
    |> member field
    |> to_list
    |> List.filter_map (fun v ->
      try
        let s = to_string v in
        if String.equal s "" then None else Some s
      with
      | _ -> None)
  with
  | _ -> []
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
  ; applied_fill_ids = get_strings json "applied_fill_ids"
  }
;;

let to_json t =
  `Assoc
    ([ "reserved_base", `Float t.reserved_base
     ; "accumulated_profit", `Float t.accumulated_profit
     ; "carry_credit_seq", `Int t.carry_credit_seq
     ; "buy_lots", lots_json t.buy_lots
     ; "applied_fill_ids", `List (List.map (fun id -> `String id) t.applied_fill_ids)
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

(** Booked fill ids retained for replay dedup. Sized to cover a venue exec ring's replay
    window; larger than the lot cap because every fill kind is remembered, not just buys. *)
let max_applied_fill_ids = 256

(** Prepends [oid] to a persisted fill-id list: bounded, deduplicated, empty ids ignored. *)
let record_fill_id_list ids oid =
  if String.equal oid "" || List.mem oid ids
  then ids
  else keep_lots max_applied_fill_ids (oid :: ids)
;;

(** Marks [oid] as booked: idempotent and bounded. *)
let record_fill_id t oid =
  let applied_fill_ids = record_fill_id_list t.applied_fill_ids oid in
  if applied_fill_ids == t.applied_fill_ids then t else { t with applied_fill_ids }
;;

let has_applied_fill t oid =
  (not (String.equal oid "")) && List.mem oid t.applied_fill_ids
;;

(** Buy fill: records an unspent lot for the next sell's profitability check and updates
    the last-buy reference. [oid] is booked for the restart replay guard. *)
let apply_buy_fill t ~price ~qty ~oid =
  let buy_lots =
    if Float.is_finite price && Float.is_finite qty && price > 0.0 && qty > 0.0
    then keep_lots max_buy_lots ((price, qty) :: t.buy_lots)
    else t.buy_lots
  in
  { t with last_buy_fill_price = Some price; last_buy_fill_qty = Some qty; buy_lots }
  |> fun t -> record_fill_id t oid
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

(** Sell fill: profit = the spread over the buy lots the sell consumes, minus [fees]
    (all-in, both legs). Signed: a losing cycle debits [accumulated_profit].

    The consumed lots are dropped either way: that base left the account, so it cannot
    fund another cycle.

    No base is reserved here. Every venue sells 1:1, so a sell never withholds a slice;
    accumulation happens on the bought side (the profit-gated lot skim in
    [Strategy_events]), and this path only credits quote. [fees] is the real all-in fee. *)
let apply_sell_fill t ~price ~qty ~oid ~fees () =
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
  let net_profit = matched_profit -. fees in
  { t with
    accumulated_profit = t.accumulated_profit +. net_profit
  ; buy_lots = lots_after_sell
  }
  |> fun t -> record_fill_id t oid
;;

(** Reconciles recorded lots with the base the venue holds. A replay (or a lost update)
    can leave more base in [buy_lots] than the account has; that surplus would fund
    impossible sells and misprice later credits. Only a surplus is corrected:
    byte-identical replay copies first, then the oldest tail. A genuine repeated fill at
    one rung is indistinguishable from a copy, so dedupe is gated on the account showing
    less base than recorded - never applied blanket. Never pads.

    Returns the surviving lots and the quantity dropped. *)
let reconcile_lots ~sellable_base lots =
  let sum = List.fold_left (fun acc (_, q) -> acc +. q) 0.0 in
  let original_total = sum lots in
  let tolerance = Float.max 1e-9 (Float.abs sellable_base *. 1e-6) in
  if (not (Float.is_finite sellable_base))
     || sellable_base < 0.0
     || original_total <= sellable_base +. tolerance
  then lots, 0.0
  else (
    (* Pass 1: drop duplicate copies of a fill already kept, newest first, until the
       recorded total fits. *)
    let seen = Hashtbl.create (List.length lots) in
    let remaining = ref original_total in
    let deduped =
      List.filter
        (fun (p, q) ->
          if !remaining <= sellable_base
          then true
          else (
            let key = Int64.bits_of_float p, Int64.bits_of_float q in
            if Hashtbl.mem seen key
            then (
              remaining := !remaining -. q;
              false)
            else (
              Hashtbl.add seen key ();
              true)))
        lots
    in
    if !remaining <= sellable_base +. tolerance
    then deduped, original_total -. !remaining
    else (
      (* Pass 2: still over (the surplus was not an exact replay copy). Keep newest-first
         and clip the boundary lot so the queue sums to the venue figure. *)
      let rec take budget acc = function
        | [] -> List.rev acc
        | _ :: _ when budget <= 0.0 -> List.rev acc
        | (p, q) :: rest ->
          let kept = Float.min q budget in
          (* A rounding residual must not survive as a dust lot. *)
          if kept <= 1e-9 *. q
          then List.rev acc
          else take (budget -. kept) ((p, kept) :: acc) rest
      in
      let kept = take sellable_base [] deduped in
      kept, original_total -. sum kept))
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
