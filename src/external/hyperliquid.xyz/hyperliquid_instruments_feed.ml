(** Hyperliquid instrument metadata feed.
    Maintains a local cache of perpetual and spot instrument definitions
    (symbol, size decimals, max leverage, asset index) derived from
    WebSocket meta payloads. Provides lookup, rounding, and subscription
    identifier resolution for downstream consumers. *)


let section = "hyperliquid_instruments_feed"

type pair_info = {
  symbol: string;
  sz_decimals: int;
  max_leverage: int option; (** [None] for spot instruments. *)
  asset_index: int; (** Numeric index used for ordering and spot identifier encoding. *)
}

let pair_cache : (string, pair_info) Hashtbl.t = Hashtbl.create 128
let cache_mutex = Mutex.create ()

let is_ready = Atomic.make false
let ready_condition = Lwt_condition.create ()

let wait_until_ready () =
  if Atomic.get is_ready then Lwt.return_unit
  else Lwt_condition.wait ready_condition

let notify_ready () =
  Atomic.set is_ready true;
  Lwt_condition.broadcast ready_condition ()

(** Parses perpetual and spot instrument metadata from JSON payloads
    received via WebSocket. Populates [pair_cache] and signals readiness
    on completion. Spot instruments are keyed by both canonical symbol
    and [@N] alias. *)
let process_meta_response payload_perp payload_spot =
  Lwt.catch (fun () ->
    let open Yojson.Safe.Util in
    
    let universe_perp = member "universe" payload_perp |> to_list in
    let universe_spot = member "universe" payload_spot |> to_list in
    let tokens_spot = member "tokens" payload_spot |> to_list in
    
    let spot_info_by_token_idx = Hashtbl.create 512 in
    List.iter (fun t ->
      try
        let idx = member "index" t |> to_int in
        let name = member "name" t |> to_string in
        let sz_decimals = member "szDecimals" t |> to_int in
        Hashtbl.replace spot_info_by_token_idx idx (name, sz_decimals)
      with _ -> ()
    ) tokens_spot;
    
    Mutex.lock cache_mutex;
    (* Iterate perpetual universe; asset_index matches list position. *)
    List.iteri (fun idx item ->
      try
        let symbol = member "name" item |> to_string in
        let sz_decimals = member "szDecimals" item |> to_int in
        let max_leverage = Some (member "maxLeverage" item |> to_int) in
        let info = { symbol; sz_decimals; max_leverage; asset_index = idx } in
        Hashtbl.replace pair_cache symbol info
      with exn ->
        Logging.warn_f ~section "Failed to parse perp item: %s" (Printexc.to_string exn)
    ) universe_perp;
    
    (* Iterate spot universe; resolve base/quote from token index table. *)
    List.iter (fun item ->
      try
        let index = member "index" item |> to_int in
        let tokens_arr = member "tokens" item |> to_list in
        let base_idx, quote_idx = match tokens_arr with 
          | b::q::_ -> (to_int b, to_int q) 
          | _ -> failwith "invalid tokens array" 
        in
        let base_name, sz_decimals = Hashtbl.find spot_info_by_token_idx base_idx in
        let quote_name, _ = Hashtbl.find spot_info_by_token_idx quote_idx in
        (* Canonicalize wrapped token names (e.g. UBTC to BTC). *)
        let canon_base = match base_name with
          | "UBTC" -> "BTC"
          | "UETH" -> "ETH"
          | "USOL" -> "SOL"
          | _ -> base_name
        in
        let symbol = canon_base ^ "/" ^ quote_name in
        let info = { symbol; sz_decimals; max_leverage = None; asset_index = 10000 + index } in
        Hashtbl.replace pair_cache symbol info;
        let alias = Printf.sprintf "@%d" index in
        let alias_info = { symbol; sz_decimals; max_leverage = None; asset_index = 10000 + index } in
        Hashtbl.replace pair_cache alias alias_info
      with exn ->
        Logging.warn_f ~section "Failed to parse spot item: %s" (Printexc.to_string exn)
    ) universe_spot;
    Mutex.unlock cache_mutex;
    
    Logging.info_f ~section "Initialized Hyperliquid instrument feed via WS payload with %d perps and %d spot pairs" (List.length universe_perp) (List.length universe_spot);
    notify_ready ();
    Lwt.return_unit
  ) (fun exn ->
    Logging.error_f ~section "Failed to process Hyperliquid instruments: %s" (Printexc.to_string exn);
    notify_ready ();
    Lwt.return_unit
  )

(** Populates the instrument cache with synthetic entries for testing.
    Assigns default sz_decimals of 4. Determines instrument type
    (spot vs perpetual) by the presence of a '/' separator in the symbol. *)
let initialize symbols =
  Mutex.lock cache_mutex;
  List.iter (fun symbol ->
    let sz_decimals = 4 in
    let max_leverage = if String.contains symbol '/' then None else Some 50 in
    let asset_index = if String.contains symbol '/' then 10000 else 0 in
    let info = { symbol; sz_decimals; max_leverage; asset_index } in
    Hashtbl.replace pair_cache symbol info
  ) symbols;
  Mutex.unlock cache_mutex;
  Logging.info_f ~section "Initialized Hyperliquid instruments feed with %d mock symbols" (List.length symbols)

(** Registers a single instrument entry with caller-specified sz_decimals.
    Also inserts a base-asset alias for perpetual-style lookups.
    Intended for test harnesses requiring fine-grained control over
    instrument parameters. *)
let register_test_instrument ~symbol ~sz_decimals =
  Mutex.lock cache_mutex;
  let max_leverage = if String.contains symbol '/' then None else Some 50 in
  let asset_index = if String.contains symbol '/' then 10000 else 0 in
  let info = { symbol; sz_decimals; max_leverage; asset_index } in
  Hashtbl.replace pair_cache symbol info;
  (* Register base asset key as a perpetual alias for fallback lookups. *)
  (match String.split_on_char '/' symbol with
   | base :: _ when base <> symbol -> Hashtbl.replace pair_cache base info
   | _ -> ());
  Mutex.unlock cache_mutex

(** Looks up instrument info by symbol. Falls back to stripping the
    quote suffix (e.g. "BTC/USDC" to "BTC") to resolve perpetuals,
    which are cached under their base name only. Spot pairs are stored
    under their full "BASE/QUOTE" key (asset_index >= 10000). *)
let lookup_info symbol =
  Mutex.lock cache_mutex;
  let direct = Hashtbl.find_opt pair_cache symbol in
  let result = match direct with
    | Some _ as r -> r
    | None ->
        (* Strip quote suffix and retry as perpetual base name. *)
        (match String.split_on_char '/' symbol with
         | base :: _ -> Hashtbl.find_opt pair_cache base
         | [] -> None)
  in
  Mutex.unlock cache_mutex;
  result

(** Returns a fixed minimum price increment.
    Hyperliquid enforces 5 significant figures with up to 6 decimal places;
    this static accessor returns the smallest representable tick. *)
let get_price_increment _symbol =

  Some 0.00001

let get_qty_increment symbol =
  match lookup_info symbol with
  | Some info -> Some (10.0 ** (-. float_of_int info.sz_decimals))
  | None -> None

let get_qty_min symbol =
  get_qty_increment symbol

let get_asset_index symbol =
  match lookup_info symbol with
  | Some info -> Some info.asset_index
  | None -> None

let resolve_symbol coin =
  Mutex.lock cache_mutex;
  let res = match Hashtbl.find_opt pair_cache coin with
    | Some info -> Some info.symbol
    | None -> None
  in
  Mutex.unlock cache_mutex;
  res

(** Returns the coin identifier expected by Hyperliquid WebSocket channels
    (l2Book, allMids). Perpetuals use the base coin name; spot pairs use
    the "@N" format derived from asset_index. *)
let get_subscription_coin symbol =
  Mutex.lock cache_mutex;
  let res = match Hashtbl.find_opt pair_cache symbol with
    | Some info when info.asset_index >= 10000 ->
        (* Spot pair: encode as @N where N = asset_index - 10000. *)
        Printf.sprintf "@%d" (info.asset_index - 10000)
    | _ ->
        (* Perpetual or unknown: extract base coin name. *)
        if String.contains symbol '/' then String.split_on_char '/' symbol |> List.hd
        else symbol
  in
  Mutex.unlock cache_mutex;
  res

(** Rounds price according to Hyperliquid per-symbol precision rules.
    Step 1: round to 5 significant figures.
    Step 2: cap decimal places at (max_decimals - sz_decimals),
    where max_decimals is 8 for spot and 6 for perpetual instruments. *)
let round_price_to_tick_for_symbol symbol price =
  if price <= 0.0 then price
  else begin
    let sz_decimals, is_spot = match lookup_info symbol with
      | Some info -> info.sz_decimals, (info.max_leverage = None)
      | None -> 0, false
    in
    let max_decimals = if is_spot then 8 else 6 in
    let allowed_decimals = max_decimals - sz_decimals in

    (* Round to 5 significant figures. *)
    let exp = floor (log10 price) in
    let shift = 4. -. exp in
    let sig_fig_multiplier = 10. ** shift in
    let rounded_5sf = floor (price *. sig_fig_multiplier +. 0.5) /. sig_fig_multiplier in

    (* Cap at allowed_decimals decimal places. *)
    let dec_multiplier = 10. ** (float_of_int allowed_decimals) in
    floor (rounded_5sf *. dec_multiplier +. 0.5) /. dec_multiplier
  end

(** Rounds price to tick without instrument context.
    Applies 5 significant figures and a fixed 6 decimal place cap. *)
let round_price_to_tick price =
  if price <= 0.0 then price
  else
    let exp = floor (log10 price) in
    let shift = 4. -. exp in
    let multiplier = 10. ** shift in
    let rounded = floor (price *. multiplier +. 0.5) /. multiplier in
    let max_decimals = 6. in
    let dec_multiplier = 10. ** max_decimals in
    floor (rounded *. dec_multiplier +. 0.5) /. dec_multiplier

(** Rounds quantity down to the instrument's sz_decimals (lot size).
    Uses floor to prevent over-allocation. Returns qty unmodified
    if the instrument is not found in the cache. *)
let round_qty_to_lot symbol qty =
  match lookup_info symbol with
  | Some info ->
      let multiplier = 10. ** (float_of_int info.sz_decimals) in
      (* Floor to avoid exceeding available balance. *)
      floor (qty *. multiplier) /. multiplier
  | None ->
      (* Unknown instrument: return qty unmodified. *)
      qty