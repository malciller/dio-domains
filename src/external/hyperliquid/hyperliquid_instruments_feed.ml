(** Hyperliquid Instruments Feed *)


let section = "hyperliquid_instruments_feed"

type pair_info = {
  symbol: string;
  sz_decimals: int;
  max_leverage: int option; (* optionally null for spot *)
  asset_index: int; (* used for ordering *)
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

(** Process instrument metadata from JSON payloads (called by higher level component pushing WS data) *)
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
    (* Process Perpetuals *)
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
    
    (* Process Spot *)
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
        (* Map wrapper spot tokens to normal names (e.g. UBTC -> BTC) *)
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

(** Mock initialization for testing - populates cache without network requests *)
let initialize symbols =
  Mutex.lock cache_mutex;
  List.iter (fun symbol ->
    let sz_decimals = 4 in (* Default for testing *)
    let max_leverage = if String.contains symbol '/' then None else Some 50 in
    let asset_index = if String.contains symbol '/' then 10000 else 0 in
    let info = { symbol; sz_decimals; max_leverage; asset_index } in
    Hashtbl.replace pair_cache symbol info
  ) symbols;
  Mutex.unlock cache_mutex;
  Logging.info_f ~section "Initialized Hyperliquid instruments feed with %d mock symbols" (List.length symbols)

(** Register a single instrument with a custom sz_decimals for testing *)
let register_test_instrument ~symbol ~sz_decimals =
  Mutex.lock cache_mutex;
  let max_leverage = if String.contains symbol '/' then None else Some 50 in
  let asset_index = if String.contains symbol '/' then 10000 else 0 in
  let info = { symbol; sz_decimals; max_leverage; asset_index } in
  Hashtbl.replace pair_cache symbol info;
  (* Also register the base asset as a perp alias so lookup_info fallback works *)
  (match String.split_on_char '/' symbol with
   | base :: _ when base <> symbol -> Hashtbl.replace pair_cache base info
   | _ -> ());
  Mutex.unlock cache_mutex

(** Look up a symbol in the cache, with a fallback for perp pairs expressed as BASE/USDC.
    Config uses "BTC/USDC" but perps are cached as just "BTC" (index 0..N-1).
    Spot pairs are stored as "BASE/QUOTE" directly (index 10000+). *)
let lookup_info symbol =
  Mutex.lock cache_mutex;
  let direct = Hashtbl.find_opt pair_cache symbol in
  let result = match direct with
    | Some _ as r -> r
    | None ->
        (* Try stripping the quote suffix to find a perp: "BTC/USDC" -> "BTC" *)
        (match String.split_on_char '/' symbol with
         | base :: _ -> Hashtbl.find_opt pair_cache base
         | [] -> None)
  in
  Mutex.unlock cache_mutex;
  result

(** Get the tick size / price increment for a symbol *)
let get_price_increment _symbol =
  (* Hyperliquid uses 5 significant figures for price, up to 6 decimals.
     For this static interface we return a fixed small minimum increment. *)
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

(** Get the correct coin identifier for WS subscriptions (l2Book, allMids matching).
    Perps use the base coin name (e.g. "HYPE"), spot pairs use "@N" format. *)
let get_subscription_coin symbol =
  Mutex.lock cache_mutex;
  let res = match Hashtbl.find_opt pair_cache symbol with
    | Some info when info.asset_index >= 10000 ->
        (* Spot pair - use @N format where N = asset_index - 10000 *)
        Printf.sprintf "@%d" (info.asset_index - 10000)
    | _ ->
        (* Perp or not found - use base coin name *)
        if String.contains symbol '/' then String.split_on_char '/' symbol |> List.hd
        else symbol
  in
  Mutex.unlock cache_mutex;
  res

(** Round price to Hyperliquid's precision rules for a specific symbol:
    - 5 significant figures
    - At most (MAX_DECIMALS - szDecimals) decimal places
    - MAX_DECIMALS = 8 for spot, 6 for perp *)
let round_price_to_tick_for_symbol symbol price =
  if price <= 0.0 then price
  else begin
    let sz_decimals, is_spot = match lookup_info symbol with
      | Some info -> info.sz_decimals, (info.max_leverage = None)
      | None -> 0, false
    in
    let max_decimals = if is_spot then 8 else 6 in
    let allowed_decimals = max_decimals - sz_decimals in

    (* Step 1: round to 5 significant figures *)
    let exp = floor (log10 price) in
    let shift = 4. -. exp in
    let sig_fig_multiplier = 10. ** shift in
    let rounded_5sf = floor (price *. sig_fig_multiplier +. 0.5) /. sig_fig_multiplier in

    (* Step 2: cap at allowed_decimals decimal places *)
    let dec_multiplier = 10. ** (float_of_int allowed_decimals) in
    floor (rounded_5sf *. dec_multiplier +. 0.5) /. dec_multiplier
  end

(** Round price to tick using symbol-unaware fallback (5 sig figs, 6 decimal cap) *)
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

(** Round quantity to the instrument's szDecimals (lot size) *)
let round_qty_to_lot symbol qty =
  match lookup_info symbol with
  | Some info ->
      let multiplier = 10. ** (float_of_int info.sz_decimals) in
      (* Use floor down to ensure we never over-allocate qty, which is typical for trading *)
      floor (qty *. multiplier) /. multiplier
  | None ->
      (* Fallback: don't round if we don't know the asset *)
      qty