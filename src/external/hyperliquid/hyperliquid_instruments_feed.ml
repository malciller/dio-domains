(** Hyperliquid Instruments Feed *)

open Lwt.Infix

let section = "hyperliquid_instruments_feed"

type pair_info = {
  symbol: string;
  sz_decimals: int;
  max_leverage: int option; (* optionally null for spot *)
  asset_index: int; (* used for ordering *)
}

let pair_cache : (string, pair_info) Hashtbl.t = Hashtbl.create 128
let cache_mutex = Mutex.create ()

(** Fetch instrument metadata from Hyperliquid REST API *)
let initialize_symbols ~testnet _symbols =
  let base_url = if testnet then "https://api.hyperliquid-testnet.xyz" else "https://api.hyperliquid.xyz" in
  Lwt.catch (fun () ->
    let open Cohttp_lwt_unix in
    let url = Uri.of_string (base_url ^ "/info") in
    
    (* Fetch Perpetuals Metadata *)
    let body_perp = Cohttp_lwt.Body.of_string "{\"type\":\"meta\"}" in
    let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
    Client.post ~headers ~body:body_perp url >>= fun (_resp, resp_body) ->
    Cohttp_lwt.Body.to_string resp_body >>= fun body_str_perp ->
    Logging.info_f ~section "Raw perp meta response: %s" (if String.length body_str_perp > 500 then String.sub body_str_perp 0 500 ^ "..." else body_str_perp);
    
    (* Fetch Spot Metadata *)
    let body_spot = Cohttp_lwt.Body.of_string "{\"type\":\"spotMeta\"}" in
    Client.post ~headers ~body:body_spot url >>= fun (_resp, resp_body_spot) ->
    Cohttp_lwt.Body.to_string resp_body_spot >>= fun body_str_spot ->
    Logging.info_f ~section "Raw spot meta response: %s" (if String.length body_str_spot > 500 then String.sub body_str_spot 0 500 ^ "..." else body_str_spot);
    
    let open Yojson.Safe.Util in
    let json_perp = Yojson.Safe.from_string body_str_perp in
    let universe_perp = member "universe" json_perp |> to_list in
    
    let json_spot = Yojson.Safe.from_string body_str_spot in
    let universe_spot = member "universe" json_spot |> to_list in
    let tokens_spot = member "tokens" json_spot |> to_list in
    
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
        let symbol = base_name ^ "/" ^ quote_name in
        let info = { symbol; sz_decimals; max_leverage = None; asset_index = 10000 + index } in
        Hashtbl.replace pair_cache symbol info;
        let alias = Printf.sprintf "@%d" index in
        let alias_info = { symbol; sz_decimals; max_leverage = None; asset_index = 10000 + index } in
        Hashtbl.replace pair_cache alias alias_info
      with exn ->
        Logging.warn_f ~section "Failed to parse spot item: %s" (Printexc.to_string exn)
    ) universe_spot;
    Mutex.unlock cache_mutex;
    
    Logging.info_f ~section "Initialized Hyperliquid instrument feed with %d perps and %d spot pairs" (List.length universe_perp) (List.length universe_spot);
    Lwt.return_unit
  ) (fun exn ->
    Logging.error_f ~section "Failed to fetch Hyperliquid instruments: %s" (Printexc.to_string exn);
    Lwt.return_unit
  )

(** Get the tick size / price increment for a symbol *)
let get_price_increment _symbol =
  (* Hyperliquid uses 5 significant figures for price, up to 6 decimals.
     For this static interface we return a fixed small minimum increment. *)
  Some 0.00001

let get_qty_increment symbol =
  Mutex.lock cache_mutex;
  let info_opt = Hashtbl.find_opt pair_cache symbol in
  Mutex.unlock cache_mutex;
  match info_opt with
  | Some info -> Some (10.0 ** (-. float_of_int info.sz_decimals))
  | None -> None

let get_qty_min symbol =
  get_qty_increment symbol

let get_asset_index symbol =
  Mutex.lock cache_mutex;
  let info_opt = Hashtbl.find_opt pair_cache symbol in
  Mutex.unlock cache_mutex;
  match info_opt with
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

(** Round price to 5 significant figures, max 6 decimals *)
let round_price_to_tick price =
  if price <= 0.0 then price
  else
    (* Find the exponent to determine sig figs *)
    let exp = floor (log10 price) in
    (* We want 5 significant figures, so we shift by 4 - exp *)
    let shift = 4. -. exp in
    let multiplier = 10. ** shift in
    let rounded = floor (price *. multiplier +. 0.5) /. multiplier in
    
    (* Hyperliquid supports at most 6 decimals *)
    let max_decimals = 6. in
    let dec_multiplier = 10. ** max_decimals in
    floor (rounded *. dec_multiplier +. 0.5) /. dec_multiplier

(** Round quantity to the instrument's szDecimals (lot size) *)
let round_qty_to_lot symbol qty =
  Mutex.lock cache_mutex;
  let info_opt = Hashtbl.find_opt pair_cache symbol in
  Mutex.unlock cache_mutex;
  match info_opt with
  | Some info ->
      let multiplier = 10. ** (float_of_int info.sz_decimals) in
      (* Use floor down to ensure we never over-allocate qty, which is typical for trading *)
      floor (qty *. multiplier) /. multiplier
  | None ->
      (* Fallback: don't round if we don't know the asset *)
      qty
