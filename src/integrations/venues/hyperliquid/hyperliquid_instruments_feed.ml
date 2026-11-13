(** Instrument metadata cache (symbol, size decimals, max leverage, asset index) built
    from WebSocket meta payloads. Provides lookup, rounding, and subscription-identifier
    resolution. *)

let section = "hyperliquid_instruments_feed"

open Lwt.Infix

type pair_info =
  { symbol : string
  ; sz_decimals : int
  ; max_leverage : int option (** [None] for spot instruments. *)
  ; asset_index : int (** Numeric index used for ordering and spot identifier encoding. *)
  }

let pair_cache : (string, pair_info) Hashtbl.t = Hashtbl.create 128
let cache_mutex = Mutex.create ()

(* Lock-free reads. [published_cache] is a copy-on-write snapshot: the single writer
   mutates [pair_cache] under [cache_mutex], then republishes via one [Atomic.set].
   [lookup_info] does one [Atomic.get] + [Hashtbl.find] with no mutex. The published table
   is never mutated. *)
let published_cache : (string, pair_info) Hashtbl.t Atomic.t =
  Atomic.make (Hashtbl.create 128)
;;

(** Builder-deployed perps price their own fees. Every asset in a builder dex's [meta]
    payload carries [deployerFeeScale] (the deployer's cut on top of the protocol rate)
    and may enable growth mode (protocol fees and rebates cut by 90%). The main perp dex
    and spot have neither, so an absent symbol is neutral. *)
type deployer_fee =
  { scale : float
  ; growth : bool
  }

(* Copy-on-write snapshot, published under [cache_mutex] alongside [published_cache]. *)
let deployer_fees : (string, deployer_fee) Hashtbl.t = Hashtbl.create 16

let published_deployer_fees : (string, deployer_fee) Hashtbl.t Atomic.t =
  Atomic.make (Hashtbl.create 16)
;;

(** Republish the current [pair_cache] as an immutable snapshot.

    The copy reads the whole table, so it must happen *inside* [cache_mutex]. All three
    call sites used to unlock first and copy afterwards — an unsynchronized traversal of a
    table another Domain could be inserting into, which contradicts this function's own
    contract. The copy now runs under the lock. [cache_mutex] is held only during the
    once-per-refresh metadata reload, never on a read path, so the extra hold costs
    nothing measurable. *)
let publish_cache_locked () =
  (* Snapshot both tables before unlocking: the fee snapshot is written by the same
     builder-dex reload that mutates [deployer_fees]. *)
  let info = Hashtbl.copy pair_cache in
  let fees = Hashtbl.copy deployer_fees in
  Atomic.set published_cache info;
  Atomic.set published_deployer_fees fees;
  Mutex.unlock cache_mutex
;;

let is_ready = Atomic.make false
let ready_condition = Lwt_condition.create ()

let wait_until_ready () =
  if Atomic.get is_ready then Lwt.return_unit else Lwt_condition.wait ready_condition
;;

let notify_ready () =
  Atomic.set is_ready true;
  Lwt_condition.broadcast ready_condition ()
;;

(** True once the metadata cache holds at least one instrument. The startup gate uses this
    to avoid signalling readiness (and letting domains place orders) on an empty cache: an
    empty WS payload used to call [notify_ready] and every subsequent order was rejected
    with "Unknown symbol". *)
let has_instruments () =
  Mutex.lock cache_mutex;
  let n = Hashtbl.length pair_cache in
  Mutex.unlock cache_mutex;
  n > 0
;;

(** Parses perpetual and spot instrument metadata from JSON payloads received via
    WebSocket. Populates [pair_cache] and signals readiness on completion. Spot
    instruments are keyed by both canonical symbol and [@N] alias. *)
let process_meta_response payload_perp payload_spot =
  Lwt.catch
    (fun () ->
      let open Yojson.Safe.Util in
      let universe_perp = member "universe" payload_perp |> to_list in
      let universe_spot = member "universe" payload_spot |> to_list in
      let tokens_spot = member "tokens" payload_spot |> to_list in
      let spot_info_by_token_idx = Hashtbl.create 512 in
      List.iter
        (fun t ->
          try
            let idx = member "index" t |> to_int in
            let name = member "name" t |> to_string in
            let sz_decimals = member "szDecimals" t |> to_int in
            Hashtbl.replace spot_info_by_token_idx idx (name, sz_decimals)
          with
          | _ -> ())
        tokens_spot;
      Mutex.lock cache_mutex;
      (* Iterate perpetual universe; asset_index matches list position. *)
      List.iteri
        (fun idx item ->
          try
            let symbol = member "name" item |> to_string in
            let sz_decimals = member "szDecimals" item |> to_int in
            let max_leverage = Some (member "maxLeverage" item |> to_int) in
            let info = { symbol; sz_decimals; max_leverage; asset_index = idx } in
            Hashtbl.replace pair_cache symbol info
          with
          | exn ->
            Logging.warn_f
              ~section
              "Failed to parse perp item: %s"
              (Printexc.to_string exn))
        universe_perp;
      (* Spot: resolve base/quote through the token index table. *)
      List.iter
        (fun item ->
          try
            let index = member "index" item |> to_int in
            let tokens_arr = member "tokens" item |> to_list in
            let base_idx, quote_idx =
              match tokens_arr with
              | b :: q :: _ -> to_int b, to_int q
              | _ -> failwith "invalid tokens array"
            in
            let base_name, sz_decimals = Hashtbl.find spot_info_by_token_idx base_idx in
            let quote_name, _ = Hashtbl.find spot_info_by_token_idx quote_idx in
            (* Canonicalize wrapped token names (e.g. UBTC to BTC). *)
            let canon_base =
              match base_name with
              | "UBTC" -> "BTC"
              | "UETH" -> "ETH"
              | "USOL" -> "SOL"
              | _ -> base_name
            in
            let symbol = canon_base ^ "/" ^ quote_name in
            let info =
              { symbol; sz_decimals; max_leverage = None; asset_index = 10000 + index }
            in
            Hashtbl.replace pair_cache symbol info;
            let alias = Printf.sprintf "@%d" index in
            let alias_info =
              { symbol; sz_decimals; max_leverage = None; asset_index = 10000 + index }
            in
            Hashtbl.replace pair_cache alias alias_info
          with
          | exn ->
            Logging.warn_f
              ~section
              "Failed to parse spot item: %s"
              (Printexc.to_string exn))
        universe_spot;
      publish_cache_locked ();
      Logging.debug_f
        ~section
        "Initialized Hyperliquid instrument feed via WS payload with %d perps and %d \
         spot pairs"
        (List.length universe_perp)
        (List.length universe_spot);
      if has_instruments () then notify_ready ();
      Lwt.return_unit)
    (fun exn ->
      Logging.error_f
        ~section
        "Failed to process Hyperliquid instruments: %s"
        (Printexc.to_string exn);
      if has_instruments () then notify_ready ();
      Lwt.return_unit)
;;

(** Populates the instrument cache with synthetic entries for testing. Assigns default
    sz_decimals of 4. Determines instrument type (spot vs perpetual) by the presence of a
    '/' separator in the symbol. *)
let initialize symbols =
  Mutex.lock cache_mutex;
  List.iter
    (fun symbol ->
      let sz_decimals = 4 in
      let max_leverage = if String.contains symbol '/' then None else Some 50 in
      let asset_index = if String.contains symbol '/' then 10000 else 0 in
      let info = { symbol; sz_decimals; max_leverage; asset_index } in
      Hashtbl.replace pair_cache symbol info)
    symbols;
  publish_cache_locked ();
  Logging.debug_f
    ~section
    "Initialized Hyperliquid instruments feed with %d mock symbols"
    (List.length symbols)
;;

(** Registers a single instrument entry with caller-specified sz_decimals. Also inserts a
    base-asset alias for perpetual-style lookups. Intended for test harnesses requiring
    fine-grained control over instrument parameters. *)
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
  publish_cache_locked ()
;;

(** Whether [symbol] names a perpetual instrument. Unlike {!lookup_info} this never falls
    back to the base name, so a spot pair such as [HYPE/USDC] is not mistaken for its
    perpetual [HYPE]. Venue payloads use the bare coin for perps and [@N] aliases for
    spot; the fallback covers the window before instrument metadata has loaded. *)
let is_perp_coin symbol =
  let cache = Atomic.get published_cache in
  match Hashtbl.find_opt cache symbol with
  | Some info -> info.max_leverage <> None
  | None ->
    (not (String.contains symbol '/')) && not (String.starts_with ~prefix:"@" symbol)
;;

(** Looks up instrument info by symbol from the published snapshot (lock-free). Falls back
    to the base name (strip "/QUOTE") to resolve perpetuals. Spot pairs are keyed by full
    "BASE/QUOTE" with [asset_index >= 10000]. *)
let lookup_info symbol =
  let cache = Atomic.get published_cache in
  let direct = Hashtbl.find_opt cache symbol in
  match direct with
  | Some _ as r -> r
  | None ->
    (* Strip quote suffix and retry as perpetual base name. *)
    (match String.split_on_char '/' symbol with
     | base :: _ -> Hashtbl.find_opt cache base
     | [] -> None)
;;

(** Effective fee rate for [symbol] after its builder-dex fee configuration.

    Hyperliquid's rule for a user rate [x] and deployer scale [s]: for a charge ([x > 0])
    the deployer's cut is added, so [s < 1] yields [x *. (1 + s)] and [s > 1] yields
    [x *. 2s]. A rebate ([x < 0]) is split between protocol and deployer but its total is
    unchanged by [s]. Growth mode scales protocol fees and rebates by 0.1. The main dex
    and spot pairs have no entry and pass through unchanged. *)
let apply_deployer_fee ~symbol rate =
  let scale, growth =
    match Hashtbl.find_opt (Atomic.get published_deployer_fees) symbol with
    | Some f -> Float.max 0.0 f.scale, f.growth
    (* No deployer: the protocol keeps the whole rate. *)
    | None -> 0.0, false
  in
  let scale_multiplier =
    if rate < 0.0 then 1.0 else if scale < 1.0 then 1.0 +. scale else 2.0 *. scale
  in
  rate *. scale_multiplier *. if growth then 0.1 else 1.0
;;

(** Coarse price increment used when no instrument context is available. Per-symbol
    pricing actually follows Hyperliquid's 5-significant-figure rule with a
    (max_decimals - sz_decimals) cap; see [round_price_to_tick_for_symbol]. *)
let get_price_increment _symbol = Some 0.01

(** Register a builder-deployed perp dex's instruments.

    Builder perps live in their own `meta` namespace: the coin is `[{dex}]:[{name}]` and
    the asset id the exchange expects is `100000 + perp_dex_index * 10000 + index_in_meta`
    (see the venue's asset-id reference). Getting that arithmetic wrong routes an order to
    an unrelated instrument, so it is computed only here and only from the dex index the
    exchange reports. *)
let process_builder_dex_meta ~dex_index ~dex_name payload =
  match
    try
      let open Yojson.Safe.Util in
      Some (member "universe" payload |> to_list)
    with
    | _ -> None
  with
  | None -> Logging.warn_f ~section "builder dex %s: unreadable meta payload" dex_name
  | Some universe ->
    Mutex.lock cache_mutex;
    List.iteri
      (fun index item ->
        match
          try
            let open Yojson.Safe.Util in
            let coin = member "name" item |> to_string in
            let sz_decimals = member "szDecimals" item |> to_int in
            let max_leverage =
              try Some (member "maxLeverage" item |> to_int) with
              | _ -> None
            in
            (* Per-asset fee configuration. A dex that reports neither charges the plain
               protocol rate, which is what a zero deployer scale means. *)
            let scale =
              try
                match member "deployerFeeScale" item with
                | `String s -> float_of_string s
                | `Float f -> f
                | `Int i -> float_of_int i
                | _ -> 0.0
              with
              | _ -> 0.0
            in
            let growth =
              try
                match member "growthMode" item with
                | `String s -> String.equal s "enabled"
                | `Bool b -> b
                | _ -> false
              with
              | _ -> false
            in
            Some (coin, sz_decimals, max_leverage, scale, growth)
          with
          | _ -> None
        with
        | None -> ()
        | Some (coin, sz_decimals, max_leverage, scale, growth) ->
          let symbol = dex_name ^ ":" ^ coin in
          let asset_index = 100_000 + (dex_index * 10_000) + index in
          Hashtbl.replace
            pair_cache
            symbol
            { symbol; sz_decimals; max_leverage; asset_index };
          Hashtbl.replace deployer_fees symbol { scale; growth })
      universe;
    publish_cache_locked ();
    Logging.info_f
      ~section
      "builder dex %s: %d perp(s) registered (ids %d..)"
      dex_name
      (List.length universe)
      (100_000 + (dex_index * 10_000))
;;

(** Fetch and register every builder-deployed perp dex. The main `meta` payload does not
    contain them, so an instrument like `xyz:SPCX` only resolves after this runs. *)
let fetch_builder_dexes ~testnet () : unit Lwt.t =
  let url =
    if testnet
    then Uri.of_string "https://api.hyperliquid-testnet.xyz/info"
    else Uri.of_string "https://api.hyperliquid.xyz/info"
  in
  let post_json json =
    let body = Cohttp_lwt.Body.of_string (Yojson.Safe.to_string json) in
    let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
    Cohttp_lwt_unix.Client.post ~headers ~body url
    >>= fun (_resp, resp_body) ->
    Cohttp_lwt.Body.to_string resp_body
    >>= fun body_str -> Lwt.return (Yojson.Safe.from_string body_str)
  in
  Lwt.catch
    (fun () ->
      post_json (`Assoc [ "type", `String "perpDexs" ])
      >>= fun dexes ->
      (* The position in this array *is* the perp dex index the asset id is built from,
         and entry 0 is the main dex (null). Filtering before indexing would shift every
         dex by one and misroute its orders to another instrument. *)
      let dex_entries =
        try
          List.mapi
            (fun index d ->
              let name =
                match d with
                | `String name -> if name = "" then None else Some name
                | `Assoc _ ->
                  (try Some Yojson.Safe.Util.(d |> member "name" |> to_string) with
                   | _ -> None)
                | _ -> None
              in
              match name with
              | Some dex_name when index > 0 -> Some (index, dex_name)
              | _ -> None)
            (Yojson.Safe.Util.to_list dexes)
          |> List.filter_map (fun x -> x)
        with
        | _ -> []
      in
      Lwt_list.iter_s
        (fun (index, dex_name) ->
          post_json (`Assoc [ "type", `String "meta"; "dex", `String dex_name ])
          >|= fun payload -> process_builder_dex_meta ~dex_index:index ~dex_name payload)
        dex_entries)
    (fun exn ->
      Logging.warn_f ~section "builder dex discovery failed: %s" (Printexc.to_string exn);
      Lwt.return_unit)
;;

(** Fetches perp + spot metadata from REST /info into the cache. Used by out-of-process
    consumers (e.g. the capital oracle CLI) that do not run the WebSocket feed.

    [~testnet] selects the endpoint. The cache is shared with the live engine, so this
    MUST fetch the same environment the engine trades on: spot pair indices differ between
    mainnet and testnet, and a mismatched asset id routes orders to an unrelated pair. *)
let fetch_meta_from_rest ~testnet () : unit Lwt.t =
  Lwt.catch
    (fun () ->
      let url =
        if testnet
        then Uri.of_string "https://api.hyperliquid-testnet.xyz/info"
        else Uri.of_string "https://api.hyperliquid.xyz/info"
      in
      let post_one typ =
        let req_body = Yojson.Safe.to_string (`Assoc [ "type", `String typ ]) in
        let body = Cohttp_lwt.Body.of_string req_body in
        let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
        Cohttp_lwt_unix.Client.post ~headers ~body url
        >>= fun (_resp, resp_body) ->
        Cohttp_lwt.Body.to_string resp_body
        >>= fun body_str -> Lwt.return (Yojson.Safe.from_string body_str)
      in
      post_one "meta"
      >>= fun payload_perp ->
      post_one "spotMeta"
      >>= fun payload_spot ->
      process_meta_response payload_perp payload_spot
      >>= fun () ->
      (* Builder-deployed perps (equities and other external markets) are not in the main
         meta payload; each dex has its own. *)
      let post_json json =
        let req_body = Yojson.Safe.to_string json in
        let body = Cohttp_lwt.Body.of_string req_body in
        let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
        Cohttp_lwt_unix.Client.post ~headers ~body url
        >>= fun (_resp, resp_body) ->
        Cohttp_lwt.Body.to_string resp_body
        >>= fun body_str -> Lwt.return (Yojson.Safe.from_string body_str)
      in
      post_json (`Assoc [ "type", `String "perpDexs" ])
      >>= fun dexes ->
      let names =
        try
          `List
            (List.map
               (fun d ->
                 `String
                   (match d with
                    | `String name -> name
                    | other -> Yojson.Safe.Util.(other |> member "name" |> to_string)))
               (Yojson.Safe.Util.to_list dexes))
          |> Yojson.Safe.Util.to_list
        with
        | _ -> []
      in
      Lwt_list.iter_s
        (fun index_and_dex ->
          let index, dex_name = index_and_dex in
          post_json (`Assoc [ "type", `String "meta"; "dex", `String dex_name ])
          >|= fun payload -> process_builder_dex_meta ~dex_index:index ~dex_name payload)
        (List.filter_map
           (fun (i, d) ->
             match d with
             | `String name when i > 0 && name <> "" -> Some (i, name)
             | _ -> None)
           (List.mapi (fun i d -> i, d) names)))
    (fun exn ->
      Logging.error_f
        ~section
        "Failed to fetch instrument metadata from REST: %s"
        (Printexc.to_string exn);
      Lwt.return_unit)
;;

let get_qty_increment symbol =
  match lookup_info symbol with
  | Some info -> Some (10.0 ** -.float_of_int info.sz_decimals)
  | None -> None
;;

let get_qty_min symbol = get_qty_increment symbol

let get_asset_index symbol =
  match lookup_info symbol with
  | Some info -> Some info.asset_index
  | None -> None
;;

let resolve_symbol coin =
  Mutex.lock cache_mutex;
  let res =
    match Hashtbl.find_opt pair_cache coin with
    | Some info -> Some info.symbol
    | None -> None
  in
  Mutex.unlock cache_mutex;
  res
;;

(** Returns the coin identifier expected by Hyperliquid WebSocket channels (l2Book).
    Perpetuals use the base coin name; spot pairs use the "@N" format derived from
    asset_index. *)
let get_subscription_coin symbol =
  Mutex.lock cache_mutex;
  let res =
    match Hashtbl.find_opt pair_cache symbol with
    | Some _ when String.contains symbol ':' ->
      (* Builder perp: the venue's coin name is already [dex:coin]. Asset ids above 100000
         are builder perps, not spot pairs - encoding them as @N subscribes to a coin that
         does not exist and no book ever arrives. *)
      symbol
    | Some info when info.asset_index >= 10000 && info.asset_index < 100_000 ->
      (* Spot pair: encode as @N where N = asset_index - 10000. *)
      Printf.sprintf "@%d" (info.asset_index - 10000)
    | _ ->
      (* Perpetual or unknown: extract base coin name. *)
      if String.contains symbol '/'
      then String.split_on_char '/' symbol |> List.hd
      else symbol
  in
  Mutex.unlock cache_mutex;
  res
;;

(** Rounds price according to Hyperliquid per-symbol precision rules. Step 1: round to 5
    significant figures. Step 2: cap decimal places at (max_decimals - sz_decimals), where
    max_decimals is 8 for spot and 6 for perpetual instruments. *)
let round_price_to_tick_for_symbol symbol price =
  if price <= 0.0
  then price
  else (
    let sz_decimals, is_spot =
      match lookup_info symbol with
      | Some info -> info.sz_decimals, info.max_leverage = None
      | None -> 0, false
    in
    let max_decimals = if is_spot then 8 else 6 in
    let allowed_decimals = max_decimals - sz_decimals in
    (* Round to 5 significant figures. *)
    let exp = floor (log10 price) in
    let shift = 4. -. exp in
    let sig_fig_multiplier = 10. ** shift in
    let rounded_5sf =
      floor ((price *. sig_fig_multiplier) +. 0.5) /. sig_fig_multiplier
    in
    (* Cap at allowed_decimals decimal places. *)
    let dec_multiplier = 10. ** float_of_int allowed_decimals in
    floor ((rounded_5sf *. dec_multiplier) +. 0.5) /. dec_multiplier)
;;

(** Rounds price to tick without instrument context. Applies 5 significant figures and a
    fixed 6 decimal place cap. *)
let round_price_to_tick price =
  if price <= 0.0
  then price
  else (
    let exp = floor (log10 price) in
    let shift = 4. -. exp in
    let multiplier = 10. ** shift in
    let rounded = floor ((price *. multiplier) +. 0.5) /. multiplier in
    let max_decimals = 6. in
    let dec_multiplier = 10. ** max_decimals in
    floor ((rounded *. dec_multiplier) +. 0.5) /. dec_multiplier)
;;

(** Rounds quantity down to the instrument's sz_decimals (lot size). Uses floor to prevent
    over-allocation. Returns qty unmodified if the instrument is not found in the cache.
    Adds a relative epsilon before the floor so binary representation error cannot drop an
    on-lot quantity a full step: [0.0003 *. 1e5 = 29.999999999999996] floors to [0.00029]
    without it, but to [0.0003] with it. *)
let round_qty_to_lot symbol qty =
  match lookup_info symbol with
  | Some info ->
    let multiplier = 10. ** float_of_int info.sz_decimals in
    (* Floor to avoid exceeding available balance. *)
    let scaled = qty *. multiplier in
    floor (scaled +. (1e-9 *. Float.max 1.0 (Float.abs scaled))) /. multiplier
  | None ->
    (* Unknown instrument: return qty unmodified. *)
    qty
;;
