(** Hyperliquid data-venue adapter for the capital oracle; implements
    [Exchange_intf.Oracle.S].

    Sources: historical daily OHLC via POST /info [{"type":"candleSnapshot"}] paginated
    forward in day-windows; account fees via [Get_fee]; spot wallet balance via
    spotClearinghouseState (spot-only; perp margin is not grid capital); instrument
    metadata via [Instruments_feed]. All HTTP calls are timeout-bounded.

    Spot/perp resolution: a bare coin is a perpetual and used as-is; a "BASE/QUOTE" symbol
    resolves through spotMeta to the candle coin ("PURR/USDC" or "@N"). A "/" symbol with
    no matching spot pair has no history and yields no bars (asset INACTIVE) - never
    perpetual data.

    [fetch_bars] returns fetched windows concatenated, sorted, and de-duplicated (ISO
    dates sort lexicographically) but NOT source-normalized; the oracle applies
    [Oracle_calendar.normalize_bars] on every read. *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf

let section = "oracle_hyperliquid"
let endpoint = "https://api.hyperliquid.xyz/info"
let interval_daily = "1d"
let window_days = 5000
let max_windows = 60
let ms_per_day = 86_400_000L
let default_start_ms = 1640995200000L (* 2022-01-01 *)
let default_timeout = 10.0

(** Bounded JSON POST to /info: a hung upstream raises after [default_timeout] instead of
    freezing the oracle pass. *)
let post_info (payload : string) : (Cohttp.Response.t * Cohttp_lwt.Body.t) Lwt.t =
  let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
  Lwt_unix.with_timeout default_timeout (fun () ->
    Cohttp_lwt_unix.Client.post
      ~headers
      ~body:(Cohttp_lwt.Body.of_string payload)
      (Uri.of_string endpoint))
;;

(* Civil-date arithmetic (ISO date <-> unix ms). No timezone dependence (mktime is
   local-time dependent). Hinnant's days-from-civil, as in Oracle_calendar. *)

let days_from_civil y m d =
  let y = if m <= 2 then y - 1 else y in
  let era = (if y >= 0 then y else y - 399) / 400 in
  let yoe = y - (era * 400) in
  let mp = (m + 9) mod 12 in
  let doy = (((153 * mp) + 2) / 5) + d - 1 in
  let doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy in
  (era * 146097) + doe - 719468
;;

let ms_of_iso (date : string) : int64 =
  let y = int_of_string (String.sub date 0 4) in
  let m = int_of_string (String.sub date 5 2) in
  let d = int_of_string (String.sub date 8 2) in
  Int64.mul (Int64.of_int (days_from_civil y m d)) 86_400_000L
;;

let number_of_json = function
  | `Float f -> Some f
  | `Int i -> Some (float_of_int i)
  | `Intlit s ->
    (try Some (float_of_string s) with
     | _ -> None)
  | `String s ->
    (try Some (float_of_string s) with
     | _ -> None)
  | _ -> None
;;

let unix_ms_to_iso (t : int64) =
  let tm = Unix.gmtime (Int64.to_float t /. 1000.0) in
  Printf.sprintf
    "%04d-%02d-%02d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
;;

(** Canonicalizes wrapped spot base tokens, matching [Hyperliquid_instruments_feed] spot
    keys. *)
let canon_base = function
  | "UBTC" -> "BTC"
  | "UETH" -> "ETH"
  | "USOL" -> "SOL"
  | other -> other
;;

(** Feed-style spot symbol ("BTC/USDC") to candleSnapshot coin ("PURR/USDC" or "@N")
    mapping from the last spotMeta fetch. *)
let spot_meta_pairs : (string, string) Hashtbl.t = Hashtbl.create 512

let spot_meta_fetched_at : float ref = ref 0.0
let spot_meta_mutex = Mutex.create ()
let spot_meta_ttl = 6.0 *. 3600.0

(* Symbols already reported as having no spot pair this run, to avoid re-logging the same
   warning on every oracle pass. Warn once, then debug. *)
let warned_no_spot_history : (string, unit) Hashtbl.t = Hashtbl.create 32

(** Extracts (feed_symbol, candle_coin) mappings from a spotMeta response. [feed_symbol] =
    canonicalized base (UBTC/UETH/USOL) ^ "/" ^ quote. [candle_coin] = the universe
    entry's "name", accepted by candleSnapshot for spot ("PURR/USDC" or the "@N" alias). *)
let spot_meta_pairs_of_json (json : Yojson.Safe.t) : (string * string) list =
  let open Yojson.Safe.Util in
  let token_name idx =
    match member "tokens" json with
    | `List toks ->
      List.find_map
        (fun t ->
          try
            if member "index" t |> to_int = idx
            then Some (member "name" t |> to_string)
            else None
          with
          | _ -> None)
        toks
    | _ -> None
  in
  match member "universe" json with
  | `List entries ->
    entries
    |> List.filter_map (fun entry ->
      try
        let coin = member "name" entry |> to_string in
        match member "tokens" entry |> to_list with
        | b :: q :: _ ->
          (match token_name (to_int b), token_name (to_int q) with
           | Some base_name, Some quote_name ->
             Some (canon_base base_name ^ "/" ^ quote_name, coin)
           | _ -> None)
        | _ -> None
      with
      | _ -> None)
  | _ -> []
;;

(** Refresh the spot-pair mapping cache from /info [{type:spotMeta}]. TTL-guarded; a
    failed fetch logs and keeps the previous mapping (empty on first failure). *)
let refresh_spot_meta () : unit Lwt.t =
  let now = Unix.gettimeofday () in
  if now -. !spot_meta_fetched_at < spot_meta_ttl
  then Lwt.return_unit
  else
    Lwt.catch
      (fun () ->
        let payload = `Assoc [ "type", `String "spotMeta" ] |> Yojson.Safe.to_string in
        post_info payload
        >>= fun (resp, body) ->
        Cohttp_lwt.Body.to_string body
        >>= fun body_str ->
        let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
        if status <> 200
        then
          Lwt.fail
            (Failure (Printf.sprintf "Oracle_fetch_hyperliquid: spotMeta HTTP %d" status))
        else (
          let json = Yojson.Safe.from_string body_str in
          let pairs = spot_meta_pairs_of_json json in
          Mutex.lock spot_meta_mutex;
          Hashtbl.clear spot_meta_pairs;
          List.iter (fun (key, coin) -> Hashtbl.replace spot_meta_pairs key coin) pairs;
          spot_meta_fetched_at := now;
          Mutex.unlock spot_meta_mutex;
          Logging.info_f
            ~section
            "Hyperliquid spotMeta: %d spot pair(s) mapped to candle coins"
            (List.length pairs);
          Lwt.return_unit))
      (fun exn ->
        Logging.warn_f
          ~section
          "Hyperliquid spotMeta fetch failed (%s)"
          (Printexc.to_string exn);
        Lwt.return_unit)
;;

(** Resolves a config symbol to its candleSnapshot coin.
    - Bare coin: perpetual; returned uppercased.
    - "BASE/QUOTE": spot. Quote "USD" is normalized to "USDC"; lookup is by feed-style
      key. A match yields the spot coin ("PURR/USDC" or "@N"); no match yields [None] (no
      bars, never perpetual data). *)
let coin_of_symbol ~(pairs : (string * string) list) (symbol : string) : string option =
  let sym = String.trim symbol in
  if sym = ""
  then None
  else (
    let upper = String.uppercase_ascii sym in
    if not (String.contains sym '/')
    then Some upper
    else (
      let key =
        match String.split_on_char '/' upper with
        | [ base; quote ] -> base ^ "/" ^ if quote = "USD" then "USDC" else quote
        | _ -> upper
      in
      match List.assoc_opt key pairs with
      | Some candle_coin -> Some candle_coin
      | None -> None))
;;

(** One candle row
    [{"t": time_ms, "o": open, "h": high, "l": low, "c": close, "v": volume, "n": trades}]. *)
let parse_candle (j : Yojson.Safe.t) : Exchange.Types.bar option =
  let open Yojson.Safe.Util in
  try
    let num key =
      match number_of_json (j |> member key) with
      | Some f -> f
      | None -> raise Exit
    in
    Some
      { date = unix_ms_to_iso (Int64.of_float (num "t"))
      ; open_ = num "o"
      ; high = num "h"
      ; low = num "l"
      ; close = num "c"
      ; volume = num "v"
      }
  with
  | _ -> None
;;

(** Parse the candleSnapshot response body (a JSON array of candle rows). *)
let parse_candles ~(symbol : string) (json : Yojson.Safe.t) : Exchange.Types.bar list =
  match json with
  | `List rows -> rows |> List.filter_map parse_candle
  | _ ->
    failwith
      (Printf.sprintf
         "Oracle_fetch_hyperliquid.parse_candles: %s expected array, got %s"
         symbol
         (Yojson.Safe.to_string json))
;;

(** Orders fetched windows ascending by ISO date (lexicographic sort is exact). The last
    bar must be the current close: the grid start price and ladder capital math read it.
    No source normalization here; [Oracle_calendar.normalize_bars] sorts, de-dupes, and
    filters on read. *)
let windows_to_series (windows : Exchange.Types.bar list list) : Exchange.Types.bar list =
  let bars =
    List.concat windows
    |> List.sort (fun (a : Exchange.Types.bar) b -> String.compare a.date b.date)
  in
  let rec dedup (bars : Exchange.Types.bar list) =
    match bars with
    | a :: (b :: _ as rest) when a.date = b.date -> dedup rest
    | a :: rest -> a :: dedup rest
    | [] -> []
  in
  dedup bars
;;

let calendar_kind = Exchange.Types.Crypto
let fetch_calendar ~start_date:_ ~end_date:_ : string list Lwt.t = Lwt.return []

(** Fetches daily candles forward from [from] (ISO first-day; [None] = 2022-01-01) in
    day-windows. Spot symbols resolve through the feed mapping; no matching pair yields no
    bars. Bare coins are perpetuals. *)
let fetch_bars ?feed:_ ?end_date:_ ~from ~symbol () : Exchange.Types.bar list Lwt.t =
  refresh_spot_meta ()
  >>= fun () ->
  Mutex.lock spot_meta_mutex;
  let pairs = Hashtbl.fold (fun key coin acc -> (key, coin) :: acc) spot_meta_pairs [] in
  Mutex.unlock spot_meta_mutex;
  match coin_of_symbol ~pairs symbol with
  | None ->
    let first = not (Hashtbl.mem warned_no_spot_history symbol) in
    if first then Hashtbl.add warned_no_spot_history symbol ();
    if first
    then
      Logging.warn_f
        ~section
        "Hyperliquid: no spot history for %s (no matching Hyperliquid spot pair; \
         spot-named symbols never use perpetual candles). Spot-only oracle leaves this \
         asset without history -> INACTIVE. Use the bare coin name only for perpetual \
         intent."
        symbol
    else
      Logging.debug_f
        ~section
        "Hyperliquid: no spot history for %s (already reported this run)"
        symbol;
    Lwt.return []
  | Some coin ->
    let start_ms = Option.fold ~none:default_start_ms ~some:ms_of_iso from in
    let now_ms = Int64.of_float (Unix.gettimeofday () *. 1000.0) in
    let span = Int64.mul ms_per_day (Int64.of_int window_days) in
    let rec go from_ms acc windows =
      if windows = 0
      then Lwt.return acc
      else (
        let to_ms = Int64.min (Int64.add from_ms span) now_ms in
        let payload =
          `Assoc
            [ "type", `String "candleSnapshot"
            ; ( "req"
              , `Assoc
                  [ "coin", `String coin
                  ; "interval", `String interval_daily
                  ; "startTime", `Intlit (Int64.to_string from_ms)
                  ; "endTime", `Intlit (Int64.to_string to_ms)
                  ] )
            ]
          |> Yojson.Safe.to_string
        in
        let fetch =
          post_info payload
          >>= fun (resp, body) ->
          Cohttp_lwt.Body.to_string body
          >>= fun body_str ->
          let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
          if status <> 200
          then
            Lwt.fail
              (Failure
                 (Printf.sprintf
                    "Oracle_fetch_hyperliquid: HTTP %d for %s: %s"
                    status
                    symbol
                    body_str))
          else (
            let json = Yojson.Safe.from_string body_str in
            let bars = parse_candles ~symbol json in
            if to_ms >= now_ms
            then Lwt.return (bars :: acc)
            else go to_ms (bars :: acc) (windows - 1))
        in
        Lwt.catch
          (fun () -> fetch)
          (fun exn ->
            Logging.warn_f
              ~section
              "Hyperliquid candle page failed for %s (%s), returning %d bars so far"
              symbol
              (Printexc.to_string exn)
              (List.length acc);
            Lwt.return acc))
    in
    go start_ms [] max_windows >|= windows_to_series
;;

(* ---- Fees ---- *)

let fetch_fees ~testnet ~symbol : (float * float) Lwt.t =
  Hyperliquid_get_fee.get_fee_info ~testnet ()
  >|= fun info ->
  let is_spot = String.contains symbol '/' in
  match info with
  | Some f ->
    let maker =
      if is_spot
      then Option.value f.Hyperliquid_get_fee.spot_maker_fee ~default:0.0
      else Option.value f.Hyperliquid_get_fee.maker_fee ~default:0.0002
    in
    let taker =
      if is_spot
      then Option.value f.Hyperliquid_get_fee.spot_taker_fee ~default:0.001
      else Option.value f.Hyperliquid_get_fee.taker_fee ~default:0.0005
    in
    maker, taker
  | None -> (if is_spot then 0.0 else 0.0002), if is_spot then 0.001 else 0.0005
;;

let default_fees ~symbol : float * float =
  if String.contains symbol '/' then 0.0, 0.001 else 0.0002, 0.0005
;;

(* ---- Balances (spot only) ---- *)

let rec unwrap_data json =
  match Yojson.Safe.Util.member "data" json with
  | `Null -> json
  | `Assoc _ as data -> unwrap_data data
  | _ -> json
;;

let number = function
  | `Float f -> Some f
  | `Int i -> Some (float_of_int i)
  | `Intlit s ->
    (try Some (float_of_string s) with
     | _ -> None)
  | `String s ->
    (try Some (float_of_string (String.trim s)) with
     | _ -> None)
  | _ -> None
;;

let field_float json name =
  try Yojson.Safe.Util.member name json |> number with
  | _ -> None
;;

let nonnegative f = if f < 0.0 then 0.0 else f

let normalize_asset = function
  | "UBTC" -> "BTC"
  | "UETH" -> "ETH"
  | "USOL" -> "SOL"
  | value -> String.uppercase_ascii value
;;

(** Parse the spotClearinghouseState response into normalized (asset, available, total)
    triples. Available = total - hold (the spot orderbook rejects anything beyond it). *)
let parse_spot_balances (json : Yojson.Safe.t)
  : ((string * float * float) list, string) result
  =
  let json = unwrap_data json in
  match Yojson.Safe.Util.member "balances" json with
  | `List entries ->
    let parsed =
      List.filter_map
        (fun entry ->
          let open Yojson.Safe.Util in
          match to_string_option (member "coin" entry), field_float entry "total" with
          | Some coin, Some total when Float.is_finite total ->
            let hold = Option.value (field_float entry "hold") ~default:0.0 in
            Some (normalize_asset coin, nonnegative (total -. hold), nonnegative total)
          | _ -> None)
        entries
    in
    if List.length parsed = List.length entries
    then Ok parsed
    else Error "Hyperliquid spot response contained a malformed balance"
  | _ -> Error "Hyperliquid spot response has no balances list"
;;

let base_url testnet =
  if testnet then "https://api.hyperliquid-testnet.xyz" else "https://api.hyperliquid.xyz"
;;

let post_json ~url (payload : Yojson.Safe.t) : (Yojson.Safe.t, string) result Lwt.t =
  let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
  let body = Cohttp_lwt.Body.of_string (Yojson.Safe.to_string payload) in
  Lwt.catch
    (fun () ->
      Lwt_unix.with_timeout default_timeout (fun () ->
        Cohttp_lwt_unix.Client.post ~headers ~body (Uri.of_string url))
      >>= fun (response, response_body) ->
      Cohttp_lwt.Body.to_string response_body
      >|= fun body ->
      let status = Cohttp.Response.status response |> Cohttp.Code.code_of_status in
      if status < 200 || status >= 300
      then Error (Printf.sprintf "HTTP %d: %s" status body)
      else (
        try Ok (Yojson.Safe.from_string body) with
        | exn ->
          Error (Printf.sprintf "invalid JSON response: %s" (Printexc.to_string exn))))
    (fun exn -> Lwt.return (Error (Printexc.to_string exn)))
;;

let fetch_balances ~testnet : ((string * float * float) list, string) result Lwt.t =
  match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim with
  | None | Some "" -> Lwt.return (Error "HYPERLIQUID_WALLET_ADDRESS is not set")
  | Some wallet ->
    (* Spot-only pool: the spot wallet's USDC and tokens. Perp clearinghouse margin is
       reserved for perp positions and excluded from the spot grid. *)
    post_json
      ~url:(base_url testnet ^ "/info")
      (`Assoc [ "type", `String "spotClearinghouseState"; "user", `String wallet ])
    >|= (function
     | Error error -> Error ("Hyperliquid spot: " ^ error)
     | Ok spot_json -> parse_spot_balances spot_json)
;;

(** Always [None]: the WS-fed "USDC" store aggregates perp clearinghouse USDC with the
    spot wallet, whereas the oracle pool is spot-only. REST spotClearinghouseState remains
    authoritative; the runtime falls back to [fetch_balances]. *)
let live_balances () : (string * float * float) list option = None

let default_quote = "USDC"

(** Hyperliquid spot enforces a 10 USDC MinTradeSpotNtl floor on order notional; perp and
    equity-quoted symbols are not notional-constrained in this model. [symbol] carries a
    '/' for spot pairs (e.g. "BTC/USDC"); slash-less symbols are perp/futures. *)
let min_notional ~symbol = if String.contains symbol '/' then 10.0 else 0.0

(* ---- Instrument metadata ---- *)

let init_instruments ~testnet ~symbols:_ : unit Lwt.t =
  Hyperliquid_instruments_feed.fetch_meta_from_rest ~testnet ()
;;

let name = "hyperliquid"
(* Registration happens in [Hyperliquid_module] (a module cannot register itself: the
   wrapped self-path would dangle). *)
