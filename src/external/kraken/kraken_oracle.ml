(** Kraken oracle data-venue adapter.

    Implements [Exchange_intf.Oracle.S] for the capital oracle's data
    layer: historical daily OHLC (/0/public/OHLC, paginated on "last"),
    account fees (TradeVolume via [Kraken_get_fee]), the trade-wallet balance
    snapshot (/0/private/Balance) and instrument metadata (price tick / lot
    size via [Kraken_instruments_feed]).

    The pure [parse_*] functions are fixture-testable without network.
    [fetch_bars] returns RAW bars (any order); the oracle sorts, de-duplicates
    and normalizes centrally (normalize is idempotent, so this contract is
    safe). HTTP calls are timeout-bounded so a blackholed upstream cannot
    freeze the oracle pass (mirrors the oracle's own [Oracle_http] wrapper,
    which is not reachable from the venue library). *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf

let section = "oracle_kraken"
let endpoint = "https://api.kraken.com/0/public/OHLC"
let interval_daily = 1440
let max_pages = 60
let default_timeout = 10.0

(** Bounded GET: a hung upstream raises after [default_timeout] instead of
    freezing the oracle pass. *)
let get (uri : Uri.t) : (Cohttp.Response.t * Cohttp_lwt.Body.t) Lwt.t =
  Lwt_unix.with_timeout default_timeout (fun () -> Cohttp_lwt_unix.Client.get uri)
;;

(** Bounded signed POST (the /0/private/* auth pattern used by the balance
    snapshot). *)
let post_signed
      ~(api_key : string)
      ~(api_secret : string)
      ~(path : string)
      ~(body : string)
  : (Cohttp.Response.t * Cohttp_lwt.Body.t) Lwt.t
  =
  let nonce = Kraken_common_types.nonce () in
  let signature = Kraken_common_types.sign ~secret:api_secret ~path ~body ~nonce in
  let headers =
    Cohttp.Header.of_list
      [ "API-Key", api_key
      ; "API-Sign", signature
      ; "Content-Type", "application/x-www-form-urlencoded"
      ]
  in
  Lwt_unix.with_timeout default_timeout (fun () ->
    Cohttp_lwt_unix.Client.post
      ~headers
      ~body:(Cohttp_lwt.Body.of_string body)
      (Uri.of_string ("https://api.kraken.com" ^ path)))
;;

(* ---- Civil-date arithmetic (ISO date <-> unix time). No timezone
   dependence: the oracle's calendar rules forbid mktime (which is local-time
   dependent). Hinnant's days-from-civil, same as Oracle_calendar. *)

let days_from_civil y m d =
  let y = if m <= 2 then y - 1 else y in
  let era = (if y >= 0 then y else y - 399) / 400 in
  let yoe = y - (era * 400) in
  let mp = (m + 9) mod 12 in
  let doy = (((153 * mp) + 2) / 5) + d - 1 in
  let doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy in
  (era * 146097) + doe - 719468
;;

let unix_of_iso (date : string) : int64 =
  let y = int_of_string (String.sub date 0 4) in
  let m = int_of_string (String.sub date 5 2) in
  let d = int_of_string (String.sub date 8 2) in
  Int64.of_int (days_from_civil y m d * 86400)
;;

let unix_to_iso (t : int) =
  let tm = Unix.gmtime (float_of_int t) in
  Printf.sprintf
    "%04d-%02d-%02d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
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

(** One candle row [time, open, high, low, close, vwap, volume, count]. *)
let parse_candle (j : Yojson.Safe.t) : Exchange.Types.bar option =
  let open Yojson.Safe.Util in
  try
    let fields = j |> to_list in
    let time = fields |> List.hd |> to_int in
    let num i =
      match number_of_json (List.nth fields i) with
      | Some f -> f
      | None -> raise Exit
    in
    Some
      { date = unix_to_iso time
      ; open_ = num 1
      ; high = num 2
      ; low = num 3
      ; close = num 4
      ; volume =
          (match number_of_json (List.nth fields 6) with
           | Some f -> f
           | None -> 0.0)
      }
  with
  | _ -> None
;;

(** Parse the /0/public/OHLC response body. The pair key in ["result"] is not
    reliable (e.g. BTC/USD arrives as XBTUSD), so every list under ["result"]
    except ["last"] is parsed. *)
let parse_ohlc ~(symbol : string) (json : Yojson.Safe.t) : Exchange.Types.bar list =
  let open Yojson.Safe.Util in
  let errors = member "error" json |> to_list in
  if errors <> []
  then
    failwith
      (Printf.sprintf
         "Oracle_fetch_kraken.parse_ohlc: %s errors %s"
         symbol
         (Yojson.Safe.to_string json));
  json
  |> member "result"
  |> to_assoc
  |> List.filter (fun (k, _) -> k <> "last")
  |> List.concat_map (fun (_, v) -> v |> to_list |> List.filter_map parse_candle)
;;

let calendar_kind = Exchange.Types.Crypto
let fetch_calendar ~start_date:_ ~end_date:_ : string list Lwt.t = Lwt.return []

(** Fetch daily OHLC back to [from] (ISO date of the first day; [None] = the
    pair's full history), paginating on ["last"]. *)
let fetch_bars ?feed:_ ?end_date:_ ~from ~symbol () : Exchange.Types.bar list Lwt.t =
  let since = Option.fold ~none:0L ~some:unix_of_iso from in
  let rec go since acc pages =
    if pages = 0
    then Lwt.return (List.rev acc)
    else (
      let url =
        Printf.sprintf
          "%s?pair=%s&interval=%d&since=%s"
          endpoint
          (Uri.pct_encode symbol)
          interval_daily
          (Int64.to_string since)
      in
      let fetch =
        get (Uri.of_string url)
        >>= fun (resp, body) ->
        Cohttp_lwt.Body.to_string body
        >>= fun body_str ->
        let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
        if status <> 200
        then
          Lwt.fail
            (Failure
               (Printf.sprintf
                  "Oracle_fetch_kraken: HTTP %d for %s: %s"
                  status
                  symbol
                  body_str))
        else (
          let json = Yojson.Safe.from_string body_str in
          let bars = parse_ohlc ~symbol json in
          let last =
            try
              json
              |> Yojson.Safe.Util.member "result"
              |> Yojson.Safe.Util.member "last"
              |> function
              | `Intlit s -> Int64.of_string s
              | `Int i -> Int64.of_int i
              | `Float f -> Int64.of_float f
              | `String s -> Int64.of_string s
              | _ -> raise Exit
            with
            | _ -> since
          in
          if last = since
          then Lwt.return (List.rev (List.rev_append bars acc))
          else go last (List.rev_append bars acc) (pages - 1))
      in
      Lwt.catch
        (fun () -> fetch)
        (fun exn ->
           Logging.warn_f
             ~section
             "Kraken OHLC page failed for %s (%s), returning %d bars so far"
             symbol
             (Printexc.to_string exn)
             (List.length acc);
           Lwt.return (List.rev acc)))
  in
  go since [] max_pages
;;

(* ---- Fees ---- *)

let fetch_fees ~testnet:_ ~symbol : (float * float) Lwt.t =
  Kraken_get_fee.get_fee_info symbol
  >|= fun info ->
  match info with
  | Some f ->
    ( Option.value f.Kraken_get_fee.maker_fee ~default:0.0016
    , Option.value f.Kraken_get_fee.taker_fee ~default:0.0026 )
  | None -> 0.0016, 0.0026
;;

let default_fees ~symbol:_ : float * float = 0.0016, 0.0026

(* ---- Balances ---- *)

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

let nonnegative f = if f < 0.0 then 0.0 else f

let normalize_asset raw =
  let upper = String.uppercase_ascii (String.trim raw) in
  let upper =
    List.fold_left
      (fun value suffix ->
         if
           String.length value > String.length suffix
           && String.sub
                value
                (String.length value - String.length suffix)
                (String.length suffix)
              = suffix
         then String.sub value 0 (String.length value - String.length suffix)
         else value)
      upper
      [ ".HOLD"; ".F"; ".B" ]
  in
  match upper with
  | "XXBT" | "XBT" -> "BTC"
  | "XETH" -> "ETH"
  | "XXDG" -> "DOGE"
  | "ZUSD" -> "USD"
  | "ZEUR" -> "EUR"
  | "ZGBP" -> "GBP"
  | "ZJPY" -> "JPY"
  | value -> value
;;

let error_fields json =
  match Yojson.Safe.Util.member "error" json with
  | `List [] | `Null -> None
  | `List errors ->
    Some
      (List.filter_map
         (function
           | `String s -> Some s
           | other ->
             (try Some (Yojson.Safe.to_string other) with
              | _ -> None))
         errors
       |> String.concat "; ")
  | other -> Some (Yojson.Safe.to_string other)
;;

(** Parse the /0/private/Balance response into normalized
    (asset, available, total) triples. The trade wallet has no hold concept,
    so available = total. *)
let parse_balances (json : Yojson.Safe.t) : ((string * float * float) list, string) result
  =
  match error_fields json with
  | Some error when error <> "" -> Error ("Kraken API error: " ^ error)
  | _ ->
    (match Yojson.Safe.Util.member "result" json with
     | `Assoc entries ->
       let parsed =
         List.filter_map
           (fun (asset, value) ->
              match number value with
              | Some total when Float.is_finite total ->
                Some (normalize_asset asset, nonnegative total, nonnegative total)
              | _ -> None)
           entries
       in
       if List.length parsed = List.length entries
       then Ok parsed
       else Error "Kraken balance response contained a malformed amount"
     | _ -> Error "Kraken balance response has no result object")
;;

let fetch_balances ~testnet:_ : ((string * float * float) list, string) result Lwt.t =
  Lwt.catch
    (fun () ->
       Kraken_generate_auth_token.get_api_credentials_from_env ()
       >>= fun (api_key, api_secret) ->
       let path = "/0/private/Balance" in
       let body = "nonce=" ^ Kraken_common_types.nonce () in
       post_signed ~api_key ~api_secret ~path ~body
       >>= fun (response, response_body) ->
       Cohttp_lwt.Body.to_string response_body
       >|= fun body ->
       let status = Cohttp.Response.status response |> Cohttp.Code.code_of_status in
       if status <> 200
       then Error (Printf.sprintf "Kraken HTTP %d: %s" status body)
       else (
         try parse_balances (Yojson.Safe.from_string body) with
         | exn ->
           Error
             (Printf.sprintf "Kraken response parse failed: %s" (Printexc.to_string exn))))
    (fun exn -> Lwt.return (Error (Printexc.to_string exn)))
;;

(* ---- Instrument metadata ---- *)

let init_instruments ~testnet:_ ~symbols : unit Lwt.t =
  Kraken_instruments_feed.initialize_symbols symbols
;;

let name = "kraken"
(* Registration happens in [Kraken_module] (a module cannot register itself:
   the wrapped self-path would dangle). *)
