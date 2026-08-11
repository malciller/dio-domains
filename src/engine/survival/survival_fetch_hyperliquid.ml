(* Survival_fetch_hyperliquid - historical daily OHLC for crypto via the
   Hyperliquid public Info API (no auth). Endpoint /info with a POST body
   {"type":"candleSnapshot","req":{coin, interval:"1d", startTime, endTime}},
   paginated forward in day-windows. Pure [parse_*] functions are
   fixture-testable without network. *)

open Lwt.Infix
open Cohttp_lwt_unix

let section = "survival_hyperliquid"
let endpoint = "https://api.hyperliquid.xyz/info"
let interval_daily = "1d"
let window_days = 5000
let max_windows = 60
let ms_per_day = 86_400_000L
let default_start_ms = 1640995200000L (* 2022-01-01 *)

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

(** Config symbol "BTC/USDC" -> coin "BTC". *)
let coin_of_symbol (symbol : string) : string =
  match String.split_on_char '/' symbol with
  | c :: _ when c <> "" -> String.uppercase_ascii c
  | _ -> String.uppercase_ascii symbol
;;

(** One candle row {"t": time_ms, "o": open, "h": high, "l": low, "c": close,
    "v": volume, "n": trades}. *)
let parse_candle (j : Yojson.Safe.t) : Survival_types.bar option =
  let open Yojson.Safe.Util in
  try
    let num key =
      match number_of_json (j |> member key) with
      | Some f -> f
      | None -> raise Exit
    in
    Some
      { Survival_types.date = unix_ms_to_iso (Int64.of_float (num "t"))
      ; open_ = num "o"
      ; high = num "h"
      ; low = num "l"
      ; close = num "c"
      ; volume = num "v"
      }
  with
  | _ -> None
;;

(** Parse the candleSnapshot response body (a JSON array of candle rows).
    Ascending, de-duplicated by date. *)
let parse_candles ~(symbol : string) (json : Yojson.Safe.t) : Survival_types.bar list =
  match json with
  | `List rows ->
    rows
    |> List.filter_map parse_candle
    |> Array.of_list
    |> Survival_calendar.sort_bars
    |> Survival_calendar.dedup
    |> Array.to_list
  | _ ->
    failwith
      (Printf.sprintf
         "Survival_fetch_hyperliquid.parse_candles: %s expected array, got %s"
         symbol
         (Yojson.Safe.to_string json))
;;

let series_of_bars ~(symbol : string) (bars : Survival_types.bar list)
  : Survival_types.series
  =
  { Survival_types.symbol
  ; calendar_kind = Survival_types.Crypto
  ; bars = Array.of_list bars
  ; gaps = []
  }
;;

(** Fetch daily candles forward from [start_ms] (unix ms), in day-windows. *)
let fetch_candles ?(start_ms = default_start_ms) ~(symbol : string) ()
  : Survival_types.bar list Lwt.t
  =
  let coin = coin_of_symbol symbol in
  let now_ms = Int64.of_float (Unix.gettimeofday () *. 1000.0) in
  let span = Int64.mul ms_per_day (Int64.of_int window_days) in
  let rec go from_ms acc windows =
    if windows = 0
    then Lwt.return (List.rev acc)
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
      let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
      let fetch =
        Client.post
          ~headers
          ~body:(Cohttp_lwt.Body.of_string payload)
          (Uri.of_string endpoint)
        >>= fun (resp, body) ->
        Cohttp_lwt.Body.to_string body
        >>= fun body_str ->
        let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
        if status <> 200
        then
          Lwt.fail
            (Failure
               (Printf.sprintf
                  "Survival_fetch_hyperliquid: HTTP %d for %s: %s"
                  status
                  symbol
                  body_str))
        else (
          let json = Yojson.Safe.from_string body_str in
          let bars = parse_candles ~symbol json in
          if to_ms >= now_ms
          then Lwt.return (List.rev_append bars acc)
          else go to_ms (List.rev_append bars acc) (windows - 1))
      in
      Lwt.catch
        (fun () -> fetch)
        (fun exn ->
           Logging.warn_f
             ~section
             "Hyperliquid candle page failed (%s), returning %d bars so far"
             (Printexc.to_string exn)
             (List.length acc);
           Lwt.return (List.rev acc)))
  in
  go start_ms [] max_windows
;;
