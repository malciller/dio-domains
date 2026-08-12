(* Oracle_fetch_kraken - historical daily OHLC for crypto via the Kraken
   public API (no auth). Endpoint /0/public/OHLC with interval=1440, paginated
   through the response's ["last"] timestamp. Pure [parse_*] functions are
   fixture-testable without network. *)

open Lwt.Infix

let section = "oracle_kraken"
let endpoint = "https://api.kraken.com/0/public/OHLC"
let interval_daily = 1440
let max_pages = 60

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
let parse_candle (j : Yojson.Safe.t) : Oracle_types.bar option =
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
      { Oracle_types.date = unix_to_iso time
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
    except ["last"] is parsed. Ascending, de-duplicated by date. *)
let parse_ohlc ~(symbol : string) (json : Yojson.Safe.t) : Oracle_types.bar list =
  let open Yojson.Safe.Util in
  let errors = member "error" json |> to_list in
  if errors <> []
  then
    failwith
      (Printf.sprintf
         "Oracle_fetch_kraken.parse_ohlc: %s errors %s"
         symbol
         (Yojson.Safe.to_string json));
  let candles =
    json
    |> member "result"
    |> to_assoc
    |> List.filter (fun (k, _) -> k <> "last")
    |> List.concat_map (fun (_, v) -> v |> to_list |> List.filter_map parse_candle)
  in
  candles
  |> Array.of_list
  |> Oracle_calendar.sort_bars
  |> Oracle_calendar.dedup
  |> Array.to_list
;;

let series_of_bars ~(symbol : string) (bars : Oracle_types.bar list) : Oracle_types.series
  =
  { Oracle_types.symbol
  ; calendar_kind = Oracle_types.Crypto
  ; bars = Array.of_list bars
  ; gaps = []
  }
;;

(** Fetch daily OHLC back to [since] (unix seconds), paginating on ["last"]. *)
let fetch_ohlc ?(since = 0L) ~(symbol : string) () : Oracle_types.bar list Lwt.t =
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
        Oracle_http.get (Uri.of_string url)
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
             "Kraken OHLC page failed (%s), returning %d bars so far"
             (Printexc.to_string exn)
             (List.length acc);
           Lwt.return (List.rev acc)))
  in
  go since [] max_pages
;;
