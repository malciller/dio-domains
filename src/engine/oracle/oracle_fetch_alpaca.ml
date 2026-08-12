(* Oracle_fetch_alpaca - historical daily bars and the market calendar for
   equity via the Alpaca API (data + paper endpoints).

   - /v2/stocks/{symbol}/bars (daily, IEX/SIP per data_feed) -> equity series.
   - /v2/calendar (paper endpoint, key from .env) -> expected session dates;
     holidays are US weekdays missing from that calendar.

   The expected-session model adapts the regular-session notion of
   Alpaca_market_hours (US Eastern Mon-Fri) and subtracts the holiday set, so
   equity gap detection reports market holidays as skipped sessions. Pure
   [parse_*] functions are fixture-testable without network. *)

open Lwt.Infix
open Cohttp_lwt_unix

let section = "oracle_alpaca"
let data_base_url = "https://data.alpaca.markets"
let trading_base_url = "https://paper-api.alpaca.markets"
let max_pages = 30

let load_dotenv () =
  try Dotenv.export ~path:".env" () with
  | _ -> ()
;;

let auth_headers () =
  let key = Alpaca.Types.Config.api_key () in
  let secret = Alpaca.Types.Config.api_secret () in
  if key = "" || secret = ""
  then
    failwith
      "Oracle_fetch_alpaca: ALPACA_API_KEY / ALPACA_API_SECRET not set (add to .env)";
  Cohttp.Header.of_list [ "APCA-API-KEY-ID", key; "APCA-API-SECRET-KEY", secret ]
;;

let iso_date_of_timestamp t = if String.length t >= 10 then String.sub t 0 10 else t

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

(** Parse a /v2/bars response body. Bar values can be JSON floats or whole
    integers, so numbers are read tolerantly. *)
let parse_bars (json : Yojson.Safe.t) : Oracle_types.bar list =
  let open Yojson.Safe.Util in
  match member "bars" json with
  | `List items ->
    items
    |> List.filter_map (fun b ->
      try
        let date = member "t" b |> to_string |> iso_date_of_timestamp in
        let f key =
          match number_of_json (member key b) with
          | Some x -> x
          | None -> raise Exit
        in
        Some
          { Oracle_types.date
          ; open_ = f "o"
          ; high = f "h"
          ; low = f "l"
          ; close = f "c"
          ; volume = f "v"
          }
      with
      | _ -> None)
    |> Array.of_list
    |> Oracle_calendar.sort_bars
    |> Oracle_calendar.dedup
    |> Array.to_list
  | _ -> []
;;

(** Parse a /v2/calendar response body into ascending session dates. *)
let parse_calendar (json : Yojson.Safe.t) : string list =
  let open Yojson.Safe.Util in
  json
  |> to_list
  |> List.filter_map (fun d -> member "date" d |> to_string_option)
  |> List.sort_uniq String.compare
;;

(** Expected-session model over the Alpaca regular-session weekdays minus the
    holiday set: US Eastern Mon-Fri (as per Alpaca_market_hours), minus the
    weekdays the /v2/calendar does not list. *)
let model_of_calendar_dates (calendar_dates : string list) : Oracle_sessions.model =
  Oracle_sessions.alpaca_model calendar_dates
;;

(** Adapter over Alpaca_market_hours: its regular session is US Eastern
    weekdays; holidays are subtracted from the /v2/calendar feed. *)
let regular_session_model () : Oracle_sessions.model = Oracle_sessions.business_weekday

let series_of_bars ~(symbol : string) (bars : Oracle_types.bar list) : Oracle_types.series
  =
  { Oracle_types.symbol
  ; calendar_kind = Oracle_types.Equity
  ; bars = Array.of_list bars
  ; gaps = []
  }
;;

(** Fetch daily bars for [symbol] over [start_date]..[end_date], paginating on
    next_page_token. *)
let fetch_bars
      ?(feed = "iex")
      ~(symbol : string)
      ~(start_date : string)
      ~(end_date : string)
      ()
  : Oracle_types.bar list Lwt.t
  =
  load_dotenv ();
  let rec go page_token acc pages =
    if pages = 0
    then Lwt.return (List.rev acc)
    else (
      let url =
        Printf.sprintf
          "%s/v2/stocks/%s/bars?start=%sT00:00:00Z&end=%sT00:00:00Z&timeframe=1Day&feed=%s&limit=10000%s"
          data_base_url
          (Uri.pct_encode symbol)
          start_date
          end_date
          feed
          (match page_token with
           | Some t -> "&page_token=" ^ Uri.pct_encode t
           | None -> "")
      in
      let fetch =
        Client.get ~headers:(auth_headers ()) (Uri.of_string url)
        >>= fun (resp, body) ->
        Cohttp_lwt.Body.to_string body
        >>= fun body_str ->
        let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
        if status <> 200
        then
          Lwt.fail
            (Failure
               (Printf.sprintf
                  "Oracle_fetch_alpaca: HTTP %d for %s: %s"
                  status
                  symbol
                  body_str))
        else (
          let json = Yojson.Safe.from_string body_str in
          let bars = parse_bars json in
          let next =
            match Yojson.Safe.Util.member "next_page_token" json with
            | `String s when s <> "" -> Some s
            | _ -> None
          in
          match next with
          | None -> Lwt.return (List.rev (List.rev_append bars acc))
          | Some t -> go (Some t) (List.rev_append bars acc) (pages - 1))
      in
      Lwt.catch
        (fun () -> fetch)
        (fun exn ->
           Logging.warn_f
             ~section
             "Alpaca bars fetch failed for %s (%s), returning %d bars so far"
             symbol
             (Printexc.to_string exn)
             (List.length acc);
           Lwt.return (List.rev acc)))
  in
  go None [] max_pages
;;

(** Fetch the market calendar (open days) over [start_date]..[end_date]. *)
let fetch_calendar ~(start_date : string) ~(end_date : string) () : string list Lwt.t =
  load_dotenv ();
  let url =
    Printf.sprintf "%s/v2/calendar?start=%s&end=%s" trading_base_url start_date end_date
  in
  Client.get ~headers:(auth_headers ()) (Uri.of_string url)
  >>= fun (resp, body) ->
  Cohttp_lwt.Body.to_string body
  >>= fun body_str ->
  let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
  if status <> 200
  then
    Lwt.fail
      (Failure
         (Printf.sprintf "Oracle_fetch_alpaca: calendar HTTP %d: %s" status body_str))
  else Lwt.return (parse_calendar (Yojson.Safe.from_string body_str))
;;
