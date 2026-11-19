(** Alpaca oracle data-venue adapter; implements [Exchange_intf.Oracle.S].

    Endpoints: daily bars (/v2/stocks/[{symbol}]/bars, paginated on next_page_token),
    market calendar (/v2/calendar), account balances (account + positions via
    [Alpaca_rest]; commission-free fees), instrument metadata (static 0.01 tick /
    fractional lots, nothing to fetch).

    [parse_*] functions are fixture-testable without network. [fetch_bars] returns RAW
    bars; the oracle sorts, de-duplicates, normalizes centrally. HTTP calls are
    timeout-bounded. *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf

let section = "oracle_alpaca"
let data_base_url = "https://data.alpaca.markets"
let trading_base_url = "https://paper-api.alpaca.markets"
let max_pages = 30
let default_timeout = 10.0

(** Bounded GET: a hung upstream raises after [default_timeout]; does not freeze the
    oracle pass. *)
let get ?(headers = Cohttp.Header.init ()) (uri : Uri.t)
  : (Cohttp.Response.t * Cohttp_lwt.Body.t) Lwt.t
  =
  Lwt_unix.with_timeout default_timeout (fun () ->
    Cohttp_lwt_unix.Client.get ~headers uri)
;;

let load_dotenv () =
  try Logging.load_dotenv ~path:".env" () with
  | _ -> ()
;;

let auth_headers () =
  let key = Alpaca_types.Config.api_key () in
  let secret = Alpaca_types.Config.api_secret () in
  if key = "" || secret = ""
  then
    failwith
      "Oracle_fetch_alpaca: ALPACA_API_KEY / ALPACA_API_SECRET not set (add to .env)";
  Cohttp.Header.of_list [ "APCA-API-KEY-ID", key; "APCA-API-SECRET-KEY", secret ]
;;

let today_iso () =
  let tm = Unix.localtime (Unix.time ()) in
  Printf.sprintf
    "%04d-%02d-%02d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
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

(** Parse a /v2/bars response body. Bar values can be JSON floats or whole integers, so
    numbers are read tolerantly. *)
let parse_bars (json : Yojson.Safe.t) : Exchange.Types.bar list =
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
          { Exchange.Types.date
          ; open_ = f "o"
          ; high = f "h"
          ; low = f "l"
          ; close = f "c"
          ; volume = f "v"
          }
      with
      | _ -> None)
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

let calendar_kind = Exchange.Types.Equity

let fetch_calendar ~start_date ~end_date : string list Lwt.t =
  load_dotenv ();
  let url =
    Printf.sprintf "%s/v2/calendar?start=%s&end=%s" trading_base_url start_date end_date
  in
  get ~headers:(auth_headers ()) (Uri.of_string url)
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

(** Daily bars for [symbol] from [from] (ISO date; [None] = "2010-01-01"), paginating on
    next_page_token. [feed] is "iex" or "sip"; [end_date] bounds the request window
    (default today). *)
let fetch_range
  ~(timeframe : string)
  ~(start_date : string)
  ~(end_date : string)
  ~symbol
  ~feed
  : Exchange.Types.bar list Lwt.t
  =
  let rec go page_token acc pages =
    if pages = 0
    then Lwt.return (List.rev acc)
    else (
      let url =
        Printf.sprintf
          "%s/v2/stocks/%s/bars?start=%sT00:00:00Z&end=%sT00:00:00Z&timeframe=%s&feed=%s&limit=10000%s"
          data_base_url
          (Uri.pct_encode symbol)
          start_date
          end_date
          timeframe
          feed
          (match page_token with
           | Some t -> "&page_token=" ^ Uri.pct_encode t
           | None -> "")
      in
      let fetch =
        get ~headers:(auth_headers ()) (Uri.of_string url)
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

(** Fold intraday bars into one daily bar per session: open from the first, close from the
    last, high/low the extremes, volume summed. A fresh listing can print intraday on a
    thin feed while the daily endpoint returns nothing, and a symbol with no history gets
    no capital-oracle decision at all (its domain withholds every order). *)
let aggregate_daily (bars : Exchange.Types.bar list) : Exchange.Types.bar list =
  let order = ref [] in
  let by_date = Hashtbl.create 64 in
  List.iter
    (fun (b : Exchange.Types.bar) ->
      match Hashtbl.find_opt by_date b.date with
      | None ->
        Hashtbl.replace by_date b.date b;
        order := b.date :: !order
      | Some (acc : Exchange.Types.bar) ->
        Hashtbl.replace
          by_date
          b.date
          { acc with
            Exchange.Types.high = Float.max acc.high b.high
          ; low = Float.min acc.low b.low
          ; close = b.close
          ; volume = acc.volume +. b.volume
          })
    bars;
  List.rev_map (fun d -> Hashtbl.find by_date d) !order
;;

let iso_of_days_ago days =
  let tm = Unix.localtime (Unix.time () -. (float_of_int days *. 86_400.0)) in
  Printf.sprintf
    "%04d-%02d-%02d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
;;

let fetch_bars ?(feed = "iex") ?end_date ~from ~symbol () : Exchange.Types.bar list Lwt.t =
  load_dotenv ();
  let start_date = Option.value from ~default:"2010-01-01" in
  let end_date = Option.value end_date ~default:(today_iso ()) in
  let intraday_start =
    let recent = iso_of_days_ago 60 in
    if String.compare start_date recent > 0 then start_date else recent
  in
  (* One feed's history: daily first, then hourly folded into sessions. A recent IPO on a
     thin feed has neither daily bars nor an intraday print for the symbol, so the caller
     tries the consolidated feed next. *)
  let history_on feed =
    fetch_range ~timeframe:"1Day" ~start_date ~end_date ~symbol ~feed
    >>= fun daily ->
    if daily <> []
    then Lwt.return daily
    else
      fetch_range ~timeframe:"1Hour" ~start_date:intraday_start ~end_date ~symbol ~feed
      >|= fun intraday ->
      let sessions = aggregate_daily intraday in
      Logging.info_f
        ~section
        "%s: no daily bars on feed %s; folded %d intraday bar(s) into %d session(s)"
        symbol
        feed
        (List.length intraday)
        (List.length sessions);
      sessions
  in
  let feeds = if feed = "sip" then [ "sip" ] else [ feed; "sip" ] in
  let rec first_history = function
    | [] -> Lwt.return []
    | f :: rest ->
      Lwt.catch
        (fun () ->
          history_on f
          >>= fun bars -> if bars <> [] then Lwt.return bars else first_history rest)
        (fun exn ->
          (* A plan without consolidated data rejects the sip request; the thin feed is
             then the only source, and the oracle reports "no decision published". *)
          Logging.warn_f
            ~section
            "%s: %s history unavailable (%s)"
            symbol
            f
            (Printexc.to_string exn);
          first_history rest)
  in
  first_history feeds
;;

(* ---- Fees (Alpaca is commission-free) ---- *)

let fetch_fees ~testnet:_ ~symbol:_ : (float * float) Lwt.t = Lwt.return (0.0, 0.0)
let default_fees ~symbol:_ : float * float = 0.0, 0.0

(* ---- Balances ---- *)

let nonnegative f = if f < 0.0 then 0.0 else f

let fetch_balances ~testnet : ((string * float * float) list, string) result Lwt.t =
  load_dotenv ();
  Alpaca_types.Config.set_testnet testnet;
  Lwt.catch
    (fun () ->
      Alpaca_rest.get_account ()
      >>= function
      | Error error -> Lwt.return (Error error)
      | Ok account ->
        Alpaca_rest.get_positions ()
        >|= (function
         | Error error -> Error error
         | Ok positions ->
           let account_balance =
             ( String.uppercase_ascii account.currency
             , nonnegative account.cash
             , nonnegative account.equity )
           in
           let position_balances =
             List.map
               (fun (position : Alpaca_types.position_record) ->
                 ( String.uppercase_ascii position.symbol
                 , nonnegative position.qty
                 , nonnegative position.qty ))
               positions
           in
           Ok (account_balance :: position_balances)))
    (fun exn -> Lwt.return (Error (Printexc.to_string exn)))
;;

(** Live websocket-fed balance snapshot: cash (available), equity (total), and per-symbol
    positions, mirroring [fetch_balances]. Returns [Some] triples when the store holds
    data, [None] otherwise (the oracle runtime falls back to the REST one-shot). *)
let live_balances () : (string * float * float) list option =
  match Exchange.Registry.get "alpaca" with
  | None -> None
  | Some (module Ex) ->
    let balances = Ex.get_all_balances () in
    if balances = []
    then None
    else
      Some
        (List.map
           (fun (asset, total) ->
             let available =
               try Ex.get_tradeable_balance ~asset with
               | _ -> 0.0
             in
             asset, available, total)
           balances)
;;

let default_quote = "USD"

(** Venue floor is DOLLAR notional, not base quantity: Alpaca requires at least $1 order
    value (minimum fractional-share order). The sell-leg inventory gate attempts a sell
    only when non-accrued inventory is worth >= this. *)
let min_notional ~symbol:_ = 1.0

(* ---- Instrument metadata (static 0.01 tick, fractional lots) ---- *)

let init_instruments ~testnet:_ ~symbols:_ : unit Lwt.t = Lwt.return_unit
let name = "alpaca"
(* Registered in [Alpaca_module]; a module cannot register itself (the wrapped self-path
   dangles). *)
