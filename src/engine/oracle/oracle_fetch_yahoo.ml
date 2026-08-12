(* Oracle_fetch_yahoo - deep-history daily OHLC via the Yahoo Finance chart
   API (no auth, browser-ish User-Agent).

   Some venue feeds cannot supply deep history: Kraken's public OHLC is
   hard-capped at the most recent ~720 daily candles, and Alpaca's IEX feed
   starts 2020-07-27. For the same underlying asset the Yahoo chart API
   serves full daily history back to listing (BTC 2014+, ETH/XMR/ADA/DOGE
   2017+, SOL 2020+, QQQ/SPY 1999+), so those histories are used to EXTEND
   the venue series backward - the asset's own real price history, not
   fabricated data (the no-forward-fill rule still holds; nothing is
   synthesized).

   Safety: the Yahoo crypto symbol space is not authoritative. A token that
   died can leave a stale "FOO-USD" feed that Yahoo keeps serving (HYPE-USD
   still shows a dead 2021 token's prices), so crypto symbols are only mapped
   through an explicit whitelist of known-continuous pairs (LTC/XRP/LINK/AVAX
   /DOT included: continuous price charts with no forks or dead-token
   collisions). Equities are unambiguous (Yahoo QQQ is QQQ), so any equity
   symbol maps by identity.

   The API caps a request at ~2000 points, so the history is walked forward
   in ~35-month windows from the requested start. Pure [parse_*] functions
   are fixture-testable without network. *)

open Lwt.Infix
open Cohttp_lwt_unix

let section = "oracle_yahoo"
let endpoint = "https://query1.finance.yahoo.com/v8/finance/chart/%s"
let window_seconds = 1_100_000_000L (* ~35 months: ~1050 daily points per request *)
let day_seconds = 86_400L

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

let unix_to_iso (t : int64) =
  let tm = Unix.gmtime (Int64.to_float t) in
  Printf.sprintf
    "%04d-%02d-%02d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
;;

(** ISO date -> unix epoch. *)
let epoch_of_iso date =
  let tm = Unix.gmtime 0.0 in
  let y = int_of_string (String.sub date 0 4) in
  let m = int_of_string (String.sub date 5 2) in
  let d = int_of_string (String.sub date 8 2) in
  let t = Unix.mktime { tm with tm_year = y - 1900; tm_mon = m - 1; tm_mday = d } in
  Int64.of_float (fst t)
;;

(** Yahoo symbol for an asset, or None when the symbol is not trusted for
    deep history (see the module header: dead-token collisions). *)
let symbol_of ~(exchange : string) (symbol : string) : string option =
  match String.lowercase_ascii exchange with
  | "alpaca" -> Some (String.uppercase_ascii symbol)
  | _ ->
    let base =
      match String.split_on_char '/' symbol with
      | b :: _ when b <> "" -> String.uppercase_ascii b
      | _ -> String.uppercase_ascii symbol
    in
    (match base with
     | "BTC"
     | "ETH"
     | "LTC"
     | "XRP"
     | "SOL"
     | "XMR"
     | "ADA"
     | "DOGE"
     | "LINK"
     | "AVAX"
     | "DOT" -> Some (base ^ "-USD")
     | _ -> None)
;;

(** Parse one chart response into ascending daily bars. Rows with any null
    field are dropped (the API fills sparse rows with nulls). *)
let parse_daily ~(symbol : string) (json : Yojson.Safe.t) : Oracle_types.bar list =
  let open Yojson.Safe.Util in
  try
    let result = json |> member "chart" |> member "result" |> to_list in
    match result with
    | [] -> []
    | head :: _ ->
      let ts = head |> member "timestamp" |> to_list |> List.filter_map number_of_json in
      let quote = head |> member "indicators" |> member "quote" |> to_list in
      (match quote with
       | [] -> []
       | q :: _ ->
         let f key = q |> member key |> to_list |> List.map number_of_json in
         let opens = f "open" in
         let highs = f "high" in
         let lows = f "low" in
         let closes = f "close" in
         let volumes = f "volume" in
         let n = List.length ts in
         let rows = ref [] in
         for i = 0 to n - 1 do
           let num arr =
             match List.nth_opt arr i with
             | Some (Some v) when Float.is_finite v -> Some v
             | _ -> None
           in
           match num opens, num highs, num lows, num closes with
           | Some o, Some h, Some l, Some c when h >= l && c > 0.0 ->
             let volume =
               match num volumes with
               | Some v -> v
               | None -> 0.0
             in
             rows
             := { Oracle_types.date = unix_to_iso (Int64.of_float (List.nth ts i))
                ; open_ = o
                ; high = h
                ; low = l
                ; close = c
                ; volume
                }
                :: !rows
           | _ -> ()
         done;
         let bars = List.rev !rows in
         bars
         |> Array.of_list
         |> Oracle_calendar.sort_bars
         |> Oracle_calendar.dedup
         |> Array.to_list)
  with
  | _ ->
    Logging.warn_f ~section "unexpected Yahoo chart response for %s (no bars)" symbol;
    []
;;

let series_of_bars ~(symbol : string) (bars : Oracle_types.bar list) : Oracle_types.series
  =
  { Oracle_types.symbol
  ; calendar_kind = Oracle_types.Crypto
  ; bars = Array.of_list bars
  ; gaps = []
  }
;;

(** Merge a deep-history series into the venue's own series: the deep bars
    (strictly before the venue's first bar) are prepended, the venue's bars
    win on any overlap, and the result is sorted and de-duplicated by date.
    Returns the number of deep bars actually added. The venue's earliest bar
    is taken as the minimum date over its bars (venue feeds must not be
    assumed ascending). *)
let merge_series ~(venue : Oracle_types.series) ~(deep : Oracle_types.series)
  : Oracle_types.series * int
  =
  let venue_bars = venue.bars in
  let deep_bars = deep.bars in
  if Array.length venue_bars = 0
  then venue, 0
  else (
    let venue_first =
      Array.fold_left
        (fun acc (b : Oracle_types.bar) -> if b.date < acc then b.date else acc)
        venue_bars.(0).Oracle_types.date
        venue_bars
    in
    let added =
      Array.to_list deep_bars
      |> List.filter (fun (b : Oracle_types.bar) -> b.date < venue_first)
    in
    if added = []
    then venue, 0
    else (
      let merged =
        Array.of_list (added @ Array.to_list venue_bars)
        |> Oracle_calendar.sort_bars
        |> Oracle_calendar.dedup
      in
      { venue with bars = merged }, List.length added))
;;

(** Fetch daily bars for the Yahoo [symbol] from [start_date] to [end_date]
    (ISO), walking forward in fixed windows (the API caps a request at ~2000
    points). Returns what was fetched; a failed window logs a warning and
    stops the walk with what it has. *)
let fetch_daily ?(start_date = "2016-01-01") ~(symbol : string) ~(end_date : string) ()
  : Oracle_types.bar list Lwt.t
  =
  let start_epoch = epoch_of_iso start_date in
  let end_epoch = epoch_of_iso end_date in
  let base_url =
    Printf.sprintf
      "https://query1.finance.yahoo.com/v8/finance/chart/%s"
      (Uri.pct_encode symbol)
  in
  let headers = Cohttp.Header.of_list [ "User-Agent", "Mozilla/5.0 (dio-oracle)" ] in
  let rec go from_ms acc =
    if Int64.compare from_ms end_epoch > 0
    then Lwt.return (List.rev acc)
    else (
      let to_ms = Int64.min (Int64.add from_ms window_seconds) end_epoch in
      let url =
        Printf.sprintf "%s?period1=%Ld&period2=%Ld&interval=1d" base_url from_ms to_ms
      in
      let fetch =
        Client.get ~headers (Uri.of_string url)
        >>= fun (resp, body) ->
        Cohttp_lwt.Body.to_string body
        >>= fun body_str ->
        let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
        if status <> 200
        then
          Lwt.fail
            (Failure
               (Printf.sprintf
                  "Oracle_fetch_yahoo: HTTP %d for %s (%s)"
                  status
                  symbol
                  body_str))
        else (
          let json = Yojson.Safe.from_string body_str in
          let bars = parse_daily ~symbol json in
          if Int64.compare to_ms end_epoch >= 0
          then Lwt.return (List.rev_append bars acc)
          else go (Int64.add to_ms day_seconds) (List.rev_append bars acc))
      in
      Lwt.catch
        (fun () -> fetch)
        (fun exn ->
           Logging.warn_f
             ~section
             "Yahoo daily fetch for %s stopped at %s (%s), returning %d bars"
             symbol
             (unix_to_iso from_ms)
             (Printexc.to_string exn)
             (List.length acc);
           Lwt.return (List.rev acc)))
  in
  go start_epoch []
;;
