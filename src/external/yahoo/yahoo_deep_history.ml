(* Yahoo Finance chart API deep-history daily OHLC client. No auth; uses a
   browser-like User-Agent.

   Rationale: venue feeds cannot supply deep history - Kraken public OHLC is
   hard-capped at the most recent ~720 daily candles, Alpaca IEX starts
   2020-07-27. Yahoo serves full daily history back to listing for the same
   asset (BTC 2014+, ETH/XMR/ADA/DOGE 2017+, SOL 2020+, QQQ/SPY 1999+), so it
   EXTENDS the venue series backward with real price history. No synthesis, no
   forward-fill.

   Safety: the Yahoo crypto symbol space is not authoritative. A dead token can
   leave a stale "FOO-USD" feed Yahoo keeps serving (HYPE-USD serves a dead 2021
   token's prices), so crypto symbols map only through a whitelist of
   known-continuous pairs (LTC/XRP/LINK/AVAX/DOT included: continuous charts
   with no forks or dead-token collisions). Equity symbols map by identity.

   The walk advances in [window_seconds] strides from the requested start,
   sized to cover a full listing-to-now span in one request; the loop is a
   fallback for longer spans. Pure [parse_*] functions are fixture-testable
   without network.

   Unit: [dio.yahoo] (src/external/yahoo/), a leaf data client (bar/date
   helpers from [Exchange_intf.Types]) consumed by the capital oracle's
   deep-history pipeline in [oracle_fetch.ml]. *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf

let section = "yahoo"
let window_seconds = 1_100_000_000L (* ~35y: full listing-to-now span per request *)
let day_seconds = 86_400L

(* Yahoo throttles sustained bursts; the crumbless chart API degrades to empty
   200s when hammered (the "unexpected ... (no bars)" symptom). Oracle fetches
   run concurrently, so two guards apply: [yahoo_mutex] serializes the walks
   (one at a time), and [pace] enforces a global minimum gap between individual
   requests (~2/s). *)
let yahoo_mutex = Lwt_mutex.create ()
let last_request_at : float ref = ref 0.0
let min_request_gap = 0.5

let pace () =
  let now = Unix.gettimeofday () in
  let wait = !last_request_at +. min_request_gap -. now in
  if wait > 0.0
  then Lwt_unix.sleep wait >|= fun () -> last_request_at := Unix.gettimeofday ()
  else (
    last_request_at := now;
    Lwt.return_unit)
;;

(* Yahoo soft-blocks hammered IPs by serving empty 200s ("result": null) for a
   while instead of a 429. Without memory of the block, the walk returns []
   every pass and the oracle re-attempts the whole history each refresh, which
   keeps the block alive. On the all-empty signature the symbol is remembered
   for [soft_block_backoff] seconds and its requests are skipped entirely. *)
let soft_blocked_until : (string, float) Hashtbl.t = Hashtbl.create 64
let soft_block_backoff = 300.0

let remember_block ~(symbol : string) ~(windows : int) =
  let until = Unix.gettimeofday () +. soft_block_backoff in
  Hashtbl.replace soft_blocked_until symbol until;
  Logging.warn_f
    ~section
    "Yahoo served %d empty response(s) for %s (soft-blocked/rate-limited IP); backing \
     off %d seconds before trying again"
    windows
    symbol
    (int_of_float soft_block_backoff)
;;


(* Pre-listing window handling: a request whose range lies entirely before the
   symbol's listing is answered with HTTP 400 and "Data doesn't exist for
   startDate = ...". The walk skips such windows instead of failing and caches
   the confirmed empty prefix per symbol, so later fetches clamp their start
   date past it (zero requests for the empty range). *)

(** Classify a failed window request: a Yahoo "data doesn't exist" answer is an
    empty range (skip it); anything else is a real failure (stop). *)
let classify_error (status : int) (body : string) : [ `Missing_data | `Fatal ] =
  if
    status = 400
    &&
    let b = String.lowercase_ascii body in
    let needle = "data doesn't exist" in
    let nl = String.length needle in
    let hl = String.length b in
    let rec go i = i + nl <= hl && (String.sub b i nl = needle || go (i + 1)) in
    nl > 0 && go 0
  then `Missing_data
  else `Fatal
;;

(** Classify a fetch exception: the [Failure] message carries the
    "HTTP <status> for <symbol> (<body>)" envelope; extract the status and
    response body to tell a pre-listing empty range from a real failure. *)
let classify_exn (exn : exn) : [ `Missing_data | `Fatal ] =
  match exn with
  | Failure msg ->
    let body =
      try
        let i = String.index msg '{' in
        String.sub msg i (String.length msg - i)
      with
      | Not_found -> ""
    in
    let status =
      let rec find i =
        if i + 5 > String.length msg
        then 0
        else if String.sub msg i 5 = "HTTP "
        then (
          let a = i + 5 in
          let rec digits j =
            if j < String.length msg && msg.[j] >= '0' && msg.[j] <= '9'
            then digits (j + 1)
            else j
          in
          try int_of_string (String.sub msg a (digits a - a)) with
          | _ -> 0)
        else find (i + 1)
      in
      find 0
    in
    classify_error status body
  | _ -> `Fatal
;;

(** Per-symbol cache of the confirmed-empty history prefix: the latest end
    date for which Yahoo answered "no data in [requested start, end]". Fetches
    clamp their start past it. Process-lifetime; the oracle re-fetches deep
    history every pass. *)
let no_data_before : (string, string) Hashtbl.t = Hashtbl.create 16

let known_empty_before ~(symbol : string) : string option =
  Hashtbl.find_opt no_data_before symbol
;;

let remember_empty ~(symbol : string) (date : string) =
  match Hashtbl.find_opt no_data_before symbol with
  | Some prev when prev >= date -> ()
  | _ -> Hashtbl.replace no_data_before symbol date
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

(** Yahoo symbol for an asset, or [None] when the symbol is not trusted for
    deep history (see the module header: dead-token collisions). Equity maps by
    identity (Yahoo QQQ is QQQ); crypto only through the whitelist of
    known-continuous pairs. *)
let symbol_of ~(calendar_kind : Exchange.Types.calendar_kind) (symbol : string)
  : string option
  =
  match calendar_kind with
  | Exchange.Types.Equity -> Some (String.uppercase_ascii symbol)
  | Exchange.Types.Crypto ->
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
let parse_daily ~(symbol : string) (json : Yojson.Safe.t) : Exchange.Types.bar list =
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
             := { Exchange.Types.date = unix_to_iso (Int64.of_float (List.nth ts i))
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
         |> Exchange.Types.sort_bars
         |> Exchange.Types.dedup
         |> Array.to_list)
  with
  | _ ->
    Logging.warn_f ~section "unexpected Yahoo chart response for %s (no bars)" symbol;
    []
;;

(** HTTP GET with [default_timeout]; a request the upstream blackholes must not
    freeze a fetch forever. Raises on timeout/transport errors like Cohttp. *)
let default_timeout = 10.0

let get ?(headers = Cohttp.Header.init ()) (uri : Uri.t)
  : (Cohttp.Response.t * Cohttp_lwt.Body.t) Lwt.t
  =
  Lwt_unix.with_timeout default_timeout (fun () ->
    Cohttp_lwt_unix.Client.get ~headers uri)
;;

(** Fetch daily bars for the Yahoo [symbol] from [start_date] to [end_date]
    (ISO), walking forward in fixed windows (the API caps a request at ~2000
    points). Windows Yahoo reports pre-listing ("Data doesn't exist") are
    SKIPPED rather than aborting the walk; the confirmed-empty prefix is cached
    per symbol so the next fetch clamps its start past it (no re-requesting
    dates that do not exist). Returns what was fetched; a real (non-empty-range)
    failure logs a warning and stops the walk with what it has. *)
let fetch_daily ?(start_date = "2016-01-01") ~(symbol : string) ~(end_date : string) ()
  : Exchange.Types.bar list Lwt.t
  =
  let start_epoch = epoch_of_iso start_date in
  let end_epoch = epoch_of_iso end_date in
  (* Clamp past the confirmed-empty prefix: no data exists before it, so a
     repeat request reproduces the same 400. *)
  let start_epoch =
    match known_empty_before ~symbol with
    | Some floor when epoch_of_iso floor >= start_epoch ->
      let clamp = Int64.add (epoch_of_iso floor) day_seconds in
      if Int64.compare clamp start_epoch > 0 then clamp else start_epoch
    | _ -> start_epoch
  in
  if Int64.compare start_epoch end_epoch > 0
  then (
    (* Whole requested range known empty: no request. *)
    Logging.debug_f
      ~section
      "Yahoo daily fetch for %s: whole range [%s, %s] before the known listing (no data \
       exists); skipping %d request(s)"
      symbol
      (unix_to_iso start_epoch)
      (unix_to_iso end_epoch)
      0;
    Lwt.return [])
  else (
    (* Soft-block memory (see [remember_block]): while backed off, do not even
       attempt the requests, so the pass does not keep the block alive. *)
    match Hashtbl.find_opt soft_blocked_until symbol with
    | Some until when Unix.gettimeofday () < until ->
      Logging.debug_f
        ~section
        "Yahoo soft-blocked for %s; backing off (%.0fs left)"
        symbol
        (until -. Unix.gettimeofday ());
      Lwt.return []
    | _ ->
      let base_url =
        Printf.sprintf
          "https://query1.finance.yahoo.com/v8/finance/chart/%s"
          (Uri.pct_encode symbol)
      in
      let headers = Cohttp.Header.of_list [ "User-Agent", "Mozilla/5.0 (dio-oracle)" ] in
      let rec go from_ms acc ~(skipped : int) ~(empty_200 : int) =
        if Int64.compare from_ms end_epoch > 0
        then Lwt.return (List.rev acc, skipped, empty_200)
        else (
          let to_ms = Int64.min (Int64.add from_ms window_seconds) end_epoch in
          if Int64.compare to_ms from_ms <= 0
          then Lwt.return (List.rev acc, skipped, empty_200)
          else (
            let url =
              Printf.sprintf
                "%s?period1=%Ld&period2=%Ld&interval=1d"
                base_url
                from_ms
                to_ms
            in
            let fetch =
              pace ()
              >>= fun () ->
              get ~headers (Uri.of_string url)
              >>= fun (resp, body) ->
              Cohttp_lwt.Body.to_string body
              >>= fun body_str ->
              let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
              if status <> 200
              then
                Lwt.fail
                  (Failure
                     (Printf.sprintf "Yahoo: HTTP %d for %s (%s)" status symbol body_str))
              else (
                let json = Yojson.Safe.from_string body_str in
                let bars = parse_daily ~symbol json in
                Lwt.return bars)
            in
            Lwt.catch
              (fun () ->
                 fetch
                 >|= fun bars ->
                 (* An empty 200 is the soft-block signature (Yahoo serves
                    "result": null instead of a 429). Count it; an all-empty-200
                    walk records the block. *)
                 bars, skipped, empty_200 + if bars = [] then 1 else 0)
              (fun exn ->
                 match classify_exn exn with
                 | `Missing_data ->
                   (* Window lies entirely before the symbol's listing: record
                      the confirmed empty prefix and skip it rather than fail
                      the whole walk. *)
                   remember_empty ~symbol (unix_to_iso to_ms);
                   Logging.debug_f
                     ~section
                     "Yahoo daily fetch for %s: no data before %s (pre-listing); \
                      skipping this window"
                     symbol
                     (unix_to_iso to_ms);
                   go
                     (Int64.add from_ms window_seconds)
                     acc
                     ~skipped:(skipped + 1)
                     ~empty_200
                 | `Fatal ->
                   Logging.warn_f
                     ~section
                     "Yahoo daily fetch for %s stopped at %s (%s), returning %d bars"
                     symbol
                     (unix_to_iso from_ms)
                     (Printexc.to_string exn)
                     (List.length acc);
                   Lwt.return (List.rev acc, skipped, empty_200))
            >>= fun (bars, skipped, empty_200) ->
            (* A successful window ends the empty prefix: a later fetch may
                 start at this window's beginning. *)
            if bars <> []
            then
              remember_empty ~symbol (Exchange.Types.add_days (unix_to_iso from_ms) (-1));
            let acc = List.rev_append bars acc in
            if Int64.compare to_ms end_epoch >= 0
            then Lwt.return (List.rev acc, skipped, empty_200)
            else go (Int64.add to_ms day_seconds) acc ~skipped ~empty_200))
      in
      (* One walk at a time (see [yahoo_mutex]): the pass fetches many symbols
         concurrently and Yahoo throttles parallel bursts. *)
      Lwt_mutex.with_lock yahoo_mutex (fun () ->
        go start_epoch [] ~skipped:0 ~empty_200:0)
      >|= fun (bars, skipped, empty_200) ->
      (* An all-empty-200 walk is the soft-block signature only when the
         requested range spans more than a few days: a weekend/holiday sliver
         at the deep-history boundary (equity venue_first - 1 often lands on a
         Sunday) legitimately has zero trading days and must not be classified
         as a block. A blocked IP comes back empty over the whole long range. *)
      let span_days = Int64.div (Int64.sub end_epoch start_epoch) day_seconds in
      if bars = [] && empty_200 > 0 && skipped = 0 && span_days > 7L
      then remember_block ~symbol ~windows:empty_200;
      if skipped > 0
      then
        Logging.info_f
          ~section
          "Yahoo daily fetch for %s: skipped %d pre-listing window(s) (no data before \
           %s); %d bar(s) fetched"
          symbol
          skipped
          (unix_to_iso start_epoch)
          (List.length bars);
      bars)
;;
