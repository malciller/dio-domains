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

let section = "oracle_yahoo"
let endpoint = "https://query1.finance.yahoo.com/v8/finance/chart/%s"
let window_seconds = 1_100_000_000L (* ~35 months: ~1050 daily points per request *)
let day_seconds = 86_400L

(* Yahoo throttles sustained bursts (and the crumbless chart API degrades to
   empty 200s when hammered - the "unexpected ... (no bars)" symptom). The
   oracle pass now analyzes assets concurrently, so the deep/class fetches
   would fire many at once. Two guards: [yahoo_mutex] serializes the walks
   (one at a time), and [pace] keeps a global minimum gap between individual
   requests (~2/s - the pre-cache sequential engine stayed below Yahoo's
   tolerance and never hit this). *)
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

(* Yahoo soft-blocks hammered IPs by serving empty 200s ("result": null) for
   a while instead of a 429. Without a memory of it, a blocked walk returns
   [] every pass and the oracle re-attempts the whole history each refresh -
   wasted work, and it is what keeps the block alive. On the all-empty
   signature the symbol is remembered for [soft_block_backoff] seconds and
   its requests are skipped entirely during that window. *)
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

(* ------------------------------------------------------------------ *)
(* Pre-listing window handling: Yahoo answers a request whose range sits
   entirely before the symbol's listing with HTTP 400 and
   "Data doesn't exist for startDate = ...". Assets that listed recently
   (e.g. a stock that IPO'd this year) would otherwise re-request the same
   doomed range on every deep-history fetch - the SPCX spam in the engine
   log. The walk skips those windows instead of failing, and the confirmed
   empty prefix is cached per symbol so later fetches clamp their start date
   past it (zero requests for the empty range). *)

(** Classify a failed window request: a Yahoo "data doesn't exist" answer is
    an empty range (skip it), anything else is a real failure (stop). *)
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

(** Classify a fetch failure exception: the [Failure] message carries the
    "HTTP <status> for <symbol> (<body>)" envelope from the fetch; dig out
    the status and the response body to tell a pre-listing empty range from
    a real failure. *)
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
    date for which Yahoo has answered "no data in [requested start, end]".
    Fetches clamp their start date past it, so a pre-listing range is never
    re-requested (process-lifetime; the engine's oracle re-fetches deep
    history every pass). *)
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

(** Yahoo symbol for an asset, or None when the symbol is not trusted for
    deep history (see the module header: dead-token collisions). Equity
    assets map by identity (Yahoo QQQ is QQQ); crypto symbols only through
    the whitelist of known-continuous pairs. *)
let symbol_of ~(calendar_kind : Oracle_types.calendar_kind) (symbol : string)
  : string option
  =
  match calendar_kind with
  | Equity -> Some (String.uppercase_ascii symbol)
  | Crypto ->
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
    points). Windows that Yahoo reports as pre-listing ("Data doesn't exist")
    are SKIPPED rather than aborting the walk - an asset that listed recently
    contributes the empty prefix only once, and the confirmed-empty prefix is
    cached per symbol so the next fetch clamps its start past it (no
    re-requesting dates that do not exist). Returns what was fetched; a real
    (non-empty-range) failure logs a warning and stops the walk with what it
    has. *)
let fetch_daily ?(start_date = "2016-01-01") ~(symbol : string) ~(end_date : string) ()
  : Oracle_types.bar list Lwt.t
  =
  let start_epoch = epoch_of_iso start_date in
  let end_epoch = epoch_of_iso end_date in
  (* Clamp the start past the confirmed-empty prefix: no data exists before
     it, so requesting it again would only reproduce the same 400. *)
  let start_epoch =
    match known_empty_before ~symbol with
    | Some floor when epoch_of_iso floor >= start_epoch ->
      let clamp = Int64.add (epoch_of_iso floor) day_seconds in
      if Int64.compare clamp start_epoch > 0 then clamp else start_epoch
    | _ -> start_epoch
  in
  if Int64.compare start_epoch end_epoch > 0
  then (
    (* The whole requested range is known empty: nothing to ask for. *)
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
    (* Soft-block memory (see [remember_block]): while the symbol is backed
       off, do not even attempt its requests - the pass must not keep the
       block alive, and an asset whose deep history is blocked keeps the
       (empty) cached state instead of re-paying a doomed walk every pass. *)
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
              Oracle_http.get ~headers (Uri.of_string url)
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
                Lwt.return bars)
            in
            Lwt.catch
              (fun () ->
                 fetch
                 >|= fun bars ->
                 (* An empty 200 is the soft-block signature (Yahoo serves
                    "result": null for blocked IPs instead of a 429). Count
                    it; a walk that is ALL empty-200 (no pre-listing skips)
                    records the block. *)
                 bars, skipped, empty_200 + if bars = [] then 1 else 0)
              (fun exn ->
                 match classify_exn exn with
                 | `Missing_data ->
                   (* The window sits entirely before the symbol's listing: no
                      data exists in [from_ms, to_ms]. Record the confirmed
                      empty prefix and skip the window instead of failing the
                      whole walk (a recently-listed asset would otherwise spam
                      the same doomed request on every pass). *)
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
            (* A successful window ends the empty prefix: from here on the
                 symbol has data, so a later fetch can start at this window's
                 beginning (the walk re-checks nothing before it). *)
            if bars <> []
            then
              remember_empty ~symbol (Oracle_calendar.add_days (unix_to_iso from_ms) (-1));
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
      (* An all-empty-200 walk is the soft-block signature - BUT only when
         the requested range spans more than a few days: a weekend/holiday
         sliver at the deep-history boundary legitimately holds zero trading
         days (equity venue_first - 1 often lands on a Sunday) and must not
         be classified as a block. A blocked IP comes back empty over the
         whole multi-month/year range. *)
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
