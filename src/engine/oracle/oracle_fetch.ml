(* Oracle_fetch - shared venue-series pipeline for the capital oracle.

   One registry-driven implementation of "fetch a symbol's daily series",
   used by both the live runtime (oracle_runtime.ml) and the CLI
   (bin/oracle.ml). Dispatch goes exclusively through
   [Exchange_intf.Oracle.Registry]: each venue adapter
   ([Exchange_intf.Oracle.S]) supplies raw bars, calendar kind, session
   calendar and fee/balance/instrument endpoints. A new venue only implements
   the signature, registers and links it.

   The pipeline owns venue-independent concerns: the disk cache policy
   (Oracle_cache, raw bars + delta fetch), clean-series normalization
   ([Oracle_calendar.normalize_bars], applied on every read), Yahoo deep
   history (src/external/yahoo/) and the equity session-calendar model. *)

open Lwt.Infix

let section = "oracle_fetch"

module Exchange = Dio_exchange.Exchange_intf

(* The Yahoo deep-history client (src/external/yahoo/, [dio.yahoo]); the
   wrapper module [Yahoo] re-exports the [Yahoo_deep_history] module. *)
module Yahoo_deep_history = Yahoo.Yahoo_deep_history

let today_iso () =
  let tm = Unix.localtime (Unix.time ()) in
  Printf.sprintf
    "%04d-%02d-%02d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
;;

(* Symbols already reported as normalized this run: drop/clamp counts are
   logged once, then at debug level. *)
let warned_normalized : (string, unit) Hashtbl.t = Hashtbl.create 32

(** Clean-series view: sort, de-duplicate and normalize through
    [Oracle_calendar.normalize_bars], logging drop/clamp counts once per
    symbol. Idempotent, safe on an already-clean series. *)
let clean_bars ~(exchange : string) ~(symbol : string) (bars : Oracle_types.bar list)
  : Oracle_types.bar list
  =
  let clean, dropped, clamped = Oracle_calendar.normalize_bars bars in
  if dropped > 0 || clamped > 0
  then (
    let key = exchange ^ "/" ^ symbol in
    let first = not (Hashtbl.mem warned_normalized key) in
    if first then Hashtbl.add warned_normalized key ();
    if first
    then
      Logging.info_f
        ~section
        "oracle_fetch: normalized %s history: dropped %d placeholder/outlier candle(s), \
         clamped %d absurd extreme print(s) (fabricated rows never enter the \
         drawdown/floor math)"
        key
        dropped
        clamped
    else
      Logging.debug_f
        ~section
        "oracle_fetch: normalized %s history (already reported this run)"
        key);
  Array.to_list clean
;;

(** Build a [series] from (already clean) bars, using the venue's calendar
    kind from the registry (static known-exchange fallback for pure/offline/
    test contexts; unknown exchanges default to crypto). *)
let series_of_bars ~(exchange : string) ~(symbol : string) (bars : Oracle_types.bar list)
  : Oracle_types.series
  =
  { Oracle_types.symbol
  ; calendar_kind = Oracle_tasks.calendar_kind_of_exchange exchange
  ; bars = Array.of_list bars
  ; gaps = []
  }
;;

(** Per-pass cache of fetched series, shared across assets so e.g. ETH/USD is
    downloaded once per pass. The durable cache is Oracle_cache; this de-dupes
    within a pass. *)
let fetch_cache : (string * string, Oracle_types.series) Hashtbl.t = Hashtbl.create 32

(** Drop the per-pass series cache so a new refresh cycle re-fetches rather
    than reusing the previous cycle's series objects. *)
let clear_cache () = Hashtbl.clear fetch_cache

(** Build a deep-history [series] from clean Yahoo bars under the venue's
    calendar kind. The deep merge consumes only [bars], but class members
    analyzed purely from Yahoo need the right kind for labels/gap semantics. *)
let deep_series_of_bars
      ~(calendar_kind : Oracle_types.calendar_kind)
      ~(symbol : string)
      (bars : Oracle_types.bar list)
  : Oracle_types.series
  =
  { Oracle_types.symbol; calendar_kind; bars = Array.of_list bars; gaps = [] }
;;

(** Merge deep history into the venue series: deep bars strictly before the
    venue's first bar are prepended; venue bars win on overlap; result is
    sorted and de-duplicated. Returns the number of deep bars added. The
    venue's first bar is the minimum date over its bars (feeds are not
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

(** Fetch one symbol's daily series through the registry, cached per run and
    disk-cached via Oracle_cache (full history first, delta per refresh).
    [offline] serves only the disk cache, no network. [feed] is the
    Alpaca-only IEX/SIP knob. *)
let fetch_series_for
      ?(offline = false)
      ~(exchange : string)
      ~(symbol : string)
      ?feed
      ?end_date
      ()
  : Oracle_types.series Lwt.t
  =
  match Hashtbl.find_opt fetch_cache (exchange, symbol) with
  | Some series -> Lwt.return series
  | None ->
    let fetch =
      if offline
      then Lwt.return (Oracle_cache.read_cached ~exchange ~symbol ())
      else (
        match Exchange.Oracle.Registry.get exchange with
        | Some (module V) ->
          Oracle_cache.with_delta
            ~exchange
            ~symbol
            ~today:(today_iso ())
            ~fetch:(fun boundary ->
              V.fetch_bars ?feed ?end_date ~from:boundary ~symbol ())
            ()
        | None -> invalid_arg ("oracle_fetch: unknown exchange " ^ exchange))
    in
    fetch
    >|= fun bars ->
    let bars = clean_bars ~exchange ~symbol bars in
    let series = series_of_bars ~exchange ~symbol bars in
    Hashtbl.replace fetch_cache (exchange, symbol) series;
    series
;;

(** Extend a venue series backward with Yahoo deep history for the same
    underlying asset (venue bars win on overlap; nothing synthesized). Deep
    history is disk-cached and delta-fetched like the venue series, keyed on
    the resolved Yahoo symbol. Returns the deepened series and deep bars
    added. *)
let deepen_series
      ?(no_deep_history = false)
      ?(offline = false)
      (series : Oracle_types.series)
  : (Oracle_types.series * int) Lwt.t
  =
  let venue_bars = series.bars in
  if no_deep_history || offline || Array.length venue_bars = 0
  then Lwt.return (series, 0)
  else (
    match
      Yahoo_deep_history.symbol_of ~calendar_kind:series.calendar_kind series.symbol
    with
    | None -> Lwt.return (series, 0)
    | Some yahoo_symbol ->
      let venue_first = venue_bars.(0).Oracle_types.date in
      let end_date = Oracle_calendar.add_days venue_first (-1) in
      (* Deep history is bounded by [end_date] (day before the venue series
         starts) and complete once its last bar reaches it; a "today"
         freshness check would re-fetch it with start > end every pass. *)
      Oracle_cache.with_delta
        ~exchange:"yahoo-deep"
        ~symbol:yahoo_symbol
        ~today:(today_iso ())
        ~complete_through:end_date
        ~fetch:(fun boundary ->
          let start_date = Option.value boundary ~default:"2015-01-01" in
          Yahoo_deep_history.fetch_daily ~start_date ~symbol:yahoo_symbol ~end_date ())
        ()
      >|= fun deep_bars ->
      let deep =
        deep_series_of_bars
          ~calendar_kind:series.calendar_kind
          ~symbol:yahoo_symbol
          deep_bars
      in
      merge_series ~venue:series ~deep)
;;
