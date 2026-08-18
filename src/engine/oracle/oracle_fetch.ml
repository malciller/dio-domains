(* Oracle_fetch - the shared venue-series pipeline for the capital oracle.

   ONE registry-driven implementation of "fetch a symbol's daily series"
   used by BOTH the live runtime (oracle_runtime.ml) and the CLI
   (bin/oracle.ml) - replacing the old per-venue hardcoded dispatch and the
   duplicated fetch_series_for bodies.

   Dispatch is exclusively through [Exchange_intf.Oracle.Registry]: each
   venue's adapter (implementing [Exchange_intf.Oracle.S]) supplies raw
   historical bars, its calendar kind, its session calendar and its fee /
   balance / instrument endpoints. A new venue is plug-and-play: implement
   the signature, register it, link it - no dispatch edits here.

   The pipeline owns everything venue-independent:
   - the disk cache policy (Oracle_cache, raw bars + delta fetch);
   - the shared clean-series normalization (Oracle_calendar.normalize_bars,
     applied on every read so cache and direct fetches agree and a corrected
     rule self-heals without a refetch);
   - Yahoo deep history (the [Yahoo] client in src/external/yahoo/) for the
     same underlying asset;
   - the equity session-calendar model. *)

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

(* Symbols already reported as normalized this run: report the drop/clamp
   counts once per pass, then debug. *)
let warned_normalized : (string, unit) Hashtbl.t = Hashtbl.create 32

(** The shared clean-series view: sort, de-duplicate and source-normalize
    through [Oracle_calendar.normalize_bars], logging dropped/clamped counts
    once per symbol. Idempotent, so it is safe to apply on top of an
    already-clean series (e.g. the cache's read view). *)
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

(** Per-pass cache of fetched series, shared across assets and class members
    so e.g. ETH/USD is only downloaded once per pass. The durable cache lives
    in Oracle_cache (disk-persisted, delta-fetched); this one just de-dupes
    within one pass. *)
let fetch_cache : (string * string, Oracle_types.series) Hashtbl.t = Hashtbl.create 32

(** Drop the per-pass series cache (called by the live runtime between
    refresh cycles so a new pass re-fetches rather than reusing last
    cycle's series objects). *)
let clear_cache () = Hashtbl.clear fetch_cache

(** Build a deep-history [series] from (already clean) Yahoo bars, using the
    venue's calendar kind. The deep merge itself only consumes [bars] (the
    result keeps the venue series' kind), but class members analyzed purely
    from Yahoo need the right kind for their own labels/gap semantics. *)
let deep_series_of_bars
      ~(calendar_kind : Oracle_types.calendar_kind)
      ~(symbol : string)
      (bars : Oracle_types.bar list)
  : Oracle_types.series
  =
  { Oracle_types.symbol; calendar_kind; bars = Array.of_list bars; gaps = [] }
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

(** Fetch one symbol's daily series through the registry (cached per run,
    and disk-cached via Oracle_cache: full history on first use, one small
    delta request per refresh after that). [offline] bypasses the disk cache
    and requests exactly the [start_date]..[end_date] window (CLI use);
    [feed] is the Alpaca-only IEX/SIP knob. *)
let fetch_series_for
      ?(offline = false)
      ~(exchange : string)
      ~(symbol : string)
      ?feed
      ?start_date
      ?end_date
      ()
  : Oracle_types.series Lwt.t
  =
  match Hashtbl.find_opt fetch_cache (exchange, symbol) with
  | Some series -> Lwt.return series
  | None ->
    let fetch =
      match Exchange.Oracle.Registry.get exchange with
      | Some (module V) ->
        if offline
        then V.fetch_bars ?feed ?end_date ~from:start_date ~symbol ()
        else
          Oracle_cache.with_delta
            ~exchange
            ~symbol
            ~today:(today_iso ())
            ~fetch:(fun boundary ->
              V.fetch_bars ?feed ?end_date ~from:boundary ~symbol ())
            ()
      | None -> invalid_arg ("oracle_fetch: unknown exchange " ^ exchange)
    in
    fetch
    >|= fun bars ->
    let bars = clean_bars ~exchange ~symbol bars in
    let series = series_of_bars ~exchange ~symbol bars in
    Hashtbl.replace fetch_cache (exchange, symbol) series;
    series
;;

(** Extend a venue series backward with the Yahoo deep history for the same
    underlying asset (venue bars win on overlap; nothing is synthesized).
    The deep history is disk-cached and delta-fetched like the venue series
    (keyed on the resolved Yahoo symbol): once downloaded, a pass only
    fetches the days the deep history does not cover yet. Returns the
    deepened series and the number of deep bars added. *)
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
      (* The deep history is BOUNDED by [end_date] (the day before the venue
         series starts): it is complete once its last bar reaches it - a
         freshness check against "today" would re-fetch it (with start >
         end) on every pass. *)
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

(** Pure: the source of one class member's price history. The class surface
    is a blend input, never a decision subject - members gather their
    history PURELY from Yahoo (whitelisted symbols only, see
    [Yahoo_deep_history.symbol_of]) UNLESS the member IS the active asset on
    this exchange, which uses the exchange's own history (it is the decision
    subject and needs venue data). A symbol with no trusted Yahoo mapping
    ([`None]) contributes nothing to the class. *)
let class_member_source
      ~(exchange : string)
      ~(asset_symbol : string)
      (member_symbol : string)
  =
  if String.lowercase_ascii member_symbol = String.lowercase_ascii asset_symbol
  then `Exchange
  else (
    match
      Yahoo_deep_history.symbol_of
        ~calendar_kind:(Oracle_tasks.calendar_kind_of_exchange exchange)
        member_symbol
    with
    | Some yahoo_symbol -> `Yahoo yahoo_symbol
    | None -> `None)
;;

(** Load the class member pool for [class_name]: the member that IS the
    active asset uses the exchange (fetched and deepened like the asset),
    every other member is fetched purely from Yahoo (whitelisted) - see
    [class_member_source]. [explicit_members] overrides the configured pool
    (CLI --members). Falls back to the asset alone when no pool is known. *)
let load_members
      ?(explicit_members : string list option)
      ~(no_deep_history : bool)
      ~(offline : bool)
      ~(exchange : string)
      (pool_members : string list)
      ~(class_name : string)
      (asset : Oracle_types.series)
      ~(fetch_series : string -> Oracle_types.series Lwt.t)
  : Oracle_types.series list Lwt.t
  =
  let syms =
    match explicit_members with
    | Some ms when ms <> [] -> ms
    | _ -> pool_members
  in
  if syms = []
  then (
    Logging.debug_f
      ~section
      "no class members known for '%s'; analyzing %s alone"
      class_name
      asset.symbol;
    Lwt.return [ asset ])
  else (
    let rec go = function
      | [] -> Lwt.return []
      | symbol :: rest ->
        (match
           class_member_source ~exchange ~asset_symbol:asset.Oracle_types.symbol symbol
         with
         | `None -> go rest
         | `Exchange ->
           (* The active asset itself: the exchange's history (deepened like
              the asset - it is the decision subject). *)
           fetch_series symbol
           >>= fun series ->
           go rest
           >>= fun acc ->
           if Array.length series.bars = 0
           then Lwt.return acc
           else
             deepen_series ~no_deep_history ~offline series
             >>= fun (series, _) -> Lwt.return (series :: acc)
         | `Yahoo yahoo_symbol ->
           (* Any other member: purely Yahoo, disk-cached, delta through
              today - the exchange never sees it. *)
           let calendar_kind = Oracle_tasks.calendar_kind_of_exchange exchange in
           Oracle_cache.with_delta
             ~exchange:"yahoo-class"
             ~symbol:yahoo_symbol
             ~today:(today_iso ())
             ~fetch:(fun boundary ->
               let start_date = Option.value boundary ~default:"2015-01-01" in
               Yahoo_deep_history.fetch_daily
                 ~start_date
                 ~symbol:yahoo_symbol
                 ~end_date:(today_iso ())
                 ())
             ()
           >>= fun bars ->
           go rest
           >>= fun acc ->
           if bars = []
           then Lwt.return acc
           else
             Lwt.return
               (deep_series_of_bars ~calendar_kind ~symbol:yahoo_symbol bars :: acc))
    in
    go syms
    >>= fun members -> if members = [] then Lwt.return [ asset ] else Lwt.return members)
;;

(** Resolve the equity session-calendar model for an exchange: equity venues
    (per the registry's [calendar_kind]) fetch their expected session dates
    and build the model; crypto venues (or unregistered exchanges) get
    [None]. Fetch failures fall back to business weekdays. *)
let fetch_calendar_model
      ~(exchange : string)
      ~(start_date : string)
      ~(end_date : string)
      ()
  : (Oracle_sessions.model * string) option Lwt.t
  =
  match Exchange.Oracle.Registry.get exchange with
  | Some (module V) when V.calendar_kind = Exchange.Types.Equity ->
    Lwt.catch
      (fun () ->
         V.fetch_calendar ~start_date ~end_date
         >|= fun dates ->
         let fp =
           match dates with
           | [] -> "empty"
           | d0 :: _ -> d0 ^ ".." ^ List.nth dates (List.length dates - 1)
         in
         let model =
           if dates = []
           then Oracle_sessions.business_weekday
           else Oracle_sessions.alpaca_model dates
         in
         Some (model, fp))
      (fun exn ->
         Logging.warn_f
           ~section
           "calendar fetch failed for %s (%s); using business weekdays"
           exchange
           (Printexc.to_string exn);
         Lwt.return (Some (Oracle_sessions.business_weekday, "weekday")))
  | _ -> Lwt.return None
;;
