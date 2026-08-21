(* Oracle_cache - disk-persisted daily OHLC history for the capital oracle.

   Why: the pass pipeline re-fetched FULL histories on every refresh - the
   Kraken feed alone walks back to each pair's inception (up to 60 pages)
   per pass, Hyperliquid re-downloads the whole candleSnapshot range, Yahoo
   re-downloads the entire deep history. That is what made a pass take 20-30s
   and the per-asset ORACLE latency read 2-8s on the dashboard.

   Established engine practice (same as Dio_persistence.State_persistence):
   persist state to disk under data/ (/app/data in Docker), write atomically,
   treat corrupt files as a fresh start, never let persistence fail the
   caller. History bars are immutable except the latest one, so each pass
   fetches only the DELTA since the last cached bar (one small request per
   asset), merges (dedup keeps the newest occurrence of the current day's
   bar), re-normalizes through the shared [Oracle_calendar.normalize_bars]
   so every consumer - runtime, CLI, replay - still sees one clean series,
   and persists the merged result. A cache that is already current skips the
   network entirely; a failed delta fetch falls back to the cached history
   (stale but real beats nothing - the runtime's last-known-good machinery
   sits on top).

   File layout: <dir>/<exchange>/<symbol>.json - one JSON array of bars
   [{date,o,h,l,c,v}], ascending. The cache stores RAW bars (the source
   truth, sorted + de-duplicated); the shared clean-series normalization
   (Oracle_calendar.normalize_bars) is applied on READ, so a corrected
   normalization rule self-heals the served series without a refetch. v2:
   the v1 cache persisted already-normalized (gapped) series, so the layout
   was versioned to force one cold refetch. *)

open Lwt.Infix

let section = "oracle_cache"

(** Base directory for history files. Resolves to /app/data in Docker,
    ./data locally - same convention as State_persistence. v2: raw bars +
    read-time normalization (v1 stored normalized series and could not
    self-heal after a rule change). *)
let cache_dir =
  if Sys.file_exists "/app"
  then "/app/data/oracle_history/v2"
  else "data/oracle_history/v2"
;;

(* Mutex guarding all file I/O (shared with other modules' threads). *)
let file_mutex = Mutex.create ()

(** mkdir -p: create [dir] and any missing parents; an existing component
    is fine (idempotent, tolerates a racing writer). *)
let mkdir_p (dir : string) =
  let rec create path =
    if not (Sys.file_exists path)
    then (
      create (Filename.dirname path);
      try Unix.mkdir path 0o755 with
      | Unix.Unix_error (Unix.EEXIST, _, _) -> ())
  in
  create dir
;;

let ensure_dir ~(dir : string) =
  if not (Sys.file_exists dir)
  then (
    try mkdir_p dir with
    | Unix.Unix_error (errno, _, _) ->
      Logging.warn_f
        ~section
        "Could not create history dir %s: %s"
        dir
        (Unix.error_message errno))
;;

let sanitize (s : string) =
  String.map
    (fun c ->
       if c = '/' || c = '\\' || c = ':' || c = ' ' || c = '*' || c = '?' then '_' else c)
    s
;;

let path_of ~(dir : string) ~(exchange : string) ~(symbol : string) =
  Filename.concat (Filename.concat dir exchange) (sanitize symbol ^ ".json")
;;

(* ---- (de)serialization ---- *)

let bar_to_json (b : Oracle_types.bar) =
  `Assoc
    [ "date", `String b.date
    ; "o", `Float b.open_
    ; "h", `Float b.high
    ; "l", `Float b.low
    ; "c", `Float b.close
    ; "v", `Float b.volume
    ]
;;

let bar_of_json (j : Yojson.Safe.t) : Oracle_types.bar option =
  let open Yojson.Safe.Util in
  try
    let num key = member key j |> to_number in
    Some
      { Oracle_types.date = member "date" j |> to_string
      ; open_ = num "o"
      ; high = num "h"
      ; low = num "l"
      ; close = num "c"
      ; volume = num "v"
      }
  with
  | _ -> None
;;

let load_bars ~(dir : string) ~(exchange : string) ~(symbol : string)
  : Oracle_types.bar list
  =
  let path = path_of ~dir ~exchange ~symbol in
  if not (Sys.file_exists path)
  then []
  else (
    Mutex.lock file_mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock file_mutex)
      (fun () ->
         try
           match Yojson.Safe.from_file path with
           | `List rows -> List.filter_map bar_of_json rows
           | _ -> []
         with
         | Yojson.Json_error msg ->
           Logging.warn_f ~section "Corrupt history cache %s: %s (refetching)" path msg;
           []
         | Sys_error msg ->
           Logging.warn_f ~section "Cannot read history cache %s: %s" path msg;
           []))
;;

let save_bars
      ~(dir : string)
      ~(exchange : string)
      ~(symbol : string)
      (bars : Oracle_types.bar list)
  =
  if bars <> []
  then (
    let path = path_of ~dir ~exchange ~symbol in
    let tmp = path ^ ".tmp" in
    (* mkdir_p creates the whole chain, exchange dir included. *)
    ensure_dir ~dir:(Filename.concat dir exchange);
    Mutex.lock file_mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock file_mutex)
      (fun () ->
         try
           Yojson.Safe.to_file tmp (`List (List.map bar_to_json bars));
           Sys.rename tmp path
         with
         | Sys_error msg ->
           Logging.warn_f ~section "Could not write history cache %s: %s" path msg))
;;

(* ---- date helpers (exact civil-date math, no timezone dependence) ---- *)

let ms_of_iso (date : string) : int64 =
  let y, m, d = Oracle_calendar.iso_ymd date in
  Int64.mul (Int64.of_int (Oracle_calendar.days_from_civil y m d)) 86_400_000L
;;

let unix_of_iso (date : string) : int64 = Int64.div (ms_of_iso date) 1000L

(* ---- freshness / merge / delta policy ---- *)

(** A cached history is current when its last bar covers today or yesterday
    (the in-progress daily bar may lag a day; the grid start price prefers
    the live websocket bid anyway). *)
let is_fresh ~(today : string) (bars : Oracle_types.bar list) =
  match List.rev bars with
  | b :: _ -> String.compare b.date (Oracle_calendar.add_days today (-1)) >= 0
  | [] -> false
;;

(** A bounded history (e.g. the Yahoo deep extension, which only covers up
    to the day before the venue series starts) is COMPLETE when its last bar
    reaches its end date - after that it never needs a fetch again, however
    far today has moved on. [tolerance_days] absorbs non-trading days: the
    deep boundary for an equity asset usually lands on a weekend/holiday
    (venue_first - 1, e.g. a Sunday when the venue series starts Monday),
    and the venue's last bar is then the Friday before - a weekend-only
    sliver holds no data at all, so requiring an exact date match would
    re-request the same empty range on every pass forever. 7 days covers
    any weekend + holiday span. *)
let covers_through ?(tolerance_days = 0) ~(date : string) (bars : Oracle_types.bar list) =
  let floor = Oracle_calendar.add_days date (-tolerance_days) in
  match List.rev bars with
  | b :: _ -> String.compare b.date floor >= 0
  | [] -> false
;;

(** Merge cached history with freshly fetched bars, RAW (the cache is the
    source truth; normalization happens on read). [dedup] keeps the LAST
    occurrence of a date, so a revised current-day bar replaces the cached
    one. *)
let merge_bars (cached : Oracle_types.bar list) (fresh : Oracle_types.bar list) =
  cached @ fresh
  |> Array.of_list
  |> Oracle_calendar.sort_bars
  |> Oracle_calendar.dedup
  |> Array.to_list
;;

(** The shared clean-series view of a (raw) cached history: normalization
    applies at read time, so the served series always reflects the current
    rules without a refetch. *)
let clean_bars (bars : Oracle_types.bar list) : Oracle_types.bar list =
  let clean, _, _ = Oracle_calendar.normalize_bars bars in
  Array.to_list clean
;;

(** The delta-fetch policy, one asset at a time:
    - cache current (last bar >= today-1, or - for a bounded history given
      [complete_through] - the last bar already reaches that end date):
      return the clean view, no network;
    - else call [fetch] with [Some start_date] = the day AFTER the last
      cached bar (None = no cache yet, fetch the full history), merge raw,
      persist raw, return the clean view. A failed delta fetch logs and
      returns the cached history (stale but real); an empty cache that
      fails to fetch returns [] and the caller's existing failure handling
      applies. *)
let with_delta
      ?(dir = cache_dir)
      ?(complete_through : string option)
      ~(exchange : string)
      ~(symbol : string)
      ~(today : string)
      ~(fetch : string option -> Oracle_types.bar list Lwt.t)
      ()
  : Oracle_types.bar list Lwt.t
  =
  let cached = load_bars ~dir ~exchange ~symbol in
  let current =
    is_fresh ~today cached
    ||
    match complete_through with
    | Some end_date -> covers_through ~tolerance_days:7 ~date:end_date cached
    | None -> false
  in
  if current
  then Lwt.return (clean_bars cached)
  else (
    let boundary =
      match List.rev cached with
      | b :: _ -> Some (Oracle_calendar.add_days b.date 1)
      | [] -> None
    in
    Lwt.catch
      (fun () ->
         fetch boundary
         >|= fun fresh_bars ->
         let merged = merge_bars cached fresh_bars in
         save_bars ~dir ~exchange ~symbol merged;
         clean_bars merged)
      (fun exn ->
         Logging.warn_f
           ~section
           "%s/%s history delta fetch failed (%s); using cached history (%d bar(s) \
            through %s)"
           exchange
           symbol
           (Printexc.to_string exn)
           (List.length cached)
           (match List.rev cached with
            | b :: _ -> b.date
            | [] -> "-");
         Lwt.return cached))
;;
