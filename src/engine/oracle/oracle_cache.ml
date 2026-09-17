(* Oracle_cache - disk-persisted daily OHLC history.

   Full-history refetch per pass was too slow (Kraken walks to pair inception, up to 60
   pages; Hyperliquid re-downloads the candleSnapshot range; Yahoo re-downloads deep
   history). Bars are immutable except the latest, so each pass fetches only the delta
   since the last cached bar, merges (dedup keeps the newest occurrence of a date),
   re-normalizes through [Oracle_calendar.normalize_bars] and persists. A current cache
   skips the network; a failed delta fetch falls back to the cached history (stale but
   real). Persistence never fails the caller; corrupt files are treated as a fresh start;
   writes are atomic.

   File layout: <dir>/<exchange>/<symbol>.json - ascending JSON array of raw bars
   [{date,o,h,l,c,v}]. Normalization is applied on read, so a corrected rule self-heals
   without a refetch. v2 stores raw bars (v1 stored normalized series and could not
   self-heal). *)

open Lwt.Infix

let section = "oracle_cache"

(** Base history directory: /app/data in Docker, ./data locally. v2 stores raw bars with
    read-time normalization. *)
let cache_dir =
  if Sys.file_exists "/app"
  then "/app/data/oracle_history/v2"
  else "data/oracle_history/v2"
;;

(* Mutex guarding all file I/O (shared with other modules' threads). *)
let file_mutex = Mutex.create ()

(** mkdir -p: create [dir] and any missing parents; an existing component is fine
    (idempotent, tolerates a racing writer). *)
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

(** A cached history is current when its last bar covers today or yesterday (the
    in-progress daily bar may lag a day; the grid start price prefers the live websocket
    bid anyway). *)
let is_fresh ~(today : string) (bars : Oracle_types.bar list) =
  match List.rev bars with
  | b :: _ -> String.compare b.date (Oracle_calendar.add_days today (-1)) >= 0
  | [] -> false
;;

(** A bounded history (e.g. the Yahoo deep extension, covering up to the day before the
    venue series starts) is complete when its last bar reaches [date] - afterwards it
    never needs another fetch. [tolerance_days] absorbs non-trading days so a
    weekend/holiday boundary is not re-requested forever; 7 days covers any weekend plus
    holiday span. *)
let covers_through ?(tolerance_days = 0) ~(date : string) (bars : Oracle_types.bar list) =
  let floor = Oracle_calendar.add_days date (-tolerance_days) in
  match List.rev bars with
  | b :: _ -> String.compare b.date floor >= 0
  | [] -> false
;;

(** Merge cached and fresh bars raw (the cache is source truth; normalization is on read).
    [dedup] keeps the last occurrence of a date, so a revised current-day bar replaces the
    cached one. *)
let merge_bars (cached : Oracle_types.bar list) (fresh : Oracle_types.bar list) =
  cached @ fresh
  |> Array.of_list
  |> Oracle_calendar.sort_bars
  |> Oracle_calendar.dedup
  |> Array.to_list
;;

(** Clean-series view of raw cached history: normalization applies at read time, so the
    served series reflects current rules without a refetch. *)
let clean_bars (bars : Oracle_types.bar list) : Oracle_types.bar list =
  let clean, _, _ = Oracle_calendar.normalize_bars bars in
  Array.to_list clean
;;

(** Read-only cache access for offline/cache-only runs: cleaned on-disk bars for this
    asset, no network fallback. A cache miss returns []. *)
let read_cached ?(dir = cache_dir) ~(exchange : string) ~(symbol : string) ()
  : Oracle_types.bar list
  =
  load_bars ~dir ~exchange ~symbol |> clean_bars
;;

(** Delta-fetch policy for one asset. If the cache is current (last bar >= today-1, or a
    bounded history reaching [complete_through]), return the clean view with no network.
    Otherwise call [fetch] with [Some start_date] = day after the last cached bar (None =
    full history), merge raw, persist raw, return the clean view. A failed delta fetch
    logs and returns the cached history (stale but real); an empty failing cache returns
    []. *)
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
