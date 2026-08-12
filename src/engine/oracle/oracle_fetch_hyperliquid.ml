(* Oracle_fetch_hyperliquid - historical daily OHLC for crypto via the
   Hyperliquid public Info API (no auth). Endpoint /info with a POST body
   {"type":"candleSnapshot","req":{coin, interval:"1d", startTime, endTime}},
   paginated forward in day-windows. Pure [parse_*] functions are
   fixture-testable without network.

   Source normalization. The candleSnapshot response is normalized here, at
   the source, because it can contain rows that are not real market prints
   and both corrupt the peak-to-valley drawdown and the ATH/floor references:
   - placeholder candles for wrapped spot pairs before real trading started
     (constant fabricated OHLC - e.g. 6,969,696 / 7,979,573 for UBTC/USDC on
     2025-02-03..2025-02-13 - with zero or dust volume). A fabricated
     $7.98M close read as a 99.3% drawdown; the market was never there.
   - rows whose extreme prints never traded (e.g. open/high 240,000 on
     2025-02-14 whose close was 97,578) - they fabricate an ATH/floor.
   [normalize_bars] drops the first and folds the second into the row's
   close, so the runtime, the CLI and the replay all consume one clean
   series and never contradict each other.

   Spot vs perpetual resolution. The candleSnapshot endpoint serves:
   - perpetual candles under the bare coin name (e.g. "BTC");
   - spot candles under the pair's spotMeta universe "name" field: the
     canonical "PURR/USDC" for the one canonical pair, or the "@N" alias
     (N = universe index) for every wrapped pair (e.g. "@142" for BTC spot =
     UBTC/USDC). Both forms are used verbatim as the request coin; "@0"
     (PURR's index) is rejected, which is why the named form is required
     there.
   Config symbols replicate the mapping of Hyperliquid_instruments_feed: a
   bare coin name denotes a perpetual and is used as-is; a "BASE/QUOTE"
   symbol resolves through the feed's spot key - base token canonicalized
   (UBTC -> BTC, UETH -> ETH, USOL -> SOL; everything else keeps its
   spotMeta name, e.g. LINK0/USDC) with the quote normalized ("USD" ->
   "USDC") - to the mapped spot asset and its candle coin ("PURR/USDC" or
   "@N"). A "/" symbol with no matching Hyperliquid spot pair has no spot
   history and is left empty (the asset is then INACTIVE) instead of
   silently substituting perpetual candles. *)

open Lwt.Infix

let section = "oracle_hyperliquid"
let endpoint = "https://api.hyperliquid.xyz/info"
let interval_daily = "1d"
let window_days = 5000
let max_windows = 60
let ms_per_day = 86_400_000L
let default_start_ms = 1640995200000L (* 2022-01-01 *)

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

let unix_ms_to_iso (t : int64) =
  let tm = Unix.gmtime (Int64.to_float t /. 1000.0) in
  Printf.sprintf
    "%04d-%02d-%02d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
;;

(** Canonicalize the wrapped spot base tokens the same way
    [Hyperliquid_instruments_feed] does when it builds its spot keys. *)
let canon_base = function
  | "UBTC" -> "BTC"
  | "UETH" -> "ETH"
  | "USOL" -> "SOL"
  | other -> other
;;

(** Cached mapping of feed-style spot symbols ("BASE/QUOTE", e.g. "BTC/USDC")
    to the candleSnapshot coin (the spotMeta universe "name": "PURR/USDC" or
    the "@N" alias, e.g. "@142") from the last spotMeta fetch. *)
let spot_meta_pairs : (string, string) Hashtbl.t = Hashtbl.create 512

let spot_meta_fetched_at : float ref = ref 0.0
let spot_meta_mutex = Mutex.create ()
let spot_meta_ttl = 6.0 *. 3600.0

(* Symbols already reported as having no Hyperliquid spot pair this run.
   These are expected for class members that never had a spot listing
   (e.g. DOGE/USD), so every oracle pass would otherwise re-log the same
   warning on each refresh. Warn once, then debug. *)
let warned_no_spot_history : (string, unit) Hashtbl.t = Hashtbl.create 32

(* Symbols whose history was normalized (placeholder/outlier candles removed
   or clamped) this run: report the counts once per pass, then debug. *)
let warned_normalized : (string, unit) Hashtbl.t = Hashtbl.create 32

(** Pure: extract (feed_symbol, candle_coin) mappings from a spotMeta
    response. [feed_symbol] replicates the instruments-feed spot key: the base
    token name canonicalized for the wrapped majors (UBTC/UETH/USOL) plus the
    quote name (e.g. "BTC/USDC", "LINK0/USDC"). [candle_coin] is the universe
    entry's "name" field, which candleSnapshot accepts directly for spot: the
    canonical "PURR/USDC" or the "@N" alias (e.g. "@142") for every wrapped
    pair. *)
let spot_meta_pairs_of_json (json : Yojson.Safe.t) : (string * string) list =
  let open Yojson.Safe.Util in
  let token_name idx =
    match member "tokens" json with
    | `List toks ->
      List.find_map
        (fun t ->
           try
             if member "index" t |> to_int = idx
             then Some (member "name" t |> to_string)
             else None
           with
           | _ -> None)
        toks
    | _ -> None
  in
  match member "universe" json with
  | `List entries ->
    entries
    |> List.filter_map (fun entry ->
      try
        let coin = member "name" entry |> to_string in
        match member "tokens" entry |> to_list with
        | b :: q :: _ ->
          (match token_name (to_int b), token_name (to_int q) with
           | Some base_name, Some quote_name ->
             Some (canon_base base_name ^ "/" ^ quote_name, coin)
           | _ -> None)
        | _ -> None
      with
      | _ -> None)
  | _ -> []
;;

(** Refresh the spot-pair mapping cache from /info {type:spotMeta}.
    TTL-guarded; a failed fetch logs and keeps the previous mapping (empty on
    first failure). *)
let refresh_spot_meta () : unit Lwt.t =
  let now = Unix.gettimeofday () in
  if now -. !spot_meta_fetched_at < spot_meta_ttl
  then Lwt.return_unit
  else
    Lwt.catch
      (fun () ->
         let payload = `Assoc [ "type", `String "spotMeta" ] |> Yojson.Safe.to_string in
         let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
         Oracle_http.post
           ~headers
           ~body:(Cohttp_lwt.Body.of_string payload)
           (Uri.of_string endpoint)
         >>= fun (resp, body) ->
         Cohttp_lwt.Body.to_string body
         >>= fun body_str ->
         let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
         if status <> 200
         then
           Lwt.fail
             (Failure (Printf.sprintf "Oracle_fetch_hyperliquid: spotMeta HTTP %d" status))
         else (
           let json = Yojson.Safe.from_string body_str in
           let pairs = spot_meta_pairs_of_json json in
           Mutex.lock spot_meta_mutex;
           Hashtbl.clear spot_meta_pairs;
           List.iter (fun (key, coin) -> Hashtbl.replace spot_meta_pairs key coin) pairs;
           spot_meta_fetched_at := now;
           Mutex.unlock spot_meta_mutex;
           Logging.info_f
             ~section
             "Hyperliquid spotMeta: %d spot pair(s) mapped to candle coins"
             (List.length pairs);
           Lwt.return_unit))
      (fun exn ->
         Logging.warn_f
           ~section
           "Hyperliquid spotMeta fetch failed (%s)"
           (Printexc.to_string exn);
         Lwt.return_unit)
;;

(** Pure: resolve a config symbol to the candleSnapshot coin, replicating the
    instruments-feed mapping.
    - Bare coin (no "/"): a perpetual -> the coin itself (perp candles).
    - "BASE/QUOTE": a spot pair. The quote is normalized ("USD" -> "USDC",
      Hyperliquid spot is USDC-quoted) and the pair is looked up by its
      feed-style key (canonicalized base). A match yields the spot candle coin
      - the canonical "PURR/USDC" or the "@N" alias (e.g. "@142") - so the
      asset's spot history is used; no match means the symbol is not a
      Hyperliquid spot pair -> [None], and the caller returns no bars instead
      of substituting perpetual data.
    Config convention: a perp is named by its bare coin, a spot pair by its
    "BASE/QUOTE" symbol. *)
let coin_of_symbol ~(pairs : (string * string) list) (symbol : string) : string option =
  let sym = String.trim symbol in
  if sym = ""
  then None
  else (
    let upper = String.uppercase_ascii sym in
    if not (String.contains sym '/')
    then Some upper
    else (
      let key =
        match String.split_on_char '/' upper with
        | [ base; quote ] -> base ^ "/" ^ if quote = "USD" then "USDC" else quote
        | _ -> upper
      in
      match List.assoc_opt key pairs with
      | Some candle_coin -> Some candle_coin
      | None -> None))
;;

(** One candle row {"t": time_ms, "o": open, "h": high, "l": low, "c": close,
    "v": volume, "n": trades}. *)
let parse_candle (j : Yojson.Safe.t) : Oracle_types.bar option =
  let open Yojson.Safe.Util in
  try
    let num key =
      match number_of_json (j |> member key) with
      | Some f -> f
      | None -> raise Exit
    in
    Some
      { Oracle_types.date = unix_ms_to_iso (Int64.of_float (num "t"))
      ; open_ = num "o"
      ; high = num "h"
      ; low = num "l"
      ; close = num "c"
      ; volume = num "v"
      }
  with
  | _ -> None
;;

(** Parse the candleSnapshot response body (a JSON array of candle rows).
    Ascending, de-duplicated by date. *)
let parse_candles ~(symbol : string) (json : Yojson.Safe.t) : Oracle_types.bar list =
  match json with
  | `List rows ->
    rows
    |> List.filter_map parse_candle
    |> Array.of_list
    |> Oracle_calendar.sort_bars
    |> Oracle_calendar.dedup
    |> Array.to_list
  | _ ->
    failwith
      (Printf.sprintf
         "Oracle_fetch_hyperliquid.parse_candles: %s expected array, got %s"
         symbol
         (Yojson.Safe.to_string json))
;;

let series_of_bars ~(symbol : string) (bars : Oracle_types.bar list) : Oracle_types.series
  =
  { Oracle_types.symbol
  ; calendar_kind = Oracle_types.Crypto
  ; bars = Array.of_list bars
  ; gaps = []
  }
;;

(* ---- Source normalization (see the module doc) ---- *)

let median_of (xs : float array) =
  let n = Array.length xs in
  if n = 0
  then None
  else (
    let arr = Array.copy xs in
    Array.sort Float.compare arr;
    Some (if n mod 2 = 1 then arr.(n / 2) else (arr.((n / 2) - 1) +. arr.(n / 2)) /. 2.0))
;;

(** Normalize a candle list into the canonical clean series: ascending,
    de-duplicated, fabricated rows dropped, absurd intra-row extremes folded
    into the row's close. Returns the clean bars plus the counts of dropped
    and clamped rows (for the once-per-symbol log).
    - Pass 1 drops rows with non-finite/non-positive fields or an impossible
      single-candle range (>10x between the row's extreme prints).
    - Pass 2 folds rows whose extreme prints sit >2x away from the row's own
      close into a flat close (liquid assets never trade a >2x daily span;
      the close is the day's real level and is kept).
    - Pass 3 drops rows whose close deviates >8x from the series' median
      close (fabricated placeholder levels sit ~100x off the real market).
      The median is computed over rows with real trading volume (>= 0.01)
      so a long dead/placeholder run cannot drag the reference itself into
      the gutter; no such rows falls back to all survivors, and no reference
      at all keeps everything. *)
let normalize_bars (bars : Oracle_types.bar list) : Oracle_types.bar array * int * int =
  let arr = bars |> Array.of_list |> Oracle_calendar.sort_bars |> Oracle_calendar.dedup in
  let n = Array.length arr in
  let dropped = ref 0 in
  let clamped = ref 0 in
  let good = Array.make n false in
  for i = 0 to n - 1 do
    let b = arr.(i) in
    let lo = Float.min b.open_ (Float.min b.high (Float.min b.low b.close)) in
    let hi = Float.max b.open_ (Float.max b.high (Float.max b.low b.close)) in
    let sane =
      Float.is_finite b.open_
      && Float.is_finite b.high
      && Float.is_finite b.low
      && Float.is_finite b.close
      && b.open_ > 0.0
      && b.high > 0.0
      && b.low > 0.0
      && b.close > 0.0
      && hi /. lo <= 10.0
    in
    good.(i) <- sane;
    if not sane then incr dropped
  done;
  for i = 0 to n - 1 do
    if good.(i)
    then (
      let b = arr.(i) in
      let lo = Float.min b.open_ (Float.min b.high (Float.min b.low b.close)) in
      let hi = Float.max b.open_ (Float.max b.high (Float.max b.low b.close)) in
      if hi > 2.0 *. b.close || lo < b.close /. 2.0
      then (
        arr.(i) <- { b with open_ = b.close; high = b.close; low = b.close };
        incr clamped))
  done;
  let closes_of (predicate : int -> Oracle_types.bar -> bool) =
    Array.to_list arr
    |> List.mapi (fun i (b : Oracle_types.bar) -> i, b)
    |> List.filter (fun (i, b) -> predicate i b)
    |> List.map (fun (_, (b : Oracle_types.bar)) -> b.close)
    |> Array.of_list
  in
  let ref_closes =
    let with_volume = closes_of (fun i b -> good.(i) && b.volume >= 0.01) in
    if Array.length with_volume > 0 then with_volume else closes_of (fun i _ -> good.(i))
  in
  (match median_of ref_closes with
   | None -> ()
   | Some m when m > 0.0 ->
     for i = 0 to n - 1 do
       if good.(i)
       then (
         let c = arr.(i).close in
         if Float.max c m /. Float.min c m > 8.0
         then (
           good.(i) <- false;
           incr dropped))
     done
   | Some _ -> ());
  (* A series with no real trading at all (not one surviving row with volume
     >= 0.01) is entirely fabricated: empty it rather than feed placeholder
     candles into the drawdown/floor math. *)
  let any_real = ref false in
  for i = 0 to n - 1 do
    if good.(i) && arr.(i).volume >= 0.01 then any_real := true
  done;
  if not !any_real
  then
    for i = 0 to n - 1 do
      if good.(i)
      then (
        good.(i) <- false;
        incr dropped)
    done;
  let out = ref [] in
  for i = n - 1 downto 0 do
    if good.(i) then out := arr.(i) :: !out
  done;
  Array.of_list !out, !dropped, !clamped
;;

(** Order the fetched candle windows into ascending time (oldest -> newest)
    and normalize the whole series ([normalize_bars] sorts and de-duplicates,
    so window boundary disorder and response-level order reversals are both
    absorbed). The LAST bar must be the CURRENT close: the grid start price
    and all ladder capital math read it, so an unordered series prices every
    ladder from a stale close (regression: the pre-fix accumulation returned
    newest-first). *)
let windows_to_series (windows : Oracle_types.bar list list) : Oracle_types.bar list =
  let clean, _, _ = normalize_bars (List.concat windows) in
  Array.to_list clean
;;

(** Fetch daily candles forward from [start_ms] (unix ms), in day-windows.
    Spot symbols (containing "/") resolve through the feed-style mapping to
    the mapped spot asset's candle coin ("PURR/USDC" or "@N"); symbols
    without a matching spot pair resolve to no bars (never perpetual data
    for a spot-named symbol). Bare coin names denote perpetuals. *)
let fetch_candles ?(start_ms = default_start_ms) ~(symbol : string) ()
  : Oracle_types.bar list Lwt.t
  =
  refresh_spot_meta ()
  >>= fun () ->
  Mutex.lock spot_meta_mutex;
  let pairs = Hashtbl.fold (fun key coin acc -> (key, coin) :: acc) spot_meta_pairs [] in
  Mutex.unlock spot_meta_mutex;
  match coin_of_symbol ~pairs symbol with
  | None ->
    let first = not (Hashtbl.mem warned_no_spot_history symbol) in
    if first then Hashtbl.add warned_no_spot_history symbol ();
    if first
    then
      Logging.warn_f
        ~section
        "Hyperliquid: no spot history for %s (no matching Hyperliquid spot pair; \
         spot-named symbols never use perpetual candles). Spot-only oracle leaves this \
         asset without history -> INACTIVE. Use the bare coin name only for perpetual \
         intent."
        symbol
    else
      Logging.debug_f
        ~section
        "Hyperliquid: no spot history for %s (already reported this run)"
        symbol;
    Lwt.return []
  | Some coin ->
    let now_ms = Int64.of_float (Unix.gettimeofday () *. 1000.0) in
    let span = Int64.mul ms_per_day (Int64.of_int window_days) in
    let rec go from_ms acc windows =
      if windows = 0
      then Lwt.return acc
      else (
        let to_ms = Int64.min (Int64.add from_ms span) now_ms in
        let payload =
          `Assoc
            [ "type", `String "candleSnapshot"
            ; ( "req"
              , `Assoc
                  [ "coin", `String coin
                  ; "interval", `String interval_daily
                  ; "startTime", `Intlit (Int64.to_string from_ms)
                  ; "endTime", `Intlit (Int64.to_string to_ms)
                  ] )
            ]
          |> Yojson.Safe.to_string
        in
        let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
        let fetch =
          Oracle_http.post
            ~headers
            ~body:(Cohttp_lwt.Body.of_string payload)
            (Uri.of_string endpoint)
          >>= fun (resp, body) ->
          Cohttp_lwt.Body.to_string body
          >>= fun body_str ->
          let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
          if status <> 200
          then
            Lwt.fail
              (Failure
                 (Printf.sprintf
                    "Oracle_fetch_hyperliquid: HTTP %d for %s: %s"
                    status
                    symbol
                    body_str))
          else (
            let json = Yojson.Safe.from_string body_str in
            let bars = parse_candles ~symbol json in
            if to_ms >= now_ms
            then Lwt.return (bars :: acc)
            else go to_ms (bars :: acc) (windows - 1))
        in
        Lwt.catch
          (fun () -> fetch)
          (fun exn ->
             Logging.warn_f
               ~section
               "Hyperliquid candle page failed (%s), returning %d bars so far"
               (Printexc.to_string exn)
               (List.length acc);
             Lwt.return acc))
    in
    go start_ms [] max_windows
    >|= fun windows ->
    let clean, dropped, clamped = normalize_bars (List.concat windows) in
    if dropped > 0 || clamped > 0
    then (
      let first = not (Hashtbl.mem warned_normalized symbol) in
      if first then Hashtbl.add warned_normalized symbol ();
      if first
      then
        Logging.info_f
          ~section
          "Hyperliquid: normalized %s history at source: dropped %d placeholder/outlier \
           candle(s), clamped %d absurd extreme print(s) (fabricated rows never enter \
           the drawdown/floor math)"
          symbol
          dropped
          clamped
      else
        Logging.debug_f
          ~section
          "Hyperliquid: normalized %s history (already reported this run)"
          symbol);
    Array.to_list clean
;;
