(* Oracle calendar - session-consistent views over raw bars.

   - Sorts bars by ISO date, de-duplicates.
   - Detects missing sessions (gaps): for Crypto a session is a calendar day;
     for Equity an expected-session predicate (US weekdays minus holidays, from
     Oracle_sessions) drives gap detection.
   - The user's review rule: never forward-fill missing bars; gaps are surfaced
     as metadata and the analysis fails when max_gap > tolerance (checked by
     the caller / CLI). *)

open Oracle_types

(* ---- ISO date helpers (YYYY-MM-DD) ----
   All arithmetic is pure civil-date math (days from the 1970-01-01 epoch, via
   Howard Hinnant's algorithms). The previous mktime/gmtime implementation was
   local-timezone dependent: in any positive-UTC-offset timezone the reconstructed
   weekday and day+1 were off by one day, which corrupted equity gap detection
   and could non-terminate the gap-bounding walks in [gaps_of_missing]. *)

let iso_ymd s =
  if String.length s < 10 then invalid_arg ("Oracle_calendar.iso_ymd: bad date " ^ s);
  let y = int_of_string (String.sub s 0 4) in
  let m = int_of_string (String.sub s 5 2) in
  let d = int_of_string (String.sub s 8 2) in
  y, m, d
;;

(** Days since 1970-01-01 of a civil (y, m, d) date. Exact integer arithmetic;
    independent of timezone, DST and Unix.mktime. Valid for the proleptic
    Gregorian calendar (all dates in use here are >= 1970). *)
let days_from_civil y m d =
  let y = if m <= 2 then y - 1 else y in
  let era = (if y >= 0 then y else y - 399) / 400 in
  let yoe = y - (era * 400) in
  let doy = (((153 * if m > 2 then m - 3 else m + 9) + 2) / 5) + d - 1 in
  let doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy in
  (era * 146097) + doe - 719468
;;

(** Inverse of [days_from_civil]: the civil (y, m, d) date of a day count. *)
let civil_from_days z =
  let z = z + 719468 in
  let era = (if z >= 0 then z else z - 146096) / 146097 in
  let doe = z - (era * 146097) in
  let yoe = (doe - (doe / 1460) + (doe / 36524) - (doe / 146096)) / 365 in
  let y = yoe + (era * 400) in
  let doy = doe - ((365 * yoe) + (yoe / 4) - (yoe / 100)) in
  let mp = ((5 * doy) + 2) / 153 in
  let d = doy - (((153 * mp) + 2) / 5) + 1 in
  let m = if mp < 10 then mp + 3 else mp - 9 in
  (y + if m <= 2 then 1 else 0), m, d
;;

(** Day of week of an ISO date: 0 = Sunday .. 6 = Saturday. *)
let iso_wday s =
  let y, m, d = iso_ymd s in
  (* 1970-01-01 was a Thursday (4). *)
  (days_from_civil y m d + 4) mod 7
;;

(** Number of calendar days between two ISO dates (b - a). *)
let n_days_between a b =
  let ya, ma, da = iso_ymd a in
  let yb, mb, db = iso_ymd b in
  days_from_civil yb mb db - days_from_civil ya ma da
;;

(** ISO date [n] calendar days after [d]. *)
let add_days d n =
  let y, m, d = iso_ymd d in
  let y, m, d = civil_from_days (days_from_civil y m d + n) in
  Printf.sprintf "%04d-%02d-%02d" y m d
;;

(** Inclusive ascending list of ISO dates from [from_date] to [to_date]. *)
let dates_between ~(from_date : string) ~(to_date : string) =
  let n = n_days_between from_date to_date in
  if n < 0 then [] else List.init (n + 1) (fun i -> add_days from_date i)
;;

(* ---- Sorting / dedup ---- *)

let sort_bars (bars : bar array) =
  let arr = Array.copy bars in
  Array.sort (fun a b -> String.compare a.date b.date) arr;
  arr
;;

let dedup bars =
  let n = Array.length bars in
  if n = 0
  then bars
  else (
    let out = ref [ bars.(0) ] in
    let prev = ref bars.(0).date in
    Array.iteri
      (fun i b ->
         if i > 0 && b.date <> !prev
         then (
           out := b :: !out;
           prev := b.date))
      bars;
    Array.of_list (List.rev !out))
;;

(* ---- Series normalization (one clean series for every consumer) ----
   Venue feeds can return rows that are not real market prints; both corrupt
   the peak-to-valley drawdown and the ATH/floor references:
   - placeholder candles (e.g. Hyperliquid's fabricated pre-listing rows for
     wrapped spot pairs - constant dummy OHLC like 6,969,696 / 7,979,573
     with zero or dust volume - which read as a phantom 99.3% drawdown);
   - rows whose extreme prints never traded (e.g. open/high 240,000 on a day
     whose close was 97,578 - they fabricate an ATH/floor).
   [normalize_bars] drops the first and folds the second into the row's
   close. It is applied at every fetch source AND on every history-cache
   read, so the runtime, the CLI and the replay always share one clean
   series and never contradict each other. The judge is deliberately LOCAL
   (each row vs its nearest real-trading neighbor), never a global median:
   a series that genuinely 100x'd (BTC ~$1k in 2017 vs ~$40k now) must keep
   its early cheap-era rows - only rows that deviate ~100x from the market
   AROUND THEM (fabricated placeholder levels) are dropped. *)

(** Normalize a candle list into the canonical clean series: ascending,
    de-duplicated, fabricated rows dropped, absurd intra-row extremes folded
    into the row's close. Returns the clean bars plus the counts of dropped
    and clamped rows (for the once-per-symbol log).
    - Pass 1 drops rows with non-finite/non-positive fields or an impossible
      single-candle range (>10x between the row's extreme prints).
    - Pass 2 folds rows whose extreme prints sit >2x away from the row's own
      close into a flat close (a daily candle never trades a >2x span for
      the oracle's assets; the close is the day's real level and is kept).
    - Pass 3 drops rows whose close deviates >8x from the nearest REAL
      trading neighbor (left first, else right; real = volume >= 0.01). A
      fabricated placeholder level (zero/dust volume, ~100x off the market)
      fails this; a genuine cheap-era row sits next to its own era's rows
      and passes; a zero-volume carried price near the market also passes.
      Rows with no real-trading neighbor at all are kept (cannot judge).
    - A series with no real trading at all (not one surviving row with
      volume >= 0.01) is entirely fabricated and normalizes to empty. *)
let normalize_bars (bars : bar list) : bar array * int * int =
  let arr = bars |> Array.of_list |> sort_bars |> dedup in
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
  (* Pass 3: local, volume-aware outlier guard (see the module doc). A
     fabricated placeholder level (zero/dust volume, ~100x off the market
     around it) is dropped; a genuine cheap-era row sits next to its own
     era's rows and survives; a zero-volume carried price near the market
     survives. *)
  let is_real (b : bar) = b.volume >= 0.01 in
  for i = 0 to n - 1 do
    if good.(i)
    then (
      let ref_price =
        let left = ref None in
        let j = ref (i - 1) in
        while !j >= 0 && !left = None do
          if good.(!j) && is_real arr.(!j) then left := Some arr.(!j).close;
          decr j
        done;
        match !left with
        | Some c -> Some c
        | None ->
          let right = ref None in
          let j = ref (i + 1) in
          while !j < n && !right = None do
            if good.(!j) && is_real arr.(!j) then right := Some arr.(!j).close;
            incr j
          done;
          !right
      in
      match ref_price with
      | Some c when c > 0.0 ->
        let hi = Float.max arr.(i).close c in
        let lo = Float.min arr.(i).close c in
        if hi /. lo > 8.0
        then (
          good.(i) <- false;
          incr dropped)
      | _ -> ())
  done;
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

(** Expected sessions between the first and last bar date for a session
    predicate (e.g. US weekdays minus holidays). Ascending. *)
let expected_sessions ~(is_session : string -> bool) (bars : bar array) =
  let n = Array.length bars in
  if n = 0
  then []
  else (
    let from_date = bars.(0).date in
    let to_date = bars.(n - 1).date in
    dates_between ~from_date ~to_date |> List.filter is_session)
;;

(** Expected sessions skipped by a bar series, per the session predicate. *)
let missing_sessions ~(is_session : string -> bool) (bars : bar array) =
  let present = Hashtbl.create 64 in
  Array.iter (fun b -> Hashtbl.replace present b.date ()) bars;
  expected_sessions ~is_session bars |> List.filter (fun d -> not (Hashtbl.mem present d))
;;

(** Group skipped sessions into gap runs: consecutive calendar-day runs become
    one gap whose [after]/[before] are the bounding present sessions and
    [missing_days] is the run length. *)
let gaps_of_missing ~(bars : bar array) (missing : string list) =
  let present = Hashtbl.create 64 in
  Array.iter (fun b -> Hashtbl.replace present b.date ()) bars;
  let rec runs acc cur = function
    | [] -> List.rev (List.rev cur :: acc)
    | d :: rest ->
      (match cur with
       | prev :: _ when n_days_between prev d = 1 -> runs acc (d :: cur) rest
       | _ -> runs (List.rev cur :: acc) [ d ] rest)
  in
  let runs = runs [] [] missing |> List.filter (fun r -> r <> []) in
  List.map
    (fun run ->
       (* Walk backward from the run head to the last present session before
          it; walk forward from the run tail to the first present session
          after it. *)
       let after =
         let rec back d =
           let prev = add_days d (-1) in
           if Hashtbl.mem present prev then Some prev else back prev
         in
         back (List.hd run)
       in
       let before =
         let rec fwd d =
           let next = add_days d 1 in
           if Hashtbl.mem present next then Some next else fwd next
         in
         fwd (List.rev run |> List.hd)
       in
       { after = Option.value ~default:"-" after
       ; before = Option.value ~default:"-" before
       ; missing_days = List.length run
       })
    runs
;;

(** Detects missing-session runs. For Crypto: gaps are days with no bar. For
    Equity: gaps are expected sessions (per the [is_session] predicate, e.g.
    US weekdays minus holidays) with no bar; without a predicate no gaps are
    reported. *)
let detect_gaps
      ~(calendar_kind : calendar_kind)
      ?(is_session : (string -> bool) option)
      (bars : bar array)
  =
  match calendar_kind with
  | Equity ->
    (match is_session with
     | None -> []
     | Some f ->
       let bars = sort_bars bars |> dedup in
       gaps_of_missing ~bars (missing_sessions ~is_session:f bars))
  | Crypto ->
    let acc = ref [] in
    let n = Array.length bars in
    for i = 1 to n - 1 do
      let diff = n_days_between bars.(i - 1).date bars.(i).date in
      if diff > 1
      then
        acc
        := { after = bars.(i - 1).date; before = bars.(i).date; missing_days = diff - 1 }
           :: !acc
    done;
    List.rev !acc
;;

let max_gap (gaps : gap list) = List.fold_left (fun m g -> max m g.missing_days) 0 gaps
