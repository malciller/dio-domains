(* Survival sessions - expected-market-session model (Phase 2).

   The equity calendar is not a set of raw bars: a weekday with no bar is a gap
   (market holiday or missing data). This module provides the expected-session
   abstraction:

   - [business_weekday]: US weekdays (Mon-Fri), the base model matching the
     regular-session notion of Alpaca_market_hours.
   - [with_holidays]: subtracts an explicit holiday set.
   - [explicit]: an exact session-date set (e.g. built from Alpaca /v2/calendar,
     the live holiday source; fixtures in tests).
   - [missing_sessions] / [gaps_of_missing]: expected sessions skipped by a bar
     series, grouped into gap runs for Survival_calendar.detect_gaps.

   Pure math: no network, no Yojson. The Alpaca calendar adapter lives in
   Survival_fetch_alpaca and composes this module. *)

open Survival_types

(** Expected-session model. [is_session] answers "is ISO date [d] a trading
    session for this calendar?" (holidays already excluded). *)
type model =
  { name : string
  ; holidays : string list
  ; is_session : string -> bool
  }

(** Base US-weekday model (Mon-Fri). *)
let business_weekday : model =
  let is_session d =
    let w = Survival_calendar.iso_wday d in
    w >= 1 && w <= 5
  in
  { name = "us_weekdays"; holidays = []; is_session }
;;

(** Exact session-date set (order-independent); e.g. from an Alpaca /v2/calendar
    fixture or fetch. *)
let explicit_model ?(name = "explicit_sessions") dates =
  let set = Hashtbl.create (List.length dates * 2) in
  List.iter (fun d -> Hashtbl.replace set d ()) dates;
  { name; holidays = []; is_session = (fun d -> Hashtbl.mem set d) }
;;

(** The live Alpaca calendar lists only open days; holidays are US weekdays not
    present in that list over the covered range. [calendar_dates] must span the
    bars being analyzed. *)
let alpaca_model calendar_dates =
  let set = Hashtbl.create (List.length calendar_dates * 2) in
  List.iter (fun d -> Hashtbl.replace set d ()) calendar_dates;
  let is_session d =
    let w = Survival_calendar.iso_wday d in
    w >= 1 && w <= 5 && Hashtbl.mem set d
  in
  { name = "alpaca_calendar"; holidays = []; is_session }
;;

(** Subtract holidays from a weekday-based model. *)
let with_holidays holidays (m : model) =
  let set = Hashtbl.create (List.length holidays * 2) in
  List.iter (fun d -> Hashtbl.replace set d ()) holidays;
  { name = m.name ^ "_minus_holidays"
  ; holidays = List.sort_uniq String.compare holidays
  ; is_session = (fun d -> m.is_session d && not (Hashtbl.mem set d))
  }
;;

let is_session (m : model) d = m.is_session d

(** Ascending expected session dates between two ISO dates. *)
let expected_sessions (m : model) ~(from_date : string) ~(to_date : string) =
  Survival_calendar.dates_between ~from_date ~to_date |> List.filter m.is_session
;;

(** Expected sessions skipped by a bar series. *)
let missing_sessions (m : model) ~(bars : bar array) =
  Survival_calendar.missing_sessions ~is_session:m.is_session bars
;;

(** Gap runs for a bar series under this model (bounding sessions resolved
    against the present bar dates). *)
let gaps_of_missing (m : model) ~(bars : bar array) =
  Survival_calendar.gaps_of_missing ~bars (missing_sessions m ~bars)
;;

(** Full detect_gaps for a series under this model. *)
let detect_gaps (m : model) (bars : bar array) =
  Survival_calendar.detect_gaps ~calendar_kind:Equity ~is_session:m.is_session bars
;;
