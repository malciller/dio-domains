(** US equity market-hours evaluation for the Alpaca connection manager and
    order router.

    Sessions (US/Eastern): 24/5 window Sunday 20:00 to Friday 20:00
    (extended + overnight + regular, continuous); regular session 09:30-16:00.
    Outside the 24/5 window (weekend) Alpaca runs no session - orders rest
    unfilled and data feeds stop - so the market is closed regardless of
    account mode. Handles after-hours order flags and market-data management. *)

let section = "alpaca_market_hours"

(** Clock seam: when set (tests), overrides the wall clock for every time-based
    evaluation here, making the session schedule deterministic. Production must
    leave it unset. *)
let now_override : float option ref = ref None

let now () =
  match !now_override with
  | Some t -> t
  | None -> Unix.gettimeofday ()
;;

(** Uncached US Eastern UTC offset, DST-adjusted. Cost: five [gmtime], three
    [mktime], three [tm] allocations; [mktime] alone ~100us, so do not call per
    session evaluation. *)
let compute_eastern_offset () =
  let t = now () in
  let tm = Unix.gmtime t in
  let march_1 =
    fst
      (Unix.mktime
         { tm with
           Unix.tm_mon = 2
         ; tm_mday = 1
         ; tm_hour = 7
         ; tm_min = 0
         ; tm_sec = 0
         ; tm_wday = 0
         ; tm_yday = 0
         ; tm_isdst = false
         })
  in
  let march_1_tm = Unix.gmtime march_1 in
  let march_1_wday = march_1_tm.Unix.tm_wday in
  (* tm_wday values start at 0 for Sunday. *)
  let first_sun = if march_1_wday = 0 then 1 else 8 - march_1_wday in
  let second_sun = first_sun + 7 in
  let dst_start =
    fst
      (Unix.mktime
         { tm with
           Unix.tm_mon = 2
         ; tm_mday = second_sun
         ; tm_hour = 7
         ; tm_min = 0
         ; tm_sec = 0
         ; tm_wday = 0
         ; tm_yday = 0
         ; tm_isdst = false
         })
  in
  let nov_1 =
    fst
      (Unix.mktime
         { tm with
           Unix.tm_mon = 10
         ; tm_mday = 1
         ; tm_hour = 6
         ; tm_min = 0
         ; tm_sec = 0
         ; tm_wday = 0
         ; tm_yday = 0
         ; tm_isdst = false
         })
  in
  let nov_1_tm = Unix.gmtime nov_1 in
  let nov_1_wday = nov_1_tm.Unix.tm_wday in
  let first_sun_nov = if nov_1_wday = 0 then 1 else 8 - nov_1_wday in
  let dst_end =
    fst
      (Unix.mktime
         { tm with
           Unix.tm_mon = 10
         ; tm_mday = first_sun_nov
         ; tm_hour = 6
         ; tm_min = 0
         ; tm_sec = 0
         ; tm_wday = 0
         ; tm_yday = 0
         ; tm_isdst = false
         })
  in
  if t >= dst_start && t < dst_end then -4 else -5
;;

(** Cached Eastern UTC offset. Constant between the two yearly DST transitions;
    callers (market-hours gate, WS per-message RTH check) otherwise run the
    uncached [mktime] math per evaluation, producing 300-560us PREP spikes.
    TTL 1h. Staleness is possible only near a transition; both US transitions
    occur Sunday 02:00 ET inside the weekend closure, when no session is
    evaluated. [now_override] bypasses the cache for deterministic tests. *)
let eastern_offset_cache : (float * int) Atomic.t = Atomic.make (0.0, -5)

let us_eastern_offset_hours () =
  match !now_override with
  | Some _ -> compute_eastern_offset ()
  | None ->
    let t = Unix.gettimeofday () in
    let last_t, last_v = Atomic.get eastern_offset_cache in
    if t >= last_t && t -. last_t < 3600.0
    then last_v
    else (
      let v = compute_eastern_offset () in
      Atomic.set eastern_offset_cache (t, v);
      v)
;;

(** Current (day-of-week, hour, minute) in US Eastern Time; day-of-week 0 = Sunday. *)
let current_eastern_time () =
  let t = now () in
  let offset = us_eastern_offset_hours () in
  let eastern_t = t +. (float_of_int offset *. 3600.0) in
  let tm = Unix.gmtime eastern_t in
  tm.Unix.tm_wday, tm.Unix.tm_hour, tm.Unix.tm_min
;;

let extended_open_hour = 4
let extended_open_min = 0
let extended_close_hour = 20
let extended_close_min = 0

(** Regular Trading Hours (09:30-16:00 ET), cached with 1s TTL. Full evaluation
    costs ~10-100us (gmtime/mktime); the WS feed handler calls it per trade
    message. *)
let regular_open_cache : (float * bool) Atomic.t = Atomic.make (0.0, false)

let is_regular_market_open () =
  let now = Unix.gettimeofday () in
  let last_t, last_v = Atomic.get regular_open_cache in
  if now -. last_t < 1.0
  then last_v
  else (
    let wday, hour, min = current_eastern_time () in
    let is_weekday = wday >= 1 && wday <= 5 in
    let v =
      if not is_weekday
      then false
      else (
        let time_mins = (hour * 60) + min in
        let open_mins = (9 * 60) + 30 in
        let close_mins = 16 * 60 in
        time_mins >= open_mins && time_mins < close_mins)
    in
    Atomic.set regular_open_cache (now, v);
    v)
;;

(** Overnight trading hours: 20:00-04:00 ET. *)
let is_overnight_hours () =
  let wday, hour, _min = current_eastern_time () in
  match wday with
  | 0 -> hour >= 20
  | 1 | 2 | 3 | 4 -> hour < 4 || hour >= 20
  | 5 -> hour < 4
  | _ -> false
;;

(** Alpaca trading schedule: 24/5, Sunday 20:00 ET to Friday 20:00 ET. Live and
    paper share the calendar. Weekend: no simulated session - orders rest
    unfilled and feeds stop - so closed Friday 20:00 ET to Sunday 20:00 ET. *)
let is_market_open () =
  let wday, hour, _min = current_eastern_time () in
  match wday with
  | 0 -> hour >= 20
  | 1 | 2 | 3 | 4 -> true
  | 5 -> hour < 20
  | _ -> false
;;

(** Extended session: pre-market 04:00-09:30, after-hours 16:00-20:00, or overnight 20:00-04:00 ET. *)
let is_extended_hours () = is_market_open () && not (is_regular_market_open ())

(** Seconds until the next 24/5 market open (Sunday 20:00 ET); 0.0 while open. *)
let seconds_until_next_open () =
  if is_market_open ()
  then 0.0
  else (
    let t = now () in
    let offset = us_eastern_offset_hours () in
    let eastern_t = t +. (float_of_int offset *. 3600.0) in
    let tm = Unix.gmtime eastern_t in
    let wday = tm.Unix.tm_wday in
    let days_ahead =
      match wday with
      | 5 -> 2 (* Friday evening -> Sunday *)
      | 6 -> 1 (* Saturday -> Sunday *)
      | 0 -> 0 (* Sunday morning/afternoon -> Sunday 8 PM *)
      | _ -> 0
    in
    let today_midnight =
      eastern_t
      -. float_of_int ((tm.Unix.tm_hour * 3600) + (tm.Unix.tm_min * 60) + tm.Unix.tm_sec)
    in
    let target_eastern_midnight =
      today_midnight +. (float_of_int days_ahead *. 86400.0)
    in
    let target_open_eastern = target_eastern_midnight +. (20.0 *. 3600.0) in
    let target_open_utc = target_open_eastern -. (float_of_int offset *. 3600.0) in
    let delta = target_open_utc -. t in
    Float.max delta 1.0)
;;

(** Human-readable current Alpaca session status. *)
let market_status_string () =
  let _wday, hour, min = current_eastern_time () in
  if not (is_market_open ())
  then "closed (weekend, overnight opens Sun 8:00 PM ET)"
  else if is_overnight_hours ()
  then "open (overnight 24/5)"
  else if hour < 9 || (hour = 9 && min < 30)
  then "open (pre-market)"
  else if hour >= 16 && hour < 20
  then "open (after-hours)"
  else "open (regular hours)"
;;

let log_market_status () =
  let status = market_status_string () in
  let secs = seconds_until_next_open () in
  if secs > 0.0
  then (
    let hours = secs /. 3600.0 in
    Logging.info_f
      ~section
      "Alpaca market status: %s (next open in %.1f hours)"
      status
      hours)
  else Logging.info_f ~section "Alpaca market status: %s" status
;;
