(** US equity market session state.

    Reports whether the extended session (4:00 AM to 8:00 PM ET,
    Monday-Friday) is open; the supervisor gates connection and contract
    resolution attempts on this status so it does not spin against a
    closed gateway.

    Exchange holidays are not tracked; on a holiday the gateway rejects
    once and the supervisor defers until the next window. *)

let section = "ibkr_market_hours"

(** When true, narrows [is_market_open] to regular trading hours (9:30 AM to 4 PM)
    because IB Gateway in paper mode does not serve useful data during
    pre-market or after-hours. Set by [Ibkr_module.Config.set_testnet]. *)
let paper_mode = ref false

(** Current US Eastern UTC offset: -4 (EDT) from 2:00 AM on the second
    Sunday of March to 2:00 AM on the first Sunday of November, -5
    (EST) otherwise. *)
let us_eastern_offset_hours () =
  let t = Unix.gettimeofday () in
  let tm = Unix.gmtime t in
  let year = tm.Unix.tm_year + 1900 in
  (* Second Sunday in March: find day-of-week of March 1,
     then compute the date of the second Sunday. *)
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
  (* 0=Sun *)
  let first_sun = if march_1_wday = 0 then 1 else 8 - march_1_wday in
  let second_sun = first_sun + 7 in
  (* DST starts at 2:00 AM EST = 7:00 AM UTC on second Sunday of March *)
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
  (* First Sunday in November *)
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
  (* DST ends at 2:00 AM EDT = 6:00 AM UTC on first Sunday of November *)
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
  ignore year;
  if t >= dst_start && t < dst_end then -4 else -5
;;

(** Current (wday, hour, minute) in US Eastern time. *)
let current_eastern_time () =
  let t = Unix.gettimeofday () in
  let offset = us_eastern_offset_hours () in
  let eastern_t = t +. (float_of_int offset *. 3600.0) in
  let tm = Unix.gmtime eastern_t in
  tm.Unix.tm_wday, tm.Unix.tm_hour, tm.Unix.tm_min
;;

(** Extended session bounds: 4:00 AM to 8:00 PM ET. *)
let extended_open_hour = 4

let extended_open_min = 0
let extended_close_hour = 20
let extended_close_min = 0

(** Regular trading hours: [true] between 9:30 AM and 4:00 PM ET on
    weekdays. Drives the dashboard open/paused flag. *)
let is_regular_market_open () =
  let wday, hour, min = current_eastern_time () in
  let is_weekday = wday >= 1 && wday <= 5 in
  if not is_weekday
  then false
  else (
    let time_mins = (hour * 60) + min in
    let open_mins = (9 * 60) + 30 in
    let close_mins = 16 * 60 in
    time_mins >= open_mins && time_mins < close_mins)
;;

(** [true] within the 4:00 AM to 8:00 PM ET weekday window (extended
    session), or RTH only when [paper_mode] is set. *)
let is_market_open () =
  let wday, hour, min = current_eastern_time () in
  (* Monday=1 through Friday=5; Saturday=6, Sunday=0 *)
  let is_weekday = wday >= 1 && wday <= 5 in
  if not is_weekday
  then false
  else (
    let time_mins = (hour * 60) + min in
    if !paper_mode
    then (
      (* Paper mode: restrict to regular trading hours only.
         IB Gateway paper does not support pre-market/after-hours trading
         and may not accept connections outside RTH. *)
      let rth_open = (9 * 60) + 30 in
      let rth_close = 16 * 60 in
      time_mins >= rth_open && time_mins < rth_close)
    else (
      let open_mins = (extended_open_hour * 60) + extended_open_min in
      let close_mins = (extended_close_hour * 60) + extended_close_min in
      time_mins >= open_mins && time_mins < close_mins))
;;

(** Seconds until the next session open; 0.0 if the market is open now.
    Targets 9:30 AM ET in paper mode, 4:00 AM ET otherwise. Handles
    weekend rollover, but uses only the current UTC offset: a DST
    transition inside the interval shifts the result by an hour. *)
let seconds_until_next_open () =
  if is_market_open ()
  then 0.0
  else (
    let t = Unix.gettimeofday () in
    let offset = us_eastern_offset_hours () in
    let eastern_t = t +. (float_of_int offset *. 3600.0) in
    let tm = Unix.gmtime eastern_t in
    let wday = tm.Unix.tm_wday in
    let time_mins = (tm.Unix.tm_hour * 60) + tm.Unix.tm_min in
    let target_open_hour, target_open_min =
      if !paper_mode then 9, 30 else extended_open_hour, extended_open_min
    in
    let open_mins = (target_open_hour * 60) + target_open_min in
    (* How many days until the next weekday open? *)
    let days_ahead =
      if wday >= 1 && wday <= 5
      then
        (* Weekday: if before open today, 0 days; if after close, next day *)
        if time_mins < open_mins
        then 0
        else if wday = 5
        then 3 (* Friday after close → Monday *)
        else 1
      else if wday = 6
      then 2 (* Saturday → Monday *)
      else 1 (* Sunday → Monday *)
    in
    (* Compute the target open time in UTC *)
    let target_eastern_midnight =
      (* Truncate to midnight eastern *)
      let today_midnight =
        eastern_t
        -. float_of_int ((tm.Unix.tm_hour * 3600) + (tm.Unix.tm_min * 60) + tm.Unix.tm_sec)
      in
      today_midnight +. (float_of_int days_ahead *. 86400.0)
    in
    let target_open_eastern =
      target_eastern_midnight
      +. float_of_int ((target_open_hour * 3600) + (target_open_min * 60))
    in
    (* Convert back to UTC *)
    let target_open_utc = target_open_eastern -. (float_of_int offset *. 3600.0) in
    let delta = target_open_utc -. t in
    (* Safety: never return negative; minimum 1 second *)
    Float.max delta 1.0)
;;

(** Human-readable session status (pre-market, regular, after-hours, or
    closed) for logging and telemetry. *)
let market_status_string () =
  let wday, hour, min = current_eastern_time () in
  let is_weekday = wday >= 1 && wday <= 5 in
  if not is_weekday
  then "closed (weekend)"
  else (
    let time_mins = (hour * 60) + min in
    let open_mins = (extended_open_hour * 60) + extended_open_min in
    let close_mins = (extended_close_hour * 60) + extended_close_min in
    if time_mins < open_mins
    then
      Printf.sprintf
        "closed (pre-market opens at %d:%02d ET)"
        extended_open_hour
        extended_open_min
    else if time_mins >= close_mins
    then "closed (after hours ended)"
    else if hour < 9 || (hour = 9 && min < 30)
    then "open (pre-market)"
    else if hour >= 16
    then "open (after-hours)"
    else "open (regular hours)")
;;

(** Logs the session status and time to next open; called at startup and
    after reconnects. *)
let log_market_status () =
  let status = market_status_string () in
  let secs = seconds_until_next_open () in
  if secs > 0.0
  then (
    let hours = secs /. 3600.0 in
    Logging.info_f
      ~section
      "US equity market status: %s (next open in %.1f hours)"
      status
      hours)
  else Logging.info_f ~section "US equity market status: %s" status
;;
