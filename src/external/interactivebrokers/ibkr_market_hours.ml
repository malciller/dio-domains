(** Provides the core US equity market hours evaluation logic for the Interactive Brokers connection manager.

    This module evaluates whether US equity markets are currently operating within the
    extended trading session, spanning from pre-market open through after-hours close. This state
    evaluation is critical for the supervisor component, which utilizes this status to suspend
    connection and contract resolution attempts during market closures, thereby preventing
    unnecessary overhead and gateway rejection cycles.

    The defined extended trading window is from 4:00 AM to 8:00 PM Eastern Time, Monday through Friday.
    Note that specific exchange holidays are not independently tracked. In the event of a holiday closure,
    the gateway will issue a single rejection, prompting the supervisor to automatically defer operations
    until the subsequent valid trading window. *)

let section = "ibkr_market_hours"

(** Computes the current UTC offset for US Eastern Time, dynamically adjusting for Daylight Saving Time.
    The Daylight Saving Time configuration (EDT, UTC-4) applies from 2:00 AM on the second Sunday in March
    until 2:00 AM on the first Sunday in November. For all other periods, the standard time configuration 
    (EST, UTC-5) is returned. *)
let us_eastern_offset_hours () =
  let t = Unix.gettimeofday () in
  let tm = Unix.gmtime t in
  let year = tm.Unix.tm_year + 1900 in
  (* Second Sunday in March: find day-of-week of March 1,
     then compute the date of the second Sunday. *)
  let march_1 = fst (Unix.mktime { tm with
    Unix.tm_mon = 2; tm_mday = 1; tm_hour = 7; tm_min = 0; tm_sec = 0;
    tm_wday = 0; tm_yday = 0; tm_isdst = false }) in
  let march_1_tm = Unix.gmtime march_1 in
  let march_1_wday = march_1_tm.Unix.tm_wday in  (* 0=Sun *)
  let first_sun = if march_1_wday = 0 then 1 else 8 - march_1_wday in
  let second_sun = first_sun + 7 in
  (* DST starts at 2:00 AM EST = 7:00 AM UTC on second Sunday of March *)
  let dst_start = fst (Unix.mktime { tm with
    Unix.tm_mon = 2; tm_mday = second_sun; tm_hour = 7; tm_min = 0; tm_sec = 0;
    tm_wday = 0; tm_yday = 0; tm_isdst = false }) in
  (* First Sunday in November *)
  let nov_1 = fst (Unix.mktime { tm with
    Unix.tm_mon = 10; tm_mday = 1; tm_hour = 6; tm_min = 0; tm_sec = 0;
    tm_wday = 0; tm_yday = 0; tm_isdst = false }) in
  let nov_1_tm = Unix.gmtime nov_1 in
  let nov_1_wday = nov_1_tm.Unix.tm_wday in
  let first_sun_nov = if nov_1_wday = 0 then 1 else 8 - nov_1_wday in
  (* DST ends at 2:00 AM EDT = 6:00 AM UTC on first Sunday of November *)
  let dst_end = fst (Unix.mktime { tm with
    Unix.tm_mon = 10; tm_mday = first_sun_nov; tm_hour = 6; tm_min = 0; tm_sec = 0;
    tm_wday = 0; tm_yday = 0; tm_isdst = false }) in
  ignore year;
  if t >= dst_start && t < dst_end then -4 else -5

(** Calculates the current day of the week, hour, and minute localized to US Eastern Time based on the current UTC timestamp and daylight saving adjustments. *)
let current_eastern_time () =
  let t = Unix.gettimeofday () in
  let offset = us_eastern_offset_hours () in
  let eastern_t = t +. (float_of_int offset *. 3600.0) in
  let tm = Unix.gmtime eastern_t in
  (tm.Unix.tm_wday, tm.Unix.tm_hour, tm.Unix.tm_min)

(** Defines the operational hour and minute boundaries for the extended US equity trading session, spanning from 4:00 AM to 8:00 PM Eastern Time. *)
let extended_open_hour = 4
let extended_open_min = 0
let extended_close_hour = 20
let extended_close_min = 0

(** Evaluates whether the current system time falls strictly within the US equity Regular Trading Hours (RTH) session.
    Returns [true] between 9:30 AM and 4:00 PM Eastern Time on weekdays.
    This precise window directs the dashboard UI to flag trading logic as active (▶) or paused (⏸). *)
let is_regular_market_open () =
  let (wday, hour, min) = current_eastern_time () in
  let is_weekday = wday >= 1 && wday <= 5 in
  if not is_weekday then false
  else
    let time_mins = hour * 60 + min in
    let open_mins = 9 * 60 + 30 in
    let close_mins = 16 * 60 in
    time_mins >= open_mins && time_mins < close_mins

(** Evaluates the current system time against the predefined US equity extended trading schedule.
    Returns [true] if the current time resides within the 4:00 AM to 8:00 PM Eastern Time window on a weekday,
    indicating that the Interactive Brokers gateway is expected to accept connection and routing requests. *)
let is_market_open () =
  let (wday, hour, min) = current_eastern_time () in
  (* Monday=1 through Friday=5; Saturday=6, Sunday=0 *)
  let is_weekday = wday >= 1 && wday <= 5 in
  if not is_weekday then false
  else
    let time_mins = hour * 60 + min in
    let open_mins = extended_open_hour * 60 + extended_open_min in
    let close_mins = extended_close_hour * 60 + extended_close_min in
    time_mins >= open_mins && time_mins < close_mins

(** Calculates the precise duration in seconds until the commencement of the next valid extended trading session.
    If the market is currently evaluated as open, this function returns 0.0. The calculation comprehensively accounts
    for weekend rollovers and time zone offsets to accurately target the next 4:00 AM Eastern Time opening sequence. *)
let seconds_until_next_open () =
  if is_market_open () then 0.0
  else begin
    let t = Unix.gettimeofday () in
    let offset = us_eastern_offset_hours () in
    let eastern_t = t +. (float_of_int offset *. 3600.0) in
    let tm = Unix.gmtime eastern_t in
    let wday = tm.Unix.tm_wday in
    let time_mins = tm.Unix.tm_hour * 60 + tm.Unix.tm_min in
    let open_mins = extended_open_hour * 60 + extended_open_min in

    (* How many days until the next weekday open? *)
    let days_ahead =
      if wday >= 1 && wday <= 5 then begin
        (* Weekday: if before open today, 0 days; if after close, next day *)
        if time_mins < open_mins then 0
        else if wday = 5 then 3  (* Friday after close → Monday *)
        else 1
      end else if wday = 6 then 2  (* Saturday → Monday *)
      else 1  (* Sunday → Monday *)
    in

    (* Compute the target open time in UTC *)
    let target_eastern_midnight =
      (* Truncate to midnight eastern *)
      let today_midnight = eastern_t -. (float_of_int (tm.Unix.tm_hour * 3600 + tm.Unix.tm_min * 60 + tm.Unix.tm_sec)) in
      today_midnight +. (float_of_int days_ahead *. 86400.0)
    in
    let target_open_eastern = target_eastern_midnight +. (float_of_int (extended_open_hour * 3600 + extended_open_min * 60)) in
    (* Convert back to UTC *)
    let target_open_utc = target_open_eastern -. (float_of_int offset *. 3600.0) in
    let delta = target_open_utc -. t in
    (* Safety: never return negative; minimum 1 second *)
    Float.max delta 1.0
  end

(** Generates an exact, human-readable string representation of the current US equity market session status.
    This output is utilized by the telemetry and logging systems to provide clear operational context regarding
    pre-market, regular, after-hours, or closed states. *)
let market_status_string () =
  let (wday, hour, min) = current_eastern_time () in
  let is_weekday = wday >= 1 && wday <= 5 in
  if not is_weekday then "closed (weekend)"
  else begin
    let time_mins = hour * 60 + min in
    let open_mins = extended_open_hour * 60 + extended_open_min in
    let close_mins = extended_close_hour * 60 + extended_close_min in
    if time_mins < open_mins then
      Printf.sprintf "closed (pre-market opens at %d:%02d ET)" extended_open_hour extended_open_min
    else if time_mins >= close_mins then
      "closed (after hours ended)"
    else if hour < 9 || (hour = 9 && min < 30) then
      "open (pre-market)"
    else if hour >= 16 then
      "open (after-hours)"
    else
      "open (regular hours)"
  end

(** Dispatches an informational log entry detailing the current market session status and the calculated time
    until the next trading window. This operation is typically invoked during system initialization and upon successful
    reconnection cycles to establish the operational baseline. *)
let log_market_status () =
  let status = market_status_string () in
  let secs = seconds_until_next_open () in
  if secs > 0.0 then begin
    let hours = secs /. 3600.0 in
    Logging.info_f ~section "US equity market status: %s (next open in %.1f hours)" status hours
  end else
    Logging.info_f ~section "US equity market status: %s" status
