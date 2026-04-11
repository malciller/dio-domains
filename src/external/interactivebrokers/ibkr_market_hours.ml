(** US equity market hours logic for IBKR.

    Determines whether US equity markets are currently within the
    extended trading window (pre-market through after-hours) so the
    supervisor can avoid futile reconnection attempts outside market
    hours when IB Gateway rejects contract resolution requests.

    Extended hours: 4:00 AM – 8:00 PM ET, Monday–Friday.
    Holidays are NOT tracked — the gateway will reject once and the
    supervisor will sleep until the next day's window. *)

let section = "ibkr_market_hours"

(** US Eastern Time UTC offset, accounting for DST.
    DST (EDT, UTC-4): second Sunday in March 2:00 AM to
    first Sunday in November 2:00 AM.
    Standard (EST, UTC-5): rest of the year. *)
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

(** Convert current UTC time to US Eastern hour and minute. *)
let current_eastern_time () =
  let t = Unix.gettimeofday () in
  let offset = us_eastern_offset_hours () in
  let eastern_t = t +. (float_of_int offset *. 3600.0) in
  let tm = Unix.gmtime eastern_t in
  (tm.Unix.tm_wday, tm.Unix.tm_hour, tm.Unix.tm_min)

(** Extended trading window: 4:00 AM – 8:00 PM ET, Monday–Friday. *)
let extended_open_hour = 4
let extended_open_min = 0
let extended_close_hour = 20
let extended_close_min = 0

(** [is_market_open ()] returns [true] if the current time falls within
    the extended US equity trading window (4:00 AM – 8:00 PM ET, Mon–Fri). *)
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

(** [seconds_until_next_open ()] returns the number of seconds until the
    next extended-hours market open. Returns 0.0 if already within the window. *)
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

(** Human-readable market status for logging. *)
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

(** Log the current market status once at startup or reconnection. *)
let log_market_status () =
  let status = market_status_string () in
  let secs = seconds_until_next_open () in
  if secs > 0.0 then begin
    let hours = secs /. 3600.0 in
    Logging.info_f ~section "US equity market status: %s (next open in %.1f hours)" status hours
  end else
    Logging.info_f ~section "US equity market status: %s" status
