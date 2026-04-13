let parse_kraken_time s =
  try
    Scanf.sscanf s "%d-%d-%dT%d:%d:%f" (fun y m d h min s ->
      let tm, _ = Unix.mktime { Unix.tm_sec = int_of_float s; tm_min = min; tm_hour = h; 
                 tm_mday = d; tm_mon = m - 1; tm_year = y - 1900; 
                 tm_wday = 0; tm_yday = 0; tm_isdst = false } in
      let _tz_offset = fst (Unix.mktime (Unix.gmtime 0.0)) in
      (* Actually mktime converts local to UTC timestamp. Since input is UTC, this is wrong without adjustment *)
      let local_tm = Unix.localtime (Unix.gettimeofday ()) in
      let gm_tm = Unix.gmtime (Unix.gettimeofday ()) in
      let diff = fst (Unix.mktime local_tm) -. fst (Unix.mktime gm_tm) in
      tm +. diff +. (s -. floor s)
    )
  with _ -> Unix.gettimeofday ()

let () = Printf.printf "%f\n" (parse_kraken_time "2024-03-25T14:32:00.123456Z")
