(* Oracle_loader - shared CSV/JSON fixture IO for the CLI (--from-csv) and
   the test suites. No network.

   CSV: one row per line, optional header
   "date,open,high,low,close,volume", fields comma-separated.

   JSON: either an array of {date,open,high,low,close,volume} objects or an
   envelope {"bars": [...]} (Alpaca-shaped). *)

open Oracle_types

let parse_csv ~(symbol : string) ~(calendar_kind : calendar_kind) (csv : string) : series =
  let rows =
    String.split_on_char '\n' csv
    |> List.map String.trim
    |> List.filter (fun l -> l <> "")
  in
  let rows =
    match rows with
    | h :: t when String.lowercase_ascii h |> String.starts_with ~prefix:"date" -> t
    | _ -> rows
  in
  let bar_of_line line =
    match String.split_on_char ',' line with
    | [ date; o; h; l; c; v ] ->
      (try
         Some
           { date
           ; open_ = float_of_string o
           ; high = float_of_string h
           ; low = float_of_string l
           ; close = float_of_string c
           ; volume = float_of_string v
           }
       with
       | _ -> None)
    | _ -> None
  in
  let bars =
    rows
    |> List.filter_map bar_of_line
    |> Array.of_list
    |> Oracle_calendar.sort_bars
    |> Oracle_calendar.dedup
  in
  { symbol; calendar_kind; bars; gaps = [] }
;;

let csv_of_series (s : series) : string =
  let sb = Buffer.create 1024 in
  Buffer.add_string sb "date,open,high,low,close,volume\n";
  Array.iter
    (fun b ->
       Buffer.add_string
         sb
         (Printf.sprintf
            "%s,%f,%f,%f,%f,%f\n"
            b.date
            b.open_
            b.high
            b.low
            b.close
            b.volume))
    s.bars;
  Buffer.contents sb
;;

let bar_of_json (j : Yojson.Safe.t) : bar option =
  let open Yojson.Safe.Util in
  try
    let date = member "date" j |> to_string in
    let f k = member k j |> to_float in
    Some
      { date
      ; open_ = f "open"
      ; high = f "high"
      ; low = f "low"
      ; close = f "close"
      ; volume =
          (try f "volume" with
           | _ -> 0.0)
      }
  with
  | _ -> None
;;

let parse_json ~(symbol : string) ~(calendar_kind : calendar_kind) (json : Yojson.Safe.t)
  : series
  =
  let items =
    match json with
    | `List l -> l
    | _ ->
      (match Yojson.Safe.Util.member "bars" json with
       | `List l -> l
       | _ -> [])
  in
  let bars =
    items
    |> List.filter_map bar_of_json
    |> Array.of_list
    |> Oracle_calendar.sort_bars
    |> Oracle_calendar.dedup
  in
  { symbol; calendar_kind; bars; gaps = [] }
;;

let json_of_series (s : series) : Yojson.Safe.t =
  `List
    (Array.to_list
       (Array.map
          (fun b ->
             `Assoc
               [ "date", `String b.date
               ; "open", `Float b.open_
               ; "high", `Float b.high
               ; "low", `Float b.low
               ; "close", `Float b.close
               ; "volume", `Float b.volume
               ])
          s.bars))
;;

let load_csv_file ~(symbol : string) ~(calendar_kind : calendar_kind) ~(path : string)
  : series
  =
  let ic = open_in path in
  Fun.protect
    ~finally:(fun () -> close_in ic)
    (fun () ->
       let n = in_channel_length ic in
       parse_csv ~symbol ~calendar_kind (really_input_string ic n))
;;

let load_json_file ~(symbol : string) ~(calendar_kind : calendar_kind) ~(path : string)
  : series
  =
  let ic = open_in path in
  Fun.protect
    ~finally:(fun () -> close_in ic)
    (fun () ->
       let n = in_channel_length ic in
       parse_json
         ~symbol
         ~calendar_kind
         (Yojson.Safe.from_string (really_input_string ic n)))
;;
