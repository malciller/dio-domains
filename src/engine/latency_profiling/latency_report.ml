(** Reader and CLI for the latency journal written by {!Latency_journal}.

    Three verbs, all read-only. They read journal files without booting the engine, so
    they run inside the engine container or against a copy of the files anywhere:

    - [tail] current value per stage, from the newest samples in a time window.
    - [best] minimum p99 ever recorded per stage, with when it happened. Observability
      only: the minimum across runs is usually the least representative (least loaded)
      run, so it informs but never gates.
    - [compare] two runs side by side, sorted by regression. With [--gate] it exits 1 when
      any stage regresses beyond tolerance.

    [compare] summarizes a run as the *median* of its per-window p99 rather than a single
    window, because one window is noise. Run it over deterministic replay output to make a
    delta attributable to the code; over two prod nights it mostly reports market weather. *)

type sample =
  { t : float
  ; d : string
  ; s : string
  ; n : int
  ; p50 : float
  ; p99 : float
  ; p999 : float
  ; mx : float
  ; cause : string option
  ; g : string option
  }

let num (j : Yojson.Basic.t) key =
  let open Yojson.Basic.Util in
  match j |> member key with
  | `Int i -> float_of_int i
  | `Float f -> f
  | _ -> 0.0
;;

let int_of_json (j : Yojson.Basic.t) key =
  let open Yojson.Basic.Util in
  match j |> member key with
  | `Int i -> i
  | `Float f -> int_of_float f
  | _ -> 0
;;

let string_of_json (j : Yojson.Basic.t) key =
  let open Yojson.Basic.Util in
  match j |> member key with
  | `String s -> Some s
  | _ -> None
;;

(** [None] for a blank, truncated, or unparsable line. A SIGKILL can leave a partial final
    record; the whole point of line-oriented JSON is that it costs one row, not the file. *)
let parse_line line =
  let line = String.trim line in
  if String.equal line ""
  then None
  else (
    match Yojson.Basic.from_string line with
    | exception _ -> None
    | `Assoc _ as j ->
      (match string_of_json j "d", string_of_json j "s" with
       | Some d, Some s ->
         Some
           { t = num j "t"
           ; d
           ; s
           ; n = int_of_json j "n"
           ; p50 = num j "p50"
           ; p99 = num j "p99"
           ; p999 = num j "p999"
           ; mx = num j "mx"
           ; cause = string_of_json j "c"
           ; g = string_of_json j "g"
           }
       | _ -> None)
    | _ -> None)
;;

(** Every parsable sample in [path]. Missing or unreadable files yield [[]]. *)
let read_file path =
  let ic =
    try Some (open_in_bin path) with
    | _ -> None
  in
  match ic with
  | None -> []
  | Some ic ->
    let rec loop acc =
      match input_line ic with
      | line ->
        let acc =
          match parse_line line with
          | Some s -> s :: acc
          | None -> acc
        in
        loop acc
      | exception End_of_file -> List.rev acc
    in
    let rows = loop [] in
    close_in_noerr ic;
    rows
;;

let read_dir dir = List.concat_map read_file (Latency_journal.journal_files dir)

(** Resolve a CLI path argument: a file stands for itself, a directory for its newest run
    that actually contains samples. Skipping empty files matters because a SIGKILL right
    after start leaves a zero-row file, which is newer but useless. *)
let resolve path =
  if Sys.file_exists path && Sys.is_directory path
  then (
    let candidates = List.rev (Latency_journal.journal_files path) in
    let rec first_nonempty = function
      | [] -> None
      | p :: rest -> if read_file p = [] then first_nonempty rest else Some p
    in
    first_nonempty candidates)
  else if Sys.file_exists path
  then Some path
  else None
;;

type common =
  { dir : string
  ; file : string option
  ; since_s : float
  ; domain : string option
  ; tolerance_pct : float
  ; min_samples : int
  ; gate : bool
  }

let usage =
  "usage:\n\
  \  dio latency tail    [--dir D | --file F] [--since 1h] [--domain BTC]\n\
  \  dio latency best    [--dir D] [--domain BTC]\n\
  \  dio latency compare OLD NEW [--tolerance-pct 10] [--min-samples 100] [--gate]\n\n\
  \  OLD/NEW may each be a .jsonl file or a directory (its newest run).\n\
  \  --gate makes compare exit 1 when any stage's p99 regresses past the tolerance."
;;

(** [30s], [5m], [2h], [1d] or a bare number of seconds. [None] for anything else, so a
    typo is an error rather than an empty result table. *)
let parse_duration s =
  let s = String.trim s in
  let len = String.length s in
  if len = 0
  then None
  else (
    let suffix = s.[len - 1] in
    let scale, body =
      match suffix with
      | 's' | 'S' -> 1.0, String.sub s 0 (len - 1)
      | 'm' | 'M' -> 60.0, String.sub s 0 (len - 1)
      | 'h' | 'H' -> 3600.0, String.sub s 0 (len - 1)
      | 'd' | 'D' -> 86400.0, String.sub s 0 (len - 1)
      | _ -> 1.0, s
    in
    Option.map (fun f -> f *. scale) (float_of_string_opt (String.trim body)))
;;

let default_common () =
  { dir = Latency_journal.default_dir ()
  ; file = None
  ; since_s = 3600.0
  ; domain = None
  ; tolerance_pct = 10.0
  ; min_samples = 100
  ; gate = false
  }
;;

(** Parse flags left to right, collecting non-flag arguments as positionals. A flag's
    value is consumed with the flag, so [--min-samples 100] never leaves "100" looking
    like a path. *)
let parse_common argv =
  let rec go c positionals = function
    | [] -> Ok (c, List.rev positionals)
    | "--dir" :: d :: rest -> go { c with dir = d } positionals rest
    | "--file" :: f :: rest -> go { c with file = Some f } positionals rest
    | "--since" :: v :: rest ->
      (match parse_duration v with
       | Some since_s -> go { c with since_s } positionals rest
       | None -> Error (Printf.sprintf "invalid --since %S" v))
    | "--domain" :: d :: rest -> go { c with domain = Some d } positionals rest
    | "--tolerance-pct" :: v :: rest ->
      (match float_of_string_opt v with
       | Some f -> go { c with tolerance_pct = f } positionals rest
       | None -> Error (Printf.sprintf "invalid --tolerance-pct %S" v))
    | "--min-samples" :: v :: rest ->
      (match int_of_string_opt v with
       | Some i -> go { c with min_samples = i } positionals rest
       | None -> Error (Printf.sprintf "invalid --min-samples %S" v))
    | "--gate" :: rest -> go { c with gate = true } positionals rest
    | flag :: _ when String.length flag > 0 && flag.[0] = '-' ->
      Error (Printf.sprintf "unknown argument %S" flag)
    | arg :: rest -> go c (arg :: positionals) rest
  in
  go (default_common ()) [] argv
;;

let median xs =
  match List.sort compare xs with
  | [] -> 0.0
  | ys ->
    let a = Array.of_list ys in
    let n = Array.length a in
    if n mod 2 = 1 then a.(n / 2) else (a.((n / 2) - 1) +. a.(n / 2)) /. 2.0
;;

let pad_right s w =
  if String.length s >= w then s else s ^ String.make (w - String.length s) ' '
;;

let pad_left s w =
  if String.length s >= w then s else String.make (w - String.length s) ' ' ^ s
;;

(** A duration for humans; [Latency_profiler.format_us] is for latencies, not spans. *)
let human_duration s =
  if s >= 86_400.0
  then Printf.sprintf "%.0fd" (s /. 86_400.0)
  else if s >= 3_600.0
  then Printf.sprintf "%.0fh" (s /. 3_600.0)
  else if s >= 60.0
  then Printf.sprintf "%.0fm" (s /. 60.0)
  else Printf.sprintf "%.0fs" s
;;

let header cols widths =
  String.concat "  " (List.map2 (fun c w -> pad_right c w) cols widths)
;;

module Key = struct
  type t = string * string

  let compare = compare
end

module KeyMap = Map.Make (Key)

(** Group samples by (domain, stage). *)
let group rows =
  List.fold_left
    (fun acc (s : sample) ->
      KeyMap.update
        (s.d, s.s)
        (function
          | None -> Some [ s ]
          | Some l -> Some (s :: l))
        acc)
    KeyMap.empty
    rows
;;

let keep_domain domain rows =
  match domain with
  | None -> rows
  | Some d -> List.filter (fun (s : sample) -> String.equal s.d d) rows
;;

let tail ~(c : common) =
  let rows =
    match c.file with
    | Some f -> read_file f
    | None -> read_dir c.dir
  in
  let rows = keep_domain c.domain rows in
  let cutoff = Unix.gettimeofday () -. c.since_s in
  let recent = List.filter (fun (s : sample) -> s.t >= cutoff && s.n > 0) rows in
  if recent = []
  then (
    Printf.printf "no samples in the last %s\n" (human_duration c.since_s);
    0)
  else (
    (* Newest sample per (domain, stage): "tail" means current state, not history. *)
    let latest =
      KeyMap.mapi
        (fun _ l ->
          List.fold_left (fun a b -> if b.t > a.t then b else a) (List.hd l) (List.tl l))
        (group recent)
    in
    let entries = KeyMap.bindings latest |> List.map (fun ((d, s), v) -> d, s, v) in
    let wd =
      List.fold_left
        (fun m (d, _, _) -> max m (String.length d))
        (String.length "domain")
        entries
    in
    let ws =
      List.fold_left
        (fun m (_, s, _) -> max m (String.length s))
        (String.length "stage")
        entries
    in
    Printf.printf
      "%s\n"
      (header
         [ "domain"; "stage"; "n"; "p50"; "p99"; "p999"; "max"; "cause" ]
         [ wd; ws; 7; 9; 9; 9; 9; 0 ]);
    List.iter
      (fun (d, s, (v : sample)) ->
        Printf.printf
          "%s  %s  %s  %s  %s  %s  %s  %s\n"
          (pad_right d wd)
          (pad_right s ws)
          (pad_left (string_of_int v.n) 7)
          (pad_left (Latency_profiler.format_us v.p50) 9)
          (pad_left (Latency_profiler.format_us v.p99) 9)
          (pad_left (Latency_profiler.format_us v.p999) 9)
          (pad_left (Latency_profiler.format_us v.mx) 9)
          (Option.value v.cause ~default:""))
      entries;
    0)
;;

let best ~(c : common) =
  let rows = keep_domain c.domain (read_dir c.dir) in
  if rows = []
  then (
    Printf.printf "no journal files in %s\n" c.dir;
    0)
  else (
    let minima =
      List.fold_left
        (fun acc (s : sample) ->
          if s.n <= 0 || s.p99 <= 0.0
          then acc
          else
            KeyMap.update
              (s.d, s.s)
              (function
                | None -> Some s
                | Some (prev : sample) when s.p99 < prev.p99 -> Some s
                | some -> some)
              acc)
        KeyMap.empty
        rows
    in
    let entries = KeyMap.bindings minima |> List.map (fun ((d, s), v) -> d, s, v) in
    let wd =
      List.fold_left
        (fun m (d, _, _) -> max m (String.length d))
        (String.length "domain")
        entries
    in
    let ws =
      List.fold_left
        (fun m (_, s, _) -> max m (String.length s))
        (String.length "stage")
        entries
    in
    Printf.printf
      "%s\n"
      (header [ "domain"; "stage"; "best p99"; "when"; "commit" ] [ wd; ws; 9; 19; 0 ]);
    List.iter
      (fun (d, s, (v : sample)) ->
        let when_ =
          let tm = Unix.localtime v.t in
          Printf.sprintf
            "%04d-%02d-%02d %02d:%02d"
            (tm.Unix.tm_year + 1900)
            (tm.Unix.tm_mon + 1)
            tm.Unix.tm_mday
            tm.Unix.tm_hour
            tm.Unix.tm_min
        in
        let commit =
          match v.g with
          | Some sha when String.trim sha <> "" ->
            String.sub sha 0 (min 12 (String.length sha))
          | _ -> "-"
        in
        Printf.printf
          "%s  %s  %s  %s  %s\n"
          (pad_right d wd)
          (pad_right s ws)
          (pad_left (Latency_profiler.format_us v.p99) 9)
          when_
          commit)
      entries;
    Printf.printf
      "\n\
       best-ever p99 is an observation, not a target: the minimum usually came from the \
       least loaded run.\n";
    0)
;;

type agg =
  { samples : int
  ; p99_median : float
  ; p99_max : float
  }

let summarize rows =
  KeyMap.mapi
    (fun _ l ->
      let l = (l : sample list) in
      { samples = List.fold_left (fun a (s : sample) -> a + s.n) 0 l
      ; p99_median = median (List.map (fun (s : sample) -> s.p99) l)
      ; p99_max = List.fold_left (fun a (s : sample) -> Float.max a s.p99) 0.0 l
      })
    (group rows)
;;

let compare_runs ~(c : common) old_arg new_arg =
  match resolve old_arg, resolve new_arg with
  | None, _ ->
    Printf.eprintf "latency: no journal file at or in %S\n" old_arg;
    2
  | _, None ->
    Printf.eprintf "latency: no journal file at or in %S\n" new_arg;
    2
  | Some old_path, Some new_path ->
    let old_rows = read_file old_path in
    let new_rows = read_file new_path in
    if old_rows = [] || new_rows = []
    then (
      Printf.eprintf "latency: %S or %S has no parsable samples\n" old_path new_path;
      2)
    else (
      let old_agg = summarize old_rows in
      let new_agg = summarize new_rows in
      let rows =
        KeyMap.fold
          (fun (d, s) (n : agg) acc ->
            match KeyMap.find_opt (d, s) old_agg with
            | None -> acc
            | Some (o : agg) ->
              (* The canary row carries a window maximum, not a distribution, so its "p99"
                 swings with a single pause. Report it in [tail] and [best]; never let it
                 fail a gate. *)
              if String.equal s "canary"
              then acc
              else if o.samples < c.min_samples || n.samples < c.min_samples
              then acc
              else if o.p99_median <= 0.0
              then acc
              else (
                let delta_pct = (n.p99_median -. o.p99_median) /. o.p99_median *. 100.0 in
                (d, s, o, n, delta_pct) :: acc))
          new_agg
          []
      in
      let rows =
        List.sort (fun (_, _, _, _, a) (_, _, _, _, b) -> Float.compare b a) rows
      in
      if rows = []
      then (
        Printf.printf
          "no comparable stages (need >= %d samples in both runs)\n"
          c.min_samples;
        0)
      else (
        let wd =
          List.fold_left (fun m (d, _, _, _, _) -> max m (String.length d)) 6 rows
        in
        let ws =
          List.fold_left (fun m (_, s, _, _, _) -> max m (String.length s)) 5 rows
        in
        Printf.printf "old: %s\n" old_path;
        Printf.printf "new: %s\n\n" new_path;
        Printf.printf
          "%s\n"
          (header
             [ "domain"; "stage"; "old p99"; "new p99"; "delta"; "peak"; "samples" ]
             [ wd; ws; 9; 9; 8; 9; 0 ]);
        let regressions =
          List.fold_left
            (fun acc (d, s, (o : agg), (n : agg), delta_pct) ->
              let flag = if delta_pct > c.tolerance_pct then " >" else "  " in
              Printf.printf
                "%s  %s  %s  %s  %s%s  %s  %d/%d\n"
                (pad_right d wd)
                (pad_right s ws)
                (pad_left (Latency_profiler.format_us o.p99_median) 9)
                (pad_left (Latency_profiler.format_us n.p99_median) 9)
                (pad_left (Printf.sprintf "%+.1f%%" delta_pct) 8)
                flag
                (pad_left (Latency_profiler.format_us n.p99_max) 9)
                o.samples
                n.samples;
              if delta_pct > c.tolerance_pct then acc + 1 else acc)
            0
            rows
        in
        Printf.printf
          "\n%d stage(s) compared, %d beyond %.1f%% tolerance\n"
          (List.length rows)
          regressions
          c.tolerance_pct;
        if c.gate && regressions > 0
        then (
          Printf.eprintf "latency: gate failed (%d regression(s))\n" regressions;
          1)
        else 0))
;;

(** [run argv] dispatches `dio latency ...`. Returns the process exit code. *)
let run argv =
  match argv with
  | [] | [ "--help" ] | [ "-h" ] ->
    print_string usage;
    print_newline ();
    0
  | "tail" :: rest ->
    (match parse_common rest with
     | Error e ->
       Printf.eprintf "latency: %s\n%s\n" e usage;
       2
     | Ok (c, []) -> tail ~c
     | Ok (_, _ :: _) ->
       Printf.eprintf "latency: tail takes no positional arguments\n%s\n" usage;
       2)
  | "best" :: rest ->
    (match parse_common rest with
     | Error e ->
       Printf.eprintf "latency: %s\n%s\n" e usage;
       2
     | Ok (c, []) -> best ~c
     | Ok (_, _ :: _) ->
       Printf.eprintf "latency: best takes no positional arguments\n%s\n" usage;
       2)
  | "compare" :: rest ->
    (match parse_common rest with
     | Error e ->
       Printf.eprintf "latency: %s\n%s\n" e usage;
       2
     | Ok (c, [ old_arg; new_arg ]) -> compare_runs ~c old_arg new_arg
     | Ok (_, _) ->
       Printf.eprintf "latency: compare needs exactly two paths\n%s\n" usage;
       2)
  | verb :: _ ->
    Printf.eprintf "latency: unknown verb %S\n%s\n" verb usage;
    2
;;
