open Alcotest
module J = Latency_journal
module R = Latency_report

let tmp_prefix = "dio-latency-journal-"

let fresh_dir () =
  let base = Filename.get_temp_dir_name () in
  let dir =
    Filename.concat
      base
      (Printf.sprintf "%s%d-%d" tmp_prefix (Unix.getpid ()) (Random.int 1_000_000))
  in
  (try Unix.mkdir dir 0o755 with
   | Unix.Unix_error (Unix.EEXIST, _, _) -> ());
  dir
;;

let rm_rf dir =
  try
    Sys.readdir dir
    |> Array.iter (fun f ->
      try Unix.unlink (Filename.concat dir f) with
      | _ -> ());
    Unix.rmdir dir
  with
  | _ -> ()
;;

(** A real snapshot with [samples] samples at [p99_us], so the journal tests exercise the
    same values production writes instead of a hand-built record. *)
let snapshot ?(samples = 100) ?(p99_us = 2.0) name =
  let p = Latency_profiler.create name in
  let ns = int_of_float (p99_us *. 1000.0) in
  for _ = 1 to samples do
    Latency_profiler.record_ns p ns
  done;
  Latency_profiler.snapshot_and_reset p
;;

let row ?(domain = "BTC") ?(stage = "prep") ?(window_end = 1.0) snap : J.row =
  { domain; stage; snap = { snap with Latency_profiler.window_end } }
;;

let make_journal ?(dir = "") ?(retain_days = 14) ?(max_total_mb = 128) rows =
  { J.opts =
      { enabled = true
      ; interval_s = 30.0
      ; dir
      ; retain_days
      ; max_total_mb
      ; min_free_mb = 0
      }
  ; produce = (fun () -> rows)
  ; seen = Hashtbl.create 16
  ; fd = None
  ; path = ""
  ; ticks = 0
  ; space_warned = false
  }
;;

let rows_in dir = List.concat_map R.read_file (J.journal_files dir)

(* ---------------------------------------------------------------- *)

let test_journal_files_filter () =
  let dir = fresh_dir () in
  let touch name =
    let oc = open_out (Filename.concat dir name) in
    close_out oc
  in
  touch "latency-20260101-000000-1.jsonl";
  touch "latency-20260102-000000-2.jsonl";
  touch "notes.txt";
  touch "other-latency-20260103-000000-3.jsonl";
  let files = J.journal_files dir in
  check int "only journal names, in name order" 2 (List.length files);
  check
    string
    "oldest first"
    (Filename.concat dir "latency-20260101-000000-1.jsonl")
    (List.hd files);
  rm_rf dir
;;

let test_prune_by_age () =
  let dir = fresh_dir () in
  let touch_aged name days =
    let path = Filename.concat dir name in
    let oc = open_out path in
    close_out oc;
    let t = Unix.gettimeofday () -. (float_of_int days *. 86_400.0) in
    Unix.utimes path t t
  in
  touch_aged "latency-20260101-000000-1.jsonl" 30;
  touch_aged "latency-20260102-000000-2.jsonl" 1;
  J.prune ~dir ~retain_days:14 ~max_total_bytes:100_000_000 ~current:None;
  let names = J.journal_files dir |> List.map Filename.basename in
  check
    (list string)
    "old file pruned, recent kept"
    [ "latency-20260102-000000-2.jsonl" ]
    names;
  rm_rf dir
;;

let test_prune_by_size () =
  let dir = fresh_dir () in
  let write name bytes =
    let path = Filename.concat dir name in
    let oc = open_out_bin path in
    output_string oc (String.make bytes 'x');
    close_out oc
  in
  write "latency-20260101-000000-1.jsonl" 60_000;
  write "latency-20260102-000000-2.jsonl" 60_000;
  (* cap at 1 MB is not reached, but 100 KB cap forces one deletion, oldest first *)
  J.prune ~dir ~retain_days:3650 ~max_total_bytes:100_000 ~current:None;
  let names = J.journal_files dir |> List.map Filename.basename in
  check (list string) "oldest deleted first" [ "latency-20260102-000000-2.jsonl" ] names;
  rm_rf dir
;;

let test_prune_keeps_current () =
  let dir = fresh_dir () in
  let write name bytes =
    let path = Filename.concat dir name in
    let oc = open_out_bin path in
    output_string oc (String.make bytes 'x');
    close_out oc;
    path
  in
  ignore (write "latency-20260101-000000-1.jsonl" 60_000);
  let current = write "latency-20260102-000000-2.jsonl" 60_000 in
  J.prune ~dir ~retain_days:3650 ~max_total_bytes:1 ~current:(Some current);
  check
    bool
    "the open file survives a prune that cannot meet the cap"
    true
    (Sys.file_exists current);
  rm_rf dir
;;

let test_tick_writes_and_dedupes () =
  let dir = fresh_dir () in
  let snap = snapshot "prep" in
  let rows = [ row ~window_end:1.0 snap; row ~domain:"ETH" ~window_end:1.0 snap ] in
  let t = make_journal ~dir rows in
  J.tick t;
  check int "one row per domain" 2 (List.length (rows_in dir));
  (* Same window_end again: a re-sampled window must not be written twice. *)
  J.tick t;
  check int "repeated window is deduped" 2 (List.length (rows_in dir));
  (* A new window is written. *)
  t.produce <- (fun () -> [ row ~window_end:2.0 snap ]);
  J.tick t;
  check int "new window appended" 3 (List.length (rows_in dir));
  rm_rf dir
;;

let test_tick_skips_idle () =
  let dir = fresh_dir () in
  let idle = snapshot ~samples:0 "prep" in
  let t = make_journal ~dir [ row idle ] in
  J.tick t;
  check int "a window with no samples is not journalled" 0 (List.length (rows_in dir));
  rm_rf dir
;;

let test_truncated_line_tolerated () =
  let dir = fresh_dir () in
  let snap = snapshot "prep" in
  let t = make_journal ~dir [ row ~window_end:1.0 snap ] in
  J.tick t;
  let path = List.hd (J.journal_files dir) in
  let oc = open_out_gen [ Open_append ] 0o644 path in
  output_string oc "{\"t\":1,\"d\":\"BTC\",\"s\":\"pr";
  close_out oc;
  check
    int
    "the partial trailing record costs one row, not the file"
    1
    (List.length (R.read_file path));
  rm_rf dir
;;

(** Write a run of 20 windows for one domain/stage at a fixed p99. *)
let write_run ?(stage = "prep") dir name ~p99_us ~samples =
  let path = Filename.concat dir name in
  let oc = open_out path in
  for i = 0 to 19 do
    Printf.fprintf
      oc
      "{\"t\":%d,\"d\":\"BTC\",\"s\":\"%s\",\"n\":%d,\"p50\":%.3f,\"p99\":%.3f,\"p999\":%.3f,\"mx\":%.3f,\"ov\":0,\"w\":%d,\"c\":null}\n"
      i
      stage
      samples
      (p99_us /. 2.0)
      p99_us
      (p99_us *. 2.0)
      (p99_us *. 4.0)
      i
  done;
  close_out oc;
  path
;;

let test_compare_gate () =
  let dir = fresh_dir () in
  let base = write_run dir "latency-20260101-000000-1.jsonl" ~p99_us:2.0 ~samples:500 in
  ignore (write_run dir "latency-20260102-000000-2.jsonl" ~p99_us:2.0 ~samples:500);
  let worse = write_run dir "latency-20260103-000000-3.jsonl" ~p99_us:2.8 ~samples:500 in
  let common =
    { R.dir
    ; file = None
    ; since_s = 3600.0
    ; domain = None
    ; tolerance_pct = 10.0
    ; min_samples = 100
    ; gate = true
    }
  in
  check int "identical runs pass the gate" 0 (R.compare_runs ~c:common base base);
  check int "a 40% p99 regression fails the gate" 1 (R.compare_runs ~c:common base worse);
  rm_rf dir
;;

(** The canary row is a window maximum, not a distribution; a single pause would flake a
    gate, so compare must never consider it. *)
let test_compare_excludes_canary () =
  let dir = fresh_dir () in
  let old =
    write_run
      ~stage:"canary"
      dir
      "latency-20260101-000000-1.jsonl"
      ~p99_us:1.0
      ~samples:200
  in
  let new_ =
    write_run
      ~stage:"canary"
      dir
      "latency-20260102-000000-2.jsonl"
      ~p99_us:50.0
      ~samples:200
  in
  let common =
    { R.dir
    ; file = None
    ; since_s = 3600.0
    ; domain = None
    ; tolerance_pct = 0.0
    ; min_samples = 1
    ; gate = true
    }
  in
  check
    int
    "a canary max swing never fails the gate"
    0
    (R.compare_runs ~c:common old new_);
  rm_rf dir
;;

let test_compare_ignores_idle () =
  let dir = fresh_dir () in
  let path name =
    let p = Filename.concat dir name in
    let oc = open_out p in
    (* 20 windows with 0 samples each: an idle domain must not be compared. *)
    for i = 0 to 19 do
      Printf.fprintf
        oc
        "{\"t\":%d,\"d\":\"SOL\",\"s\":\"cycle\",\"n\":0,\"p50\":0,\"p99\":0,\"p999\":0,\"mx\":0,\"ov\":0,\"w\":%d,\"c\":null}\n"
        i
        i
    done;
    close_out oc;
    p
  in
  let a = path "latency-20260101-000000-1.jsonl" in
  let b = path "latency-20260102-000000-2.jsonl" in
  let common =
    { R.dir
    ; file = None
    ; since_s = 3600.0
    ; domain = None
    ; tolerance_pct = 0.0
    ; min_samples = 1
    ; gate = true
    }
  in
  check int "no comparable stages, no gate failure" 0 (R.compare_runs ~c:common a b);
  rm_rf dir
;;

let test_median () =
  check (float 1e-9) "odd" 2.0 (R.median [ 3.0; 1.0; 2.0 ]);
  check (float 1e-9) "even averages the middle pair" 2.5 (R.median [ 4.0; 1.0; 3.0; 2.0 ]);
  check (float 1e-9) "empty" 0.0 (R.median [])
;;

let test_parse_duration () =
  let secs s =
    match R.parse_duration s with
    | Some f -> f
    | None -> Alcotest.failf "expected a duration from %S" s
  in
  check (float 1e-9) "bare seconds" 42.0 (secs "42");
  check (float 1e-9) "h suffix" 7200.0 (secs "2h");
  check (float 1e-9) "m suffix" 90.0 (secs "1.5m");
  check bool "garbage is rejected" true (R.parse_duration "later" = None)
;;

let () =
  Random.self_init ();
  run
    "latency journal"
    [ ( "files"
      , [ test_case "journal file filter" `Quick test_journal_files_filter
        ; test_case "prune by age" `Quick test_prune_by_age
        ; test_case "prune by size, oldest first" `Quick test_prune_by_size
        ; test_case "prune never deletes the open file" `Quick test_prune_keeps_current
        ] )
    ; ( "writer"
      , [ test_case "tick writes once per window" `Quick test_tick_writes_and_dedupes
        ; test_case "idle window is skipped" `Quick test_tick_skips_idle
        ; test_case "truncated line tolerated" `Quick test_truncated_line_tolerated
        ] )
    ; ( "reader"
      , [ test_case "gate fails on regression" `Quick test_compare_gate
        ; test_case "idle domain excluded" `Quick test_compare_ignores_idle
        ; test_case "canary excluded from the gate" `Quick test_compare_excludes_canary
        ; test_case "median" `Quick test_median
        ; test_case "duration parsing" `Quick test_parse_duration
        ] )
    ]
;;
