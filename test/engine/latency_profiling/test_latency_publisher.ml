open Alcotest

let record profiler ns samples =
  for _ = 1 to samples do
    Latency_profiler.record_ns profiler ns
  done
;;

let samples_of profiler =
  match Latency_profiler.published_snapshot profiler with
  | Some snap -> snap.Latency_profiler.samples
  | None -> -1
;;

let group ?(silent = []) ~key stages : Latency_publisher.group =
  { key
  ; window_seconds = 5.0
  ; threshold_us = 10.0
  ; report_seconds = 5.0
  ; report = false
  ; stages
  ; silent = Atomic.make silent
  ; last_report = 0.0
  }
;;

let test_publish_group_drains_every_window () =
  let book = Latency_profiler.create "book" in
  let total = Latency_profiler.create "total" in
  let overlay = Latency_profiler.create "overlay" in
  record book 2_000 3;
  record total 9_000 2;
  record overlay 1_000 4;
  let g =
    Latency_publisher.register
      (group ~key:"test/drain" ~silent:[ overlay ] [ "BOOK", book; "TOTAL", total ])
  in
  List.iter Latency_profiler.rotate [ book; total; overlay ];
  Latency_publisher.publish_group g;
  check int "stage window published" 3 (samples_of book);
  check int "second stage published" 2 (samples_of total);
  check int "silent window published" 4 (samples_of overlay);
  (* Nothing was rotated since, so a second poll republishes nothing and leaves the
     snapshots untouched. *)
  Latency_publisher.publish_group g;
  check int "no duplicate publication" 3 (samples_of book)
;;

let test_add_silent_takes_effect_next_rotation () =
  let total = Latency_profiler.create "total" in
  let late = Latency_profiler.create "late" in
  let g = Latency_publisher.register (group ~key:"test/late" [ "TOTAL", total ]) in
  record total 5_000 1;
  Latency_profiler.rotate total;
  Latency_publisher.publish_group g;
  check int "late profiler had no group yet" (-1) (samples_of late);
  Latency_publisher.add_silent g [ late ];
  record late 7_000 2;
  List.iter Latency_profiler.rotate [ total; late ];
  Latency_publisher.publish_group g;
  check int "added profiler is drained on the next window" 2 (samples_of late)
;;

let test_register_replaces_same_key () =
  let a = Latency_profiler.create "a" in
  let b = Latency_profiler.create "b" in
  ignore (Latency_publisher.register (group ~key:"test/replace" [ "TOTAL", a ]));
  let g = Latency_publisher.register (group ~key:"test/replace" [ "TOTAL", b ]) in
  record a 1_000 1;
  record b 1_000 1;
  List.iter Latency_profiler.rotate [ a; b ];
  Latency_publisher.publish_group g;
  check int "replacement group drives the new profiler" 1 (samples_of b);
  check int "the replaced profiler is no longer polled" (-1) (samples_of a)
;;

let test_rotate_without_publisher_publishes_now () =
  let total = Latency_profiler.create "total" in
  record total 5_000 2;
  let g = Latency_publisher.register (group ~key:"test/fallback" [ "TOTAL", total ]) in
  (* No publisher domain in this process, so rotation must not strand the window. *)
  Latency_publisher.rotate g;
  check int "fallback publishes synchronously" 2 (samples_of total)
;;

let () =
  run
    "latency publisher"
    [ ( "groups"
      , [ test_case
            "drains stages and silent windows"
            `Quick
            test_publish_group_drains_every_window
        ; test_case
            "added silent windows join the next rotation"
            `Quick
            test_add_silent_takes_effect_next_rotation
        ; test_case
            "register replaces a domain's group"
            `Quick
            test_register_replaces_same_key
        ; test_case
            "rotation without a publisher publishes now"
            `Quick
            test_rotate_without_publisher_publishes_now
        ] )
    ]
;;
