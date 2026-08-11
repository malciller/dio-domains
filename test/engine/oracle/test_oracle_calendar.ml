(* Oracle calendar tests: date arithmetic, sorting/dedup, gap detection. *)

let bar date =
  Dio_oracle.Oracle_types.
    { date; open_ = 100.; high = 100.; low = 100.; close = 100.; volume = 0. }
;;

let test_n_days_between () =
  Alcotest.(check int)
    "same day"
    0
    (Dio_oracle.Oracle_calendar.n_days_between "2024-01-01" "2024-01-01");
  Alcotest.(check int)
    "three days apart"
    2
    (Dio_oracle.Oracle_calendar.n_days_between "2024-01-01" "2024-01-03")
;;

let test_n_days_across_month () =
  Alcotest.(check int)
    "month boundary"
    30
    (Dio_oracle.Oracle_calendar.n_days_between "2024-01-31" "2024-03-01")
;;

let test_add_days () =
  let add = Dio_oracle.Oracle_calendar.add_days in
  Alcotest.(check string) "+0" "2024-01-01" (add "2024-01-01" 0);
  Alcotest.(check string) "+1" "2024-01-02" (add "2024-01-01" 1);
  Alcotest.(check string) "-1" "2023-12-31" (add "2024-01-01" (-1));
  Alcotest.(check string) "month end" "2024-03-01" (add "2024-01-31" 30);
  Alcotest.(check string) "leap day" "2024-02-29" (add "2024-01-01" 59);
  Alcotest.(check string) "year roll" "2025-01-01" (add "2024-12-31" 1)
;;

let test_iso_wday () =
  let wday = Dio_oracle.Oracle_calendar.iso_wday in
  Alcotest.(check int) "epoch is Thursday" 4 (wday "1970-01-01");
  Alcotest.(check int) "2024-01-01 Monday" 1 (wday "2024-01-01");
  Alcotest.(check int) "2024-02-29 Thursday" 4 (wday "2024-02-29");
  Alcotest.(check int) "2026-01-15 Thursday" 4 (wday "2026-01-15");
  Alcotest.(check int) "2026-07-15 Wednesday" 3 (wday "2026-07-15")
;;

let test_dates_between () =
  Alcotest.(check (list string))
    "inclusive range"
    [ "2024-01-31"; "2024-02-01"; "2024-02-02" ]
    (Dio_oracle.Oracle_calendar.dates_between
       ~from_date:"2024-01-31"
       ~to_date:"2024-02-02");
  Alcotest.(check (list string))
    "reversed range is empty"
    []
    (Dio_oracle.Oracle_calendar.dates_between
       ~from_date:"2024-02-02"
       ~to_date:"2024-01-31")
;;

let test_sort_dedup () =
  let bars =
    [| bar "2024-01-03"; bar "2024-01-01"; bar "2024-01-01"; bar "2024-01-02" |]
  in
  let sorted =
    bars
    |> Dio_oracle.Oracle_calendar.sort_bars
    |> Dio_oracle.Oracle_calendar.dedup
  in
  Alcotest.(check int) "len" 3 (Array.length sorted);
  Alcotest.(check string) "first" "2024-01-01" sorted.(0).date;
  Alcotest.(check string) "last" "2024-01-03" sorted.(2).date
;;

let test_detect_gaps_crypto () =
  let bars =
    [| bar "2024-01-01"; bar "2024-01-02"; bar "2024-01-05"; bar "2024-01-06" |]
  in
  let gaps =
    Dio_oracle.Oracle_calendar.detect_gaps
      ~calendar_kind:Dio_oracle.Oracle_types.Crypto
      bars
  in
  Alcotest.(check int) "one gap" 1 (List.length gaps);
  match gaps with
  | g :: _ ->
    Alcotest.(check string) "after" "2024-01-02" g.after;
    Alcotest.(check string) "before" "2024-01-05" g.before;
    Alcotest.(check int) "missing days" 2 g.missing_days
  | [] -> Alcotest.fail "expected gap"
;;

let test_detect_gaps_equity_skipped () =
  let bars = [| bar "2024-01-05"; bar "2024-01-08" |] in
  let gaps =
    Dio_oracle.Oracle_calendar.detect_gaps
      ~calendar_kind:Dio_oracle.Oracle_types.Equity
      bars
  in
  Alcotest.(check int) "equity gaps skipped" 0 (List.length gaps)
;;

let test_max_gap () =
  Alcotest.(check int)
    "max gap"
    2
    (Dio_oracle.Oracle_calendar.max_gap
       [ { Dio_oracle.Oracle_types.after = "a"; before = "b"; missing_days = 2 }
       ; { after = "c"; before = "d"; missing_days = 1 }
       ])
;;

let () =
  Alcotest.run
    "oracle_calendar"
    [ ( "calendar"
      , [ Alcotest.test_case "n days between" `Quick test_n_days_between
        ; Alcotest.test_case "n days across month" `Quick test_n_days_across_month
        ; Alcotest.test_case "add days" `Quick test_add_days
        ; Alcotest.test_case "iso weekday" `Quick test_iso_wday
        ; Alcotest.test_case "dates between" `Quick test_dates_between
        ; Alcotest.test_case "sort and dedup" `Quick test_sort_dedup
        ; Alcotest.test_case "detect gaps crypto" `Quick test_detect_gaps_crypto
        ; Alcotest.test_case "detect gaps equity" `Quick test_detect_gaps_equity_skipped
        ; Alcotest.test_case "max gap" `Quick test_max_gap
        ] )
    ]
;;
