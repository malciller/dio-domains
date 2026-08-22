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
    bars |> Dio_oracle.Oracle_calendar.sort_bars |> Dio_oracle.Oracle_calendar.dedup
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

(* ---- Source normalization (Oracle_calendar.normalize_bars) ----
   Real @142 (UBTC/USDC) fixture: the pre-listing placeholder run
   (2025-02-03..2025-02-13: fabricated constant candles 6,969,696 / 7,979,573
   with zero or dust volume) plus the first real trading day whose open/high
   (240,000) never traded (close 97,578) - exactly the rows observed from
   candleSnapshot that made the oracle read a 99.3% drawdown and a $7.98M
   peak. The normalization is shared by every venue's fetch path. *)

let mk_bar ~date ~open_ ~high ~low ~close ~volume =
  Dio_oracle.Oracle_types.{ date; open_; high; low; close; volume }
;;

let placeholder_bars () =
  let flat date close volume =
    mk_bar ~date ~open_:close ~high:close ~low:close ~close ~volume
  in
  [ mk_bar
      ~date:"2025-02-03"
      ~open_:100002060.0
      ~high:100002060.0
      ~low:6969696.0
      ~close:6969696.0
      ~volume:0.00005
  ; flat "2025-02-04" 6969696.0 0.0
  ; flat "2025-02-05" 6969696.0 0.0
  ; flat "2025-02-06" 6969696.0 0.0
  ; flat "2025-02-07" 6969696.0 0.0
  ; flat "2025-02-08" 7979573.0 0.00001
  ; flat "2025-02-09" 7979573.0 0.0
  ; flat "2025-02-10" 7979573.0 0.0
  ; flat "2025-02-11" 7979573.0 0.0
  ; flat "2025-02-12" 7979573.0 0.0
  ; flat "2025-02-13" 7979573.0 0.0
  ]
;;

let real_bars () =
  [ mk_bar
      ~date:"2025-02-14"
      ~open_:240000.0
      ~high:240000.0
      ~low:96000.0
      ~close:97578.0
      ~volume:145.31151
  ; mk_bar
      ~date:"2025-02-15"
      ~open_:97578.0
      ~high:97900.0
      ~low:97200.0
      ~close:97500.0
      ~volume:150.0
  ; mk_bar
      ~date:"2025-02-16"
      ~open_:97500.0
      ~high:97100.0
      ~low:96600.0
      ~close:96900.0
      ~volume:148.0
  ; mk_bar
      ~date:"2025-02-17"
      ~open_:96900.0
      ~high:96800.0
      ~low:96300.0
      ~close:96600.0
      ~volume:140.0
  ]
;;

let test_normalize_drops_fabricated_placeholders () =
  let clean, dropped, clamped =
    Dio_oracle.Oracle_calendar.normalize_bars (placeholder_bars () @ real_bars ())
  in
  Alcotest.(check int) "11 fabricated candles dropped" 11 dropped;
  Alcotest.(check int) "1 absurd-extreme row clamped" 1 clamped;
  Alcotest.(check int) "4 real rows survive" 4 (Array.length clean);
  let first = clean.(0) in
  Alcotest.(check string)
    "first survivor is the first real trading day"
    "2025-02-14"
    first.date;
  (* The 240,000 open/high never traded: folded into the real close. *)
  Alcotest.(check (float 1e-9)) "clamped row is flat at its close" 97578.0 first.open_;
  Alcotest.(check (float 1e-9)) "clamped high" 97578.0 first.high;
  Alcotest.(check (float 1e-9)) "clamped low" 97578.0 first.low;
  Alcotest.(check (float 1e-9)) "close preserved" 97578.0 first.close;
  let dates = Array.map (fun b -> b.Dio_oracle.Oracle_types.date) clean in
  Alcotest.(check (array string))
    "ascending dates"
    [| "2025-02-14"; "2025-02-15"; "2025-02-16"; "2025-02-17" |]
    dates;
  (* The phantom $7.98M peak is gone: the drawdown the oracle would now
     report is the fixture's mild drift, not the fabricated 99.3%. *)
  let series =
    Dio_oracle.Oracle_types.
      { symbol = "BTC/USDC"; calendar_kind = Crypto; bars = clean; gaps = [] }
  in
  let refs =
    Option.get (Dio_oracle.Oracle_core.references_of ~bars:series.bars)
  in
  (* The close-peak -> subsequent-low drawdown of the mild drift fixture. *)
  Alcotest.(check bool) "no phantom 99% drawdown" true (refs.max_drawdown_pct < 0.05)
;;

let test_normalize_keeps_carried_zero_volume () =
  (* A zero-volume day whose price is carried near the real market is kept:
     normalization must only remove FABRICATED levels, not dead-but-real
     days (dropping them would fabricate gaps that fail max_gap). *)
  let clean, dropped, clamped =
    Dio_oracle.Oracle_calendar.normalize_bars
      (placeholder_bars ()
       @ real_bars ()
       @ [ mk_bar
             ~date:"2025-02-18"
             ~open_:96600.0
             ~high:96600.0
             ~low:96600.0
             ~close:96600.0
             ~volume:0.0
         ])
  in
  Alcotest.(check int) "placeholders still dropped" 11 dropped;
  Alcotest.(check int) "clamps unchanged" 1 clamped;
  Alcotest.(check int) "carried zero-volume day survives" 5 (Array.length clean);
  Alcotest.(check string)
    "last bar is the carried day"
    "2025-02-18"
    clean.(Array.length clean - 1).Dio_oracle.Oracle_types.date
;;

let test_normalize_garbage_only_series_empties () =
  (* A series that is ALL fabricated placeholders normalizes to nothing:
     an empty history is INACTIVE - a garbage decision would be worse. *)
  let clean, dropped, _ =
    Dio_oracle.Oracle_calendar.normalize_bars (placeholder_bars ())
  in
  Alcotest.(check int) "all 11 dropped" 11 dropped;
  Alcotest.(check int) "series is empty" 0 (Array.length clean)
;;

let test_normalize_real_series_untouched () =
  (* A clean liquid series (flat rows, mild drift) must pass through
     untouched: no drops, no clamps. *)
  let mk i =
    let date = Printf.sprintf "2026-0%d-%02d" ((i / 28) + 1) ((i mod 28) + 1) in
    let close = 60000.0 +. (float_of_int i *. 10.0) in
    mk_bar
      ~date
      ~open_:close
      ~high:(close +. 50.0)
      ~low:(close -. 50.0)
      ~close
      ~volume:100.0
  in
  let clean, dropped, clamped =
    Dio_oracle.Oracle_calendar.normalize_bars (List.init 56 mk)
  in
  Alcotest.(check int) "nothing dropped" 0 dropped;
  Alcotest.(check int) "nothing clamped" 0 clamped;
  Alcotest.(check int) "all rows survive" 56 (Array.length clean)
;;

let test_normalize_long_horizon_100x_kept () =
  (* Regression: a series that genuinely appreciated ~8000x (BTC ~$1k in
     2017 vs ~$40k+ median; ETH ~$90 in late 2018) must KEEP its early
     cheap-era rows. The outlier judge is local (nearest real-trading
     neighbor), never a global median. *)
  let rows = ref [] in
  for year = 2015 to 2026 do
    for month = 1 to 12 do
      let date = Printf.sprintf "%04d-%02d-15" year month in
      let close =
        0.5 *. Float.pow 2.0 (float_of_int (((year - 2015) * 12) + month - 1) /. 8.0)
      in
      rows
      := mk_bar
           ~date
           ~open_:close
           ~high:(close *. 1.02)
           ~low:(close *. 0.98)
           ~close
           ~volume:100.0
         :: !rows
    done
  done;
  let clean, dropped, clamped =
    Dio_oracle.Oracle_calendar.normalize_bars (List.rev !rows)
  in
  Alcotest.(check int) "no cheap-era rows dropped" 0 dropped;
  Alcotest.(check int) "no clamps" 0 clamped;
  Alcotest.(check int) "all 144 rows survive" 144 (Array.length clean);
  Alcotest.(check (float 1e-9))
    "first bar is the 2015 cheap era"
    0.5
    clean.(0).Dio_oracle.Oracle_types.close
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
    ; ( "source normalization"
      , [ Alcotest.test_case
            "fabricated pre-listing placeholders are dropped"
            `Quick
            test_normalize_drops_fabricated_placeholders
        ; Alcotest.test_case
            "carried zero-volume days are kept"
            `Quick
            test_normalize_keeps_carried_zero_volume
        ; Alcotest.test_case
            "all-placeholder series empties to INACTIVE"
            `Quick
            test_normalize_garbage_only_series_empties
        ; Alcotest.test_case
            "clean series passes through untouched"
            `Quick
            test_normalize_real_series_untouched
        ; Alcotest.test_case
            "100x long-horizon cheap era is kept"
            `Quick
            test_normalize_long_horizon_100x_kept
        ] )
    ]
;;
