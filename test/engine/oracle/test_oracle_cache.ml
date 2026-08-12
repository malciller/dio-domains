(* Tests for the disk-persisted history cache (Oracle_cache): save/load
   roundtrip, freshness policy, delta-fetch boundary, merge normalization
   and failure fallback. No network; file IO goes to a temp dir. *)

let mk_bar ~date ~close ~(volume : float) =
  Dio_oracle.Oracle_types.
    { date; open_ = close; high = close; low = close; close; volume }
;;

let with_tmp_dir f =
  let dir = Filename.concat (Filename.get_temp_dir_name ()) "dio_oracle_cache_test" in
  (try Unix.mkdir dir 0o755 with
   | Unix.Unix_error (Unix.EEXIST, _, _) -> ());
  Fun.protect
    ~finally:(fun () ->
      try Unix.system ("rm -rf " ^ Filename.quote dir) |> ignore with
      | _ -> ())
    (fun () -> f dir)
;;

let test_save_load_roundtrip () =
  with_tmp_dir (fun dir ->
    let bars =
      [ mk_bar ~date:"2025-01-01" ~close:10.0 ~volume:1.0
      ; mk_bar ~date:"2025-01-02" ~close:11.0 ~volume:2.0
      ]
    in
    Dio_oracle.Oracle_cache.save_bars ~dir ~exchange:"kraken" ~symbol:"XMR/USD" bars;
    let loaded =
      Dio_oracle.Oracle_cache.load_bars ~dir ~exchange:"kraken" ~symbol:"XMR/USD"
    in
    Alcotest.(check (list (float 1e-9)))
      "roundtrip closes"
      [ 10.0; 11.0 ]
      (List.map (fun b -> b.Dio_oracle.Oracle_types.close) loaded);
    (* Missing / other-symbol files load as empty. *)
    Alcotest.(check int)
      "unknown symbol is empty"
      0
      (List.length
         (Dio_oracle.Oracle_cache.load_bars ~dir ~exchange:"kraken" ~symbol:"NOPE")))
;;

let test_ms_of_iso () =
  Alcotest.(check int64)
    "2022-01-01 epoch ms"
    1640995200000L
    (Dio_oracle.Oracle_cache.ms_of_iso "2022-01-01");
  Alcotest.(check int64)
    "1970-01-01 is zero"
    0L
    (Dio_oracle.Oracle_cache.ms_of_iso "1970-01-01");
  Alcotest.(check int64)
    "unix seconds"
    1640995200L
    (Dio_oracle.Oracle_cache.unix_of_iso "2022-01-01")
;;

let test_is_fresh () =
  let fresh_today =
    [ mk_bar ~date:"2026-08-11" ~close:1.0 ~volume:1.0
    ; mk_bar ~date:"2026-08-12" ~close:1.1 ~volume:1.0
    ]
  in
  Alcotest.(check bool)
    "last bar today is fresh"
    true
    (Dio_oracle.Oracle_cache.is_fresh ~today:"2026-08-12" fresh_today);
  let fresh_yesterday = [ mk_bar ~date:"2026-08-11" ~close:1.0 ~volume:1.0 ] in
  Alcotest.(check bool)
    "last bar yesterday is fresh"
    true
    (Dio_oracle.Oracle_cache.is_fresh ~today:"2026-08-12" fresh_yesterday);
  let stale = [ mk_bar ~date:"2026-08-10" ~close:1.0 ~volume:1.0 ] in
  Alcotest.(check bool)
    "last bar two days back is stale"
    false
    (Dio_oracle.Oracle_cache.is_fresh ~today:"2026-08-12" stale);
  Alcotest.(check bool)
    "empty cache is never fresh"
    false
    (Dio_oracle.Oracle_cache.is_fresh ~today:"2026-08-12" [])
;;

let test_merge_bars () =
  (* Merge is RAW (the cache is the source truth; normalization happens on
     read): a revised current-day bar replaces the cached one (dedup keeps
     last), order is ascending, and nothing is dropped here. *)
  let cached =
    [ mk_bar ~date:"2025-02-14" ~close:97578.0 ~volume:145.0
    ; mk_bar ~date:"2025-02-15" ~close:97500.0 ~volume:150.0
    ]
  in
  let fresh =
    [ mk_bar ~date:"2025-02-15" ~close:97200.0 ~volume:160.0
    ; mk_bar ~date:"2025-02-16" ~close:96900.0 ~volume:148.0
    ; Dio_oracle.Oracle_types.
        { date = "2025-02-17"
        ; open_ = 6969696.0
        ; high = 6969696.0
        ; low = 6969696.0
        ; close = 6969696.0
        ; volume = 0.0
        }
    ]
  in
  let merged = Dio_oracle.Oracle_cache.merge_bars cached fresh in
  Alcotest.(check (list (float 1e-9)))
    "raw merge: revised bar wins, ascending, placeholder kept"
    [ 97578.0; 97200.0; 96900.0; 6969696.0 ]
    (List.map (fun b -> b.Dio_oracle.Oracle_types.close) merged)
;;

let test_with_delta_returns_clean_view_but_stores_raw () =
  (* with_delta serves the clean series (fabricated rows dropped) while the
     cache file stores the raw bars - so a corrected normalization rule
     self-heals without a refetch. *)
  with_tmp_dir (fun dir ->
    let bars =
      [ mk_bar ~date:"2025-02-14" ~close:97578.0 ~volume:145.0
      ; Dio_oracle.Oracle_types.
          { date = "2025-02-15"
          ; open_ = 6969696.0
          ; high = 6969696.0
          ; low = 6969696.0
          ; close = 6969696.0
          ; volume = 0.0
          }
      ]
    in
    let result =
      Lwt_main.run
        (Dio_oracle.Oracle_cache.with_delta
           ~dir
           ~exchange:"hl"
           ~symbol:"BTC/USDC"
           ~today:"2025-02-15"
           ~fetch:(fun _ -> Lwt.return bars)
           ())
    in
    Alcotest.(check (list (float 1e-9)))
      "served series is clean (placeholder dropped)"
      [ 97578.0 ]
      (List.map (fun b -> b.Dio_oracle.Oracle_types.close) result);
    let stored =
      Dio_oracle.Oracle_cache.load_bars ~dir ~exchange:"hl" ~symbol:"BTC/USDC"
    in
    Alcotest.(check int) "file stores raw bars" 2 (List.length stored))
;;

let test_with_delta_fresh_skips_network () =
  with_tmp_dir (fun dir ->
    let cached = [ mk_bar ~date:"2026-08-11" ~close:1.0 ~volume:1.0 ] in
    Dio_oracle.Oracle_cache.save_bars ~dir ~exchange:"hl" ~symbol:"BTC/USDC" cached;
    let called = ref false in
    let fetch _ =
      called := true;
      Lwt.return []
    in
    Lwt_main.run
      (Dio_oracle.Oracle_cache.with_delta
         ~dir
         ~exchange:"hl"
         ~symbol:"BTC/USDC"
         ~today:"2026-08-12"
         ~fetch
         ())
    |> ignore;
    Alcotest.(check bool) "fresh cache never touches the network" false !called)
;;

let test_with_delta_stale_fetches_boundary_and_persists () =
  with_tmp_dir (fun dir ->
    let cached = [ mk_bar ~date:"2026-08-10" ~close:1.0 ~volume:1.0 ] in
    Dio_oracle.Oracle_cache.save_bars ~dir ~exchange:"hl" ~symbol:"BTC/USDC" cached;
    let fetched_boundary = ref None in
    let fetch boundary =
      fetched_boundary := boundary;
      Lwt.return
        [ mk_bar ~date:"2026-08-11" ~close:1.05 ~volume:1.0
        ; mk_bar ~date:"2026-08-12" ~close:1.1 ~volume:1.0
        ]
    in
    let result =
      Lwt_main.run
        (Dio_oracle.Oracle_cache.with_delta
           ~dir
           ~exchange:"hl"
           ~symbol:"BTC/USDC"
           ~today:"2026-08-12"
           ~fetch
           ())
    in
    (* Boundary is the day AFTER the last cached bar (exclusive start). *)
    Alcotest.(check (option string))
      "delta boundary"
      (Some "2026-08-11")
      !fetched_boundary;
    Alcotest.(check (list (float 1e-9)))
      "merged result"
      [ 1.0; 1.05; 1.1 ]
      (List.map (fun b -> b.Dio_oracle.Oracle_types.close) result);
    (* Merged history is persisted for the next pass. *)
    let loaded =
      Dio_oracle.Oracle_cache.load_bars ~dir ~exchange:"hl" ~symbol:"BTC/USDC"
    in
    Alcotest.(check int) "persisted 3 bars" 3 (List.length loaded);
    (* And now fresh: the next call skips the network. *)
    let called = ref false in
    Lwt_main.run
      (Dio_oracle.Oracle_cache.with_delta
         ~dir
         ~exchange:"hl"
         ~symbol:"BTC/USDC"
         ~today:"2026-08-12"
         ~fetch:(fun _ ->
           called := true;
           Lwt.return [])
         ())
    |> ignore;
    Alcotest.(check bool) "now fresh, no fetch" false !called)
;;

let test_with_delta_cold_start_fetches_full () =
  with_tmp_dir (fun dir ->
    let fetched_boundary = ref (Some "unset") in
    let fetch boundary =
      fetched_boundary := boundary;
      Lwt.return [ mk_bar ~date:"2025-01-01" ~close:10.0 ~volume:1.0 ]
    in
    let result =
      Lwt_main.run
        (Dio_oracle.Oracle_cache.with_delta
           ~dir
           ~exchange:"kraken"
           ~symbol:"XMR/USD"
           ~today:"2026-08-12"
           ~fetch
           ())
    in
    Alcotest.(check (option string)) "no cache -> full fetch" None !fetched_boundary;
    Alcotest.(check int) "one bar fetched" 1 (List.length result))
;;

let test_with_delta_complete_through () =
  (* Bounded histories (Yahoo deep) are complete once their last bar reaches
     the end date - however far "today" has moved - and are never re-fetched
     with a start past their end. *)
  with_tmp_dir (fun dir ->
    let cached =
      [ mk_bar ~date:"2020-08-30" ~close:1.0 ~volume:1.0
      ; mk_bar ~date:"2020-08-31" ~close:1.0 ~volume:1.0
      ]
    in
    Dio_oracle.Oracle_cache.save_bars ~dir ~exchange:"yahoo-deep" ~symbol:"QQQ" cached;
    let called = ref false in
    let result =
      Lwt_main.run
        (Dio_oracle.Oracle_cache.with_delta
           ~dir
           ~exchange:"yahoo-deep"
           ~symbol:"QQQ"
           ~today:"2026-08-12"
           ~complete_through:"2020-08-31"
           ~fetch:(fun _ ->
             called := true;
             Lwt.return [])
           ())
    in
    Alcotest.(check bool) "complete-through cache skips the network" false !called;
    Alcotest.(check int) "serves the cached bars" 2 (List.length result);
    (* Incomplete cache (last bar far before the end date): fetches the
       delta with the day AFTER the last bar as the start boundary. *)
    let called2 = ref false in
    let boundary2 = ref None in
    let incomplete = [ mk_bar ~date:"2020-08-01" ~close:1.0 ~volume:1.0 ] in
    Dio_oracle.Oracle_cache.save_bars ~dir ~exchange:"yahoo-deep" ~symbol:"QQQ" incomplete;
    let result2 =
      Lwt_main.run
        (Dio_oracle.Oracle_cache.with_delta
           ~dir
           ~exchange:"yahoo-deep"
           ~symbol:"QQQ"
           ~today:"2026-08-12"
           ~complete_through:"2020-08-31"
           ~fetch:(fun boundary ->
             called2 := true;
             boundary2 := boundary;
             Lwt.return [ mk_bar ~date:"2020-08-02" ~close:1.0 ~volume:1.0 ])
           ())
    in
    Alcotest.(check bool) "incomplete cache fetches" true !called2;
    Alcotest.(check (option string)) "delta boundary" (Some "2020-08-02") !boundary2;
    Alcotest.(check int) "merged" 2 (List.length result2))
;;

let test_with_delta_complete_through_weekend () =
  (* Equity deep histories: the bounded end date is venue_first - 1, which
     often lands on a weekend/holiday (venue starts Monday -> end Sunday),
     and the last trading day is the Friday before. The cache must count as
     complete there - an exact-date match would re-request a weekend-only
     sliver (no trading days at all) on every pass forever. *)
  with_tmp_dir (fun dir ->
    (* Venue starts Tue 2020-09-01 -> deep end_date Mon 2020-08-31; the
         last cached (trading) bar is Friday 2020-08-28. *)
    let cached = [ mk_bar ~date:"2020-08-28" ~close:1.0 ~volume:1.0 ] in
    Dio_oracle.Oracle_cache.save_bars ~dir ~exchange:"yahoo-deep" ~symbol:"QQQ" cached;
    let called = ref false in
    let result =
      Lwt_main.run
        (Dio_oracle.Oracle_cache.with_delta
           ~dir
           ~exchange:"yahoo-deep"
           ~symbol:"QQQ"
           ~today:"2026-08-12"
           ~complete_through:"2020-08-31"
           ~fetch:(fun _ ->
             called := true;
             Lwt.return [])
           ())
    in
    Alcotest.(check bool) "weekend-bounded equity deep is complete" false !called;
    Alcotest.(check int) "serves the cached bars" 1 (List.length result))
;;

let test_with_delta_fetch_failure_falls_back () =
  with_tmp_dir (fun dir ->
    let cached = [ mk_bar ~date:"2026-08-09" ~close:1.0 ~volume:1.0 ] in
    Dio_oracle.Oracle_cache.save_bars ~dir ~exchange:"hl" ~symbol:"BTC/USDC" cached;
    let fetch _ = Lwt.fail (Failure "network down") in
    let result =
      Lwt_main.run
        (Dio_oracle.Oracle_cache.with_delta
           ~dir
           ~exchange:"hl"
           ~symbol:"BTC/USDC"
           ~today:"2026-08-12"
           ~fetch
           ())
    in
    (* Stale but real beats nothing: cached history is returned. *)
    Alcotest.(check (list (float 1e-9)))
      "stale fallback"
      [ 1.0 ]
      (List.map (fun b -> b.Dio_oracle.Oracle_types.close) result))
;;

let () =
  Alcotest.run
    "oracle_cache"
    [ ( "persistence"
      , [ Alcotest.test_case "save/load roundtrip" `Quick test_save_load_roundtrip
        ; Alcotest.test_case "date helpers" `Quick test_ms_of_iso
        ; Alcotest.test_case "freshness policy" `Quick test_is_fresh
        ; Alcotest.test_case "merge dedups + normalizes" `Quick test_merge_bars
        ] )
    ; ( "delta policy"
      , [ Alcotest.test_case
            "fresh cache skips the network"
            `Quick
            test_with_delta_fresh_skips_network
        ; Alcotest.test_case
            "stale cache fetches the delta and persists"
            `Quick
            test_with_delta_stale_fetches_boundary_and_persists
        ; Alcotest.test_case
            "cold start fetches full history"
            `Quick
            test_with_delta_cold_start_fetches_full
        ; Alcotest.test_case
            "bounded cache completes through its end date"
            `Quick
            test_with_delta_complete_through
        ; Alcotest.test_case
            "weekend-bounded equity deep is complete"
            `Quick
            test_with_delta_complete_through_weekend
        ; Alcotest.test_case
            "served series is clean, file stores raw"
            `Quick
            test_with_delta_returns_clean_view_but_stores_raw
        ; Alcotest.test_case
            "failed delta falls back to cached history"
            `Quick
            test_with_delta_fetch_failure_falls_back
        ] )
    ]
;;
