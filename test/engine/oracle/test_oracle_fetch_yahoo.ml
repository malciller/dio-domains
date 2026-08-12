(* Oracle_fetch_yahoo tests: deep-history symbol whitelist, chart parsing,
   and venue/deep merge.

   The whitelist is the safety-critical part: Yahoo's crypto symbol space
   carries dead-token collisions (HYPE-USD still serves a dead 2021 token's
   prices), so only known-continuous pairs may be deepened. Equities are
   unambiguous and map by identity. *)

open Dio_oracle

let check_symbol ~exchange symbol expected =
  Alcotest.(check (option string))
    (Printf.sprintf "symbol_of %s/%s" exchange symbol)
    expected
    (Oracle_fetch_yahoo.symbol_of ~exchange symbol)
;;

let test_symbol_whitelist () =
  check_symbol ~exchange:"kraken" "ETH/USD" (Some "ETH-USD");
  check_symbol ~exchange:"hyperliquid" "BTC/USDC" (Some "BTC-USD");
  check_symbol ~exchange:"kraken" "SOL/USD" (Some "SOL-USD");
  check_symbol ~exchange:"kraken" "XMR/USD" (Some "XMR-USD");
  check_symbol ~exchange:"kraken" "DOGE/USD" (Some "DOGE-USD");
  check_symbol ~exchange:"hyperliquid" "ADA/USDC" (Some "ADA-USD");
  check_symbol ~exchange:"kraken" "LTC/USD" (Some "LTC-USD");
  check_symbol ~exchange:"kraken" "XRP/USD" (Some "XRP-USD");
  check_symbol ~exchange:"kraken" "LINK/USD" (Some "LINK-USD");
  check_symbol ~exchange:"kraken" "AVAX/USD" (Some "AVAX-USD");
  check_symbol ~exchange:"kraken" "DOT/USD" (Some "DOT-USD");
  (* The dead-token trap: HYPE/USDC must never be deepened from Yahoo. *)
  check_symbol ~exchange:"hyperliquid" "HYPE/USDC" None;
  (* Equities map by identity (Yahoo QQQ is QQQ). *)
  check_symbol ~exchange:"alpaca" "QQQ" (Some "QQQ");
  check_symbol ~exchange:"alpaca" "SPCX" (Some "SPCX");
  check_symbol ~exchange:"alpaca" "NVDA" (Some "NVDA")
;;

(** Minimal chart fixture: two days of data with one null row dropped. *)
let fixture_json =
  `Assoc
    [ ( "chart"
      , `Assoc
          [ ( "result"
            , `List
                [ `Assoc
                    [ ( "timestamp"
                      , `List
                          [ `Int 1_700_000_000; `Int 1_700_086_400; `Int 1_700_172_800 ] )
                    ; ( "indicators"
                      , `Assoc
                          [ ( "quote"
                            , `List
                                [ `Assoc
                                    [ "open", `List [ `Float 100.0; `Null; `Float 102.0 ]
                                    ; "high", `List [ `Float 101.0; `Null; `Float 103.0 ]
                                    ; "low", `List [ `Float 99.0; `Null; `Float 101.0 ]
                                    ; "close", `List [ `Float 100.5; `Null; `Float 102.5 ]
                                    ; "volume", `List [ `Int 1000; `Null; `Int 1200 ]
                                    ]
                                ] )
                          ] )
                    ]
                ] )
          ] )
    ]
;;

let test_parse_daily () =
  let bars = Oracle_fetch_yahoo.parse_daily ~symbol:"ETH-USD" fixture_json in
  Alcotest.(check int) "null rows dropped" 2 (List.length bars);
  match bars with
  | [ first; second ] ->
    Alcotest.(check bool)
      "ascending"
      (first.Oracle_types.date < second.Oracle_types.date)
      true;
    Alcotest.(check bool) "has high/low/close" (first.high >= first.low) true;
    Alcotest.(check (float 1e-9)) "volume kept" 1000.0 first.volume
  | _ -> Alcotest.fail "expected two bars"
;;

let bar ~date ~close =
  { Oracle_types.date; open_ = close; high = close; low = close; close; volume = 0.0 }
;;

let test_merge_series () =
  let venue =
    { Oracle_types.symbol = "ETH/USD"
    ; calendar_kind = Oracle_types.Crypto
    ; bars =
        [| bar ~date:"2024-08-21" ~close:2600.0; bar ~date:"2024-08-22" ~close:2650.0 |]
    ; gaps = []
    }
  in
  (* Deep bars strictly before the venue start are prepended; an overlapping
     date stays with the venue. *)
  let deep =
    { Oracle_types.symbol = "ETH-USD"
    ; calendar_kind = Oracle_types.Crypto
    ; bars =
        [| bar ~date:"2024-08-19" ~close:2550.0
         ; bar ~date:"2024-08-20" ~close:2570.0
         ; bar ~date:"2024-08-21" ~close:9999.0
        |]
    ; gaps = []
    }
  in
  let merged, added = Oracle_fetch_yahoo.merge_series ~venue ~deep in
  Alcotest.(check int) "two deep bars added" 2 added;
  Alcotest.(check int) "merged length" 4 (Array.length merged.bars);
  (* Venue bar wins on the overlap date. *)
  let overlap =
    Array.to_list merged.bars
    |> List.find (fun (b : Oracle_types.bar) -> b.date = "2024-08-21")
  in
  Alcotest.(check (float 1e-9)) "venue wins overlap" 2600.0 overlap.close;
  (* No deep bars before the venue start -> unchanged. *)
  let empty_deep = { deep with bars = [||] } in
  let merged2, added2 = Oracle_fetch_yahoo.merge_series ~venue ~deep:empty_deep in
  Alcotest.(check int) "no deep bars" 0 added2;
  Alcotest.(check int) "unchanged" 2 (Array.length merged2.bars);
  (* A DESCENDING venue series (some venue feeds return newest-first) must
     merge the same way: the venue start is its minimum date. *)
  let desc_venue =
    { venue with bars = Array.of_list (Array.to_list venue.bars |> List.rev) }
  in
  let merged3, added3 = Oracle_fetch_yahoo.merge_series ~venue:desc_venue ~deep in
  Alcotest.(check int) "descending venue adds same deep bars" 2 added3;
  Alcotest.(check int) "descending venue merged length" 4 (Array.length merged3.bars);
  let overlap3 =
    Array.to_list merged3.bars
    |> List.find (fun (b : Oracle_types.bar) -> b.date = "2024-08-21")
  in
  Alcotest.(check (float 1e-9)) "descending venue wins overlap" 2600.0 overlap3.close
;;

let test_classify_error () =
  (* Yahoo's pre-listing answer (HTTP 400, "Data doesn't exist for
     startDate = ...") is an EMPTY RANGE, not a failure: the walk skips it
     instead of aborting (the SPCX spam fix - a recently-listed asset must
     not re-request the same doomed range on every pass). *)
  Alcotest.(check bool)
    "400 + data-doesn't-exist = missing data"
    (Oracle_fetch_yahoo.classify_error
       400
       "{\"chart\":{\"result\":null,\"error\":{\"code\":\"Bad \
        Request\",\"description\":\"Data doesn't exist for startDate = 1420088400, \
        endDate = 1781150400\"}}}"
     = `Missing_data)
    true;
  (* Case-insensitive match. *)
  Alcotest.(check bool)
    "lowercase body matches"
    (Oracle_fetch_yahoo.classify_error
       400
       "{\"chart\":{\"result\":null,\"error\":{\"description\":\"data doesn't exist\"}}}"
     = `Missing_data)
    true;
  (* Any other error is fatal. *)
  Alcotest.(check bool)
    "401 = fatal"
    (Oracle_fetch_yahoo.classify_error 401 "{\"error\":\"Unauthorized\"}" = `Fatal)
    true;
  Alcotest.(check bool)
    "400 without the marker = fatal"
    (Oracle_fetch_yahoo.classify_error 400 "{\"error\":\"Invalid Crumb\"}" = `Fatal)
    true;
  Alcotest.(check bool)
    "500 = fatal"
    (Oracle_fetch_yahoo.classify_error 500 "oops" = `Fatal)
    true
;;

let test_empty_prefix_cache () =
  (* The confirmed-empty prefix is cached per symbol: a fetch whose whole
     requested range sits before the known listing is answered locally with
     zero bars and zero HTTP requests (the pre-listing dates are never
     re-requested). *)
  let symbol = "SPCX" in
  (* Simulate the first pass: the walk recorded "no data before 2026-06-15". *)
  Oracle_fetch_yahoo.remember_empty ~symbol "2026-06-15";
  (match Oracle_fetch_yahoo.known_empty_before ~symbol with
   | Some d -> Alcotest.(check string) "floor cached" "2026-06-15" d
   | None -> Alcotest.fail "expected the cached empty prefix");
  (* The prefix only grows forward: a later, deeper empty answer is kept. *)
  Oracle_fetch_yahoo.remember_empty ~symbol "2026-06-01";
  (match Oracle_fetch_yahoo.known_empty_before ~symbol with
   | Some d -> Alcotest.(check string) "floor does not shrink" "2026-06-15" d
   | None -> Alcotest.fail "expected the cached empty prefix");
  Oracle_fetch_yahoo.remember_empty ~symbol "2026-08-01";
  (match Oracle_fetch_yahoo.known_empty_before ~symbol with
   | Some d -> Alcotest.(check string) "floor extends" "2026-08-01" d
   | None -> Alcotest.fail "expected the cached empty prefix");
  (* Symbols are independent. *)
  Alcotest.(check bool)
    "other symbols unaffected"
    (Oracle_fetch_yahoo.known_empty_before ~symbol:"QQQ" = None)
    true
;;

let test_classify_exn () =
  (* The fetch wraps failures as "Oracle_fetch_yahoo: HTTP <status> for
     <symbol> (<body>)"; classification must dig the status and body out. *)
  Alcotest.(check bool)
    "missing-data failure classified from the message"
    (Oracle_fetch_yahoo.classify_exn
       (Failure
          "Oracle_fetch_yahoo: HTTP 400 for SPCX \
           ({\"chart\":{\"result\":null,\"error\":{\"code\":\"Bad \
           Request\",\"description\":\"Data doesn't exist for startDate = \
           1420088400\"}}})")
     = `Missing_data)
    true;
  Alcotest.(check bool)
    "non-empty-range failure is fatal"
    (Oracle_fetch_yahoo.classify_exn
       (Failure "Oracle_fetch_yahoo: HTTP 503 for QQQ (boom)")
     = `Fatal)
    true;
  Alcotest.(check bool)
    "non-Failure exceptions are fatal"
    (Oracle_fetch_yahoo.classify_exn (Invalid_argument "x") = `Fatal)
    true
;;

let () =
  Alcotest.run
    "oracle_fetch_yahoo"
    [ ( "symbol whitelist"
      , [ Alcotest.test_case
            "known pairs map, dead tokens do not"
            `Quick
            test_symbol_whitelist
        ] )
    ; "parse", [ Alcotest.test_case "chart fixture with nulls" `Quick test_parse_daily ]
    ; ( "merge"
      , [ Alcotest.test_case "deep prepend, venue wins overlap" `Quick test_merge_series ]
      )
    ; ( "pre-listing windows"
      , [ Alcotest.test_case
            "400 data-doesn't-exist classifies as empty"
            `Quick
            test_classify_error
        ; Alcotest.test_case
            "empty prefix cached per symbol (no re-request spam)"
            `Quick
            test_empty_prefix_cache
        ; Alcotest.test_case
            "fetch failure exceptions classify by status+body"
            `Quick
            test_classify_exn
        ] )
    ]
;;
