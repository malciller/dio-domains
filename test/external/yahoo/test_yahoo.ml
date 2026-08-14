(* Yahoo deep-history client tests: symbol whitelist, chart parsing, and
   pre-listing window classification.

   The whitelist is the safety-critical part: Yahoo's crypto symbol space
   carries dead-token collisions (HYPE-USD still serves a dead 2021 token's
   prices), so only known-continuous pairs may be deepened. Equities are
   unambiguous and map by identity. *)

module Exchange = Dio_exchange.Exchange_intf

(* The library [dio.yahoo] wraps its module under the library name [Yahoo]. *)
module Yahoo_deep_history = Yahoo.Yahoo_deep_history

let check_symbol ~calendar_kind symbol expected =
  Alcotest.(check (option string))
    (Printf.sprintf "symbol_of %s" symbol)
    expected
    (Yahoo_deep_history.symbol_of ~calendar_kind symbol)
;;

let test_symbol_whitelist () =
  check_symbol ~calendar_kind:Exchange.Types.Crypto "ETH/USD" (Some "ETH-USD");
  check_symbol ~calendar_kind:Exchange.Types.Crypto "BTC/USDC" (Some "BTC-USD");
  check_symbol ~calendar_kind:Exchange.Types.Crypto "SOL/USD" (Some "SOL-USD");
  check_symbol ~calendar_kind:Exchange.Types.Crypto "XMR/USD" (Some "XMR-USD");
  check_symbol ~calendar_kind:Exchange.Types.Crypto "DOGE/USD" (Some "DOGE-USD");
  check_symbol ~calendar_kind:Exchange.Types.Crypto "ADA/USDC" (Some "ADA-USD");
  check_symbol ~calendar_kind:Exchange.Types.Crypto "LTC/USD" (Some "LTC-USD");
  check_symbol ~calendar_kind:Exchange.Types.Crypto "XRP/USD" (Some "XRP-USD");
  check_symbol ~calendar_kind:Exchange.Types.Crypto "LINK/USD" (Some "LINK-USD");
  check_symbol ~calendar_kind:Exchange.Types.Crypto "AVAX/USD" (Some "AVAX-USD");
  check_symbol ~calendar_kind:Exchange.Types.Crypto "DOT/USD" (Some "DOT-USD");
  (* The dead-token trap: HYPE/USDC must never be deepened from Yahoo. *)
  check_symbol ~calendar_kind:Exchange.Types.Crypto "HYPE/USDC" None;
  (* Equities map by identity (Yahoo QQQ is QQQ). *)
  check_symbol ~calendar_kind:Exchange.Types.Equity "QQQ" (Some "QQQ");
  check_symbol ~calendar_kind:Exchange.Types.Equity "SPCX" (Some "SPCX");
  check_symbol ~calendar_kind:Exchange.Types.Equity "NVDA" (Some "NVDA")
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
  let bars = Yahoo_deep_history.parse_daily ~symbol:"ETH-USD" fixture_json in
  Alcotest.(check int) "null rows dropped" 2 (List.length bars);
  match bars with
  | [ first; second ] ->
    Alcotest.(check bool)
      "ascending"
      (first.Exchange.Types.date < second.Exchange.Types.date)
      true;
    Alcotest.(check bool) "has high/low/close" (first.high >= first.low) true;
    Alcotest.(check (float 1e-9)) "volume kept" 1000.0 first.volume
  | _ -> Alcotest.fail "expected two bars"
;;

let test_classify_error () =
  (* Yahoo's pre-listing answer (HTTP 400, "Data doesn't exist for
     startDate = ...") is an EMPTY RANGE, not a failure: the walk skips it
     instead of aborting (the SPCX spam fix - a recently-listed asset must
     not re-request the same doomed range on every pass). *)
  Alcotest.(check bool)
    "400 + data-doesn't-exist = missing data"
    (Yahoo_deep_history.classify_error
       400
       "{\"chart\":{\"result\":null,\"error\":{\"code\":\"Bad \
        Request\",\"description\":\"Data doesn't exist for startDate = 1420088400, \
        endDate = 1781150400\"}}}"
     = `Missing_data)
    true;
  (* Case-insensitive match. *)
  Alcotest.(check bool)
    "lowercase body matches"
    (Yahoo_deep_history.classify_error
       400
       "{\"chart\":{\"result\":null,\"error\":{\"description\":\"data doesn't exist\"}}}"
     = `Missing_data)
    true;
  (* Any other error is fatal. *)
  Alcotest.(check bool)
    "401 = fatal"
    (Yahoo_deep_history.classify_error 401 "{\"error\":\"Unauthorized\"}" = `Fatal)
    true;
  Alcotest.(check bool)
    "400 without the marker = fatal"
    (Yahoo_deep_history.classify_error 400 "{\"error\":\"Invalid Crumb\"}" = `Fatal)
    true;
  Alcotest.(check bool)
    "500 = fatal"
    (Yahoo_deep_history.classify_error 500 "oops" = `Fatal)
    true
;;

let test_empty_prefix_cache () =
  (* The confirmed-empty prefix is cached per symbol: a fetch whose whole
     requested range sits before the known listing is answered locally with
     zero bars and zero HTTP requests (the pre-listing dates are never
     re-requested). *)
  let symbol = "SPCX" in
  (* Simulate the first pass: the walk recorded "no data before 2026-06-15". *)
  Yahoo_deep_history.remember_empty ~symbol "2026-06-15";
  (match Yahoo_deep_history.known_empty_before ~symbol with
   | Some d -> Alcotest.(check string) "floor cached" "2026-06-15" d
   | None -> Alcotest.fail "expected the cached empty prefix");
  (* The prefix only grows forward: a later, deeper empty answer is kept. *)
  Yahoo_deep_history.remember_empty ~symbol "2026-06-01";
  (match Yahoo_deep_history.known_empty_before ~symbol with
   | Some d -> Alcotest.(check string) "floor does not shrink" "2026-06-15" d
   | None -> Alcotest.fail "expected the cached empty prefix");
  Yahoo_deep_history.remember_empty ~symbol "2026-08-01";
  (match Yahoo_deep_history.known_empty_before ~symbol with
   | Some d -> Alcotest.(check string) "floor extends" "2026-08-01" d
   | None -> Alcotest.fail "expected the cached empty prefix");
  (* Symbols are independent. *)
  Alcotest.(check bool)
    "other symbols unaffected"
    (Yahoo_deep_history.known_empty_before ~symbol:"QQQ" = None)
    true
;;

let test_classify_exn () =
  (* The fetch wraps failures as "Yahoo: HTTP <status> for <symbol> (<body>)";
     classification must dig the status and body out. *)
  Alcotest.(check bool)
    "missing-data failure classified from the message"
    (Yahoo_deep_history.classify_exn
       (Failure
          "Yahoo: HTTP 400 for SPCX \
           ({\"chart\":{\"result\":null,\"error\":{\"code\":\"Bad \
           Request\",\"description\":\"Data doesn't exist for startDate = \
           1420088400\"}}})")
     = `Missing_data)
    true;
  Alcotest.(check bool)
    "non-empty-range failure is fatal"
    (Yahoo_deep_history.classify_exn (Failure "Yahoo: HTTP 503 for QQQ (boom)") = `Fatal)
    true;
  Alcotest.(check bool)
    "non-Failure exceptions are fatal"
    (Yahoo_deep_history.classify_exn (Invalid_argument "x") = `Fatal)
    true
;;

let () =
  Alcotest.run
    "yahoo"
    [ ( "symbol whitelist"
      , [ Alcotest.test_case
            "known pairs map, dead tokens do not"
            `Quick
            test_symbol_whitelist
        ] )
    ; "parse", [ Alcotest.test_case "chart fixture with nulls" `Quick test_parse_daily ]
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
