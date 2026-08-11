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
    ]
;;
