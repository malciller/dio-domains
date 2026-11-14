let l2book_json ~coin ~bid ~ask =
  Printf.sprintf
    "{\"channel\":\"l2Book\",\"data\":{\"coin\":\"%s\",\"levels\":[[{\"px\":\"%.4f\",\"sz\":\"1.5\"}],[{\"px\":\"%.4f\",\"sz\":\"2.0\"}]]}}"
    coin
    bid
    ask
;;

let check_book ~symbol ~expected_bid =
  match Hyperliquid.Orderbook_feed.get_best_bid_ask symbol with
  | Some (bid_px, _, _, _) ->
    Alcotest.(check (float 0.000001)) (symbol ^ " bid") expected_bid bid_px
  | None -> Alcotest.fail ("no book for " ^ symbol)
;;

let test_process_market_data () =
  (* Register the symbol so market-data processing accepts it. *)
  Hyperliquid.Instruments_feed.initialize [ "HYPE" ];
  Hyperliquid.Orderbook_feed.initialize [ "HYPE" ];
  let raw_json_str =
    "{\"channel\":\"l2Book\",\"data\":{\"coin\":\"HYPE\",\"levels\":[[{\"px\":\"100.5\",\"sz\":\"1.5\"}],[{\"px\":\"101.0\",\"sz\":\"2.0\"}]]}}"
  in
  Hyperliquid.Orderbook_feed.process_raw_market_data raw_json_str;
  let best_opt = Hyperliquid.Orderbook_feed.get_best_bid_ask "HYPE" in
  match best_opt with
  | Some (bid_px, bid_sz, ask_px, ask_sz) ->
    Alcotest.(check (float 0.000001)) "bid_px" 100.5 bid_px;
    Alcotest.(check (float 0.000001)) "bid_sz" 1.5 bid_sz;
    Alcotest.(check (float 0.000001)) "ask_px" 101.0 ask_px;
    Alcotest.(check (float 0.000001)) "ask_sz" 2.0 ask_sz
  | None -> Alcotest.fail "Orderbook update not processed"
;;

(* A perp tick and a spot tick are different markets and must not share a store. *)
let test_spot_and_perp_stay_separate () =
  Hyperliquid.Instruments_feed.initialize [ "RT"; "RT/USDC" ];
  Hyperliquid.Orderbook_feed.initialize [ "RT"; "RT/USDC" ];
  Hyperliquid.Orderbook_feed.process_raw_market_data
    (l2book_json ~coin:"RT" ~bid:10.0 ~ask:10.1);
  check_book ~symbol:"RT" ~expected_bid:10.0;
  Alcotest.(check bool)
    "perp tick does not reach the spot pair"
    true
    (Option.is_none (Hyperliquid.Orderbook_feed.get_best_bid_ask "RT/USDC"));
  Hyperliquid.Orderbook_feed.process_raw_market_data
    (l2book_json ~coin:"@0" ~bid:20.0 ~ask:20.1);
  check_book ~symbol:"RT/USDC" ~expected_bid:20.0;
  check_book ~symbol:"RT" ~expected_bid:10.0
;;

(* A configured pair the venue has no spot listing for is an explicit alias of its base
   perpetual (controller symbols are pair-form even when the market is a perp). *)
let test_unlisted_pair_aliases_its_perp () =
  Hyperliquid.Instruments_feed.initialize [ "ALIAS" ];
  Hyperliquid.Orderbook_feed.initialize [ "ALIAS"; "ALIAS/USDC" ];
  Hyperliquid.Orderbook_feed.process_raw_market_data
    (l2book_json ~coin:"ALIAS" ~bid:30.0 ~ask:30.1);
  check_book ~symbol:"ALIAS" ~expected_bid:30.0;
  check_book ~symbol:"ALIAS/USDC" ~expected_bid:30.0
;;

(* A bbo touch is the venue's low-latency feed: it must price the book and advance the
   revision the decision loop polls, without entering the snapshot replay ring. *)
let test_bbo_updates_touch () =
  Hyperliquid.Instruments_feed.initialize [ "BBOT" ];
  Hyperliquid.Orderbook_feed.initialize [ "BBOT" ];
  Hyperliquid.Orderbook_feed.process_raw_market_data
    "{\"channel\":\"bbo\",\"data\":{\"coin\":\"BBOT\",\"time\":1794681971890,\"bbo\":[{\"px\":\"11.0\",\"sz\":\"1.5\",\"n\":2},{\"px\":\"11.1\",\"sz\":\"2.0\",\"n\":3}]}}";
  check_book ~symbol:"BBOT" ~expected_bid:11.0;
  Alcotest.(check int)
    "revision advanced"
    1
    ((Hyperliquid.Orderbook_feed.get_current_position_fast "BBOT") ());
  Alcotest.(check int)
    "ring untouched by a touch frame"
    0
    (Hyperliquid.Orderbook_feed.get_current_position "BBOT")
;;

(* Builder-dex meta names its coins [dex:coin]; the cache key must match. *)
let test_builder_dex_symbol_is_canonical () =
  Hyperliquid.Instruments_feed.process_builder_dex_meta
    ~dex_index:9
    ~dex_name:"bdxtest"
    (`Assoc
      [ ( "universe"
        , `List
            [ `Assoc
                [ "name", `String "bdxtest:SPCX"
                ; "szDecimals", `Int 2
                ; "maxLeverage", `Int 20
                ]
            ; `Assoc
                [ "name", `String "BARE"; "szDecimals", `Int 2; "maxLeverage", `Int 10 ]
            ] )
      ]);
  (match Hyperliquid.Instruments_feed.lookup_market "bdxtest:SPCX" with
   | Some (Hyperliquid.Instruments_feed.Perp, coin) ->
     Alcotest.(check string) "prefixed builder coin" "bdxtest:SPCX" coin
   | _ -> Alcotest.fail "bdxtest:SPCX should resolve as a builder perp");
  match Hyperliquid.Instruments_feed.lookup_market "bdxtest:BARE" with
  | Some (Hyperliquid.Instruments_feed.Perp, coin) ->
    Alcotest.(check string) "bare builder coin gets the prefix" "bdxtest:BARE" coin
  | _ -> Alcotest.fail "bdxtest:BARE should resolve as a builder perp"
;;

(* A builder perp resolves from its name even before the meta fetch lands. *)
let test_builder_dex_resolves_without_cache () =
  match Hyperliquid.Instruments_feed.lookup_market "nocache:THING" with
  | Some (Hyperliquid.Instruments_feed.Perp, coin) ->
    Alcotest.(check string) "channel identifier is the name" "nocache:THING" coin
  | _ -> Alcotest.fail "a [dex:coin] name should resolve without the cache"
;;

let () =
  Alcotest.run
    "Hyperliquid Orderbook Feed"
    [ ( "processing"
      , [ Alcotest.test_case "process_market_data" `Quick test_process_market_data ] )
    ; ( "routing"
      , [ Alcotest.test_case
            "spot and perp stay separate"
            `Quick
            test_spot_and_perp_stay_separate
        ; Alcotest.test_case
            "unlisted pair aliases its perp"
            `Quick
            test_unlisted_pair_aliases_its_perp
        ; Alcotest.test_case "bbo updates touch" `Quick test_bbo_updates_touch
        ; Alcotest.test_case
            "builder dex symbol is canonical"
            `Quick
            test_builder_dex_symbol_is_canonical
        ; Alcotest.test_case
            "builder dex resolves without cache"
            `Quick
            test_builder_dex_resolves_without_cache
        ] )
    ]
;;
