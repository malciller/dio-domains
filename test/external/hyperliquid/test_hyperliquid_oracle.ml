(* Hyperliquid oracle adapter tests: spot/perpetual resolution, candle
   parsing and window ordering. These exercise the pure functions of
   [Hyperliquid.Hyperliquid_oracle] only (no network). Source normalization
   lives centrally in Oracle_calendar.normalize_bars and is tested with the
   oracle calendar suite (test_oracle_calendar.ml). *)

let meta_fixture () =
  (* A spotMeta slice: token index table plus one canonical pair (PURR/USDC)
     and two wrapped pairs exposed as "@N" aliases (BTC spot = UBTC/USDC,
     "@142"; HFUN/USDC, "@1"). *)
  {|{
     "tokens": [
       {"name":"USDC","index":0,"szDecimals":8},
       {"name":"PURR","index":1,"szDecimals":8},
       {"name":"HFUN","index":2,"szDecimals":8},
       {"name":"UBTC","index":197,"szDecimals":8}
     ],
     "universe": [
       {"name":"PURR/USDC","tokens":[1,0],"index":0,"isCanonical":true},
       {"name":"@1","tokens":[2,0],"index":1,"isCanonical":false},
       {"name":"@142","tokens":[197,0],"index":142,"isCanonical":false}
     ]
   }|}
;;

let pairs_fixture () =
  let json = Yojson.Safe.from_string (meta_fixture ()) in
  Hyperliquid.Hyperliquid_oracle.spot_meta_pairs_of_json json
;;

let test_spot_meta_pairs () =
  let pairs = pairs_fixture () in
  Alcotest.(check (list (pair string string)))
    "feed symbol -> candle coin"
    [ "PURR/USDC", "PURR/USDC"; "HFUN/USDC", "@1"; "BTC/USDC", "@142" ]
    pairs
;;

let test_coin_of_symbol_perp () =
  let pairs = pairs_fixture () in
  (* Bare coin names are perpetuals: the coin is used as-is. *)
  let c = Hyperliquid.Hyperliquid_oracle.coin_of_symbol ~pairs "BTC" in
  Alcotest.(check (option string)) "bare coin is a perp" (Some "BTC") c;
  let c = Hyperliquid.Hyperliquid_oracle.coin_of_symbol ~pairs " sol " in
  Alcotest.(check (option string)) "bare coin trimmed/upper" (Some "SOL") c
;;

let test_coin_of_symbol_spot () =
  let pairs = pairs_fixture () in
  let coin = Hyperliquid.Hyperliquid_oracle.coin_of_symbol ~pairs in
  (* Named spot pair resolves to its exact candle coin. *)
  Alcotest.(check (option string)) "named spot pair" (Some "PURR/USDC") (coin "PURR/USDC");
  (* "USD" quote is normalized to the Hyperliquid spot quote "USDC". *)
  Alcotest.(check (option string))
    "USD quote normalized"
    (Some "PURR/USDC")
    (coin "PURR/USD");
  (* Wrapped majors resolve through the feed-style key to the "@N" alias, so
     spot history is used instead of leaving the asset inactive. *)
  Alcotest.(check (option string)) "wrapped major -> @N" (Some "@142") (coin "BTC/USDC");
  Alcotest.(check (option string))
    "wrapped major USD quote"
    (Some "@142")
    (coin "BTC/USD")
;;

let test_coin_of_symbol_no_spot_pair () =
  let pairs = pairs_fixture () in
  let coin = Hyperliquid.Hyperliquid_oracle.coin_of_symbol ~pairs in
  (* Symbols that are not a Hyperliquid spot pair have no spot history: they
     resolve to None, never to a perpetual proxy. *)
  let cases = [ "XRP/USDC"; "LINK/USD"; "UBTC/USDC"; "BTC/USDT" ] in
  List.iter
    (fun symbol ->
       Alcotest.(check (option string))
         (Printf.sprintf "%s is not a spot pair" symbol)
         None
         (coin symbol))
    cases
;;

let test_coin_of_symbol_edge_cases () =
  let pairs = pairs_fixture () in
  let coin = Hyperliquid.Hyperliquid_oracle.coin_of_symbol ~pairs in
  Alcotest.(check (option string)) "empty symbol" None (coin "");
  Alcotest.(check (option string))
    "lowercase named pair"
    (Some "PURR/USDC")
    (coin "purr/usdc");
  Alcotest.(check (option string))
    "lowercase wrapped major"
    (Some "@142")
    (coin "btc/usdc")
;;

let test_parse_candles_sorts_and_dedups () =
  let json =
    Yojson.Safe.from_string
      {|[ {"t":1705000000000,"o":"102.0","h":"103.0","l":"101.0","c":"102.5","v":"9.0","n":3}
        , {"t":1700000000000,"o":"100.0","h":"101.0","l":"99.0","c":"100.5","v":"10.0","n":2}
        , {"t":1700000000000,"o":"100.0","h":"101.0","l":"99.0","c":"100.5","v":"10.0","n":2} ]|}
  in
  let bars = Hyperliquid.Hyperliquid_oracle.parse_candles ~symbol:"BTC/USDC" json in
  (* Raw-bar contract: parse_candles preserves every row; ordering + dedup
     happen centrally (windows_to_series / the oracle pipeline). *)
  Alcotest.(check int) "raw rows preserved" 3 (List.length bars);
  let dates = List.map (fun (b : Dio_exchange.Exchange_intf.Types.bar) -> b.date) bars in
  Alcotest.(check (list string))
    "response order preserved"
    [ "2024-01-11"; "2023-11-14"; "2023-11-14" ]
    dates;
  (* The window helper restores ascending order and de-duplicates, so the
     served series has one bar per date, oldest first. *)
  let out = Hyperliquid.Hyperliquid_oracle.windows_to_series [ bars ] in
  Alcotest.(check int) "two bars after dedup" 2 (List.length out);
  let dates = List.map (fun (b : Dio_exchange.Exchange_intf.Types.bar) -> b.date) out in
  Alcotest.(check (list string)) "ascending dates" [ "2023-11-14"; "2024-01-11" ] dates;
  Alcotest.(check (float 1e-9))
    "first close"
    100.5
    (List.hd out).Dio_exchange.Exchange_intf.Types.close;
  Alcotest.(check (float 1e-9))
    "second close"
    102.5
    (List.nth out 1).Dio_exchange.Exchange_intf.Types.close
;;

let test_parse_candles_bad_shape () =
  Alcotest.check_raises
    "object body rejected"
    (Failure
       "Oracle_fetch_hyperliquid.parse_candles: BTC/USDC expected array, got {\"oops\":1}")
    (fun () ->
       ignore
         (Hyperliquid.Hyperliquid_oracle.parse_candles
            ~symbol:"BTC/USDC"
            (`Assoc [ "oops", `Int 1 ])))
;;

let test_windows_to_series_ascending () =
  (* Regression: the window accumulation must restore ascending time order.
     The LAST bar is the CURRENT close - the grid start price and all ladder
     capital math read it, so a missing final sort prices every ladder from
     the oldest fetched close. *)
  let mk date close =
    Dio_exchange.Exchange_intf.Types.
      { date; open_ = close; high = close; low = close; close; volume = 100.0 }
  in
  let out =
    Hyperliquid.Hyperliquid_oracle.windows_to_series
      [ [ mk "2022-01-01" 1.0; mk "2022-01-02" 2.0 ]
      ; [ mk "2022-01-03" 3.0; mk "2022-01-04" 4.0 ]
      ]
  in
  Alcotest.(check (list (float 1e-9)))
    "ascending closes, last = newest"
    [ 1.0; 2.0; 3.0; 4.0 ]
    (List.map (fun b -> b.Dio_exchange.Exchange_intf.Types.close) out)
;;

let () =
  Alcotest.run
    "hyperliquid_oracle"
    [ ( "spot resolution"
      , [ Alcotest.test_case
            "feed symbol mapping from spotMeta"
            `Quick
            test_spot_meta_pairs
        ; Alcotest.test_case "perp coins pass through" `Quick test_coin_of_symbol_perp
        ; Alcotest.test_case "spot pairs resolve" `Quick test_coin_of_symbol_spot
        ; Alcotest.test_case
            "non-spot-pair symbols return None"
            `Quick
            test_coin_of_symbol_no_spot_pair
        ; Alcotest.test_case "edge cases" `Quick test_coin_of_symbol_edge_cases
        ] )
    ; ( "candle parsing"
      , [ Alcotest.test_case "parse candles" `Quick test_parse_candles_sorts_and_dedups
        ; Alcotest.test_case "reject bad shape" `Quick test_parse_candles_bad_shape
        ] )
    ; ( "window ordering"
      , [ Alcotest.test_case
            "windows accumulate ascending (last = current close)"
            `Quick
            test_windows_to_series_ascending
        ] )
    ]
;;
