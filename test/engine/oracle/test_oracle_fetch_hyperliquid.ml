(* Tests for the Hyperliquid oracle fetch's spot/perpetual resolution logic.
   These exercise the pure functions only (no network). *)

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
  Dio_oracle.Oracle_fetch_hyperliquid.spot_meta_pairs_of_json json
;;

let test_spot_meta_pairs () =
  let pairs = pairs_fixture () in
  (* The canonical pair maps to its own name; wrapped pairs map to the "@N"
     alias that candleSnapshot accepts (the universe "name" field). *)
  Alcotest.(check (list (pair string string)))
    "feed symbol -> candle coin"
    [ "PURR/USDC", "PURR/USDC"; "HFUN/USDC", "@1"; "BTC/USDC", "@142" ]
    pairs
;;

let test_coin_of_symbol_perp () =
  let pairs = pairs_fixture () in
  (* Bare coin names are perpetuals: the coin is used as-is. *)
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "BTC" in
  Alcotest.(check (option string)) "bare coin is a perp" (Some "BTC") c;
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs " sol " in
  Alcotest.(check (option string)) "bare coin trimmed/upper" (Some "SOL") c
;;

let test_coin_of_symbol_spot () =
  let pairs = pairs_fixture () in
  (* Named spot pair resolves to its exact candle coin. *)
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "PURR/USDC" in
  Alcotest.(check (option string)) "named spot pair" (Some "PURR/USDC") c;
  (* "USD" quote is normalized to the Hyperliquid spot quote "USDC". *)
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "PURR/USD" in
  Alcotest.(check (option string)) "USD quote normalized" (Some "PURR/USDC") c;
  (* Wrapped majors resolve through the feed-style key to the "@N" alias, so
     spot history is used instead of leaving the asset inactive. *)
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "BTC/USDC" in
  Alcotest.(check (option string)) "wrapped major -> @N" (Some "@142") c;
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "BTC/USD" in
  Alcotest.(check (option string)) "wrapped major USD quote" (Some "@142") c
;;

let test_coin_of_symbol_no_spot_pair () =
  let pairs = pairs_fixture () in
  (* Symbols that are not a Hyperliquid spot pair have no spot history: they
     resolve to None, never to a perpetual proxy. The raw wrapped token name
     ("UBTC/USDC") is not the feed-style key either - configs use the
     canonical base ("BTC/USDC"), same as the instruments feed. *)
  let cases = [ "XRP/USDC"; "LINK/USD"; "UBTC/USDC"; "BTC/USDT" ] in
  List.iter
    (fun symbol ->
       let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs symbol in
       Alcotest.(check (option string))
         (Printf.sprintf "%s is not a spot pair" symbol)
         None
         c)
    cases
;;

let test_coin_of_symbol_edge_cases () =
  let pairs = pairs_fixture () in
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "" in
  Alcotest.(check (option string)) "empty symbol" None c;
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "purr/usdc" in
  Alcotest.(check (option string)) "lowercase named pair" (Some "PURR/USDC") c;
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "btc/usdc" in
  Alcotest.(check (option string)) "lowercase wrapped major" (Some "@142") c
;;

let test_windows_to_series_ascending () =
  (* Regression: the window accumulation must restore ascending time order.
     The LAST bar is the CURRENT close - the grid start price and all ladder
     capital math read it, so a missing final List.rev prices every ladder
     from the oldest fetched close (the bug that made HYPE/ETH rung costs
     come from 2022-era prices). *)
  let mk date close =
    (* Real trading volume: the source-normalization guard keeps a series
       only if it has real-volume rows (this test is about ordering). *)
    Dio_oracle.Oracle_types.
      { date; open_ = close; high = close; low = close; close; volume = 100.0 }
  in
  let out =
    Dio_oracle.Oracle_fetch_hyperliquid.windows_to_series
      [ [ mk "2022-01-01" 1.0; mk "2022-01-02" 2.0 ]
      ; [ mk "2022-01-03" 3.0; mk "2022-01-04" 4.0 ]
      ]
  in
  Alcotest.(check (list (float 1e-9)))
    "ascending closes, last = newest"
    [ 1.0; 2.0; 3.0; 4.0 ]
    (List.map (fun b -> b.Dio_oracle.Oracle_types.close) out)
;;

(* ---- Source normalization: real @142 (UBTC/USDC) fixture ----
   The pre-listing placeholder run (2025-02-03..2025-02-13: fabricated
   constant candles 6,969,696 / 7,979,573 with zero or dust volume) plus the
   first real trading day whose open/high (240,000) never traded (close
   97,578) - exactly the rows observed from candleSnapshot that made the
   oracle read a 99.3% drawdown and a $7.98M peak. *)

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
    Dio_oracle.Oracle_fetch_hyperliquid.normalize_bars (placeholder_bars () @ real_bars ())
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
     report is a real one, far below the fabricated 99.3%. *)
  let series =
    Dio_oracle.Oracle_types.
      { symbol = "BTC/USDC"; calendar_kind = Crypto; bars = clean; gaps = [] }
  in
  let p = Option.get (Dio_oracle.Oracle_math.peak_to_valley_stats_of series) in
  (* The phantom $7.98M peak is gone: the drawdown the oracle would now
     report is the fixture's mild 1.3% drift, not the fabricated 99.3%. *)
  Alcotest.(check bool) "no phantom 99% drawdown" true (p.max_drawdown < 0.05)
;;

let test_normalize_keeps_carried_zero_volume () =
  (* A zero-volume day whose price is carried near the real market is kept:
     normalization must only remove FABRICATED levels, not dead-but-real
     days (dropping them would fabricate gaps that fail max_gap). *)
  let clean, dropped, clamped =
    Dio_oracle.Oracle_fetch_hyperliquid.normalize_bars
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
    Dio_oracle.Oracle_fetch_hyperliquid.normalize_bars (placeholder_bars ())
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
    Dio_oracle.Oracle_fetch_hyperliquid.normalize_bars (List.init 56 mk)
  in
  Alcotest.(check int) "nothing dropped" 0 dropped;
  Alcotest.(check int) "nothing clamped" 0 clamped;
  Alcotest.(check int) "all rows survive" 56 (Array.length clean)
;;

let () =
  Alcotest.run
    "oracle_fetch_hyperliquid"
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
    ; ( "window ordering"
      , [ Alcotest.test_case
            "windows accumulate ascending (last = current close)"
            `Quick
            test_windows_to_series_ascending
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
        ] )
    ]
;;
