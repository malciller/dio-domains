(* Tests for Dio_oracle.Oracle_tasks (symbol/exchange resolution, calendar kind) and the
   Hyperliquid candle parser. *)

let trading_config ?(exchange = "kraken") ?(symbol = "X") ?(asset_class = None) ()
  : Dio_strategies.Strategy_common.trading_config
  =
  { exchange
  ; symbol
  ; qty = "1.0"
  ; grid_interval = 1.0, 1.0
  ; sell_mult = "1.0"
  ; min_usd_balance = None
  ; max_exposure = None
  ; strategy = "Grid"
  ; maker_fee = None
  ; taker_fee = None
  ; testnet = false
  ; hedge = false
  ; accumulation_buffer = 0.01, 0.01
  ; data_feed = None
  ; asset_class
  }
;;

let resolve
      ?(symbol = "")
      ?(exchange = "kraken")
      ?(exchange_explicit = false)
      ~(trading : Dio_strategies.Strategy_common.trading_config list)
      ?(offline = false)
      ()
  =
  Dio_oracle.Oracle_tasks.resolve_tasks
    ~symbol
    ~exchange
    ~exchange_explicit
    ~trading
    ~offline
;;

let pairs tasks =
  List.map
    (fun (t : Dio_oracle.Oracle_tasks.task) ->
       t.Dio_oracle.Oracle_tasks.symbol, t.Dio_oracle.Oracle_tasks.exchange)
    tasks
;;

let test_all_assets () =
  let trading =
    [ trading_config ~exchange:"kraken" ~symbol:"ETH/USD" ()
    ; trading_config ~exchange:"hyperliquid" ~symbol:"BTC/USDC" ()
    ; trading_config ~exchange:"alpaca" ~symbol:"QQQ" ()
    ]
  in
  let tasks, _ = resolve ~trading () in
  Alcotest.(check (list (pair string string)))
    "one task per trading entry, each on its own exchange"
    [ "ETH/USD", "kraken"; "BTC/USDC", "hyperliquid"; "QQQ", "alpaca" ]
    (pairs tasks)
;;

let test_all_assets_skips_unsupported () =
  let trading =
    [ trading_config ~exchange:"kraken" ~symbol:"ETH/USD" ()
    ; trading_config ~exchange:"binance" ~symbol:"DOGE/USD" ()
    ; trading_config ~exchange:"" ~symbol:"GONE" ()
    ]
  in
  let tasks, unsupported = resolve ~trading () in
  Alcotest.(check (list (pair string string)))
    "unsupported / empty exchanges skipped"
    [ "ETH/USD", "kraken" ]
    (pairs tasks);
  Alcotest.(check (list (pair string string)))
    "unsupported exchanges reported"
    [ "DOGE/USD", "binance"; "GONE", "" ]
    unsupported
;;

let test_symbol_uses_config_exchange () =
  let trading = [ trading_config ~exchange:"hyperliquid" ~symbol:"BTC/USDC" () ] in
  let tasks, _ = resolve ~symbol:"BTC/USDC" ~trading () in
  Alcotest.(check int) "single task" 1 (List.length tasks);
  Alcotest.(check string)
    "exchange comes from config entry"
    "hyperliquid"
    (List.hd tasks).Dio_oracle.Oracle_tasks.exchange
;;

let test_symbol_explicit_exchange_wins () =
  let trading = [ trading_config ~exchange:"hyperliquid" ~symbol:"BTC/USDC" () ] in
  let tasks, _ =
    resolve ~symbol:"BTC/USDC" ~exchange:"kraken" ~exchange_explicit:true ~trading ()
  in
  Alcotest.(check int) "single task" 1 (List.length tasks);
  Alcotest.(check string)
    "explicit --exchange wins over config"
    "kraken"
    (List.hd tasks).Dio_oracle.Oracle_tasks.exchange
;;

let test_symbol_case_insensitive () =
  let trading = [ trading_config ~exchange:"hyperliquid" ~symbol:"BTC/USDC" () ] in
  let tasks, _ = resolve ~symbol:"btc/usdc" ~trading () in
  Alcotest.(check int) "single task" 1 (List.length tasks);
  Alcotest.(check string)
    "matched entry exchange"
    "hyperliquid"
    (List.hd tasks).Dio_oracle.Oracle_tasks.exchange
;;

let test_unknown_symbol_uses_defaults () =
  let tasks, _ = resolve ~symbol:"FOO/BAR" ~exchange:"kraken" ~trading:[] () in
  Alcotest.(check int) "single task" 1 (List.length tasks);
  let t = List.hd tasks in
  Alcotest.(check string) "symbol preserved" "FOO/BAR" t.Dio_oracle.Oracle_tasks.symbol;
  Alcotest.(check string)
    "exchange falls back to arg"
    "kraken"
    t.Dio_oracle.Oracle_tasks.exchange;
  Alcotest.(check string) "default qty" "1.0" t.Dio_oracle.Oracle_tasks.config.qty
;;

let test_unsupported_config_exchange_reports_no_task () =
  let trading = [ trading_config ~exchange:"binance" ~symbol:"DOGE/USD" () ] in
  let tasks, unsupported = resolve ~symbol:"DOGE/USD" ~trading () in
  Alcotest.(check int) "no task" 0 (List.length tasks);
  Alcotest.(check (list (pair string string)))
    "unsupported exchange reported"
    [ "DOGE/USD", "binance" ]
    unsupported
;;

let test_no_symbol_empty_trading () =
  let tasks, unsupported = resolve ~trading:[] () in
  Alcotest.(check int) "no tasks" 0 (List.length tasks);
  Alcotest.(check (list (pair string string))) "no unsupported" [] unsupported
;;

let test_offline_requires_symbol () =
  Alcotest.check_raises
    "offline mode without symbol"
    (Failure "offline mode (--from-csv / --from-json) requires a SYMBOL argument")
    (fun () -> ignore (resolve ~trading:[] ~offline:true ()))
;;

let test_calendar_kind () =
  Alcotest.(check bool)
    "hyperliquid is crypto"
    true
    (Dio_oracle.Oracle_tasks.calendar_kind_of_exchange "hyperliquid"
     = Dio_oracle.Oracle_types.Crypto);
  Alcotest.(check bool)
    "kraken is crypto"
    true
    (Dio_oracle.Oracle_tasks.calendar_kind_of_exchange "kraken"
     = Dio_oracle.Oracle_types.Crypto);
  Alcotest.(check bool)
    "alpaca is equity"
    true
    (Dio_oracle.Oracle_tasks.calendar_kind_of_exchange "alpaca"
     = Dio_oracle.Oracle_types.Equity);
  Alcotest.(check bool)
    "unknown defaults to crypto"
    true
    (Dio_oracle.Oracle_tasks.calendar_kind_of_exchange "binance"
     = Dio_oracle.Oracle_types.Crypto)
;;

let test_coin_of_symbol () =
  (* Fixture feed-symbol -> candle coin mapping, mirroring spotMeta: the
     canonical PURR/USDC pair and the wrapped BTC spot (@142 = UBTC/USDC). *)
  let pairs = [ "PURR/USDC", "PURR/USDC"; "BTC/USDC", "@142" ] in
  (* Bare coin names are perpetuals and pass through as-is. *)
  Alcotest.(check (option string))
    "bare coin is a perp"
    (Some "BTC")
    (Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "BTC");
  (* Spot symbols resolve through the feed-style mapping to the mapped spot
     asset's candle coin; USD quotes normalize to the USDC spot quote. *)
  Alcotest.(check (option string))
    "named spot pair"
    (Some "PURR/USDC")
    (Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "PURR/USDC");
  Alcotest.(check (option string))
    "USD quote normalized"
    (Some "PURR/USDC")
    (Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "PURR/USD");
  (* Wrapped majors (BTC spot is UBTC/USDC, the "@142" pair) resolve to the
     "@N" candle coin, so their spot history is used. *)
  Alcotest.(check (option string))
    "wrapped major -> @N"
    (Some "@142")
    (Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "BTC/USDC");
  Alcotest.(check (option string))
    "wrapped major USD quote -> @N"
    (Some "@142")
    (Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "BTC/USD");
  (* Symbols that are not a Hyperliquid spot pair resolve to None - never to
     a perpetual proxy. *)
  Alcotest.(check (option string))
    "eth/usd is not a spot pair"
    None
    (Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "ETH/USD");
  Alcotest.(check (option string))
    "lowercase perp"
    (Some "BTC")
    (Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "btc")
;;

let test_parse_candles_sorts_and_dedups () =
  let json =
    Yojson.Safe.from_string
      {|[ {"t":1705000000000,"o":"102.0","h":"103.0","l":"101.0","c":"102.5","v":"9.0","n":3}
        , {"t":1700000000000,"o":"100.0","h":"101.0","l":"99.0","c":"100.5","v":"10.0","n":2}
        , {"t":1700000000000,"o":"100.0","h":"101.0","l":"99.0","c":"100.5","v":"10.0","n":2} ]|}
  in
  let bars = Dio_oracle.Oracle_fetch_hyperliquid.parse_candles ~symbol:"BTC/USDC" json in
  Alcotest.(check int) "two bars after dedup" 2 (List.length bars);
  let dates =
    List.map
      (fun (b : Dio_oracle.Oracle_types.bar) -> b.Dio_oracle.Oracle_types.date)
      bars
  in
  Alcotest.(check (list string)) "ascending dates" [ "2023-11-14"; "2024-01-11" ] dates;
  Alcotest.(check (float 1e-9))
    "first close"
    100.5
    (List.hd bars).Dio_oracle.Oracle_types.close;
  Alcotest.(check (float 1e-9))
    "second close"
    102.5
    (List.nth bars 1).Dio_oracle.Oracle_types.close
;;

let test_parse_candles_bad_shape () =
  Alcotest.check_raises
    "object body rejected"
    (Failure
       "Oracle_fetch_hyperliquid.parse_candles: BTC/USDC expected array, got {\"oops\":1}")
    (fun () ->
       ignore
         (Dio_oracle.Oracle_fetch_hyperliquid.parse_candles
            ~symbol:"BTC/USDC"
            (`Assoc [ "oops", `Int 1 ])))
;;

let test_min_notional_defaults () =
  let open Dio_oracle.Grid_adapter in
  Alcotest.(check (float 1e-9))
    "hyperliquid spot floor = 10 USDC"
    10.0
    (default_min_notional Dio_strategies.Grid_core_types.Hyperliquid);
  Alcotest.(check (float 1e-9))
    "kraken not notional-constrained"
    0.0
    (default_min_notional Dio_strategies.Grid_core_types.Kraken);
  Alcotest.(check (float 1e-9))
    "alpaca not notional-constrained"
    0.0
    (default_min_notional Dio_strategies.Grid_core_types.Alpaca)
;;

let test_adapter_min_notional_flows_into_config () =
  (* The venue default must reach the Grid_core.config the adapter builds. *)
  let tc = trading_config ~exchange:"hyperliquid" ~symbol:"BTC/USDC" () in
  let grid =
    Dio_oracle.Grid_adapter.of_trading_config
      tc
      ~start_price:100.0
      ~start_quote:1000.0
      ~grid_interval_pct:1.0
  in
  Alcotest.(check (float 1e-9)) "min_notional = 10" 10.0 grid.min_notional
;;

let () =
  Alcotest.run
    "oracle-config"
    [ ( "resolve_tasks"
      , [ Alcotest.test_case "all assets" `Quick test_all_assets
        ; Alcotest.test_case
            "all assets skip unsupported"
            `Quick
            test_all_assets_skips_unsupported
        ; Alcotest.test_case
            "symbol uses config exchange"
            `Quick
            test_symbol_uses_config_exchange
        ; Alcotest.test_case
            "explicit exchange wins"
            `Quick
            test_symbol_explicit_exchange_wins
        ; Alcotest.test_case "symbol case-insensitive" `Quick test_symbol_case_insensitive
        ; Alcotest.test_case
            "unknown symbol defaults"
            `Quick
            test_unknown_symbol_uses_defaults
        ; Alcotest.test_case "no symbol empty trading" `Quick test_no_symbol_empty_trading
        ; Alcotest.test_case
            "unsupported config exchange"
            `Quick
            test_unsupported_config_exchange_reports_no_task
        ; Alcotest.test_case "offline requires symbol" `Quick test_offline_requires_symbol
        ] )
    ; "calendar", [ Alcotest.test_case "exchange kinds" `Quick test_calendar_kind ]
    ; ( "hyperliquid"
      , [ Alcotest.test_case "coin mapping" `Quick test_coin_of_symbol
        ; Alcotest.test_case "parse candles" `Quick test_parse_candles_sorts_and_dedups
        ; Alcotest.test_case "reject bad shape" `Quick test_parse_candles_bad_shape
        ] )
    ; ( "grid_adapter"
      , [ Alcotest.test_case
            "min_notional venue defaults"
            `Quick
            test_min_notional_defaults
        ; Alcotest.test_case
            "min_notional flows into config"
            `Quick
            test_adapter_min_notional_flows_into_config
        ] )
    ]
;;
