(* Tests for Dio_oracle.Oracle_tasks (symbol/exchange resolution, calendar kind) and the
   Hyperliquid candle parser. *)

let trading_config ?(exchange = "kraken") ?(symbol = "X") ()
  : Dio_strategies.Strategy_common.trading_config
  =
  { exchange
  ; symbol
  ; qty = "1.0"
  ; grid_interval = 1.0, 1.0
  ; sell_mult = "1.0"
  ; min_usd_balance = None
  ; max_exposure = None
  ; strategy = "Ladder"
  ; maker_fee = None
  ; taker_fee = None
  ; testnet = false
  ; hedge = false
  ; accumulation_buffer = 0.01, 0.01
  ; data_feed = None
  ; base_accumulation = true
  ; sell_levels = true
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
    ]
;;
