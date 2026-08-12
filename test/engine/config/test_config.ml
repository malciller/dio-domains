let test_parse_trading_config_valid () =
  let json_str =
    {|{"symbol": "BTC/USD", "exchange": "kraken", "qty": "0.001", "sell_mult": "1.1", "strategy": "market_maker", "maker_fee": 0.001, "taker_fee": 0.002, "asset_class": "crypto_core"}|}
  in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_config json in
  Alcotest.(check string) "symbol" "BTC/USD" config.symbol;
  Alcotest.(check string) "exchange" "kraken" config.exchange;
  Alcotest.(check string) "qty" "0.001" config.qty;
  Alcotest.(check string) "sell_mult" "1.1" config.sell_mult;
  Alcotest.(check string) "strategy" "market_maker" config.strategy;
  Alcotest.(check (option (float 0.001))) "maker_fee" (Some 0.001) config.maker_fee;
  Alcotest.(check (option (float 0.001))) "taker_fee" (Some 0.002) config.taker_fee;
  Alcotest.(check (option string)) "asset_class" (Some "crypto_core") config.asset_class
;;

let test_parse_trading_config_defaults () =
  let json_str = {|{"symbol": "ETH/USD", "qty": "0.01", "strategy": "Grid"}|} in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_config json in
  Alcotest.(check string) "symbol" "ETH/USD" config.symbol;
  Alcotest.(check string) "exchange default" "kraken" config.exchange;
  Alcotest.(check string) "qty" "0.01" config.qty;
  Alcotest.(check (pair (float 0.0001) (float 0.0001)))
    "grid_interval default"
    (1.0, 1.0)
    config.grid_interval;
  Alcotest.(check string) "sell_mult default" "1.0" config.sell_mult;
  Alcotest.(check string) "strategy" "Grid" config.strategy;
  Alcotest.(check (option (float 0.001))) "maker_fee none" None config.maker_fee;
  Alcotest.(check (option (float 0.001))) "taker_fee none" None config.taker_fee;
  Alcotest.(check (option string)) "asset_class none" None config.asset_class;
  Alcotest.(check (option string)) "min_usd_balance none" None config.min_usd_balance;
  Alcotest.(check (option string)) "max_exposure none" None config.max_exposure
;;

let test_parse_trading_config_optional_fields () =
  let json_str =
    {|{"symbol": "LTC/USD", "qty": "0.1", "strategy": "grid", "min_usd_balance": "100", "max_exposure": "500"}|}
  in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_config json in
  Alcotest.(check (option string)) "min_usd_balance" (Some "100") config.min_usd_balance;
  Alcotest.(check (option string)) "max_exposure" (Some "500") config.max_exposure
;;

let test_parse_logging_config_valid () =
  let json_str =
    {|{"logging_level": "debug", "logging_sections": "engine,trading,api"}|}
  in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_logging_config json in
  Alcotest.(check bool) "debug level" true (config.level = Logging.DEBUG);
  Alcotest.(check (list string)) "sections" [ "engine"; "trading"; "api" ] config.sections
;;

let test_parse_logging_config_defaults () =
  let json_str = {|{}|} in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_logging_config json in
  Alcotest.(check bool) "info level default" true (config.level = Logging.INFO);
  Alcotest.(check (list string)) "empty sections default" [] config.sections
;;

let test_parse_logging_config_invalid_level () =
  let json_str = {|{"logging_level": "invalid_level", "logging_sections": "test"}|} in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_logging_config json in
  Alcotest.(check bool) "invalid level defaults to INFO" true (config.level = Logging.INFO);
  Alcotest.(check (list string)) "sections with invalid level" [ "test" ] config.sections
;;

let test_parse_logging_config_empty_sections () =
  let json_str = {|{"logging_level": "warn", "logging_sections": ""}|} in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_logging_config json in
  Alcotest.(check bool) "warn level" true (config.level = Logging.WARN);
  Alcotest.(check (list string)) "empty sections string" [] config.sections
;;

let test_read_config_defaults () =
  (* Test default config when no config file exists *)
  (* We'll assume config.json doesn't exist for this test *)
  let config = Dio_engine.Config.read_config () in
  Alcotest.(check bool) "default logging level" true (config.logging.level = Logging.INFO);
  Alcotest.(check (list string)) "default logging sections" [] config.logging.sections;
  Alcotest.(check bool) "empty trading config" true (config.trading = [])
;;

let test_parse_classes () =
  let json_str =
    {|{"classes": {"crypto_core": ["BTC/USD", "ETH/USD"], "equity_etf": ["SPY", "QQQ"]}}|}
  in
  let json = Yojson.Basic.from_string json_str in
  let classes = Dio_engine.Config.parse_classes json in
  let open Dio_engine.Config in
  Alcotest.(check (list (pair string (list string))))
    "legacy schema members"
    [ "crypto_core", [ "BTC/USD"; "ETH/USD" ]; "equity_etf", [ "SPY"; "QQQ" ] ]
    (List.map (fun ((name, pool) : string * class_pool) -> name, pool.members) classes);
  Alcotest.(check (list (pair string (option int))))
    "legacy schema kappa unset"
    [ "crypto_core", None; "equity_etf", None ]
    (List.map (fun ((name, pool) : string * class_pool) -> name, pool.kappa) classes)
;;

let test_parse_classes_extended_schema () =
  let json_str =
    {|{"classes": {"crypto_core": {"members": ["BTC/USD", "ETH/USD"], "kappa": 250}, "equity_etf": {"members": ["SPY", "QQQ"]}}}|}
  in
  let json = Yojson.Basic.from_string json_str in
  let classes = Dio_engine.Config.parse_classes json in
  let open Dio_engine.Config in
  Alcotest.(check (list (pair string (list string))))
    "extended schema members"
    [ "crypto_core", [ "BTC/USD"; "ETH/USD" ]; "equity_etf", [ "SPY"; "QQQ" ] ]
    (List.map (fun ((name, pool) : string * class_pool) -> name, pool.members) classes);
  Alcotest.(check (list (pair string (option int))))
    "extended schema kappa"
    [ "crypto_core", Some 250; "equity_etf", None ]
    (List.map (fun ((name, pool) : string * class_pool) -> name, pool.kappa) classes)
;;

let test_parse_classes_absent () =
  Alcotest.(check int)
    "absent classes key parses to empty list"
    0
    (List.length
       (Dio_engine.Config.parse_classes (Yojson.Basic.from_string {|{"trading": []}|})))
;;

let test_parse_oracle_config_full () =
  let json_str =
    {|{"oracle": {"qty_cap_mult": 0.0, "poll_seconds": 5.0, "refresh_seconds": 60.0, "target_survival": 0.95, "max_capital": 1000.5, "horizons": [1, 7, 30], "startup_wait_seconds": 45.0}}|}
  in
  let json = Yojson.Basic.from_string json_str in
  match Dio_engine.Config.parse_oracle_config json with
  | None -> Alcotest.fail "oracle section should parse to Some"
  | Some c ->
    Alcotest.(check (float 0.0001)) "qty_cap_mult" 0.0 c.qty_cap_mult;
    Alcotest.(check (float 0.0001)) "poll_seconds" 5.0 c.poll_seconds;
    Alcotest.(check (float 0.0001)) "refresh_seconds" 60.0 c.refresh_seconds;
    Alcotest.(check (float 0.0001)) "target_survival" 0.95 c.target_survival;
    Alcotest.(check (float 0.0001)) "startup_wait_seconds" 45.0 c.startup_wait_seconds;
    Alcotest.(check (option (float 0.0001))) "max_capital" (Some 1000.5) c.max_capital;
    Alcotest.(check (option (list int))) "horizons" (Some [ 1; 7; 30 ]) c.horizons
;;

let test_parse_oracle_config_partial_defaults () =
  (* A partial section falls back to the runtime defaults for absent keys. *)
  let json_str = {|{"oracle": {"qty_cap_mult": 0.0}}|} in
  let json = Yojson.Basic.from_string json_str in
  match Dio_engine.Config.parse_oracle_config json with
  | None -> Alcotest.fail "oracle section should parse to Some"
  | Some c ->
    let d = Dio_oracle.Oracle_runtime.default_config () in
    Alcotest.(check (float 0.0001)) "qty_cap_mult" 0.0 c.qty_cap_mult;
    Alcotest.(check (float 0.0001)) "poll_seconds default" d.poll_seconds c.poll_seconds;
    Alcotest.(check (float 0.0001))
      "refresh_seconds default"
      d.refresh_seconds
      c.refresh_seconds;
    Alcotest.(check bool) "no_deep_history default" d.no_deep_history c.no_deep_history;
    Alcotest.(check (option (float 0.0001)))
      "max_capital default"
      d.max_capital
      c.max_capital;
    Alcotest.(check (float 0.0001))
      "startup_wait_seconds default"
      d.startup_wait_seconds
      c.startup_wait_seconds
;;

let test_parse_oracle_config_absent () =
  let json = Yojson.Basic.from_string {|{"trading": []}|} in
  match Dio_engine.Config.parse_oracle_config json with
  | None -> ()
  | Some _ -> Alcotest.fail "absent oracle section should parse to None"
;;

let test_parse_oracle_config_default_qty_cap_mult () =
  (* The runtime default is uncapped (0.0) so the oracle can deploy the whole pool. *)
  let d = Dio_oracle.Oracle_runtime.default_config () in
  Alcotest.(check (float 0.0001)) "qty_cap_mult default uncapped" 0.0 d.qty_cap_mult
;;

let () =
  Alcotest.run
    "Config"
    [ ( "trading_config"
      , [ Alcotest.test_case "valid config" `Quick test_parse_trading_config_valid
        ; Alcotest.test_case "defaults" `Quick test_parse_trading_config_defaults
        ; Alcotest.test_case
            "optional fields"
            `Quick
            test_parse_trading_config_optional_fields
        ] )
    ; ( "logging_config"
      , [ Alcotest.test_case "valid logging" `Quick test_parse_logging_config_valid
        ; Alcotest.test_case "logging defaults" `Quick test_parse_logging_config_defaults
        ; Alcotest.test_case
            "invalid level"
            `Quick
            test_parse_logging_config_invalid_level
        ; Alcotest.test_case
            "empty sections"
            `Quick
            test_parse_logging_config_empty_sections
        ] )
    ; ( "classes"
      , [ Alcotest.test_case "parse classes" `Quick test_parse_classes
        ; Alcotest.test_case
            "parse classes extended schema"
            `Quick
            test_parse_classes_extended_schema
        ; Alcotest.test_case "classes absent" `Quick test_parse_classes_absent
        ] )
    ; ( "oracle"
      , [ Alcotest.test_case "parse full section" `Quick test_parse_oracle_config_full
        ; Alcotest.test_case
            "partial section uses defaults"
            `Quick
            test_parse_oracle_config_partial_defaults
        ; Alcotest.test_case
            "absent section is None"
            `Quick
            test_parse_oracle_config_absent
        ; Alcotest.test_case
            "qty_cap_mult default uncapped"
            `Quick
            test_parse_oracle_config_default_qty_cap_mult
        ] )
    ; ( "file_handling"
      , [ Alcotest.test_case "config defaults" `Quick test_read_config_defaults ] )
    ]
;;
