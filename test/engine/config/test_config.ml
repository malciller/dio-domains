let test_parse_trading_config_valid () =
  let json_str = {|{"symbol": "BTC/USD", "exchange": "kraken", "qty": "0.001", "grid_interval": "0.5", "sell_mult": "1.1", "strategy": "market_maker", "maker_fee": 0.001, "taker_fee": 0.002}|} in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_config json in

  Alcotest.(check string) "symbol" "BTC/USD" config.symbol;
  Alcotest.(check string) "exchange" "kraken" config.exchange;
  Alcotest.(check string) "qty" "0.001" config.qty;
  Alcotest.(check string) "grid_interval" "0.5" config.grid_interval;
  Alcotest.(check string) "sell_mult" "1.1" config.sell_mult;
  Alcotest.(check string) "strategy" "market_maker" config.strategy;
  Alcotest.(check (option (float 0.001))) "maker_fee" (Some 0.001) config.maker_fee;
  Alcotest.(check (option (float 0.001))) "taker_fee" (Some 0.002) config.taker_fee

let test_parse_trading_config_defaults () =
  let json_str = {|{"symbol": "ETH/USD", "qty": "0.01", "strategy": "suicide_grid"}|} in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_config json in

  Alcotest.(check string) "symbol" "ETH/USD" config.symbol;
  Alcotest.(check string) "exchange default" "kraken" config.exchange;
  Alcotest.(check string) "qty" "0.01" config.qty;
  Alcotest.(check string) "grid_interval default" "1.0" config.grid_interval;
  Alcotest.(check string) "sell_mult default" "1.0" config.sell_mult;
  Alcotest.(check string) "strategy" "suicide_grid" config.strategy;
  Alcotest.(check (option (float 0.001))) "maker_fee none" None config.maker_fee;
  Alcotest.(check (option (float 0.001))) "taker_fee none" None config.taker_fee;
  Alcotest.(check (option string)) "min_usd_balance none" None config.min_usd_balance;
  Alcotest.(check (option string)) "max_exposure none" None config.max_exposure

let test_parse_trading_config_optional_fields () =
  let json_str = {|{"symbol": "LTC/USD", "qty": "0.1", "strategy": "grid", "min_usd_balance": "100", "max_exposure": "500"}|} in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_config json in

  Alcotest.(check (option string)) "min_usd_balance" (Some "100") config.min_usd_balance;
  Alcotest.(check (option string)) "max_exposure" (Some "500") config.max_exposure

let test_parse_logging_config_valid () =
  let json_str = {|{"logging_level": "debug", "logging_sections": "engine,trading,api"}|} in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_logging_config json in

  Alcotest.(check bool) "debug level" true (config.level = Logging.DEBUG);
  Alcotest.(check (list string)) "sections" ["engine"; "trading"; "api"] config.sections

let test_parse_logging_config_defaults () =
  let json_str = {|{}|} in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_logging_config json in

  Alcotest.(check bool) "info level default" true (config.level = Logging.INFO);
  Alcotest.(check (list string)) "empty sections default" [] config.sections

let test_parse_logging_config_invalid_level () =
  let json_str = {|{"logging_level": "invalid_level", "logging_sections": "test"}|} in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_logging_config json in

  Alcotest.(check bool) "invalid level defaults to INFO" true (config.level = Logging.INFO);
  Alcotest.(check (list string)) "sections with invalid level" ["test"] config.sections

let test_parse_logging_config_empty_sections () =
  let json_str = {|{"logging_level": "warn", "logging_sections": ""}|} in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_logging_config json in

  Alcotest.(check bool) "warn level" true (config.level = Logging.WARN);
  Alcotest.(check (list string)) "empty sections string" [] config.sections

let test_read_config_defaults () =
  (* Test default config when no config file exists *)
  (* We'll assume config.json doesn't exist for this test *)
  let config = Dio_engine.Config.read_config () in

  Alcotest.(check bool) "default logging level" true (config.logging.level = Logging.INFO);
  Alcotest.(check (list string)) "default logging sections" [] config.logging.sections;
  Alcotest.(check bool) "empty trading config" true (config.trading = [])

let () =
  Alcotest.run "Config" [
    "trading_config", [
      Alcotest.test_case "valid config" `Quick test_parse_trading_config_valid;
      Alcotest.test_case "defaults" `Quick test_parse_trading_config_defaults;
      Alcotest.test_case "optional fields" `Quick test_parse_trading_config_optional_fields;
    ];
    "logging_config", [
      Alcotest.test_case "valid logging" `Quick test_parse_logging_config_valid;
      Alcotest.test_case "logging defaults" `Quick test_parse_logging_config_defaults;
      Alcotest.test_case "invalid level" `Quick test_parse_logging_config_invalid_level;
      Alcotest.test_case "empty sections" `Quick test_parse_logging_config_empty_sections;
    ];
    "file_handling", [
      Alcotest.test_case "config defaults" `Quick test_read_config_defaults;
    ];
  ]
