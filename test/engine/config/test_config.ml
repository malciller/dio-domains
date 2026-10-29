let test_parse_trading_config_valid () =
  let json_str =
    {|{"symbol": "BTC/USD", "exchange": "kraken", "qty": "0.001", "sell_mult": "1.1", "strategy": "market_maker", "maker_fee": 0.001, "taker_fee": 0.002}|}
  in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_config json in
  Alcotest.(check string) "symbol" "BTC/USD" config.symbol;
  Alcotest.(check string) "exchange" "kraken" config.exchange;
  Alcotest.(check string) "qty" "0.001" config.qty;
  Alcotest.(check string) "sell_mult" "1.1" config.sell_mult;
  Alcotest.(check string) "strategy" "market_maker" config.strategy;
  Alcotest.(check (option (float 0.001))) "maker_fee" (Some 0.001) config.maker_fee;
  Alcotest.(check (option (float 0.001))) "taker_fee" (Some 0.002) config.taker_fee
;;

let test_parse_trading_config_defaults () =
  let json_str = {|{"symbol": "ETH/USD", "qty": "0.01", "strategy": "Ladder"}|} in
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
  Alcotest.(check string) "strategy preserved" "Ladder" config.strategy;
  Alcotest.(check (option (float 0.001))) "maker_fee none" None config.maker_fee;
  Alcotest.(check (option (float 0.001))) "taker_fee none" None config.taker_fee;
  Alcotest.(check (option string)) "min_usd_balance none" None config.min_usd_balance;
  Alcotest.(check (option string)) "max_exposure none" None config.max_exposure
;;

let test_parse_trading_config_optional_fields () =
  let json_str =
    {|{"symbol": "LTC/USD", "qty": "0.1", "strategy": "grid", "min_usd_balance": "100", "max_exposure": "500"}|}
  in
  let json = Yojson.Basic.from_string json_str in
  let config = Dio_engine.Config.parse_config json in
  Alcotest.(check string) "strategy preserved verbatim" "grid" config.strategy;
  Alcotest.(check (option string)) "min_usd_balance" (Some "100") config.min_usd_balance;
  Alcotest.(check (option string)) "max_exposure" (Some "500") config.max_exposure
;;

let test_parse_overlays () =
  let json =
    Yojson.Basic.from_string
      {|{"trading":[
        {"symbol":"BTC/USDC","exchange":"hyperliquid","overlay":{
          "strategy":"carry_overlay",
          "params":{"start_ratio_pct":30,"add_threshold_pct":8,"max_ratio_pct":80,
                    "short_tp_roe":3,"recycle_base_share_pct":60,
                    "min_entry_margin_ratio":2.5,"min_liq_mult":3.0,
                    "full_balance_ref":true}}},
        {"symbol":"HYPE/USDC","exchange":"hyperliquid"}]}|}
  in
  Dio_engine.Config.parse_overlays json;
  Alcotest.(check (option string))
    "overlay bound to its owning entry"
    (Some "carry_overlay")
    (Dio_engine.Config.get_overlay_strategy ~exchange:"hyperliquid" ~symbol:"BTC/USDC");
  Alcotest.(check (option string))
    "entry without overlay unbound"
    None
    (Dio_engine.Config.get_overlay_strategy ~exchange:"hyperliquid" ~symbol:"HYPE/USDC");
  let p =
    Dio_engine.Config.get_overlay_params ~exchange:"hyperliquid" ~symbol:"BTC/USDC"
  in
  Alcotest.(check (float 1e-9)) "start ratio" 30.0 p.Dio_engine.Config.start_ratio_pct;
  Alcotest.(check (float 1e-9))
    "recycle base share"
    60.0
    p.Dio_engine.Config.recycle_base_share_pct;
  Alcotest.(check (float 1e-9))
    "entry margin ratio"
    2.5
    p.Dio_engine.Config.min_entry_margin_ratio;
  Alcotest.(check bool) "full balance ref" true p.Dio_engine.Config.full_balance_ref
;;

let test_parse_overlay_params_defaults () =
  let json =
    Yojson.Basic.from_string
      {|{"trading":[{"symbol":"ETH/USDC","exchange":"hyperliquid",
        "overlay":{"strategy":"carry_overlay"}}]}|}
  in
  Dio_engine.Config.parse_overlays json;
  let p =
    Dio_engine.Config.get_overlay_params ~exchange:"hyperliquid" ~symbol:"ETH/USDC"
  in
  Alcotest.(check (float 1e-9))
    "split default"
    50.0
    p.Dio_engine.Config.recycle_base_share_pct;
  Alcotest.(check (float 1e-9))
    "entry margin default"
    2.0
    p.Dio_engine.Config.min_entry_margin_ratio;
  Alcotest.(check bool)
    "full balance ref default"
    false
    p.Dio_engine.Config.full_balance_ref
;;

let test_parse_overlay_accepts_repeated_owner () =
  (* Repeating the owning entry's exchange/symbol is allowed; the symbol can be omitted,
     which is the intended shape. *)
  let json =
    Yojson.Basic.from_string
      {|{"trading":[{"symbol":"SOL/USD","exchange":"hyperliquid",
        "overlay":{"strategy":"carry_overlay","exchange":"hyperliquid","symbol":"SOL/USD"}}]}|}
  in
  Dio_engine.Config.parse_overlays json;
  Alcotest.(check (option string))
    "explicit owner matches"
    (Some "carry_overlay")
    (Dio_engine.Config.get_overlay_strategy ~exchange:"hyperliquid" ~symbol:"SOL/USD");
  Alcotest.(check (option string))
    "no binding under the overlay's absent symbol elsewhere"
    None
    (Dio_engine.Config.get_overlay_strategy ~exchange:"hyperliquid" ~symbol:"BTC/USDC")
;;

let grouped_json =
  {|{"trading":{
    "defaults":{
      "strategy":{
        "kraken":{"name":"jacobs_ladder","sell_mult":"0.99",
                  "accumulation_buffer":[0.1,1.5],"sell_levels":false},
        "hyperliquid":{"name":"jacobs_ladder","sell_mult":"0.98"},
        "alpaca":{"name":"jacobs_ladder","sell_mult":"0.999",
                  "accumulation_buffer":[0.05,0.4],"testnet":true,
                  "sell_levels":true,"data_feed":"iex"}
      },
      "overlay":{"carry_overlay":{"exchange":"hyperliquid",
                                  "params":{"min_entry_margin_ratio":1.0,
                                            "min_liq_mult":1.0,
                                            "recycle_base_share_pct":0}},
                 "long_overlay":{"exchange":"lighter",
                                 "params":{"stress_drop":0.8}}},
      "oracle":{"target_survival":0.95,"min_active_dsurv":0.0,
                "qty_cap_mult":1.5,"refresh_seconds":300}
    },
    "assets":[
      {"asset":"BTC",
       "oracle":{"target_survival":0.65,"qty_cap_mult":3.0},
       "strategy":{"exchange":"hyperliquid","symbol":"BTC/USDC","qty":"0.0003",
                   "grid_interval":[0.15,0.18],
                   "accumulation_buffer":[0.04,0.35],"cpu_priority":10},
       "overlay":[
         {"name":"carry_overlay",
          "params":{"start_ratio_pct":25,"add_threshold_pct":5,
                    "max_ratio_pct":75,"short_tp_roe":5,
                    "recycle_base_share_pct":50},
          "inventory":[{"exchange":"kraken","symbol":"BTC/USD"}]},
         {"name":"long_overlay","symbol":"BTC",
          "params":{"stress_drop":80}}]},
      {"asset":"ETH",
       "strategy":{"exchange":"kraken","symbol":"ETH/USD","qty":"0.0045",
                   "grid_interval":[2.5,10.0]},
       "overlay":[
         {"name":"carry_overlay",
          "params":{"start_ratio_pct":40,"add_threshold_pct":5,
                    "max_ratio_pct":75,"short_tp_roe":5},
          "inventory":[{"exchange":"hyperliquid","symbol":"ETH/USDC"}]}]},
      {"asset":"BOTZ",
       "oracle":{"target_survival":0.7,"qty_cap_mult":2.0},
       "strategy":{"exchange":"alpaca","symbol":"BOTZ","qty":"0.28",
                   "grid_interval":[0.15,0.25]}}
    ]
  }}|}
;;

let test_grouped_trading_flattens_and_merges () =
  let json = Yojson.Basic.from_string grouped_json in
  let entries =
    match Dio_engine.Config.trading_entries_json json with
    | `List es -> List.map Dio_engine.Config.parse_config es
    | _ -> Alcotest.fail "expected a flattened trading list"
  in
  Alcotest.(check int) "strategy + inventory legs" 5 (List.length entries);
  let btc_strategy =
    List.find
      (fun (t : Dio_engine.Config.trading_config) -> String.equal t.symbol "BTC/USDC")
      entries
  in
  Alcotest.(check string)
    "strategy from venue defaults"
    "jacobs_ladder"
    btc_strategy.strategy;
  Alcotest.(check string) "sell_mult from venue defaults" "0.98" btc_strategy.sell_mult;
  Alcotest.(check (pair (float 1e-9) (float 1e-9)))
    "asset buffer overrides venue default"
    (0.04, 0.35)
    btc_strategy.accumulation_buffer;
  Alcotest.(check int) "cpu priority" 10 btc_strategy.cpu_priority;
  let inv =
    List.find
      (fun (t : Dio_engine.Config.trading_config) -> String.equal t.symbol "BTC/USD")
      entries
  in
  Alcotest.(check string) "inventory leg has no strategy" "" inv.strategy;
  let botz =
    List.find
      (fun (t : Dio_engine.Config.trading_config) -> String.equal t.symbol "BOTZ")
      entries
  in
  Alcotest.(check string) "alpaca strategy default" "jacobs_ladder" botz.strategy;
  Alcotest.(check string) "alpaca sell_mult default" "0.999" botz.sell_mult;
  Alcotest.(check (pair (float 1e-9) (float 1e-9)))
    "alpaca buffer default"
    (0.05, 0.4)
    botz.accumulation_buffer;
  Alcotest.(check bool) "alpaca testnet default" true botz.testnet;
  Alcotest.(check bool) "alpaca sell_levels default" true botz.sell_levels;
  Alcotest.(check (option string)) "alpaca data feed default" (Some "iex") botz.data_feed;
  (* Overlay params are declared once on the group and attached to every leg. *)
  Dio_engine.Config.parse_overlays json;
  let btc =
    Dio_engine.Config.get_overlay_controller ~exchange:"hyperliquid" ~symbol:"BTC/USDC"
  in
  (match btc with
   | None -> Alcotest.fail "expected the BTC controller"
   | Some c ->
     Alcotest.(check int) "two BTC legs" 2 (List.length c.oc_legs);
     Alcotest.(check (float 1e-9)) "start ratio" 25.0 c.oc_params.start_ratio_pct;
     Alcotest.(check (float 1e-9)) "base share" 50.0 c.oc_params.recycle_base_share_pct;
     Alcotest.(check bool) "hl spot leg" true c.oc_has_hl_spot_leg);
  let eth =
    Dio_engine.Config.get_overlay_controller ~exchange:"hyperliquid" ~symbol:"ETH/USDC"
  in
  (match eth with
   | None -> Alcotest.fail "expected the ETH controller"
   | Some c ->
     Alcotest.(check int) "two ETH legs" 2 (List.length c.oc_legs);
     Alcotest.(check bool) "hl inventory leg" true c.oc_has_hl_spot_leg);
  Alcotest.(check (option string))
    "cross-venue leg resolves its controller"
    (Some "carry_overlay")
    (Dio_engine.Config.get_overlay_strategy ~exchange:"kraken" ~symbol:"BTC/USD")
;;

let test_grouped_oracle_inline () =
  let json = Yojson.Basic.from_string grouped_json in
  match Dio_engine.Config.parse_oracle_config json with
  | None -> Alcotest.fail "grouped oracle should parse"
  | Some c ->
    Alcotest.(check (float 1e-9)) "global target" 0.95 c.target_survival;
    Alcotest.(check (float 1e-9)) "global qty cap" 1.5 c.qty_cap_mult;
    Alcotest.(check (float 1e-9)) "refresh" 300.0 c.refresh_seconds;
    Alcotest.(check int) "two asset overrides" 2 (List.length c.assets);
    let btc = List.assoc "BTC/USDC" c.assets in
    Alcotest.(check (option (float 1e-9))) "BTC target" (Some 0.65) btc.target_survival;
    Alcotest.(check (option (float 1e-9))) "BTC qty cap" (Some 3.0) btc.qty_cap_mult;
    let botz = List.assoc "BOTZ" c.assets in
    Alcotest.(check (option (float 1e-9))) "BOTZ target" (Some 0.7) botz.target_survival;
    Alcotest.(check (option (float 1e-9)))
      "unset min_active_dsurv stays unset"
      None
      botz.min_active_dsurv
;;

let test_grouped_domain_assets () =
  let json = Yojson.Basic.from_string grouped_json in
  let entries =
    match Dio_engine.Config.trading_entries_json json with
    | `List es -> List.map Dio_engine.Config.parse_config es
    | _ -> []
  in
  Dio_engine.Config.parse_overlays json;
  let assets = Dio_engine.Config.domain_assets entries in
  Alcotest.(check int)
    "three grids plus the synthesized HL ETH controller"
    4
    (List.length assets);
  let symbols =
    List.map
      (fun (t : Dio_engine.Config.trading_config) -> t.exchange ^ ":" ^ t.symbol)
      assets
  in
  Alcotest.(check (list string))
    "strategy order then synthesized controllers"
    [ "hyperliquid:BTC/USDC"; "kraken:ETH/USD"; "alpaca:BOTZ"; "hyperliquid:ETH/USDC" ]
    symbols
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

let test_parse_flow_log_config () =
  let json =
    Yojson.Basic.from_string
      {|{"flow_log": {"enabled": true, "min_interval_seconds": 0.5, "heartbeat_seconds": 2.0, "pulse_seconds": 0.0}}|}
  in
  let c = Dio_engine.Config.parse_flow_log_config json in
  Alcotest.(check bool) "enabled" true c.enabled;
  Alcotest.(check (float 0.0001)) "min_interval" 0.5 c.min_interval_seconds;
  Alcotest.(check (float 0.0001)) "heartbeat" 2.0 c.heartbeat_seconds;
  Alcotest.(check (float 0.0001)) "pulse" 0.0 c.pulse_seconds;
  Alcotest.(check bool) "price_ticks defaults on" true c.price_ticks;
  (* An absent section is disabled unless the DIO_FLOW_LOG demo switch is set. *)
  let absent = Dio_engine.Config.parse_flow_log_config (Yojson.Basic.from_string "{}") in
  Alcotest.(check bool)
    "absent follows env switch"
    (Dio_engine.Config.flow_log_env_enabled ())
    absent.enabled
;;

let test_read_config_defaults () =
  (* Precondition: config.json absent. *)
  let config = Dio_engine.Config.read_config () in
  Alcotest.(check bool) "default logging level" true (config.logging.level = Logging.INFO);
  Alcotest.(check (list string)) "default logging sections" [] config.logging.sections;
  Alcotest.(check bool) "empty trading config" true (config.trading = [])
;;

let test_parse_oracle_config_full () =
  let json_str =
    {|{"oracle": {"qty_cap_mult": 2.0, "target_survival": 0.9, "min_active_dsurv": 0.5, "lookback_days": 180, "refresh_seconds": 60.0}}|}
  in
  let json = Yojson.Basic.from_string json_str in
  match Dio_engine.Config.parse_oracle_config json with
  | None -> Alcotest.fail "oracle section should parse to Some"
  | Some c ->
    Alcotest.(check (float 0.0001)) "qty_cap_mult" 2.0 c.qty_cap_mult;
    Alcotest.(check (float 0.0001)) "refresh_seconds" 60.0 c.refresh_seconds;
    Alcotest.(check (float 0.0001)) "target_survival" 0.9 c.target_survival;
    Alcotest.(check (float 0.0001)) "min_active_dsurv" 0.5 c.min_active_dsurv;
    Alcotest.(check int) "lookback_days" 180 c.lookback_days
;;

let test_parse_oracle_config_partial_defaults () =
  (* A partial section falls back to the runtime defaults for absent keys. *)
  let json_str = {|{"oracle": {"qty_cap_mult": 3.0}}|} in
  let json = Yojson.Basic.from_string json_str in
  match Dio_engine.Config.parse_oracle_config json with
  | None -> Alcotest.fail "oracle section should parse to Some"
  | Some c ->
    let d = Dio_oracle.Oracle_runtime.default_config () in
    Alcotest.(check (float 0.0001)) "qty_cap_mult override" 3.0 c.qty_cap_mult;
    Alcotest.(check (float 0.0001))
      "refresh_seconds default"
      d.refresh_seconds
      c.refresh_seconds;
    Alcotest.(check (float 0.0001))
      "target_survival default"
      d.target_survival
      c.target_survival;
    Alcotest.(check (float 0.0001))
      "min_active_dsurv default"
      d.min_active_dsurv
      c.min_active_dsurv
;;

let test_parse_oracle_config_absent () =
  let json = Yojson.Basic.from_string {|{"trading": []}|} in
  match Dio_engine.Config.parse_oracle_config json with
  | None -> ()
  | Some _ -> Alcotest.fail "absent oracle section should parse to None"
;;

let test_parse_oracle_config_default_qty_cap_mult () =
  (* The spec default caps buy sizing at 1.5x the configured qty. *)
  let d = Dio_oracle.Oracle_runtime.default_config () in
  Alcotest.(check (float 0.0001)) "qty_cap_mult default" 1.5 d.qty_cap_mult;
  (* Two years: long enough that the window still contains the current cycle's high for
     most assets (a 1y window silently drops it), short enough that a dead cycle no longer
     sets today's floor. Per-asset overrides go on the asset group. *)
  Alcotest.(check int) "lookback_days default" 730 d.lookback_days
;;

let test_parse_oracle_config_assets () =
  let json_str =
    {|{"oracle": {"target_survival": 0.95, "assets": {
       "HYPE/USDC": {"target_survival": 0.98, "qty_cap_mult": 3.0, "lookback_days": 90},
       "QQQ": {"min_active_dsurv": 0.5}}}}|}
  in
  let json = Yojson.Basic.from_string json_str in
  match Dio_engine.Config.parse_oracle_config json with
  | None -> Alcotest.fail "oracle section should parse to Some"
  | Some c ->
    Alcotest.(check (float 0.0001)) "global target_survival" 0.95 c.target_survival;
    Alcotest.(check int) "two overrides" 2 (List.length c.assets);
    let hype = List.assoc "HYPE/USDC" c.assets in
    let qqq = List.assoc "QQQ" c.assets in
    Alcotest.(check (option (float 0.0001)))
      "HYPE target_survival"
      (Some 0.98)
      hype.target_survival;
    Alcotest.(check (option (float 0.0001)))
      "HYPE qty_cap_mult"
      (Some 3.0)
      hype.qty_cap_mult;
    Alcotest.(check (option (float 0.0001)))
      "HYPE min_active_dsurv absent"
      None
      hype.min_active_dsurv;
    Alcotest.(check (option int)) "HYPE lookback_days" (Some 90) hype.lookback_days;
    Alcotest.(check (option int)) "QQQ lookback_days absent" None qqq.lookback_days;
    Alcotest.(check (option (float 0.0001)))
      "QQQ min_active_dsurv"
      (Some 0.5)
      qqq.min_active_dsurv
;;

let test_oracle_asset_keys_validation () =
  (* Asset override entries accept only the per-asset knobs; global cadence knobs and
     unknown keys are rejected. *)
  let allowed = Dio_engine.Config.known_oracle_asset_keys in
  let good = Yojson.Basic.from_string {|{"target_survival": 0.98}|} in
  let bad = Yojson.Basic.from_string {|{"refresh_seconds": 60.0}|} in
  let also_bad = Yojson.Basic.from_string {|{"no_such_knob": 1}|} in
  Alcotest.(check bool)
    "sizing knobs accepted"
    false
    (Dio_engine.Config.validate_keys ~context:"t" ~allowed good);
  Alcotest.(check bool)
    "cadence knob rejected"
    true
    (Dio_engine.Config.validate_keys ~context:"t" ~allowed bad);
  Alcotest.(check bool)
    "unknown knob rejected"
    true
    (Dio_engine.Config.validate_keys ~context:"t" ~allowed also_bad)
;;

let test_latency_spike_report_parse () =
  let open Dio_engine.Config in
  Alcotest.(check bool)
    "internal"
    true
    (latency_spike_report_of_string "internal" = Spike_report_internal);
  Alcotest.(check bool)
    "network"
    true
    (latency_spike_report_of_string "network" = Spike_report_network);
  Alcotest.(check bool)
    "both"
    true
    (latency_spike_report_of_string "BOTH" = Spike_report_both);
  Alcotest.(check bool)
    "off"
    true
    (latency_spike_report_of_string "off" = Spike_report_none);
  Alcotest.(check bool)
    "unknown falls back to internal"
    true
    (latency_spike_report_of_string "bogus" = Spike_report_internal);
  Alcotest.(check bool) "both reports internal" true (reports_internal Spike_report_both);
  Alcotest.(check bool)
    "internal does not report network"
    false
    (reports_network Spike_report_internal)
;;

let test_to_float_opt_accepts_int () =
  (* Regression: integer JSON literals ("latency_spike_threshold_us": 10) must not crash
     startup via an uncaught Yojson Type_error from [to_float_option]. *)
  let open Dio_engine.Config in
  Alcotest.(check (option (float 1e-9)))
    "int accepted"
    (Some 10.0)
    (to_float_opt (`Int 10));
  Alcotest.(check (option (float 1e-9)))
    "float accepted"
    (Some 20000.0)
    (to_float_opt (`Float 20000.0));
  Alcotest.(check (option (float 1e-9))) "null absent" None (to_float_opt `Null);
  Alcotest.(check (option (float 1e-9))) "string absent" None (to_float_opt (`String "x"))
;;

let test_parse_trading_config_cpu_priority () =
  let default =
    Dio_engine.Config.parse_config
      (Yojson.Basic.from_string {|{"symbol": "X/USD", "qty": "1", "strategy": "Ladder"}|})
  in
  Alcotest.(check int) "cpu_priority defaults to 0" 0 default.cpu_priority;
  let explicit =
    Dio_engine.Config.parse_config
      (Yojson.Basic.from_string
         {|{"symbol": "Y/USD", "qty": "1", "strategy": "Ladder", "cpu_priority": 7}|})
  in
  Alcotest.(check int) "explicit cpu_priority parsed" 7 explicit.cpu_priority
;;

let test_parse_trading_config_overlay_key () =
  (* The optional [overlay] object belongs to the trading entry and is accepted here; the
     side-effecting binding is done by [parse_overlays]. *)
  let config =
    Dio_engine.Config.parse_config
      (Yojson.Basic.from_string
         {|{"symbol": "BTC/USDC", "exchange": "hyperliquid", "qty": "1",
            "strategy": "Ladder", "overlay": {"strategy": "carry_overlay"}}|})
  in
  Alcotest.(check string) "symbol" "BTC/USDC" config.symbol;
  Alcotest.(check string) "strategy" "Ladder" config.strategy
;;

let cross_venue_json =
  {|{"trading":[
    {"symbol":"BTC/USD","exchange":"Kraken","overlay":{
      "strategy":"carry_overlay","exchange":"hyperliquid",
      "params":{"start_ratio_pct":25,"add_threshold_pct":7.5,"max_ratio_pct":75,
                "short_tp_roe":1.5,"recycle_base_share_pct":50,
                "min_entry_margin_ratio":2.0,"min_liq_mult":2.5}}}]}|}
;;

let test_overlay_cross_venue () =
  let json = Yojson.Basic.from_string cross_venue_json in
  Dio_engine.Config.parse_overlays json;
  let c =
    Dio_engine.Config.get_overlay_controller ~exchange:"hyperliquid" ~symbol:"BTC/USDC"
  in
  (match c with
   | None -> Alcotest.fail "expected a hyperliquid/BTC controller"
   | Some c ->
     Alcotest.(check string) "hedge coin" "BTC" c.oc_hedge_coin;
     Alcotest.(check bool) "no hl spot leg" false c.oc_has_hl_spot_leg;
     Alcotest.(check int) "one leg" 1 (List.length c.oc_legs);
     Alcotest.(check bool)
       "inventory-only kraken leg"
       true
       (List.exists
          (fun (l : Dio_engine.Config.overlay_leg) ->
            String.equal l.ol_exchange "kraken"
            && String.equal l.ol_symbol "BTC/USD"
            && not l.ol_has_grid)
          c.oc_legs));
  Alcotest.(check bool)
    "the inventory leg is not a controller"
    true
    (Option.is_none
       (Dio_engine.Config.get_overlay_controller ~exchange:"kraken" ~symbol:"BTC/USD"));
  Alcotest.(check (option string))
    "strategy resolves through the leg"
    (Some "carry_overlay")
    (Dio_engine.Config.get_overlay_strategy ~exchange:"kraken" ~symbol:"BTC/USD");
  Alcotest.(check (float 1e-9))
    "params resolve through the hedge"
    25.0
    (Dio_engine.Config.get_overlay_params ~exchange:"hyperliquid" ~symbol:"BTC/USDC")
      .Dio_engine.Config.start_ratio_pct;
  Alcotest.(check (float 1e-9))
    "params resolve through the leg"
    25.0
    (Dio_engine.Config.get_overlay_params ~exchange:"kraken" ~symbol:"BTC/USD")
      .Dio_engine.Config.start_ratio_pct
;;

let test_overlay_merges_legs () =
  let json =
    Yojson.Basic.from_string
      {|{"trading":[
        {"symbol":"BTC/USD","exchange":"kraken","overlay":{
          "strategy":"carry_overlay","exchange":"hyperliquid",
          "params":{"start_ratio_pct":25,"max_ratio_pct":75}}},
        {"symbol":"BTC/USDC","exchange":"hyperliquid","strategy":"jacobs_ladder",
         "overlay":{"strategy":"carry_overlay","exchange":"hyperliquid",
          "params":{"start_ratio_pct":25,"max_ratio_pct":75}}}]}|}
  in
  Dio_engine.Config.parse_overlays json;
  let c =
    Dio_engine.Config.get_overlay_controller ~exchange:"hyperliquid" ~symbol:"BTC/USDC"
  in
  (match c with
   | None -> Alcotest.fail "expected one merged controller"
   | Some c ->
     Alcotest.(check int) "two legs" 2 (List.length c.oc_legs);
     Alcotest.(check bool) "hl spot leg present" true c.oc_has_hl_spot_leg);
  let entries =
    match json |> Yojson.Basic.Util.member "trading" with
    | `List es -> List.map Dio_engine.Config.parse_config es
    | _ -> []
  in
  let assets = Dio_engine.Config.domain_assets entries in
  Alcotest.(check int) "only the hl grid domain is spawned" 1 (List.length assets);
  Alcotest.(check string)
    "controller attaches to the hl grid"
    "BTC/USDC"
    (List.hd assets).Dio_engine.Config.symbol
;;

let test_domain_assets_spawns_overlay_only () =
  let json = Yojson.Basic.from_string cross_venue_json in
  Dio_engine.Config.parse_overlays json;
  let entries =
    match json |> Yojson.Basic.Util.member "trading" with
    | `List es -> List.map Dio_engine.Config.parse_config es
    | _ -> []
  in
  let assets = Dio_engine.Config.domain_assets entries in
  Alcotest.(check int) "one synthetic controller domain" 1 (List.length assets);
  let a = List.hd assets in
  Alcotest.(check string) "controller venue" "hyperliquid" a.Dio_engine.Config.exchange;
  Alcotest.(check string) "controller symbol" "BTC/USDC" a.Dio_engine.Config.symbol;
  Alcotest.(check string) "controller runs no grid" "" a.Dio_engine.Config.strategy
;;

let test_inventory_formula () =
  let q = Dio_engine.Overlay_inventory.q_of_balances in
  Alcotest.(check (float 1e-9))
    "no sells: free + staked"
    10.0
    (q ~full_balance_ref:false ~free:8.0 ~total:10.0 ~open_sell_base:0.0);
  Alcotest.(check (float 1e-9))
    "staked backing a pending sell is excluded"
    8.0
    (q ~full_balance_ref:false ~free:8.0 ~total:10.0 ~open_sell_base:2.0);
  Alcotest.(check (float 1e-9))
    "sell larger than staked excludes all staked"
    8.0
    (q ~full_balance_ref:false ~free:8.0 ~total:10.0 ~open_sell_base:9.0);
  Alcotest.(check (float 1e-9))
    "all inventory earn-backed"
    0.0
    (q ~full_balance_ref:false ~free:0.0 ~total:5.0 ~open_sell_base:5.0);
  Alcotest.(check (float 1e-9))
    "full balance ref uses total holdings"
    10.0
    (q ~full_balance_ref:true ~free:8.0 ~total:10.0 ~open_sell_base:2.0)
;;

let test_inventory_only_hedge_venue_leg () =
  (* A balance-only inventory leg sitting on the hedge venue shares the controller's
     exchange/symbol. It must still be classified as inventory-only (balances section),
     not as an active strategy: the predicate follows the leg's own [ol_has_grid]. *)
  let json =
    Yojson.Basic.from_string
      {|{"trading":[
        {"symbol":"ETH/USD","exchange":"kraken","strategy":"jacobs_ladder","overlay":{
          "strategy":"carry_overlay","exchange":"hyperliquid",
          "params":{"start_ratio_pct":40,"max_ratio_pct":75}}},
        {"symbol":"ETH/USDC","exchange":"hyperliquid","overlay":{
          "strategy":"carry_overlay","exchange":"hyperliquid",
          "params":{"start_ratio_pct":40,"max_ratio_pct":75}}}]}|}
  in
  Dio_engine.Config.parse_overlays json;
  Alcotest.(check bool)
    "kraken grid is a strategy"
    false
    (Dio_engine.Config.is_inventory_only ~exchange:"kraken" ~symbol:"ETH/USD");
  Alcotest.(check bool)
    "hl spot leg is inventory-only"
    true
    (Dio_engine.Config.is_inventory_only ~exchange:"hyperliquid" ~symbol:"ETH/USDC");
  Alcotest.(check bool)
    "unconfigured symbol is not a leg"
    false
    (Dio_engine.Config.is_inventory_only ~exchange:"hyperliquid" ~symbol:"BTC/USDC")
;;

let test_read_config_keeps_last_good () =
  (* A deploy that copies config.json over a live process can expose a partial file to the
     10 s monitor re-read or to shutdown. Once a config has parsed, a transient failure
     must return the cached one instead of raising into the caller's Lwt fiber. *)
  let dir = Filename.temp_dir "dio-config-" "" in
  let saved_cwd = Sys.getcwd () in
  let restore () =
    Sys.chdir saved_cwd;
    (try Sys.remove (Filename.concat dir "config.json") with
     | _ -> ());
    try Unix.rmdir dir with
    | _ -> ()
  in
  Fun.protect ~finally:restore (fun () ->
    Sys.chdir dir;
    let write s =
      Out_channel.with_open_bin "config.json" (fun oc -> Out_channel.output_string oc s)
    in
    write {|{"trading": []}|};
    let first = Dio_engine.Config.read_config () in
    Alcotest.(check int) "initial read" 10000 first.cycle_mod;
    (* Mid-copy: syntactically incomplete. *)
    write {|{"trading": [|};
    let during_copy = Dio_engine.Config.read_config () in
    Alcotest.(check int)
      "partial copy falls back to last known good"
      10000
      during_copy.cycle_mod;
    write {|{"trading": [], "cycle_mod": 4242}|};
    let recovered = Dio_engine.Config.read_config () in
    Alcotest.(check int) "complete file replaces the fallback" 4242 recovered.cycle_mod)
;;

let test_short_overlay_instance () =
  let entry =
    {|{"symbol":"HYPE/USDC","exchange":"hyperliquid","qty":"0.15",
       "strategy":"jacobs_ladder",
       "overlay":[{"name":"short_overlay","exchange":"hyperliquid",
                    "params":{"short_pct":25,"stress_rise":80}}]}|}
  in
  ignore (Dio_engine.Config.parse_config (Yojson.Basic.from_string entry));
  Dio_engine.Config.parse_overlay_instances
    (Yojson.Basic.from_string (Printf.sprintf {|{"trading":[%s]}|} entry));
  (match
     Dio_engine.Config.get_overlay_instances_for
       ~exchange:"hyperliquid"
       ~symbol:"HYPE/USDC"
   with
   | [ oi ] ->
     Alcotest.(check string) "strategy" "short_overlay" oi.Dio_engine.Config.oi_strategy;
     Alcotest.(check string)
       "overlay venue"
       "hyperliquid"
       oi.Dio_engine.Config.oi_exchange;
     Alcotest.(check string)
       "symbol defaults to the owner's base coin"
       "HYPE"
       oi.Dio_engine.Config.oi_symbol;
     let open Yojson.Basic.Util in
     Alcotest.(check (float 1e-9))
       "short_pct"
       25.0
       (oi.Dio_engine.Config.oi_params
        |> member "short_pct"
        |> Dio_engine.Config.to_float_opt
        |> Option.value ~default:0.0)
   | _ -> Alcotest.fail "expected one short_overlay instance for hyperliquid/HYPE/USDC");
  Alcotest.(check bool)
    "short_overlay is not the inventory controller"
    true
    (match
       Dio_engine.Config.get_overlay_controller
         ~exchange:"hyperliquid"
         ~symbol:"HYPE/USDC"
     with
     | None -> true
     | Some _ -> false)
;;

let test_parse_overlay_instances () =
  let entry =
    {|{"symbol":"BTC/USDC","exchange":"hyperliquid","qty":"0.001",
       "strategy":"jacobs_ladder",
       "overlays":[{"name":"long_overlay","exchange":"lighter",
                    "params":{"long_pct":0.25,"stress_drop":0.8}}]}|}
  in
  ignore (Dio_engine.Config.parse_config (Yojson.Basic.from_string entry));
  Dio_engine.Config.parse_overlay_instances
    (Yojson.Basic.from_string (Printf.sprintf {|{"trading":[%s]}|} entry));
  (match
     Dio_engine.Config.get_overlay_instances_for
       ~exchange:"hyperliquid"
       ~symbol:"BTC/USDC"
   with
   | [ oi ] ->
     Alcotest.(check string)
       "id defaults to the strategy name"
       "long_overlay"
       oi.Dio_engine.Config.oi_id;
     Alcotest.(check string) "strategy" "long_overlay" oi.Dio_engine.Config.oi_strategy;
     Alcotest.(check string) "overlay venue" "lighter" oi.Dio_engine.Config.oi_exchange;
     Alcotest.(check string)
       "symbol defaults to the owner's base coin"
       "BTC"
       oi.Dio_engine.Config.oi_symbol;
     let open Yojson.Basic.Util in
     Alcotest.(check (float 1e-9))
       "param"
       0.25
       (oi.Dio_engine.Config.oi_params
        |> member "long_pct"
        |> Dio_engine.Config.to_float_opt
        |> Option.value ~default:0.0)
   | _ -> Alcotest.fail "expected one overlay instance for hyperliquid/BTC/USDC");
  Alcotest.(check int)
    "other owners have none"
    0
    (List.length
       (Dio_engine.Config.get_overlay_instances_for
          ~exchange:"hyperliquid"
          ~symbol:"HYPE/USDC"))
;;

let test_parse_overlay_instances_grouped () =
  let json =
    Yojson.Basic.from_string
      {|{"trading":{
        "defaults":{"strategy":{"hyperliquid":{"name":"jacobs_ladder",
                                                "sell_mult":"0.98"}}},
        "assets":[{"asset":"BTC",
          "strategy":{"exchange":"hyperliquid","symbol":"BTC/USDC","qty":"0.001",
                      "grid_interval":[0.1,0.2]},
          "overlay":[
            {"name":"carry_overlay","exchange":"hyperliquid",
             "params":{"start_ratio_pct":25}},
            {"name":"long_overlay","exchange":"lighter",
             "params":{"stress_drop":0.8}}]}]}}|}
  in
  Dio_engine.Config.parse_overlay_instances json;
  (match
     Dio_engine.Config.get_overlay_instances_for
       ~exchange:"hyperliquid"
       ~symbol:"BTC/USDC"
   with
   | [ oi ] ->
     Alcotest.(check string)
       "grouped asset owner"
       "BTC/USDC"
       oi.Dio_engine.Config.oi_owner_symbol;
     let open Yojson.Basic.Util in
     Alcotest.(check (float 1e-9))
       "grouped param"
       0.8
       (oi.Dio_engine.Config.oi_params
        |> member "stress_drop"
        |> Dio_engine.Config.to_float_opt
        |> Option.value ~default:0.0)
   | _ -> Alcotest.fail "expected one grouped overlay instance");
  match
    Dio_engine.Config.get_overlay_controller ~exchange:"hyperliquid" ~symbol:"BTC/USDC"
  with
  | Some c ->
    Alcotest.(check (float 1e-9))
      "controller start ratio"
      25.0
      c.oc_params.start_ratio_pct
  | None -> Alcotest.fail "expected the carry controller from the same overlay list"
;;

let test_parse_overlay_flat_params () =
  (* Older configs put the carry params directly on the overlay object. *)
  let json =
    Yojson.Basic.from_string
      {|{"trading":[{"symbol":"BTC/USD","exchange":"kraken",
         "overlay":{"name":"carry_overlay","exchange":"hyperliquid",
                    "start_ratio_pct":25}}]}|}
  in
  Dio_engine.Config.parse_overlays json;
  match
    Dio_engine.Config.get_overlay_controller ~exchange:"hyperliquid" ~symbol:"BTC/USDC"
  with
  | Some c ->
    Alcotest.(check (float 1e-9)) "flat start ratio" 25.0 c.oc_params.start_ratio_pct
  | None -> Alcotest.fail "expected a hyperliquid/BTC controller"
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
        ; Alcotest.test_case "cpu priority" `Quick test_parse_trading_config_cpu_priority
        ; Alcotest.test_case
            "overlay key accepted"
            `Quick
            test_parse_trading_config_overlay_key
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
    ; ( "grouped"
      , [ Alcotest.test_case
            "flattens and merges defaults"
            `Quick
            test_grouped_trading_flattens_and_merges
        ; Alcotest.test_case
            "oracle overrides are inline"
            `Quick
            test_grouped_oracle_inline
        ; Alcotest.test_case "domain assets" `Quick test_grouped_domain_assets
        ] )
    ; ( "overlays"
      , [ Alcotest.test_case "parse and bind" `Quick test_parse_overlays
        ; Alcotest.test_case "params defaults" `Quick test_parse_overlay_params_defaults
        ; Alcotest.test_case
            "repeated owner keys accepted"
            `Quick
            test_parse_overlay_accepts_repeated_owner
        ; Alcotest.test_case "cross-venue leg" `Quick test_overlay_cross_venue
        ; Alcotest.test_case "legs merge per hedge" `Quick test_overlay_merges_legs
        ; Alcotest.test_case
            "overlay-only domain spawned"
            `Quick
            test_domain_assets_spawns_overlay_only
        ; Alcotest.test_case "inventory formula" `Quick test_inventory_formula
        ; Alcotest.test_case
            "short_overlay is a generic instance"
            `Quick
            test_short_overlay_instance
        ; Alcotest.test_case "instances parse flat" `Quick test_parse_overlay_instances
        ; Alcotest.test_case
            "instances parse grouped"
            `Quick
            test_parse_overlay_instances_grouped
        ; Alcotest.test_case
            "flat params are hoisted"
            `Quick
            test_parse_overlay_flat_params
        ; Alcotest.test_case
            "hedge-venue inventory leg is balance-only"
            `Quick
            test_inventory_only_hedge_venue_leg
        ] )
    ; "flow_log", [ Alcotest.test_case "parse section" `Quick test_parse_flow_log_config ]
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
        ; Alcotest.test_case
            "per-asset override map parses"
            `Quick
            test_parse_oracle_config_assets
        ; Alcotest.test_case
            "asset override key validation"
            `Quick
            test_oracle_asset_keys_validation
        ] )
    ; ( "latency_spike_report"
      , [ Alcotest.test_case
            "parses and gates families"
            `Quick
            test_latency_spike_report_parse
        ; Alcotest.test_case
            "float config fields accept int JSON literals"
            `Quick
            test_to_float_opt_accepts_int
        ] )
    ; ( "file_handling"
      , [ Alcotest.test_case "config defaults" `Quick test_read_config_defaults
        ; Alcotest.test_case
            "transient partial copy keeps last known good"
            `Quick
            test_read_config_keeps_last_good
        ] )
    ]
;;
