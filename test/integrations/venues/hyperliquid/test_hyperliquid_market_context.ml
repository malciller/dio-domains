open Alcotest

let json = Yojson.Safe.from_string

let test_active_asset_ctx () =
  Dio_exchange.Market_context.clear ~exchange:"hyperliquid";
  let frame =
    json
      {|{"channel":"activeAssetCtx","data":{"coin":"TESTBTC","ctx":{"funding":"0.0000125","premium":"-0.0002083","markPx":"81065","oraclePx":"81080"}}}|}
  in
  Hyperliquid.Asset_ctx.process_market_data frame;
  (match
     Dio_exchange.Market_context.get_funding ~exchange:"hyperliquid" ~coin:"TESTBTC"
   with
   | None -> fail "expected funding snapshot"
   | Some f ->
     check (float 1e-12) "funding" 0.0000125 f.funding;
     check (float 1e-9) "premium" (-0.0002083) f.premium;
     check (float 1e-6) "mark" 81065.0 f.mark;
     check (float 1e-6) "oracle" 81080.0 f.oracle);
  (match
     Dio_exchange.Market_context.annualized_funding
       ~exchange:"hyperliquid"
       ~coin:"TESTBTC"
   with
   | None -> fail "expected APR"
   | Some apr -> check (float 1e-9) "apr" (0.0000125 *. 24.0 *. 365.0) apr);
  (* Unknown channels and markless frames must not overwrite a valid snapshot. *)
  Hyperliquid.Asset_ctx.process_market_data
    (json {|{"channel":"l2Book","data":{"coin":"TESTBTC"}}|});
  Hyperliquid.Asset_ctx.process_market_data
    (json
       {|{"channel":"activeAssetCtx","data":{"coin":"TESTBTC","ctx":{"funding":"0.1","markPx":"0"}}}|});
  match
    Dio_exchange.Market_context.get_funding ~exchange:"hyperliquid" ~coin:"TESTBTC"
  with
  | None -> fail "snapshot disappeared"
  | Some f -> check (float 1e-12) "funding unchanged" 0.0000125 f.funding
;;

let test_positions () =
  Hyperliquid.Account_mode.set Hyperliquid.Account_mode.Unknown;
  Dio_exchange.Market_context.clear ~exchange:"hyperliquid";
  let frame =
    json
      {|{"channel":"webData2","data":{"clearinghouseState":{"marginSummary":{"accountValue":"5000","totalMarginUsed":"100","totalNtlPos":"200"},"crossMaintenanceMarginUsed":"50","withdrawable":"4500","assetPositions":[{"position":{"coin":"TESTBTC","szi":"-0.1","entryPx":"80000","positionValue":"8100","unrealizedPnl":"-100","marginUsed":"100","liquidationPx":"120000","leverage":{"type":"cross","value":10}},"type":"oneWay"}]}}}|}
  in
  check bool "applied" true (Hyperliquid.Positions.process_market_data frame);
  (match
     Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"TESTBTC"
   with
   | None -> fail "expected position"
   | Some p ->
     check (float 1e-12) "qty" (-0.1) p.qty;
     check (float 1e-6) "entry" 80000.0 p.entry;
     check (float 1e-6) "liq" 120000.0 (Option.value p.liquidation ~default:0.0);
     check (float 1e-6) "margin" 100.0 p.margin_used);
  (match Dio_exchange.Market_context.get_margin ~exchange:"hyperliquid" with
   | None -> fail "expected margin"
   | Some m ->
     check (float 1e-6) "account value" 5000.0 m.account_value;
     check (float 1e-6) "maintenance" 50.0 m.maintenance_margin);
  (match Dio_exchange.Market_context.get_margin_ratio ~exchange:"hyperliquid" with
   | None -> fail "expected margin ratio"
   | Some r -> check (float 1e-9) "ratio" 100.0 r);
  (* A full snapshot with no positions is flat, not stale. *)
  let flat =
    json
      {|{"channel":"webData2","data":{"clearinghouseState":{"marginSummary":{"accountValue":"5000","totalMarginUsed":"0","totalNtlPos":"0"},"crossMaintenanceMarginUsed":"0","withdrawable":"5000","assetPositions":[]}}}|}
  in
  check bool "flat applied" true (Hyperliquid.Positions.process_market_data flat);
  check
    bool
    "flat"
    true
    (Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"TESTBTC"
     = None);
  check
    bool
    "snapshot tick"
    true
    (Dio_exchange.Market_context.get_positions_updated_at ~exchange:"hyperliquid" <> None)
;;

let test_reservation () =
  Dio_exchange.Capital_reservation.release ~exchange:"test" ~owner:"a";
  Dio_exchange.Capital_reservation.release ~exchange:"test" ~owner:"b";
  Dio_exchange.Capital_reservation.set ~exchange:"test" ~owner:"a" ~amount:100.0;
  Dio_exchange.Capital_reservation.set ~exchange:"test" ~owner:"b" ~amount:50.0;
  check
    (float 1e-9)
    "total"
    150.0
    (Dio_exchange.Capital_reservation.get_total ~exchange:"test");
  check
    (float 1e-9)
    "available"
    850.0
    (Dio_exchange.Capital_reservation.available ~exchange:"test" ~quote:1000.0);
  check
    (float 1e-9)
    "clamped"
    0.0
    (Dio_exchange.Capital_reservation.available ~exchange:"test" ~quote:100.0);
  Dio_exchange.Capital_reservation.set ~exchange:"test" ~owner:"a" ~amount:0.0;
  check
    (float 1e-9)
    "release"
    50.0
    (Dio_exchange.Capital_reservation.get_total ~exchange:"test")
;;

let test_capabilities () =
  (* Force the exchange module (not just its aliases) to load so its capability
     registration runs. *)
  ignore Hyperliquid.Module.Hyperliquid_impl.name;
  let module Registry = Dio_exchange.Exchange_intf.Registry in
  (match Registry.get_market_context "hyperliquid" with
   | Some (module Mc : Dio_exchange.Exchange_intf.Market_context.S) ->
     Alcotest.(check string) "market context name" "hyperliquid" Mc.name
   | None -> Alcotest.fail "Hyperliquid market-context capability not registered");
  match Registry.get_capital_reservation "hyperliquid" with
  | Some (module Cr : Dio_exchange.Exchange_intf.Capital_reservation.S) ->
    Alcotest.(check string) "reservation name" "hyperliquid" Cr.name
  | None -> Alcotest.fail "Hyperliquid capital-reservation capability not registered"
;;

let test_flat_without_asset_positions () =
  Dio_exchange.Market_context.clear ~exchange:"hyperliquid";
  let frame =
    json
      {|{"channel":"webData2","data":{"clearinghouseState":{"marginSummary":{"accountValue":"5000","totalMarginUsed":"0","totalNtlPos":"0"},"crossMaintenanceMarginUsed":"0","withdrawable":"5000"}}}|}
  in
  check bool "applied flat frame" true (Hyperliquid.Positions.process_market_data frame);
  check
    bool
    "snapshot tick"
    true
    (Dio_exchange.Market_context.get_positions_updated_at ~exchange:"hyperliquid" <> None);
  check
    bool
    "no stale position"
    true
    (Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"TESTBTC"
     = None)
;;

let test_clearinghouse_state () =
  Hyperliquid.Account_mode.set Hyperliquid.Account_mode.Unknown;
  Dio_exchange.Market_context.clear ~exchange:"hyperliquid";
  let frame =
    json
      {|{"channel":"clearinghouseState","data":{"dex":"","user":"0xabc","clearinghouseState":{"assetPositions":[{"position":{"coin":"TESTHYPE","szi":"-1.5","entryPx":"80","positionValue":"120","unrealizedPnl":"0","marginUsed":"12","liquidationPx":"160","leverage":{"type":"cross","value":10}},"type":"oneWay"}],"marginSummary":{"accountValue":"1000","totalMarginUsed":"100","totalNtlPos":"200"},"crossMaintenanceMarginUsed":"50","withdrawable":"900"}}}|}
  in
  check
    bool
    "applied clearinghouseState"
    true
    (Hyperliquid.Positions.process_market_data frame);
  (match
     Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"TESTHYPE"
   with
   | None -> fail "expected clearinghouseState position"
   | Some p -> check (float 1e-9) "clearinghouse qty" (-1.5) p.qty);
  match Dio_exchange.Market_context.get_margin_ratio ~exchange:"hyperliquid" with
  | None -> fail "expected clearinghouseState margin"
  | Some r -> check (float 1e-9) "clearinghouse ratio" 20.0 r
;;

let test_unified_margin () =
  Hyperliquid.Account_mode.set Hyperliquid.Account_mode.Unified;
  Dio_exchange.Market_context.clear ~exchange:"hyperliquid";
  let store = Hyperliquid.Balances.get_balance_store "USDC" in
  Hyperliquid.Balances.BalanceStore.update_wallet
    store
    ~available:433.12
    ~total:490.16
    "spot"
    "account";
  let frame =
    json
      {|{"channel":"webData2","data":{"clearinghouseState":{"marginSummary":{"accountValue":"20.16763","totalMarginUsed":"16.839247","totalNtlPos":"200.6599"},"crossMaintenanceMarginUsed":"7.612937","withdrawable":"0.10164","assetPositions":[{"position":{"coin":"TESTBTC","szi":"-0.00078","entryPx":"82710","positionValue":"64.53486","unrealizedPnl":"0","marginUsed":"3.226743","liquidationPx":"0","leverage":{"type":"cross","value":20}},"type":"oneWay"},{"position":{"coin":"TESTHYPE","szi":"-1.67","entryPx":"81.472","positionValue":"136.12504","unrealizedPnl":"0","marginUsed":"13.612504","liquidationPx":"0","leverage":{"type":"cross","value":10}},"type":"oneWay"}]}}}|}
  in
  check
    bool
    "applied unified frame"
    true
    (Hyperliquid.Positions.process_market_data frame);
  (match Dio_exchange.Market_context.get_margin ~exchange:"hyperliquid" with
   | None -> fail "expected unified margin"
   | Some m ->
     check (float 1e-6) "unified equity" 490.16 m.account_value;
     check (float 1e-6) "unified maintenance" 7.612937 m.maintenance_margin;
     check (float 1e-6) "unified margin used" 16.839247 m.margin_used;
     check (float 1e-6) "unified withdrawable" 470.09401 m.withdrawable);
  (match Dio_exchange.Market_context.get_margin_ratio ~exchange:"hyperliquid" with
   | None -> fail "expected unified ratio"
   | Some r -> check (float 1e-6) "unified ratio" (490.16 /. 7.612937) r);
  (* A spot balance change republishes equity without waiting for a perp frame. *)
  Hyperliquid.Balances.BalanceStore.update_wallet
    store
    ~available:533.12
    ~total:590.16
    "spot"
    "account";
  Hyperliquid.Balances.notify_spot_update ();
  (match Dio_exchange.Market_context.get_margin ~exchange:"hyperliquid" with
   | None -> fail "expected republished margin"
   | Some m -> check (float 1e-6) "republished equity" 590.16 m.account_value);
  (* Isolated margin is not cross collateral: HL's unified formula subtracts it. *)
  let isolated_frame =
    json
      {|{"channel":"webData2","data":{"clearinghouseState":{"marginSummary":{"accountValue":"20.16763","totalMarginUsed":"66.839247","totalNtlPos":"200.6599"},"crossMaintenanceMarginUsed":"7.612937","withdrawable":"0.10164","assetPositions":[{"position":{"coin":"TESTBTC","szi":"-0.00078","entryPx":"82710","positionValue":"64.53486","unrealizedPnl":"0","marginUsed":"3.226743","liquidationPx":"0","leverage":{"type":"cross","value":20}},"type":"oneWay"},{"position":{"coin":"TESTHYPE","szi":"-1.67","entryPx":"81.472","positionValue":"136.12504","unrealizedPnl":"0","marginUsed":"13.612504","liquidationPx":"0","leverage":{"type":"cross","value":10}},"type":"oneWay"},{"position":{"coin":"TESTETH","szi":"-0.1","entryPx":"3000","positionValue":"300","unrealizedPnl":"0","marginUsed":"50","liquidationPx":"0","leverage":{"type":"isolated","value":5}},"type":"oneWay"}]}}}|}
  in
  check
    bool
    "applied isolated frame"
    true
    (Hyperliquid.Positions.process_market_data isolated_frame);
  (match Dio_exchange.Market_context.get_margin ~exchange:"hyperliquid" with
   | None -> fail "expected isolated margin"
   | Some m ->
     check (float 1e-6) "isolated equity" 540.16 m.account_value;
     check (float 1e-6) "isolated margin used" 66.839247 m.margin_used);
  (* Standard accounts keep spot and perp balances separate. *)
  Hyperliquid.Account_mode.set Hyperliquid.Account_mode.Standard;
  check
    bool
    "applied standard frame"
    true
    (Hyperliquid.Positions.process_market_data frame);
  (match Dio_exchange.Market_context.get_margin ~exchange:"hyperliquid" with
   | None -> fail "expected standard margin"
   | Some m -> check (float 1e-6) "standard per-dex equity" 20.16763 m.account_value);
  Hyperliquid.Account_mode.set Hyperliquid.Account_mode.Unknown
;;

let test_account_mode_strings () =
  let open Hyperliquid.Account_mode in
  check bool "unified" true (of_string "unifiedAccount" = Unified);
  check bool "portfolio" true (of_string "portfolioMargin" = Portfolio);
  check bool "disabled" true (of_string "disabled" = Standard);
  check bool "default" true (of_string "default" = Standard);
  check bool "unknown" true (of_string "something-else" = Unknown);
  check bool "spot collateral" true (uses_spot_collateral Unified);
  check bool "no spot collateral" false (uses_spot_collateral Standard)
;;

let test_replace_positions_scope () =
  Dio_exchange.Market_context.clear ~exchange:"hyperliquid";
  let pos coin qty =
    { Dio_exchange.Exchange_intf.Market_context.coin
    ; qty
    ; entry = 100.0
    ; position_value = 50.0
    ; unrealized_pnl = 0.0
    ; margin_used = 5.0
    ; liquidation = None
    ; leverage = Some 10.0
    ; updated_at = 0.0
    }
  in
  Dio_exchange.Market_context.update_position
    ~at:1.0
    ~exchange:"hyperliquid"
    (pos "xyz:TEST" (-0.5));
  (match
     Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"xyz:TEST"
   with
   | Some p -> check (float 1e-12) "builder qty" (-0.5) p.qty
   | None -> fail "expected builder position");
  (* A main-dex snapshot replaces only the main-dex coins. *)
  Dio_exchange.Market_context.replace_positions
    ~only:(fun c -> not (String.contains c ':'))
    ~exchange:"hyperliquid"
    ~updated_at:2.0
    [ pos "MAIN" 1.0 ];
  (match
     Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"MAIN"
   with
   | Some p -> check (float 1e-12) "main qty" 1.0 p.qty
   | None -> fail "expected main position");
  (match
     Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"xyz:TEST"
   with
   | Some _ -> ()
   | None -> fail "main snapshot wiped a builder position");
  (* A builder-dex snapshot replaces only its own namespace. *)
  Dio_exchange.Market_context.replace_positions
    ~only:(fun c -> String.starts_with ~prefix:"xyz:" c)
    ~exchange:"hyperliquid"
    ~updated_at:3.0
    [ pos "xyz:TEST" (-0.7) ];
  (match
     Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"MAIN"
   with
   | Some p -> check (float 1e-12) "main survives builder snapshot" 1.0 p.qty
   | None -> fail "builder snapshot wiped a main position");
  match
    Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"xyz:TEST"
  with
  | Some p -> check (float 1e-12) "builder refreshed" (-0.7) p.qty
  | None -> fail "expected builder position"
;;

let test_builder_publish_drops_closed () =
  Dio_exchange.Market_context.clear ~exchange:"hyperliquid";
  let pos coin =
    `Assoc
      [ ( "position"
        , `Assoc
            [ "coin", `String coin
            ; "szi", `String "-0.5"
            ; "entryPx", `String "100"
            ; "positionValue", `String "50"
            ; "unrealizedPnl", `String "0"
            ; "marginUsed", `String "5"
            ; "leverage", `Assoc [ "type", `String "cross"; "value", `Int 10 ]
            ] )
      ]
  in
  let frame positions = `Assoc [ "assetPositions", `List positions ] in
  Hyperliquid.Dex_positions.publish ~now:1.0 ~dex:"xyz" (frame [ pos "ANT" ]);
  (match
     Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"xyz:ANT"
   with
   | Some p -> check (float 1e-12) "builder qty" (-0.5) p.qty
   | None -> fail "expected builder position");
  (* A main-dex coin is outside this dex's namespace and must survive. *)
  Dio_exchange.Market_context.update_position
    ~at:1.5
    ~exchange:"hyperliquid"
    { Dio_exchange.Exchange_intf.Market_context.coin = "MAIN"
    ; qty = 1.0
    ; entry = 100.0
    ; position_value = 100.0
    ; unrealized_pnl = 0.0
    ; margin_used = 10.0
    ; liquidation = None
    ; leverage = Some 10.0
    ; updated_at = 0.0
    };
  (* The dex's whole set is now empty: its position is closed and is dropped. *)
  Hyperliquid.Dex_positions.publish ~now:2.0 ~dex:"xyz" (frame []);
  check
    bool
    "closed builder position dropped"
    true
    (Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"xyz:ANT"
     = None);
  match Dio_exchange.Market_context.get_position ~exchange:"hyperliquid" ~coin:"MAIN" with
  | Some p -> check (float 1e-12) "main survives" 1.0 p.qty
  | None -> fail "builder publish wiped a main position"
;;

let () =
  run
    "hyperliquid_market_context"
    [ "asset_ctx", [ test_case "activeAssetCtx" `Quick test_active_asset_ctx ]
    ; ( "positions"
      , [ test_case "webData2" `Quick test_positions
        ; test_case "replace scope per dex" `Quick test_replace_positions_scope
        ; test_case
            "builder publish drops a closed position"
            `Quick
            test_builder_publish_drops_closed
        ] )
    ; "reservation", [ test_case "quote reservation" `Quick test_reservation ]
    ; "capabilities", [ test_case "optional exchange-intf" `Quick test_capabilities ]
    ; ( "flat"
      , [ test_case "missing assetPositions" `Quick test_flat_without_asset_positions ] )
    ; "clearinghouse", [ test_case "clearinghouseState" `Quick test_clearinghouse_state ]
    ; "unified", [ test_case "spot-collateral margin" `Quick test_unified_margin ]
    ; "account_mode", [ test_case "abstraction strings" `Quick test_account_mode_strings ]
    ]
;;
