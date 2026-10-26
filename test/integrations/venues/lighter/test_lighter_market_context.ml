open Alcotest

let json = Yojson.Safe.from_string

let reset_context () =
  Dio_exchange.Market_context.clear ~exchange:"lighter";
  Lighter.Positions.reset ()
;;

let test_market_stats () =
  reset_context ();
  Lighter.Instruments_feed.initialize [ "TESTBTC"; "TESTETH" ];
  Lighter.Market_stats.process_market_data
    (json
       {|{"channel":"market_stats:0","market_stats":{"symbol":"TESTBTC","market_id":0,"mark_price":"101.5","index_price":"100","current_funding_rate":"0.000125","premium":"0.05"}}|});
  (match Dio_exchange.Market_context.get_funding ~exchange:"lighter" ~coin:"TESTBTC" with
   | None -> fail "expected funding snapshot"
   | Some f ->
     check (float 1e-12) "funding" 0.000125 f.funding;
     check (float 1e-9) "premium" 0.0005 f.premium;
     check (float 1e-9) "mark" 101.5 f.mark;
     check (float 1e-9) "oracle" 100.0 f.oracle);
  (* All-market payloads are keyed by market id. *)
  Lighter.Market_stats.process_market_data
    (json
       {|{"channel":"market_stats:all","market_stats":{"1":{"symbol":"TESTETH","market_id":1,"mark_price":"2000","index_price":"1999","current_funding_rate":"-0.00001","premium":"-0.01"}}}|});
  (match Dio_exchange.Market_context.get_funding ~exchange:"lighter" ~coin:"TESTETH" with
   | None -> fail "expected all-market funding snapshot"
   | Some f ->
     check (float 1e-12) "funding" (-0.00001) f.funding;
     check (float 1e-9) "premium" (-0.0001) f.premium);
  (* Unknown channels and markless frames must not overwrite a valid snapshot. *)
  Lighter.Market_stats.process_market_data (json {|{"channel":"l2Book"}|});
  Lighter.Market_stats.process_market_data
    (json
       {|{"channel":"market_stats:0","market_stats":{"symbol":"TESTBTC","mark_price":"0","index_price":"100"}}|});
  match Dio_exchange.Market_context.get_funding ~exchange:"lighter" ~coin:"TESTBTC" with
  | None -> fail "snapshot disappeared"
  | Some f -> check (float 1e-12) "funding unchanged" 0.000125 f.funding
;;

let positions_frame =
  json
    {|{"channel":"account_all:1","positions":{"0":{"market_id":0,"symbol":"TESTBTC","sign":-1,"position":"0.5","avg_entry_price":"100","position_value":"50","unrealized_pnl":"-1","liquidation_price":"150","allocated_margin":"10","initial_margin_fraction":"0.1"}}}|}
;;

let user_stats_frame =
  json
    {|{"channel":"user_stats:1","stats":{"collateral":"100","portfolio_value":"90","available_balance":"80","leverage":"2","margin_usage":"20"}}|}
;;

let test_positions_and_margin () =
  reset_context ();
  Lighter.Instruments_feed.initialize ~maintenance_margin_fraction:1200.0 [ "TESTBTC" ];
  check
    bool
    "positions applied"
    true
    (Lighter.Positions.process_account_all positions_frame);
  (match Dio_exchange.Market_context.get_position ~exchange:"lighter" ~coin:"TESTBTC" with
   | None -> fail "expected position"
   | Some p ->
     check (float 1e-12) "qty" (-0.5) p.qty;
     check (float 1e-9) "entry" 100.0 p.entry;
     check (float 1e-9) "value" 50.0 p.position_value;
     check (float 1e-9) "margin" 10.0 p.margin_used;
     check (option (float 1e-9)) "liquidation" (Some 150.0) p.liquidation;
     check (option (float 1e-9)) "leverage" (Some 10.0) p.leverage);
  check
    bool
    "positions tick"
    true
    (Dio_exchange.Market_context.get_positions_updated_at ~exchange:"lighter" <> None);
  check
    bool
    "user stats applied"
    true
    (Lighter.Positions.process_user_stats user_stats_frame);
  (match Dio_exchange.Market_context.get_margin ~exchange:"lighter" with
   | None -> fail "expected margin"
   | Some m ->
     check (float 1e-9) "account value" 90.0 m.account_value;
     check (float 1e-9) "margin used" 20.0 m.margin_used;
     check (float 1e-9) "withdrawable" 80.0 m.withdrawable;
     check (float 1e-9) "maintenance" 6.0 m.maintenance_margin);
  (* Negative equity must survive, not fall back to collateral. *)
  ignore
    (Lighter.Positions.process_user_stats
       (json
          {|{"channel":"user_stats:1","stats":{"collateral":"100","portfolio_value":"-5","available_balance":"0"}}|}));
  (match Dio_exchange.Market_context.get_margin ~exchange:"lighter" with
   | None -> fail "expected negative margin snapshot"
   | Some m -> check (float 1e-9) "negative account value" (-5.0) m.account_value);
  (* A full snapshot with no positions is flat, not stale. *)
  check
    bool
    "flat applied"
    true
    (Lighter.Positions.process_account_all
       (json {|{"channel":"account_all:1","positions":{}}|}));
  check
    bool
    "flat"
    true
    (Dio_exchange.Market_context.get_position ~exchange:"lighter" ~coin:"TESTBTC" = None);
  (* Partial frames without a positions field must not clear the store. *)
  check
    bool
    "missing ignored"
    false
    (Lighter.Positions.process_account_all (json {|{"channel":"account_all:1"}|}))
;;

let test_rest_snapshot () =
  reset_context ();
  Lighter.Instruments_feed.initialize [ "TESTBTC" ];
  Lighter.Positions.process_rest_account
    (json
       {|{"code":200,"accounts":[{"positions":[{"market_id":0,"symbol":"TESTBTC","sign":1,"position":"2","avg_entry_price":"100","position_value":"200","unrealized_pnl":"4","liquidation_price":"0","allocated_margin":"20","initial_margin_fraction":"0.2"}],"total_asset_value":"1000","available_balance":"800","collateral":"900","cross_initial_margin_requirement":"200","cross_maintenance_margin_requirement":"25"}]}|});
  (match Dio_exchange.Market_context.get_position ~exchange:"lighter" ~coin:"TESTBTC" with
   | None -> fail "expected REST position"
   | Some p ->
     check (float 1e-9) "qty" 2.0 p.qty;
     check (float 1e-9) "value" 200.0 p.position_value;
     check (option (float 1e-9)) "liquidation" None p.liquidation);
  (match Dio_exchange.Market_context.get_margin ~exchange:"lighter" with
   | None -> fail "expected REST margin"
   | Some m ->
     check (float 1e-9) "account value" 1000.0 m.account_value;
     check (float 1e-9) "margin used" 200.0 m.margin_used;
     check (float 1e-9) "maintenance" 25.0 m.maintenance_margin;
     check (float 1e-9) "withdrawable" 800.0 m.withdrawable);
  match Dio_exchange.Market_context.get_margin_ratio ~exchange:"lighter" with
  | None -> fail "expected margin ratio"
  | Some r -> check (float 1e-9) "ratio" 40.0 r
;;

let test_open_orders () =
  Lighter.Executions_feed.clear_all_open_orders ();
  Lighter.Executions_feed.initialize [ "TESTBTC" ];
  Lighter.Executions_feed.inject_order
    ~symbol:"TESTBTC"
    ~order_id:"ctx-order"
    ~side:Lighter.Executions_feed.Buy
    ~qty:1.0
    ~price:100.0
    ();
  ignore Lighter.Module.Lighter_impl.name;
  (match Dio_exchange.Exchange_intf.Registry.get_market_context "lighter" with
   | None -> fail "lighter market context not registered"
   | Some (module Mc : Dio_exchange.Exchange_intf.Market_context.S) ->
     let orders = Mc.get_open_orders ~coin:"TESTBTC" in
     check int "context orders" 1 (List.length orders));
  let all = Lighter.Module.Lighter_impl.get_all_orders_for_asset ~asset:"TESTBTC" in
  check int "asset orders" 1 (List.length all)
;;

let test_ws_routing () =
  reset_context ();
  Lighter.Instruments_feed.initialize [ "TESTROUTE" ];
  ignore
    (Lighter.Ws.decode_text_frame
       (Yojson.Safe.to_string
          (json
             {|{"type":"update/account_all","channel":"account_all:1","positions":{"0":{"market_id":0,"symbol":"TESTROUTE","sign":1,"position":"1","avg_entry_price":"10","position_value":"10","allocated_margin":"2"}}}|})));
  check
    bool
    "position routed"
    true
    (Dio_exchange.Market_context.get_position ~exchange:"lighter" ~coin:"TESTROUTE"
     <> None);
  ignore
    (Lighter.Ws.decode_text_frame
       (Yojson.Safe.to_string
          (json
             {|{"type":"update/market_stats","channel":"market_stats:0","market_stats":{"symbol":"TESTROUTE","market_id":0,"mark_price":"10","index_price":"10","current_funding_rate":"0.0002"}}|})));
  check
    bool
    "funding routed"
    true
    (Dio_exchange.Market_context.get_funding ~exchange:"lighter" ~coin:"TESTROUTE" <> None)
;;

let test_oracle () =
  Lighter.Instruments_feed.initialize [ "TESTBTC" ];
  check string "quote" "USDC" Lighter.Oracle.default_quote;
  check (float 1e-9) "min notional" 1.0 (Lighter.Oracle.min_notional ~symbol:"TESTBTC");
  let maker, taker = Lighter.Oracle.default_fees ~symbol:"TESTBTC" in
  check (float 1e-9) "maker fee" 0.0 maker;
  check (float 1e-9) "taker fee" 0.0 taker;
  let bars =
    Lighter.Oracle.parse_candles
      (json
         {|{"code":200,"r":"1d","c":[{"t":1704153600000,"o":1.0,"h":2.0,"l":0.5,"c":1.5,"v":10.0}]}|})
  in
  (match bars with
   | [ bar ] ->
     check string "bar date" "2024-01-02" bar.Dio_exchange.Exchange_intf.Types.date;
     check (float 1e-9) "bar close" 1.5 bar.close
   | _ -> fail "expected one candle");
  (match
     Lighter.Oracle.parse_account_balances
       (json
          {|{"accounts":[{"assets":[{"symbol":"USDC","balance":"10","locked_balance":"2","margin_balance":"5"}],"collateral":"15","available_balance":"12","total_asset_value":"15"}]}|})
   with
   | Ok [ ("USDC", available, total) ] ->
     check (float 1e-9) "available" 13.0 available;
     check (float 1e-9) "total" 15.0 total
   | Ok _ -> fail "expected one USDC balance"
   | Error error -> fail error);
  (match
     Lighter.Oracle.parse_account_balances
       (json {|{"accounts":[{"assets":[],"total_asset_value":"-5"}]}|})
   with
   | Ok [ ("USDC", _, total) ] -> check (float 1e-9) "negative equity" (-5.0) total
   | Ok _ -> fail "expected negative USDC equity"
   | Error error -> fail error);
  (match Lighter.Oracle.parse_account_balances (json {|{"code":500}|}) with
   | Error _ -> ()
   | Ok _ -> fail "malformed account response must be an error");
  check
    int
    "from clip"
    0
    (List.length (Lighter.Oracle.clip_from (Some "2024-01-03") bars))
;;

let test_risk_parameters () =
  reset_context ();
  Lighter.Instruments_feed.initialize
    ~maintenance_margin_fraction:120.0
    ~min_initial_margin_fraction:200.0
    [ "TESTBTC" ];
  match Lighter.Market_context.Impl.get_risk ~coin:"TESTBTC" with
  | None -> fail "expected risk parameters"
  | Some r ->
    check (float 1e-9) "maintenance fraction" 0.012 r.maintenance_frac;
    check (option (float 1e-9)) "max leverage" (Some 50.0) r.max_leverage
;;

let test_capabilities () =
  ignore Lighter.Module.Lighter_impl.name;
  let module Registry = Dio_exchange.Exchange_intf.Registry in
  (match Registry.get_market_context "lighter" with
   | Some (module Mc : Dio_exchange.Exchange_intf.Market_context.S) ->
     check string "market context name" "lighter" Mc.name
   | None -> fail "market context capability missing");
  (match Registry.get_capital_reservation "lighter" with
   | Some (module Cr : Dio_exchange.Exchange_intf.Capital_reservation.S) ->
     check string "reservation name" "lighter" Cr.name;
     Cr.set ~owner:"test" ~amount:25.0;
     check (float 1e-9) "reserved" 25.0 (Cr.get_total ());
     check (float 1e-9) "available" 75.0 (Cr.available ~quote:100.0);
     Cr.release ~owner:"test";
     check (float 1e-9) "released" 0.0 (Cr.get_total ())
   | None -> fail "capital reservation capability missing");
  match Dio_exchange.Exchange_intf.Oracle.Registry.get "lighter" with
  | Some (module O : Dio_exchange.Exchange_intf.Oracle.S) ->
    check string "oracle name" "lighter" O.name
  | None -> fail "oracle capability missing"
;;

let () =
  Alcotest.run
    "Lighter Market Context"
    [ ( "market_stats"
      , [ test_case "funding and premium snapshots" `Quick test_market_stats ] )
    ; ( "positions"
      , [ test_case "positions and margin" `Quick test_positions_and_margin
        ; test_case "REST snapshot" `Quick test_rest_snapshot
        ; test_case "open orders" `Quick test_open_orders
        ; test_case "WS routing" `Quick test_ws_routing
        ] )
    ; ( "risk"
      , [ test_case "venue metadata" `Quick test_risk_parameters ] )
    ; "capabilities", [ test_case "registered" `Quick test_capabilities ]
    ; "oracle", [ test_case "fees, candles, balances" `Quick test_oracle ]
    ]
;;
