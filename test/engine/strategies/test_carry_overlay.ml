module Test_market_context : Dio_exchange.Exchange_intf.Market_context.S = struct
  let name = "hyperliquid"
  let get_funding ~coin = Dio_exchange.Market_context.get_funding ~exchange:name ~coin

  let get_positions_updated_at () =
    Dio_exchange.Market_context.get_positions_updated_at ~exchange:name
  ;;

  let get_position ~coin = Dio_exchange.Market_context.get_position ~exchange:name ~coin
  let get_margin () = Dio_exchange.Market_context.get_margin ~exchange:name
  let get_margin_ratio () = Dio_exchange.Market_context.get_margin_ratio ~exchange:name
end

module Test_capital_reservation : Dio_exchange.Exchange_intf.Capital_reservation.S =
struct
  let name = "hyperliquid"

  let set ~owner ~amount =
    Dio_exchange.Capital_reservation.set ~exchange:name ~owner ~amount
  ;;

  let release ~owner = Dio_exchange.Capital_reservation.release ~exchange:name ~owner
  let get_total () = Dio_exchange.Capital_reservation.get_total ~exchange:name
  let available ~quote = Dio_exchange.Capital_reservation.available ~exchange:name ~quote
end

let () =
  let module Registry = Dio_exchange.Exchange_intf.Registry in
  Registry.register_market_context (module Test_market_context);
  Registry.register_capital_reservation (module Test_capital_reservation)
;;

open Alcotest

let asset : Dio_strategies.Strategy_state.trading_config =
  { exchange = "hyperliquid"
  ; symbol = "TEST/USDC"
  ; qty = "1.0"
  ; grid_interval = 1.0
  ; sell_mult = "1.0"
  ; strategy = "carry_overlay"
  ; maker_fee = None
  ; taker_fee = None
  ; accumulation_buffer = 0.01
  ; base_accumulation = false
  ; sell_levels_persistence = false
  }
;;

let script =
  {|
strategy carry_test
version 1

when book.updates:
  context:
    market.facts

  feeds:
    if not $platform.funding.valid or not $platform.perp.valid:
      stop

  entry:
    if $platform.funding.apr >= 0.0:
      order.place(market: "perp", side: "sell", qty: 0.1, price: 100.0,
                  tif: "IOC", reduce_only: false, dedup_key: "test:entry")
|}
;;

let parse_script () =
  match Dio_strategies.Strategy_script.parse_string script with
  | Error e -> failf "parse error: %s" e
  | Ok f ->
    let errors =
      Dio_strategies.Strategy_compile.errors (Dio_strategies.Strategy_compile.validate f)
    in
    (match errors with
     | [] -> f
     | _ ->
       failf
         "validation errors: %s"
         (String.concat
            "; "
            (List.map
               (fun (d : Dio_strategies.Strategy_compile.diagnostic) -> d.msg)
               errors)))
;;

let test_parse_and_validate () =
  Dio_strategies.Strategy_actions_builtin.register_all ();
  let f = parse_script () in
  check string "name" "carry_test" f.name
;;

let test_order_action () =
  Dio_strategies.Strategy_actions_builtin.register_all ();
  Dio_exchange.Market_context.clear ~exchange:"hyperliquid";
  (* A flat position snapshot is still a valid snapshot. *)
  Dio_exchange.Market_context.replace_positions ~exchange:"hyperliquid" ~updated_at:1.0 [];
  Dio_exchange.Market_context.update_funding
    ~exchange:"hyperliquid"
    ~coin:"TEST"
    { Dio_exchange.Market_context.funding = 0.0000125
    ; premium = -0.0002
    ; mark = 100.0
    ; oracle = 100.02
    ; updated_at = 1.0
    };
  let ctx = Dio_strategies.Strategy_cycle_engine.create () in
  ctx.cg_asset <- Some asset;
  ctx.cg_symbol <- asset.symbol;
  ctx.cg_now <- 10.0;
  let module H =
    Dio_strategies.Strategy_actions_cycle.Make (Dio_strategies.Strategy_cycle_engine)
  in
  let rt =
    Dio_strategies.Strategy_runtime.create ~handlers:(H.handler ctx) (parse_script ())
  in
  ignore (Dio_strategies.Strategy_api.get_pending_orders 32 : _ list);
  ignore
    (Dio_strategies.Strategy_runtime.run_cycle
       rt
       ~price:100.0
       ~now:10.0
       ~event:(Dio_strategies.Strategy_runtime.make_event "book_update" [])
     : _ list);
  match Dio_strategies.Strategy_api.get_pending_orders 32 with
  | [] -> fail "expected a perp order"
  | order :: _ ->
    check string "symbol" "TEST" order.Dio_strategies.Strategy_common.symbol;
    check bool "sell" true (order.side = Dio_strategies.Strategy_common.Sell);
    check (float 1e-12) "qty" 0.1 order.qty;
    check bool "carry id" true (order.strategy = Dio_strategies.Strategy_common.Carry);
    check (option int) "userref" (Some 3) order.userref;
    check bool "not reduce-only" false order.reduce_only;
    check string "tif" "IOC" order.time_in_force;
    check string "dedup" "test:entry" order.duplicate_key
;;

let test_market_facts () =
  Dio_exchange.Market_context.clear ~exchange:"hyperliquid";
  Dio_exchange.Market_context.update_funding
    ~exchange:"hyperliquid"
    ~coin:"TEST"
    { Dio_exchange.Market_context.funding = 0.0000125
    ; premium = -0.0002
    ; mark = 100.0
    ; oracle = 100.02
    ; updated_at = 1.0
    };
  Dio_exchange.Market_context.replace_positions
    ~exchange:"hyperliquid"
    ~updated_at:1.0
    [ { Dio_exchange.Market_context.coin = "TEST"
      ; qty = -2.0
      ; entry = 105.0
      ; position_value = 200.0
      ; unrealized_pnl = 10.0
      ; margin_used = 20.0
      ; liquidation = Some 150.0
      ; leverage = Some 10.0
      ; updated_at = 1.0
      }
    ];
  Dio_exchange.Market_context.update_margin
    ~exchange:"hyperliquid"
    { Dio_exchange.Market_context.account_value = 1000.0
    ; margin_used = 100.0
    ; maintenance_margin = 100.0
    ; withdrawable = 900.0
    ; updated_at = 1.0
    };
  let ctx = Dio_strategies.Strategy_cycle_engine.create () in
  ctx.cg_asset <- Some asset;
  ctx.cg_symbol <- asset.symbol;
  ctx.cg_now <- 10.0;
  let f = parse_script () in
  let module H =
    Dio_strategies.Strategy_actions_cycle.Make (Dio_strategies.Strategy_cycle_engine)
  in
  let rt = Dio_strategies.Strategy_runtime.create ~handlers:(H.handler ctx) f in
  Dio_strategies.Strategy_cycle_engine.market_facts ctx rt;
  let open Dio_strategies.Strategy_expr in
  let get name = Dio_strategies.Strategy_runtime.get_platform rt name in
  (match get "funding_apr" with
   | V_float v -> check (float 1e-9) "funding apr" (0.0000125 *. 24.0 *. 365.0) v
   | _ -> fail "funding apr missing");
  (match get "perp_qty" with
   | V_float v -> check (float 1e-12) "perp qty" (-2.0) v
   | _ -> fail "perp qty missing");
  (match get "margin_ratio" with
   | V_float v -> check (float 1e-9) "margin ratio" 10.0 v
   | _ -> fail "margin ratio missing");
  match get "perp_valid" with
  | V_bool v -> check bool "perp valid" true v
  | _ -> fail "perp valid missing"
;;

let test_replace_action () =
  Dio_strategies.Strategy_actions_builtin.register_all ();
  let ctx = Dio_strategies.Strategy_cycle_engine.create () in
  ctx.cg_asset <- Some asset;
  ctx.cg_symbol <- asset.symbol;
  ctx.cg_now <- 10.0;
  ignore (Dio_strategies.Strategy_api.get_pending_orders 32 : _ list);
  Dio_strategies.Strategy_cycle_engine.order_replace
    ctx
    ~symbol:None
    ~market:"perp"
    ~side:"sell"
    ~qty:0.1
    ~price:(Some 100.0)
    ~tif:"ALO"
    ~reduce_only:false
    ~post_only:false
    ~dedup_key:"test:replace";
  match Dio_strategies.Strategy_api.get_pending_orders 32 with
  | [] -> fail "expected a replacement order"
  | order :: _ ->
    check string "replace symbol" "TEST" order.Dio_strategies.Strategy_common.symbol;
    check bool "replace post-only" true order.post_only;
    check string "replace tif" "GTC" order.time_in_force;
    check string "replace type" "limit" order.order_type;
    check string "replace dedup" "test:replace" order.duplicate_key;
    check bool "replace carry" true (order.strategy = Dio_strategies.Strategy_common.Carry)
;;

let () =
  run
    "carry_overlay"
    [ "script", [ test_case "parse/validate" `Quick test_parse_and_validate ]
    ; "order", [ test_case "perp order action" `Quick test_order_action ]
    ; "facts", [ test_case "market facts" `Quick test_market_facts ]
    ; "replace", [ test_case "order replace" `Quick test_replace_action ]
    ]
;;
