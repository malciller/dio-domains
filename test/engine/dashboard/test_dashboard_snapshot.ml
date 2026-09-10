(* Snapshot parser tests: the typed model must decode the engine's JSON
   faithfully (defaults, derived mids, asset classification) so renderers can
   trust record fields instead of re-reading raw Yojson. *)

open Dashboard_ui

let mk_order ?(id = "o") ?(price = 1.0) ?(qty = 1.0) () =
  `Assoc [ "id", `String id; "price", `Float price; "qty", `Float qty ]
;;

let strategy_json
      ?(exchange = "hyperliquid")
      ?(type_ = "Ladder")
      ?(capital_low = false)
      ?(market_closed = false)
      ?(oracle = `Null)
      ?(buy_price = 0.0)
      ?(sell_orders = [])
      ?(bid = 100.0)
      ?(ask = 101.0)
      ?(base_asset = "BTC")
      ()
  =
  `Assoc
    [ "exchange", `String exchange
    ; ( "strategy"
      , `Assoc
          [ "type", `String type_
          ; "capital_low", `Bool capital_low
          ; "market_is_closed", `Bool market_closed
          ; "buy_price", `Float buy_price
          ; "sell_orders", `List sell_orders
          ] )
    ; ( "market"
      , `Assoc
          [ "bid", `Float bid
          ; "ask", `Float ask
          ; "base_asset", `String base_asset
          ; "base_balance", `Float 2.0
          ] )
    ; "oracle", oracle
    ; "grid_interval_lo", `Float 0.5
    ]
;;

let snapshot_json strategies balances =
  `Assoc
    [ "timestamp", `Float 1000.0
    ; "uptime_s", `Float 42.0
    ; "fear_and_greed", `Float 55.0
    ; "memory", `Assoc [ "heap_mb", `Int 10; "live_kb", `Int 20; "free_kb", `Int 30 ]
    ; "strategies", `Assoc strategies
    ; "all_balances", `List balances
    ]
;;

let test_market_mid_and_defaults () =
  let s = Snapshot.of_json (snapshot_json [ "BTC/USDC", strategy_json () ] []) in
  match s.strategies with
  | [ (sym, st) ] ->
    Alcotest.(check string) "symbol key" "BTC/USDC" sym;
    Alcotest.(check (float 1e-9)) "mid is (bid+ask)/2" 100.5 st.market.mid;
    Alcotest.(check string) "base asset" "BTC" st.market.base_asset;
    Alcotest.(check (float 1e-9)) "base balance" 2.0 st.market.base_balance;
    Alcotest.(check (float 1e-9)) "grid interval" 0.5 st.grid_interval_lo
  | _ -> Alcotest.fail "expected exactly one strategy"
;;

let test_null_prices_default_to_zero () =
  let json =
    `Assoc
      [ ( "strategies"
        , `Assoc
            [ ( "ETH/USDC"
              , `Assoc
                  [ "exchange", `String "hyperliquid"
                  ; "strategy", `Assoc [ "type", `String "Ladder" ]
                  ; "market", `Assoc [ "bid", `Null; "ask", `Null ]
                  ] )
            ] )
      ]
  in
  let s = Snapshot.of_json json in
  match s.strategies with
  | [ (_, st) ] -> Alcotest.(check (float 1e-9)) "null prices -> 0 mid" 0.0 st.market.mid
  | _ -> Alcotest.fail "expected one strategy"
;;

let test_fear_and_greed_option () =
  let some = Snapshot.of_json (snapshot_json [] []) in
  Alcotest.(check (option (float 1e-9))) "float f&g" (Some 55.0) some.fear_and_greed;
  let none = Snapshot.of_json (`Assoc [ "fear_and_greed", `Null ]) in
  Alcotest.(check (option (float 1e-9))) "null f&g" None none.fear_and_greed
;;

let balance_json ?(asset = "XYZ") ?(balance = 5.0) () =
  `Assoc
    [ "exchange", `String "kraken"
    ; "asset", `String asset
    ; "symbol", `String (asset ^ "/USD")
    ; "balance", `Float balance
    ; "bid", `Float 10.0
    ; "ask", `Float 11.0
    ]
;;

let test_selectable_assets_order_and_kind () =
  let s =
    Snapshot.of_json
      (snapshot_json
         [ "BTC/USDC", strategy_json () ]
         [ balance_json (); balance_json ~asset:"USD" ~balance:100.0 () ])
  in
  (* The strategy comes first; its asset is the market base. *)
  match s.assets with
  | first :: second :: _ ->
    Alcotest.(check bool) "strategy is first" first.is_strategy true;
    Alcotest.(check string) "strategy key" "strat:hyperliquid:BTC/USDC" first.key;
    (match first.kind with
     | Snapshot.Strategy _ -> ()
     | Snapshot.Balance _ -> Alcotest.fail "first asset should be a strategy");
    Alcotest.(check bool) "balance is not a strategy" second.is_strategy false;
    (match second.kind with
     | Snapshot.Balance b -> Alcotest.(check string) "balance asset" "XYZ" b.asset
     | Snapshot.Strategy _ -> Alcotest.fail "second asset should be a balance")
  | _ -> Alcotest.fail "expected at least two selectable assets"
;;

let test_orders_filter_nonpositive () =
  let json =
    snapshot_json
      [ ( "BTC/USDC"
        , strategy_json
            ~sell_orders:
              [ mk_order ~id:"a" ~price:100.0 ~qty:1.0 ()
              ; mk_order ~id:"b" ~price:0.0 ~qty:1.0 ()
              ; mk_order ~id:"c" ~price:100.0 ~qty:0.0 ()
              ]
            () )
      ]
      []
  in
  let s = Snapshot.of_json json in
  match s.strategies with
  | [ (_, st) ] ->
    Alcotest.(check int) "only the valid order survives" 1 (List.length st.sell_orders);
    Alcotest.(check int) "sell_count tracks orders" 1 st.sell_count
  | _ -> Alcotest.fail "expected one strategy"
;;

let test_latency_and_oracle_parsing () =
  let metric =
    `Assoc
      [ "p50", `Float 1.0
      ; "p99", `Float 3.0
      ; "p999", `Float 4.0
      ; "samples", `Int 7
      ; "window_end", `Float 999.0
      ]
  in
  let json =
    `Assoc
      [ "latencies", `Assoc [ "BTC/USDC", `Assoc [ "strategy", metric ] ]
      ; "oracle_latency", `Assoc [ "pass", metric ]
      ; "strategies", `Assoc [ "BTC/USDC", `Assoc [ "exchange", `String "hyperliquid" ] ]
      ]
  in
  let s = Snapshot.of_json json in
  (match List.assoc_opt "BTC/USDC" s.latencies with
   | Some ms ->
     (match List.assoc_opt "strategy" ms with
      | Some m ->
        Alcotest.(check (float 1e-9)) "p50" 1.0 m.p50;
        Alcotest.(check int) "samples" 7 m.samples
      | None -> Alcotest.fail "missing strategy metric")
   | None -> Alcotest.fail "missing latency row");
  match List.assoc_opt "pass" s.oracle_latency with
  | Some m -> Alcotest.(check (float 1e-9)) "oracle p99" 3.0 m.p99
  | None -> Alcotest.fail "missing oracle pass metric"
;;

let () =
  Alcotest.run
    "dashboard_snapshot"
    [ ( "parse"
      , [ Alcotest.test_case
            "mid derivation and defaults"
            `Quick
            test_market_mid_and_defaults
        ; Alcotest.test_case
            "null prices default to zero"
            `Quick
            test_null_prices_default_to_zero
        ; Alcotest.test_case "fear and greed option" `Quick test_fear_and_greed_option
        ; Alcotest.test_case
            "selectable asset order and kind"
            `Quick
            test_selectable_assets_order_and_kind
        ; Alcotest.test_case
            "orders filter non-positive"
            `Quick
            test_orders_filter_nonpositive
        ; Alcotest.test_case
            "latency and oracle parsing"
            `Quick
            test_latency_and_oracle_parsing
        ] )
    ]
;;
