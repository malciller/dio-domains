(* Snapshot parser tests: the typed model must decode the engine's JSON faithfully
   (defaults, derived mids, asset classification) so renderers can trust record fields
   instead of re-reading raw Yojson. *)

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

let test_log_parsing () =
  let json =
    `Assoc
      [ "timestamp", `Float 1000.0
      ; ( "logs"
        , `List
            [ `Assoc
                [ "seq", `Int 7
                ; "ts", `Float 12.5
                ; "level", `String "WARN"
                ; "section", `String "kraken_get_fee"
                ; "message", `String "fee cache miss"
                ]
            ] )
      ]
  in
  let s = Snapshot.of_json json in
  match s.logs with
  | [ e ] ->
    Alcotest.(check int) "seq" 7 e.le_seq;
    Alcotest.(check string) "level" "WARN" e.le_level;
    Alcotest.(check string) "section" "kraken_get_fee" e.le_section;
    Alcotest.(check string) "message" "fee cache miss" e.le_message;
    Alcotest.(check (float 1e-9)) "ts" 12.5 e.le_ts
  | _ -> Alcotest.fail "expected exactly one log entry"
;;

let test_alert_thresholds () =
  let w = Alerts.threshold_of_string "warn" in
  Alcotest.(check bool) "warn notifies warn" true (Alerts.notifies w "WARN");
  Alcotest.(check bool) "warn notifies error" true (Alerts.notifies w "ERROR");
  Alcotest.(check bool) "warn notifies critical" true (Alerts.notifies w "CRITICAL");
  Alcotest.(check bool) "warn rejects info" false (Alerts.notifies w "INFO");
  Alcotest.(check bool) "warn rejects debug" false (Alerts.notifies w "DEBUG");
  Alcotest.(check bool)
    "off notifies nothing"
    false
    (Alerts.notifies Alerts.Off "CRITICAL");
  Alcotest.(check bool)
    "critical only"
    false
    (Alerts.notifies (Alerts.threshold_of_string "critical") "ERROR");
  Alcotest.(check bool)
    "critical admits critical"
    true
    (Alerts.notifies (Alerts.threshold_of_string "critical") "CRITICAL");
  Alcotest.(check bool)
    "unknown defaults to warn"
    true
    (Alerts.threshold_of_string "bogus" = Alerts.Warn)
;;

let test_alert_batching () =
  let e = Alerts.add [] ~level:"error" ~section:"s" ~message:"boom" in
  let e = Alerts.add e ~level:"ERROR" ~section:"s" ~message:"boom" in
  Alcotest.(check int) "identical lines batched into one row" 1 (List.length e);
  (match e with
   | [ row ] -> Alcotest.(check int) "count bumped" 2 row.a_count
   | _ -> Alcotest.fail "expected exactly one batched row");
  let e = Alerts.add e ~level:"WARN" ~section:"s" ~message:"different" in
  Alcotest.(check int) "distinct line added" 2 (List.length e)
;;

let test_alert_modal_wraps_long_tokens () =
  (* Compact JSON error bodies contain no spaces. Before hard-breaking, such a "word"
     stretched the modal past the terminal width and pushed the right border off-screen
     (and wrapped the dashboard underneath into the modal's rows). *)
  let width = 32 in
  let msg =
    "Cancel order failed HTTP 422 for db72db3e: {\"code\":42210000,\"message\":\"order \
     pending cancel\",\"related_orders\":[\"20701f00-3be1-4ef7\"]}"
  in
  let segs = Alerts.wrap_words ~width msg in
  Alcotest.(check bool) "not empty" true (segs <> []);
  List.iter
    (fun seg ->
      let w = Notty.I.width (Notty.I.string Notty.A.empty seg) in
      Alcotest.(check bool)
        (Printf.sprintf "segment %S fits (%d <= %d)" seg w width)
        true
        (w <= width))
    segs;
  (* Unbroken text must be preserved, not dropped. *)
  let strip_spaces s =
    String.to_seq s |> Seq.filter (fun c -> c <> ' ') |> String.of_seq
  in
  let joined = String.concat "" segs in
  Alcotest.(check string) "json body preserved" (strip_spaces msg) (strip_spaces joined)
;;

let test_alert_modal_renders () =
  let entries =
    [ { Alerts.a_level = "CRITICAL"
      ; a_section = "main"
      ; a_message = "SIGTERM received; beginning graceful shutdown"
      ; a_count = 1
      }
    ; { Alerts.a_level = "ERROR"
      ; a_section = "order_executor"
      ; a_message = "place rejected for SOL/USDC: insufficient margin"
      ; a_count = 3
      }
    ]
  in
  let img = Alerts.render_modal ~target_w:120 ~target_h:40 entries in
  Alcotest.(check int) "modal fills target width" 120 (Notty.I.width img);
  Alcotest.(check int) "modal fills target height" 40 (Notty.I.height img)
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
        ; Alcotest.test_case "log parsing" `Quick test_log_parsing
        ; Alcotest.test_case "alert thresholds" `Quick test_alert_thresholds
        ; Alcotest.test_case "alert batching" `Quick test_alert_batching
        ; Alcotest.test_case
            "alert wraps long tokens"
            `Quick
            test_alert_modal_wraps_long_tokens
        ; Alcotest.test_case "alert modal renders" `Quick test_alert_modal_renders
        ] )
    ]
;;
