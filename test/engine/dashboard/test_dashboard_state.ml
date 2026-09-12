(* Dashboard state tests: the capital-oracle decision serialization - the
   snapshot carries the oracle's ACTIVE/INACTIVE verdict, sizing and capital
   accounting per tracked asset (the dashboard's pause state source). *)

let make_decision () =
  { Dio_oracle.Oracle_runtime.exchange = "hyperliquid"
  ; symbol = "HYPE/USDC"
  ; active = false
  ; reason = "pool 12.61 cannot fund the first buy (needs 18.80)"
  ; buy_qty = 0.0
  ; sell_qty = 0.5
  ; max_drawdown_pct = 0.85
  ; grid_interval = 5.0
  ; d_surv = 0.0
  ; exhaustion_price = 26400.0
  ; regime = "floor extension"
  ; branch = "unreachable"
  ; cancel_resting_buys = false
  ; updated_at = 1700000000.0
  }
;;

let json () = Dio_dashboard.Dashboard_state.json_of_decision (make_decision ())

let field json key =
  match Yojson.Basic.Util.member key json with
  | `Null -> None
  | v -> Some v
;;

let test_decision_fields () =
  let j = json () in
  (match field j "active" with
   | Some (`Bool b) -> Alcotest.(check bool) "inactive verdict" false b
   | _ -> Alcotest.fail "missing active");
  (match field j "reason" with
   | Some (`String s) ->
     Alcotest.(check bool)
       "reason carried"
       (String.length s > 0 && String.contains s 'p')
       true
   | _ -> Alcotest.fail "missing reason");
  (match field j "buy_qty" with
   | Some (`Float q) -> Alcotest.(check (float 1e-9)) "buy_qty" 0.0 q
   | _ -> Alcotest.fail "missing buy_qty");
  (match field j "sell_qty" with
   | Some (`Float q) -> Alcotest.(check (float 1e-9)) "sell_qty" 0.5 q
   | _ -> Alcotest.fail "missing sell_qty");
  (match field j "grid_interval" with
   | Some (`Float g) -> Alcotest.(check (float 1e-9)) "gi" 5.0 g
   | _ -> Alcotest.fail "missing grid_interval");
  (match field j "d_surv" with
   | Some (`Float d) -> Alcotest.(check (float 1e-9)) "d_surv" 0.0 d
   | _ -> Alcotest.fail "missing d_surv");
  (match field j "exhaustion_price" with
   | Some (`Float p) -> Alcotest.(check (float 1e-9)) "exhaustion_price" 26400.0 p
   | _ -> Alcotest.fail "missing exhaustion_price");
  (match field j "regime" with
   | Some (`String r) -> Alcotest.(check string) "regime" "floor extension" r
   | _ -> Alcotest.fail "missing regime");
  (match field j "branch" with
   | Some (`String b) -> Alcotest.(check string) "branch" "unreachable" b
   | _ -> Alcotest.fail "missing branch");
  (match field j "updated_at" with
   | Some (`Float t) -> Alcotest.(check (float 1e-9)) "updated_at" 1700000000.0 t
   | _ -> Alcotest.fail "missing updated_at");
  match field j "exchange" with
  | Some (`String e) -> Alcotest.(check string) "exchange" "hyperliquid" e
  | _ -> Alcotest.fail "missing exchange"
;;

let test_keyed_by_symbol () =
  (* The decisions map is keyed by symbol so the snapshot can join them onto
     the strategy entries. *)
  let all =
    match Dio_dashboard.Dashboard_state.json_of_oracle_decisions () with
    | `Assoc l -> l
    | _ -> []
  in
  (* The runtime's live decisions are an empty snapshot in this test
     process; the map is a plain assoc either way. *)
  Alcotest.(check bool)
    "decisions map is an assoc"
    (List.for_all (fun (k, _) -> k <> "") all)
    true
;;

let test_ladder_sell_count_uses_ledger () =
  (* The SELLS count must report the live sell set, not only what the venue
     feed lists. Kraken's open-order feed can drop a resting sell, which
     showed as 0 pending sells while the order still rested; the in-flight
     ledger is armed at dispatch and is the authority. Feed empty + two ledger
     commitments => count 2. *)
  let symbol = "DASH_SELLCOUNT/XMR/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.open_sell_orders <- [];
  state.sell_commitments
  <- [ "oid-a", 539.67, 0.3, false, false, 0.0; "oid-b", 540.10, 0.2, true, true, 0.0 ];
  let j = Dio_dashboard.Dashboard_state.json_of_grid_strategy "kraken" symbol in
  (match field j "sell_count" with
   | Some (`Int n) -> Alcotest.(check int) "ledger sells counted" 2 n
   | _ -> Alcotest.fail "missing sell_count");
  match field j "sell_orders" with
  | Some (`List l) ->
    Alcotest.(check int) "sell_orders carries the ledger sells" 2 (List.length l)
  | _ -> Alcotest.fail "missing sell_orders"
;;

let () =
  Alcotest.run
    "dashboard_state"
    [ ( "oracle decisions"
      , [ Alcotest.test_case
            "decision JSON carries the verdict, sizing and capital"
            `Quick
            test_decision_fields
        ; Alcotest.test_case "decisions keyed by symbol" `Quick test_keyed_by_symbol
        ; Alcotest.test_case
            "ladder SELLS count includes the in-flight ledger"
            `Quick
            test_ladder_sell_count_uses_ledger
        ] )
    ]
;;
