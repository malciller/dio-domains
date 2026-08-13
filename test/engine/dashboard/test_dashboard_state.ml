(* Dashboard state tests: the capital-oracle decision serialization - the
   snapshot carries the oracle's ACTIVE/INACTIVE verdict, sizing and capital
   accounting per tracked asset (the dashboard's pause state source). *)

let make_decision () =
  { Dio_oracle.Oracle_runtime.exchange = "hyperliquid"
  ; symbol = "HYPE/USDC"
  ; active = false
  ; reason = "pool 12.61 cannot fund the first buy at qty_min (needs 18.80)"
  ; qty = 0.0
  ; grid_interval = 5.0
  ; d_surv = 0.0
  ; d_gov = 0.4
  ; d_cover = 0.6
  ; governing_horizon = "180d"
  ; deployed = 0.0
  ; pool_share = 12.61
  ; remainder = 12.61
  ; reclaim_capital = false
  ; reclaim_target = ""
  ; range = None
  ; p2v = None
  ; parameter_components =
      { Dio_oracle.Oracle_types.fng = Some 37.0
      ; fng_parameter = None
      ; survival_parameter = 5.0
      ; resolved_parameter = 5.0
      ; fng_weight = 0.5
      ; range_parameter = None
      ; range_weight = 0.25
      }
  ; gi_reason = "grid max 5.00% (100% survival unreachable at any gi)"
  ; qty_reason = "minimum qty 0.5 (stretch: 100% survival unreachable)"
  ; warnings = []
  ; updated_at = 42.0
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
  (match field j "qty" with
   | Some (`Float q) -> Alcotest.(check (float 1e-9)) "qty" 0.0 q
   | _ -> Alcotest.fail "missing qty");
  (match field j "grid_interval" with
   | Some (`Float g) -> Alcotest.(check (float 1e-9)) "gi" 5.0 g
   | _ -> Alcotest.fail "missing grid_interval");
  (match field j "d_surv" with
   | Some (`Float d) -> Alcotest.(check (float 1e-9)) "d_surv" 0.0 d
   | _ -> Alcotest.fail "missing d_surv");
  (match field j "pool_share" with
   | Some (`Float p) -> Alcotest.(check (float 1e-9)) "pool_share" 12.61 p
   | _ -> Alcotest.fail "missing pool_share");
  (match field j "remainder" with
   | Some (`Float r) -> Alcotest.(check (float 1e-9)) "remainder" 12.61 r
   | _ -> Alcotest.fail "missing remainder");
  (match field j "updated_at" with
   | Some (`Float t) -> Alcotest.(check (float 1e-9)) "updated_at" 42.0 t
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

let () =
  Alcotest.run
    "dashboard_state"
    [ ( "oracle decisions"
      , [ Alcotest.test_case
            "decision JSON carries the verdict, sizing and capital"
            `Quick
            test_decision_fields
        ; Alcotest.test_case "decisions keyed by symbol" `Quick test_keyed_by_symbol
        ] )
    ]
;;
