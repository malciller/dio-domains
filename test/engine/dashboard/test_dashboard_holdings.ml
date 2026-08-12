(* Holdings pause-state tests: the dashboard's paused status must reflect the
   capital oracle's INACTIVE verdict (the oracle-paused state), not just the
   grid's internal capital-low flag - an asset the oracle says cannot fund
   its first buy is paused even when the grid state is quiet. *)

let strategy_json ?(oracle = `Null) ?(capital_low = false) ?(market_closed = false) () =
  `Assoc
    [ "exchange", `String "hyperliquid"
    ; ( "strategy"
      , `Assoc
          [ "type", `String "Grid"
          ; "capital_low", `Bool capital_low
          ; "market_is_closed", `Bool market_closed
          ] )
    ; "oracle", oracle
    ]
;;

let oracle_json active = `Assoc [ "active", `Bool active; "reason", `String "test" ]

let test_oracle_inactive () =
  (* No oracle decision yet (before the first pass): not oracle-paused. *)
  Alcotest.(check bool)
    "no decision -> not oracle-paused"
    (Dashboard_ui.Holdings.oracle_inactive (strategy_json ()))
    false;
  (* Oracle says ACTIVE -> not paused. *)
  Alcotest.(check bool)
    "active decision -> not oracle-paused"
    (Dashboard_ui.Holdings.oracle_inactive (strategy_json ~oracle:(oracle_json true) ()))
    false;
  (* Oracle says INACTIVE -> paused. *)
  Alcotest.(check bool)
    "inactive decision -> oracle-paused"
    (Dashboard_ui.Holdings.oracle_inactive (strategy_json ~oracle:(oracle_json false) ()))
    true;
  (* A balance (non-strategy) entry has no oracle field at all. *)
  Alcotest.(check bool)
    "balance entry -> not oracle-paused"
    (Dashboard_ui.Holdings.oracle_inactive
       (`Assoc [ "asset", `String "X"; "balance", `Float 1.0 ]))
    false
;;

let test_strategy_paused () =
  (* Paused = oracle INACTIVE, or the grid's capital-low flag, or the market
     closed. *)
  Alcotest.(check bool)
    "quiet active grid -> running"
    (Dashboard_ui.Holdings.strategy_paused (strategy_json ()))
    false;
  Alcotest.(check bool)
    "capital-low grid -> paused"
    (Dashboard_ui.Holdings.strategy_paused (strategy_json ~capital_low:true ()))
    true;
  Alcotest.(check bool)
    "oracle-INACTIVE grid -> paused (the fix)"
    (Dashboard_ui.Holdings.strategy_paused (strategy_json ~oracle:(oracle_json false) ()))
    true;
  Alcotest.(check bool)
    "market closed -> paused"
    (Dashboard_ui.Holdings.strategy_paused (strategy_json ~market_closed:true ()))
    true;
  Alcotest.(check bool)
    "oracle-ACTIVE grid -> running"
    (Dashboard_ui.Holdings.strategy_paused (strategy_json ~oracle:(oracle_json true) ()))
    false
;;

let () =
  Alcotest.run
    "dashboard_holdings"
    [ ( "pause state"
      , [ Alcotest.test_case
            "oracle verdict drives the paused state"
            `Quick
            test_oracle_inactive
        ; Alcotest.test_case
            "paused = oracle OR capital-low OR market closed"
            `Quick
            test_strategy_paused
        ] )
    ]
;;
