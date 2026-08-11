let test_round_trip () =
  let path = Filename.temp_file "oracle-positions" ".json" in
  let key =
    Dio_oracle.Oracle_topology.key ~venue:"hyperliquid" ~symbol:"BTC/USDC" ()
  in
  let values =
    [ { Dio_oracle.Oracle_portfolio_state.key; pool = 125.0; base = 0.5 } ]
  in
  (try
     Dio_oracle.Oracle_portfolio_state.save path values;
     match Dio_oracle.Oracle_portfolio_state.load path with
     | Error error -> Alcotest.fail error
     | Ok [ value ] ->
       Alcotest.(check string) "symbol" "BTC/USDC" value.key.symbol;
       Alcotest.(check (float 1e-9)) "pool" 125.0 value.pool;
       Alcotest.(check (float 1e-9)) "base" 0.5 value.base
     | Ok _ -> Alcotest.fail "unexpected saved position count"
   with
   | exn ->
     Sys.remove path;
     raise exn);
  Sys.remove path
;;

let () =
  Alcotest.run
    "oracle_portfolio_state"
    [ "state", [ Alcotest.test_case "round trip" `Quick test_round_trip ] ]
;;
