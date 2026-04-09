let test_get_balance_unknown () =
  (* Unknown asset should return 0.0 *)
  let balance = Ibkr.Balances.get_balance ~asset:"IBKR_UNKNOWN_ASSET" in
  Alcotest.(check (float 0.001)) "unknown asset balance is 0.0" 0.0 balance

let test_get_total_cash () =
  let cash = Ibkr.Balances.get_total_cash () in
  (* No connection, should be 0.0 *)
  Alcotest.(check (float 0.001)) "total cash default" 0.0 cash

let test_get_available_funds () =
  let funds = Ibkr.Balances.get_available_funds () in
  Alcotest.(check (float 0.001)) "available funds default" 0.0 funds

let test_get_settled_cash () =
  let cash = Ibkr.Balances.get_settled_cash () in
  Alcotest.(check (float 0.001)) "settled cash default" 0.0 cash

let test_get_buying_power () =
  let bp = Ibkr.Balances.get_buying_power () in
  Alcotest.(check (float 0.001)) "buying power default" 0.0 bp

let test_get_net_liquidation () =
  let nlv = Ibkr.Balances.get_net_liquidation () in
  Alcotest.(check (float 0.001)) "net liquidation default" 0.0 nlv

let test_get_unrealized_pnl () =
  let upnl = Ibkr.Balances.get_unrealized_pnl () in
  Alcotest.(check (float 0.001)) "unrealized pnl default" 0.0 upnl

let test_get_balance_usd () =
  (* "USD" maps to AvailableFunds *)
  let balance = Ibkr.Balances.get_balance ~asset:"USD" in
  Alcotest.(check (float 0.001)) "USD balance" 0.0 balance

let test_get_balance_total_cash () =
  let balance = Ibkr.Balances.get_balance ~asset:"TOTAL_CASH" in
  Alcotest.(check (float 0.001)) "TOTAL_CASH balance" 0.0 balance

let test_get_balance_settled () =
  let balance = Ibkr.Balances.get_balance ~asset:"SETTLED" in
  Alcotest.(check (float 0.001)) "SETTLED balance" 0.0 balance

let test_get_balance_net_liq () =
  let balance = Ibkr.Balances.get_balance ~asset:"NET_LIQ" in
  Alcotest.(check (float 0.001)) "NET_LIQ balance" 0.0 balance

let test_get_balance_buying_power () =
  let balance = Ibkr.Balances.get_balance ~asset:"BUYING_POWER" in
  Alcotest.(check (float 0.001)) "BUYING_POWER balance" 0.0 balance

let test_get_all_balances () =
  let balances = Ibkr.Balances.get_all_balances () in
  (* Should be empty list when no updates received *)
  Alcotest.(check int) "all balances empty" 0 (List.length balances)

let test_get_position_unknown () =
  match Ibkr.Balances.get_position ~symbol:"IBKR_NO_POSITION" with
  | None -> Alcotest.(check bool) "no position for unknown" true true
  | Some _ -> Alcotest.fail "unexpected position"

let test_get_account_value_unknown () =
  let v = Ibkr.Balances.get_account_value "NonExistentKey" in
  Alcotest.(check (float 0.001)) "unknown account value" 0.0 v

let () =
  Alcotest.run "IBKR Balances" [
    "default values", [
      Alcotest.test_case "get_total_cash" `Quick test_get_total_cash;
      Alcotest.test_case "get_available_funds" `Quick test_get_available_funds;
      Alcotest.test_case "get_settled_cash" `Quick test_get_settled_cash;
      Alcotest.test_case "get_buying_power" `Quick test_get_buying_power;
      Alcotest.test_case "get_net_liquidation" `Quick test_get_net_liquidation;
      Alcotest.test_case "get_unrealized_pnl" `Quick test_get_unrealized_pnl;
    ];
    "balance lookups", [
      Alcotest.test_case "get_balance_unknown" `Quick test_get_balance_unknown;
      Alcotest.test_case "get_balance_usd" `Quick test_get_balance_usd;
      Alcotest.test_case "get_balance_total_cash" `Quick test_get_balance_total_cash;
      Alcotest.test_case "get_balance_settled" `Quick test_get_balance_settled;
      Alcotest.test_case "get_balance_net_liq" `Quick test_get_balance_net_liq;
      Alcotest.test_case "get_balance_buying_power" `Quick test_get_balance_buying_power;
      Alcotest.test_case "get_account_value_unknown" `Quick test_get_account_value_unknown;
    ];
    "aggregated data", [
      Alcotest.test_case "get_all_balances" `Quick test_get_all_balances;
    ];
    "positions", [
      Alcotest.test_case "get_position_unknown" `Quick test_get_position_unknown;
    ];
  ]
