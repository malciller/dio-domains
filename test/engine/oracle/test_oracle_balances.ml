let near a b = Alcotest.(check (float 1e-9)) "approx" a b

let test_kraken_parser () =
  let json =
    Yojson.Safe.from_string
      {|{"error":[],"result":{"XXBT":"1.25","ZUSD":100.5,"BTC.HOLD":"0.25"}}|}
  in
  match Dio_oracle.Oracle_balances.parse_kraken json with
  | Error error -> Alcotest.fail error
  | Ok balances ->
    near
      1.25
      (List.find (fun b -> b.Dio_oracle.Oracle_balances.asset = "BTC") balances).total;
    near
      100.5
      (List.find (fun b -> b.Dio_oracle.Oracle_balances.asset = "USD") balances)
        .available;
    near
      1.5
      (List.fold_left
         (fun total b ->
            if b.Dio_oracle.Oracle_balances.asset = "BTC"
            then total +. b.total
            else total)
         0.0
         balances)
;;

let test_hyperliquid_spot_parser () =
  let json =
    Yojson.Safe.from_string
      {|{"balances":[{"coin":"USDC","total":"125.5","hold":"5.5"},{"coin":"UBTC","total":2.0,"hold":0.25}]}|}
  in
  match Dio_oracle.Oracle_balances.parse_hyperliquid_spot json with
  | Error error -> Alcotest.fail error
  | Ok balances ->
    let usdc =
      List.find (fun b -> b.Dio_oracle.Oracle_balances.asset = "USDC") balances
    in
    let btc =
      List.find (fun b -> b.Dio_oracle.Oracle_balances.asset = "BTC") balances
    in
    near 120.0 usdc.available;
    near 125.5 usdc.total;
    near 1.75 btc.available
;;

let test_alpaca_parsers () =
  let account =
    Yojson.Safe.from_string {|{"currency":"USD","cash":"500.0","equity":"750.0"}|}
  in
  let positions =
    Yojson.Safe.from_string {|[{"symbol":"AAPL","qty":"2.5"},{"symbol":"MSFT","qty":1}]|}
  in
  let account_balance =
    match Dio_oracle.Oracle_balances.parse_alpaca_account account with
    | Ok [ value ] -> value
    | Ok _ -> Alcotest.fail "unexpected account balance count"
    | Error error -> Alcotest.fail error
  in
  near 500.0 account_balance.available;
  near 750.0 account_balance.total;
  match Dio_oracle.Oracle_balances.parse_alpaca_positions positions with
  | Error error -> Alcotest.fail error
  | Ok values ->
    near
      2.5
      (List.find (fun b -> b.Dio_oracle.Oracle_balances.asset = "AAPL") values).total
;;

let test_quote_aggregation () =
  let snapshot : Dio_oracle.Oracle_balances.snapshot =
    { exchange = "hyperliquid"
    ; testnet = false
    ; fetched_at = 0.0
    ; balances =
        [ { asset = "USDC"
          ; available = 10.0
          ; total = 10.0
          ; wallet_type = "spot"
          ; wallet_id = "account"
          }
        ; { asset = "USDC"
          ; available = 2.5
          ; total = 3.0
          ; wallet_type = "perp"
          ; wallet_id = "account"
          }
        ]
    }
  in
  near 12.5 (Dio_oracle.Oracle_balances.available_quote snapshot ~quote:"USDC");
  near 13.0 (Dio_oracle.Oracle_balances.total_asset snapshot ~asset:"USDC")
;;

let () =
  Alcotest.run
    "oracle_balances"
    [ ( "parsers"
      , [ Alcotest.test_case "kraken" `Quick test_kraken_parser
        ; Alcotest.test_case "hyperliquid spot" `Quick test_hyperliquid_spot_parser
        ; Alcotest.test_case "alpaca" `Quick test_alpaca_parsers
        ; Alcotest.test_case "quote aggregation" `Quick test_quote_aggregation
        ] )
    ]
;;
