let bar date low : Dio_survival.Survival_types.bar =
  { date; open_ = 100.0; high = 100.0; low; close = low; volume = 1.0 }
;;

let test_qualified_identity () =
  let hl =
    Dio_survival.Survival_topology.key ~venue:"hyperliquid" ~symbol:"BTC/USDC" ()
  in
  let kr = Dio_survival.Survival_topology.key ~venue:"kraken" ~symbol:"BTC/USD" () in
  Alcotest.(check string) "hl quote" "USDC" hl.quote;
  Alcotest.(check string) "kraken quote" "USD" kr.quote;
  Alcotest.(check bool)
    "different instruments"
    false
    (Dio_survival.Survival_topology.equal_key hl kr)
;;

let test_parse_and_validate () =
  let json =
    Yojson.Safe.from_string
      {|{"positions":[{"venue":"hyperliquid","symbol":"BTC/USDC","capital":1000},{"venue":"lighter","symbol":"ETH/USDC","capital":1000}],"transfers":[{"session":2,"from":"hyperliquid/BTC/USDC","to":"lighter/ETH/USDC","amount":25}]}|}
  in
  match Dio_survival.Survival_topology.parse json with
  | Error error -> Alcotest.fail error
  | Ok definition ->
    (match Dio_survival.Survival_topology.validate definition with
     | Ok () -> ()
     | Error errors -> Alcotest.fail (String.concat "; " errors))
;;

let test_reject_same_venue_transfer () =
  let hl1 =
    Dio_survival.Survival_topology.key ~venue:"hyperliquid" ~symbol:"BTC/USDC" ()
  in
  let hl2 =
    Dio_survival.Survival_topology.key ~venue:"hyperliquid" ~symbol:"ETH/USDC" ()
  in
  let definition =
    { Dio_survival.Survival_topology.positions =
        [ { key = hl1; capital = Some 1.0 }; { key = hl2; capital = Some 1.0 } ]
    ; transfers = [ { session = 0; from_key = hl1; to_key = hl2; amount = 1.0 } ]
    }
  in
  match Dio_survival.Survival_topology.validate definition with
  | Ok () -> Alcotest.fail "expected same-venue transfer validation error"
  | Error errors ->
    Alcotest.(check bool)
      "same venue rejected"
      true
      (List.exists (fun error -> String.contains error 'o') errors)
;;

let test_reject_cross_quote_transfer () =
  let hl =
    Dio_survival.Survival_topology.key ~venue:"hyperliquid" ~symbol:"BTC/USDC" ()
  in
  let kr = Dio_survival.Survival_topology.key ~venue:"kraken" ~symbol:"ETH/USD" () in
  let definition =
    { Dio_survival.Survival_topology.positions =
        [ { key = hl; capital = Some 1.0 }; { key = kr; capital = Some 1.0 } ]
    ; transfers = [ { session = 0; from_key = hl; to_key = kr; amount = 1.0 } ]
    }
  in
  match Dio_survival.Survival_topology.validate definition with
  | Ok () -> Alcotest.fail "expected cross-quote validation error"
  | Error errors ->
    Alcotest.(check bool)
      "cross quote rejected"
      true
      (List.exists (fun error -> String.contains error 'q') errors)
;;

let test_align_without_forward_fill () =
  let series : Dio_survival.Survival_types.series =
    { symbol = "BTC/USDC"
    ; calendar_kind = Crypto
    ; bars = [| bar "2024-01-01" 99.0; bar "2024-01-03" 97.0 |]
    ; gaps = []
    }
  in
  let timeline = [| "2024-01-01"; "2024-01-02"; "2024-01-03" |] in
  let aligned = Dio_survival.Survival_topology.align_series timeline series in
  Alcotest.(check bool) "first present" true (Option.is_some aligned.(0));
  Alcotest.(check bool) "missing remains missing" true (Option.is_none aligned.(1));
  Alcotest.(check bool) "third present" true (Option.is_some aligned.(2))
;;

let () =
  Alcotest.run
    "survival_topology"
    [ ( "topology"
      , [ Alcotest.test_case "qualified identity" `Quick test_qualified_identity
        ; Alcotest.test_case "parse and validate" `Quick test_parse_and_validate
        ; Alcotest.test_case "reject cross quote" `Quick test_reject_cross_quote_transfer
        ; Alcotest.test_case "reject same venue" `Quick test_reject_same_venue_transfer
        ; Alcotest.test_case
            "align without forward fill"
            `Quick
            test_align_without_forward_fill
        ] )
    ]
;;
