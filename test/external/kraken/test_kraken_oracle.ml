(* Kraken oracle adapter tests: balance parsing (asset normalization +
   wallet-suffix stripping). Pure functions only (no network). *)

let near a b = Alcotest.(check (float 1e-9)) "approx" a b

let test_parse_balances_normalizes_assets () =
  let json =
    Yojson.Safe.from_string
      {|{"error":[],"result":{"XXBT":"1.25","ZUSD":100.5,"BTC.HOLD":"0.25"}}|}
  in
  match Kraken.Kraken_oracle.parse_balances json with
  | Error error -> Alcotest.fail error
  | Ok triples ->
    (* XXBT -> BTC; BTC.HOLD (staking) keeps the BTC asset after suffix
       stripping; ZUSD -> USD. Both BTC entries survive (the snapshot
       layer merges them). *)
    let btc = List.filter (fun (a, _, _) -> a = "BTC") triples in
    Alcotest.(check int) "two BTC entries" 2 (List.length btc);
    near 1.5 (List.fold_left (fun acc (_, _, total) -> acc +. total) 0.0 btc);
    let usd =
      match List.find_opt (fun (a, _, _) -> a = "USD") triples with
      | Some (_, available, total) -> available, total
      | None -> Alcotest.fail "USD missing"
    in
    near 100.5 (fst usd);
    near 100.5 (snd usd)
;;

let test_parse_balances_api_error () =
  let json = Yojson.Safe.from_string {|{"error":["EAPI:Invalid key"],"result":{}}|} in
  match Kraken.Kraken_oracle.parse_balances json with
  | Error error -> Alcotest.(check bool) "error surfaced" true (String.length error > 0)
  | Ok _ -> Alcotest.fail "expected an API error"
;;

let () =
  Alcotest.run
    "kraken_oracle"
    [ ( "balances"
      , [ Alcotest.test_case
            "asset normalization and suffix stripping"
            `Quick
            test_parse_balances_normalizes_assets
        ; Alcotest.test_case "API error surfaced" `Quick test_parse_balances_api_error
        ] )
    ]
;;
