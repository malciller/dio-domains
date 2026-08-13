(* Oracle_balances tests: the venue-independent snapshot aggregation. The
   per-venue balance PARSERS now live in each venue's oracle adapter and are
   tested there (test/external/<venue>/test_<venue>_oracle.ml); this file
   covers the snapshot plumbing (merge, quote/asset aggregation). *)

let near a b = Alcotest.(check (float 1e-9)) "approx" a b

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

let test_merge_same_wallet () =
  (* merge_balances sums entries sharing (asset, wallet_type, wallet_id). *)
  let merged =
    Dio_oracle.Oracle_balances.merge_balances
      [ { asset = "USDC"
        ; available = 10.0
        ; total = 10.0
        ; wallet_type = "rest"
        ; wallet_id = "account"
        }
      ; { asset = "USDC"
        ; available = 2.5
        ; total = 3.0
        ; wallet_type = "rest"
        ; wallet_id = "account"
        }
      ]
  in
  Alcotest.(check int) "one entry after merge" 1 (List.length merged);
  match merged with
  | b :: _ ->
    near 12.5 b.Dio_oracle.Oracle_balances.available;
    near 13.0 b.Dio_oracle.Oracle_balances.total
  | [] -> Alcotest.fail "expected one merged balance"
;;

let () =
  Alcotest.run
    "oracle_balances"
    [ ( "aggregation"
      , [ Alcotest.test_case "quote aggregation" `Quick test_quote_aggregation
        ; Alcotest.test_case "same-wallet merge" `Quick test_merge_same_wallet
        ] )
    ]
;;
