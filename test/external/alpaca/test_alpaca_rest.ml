(* Alpaca REST submission guards: the venue rejects limit prices off the
   valid tick band with HTTP 422 ("sub-penny increment does not fulfill
   minimum pricing criteria"). Prices >= $1.00 must land on penny increments;
   sub-penny ($0.0001) is only valid below $1.00. *)

let near ?(eps = 1e-9) a b = Alcotest.(check (float eps)) "approx" a b

let test_rounds_above_one_dollar_to_penny () =
  (* The production failure: grid level 710.8678 must not reach the wire. *)
  near 710.87 (Alpaca.Rest.round_limit_price 710.8678);
  near 100.0 (Alpaca.Rest.round_limit_price 99.999999);
  near 2.55 (Alpaca.Rest.round_limit_price 2.5468)
;;

let test_keeps_sub_penny_below_one_dollar () =
  near 0.8678 (Alpaca.Rest.round_limit_price 0.8678);
  near 0.0001 (Alpaca.Rest.round_limit_price 0.00014);
  near 0.9999 (Alpaca.Rest.round_limit_price 0.99994)
;;

let test_exact_prices_pass_through () =
  near 710.87 (Alpaca.Rest.round_limit_price 710.87);
  near 1.0 (Alpaca.Rest.round_limit_price 1.0);
  near 133.94 (Alpaca.Rest.round_limit_price 133.94)
;;

let test_non_finite_pass_through () =
  Alcotest.(check bool)
    "nan preserved"
    (Float.is_nan (Alpaca.Rest.round_limit_price Float.nan))
    true;
  Alcotest.(check bool)
    "infinity preserved"
    (Float.is_infinite (Alpaca.Rest.round_limit_price Float.infinity))
    true
;;

let () =
  Alcotest.run
    "alpaca_rest"
    [ ( "tick rounding"
      , [ Alcotest.test_case
            ">= $1 rounds to penny"
            `Quick
            test_rounds_above_one_dollar_to_penny
        ; Alcotest.test_case
            "< $1 keeps sub-penny"
            `Quick
            test_keeps_sub_penny_below_one_dollar
        ; Alcotest.test_case "on-tick pass-through" `Quick test_exact_prices_pass_through
        ; Alcotest.test_case "non-finite pass-through" `Quick test_non_finite_pass_through
        ] )
    ]
;;
