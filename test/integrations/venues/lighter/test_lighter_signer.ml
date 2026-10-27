open Alcotest

let test_advance_never_rewinds () =
  (* A resync fetches the exchange's next nonce while higher-nonce txs may already be in
     flight locally; adopting it verbatim would hand out duplicates. *)
  Lighter.Signer.advance_nonce_to 100;
  Lighter.Signer.advance_nonce_to 42;
  check int "stale resync ignored" 100 (Lighter.Signer.get_and_increment_nonce ())
;;

let test_advance_moves_forward () =
  Lighter.Signer.advance_nonce_to 200;
  check
    int
    "counter adopts exchange nonce"
    200
    (Lighter.Signer.get_and_increment_nonce ())
;;

let () =
  run
    "lighter_signer"
    [ ( "nonce"
      , [ test_case "advance_never_rewinds" `Quick test_advance_never_rewinds
        ; test_case "advance_moves_forward" `Quick test_advance_moves_forward
        ] )
    ]
;;
