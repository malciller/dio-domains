let test_cache_initially_empty () =
  match Ibkr.Contracts.get_cached ~symbol:"IBKR_CONTRACT_UNKNOWN" with
  | None -> Alcotest.(check bool) "cache empty for unknown" true true
  | Some _ -> Alcotest.fail "unexpected cached contract"

let test_get_precision_unknown () =
  match Ibkr.Contracts.get_precision ~symbol:"IBKR_PREC_UNKNOWN" with
  | None -> Alcotest.(check bool) "no precision for unknown" true true
  | Some _ -> Alcotest.fail "unexpected precision for unknown symbol"

let () =
  Alcotest.run "IBKR Contracts" [
    "cache", [
      Alcotest.test_case "cache_initially_empty" `Quick test_cache_initially_empty;
    ];
    "precision", [
      Alcotest.test_case "get_precision_unknown" `Quick test_get_precision_unknown;
    ];
  ]
