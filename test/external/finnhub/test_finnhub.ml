open Alcotest

let test_mark_store_push_get () =
  Finnhub.Mark_store.push "AAPL" ~price:150.0 ~size:10.0;
  match Finnhub.Mark_store.get_mark "AAPL" with
  | Some (price, size, _ts) ->
    check (float 0.001) "mark price" 150.0 price;
    check (float 0.001) "mark size" 10.0 size
  | None -> check bool "mark present" true false
;;

let test_mark_store_age () =
  Finnhub.Mark_store.push "MSFT" ~price:300.0 ~size:5.0;
  match Finnhub.Mark_store.get_mark_age "MSFT" with
  | Some age -> check bool "mark age is non-negative" true (age >= 0.0)
  | None -> check bool "mark age present" true false
;;

let test_mark_store_rejects_nonpositive () =
  Finnhub.Mark_store.push "NVDA" ~price:0.0 ~size:1.0;
  check bool "non-positive price ignored" true (Finnhub.Mark_store.get_mark "NVDA" = None)
;;

let test_synthetic_quote () =
  let bid, ask = Alpaca.Fallback.synthetic_quote ~mark:100.0 ~half_spread:0.01 in
  check (float 0.0001) "bid" 99.99 bid;
  check (float 0.0001) "ask" 100.01 ask
;;

let test_within_bounds () =
  check
    bool
    "within bounds"
    true
    (Alpaca.Fallback.within_bounds ~mark:100.0 ~mid:100.0 ~max_divergence:0.05);
  check
    bool
    "exceeds bounds"
    false
    (Alpaca.Fallback.within_bounds ~mark:120.0 ~mid:100.0 ~max_divergence:0.05);
  check
    bool
    "no real mid permits fallback"
    true
    (Alpaca.Fallback.within_bounds ~mark:120.0 ~mid:0.0 ~max_divergence:0.05)
;;

let test_poll_interval_budget () =
  check
    (float 0.001)
    "2 symbols"
    5.0
    (Finnhub.Poller.poll_interval_seconds ~num_symbols:2);
  check
    (float 0.001)
    "10 symbols"
    11.0
    (Finnhub.Poller.poll_interval_seconds ~num_symbols:10);
  check
    (float 0.001)
    "30 symbols"
    31.0
    (Finnhub.Poller.poll_interval_seconds ~num_symbols:30);
  let budget_ok n =
    let iv = Finnhub.Poller.poll_interval_seconds ~num_symbols:n in
    float_of_int n *. (60.0 /. iv) <= 60.0
  in
  check
    bool
    "budget within 60/min for 1..50 symbols"
    true
    (List.for_all budget_ok (List.init 50 (fun i -> i + 1)))
;;

let () =
  Alcotest.run
    "finnhub"
    [ ( "mark_store"
      , [ test_case "push/get" `Quick test_mark_store_push_get
        ; test_case "age" `Quick test_mark_store_age
        ; test_case "rejects non-positive" `Quick test_mark_store_rejects_nonpositive
        ] )
    ; ( "fallback_math"
      , [ test_case "synthetic quote" `Quick test_synthetic_quote
        ; test_case "within bounds" `Quick test_within_bounds
        ] )
    ; "poller", [ test_case "poll interval rate budget" `Quick test_poll_interval_budget ]
    ]
;;
