open Dio_strategies

let approx a b = Float.abs (a -. b) < 1e-9

let holds_eq a b =
  List.length a = List.length b
  && List.for_all2 (fun (t1, q1) (t2, q2) -> approx t1 t2 && approx q1 q2) a b
;;

let test_arm_then_outstanding () =
  (* positive balance delta cannot certify netting; fresh age keeps the hold *)
  let holds = Platform_accounting.arm_sell_hold ~holds:[] ~qty:2.0 ~now:100.0 in
  let holds', unnetted =
    Platform_accounting.unnetted_sell_hold
      ~use_unnetted:true
      ~holds
      ~last_balance_delta:1.0
      ~now:101.0
      ~base_balance_age:(Some 0.0)
  in
  Alcotest.(check bool) "outstanding hold kept" true (approx unnetted 2.0);
  Alcotest.(check bool) "holds unchanged" true (holds_eq holds' [ 100.0, 2.0 ])
;;

let test_released_by_flat_message () =
  (* flat message after placement, fresh age -> hold retired *)
  let holds = [ 100.0, 2.0 ] in
  let holds', unnetted =
    Platform_accounting.unnetted_sell_hold
      ~use_unnetted:true
      ~holds
      ~last_balance_delta:0.0
      ~now:101.0
      ~base_balance_age:(Some 0.0)
  in
  Alcotest.(check bool) "released" true (approx unnetted 0.0);
  Alcotest.(check bool) "no holds" true (holds' = [])
;;

let test_released_by_grace () =
  (* dead feed (age None): the grace retires an old hold *)
  let holds = [ 100.0, 2.0 ] in
  let _, unnetted =
    Platform_accounting.unnetted_sell_hold
      ~use_unnetted:true
      ~holds
      ~last_balance_delta:1.0
      ~now:120.0
      ~base_balance_age:None
  in
  Alcotest.(check bool) "grace expired" true (approx unnetted 0.0)
;;

let test_disabled () =
  let holds = [ 100.0, 2.0 ] in
  let holds', unnetted =
    Platform_accounting.unnetted_sell_hold
      ~use_unnetted:false
      ~holds
      ~last_balance_delta:1.0
      ~now:101.0
      ~base_balance_age:(Some 0.0)
  in
  Alcotest.(check bool) "disabled returns zero" true (approx unnetted 0.0);
  Alcotest.(check bool) "disabled keeps holds" true (holds_eq holds' holds)
;;

let test_consume_fifo () =
  (* consume 1.5 oldest-first: first hold (1.0) fully, remainder (0.5) off the second *)
  let holds = [ 0.0, 1.0; 1.0, 2.0 ] in
  let holds' = Platform_accounting.consume_sell_hold_netting ~holds ~amount:1.5 in
  Alcotest.(check bool) "fifo partial" true (holds_eq holds' [ 1.0, 1.5 ])
;;

let test_unreflected_credit () =
  let credits = [ 100.0, 2.0; 50.0, 3.0 ] in
  let remaining, sum =
    Platform_accounting.unreflected_credit
      ~credits
      ~now:100.0
      ~base_balance_age:(Some 0.0)
  in
  Alcotest.(check bool) "sum keeps fresh" true (approx sum 2.0);
  Alcotest.(check bool) "drops stale" true (holds_eq remaining [ 100.0, 2.0 ])
;;

let test_committed_sell_base () =
  Alcotest.(check bool)
    "nets + venue-netted -> unnetted only"
    true
    (approx
       (Platform_accounting.effective_committed_sell_base
          ~balance_nets_open_order_holds:true
          ~hold_netted_from_venue_state:true
          ~ledger_total:10.0
          ~feed_total:4.0
          ~unnetted_hold:1.0)
       1.0);
  Alcotest.(check bool)
    "nets + feed loop -> ledger excess floored by unnetted"
    true
    (approx
       (Platform_accounting.effective_committed_sell_base
          ~balance_nets_open_order_holds:true
          ~hold_netted_from_venue_state:false
          ~ledger_total:10.0
          ~feed_total:4.0
          ~unnetted_hold:1.0)
       6.0);
  Alcotest.(check bool)
    "gross -> whole ledger"
    true
    (approx
       (Platform_accounting.effective_committed_sell_base
          ~balance_nets_open_order_holds:false
          ~hold_netted_from_venue_state:false
          ~ledger_total:10.0
          ~feed_total:4.0
          ~unnetted_hold:1.0)
       10.0)
;;

let test_available_base () =
  (* authoritative venue with a live figure: max(0, venue + credit - reserve - hold) *)
  Alcotest.(check bool)
    "alpaca venue figure"
    true
    (approx
       (Platform_accounting.alpaca_available_base
          ~venue_available:100.0
          ~ledger_balance:50.0
          ~unreflected_credit:1.0
          ~reserved_base:5.0
          ~committed_sell:2.0
          ~unnetted_hold:0.5)
       95.5);
  (* no venue figure: ledger basis *)
  Alcotest.(check bool)
    "alpaca fallback to ledger"
    true
    (approx
       (Platform_accounting.alpaca_available_base
          ~venue_available:nan
          ~ledger_balance:50.0
          ~unreflected_credit:1.0
          ~reserved_base:5.0
          ~committed_sell:2.0
          ~unnetted_hold:0.5)
       43.0);
  (* NaN venue balance dominates -> 0 *)
  Alcotest.(check bool)
    "nan venue balance -> 0"
    true
    (approx
       (Platform_accounting.available_base
          ~is_venue_authoritative:false
          ~asset_balance_nan:true
          ~venue_available:100.0
          ~ledger_balance:50.0
          ~unreflected_credit:1.0
          ~reserved_base:5.0
          ~committed_sell:2.0
          ~unnetted_hold:0.5)
       0.0);
  (* non-authoritative (ledger basis) *)
  Alcotest.(check bool)
    "ledger basis"
    true
    (approx
       (Platform_accounting.available_base
          ~is_venue_authoritative:false
          ~asset_balance_nan:false
          ~venue_available:100.0
          ~ledger_balance:50.0
          ~unreflected_credit:1.0
          ~reserved_base:5.0
          ~committed_sell:2.0
          ~unnetted_hold:0.5)
       43.0)
;;

let test_persisted_levels () =
  Alcotest.(check int) "price key rounds" 1000000 (Platform_accounting.price_key 100.0);
  Alcotest.(check bool)
    "within tolerance"
    true
    (Platform_accounting.price_within_tolerance ~reference:100.0 100.005);
  Alcotest.(check bool)
    "outside tolerance"
    false
    (Platform_accounting.price_within_tolerance ~reference:100.0 100.02);
  let open_levels, missing =
    Platform_accounting.partition_persisted_sell_levels
      [ 100.0, 1.0; 110.0, 2.0 ]
      [ "a", 100.0, 1.0 ]
  in
  Alcotest.(check int) "open levels" 1 (List.length open_levels);
  Alcotest.(check int) "missing levels" 1 (List.length missing);
  let deduped =
    Platform_accounting.dedupe_persisted_sell_levels [ 100.0, 1.0; 100.005, 2.0 ]
  in
  Alcotest.(check bool) "dedupe within tolerance" true (holds_eq deduped [ 100.005, 2.0 ])
;;

let () =
  Alcotest.run
    "platform_accounting"
    [ ( "sell_hold"
      , [ Alcotest.test_case "arm then outstanding" `Quick test_arm_then_outstanding
        ; Alcotest.test_case
            "released by flat message"
            `Quick
            test_released_by_flat_message
        ; Alcotest.test_case "released by grace" `Quick test_released_by_grace
        ; Alcotest.test_case "disabled" `Quick test_disabled
        ; Alcotest.test_case "consume FIFO" `Quick test_consume_fifo
        ] )
    ; ( "credit_and_ceiling"
      , [ Alcotest.test_case "unreflected credit" `Quick test_unreflected_credit
        ; Alcotest.test_case
            "effective committed sell base"
            `Quick
            test_committed_sell_base
        ; Alcotest.test_case "available base" `Quick test_available_base
        ] )
    ; ( "persisted_levels"
      , [ Alcotest.test_case "matching helpers" `Quick test_persisted_levels ] )
    ]
;;
