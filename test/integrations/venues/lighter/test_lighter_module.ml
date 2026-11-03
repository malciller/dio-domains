module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types
module ExecFeed = Lighter.Executions_feed

let test_module_name () =
  Alcotest.(check string)
    "module name is lighter"
    "lighter"
    Lighter.Module.Lighter_impl.name
;;

let test_status_conversions () =
  let check_status exec_status expected_name =
    let result = Lighter.Module.Lighter_impl.status_of_lighter_status exec_status in
    let name =
      match result with
      | Types.Pending -> "Pending"
      | Types.New -> "New"
      | Types.PartiallyFilled -> "PartiallyFilled"
      | Types.Filled -> "Filled"
      | Types.Canceled -> "Canceled"
      | Types.Expired -> "Expired"
      | Types.Rejected -> "Rejected"
      | Types.Unknown s -> "Unknown:" ^ s
    in
    Alcotest.(check string) expected_name expected_name name
  in
  check_status ExecFeed.PendingStatus "Pending";
  check_status ExecFeed.NewStatus "New";
  check_status ExecFeed.PartiallyFilledStatus "PartiallyFilled";
  check_status ExecFeed.FilledStatus "Filled";
  check_status ExecFeed.CanceledStatus "Canceled";
  check_status ExecFeed.ExpiredStatus "Expired";
  check_status ExecFeed.RejectedStatus "Rejected";
  check_status (ExecFeed.UnknownStatus "foo") "Unknown:foo"
;;

let test_side_conversions () =
  let buy = Lighter.Module.Lighter_impl.side_of_lighter_side ExecFeed.Buy in
  Alcotest.(check bool) "Buy -> Buy" true (buy = Types.Buy);
  let sell = Lighter.Module.Lighter_impl.side_of_lighter_side ExecFeed.Sell in
  Alcotest.(check bool) "Sell -> Sell" true (sell = Types.Sell)
;;

let test_data_accessors_empty () =
  let tob = Lighter.Module.Lighter_impl.get_top_of_book ~symbol:"NODATA_L_TOB" in
  Alcotest.(check bool) "no top of book" true (Option.is_none tob);
  let bal = Lighter.Module.Lighter_impl.get_tradeable_balance ~asset:"NODATA_L_ASSET" in
  Alcotest.(check (float 0.000001)) "no balance" 0.0 bal
;;

let test_get_fees_with_instruments () =
  (* Mock instruments carry 0.0 maker/taker fees. *)
  Lighter.Instruments_feed.initialize [ "FEETEST" ];
  let maker, taker = Lighter.Module.Lighter_impl.get_fees ~symbol:"FEETEST" in
  Alcotest.(check bool) "maker fee found" true (Option.is_some maker);
  Alcotest.(check bool) "taker fee found" true (Option.is_some taker);
  (* Unknown symbol *)
  let mk_miss, tk_miss = Lighter.Module.Lighter_impl.get_fees ~symbol:"NO_FEE_SYM" in
  Alcotest.(check bool) "maker None" true (Option.is_none mk_miss);
  Alcotest.(check bool) "taker None" true (Option.is_none tk_miss)
;;

let test_amend_requires_tracked_order () =
  Lighter.Instruments_feed.initialize [ "ETH/USDC" ];
  let result =
    Lwt_main.run
      (Lighter.Module.Lighter_impl.amend_order
         ~token:""
         ~order_id:"missing_order"
         ~symbol:"ETH/USDC"
         ~qty:0.006
         ~limit_price:2222.0
         ())
  in
  match result with
  | Ok _ -> Alcotest.fail "expected missing-order amend to be rejected"
  | Error msg ->
    Alcotest.(check string)
      "missing order guard"
      "Order not found for amendment: missing_order"
      msg
;;

let test_classify_send_tx_reply () =
  let is_ok = function
    | Ok () -> true
    | Error _ -> false
  in
  Alcotest.(check bool)
    "code 200 is success"
    true
    (is_ok (Lighter.Actions.classify_send_tx_reply {|{"code":200,"tx_hash":"0xabc"}|}));
  Alcotest.(check bool)
    "non-JSON body falls back to the HTTP status"
    true
    (is_ok (Lighter.Actions.classify_send_tx_reply "ok"));
  match
    Lighter.Actions.classify_send_tx_reply
      {|{"code":21107,"message":"reduce only would not reduce position"}|}
  with
  | Ok () -> Alcotest.fail "venue rejection reported as success"
  | Error msg ->
    Alcotest.(check string)
      "rejection carries the venue reason"
      "sendTx rejected by the venue (code=21107): reduce only would not reduce position"
      msg
;;

let test_resync_guard_bounds_each_step () =
  (* A hung REST call must cost one pass, not the loop. *)
  let started = Unix.gettimeofday () in
  Lwt_main.run
    (Lighter.Module.with_resync_guard ~timeout_s:0.05 "hang" (fun () -> fst (Lwt.wait ())));
  let elapsed = Unix.gettimeofday () -. started in
  Alcotest.(check bool) "returns after the deadline" true (elapsed < 2.0);
  (* A raising step is reported, never propagated. *)
  Lwt_main.run
    (Lighter.Module.with_resync_guard ~timeout_s:1.0 "raise" (fun () ->
       Lwt.fail (Failure "boom")));
  Lwt_main.run
    (Lighter.Module.with_resync_guard ~timeout_s:1.0 "ok" (fun () -> Lwt.return_unit))
;;

let () =
  Alcotest.run
    "Lighter Module"
    [ "basics", [ Alcotest.test_case "module name" `Quick test_module_name ]
    ; ( "conversions"
      , [ Alcotest.test_case "status conversions" `Quick test_status_conversions
        ; Alcotest.test_case "side conversions" `Quick test_side_conversions
        ] )
    ; ( "data_access"
      , [ Alcotest.test_case "empty data accessors" `Quick test_data_accessors_empty
        ; Alcotest.test_case
            "get_fees with instruments"
            `Quick
            test_get_fees_with_instruments
        ; Alcotest.test_case
            "amend requires tracked order"
            `Quick
            test_amend_requires_tracked_order
        ] )
    ; ( "actions"
      , [ Alcotest.test_case
            "sendTx reply classification"
            `Quick
            test_classify_send_tx_reply
        ] )
    ; ( "resync"
      , [ Alcotest.test_case
            "each step is bounded"
            `Quick
            test_resync_guard_bounds_each_step
        ] )
    ]
;;
