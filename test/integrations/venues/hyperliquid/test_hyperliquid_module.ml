module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types
module ExecFeed = Hyperliquid.Executions_feed

let test_module_registration () =
  Alcotest.(check string)
    "module name is hyperliquid"
    "hyperliquid"
    Hyperliquid.Module.Hyperliquid_impl.name
;;

let test_type_conversions () =
  let side_buy = Hyperliquid.Module.Hyperliquid_impl.string_of_side Types.Buy in
  Alcotest.(check string) "Buy" "buy" side_buy;
  let side_sell = Hyperliquid.Module.Hyperliquid_impl.string_of_side Types.Sell in
  Alcotest.(check string) "Sell" "sell" side_sell;
  let order_limit =
    Hyperliquid.Module.Hyperliquid_impl.string_of_order_type Types.Limit
  in
  Alcotest.(check string) "Limit" "limit" order_limit;
  let order_market =
    Hyperliquid.Module.Hyperliquid_impl.string_of_order_type Types.Market
  in
  Alcotest.(check string) "Market" "market" order_market
;;

let test_status_conversions () =
  let check_status hl_status expected_name =
    let result =
      Hyperliquid.Module.Hyperliquid_impl.status_of_hyperliquid_status hl_status
    in
    let name =
      match result with
      | Types.New -> "New"
      | Types.PartiallyFilled -> "PartiallyFilled"
      | Types.Filled -> "Filled"
      | Types.Canceled -> "Canceled"
      | Types.Expired -> "Expired"
      | Types.Rejected -> "Rejected"
      | Types.Unknown s -> "Unknown:" ^ s
      | _ -> "other"
    in
    Alcotest.(check string) expected_name expected_name name
  in
  check_status ExecFeed.PendingNewStatus "New";
  check_status ExecFeed.NewStatus "New";
  check_status ExecFeed.PartiallyFilledStatus "PartiallyFilled";
  check_status ExecFeed.FilledStatus "Filled";
  check_status ExecFeed.CanceledStatus "Canceled";
  check_status ExecFeed.ExpiredStatus "Expired";
  check_status ExecFeed.RejectedStatus "Rejected";
  check_status (ExecFeed.UnknownStatus "foo") "Unknown:foo"
;;

let test_side_conversions () =
  let buy = Hyperliquid.Module.Hyperliquid_impl.side_of_hyperliquid_side ExecFeed.Buy in
  Alcotest.(check bool) "Buy -> Buy" true (buy = Types.Buy);
  let sell = Hyperliquid.Module.Hyperliquid_impl.side_of_hyperliquid_side ExecFeed.Sell in
  Alcotest.(check bool) "Sell -> Sell" true (sell = Types.Sell)
;;

let test_data_accessors_empty () =
  let tob = Hyperliquid.Module.Hyperliquid_impl.get_top_of_book ~symbol:"NODATA_TOB" in
  Alcotest.(check bool) "no top of book" true (Option.is_none tob);
  let bal =
    Hyperliquid.Module.Hyperliquid_impl.get_tradeable_balance ~asset:"NODATA_ASSET"
  in
  Alcotest.(check (float 0.000001)) "no balance" 0.0 bal
;;

let test_spot_view_excludes_perp () =
  ExecFeed.initialize [ "MODPERP"; "MODPERP/USDC" ];
  ExecFeed.inject_open_orders
    (`List
      [ `Assoc
          [ "coin", `String "MODPERP"
          ; "oid", `Int 777
          ; "side", `String "B"
          ; "limitPx", `String "90.0"
          ; "origSz", `String "1.0"
          ; "timestamp", `Int 1_000_000
          ]
      ]);
  let spot = Hyperliquid.Module.Hyperliquid_impl.get_open_orders ~symbol:"MODPERP/USDC" in
  Alcotest.(check int) "spot pair has no perp order" 0 (List.length spot);
  let perp = Hyperliquid.Module.Hyperliquid_impl.get_open_orders ~symbol:"MODPERP" in
  Alcotest.(check int) "perp view has the order" 1 (List.length perp);
  (match perp with
   | [ o ] ->
     Alcotest.(check bool)
       "perp order is labelled perp"
       true
       (o.Dio_exchange.Exchange_intf.Types.market = Dio_exchange.Exchange_intf.Types.Perp)
   | _ -> Alcotest.fail "expected exactly one perp order");
  let seen = ref [] in
  Hyperliquid.Module.Hyperliquid_impl.iter_open_orders_fast
    ~symbol:"MODPERP/USDC"
    (fun id _ _ _ _ -> seen := id :: !seen);
  Alcotest.(check bool) "spot iterator skips the perp order" false (List.mem "777" !seen)
;;

let test_get_open_order_by_coin () =
  (* Placement injection stores a perp order under the registered spot pair. Reading it by
     the bare coin must resolve through the global order index. *)
  ExecFeed.initialize [ "MODFETCH"; "MODFETCH/USDC" ];
  ExecFeed.inject_order
    ~symbol:"MODFETCH"
    ~order_id:"coin-order"
    ~side:ExecFeed.Buy
    ~qty:1.0
    ~price:100.0
    ();
  match
    Hyperliquid.Module.Hyperliquid_impl.get_open_order
      ~symbol:"MODFETCH"
      ~order_id:"coin-order"
  with
  | Some o ->
    Alcotest.(check string)
      "order id"
      "coin-order"
      o.Dio_exchange.Exchange_intf.Types.order_id
  | None -> Alcotest.fail "bare-coin lookup did not find the pair-keyed order"
;;

let test_resolve_amend_replacement_from_feed () =
  (* The modify ack has no replacement oid; the order-updates feed links the new leg by
     cloid. Resolution must find it without waiting for a round trip. *)
  ExecFeed.initialize [ "AMEND_RESOLVE/USDC" ];
  ExecFeed.inject_order
    ~symbol:"AMEND_RESOLVE/USDC"
    ~order_id:"new-oid"
    ~side:ExecFeed.Buy
    ~qty:1.0
    ~price:100.0
    ~cl_ord_id:"0xRESOLVE"
    ();
  let resolved =
    Lwt_main.run
      (Hyperliquid.Module.Hyperliquid_impl.resolve_amend_replacement
         ~cloid:"0xresolve"
         ~old_order_id:"old-oid")
  in
  Alcotest.(check (option string)) "replacement found by cloid" (Some "new-oid") resolved
;;

let () =
  Alcotest.run
    "Hyperliquid Module"
    [ ( "basics"
      , [ Alcotest.test_case "registration" `Quick test_module_registration
        ; Alcotest.test_case "type conversions" `Quick test_type_conversions
        ] )
    ; ( "conversions"
      , [ Alcotest.test_case "status conversions" `Quick test_status_conversions
        ; Alcotest.test_case "side conversions" `Quick test_side_conversions
        ] )
    ; ( "amend"
      , [ Alcotest.test_case
            "resolve replacement from feed"
            `Quick
            test_resolve_amend_replacement_from_feed
        ] )
    ; ( "data_access"
      , [ Alcotest.test_case "empty data accessors" `Quick test_data_accessors_empty
        ; Alcotest.test_case "spot view excludes perp" `Quick test_spot_view_excludes_perp
        ; Alcotest.test_case
            "open order resolves by bare coin"
            `Quick
            test_get_open_order_by_coin
        ] )
    ]
;;
