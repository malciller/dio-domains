let test_hl_order_type_mapping () =
  let hl_ot1 = Hyperliquid.Hyperliquid_actions.hl_order_type Dio_exchange.Exchange_intf.Types.Limit None in
  (match hl_ot1 with
   | Hyperliquid.Hyperliquid_types.Limit { tif = Gtc } -> Alcotest.(check bool) "Limit default Gtc" true true
   | _ -> Alcotest.fail "Expected Limit GTC");

  let hl_ot2 = Hyperliquid.Hyperliquid_actions.hl_order_type Dio_exchange.Exchange_intf.Types.Market (Some Dio_exchange.Exchange_intf.Types.IOC) in
  (match hl_ot2 with
   | Hyperliquid.Hyperliquid_types.Limit { tif = Ioc } -> Alcotest.(check bool) "Market IOC maps to Limit IOC" true true
   | _ -> Alcotest.fail "Expected Limit IOC");

  let hl_ot3 = Hyperliquid.Hyperliquid_actions.hl_order_type Dio_exchange.Exchange_intf.Types.TakeProfit None in
  (match hl_ot3 with
   | Hyperliquid.Hyperliquid_types.Trigger { tpsl = Tp; _ } -> Alcotest.(check bool) "TakeProfit maps to Trigger Tp" true true
   | _ -> Alcotest.fail "Expected Trigger Tp")

let test_to_hl_order_wire () =
  let hl_ot = Hyperliquid.Hyperliquid_types.Limit { tif = Gtc } in
  let wire = Hyperliquid.Hyperliquid_actions.to_hl_order_wire 
      ~qty:0.5 
      ~symbol:"BTC" 
      ~asset_index:0
      ~ot:hl_ot 
      ~side:Dio_exchange.Exchange_intf.Types.Buy 
      ~limit_price:(Some 50000.0) 
      ~reduce_only:(Some true) 
      ~cl_ord_id:(Some "test_id") 
  in
  
  Alcotest.(check bool) "is buy" true wire.b;
  Alcotest.(check string) "price formatted" "50000" wire.p;
  Alcotest.(check string) "size formatted" "0.5" wire.s;
  Alcotest.(check bool) "reduce_only matches" true wire.r;
  Alcotest.(check (option string)) "cl_ord_id matches" (Some "test_id") wire.c;
  
  let wire2 = Hyperliquid.Hyperliquid_actions.to_hl_order_wire 
      ~qty:1.0 
      ~symbol:"ETH" 
      ~asset_index:0
      ~ot:hl_ot 
      ~side:Dio_exchange.Exchange_intf.Types.Sell 
      ~limit_price:None 
      ~reduce_only:None
      ~cl_ord_id:None
  in
  
  Alcotest.(check bool) "is sell" false wire2.b;
  Alcotest.(check string) "market defaults to 0.0" "0.0" wire2.p;
  Alcotest.(check bool) "reduce_only defaults false" false wire2.r;
  Alcotest.(check (option string)) "cl_ord_id none" None wire2.c

let () =
  Alcotest.run "Hyperliquid Actions" [
    "mappings", [
      Alcotest.test_case "hl_order_type_mapping" `Quick test_hl_order_type_mapping;
      Alcotest.test_case "to_hl_order_wire" `Quick test_to_hl_order_wire;
    ]
  ]
