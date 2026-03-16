module ExTypes = Dio_exchange.Exchange_intf.Types

let test_hl_order_type () =
  let hl_type1 = Hyperliquid.Actions.hl_order_type ExTypes.Limit (Some ExTypes.GTC) in
  (match hl_type1 with
  | Hyperliquid.Types.Limit { tif } -> Alcotest.(check string) "Limit GTC" "Alo" (Hyperliquid.Types.tif_to_string tif) (* The code maps Limit to Alo always, tif arg is ignored *)
  | _ -> Alcotest.fail "Expected Limit");
  
  let hl_type2 = Hyperliquid.Actions.hl_order_type ExTypes.Market None in
  (match hl_type2 with
  | Hyperliquid.Types.Limit { tif } -> Alcotest.(check string) "Market IOC" "Ioc" (Hyperliquid.Types.tif_to_string tif)
  | _ -> Alcotest.fail "Expected Limit")

let test_format_number () =
  Alcotest.(check string) "integer" "1" (Hyperliquid.Actions.format_number 1.0);
  Alcotest.(check string) "decimals" "1.234" (Hyperliquid.Actions.format_number 1.234);
  Alcotest.(check string) "trailing zeros stripped" "1.234" (Hyperliquid.Actions.format_number 1.23400000);
  Alcotest.(check string) "zero" "0" (Hyperliquid.Actions.format_number 0.0);
  Alcotest.(check string) "negative zero" "0" (Hyperliquid.Actions.format_number (-0.0));
  Alcotest.(check string) "small number" "0.0001" (Hyperliquid.Actions.format_number 0.0001)

let test_to_hl_order_wire () =
  let wire = Hyperliquid.Actions.to_hl_order_wire
    ~qty:1.5
    ~symbol:"TEST"
    ~asset_index:5
    ~ot:(Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Alo })
    ~side:ExTypes.Buy
    ~limit_price:(Some 123.45)
    ~reduce_only:None
    ~cl_ord_id:(Some "client_id")
  in
  Alcotest.(check int) "a" 5 wire.a;
  Alcotest.(check bool) "b" true wire.b;
  Alcotest.(check string) "p" "123.45" wire.p;
  Alcotest.(check string) "s" "1.5" wire.s;
  Alcotest.(check bool) "r" false wire.r;
  Alcotest.(check (option string)) "c" (Some "client_id") wire.c

let () =
  Alcotest.run "Hyperliquid Actions" [
    "conversion", [
      Alcotest.test_case "hl_order_type" `Quick test_hl_order_type;
      Alcotest.test_case "format_number" `Quick test_format_number;
      Alcotest.test_case "to_hl_order_wire" `Quick test_to_hl_order_wire;
    ]
  ]
