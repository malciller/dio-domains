let test_parse_fee_info_valid () =
  let json_str = {|{"userAddRate":"0.0001","userCrossRate":"0.0003"}|} in
  match Hyperliquid.Get_fee.parse_fee_info json_str with
  | Some info ->
      Alcotest.(check string) "exchange" "hyperliquid" info.exchange;
      Alcotest.(check (option (float 0.000001))) "maker" (Some 0.0001) info.maker_fee;
      Alcotest.(check (option (float 0.000001))) "taker" (Some 0.0003) info.taker_fee
  | None -> Alcotest.fail "Expected Some fee_info"

let test_parse_fee_info_malformed () =
  let json_str = "not valid json {{" in
  let result = Hyperliquid.Get_fee.parse_fee_info json_str in
  Alcotest.(check bool) "None for malformed" true (Option.is_none result)

let test_parse_fee_info_missing_fields () =
  let json_str = {|{"someOtherField":"value"}|} in
  match Hyperliquid.Get_fee.parse_fee_info json_str with
  | Some info ->
      Alcotest.(check (option (float 0.000001))) "maker None" None info.maker_fee;
      Alcotest.(check (option (float 0.000001))) "taker None" None info.taker_fee
  | None -> Alcotest.fail "Expected Some with None fields"

let test_parse_fee_info_zero_rates () =
  let json_str = {|{"userAddRate":"0.0","userCrossRate":"0.0"}|} in
  match Hyperliquid.Get_fee.parse_fee_info json_str with
  | Some info ->
      Alcotest.(check (option (float 0.000001))) "maker 0" (Some 0.0) info.maker_fee;
      Alcotest.(check (option (float 0.000001))) "taker 0" (Some 0.0) info.taker_fee
  | None -> Alcotest.fail "Expected Some fee_info"

let () =
  Alcotest.run "Hyperliquid Get Fee" [
    "parsing", [
      Alcotest.test_case "valid fee response" `Quick test_parse_fee_info_valid;
      Alcotest.test_case "malformed JSON" `Quick test_parse_fee_info_malformed;
      Alcotest.test_case "missing fields" `Quick test_parse_fee_info_missing_fields;
      Alcotest.test_case "zero rates" `Quick test_parse_fee_info_zero_rates;
    ]
  ]
