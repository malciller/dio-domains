let test_process_market_data () =
  (* Ensure symbol is registered so we can test the data processing *)
  Hyperliquid.Instruments_feed.initialize ["HYPE"];
  Hyperliquid.Orderbook_feed.initialize ["HYPE"];
  
  let raw_json_str = "{\"channel\":\"l2Book\",\"data\":{\"coin\":\"HYPE\",\"levels\":[[{\"px\":\"100.5\",\"sz\":\"1.5\"}],[{\"px\":\"101.0\",\"sz\":\"2.0\"}]]}}" in
  Hyperliquid.Orderbook_feed.process_raw_market_data raw_json_str;
  
  let best_opt = Hyperliquid.Orderbook_feed.get_best_bid_ask "HYPE" in
  match best_opt with
  | Some (bid_px, bid_sz, ask_px, ask_sz) ->
      Alcotest.(check (float 0.000001)) "bid_px" 100.5 bid_px;
      Alcotest.(check (float 0.000001)) "bid_sz" 1.5 bid_sz;
      Alcotest.(check (float 0.000001)) "ask_px" 101.0 ask_px;
      Alcotest.(check (float 0.000001)) "ask_sz" 2.0 ask_sz
  | None -> Alcotest.fail "Orderbook update not processed"

let () =
  Alcotest.run "Hyperliquid Orderbook Feed" [
    "processing", [
      Alcotest.test_case "process_market_data" `Quick test_process_market_data;
    ]
  ]
