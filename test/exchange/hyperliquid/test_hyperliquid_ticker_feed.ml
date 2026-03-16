let test_process_allMids () =
  Hyperliquid.Instruments_feed.initialize ["HYPE"];
  Hyperliquid.Ticker_feed.initialize ["HYPE"];
  
  let json = `Assoc [
    ("channel", `String "allMids");
    ("data", `Assoc [
      ("mids", `Assoc [
        ("HYPE", `String "105.0");
        ("UNKNOWN", `String "99.0")
      ])
    ])
  ] in
  
  Hyperliquid.Ticker_feed.process_market_data json;
  
  let ticker_opt = Hyperliquid.Ticker_feed.get_latest_ticker "HYPE" in
  match ticker_opt with
  | Some t ->
      Alcotest.(check (float 0.000001)) "bid" 105.0 t.bid;
      Alcotest.(check (float 0.000001)) "ask" 105.0 t.ask;
      Alcotest.(check (float 0.000001)) "last" 105.0 t.last
  | None -> Alcotest.fail "Ticker update not processed"

let test_process_l2Book_ticker () =
  Hyperliquid.Instruments_feed.initialize ["HYPE"];
  Hyperliquid.Ticker_feed.initialize ["HYPE"];
  
  let json = `Assoc [
    ("channel", `String "l2Book");
    ("data", `Assoc [
      ("coin", `String "HYPE");
      ("levels", `List [
        `List [`Assoc [("px", `String "106.0")]]; (* bid *)
        `List [`Assoc [("px", `String "108.0")]]  (* ask *)
      ])
    ])
  ] in
  
  Hyperliquid.Ticker_feed.process_market_data json;
  
  let price_opt = Hyperliquid.Ticker_feed.get_latest_price "HYPE" in
  match price_opt with
  | Some p -> Alcotest.(check (float 0.000001)) "mid price" 107.0 p
  | None -> Alcotest.fail "l2Book ticker update not processed"

let () =
  Alcotest.run "Hyperliquid Ticker Feed" [
    "processing", [
      Alcotest.test_case "process_allMids" `Quick test_process_allMids;
      Alcotest.test_case "process_l2Book_ticker" `Quick test_process_l2Book_ticker;
    ]
  ]
