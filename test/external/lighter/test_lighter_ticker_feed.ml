let test_process_ticker_update () =
  (* Initialize instruments feed with mock data so get_symbol works *)
  Lighter.Instruments_feed.initialize ["ETH"];
  Lighter.Ticker_feed.initialize ["ETH"];

  (* Lighter nests ticker data under "ticker" with "b" (bid) and "a" (ask) objects *)
  let json = `Assoc [
    ("type", `String "update/ticker");
    ("channel", `String "ticker:0");
    ("ticker", `Assoc [
      ("s", `String "ETH");
      ("b", `Assoc [("price", `String "2180.50"); ("size", `String "10.5")]);
      ("a", `Assoc [("price", `String "2181.00"); ("size", `String "8.3")])
    ])
  ] in

  Lighter.Ticker_feed.process_ticker_update ~market_index:0 json;

  let ticker_opt = Lighter.Ticker_feed.get_latest_ticker "ETH" in
  match ticker_opt with
  | Some t ->
      Alcotest.(check (float 0.000001)) "bid" 2180.50 t.bid;
      Alcotest.(check (float 0.000001)) "ask" 2181.00 t.ask;
      Alcotest.(check (float 0.000001)) "bid_size" 10.5 t.bid_size;
      Alcotest.(check (float 0.000001)) "ask_size" 8.3 t.ask_size;
      Alcotest.(check string) "symbol" "ETH" t.symbol
  | None -> Alcotest.fail "Ticker update not processed"

let test_ticker_deduplication () =
  Lighter.Instruments_feed.initialize ["DEDUP"];
  Lighter.Ticker_feed.initialize ["DEDUP"];

  let json = `Assoc [
    ("type", `String "update/ticker");
    ("channel", `String "ticker:0");
    ("ticker", `Assoc [
      ("b", `Assoc [("price", `String "100.0"); ("size", `String "1.0")]);
      ("a", `Assoc [("price", `String "101.0"); ("size", `String "2.0")])
    ])
  ] in

  Lighter.Ticker_feed.process_ticker_update ~market_index:0 json;
  let pos1 = Lighter.Ticker_feed.get_current_position "DEDUP" in

  (* Feed identical data — should not write a new entry *)
  Lighter.Ticker_feed.process_ticker_update ~market_index:0 json;
  let pos2 = Lighter.Ticker_feed.get_current_position "DEDUP" in

  Alcotest.(check int) "no duplicate write" pos1 pos2

let test_has_price_data () =
  Lighter.Instruments_feed.initialize ["HASPRICE"];
  Lighter.Ticker_feed.initialize ["HASPRICE"];

  Alcotest.(check bool) "initially false" false
    (Lighter.Ticker_feed.has_price_data "HASPRICE");

  let json = `Assoc [
    ("type", `String "update/ticker");
    ("channel", `String "ticker:0");
    ("ticker", `Assoc [
      ("b", `Assoc [("price", `String "50.0"); ("size", `String "1.0")]);
      ("a", `Assoc [("price", `String "51.0"); ("size", `String "1.0")])
    ])
  ] in
  Lighter.Ticker_feed.process_ticker_update ~market_index:0 json;

  Alcotest.(check bool) "true after update" true
    (Lighter.Ticker_feed.has_price_data "HASPRICE")

let test_get_latest_ticker_unknown () =
  let ticker = Lighter.Ticker_feed.get_latest_ticker "TOTALLY_UNKNOWN" in
  Alcotest.(check bool) "None for unknown" true (Option.is_none ticker)

let test_null_ticker_field () =
  Lighter.Instruments_feed.initialize ["NULLTICK"];
  Lighter.Ticker_feed.initialize ["NULLTICK"];

  (* A message with null ticker field should not crash *)
  let json = `Assoc [
    ("type", `String "subscribed/ticker");
    ("channel", `String "ticker:0");
    ("ticker", `Null)
  ] in
  Lighter.Ticker_feed.process_ticker_update ~market_index:0 json;

  let ticker = Lighter.Ticker_feed.get_latest_ticker "NULLTICK" in
  Alcotest.(check bool) "still None" true (Option.is_none ticker)

let () =
  Alcotest.run "Lighter Ticker Feed" [
    "processing", [
      Alcotest.test_case "process_ticker_update" `Quick test_process_ticker_update;
      Alcotest.test_case "deduplication" `Quick test_ticker_deduplication;
      Alcotest.test_case "null ticker field" `Quick test_null_ticker_field;
    ];
    "state", [
      Alcotest.test_case "has_price_data" `Quick test_has_price_data;
      Alcotest.test_case "unknown ticker" `Quick test_get_latest_ticker_unknown;
    ]
  ]
