let test_process_orderbook_snapshot () =
  Lighter.Instruments_feed.initialize ["OBETH"];
  Lighter.Orderbook_feed.initialize ["OBETH"];

  let json = `Assoc [
    ("type", `String "snapshot/order_book");
    ("channel", `String "order_book:0");
    ("order_book", `Assoc [
      ("b", `List [
        `Assoc [("price", `String "2180.00"); ("size", `String "5.0")];
        `Assoc [("price", `String "2179.00"); ("size", `String "3.0")]
      ]);
      ("a", `List [
        `Assoc [("price", `String "2181.00"); ("size", `String "4.0")];
        `Assoc [("price", `String "2182.00"); ("size", `String "2.0")]
      ])
    ])
  ] in

  Lighter.Orderbook_feed.process_orderbook_snapshot ~market_index:0 json;

  let best_opt = Lighter.Orderbook_feed.get_best_bid_ask "OBETH" in
  match best_opt with
  | Some (bid_px, bid_sz, ask_px, ask_sz) ->
      Alcotest.(check (float 0.000001)) "bid_px" 2180.00 bid_px;
      Alcotest.(check (float 0.000001)) "bid_sz" 5.0 bid_sz;
      Alcotest.(check (float 0.000001)) "ask_px" 2181.00 ask_px;
      Alcotest.(check (float 0.000001)) "ask_sz" 4.0 ask_sz
  | None -> Alcotest.fail "Orderbook snapshot not processed"

let test_process_orderbook_delta () =
  Lighter.Instruments_feed.initialize ["OBDELTA"];
  Lighter.Orderbook_feed.initialize ["OBDELTA"];

  (* Initial snapshot *)
  let snapshot = `Assoc [
    ("type", `String "snapshot/order_book");
    ("channel", `String "order_book:0");
    ("order_book", `Assoc [
      ("b", `List [
        `Assoc [("price", `String "100.0"); ("size", `String "10.0")]
      ]);
      ("a", `List [
        `Assoc [("price", `String "101.0"); ("size", `String "8.0")]
      ])
    ])
  ] in
  Lighter.Orderbook_feed.process_orderbook_snapshot ~market_index:0 snapshot;

  (* Delta: update bid size, add new ask level *)
  let delta = `Assoc [
    ("type", `String "update/order_book");
    ("channel", `String "order_book:0");
    ("order_book", `Assoc [
      ("b", `List [
        `Assoc [("price", `String "100.0"); ("size", `String "15.0")]
      ]);
      ("a", `List [
        `Assoc [("price", `String "102.0"); ("size", `String "3.0")]
      ])
    ])
  ] in
  Lighter.Orderbook_feed.process_orderbook_update ~market_index:0 delta;

  let best = Lighter.Orderbook_feed.get_best_bid_ask "OBDELTA" in
  (match best with
   | Some (bid_px, bid_sz, ask_px, ask_sz) ->
       Alcotest.(check (float 0.000001)) "bid_px unchanged" 100.0 bid_px;
       Alcotest.(check (float 0.000001)) "bid_sz updated" 15.0 bid_sz;
       Alcotest.(check (float 0.000001)) "ask_px best" 101.0 ask_px;
       Alcotest.(check (float 0.000001)) "ask_sz unchanged" 8.0 ask_sz
   | None -> Alcotest.fail "Orderbook delta not applied")

let test_process_orderbook_delta_remove () =
  Lighter.Instruments_feed.initialize ["OBREM"];
  Lighter.Orderbook_feed.initialize ["OBREM"];

  let snapshot = `Assoc [
    ("type", `String "snapshot/order_book");
    ("channel", `String "order_book:0");
    ("order_book", `Assoc [
      ("b", `List [
        `Assoc [("price", `String "100.0"); ("size", `String "10.0")];
        `Assoc [("price", `String "99.0"); ("size", `String "5.0")]
      ]);
      ("a", `List [
        `Assoc [("price", `String "101.0"); ("size", `String "8.0")]
      ])
    ])
  ] in
  Lighter.Orderbook_feed.process_orderbook_snapshot ~market_index:0 snapshot;

  (* Remove top bid level via size=0 *)
  let delta = `Assoc [
    ("type", `String "update/order_book");
    ("channel", `String "order_book:0");
    ("order_book", `Assoc [
      ("b", `List [
        `Assoc [("price", `String "100.0"); ("size", `String "0.0")]
      ]);
      ("a", `List [])
    ])
  ] in
  Lighter.Orderbook_feed.process_orderbook_update ~market_index:0 delta;

  let best = Lighter.Orderbook_feed.get_best_bid_ask "OBREM" in
  (match best with
   | Some (bid_px, _, _, _) ->
       Alcotest.(check (float 0.000001)) "new best bid" 99.0 bid_px
   | None -> Alcotest.fail "Expected orderbook after removal")

let test_has_orderbook_data () =
  Lighter.Instruments_feed.initialize ["OBREADY"];
  Lighter.Orderbook_feed.initialize ["OBREADY"];

  Alcotest.(check bool) "initially false" false
    (Lighter.Orderbook_feed.has_orderbook_data "OBREADY");

  let json = `Assoc [
    ("type", `String "snapshot/order_book");
    ("channel", `String "order_book:0");
    ("order_book", `Assoc [
      ("b", `List [`Assoc [("price", `String "50.0"); ("size", `String "1.0")]]);
      ("a", `List [`Assoc [("price", `String "51.0"); ("size", `String "1.0")]])
    ])
  ] in
  Lighter.Orderbook_feed.process_orderbook_snapshot ~market_index:0 json;

  Alcotest.(check bool) "true after snapshot" true
    (Lighter.Orderbook_feed.has_orderbook_data "OBREADY")

let test_get_best_bid_ask_unknown () =
  let result = Lighter.Orderbook_feed.get_best_bid_ask "TOTALLY_UNKNOWN_OB" in
  Alcotest.(check bool) "None for unknown" true (Option.is_none result)

let test_null_order_book_field () =
  Lighter.Instruments_feed.initialize ["OBNULL"];
  Lighter.Orderbook_feed.initialize ["OBNULL"];

  let json = `Assoc [
    ("type", `String "snapshot/order_book");
    ("channel", `String "order_book:0");
    ("order_book", `Null)
  ] in
  (* Should not crash *)
  Lighter.Orderbook_feed.process_orderbook_snapshot ~market_index:0 json;
  let result = Lighter.Orderbook_feed.get_best_bid_ask "OBNULL" in
  Alcotest.(check bool) "still None" true (Option.is_none result)

let () =
  Alcotest.run "Lighter Orderbook Feed" [
    "processing", [
      Alcotest.test_case "process_snapshot" `Quick test_process_orderbook_snapshot;
      Alcotest.test_case "process_delta_update" `Quick test_process_orderbook_delta;
      Alcotest.test_case "process_delta_remove" `Quick test_process_orderbook_delta_remove;
      Alcotest.test_case "null order_book field" `Quick test_null_order_book_field;
    ];
    "state", [
      Alcotest.test_case "has_orderbook_data" `Quick test_has_orderbook_data;
      Alcotest.test_case "unknown symbol" `Quick test_get_best_bid_ask_unknown;
    ]
  ]
