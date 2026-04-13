let test_contract_structure () =
  let contract = {
    Ibkr.Types.con_id = 265598;
    symbol = "AAPL";
    sec_type = "STK";
    exchange = "SMART";
    currency = "USD";
    local_symbol = "AAPL";
    trading_class = "AAPL";
    min_tick = 0.01;
    multiplier = "";
  } in

  Alcotest.(check int) "contract con_id" 265598 contract.con_id;
  Alcotest.(check string) "contract symbol" "AAPL" contract.symbol;
  Alcotest.(check string) "contract sec_type" "STK" contract.sec_type;
  Alcotest.(check string) "contract exchange" "SMART" contract.exchange;
  Alcotest.(check string) "contract currency" "USD" contract.currency;
  Alcotest.(check string) "contract local_symbol" "AAPL" contract.local_symbol;
  Alcotest.(check string) "contract trading_class" "AAPL" contract.trading_class;
  Alcotest.(check (float 0.001)) "contract min_tick" 0.01 contract.min_tick;
  Alcotest.(check string) "contract multiplier" "" contract.multiplier

let test_empty_contract () =
  let c = Ibkr.Types.empty_contract in
  Alcotest.(check int) "empty con_id" 0 c.con_id;
  Alcotest.(check string) "empty symbol" "" c.symbol;
  Alcotest.(check string) "empty sec_type" "STK" c.sec_type;
  Alcotest.(check string) "empty exchange" "SMART" c.exchange;
  Alcotest.(check string) "empty currency" "USD" c.currency;
  Alcotest.(check (float 0.001)) "empty min_tick" 0.01 c.min_tick

let test_make_stk_contract () =
  let c = Ibkr.Types.make_stk_contract ~symbol:"SPY" in
  Alcotest.(check string) "stk contract symbol" "SPY" c.symbol;
  Alcotest.(check string) "stk contract sec_type" "STK" c.sec_type;
  Alcotest.(check string) "stk contract exchange" "SMART" c.exchange;
  Alcotest.(check string) "stk contract currency" "USD" c.currency;
  Alcotest.(check int) "stk contract con_id" 0 c.con_id

let test_make_market_order () =
  let o = Ibkr.Types.make_market_order ~order_id:42 ~action:"BUY" ~qty:100.0 in
  Alcotest.(check int) "market order_id" 42 o.order_id;
  Alcotest.(check string) "market action" "BUY" o.action;
  Alcotest.(check (float 0.01)) "market total_qty" 100.0 o.total_qty;
  Alcotest.(check string) "market order_type" "MKT" o.order_type;
  Alcotest.(check (float 0.01)) "market lmt_price" 0.0 o.lmt_price;
  Alcotest.(check string) "market tif" "DAY" o.tif

let test_make_limit_order () =
  let o = Ibkr.Types.make_limit_order ~order_id:99 ~action:"SELL" ~qty:50.0 ~price:150.25 in
  Alcotest.(check int) "limit order_id" 99 o.order_id;
  Alcotest.(check string) "limit action" "SELL" o.action;
  Alcotest.(check (float 0.01)) "limit total_qty" 50.0 o.total_qty;
  Alcotest.(check string) "limit order_type" "LMT" o.order_type;
  Alcotest.(check (float 0.01)) "limit lmt_price" 150.25 o.lmt_price;
  Alcotest.(check string) "limit tif" "GTC" o.tif

let test_order_structure () =
  let order = {
    Ibkr.Types.order_id = 1;
    action = "BUY";
    total_qty = 10.0;
    order_type = "LMT";
    lmt_price = 200.50;
    tif = "IOC";
  } in

  Alcotest.(check int) "order order_id" 1 order.order_id;
  Alcotest.(check string) "order action" "BUY" order.action;
  Alcotest.(check (float 0.01)) "order total_qty" 10.0 order.total_qty;
  Alcotest.(check string) "order order_type" "LMT" order.order_type;
  Alcotest.(check (float 0.01)) "order lmt_price" 200.50 order.lmt_price;
  Alcotest.(check string) "order tif" "IOC" order.tif

let test_execution_structure () =
  let exec = {
    Ibkr.Types.exec_id = "0001f4e8.67890abc.01.01";
    exec_order_id = 42;
    exec_symbol = "AAPL";
    exec_side = "BOT";
    exec_qty = 100.0;
    exec_price = 175.50;
    exec_time = "20240101-09:30:00";
    exec_account = "DU12345";
    exec_exchange = "ISLAND";
  } in

  Alcotest.(check string) "exec_id" "0001f4e8.67890abc.01.01" exec.exec_id;
  Alcotest.(check int) "exec_order_id" 42 exec.exec_order_id;
  Alcotest.(check string) "exec_symbol" "AAPL" exec.exec_symbol;
  Alcotest.(check string) "exec_side" "BOT" exec.exec_side;
  Alcotest.(check (float 0.01)) "exec_qty" 100.0 exec.exec_qty;
  Alcotest.(check (float 0.01)) "exec_price" 175.50 exec.exec_price;
  Alcotest.(check string) "exec_time" "20240101-09:30:00" exec.exec_time;
  Alcotest.(check string) "exec_account" "DU12345" exec.exec_account;
  Alcotest.(check string) "exec_exchange" "ISLAND" exec.exec_exchange

let test_parse_tws_order_status () =
  let test_cases = [
    ("PreSubmitted", Ibkr.Types.PreSubmitted);
    ("Submitted", Ibkr.Types.Submitted);
    ("Filled", Ibkr.Types.Filled);
    ("Cancelled", Ibkr.Types.Cancelled);
    ("Inactive", Ibkr.Types.Inactive);
    ("ApiPending", Ibkr.Types.ApiPending);
    ("ApiCancelled", Ibkr.Types.ApiCancelled);
  ] in

  List.iter (fun (str, expected) ->
    let result = Ibkr.Types.parse_tws_order_status str in
    Alcotest.(check bool) (Printf.sprintf "parse status %s" str) true (result = expected)
  ) test_cases

let test_parse_unknown_status () =
  match Ibkr.Types.parse_tws_order_status "SomeWeirdStatus" with
  | Ibkr.Types.Unknown_status s ->
      Alcotest.(check string) "unknown status preserved" "SomeWeirdStatus" s
  | _ -> Alcotest.fail "Expected Unknown_status"

let test_to_exchange_order_status () =
  let open Dio_exchange.Exchange_intf.Types in
  let test_cases = [
    (Ibkr.Types.PreSubmitted, Pending);
    (Ibkr.Types.Submitted, New);
    (Ibkr.Types.Filled, Filled);
    (Ibkr.Types.Cancelled, Canceled);
    (Ibkr.Types.Inactive, Rejected);
    (Ibkr.Types.ApiPending, Pending);
    (Ibkr.Types.ApiCancelled, Canceled);
  ] in

  List.iter (fun (tws_status, expected) ->
    let result = Ibkr.Types.to_exchange_order_status tws_status in
    Alcotest.(check bool) "status mapping" true (result = expected)
  ) test_cases

let test_unknown_exchange_status () =
  match Ibkr.Types.to_exchange_order_status (Ibkr.Types.Unknown_status "Custom") with
  | Dio_exchange.Exchange_intf.Types.Unknown s ->
      Alcotest.(check string) "unknown maps through" "Custom" s
  | _ -> Alcotest.fail "Expected Unknown"

let test_message_id_constants () =
  (* Verify outbound message IDs *)
  Alcotest.(check int) "msg_req_mkt_data" 1 Ibkr.Types.msg_req_mkt_data;
  Alcotest.(check int) "msg_cancel_mkt_data" 2 Ibkr.Types.msg_cancel_mkt_data;
  Alcotest.(check int) "msg_place_order" 3 Ibkr.Types.msg_place_order;
  Alcotest.(check int) "msg_cancel_order" 4 Ibkr.Types.msg_cancel_order;
  Alcotest.(check int) "msg_req_open_orders" 5 Ibkr.Types.msg_req_open_orders;
  Alcotest.(check int) "msg_req_account_updates" 6 Ibkr.Types.msg_req_account_updates;
  Alcotest.(check int) "msg_req_executions" 7 Ibkr.Types.msg_req_executions;
  Alcotest.(check int) "msg_req_ids" 8 Ibkr.Types.msg_req_ids;
  Alcotest.(check int) "msg_req_contract_details" 9 Ibkr.Types.msg_req_contract_details;
  Alcotest.(check int) "msg_req_mkt_depth" 10 Ibkr.Types.msg_req_mkt_depth;
  Alcotest.(check int) "msg_cancel_mkt_depth" 11 Ibkr.Types.msg_cancel_mkt_depth;
  Alcotest.(check int) "msg_start_api" 71 Ibkr.Types.msg_start_api;
  Alcotest.(check int) "msg_req_market_data_type" 59 Ibkr.Types.msg_req_market_data_type

let test_inbound_message_id_constants () =
  Alcotest.(check int) "msg_in_tick_price" 1 Ibkr.Types.msg_in_tick_price;
  Alcotest.(check int) "msg_in_tick_size" 2 Ibkr.Types.msg_in_tick_size;
  Alcotest.(check int) "msg_in_order_status" 3 Ibkr.Types.msg_in_order_status;
  Alcotest.(check int) "msg_in_error" 4 Ibkr.Types.msg_in_error;
  Alcotest.(check int) "msg_in_open_order" 5 Ibkr.Types.msg_in_open_order;
  Alcotest.(check int) "msg_in_next_valid_id" 9 Ibkr.Types.msg_in_next_valid_id;
  Alcotest.(check int) "msg_in_contract_data" 10 Ibkr.Types.msg_in_contract_data;
  Alcotest.(check int) "msg_in_execution_data" 11 Ibkr.Types.msg_in_execution_data;
  Alcotest.(check int) "msg_in_managed_accounts" 15 Ibkr.Types.msg_in_managed_accounts

let test_tick_type_constants () =
  (* Live tick types *)
  Alcotest.(check int) "tick_bid" 1 Ibkr.Types.tick_bid;
  Alcotest.(check int) "tick_ask" 2 Ibkr.Types.tick_ask;
  Alcotest.(check int) "tick_last" 4 Ibkr.Types.tick_last;
  Alcotest.(check int) "tick_bid_size" 0 Ibkr.Types.tick_bid_size;
  Alcotest.(check int) "tick_ask_size" 3 Ibkr.Types.tick_ask_size;
  Alcotest.(check int) "tick_last_size" 5 Ibkr.Types.tick_last_size;
  Alcotest.(check int) "tick_volume" 8 Ibkr.Types.tick_volume;
  Alcotest.(check int) "tick_close" 9 Ibkr.Types.tick_close;

  (* Delayed tick types — offset +65 from live *)
  Alcotest.(check int) "tick_delayed_bid" 66 Ibkr.Types.tick_delayed_bid;
  Alcotest.(check int) "tick_delayed_ask" 67 Ibkr.Types.tick_delayed_ask;
  Alcotest.(check int) "tick_delayed_last" 68 Ibkr.Types.tick_delayed_last;
  Alcotest.(check int) "tick_delayed_bid_size" 69 Ibkr.Types.tick_delayed_bid_size;
  Alcotest.(check int) "tick_delayed_ask_size" 70 Ibkr.Types.tick_delayed_ask_size;
  Alcotest.(check int) "tick_delayed_volume" 74 Ibkr.Types.tick_delayed_volume

let test_api_version_constants () =
  Alcotest.(check int) "api_version_min" 100 Ibkr.Types.api_version_min;
  Alcotest.(check int) "api_version_max" 176 Ibkr.Types.api_version_max;
  Alcotest.(check int) "default_client_id" 0 Ibkr.Types.default_client_id

let test_feed_defaults () =
  Alcotest.(check int) "default_ring_buffer_size_ticker" 100 Ibkr.Types.default_ring_buffer_size_ticker;
  Alcotest.(check int) "default_ring_buffer_size_orderbook" 16 Ibkr.Types.default_ring_buffer_size_orderbook;
  Alcotest.(check int) "default_ring_buffer_size_executions" 32 Ibkr.Types.default_ring_buffer_size_executions;
  Alcotest.(check int) "default_orderbook_depth" 1 Ibkr.Types.default_orderbook_depth;
  Alcotest.(check (float 0.1)) "default_stale_order_threshold_s" 86400.0 Ibkr.Types.default_stale_order_threshold_s;
  Alcotest.(check (float 0.1)) "default_reconnect_base_delay_ms" 1000.0 Ibkr.Types.default_reconnect_base_delay_ms;
  Alcotest.(check (float 0.1)) "default_reconnect_max_delay_ms" 30000.0 Ibkr.Types.default_reconnect_max_delay_ms;
  Alcotest.(check (float 0.01)) "default_reconnect_backoff_factor" 2.0 Ibkr.Types.default_reconnect_backoff_factor

let () =
  Alcotest.run "IBKR Types" [
    "contract", [
      Alcotest.test_case "contract_structure" `Quick test_contract_structure;
      Alcotest.test_case "empty_contract" `Quick test_empty_contract;
      Alcotest.test_case "make_stk_contract" `Quick test_make_stk_contract;
    ];
    "order", [
      Alcotest.test_case "make_market_order" `Quick test_make_market_order;
      Alcotest.test_case "make_limit_order" `Quick test_make_limit_order;
      Alcotest.test_case "order_structure" `Quick test_order_structure;
    ];
    "execution", [
      Alcotest.test_case "execution_structure" `Quick test_execution_structure;
    ];
    "order status", [
      Alcotest.test_case "parse_tws_order_status" `Quick test_parse_tws_order_status;
      Alcotest.test_case "parse_unknown_status" `Quick test_parse_unknown_status;
      Alcotest.test_case "to_exchange_order_status" `Quick test_to_exchange_order_status;
      Alcotest.test_case "unknown_exchange_status" `Quick test_unknown_exchange_status;
    ];
    "constants", [
      Alcotest.test_case "outbound_message_ids" `Quick test_message_id_constants;
      Alcotest.test_case "inbound_message_ids" `Quick test_inbound_message_id_constants;
      Alcotest.test_case "tick_type_constants" `Quick test_tick_type_constants;
      Alcotest.test_case "api_version_constants" `Quick test_api_version_constants;
      Alcotest.test_case "feed_defaults" `Quick test_feed_defaults;
    ];
  ]
