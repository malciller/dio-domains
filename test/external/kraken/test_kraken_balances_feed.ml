let test_balance_store_operations () =
  (* BalanceStore create/update/read. *)
  let store = Kraken.Kraken_balances_feed.BalanceStore.create () in
  let initial_balance = Kraken.Kraken_balances_feed.BalanceStore.get_balance store in
  Alcotest.(check (float 0.001)) "initial balance is zero" 0.0 initial_balance;
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet
    store
    123.45
    "margin"
    "wallet123"
    "BTC";
  let updated_balance = Kraken.Kraken_balances_feed.BalanceStore.get_balance store in
  Alcotest.(check (float 0.001)) "updated balance" 123.45 updated_balance;
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet
    store
    67.89
    "earn"
    "wallet456"
    "BTC.HOLD";
  (* Earn wallets are excluded from the trading balance. *)
  let aggregated_balance = Kraken.Kraken_balances_feed.BalanceStore.get_balance store in
  Alcotest.(check (float 0.001)) "aggregated balance" 123.45 aggregated_balance;
  (* Total balance includes earn wallets. *)
  let total_balance = Kraken.Kraken_balances_feed.BalanceStore.get_total_balance store in
  Alcotest.(check (float 0.001)) "aggregated total balance" 191.34 total_balance;
  let data = Kraken.Kraken_balances_feed.BalanceStore.get_all store in
  Alcotest.(check (float 0.001)) "full data balance" 191.34 data.balance;
  Alcotest.(check string) "full data wallet_type" "aggregated" data.wallet_type;
  Alcotest.(check string) "full data wallet_id" "all" data.wallet_id;
  Alcotest.(check bool) "last_updated timestamp is positive" true (data.last_updated > 0.0)
;;

let test_balance_management () =
  (* has_balance_data / get_balance / get_balance_data. *)
  let asset = "BTC_TEST" in
  Alcotest.(check bool)
    "initially no balance data"
    false
    (Kraken.Kraken_balances_feed.has_balance_data asset);
  let store = Kraken.Kraken_balances_feed.get_balance_store asset in
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet store 1.5 "spot" "main" asset;
  Alcotest.(check bool)
    "has balance data after update"
    true
    (Kraken.Kraken_balances_feed.has_balance_data asset);
  let balance = Kraken.Kraken_balances_feed.get_balance asset in
  Alcotest.(check (float 0.001)) "get_balance returns correct value" 1.5 balance;
  let data = Kraken.Kraken_balances_feed.get_balance_data asset in
  Alcotest.(check string) "balance data asset" asset data.asset;
  Alcotest.(check (float 0.001)) "balance data balance" 1.5 data.balance;
  Alcotest.(check string) "balance data wallet_type" "aggregated" data.wallet_type;
  Alcotest.(check string) "balance data wallet_id" "all" data.wallet_id
;;

let test_balance_staleness () =
  (* Balance staleness. *)
  let asset = "ETH_TEST" in
  (* Never-updated asset is stale. *)
  Alcotest.(check bool)
    "initially stale"
    true
    (Kraken.Kraken_balances_feed.is_balance_stale asset 60.0);
  Kraken.Kraken_balances_feed.update_balance_timestamp asset;
  Alcotest.(check bool)
    "not stale after update"
    false
    (Kraken.Kraken_balances_feed.is_balance_stale asset 60.0);
  (* Fresh after a timestamp update, even at a short threshold. *)
  Alcotest.(check bool)
    "fresh with short threshold"
    false
    (Kraken.Kraken_balances_feed.is_balance_stale asset 0.001)
;;

let test_wait_for_balance_data () =
  (* wait_for_balance_data timeout behavior. *)
  let assets = [ "ADA_TEST"; "DOT_TEST" ] in
  Alcotest.(check bool)
    "initially no data"
    false
    (List.for_all Kraken.Kraken_balances_feed.has_balance_data assets);
  let result =
    Lwt_main.run (Kraken.Kraken_balances_feed.wait_for_balance_data assets 0.001)
  in
  Alcotest.(check bool) "times out with no data" false result;
  let store = Kraken.Kraken_balances_feed.get_balance_store "ADA_TEST" in
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet
    store
    100.0
    "spot"
    "test"
    "ADA_TEST";
  (* Partial data still times out: all requested assets are required. *)
  let result2 =
    Lwt_main.run (Kraken.Kraken_balances_feed.wait_for_balance_data assets 0.001)
  in
  Alcotest.(check bool) "times out with partial data" false result2;
  let store2 = Kraken.Kraken_balances_feed.get_balance_store "DOT_TEST" in
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet
    store2
    200.0
    "margin"
    "test"
    "DOT_TEST";
  let result3 =
    Lwt_main.run (Kraken.Kraken_balances_feed.wait_for_balance_data assets 1.0)
  in
  Alcotest.(check bool) "succeeds with all data" true result3
;;

let test_tradeable_balance_nets_open_order_holds () =
  (* Kraken snapshots report TOTAL balances; the tradeable figure must net out what is
     locked in resting orders (total - hold), as Hyperliquid's store does. Otherwise sell
     sizing counts inventory committed to a resting sell and the exchange rejects with
     EOrder:Insufficient funds. XMR startup case: 0.08004 total - 0.04 resting sell =
     0.04004 tradeable; for a quote asset, resting buys lock quote value. *)
  let base = "XMR_HOLD_TEST" in
  let quote = "USD_HOLD_TEST" in
  let pair = base ^ "/" ^ quote in
  let base_store = Kraken.Kraken_balances_feed.get_balance_store base in
  let quote_store = Kraken.Kraken_balances_feed.get_balance_store quote in
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet
    base_store
    0.08004
    "spot"
    "main"
    base;
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet
    quote_store
    100.0
    "spot"
    "main"
    quote;
  (* No orders yet: tradeable equals total. *)
  Alcotest.(check (float 1e-6))
    "no holds -> tradeable = total (base)"
    0.08004
    (Kraken.Kraken_module.Kraken_impl.get_tradeable_balance ~asset:base);
  Alcotest.(check (float 1e-6))
    "no holds -> tradeable = total (quote)"
    100.0
    (Kraken.Kraken_module.Kraken_impl.get_tradeable_balance ~asset:quote);
  (* Inject a resting sell of 0.04 and a resting buy of 0.02 @ 420. *)
  let mk_event side qty price =
    { Kraken.Kraken_executions_feed.order_id = "hold-" ^ side ^ "-" ^ string_of_float qty
    ; symbol = pair
    ; exec_type = Kraken.Kraken_executions_feed.New
    ; order_status = Kraken.Kraken_executions_feed.NewStatus
    ; side =
        (if side = "sell"
         then Kraken.Kraken_executions_feed.Sell
         else Kraken.Kraken_executions_feed.Buy)
    ; order_qty = qty
    ; cum_qty = 0.0
    ; cum_cost = 0.0
    ; avg_price = 0.0
    ; limit_price = Some price
    ; last_qty = None
    ; last_price = None
    ; fee = None
    ; trade_id = None
    ; order_userref = None
    ; cl_ord_id = None
    ; timestamp = Unix.gettimeofday ()
    }
  in
  Kraken.Kraken_executions_feed.update_open_orders
    (Kraken.Kraken_executions_feed.get_symbol_store pair)
    (mk_event "sell" 0.04 462.13);
  Kraken.Kraken_executions_feed.update_open_orders
    (Kraken.Kraken_executions_feed.get_symbol_store pair)
    (mk_event "buy" 0.02 420.0);
  Alcotest.(check (float 1e-6))
    "resting sell hold nets from tradeable base"
    0.04004
    (Kraken.Kraken_module.Kraken_impl.get_tradeable_balance ~asset:base);
  Alcotest.(check (float 1e-6))
    "resting buy hold nets from tradeable quote"
    91.6
    (Kraken.Kraken_module.Kraken_impl.get_tradeable_balance ~asset:quote);
  (* Holds clamp tradeable at zero. *)
  Kraken.Kraken_executions_feed.update_open_orders
    (Kraken.Kraken_executions_feed.get_symbol_store pair)
    (mk_event "sell" 5.0 462.13);
  Alcotest.(check (float 1e-6))
    "holds clamp at zero, never negative"
    0.0
    (Kraken.Kraken_module.Kraken_impl.get_tradeable_balance ~asset:base)
;;

let test_balance_data_structure () =
  (* balance_data record fields. *)
  let test_data =
    { Kraken.Kraken_balances_feed.asset = "SOL"
    ; balance = 50.5
    ; wallet_type = "futures"
    ; wallet_id = "futures_wallet"
    ; last_updated = Unix.time ()
    }
  in
  Alcotest.(check string) "balance data asset field" "SOL" test_data.asset;
  Alcotest.(check (float 0.001)) "balance data balance field" 50.5 test_data.balance;
  Alcotest.(check string) "balance data wallet_type field" "futures" test_data.wallet_type;
  Alcotest.(check string)
    "balance data wallet_id field"
    "futures_wallet"
    test_data.wallet_id;
  Alcotest.(check bool)
    "balance data last_updated field is positive"
    true
    (test_data.last_updated > 0.0)
;;

let test_concurrent_balance_updates () =
  (* Concurrent updates: final value is one of the written values. *)
  let asset = "CONCURRENT_TEST" in
  let store = Kraken.Kraken_balances_feed.get_balance_store asset in
  let update_thread balance_value delay =
    Thread.create
      (fun () ->
        Thread.delay delay;
        Kraken.Kraken_balances_feed.BalanceStore.update_wallet
          store
          balance_value
          "concurrent"
          "thread_test"
          asset)
      ()
  in
  let thread1 = update_thread 100.0 0.01 in
  let thread2 = update_thread 200.0 0.02 in
  let thread3 = update_thread 300.0 0.03 in
  Thread.join thread1;
  Thread.join thread2;
  Thread.join thread3;
  let final_balance = Kraken.Kraken_balances_feed.get_balance asset in
  let valid_values = [ 100.0; 200.0; 300.0 ] in
  Alcotest.(check bool)
    "final balance is one of the updated values"
    true
    (List.mem final_balance valid_values)
;;

let () =
  Alcotest.run
    "Kraken Balances Feed"
    [ ( "balance store"
      , [ Alcotest.test_case
            "balance store operations"
            `Quick
            test_balance_store_operations
        ] )
    ; ( "balance management"
      , [ Alcotest.test_case "balance management functions" `Quick test_balance_management
        ; Alcotest.test_case "balance staleness detection" `Quick test_balance_staleness
        ] )
    ; ( "async operations"
      , [ Alcotest.test_case "wait for balance data" `Quick test_wait_for_balance_data ] )
    ; ( "data structures"
      , [ Alcotest.test_case "balance data structure" `Quick test_balance_data_structure ]
      )
    ; ( "tradeable balance"
      , [ Alcotest.test_case
            "open-order holds net out of tradeable balance"
            `Quick
            test_tradeable_balance_nets_open_order_holds
        ] )
    ; ( "concurrency"
      , [ Alcotest.test_case
            "concurrent balance updates"
            `Quick
            test_concurrent_balance_updates
        ] )
    ]
;;
