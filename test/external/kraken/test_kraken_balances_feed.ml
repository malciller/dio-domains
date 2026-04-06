let test_balance_store_operations () =
  (* Test BalanceStore create, update, and read operations *)
  let store = Kraken.Kraken_balances_feed.BalanceStore.create () in

  (* Check initial values *)
  let initial_balance = Kraken.Kraken_balances_feed.BalanceStore.get_balance store in
  Alcotest.(check (float 0.001)) "initial balance is zero" 0.0 initial_balance;

  (* Update balance data for first wallet *)
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet store 123.45 "margin" "wallet123";

  (* Check updated balance *)
  let updated_balance = Kraken.Kraken_balances_feed.BalanceStore.get_balance store in
  Alcotest.(check (float 0.001)) "updated balance" 123.45 updated_balance;

  (* Update balance for second wallet *)
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet store 67.89 "earn" "wallet456";

  (* Check aggregated balance *)
  let aggregated_balance = Kraken.Kraken_balances_feed.BalanceStore.get_balance store in
  Alcotest.(check (float 0.001)) "aggregated balance" 191.34 aggregated_balance;

  (* Get full balance data *)
  let data = Kraken.Kraken_balances_feed.BalanceStore.get_all store in
  Alcotest.(check (float 0.001)) "full data balance" 191.34 data.balance;
  Alcotest.(check string) "full data wallet_type" "aggregated" data.wallet_type;
  Alcotest.(check string) "full data wallet_id" "all" data.wallet_id;
  Alcotest.(check bool) "last_updated timestamp is positive" true (data.last_updated > 0.0)

let test_balance_management () =
  (* Test balance store management functions *)
  let asset = "BTC_TEST" in

  (* Initially should not have balance data *)
  Alcotest.(check bool) "initially no balance data" false (Kraken.Kraken_balances_feed.has_balance_data asset);

  (* Get balance store (creates it if needed) *)
  let store = Kraken.Kraken_balances_feed.get_balance_store asset in

  (* Update the store *)
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet store 1.5 "spot" "main";

  (* Now should have balance data *)
  Alcotest.(check bool) "has balance data after update" true (Kraken.Kraken_balances_feed.has_balance_data asset);

  (* Test get_balance function *)
  let balance = Kraken.Kraken_balances_feed.get_balance asset in
  Alcotest.(check (float 0.001)) "get_balance returns correct value" 1.5 balance;

  (* Test get_balance_data function *)
  let data = Kraken.Kraken_balances_feed.get_balance_data asset in
  Alcotest.(check string) "balance data asset" asset data.asset;
  Alcotest.(check (float 0.001)) "balance data balance" 1.5 data.balance;
  Alcotest.(check string) "balance data wallet_type" "aggregated" data.wallet_type;
  Alcotest.(check string) "balance data wallet_id" "all" data.wallet_id

let test_balance_staleness () =
  (* Test balance staleness detection *)
  let asset = "ETH_TEST" in

  (* Initially should be stale (no updates ever) *)
  Alcotest.(check bool) "initially stale" true (Kraken.Kraken_balances_feed.is_balance_stale asset 60.0);

  (* Update balance timestamp *)
  Kraken.Kraken_balances_feed.update_balance_timestamp asset;

  (* Now should not be stale *)
  Alcotest.(check bool) "not stale after update" false (Kraken.Kraken_balances_feed.is_balance_stale asset 60.0);

  (* Test with short threshold - should still be fresh *)
  Alcotest.(check bool) "fresh with short threshold" false (Kraken.Kraken_balances_feed.is_balance_stale asset 0.001)

let test_wait_for_balance_data () =
  (* Test waiting for balance data (with timeout) *)
  let assets = ["ADA_TEST"; "DOT_TEST"] in

  (* Initially should not have data *)
  Alcotest.(check bool) "initially no data" false (List.for_all Kraken.Kraken_balances_feed.has_balance_data assets);

  (* Start waiting with very short timeout *)
  let result = Lwt_main.run (Kraken.Kraken_balances_feed.wait_for_balance_data assets 0.001) in

  (* Should timeout since no data is available *)
  Alcotest.(check bool) "times out with no data" false result;

  (* Now add data for one asset *)
  let store = Kraken.Kraken_balances_feed.get_balance_store "ADA_TEST" in
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet store 100.0 "spot" "test";

  (* Wait again - should still timeout since not all assets have data *)
  let result2 = Lwt_main.run (Kraken.Kraken_balances_feed.wait_for_balance_data assets 0.001) in
  Alcotest.(check bool) "times out with partial data" false result2;

  (* Add data for second asset *)
  let store2 = Kraken.Kraken_balances_feed.get_balance_store "DOT_TEST" in
  Kraken.Kraken_balances_feed.BalanceStore.update_wallet store2 200.0 "margin" "test";

  (* Wait again - should succeed now *)
  let result3 = Lwt_main.run (Kraken.Kraken_balances_feed.wait_for_balance_data assets 1.0) in
  Alcotest.(check bool) "succeeds with all data" true result3

let test_balance_data_structure () =
  (* Test balance_data record structure *)
  let test_data = {
    Kraken.Kraken_balances_feed.asset = "SOL";
    balance = 50.5;
    wallet_type = "futures";
    wallet_id = "futures_wallet";
    last_updated = Unix.time ();
  } in

  Alcotest.(check string) "balance data asset field" "SOL" test_data.asset;
  Alcotest.(check (float 0.001)) "balance data balance field" 50.5 test_data.balance;
  Alcotest.(check string) "balance data wallet_type field" "futures" test_data.wallet_type;
  Alcotest.(check string) "balance data wallet_id field" "futures_wallet" test_data.wallet_id;
  Alcotest.(check bool) "balance data last_updated field is positive" true (test_data.last_updated > 0.0)

let test_concurrent_balance_updates () =
  (* Test that balance updates work correctly under concurrent access *)
  let asset = "CONCURRENT_TEST" in
  let store = Kraken.Kraken_balances_feed.get_balance_store asset in

  (* Start multiple threads updating the same balance *)
  let update_thread balance_value delay =
    Thread.create (fun () ->
      Thread.delay delay;
      Kraken.Kraken_balances_feed.BalanceStore.update_wallet store balance_value "concurrent" "thread_test"
    ) ()
  in

  let thread1 = update_thread 100.0 0.01 in
  let thread2 = update_thread 200.0 0.02 in
  let thread3 = update_thread 300.0 0.03 in

  (* Wait for all threads to complete *)
  Thread.join thread1;
  Thread.join thread2;
  Thread.join thread3;

  (* Check that final balance is one of the updated values *)
  let final_balance = Kraken.Kraken_balances_feed.get_balance asset in
  let valid_values = [100.0; 200.0; 300.0] in
  Alcotest.(check bool) "final balance is one of the updated values" true (List.mem final_balance valid_values)

let () =
  Alcotest.run "Kraken Balances Feed" [
    "balance store", [
      Alcotest.test_case "balance store operations" `Quick test_balance_store_operations;
    ];
    "balance management", [
      Alcotest.test_case "balance management functions" `Quick test_balance_management;
      Alcotest.test_case "balance staleness detection" `Quick test_balance_staleness;
    ];
    "async operations", [
      Alcotest.test_case "wait for balance data" `Quick test_wait_for_balance_data;
    ];
    "data structures", [
      Alcotest.test_case "balance data structure" `Quick test_balance_data_structure;
    ];
    "concurrency", [
      Alcotest.test_case "concurrent balance updates" `Quick test_concurrent_balance_updates;
    ];
  ]
