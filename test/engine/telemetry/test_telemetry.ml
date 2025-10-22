open Alcotest
open Unix  (* For sleepf *)

let test_counter_operations () =
  (* Test counter creation, increment, and reading *)
  let counter1 = Telemetry.counter "test_counter" () in

  (* Initial value should be 0 *)
  check int "counter initial value" 0 (Telemetry.get_counter counter1);

  (* Increment by 1 (default) *)
  Telemetry.inc_counter counter1 ();
  check int "counter after increment by 1" 1 (Telemetry.get_counter counter1);

  (* Increment by specific value *)
  Telemetry.inc_counter counter1 ~value:5 ();
  check int "counter after increment by 5" 6 (Telemetry.get_counter counter1);

  (* Test counter with labels *)
  let counter2 = Telemetry.counter "test_counter" ~labels:["service", "test"; "env", "dev"] () in
  Telemetry.inc_counter counter2 ~value:10 ();
  check int "labeled counter value" 10 (Telemetry.get_counter counter2)

let test_gauge_operations () =
  (* Test gauge creation, setting, and incrementing *)
  let gauge1 = Telemetry.gauge "test_gauge" () in

  (* Initial value should be 0.0 *)
  check (float 0.001) "gauge initial value" 0.0 (Telemetry.get_gauge gauge1);

  (* Set to specific value *)
  Telemetry.set_gauge gauge1 42.5;
  check (float 0.001) "gauge after set" 42.5 (Telemetry.get_gauge gauge1);

  (* Increment gauge *)
  Telemetry.inc_gauge gauge1 7.5;
  check (float 0.001) "gauge after increment" 50.0 (Telemetry.get_gauge gauge1);

  (* Decrement gauge *)
  Telemetry.inc_gauge gauge1 (-10.0);
  check (float 0.001) "gauge after decrement" 40.0 (Telemetry.get_gauge gauge1);

  (* Test gauge with labels *)
  let gauge2 = Telemetry.gauge "test_gauge" ~labels:["component", "metrics"] () in
  Telemetry.set_gauge gauge2 123.45;
  check (float 0.001) "labeled gauge value" 123.45 (Telemetry.get_gauge gauge2)

let test_histogram_operations () =
  (* Test histogram creation, observation, and statistics *)
  let hist1 = Telemetry.histogram "test_histogram" () in

  (* Observe some values *)
  Telemetry.observe_histogram hist1 1.0;
  Telemetry.observe_histogram hist1 2.0;
  Telemetry.observe_histogram hist1 3.0;
  Telemetry.observe_histogram hist1 4.0;
  Telemetry.observe_histogram hist1 5.0;

  (* Check statistics *)
  let (mean, p50, p95, p99, count) = Telemetry.histogram_stats hist1 in
  check int "histogram count" 5 count;
  check (float 0.001) "histogram mean" 3.0 mean;
  check (float 0.001) "histogram p50" 3.0 p50;
  check (float 0.001) "histogram p95" 5.0 p95;
  check (float 0.001) "histogram p99" 5.0 p99;

  (* Test with more samples for percentiles *)
  let hist2 = Telemetry.histogram "test_histogram2" () in
  for i = 1 to 100 do
    Telemetry.observe_histogram hist2 (float_of_int i)
  done;

  let (_, p50_2, p95_2, p99_2, count2) = Telemetry.histogram_stats hist2 in
  check int "histogram2 count" 100 count2;
  check (float 0.001) "histogram2 p50" 50.5 p50_2;  (* median of 1-100 is 50.5 *)
  check bool "histogram2 p95 >= 95" true (p95_2 >= 95.0);  (* 95th percentile *)
  check bool "histogram2 p99 >= 99" true (p99_2 >= 99.0)   (* 99th percentile *)

let test_timing_operations () =
  (* Test timing utilities *)
  let timer = Telemetry.start_timer () in
  (* Simulate some work *)
  sleepf 0.01;  (* 10ms delay *)
  let hist = Telemetry.histogram "timing_test" () in
  let duration = Telemetry.record_duration hist timer in

  (* Duration should be around 0.01 seconds *)
  check bool "duration in range" true (duration >= 0.005 && duration <= 0.1);

  (* Check that histogram recorded the value *)
  let (_, _, _, _, count) = Telemetry.histogram_stats hist in
  check int "timing histogram count" 1 count

let test_common_metrics () =
  (* Test the Common metrics module *)
  Telemetry.inc_counter Telemetry.Common.orders_placed ~value:3 ();
  Telemetry.inc_counter Telemetry.Common.orders_failed ~value:1 ();
  Telemetry.inc_counter Telemetry.Common.domain_cycles ~value:100 ();

  check int "orders_placed counter" 3 (Telemetry.get_counter Telemetry.Common.orders_placed);
  check int "orders_failed counter" 1 (Telemetry.get_counter Telemetry.Common.orders_failed);
  check int "domain_cycles counter" 100 (Telemetry.get_counter Telemetry.Common.domain_cycles)

let test_asset_metrics () =
  (* Test asset-specific metric helpers *)
  let btc_counter = Telemetry.asset_counter "asset_trades" "BTC" () in
  let eth_gauge = Telemetry.asset_gauge "asset_balance" "ETH" in
  let ada_hist = Telemetry.asset_histogram "asset_latency" "ADA" in

  (* Test counter *)
  Telemetry.inc_counter btc_counter ~value:5 ();
  check int "asset counter value" 5 (Telemetry.get_counter btc_counter);

  (* Test gauge *)
  Telemetry.set_gauge eth_gauge 1234.56;
  check (float 0.001) "asset gauge value" 1234.56 (Telemetry.get_gauge eth_gauge);

  (* Test histogram *)
  Telemetry.observe_histogram ada_hist 0.05;
  Telemetry.observe_histogram ada_hist 0.08;
  let (_, _, _, _, count) = Telemetry.histogram_stats ada_hist in
  check int "asset histogram count" 2 count

let test_metric_uniqueness () =
  (* Test that metrics with different labels are separate *)
  let counter1 = Telemetry.counter "unique_test" ~labels:["env", "prod"] () in
  let counter2 = Telemetry.counter "unique_test" ~labels:["env", "dev"] () in
  let counter3 = Telemetry.counter "unique_test" () in  (* no labels *)

  Telemetry.inc_counter counter1 ~value:1 ();
  Telemetry.inc_counter counter2 ~value:2 ();
  Telemetry.inc_counter counter3 ~value:3 ();

  check int "unique counter1" 1 (Telemetry.get_counter counter1);
  check int "unique counter2" 2 (Telemetry.get_counter counter2);
  check int "unique counter3" 3 (Telemetry.get_counter counter3)

let test_metric_persistence () =
  (* Test that getting the same metric again returns the same instance *)
  let counter1 = Telemetry.counter "persistence_test" ~labels:["test", "yes"] () in
  Telemetry.inc_counter counter1 ~value:5 ();

  let counter2 = Telemetry.counter "persistence_test" ~labels:["test", "yes"] () in
  Telemetry.inc_counter counter2 ~value:3 ();

  check int "persistence counter1" 8 (Telemetry.get_counter counter1);
  check int "persistence counter2" 8 (Telemetry.get_counter counter2)

let () =
  run "Telemetry" [
    "counters", [
      test_case "counter operations" `Quick test_counter_operations;
    ];
    "gauges", [
      test_case "gauge operations" `Quick test_gauge_operations;
    ];
    "histograms", [
      test_case "histogram operations" `Quick test_histogram_operations;
    ];
    "timing", [
      test_case "timing operations" `Quick test_timing_operations;
    ];
    "common", [
      test_case "common metrics" `Quick test_common_metrics;
    ];
    "assets", [
      test_case "asset metrics" `Quick test_asset_metrics;
    ];
    "uniqueness", [
      test_case "metric uniqueness" `Quick test_metric_uniqueness;
    ];
    "persistence", [
      test_case "metric persistence" `Quick test_metric_persistence;
    ];
  ]