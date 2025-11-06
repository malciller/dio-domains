(** JSON serialization module for metrics broadcast

    Provides JSON serialization functions for all metric types that will be
    broadcast via WebSocket streams.
*)

open Ui_types
open Dio_ui_cache.Balance_cache
open Dio_ui_cache.Logs_cache
open Telemetry

(** Serialize a single metric to JSON *)
let metric_to_json metric =
  let metric_type_json = match metric.metric_type with
    | Counter r ->
        `Assoc [
          ("type", `String "counter");
          ("value", `Int !r)
        ]
    | Gauge r ->
        `Assoc [
          ("type", `String "gauge");
          ("value", `Float !r)
        ]
    | SlidingCounter sliding ->
        `Assoc [
          ("type", `String "sliding_counter");
          ("total_count", `Int !(sliding.total_count));
          ("window_size", `Int sliding.window_size);
          ("last_cleanup", `Float !(sliding.last_cleanup))
        ]
    | Histogram _ ->
        let (mean, p50, p95, p99, count) = Telemetry.histogram_stats metric in
        `Assoc [
          ("type", `String "histogram");
          ("value", `Float mean);  (* Use mean as the primary value for consistency *)
          ("mean", `Float mean);
          ("p50", `Float p50);
          ("p95", `Float p95);
          ("p99", `Float p99);
          ("count", `Int count)
        ]
  in
  `Assoc [
    ("name", `String metric.name);
    ("labels", `Assoc (List.map (fun (k, v) -> (k, `String v)) metric.labels));
    ("last_updated", `Float metric.last_updated);
    ("cached_rate", `Float metric.cached_rate);
    ("metric_type", metric_type_json)
  ]

(** Serialize telemetry snapshot to JSON *)
let telemetry_snapshot_to_json (snapshot : Ui_types.telemetry_snapshot) =
  `Assoc [
    ("type", `String "telemetry");
    ("timestamp", `Float snapshot.timestamp);
    ("version", `Int snapshot.version);
    ("uptime", `Float snapshot.uptime);
    ("metrics", `List (List.map metric_to_json snapshot.metrics));
    ("categories", `List (List.map (fun (cat_name, metrics) ->
      `Assoc [
        ("name", `String cat_name);
        ("metrics", `List (List.map metric_to_json metrics))
      ]
    ) snapshot.categories))
  ]

(** Serialize system stats to JSON *)
let system_stats_to_json (stats : Ui_types.system_stats) =
  `Assoc [
    ("type", `String "system");
    ("timestamp", `Float (Unix.time ()));
    ("cpu_usage", `Float stats.cpu_usage);
    ("core_usages", `List (List.map (fun f -> `Float f) stats.core_usages));
    ("cpu_cores", match stats.cpu_cores with Some i -> `Int i | None -> `Null);
    ("cpu_vendor", match stats.cpu_vendor with Some s -> `String s | None -> `Null);
    ("cpu_model", match stats.cpu_model with Some s -> `String s | None -> `Null);
    ("memory_total", `Int stats.memory_total);
    ("memory_used", `Int stats.memory_used);
    ("memory_free", `Int stats.memory_free);
    ("swap_total", `Int stats.swap_total);
    ("swap_used", `Int stats.swap_used);
    ("load_avg_1", `Float stats.load_avg_1);
    ("load_avg_5", `Float stats.load_avg_5);
    ("load_avg_15", `Float stats.load_avg_15);
    ("processes", `Int stats.processes);
    ("cpu_temp", match stats.cpu_temp with Some f -> `Float f | None -> `Null);
    ("gpu_temp", match stats.gpu_temp with Some f -> `Float f | None -> `Null);
    ("temps", `List (List.map (fun (name, temp) ->
      `Assoc [("name", `String name); ("temp", `Float temp)]
    ) stats.temps))
  ]

(** Serialize balance snapshot to JSON *)
let balance_snapshot_to_json (snapshot : Dio_ui_cache.Balance_cache.balance_snapshot) =
  let balance_entry_to_json entry =
    `Assoc [
      ("asset", `String entry.asset);
      ("total_balance", `Float entry.total_balance);
      ("last_updated", `Float entry.last_updated);
      ("wallets", `List (List.map (fun wallet ->
        `Assoc [
          ("balance", `Float wallet.balance);
          ("wallet_type", `String wallet.wallet_type);
          ("wallet_id", `String wallet.wallet_id);
          ("last_updated", `Float wallet.last_updated)
        ]
      ) entry.wallets))
    ]
  in
  let open_order_entry_to_json entry =
    `Assoc [
      ("order_id", `String entry.order_id);
      ("symbol", `String entry.symbol);
      ("side", `String entry.side);
      ("order_qty", `Float entry.order_qty);
      ("remaining_qty", `Float entry.remaining_qty);
      ("limit_price", match entry.limit_price with Some f -> `Float f | None -> `Null);
      ("avg_price", `Float entry.avg_price);
      ("order_status", `String entry.order_status);
      ("maker_fee", match entry.maker_fee with Some f -> `Float f | None -> `Null);
      ("taker_fee", match entry.taker_fee with Some f -> `Float f | None -> `Null);
      ("last_updated", `Float entry.last_updated)
    ]
  in
  `Assoc [
    ("type", `String "balance");
    ("timestamp", `Float snapshot.timestamp);
    ("balances", `List (List.map balance_entry_to_json snapshot.balances));
    ("open_orders", `List (List.map open_order_entry_to_json snapshot.open_orders))
  ]

(** Serialize log entry to JSON *)
let log_entry_to_json (entry : Dio_ui_cache.Logs_cache.log_entry) =
  `Assoc [
    ("type", `String "log");
    ("id", `Int entry.id);
    ("timestamp", `String entry.timestamp);
    ("level", `String entry.level);
    ("section", `String entry.section);
    ("message", `String entry.message)
  ]

(** Convert any JSON value to string *)
let json_to_string json = Yojson.Basic.to_string json
