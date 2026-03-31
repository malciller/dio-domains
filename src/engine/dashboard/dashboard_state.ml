(** Dashboard State Serialization
    Reads engine state from various modules and serializes to JSON
    for transmission over Unix domain socket to the TUI dashboard. *)

let _section = "dashboard_state"

module Exchange = Dio_exchange.Exchange_intf
module Fear_and_greed = Cmc.Fear_and_greed

(** Cached engine start time — set once by the server at startup *)
let engine_start_time = ref 0.0

let set_start_time t = engine_start_time := t

(* ── JSON helpers ────────────────────────────────────────────────── *)

let json_of_float_opt = function
  | Some f -> `Float f
  | None -> `Null

let json_of_string_opt = function
  | Some s -> `String s
  | None -> `Null

(* ── Connection snapshots ────────────────────────────────────────── *)

let json_of_connection_snapshot (snap : Supervisor_cache.connection_snapshot) =
  let state_str = match snap.state with
    | Supervisor_types.Connected -> "Connected"
    | Supervisor_types.Disconnected -> "Disconnected"
    | Supervisor_types.Connecting -> "Connecting"
    | Supervisor_types.Failed reason -> Printf.sprintf "Failed: %s" reason
  in
  let cb_str = match snap.circuit_breaker_state with
    | x when x = 0.0 -> "Closed"
    | x when x = 1.0 -> "Open"
    | _ -> "HalfOpen"
  in
  `Assoc [
    "name", `String snap.name;
    "state", `String state_str;
    "uptime_s", `Float snap.uptime;
    "heartbeat", `Bool snap.heartbeat_active;
    "circuit_breaker", `String cb_str;
    "cb_failures", `Int snap.circuit_breaker_failures;
    "reconnects", `Int snap.reconnect_attempts;
    "total_conns", `Int snap.total_connections;
  ]

(* ── Domain state ────────────────────────────────────────────────── *)

let json_of_domains () =
  let statuses = Dio_engine.Domain_spawner.get_domain_status () in
  `List (List.map (fun (key, (running, restart_count, last_restart)) ->
    `Assoc [
      "key", `String key;
      "running", `Bool running;
      "restart_count", `Int restart_count;
      "last_restart", `Float last_restart;
    ]
  ) statuses)

(* ── Strategy state — Grid ───────────────────────────────────────── *)

let json_of_grid_strategy symbol =
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  `Assoc [
    "type", `String "Grid";
    "buy_price", json_of_float_opt state.last_buy_order_price;
    "buy_id", json_of_string_opt state.last_buy_order_id;
    "sell_orders", `List (List.map (fun (oid, price, qty) ->
      `Assoc ["id", `String oid; "price", `Float price; "qty", `Float qty]
    ) state.open_sell_orders);
    "sell_count", `Int (List.length state.open_sell_orders);
    "accumulated_profit", `Float state.accumulated_profit;
    "reserved_base", `Float state.reserved_base;
    "reserved_quote", `Float state.reserved_quote;
    "capital_low", `Bool state.capital_low;
    "asset_low", `Bool state.asset_low;
    "inflight_buy", `Bool state.inflight_buy;
    "inflight_sell", `Bool state.inflight_sell;
    "last_buy_fill", json_of_float_opt state.last_buy_fill_price;
    "last_sell_fill", json_of_float_opt state.last_sell_fill_price;
    "pending_count", `Int (List.length state.pending_orders);
    "grid_qty", `Float state.grid_qty;
    "maker_fee", `Float state.maker_fee;
  ]

(* ── Strategy state — Market Maker ───────────────────────────────── *)

let json_of_mm_strategy symbol =
  let state = Dio_strategies.Market_maker.get_strategy_state symbol in
  `Assoc [
    "type", `String "MM";
    "buy_price", json_of_float_opt state.last_buy_order_price;
    "buy_id", json_of_string_opt state.last_buy_order_id;
    "sell_orders", `List (List.map (fun (oid, price, qty) ->
      `Assoc ["id", `String oid; "price", `Float price; "qty", `Float qty]
    ) state.open_sell_orders);
    "sell_count", `Int (List.length state.open_sell_orders);
    "capital_low", `Bool state.capital_low;
    "asset_low", `Bool state.asset_low;
    "inflight_buy", `Bool state.inflight_buy;
    "inflight_sell", `Bool state.inflight_sell;
    "pending_count", `Int (List.length state.pending_orders);
  ]

(* ── Market data per symbol ──────────────────────────────────────── *)

let json_of_market_data exchange symbol =
  match Exchange.Registry.get exchange with
  | None -> `Null
  | Some (module Ex) ->
      let ticker = Ex.get_ticker ~symbol in
      let tob = Ex.get_top_of_book ~symbol in
      let base_asset =
        if String.contains symbol '/' then
          String.split_on_char '/' symbol |> List.hd
        else symbol
      in
      let quote_currency =
        if String.contains symbol '/' then
          let parts = String.split_on_char '/' symbol in
          (match parts with _ :: q :: _ -> q | _ -> "USD")
        else "USD"
      in
      let base_balance = (try Ex.get_balance ~asset:base_asset with _ -> 0.0) in
      let quote_balance = (try Ex.get_balance ~asset:quote_currency with _ -> 0.0) in
      `Assoc [
        "bid", (match ticker with Some (b, _) -> `Float b | None -> `Null);
        "ask", (match ticker with Some (_, a) -> `Float a | None -> `Null);
        "tob_bid", (match tob with Some (b, _, _, _) -> `Float b | None -> `Null);
        "tob_bid_size", (match tob with Some (_, bs, _, _) -> `Float bs | None -> `Null);
        "tob_ask", (match tob with Some (_, _, a, _) -> `Float a | None -> `Null);
        "tob_ask_size", (match tob with Some (_, _, _, as_) -> `Float as_ | None -> `Null);
        "base_asset", `String base_asset;
        "quote_currency", `String quote_currency;
        "base_balance", `Float base_balance;
        "quote_balance", `Float quote_balance;
      ]

(* ── Latency profiler snapshots ──────────────────────────────────── *)

let json_of_latency_snapshot (snap : Latency_profiler.snapshot) =
  `Assoc [
    "name", `String snap.name;
    "p50", `Float snap.p50;
    "p90", `Float snap.p90;
    "p95", `Float snap.p95;
    "p99", `Float snap.p99;
    "p999", `Float snap.p999;
    "samples", `Int snap.samples;
    "overflow", `Int snap.overflow;
  ]

let json_of_domain_latencies () =
  let profilers = Dio_engine.Domain_spawner.get_domain_profiler_snapshots () in
  `Assoc (List.map (fun (symbol, snaps) ->
    symbol, `Assoc (List.filter_map (fun (label, snap_opt) ->
      match snap_opt with
      | Some snap -> Some (label, json_of_latency_snapshot snap)
      | None -> None
    ) snaps)
  ) profilers)

(* ── Memory / GC stats ───────────────────────────────────────────── *)

let json_of_memory () =
  let s = Gc.stat () in
  let heap_mb = s.heap_words * (Sys.word_size / 8) / 1048576 in
  let live_kb = s.live_words * (Sys.word_size / 8) / 1024 in
  let free_kb = (s.heap_words - s.live_words) * (Sys.word_size / 8) / 1024 in
  `Assoc [
    "heap_mb", `Int heap_mb;
    "live_kb", `Int live_kb;
    "free_kb", `Int free_kb;
    "gc_major", `Int s.major_collections;
    "gc_minor", `Int s.minor_collections;
    "compactions", `Int s.compactions;
    "fragments", `Int s.fragments;
    "heap_chunks", `Int s.heap_chunks;
  ]

(* ── Full state snapshot ─────────────────────────────────────────── *)

let build_snapshot () =
  let now = Unix.gettimeofday () in
  let uptime = now -. !engine_start_time in
  let config = Dio_engine.Config.read_config () in

  (* Connections *)
  let connections = Supervisor_cache.get_snapshots () in

  (* Fear & Greed *)
  let fng = match Fear_and_greed.get_cached () with
    | Some v -> `Float v
    | None -> `Null
  in

  (* Per-symbol strategy + market data *)
  let strategies = List.map (fun (tc : Dio_engine.Config.trading_config) ->
    let strategy_json = match tc.strategy with
      | "Grid" | "suicide_grid" -> json_of_grid_strategy tc.symbol
      | "MM" -> json_of_mm_strategy tc.symbol
      | other -> `Assoc ["type", `String other]
    in
    let market_json = json_of_market_data tc.exchange tc.symbol in
    tc.symbol, `Assoc [
      "exchange", `String tc.exchange;
      "strategy", strategy_json;
      "market", market_json;
      "qty", `String tc.qty;
      "grid_interval_lo", `Float (fst tc.grid_interval);
      "grid_interval_hi", `Float (snd tc.grid_interval);
      "sell_mult", `String tc.sell_mult;
    ]
  ) config.trading in

  `Assoc [
    "type", `String "snapshot";
    "timestamp", `Float now;
    "uptime_s", `Float uptime;
    "fear_and_greed", fng;
    "memory", json_of_memory ();
    "connections", `List (List.map json_of_connection_snapshot connections);
    "domains", json_of_domains ();
    "strategies", `Assoc strategies;
    "latencies", json_of_domain_latencies ();
  ]

(** Build a JSON snapshot and serialize to string *)
let snapshot_to_string () =
  let json = build_snapshot () in
  Yojson.Basic.to_string json
