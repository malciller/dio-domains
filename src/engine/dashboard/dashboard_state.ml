(** Dashboard state serialization.
    Aggregates engine state from multiple modules and serializes it
    as JSON for transmission to the TUI dashboard via Unix domain socket. *)

module Exchange = Dio_exchange.Exchange_intf
module Fear_and_greed = Cmc.Fear_and_greed

(** Cached engine start time. Set once by the server at startup. *)
let engine_start_time = ref 0.0

let set_start_time t = engine_start_time := t

(** Trading config ref. Cached on first read; immutable at runtime. *)
let cached_config : Dio_engine.Config.config option ref = ref None

let get_config () =
  match !cached_config with
  | Some c -> c
  | None ->
      let c = Dio_engine.Config.read_config () in
      cached_config := Some c;
      c

(* JSON helpers *)

let json_of_float_opt = function
  | Some f -> `Float f
  | None -> `Null

let json_of_string_opt = function
  | Some s -> `String s
  | None -> `Null

(* Connection snapshots *)

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

(* Domain state *)

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

(* Strategy state: Grid *)

let json_of_grid_strategy exchange symbol =
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  let market_is_closed = exchange = "ibkr" && not (Ibkr.Market_hours.is_regular_market_open ()) in
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
    "capital_low", `Bool (state.capital_low || market_is_closed);
    "asset_low", `Bool state.asset_low;
    "inflight_buy", `Bool state.inflight_buy;
    "inflight_sell", `Bool state.inflight_sell;
    "last_buy_fill", json_of_float_opt state.last_buy_fill_price;
    "last_sell_fill", json_of_float_opt state.last_sell_fill_price;
    "pending_count", `Int (List.length state.pending_orders);
    "grid_qty", `Float state.grid_qty;
    "maker_fee", `Float state.maker_fee;
  ]

(* Strategy state: Market Maker *)

let json_of_mm_strategy exchange symbol =
  let state = Dio_strategies.Market_maker.get_strategy_state symbol in
  let market_is_closed = exchange = "ibkr" && not (Ibkr.Market_hours.is_regular_market_open ()) in
  `Assoc [
    "type", `String "MM";
    "buy_price", json_of_float_opt state.last_buy_order_price;
    "buy_id", json_of_string_opt state.last_buy_order_id;
    "sell_orders", `List (List.map (fun (oid, price, qty) ->
      `Assoc ["id", `String oid; "price", `Float price; "qty", `Float qty]
    ) state.open_sell_orders);
    "sell_count", `Int (List.length state.open_sell_orders);
    "capital_low", `Bool (state.capital_low || market_is_closed);
    "asset_low", `Bool state.asset_low;
    "inflight_buy", `Bool state.inflight_buy;
    "inflight_sell", `Bool state.inflight_sell;
    "pending_count", `Int (List.length state.pending_orders);
  ]

(* Per-symbol market data *)

(** Splits a trading symbol on '/' into (base_asset, quote_currency).
    Defaults quote to "USD" when no delimiter is present. *)
let split_symbol symbol =
  if String.contains symbol '/' then
    match String.split_on_char '/' symbol with
    | base :: quote :: _ -> (base, quote)
    | base :: _ -> (base, "USD")
    | [] -> (symbol, "USD")
  else (symbol, "USD")

let json_of_market_data exchange symbol base_asset quote_currency =
  match Exchange.Registry.get exchange with
  | None -> `Null
  | Some (module Ex) ->
      let tob = Ex.get_top_of_book ~symbol in
      let base_balance = (try Ex.get_balance ~asset:base_asset with _ -> 0.0) in
      let quote_balance = (try Ex.get_balance ~asset:quote_currency with _ -> 0.0) in
      `Assoc [
        "bid", (match tob with Some (b, _, _, _) -> `Float b | None -> `Null);
        "ask", (match tob with Some (_, _, a, _) -> `Float a | None -> `Null);
        "tob_bid", (match tob with Some (b, _, _, _) -> `Float b | None -> `Null);
        "tob_bid_size", (match tob with Some (_, bs, _, _) -> `Float bs | None -> `Null);
        "tob_ask", (match tob with Some (_, _, a, _) -> `Float a | None -> `Null);
        "tob_ask_size", (match tob with Some (_, _, _, as_) -> `Float as_ | None -> `Null);
        "base_asset", `String base_asset;
        "quote_currency", `String quote_currency;
        "base_balance", `Float base_balance;
        "quote_balance", `Float quote_balance;
      ]

(* Latency profiler snapshots *)

let json_of_latency_snapshot (snap : Latency_profiler.snapshot) =
  `Assoc [
    "name", `String snap.name;
    "p50", `Float snap.p50;
    "p90", `Float snap.p90;
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

(* Memory and GC stats *)

let json_of_memory () =
  let s = Gc.quick_stat () in
  let heap_mb = s.heap_words * (Sys.word_size / 8) / 1048576 in
  let live_kb = s.live_words * (Sys.word_size / 8) / 1024 in
  let free_kb = (s.heap_words - s.live_words) * (Sys.word_size / 8) / 1024 in
  let space_overhead = match !cached_config with
    | Some c -> (match c.gc with Some gc -> gc.space_overhead | None -> 80)
    | None -> 80
  in
  `Assoc [
    "heap_mb", `Int heap_mb;
    "live_kb", `Int live_kb;
    "free_kb", `Int free_kb;
    "space_overhead", `Int space_overhead;
    "gc_major", `Int s.major_collections;
    "gc_minor", `Int s.minor_collections;
    "compactions", `Int s.compactions;
    "fragments", `Int s.fragments;
    "heap_chunks", `Int s.heap_chunks;
  ]

(* Full state snapshot *)

let take n l =
  let rec aux a n l =
    if n <= 0 then a else match l with [] -> a | h::t -> aux (h::a) (n-1) t 
  in List.rev (aux [] n l)

let json_of_recent_fills () =
  let module B = Concurrency.Fill_event_bus in
  let fills = B.get_recent_fills () in
  let recent = take 50 fills in
  `List (List.map (fun (f : B.fill_event) ->
    `Assoc [
      "venue", `String f.venue;
      "symbol", `String f.symbol;
      "side", `String f.side;
      "amount", `Float f.amount;
      "fill_price", `Float f.fill_price;
      "value", `Float f.value;
      "timestamp", `Float f.timestamp;
    ]
  ) recent)

let build_snapshot () =
  let now = Unix.gettimeofday () in
  let uptime = now -. !engine_start_time in
  let config = get_config () in

  (* Supervisor connection snapshots *)
  let connections = Supervisor_cache.get_snapshots () in

  (* Fear and Greed index value *)
  let fng = match Fear_and_greed.get_cached () with
    | Some v -> `Float v
    | None -> `Null
  in

  (* Per-symbol strategy state and market data *)
  let strategies = List.map (fun (tc : Dio_engine.Config.trading_config) ->
    let strategy_json = match tc.strategy with
      | "Grid" | "suicide_grid" -> json_of_grid_strategy tc.exchange tc.symbol
      | "MM" -> json_of_mm_strategy tc.exchange tc.symbol
      | other -> `Assoc ["type", `String other]
    in
    let (base_asset, quote_currency) = split_symbol tc.symbol in
    let market_json = json_of_market_data tc.exchange tc.symbol base_asset quote_currency in
    tc.symbol, `Assoc [
      "exchange", `String tc.exchange;
      "strategy", strategy_json;
      "market", market_json;
      "qty", `String tc.qty;
      "grid_interval_lo", `Float (fst tc.grid_interval);
      "grid_interval_hi", `Float (snd tc.grid_interval);
      "accumulation_buffer_lo", `Float (fst tc.accumulation_buffer);
      "accumulation_buffer_hi", `Float (snd tc.accumulation_buffer);
      "sell_mult", `String tc.sell_mult;
    ]
  ) config.trading in

  (* Aggregate balances from all registered exchanges,
     enriched with ticker data and open sell orders *)
  let configured_symbols = List.map (fun (tc : Dio_engine.Config.trading_config) ->
    (tc.exchange, tc.symbol)
  ) config.trading in
  let exchange_names = List.sort_uniq String.compare
    (List.map (fun (tc : Dio_engine.Config.trading_config) -> tc.exchange) config.trading) in
  let all_balances = List.concat_map (fun exch_name ->
    match Exchange.Registry.get exch_name with
    | None -> []
    | Some (module Ex) ->
        List.filter_map (fun (asset, bal) ->
          (* Infer trading pair for this asset *)
          let quote = match exch_name with
            | "hyperliquid" | "lighter" -> "USDC"
            | _ -> "USD"
          in
          let symbol = asset ^ "/" ^ quote in
          (* Skip assets already covered by a configured strategy *)
          let is_configured = List.exists (fun (ex, sym) ->
            ex = exch_name && sym = symbol
          ) configured_symbols in
          if is_configured then None
          else begin
            let is_quote = (asset = "USD") || (asset = "USDC") || (asset = "ZUSD") || (asset = "USDT") || (asset = quote) || (asset = "USDe") in
            let tob = Ex.get_top_of_book ~symbol in
            let bid_json, ask_json = match tob with
              | Some (b, _, a, _) -> `Float b, `Float a
              | None ->
                  if is_quote then `Float 1.0, `Float 1.0
                  else `Null, `Null
            in
            (* Retrieve open sell orders for this symbol *)
            let open_orders = Ex.get_open_orders ~symbol in
            let sell_orders = List.filter (fun (o : Exchange.Types.open_order) ->
              o.side = Exchange.Types.Sell && o.remaining_qty > 0.0
            ) open_orders in
            let sell_orders_json = `List (List.map (fun (o : Exchange.Types.open_order) ->
              `Assoc [
                "id", `String o.order_id;
                "price", `Float (Option.value o.limit_price ~default:0.0);
                "qty", `Float o.remaining_qty;
              ]
            ) sell_orders) in
            Some (`Assoc [
              "exchange", `String exch_name;
              "asset", `String asset;
              "symbol", `String symbol;
              "balance", `Float bal;
              "bid", bid_json;
              "ask", ask_json;
              "sell_orders", sell_orders_json;
              "sell_count", `Int (List.length sell_orders);
            ])
          end
        ) (Ex.get_all_balances ())
  ) exchange_names in

  `Assoc [
    "type", `String "snapshot";
    "timestamp", `Float now;
    "uptime_s", `Float uptime;
    "fear_and_greed", fng;
    "memory", json_of_memory ();
    "connections", `List (List.map json_of_connection_snapshot connections);
    "domains", json_of_domains ();
    "strategies", `Assoc strategies;
    "all_balances", `List all_balances;
    "recent_fills", json_of_recent_fills ();
    "latencies", json_of_domain_latencies ();
  ]

(** Builds a full JSON snapshot and serializes it to a string. *)
let snapshot_to_string () =
  let json = build_snapshot () in
  Yojson.Basic.to_string json
