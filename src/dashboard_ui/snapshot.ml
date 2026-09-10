(** Typed, parse-once model of the engine's dashboard snapshot.

    Every renderer consumes [t] instead of walking the raw Yojson tree with
    [|?>]. The snapshot is decoded exactly once per received frame, and the
    derived values (mid prices, selectable assets, pause state) are computed
    once rather than once per module. *)

open Theme

type order =
  { id : string
  ; price : float
  ; qty : float
  }

type level =
  { price : float
  ; qty : float
  }

type trade =
  { price : float
  ; qty : float
  ; timestamp : float
  ; side : string
  }

type market =
  { bid : float
  ; ask : float
  ; mid : float
  ; base_asset : string
  ; quote_currency : string
  ; base_balance : float
  ; staked_balance : float
  ; tradeable_balance : float
  ; quote_balance : float
  ; bids : level list
  ; asks : level list
  ; trades : trade list
  ; buy_orders : order list
  ; sell_orders : order list
  }

type oracle =
  { active : bool
  ; reason : string
  ; buy_qty : float
  ; max_drawdown_pct : float
  ; grid_interval : float
  ; d_surv : float
  ; exhaustion_price : float
  }

type strategy =
  { symbol : string
  ; exchange : string
  ; type_ : string
  ; buy_price : float
  ; buy_qty : float
  ; buy_id : string
  ; sell_orders : order list
  ; sell_count : int
  ; capital_low : bool
  ; market_is_closed : bool
  ; last_buy_fill : float
  ; last_sell_fill : float
  ; grid_interval_lo : float
  ; market : market
  ; oracle : oracle option
  }

type balance =
  { exchange : string
  ; asset : string
  ; symbol : string
  ; balance : float
  ; staked_balance : float
  ; tradeable_balance : float
  ; bid : float
  ; ask : float
  ; mid : float
  ; bids : level list
  ; asks : level list
  ; sell_orders : order list
  ; sell_count : int
  }

type fill =
  { venue : string
  ; symbol : string
  ; side : string
  ; amount : float
  ; fill_price : float
  ; value : float
  ; timestamp : float
  }

type latency_metric =
  { p50 : float
  ; p90 : float
  ; p99 : float
  ; p999 : float
  ; samples : int
  ; sub_us_samples : int
  ; overflow : int
  ; executions : int
  ; executions_per_sec : float
  ; last_exec_time : float
  ; window_start : float
  ; window_end : float
  ; max_cause : string
  }

type memory =
  { heap_mb : int
  ; live_kb : int
  ; free_kb : int
  ; space_overhead : int
  ; gc_major : int
  ; gc_minor : int
  ; compactions : int
  ; fragments : int
  ; heap_chunks : int
  }

type asset_kind =
  | Strategy of strategy
  | Balance of balance

type selectable_asset =
  { key : string
  ; display_name : string
  ; exchange : string
  ; symbol : string
  ; asset : string
  ; is_strategy : bool
  ; kind : asset_kind
  }

type t =
  { timestamp : float
  ; uptime_s : float
  ; fear_and_greed : float option
  ; memory : memory
  ; strategies : (string * strategy) list
  ; balances : balance list
  ; fills : fill list
  ; latencies : (string * (string * latency_metric) list) list
  ; oracle_latency : (string * latency_metric) list
  ; assets : selectable_asset list
  }

let mid_of bid ask = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else max bid ask

let parse_order j =
  let price = j |?> "price" |> to_float_d 0.0 in
  let qty = j |?> "qty" |> to_float_d 0.0 in
  if price > 0.0 && qty > 0.0
  then Some { id = j |?> "id" |> to_string_d "?"; price; qty }
  else None
;;

let parse_orders j = j |> to_list_d |> List.filter_map parse_order

let parse_level j =
  let price = j |?> "price" |> to_float_d 0.0 in
  let qty = j |?> "qty" |> to_float_d 0.0 in
  if price > 0.0 then Some { price; qty } else None
;;

let parse_levels j = j |> to_list_d |> List.filter_map parse_level

let parse_trade j =
  let price = j |?> "price" |> to_float_d 0.0 in
  let qty = j |?> "qty" |> to_float_d 0.0 in
  let timestamp = j |?> "timestamp" |> to_float_d 0.0 in
  let side = j |?> "side" |> to_string_d "trade" in
  if price > 0.0 then Some { price; qty; timestamp; side } else None
;;

let parse_trades j = j |> to_list_d |> List.filter_map parse_trade

let parse_market j =
  let bid = j |?> "bid" |> to_float_d 0.0 in
  let ask = j |?> "ask" |> to_float_d 0.0 in
  { bid
  ; ask
  ; mid = mid_of bid ask
  ; base_asset = j |?> "base_asset" |> to_string_d ""
  ; quote_currency = j |?> "quote_currency" |> to_string_d ""
  ; base_balance = j |?> "base_balance" |> to_float_d 0.0
  ; staked_balance = j |?> "staked_balance" |> to_float_d 0.0
  ; tradeable_balance = j |?> "tradeable_balance" |> to_float_d 0.0
  ; quote_balance = j |?> "quote_balance" |> to_float_d 0.0
  ; bids = parse_levels (j |?> "bids")
  ; asks = parse_levels (j |?> "asks")
  ; trades = parse_trades (j |?> "trades")
  ; buy_orders = parse_orders (j |?> "buy_orders")
  ; sell_orders = parse_orders (j |?> "sell_orders")
  }
;;

let empty_market = parse_market `Null

let parse_oracle j =
  { active = j |?> "active" |> to_bool_d false
  ; reason = j |?> "reason" |> to_string_d ""
  ; buy_qty =
      (match j |?> "buy_qty" with
       | `Float f -> f
       | _ -> j |?> "qty" |> to_float_d 0.0)
  ; max_drawdown_pct = j |?> "max_drawdown_pct" |> to_float_d 0.0
  ; grid_interval = j |?> "grid_interval" |> to_float_d 0.0
  ; d_surv = j |?> "d_surv" |> to_float_d 0.0
  ; exhaustion_price = j |?> "exhaustion_price" |> to_float_d 0.0
  }
;;

let parse_strategy symbol j =
  let strat = j |?> "strategy" in
  let sell_orders = parse_orders (strat |?> "sell_orders") in
  { symbol
  ; exchange = j |?> "exchange" |> to_string_d "?"
  ; type_ = strat |?> "type" |> to_string_d "?"
  ; buy_price = strat |?> "buy_price" |> to_float_d 0.0
  ; buy_qty = strat |?> "buy_qty" |> to_float_d (strat |?> "grid_qty" |> to_float_d 0.0)
  ; buy_id = strat |?> "buy_id" |> to_string_d "buy"
  ; sell_orders
  ; sell_count =
      (let sc = strat |?> "sell_count" |> to_int_d 0 in
       if sc > 0 then sc else List.length sell_orders)
  ; capital_low = strat |?> "capital_low" |> to_bool_d false
  ; market_is_closed = strat |?> "market_is_closed" |> to_bool_d false
  ; last_buy_fill = strat |?> "last_buy_fill" |> to_float_d 0.0
  ; last_sell_fill = strat |?> "last_sell_fill" |> to_float_d 0.0
  ; grid_interval_lo = j |?> "grid_interval_lo" |> to_float_d 1.0
  ; market = parse_market (j |?> "market")
  ; oracle =
      (match j |?> "oracle" with
       | `Assoc _ as o -> Some (parse_oracle o)
       | _ -> None)
  }
;;

let parse_balance j =
  let bid = j |?> "bid" |> to_float_d 0.0 in
  let ask = j |?> "ask" |> to_float_d 0.0 in
  let asset = j |?> "asset" |> to_string_d "?" in
  let sell_orders = parse_orders (j |?> "sell_orders") in
  { exchange = j |?> "exchange" |> to_string_d "?"
  ; asset
  ; symbol = j |?> "symbol" |> to_string_d asset
  ; balance = j |?> "balance" |> to_float_d 0.0
  ; staked_balance = j |?> "staked_balance" |> to_float_d 0.0
  ; tradeable_balance = j |?> "tradeable_balance" |> to_float_d 0.0
  ; bid
  ; ask
  ; mid = mid_of bid ask
  ; bids = parse_levels (j |?> "bids")
  ; asks = parse_levels (j |?> "asks")
  ; sell_orders
  ; sell_count =
      (let sc = j |?> "sell_count" |> to_int_d 0 in
       if sc > 0 then sc else List.length sell_orders)
  }
;;

let parse_fill j =
  { venue = j |?> "venue" |> to_string_d "?"
  ; symbol = j |?> "symbol" |> to_string_d "?"
  ; side = j |?> "side" |> to_string_d "?"
  ; amount = j |?> "amount" |> to_float_d 0.0
  ; fill_price = j |?> "fill_price" |> to_float_d 0.0
  ; value = j |?> "value" |> to_float_d 0.0
  ; timestamp = j |?> "timestamp" |> to_float_d 0.0
  }
;;

let parse_latency_metric j =
  { p50 = j |?> "p50" |> to_float_d 0.0
  ; p90 = j |?> "p90" |> to_float_d 0.0
  ; p99 = j |?> "p99" |> to_float_d 0.0
  ; p999 = j |?> "p999" |> to_float_d 0.0
  ; samples = j |?> "samples" |> to_int_d 0
  ; sub_us_samples = j |?> "sub_us_samples" |> to_int_d 0
  ; overflow = j |?> "overflow" |> to_int_d 0
  ; executions = j |?> "executions" |> to_int_d 0
  ; executions_per_sec = j |?> "executions_per_sec" |> to_float_d 0.0
  ; last_exec_time = j |?> "last_exec_time" |> to_float_d 0.0
  ; window_start = j |?> "window_start" |> to_float_d 0.0
  ; window_end = j |?> "window_end" |> to_float_d 0.0
  ; max_cause = j |?> "max_cause" |> to_string_d ""
  }
;;

let parse_latency_map j =
  match j with
  | `Assoc l -> List.map (fun (label, m) -> label, parse_latency_metric m) l
  | _ -> []
;;

let parse_memory j =
  { heap_mb = j |?> "heap_mb" |> to_int_d 0
  ; live_kb = j |?> "live_kb" |> to_int_d 0
  ; free_kb = j |?> "free_kb" |> to_int_d 0
  ; space_overhead = j |?> "space_overhead" |> to_int_d 80
  ; gc_major = j |?> "gc_major" |> to_int_d 0
  ; gc_minor = j |?> "gc_minor" |> to_int_d 0
  ; compactions = j |?> "compactions" |> to_int_d 0
  ; fragments = j |?> "fragments" |> to_int_d 0
  ; heap_chunks = j |?> "heap_chunks" |> to_int_d 0
  }
;;

(* -------------------------------------------------------------------------- *)
(* Pause state and selectable-asset derivation                                *)
(* -------------------------------------------------------------------------- *)

(** [true] when the capital oracle's decision for this strategy says
    INACTIVE (the oracle-paused state). [false] when there is no decision. *)
let oracle_inactive (s : strategy) =
  match s.oracle with
  | Some o -> not o.active
  | None -> false
;;

(** Paused = the capital oracle says INACTIVE, or the grid's own capital-low
    flag is set (or the market is closed). If an open resting buy order
    already exists, the strategy cannot be considered paused by
    capital/oracle gates since the order is already placed and funded. *)
let strategy_paused (s : strategy) =
  let has_resting_buy = s.buy_price > 0.0 || s.market.buy_orders <> [] in
  if has_resting_buy
  then s.market_is_closed
  else oracle_inactive s || s.capital_low || s.market_is_closed
;;

let quote_assets = [ "USD"; "USDC"; "USDT"; "ZUSD"; "USDe" ]
let is_quote_asset a = List.mem a quote_assets

let selectable_assets ~strategies ~balances =
  let active_strats, paused_strats =
    List.partition (fun (_symbol, s) -> not (strategy_paused s)) strategies
  in
  let strat_keys =
    List.map
      (fun (_sym, s) ->
         let base = if s.market.base_asset = "" then s.symbol else s.market.base_asset in
         (s.exchange, s.symbol), (s.exchange, base))
      strategies
  in
  let is_strat_asset (b : balance) =
    List.exists
      (fun ((ex1, s1), (ex2, b2)) ->
         (ex1 = b.exchange && (s1 = b.symbol || s1 = b.asset))
         || (ex2 = b.exchange && b2 = b.asset))
      strat_keys
  in
  let valid_balances =
    List.filter (fun (b : balance) -> b.balance > 0.0 && not (is_strat_asset b)) balances
  in
  let inactive_jsons =
    List.filter (fun (b : balance) -> not (is_quote_asset b.asset)) valid_balances
  in
  let mk_strategy_item (sym, s) =
    let base = if s.market.base_asset = "" then sym else s.market.base_asset in
    { key = "strat:" ^ s.exchange ^ ":" ^ sym
    ; display_name = Printf.sprintf "%s (%s)" sym (exch_tag_of s.exchange)
    ; exchange = s.exchange
    ; symbol = sym
    ; asset = base
    ; is_strategy = true
    ; kind = Strategy s
    }
  in
  let active_items = List.map mk_strategy_item active_strats in
  let paused_items = List.map mk_strategy_item paused_strats in
  let inactive_items =
    List.map
      (fun (b : balance) ->
         { key = "bal:" ^ b.exchange ^ ":" ^ b.asset
         ; display_name = Printf.sprintf "%s (%s)" b.asset (exch_tag_of b.exchange)
         ; exchange = b.exchange
         ; symbol = b.symbol
         ; asset = b.asset
         ; is_strategy = false
         ; kind = Balance b
         })
      inactive_jsons
  in
  active_items @ paused_items @ inactive_items
;;

let of_json json =
  let strategies =
    match json |?> "strategies" with
    | `Assoc l -> List.map (fun (sym, j) -> sym, parse_strategy sym j) l
    | _ -> []
  in
  let balances = json |?> "all_balances" |> to_list_d |> List.map parse_balance in
  let fills = json |?> "recent_fills" |> to_list_d |> List.map parse_fill in
  let latencies =
    match json |?> "latencies" with
    | `Assoc l -> List.map (fun (sym, j) -> sym, parse_latency_map j) l
    | _ -> []
  in
  let oracle_latency =
    match json |?> "oracle_latency" with
    | `Assoc l -> List.map (fun (label, m) -> label, parse_latency_metric m) l
    | _ -> []
  in
  let memory = parse_memory (json |?> "memory") in
  let timestamp = json |?> "timestamp" |> to_float_d 0.0 in
  let uptime_s = json |?> "uptime_s" |> to_float_d 0.0 in
  let fear_and_greed =
    match json |?> "fear_and_greed" with
    | `Float f -> Some f
    | `Int i -> Some (float_of_int i)
    | _ -> None
  in
  let assets = selectable_assets ~strategies ~balances in
  { timestamp
  ; uptime_s
  ; fear_and_greed
  ; memory
  ; strategies
  ; balances
  ; fills
  ; latencies
  ; oracle_latency
  ; assets
  }
;;
