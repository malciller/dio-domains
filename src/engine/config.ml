(** Per-symbol trading parameters parsed from a single entry in the "trading" array of
    config.json. *)
type trading_config = Dio_strategies.Strategy_common.trading_config =
  { exchange : string
  ; symbol : string
  ; qty : string
  ; grid_interval : float * float
  (** (min, max) grid interval percentages; resolved to equal bounds when a scalar is
      provided *)
  ; sell_mult : string
  ; min_usd_balance : string option
  ; max_exposure : string option
  ; strategy : string
  ; maker_fee : float option
  ; taker_fee : float option
  ; testnet : bool
  ; hedge : bool
  ; accumulation_buffer : float * float
  (** (min, max) quote profit buffer; interpolated at runtime via Fear and Greed index *)
  ; data_feed : string option
  ; base_accumulation : bool (** Per-strategy opt-in to base-accumulation persistence. *)
  ; sell_levels : bool (** Per-strategy opt-in to pending-sell-level persistence. *)
  ; cpu_priority : int
  (** Core-assignment priority (descending); ties keep config order. Default 0. *)
  }

type logging_config =
  { level : Logging.level
  ; sections : string list
  ; width : int option
  (** Fixed log-wrap width. None = auto: TTY width, else `COLUMNS` env var, else 200. *)
  }

(** Optional top-level "flow_log" section: the opt-in decision-flow telemetry stream
    rendered by {!Flow_log}. Absent = disabled (production default). *)
type flow_log_config =
  { enabled : bool
  ; min_interval_seconds : float
  ; heartbeat_seconds : float
  ; pulse_seconds : float
  ; price_ticks : bool
  }

type gc_config =
  { minor_heap_size : int
  ; space_overhead : int
  ; max_overhead : int
  ; window_size : int
  ; allocation_policy : int
  ; major_heap_increment : int
  }

(** Latency families that emit spike logs. [Spike_report_internal] covers the per-domain
    pipeline stages (OB/EXEC/PREP/STRAT/CYCLE); [Spike_report_network] covers per-venue
    ws_ping/ws_feed/rest_request/signer windows. Separate so the network tail can be
    silenced independently. *)
type latency_spike_report =
  | Spike_report_internal
  | Spike_report_network
  | Spike_report_both
  | Spike_report_none

type config =
  { cycle_mod : int
  ; logging : logging_config
  ; flow_log : flow_log_config
  ; gc : gc_config option
  ; trading : trading_config list
  ; oracle : Dio_oracle.Oracle_runtime.runtime_config option
  (** Capital-oracle runtime knobs from the top-level "oracle" section. [None] = engine
      uses [Oracle_runtime.default_config]. *)
  ; fng_check_threshold : float
  ; latency_window_seconds : float
  (** Per-domain latency accumulation window, in seconds: the histogram is snapshotted and
      reset each window. Shorter = faster dashboard movement, fewer samples per window. *)
  ; latency_spike_threshold_us : float
  (** Per-stage latency ceiling, in microseconds. A window with samples above it emits one
      INFO line naming offending stages, worst spike, and breach count. Target: 10us. *)
  ; latency_spike_report : latency_spike_report
  (** Latency families that emit the spike logs. Default [Spike_report_internal]: internal
      ops only, network silenced. *)
  ; latency_spike_report_seconds : float
  (** Minimum wall-clock interval between per-domain internal spike log lines, in seconds.
      Percentile windows still publish on [latency_window_seconds]. Default 30; 0 logs
      every window. *)
  ; latency_network_spike_threshold_us : float
  (** Network-family latency ceiling, in microseconds. Separate from
      [latency_spike_threshold_us] because network metrics (ws ping/feed, REST, signer)
      are millisecond-scale and would flag every window at 10us. *)
  ; theme : string option (** Optional UI theme name for the terminal dashboard. *)
  ; strategy_trace : bool
  (** When true, the domain loop records per-cycle observable traces for the
      behavioral-equivalence harness. Default false: production behavior is unchanged
      unless explicitly enabled. *)
  }

(** Logging section identifier for this module. *)
let section = "config"

(** Accepts [Int] and [Float] JSON numbers; all else (including [Null]) is [None].
    Yojson's [to_float_option] raises on [Int], which aborts startup with an uncaught
    [Type_error]. *)
let to_float_opt = function
  | `Int i -> Some (float_of_int i)
  | `Float f -> Some f
  | _ -> None
;;

(** Normalizes a configured venue name to the canonical lowercase identifier used by the
    exchange registry. Built-in venue names are case-insensitive in config.json. *)
let normalize_exchange s = String.lowercase_ascii (String.trim s)

(** Base asset of a spot pair: [BTC/USD] -> [BTC]. *)
let base_coin_of_symbol symbol =
  match String.split_on_char '/' symbol with
  | b :: _ -> b
  | [] -> symbol
;;

(** Parses a [latency_spike_report] from its config string. Unknown values warn and fall
    back to internal-only. *)
let latency_spike_report_of_string s =
  match String.lowercase_ascii (String.trim s) with
  | "internal" -> Spike_report_internal
  | "network" -> Spike_report_network
  | "both" | "all" -> Spike_report_both
  | "none" | "off" -> Spike_report_none
  | other ->
    Logging.warn_f
      ~section
      "Unknown latency_spike_report '%s'; defaulting to 'internal'"
      other;
    Spike_report_internal
;;

(** [reports_internal r] is true when internal pipeline spikes are logged. *)
let reports_internal = function
  | Spike_report_internal | Spike_report_both -> true
  | Spike_report_network | Spike_report_none -> false
;;

(** [reports_network r] is true when network latency spikes should be logged. *)
let reports_network = function
  | Spike_report_network | Spike_report_both -> true
  | Spike_report_internal | Spike_report_none -> false
;;

(** Permitted key sets used by [validate_keys] for strict schema enforcement at each
    nesting level. *)
let known_top_level_keys =
  [ "logging_level"
  ; "logging_sections"
  ; "logging_width"
  ; "cycle_mod"
  ; "latency_window_seconds"
  ; "latency_spike_threshold_us"
  ; "latency_spike_report"
  ; "latency_spike_report_seconds"
  ; "latency_network_spike_threshold_us"
  ; "trading"
  ; "gc"
  ; "oracle"
  ; "fng_check_threshold"
  ; "theme"
  ; "strategy_trace"
  ; "flow_log"
  ; "alert_level"
  ]
;;

let known_flow_log_keys =
  [ "enabled"
  ; "min_interval_seconds"
  ; "heartbeat_seconds"
  ; "pulse_seconds"
  ; "price_ticks"
  ]
;;

let known_gc_keys =
  [ "minor_heap_size"
  ; "space_overhead"
  ; "max_overhead"
  ; "window_size"
  ; "allocation_policy"
  ; "major_heap_increment"
  ]
;;

(** Permitted keys of the optional top-level "oracle" section. All optional; absent keys
    fall back to [Oracle_runtime.default_config]. *)
let known_oracle_keys =
  [ "qty_cap_mult"; "target_survival"; "min_active_dsurv"; "refresh_seconds"; "assets" ]
;;

(** Keys accepted inside each "oracle"."assets" entry (per-asset overrides, keyed by
    symbol). *)
let known_oracle_asset_keys = [ "target_survival"; "min_active_dsurv"; "qty_cap_mult" ]

let known_trading_keys =
  [ "symbol"
  ; "exchange"
  ; "qty"
  ; "grid_interval"
  ; "sell_mult"
  ; "min_usd_balance"
  ; "max_exposure"
  ; "strategy"
  ; "maker_fee"
  ; "taker_fee"
  ; "testnet"
  ; "hedge"
  ; "accumulation_buffer"
  ; "data_feed"
  ; "base_accumulation"
  ; "sell_levels"
  ; "cpu_priority"
  ; "overlay"
  ]
;;

(** Logs CRITICAL for each key of [json] not in [allowed]; returns [true] if any unknown
    keys are present. *)
let validate_keys ~context ~allowed json =
  let open Yojson.Basic.Util in
  let actual = json |> to_assoc |> List.map fst in
  let unknown = List.filter (fun k -> not (List.mem k allowed)) actual in
  List.iter
    (fun k -> Logging.critical_f ~section "Unknown config key '%s' in %s" k context)
    unknown;
  unknown <> []
;;

(** Parses "grid_interval": [min; max] list or a numeric/string scalar (promoted to equal
    bounds). Default (1.0, 1.0). *)
let parse_grid_interval json exchange symbol =
  let open Yojson.Basic.Util in
  let default = 1.0, 1.0 in
  let float_of_json = function
    | `Float f -> Some f
    | `Int i -> Some (float_of_int i)
    | `String s ->
      (try Some (float_of_string s) with
       | _ -> None)
    | _ -> None
  in
  match json |> member "grid_interval" with
  | `List [ lo; hi ] ->
    (match float_of_json lo, float_of_json hi with
     | Some a, Some b ->
       let low = min a b in
       let high = max a b in
       low, high
     | _ ->
       Logging.warn_f
         ~section
         "Invalid grid_interval list for %s/%s, using default %.2f-%.2f"
         exchange
         symbol
         (fst default)
         (snd default);
       default)
  | `List _ ->
    Logging.warn_f
      ~section
      "grid_interval must be a two-value list for %s/%s, using default %.2f-%.2f"
      exchange
      symbol
      (fst default)
      (snd default);
    default
  | (`Float _ | `Int _ | `String _) as v ->
    (match float_of_json v with
     | Some x -> x, x
     | None ->
       Logging.warn_f
         ~section
         "Invalid grid_interval value for %s/%s, using default %.2f-%.2f"
         exchange
         symbol
         (fst default)
         (snd default);
       default)
  | _ -> default
;;

(** Parses "accumulation_buffer": [min; max] list or a numeric/string scalar (promoted to
    equal bounds). Default (0.01, 0.01). *)
let parse_accumulation_buffer json exchange symbol =
  let open Yojson.Basic.Util in
  let default = 0.01, 0.01 in
  let float_of_json = function
    | `Float f -> Some f
    | `Int i -> Some (float_of_int i)
    | `String s ->
      (try Some (float_of_string s) with
       | _ -> None)
    | _ -> None
  in
  match json |> member "accumulation_buffer" with
  | `List [ lo; hi ] ->
    (match float_of_json lo, float_of_json hi with
     | Some a, Some b ->
       let low = min a b in
       let high = max a b in
       low, high
     | _ ->
       Logging.warn_f
         ~section
         "Invalid accumulation_buffer list for %s/%s, using default %.2f-%.2f"
         exchange
         symbol
         (fst default)
         (snd default);
       default)
  | `List _ ->
    Logging.warn_f
      ~section
      "accumulation_buffer must be a two-value list for %s/%s, using default %.2f-%.2f"
      exchange
      symbol
      (fst default)
      (snd default);
    default
  | (`Float _ | `Int _ | `String _) as v ->
    (match float_of_json v with
     | Some x -> x, x
     | None ->
       Logging.warn_f
         ~section
         "Invalid accumulation_buffer value for %s/%s, using default %.2f-%.2f"
         exchange
         symbol
         (fst default)
         (snd default);
       default)
  | _ -> default
;;

(** Parses one entry of the JSON "trading" array into a [trading_config]. Validates keys
    and venue restrictions; [exit 1] on schema violation. *)
let parse_config json =
  if validate_keys ~context:"trading entry" ~allowed:known_trading_keys json then exit 1;
  let open Yojson.Basic.Util in
  let symbol = json |> member "symbol" |> to_string in
  let exchange =
    json
    |> member "exchange"
    |> to_string_option
    |> Option.value ~default:"kraken"
    |> normalize_exchange
  in
  let exch_id = Dio_exchange.Exchange_intf.Types.exchange_of_string exchange in
  (* testnet/hedge are venue-limited; accumulation_buffer is valid on every venue (Kraken
     runs the same reserved_base accrual; see jacobs_ladder_config.kraken_config). *)
  (match exch_id with
   | Hyperliquid | Ibkr | Lighter | Alpaca | Kraken -> ()
   | Custom _ ->
     let restricted = [ "testnet"; "hedge"; "accumulation_buffer"; "data_feed" ] in
     let actual = json |> to_assoc |> List.map fst in
     let bad = List.filter (fun k -> List.mem k restricted) actual in
     if bad <> []
     then (
       List.iter
         (fun k ->
           Logging.critical_f
             ~section
             "Key '%s' is not valid for exchange '%s' (found in %s/%s)"
             k
             exchange
             exchange
             symbol)
         bad;
       exit 1));
  (match exch_id with
   | Hyperliquid -> ()
   | _ ->
     let hl_only = [ "hedge" ] in
     let actual = json |> to_assoc |> List.map fst in
     let bad = List.filter (fun k -> List.mem k hl_only) actual in
     if bad <> []
     then (
       List.iter
         (fun k ->
           Logging.critical_f
             ~section
             "Key '%s' is only valid for hyperliquid (found in %s/%s)"
             k
             exchange
             symbol)
         bad;
       exit 1));
  (match exch_id with
   | Hyperliquid | Ibkr | Lighter | Alpaca -> ()
   | Kraken | Custom _ ->
     let actual = json |> to_assoc |> List.map fst in
     if List.mem "testnet" actual
     then (
       Logging.critical_f
         ~section
         "Key 'testnet' is only valid for hyperliquid, ibkr, lighter, and alpaca (found \
          in %s/%s)"
         exchange
         symbol;
       exit 1));
  (* An entry without [strategy] is an inventory-only declaration: it contributes a leg to
     a carry overlay but runs no grid. Its grid fields are therefore optional. *)
  let strategy =
    json |> member "strategy" |> to_string_option |> Option.value ~default:""
  in
  (* The strategy file convention is validated by [read_config] (not here) so the pure
     per-entry parser stays side-effect free for tests and tooling. *)
  (* grid_interval carries the hardened search bounds (gi_min, gi_max) walked by the
     oracle's parameter search. *)
  let testnet =
    json |> member "testnet" |> to_bool_option |> Option.value ~default:false
  in
  let hedge = json |> member "hedge" |> to_bool_option |> Option.value ~default:false in
  let data_feed = json |> member "data_feed" |> to_string_option in
  { exchange
  ; symbol
  ; qty = json |> member "qty" |> to_string_option |> Option.value ~default:"0"
  ; grid_interval = parse_grid_interval json exchange symbol
  ; sell_mult =
      json |> member "sell_mult" |> to_string_option |> Option.value ~default:"1.0"
  ; min_usd_balance = json |> member "min_usd_balance" |> to_string_option
  ; max_exposure = json |> member "max_exposure" |> to_string_option
  ; strategy
  ; maker_fee = json |> member "maker_fee" |> to_float_opt
  ; taker_fee = json |> member "taker_fee" |> to_float_opt
  ; testnet
  ; hedge
  ; accumulation_buffer = parse_accumulation_buffer json exchange symbol
  ; data_feed
  ; base_accumulation =
      json |> member "base_accumulation" |> to_bool_option |> Option.value ~default:true
  ; sell_levels =
      json |> member "sell_levels" |> to_bool_option |> Option.value ~default:false
  ; cpu_priority =
      json |> member "cpu_priority" |> to_int_option |> Option.value ~default:0
  }
;;

(** Parses "logging_level"/"logging_sections"/"logging_width". Defaults to INFO and no
    section filters when absent or invalid. *)
let parse_logging_config json : logging_config =
  let open Yojson.Basic.Util in
  let level_str =
    json |> member "logging_level" |> to_string_option |> Option.value ~default:"info"
  in
  let sections_str =
    json |> member "logging_sections" |> to_string_option |> Option.value ~default:""
  in
  let level =
    match Logging.level_of_string level_str with
    | Some lvl -> lvl
    | None ->
      Logging.warn_f
        ~section:"config"
        "Unknown logging level '%s', defaulting to INFO"
        level_str;
      Logging.INFO
  in
  let sections =
    sections_str
    |> String.split_on_char ','
    |> List.map String.trim
    |> List.filter (( <> ) "")
  in
  let width = json |> member "logging_width" |> to_int_option in
  { level; sections; width }
;;

(** [DIO_FLOW_LOG] env kill/boost switch for a quick demo without editing config.json. *)
let flow_log_env_enabled () =
  match Sys.getenv_opt "DIO_FLOW_LOG" with
  | Some s ->
    (match String.lowercase_ascii (String.trim s) with
     | "1" | "true" | "on" | "yes" -> true
     | _ -> false)
  | None -> false
;;

(** Parses the optional "flow_log" object. Absent = disabled unless [DIO_FLOW_LOG] is set.
    Exits on unknown sub-keys. *)
let parse_flow_log_config json : flow_log_config =
  let open Yojson.Basic.Util in
  match json |> member "flow_log" with
  | `Null ->
    { enabled = flow_log_env_enabled ()
    ; min_interval_seconds = 0.2
    ; heartbeat_seconds = 5.0
    ; pulse_seconds = 5.0
    ; price_ticks = true
    }
  | f ->
    if validate_keys ~context:"flow_log" ~allowed:known_flow_log_keys f then exit 1;
    let enabled =
      f
      |> member "enabled"
      |> to_bool_option
      |> Option.value ~default:(flow_log_env_enabled ())
    in
    let optf key default = f |> member key |> to_float_opt |> Option.value ~default in
    { enabled
    ; min_interval_seconds = optf "min_interval_seconds" 0.2
    ; heartbeat_seconds = optf "heartbeat_seconds" 5.0
    ; pulse_seconds = optf "pulse_seconds" 5.0
    ; price_ticks =
        f |> member "price_ticks" |> to_bool_option |> Option.value ~default:true
    }
;;

(** Parses the optional "gc" object into OCaml GC tuning parameters. Returns [None] when
    the key is absent. Exits on unknown sub-keys. *)
let parse_gc_config json : gc_config option =
  let open Yojson.Basic.Util in
  match json |> member "gc" with
  | `Null -> None
  | gc_json ->
    if validate_keys ~context:"gc" ~allowed:known_gc_keys gc_json then exit 1;
    let minor_heap_size =
      gc_json
      |> member "minor_heap_size"
      |> to_int_option
      |> Option.value ~default:33_554_432
    in
    let space_overhead =
      gc_json |> member "space_overhead" |> to_int_option |> Option.value ~default:120
    in
    let max_overhead =
      gc_json |> member "max_overhead" |> to_int_option |> Option.value ~default:1_000_000
    in
    let window_size =
      gc_json |> member "window_size" |> to_int_option |> Option.value ~default:10
    in
    let allocation_policy =
      gc_json |> member "allocation_policy" |> to_int_option |> Option.value ~default:2
    in
    let major_heap_increment =
      gc_json
      |> member "major_heap_increment"
      |> to_int_option
      |> Option.value ~default:100
    in
    Some
      { minor_heap_size
      ; space_overhead
      ; max_overhead
      ; window_size
      ; allocation_policy
      ; major_heap_increment
      }
;;

(** Parses the optional top-level "oracle" object. [None] when absent (engine uses
    [Oracle_runtime.default_config]). Exits on unknown sub-keys. All values optional;
    "assets" entries, keyed by symbol, override per field. *)
let parse_oracle_config json : Dio_oracle.Oracle_runtime.runtime_config option =
  let open Yojson.Basic.Util in
  match json |> member "oracle" with
  | `Null -> None
  | oracle_json ->
    if validate_keys ~context:"oracle" ~allowed:known_oracle_keys oracle_json then exit 1;
    let defaults = Dio_oracle.Oracle_runtime.default_config () in
    let opt_float key default =
      oracle_json |> member key |> to_float_opt |> Option.value ~default
    in
    Some
      { target_survival = opt_float "target_survival" defaults.target_survival
      ; min_active_dsurv = opt_float "min_active_dsurv" defaults.min_active_dsurv
      ; qty_cap_mult = opt_float "qty_cap_mult" defaults.qty_cap_mult
      ; refresh_seconds = opt_float "refresh_seconds" defaults.refresh_seconds
      ; assets =
          (match oracle_json |> member "assets" with
           | `Null -> defaults.assets
           | assets_json ->
             assets_json
             |> to_assoc
             |> List.map (fun (symbol, entry) ->
               if validate_keys
                    ~context:("oracle asset '" ^ symbol ^ "'")
                    ~allowed:known_oracle_asset_keys
                    entry
               then exit 1;
               let okey key = entry |> member key |> to_float_opt in
               ( symbol
               , ({ target_survival = okey "target_survival"
                  ; min_active_dsurv = okey "min_active_dsurv"
                  ; qty_cap_mult = okey "qty_cap_mult"
                  }
                  : Dio_oracle.Oracle_runtime.asset_overrides) )))
      }
;;

let is_hyperliquid exchange = String.equal (normalize_exchange exchange) "hyperliquid"

(** Optional [overlay] object inside a "trading" entry.

    The trading entry is the *inventory leg*: it names the venue/symbol whose held balance
    is hedged. The overlay names the *hedge controller*: [overlay.exchange] (default: the
    owning entry) is the perp venue. A controller is keyed by hedge venue + perp coin, so
    multiple inventory legs (e.g. Kraken BTC and Hyperliquid BTC) attach to one controller
    and therefore one perp position. This is what prevents two overlays from fighting over
    the same short.

    [overlay.symbol] is optional and defaults to the owning symbol for same-venue
    overlays, or [<base>/USDC] on Hyperliquid for cross-venue overlays.

    The hedged quantity is the same whether or not a grid is active: the free spot balance
    not in pending sell orders, plus the staked balance, excluding auto-earn balances
    backing pending sells. The grid's [reserved_base] is part of that spot figure; it is
    not added separately. The configured overlay ratio scales this accumulated balance.

    This is deliberately generic: the engine does not know what an overlay does, only that
    the named file is loaded and cycled by the controller domain. *)
let known_overlay_keys = [ "strategy"; "exchange"; "symbol"; "params" ]

(** Permitted keys of the [overlay.params] object. These are specific to the built-in
    carry overlay; custom overlays read their own [tunable:] defaults instead. *)
let known_overlay_param_keys =
  [ "start_ratio_pct"
  ; "add_threshold_pct"
  ; "max_ratio_pct"
  ; "recycle_profit_pct"
  ; "recycle_base_share_pct"
  ; "min_entry_margin_ratio"
  ; "min_liq_mult"
  ; "leverage"
  ]
;;

(** Per-asset carry policy. Percentages are whole numbers in config.json (25, 10, 75, 1,
    50); [min_liq_mult] is a multiple of the mark (2.5), [min_entry_margin_ratio] is a
    margin ratio (2.0), and [leverage] seeds the fill-time take-profit before the first
    position snapshot (0 means unknown). *)
type overlay_params =
  { start_ratio_pct : float
  ; add_threshold_pct : float
  ; max_ratio_pct : float
  ; recycle_profit_pct : float
  ; recycle_base_share_pct : float
  ; min_entry_margin_ratio : float
  ; min_liq_mult : float
  ; leverage : float
  }

let default_overlay_params =
  { start_ratio_pct = 25.0
  ; add_threshold_pct = 10.0
  ; max_ratio_pct = 75.0
  ; recycle_profit_pct = 1.0
  ; recycle_base_share_pct = 50.0
  ; min_entry_margin_ratio = 2.0
  ; min_liq_mult = 2.5
  ; leverage = 0.0
  }
;;

(** One inventory source feeding a hedge controller. *)
type overlay_leg =
  { ol_strategy : string
  ; ol_exchange : string (** inventory venue *)
  ; ol_symbol : string (** inventory symbol *)
  ; ol_has_grid : bool (** true when the owning entry runs a primary strategy *)
  ; ol_testnet : bool
  }

(** One perp position owner. [oc_legs] are all inventory sources hedged by the same perp. *)
type overlay_controller =
  { oc_strategy : string
  ; oc_hedge_exchange : string
  ; oc_hedge_symbol : string (** controller domain symbol (spot-pair form) *)
  ; oc_hedge_coin : string (** perp coin, e.g. [BTC] *)
  ; oc_params : overlay_params
  ; oc_legs : overlay_leg list
  ; oc_testnet : bool
  ; oc_has_hl_spot_leg : bool
  (** true when a leg is on Hyperliquid, so recycle base accumulation applies *)
  }

(** Controllers in config.json first-seen order. *)
let overlay_controllers : overlay_controller list ref = ref []

(** Legs in config.json order. *)
let overlay_legs : overlay_leg list ref = ref []

let hedge_key ~exchange ~coin =
  normalize_exchange exchange ^ "|" ^ String.uppercase_ascii coin
;;

let controller_key (c : overlay_controller) =
  hedge_key ~exchange:c.oc_hedge_exchange ~coin:c.oc_hedge_coin
;;

let get_overlay_controllers () = !overlay_controllers

(** The controller owned by the domain at [(exchange, symbol)]. The domain symbol is the
    hedge symbol; the key is the perp coin, so [BTC] and [BTC/USDC] resolve identically. *)
let get_overlay_controller ~exchange ~symbol =
  let key = hedge_key ~exchange ~coin:(base_coin_of_symbol symbol) in
  List.find_opt (fun c -> String.equal (controller_key c) key) !overlay_controllers
;;

let get_overlay_legs ~exchange ~symbol =
  match get_overlay_controller ~exchange ~symbol with
  | Some c -> c.oc_legs
  | None -> []
;;

let find_leg ~exchange ~symbol =
  let exchange = normalize_exchange exchange in
  List.find_opt
    (fun l -> String.equal l.ol_exchange exchange && String.equal l.ol_symbol symbol)
    !overlay_legs
;;

let get_overlay_strategy ~exchange ~symbol =
  Option.map (fun l -> l.ol_strategy) (find_leg ~exchange ~symbol)
;;

let get_overlay_params ~exchange ~symbol =
  match get_overlay_controller ~exchange ~symbol with
  | Some c -> c.oc_params
  | None ->
    (match find_leg ~exchange ~symbol with
     | Some l ->
       (match
          List.find_opt
            (fun c ->
              List.exists
                (fun x ->
                  String.equal x.ol_exchange l.ol_exchange
                  && String.equal x.ol_symbol l.ol_symbol)
                c.oc_legs)
            !overlay_controllers
        with
        | Some c -> c.oc_params
        | None -> default_overlay_params)
     | None -> default_overlay_params)
;;

(** True for a [trading] entry that only feeds a carry overlay and runs no grid. *)
let is_inventory_only ~exchange ~symbol =
  match find_leg ~exchange ~symbol with
  | Some l -> not l.ol_has_grid
  | None -> false
;;

let params_equal a b =
  let eq x y = Float.abs (x -. y) <= 1e-9 in
  eq a.start_ratio_pct b.start_ratio_pct
  && eq a.add_threshold_pct b.add_threshold_pct
  && eq a.max_ratio_pct b.max_ratio_pct
  && eq a.recycle_profit_pct b.recycle_profit_pct
  && eq a.recycle_base_share_pct b.recycle_base_share_pct
  && eq a.min_entry_margin_ratio b.min_entry_margin_ratio
  && eq a.min_liq_mult b.min_liq_mult
  && eq a.leverage b.leverage
;;

let parse_overlays json =
  overlay_controllers := [];
  overlay_legs := [];
  let open Yojson.Basic.Util in
  let params_of_overlay overlay =
    let p name default =
      match overlay |> member "params" with
      | `Assoc _ as params ->
        (match params |> member name with
         | (`Int _ | `Float _) as v -> Option.value (to_float_opt v) ~default
         | _ -> default)
      | _ -> default
    in
    { start_ratio_pct = p "start_ratio_pct" default_overlay_params.start_ratio_pct
    ; add_threshold_pct = p "add_threshold_pct" default_overlay_params.add_threshold_pct
    ; max_ratio_pct = p "max_ratio_pct" default_overlay_params.max_ratio_pct
    ; recycle_profit_pct =
        p "recycle_profit_pct" default_overlay_params.recycle_profit_pct
    ; recycle_base_share_pct =
        p "recycle_base_share_pct" default_overlay_params.recycle_base_share_pct
    ; min_entry_margin_ratio =
        p "min_entry_margin_ratio" default_overlay_params.min_entry_margin_ratio
    ; min_liq_mult = p "min_liq_mult" default_overlay_params.min_liq_mult
    ; leverage = p "leverage" default_overlay_params.leverage
    }
  in
  let controllers = ref [] in
  let find_controller ~exchange ~coin =
    let key = hedge_key ~exchange ~coin in
    List.find_opt (fun c -> String.equal (controller_key c) key) !controllers
  in
  let register_leg ~owner_exchange ~owner_symbol ~owner_strategy ~owner_testnet overlay =
    let strategy =
      overlay |> member "strategy" |> to_string_option |> Option.value ~default:""
    in
    if strategy <> ""
    then (
      let declared_hedge =
        overlay
        |> member "exchange"
        |> to_string_option
        |> Option.map normalize_exchange
        |> Option.value ~default:owner_exchange
      in
      let candidate_symbol =
        match overlay |> member "symbol" |> to_string_option with
        | Some s -> s
        | None ->
          if String.equal declared_hedge owner_exchange
          then owner_symbol
          else if is_hyperliquid declared_hedge
          then base_coin_of_symbol owner_symbol ^ "/USDC"
          else owner_symbol
      in
      let hedge_symbol =
        if is_hyperliquid declared_hedge && not (String.contains candidate_symbol '/')
        then candidate_symbol ^ "/USDC"
        else candidate_symbol
      in
      let hedge_coin = base_coin_of_symbol hedge_symbol in
      let has_grid = owner_strategy <> "" in
      let params = params_of_overlay overlay in
      let leg =
        { ol_strategy = strategy
        ; ol_exchange = owner_exchange
        ; ol_symbol = owner_symbol
        ; ol_has_grid = has_grid
        ; ol_testnet = owner_testnet
        }
      in
      (match find_leg ~exchange:owner_exchange ~symbol:owner_symbol with
       | Some _ ->
         Logging.critical_f
           ~section
           "Duplicate carry inventory leg %s/%s: each venue/asset may feed only one \
            hedge controller"
           owner_exchange
           owner_symbol;
         exit 1
       | None -> ());
      overlay_legs := leg :: !overlay_legs;
      match find_controller ~exchange:declared_hedge ~coin:hedge_coin with
      | Some c ->
        if not (String.equal c.oc_strategy strategy)
        then (
          Logging.critical_f
            ~section
            "Carry controller %s/%s declares strategy '%s' but leg %s/%s declares '%s'; \
             all legs of one hedge must share one overlay strategy"
            c.oc_hedge_exchange
            c.oc_hedge_symbol
            c.oc_strategy
            owner_exchange
            owner_symbol
            strategy;
          exit 1);
        if not (params_equal c.oc_params params)
        then (
          Logging.critical_f
            ~section
            "Carry controller %s/%s has conflicting params: leg %s/%s differs from the \
             first leg. Legs of one hedge must share the same params."
            c.oc_hedge_exchange
            c.oc_hedge_symbol
            owner_exchange
            owner_symbol;
          exit 1);
        let updated =
          { c with
            oc_legs = c.oc_legs @ [ leg ]
          ; oc_testnet = c.oc_testnet || owner_testnet
          ; oc_has_hl_spot_leg = c.oc_has_hl_spot_leg || is_hyperliquid owner_exchange
          }
        in
        controllers
        := List.map
             (fun x ->
               if String.equal (controller_key x) (controller_key c) then updated else x)
             !controllers
      | None ->
        controllers
        := !controllers
           @ [ { oc_strategy = strategy
               ; oc_hedge_exchange = declared_hedge
               ; oc_hedge_symbol = hedge_symbol
               ; oc_hedge_coin = hedge_coin
               ; oc_params = params
               ; oc_legs = [ leg ]
               ; oc_testnet = owner_testnet
               ; oc_has_hl_spot_leg = is_hyperliquid owner_exchange
               }
             ])
  in
  let entries =
    match json |> member "trading" with
    | `List entries -> entries
    | _ -> []
  in
  List.iter
    (fun entry ->
      match entry |> member "overlay" with
      | `Assoc _ as overlay ->
        let owner_exchange =
          entry
          |> member "exchange"
          |> to_string_option
          |> Option.value ~default:"kraken"
          |> normalize_exchange
        in
        let owner_symbol = entry |> member "symbol" |> to_string in
        let owner_strategy =
          entry |> member "strategy" |> to_string_option |> Option.value ~default:""
        in
        let owner_testnet =
          entry |> member "testnet" |> to_bool_option |> Option.value ~default:false
        in
        if validate_keys
             ~context:(Printf.sprintf "overlay for %s/%s" owner_exchange owner_symbol)
             ~allowed:known_overlay_keys
             overlay
        then exit 1;
        (match overlay |> member "params" with
         | `Assoc _ as params ->
           if validate_keys
                ~context:
                  (Printf.sprintf "overlay params for %s/%s" owner_exchange owner_symbol)
                ~allowed:known_overlay_param_keys
                params
           then exit 1
         | _ -> ());
        register_leg ~owner_exchange ~owner_symbol ~owner_strategy ~owner_testnet overlay
      | _ -> ())
    entries;
  overlay_legs := List.rev !overlay_legs;
  overlay_controllers := !controllers
;;

(** Domain assets to spawn: primary-strategy entries plus one controller asset per hedge
    instrument. Inventory-only entries (no [strategy]) contribute a leg but no domain. A
    controller whose hedge venue/symbol already has a primary-strategy entry attaches to
    that existing domain instead of spawning a duplicate. *)
let domain_assets (trading : trading_config list) : trading_config list =
  let grid = List.filter (fun (t : trading_config) -> t.strategy <> "") trading in
  let synthesized =
    List.filter_map
      (fun (c : overlay_controller) ->
        let already =
          List.exists
            (fun (t : trading_config) ->
              String.equal (normalize_exchange t.exchange) c.oc_hedge_exchange
              && String.equal t.symbol c.oc_hedge_symbol)
            grid
        in
        if already
        then None
        else
          Some
            { exchange = c.oc_hedge_exchange
            ; symbol = c.oc_hedge_symbol
            ; qty = "0"
            ; grid_interval = 1.0, 1.0
            ; sell_mult = "1.0"
            ; min_usd_balance = None
            ; max_exposure = None
            ; strategy = ""
            ; maker_fee = None
            ; taker_fee = None
            ; testnet = c.oc_testnet
            ; hedge = false
            ; accumulation_buffer = 0.01, 0.01
            ; data_feed = None
            ; base_accumulation = true
            ; sell_levels = false
            ; cpu_priority = 0
            })
      !overlay_controllers
  in
  grid @ synthesized
;;

let read_config () : config =
  try
    (* DIO_CONFIG overrides the config path; default "config.json". Used to run the
       reference and config-driven candidate side by side. *)
    let config_path =
      match Sys.getenv_opt "DIO_CONFIG" with
      | Some p -> p
      | None -> "config.json"
    in
    let json = Jsonc.from_file config_path in
    let open Yojson.Basic.Util in
    if validate_keys ~context:"top-level" ~allowed:known_top_level_keys json then exit 1;
    let cycle_mod =
      json |> member "cycle_mod" |> to_int_option |> Option.value ~default:10000
    in
    let logging = parse_logging_config json in
    let flow_log = parse_flow_log_config json in
    let gc = parse_gc_config json in
    let oracle = parse_oracle_config json in
    let trading = json |> member "trading" |> to_list |> List.map parse_config in
    (* Overlay declarations are parsed first: a trading entry without [strategy] is valid
       only as an inventory leg of a carry overlay. *)
    parse_overlays json;
    (* Fail fast if any entry's strategy file is missing/invalid/name-mismatched: without
       it the domain starts but binds no strategy and trades nothing. Checked here (engine
       config load) rather than in [parse_config] so the pure parser stays side-effect
       free for tests and tooling. *)
    List.iter
      (fun (t : trading_config) ->
        if String.equal t.strategy ""
        then (
          if not (is_inventory_only ~exchange:t.exchange ~symbol:t.symbol)
          then (
            Logging.critical_f
              ~section
              "Trading entry %s/%s has no strategy and no carry overlay; it would \
               neither trade nor hedge. Add a \"strategy\" or an \"overlay\"."
              t.exchange
              t.symbol;
            exit 1))
        else (
          match Dio_strategies.Strategy_loader.locate ~name:t.strategy with
          | None ->
            Logging.critical_f
              ~section
              "Strategy file for %s/%s does not exist (tried strategies/<name>.strategy \
               and strategies/<name>.json). Strategy files must be deployed alongside \
               config.json (e.g. /app/strategies); refusing to start with no strategy \
               bound."
              t.exchange
              t.symbol;
            exit 1
          | Some path ->
            (match Dio_strategies.Strategy_loader.parse_file ~path with
             | Error msg ->
               Logging.critical_f
                 ~section
                 "Strategy file '%s' for %s/%s is invalid: %s"
                 path
                 t.exchange
                 t.symbol
                 msg;
               exit 1
             | Ok file ->
               if not (String.equal file.name t.strategy)
               then (
                 Logging.critical_f
                   ~section
                   "Strategy name mismatch for %s/%s: config.json declares '%s' but \
                    strategy file '%s' declares '%s'"
                   t.exchange
                   t.symbol
                   t.strategy
                   path
                   file.name;
                 exit 1))))
      trading;
    List.iter
      (fun (c : overlay_controller) ->
        let strategy = c.oc_strategy in
        match Dio_strategies.Strategy_loader.locate ~name:strategy with
        | None ->
          Logging.critical_f
            ~section
            "Overlay strategy file for %s/%s does not exist (tried \
             strategies/<name>.strategy and strategies/<name>.json). Refusing to start \
             with a half-bound overlay."
            c.oc_hedge_exchange
            c.oc_hedge_symbol;
          exit 1
        | Some path ->
          (match Dio_strategies.Strategy_loader.parse_file ~path with
           | Error msg ->
             Logging.critical_f
               ~section
               "Overlay strategy file '%s' for %s/%s is invalid: %s"
               path
               c.oc_hedge_exchange
               c.oc_hedge_symbol
               msg;
             exit 1
           | Ok file ->
             if not (String.equal file.name strategy)
             then (
               Logging.critical_f
                 ~section
                 "Overlay strategy name mismatch for %s/%s: config.json declares '%s' \
                  but strategy file '%s' declares '%s'"
                 c.oc_hedge_exchange
                 c.oc_hedge_symbol
                 strategy
                 path
                 file.name;
               exit 1)))
      (get_overlay_controllers ());
    let fng_check_threshold =
      json |> member "fng_check_threshold" |> to_float_opt |> Option.value ~default:1.5
    in
    let latency_window_seconds =
      json |> member "latency_window_seconds" |> to_float_opt |> Option.value ~default:5.0
    in
    let latency_spike_threshold_us =
      json
      |> member "latency_spike_threshold_us"
      |> to_float_opt
      |> Option.value ~default:10.0
    in
    let latency_spike_report =
      json
      |> member "latency_spike_report"
      |> to_string_option
      |> Option.value ~default:"internal"
      |> latency_spike_report_of_string
    in
    let latency_spike_report_seconds =
      json
      |> member "latency_spike_report_seconds"
      |> to_float_opt
      |> Option.value ~default:30.0
    in
    let latency_network_spike_threshold_us =
      json
      |> member "latency_network_spike_threshold_us"
      |> to_float_opt
      |> Option.value ~default:20_000.0
    in
    let theme = json |> member "theme" |> to_string_option in
    let strategy_trace =
      json |> member "strategy_trace" |> to_bool_option |> Option.value ~default:false
    in
    { cycle_mod
    ; logging
    ; flow_log
    ; gc
    ; oracle
    ; trading
    ; fng_check_threshold
    ; latency_window_seconds
    ; latency_spike_threshold_us
    ; latency_spike_report
    ; latency_spike_report_seconds
    ; latency_network_spike_threshold_us
    ; theme
    ; strategy_trace
    }
  with
  | Yojson.Json_error msg ->
    Logging.critical_f ~section "Failed to parse config.json: %s" msg;
    exit 1
  | Sys_error msg ->
    Logging.warn_f ~section "Cannot read config.json: %s, using defaults" msg;
    { cycle_mod = 10000
    ; logging = { level = Logging.INFO; sections = []; width = None }
    ; flow_log =
        { enabled = flow_log_env_enabled ()
        ; min_interval_seconds = 0.2
        ; heartbeat_seconds = 5.0
        ; pulse_seconds = 5.0
        ; price_ticks = true
        }
    ; gc = None
    ; oracle = None
    ; trading = []
    ; fng_check_threshold = 1.5
    ; latency_window_seconds = 5.0
    ; latency_spike_threshold_us = 10.0
    ; latency_spike_report = Spike_report_internal
    ; latency_spike_report_seconds = 30.0
    ; latency_network_spike_threshold_us = 20_000.0
    ; theme = None
    ; strategy_trace = false
    }
;;

(** Cached GC config, parsed once on first access. Thread-safe via Lazy. *)
let cached_gc_config : gc_config option Lazy.t =
  lazy
    (let config = read_config () in
     config.gc)
;;

(** Applies GC tuning from the cached config. Must be called once per OCaml 5 domain (each
    domain has its own minor heap). No-op if [gc] is absent. *)
let apply_gc_config () =
  match Lazy.force cached_gc_config with
  | None -> ()
  | Some gc ->
    let ctrl = Gc.get () in
    Logging.info_f
      ~section
      "GC config requested: minor_heap_size=%d words (%d MB) space_overhead=%d \
       max_overhead=%d window_size=%d allocation_policy=%d major_heap_increment=%d"
      gc.minor_heap_size
      (gc.minor_heap_size * 8 / 1_048_576)
      gc.space_overhead
      gc.max_overhead
      gc.window_size
      gc.allocation_policy
      gc.major_heap_increment;
    Gc.set
      { ctrl with
        minor_heap_size = gc.minor_heap_size
      ; space_overhead = gc.space_overhead
      ; max_overhead = gc.max_overhead
      ; window_size = gc.window_size
      ; allocation_policy = gc.allocation_policy
      ; major_heap_increment = gc.major_heap_increment
      };
    (* Log the values the runtime actually applied. The runtime may silently clamp any
       field (on OxCaml [window_size] and [allocation_policy] are ignored), and a large or
       small minor heap changes pause frequency/duration, so report every requested-vs-
       applied difference rather than only [minor_heap_size]. *)
    let eff = Gc.get () in
    let clamp_note =
      let diffs =
        List.filter_map
          (fun (name, requested, applied) ->
            if requested = applied
            then None
            else Some (Printf.sprintf "%s requested %d applied %d" name requested applied))
          [ "minor_heap_size", gc.minor_heap_size, eff.minor_heap_size
          ; "space_overhead", gc.space_overhead, eff.space_overhead
          ; "max_overhead", gc.max_overhead, eff.max_overhead
          ; "window_size", gc.window_size, eff.window_size
          ; "allocation_policy", gc.allocation_policy, eff.allocation_policy
          ; "major_heap_increment", gc.major_heap_increment, eff.major_heap_increment
          ]
      in
      match diffs with
      | [] -> ""
      | l -> " [runtime clamp: " ^ String.concat "; " l ^ "]"
    in
    Logging.info_f
      ~section
      "GC config effective: minor_heap_size=%d words (%d MB) space_overhead=%d \
       max_overhead=%d window_size=%d allocation_policy=%d major_heap_increment=%d%s"
      eff.minor_heap_size
      (eff.minor_heap_size * 8 / 1_048_576)
      eff.space_overhead
      eff.max_overhead
      eff.window_size
      eff.allocation_policy
      eff.major_heap_increment
      clamp_note
;;

(* config.json is the single source of truth: expose the applier to the lower-level
   libraries that spawn domains so they apply it too (see [Gc_config_hook]). *)
let () = Gc_config.install apply_gc_config
