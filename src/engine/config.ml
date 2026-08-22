(** Per-symbol trading parameters parsed from a single entry in the "trading" array of config.json. *)
type trading_config = Dio_strategies.Strategy_common.trading_config =
  { exchange : string
  ; symbol : string
  ; qty : string
  ; grid_interval : float * float
    (** (min, max) grid interval percentages; resolved to equal bounds when a scalar is provided *)
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
  ; asset_class : string option
    (** Risk class for capital-oracle modeling (explicit from config.json). *)
  ; base_accumulation : bool (** Per-strategy opt-in to base-accumulation persistence. *)
  ; sell_levels : bool (** Per-strategy opt-in to pending-sell-level persistence. *)
  }

type logging_config =
  { level : Logging.level
  ; sections : string list
  ; width : int option
    (** Optional fixed line width for log wrapping. None = auto: the terminal
        width when output is a TTY, else the `COLUMNS` env var when set, else
        a generous default (200). *)
  }

type gc_config =
  { minor_heap_size : int
  ; space_overhead : int
  ; max_overhead : int
  ; window_size : int
  ; allocation_policy : int
  ; major_heap_increment : int
  }

(** Risk-class member pool: member symbols plus an optional per-class blend
    weight (kappa). *)
type class_pool =
  { members : string list
  ; kappa : int option
  }

type config =
  { cycle_mod : int
  ; logging : logging_config
  ; gc : gc_config option
  ; trading : trading_config list
  ; classes : (string * class_pool) list
    (** Risk-class definitions (class name -> member pool) from the top-level
        "classes" map; the class pools backing the kappa blend. The legacy
        schema `"name": [syms]` parses with kappa = None; the extended schema
        `"name": {"members": [syms], "kappa": N}` carries a per-class blend
        weight. *)
  ; oracle : Dio_oracle.Oracle_runtime.runtime_config option
    (** Capital-oracle runtime knobs from the top-level "oracle" section;
        [None] means the engine runs the oracle's built-in defaults (see
        Oracle_runtime.default_config). *)
  ; fng_check_threshold : float
  ; latency_window_seconds : float
    (** Duration of each per-domain latency accumulation window before the
        histogram is snapshotted and reset. Shorter windows make the dashboard
        percentiles move faster but reduce sample counts per window. *)
  }

(** Logging section identifier for this module. *)
let section = "config"

(** Permitted key sets used by [validate_keys] for strict schema enforcement at each nesting level. *)
let known_top_level_keys =
  [ "logging_level"
  ; "logging_sections"
  ; "logging_width"
  ; "cycle_mod"
  ; "latency_window_seconds"
  ; "engine"
  ; "trading"
  ; "gc"
  ; "classes"
  ; "oracle"
  ; "fng_check_threshold"
  ]
;;

let known_engine_keys = []

let known_gc_keys =
  [ "minor_heap_size"
  ; "space_overhead"
  ; "max_overhead"
  ; "window_size"
  ; "allocation_policy"
  ; "major_heap_increment"
  ]
;;

(** Permitted keys of the optional top-level "oracle" section (capital-oracle
    runtime knobs). Every key is optional; absent keys fall back to
    Oracle_runtime.default_config. *)
let known_oracle_keys =
  [ "target_survival"
  ; "fng_weight"
  ; "range_weight"
  ; "min_active_dsurv"
  ; "qty_cap_mult"
  ; "no_deep_history"
  ; "weight_by_sessions"
  ; "refresh_seconds"
  ; "poll_seconds"
  ; "horizons"
  ; "max_capital"
  ; "startup_wait_seconds"
  ; "assets"
  ]
;;

(** Keys accepted inside each "oracle" -> "assets" entry (the per-asset
    override layer): the sizing/blend/history knobs only. Cadence/machinery
    knobs stay global. *)
let known_oracle_asset_keys =
  [ "target_survival"
  ; "fng_weight"
  ; "range_weight"
  ; "min_active_dsurv"
  ; "qty_cap_mult"
  ; "no_deep_history"
  ; "weight_by_sessions"
  ; "horizons"
  ]
;;

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
  ; "asset_class"
  ; "base_accumulation"
  ; "sell_levels"
  ]
;;

(** Validates that all keys in a JSON associative object belong to the [allowed] set.
    Logs at CRITICAL level for each unknown key. Returns [true] if any unknown keys are present. *)
let validate_keys ~context ~allowed json =
  let open Yojson.Basic.Util in
  let actual = json |> to_assoc |> List.map fst in
  let unknown = List.filter (fun k -> not (List.mem k allowed)) actual in
  List.iter
    (fun k -> Logging.critical_f ~section "Unknown config key '%s' in %s" k context)
    unknown;
  unknown <> []
;;

(** Parses the "grid_interval" field from a trading entry JSON object.
    Accepts a two-element list [min; max] or a single numeric/string scalar
    (promoted to equal bounds for backward compatibility). Defaults to (1.0, 1.0). *)
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

(** Parses the "accumulation_buffer" field from a trading entry JSON object.
    Accepts a two-element list [min; max] or a single numeric/string scalar
    (promoted to equal bounds for backward compatibility). Defaults to (0.01, 0.01). *)
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

(** Parses the optional top-level "classes" object. Two schemas:
    - legacy: class name -> [member symbols]
    - extended: class name -> {"members": [...], "kappa": N (optional)}
    Class pools for the capital-oracle kappa blend are read from here (no
    hardcoded lists in code). Returns [] when the key is absent. *)
let parse_classes json =
  let open Yojson.Basic.Util in
  match json |> member "classes" with
  | `Assoc entries ->
    List.map
      (fun (name, value) ->
         let pool =
           match value with
           | `List _ ->
             (* Legacy schema: bare member symbol list, default kappa. *)
             { members = value |> to_list |> List.map to_string; kappa = None }
           | `Assoc _ ->
             { members = value |> member "members" |> to_list |> List.map to_string
             ; kappa = value |> member "kappa" |> to_int_option
             }
           | _ -> { members = []; kappa = None }
         in
         name, pool)
      entries
  | _ -> []
;;

(** Parses a single trading entry from the JSON "trading" array into a [trading_config].
    Validates keys, enforces exchange-specific constraints (e.g. testnet/hedge/accumulation_buffer
    are restricted to Hyperliquid), and restricts grid_interval to the Grid strategy. Exits on
    schema violations. *)
let parse_config json =
  if validate_keys ~context:"trading entry" ~allowed:known_trading_keys json then exit 1;
  let open Yojson.Basic.Util in
  let symbol = json |> member "symbol" |> to_string in
  let exchange =
    json |> member "exchange" |> to_string_option |> Option.value ~default:"kraken"
  in
  let exch_id = Dio_exchange.Exchange_intf.Types.exchange_of_string exchange in
  (* Enforce that testnet and hedge are only valid for supported entries.
     accumulation_buffer is valid for hyperliquid, ibkr, lighter, and alpaca. *)
  (match exch_id with
   | Hyperliquid | Ibkr | Lighter | Alpaca -> ()
   | Kraken | Custom _ ->
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
  let strategy = json |> member "strategy" |> to_string in
  (* Reject grid_interval when strategy is not Grid. *)
  if strategy <> "Ladder"
  then (
    let actual = json |> to_assoc |> List.map fst in
    if List.mem "grid_interval" actual
    then (
      Logging.critical_f
        ~section
        "Key 'grid_interval' is only valid for Grid strategy (found in %s/%s with \
         strategy=%s)"
        exchange
        symbol
        strategy;
      exit 1));
  let testnet =
    json |> member "testnet" |> to_bool_option |> Option.value ~default:false
  in
  let hedge = json |> member "hedge" |> to_bool_option |> Option.value ~default:false in
  let data_feed = json |> member "data_feed" |> to_string_option in
  let asset_class = json |> member "asset_class" |> to_string_option in
  { exchange
  ; symbol
  ; qty = json |> member "qty" |> to_string
  ; grid_interval = parse_grid_interval json exchange symbol
  ; sell_mult =
      json |> member "sell_mult" |> to_string_option |> Option.value ~default:"1.0"
  ; min_usd_balance = json |> member "min_usd_balance" |> to_string_option
  ; max_exposure = json |> member "max_exposure" |> to_string_option
  ; strategy
  ; maker_fee = json |> member "maker_fee" |> to_option to_float
  ; taker_fee = json |> member "taker_fee" |> to_option to_float
  ; testnet
  ; hedge
  ; accumulation_buffer = parse_accumulation_buffer json exchange symbol
  ; data_feed
  ; asset_class
  ; base_accumulation =
      json |> member "base_accumulation" |> to_bool_option |> Option.value ~default:true
  ; sell_levels =
      json |> member "sell_levels" |> to_bool_option |> Option.value ~default:false
  }
;;

(** Parses top-level "logging_level" and "logging_sections" fields into a [logging_config].
    Defaults to INFO level and no section filters when fields are absent or invalid. *)
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

(** Parses the optional "gc" object into OCaml GC tuning parameters.
    Returns [None] when the key is absent. Exits on unknown sub-keys. *)
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

(** Parses the optional top-level "oracle" object into the capital-oracle
    runtime knobs. Returns [None] when the key is absent (the engine then uses
    Oracle_runtime.default_config). Exits on unknown sub-keys. Every value is
    optional and falls back to the runtime defaults, so a minimal section like
    {"qty_cap_mult": 0.0} is valid. *)
let parse_oracle_config json : Dio_oracle.Oracle_runtime.runtime_config option =
  let open Yojson.Basic.Util in
  match json |> member "oracle" with
  | `Null -> None
  | oracle_json ->
    if validate_keys ~context:"oracle" ~allowed:known_oracle_keys oracle_json then exit 1;
    let defaults = Dio_oracle.Oracle_runtime.default_config () in
    Some
      { target_survival =
          oracle_json
          |> member "target_survival"
          |> to_float_option
          |> Option.value ~default:defaults.target_survival
      ; fng_weight =
          oracle_json
          |> member "fng_weight"
          |> to_float_option
          |> Option.value ~default:defaults.fng_weight
      ; range_weight =
          oracle_json
          |> member "range_weight"
          |> to_float_option
          |> Option.value ~default:defaults.range_weight
      ; min_active_dsurv =
          oracle_json
          |> member "min_active_dsurv"
          |> to_float_option
          |> Option.value ~default:defaults.min_active_dsurv
      ; qty_cap_mult =
          oracle_json
          |> member "qty_cap_mult"
          |> to_float_option
          |> Option.value ~default:defaults.qty_cap_mult
      ; no_deep_history =
          oracle_json
          |> member "no_deep_history"
          |> to_bool_option
          |> Option.value ~default:defaults.no_deep_history
      ; weight_by_sessions =
          oracle_json
          |> member "weight_by_sessions"
          |> to_bool_option
          |> Option.value ~default:defaults.weight_by_sessions
      ; refresh_seconds =
          oracle_json
          |> member "refresh_seconds"
          |> to_float_option
          |> Option.value ~default:defaults.refresh_seconds
      ; poll_seconds =
          oracle_json
          |> member "poll_seconds"
          |> to_float_option
          |> Option.value ~default:defaults.poll_seconds
      ; horizons =
          (match oracle_json |> member "horizons" with
           | `Null -> defaults.horizons
           | horizons_json -> Some (horizons_json |> to_list |> List.map to_int))
      ; max_capital =
          (match oracle_json |> member "max_capital" with
           | `Null -> defaults.max_capital
           | v -> to_float_option v)
      ; startup_wait_seconds =
          oracle_json
          |> member "startup_wait_seconds"
          |> to_float_option
          |> Option.value ~default:defaults.startup_wait_seconds
      ; assets =
          (match oracle_json |> member "assets" with
           | `Null -> defaults.assets
           | assets_json ->
             assets_json
             |> to_assoc
             |> List.map (fun (symbol, entry) ->
               if
                 validate_keys
                   ~context:("oracle asset '" ^ symbol ^ "'")
                   ~allowed:known_oracle_asset_keys
                   entry
               then exit 1;
               let opt key parse = entry |> member key |> parse in
               ( symbol
               , ({ target_survival = opt "target_survival" to_float_option
                  ; fng_weight = opt "fng_weight" to_float_option
                  ; range_weight = opt "range_weight" to_float_option
                  ; min_active_dsurv = opt "min_active_dsurv" to_float_option
                  ; qty_cap_mult = opt "qty_cap_mult" to_float_option
                  ; no_deep_history = opt "no_deep_history" to_bool_option
                  ; weight_by_sessions = opt "weight_by_sessions" to_bool_option
                  ; horizons =
                      (match entry |> member "horizons" with
                       | `Null -> None
                       | horizons_json ->
                         Some (horizons_json |> to_list |> List.map to_int))
                  }
                  : Dio_oracle.Oracle_runtime.asset_overrides) )))
      }
;;

(** Reads config.json from the working directory and parses it into a [config] record.
    Performs strict key validation at each nesting level, exiting on schema violations
    or JSON parse errors. Falls back to defaults on filesystem errors. *)
let read_config () : config =
  try
    let json = Yojson.Basic.from_file "config.json" in
    let open Yojson.Basic.Util in
    if validate_keys ~context:"top-level" ~allowed:known_top_level_keys json then exit 1;
    (match json |> member "engine" with
     | `Null -> ()
     | engine_json ->
       if validate_keys ~context:"engine" ~allowed:known_engine_keys engine_json
       then exit 1);
    let cycle_mod =
      json |> member "cycle_mod" |> to_int_option |> Option.value ~default:10000
    in
    let logging = parse_logging_config json in
    let gc = parse_gc_config json in
    let oracle = parse_oracle_config json in
    let trading = json |> member "trading" |> to_list |> List.map parse_config in
    let classes = parse_classes json in
    let fng_check_threshold =
      json |> member "fng_check_threshold" |> to_float_option |> Option.value ~default:1.5
    in
    let latency_window_seconds =
      json
      |> member "latency_window_seconds"
      |> to_float_option
      |> Option.value ~default:5.0
    in
    { cycle_mod
    ; logging
    ; gc
    ; oracle
    ; trading
    ; classes
    ; fng_check_threshold
    ; latency_window_seconds
    }
  with
  | Yojson.Json_error msg ->
    Logging.critical_f ~section "Failed to parse config.json: %s" msg;
    exit 1
  | Sys_error msg ->
    Logging.warn_f ~section "Cannot read config.json: %s, using defaults" msg;
    { cycle_mod = 10000
    ; logging = { level = Logging.INFO; sections = []; width = None }
    ; gc = None
    ; oracle = None
    ; trading = []
    ; classes = []
    ; fng_check_threshold = 1.5
    ; latency_window_seconds = 5.0
    }
;;

(** Cached GC config, parsed once on first access. Thread-safe via Lazy. *)
let cached_gc_config : gc_config option Lazy.t =
  lazy
    (let config = read_config () in
     config.gc)
;;

(** Apply GC tuning parameters from the cached config. Must be called
    once per OCaml 5 domain (each domain has its own minor heap).
    No-op if [gc] is absent from config.json. *)
let apply_gc_config () =
  match Lazy.force cached_gc_config with
  | None -> ()
  | Some gc ->
    let ctrl = Gc.get () in
    Gc.set
      { ctrl with
        minor_heap_size = gc.minor_heap_size
      ; space_overhead = gc.space_overhead
      ; max_overhead = gc.max_overhead
      ; window_size = gc.window_size
      ; allocation_policy = gc.allocation_policy
      ; major_heap_increment = gc.major_heap_increment
      }
;;
