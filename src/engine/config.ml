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
  ; base_accumulation : bool (** Per-strategy opt-in to base-accumulation persistence. *)
  ; sell_levels : bool (** Per-strategy opt-in to pending-sell-level persistence. *)
  }

type logging_config =
  { level : Logging.level
  ; sections : string list
  ; width : int option
    (** Fixed log-wrap width. None = auto: TTY width, else `COLUMNS` env var,
        else 200. *)
  }

type gc_config =
  { minor_heap_size : int
  ; space_overhead : int
  ; max_overhead : int
  ; window_size : int
  ; allocation_policy : int
  ; major_heap_increment : int
  }

(** Latency families that emit spike logs. [Spike_report_internal] covers the
    per-domain pipeline stages (OB/EXEC/PREP/STRAT/CYCLE); [Spike_report_network]
    covers per-venue ws_ping/ws_feed/rest_request/signer windows. Separate so the
    network tail can be silenced independently. *)
type latency_spike_report =
  | Spike_report_internal
  | Spike_report_network
  | Spike_report_both
  | Spike_report_none

type config =
  { cycle_mod : int
  ; logging : logging_config
  ; gc : gc_config option
  ; trading : trading_config list
  ; oracle : Dio_oracle.Oracle_runtime.runtime_config option
    (** Capital-oracle runtime knobs from the top-level "oracle" section.
        [None] = engine uses [Oracle_runtime.default_config]. *)
  ; fng_check_threshold : float
  ; latency_window_seconds : float
    (** Per-domain latency accumulation window, in seconds: the histogram is
        snapshotted and reset each window. Shorter = faster dashboard movement,
        fewer samples per window. *)
  ; latency_spike_threshold_us : float
    (** Per-stage latency ceiling, in microseconds. A window with samples above
        it emits one INFO line naming offending stages, worst spike, and breach
        count. Target: 10us. *)
  ; latency_spike_report : latency_spike_report
    (** Latency families that emit the spike logs. Default
        [Spike_report_internal]: internal ops only, network silenced. *)
  ; latency_spike_report_seconds : float
    (** Minimum wall-clock interval between per-domain internal spike log lines,
        in seconds. Percentile windows still publish on
        [latency_window_seconds]. Default 30; 0 logs every window. *)
  ; latency_network_spike_threshold_us : float
    (** Network-family latency ceiling, in microseconds. Separate from
        [latency_spike_threshold_us] because network metrics (ws ping/feed, REST,
        signer) are millisecond-scale and would flag every window at 10us. *)
  ; theme : string option (** Optional UI theme name for the terminal dashboard. *)
  }

(** Logging section identifier for this module. *)
let section = "config"

(** Accepts [Int] and [Float] JSON numbers; all else (including [Null]) is
    [None]. Yojson's [to_float_option] raises on [Int], which aborts startup
    with an uncaught [Type_error]. *)
let to_float_opt = function
  | `Int i -> Some (float_of_int i)
  | `Float f -> Some f
  | _ -> None
;;

(** Parses a [latency_spike_report] from its config string. Unknown values warn
    and fall back to internal-only. *)
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

(** Permitted key sets used by [validate_keys] for strict schema enforcement at each nesting level. *)
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

(** Permitted keys of the optional top-level "oracle" section. All optional;
    absent keys fall back to [Oracle_runtime.default_config]. *)
let known_oracle_keys =
  [ "qty_cap_mult"; "target_survival"; "min_active_dsurv"; "refresh_seconds"; "assets" ]
;;

(** Keys accepted inside each "oracle"."assets" entry (per-asset overrides,
    keyed by symbol). *)
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
  ]
;;

(** Logs CRITICAL for each key of [json] not in [allowed]; returns [true] if any
    unknown keys are present. *)
let validate_keys ~context ~allowed json =
  let open Yojson.Basic.Util in
  let actual = json |> to_assoc |> List.map fst in
  let unknown = List.filter (fun k -> not (List.mem k allowed)) actual in
  List.iter
    (fun k -> Logging.critical_f ~section "Unknown config key '%s' in %s" k context)
    unknown;
  unknown <> []
;;

(** Parses "grid_interval": [min; max] list or a numeric/string scalar
    (promoted to equal bounds). Default (1.0, 1.0). *)
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

(** Parses "accumulation_buffer": [min; max] list or a numeric/string scalar
    (promoted to equal bounds). Default (0.01, 0.01). *)
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

(** Parses one entry of the JSON "trading" array into a [trading_config].
    Validates keys and venue restrictions; [exit 1] on schema violation. *)
let parse_config json =
  if validate_keys ~context:"trading entry" ~allowed:known_trading_keys json then exit 1;
  let open Yojson.Basic.Util in
  let symbol = json |> member "symbol" |> to_string in
  let exchange =
    json |> member "exchange" |> to_string_option |> Option.value ~default:"kraken"
  in
  let exch_id = Dio_exchange.Exchange_intf.Types.exchange_of_string exchange in
  (* testnet/hedge are venue-limited; accumulation_buffer is valid on every venue
     (Kraken runs the same reserved_base accrual; see
     jacobs_ladder_config.kraken_config). *)
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
  let strategy = json |> member "strategy" |> to_string in
  (* grid_interval carries the hardened search bounds (gi_min, gi_max) walked by
     the oracle's parameter search. *)
  let testnet =
    json |> member "testnet" |> to_bool_option |> Option.value ~default:false
  in
  let hedge = json |> member "hedge" |> to_bool_option |> Option.value ~default:false in
  let data_feed = json |> member "data_feed" |> to_string_option in
  { exchange
  ; symbol
  ; qty = json |> member "qty" |> to_string
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
  }
;;

(** Parses "logging_level"/"logging_sections"/"logging_width". Defaults to INFO
    and no section filters when absent or invalid. *)
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

(** Parses the optional top-level "oracle" object. [None] when absent (engine
    uses [Oracle_runtime.default_config]). Exits on unknown sub-keys. All values
    optional; "assets" entries, keyed by symbol, override per field. *)
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
               if
                 validate_keys
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

let read_config () : config =
  try
    let json = Yojson.Basic.from_file "config.json" in
    let open Yojson.Basic.Util in
    if validate_keys ~context:"top-level" ~allowed:known_top_level_keys json then exit 1;
    let cycle_mod =
      json |> member "cycle_mod" |> to_int_option |> Option.value ~default:10000
    in
    let logging = parse_logging_config json in
    let gc = parse_gc_config json in
    let oracle = parse_oracle_config json in
    let trading = json |> member "trading" |> to_list |> List.map parse_config in
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
    { cycle_mod
    ; logging
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
    ; fng_check_threshold = 1.5
    ; latency_window_seconds = 5.0
    ; latency_spike_threshold_us = 10.0
    ; latency_spike_report = Spike_report_internal
    ; latency_spike_report_seconds = 30.0
    ; latency_network_spike_threshold_us = 20_000.0
    ; theme = None
    }
;;

(** Cached GC config, parsed once on first access. Thread-safe via Lazy. *)
let cached_gc_config : gc_config option Lazy.t =
  lazy
    (let config = read_config () in
     config.gc)
;;

(** Applies GC tuning from the cached config. Must be called once per OCaml 5
    domain (each domain has its own minor heap). No-op if [gc] is absent. *)
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
