(** Per-symbol trading parameters parsed from a single entry in the "trading" array of config.json. *)
type trading_config = {
  exchange: string;
  symbol: string;
  qty: string;
  grid_interval: float * float;  (** (min, max) grid interval percentages; resolved to equal bounds when a scalar is provided *)
  sell_mult: string;
  min_usd_balance: string option;
  max_exposure: string option;
  strategy: string;
  maker_fee: float option;
  taker_fee: float option;
  testnet: bool;
  hedge: bool;
  accumulation_buffer: float * float;  (** (min, max) quote profit buffer; interpolated at runtime via Fear and Greed index *)
}
type logging_config = {
  level: Logging.level;
  sections: string list;
}

type gc_config = {
  minor_heap_size: int;
  space_overhead: int;
  max_overhead: int;
  window_size: int;
  allocation_policy: int;
  major_heap_increment: int;
}

type config = {
  cycle_mod: int;
  logging: logging_config;
  gc: gc_config option;
  trading: trading_config list;
  fng_check_threshold: float;
}

(** Logging section identifier for this module. *)
let section = "config"

(** Permitted key sets used by [validate_keys] for strict schema enforcement at each nesting level. *)
let known_top_level_keys =
  [ "logging_level"; "logging_sections"; "cycle_mod";
    "engine"; "trading"; "gc"; "fng_check_threshold" ]

let known_engine_keys = []

let known_gc_keys =
  [ "minor_heap_size"; "space_overhead"; "max_overhead";
    "window_size"; "allocation_policy"; "major_heap_increment" ]

let known_trading_keys =
  [ "symbol"; "exchange"; "qty"; "grid_interval"; "sell_mult";
    "min_usd_balance"; "max_exposure"; "strategy"; "maker_fee";
    "taker_fee"; "testnet"; "hedge"; "accumulation_buffer" ]

(** Validates that all keys in a JSON associative object belong to the [allowed] set.
    Logs at CRITICAL level for each unknown key. Returns [true] if any unknown keys are present. *)
let validate_keys ~context ~allowed json =
  let open Yojson.Basic.Util in
  let actual = json |> to_assoc |> List.map fst in
  let unknown = List.filter (fun k -> not (List.mem k allowed)) actual in
  List.iter (fun k ->
    Logging.critical_f ~section "Unknown config key '%s' in %s" k context
  ) unknown;
  unknown <> []

(** Parses the "grid_interval" field from a trading entry JSON object.
    Accepts a two-element list [min; max] or a single numeric/string scalar
    (promoted to equal bounds for backward compatibility). Defaults to (1.0, 1.0). *)
let parse_grid_interval json exchange symbol =
  let open Yojson.Basic.Util in
  let default = (1.0, 1.0) in
  let float_of_json = function
    | `Float f -> Some f
    | `Int i -> Some (float_of_int i)
    | `String s -> (try Some (float_of_string s) with _ -> None)
    | _ -> None
  in
  match json |> member "grid_interval" with
  | `List [lo; hi] -> (
      match float_of_json lo, float_of_json hi with
      | Some a, Some b ->
          let low = min a b in
          let high = max a b in
          (low, high)
      | _ ->
          Logging.warn_f ~section "Invalid grid_interval list for %s/%s, using default %.2f-%.2f"
            exchange symbol (fst default) (snd default);
          default)
  | `List _ ->
      Logging.warn_f ~section "grid_interval must be a two-value list for %s/%s, using default %.2f-%.2f"
        exchange symbol (fst default) (snd default);
      default
  | (`Float _ | `Int _ | `String _) as v -> (
      match float_of_json v with
      | Some x -> (x, x)
      | None ->
          Logging.warn_f ~section "Invalid grid_interval value for %s/%s, using default %.2f-%.2f"
            exchange symbol (fst default) (snd default);
          default)
  | _ -> default

(** Parses the "accumulation_buffer" field from a trading entry JSON object.
    Accepts a two-element list [min; max] or a single numeric/string scalar
    (promoted to equal bounds for backward compatibility). Defaults to (0.01, 0.01). *)
let parse_accumulation_buffer json exchange symbol =
  let open Yojson.Basic.Util in
  let default = (0.01, 0.01) in
  let float_of_json = function
    | `Float f -> Some f
    | `Int i -> Some (float_of_int i)
    | `String s -> (try Some (float_of_string s) with _ -> None)
    | _ -> None
  in
  match json |> member "accumulation_buffer" with
  | `List [lo; hi] -> (
      match float_of_json lo, float_of_json hi with
      | Some a, Some b ->
          let low = min a b in
          let high = max a b in
          (low, high)
      | _ ->
          Logging.warn_f ~section "Invalid accumulation_buffer list for %s/%s, using default %.2f-%.2f"
            exchange symbol (fst default) (snd default);
          default)
  | `List _ ->
      Logging.warn_f ~section "accumulation_buffer must be a two-value list for %s/%s, using default %.2f-%.2f"
        exchange symbol (fst default) (snd default);
      default
  | (`Float _ | `Int _ | `String _) as v -> (
      match float_of_json v with
      | Some x -> (x, x)
      | None ->
          Logging.warn_f ~section "Invalid accumulation_buffer value for %s/%s, using default %.2f-%.2f"
            exchange symbol (fst default) (snd default);
          default)
  | _ -> default

(** Parses a single trading entry from the JSON "trading" array into a [trading_config].
    Validates keys, enforces exchange-specific constraints (e.g. testnet/hedge/accumulation_buffer
    are restricted to Hyperliquid), and restricts grid_interval to the Grid strategy. Exits on
    schema violations. *)
let parse_config json =
  if validate_keys ~context:"trading entry" ~allowed:known_trading_keys json then
    exit 1;
  let open Yojson.Basic.Util in
  let symbol = json |> member "symbol" |> to_string in
  let exchange = json |> member "exchange" |> to_string_option |> Option.value ~default:"kraken" in
  (* Enforce that testnet and hedge are only valid for Hyperliquid entries.
     accumulation_buffer is also valid for ibkr (whole-share accumulation). *)
  if exchange <> "hyperliquid" && exchange <> "ibkr" then begin
    let restricted = [ "testnet"; "hedge"; "accumulation_buffer" ] in
    let actual = json |> to_assoc |> List.map fst in
    let bad = List.filter (fun k -> List.mem k restricted) actual in
    if bad <> [] then begin
      List.iter (fun k ->
        Logging.critical_f ~section "Key '%s' is not valid for exchange '%s' (found in %s/%s)" k exchange exchange symbol
      ) bad;
      exit 1
    end
  end;
  if exchange <> "hyperliquid" then begin
    let hl_only = [ "hedge" ] in
    let actual = json |> to_assoc |> List.map fst in
    let bad = List.filter (fun k -> List.mem k hl_only) actual in
    if bad <> [] then begin
      List.iter (fun k ->
        Logging.critical_f ~section "Key '%s' is only valid for hyperliquid (found in %s/%s)" k exchange symbol
      ) bad;
      exit 1
    end
  end;
  (* testnet is valid for hyperliquid and ibkr only *)
  if exchange <> "hyperliquid" && exchange <> "ibkr" then begin
    let actual = json |> to_assoc |> List.map fst in
    if List.mem "testnet" actual then begin
      Logging.critical_f ~section "Key 'testnet' is only valid for hyperliquid and ibkr (found in %s/%s)" exchange symbol;
      exit 1
    end
  end;
  let strategy = json |> member "strategy" |> to_string in
  (* Reject grid_interval when strategy is not Grid. *)
  if strategy <> "Grid" then begin
    let actual = json |> to_assoc |> List.map fst in
    if List.mem "grid_interval" actual then begin
      Logging.critical_f ~section "Key 'grid_interval' is only valid for Grid strategy (found in %s/%s with strategy=%s)" exchange symbol strategy;
      exit 1
    end
  end;
  let testnet = json |> member "testnet" |> to_bool_option |> Option.value ~default:false in
  let hedge = json |> member "hedge" |> to_bool_option |> Option.value ~default:false in
  {
    exchange;
    symbol;
    qty = json |> member "qty" |> to_string;
    grid_interval = parse_grid_interval json exchange symbol;
    sell_mult = json |> member "sell_mult" |> to_string_option |> Option.value ~default:"1.0";
    min_usd_balance = json |> member "min_usd_balance" |> to_string_option;
    max_exposure = json |> member "max_exposure" |> to_string_option;
    strategy;
    maker_fee = json |> member "maker_fee" |> to_option to_float;
    taker_fee = json |> member "taker_fee" |> to_option to_float;
    testnet;
    hedge;
    accumulation_buffer = parse_accumulation_buffer json exchange symbol;
  }

(** Parses top-level "logging_level" and "logging_sections" fields into a [logging_config].
    Defaults to INFO level and no section filters when fields are absent or invalid. *)
let parse_logging_config json : logging_config =
  let open Yojson.Basic.Util in
  let level_str = json |> member "logging_level" |> to_string_option |> Option.value ~default:"info" in
  let sections_str = json |> member "logging_sections" |> to_string_option |> Option.value ~default:"" in
  let level = match Logging.level_of_string level_str with
    | Some lvl -> lvl
    | None -> Logging.warn_f ~section:"config" "Unknown logging level '%s', defaulting to INFO" level_str; Logging.INFO
  in
  let sections = sections_str |> String.split_on_char ',' |> List.map String.trim |> List.filter ((<>) "") in
  { level; sections }

(** Parses the optional "gc" object into OCaml GC tuning parameters.
    Returns [None] when the key is absent. Exits on unknown sub-keys. *)
let parse_gc_config json : gc_config option =
  let open Yojson.Basic.Util in
  match json |> member "gc" with
  | `Null -> None
  | gc_json ->
      if validate_keys ~context:"gc" ~allowed:known_gc_keys gc_json then
        exit 1;
      let minor_heap_size = gc_json |> member "minor_heap_size" |> to_int_option |> Option.value ~default:8_388_608 in
      let space_overhead = gc_json |> member "space_overhead" |> to_int_option |> Option.value ~default:80 in
      let max_overhead = gc_json |> member "max_overhead" |> to_int_option |> Option.value ~default:150 in
      let window_size = gc_json |> member "window_size" |> to_int_option |> Option.value ~default:10 in
      let allocation_policy = gc_json |> member "allocation_policy" |> to_int_option |> Option.value ~default:2 in
      let major_heap_increment = gc_json |> member "major_heap_increment" |> to_int_option |> Option.value ~default:100 in
      Some {
        minor_heap_size;
        space_overhead;
        max_overhead;
        window_size;
        allocation_policy;
        major_heap_increment;
      }

(** Reads config.json from the working directory and parses it into a [config] record.
    Performs strict key validation at each nesting level, exiting on schema violations
    or JSON parse errors. Falls back to defaults on filesystem errors. *)
let read_config () : config =
  try
    let json = Yojson.Basic.from_file "config.json" in
    let open Yojson.Basic.Util in
    (* Reject unknown top-level keys. *)
    if validate_keys ~context:"top-level" ~allowed:known_top_level_keys json then
      exit 1;
    (* Reject unknown keys in the optional "engine" sub-object. *)
    (match json |> member "engine" with
     | `Null -> ()
     | engine_json ->
       if validate_keys ~context:"engine" ~allowed:known_engine_keys engine_json then
         exit 1);
    let cycle_mod = json |> member "cycle_mod" |> to_int_option |> Option.value ~default:10000 in
    let logging = parse_logging_config json in
    let gc = parse_gc_config json in
    let trading = json |> member "trading" |> to_list |> List.map parse_config in
    let fng_check_threshold = json |> member "fng_check_threshold" |> to_float_option |> Option.value ~default:1.5 in
    { cycle_mod; logging; gc; trading; fng_check_threshold }
  with
  | Yojson.Json_error msg ->
    Logging.critical_f ~section "Failed to parse config.json: %s" msg;
    exit 1
  | Sys_error msg ->
    Logging.warn_f ~section "Cannot read config.json: %s, using defaults" msg;
    { cycle_mod = 10000; logging = { level = Logging.INFO; sections = [] };
      gc = None; trading = []; fng_check_threshold = 1.5 }

(** Cached GC config, parsed once on first access. Thread-safe via Lazy. *)
let cached_gc_config : gc_config option Lazy.t = lazy (
  let config = read_config () in
  config.gc
)

(** Apply GC tuning parameters from the cached config. Must be called
    once per OCaml 5 domain (each domain has its own minor heap).
    No-op if [gc] is absent from config.json. *)
let apply_gc_config () =
  match Lazy.force cached_gc_config with
  | None -> ()
  | Some gc ->
      let ctrl = Gc.get () in
      Gc.set { ctrl with
        minor_heap_size = gc.minor_heap_size;
        space_overhead = gc.space_overhead;
        max_overhead = gc.max_overhead;
        window_size = gc.window_size;
        allocation_policy = gc.allocation_policy;
        major_heap_increment = gc.major_heap_increment;
      }

