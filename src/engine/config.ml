(** Trading configuration type *)
type trading_config = {
  exchange: string;
  symbol: string;
  qty: string;
  grid_interval: float * float;  (** min, max grid interval percentages *)
  sell_mult: string;
  min_usd_balance: string option;
  max_exposure: string option;
  strategy: string;
  maker_fee: float option;
  taker_fee: float option;
}

type logging_config = {
  level: Logging.level;
  sections: string list;
}

(** GC and memory monitor configuration *)
type gc_config = {
  (* OCaml GC settings *)
  space_overhead: int;
  max_overhead: int;
  allocation_policy: int;
  minor_heap_size_kb: int;
  major_heap_increment: int;
  (* Memory monitor settings *)
  target_heap_mb: int;
  check_interval_seconds: float;
  high_heap_mb: int;
  medium_heap_mb: int;
  high_fragmentation_percent: float;
  medium_fragmentation_percent: float;
}

let default_gc_config = {
  space_overhead = 20;
  max_overhead = 80;
  allocation_policy = 2;
  minor_heap_size_kb = 2048;
  major_heap_increment = 15;
  target_heap_mb = 50;
  check_interval_seconds = 5.0;
  high_heap_mb = 40;
  medium_heap_mb = 25;
  high_fragmentation_percent = 40.0;
  medium_fragmentation_percent = 25.0;
}

type config = {
  logging: logging_config;
  gc: gc_config;
  trading: trading_config list;
}

(** Internal section tag for logging *)
let section = "config"

(** Parse grid_interval from JSON, accepting:
    - list [min, max]
    - single number/string -> treated as (v, v) for backward compatibility *)
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

(** Parse a single trading config from JSON *)
let parse_config json =
  let open Yojson.Basic.Util in
  let symbol = json |> member "symbol" |> to_string in
  let exchange = json |> member "exchange" |> to_string_option |> Option.value ~default:"kraken" in
  {
    exchange;
    symbol;
    qty = json |> member "qty" |> to_string;
    grid_interval = parse_grid_interval json exchange symbol;
    sell_mult = json |> member "sell_mult" |> to_string_option |> Option.value ~default:"1.0";
    min_usd_balance = json |> member "min_usd_balance" |> to_string_option;
    max_exposure = json |> member "max_exposure" |> to_string_option;
    strategy = json |> member "strategy" |> to_string;
    maker_fee = json |> member "maker_fee" |> to_option to_float;
    taker_fee = json |> member "taker_fee" |> to_option to_float;
  }

(** Parse logging configuration *)
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

(** Parse GC configuration *)
let parse_gc_config json : gc_config =
  let open Yojson.Basic.Util in
  let gc_json = json |> member "gc" in
  let int_or default field =
    gc_json |> member field |> to_int_option |> Option.value ~default
  in
  let float_or default field =
    match gc_json |> member field with
    | `Float f -> f
    | `Int i -> float_of_int i
    | _ -> default
  in
  {
    space_overhead = int_or default_gc_config.space_overhead "space_overhead";
    max_overhead = int_or default_gc_config.max_overhead "max_overhead";
    allocation_policy = int_or default_gc_config.allocation_policy "allocation_policy";
    minor_heap_size_kb = int_or default_gc_config.minor_heap_size_kb "minor_heap_size_kb";
    major_heap_increment = int_or default_gc_config.major_heap_increment "major_heap_increment";
    target_heap_mb = int_or default_gc_config.target_heap_mb "target_heap_mb";
    check_interval_seconds = float_or default_gc_config.check_interval_seconds "check_interval_seconds";
    high_heap_mb = int_or default_gc_config.high_heap_mb "high_heap_mb";
    medium_heap_mb = int_or default_gc_config.medium_heap_mb "medium_heap_mb";
    high_fragmentation_percent = float_or default_gc_config.high_fragmentation_percent "high_fragmentation_percent";
    medium_fragmentation_percent = float_or default_gc_config.medium_fragmentation_percent "medium_fragmentation_percent";
  }


(** Read and parse engine configuration from config.json *)
let read_config () : config =
  try
    let json = Yojson.Basic.from_file "config.json" in
    let open Yojson.Basic.Util in
    let logging = parse_logging_config json in
    let gc = parse_gc_config json in
    let trading = json |> member "trading" |> to_list |> List.map parse_config in
    { logging; gc; trading }
  with
  | Yojson.Json_error _ | Sys_error _ -> { logging = { level = Logging.INFO; sections = [] }; gc = default_gc_config; trading = [] }
