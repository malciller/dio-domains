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
  testnet: bool;
}

type logging_config = {
  level: Logging.level;
  sections: string list;
  cycle_debug_mod: int;
  cycle_info_mod: int;
}

type engine_config = {
  balance_check_mod: int;
  strategy_fallback_mod: int;
}

let default_engine_config = {
  balance_check_mod = 10000;
  strategy_fallback_mod = 10000;
}



type hedging_config = {
  source_symbol: string;
  hedge_symbol: string;
  testnet: bool;
  max_basis_bps: float;  (** Max basis tolerance in bps for hedge to fill. Default 15 bps = 0.15% *)
}

type config = {
  logging: logging_config;
  engine: engine_config;
  trading: trading_config list;
  hedging: hedging_config list;
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
  let testnet = json |> member "testnet" |> to_bool_option |> Option.value ~default:false in
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
    testnet;
  }

(** Parse logging configuration *)
let parse_logging_config json : logging_config =
  let open Yojson.Basic.Util in
  let level_str = json |> member "logging_level" |> to_string_option |> Option.value ~default:"info" in
  let sections_str = json |> member "logging_sections" |> to_string_option |> Option.value ~default:"" in
  let cycle_debug_mod = json |> member "logging_cycle_debug_mod" |> to_int_option |> Option.value ~default:1000000 in
  let cycle_info_mod = json |> member "logging_cycle_info_mod" |> to_int_option |> Option.value ~default:1000000 in
  let level = match Logging.level_of_string level_str with
    | Some lvl -> lvl
    | None -> Logging.warn_f ~section:"config" "Unknown logging level '%s', defaulting to INFO" level_str; Logging.INFO
  in
  let sections = sections_str |> String.split_on_char ',' |> List.map String.trim |> List.filter ((<>) "") in
  { level; sections; cycle_debug_mod; cycle_info_mod }

(** Parse engine configuration *)
let parse_engine_config json : engine_config =
  let open Yojson.Basic.Util in
  let engine_json = json |> member "engine" in
  let int_or default field =
    engine_json |> member field |> to_int_option |> Option.value ~default
  in
  {
    balance_check_mod = int_or default_engine_config.balance_check_mod "balance_check_mod";
    strategy_fallback_mod = int_or default_engine_config.strategy_fallback_mod "strategy_fallback_mod";
  }




(** Parse a single hedging config from JSON *)
let parse_hedging_config json : hedging_config =
  let open Yojson.Basic.Util in
  let max_basis_bps = match json |> member "max_basis_bps" with
    | `Float f -> f
    | `Int i -> float_of_int i
    | `String s -> (try float_of_string s with _ -> 15.0)
    | _ -> 15.0  (* Default: 15 bps = 0.15% *)
  in
  {
    source_symbol = json |> member "source_symbol" |> to_string;
    hedge_symbol = json |> member "hedge_symbol" |> to_string;
    testnet = json |> member "testnet" |> to_bool_option |> Option.value ~default:false;
    max_basis_bps;
  }

(** Read and parse engine configuration from config.json *)
let read_config () : config =
  try
    let json = Yojson.Basic.from_file "config.json" in
    let open Yojson.Basic.Util in
    let logging = parse_logging_config json in
    let engine = parse_engine_config json in
    let trading = json |> member "trading" |> to_list |> List.map parse_config in
    let hedging_json = json |> member "hedging" in
    let hedging = match hedging_json with
      | `Null -> []
      | _ -> hedging_json |> to_list |> List.map parse_hedging_config
    in
    { logging; engine; trading; hedging }
  with
  | Yojson.Json_error _ | Sys_error _ -> { logging = { level = Logging.INFO; sections = []; cycle_debug_mod = 1000000; cycle_info_mod = 1000000 }; engine = default_engine_config; trading = []; hedging = [] }
