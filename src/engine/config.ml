(** Trading configuration type *)
type trading_config = {
  exchange: string;
  symbol: string;
  qty: string;
  grid_interval: string;
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

type metrics_broadcast_config = {
  port: int;
  enable_token_auth: bool;
  enable_ip_whitelist: bool;
  token: string option;
  ip_whitelist: string list option;
}

type config = {
  logging: logging_config;
  trading: trading_config list;
  metrics_broadcast: metrics_broadcast_config;
}

(** Parse a single trading config from JSON *)
let parse_config json =
  let open Yojson.Basic.Util in
  let symbol = json |> member "symbol" |> to_string in
  let exchange = json |> member "exchange" |> to_string_option |> Option.value ~default:"kraken" in
  {
    exchange;
    symbol;
    qty = json |> member "qty" |> to_string;
    grid_interval = json |> member "grid_interval" |> to_string_option |> Option.value ~default:"1.0";
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

(** Parse metrics broadcast configuration *)
let parse_metrics_broadcast_config json : metrics_broadcast_config =
  let open Yojson.Basic.Util in
  let port = json |> member "port" |> to_int_option |> Option.value ~default:8080 in
  let enable_token_auth = json |> member "enable_token_auth" |> to_bool_option |> Option.value ~default:false in
  let enable_ip_whitelist = json |> member "enable_ip_whitelist" |> to_bool_option |> Option.value ~default:false in

  (* Load .env file for authentication variables *)
  let _ = try Dotenv.export ~path:".env" (); () with _ -> () in

  (* Parse token if enabled *)
  let token =
    if enable_token_auth then
      match Sys.getenv_opt "ALLOWED_CLIENT_TOKEN" with
      | Some token when token <> "" -> Some token
      | _ ->
          Logging.error_f ~section:"config" "Token authentication enabled but ALLOWED_CLIENT_TOKEN environment variable is missing or empty";
          raise (Failure "ALLOWED_CLIENT_TOKEN environment variable required when enable_token_auth is true")
    else
      None
  in

  (* Parse IP whitelist if enabled *)
  let ip_whitelist =
    if enable_ip_whitelist then
      match Sys.getenv_opt "ALLOWED_CLIENT_IP" with
      | Some ip_list when ip_list <> "" ->
          let entries = ip_list
            |> String.split_on_char ','
            |> List.map String.trim
            |> List.filter ((<>) "")
            |> List.map (fun s -> s |> String.split_on_char '\n' |> List.map String.trim |> List.filter ((<>) ""))
            |> List.flatten
            |> List.filter ((<>) "")
          in
          if entries = [] then (
            Logging.error_f ~section:"config" "IP whitelist enabled but ALLOWED_CLIENT_IP environment variable contains no valid entries";
            raise (Failure "ALLOWED_CLIENT_IP environment variable must contain at least one entry when enable_ip_whitelist is true")
          ) else (
            Some entries
          )
      | _ ->
          Logging.error_f ~section:"config" "IP whitelist enabled but ALLOWED_CLIENT_IP environment variable is missing or empty";
          raise (Failure "ALLOWED_CLIENT_IP environment variable required when enable_ip_whitelist is true")
    else
      None
  in

  { port; enable_token_auth; enable_ip_whitelist; token; ip_whitelist }

(** Read and parse engine configuration from config.json *)
let read_config () : config =
  try
    let json = Yojson.Basic.from_file "config.json" in
    let open Yojson.Basic.Util in
    let logging = parse_logging_config json in
    let trading = json |> member "trading" |> to_list |> List.map parse_config in
    let metrics_broadcast = json |> member "metrics_broadcast" |> parse_metrics_broadcast_config in
    { logging; trading; metrics_broadcast }
  with
  | Yojson.Json_error _ | Sys_error _ -> { logging = { level = Logging.INFO; sections = [] }; trading = []; metrics_broadcast = { port = 8080; enable_token_auth = false; enable_ip_whitelist = false; token = None; ip_whitelist = None } }
