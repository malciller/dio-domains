(* Survival_tasks - resolve a CLI SYMBOL / --exchange into the list of analysis
   tasks to run: a single task for an explicit symbol, or one task per
   config.json "trading" entry when no symbol is given. Also maps an exchange
   name to its calendar kind. Pure, so it is unit-testable without network. *)

type task =
  { symbol : string
  ; exchange : string
  ; config : Dio_strategies.Strategy_common.trading_config
  }

let default_trading_config (exchange : string) (symbol : string)
  : Dio_strategies.Strategy_common.trading_config
  =
  { exchange
  ; symbol
  ; qty = "1.0"
  ; grid_interval = 1.0, 1.0
  ; sell_mult = "1.0"
  ; min_usd_balance = None
  ; max_exposure = None
  ; strategy = "Grid"
  ; maker_fee = None
  ; taker_fee = None
  ; testnet = false
  ; hedge = false
  ; accumulation_buffer = 0.01, 0.01
  ; data_feed = None
  }
;;

let calendar_kind_of_exchange = function
  | "kraken" -> Survival_types.Crypto
  | "hyperliquid" -> Survival_types.Crypto
  | "alpaca" -> Survival_types.Equity
  | exchange ->
    Printf.eprintf "survival: unknown exchange '%s'; assuming crypto calendar\n" exchange;
    Survival_types.Crypto
;;

let known_exchange = function
  | "kraken" | "hyperliquid" | "alpaca" -> true
  | _ -> false
;;

(** Resolve the tasks for this run. When [symbol] is empty (all-assets mode)
    every trading entry becomes a task, each on its own configured exchange.
    With a symbol, the matching config entry wins unless --exchange was given
    explicitly; unknown symbols fall back to defaults. Offline mode still
    requires a symbol for the report header. *)
let resolve_tasks
      ~(symbol : string)
      ~(exchange : string)
      ~(exchange_explicit : bool)
      ~(trading : Dio_strategies.Strategy_common.trading_config list)
      ~(offline : bool)
  : task list * (string * string) list
  =
  (* (tasks, unsupported) - [unsupported] is the list of (symbol, exchange)
     entries whose exchange cannot be used for capital survival modeling. *)
  if symbol = ""
  then
    if offline
    then failwith "offline mode (--from-csv / --from-json) requires a SYMBOL argument"
    else
      List.fold_left
        (fun (tasks, unsupported) (t : Dio_strategies.Strategy_common.trading_config) ->
           if not (known_exchange t.exchange)
           then tasks, (t.symbol, t.exchange) :: unsupported
           else
             ( { symbol = t.symbol; exchange = t.exchange; config = t } :: tasks
             , unsupported ))
        ([], [])
        trading
      |> fun (tasks, unsupported) -> List.rev tasks, List.rev unsupported
  else (
    let config =
      match
        List.find_opt
          (fun (t : Dio_strategies.Strategy_common.trading_config) ->
             String.lowercase_ascii t.symbol = String.lowercase_ascii symbol)
          trading
      with
      | Some t -> t
      | None ->
        Printf.eprintf "survival: symbol '%s' not in config.json; using defaults\n" symbol;
        default_trading_config exchange symbol
    in
    let resolved_exchange =
      if exchange_explicit
      then exchange
      else if config.exchange <> ""
      then config.exchange
      else exchange
    in
    if known_exchange resolved_exchange
    then [ { symbol; exchange = resolved_exchange; config } ], []
    else [], [ symbol, config.exchange ])
;;
