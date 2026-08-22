(* Oracle_tasks - resolve a CLI SYMBOL / --exchange into the list of analysis
   tasks to run: a single task for an explicit symbol, or one task per
   config.json "trading" entry when no symbol is given. Also maps an exchange
   name to its calendar kind. Pure, so it is unit-testable without network.

   Venue recognition is REGISTRY-FIRST: an exchange participates in oracle
   modeling by registering its [Exchange_intf.Oracle.S] adapter (see
   Oracle_fetch / Oracle_venues). The static fallback below keeps the three
   built-in venues recognizable in pure/offline/test contexts where the
   venue libraries are not linked (and thus not registered); it is never the
   authoritative source in a running binary. *)

module Exchange = Dio_exchange.Exchange_intf

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
  ; strategy = "Ladder"
  ; maker_fee = None
  ; taker_fee = None
  ; testnet = false
  ; hedge = false
  ; accumulation_buffer = 0.01, 0.01
  ; data_feed = None
  ; base_accumulation = true
  ; sell_levels = true
  }
;;

(* Static fallback for the built-in venues (see the module doc). *)
let static_known_exchange = function
  | "kraken" | "hyperliquid" | "alpaca" -> true
  | _ -> false
;;

let static_calendar_kind = function
  | "kraken" | "hyperliquid" -> Oracle_types.Crypto
  | "alpaca" -> Oracle_types.Equity
  | _ -> Oracle_types.Crypto
;;

(** A venue is known (produces oracle tasks) when its oracle adapter is
    registered, or it is one of the built-in venues (static fallback). *)
let known_exchange exchange =
  match Exchange.Oracle.Registry.get exchange with
  | Some _ -> true
  | None -> static_known_exchange exchange
;;

(** Calendar kind of an exchange: the registered adapter's [calendar_kind]
    when available, else the static fallback (unknown exchanges warn and
    default to crypto). *)
let calendar_kind_of_exchange exchange =
  match Exchange.Oracle.Registry.get exchange with
  | Some (module V) -> V.calendar_kind
  | None ->
    if static_known_exchange exchange
    then static_calendar_kind exchange
    else (
      Printf.eprintf "oracle: unknown exchange '%s'; assuming crypto calendar\n" exchange;
      Oracle_types.Crypto)
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
        Printf.eprintf "oracle: symbol '%s' not in config.json; using defaults\n" symbol;
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
