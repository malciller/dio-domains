(* Oracle_tasks - resolve a CLI SYMBOL / --exchange into the analysis tasks to run: one
   task for an explicit symbol, else one task per config.json "trading" entry. Maps
   exchange names to calendar kinds. Pure; no network.

   Venue recognition is registry-first: an exchange participates by registering its
   [Exchange_intf.Oracle.S] adapter. The static fallback keeps the three built-in venues
   recognizable in pure/offline/test contexts where venue libraries are not linked (and
   thus not registered); it is never authoritative in a running binary. *)

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
  ; strategy = ""
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

(* Static fallback for the built-in venues. *)
let static_known_exchange = function
  | "kraken" | "hyperliquid" | "alpaca" -> true
  | _ -> false
;;

let static_calendar_kind = function
  | "kraken" | "hyperliquid" -> Oracle_types.Crypto
  | "alpaca" -> Oracle_types.Equity
  | _ -> Oracle_types.Crypto
;;

(** A venue is known (produces oracle tasks) when its oracle adapter is registered, or it
    is one of the built-in venues (static fallback). *)
let known_exchange exchange =
  match Exchange.Oracle.Registry.get exchange with
  | Some _ -> true
  | None -> static_known_exchange exchange
;;

(** Calendar kind of an exchange: the registered adapter's [calendar_kind] when available,
    else the static fallback (unknown exchanges warn and default to crypto). *)
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

(** Resolve this run's tasks. Empty [symbol] (all-assets mode): one task per trading entry
    on its configured exchange. With a symbol: the matching config entry wins unless
    --exchange was explicit; unknown symbols use defaults. Offline mode requires a symbol. *)
let resolve_tasks
  ~(symbol : string)
  ~(exchange : string)
  ~(exchange_explicit : bool)
  ~(trading : Dio_strategies.Strategy_common.trading_config list)
  ~(offline : bool)
  : task list * (string * string) list
  =
  (* Returns (tasks, unsupported); [unsupported] lists (symbol, exchange) entries whose
     exchange cannot model capital survival. *)
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
            { symbol = t.symbol; exchange = t.exchange; config = t } :: tasks, unsupported)
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
