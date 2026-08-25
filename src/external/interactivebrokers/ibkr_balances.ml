(** Account balance tracking via the IB reqAccountUpdates directive.

    Maintains available and total cash balances (settled vs unsettled
    funds) plus portfolio positions pushed by updatePortfolio.

    Workstation account values tracked:
    * TotalCashBalance: cash balance including pending settlement activity.
    * AvailableFunds: liquid capital usable for new trades.
    * BuyingPower: purchasing capacity after margin requirements.
    * NetLiquidation: net liquidation value of the portfolio.
    * GrossPositionValue: aggregate market value of all positions.
    * SettledCash: cash with completed settlement only.
    * UnrealizedPnL: mark-to-market PnL on open positions.
    * RealizedPnL: closed-out PnL for the current session.
    * ExcessLiquidity: cushion above maintenance margin. *)

let section = "ibkr_balances"

(** Account values keyed by parameter name -> (value, currency). *)
let account_values : (string, float * string) Hashtbl.t = Hashtbl.create 32

let account_values_mutex = Mutex.create ()
let ready = Atomic.make false
let ready_condition = Lwt_condition.create ()

(** Positions keyed by symbol ->
    (qty, market_price, market_value, avg_cost). *)
let positions : (string, float * float * float * float) Hashtbl.t = Hashtbl.create 32

let positions_mutex = Mutex.create ()

(** updateAccountValue handler.
    Fields: version, key, value, currency, account id. *)
let handle_account_value fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let key, fields = Ibkr_codec.read_string fields in
  let value, fields = Ibkr_codec.read_float fields in
  let currency, fields = Ibkr_codec.read_string fields in
  let _account, _fields = Ibkr_codec.read_string fields in
  (* Currency-qualified key alongside the bare key so multi-currency
     accounts do not collide; USD queries use the bare key. *)
  Mutex.lock account_values_mutex;
  Hashtbl.replace account_values key (value, currency);
  if currency <> ""
  then Hashtbl.replace account_values (key ^ "-" ^ currency) (value, currency);
  Mutex.unlock account_values_mutex;
  if not (Atomic.get ready)
  then (
    Atomic.set ready true;
    try Lwt_condition.broadcast ready_condition () with
    | _ -> ());
  (* Log every key at debug level; balances update frequently and are not
     worth info-level noise. *)
  match key with
  | "TotalCashBalance"
  | "AvailableFunds"
  | "SettledCash"
  | "NetLiquidation"
  | "BuyingPower" -> Logging.debug_f ~section "Account %s = %.2f %s" key value currency
  | "UnrealizedPnL" | "RealizedPnL" ->
    Logging.debug_f ~section "Account %s = %.2f %s" key value currency
  | _ -> Logging.debug_f ~section "Account %s = %.2f %s" key value currency
;;

(** updatePortfolio handler.
    Fields: version, contract id, symbol, sec type, expiry, strike, right,
    multiplier, primary exchange, currency, local symbol, trading class,
    position, market price, market value, avg cost, unrealized PnL,
    realized PnL, account id. *)
let handle_portfolio_value fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let _con_id, fields = Ibkr_codec.read_int fields in
  let symbol, fields = Ibkr_codec.read_string fields in
  let _sec_type, fields = Ibkr_codec.read_string fields in
  let _last_trade_date, fields = Ibkr_codec.read_string fields in
  let _strike, fields = Ibkr_codec.read_float fields in
  let _right, fields = Ibkr_codec.read_string fields in
  let _multiplier, fields = Ibkr_codec.read_string fields in
  let _primary_exchange, fields = Ibkr_codec.read_string fields in
  let _currency, fields = Ibkr_codec.read_string fields in
  let _local_symbol, fields = Ibkr_codec.read_string fields in
  let _trading_class, fields = Ibkr_codec.read_string fields in
  let position, fields = Ibkr_codec.read_float fields in
  let market_price, fields = Ibkr_codec.read_float fields in
  let market_value, fields = Ibkr_codec.read_float fields in
  let avg_cost, fields = Ibkr_codec.read_float fields in
  let unrealized_pnl, fields = Ibkr_codec.read_float fields in
  let realized_pnl, _fields = Ibkr_codec.read_float fields in
  Mutex.lock positions_mutex;
  if position = 0.0
  then Hashtbl.remove positions symbol
  else Hashtbl.replace positions symbol (position, market_price, market_value, avg_cost);
  Mutex.unlock positions_mutex;
  if position <> 0.0
  then
    Logging.debug_f
      ~section
      "Position: %s qty=%.0f mktPrice=%.2f mktValue=%.2f avgCost=%.2f uPnL=%.2f rPnL=%.2f"
      symbol
      position
      market_price
      market_value
      avg_cost
      unrealized_pnl
      realized_pnl
;;

(** Registers account/portfolio handlers. *)
let register_handlers () =
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_account_value
    ~handler:handle_account_value;
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_portfolio_value
    ~handler:handle_portfolio_value
;;

(** Subscribes to account updates for [account_id]. *)
let subscribe conn ~account_id =
  Logging.info_f ~section "Subscribing to account updates for %s" account_id;
  Ibkr_connection.send
    conn
    [ string_of_int Ibkr_types.msg_req_account_updates
    ; "2"
    ; (* version *)
      "1"
    ; (* subscribe = true *)
      account_id
    ]
;;

(** Account value by key; 0.0 when absent. *)
let[@inline always] get_account_value key =
  Mutex.lock account_values_mutex;
  let r =
    match Hashtbl.find_opt account_values key with
    | Some (v, _) -> v
    | None -> 0.0
  in
  Mutex.unlock account_values_mutex;
  r
;;

(** Total cash including pending settlement. *)
let get_total_cash () = get_account_value "TotalCashBalance"

(** Funds available for new trades; caps direct asset purchases. *)
let get_available_funds () = get_account_value "AvailableFunds"

(** Cash with settlement complete. *)
let get_settled_cash () = get_account_value "SettledCash"

(** Purchasing capacity after margin requirements. *)
let get_buying_power () = get_account_value "BuyingPower"

(** Net liquidation value of the portfolio. *)
let get_net_liquidation () = get_account_value "NetLiquidation"

(** Mark-to-market PnL on open positions. *)
let get_unrealized_pnl () = get_account_value "UnrealizedPnL"

(** Maps an asset name to a balance query:
    * USD -> AvailableFunds (deployable capital)
    * TOTAL_CASH -> TotalCashBalance
    * SETTLED -> SettledCash
    * NET_LIQ -> NetLiquidation
    Other names fall back to the positions table (equities), then to a
    raw account-value key lookup. *)
let get_balance ~asset =
  match asset with
  | "USD" -> get_available_funds ()
  | "TOTAL_CASH" -> get_total_cash ()
  | "SETTLED" -> get_settled_cash ()
  | "NET_LIQ" -> get_net_liquidation ()
  | "BUYING_POWER" -> get_buying_power ()
  | symbol ->
    (* Equities: look up the positions table first, then account values
         as a fallback for unmapped keys. *)
    Mutex.lock positions_mutex;
    let r = Hashtbl.find_opt positions symbol in
    Mutex.unlock positions_mutex;
    (match r with
     | Some (qty, _, _, _) -> qty
     | None -> get_account_value symbol)
;;

(** Balance summary for supervisor asset monitoring. IBKR accounts are keyed
    by internal parameter names rather than tradable assets, so this
    surfaces only USD (mapped to AvailableFunds); individual positions are
    tracked via updatePortfolio. *)
let get_all_balances () =
  let usd = get_available_funds () in
  if usd > 0.0 then [ "USD", usd ] else []
;;

(** Position tuple (qty, market price, market value, avg cost) for [symbol]. *)
let get_position ~symbol =
  Mutex.lock positions_mutex;
  let r = Hashtbl.find_opt positions symbol in
  Mutex.unlock positions_mutex;
  r
;;

(** Registers handlers; safe to call once at startup. *)
let initialize () =
  register_handlers ();
  Logging.info ~section "Balances module initialized"
;;
