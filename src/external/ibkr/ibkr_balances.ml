(** Account balance tracking via reqAccountUpdates.

    Tracks both available and total cash to distinguish between
    settled funds (available to trade) and unsettled funds (T+1 for ETFs).

    Key TWS account values tracked:
    - TotalCashBalance:       Total cash including unsettled
    - AvailableFunds:         Cash available for new trades (settled)
    - BuyingPower:            Total buying power (margin-adjusted)
    - NetLiquidation:         Net liquidation value of the account
    - GrossPositionValue:     Total market value of all positions
    - SettledCash:            Cash from settled trades only
    - UnrealizedPnL:          Unrealized P&L across all positions
    - RealizedPnL:            Realized P&L for the session
    - ExcessLiquidity:        Cushion above maintenance margin *)

let section = "ibkr_balances"

(** Account value store: key → (value, currency). *)
let account_values : (string, float * string) Hashtbl.t = Hashtbl.create 32
let account_values_mutex = Mutex.create ()
let ready = Atomic.make false
let ready_condition = Lwt_condition.create ()

(** Position store: symbol → (position, market_price, market_value, avg_cost). *)
let positions : (string, float * float * float * float) Hashtbl.t = Hashtbl.create 32
let positions_mutex = Mutex.create ()

(** Handle updateAccountValue callback.
    Fields: version, key, value, currency, accountName *)
let handle_account_value fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let key, fields = Ibkr_codec.read_string fields in
  let value, fields = Ibkr_codec.read_float fields in
  let currency, fields = Ibkr_codec.read_string fields in
  let _account, _fields = Ibkr_codec.read_string fields in

  (* Store with currency-qualified key for multi-currency support,
     but also store the base key for USD-only access *)
  Mutex.lock account_values_mutex;
  Hashtbl.replace account_values key (value, currency);
  if currency <> "" then
    Hashtbl.replace account_values (key ^ "-" ^ currency) (value, currency);
  Mutex.unlock account_values_mutex;

  if not (Atomic.get ready) then begin
    Atomic.set ready true;
    (try Lwt_condition.broadcast ready_condition ()
     with _ -> ())
  end;

  (* Log important balance changes at info level *)
  match key with
  | "TotalCashBalance" | "AvailableFunds" | "SettledCash"
  | "NetLiquidation" | "BuyingPower" ->
      Logging.info_f ~section "Account %s = %.2f %s" key value currency
  | "UnrealizedPnL" | "RealizedPnL" ->
      Logging.debug_f ~section "Account %s = %.2f %s" key value currency
  | _ ->
      Logging.debug_f ~section "Account %s = %.2f %s" key value currency

(** Handle updatePortfolio callback (per-position updates).
    Fields: version, conId, symbol, secType, lastTradeDateOrContractMonth,
            strike, right, multiplier, primaryExchange, currency,
            localSymbol, tradingClass, position, marketPrice, marketValue,
            averageCost, unrealizedPNL, realizedPNL, accountName *)
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
  if position = 0.0 then
    Hashtbl.remove positions symbol
  else
    Hashtbl.replace positions symbol (position, market_price, market_value, avg_cost);
  Mutex.unlock positions_mutex;

  if position <> 0.0 then
    Logging.debug_f ~section "Position: %s qty=%.0f mktPrice=%.2f mktValue=%.2f avgCost=%.2f uPnL=%.2f rPnL=%.2f"
      symbol position market_price market_value avg_cost unrealized_pnl realized_pnl

(** Register handlers and subscribe to account updates. *)
let register_handlers () =
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_account_value
    ~handler:handle_account_value;
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_portfolio_value
    ~handler:handle_portfolio_value

(** Subscribe to account updates for [account_id]. *)
let subscribe conn ~account_id =
  Logging.info_f ~section "Subscribing to account updates for %s" account_id;
  Ibkr_connection.send conn [
    string_of_int Ibkr_types.msg_req_account_updates;
    "2";    (* version *)
    "1";    (* subscribe = true *)
    account_id;
  ]

(* ---- Public accessors ---- *)

(** Get a specific account value by key. Returns 0.0 if not found. *)
let[@inline always] get_account_value key =
  Mutex.lock account_values_mutex;
  let r = match Hashtbl.find_opt account_values key with
    | Some (v, _) -> v
    | None -> 0.0
  in
  Mutex.unlock account_values_mutex;
  r

(** Get total cash balance (includes unsettled funds). *)
let get_total_cash () = get_account_value "TotalCashBalance"

(** Get available funds (settled cash available to trade).
    This is the key value for determining how much you can actually buy. *)
let get_available_funds () = get_account_value "AvailableFunds"

(** Get settled cash only (excludes pending settlements). *)
let get_settled_cash () = get_account_value "SettledCash"

(** Get buying power (margin-adjusted). *)
let get_buying_power () = get_account_value "BuyingPower"

(** Get net liquidation value. *)
let get_net_liquidation () = get_account_value "NetLiquidation"

(** Get unrealized P&L. *)
let get_unrealized_pnl () = get_account_value "UnrealizedPnL"

(** Get the balance for a specific asset.
    Maps common asset names to TWS account value keys:
    - "USD" → AvailableFunds (what you can actually trade with)
    - "TOTAL_CASH" → TotalCashBalance
    - "SETTLED" → SettledCash
    - "NET_LIQ" → NetLiquidation
    - Other → looked up directly in account_values *)
let get_balance ~asset =
  match asset with
  | "USD" -> get_available_funds ()
  | "TOTAL_CASH" -> get_total_cash ()
  | "SETTLED" -> get_settled_cash ()
  | "NET_LIQ" -> get_net_liquidation ()
  | "BUYING_POWER" -> get_buying_power ()
  | other -> get_account_value other

(** Get all balances in a form compatible with the supervisor's
    non-active asset monitor. IBKR account values are internal TWS keys
    (e.g. RegTEquity, AvailableFunds), NOT tradeable asset names.
    We only expose USD (available funds) here. Positions are tracked
    separately via updatePortfolio. *)
let get_all_balances () =
  let usd = get_available_funds () in
  if usd > 0.0 then [("USD", usd)] else []

(** Get position for a symbol: (qty, market_price, market_value, avg_cost). *)
let get_position ~symbol =
  Mutex.lock positions_mutex;
  let r = Hashtbl.find_opt positions symbol in
  Mutex.unlock positions_mutex;
  r

(** Initialize and register handlers. *)
let initialize () =
  register_handlers ();
  Logging.info ~section "Balances module initialized"
