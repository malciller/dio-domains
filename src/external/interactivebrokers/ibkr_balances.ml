(** Account balance tracking mechanism utilizing the reqAccountUpdates directive from the Interactive Brokers API.

    This module maintains the programmatic state for both available and total cash balances, which provides the capacity to distinguish between settled operational funds and unsettled funds pending the standard clearance cycle.

    Detailed Interactive Brokers Workstation account parameters monitored:
    * TotalCashBalance: Aggregate cash balance inclusive of pending settlement transactions.
    * AvailableFunds: Liquid capital cleared for deployment in new trade executions.
    * BuyingPower: Aggregate purchasing capacity adjusted against account margin requirements.
    * NetLiquidation: Calculated net liquidation value of the portfolio.
    * GrossPositionValue: Aggregate current market valuation across all outstanding positions.
    * SettledCash: Cleared capital restricted strictly to completed settlement finalities.
    * UnrealizedPnL: Unsecured profit and loss aggregated over all open positions.
    * RealizedPnL: Secured profit and loss aggregated for the current active trading session.
    * ExcessLiquidity: Available clearance cushion above the strict maintenance margin threshold. *)

let section = "ibkr_balances"

(** Primary memory store for account metrics mapping parameter identifiers to a tuple containing the floating point magnitude and currency designation string. *)
let account_values : (string, float * string) Hashtbl.t = Hashtbl.create 32
let account_values_mutex = Mutex.create ()
let ready = Atomic.make false
let ready_condition = Lwt_condition.create ()

(** Primary memory store for portfolio positions mapping the instrument symbol string to a tuple representing quantity, current market quotation, calculated market value, and volume weighted average cost. *)
let positions : (string, float * float * float * float) Hashtbl.t = Hashtbl.create 32
let positions_mutex = Mutex.create ()

(** Dispatch handler for the updateAccountValue network callback.
    Expected data structure fields: protocol version, parameter key, numeric value, currency denomination string, and target account identifier. *)
let handle_account_value fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let key, fields = Ibkr_codec.read_string fields in
  let value, fields = Ibkr_codec.read_float fields in
  let currency, fields = Ibkr_codec.read_string fields in
  let _account, _fields = Ibkr_codec.read_string fields in

  (* Index payload using a currency qualified string key to support multiple currency environments, while preserving the baseline key index for default USD queries. *)
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

  (* Emit telemetry regarding significant account balance parametric shifts at the informational severity level. *)
  match key with
  | "TotalCashBalance" | "AvailableFunds" | "SettledCash"
  | "NetLiquidation" | "BuyingPower" ->
      Logging.info_f ~section "Account %s = %.2f %s" key value currency
  | "UnrealizedPnL" | "RealizedPnL" ->
      Logging.debug_f ~section "Account %s = %.2f %s" key value currency
  | _ ->
      Logging.debug_f ~section "Account %s = %.2f %s" key value currency

(** Dispatch handler for the updatePortfolio network callback delivering position level modifications.
    Expected data structure fields: protocol version, contract identifier, primary symbol string, security classification type, expiration or last trade indicator, strike price, option right classification, contract multiplier, primary venue exchange, currency denomination, localized symbol designation, proprietary trading class, net position quantity, current market quotation, calculated market value, volume weighted average cost, unsecured profit and loss, secured profit and loss, and target account identifier. *)
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

(** Execute registration sequence for ingress payload handlers and initialize subscriptions for account level metrics. *)
let register_handlers () =
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_account_value
    ~handler:handle_account_value;
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_portfolio_value
    ~handler:handle_portfolio_value

(** Transmit subscription request payload for account state parameter updates targeted at the specified account identifier string. *)
let subscribe conn ~account_id =
  Logging.info_f ~section "Subscribing to account updates for %s" account_id;
  Ibkr_connection.send conn [
    string_of_int Ibkr_types.msg_req_account_updates;
    "2";    (* Protocol structure version *)
    "1";    (* Subscription boolean flag equivalent to true *)
    account_id;
  ]

(* Public structural access procedures *)

(** Access specific account parameter value by string key index. Resolves to a zero component float magnitude if the parameter is not present in the primary memory store. *)
let[@inline always] get_account_value key =
  Mutex.lock account_values_mutex;
  let r = match Hashtbl.find_opt account_values key with
    | Some (v, _) -> v
    | None -> 0.0
  in
  Mutex.unlock account_values_mutex;
  r

(** Retrieve aggregate cash balance including pending settlement allocations. *)
let get_total_cash () = get_account_value "TotalCashBalance"

(** Retrieve cleared liquid capital available for immediate deployment in trade executions. This parameter determines the absolute upper bound for direct asset acquisition configurations. *)
let get_available_funds () = get_account_value "AvailableFunds"

(** Retrieve cleared cash parameter filtering out any pending settlement transaction allocations. *)
let get_settled_cash () = get_account_value "SettledCash"

(** Retrieve aggregate purchasing capacity adjusted against account margin requirements. *)
let get_buying_power () = get_account_value "BuyingPower"

(** Retrieve the calculated net liquidation valuation of the comprehensive portfolio. *)
let get_net_liquidation () = get_account_value "NetLiquidation"

(** Retrieve the unsecured profit and loss parametric dimension aggregated across all positional allocations. *)
let get_unrealized_pnl () = get_account_value "UnrealizedPnL"

(** Interrogate the balance query interface for a specific normalized asset designation.
    Translates fundamental system nomenclature into respective Interactive Brokers Workstation metric keys:
    * USD translates to AvailableFunds which defines deployable operational capital.
    * TOTAL_CASH translates to TotalCashBalance.
    * SETTLED translates to SettledCash.
    * NET_LIQ translates to NetLiquidation.
    * Unmapped parameters execute a direct key lookup against the primary account values memory store. *)
let get_balance ~asset =
  match asset with
  | "USD" -> get_available_funds ()
  | "TOTAL_CASH" -> get_total_cash ()
  | "SETTLED" -> get_settled_cash ()
  | "NET_LIQ" -> get_net_liquidation ()
  | "BUYING_POWER" -> get_buying_power ()
  | symbol ->
      (* For equity instruments, query the positions memory store populated via the updatePortfolio callback mechanism.
         Implement a secondary fallback to the account values memory store for unmapped operational parameters. *)
      (Mutex.lock positions_mutex;
       let r = Hashtbl.find_opt positions symbol in
       Mutex.unlock positions_mutex;
       match r with
       | Some (qty, _, _, _) -> qty
       | None -> get_account_value symbol)

(** Construct an aggregated balance payload formatted for compatibility with the supervisor inactive asset monitoring sequence. Interactive Brokers account states leverage internal parametric keys rather than tradable external asset designations. This interface strictly surfaces the USD identifier mapped to AvailableFunds metrics. Component positions are independently tracked via the updatePortfolio mechanism. *)
let get_all_balances () =
  let usd = get_available_funds () in
  if usd > 0.0 then [("USD", usd)] else []

(** Access the internal position parameters for a provided symbol string, returning an unwrapped tuple representing the numeric quantity, current market quotation, calculated market value, and volume weighted average cost parameters. *)
let get_position ~symbol =
  Mutex.lock positions_mutex;
  let r = Hashtbl.find_opt positions symbol in
  Mutex.unlock positions_mutex;
  r

(** Execute the structural module initialization sequence and mount required network dispatch payload listener targets. *)
let initialize () =
  register_handlers ();
  Logging.info ~section "Balances module initialized"
