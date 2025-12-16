(** Generic Exchange Interface definition *)

(** Common types used across exchange implementations *)
module Types = struct
  type order_type =
    | Limit
    | Market
    | StopLoss
    | TakeProfit
    | StopLossLimit
    | TakeProfitLimit
    | SettlPosition
    | Other of string

  type order_side =
    | Buy
    | Sell

  type time_in_force =
    | GTC
    | IOC
    | FOK

  (** Result of order placement *)
  type add_order_result = {
    order_id: string;
    cl_ord_id: string option;
    order_userref: int option;
  }

  type order_status =
    | Pending
    | New
    | PartiallyFilled
    | Filled
    | Canceled
    | Expired
    | Rejected
    | Unknown of string

  (** Open order details *)
  type open_order = {
    order_id: string;
    symbol: string;
    side: order_side;
    qty: float;
    cum_qty: float;
    remaining_qty: float;
    limit_price: float option;
    status: order_status;
    user_ref: int option;
    cl_ord_id: string option;
  }

  (** Result of order amendment *)
  type amend_order_result = {
    amend_id: string;
    order_id: string;
    cl_ord_id: string option;
  }

  (** Result of order cancellation *)
  type cancel_order_result = {
    order_id: string;
    cl_ord_id: string option;
  }

  (** Ticker event data *)
  type ticker_event = {
    bid: float;
    ask: float;
    timestamp: float;
  }

  (** Orderbook update event data *)
  type orderbook_event = {
    bids: (float * float) array; (* price, size *)
    asks: (float * float) array; (* price, size *)
    timestamp: float;
  }

  (** Execution report/Order update event *)
  type execution_event = {
    order_id: string;
    order_status: order_status;
    limit_price: float option;
    side: order_side;
    remaining_qty: float;
    filled_qty: float;
    timestamp: float;
  }

  (** Configuration for retrying operations *)
  type retry_config = {
    max_attempts: int;
    base_delay_ms: float;
    max_delay_ms: float;
    backoff_factor: float;
  }
end

(** Signature that all exchange modules must implement *)
module type S = sig
  val name : string

  (** Place a new order *)
  val place_order :
    token:string ->
    order_type:Types.order_type ->
    side:Types.order_side ->
    qty:float ->
    symbol:string ->
    ?limit_price:float ->
    ?time_in_force:Types.time_in_force ->
    ?post_only:bool ->
    ?reduce_only:bool ->
    ?order_userref:int ->
    ?cl_ord_id:string ->
    ?trigger_price:float ->
    ?display_qty:float ->
    ?retry_config:Types.retry_config ->
    unit ->
    (Types.add_order_result, string) result Lwt.t

  (** Amend an existing order *)
  val amend_order :
    token:string ->
    order_id:string ->
    ?cl_ord_id:string ->
    ?qty:float ->
    ?limit_price:float ->
    ?post_only:bool ->
    ?trigger_price:float ->
    ?display_qty:float ->
    ?symbol:string ->
    ?retry_config:Types.retry_config ->
    unit ->
    (Types.amend_order_result, string) result Lwt.t

  (** Cancel orders *)
  val cancel_orders :
    token:string ->
    ?order_ids:string list ->
    ?cl_ord_ids:string list ->
    ?order_userrefs:int list ->
    ?retry_config:Types.retry_config ->
    unit ->
    (Types.cancel_order_result list, string) result Lwt.t

  (** Market Data Access *)
  
  (** Get current ticker (Best Bid, Best Ask) *)
  val get_ticker : symbol:string -> (float * float) option

  (** Get top of orderbook (Bid Price, Bid Size, Ask Price, Ask Size) *)
  val get_top_of_book : symbol:string -> (float * float * float * float) option

  (** Get current asset balance *)
  val get_balance : asset:string -> float

  (** Get details of a specific open order *)
  val get_open_order : symbol:string -> order_id:string -> Types.open_order option

  (** Get all open orders for a symbol *)
  val get_open_orders : symbol:string -> Types.open_order list

  (** Get Position for Ticker Feed *)
  val get_ticker_position : symbol:string -> int

  (** Read Ticker Events from specific position *)
  val read_ticker_events : symbol:string -> start_pos:int -> Types.ticker_event list

  (** Get Position for Orderbook Feed *)
  val get_orderbook_position : symbol:string -> int

  (** Read Orderbook Events from specific position *)
  val read_orderbook_events : symbol:string -> start_pos:int -> Types.orderbook_event list

  (** Get position for consuming execution feed *)
  val get_execution_feed_position : symbol:string -> int

  (** Read Execution Events from specific position *)
  val read_execution_events : symbol:string -> start_pos:int -> Types.execution_event list

  (** Metadata Access *)
  
  (** Get price increment (tick size) for a symbol *)
  val get_price_increment : symbol:string -> float option

  (** Get quantity increment (step size) for a symbol *)
  val get_qty_increment : symbol:string -> float option

  (** Get minimum order quantity for a symbol *)
  val get_qty_min : symbol:string -> float option

  (** Get maker/taker fees for a symbol (if cached by exchange) *)
  val get_fees : symbol:string -> (float option * float option)
end

(** Registry for managing multiple exchange implementations *)
module Registry = struct
  let _exchanges : (string, (module S)) Hashtbl.t = Hashtbl.create 4

  (** Register an exchange module *)
  let register (module Exchange : S) =
    Hashtbl.replace _exchanges Exchange.name (module Exchange)

  (** Get an exchange module by name *)
  let get name =
    Hashtbl.find_opt _exchanges name
end
