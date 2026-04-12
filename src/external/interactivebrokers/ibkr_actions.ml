(** Provides order lifecycle operations for the Interactive Brokers TWS API.

    This module handles the placement, modification, and cancellation of trading orders.
    Rate limiting is explicitly omitted because the TWS API natively supports approximately
    50 messages per second, which exceeds the throughput requirements of this application.

    Orders are transmitted to the gateway using the placeOrder message protocol consisting of message ID 3.
    Execution and status confirmations are received asynchronously via orderStatus and openOrder callbacks,
    which are subsequently processed by the ibkr_executions_feed module. *)

open Lwt.Infix

let section = "ibkr_actions"

(** Places a new order on the exchange and returns the internal sequence order ID assigned to it.
    The function performs a cache resolution for the financial contract prior to message dispatch. *)
let place_order conn
    ~symbol
    ~action    (* Action specifies the transaction type, typically BUY or SELL *)
    ~qty
    ~order_type  (* Specifies the execution strategy, such as MKT for market or LMT for limit *)
    ?limit_price
    ?(tif = "DAY")
    () =
  (* Resolve the applicable contract definition using the provided symbol *)
  Ibkr_contracts.resolve conn ~symbol >>= fun contract ->

  let order_id = Ibkr_connection.get_next_order_id conn in

  (* Register the association between the new order ID and the symbol to enable execution tracking *)
  Ibkr_executions_feed.register_order ~order_id ~symbol;

  let order = match order_type with
    | "LMT" ->
        let price = match limit_price with
          | Some p -> p
          | None -> failwith "Limit price required for LMT orders"
        in
        Ibkr_types.make_limit_order ~order_id ~action ~qty ~price
    | _ ->
        Ibkr_types.make_market_order ~order_id ~action ~qty
  in
  let order = { order with Ibkr_types.tif } in

  Logging.info_f ~section "Placing %s %s order: %s %.2f%s (orderId=%d)"
    action order_type symbol qty
    (match limit_price with Some p -> Printf.sprintf " @ %.4f" p | None -> "")
    order_id;

  (* Construct the binary format placeOrder outbound message buffer.
     The sequence comprises msgId equals 3, orderId, the short form contract definition,
     secIdType, secId, action, totalQty, orderType, lmtPrice, auxPrice, and tif fields. *)
  let msg_fields =
    [string_of_int Ibkr_types.msg_place_order;
     string_of_int order_id]
    @ Ibkr_codec.encode_contract_short contract
    @ ["";     (* Empty secIdType parameter *)
       ""]     (* Empty secId parameter *)
    @ Ibkr_codec.encode_order order
    @ Ibkr_codec.encode_order_tail ()
  in
  Ibkr_connection.send conn msg_fields >|= fun () ->
  order_id

(** Modifies an active order residing on the exchange.
    This operation utilizes the placeOrder message protocol with an existing orderId. *)
let modify_order conn
    ~order_id
    ~symbol
    ~action
    ~qty
    ~order_type
    ?limit_price
    ?(tif = "GTC")
    () =
  Ibkr_contracts.resolve conn ~symbol >>= fun contract ->

  let order = match order_type with
    | "LMT" ->
        let price = match limit_price with
          | Some p -> p
          | None -> failwith "Limit price required for LMT orders"
        in
        Ibkr_types.make_limit_order ~order_id ~action ~qty ~price
    | _ ->
        Ibkr_types.make_market_order ~order_id ~action ~qty
  in
  let order = { order with Ibkr_types.tif } in

  Logging.info_f ~section "Modifying order %d: %s %s %s %.2f%s"
    order_id action order_type symbol qty
    (match limit_price with Some p -> Printf.sprintf " @ %.4f" p | None -> "");

  let msg_fields =
    [string_of_int Ibkr_types.msg_place_order;
     string_of_int order_id]
    @ Ibkr_codec.encode_contract_short contract
    @ ["";     (* Empty secIdType parameter *)
       ""]     (* Empty secId parameter *)
    @ Ibkr_codec.encode_order order
    @ Ibkr_codec.encode_order_tail ()
  in
  Ibkr_connection.send conn msg_fields

(** Transmits a cancellation request for a specific order identified by its assigned order ID. *)
let cancel_order conn ~order_id =
  Logging.info_f ~section "Cancelling order %d" order_id;
  Ibkr_connection.send conn [
    string_of_int Ibkr_types.msg_cancel_order;
    "1";    (* Message protocol version identifier *)
    string_of_int order_id;
    "";     (* Omitted manualCancelOrderTime parameter *)
  ]

(** Initiates a global cancellation request to terminate all currently open orders residing on the gateway. *)
let global_cancel conn =
  Logging.info_f ~section "Sending reqGlobalCancel to cancel all open orders";
  Ibkr_connection.send conn [
    "58";   (* Identifies the reqGlobalCancel message operation *)
    "1";    (* Message protocol version identifier *)
  ]
