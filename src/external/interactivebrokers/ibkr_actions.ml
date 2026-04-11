(** Order lifecycle operations.

    Place, modify, and cancel orders via the TWS API. No rate limiting
    needed — TWS API supports ~50 messages/sec, far above our needs.

    Orders are submitted via placeOrder (msg ID 3). Confirmations arrive
    asynchronously via orderStatus and openOrder callbacks handled by
    ibkr_executions_feed. *)

open Lwt.Infix

let section = "ibkr_actions"

(** Place a new order. Returns the assigned order ID.
    Resolves the contract from cache before sending. *)
let place_order conn
    ~symbol
    ~action    (* "BUY" or "SELL" *)
    ~qty
    ~order_type  (* "MKT" or "LMT" *)
    ?limit_price
    ?(tif = "DAY")
    () =
  (* Resolve contract *)
  Ibkr_contracts.resolve conn ~symbol >>= fun contract ->

  let order_id = Ibkr_connection.get_next_order_id conn in

  (* Register order→symbol mapping for execution tracking *)
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

  (* Build placeOrder message:
     [msgId=3, orderId, contract(short), secIdType, secId,
      action, totalQty, orderType, lmtPrice, auxPrice, tif, ...] *)
  let msg_fields =
    [string_of_int Ibkr_types.msg_place_order;
     string_of_int order_id]
    @ Ibkr_codec.encode_contract_short contract
    @ ["";     (* secIdType *)
       ""]     (* secId *)
    @ Ibkr_codec.encode_order order
    @ Ibkr_codec.encode_order_tail ()
  in
  Ibkr_connection.send conn msg_fields >|= fun () ->
  order_id

(** Modify an existing order. Uses placeOrder with the same orderId. *)
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
    @ ["";     (* secIdType *)
       ""]     (* secId *)
    @ Ibkr_codec.encode_order order
    @ Ibkr_codec.encode_order_tail ()
  in
  Ibkr_connection.send conn msg_fields

(** Cancel an order by ID. *)
let cancel_order conn ~order_id =
  Logging.info_f ~section "Cancelling order %d" order_id;
  Ibkr_connection.send conn [
    string_of_int Ibkr_types.msg_cancel_order;
    "1";    (* version *)
    string_of_int order_id;
    "";     (* manualCancelOrderTime *)
  ]

(** Cancel ALL open orders on the gateway. *)
let global_cancel conn =
  Logging.info_f ~section "Sending reqGlobalCancel — cancelling all open orders";
  Ibkr_connection.send conn [
    "58";   (* reqGlobalCancel msg_id *)
    "1";    (* version *)
  ]
