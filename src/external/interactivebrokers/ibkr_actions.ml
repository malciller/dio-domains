(** Order lifecycle operations for the IBKR TWS API: placement,
    modification, and cancellation.

    Rate limiting is omitted: TWS accepts ~50 messages/second, above this
    application's throughput.

    Orders go out as placeOrder messages (msgId 3). Fills and status
    arrive asynchronously via orderStatus/openOrder and are processed by
    [Ibkr_executions_feed]. *)

open Lwt.Infix

let section = "ibkr_actions"

(** Places a new order and returns the assigned order id. Resolves the
    contract for [symbol] before sending. *)
let place_order
      conn
      ~symbol
      ~action (* BUY or SELL *)
      ~qty
      ~order_type (* "MKT", "LMT", ... *)
      ?limit_price
      ?(tif = "DAY")
      ()
  =
  Ibkr_contracts.resolve conn ~symbol
  >>= fun contract ->
  let order_id = Ibkr_connection.get_next_order_id conn in
  (* Map order id -> symbol for execution tracking. *)
  Ibkr_executions_feed.register_order ~order_id ~symbol;
  let order =
    match order_type with
    | "LMT" ->
      let price =
        match limit_price with
        | Some p -> p
        | None -> failwith "Limit price required for LMT orders"
      in
      Ibkr_types.make_limit_order ~order_id ~action ~qty ~price
    | _ -> Ibkr_types.make_market_order ~order_id ~action ~qty
  in
  let order = { order with Ibkr_types.tif } in
  Logging.info_f
    ~section
    "Placing %s %s order: %s %.2f%s (orderId=%d)"
    action
    order_type
    symbol
    qty
    (match limit_price with
     | Some p -> Printf.sprintf " @ %.4f" p
     | None -> "")
    order_id;
  (* placeOrder wire fields: msgId, orderId, short contract, secIdType,
     secId, action, totalQty, orderType, lmtPrice, auxPrice, tif. *)
  let msg_fields =
    [ string_of_int Ibkr_types.msg_place_order; string_of_int order_id ]
    @ Ibkr_codec.encode_contract_short contract
    @ [ ""; (* secIdType *) "" ] (* secId *)
    @ Ibkr_codec.encode_order order
    @ Ibkr_codec.encode_order_tail ()
  in
  Ibkr_connection.send conn msg_fields >|= fun () -> order_id
;;

(** Modifies an active order by re-sending placeOrder with the existing
    [order_id]. *)
let modify_order
      conn
      ~order_id
      ~symbol
      ~action
      ~qty
      ~order_type
      ?limit_price
      ?(tif = "GTC")
      ()
  =
  Ibkr_contracts.resolve conn ~symbol
  >>= fun contract ->
  let order =
    match order_type with
    | "LMT" ->
      let price =
        match limit_price with
        | Some p -> p
        | None -> failwith "Limit price required for LMT orders"
      in
      Ibkr_types.make_limit_order ~order_id ~action ~qty ~price
    | _ -> Ibkr_types.make_market_order ~order_id ~action ~qty
  in
  let order = { order with Ibkr_types.tif } in
  Logging.info_f
    ~section
    "Modifying order %d: %s %s %s %.2f%s"
    order_id
    action
    order_type
    symbol
    qty
    (match limit_price with
     | Some p -> Printf.sprintf " @ %.4f" p
     | None -> "");
  let msg_fields =
    [ string_of_int Ibkr_types.msg_place_order; string_of_int order_id ]
    @ Ibkr_codec.encode_contract_short contract
    @ [ ""; (* secIdType *) "" ] (* secId *)
    @ Ibkr_codec.encode_order order
    @ Ibkr_codec.encode_order_tail ()
  in
  Ibkr_connection.send conn msg_fields
;;

(** Cancels the order with the given id. *)
let cancel_order conn ~order_id =
  Logging.info_f ~section "Cancelling order %d" order_id;
  Ibkr_connection.send
    conn
    [ string_of_int Ibkr_types.msg_cancel_order
    ; "1" (* version *)
    ; string_of_int order_id
    ; "" (* manualCancelOrderTime *)
    ]
;;

(** Cancels all open orders at the gateway (reqGlobalCancel, msgId 58). *)
let global_cancel conn =
  Logging.info_f ~section "Sending reqGlobalCancel to cancel all open orders";
  Ibkr_connection.send conn [ "58"; "1" ]
;;
(* msgId; version *)
