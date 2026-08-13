(* Suicide Grid - Order Construction & Buffer Management *)

open Strategy_common
open Suicide_grid_types
open Suicide_grid_config

(** Shared order ringbuffer across all strategy domains. *)
let order_buffer = LockFreeQueue.create ()

(** Accessor for the shared order ringbuffer. *)
let get_order_buffer () = order_buffer

let create_place_order dup_key asset_symbol side qty price post_only strategy exchange =
  let ecfg = get_exchange_config exchange in
  { operation = Place
  ; order_id = None
  ; symbol = asset_symbol
  ; exchange
  ; side
  ; order_type = "limit"
  ; qty
  ; price
  ; time_in_force = ecfg.time_in_force
  ; post_only
  ; userref = Some strategy_userref_grid
  ; strategy
  ; duplicate_key = dup_key
  }
;;

(** Constructs an Amend strategy_order targeting [order_id]. *)
let create_amend_order order_id asset_symbol side qty price post_only strategy exchange =
  let ecfg = get_exchange_config exchange in
  { operation = Amend
  ; order_id = Some order_id
  ; symbol = asset_symbol
  ; exchange
  ; side
  ; order_type = "limit"
  ; qty
  ; price
  ; time_in_force = ecfg.time_in_force
  ; post_only
  ; userref = None
  ; strategy
  ; duplicate_key = ""
  }
;;

(** Constructs a Cancel strategy_order targeting [order_id]. *)
let create_cancel_order order_id asset_symbol strategy exchange =
  { operation = Cancel
  ; order_id = Some order_id
  ; symbol = asset_symbol
  ; exchange
  ; side = Buy
  ; order_type = "limit"
  ; qty = 0.0
  ; price = None
  ; time_in_force = "GTC"
  ; post_only = false
  ; userref = None
  ; strategy
  ; duplicate_key = ""
  }
;;

(** Backwards-compatible order constructor. Delegates to create_place_order with Grid strategy. *)
let create_order dup_key asset_symbol side qty price post_only exchange =
  create_place_order dup_key asset_symbol side qty price post_only Grid exchange
;;

(** Pushes an order to the ringbuffer. Returns true on success, false on duplicate or full buffer. *)
let push_order ~now ?state order =
  let operation_str =
    match order.operation with
    | Place -> "place"
    | Amend -> "amend"
    | Cancel -> "cancel"
  in
  match order.operation with
  | Cancel ->
    let state =
      match state with
      | Some s -> s
      | None -> get_strategy_state order.symbol
    in
    (match order.order_id with
     | Some _ ->
       let write_result = LockFreeQueue.write order_buffer order in
       (match write_result with
        | Some () ->
          OrderSignal.broadcast ();
          Order_actions.incr order.symbol;
          state.last_order_time <- now;
          if order.side = Buy then state.inflight_cancel_buy <- true;
          true
        | None ->
          Logging.warn_f
            ~section
            "Order ringbuffer full, dropped %s %s order for %s"
            operation_str
            (string_of_order_side order.side)
            order.symbol;
          false)
     | None ->
       Logging.warn_f ~section "Cancel operation missing order_id for %s" order.symbol;
       false)
  | _ ->
    let is_duplicate =
      match order.operation with
      | Place -> not (InFlightOrders.add_in_flight_order order.duplicate_key)
      | Amend ->
        (match order.order_id with
         | Some oid -> not (InFlightAmendments.add_in_flight_amendment oid)
         | None -> false)
      | _ -> false
    in
    if is_duplicate
    then false
    else (
      let write_result = LockFreeQueue.write order_buffer order in
      match write_result with
      | Some () ->
        OrderSignal.broadcast ();
        Order_actions.incr order.symbol;
        let state =
          match state with
          | Some s -> s
          | None -> get_strategy_state order.symbol
        in
        state.last_order_time <- now;
        (match order.operation, order.side with
         | Place, Buy -> state.inflight_buy <- true
         | Place, Sell -> state.inflight_sell <- true
         | _ -> ());
        (match order.operation with
         | Place ->
           let order_ecfg = get_exchange_config order.exchange in
           let skip_pending = (not order_ecfg.track_pending_sells) && order.side = Sell in
           if not skip_pending
           then (
             let temp_order_id =
               Printf.sprintf
                 "pending_%s_%.2f"
                 (string_of_order_side order.side)
                 (Option.value order.price ~default:0.0)
             in
             let order_price = Option.value order.price ~default:0.0 in
             let timestamp = now in
             state.pending_orders
             <- (temp_order_id, order.side, order_price, timestamp)
                :: state.pending_orders;
             match order.side, order.price with
             | Sell, Some price ->
               state.open_sell_orders
               <- (temp_order_id, price, order.qty) :: state.open_sell_orders;
               ()
             | _ -> ())
         | Amend ->
           let temp_order_id =
             Printf.sprintf
               "pending_amend_%s"
               (Option.value order.order_id ~default:"unknown")
           in
           let order_price = Option.value order.price ~default:0.0 in
           let timestamp = now in
           state.pending_orders
           <- (temp_order_id, order.side, order_price, timestamp) :: state.pending_orders;
           if order.side = Buy then state.inflight_amend_buy <- true
         | Cancel -> ());
        true
      | None ->
        (match order.operation with
         | Place -> ignore (InFlightOrders.remove_in_flight_order order.duplicate_key)
         | Amend ->
           (match order.order_id with
            | Some oid -> ignore (InFlightAmendments.remove_in_flight_amendment oid)
            | None -> ())
         | _ -> ());
        Logging.warn_f
          ~section
          "Order ringbuffer full, dropped %s %s order for %s"
          operation_str
          (string_of_order_side order.side)
          order.symbol;
        false)
;;
