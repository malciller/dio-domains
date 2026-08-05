
(** Central order processing loop. Drains pending orders from all strategy
    ring buffers (grid, market maker, hedger) and dispatches them to the
    Order_executor via Lwt.async. Blocks on OrderSignal when idle.

    Uses a unified dispatch pipeline with strategy-specific callbacks
    to eliminate duplicated order handling code. *)

open Lwt.Infix

(* Shared order types (side, operation, strategy_order). *)
open Dio_strategies.Strategy_common

open Supervisor_types
open Supervisor_connection

let section = "order_processor"

(* --------------------------------------------------------------------------
   Strategy callback interface
   -------------------------------------------------------------------------- *)

(** Per-strategy callbacks invoked during order lifecycle events.
    Each strategy provides its own implementation to preserve unique behavior. *)
type strategy_callbacks = {
  on_place_ok      : strategy_order -> string (* order_id *) -> unit;
  on_place_fail    : strategy_order -> string (* error *) -> unit;
  on_amend_ok      : strategy_order -> string (* target_order_id *) -> string (* new_order_id *) -> unit;
  on_amend_skipped : strategy_order -> string (* target_order_id *) -> unit;
  on_amend_fail    : strategy_order -> string (* target_order_id *) -> string (* error *) -> unit;
  on_cancel_ok     : strategy_order -> string (* target_order_id *) -> unit;
  on_cancel_fail   : strategy_order -> string (* target_order_id *) -> unit;
}

let side_str order = match order.side with Buy -> "buy" | Sell -> "sell"
let price_str order = match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market"

(* --------------------------------------------------------------------------
   Strategy callback implementations
   -------------------------------------------------------------------------- *)

let grid_callbacks : strategy_callbacks = {
  on_place_ok = (fun order order_id ->
    Logging.info_f ~section "✓ Order placed successfully: %s %s %.8f @ %s (Order ID: %s)"
      (side_str order) order.symbol order.qty (price_str order) order_id;
    match order.price with
    | Some price ->
        Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged ~now:(Unix.gettimeofday ())
          order.symbol order_id order.side price
    | None -> ()
  );
  on_place_fail = (fun order err ->
    Logging.error_f ~section "✗ Order placement failed: %s %s %.8f @ %s - %s"
      (side_str order) order.symbol order.qty (price_str order) err;
    Dio_strategies.Suicide_grid.Strategy.handle_order_failed ~now:(Unix.gettimeofday ()) order.symbol order.side err;
    match order.price with
    | Some price ->
        Dio_strategies.Suicide_grid.Strategy.handle_order_rejected ~now:(Unix.gettimeofday ()) order.symbol order.side price
    | None -> ()
  );
  on_amend_ok = (fun order target_order_id new_order_id ->
    Logging.info_f ~section "✓ Order amended successfully: %s %s %.8f @ %s New Order ID: %s"
      (side_str order) order.symbol order.qty (price_str order) new_order_id;
    match order.price with
    | Some price ->
        Dio_strategies.Suicide_grid.Strategy.handle_order_amended ~now:(Unix.gettimeofday ())
          order.symbol target_order_id new_order_id order.side price
    | None -> Logging.warn_f ~section "Amendment acknowledged but no price available for strategy update: %s" new_order_id
  );
  on_amend_skipped = (fun order target_order_id ->
    match order.price with
    | Some price ->
        Dio_strategies.Suicide_grid.Strategy.handle_order_amendment_skipped ~now:(Unix.gettimeofday ())
          order.symbol target_order_id order.side price
    | None -> Logging.warn_f ~section "Amendment skipped but no price available for strategy update"
  );
  on_amend_fail = (fun order target_order_id err ->
    Logging.error_f ~section "✗ Order amendment failed: %s %s %.8f @ %s - %s"
      (side_str order) order.symbol order.qty (price_str order) err;
    Dio_strategies.Suicide_grid.Strategy.handle_order_amendment_failed ~now:(Unix.gettimeofday ()) order.symbol target_order_id order.side err
  );
  on_cancel_ok = (fun order target_order_id ->
    Logging.info_f ~section "✓ Cancelled order: %s" target_order_id;
    Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
  );
  on_cancel_fail = (fun order target_order_id ->
    Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
  );
}

let mm_callbacks : strategy_callbacks = {
  on_place_ok = (fun order order_id ->
    Logging.info_f ~section " Order placed successfully: %s %s %.8f @ %s (Order ID: %s)"
      (side_str order) order.symbol order.qty (price_str order) order_id;
  );
  on_place_fail = (fun order err ->
    Logging.error_f ~section " Order placement failed: %s %s %.8f @ %s - %s"
      (side_str order) order.symbol order.qty (price_str order) err;
    Dio_strategies.Market_maker.Strategy.handle_order_failed ~now:(Unix.gettimeofday ()) order.symbol order.side err;
    match order.price with
    | Some price ->
        Dio_strategies.Market_maker.Strategy.handle_order_rejected ~now:(Unix.gettimeofday ()) order.symbol order.side price
    | None -> ()
  );
  on_amend_ok = (fun order target_order_id new_order_id ->
    Logging.info_f ~section "✓ Order amended successfully: %s %s %.8f @ %s New Order: %s"
      (side_str order) order.symbol order.qty (price_str order) new_order_id;
    match order.price with
    | Some price ->
        Dio_strategies.Market_maker.Strategy.handle_order_amended ~now:(Unix.gettimeofday ())
          order.symbol target_order_id new_order_id order.side price
    | None -> Logging.warn_f ~section "Amendment acknowledged but no price available for strategy update: %s" new_order_id
  );
  on_amend_skipped = (fun order target_order_id ->
    match order.price with
    | Some price ->
        Dio_strategies.Market_maker.Strategy.handle_order_amendment_skipped ~now:(Unix.gettimeofday ())
          order.symbol target_order_id order.side price
    | None -> Logging.warn_f ~section "Amendment skipped but no price available for strategy update"
  );
  on_amend_fail = (fun order target_order_id err ->
    Logging.error_f ~section "✗ Order amendment failed: %s %s %.8f @ %s - %s"
      (side_str order) order.symbol order.qty (price_str order) err;
    Dio_strategies.Market_maker.Strategy.handle_order_amendment_failed ~now:(Unix.gettimeofday ()) order.symbol target_order_id order.side err
  );
  on_cancel_ok = (fun order target_order_id ->
    Logging.info_f ~section "✓ Cancelled order: %s" target_order_id;
    Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
  );
  on_cancel_fail = (fun order target_order_id ->
    Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
  );
}

let hedger_callbacks : strategy_callbacks = {
  on_place_ok = (fun order order_id ->
    Logging.info_f ~section "✓ Hedger order placed successfully: %s %s %.8f @ %s (Order ID: %s)"
      (side_str order) order.symbol order.qty (price_str order) order_id;
  );
  on_place_fail = (fun order err ->
    Logging.error_f ~section "✗ Hedger order placement failed: %s %s %.8f @ %s - %s"
      (side_str order) order.symbol order.qty (price_str order) err;
  );
  (* Hedger only supports Place — these should never fire *)
  on_amend_ok      = (fun _order _target_id _new_id -> ());
  on_amend_skipped = (fun _order _target_id -> ());
  on_amend_fail    = (fun _order _target_id _err -> ());
  on_cancel_ok     = (fun _order _target_id -> ());
  on_cancel_fail   = (fun _order _target_id -> ());
}

(** Resolves the correct callbacks for the given strategy_order.
    For orders processed in the MM batch, the strategy field determines
    routing since MM batch handles both Grid and MM amend/cancel orders. *)
let callbacks_for_strategy (order : strategy_order) =
  match order.strategy with
  | Grid   -> grid_callbacks
  | MM     -> mm_callbacks
  | Hedger -> hedger_callbacks

(* --------------------------------------------------------------------------
   Unified order dispatch
   -------------------------------------------------------------------------- *)

(** Dispatches a Place order asynchronously via Order_executor. *)
let dispatch_place ~auth_token ~orders_placed ~cb (order : strategy_order) =
  let order_request = {
    Dio_engine.Order_executor.order_type = order.order_type;
    side = (side_str order);
    quantity = order.qty;
    symbol = order.symbol;
    limit_price = order.price;
    time_in_force = Some order.time_in_force;
    post_only = Some order.post_only;
    margin = None;
    reduce_only = None;
    order_userref = order.userref;
    cl_ord_id = None;
    trigger_price = None;
    trigger_price_type = None;
    display_qty = None;
    fee_preference = None;
    duplicate_key = order.duplicate_key;
    exchange = order.exchange;
  } in
  Lwt.async (fun () ->
    let%lwt () = Lwt.pause () in
    Lwt.catch (fun () ->
      Dio_engine.Order_executor.place_order ~token:auth_token ~check_duplicate:false order_request >>= function
      | Ok result ->
          Atomic.incr orders_placed;
          cb.on_place_ok order result.order_id;
          Lwt.return_unit
      | Error err ->
          cb.on_place_fail order err;
          Lwt.return_unit
    ) (fun exn ->
      let err = Printexc.to_string exn in
      Logging.error_f ~section "✗ Exception placing order %s %s: %s" (side_str order) order.symbol err;
      cb.on_place_fail order err;
      Lwt.return_unit
    )
  )

(** Dispatches an Amend order asynchronously via Order_executor. *)
let dispatch_amend ~auth_token ~orders_placed ~cb (order : strategy_order) target_order_id =
  let amend_request = {
    Dio_engine.Order_executor.order_id = target_order_id;
    cl_ord_id = None;
    new_quantity = Some order.qty;
    new_limit_price = order.price;
    limit_price_type = None;
    post_only = Some order.post_only;
    new_trigger_price = None;
    trigger_price_type = None;
    new_display_qty = None;
    deadline = None;
    symbol = Some order.symbol;
    exchange = order.exchange;
  } in
  Lwt.async (fun () ->
    let%lwt () = Lwt.pause () in
    Lwt.catch (fun () ->
      Dio_engine.Order_executor.amend_order ~token:auth_token amend_request >>= function
      | Ok result ->
          if result.Dio_exchange.Exchange_intf.Types.amend_id = Some "skipped_no_change" then begin
            cb.on_amend_skipped order target_order_id;
            Lwt.return_unit
          end else begin
            Atomic.incr orders_placed;
            let amend_id_str = match result.Dio_exchange.Exchange_intf.Types.amend_id with Some id -> id | None -> "none" in
            Logging.info_f ~section "✓ Order amended (Amend ID: %s)" amend_id_str;
            cb.on_amend_ok order target_order_id result.Dio_exchange.Exchange_intf.Types.new_order_id;
            Lwt.return_unit
          end
      | Error err ->
          cb.on_amend_fail order target_order_id err;
          Lwt.return_unit
    ) (fun exn ->
      let err = Printexc.to_string exn in
      Logging.error_f ~section "✗ Exception amending order %s %s: %s" (side_str order) order.symbol err;
      cb.on_amend_fail order target_order_id err;
      Lwt.return_unit
    )
  )

(** Dispatches a Cancel order asynchronously via Order_executor. *)
let dispatch_cancel ~auth_token ~orders_placed ~cb (order : strategy_order) target_order_id =
  Lwt.async (fun () ->
    let%lwt () = Lwt.pause () in
    Lwt.catch (fun () ->
      let request : Dio_engine.Order_executor.cancel_request = {
        exchange = order.exchange;
        order_ids = Some [target_order_id];
        cl_ord_ids = None;
        order_userrefs = None;
        symbol = Some order.symbol;
      } in
      Dio_engine.Order_executor.cancel_orders ~token:auth_token request >>= function
      | Ok results ->
          let count = List.length results in
          Atomic.set orders_placed (Atomic.get orders_placed + count);
          Logging.info_f ~section "✓ Cancelled %d order(s) successfully: %s" count target_order_id;
          cb.on_cancel_ok order target_order_id;
          Lwt.return_unit
      | Error err ->
          Logging.error_f ~section "✗ Order cancellation failed: %s - %s" target_order_id err;
          cb.on_cancel_fail order target_order_id;
          Lwt.return_unit
    ) (fun exn ->
      Logging.error_f ~section "✗ Exception cancelling order %s: %s" target_order_id (Printexc.to_string exn);
      cb.on_cancel_fail order target_order_id;
      Lwt.return_unit
    )
  )

(* --------------------------------------------------------------------------
   Unified order processor
   -------------------------------------------------------------------------- *)

(** Processes a single order: checks connectivity, resolves auth token,
    and dispatches via the appropriate operation handler. *)
let process_single_order ~orders_placed ~order_mutex ~is_connected (order : strategy_order) =
  if Atomic.get shutdown_requested then () else
  Mutex.lock order_mutex;
  try
    let auth_token = match Token_store.get () with
      | Some token -> token
      | None ->
        Logging.warn ~section "No auth token available for order operations";
        raise (Failure "No auth token")
    in

    let cb = callbacks_for_strategy order in

    let process_fn () =
      match order.operation with
      | Place ->
          dispatch_place ~auth_token ~orders_placed ~cb order
      | Amend ->
          (match order.order_id with
           | Some target_order_id ->
               dispatch_amend ~auth_token ~orders_placed ~cb order target_order_id
           | None ->
               Logging.error_f ~section "Amendment request missing target order ID for %s %s"
                 (side_str order) order.symbol)
      | Cancel ->
          (match order.order_id with
           | Some target_order_id ->
               dispatch_cancel ~auth_token ~orders_placed ~cb order target_order_id
           | None ->
               Logging.error_f ~section "Cancel request missing target order ID for %s" order.symbol)
    in

    let reject_fn err =
      match order.operation with
      | Place -> cb.on_place_fail order err
      | Amend ->
          (match order.order_id with
           | Some target_order_id -> cb.on_amend_fail order target_order_id err
           | None -> ())
      | Cancel -> ()  (* Cancel rejections are non-critical *)
    in

    let connected = is_connected order in
    if connected then process_fn ()
    else begin
      Logging.warn_f ~section "Exchange %s not connected, dropping order %s %s"
        order.exchange (side_str order) order.symbol;
      reject_fn "Exchange not connected"
    end;

    (* Release mutex before Lwt.async callbacks execute *)
    Mutex.unlock order_mutex
  with exn ->
    Mutex.unlock order_mutex;
    Logging.error_f ~section "Error processing order %s %s: %s"
      (side_str order) order.symbol (Printexc.to_string exn)


(** The main order processing loop. *)
let order_processing_loop () =
  let cycle_count = ref 0 in
  let orders_placed = Atomic.make 0 in
  let order_mutex = Mutex.create () in

  let rec loop () =
    if Atomic.get shutdown_requested then Lwt.return_unit
    else begin
      (* Check exchange connection liveness *)
      let kraken_connected = Kraken.Kraken_trading_client.is_connected () in
      
      let is_hyperliquid_connected =
          try
            let hl_conn = Hashtbl.find connections "hyperliquid_ws" in
            get_state hl_conn = Connected
          with Not_found -> false
      in

      let is_ibkr_connected =
          try
            let ibkr_conn = Hashtbl.find connections "ibkr_gateway" in
            get_state ibkr_conn = Connected
          with Not_found -> false
      in

      let is_lighter_connected =
          try
            let lt_conn = Hashtbl.find connections "lighter_ws" in
            get_state lt_conn = Connected
          with Not_found -> false
      in

      let is_connected (order : strategy_order) =
        if order.exchange = "kraken" then kraken_connected
        else if order.exchange = "hyperliquid" then is_hyperliquid_connected
        else if order.exchange = "ibkr" then is_ibkr_connected
        else if order.exchange = "lighter" then is_lighter_connected
        else true
      in

      (* Drain ring buffers regardless of connection status to prevent backpressure *)
      let pending_grid_orders = Dio_strategies.Suicide_grid.Strategy.get_pending_orders 100 in
      let pending_mm_orders = Dio_strategies.Market_maker.Strategy.get_pending_orders 100 in
      let pending_hedge_orders = Dio_strategies.Auto_hedger.get_pending_orders 100 in

      if pending_grid_orders = [] && pending_mm_orders = [] && pending_hedge_orders = [] then
        (* No pending orders; block until signalled.
           Sever promise chain via Lwt.async to prevent Forward node accumulation. *)
        OrderSignal.wait () >>= fun () ->
        Lwt.async loop;
        Lwt.return_unit
      else begin
        incr cycle_count;
        try
            (* Process grid strategy orders *)
            List.iter (process_single_order ~orders_placed ~order_mutex ~is_connected) pending_grid_orders;

            (* Process market maker orders; abort if shutdown raised after grid batch *)
            if not (Atomic.get shutdown_requested) then
              List.iter (process_single_order ~orders_placed ~order_mutex ~is_connected) pending_mm_orders;

            (* Process hedger orders; abort if shutdown raised after MM batch.
               Hedger only supports Place — other operations log a warning. *)
            if not (Atomic.get shutdown_requested) then
              List.iter (fun order ->
                if order.operation <> Place then
                  Logging.warn_f ~section "Auto hedger only supports Place operations, got other for %s" order.symbol
                else
                  process_single_order ~orders_placed ~order_mutex ~is_connected order
              ) pending_hedge_orders;

            (* Sever promise chain before next drain cycle. *)
            Lwt.async loop;
            Lwt.return_unit

          with exn ->
            Logging.error_f ~section "Exception in order processing loop: %s" (Printexc.to_string exn);
            Lwt.async loop;
            Lwt.return_unit
        end
      end
  in
  Lwt.async loop
