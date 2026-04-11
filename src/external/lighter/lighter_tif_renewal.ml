(** Lighter Order TIF Renewal.

    Lighter only supports Good-Till-Time (GTT) orders with a maximum 28-day
    TTL. This module implements a background process that monitors open orders
    and refreshes their expiry by cancelling and re-placing them with a fresh
    28-day TTL, effectively simulating Good-Till-Cancelled (GTC) behaviour.

    Lighter's [SignModifyOrder] does not accept TIF/expiry parameters, so
    cancel-and-replace is the only option to extend order lifetime. *)

open Lwt.Infix

let section = "lighter_tif_renewal"

(** Threshold in seconds: renew orders with less than this remaining.
    Set to 27 days for testing (catches orders at ~26 days from expiry).
    Production value should be ~1 day (86400s). *)
let renewal_threshold_seconds = 1.0 *. 86400.0

(** How often to check for orders approaching expiry (in seconds).
    Hourly checks are cheap — just iterating the in-memory open orders table. *)
let check_interval_seconds = 3600.0

(** Atomic flag for shutdown coordination. *)
let shutdown_requested = ref false

let signal_shutdown () = shutdown_requested := true

(** Cancel an order and re-place it with the same parameters + fresh 28-day TTL.
    Returns [Ok ()] on success, [Error msg] on failure. *)
let renew_order (order : Lighter_executions_feed.open_order) : (unit, string) result Lwt.t =
  let symbol = order.symbol in
  let order_id = order.order_id in
  let price = match order.limit_price with
    | Some p -> p
    | None ->
        Logging.error_f ~section "TIF renewal skipped: order %s [%s] has no limit price" order_id symbol;
        0.0
  in
  if price = 0.0 then
    Lwt.return (Error "no limit price")
  else begin
    let is_buy = order.side = Lighter_executions_feed.Buy in
    let qty = order.order_qty in
    let remaining_qty = order.remaining_qty in

    Logging.info_f ~section "TIF renewal: cancelling order %s [%s] %s %.8f @ %.2f (remaining=%.8f)"
      order_id symbol (if is_buy then "BUY" else "SELL") qty price remaining_qty;

    (* Step 1: Cancel the existing order *)
    Lighter_actions.cancel_order ~symbol ~order_id >>= fun cancel_result ->
    match cancel_result with
    | Error msg ->
        Logging.error_f ~section "TIF renewal: cancel failed for %s [%s]: %s" order_id symbol msg;
        Lwt.return (Error (Printf.sprintf "cancel failed: %s" msg))
    | Ok _ ->
        (* Brief delay for cancellation to propagate through WS *)
        Lwt_unix.sleep 0.5 >>= fun () ->

        (* Step 2: Re-place with the same price and remaining quantity.
           The new order gets a fresh 28-day TTL via the default expiry=-1. *)
        Lighter_actions.place_order ~symbol ~is_buy ~qty:remaining_qty ~price () >>= fun place_result ->
        match place_result with
        | Ok new_order ->
            Logging.info_f ~section "TIF renewal: order %s [%s] renewed as %s (same price %.2f, qty %.8f)"
              order_id symbol new_order.Lighter_types.Types.order_id price remaining_qty;
            Lwt.return (Ok ())
        | Error msg ->
            (* Critical: order was cancelled but re-place failed.
               The order is now off the book entirely. *)
            Logging.error_f ~section
              "TIF RENEWAL CRITICAL: order %s [%s] was cancelled but re-place failed: %s. \
               Order is now OFF THE BOOK. Manual intervention required."
              order_id symbol msg;
            Lwt.return (Error (Printf.sprintf "re-place failed after cancel: %s" msg))
  end

(** One cycle of the renewal check. Scans all open Lighter orders, identifies
    those with expiry within [renewal_threshold_seconds], and renews them. *)
let run_renewal_cycle () =
  let now = Unix.gettimeofday () in
  let all_orders = Lighter_executions_feed.get_all_open_orders_with_expiry () in

  let expiring = List.filter (fun (order : Lighter_executions_feed.open_order) ->
    match order.order_expiry with
    | Some expiry ->
        let remaining = expiry -. now in
        remaining < renewal_threshold_seconds && remaining > 0.0
    | None ->
        (* No expiry data available — skip this order *)
        false
  ) all_orders in

  let total_open = List.length all_orders in
  let total_expiring = List.length expiring in
  let total_with_expiry = List.length (List.filter (fun (o : Lighter_executions_feed.open_order) ->
    Option.is_some o.order_expiry) all_orders) in

  if total_expiring > 0 then
    Logging.info_f ~section "TIF renewal cycle: %d/%d orders approaching expiry (threshold=%.0f days, %d with expiry data)"
      total_expiring total_open (renewal_threshold_seconds /. 86400.0) total_with_expiry
  else
    Logging.debug_f ~section "TIF renewal cycle: %d open orders, %d with expiry data, none approaching threshold (%.0f days)"
      total_open total_with_expiry (renewal_threshold_seconds /. 86400.0);

  (* Log individual order expiry details for diagnostics *)
  List.iter (fun (order : Lighter_executions_feed.open_order) ->
    let now_t = Unix.gettimeofday () in
    match order.order_expiry with
    | Some exp ->
        let remaining_days = (exp -. now_t) /. 86400.0 in
        Logging.debug_f ~section "  Order %s [%s]: expiry=%.0f, remaining=%.1f days, needs_renewal=%b"
          order.order_id order.symbol exp remaining_days
          (remaining_days *. 86400.0 < renewal_threshold_seconds)
    | None ->
        Logging.debug_f ~section "  Order %s [%s]: NO EXPIRY DATA"
          order.order_id order.symbol
  ) all_orders;

  (* Process expiring orders sequentially with a delay between each
     to avoid nonce conflicts with the Lighter signer.
     Use consume_stream_s to prevent Forward promise accumulation. *)
  let expiring_stream = Lwt_stream.of_list expiring in
  Concurrency.Lwt_util.consume_stream_s (fun (order : Lighter_executions_feed.open_order) ->
    if !shutdown_requested then Lwt.return_unit
    else begin
      let expiry = Option.get order.order_expiry in
      let remaining_days = (expiry -. now) /. 86400.0 in
      Logging.info_f ~section "Renewing order %s [%s]: %.1f days remaining (expiry=%.0f)"
        order.order_id order.symbol remaining_days expiry;
      renew_order order >>= fun result ->
      (match result with
       | Ok () -> ()
       | Error _ -> ());
      (* Rate-limit: 200ms between operations to be safe with nonces *)
      Lwt_unix.sleep 0.2
    end
  ) expiring_stream

(** Start the TIF renewal background loop. Runs every [check_interval_seconds]
    and scans open orders for those approaching expiry. *)
let start ~symbols:_ =
  Logging.info_f ~section "Starting TIF renewal monitor (check_interval=%.0fs, threshold=%.0f days)"
    check_interval_seconds (renewal_threshold_seconds /. 86400.0);

  Concurrency.Lwt_util.run_periodic
    ~initial_delay:5.0
    ~interval:check_interval_seconds
    ~stop:(fun () -> !shutdown_requested)
    (fun () ->
      Lwt.catch
        (fun () -> run_renewal_cycle ())
        (fun exn ->
          Logging.error_f ~section "TIF renewal cycle failed: %s" (Printexc.to_string exn);
          Lwt.return_unit))

