(** Lighter Order TIF Renewal Module.

    The Lighter exchange protocol mandates the use of Good-Till-Time (GTT) orders with a maximum 28-day Time-To-Live (TTL). This module supplies an asynchronous background daemon that proactively monitors the internal state of open orders and refreshes their expiration timestamps. The renewal mechanism operates by sequentially issuing a cancellation request followed immediately by a new placement request, thereby simulating Good-Till-Cancelled (GTC) order semantics across the 28-day boundary.

    Given that the Lighter [SignModifyOrder] procedure does not permit modification of Time-In-Force (TIF) or expiry parameters, the discrete cancel-and-replace sequence remains the sole architectural path for extending the temporal validity of actively resting orders without incurring execution interruptions. *)

open Lwt.Infix

let section = "lighter_tif_renewal"

(** Constant defining the proximity threshold in seconds for order renewal. Orders possessing an expiration timestamp with a delta to the current system time less than this constant are selected for the renewal cycle. For diagnostic analysis, this value is currently configured to 27 days to identify items at roughly 26 days from expiration. Recommended production configuration is 1 day (86400.0 seconds). *)
let renewal_threshold_seconds = 1.0 *. 86400.0

(** Constant defining the periodic execution frequency for the expiration verification loop in seconds. High-frequency execution is computationally inexpensive as it performs a direct iteration over the locally cached in-memory open orders hash table. *)
let check_interval_seconds = 3600.0

(** Mutable boolean standardizing concurrent shutdown coordination across asynchronous execution threads. *)
let shutdown_requested = ref false

let signal_shutdown () = shutdown_requested := true

(** Executes the sequential lifecycle logic required to cancel an existing resting order and subsequently place an identically parameterized new order with a newly initialized 28-day TTL.
    Returns [Ok ()] upon successful stabilization of the new order on the order book, or [Error msg] upon encountering network faults or protocol rejections. *)
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

    (* Step 1: Initiate a definitive cancellation instruction against the existing exchange order token. *)
    Lighter_actions.cancel_order ~symbol ~order_id >>= fun cancel_result ->
    match cancel_result with
    | Error msg ->
        Logging.error_f ~section "TIF renewal: cancel failed for %s [%s]: %s" order_id symbol msg;
        Lwt.return (Error (Printf.sprintf "cancel failed: %s" msg))
    | Ok _ ->
        (* Yield the execution thread briefly to allow the centralized protocol servers to propagate the un-booked status through the WebSocket multiplexer. *)
        Lwt_unix.sleep 0.5 >>= fun () ->

        (* Step 2: Execute the replacement placement request utilizing the identical limit price and the dynamically resolved remaining quantity. The newly constructed order receives the baseline protocol maximum TTL of 28 days via the default expiry metric constraint. *)
        Lighter_actions.place_order ~symbol ~is_buy ~qty:remaining_qty ~price () >>= fun place_result ->
        match place_result with
        | Ok new_order ->
            Logging.info_f ~section "TIF renewal: order %s [%s] renewed as %s (same price %.2f, qty %.8f)"
              order_id symbol new_order.Lighter_types.Types.order_id price remaining_qty;
            Lwt.return (Ok ())
        | Error msg ->
            (* Critical state discontinuity detected. The existing order was confirmed cancelled but the subsequent replacement procedure failed synchronization. The intended liquidity is currently removed from the exchange order book entirely. *)
            Logging.error_f ~section
              "TIF RENEWAL CRITICAL: order %s [%s] was cancelled but re-place failed: %s. \
               Order is now OFF THE BOOK. Manual intervention required."
              order_id symbol msg;
            Lwt.return (Error (Printf.sprintf "re-place failed after cancel: %s" msg))
  end

(** Executes a discrete execution cycle of the Time-In-Force renewal sub-system. It initiates an iteration over the totality of cached open orders mapping, identifies the subset possessing an expiration deadline within the bounds of [renewal_threshold_seconds], and dispatches the execution of the cancellation-and-replacement sequence for each qualifying order. *)
let run_renewal_cycle () =
  let now = Unix.gettimeofday () in
  let all_orders = Lighter_executions_feed.get_all_open_orders_with_expiry () in

  let expiring = List.filter (fun (order : Lighter_executions_feed.open_order) ->
    match order.order_expiry with
    | Some expiry ->
        let remaining = expiry -. now in
        remaining < renewal_threshold_seconds && remaining > 0.0
    | None ->
        (* The target order lacks an explicit expiration dataset and is therefore entirely bypassed from the renewal logic. *)
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

  (* Emit detailed expiration state metrics per order for backend diagnostics and integrity verification. *)
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

  (* Orchestrate the sequential execution of the renewal logic over the expiring orders list. This incorporates a deliberate throttling delay between operations to avert nonce desynchronization constraints within the downstream Lighter protocol cryptographic signer. It explicitly utilizes [consume_stream_s] to circumvent memory constraints associated with uncontrolled [Forward] promise accumulation within the asynchronous runtime. *)
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
      (* Explicit rate-limiting constraint of 200 milliseconds between consecutive renewal operations to guarantee monotonic sequencer generation for the cryptographic signer thread. *)
      Lwt_unix.sleep 0.2
    end
  ) expiring_stream

(** Initializes the daemonized asynchronous execution loop for Time-In-Force renewal operations. The recurring cycle evaluates the system cache at intervals defined by [check_interval_seconds] to proactively intercept orders rapidly nearing their specified temporal validity limit. *)
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

