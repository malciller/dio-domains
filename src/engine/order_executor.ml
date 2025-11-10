(** Order Execution Engine

    This module provides a high-level interface for executing trading orders
    through the Kraken WebSocket API v2. It handles order placement, modification,
    and cancellation with proper error handling and retry logic.
*)

open Lwt.Infix

let section = "order_executor"

(** Helper function to check if string contains substring *)
let string_contains str substr =
  let str_len = String.length str and substr_len = String.length substr in
  if substr_len > str_len then false
  else
    let rec loop i =
      if i + substr_len > str_len then false
      else if String.sub str i substr_len = substr then true
      else loop (i + 1)
    in
    loop 0

(** Global shutdown flag for order executor *)
let shutdown_requested = Atomic.make false

(** Signal shutdown to order executor *)
let signal_shutdown () =
  Atomic.set shutdown_requested true

(** Unified request key type for in-flight tracking *)
type request_key =
  | NewOrder of string  (* duplicate_key for new orders *)
  | AmendOrder of string (* order_id for amendments *)

(** Unified in-flight request cache to prevent duplicate orders and amendments *)
module InFlightRequests = struct
  module Registry = Concurrency.Event_registry.Make(struct
    type t = request_key
    let equal a b = match a, b with
      | NewOrder k1, NewOrder k2 -> String.equal k1 k2
      | AmendOrder k1, AmendOrder k2 -> String.equal k1 k2
      | _ -> false
    let hash = function
      | NewOrder k -> Hashtbl.hash ("new", k)
      | AmendOrder k -> Hashtbl.hash ("amend", k)
  end)(struct
    type t = float (* timestamp when request was added *)
  end)

  let registry = Registry.create ()

  (** Check if a request is already in-flight and add it if not *)
  let add_in_flight_request request_key =
    let now = Unix.gettimeofday () in
    match Registry.replace registry request_key now with
    | Some (_, true) -> false (* Already existed *)
    | Some (_, false) -> true (* Added successfully *)
    | None -> false (* Should not happen *)

  (** Remove a request from the in-flight cache *)
  let remove_in_flight_request request_key =
    Registry.remove registry request_key |> Option.is_some

  (** Get the current size of the in-flight requests registry *)
  let get_registry_size () =
    Registry.size registry

  (** Clean up stale in-flight requests (older than timeout_seconds) *)
  let cleanup_stale_requests ?(timeout_seconds=30.0) () =
    let now = Unix.gettimeofday () in
    let cutoff = now -. timeout_seconds in
    let cleaned = ref 0 in

    (* Get all current requests *)
    let snapshot = Registry.snapshot registry in
    Registry.Tbl.iter (fun request_key timestamp ->
      if timestamp < cutoff then (
        (* Remove stale request *)
        if Registry.remove registry request_key |> Option.is_some then (
          incr cleaned;
          let request_type = match request_key with
            | NewOrder key -> Printf.sprintf "new order (%s)" key
            | AmendOrder order_id -> Printf.sprintf "amendment for order %s" order_id
          in
          Logging.debug_f ~section:"in_flight_requests" "Cleaned up stale in-flight %s (age: %.1fs)" request_type (now -. timestamp)
        )
      )
    ) snapshot;

    !cleaned
end

(** Backwards compatibility functions *)
let add_in_flight_order duplicate_key =
  InFlightRequests.add_in_flight_request (NewOrder duplicate_key)

let remove_in_flight_order duplicate_key =
  InFlightRequests.remove_in_flight_request (NewOrder duplicate_key)

let add_in_flight_amendment order_id =
  InFlightRequests.add_in_flight_request (AmendOrder order_id)

let remove_in_flight_amendment order_id =
  InFlightRequests.remove_in_flight_request (AmendOrder order_id)

(** Check if an amendment is already in-flight without modifying the registry *)
let is_amendment_in_flight order_id =
  match InFlightRequests.Registry.find InFlightRequests.registry (AmendOrder order_id) with
  | Some _ -> true
  | None -> false

(** Timed-out amendments pending execution feed confirmation *)
module TimedOutAmendments = struct
  type timed_out_amendment = {
    order_id: string;
    symbol: string;
    requested_price: float option;
    requested_qty: float option;
    timeout_time: float;
    grace_period: float; (* seconds *)
  }

  module Registry = Concurrency.Event_registry.Make(struct
    type t = string (* order_id *)
    let equal = String.equal
    let hash = Hashtbl.hash
  end)(struct
    type t = timed_out_amendment
  end)

  let registry = Registry.create ()

  (** Add a timed-out amendment for confirmation tracking *)
  let add_timed_out_amendment order_id symbol requested_price requested_qty grace_period =
    let now = Unix.gettimeofday () in
    let amendment = {
      order_id;
      symbol;
      requested_price;
      requested_qty;
      timeout_time = now;
      grace_period;
    } in
    Registry.replace registry order_id amendment |> ignore;
    Logging.debug_f ~section "Added timed-out amendment tracking for order %s (grace period: %.1fs)" order_id grace_period

  (** Check if order update matches timed-out amendment *)
  let check_order_update order_id current_price current_qty =
    match Registry.find registry order_id with
    | Some amendment ->
        let now = Unix.gettimeofday () in
        if now -. amendment.timeout_time > amendment.grace_period then (
          (* Grace period expired, remove stale entry *)
          Registry.remove registry order_id |> ignore;
          Logging.debug_f ~section "Grace period expired for timed-out amendment %s, removing tracking" order_id;
          false
        ) else (
          (* Check if order matches requested values *)
          let price_match = match amendment.requested_price with
            | Some req_price -> current_price <> None && abs_float (Option.value current_price ~default:0.0 -. req_price) < 0.000001
            | None -> true
          in
          let qty_match = match amendment.requested_qty with
            | Some req_qty -> abs_float (current_qty -. req_qty) < 0.000001
            | None -> true
          in
          if price_match && qty_match then (
            Registry.remove registry order_id |> ignore;
            Logging.info_f ~section "Timed-out amendment confirmed via execution feed: order %s matches requested values" order_id;
            true
          ) else (
            Logging.debug_f ~section "Order update for %s doesn't match timed-out amendment (price_match=%b, qty_match=%b)" order_id price_match qty_match;
            false
          )
        )
    | None -> false

  (** Remove timed-out amendment (e.g., when confirmed or failed) *)
  let remove_timed_out_amendment order_id =
    Registry.remove registry order_id |> Option.is_some

  (** Clean up expired grace periods *)
  let cleanup_expired () =
    let now = Unix.gettimeofday () in
    let cleaned = ref 0 in
    let snapshot = Registry.snapshot registry in
    Registry.Tbl.iter (fun order_id amendment ->
      if now -. amendment.timeout_time > amendment.grace_period then (
        if Registry.remove registry order_id |> Option.is_some then (
          incr cleaned;
          Logging.debug_f ~section "Cleaned up expired timed-out amendment for order %s" order_id
        )
      )
    ) snapshot;
    !cleaned

  (** Get current size for monitoring *)
  let get_size () = Registry.size registry
end


(** Order type definitions *)
type order_type = string (* "market" | "limit" | "stop-loss" | "take-profit" | "trailing-stop" | etc. *)
type order_side = string (* "buy" | "sell" *)

(** Order request structure *)
type order_request = {
  order_type: order_type;
  side: order_side;
  quantity: float;
  symbol: string;
  limit_price: float option;
  time_in_force: string option; (* "GTC", "IOC", "FOK" *)
  post_only: bool option;
  margin: bool option;
  reduce_only: bool option;
  order_userref: int option;
  cl_ord_id: string option;
  trigger_price: float option;
  trigger_price_type: string option;
  display_qty: float option;
  fee_preference: string option;
  duplicate_key: string; (* Hash for duplicate order detection *)
}

(** Amend request structure *)
type amend_request = {
  order_id: string;
  cl_ord_id: string option;
  new_quantity: float option; (* New quantity for the order *)
  new_limit_price: float option;
  limit_price_type: string option;
  post_only: bool option;
  new_trigger_price: float option;
  trigger_price_type: string option;
  new_display_qty: float option;
  deadline: string option; (* RFC3339 format *)
  symbol: string option; (* Required for non-crypto pairs *)
}

(** Cancel request structure *)
type cancel_request = {
  order_ids: string list option;
  cl_ord_ids: string list option;
  order_userrefs: int list option;
}

(** Generate a hash key for duplicate order detection *)
let generate_duplicate_key symbol side quantity limit_price =
  let limit_price_str = match limit_price with
    | Some p -> Printf.sprintf "%.8f" p
    | None -> "market"
  in
  Printf.sprintf "%s|%s|%.8f|%s" symbol side quantity limit_price_str

(** Validation functions *)
let validate_order_request (request : order_request) : (unit, string) result =
  if request.quantity <= 0.0 then
    Error "Order quantity must be positive"
  else if request.symbol = "" then
    Error "Symbol cannot be empty"
  else if request.side <> "buy" && request.side <> "sell" then
    Error "Side must be 'buy' or 'sell'"
  else if request.order_type = "" then
    Error "Order type cannot be empty"
  else
    match request.order_type with
    | "limit" when request.limit_price = None ->
        Error "Limit orders must have a limit_price"
    | "stop-loss" | "take-profit" | "trailing-stop" when request.trigger_price = None ->
        Error (Printf.sprintf "%s orders must have a trigger_price" request.order_type)
    | _ -> Ok ()

let validate_amend_request (request : amend_request) : (unit, string) result =
  if request.order_id = "" then
    Error "Order ID cannot be empty"
  else
    Ok ()

let validate_cancel_request (request : cancel_request) : (unit, string) result =
  let has_order_ids = Option.is_some request.order_ids && Option.value request.order_ids ~default:[] <> [] in
  let has_cl_ord_ids = Option.is_some request.cl_ord_ids && Option.value request.cl_ord_ids ~default:[] <> [] in
  let has_userrefs = Option.is_some request.order_userrefs && Option.value request.order_userrefs ~default:[] <> [] in

  if not (has_order_ids || has_cl_ord_ids || has_userrefs) then
    Error "At least one of order_ids, cl_ord_ids, or order_userrefs must be provided"
  else
    Ok ()

(** Check if error is due to disconnected trading client *)
let is_connection_error exn_str =
  let error_str = String.lowercase_ascii exn_str in
  let contains_substring haystack needle =
    let rec check pos =
      if pos + String.length needle > String.length haystack then false
      else if String.sub haystack pos (String.length needle) = needle then true
      else check (pos + 1)
    in
    check 0 in
  contains_substring error_str "closed socket" ||
  contains_substring error_str "channel_closed" ||
  contains_substring error_str "tls:" ||
  contains_substring error_str "end_of_file" ||
  not (Kraken.Kraken_trading_client.is_connected ())

(** Enhanced error handling wrapper with retry for connection issues *)
let with_error_handling ~operation_name ?(max_retries=3) ?(retry_delay=1.0) f =
  let rec attempt retry_count =
    Lwt.catch
      (fun () -> f ())
      (fun exn ->
        let exn_str = Printexc.to_string exn in
        let err = Printf.sprintf "%s failed: %s" operation_name exn_str in

        (* Check if this is a connection-related error and we haven't exceeded retries *)
        if is_connection_error exn_str && retry_count < max_retries then begin
          Logging.warn_f ~section "%s - connection error detected, retrying in %.1fs (attempt %d/%d)"
            err retry_delay (retry_count + 1) max_retries;
          Lwt_unix.sleep retry_delay >>= fun () ->
          attempt (retry_count + 1)
        end else begin
          Logging.error_f ~section "%s" err;
          Lwt.return (Error err)
        end
      )
  in
  attempt 0

(** Periodic cleanup task for in-flight requests registry *)
let start_inflight_cleanup () =
  Logging.debug ~section "Starting periodic in-flight requests cleanup task";
  let rec cleanup_loop () =
    (* Check for shutdown request *)
    if Atomic.get shutdown_requested then (
      Logging.debug ~section "In-flight requests cleanup task shutting down due to shutdown request";
      Lwt.return_unit
    ) else (
      (* Perform cleanup for unified requests registry - use shorter timeout for amendments *)
      let requests_cleaned = InFlightRequests.cleanup_stale_requests ~timeout_seconds:15.0 () in
      if requests_cleaned > 0 then
        Logging.info_f ~section "Cleaned up %d stale in-flight requests (older than 15s)" requests_cleaned;

      (* Also perform registry maintenance for requests *)
      let requests_cleanup_stats = InFlightRequests.Registry.cleanup InFlightRequests.registry () in
      (match requests_cleanup_stats with
       | Some (size_drift, size_trimmed) ->
           (match size_drift with
            | Some drift when drift <> 0 ->
                Logging.info_f ~section "InFlightRequests registry size drift corrected: %d entries" drift
            | _ -> ());
           (match size_trimmed with
            | Some trimmed when trimmed > 0 ->
                Logging.info_f ~section "InFlightRequests registry size limit enforced: removed %d stale entries" trimmed
            | _ -> ())
       | None -> ());

      (* Clean up expired timed-out amendments *)
      let timed_out_cleaned = TimedOutAmendments.cleanup_expired () in
      if timed_out_cleaned > 0 then
        Logging.info_f ~section "Cleaned up %d expired timed-out amendments" timed_out_cleaned;

      (* Sleep for 2 minutes before next cleanup - more frequent for better memory management *)
      Lwt_unix.sleep 120.0 >>= cleanup_loop
    )
  in
  Lwt.async cleanup_loop

(** Initialize the order executor with authentication token *)
let init : unit Lwt.t =
  (* Trading client is now managed by the supervisor *)
  start_inflight_cleanup ();
  Lwt.return_unit

(** Place a new order *)
let place_order
    ~token
    ?retry_config
    (request : order_request) : (Kraken.Kraken_common_types.add_order_result, string) result Lwt.t =

  (* Check for shutdown before accepting new orders *)
  if Atomic.get shutdown_requested then
    Lwt.return (Error "Order placement cancelled due to shutdown")
  else
    with_error_handling ~operation_name:"place_order" (fun () ->
    match validate_order_request request with
    | Error err ->
        Logging.error_f ~section "Order validation failed: %s" err;
        Lwt.return (Error err)
    | Ok () ->
        (* Check for duplicate orders *)
        if not (add_in_flight_order request.duplicate_key) then begin
          let err = Printf.sprintf "Duplicate order detected: %s %s %f @ %s"
            request.side request.symbol request.quantity
            (match request.limit_price with Some p -> Printf.sprintf "%.2f" p | None -> "market") in
          Logging.warn_f ~section "%s" err;
          Lwt.return (Error err)
        end else begin
          Logging.info_f ~section "Placing %s order: %s %s %f @ %s" request.side request.symbol request.order_type request.quantity
            (match request.limit_price with Some p -> Printf.sprintf "%.2f" p | None -> "market");

          let default_retry_config = Kraken.Kraken_actions.default_retry_config in
          let actual_retry_config = Option.value retry_config ~default:default_retry_config in

          Lwt.catch
            (fun () ->
              Kraken.Kraken_actions.place_order
                ~token
                ~order_type:request.order_type
                ~side:request.side
                ~order_qty:request.quantity
                ~symbol:request.symbol
                ?limit_price:request.limit_price
                ?time_in_force:request.time_in_force
                ?post_only:request.post_only
                ?margin:request.margin
                ?reduce_only:request.reduce_only
                ?order_userref:request.order_userref
                ?cl_ord_id:request.cl_ord_id
                ?trigger_price:request.trigger_price
                ?trigger_price_type:request.trigger_price_type
                ?display_qty:request.display_qty
                ?fee_preference:request.fee_preference
                ~retry_config:actual_retry_config
                ()
            )
            (fun exn ->
              (* Remove from in-flight cache on error *)
              let _ = remove_in_flight_order request.duplicate_key in
              Lwt.fail exn
            )
          >>= fun result ->
          (* Remove from in-flight cache on success *)
          let _ = remove_in_flight_order request.duplicate_key in
          Lwt.return result
        end
  )

(** Amend an existing order *)
let amend_order
    ~token
    ?retry_config
    (request : amend_request) : (Kraken.Kraken_common_types.amend_order_result, string) result Lwt.t =

  (* Check for shutdown before accepting new amendments *)
  if Atomic.get shutdown_requested then
    Lwt.return (Error "Order amendment cancelled due to shutdown")
  else
    with_error_handling ~operation_name:"amend_order" (fun () ->
    match validate_amend_request request with
    | Error err ->
        Logging.error_f ~section "Amend validation failed: %s" err;
        Lwt.return (Error err)
    | Ok () ->
        (* Check if new price is the same as current price to avoid unnecessary amendments *)
        let should_skip_amendment = match request.symbol, request.new_limit_price with
          | Some symbol, Some new_price ->
              (match Kraken.Kraken_executions_feed.get_open_order symbol request.order_id with
               | Some current_order ->
                   (match current_order.limit_price with
                    | Some current_price ->
                        (* Use small epsilon for floating point comparison *)
                        abs_float (new_price -. current_price) < 0.000001
                    | None -> false)
               | None ->
                   Logging.warn_f ~section "Order %s not found in open orders cache, proceeding with amendment" request.order_id;
                   false)
          | _ -> false
        in

        if should_skip_amendment then begin
          Logging.debug_f ~section "Skipping amendment for order %s: new price equals current price" request.order_id;
          (* Return a fake successful result to avoid disrupting the flow *)
          Lwt.return (Ok {
            Kraken.Kraken_common_types.amend_id = "skipped_no_change";
            order_id = request.order_id;
            cl_ord_id = request.cl_ord_id;
          })
        end else begin
          (* Check for duplicate amendments *)
          if not (add_in_flight_amendment request.order_id) then begin
            let err = Printf.sprintf "Duplicate amendment detected for order %s" request.order_id in
            Logging.warn_f ~section "%s" err;
            Lwt.return (Error err)
          end else begin
            Logging.info_f ~section "Amending order %s: %s" request.order_id
              (match request.new_limit_price with Some p -> Printf.sprintf "price=%.2f" p | None -> "");

            let default_retry_config = Kraken.Kraken_actions.default_retry_config in
            let actual_retry_config = Option.value retry_config ~default:default_retry_config in

            Lwt.catch
              (fun () ->
                Kraken.Kraken_actions.amend_order
                  ~token
                  ~order_id:request.order_id
                  ?cl_ord_id:request.cl_ord_id
                  ?order_qty:request.new_quantity
                  ?limit_price:request.new_limit_price
                  ?limit_price_type:request.limit_price_type
                  ?post_only:request.post_only
                  ?trigger_price:request.new_trigger_price
                  ?trigger_price_type:request.trigger_price_type
                  ?display_qty:request.new_display_qty
                  ?deadline:request.deadline
                  ?symbol:request.symbol
                  ~retry_config:actual_retry_config
                  ()
              )
              (fun exn ->
                let exn_str = Printexc.to_string exn in
                (* Check if this is a timeout error that might succeed via execution feed *)
                if string_contains exn_str "Timeout waiting for response" then begin
                  (* WebSocket timeout - add to timed-out amendments tracking for grace period *)
                  let symbol_str = Option.value request.symbol ~default:"" in
                  let grace_period = 5.0 in (* 5 second grace period *)
                  TimedOutAmendments.add_timed_out_amendment
                    request.order_id symbol_str request.new_limit_price request.new_quantity grace_period;

                  Logging.warn_f ~section "Amendment timeout for order %s, starting grace period check (%.1fs) for execution feed confirmation"
                    request.order_id grace_period;

                  (* Check immediately and then periodically during grace period *)
                  let rec check_grace_period remaining_checks =
                    if remaining_checks <= 0 then begin
                      (* Grace period expired - remove tracking and fail *)
                      TimedOutAmendments.remove_timed_out_amendment request.order_id |> ignore;
                      let _ = remove_in_flight_amendment request.order_id in
                      Logging.error_f ~section "Grace period expired for timed-out amendment %s, failing request" request.order_id;
                      Lwt.fail exn
                    end else begin
                      Lwt_unix.sleep 1.0 >>= fun () -> (* Check every 1 second *)
                      (* Check current order state from execution feed *)
                      let confirmed = match Kraken.Kraken_executions_feed.get_open_order symbol_str request.order_id with
                       | Some current_order ->
                           let current_price = current_order.limit_price in
                           let current_qty = current_order.order_qty in
                           TimedOutAmendments.check_order_update request.order_id current_price current_qty
                       | None -> false
                      in
                      if confirmed then begin
                        (* Confirmed via execution feed - treat as success *)
                        let _ = remove_in_flight_amendment request.order_id in
                        Logging.info_f ~section "Timed-out amendment confirmed via execution feed: %s" request.order_id;
                        (* Return a synthetic success result since we can't get the real amend_id *)
                        Lwt.return (Ok {
                          Kraken.Kraken_common_types.amend_id = "confirmed_via_execution_feed";
                          order_id = request.order_id;
                          cl_ord_id = request.cl_ord_id;
                        })
                      end else begin
                        check_grace_period (remaining_checks - 1)
                      end
                    end
                  in
                  check_grace_period (int_of_float grace_period)
                end else begin
                  (* Non-timeout error - remove from in-flight cache and fail immediately *)
                  let _ = remove_in_flight_amendment request.order_id in
                  Lwt.fail exn
                end
              )
            >>= fun result ->
            (* Remove from in-flight cache on success *)
            let _ = remove_in_flight_amendment request.order_id in
            Lwt.return result
          end
        end
  )

(** Cancel orders *)
let cancel_orders
    ~token
    ?retry_config
    (request : cancel_request) : (Kraken.Kraken_common_types.cancel_order_result list, string) result Lwt.t =

  with_error_handling ~operation_name:"cancel_orders" (fun () ->
    match validate_cancel_request request with
    | Error err ->
        Logging.error_f ~section "Cancel validation failed: %s" err;
        Lwt.return (Error err)
    | Ok () ->
        let order_count =
          List.length (Option.value request.order_ids ~default:[]) +
          List.length (Option.value request.cl_ord_ids ~default:[]) +
          List.length (Option.value request.order_userrefs ~default:[])
        in
        Logging.info_f ~section "Cancelling %d orders" order_count;

        let default_retry_config = Kraken.Kraken_actions.default_retry_config in
        let actual_retry_config = Option.value retry_config ~default:default_retry_config in

        Kraken.Kraken_actions.cancel_orders
          ~token
          ?order_ids:request.order_ids
          ?cl_ord_ids:request.cl_ord_ids
          ?order_userrefs:request.order_userrefs
          ~retry_config:actual_retry_config
          ()
  )

(** Convenience function to place a simple market order *)
let place_market_order
    ~token
    ~side ~quantity ~symbol : (Kraken.Kraken_common_types.add_order_result, string) result Lwt.t =

  let request = {
    order_type = "market";
    side;
    quantity;
    symbol;
    limit_price = None;
    time_in_force = None;
    post_only = None;
    margin = None;
    reduce_only = None;
    order_userref = None;
    cl_ord_id = None;
    trigger_price = None;
    trigger_price_type = None;
    display_qty = None;
    fee_preference = None;
    duplicate_key = generate_duplicate_key symbol side quantity None;
  } in

  place_order ~token request

(** Convenience function to place a simple limit order *)
let place_limit_order
    ~token
    ~side ~quantity ~symbol ~price : (Kraken.Kraken_common_types.add_order_result, string) result Lwt.t =

  let request = {
    order_type = "limit";
    side;
    quantity;
    symbol;
    limit_price = Some price;
    time_in_force = None;
    post_only = None;
    margin = None;
    reduce_only = None;
    order_userref = None;
    cl_ord_id = None;
    trigger_price = None;
    trigger_price_type = None;
    display_qty = None;
    fee_preference = None;
    duplicate_key = generate_duplicate_key symbol side quantity (Some price);
  } in

  place_order ~token request

(** Convenience function to cancel a single order by ID *)
let cancel_order
    ~token
    ~order_id : (Kraken.Kraken_common_types.cancel_order_result list, string) result Lwt.t =

  let request = {
    order_ids = Some [order_id];
    cl_ord_ids = None;
    order_userrefs = None;
  } in

  cancel_orders ~token request

(** Convenience function to cancel orders by client order IDs *)
let cancel_orders_by_client_ids
    ~token
    ~cl_ord_ids : (Kraken.Kraken_common_types.cancel_order_result list, string) result Lwt.t =

  let request = {
    order_ids = None;
    cl_ord_ids = Some cl_ord_ids;
    order_userrefs = None;
  } in

  cancel_orders ~token request

(** Close the trading client connection *)
let close () : unit Lwt.t =
  Logging.info ~section "Closing order executor";
  Kraken.Kraken_trading_client.close ()

(** Test interface - exposed for unit testing *)
module Test = struct
  let generate_duplicate_key = generate_duplicate_key
  let is_amendment_in_flight = is_amendment_in_flight
  module InFlightRequests = InFlightRequests
end
