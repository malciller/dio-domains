(** Order Execution Engine

    Facade for executing trading orders through the generic [Exchange] interface.
    Handles order placement, amendment, and cancellation with request validation,
    duplicate detection via in-flight caches, retry logic for transient transport
    errors, and per-symbol latency profiling. All requests are routed to the
    concrete exchange implementation resolved from [Exchange.Registry].
*)

open Lwt.Infix

let section = "order_executor"

(** Atomic flag set to [true] when graceful shutdown is requested.
    Checked before order placement and amendment to reject new requests. *)
let shutdown_requested = Atomic.make false

(** Sets [shutdown_requested] to [true]. Subsequent place and amend calls
    return [Error] immediately. Cancellations remain permitted. *)
let signal_shutdown () =
  Atomic.set shutdown_requested true

(** Mutex-guarded hashtable of in-flight order keys for duplicate detection. *)
module InFlightOrders = Dio_strategies.Strategy_common.InFlightOrders

(** Mutex-guarded hashtable of in-flight amendment keys for duplicate detection. *)
module InFlightAmendments = Dio_strategies.Strategy_common.InFlightAmendments

(** Per-symbol, per-operation latency profiler cache.
    Keyed by ["symbol:operation"]. Capped at 64 entries; see [get_profiler]. *)
let profilers : (string, Latency_profiler.t) Hashtbl.t = Hashtbl.create 16
let profilers_mutex = Mutex.create ()

let get_profiler symbol operation =
  let key = Printf.sprintf "%s:%s" symbol operation in
  Mutex.lock profilers_mutex;
  let profiler =
    match Hashtbl.find_opt profilers key with
    | Some p -> p
    | None ->
        (* Evict the entry with the lowest sample count to keep table size
           at most 64. Guards against unbounded growth from symbol churn. *)
        if Hashtbl.length profilers >= 64 then begin
          let min_key = Hashtbl.fold (fun k p best ->
            let count = match Latency_profiler.snapshot p with
              | Some snap -> snap.Latency_profiler.samples
              | None -> 0
            in
            match best with
            | None -> Some (k, count)
            | Some (_, s) when count < s -> Some (k, count)
            | some -> some
          ) profilers None in
          Option.iter (fun (k, _) ->
            Logging.warn_f ~section "profilers table at capacity (64); evicting stale entry '%s'" k;
            Hashtbl.remove profilers k
          ) min_key
        end;
        let p = Latency_profiler.create ~bucket_us:1000 ~max_latency_us:2_000_000 key in
        Hashtbl.replace profilers key p;
        p
  in
  Mutex.unlock profilers_mutex;
  profiler

(* Exchange interface module and type aliases. *)
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

(** String aliases for order type and side.
    Parsed into [Types.order_type] and [Types.side] before exchange dispatch. *)
type order_type = string
type order_side = string

(** Parameters for placing a new order via [place_order]. *)
type order_request = {
  exchange: string; (** Exchange name resolved via [Exchange.Registry]. *)
  order_type: order_type;
  side: order_side;
  quantity: float;
  symbol: string;
  limit_price: float option;
  time_in_force: string option; (** One of ["GTC"], ["IOC"], ["FOK"]. *)
  post_only: bool option;
  margin: bool option;
  reduce_only: bool option;
  order_userref: int option;
  cl_ord_id: string option;
  trigger_price: float option;
  trigger_price_type: string option;
  display_qty: float option;
  fee_preference: string option;
  duplicate_key: string; (** Key for [InFlightOrders] duplicate detection. *)
}

(** Parameters for amending an existing order via [amend_order]. *)
type amend_request = {
  exchange: string;
  order_id: string;
  cl_ord_id: string option;
  new_quantity: float option;
  new_limit_price: float option;
  limit_price_type: string option; (** Reserved; unused by generic interface. *)
  post_only: bool option;
  new_trigger_price: float option;
  trigger_price_type: string option; (** Reserved; unused by generic interface. *)
  new_display_qty: float option;
  deadline: string option; (** Reserved; unused by generic interface. *)
  symbol: string option; (** Required by some exchanges for price rounding and routing. *)
}

(** Parameters for cancelling one or more orders via [cancel_orders]. *)
type cancel_request = {
  exchange: string;
  order_ids: string list option;
  cl_ord_ids: string list option;
  order_userrefs: int list option;
  symbol: string option;
}

(** Alias for [Strategy_common.generate_duplicate_key]. Produces a hash key
    from order parameters for [InFlightOrders] duplicate detection. *)
let generate_duplicate_key = Dio_strategies.Strategy_common.generate_duplicate_key

(** Validates required fields and type-specific constraints on an [order_request].
    Returns [Error msg] on the first violated constraint. *)
let validate_order_request (request : order_request) : (unit, string) result =
  if request.exchange = "" then
    Error "Exchange name cannot be empty"
  else if request.quantity <= 0.0 then
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
  if request.exchange = "" then
    Error "Exchange name cannot be empty"
  else if request.order_id = "" then
    Error "Order ID cannot be empty"
  else
    Ok ()

let validate_cancel_request (request : cancel_request) : (unit, string) result =
  if request.exchange = "" then
    Error "Exchange name cannot be empty"
  else
    let has_order_ids = Option.is_some request.order_ids && Option.value request.order_ids ~default:[] <> [] in
    let has_cl_ord_ids = Option.is_some request.cl_ord_ids && Option.value request.cl_ord_ids ~default:[] <> [] in
    let has_userrefs = Option.is_some request.order_userrefs && Option.value request.order_userrefs ~default:[] <> [] in

    if not (has_order_ids || has_cl_ord_ids || has_userrefs) then
      Error "At least one of order_ids, cl_ord_ids, or order_userrefs must be provided"
    else
      Ok ()

(** Returns [true] if [exn_str] matches a transport-level connection failure.
    Checks for common Lwt, Conduit, and TLS error substrings. *)
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
  contains_substring error_str "end_of_file"

(** Wraps [f] with exception handling. On connection errors (as classified by
    [is_connection_error]), retries up to [max_retries] times with [retry_delay]
    seconds between attempts. Non-connection errors return [Error] immediately. *)
let with_error_handling ~operation_name ?(max_retries=3) ?(retry_delay=1.0) f =
  let rec attempt retry_count =
    Lwt.catch
      (fun () -> f ())
      (fun exn ->
        let exn_str = Printexc.to_string exn in
        let err = Printf.sprintf "%s failed: %s" operation_name exn_str in

        (* Retry transient connection errors up to max_retries. *)
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

(** Resolves [name] to a first-class exchange module via [Exchange.Registry]. *)
let get_exchange name =
  match Exchange.Registry.get name with
  | Some m -> Ok m
  | None -> Error (Printf.sprintf "Exchange '%s' not found in registry" name)

(** Parses a string to [Types.order_type]. Unrecognized values map to [Types.Other]. *)
let parse_order_type = function
  | "limit" -> Types.Limit
  | "market" -> Types.Market
  | "stop-loss" -> Types.StopLoss
  | "take-profit" -> Types.TakeProfit
  | "stop-loss-limit" -> Types.StopLossLimit
  | "take-profit-limit" -> Types.TakeProfitLimit
  | "settl-position" -> Types.SettlPosition
  | s -> Types.Other s

let parse_side = function
  | "buy" -> Types.Buy
  | "sell" -> Types.Sell
  | _ -> Types.Buy (* Default: [Buy] for unrecognized input. *)

let parse_time_in_force = function
  | "GTC" -> Types.GTC
  | "IOC" -> Types.IOC
  | "FOK" -> Types.FOK
  | _ -> Types.GTC

(* NOTE: All return types use [Exchange.Types]. Strategies must depend on
   [Exchange.Types], not exchange-specific types (OCaml nominal typing). *)

(** Places a new order on the target exchange.

    Flow: shutdown check, in-flight cleanup, validation, duplicate detection
    via [InFlightOrders], exchange dispatch, latency profiling.
    Returns [Error] if shutdown is active, validation fails, or a duplicate
    key is already in-flight. *)
let place_order
    ~token
    ?retry_config
    ?(check_duplicate=true)
    (request : order_request) =

  (* Block new placements during graceful shutdown. *)
  if Atomic.get shutdown_requested then
    Lwt.return (Error "Order placement cancelled due to shutdown")
  else
    (* Trim stale in-flight entries before each placement attempt. *)
    let (_drift, trimmed) = InFlightOrders.cleanup () in
    if trimmed > 0 then
      Logging.debug_f ~section "InFlightOrders registry cleaned up: removed %d stale entries" trimmed;
    with_error_handling ~operation_name:"place_order" (fun () ->
    match validate_order_request request with
    | Error err ->
        Logging.error_f ~section "Order validation failed: %s" err;
        Lwt.return (Error err)
    | Ok () ->
        (* Duplicate detection: add returns false if key already exists. *)
        let is_duplicate = if check_duplicate then not (InFlightOrders.add_in_flight_order request.duplicate_key) else false in
        
        if is_duplicate then begin
          let err = Printf.sprintf "Duplicate order detected: %s %s %f @ %s"
            request.side request.symbol request.quantity
            (match request.limit_price with Some p -> Printf.sprintf "%.2f" p | None -> "market") in
          Logging.warn_f ~section "%s" err;
          Lwt.return (Error err)
        end else begin
          Logging.debug_f ~section "Placing %s order on %s: %s %s %f @ %s" 
            request.side request.exchange request.symbol request.order_type request.quantity
            (match request.limit_price with Some p -> Printf.sprintf "%.2f" p | None -> "market");

          match get_exchange request.exchange with
          | Error e -> 
              let _ = InFlightOrders.remove_in_flight_order request.duplicate_key in
              Logging.error_f ~section "%s" e;
              Lwt.return (Error e)
          | Ok (module Ex) ->
              let ex_retry_config = retry_config in

              (* Token semantics are exchange-dependent. Each module
                 interprets the value per its authentication mechanism. *)

              let profiler = get_profiler request.symbol "place" in
              let start_time = Mtime_clock.now_ns () in

              Lwt.catch
                (fun () ->
                  Ex.place_order
                    ~token
                    ~order_type:(parse_order_type request.order_type)
                    ~side:(parse_side request.side)
                    ~qty:request.quantity
                    ~symbol:request.symbol
                    ?limit_price:request.limit_price
                    ?time_in_force:(Option.map parse_time_in_force request.time_in_force)
                    ?post_only:request.post_only
                    ?reduce_only:request.reduce_only
                    ?order_userref:request.order_userref
                    ?cl_ord_id:request.cl_ord_id
                    ?trigger_price:request.trigger_price
                    ?display_qty:request.display_qty
                    ?retry_config:ex_retry_config
                    ()
                )
                (fun exn ->
                  (* Clear duplicate key from in-flight cache on exception. *)
                  let _ = InFlightOrders.remove_in_flight_order request.duplicate_key in
                  Lwt.fail exn
                )
              >>= fun result ->
              let stop_time = Mtime_clock.now_ns () in
              let span = Mtime.Span.of_uint64_ns (Int64.sub stop_time start_time) in
              Latency_profiler.record profiler span;
              Latency_profiler.report ~sample_threshold:10000 profiler;
              Lwt.return result
        end
  )

(** Amends an existing order on the target exchange.

    Performs no-op suppression: if the new price rounds to the same value
    as the current price (per exchange tick size), the amendment is skipped
    and a sentinel [amend_id = "skipped_no_change"] is returned.
    Manages [InFlightAmendments] lifecycle and profiles latency. *)
let amend_order
    ~token
    ?retry_config
    (request : amend_request) =

  (* Block amendments during graceful shutdown. *)
  if Atomic.get shutdown_requested then
    Lwt.return (Error "Order amendment cancelled due to shutdown")
  else
    (* Trim stale in-flight amendment entries before each attempt. *)
    let (_drift, trimmed) = InFlightAmendments.cleanup () in
    if trimmed > 0 then
      Logging.debug_f ~section "InFlightAmendments registry cleaned up: removed %d stale entries" trimmed;
    with_error_handling ~operation_name:"amend_order" (fun () ->
    match validate_amend_request request with
    | Error err ->
        Logging.error_f ~section "Amend validation failed: %s" err;
        Lwt.return (Error err)
    | Ok () ->
        match get_exchange request.exchange with
        | Error e -> 
            Logging.error_f ~section "%s" e;
            Lwt.return (Error e)
        | Ok (module Ex) ->
            (* No-op suppression: skip if exchange-rounded prices match.
               Log full-precision requested price for diagnostics. *)
            (match request.new_limit_price with
             | Some p -> Logging.debug_f ~section "Amendment price check for order %s: requested=%.10f" request.order_id p
             | None -> ());
            let should_skip_amendment = match request.symbol, request.new_limit_price with
              | Some symbol, Some new_price ->
                  (match Ex.get_open_order ~symbol ~order_id:request.order_id with
                   | Some current_order ->
                       (match current_order.limit_price with
                        | Some current_price ->
                            (* Compare prices after exchange-specific tick rounding. *)
                            let rounded_new_price = Ex.round_price ~symbol ~price:new_price in
                            let rounded_current_price = Ex.round_price ~symbol ~price:current_price in
                            let diff = abs_float (rounded_new_price -. rounded_current_price) in
                            (* Skip if difference is below epsilon (1e-9). *)
                            diff < 0.000000001
                        | None ->
                            false)
                   | None ->
                       (* Order absent from local WS cache. This does not imply
                          the order is invalid: WS lag or cache churn during a
                          prior amend can cause temporary absence. Proceed and
                          let the exchange reject if the order no longer exists.
                          Skipping here caused Kraken buy orders to fail trailing
                          upward. Hyperliquid avoids this via cancel-replace,
                          keeping the old ID in cache until the amend completes. *)
                       Logging.debug_f ~section "Order %s not found in open orders cache, proceeding with amendment (exchange will reject if invalid)" request.order_id;
                       false)
              | _ -> false
            in

            if should_skip_amendment then begin

              (* Clear in-flight entry; amendment was suppressed as a no-op. *)
              let _ = InFlightAmendments.remove_in_flight_amendment request.order_id in
              
              (* Return sentinel [amend_id = "skipped_no_change"] so the
                 supervisor routes to [handle_order_amendment_skipped] rather
                 than [handle_order_amended], preserving tracking state. *)
              Lwt.return (Ok {
                 Types.
                 original_order_id = request.order_id;
                 new_order_id = request.order_id;
                 amend_id = Some "skipped_no_change";
                 cl_ord_id = request.cl_ord_id;
              })
            end else begin
                Logging.debug_f ~section "Amending order %s on %s: %s" request.order_id request.exchange
                  (match request.new_limit_price with Some p -> Printf.sprintf "price=%.10f" p | None -> "");
                
                let ex_retry_config = retry_config in

                let profiler = match request.symbol with
                  | Some sym -> get_profiler sym "amend"
                  | None -> get_profiler "unknown" "amend"
                in
                let start_time = Mtime_clock.now_ns () in

                Lwt.catch
                  (fun () ->
                    Ex.amend_order
                      ~token
                      ~order_id:request.order_id
                      ?cl_ord_id:request.cl_ord_id
                      ?qty:request.new_quantity
                      ?limit_price:request.new_limit_price
                      ?post_only:request.post_only
                      ?trigger_price:request.new_trigger_price
                      ?display_qty:request.new_display_qty
                      ?symbol:request.symbol
                      ?retry_config:ex_retry_config
                      ()
                  )
                  (fun exn ->
                    (* Clear in-flight entry on exception. *)
                    let _ = InFlightAmendments.remove_in_flight_amendment request.order_id in
                    Lwt.fail exn
                  )
                >>= fun result ->
                let stop_time = Mtime_clock.now_ns () in
                let span = Mtime.Span.of_uint64_ns (Int64.sub stop_time start_time) in
                Latency_profiler.record profiler span;
                Latency_profiler.report ~sample_threshold:10000 profiler;
                (* Clear in-flight entry after successful exchange response. *)
                let _ = InFlightAmendments.remove_in_flight_amendment request.order_id in
                Lwt.return result
            end
  )

(** Cancels one or more orders on the target exchange.

    At least one identifier list ([order_ids], [cl_ord_ids], or
    [order_userrefs]) must be non-empty. Triggers [InFlightOrders]
    cleanup and profiles latency. Not blocked by shutdown. *)
let cancel_orders
    ~token
    ?retry_config
    (request : cancel_request) =

  with_error_handling ~operation_name:"cancel_orders" (fun () ->
    match validate_cancel_request request with
    | Error err ->
        Logging.error_f ~section "Cancel validation failed: %s" err;
        Lwt.return (Error err)
    | Ok () ->
        match get_exchange request.exchange with
        | Error e -> 
            Logging.error_f ~section "%s" e;
            Lwt.return (Error e)
        | Ok (module Ex) ->
            (* Trim stale in-flight entries before each cancellation attempt. *)
            let (_drift, trimmed) = InFlightOrders.cleanup () in
            if trimmed > 0 then
              Logging.debug_f ~section "InFlightOrders registry cleaned up: removed %d stale entries" trimmed;

            let order_count =
              List.length (Option.value request.order_ids ~default:[]) +
              List.length (Option.value request.cl_ord_ids ~default:[]) +
              List.length (Option.value request.order_userrefs ~default:[])
            in
            Logging.info_f ~section "Cancelling %d orders on %s" order_count request.exchange;

            let ex_retry_config = retry_config in

            let profiler = match request.symbol with
              | Some sym -> get_profiler sym "cancel"
              | None -> get_profiler request.exchange "cancel"
            in
            let start_time = Mtime_clock.now_ns () in

            Ex.cancel_orders
              ~token
              ?order_ids:request.order_ids
              ?cl_ord_ids:request.cl_ord_ids
              ?order_userrefs:request.order_userrefs
              ?symbol:request.symbol
              ?retry_config:ex_retry_config
              ()
            >>= fun result ->
            let stop_time = Mtime_clock.now_ns () in
            let span = Mtime.Span.of_uint64_ns (Int64.sub stop_time start_time) in
            Latency_profiler.record profiler span;
            Latency_profiler.report ~sample_threshold:10000 profiler;
            Lwt.return result
  )

(** Closes all underlying trading client connections (Kraken and Hyperliquid).
    Invoked during graceful shutdown to tear down WebSocket transports,
    fail pending request waiters, and release subscriber streams. *)
let close () : unit Lwt.t =
  Logging.info ~section "Closing order executor";
  Lwt.join [
    Kraken.Kraken_trading_client.close ();
    Hyperliquid.Ws.close ();
  ]

(** No-op initialization. Reserved for future setup (e.g., exchange
    pre-connection, cache warming). *)
let init : unit Lwt.t =
  Lwt.return_unit
