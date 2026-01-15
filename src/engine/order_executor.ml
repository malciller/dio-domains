(** Order Execution Engine

    This module provides a high-level interface for executing trading orders
    through the generic Exchange interface. It handles order placement, modification,
    and cancellation with proper error handling and retry logic, routing requests
    to the appropriate exchange implementation.
*)

open Lwt.Infix

let section = "order_executor"

(** Global shutdown flag for order executor *)
let shutdown_requested = Atomic.make false

(** Signal shutdown to order executor *)
let signal_shutdown () =
  Atomic.set shutdown_requested true

(** In-flight order cache to prevent duplicate orders *)
module InFlightOrders = Dio_strategies.Strategy_common.InFlightOrders

(** In-flight amendment cache to prevent duplicate amendments *)
module InFlightAmendments = Dio_strategies.Strategy_common.InFlightAmendments

(* Open the Exchange Interface Types *)
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

(** Order types *)
type order_type = string
type order_side = string

(** Order request structure *)
type order_request = {
  exchange: string; (* Name of the exchange to use *)
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
  exchange: string;
  order_id: string;
  cl_ord_id: string option;
  new_quantity: float option; (* New quantity for the order *)
  new_limit_price: float option;
  limit_price_type: string option; (* Ignored by generic interface, kept for now or remove if unused *)
  post_only: bool option;
  new_trigger_price: float option;
  trigger_price_type: string option; (* Ignored by generic interface *)
  new_display_qty: float option;
  deadline: string option; (* Ignored by generic interface *)
  symbol: string option; (* Required for some exchanges or precision handling *)
}

(** Cancel request structure *)
type cancel_request = {
  exchange: string;
  order_ids: string list option;
  cl_ord_ids: string list option;
  order_userrefs: int list option;
}

(** Generate a hash key for duplicate order detection *)
let generate_duplicate_key = Dio_strategies.Strategy_common.generate_duplicate_key

(** Validation functions *)
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

(** Check if error is due to disconnected trading client *)
(* TODO: Make this generic, currently somewhat specific to Lwt/Conduit error strings but broad enough *)
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

(** Helper to look up exchange module *)
let get_exchange name =
  match Exchange.Registry.get name with
  | Some m -> Ok m
  | None -> Error (Printf.sprintf "Exchange '%s' not found in registry" name)

(** Convert string types to Exchange types *)
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
  | _ -> Types.Buy (* Default or Error? Existing code defaulted. *)

let parse_time_in_force = function
  | "GTC" -> Types.GTC
  | "IOC" -> Types.IOC
  | "FOK" -> Types.FOK
  | _ -> Types.GTC

(** Convert API results to Kraken types to maintain compatibility if possible, 
    but we should really transition to generic types. 
    However, existing code expects Kraken types if we don't update strategies.
    Wait, strategies call place_order and expect a result.
    If I change result type here, I break strategies.
    The task is "Refactor Engine Components". Strategies come later.
    But to compile, I must return matching types OR update strategies now.
    Strategies use `Kraken_common_types.add_order_result`?
    Let's check `market_maker.ml` imports.
    `market_maker.ml` uses `Dio_engine.Order_executor.place_order`.
    If I change the return type of `place_order`, `market_maker` might break if it inspects the result.
    Actually, `market_maker` usually just checks `Ok/Error`.
    But `amend_order` result has `amend_id`.
    I defined generic results in `Exchange.Types` which look identical to Kraken ones.
    I can alias or map them.
    Since I am refactoring `Order_executor`, I should probably return `Exchange.Types` results.
    And existing strategies might need minor updates if they depend on specific type names, 
    but if the structure is compatible it might just work if OCaml infers structural equality? 
    No, nominal typing.
    I will update `Order_executor` to return `Exchange.Types` results.
    And I will subsequently update strategies to use `Exchange.Types` in the next task step.
    For now, `Order_executor` is the focus.
*)

(** Place a new order *)
let place_order
    ~token
    ?retry_config
    ?(check_duplicate=true)
    (request : order_request) =

  (* Check for shutdown before accepting new orders *)
  if Atomic.get shutdown_requested then
    Lwt.return (Error "Order placement cancelled due to shutdown")
  else
    (* Event-driven cleanup: clean up stale entries when orders are placed *)
    let (_drift, trimmed) = InFlightOrders.cleanup () in
    if trimmed > 0 then
      Logging.debug_f ~section "InFlightOrders registry cleaned up: removed %d stale entries" trimmed;
    with_error_handling ~operation_name:"place_order" (fun () ->
    match validate_order_request request with
    | Error err ->
        Logging.error_f ~section "Order validation failed: %s" err;
        Lwt.return (Error err)
    | Ok () ->
        (* Check for duplicate orders *)
        let is_duplicate = if check_duplicate then not (InFlightOrders.add_in_flight_order request.duplicate_key) else false in
        
        if is_duplicate then begin
          let err = Printf.sprintf "Duplicate order detected: %s %s %f @ %s"
            request.side request.symbol request.quantity
            (match request.limit_price with Some p -> Printf.sprintf "%.2f" p | None -> "market") in
          Logging.warn_f ~section "%s" err;
          Lwt.return (Error err)
        end else begin
          Logging.info_f ~section "Placing %s order on %s: %s %s %f @ %s" 
            request.side request.exchange request.symbol request.order_type request.quantity
            (match request.limit_price with Some p -> Printf.sprintf "%.2f" p | None -> "market");

          match get_exchange request.exchange with
          | Error e -> 
              let _ = InFlightOrders.remove_in_flight_order request.duplicate_key in
              Logging.error_f ~section "%s" e;
              Lwt.return (Error e)
          | Ok (module Ex) ->
              let ex_retry_config = retry_config in

              (* TODO: token is still passed but Generic Exchange might handle auth differently. 
                 Kraken needs token for private generic calls if using WS, or keys if REST. 
                 Kraken_module handles this. `token` arg depends on implementation.
                 For now, `Exchange.place_order` takes `token`.
              *)

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
                  (* Remove from in-flight cache on error *)
                  let _ = InFlightOrders.remove_in_flight_order request.duplicate_key in
                  Lwt.fail exn
                )
              >>= fun result ->
              (* Remove from in-flight cache on success *)
              let _ = InFlightOrders.remove_in_flight_order request.duplicate_key in
              Lwt.return result
        end
  )

(** Amend an existing order *)
let amend_order
    ~token
    ?retry_config
    (request : amend_request) =

  (* Check for shutdown before accepting new amendments *)
  if Atomic.get shutdown_requested then
    Lwt.return (Error "Order amendment cancelled due to shutdown")
  else
    (* Event-driven cleanup: clean up stale entries when orders are amended *)
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
            (* Check if new price is the same as current price to avoid unnecessary amendments *)
            let should_skip_amendment = match request.symbol, request.new_limit_price with
              | Some symbol, Some new_price ->
                  (match Ex.get_open_order ~symbol ~order_id:request.order_id with
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
              (* Remove from in-flight cache since amendment was skipped *)
              let _ = InFlightAmendments.remove_in_flight_amendment request.order_id in
              (* Return a fake successful result to avoid disrupting the flow *)
              Lwt.return (Ok {
                 Types.
                 amend_id = "skipped_no_change";
                 order_id = request.order_id;
                 cl_ord_id = request.cl_ord_id;
              })
            end else begin
                Logging.info_f ~section "Amending order %s on %s: %s" request.order_id request.exchange
                  (match request.new_limit_price with Some p -> Printf.sprintf "price=%.2f" p | None -> "");
                
                let ex_retry_config = retry_config in

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
                    (* Remove from in-flight cache on error *)
                    let _ = InFlightAmendments.remove_in_flight_amendment request.order_id in
                    Lwt.fail exn
                  )
                >>= fun result ->
                (* Remove from in-flight cache on success immediately, just like place_order *)
                let _ = InFlightAmendments.remove_in_flight_amendment request.order_id in
                Lwt.return result
            end
  )

(** Cancel orders *)
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
            (* Event-driven cleanup: clean up stale entries when orders are cancelled *)
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

            Ex.cancel_orders
              ~token
              ?order_ids:request.order_ids
              ?cl_ord_ids:request.cl_ord_ids
              ?order_userrefs:request.order_userrefs
              ?retry_config:ex_retry_config
              ()
  )

(** Close the trading client connection *)
let close () : unit Lwt.t =
  Logging.info ~section "Closing order executor";
  Kraken.Kraken_trading_client.close () 
  (* TODO: This is still tightly coupled to Kraken's client close. 
     We should probably have a generic shutdown or close method in the Registry/Exchange interface?
     Or main.ml handles it. Order_executor might not need to know.
     For now, leaving as-is but it only closes Kraken. 
     If we have multiple exchanges, we need to close all of them.
  *)

(** Initialize the order executor *)
let init : unit Lwt.t =
  Lwt.return_unit
