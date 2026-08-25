(** Order actions for Lighter: place/cancel/modify via signed transactions.
    Dispatch goes through REST [POST /api/v1/sendTx] via the configured
    LIGHTER_PROXY; signing happens in [lighter_signer.ml] over FFI. (The WS
    [jsonapi/sendtx] path in [lighter_ws.ml] exists but is currently unused.) *)

open Lwt.Infix

let section = "lighter_actions"

(** Client order index counter. Lighter requires a unique client order index
    per placement across all markets; seeded from epoch millis so restarts do
    not collide with previously issued indices. *)
let client_order_counter = Atomic.make (int_of_float (Unix.gettimeofday () *. 1000.0))

let next_client_order_index () =
  Int64.of_int (Atomic.fetch_and_add client_order_counter 1)
;;

(** Submits a signed transaction over REST as multipart form data, matching the
    Python SDK: fields [tx_type] (integer) and [tx_info] (JSON string produced
    by the Go signer). *)
let send_tx ~tx_type ~tx_info =
  let url = Lighter_proxy.api_base_url () ^ "/api/v1/sendTx" in
  Logging.debug_f ~section "Sending tx via REST (type=%d, tx_info=%s)" tx_type tx_info;
  Lwt.catch
    (fun () ->
       let uri = Uri.of_string url in
       let boundary = Printf.sprintf "---dio-boundary-%d" (Random.int 1_000_000) in
       let body_parts =
         [ Printf.sprintf
             "--%s\r\nContent-Disposition: form-data; name=\"tx_type\"\r\n\r\n%d"
             boundary
             tx_type
         ; Printf.sprintf
             "--%s\r\nContent-Disposition: form-data; name=\"tx_info\"\r\n\r\n%s"
             boundary
             tx_info
         ; Printf.sprintf "--%s--\r\n" boundary
         ]
       in
       let body_str = String.concat "\r\n" body_parts in
       let content_type = Printf.sprintf "multipart/form-data; boundary=%s" boundary in
       let headers =
         Cohttp.Header.of_list
           [ "Content-Type", content_type; "Accept", "application/json" ]
       in
       let body = Cohttp_lwt.Body.of_string body_str in
       let rest_start = Mtime_clock.now_ns () in
       let%lwt resp, resp_body = Cohttp_lwt_unix.Client.post ~headers ~body uri in
       let%lwt resp_str = Cohttp_lwt.Body.to_string resp_body in
       Network_latency.record_rest
         "lighter"
         (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) rest_start));
       let status = Cohttp.Response.status resp in
       if Cohttp.Code.is_success (Cohttp.Code.code_of_status status)
       then (
         Logging.info_f ~section "REST sendTx success: %s" resp_str;
         Lwt.return (Ok resp_str))
       else (
         Logging.error_f
           ~section
           "REST sendTx failed (status=%s): %s"
           (Cohttp.Code.string_of_status status)
           resp_str;
         (* Auto-recovery for nonce desync: operations sent during WS outages
             consume nonces locally but may never confirm on chain, diverging
             the local counter from the exchange. Re-fetch the authoritative
             nonce so later orders do not loop on invalid-nonce rejections. *)
         let lower = String.lowercase_ascii resp_str in
         if String.length lower > 0
         then
           if Error_handling.string_contains lower "invalid nonce"
           then (
             Logging.warn_f
               ~section
               "Nonce desync detected. Re-fetching correct nonce from exchange.";
             let base_url = Lighter_proxy.api_base_url () in
             Lwt.dont_wait
               (fun () ->
                  Lighter_signer.initialize_nonce
                    ~base_url
                    ~api_key_index:(Lighter_signer.get_api_key_index ())
                    ~account_index:(Lighter_signer.get_account_index ()))
               (fun exn ->
                  Logging.error_f
                    ~section
                    "Failed to initialize nonce: %s"
                    (Printexc.to_string exn)));
         Lwt.return (Error resp_str)))
    (fun exn ->
       let err = Printexc.to_string exn in
       Logging.error_f ~section "REST sendTx exception: %s" err;
       Lwt.return (Error err))
;;

(** Places a new order: converts params to Lighter's integer formats, signs the
    create-order tx, and pre-registers the order in the local execution feed
    keyed by client order index until the exchange assigns its own order id. *)
let place_order
      ~symbol
      ~is_buy
      ~qty
      ~price
      ?(order_type = Lighter_types.Types.Limit)
      ?(tif = Lighter_types.Types.GTC)
      ?(post_only = false)
      ?(reduce_only = false)
      ()
  =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | None -> Lwt.return (Error (Printf.sprintf "Unknown symbol: %s" symbol))
  | Some market_index ->
    (match Lighter_instruments_feed.lookup_info symbol with
     | None ->
       Lwt.return (Error (Printf.sprintf "No instrument info for symbol: %s" symbol))
     | Some info ->
       let is_ask = not is_buy in
       let client_order_index = next_client_order_index () in
       let base_amount =
         Lighter_types.float_to_lighter_int ~decimals:info.supported_size_decimals qty
       in
       let price_int =
         Lighter_types.float_to_lighter_int ~decimals:info.supported_price_decimals price
       in
       let lighter_ot = Lighter_types.lighter_order_type_int order_type in
       let lighter_tif = Lighter_types.lighter_tif_int ~post_only tif in
       (* Lighter has no native GTC; expiry follows GTT semantics. Passing -1
           tells the Go signer to compute the default order expiry, matching
           the Python SDK. *)
       let expiry = Int64.of_int (-1) in
       let tx_info =
         Lighter_signer.sign_create_order
           ~market_index
           ~client_order_index
           ~base_amount
           ~price:price_int
           ~is_ask
           ~order_type:lighter_ot
           ~tif:lighter_tif
           ~reduce_only
           ~expiry
       in
       send_tx ~tx_type:Lighter_types.tx_type_create_order ~tx_info
       >>= fun result ->
       (match result with
        | Ok _ ->
          let order_id = Int64.to_string client_order_index in
          Logging.info_f
            ~section
            "Order placed: %s [%s] %s %.8f @ %.2f (market=%d)"
            order_id
            symbol
            (if is_buy then "BUY" else "SELL")
            qty
            price
            market_index;
          (* Pre-register the order under the client index; the WS feed later
             confirms the exchange-assigned order index. *)
          let side =
            if is_buy then Lighter_executions_feed.Buy else Lighter_executions_feed.Sell
          in
          Lighter_executions_feed.inject_order
            ~symbol
            ~order_id
            ~side
            ~qty
            ~price
            ~cl_ord_id:order_id
            ();
          Lwt.return
            (Ok { Lighter_types.Types.order_id; cl_ord_id = None; order_userref = None })
        | Error msg ->
          Logging.error_f
            ~section
            "Order failed: [%s] %s %.8f @ %.2f: %s"
            symbol
            (if is_buy then "BUY" else "SELL")
            qty
            price
            msg;
          Lwt.return (Error msg)))
;;

(** Cancels an order: resolves market/order indices, signs, and submits the tx. *)
let cancel_order ~symbol ~order_id =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | None -> Lwt.return (Error (Printf.sprintf "Unknown symbol: %s" symbol))
  | Some market_index ->
    let order_index = Int64.of_string order_id in
    let tx_info = Lighter_signer.sign_cancel_order ~market_index ~order_index in
    send_tx ~tx_type:Lighter_types.tx_type_cancel_order ~tx_info
    >>= fun result ->
    (match result with
     | Ok _ ->
       Logging.debug_f ~section "Cancel sent: %s [%s]" order_id symbol;
       Lwt.return (Ok { Lighter_types.Types.order_id; cl_ord_id = None })
     | Error msg ->
       Logging.error_f ~section "Cancel failed: %s [%s]: %s" order_id symbol msg;
       Lwt.return (Error msg))
;;

(** Modifies an order's qty/price: converts to Lighter integers, signs, and
    submits while keeping the original order id. *)
let modify_order ~symbol ~order_id ~new_qty ~new_price =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | None -> Lwt.return (Error (Printf.sprintf "Unknown symbol: %s" symbol))
  | Some market_index ->
    (match Lighter_instruments_feed.lookup_info symbol with
     | None ->
       Lwt.return (Error (Printf.sprintf "No instrument info for symbol: %s" symbol))
     | Some info ->
       let order_index = Int64.of_string order_id in
       let new_base_amount =
         Lighter_types.float_to_lighter_int ~decimals:info.supported_size_decimals new_qty
       in
       let new_price_int =
         Lighter_types.float_to_lighter_int
           ~decimals:info.supported_price_decimals
           new_price
       in
       let tx_info =
         Lighter_signer.sign_modify_order
           ~market_index
           ~order_index
           ~new_base_amount
           ~new_price:new_price_int
       in
       send_tx ~tx_type:Lighter_types.tx_type_modify_order ~tx_info
       >>= fun result ->
       (match result with
        | Ok _ ->
          Logging.debug_f
            ~section
            "Modify sent: %s [%s] qty=%.8f price=%.2f"
            order_id
            symbol
            new_qty
            new_price;
          Lwt.return
            (Ok
               { Lighter_types.Types.original_order_id = order_id
               ; new_order_id = order_id
               ; amend_id = None
               ; cl_ord_id = None
               })
        | Error msg ->
          Logging.error_f ~section "Modify failed: %s [%s]: %s" order_id symbol msg;
          Lwt.return (Error msg)))
;;

(** Cancels all open orders in one market: resolves the market index and
    submits a signed cancel-all tx. *)
let cancel_all_orders ~symbol =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | None -> Lwt.return (Error (Printf.sprintf "Unknown symbol: %s" symbol))
  | Some market_index ->
    let tx_info = Lighter_signer.sign_cancel_all_orders ~market_index in
    send_tx ~tx_type:Lighter_types.tx_type_cancel_all_orders ~tx_info
    >>= fun result ->
    (match result with
     | Ok _ ->
       Logging.debug_f ~section "Cancel all sent for %s (market=%d)" symbol market_index;
       Lwt.return (Ok ())
     | Error msg ->
       Logging.error_f ~section "Cancel all failed for %s: %s" symbol msg;
       Lwt.return (Error msg))
;;
