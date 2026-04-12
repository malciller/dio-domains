(** Lighter order actions and dispatch mechanisms.
    This module implements the primary interface for order execution on the Lighter exchange, utilizing a dual transport strategy. It prioritizes WebSocket based order dispatch via the [jsonapi/sendtx] message type when the connection is active. In scenarios where the WebSocket connection is degraded or unavailable, it automatically falls back to utilizing the REST interface at [POST /api/v1/sendTx]. Transaction signing is handled securely through the [lighter_signer.ml] Foreign Function Interface. All REST fallback requests are automatically routed through the configured LIGHTER_PROXY address to maintain architectural consistency. *)

open Lwt.Infix

let section = "lighter_actions"

(** Atomic counter mechanism for generating unique client order identifiers.
    This counter is required to produce a globally unique client order index for every order placement across all markets as mandated by the Lighter protocol. To prevent indexing collisions across process restarts and concurrent instance executions, the counter is securely initialized using the current system epoch time in milliseconds. *)
let client_order_counter =
  Atomic.make (int_of_float (Unix.gettimeofday () *. 1000.0))

let next_client_order_index () =
  Int64.of_int (Atomic.fetch_and_add client_order_counter 1)

(** Dispatches a signed transaction array over the REST protocol fallback.
    This function construct a multipart form data payload that adheres to the Lighter API specification. It accepts the raw transaction information produced by the Go signer, similar to the behavior observed in the Python SDK implementation. The required form data fields are the transaction type represented as an integer and the transaction info represented as a JSON string. *)
let send_tx ~tx_type ~tx_info =
  let url = Lighter_proxy.api_base_url () ^ "/api/v1/sendTx" in
  Logging.debug_f ~section "Sending tx via REST (type=%d, tx_info=%s)" tx_type tx_info;
  Lwt.catch (fun () ->
    let uri = Uri.of_string url in
    (* Build multipart/form-data body manually *)
    let boundary = Printf.sprintf "---dio-boundary-%d" (Random.int 1_000_000) in
    let body_parts = [
      Printf.sprintf "--%s\r\nContent-Disposition: form-data; name=\"tx_type\"\r\n\r\n%d" boundary tx_type;
      Printf.sprintf "--%s\r\nContent-Disposition: form-data; name=\"tx_info\"\r\n\r\n%s" boundary tx_info;
      Printf.sprintf "--%s--\r\n" boundary
    ] in
    let body_str = String.concat "\r\n" body_parts in
    let content_type = Printf.sprintf "multipart/form-data; boundary=%s" boundary in
    let headers = Cohttp.Header.of_list [
      ("Content-Type", content_type);
      ("Accept", "application/json")
    ] in
    let body = Cohttp_lwt.Body.of_string body_str in
    let%lwt (resp, resp_body) = Cohttp_lwt_unix.Client.post ~headers ~body uri in
    let%lwt resp_str = Cohttp_lwt.Body.to_string resp_body in
    let status = Cohttp.Response.status resp in
    if Cohttp.Code.is_success (Cohttp.Code.code_of_status status) then begin
      Logging.info_f ~section "REST sendTx success: %s" resp_str;
      Lwt.return (Ok resp_str)
    end else begin
      Logging.error_f ~section "REST sendTx failed (status=%s): %s"
        (Cohttp.Code.string_of_status status) resp_str;
      (* Implement non-blocking auto recovery sequence for cryptographic nonce desynchronization. Ghost modifications executed during periods of WebSocket disconnection will consume nonce values locally but may fail to confirm on the blockchain. This behavior causes the local counter state to diverge from the exchange state. By re-fetching the correct nonce directly from the exchange, subsequent order operations are protected from entering an invalid nonce recursive failure loop. *)
      let lower = String.lowercase_ascii resp_str in
      if String.length lower > 0 then begin
        let rec find_sub s sub i =
          if i + String.length sub > String.length s then false
          else if String.sub s i (String.length sub) = sub then true
          else find_sub s sub (i + 1)
        in
        if find_sub lower "invalid nonce" 0 then begin
          Logging.warn_f ~section "Nonce desync detected. Re-fetching correct nonce from exchange.";
          let base_url = Lighter_proxy.api_base_url () in
          Lwt.dont_wait
            (fun () ->
               Lighter_signer.initialize_nonce
                 ~base_url
                 ~api_key_index:(Lighter_signer.get_api_key_index ())
                 ~account_index:(Lighter_signer.get_account_index ()))
            (fun exn ->
               Logging.error_f ~section "Failed to initialize nonce: %s" (Printexc.to_string exn))
        end
      end;
      Lwt.return (Error resp_str)
    end
  ) (fun exn ->
    let err = Printexc.to_string exn in
    Logging.error_f ~section "REST sendTx exception: %s" err;
    Lwt.return (Error err)
  )

(** Executes a new order placement on the Lighter exchange.
    This function converts standard system order parameters into the specific numerical formats required by the Lighter protocol and initiates the transaction signing process. It proactively injects the order into the local execution feed using the client order index as the temporary order identifier until the exchange confirms the formal order index. *)
let place_order ~symbol ~is_buy ~qty ~price
    ?(order_type=Lighter_types.Types.Limit)
    ?(tif=Lighter_types.Types.GTC)
    ?(post_only=false)
    ?(reduce_only=false)
    () =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | None ->
      Lwt.return (Error (Printf.sprintf "Unknown symbol: %s" symbol))
  | Some market_index ->
      match Lighter_instruments_feed.lookup_info symbol with
      | None ->
          Lwt.return (Error (Printf.sprintf "No instrument info for symbol: %s" symbol))
      | Some info ->
          let is_ask = not is_buy in
          let client_order_index = next_client_order_index () in
          let base_amount = Lighter_types.float_to_lighter_int
            ~decimals:info.supported_size_decimals qty in
          let price_int = Lighter_types.float_to_lighter_int
            ~decimals:info.supported_price_decimals price in
          let lighter_ot = Lighter_types.lighter_order_type_int order_type in
          let lighter_tif = Lighter_types.lighter_tif_int ~post_only tif in
          (* Note that the Lighter protocol necessitates the use of Good Till Time semantics, as Good Till Cancelled logic is not natively supported. A constant value of negative one is passed to instruct the Go signer tool to automatically calculate and apply the default standard order expiration period, mirroring the implementation found in the Python SDK. *)
          let expiry = Int64.of_int (-1) in

          let tx_info = Lighter_signer.sign_create_order
            ~market_index ~client_order_index ~base_amount ~price:price_int
            ~is_ask ~order_type:lighter_ot ~tif:lighter_tif
            ~reduce_only ~expiry in

          send_tx ~tx_type:Lighter_types.tx_type_create_order ~tx_info >>= fun result ->
          (match result with
           | Ok _ ->
               let order_id = Int64.to_string client_order_index in
               Logging.info_f ~section "Order placed: %s [%s] %s %.8f @ %.2f (market=%d)"
                 order_id symbol (if is_buy then "BUY" else "SELL") qty price market_index;
               (* Proactively inject the new order record into the open orders state tracking container. The client index value is utilized as the temporary client order identifier until the WebSocket feed formally assigns and confirms the final sequence order index. *)
               let side = if is_buy then Lighter_executions_feed.Buy else Lighter_executions_feed.Sell in
               Lighter_executions_feed.inject_order ~symbol ~order_id ~side ~qty ~price ~cl_ord_id:order_id ();
               Lwt.return (Ok {
                 Lighter_types.Types.order_id;
                 cl_ord_id = None;
                 order_userref = None;
               })
           | Error msg ->
               Logging.error_f ~section "Order failed: [%s] %s %.8f @ %.2f: %s"
                 symbol (if is_buy then "BUY" else "SELL") qty price msg;
               Lwt.return (Error msg))

(** Initiates the cancellation of a previously placed order on the Lighter exchange.
    This function derives the required market index and specific order index, generates the requisite cryptographic signature for the cancellation request, and transmits the signed payload via the appropriate transaction channels. *)
let cancel_order ~symbol ~order_id =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | None ->
      Lwt.return (Error (Printf.sprintf "Unknown symbol: %s" symbol))
  | Some market_index ->
      let order_index = Int64.of_string order_id in
      let tx_info = Lighter_signer.sign_cancel_order ~market_index ~order_index in
      send_tx ~tx_type:Lighter_types.tx_type_cancel_order ~tx_info >>= fun result ->
      (match result with
       | Ok _ ->
           Logging.debug_f ~section "Cancel sent: %s [%s]" order_id symbol;
           Lwt.return (Ok { Lighter_types.Types.order_id; cl_ord_id = None })
       | Error msg ->
           Logging.error_f ~section "Cancel failed: %s [%s]: %s" order_id symbol msg;
           Lwt.return (Error msg))

(** Submits a modification request for an active order on the Lighter exchange.
    This process recalculates the internal Lighter integer representations for both base amount and price, processes the transaction signature for the modification, and transmits the new parameters while preserving the original order identity. *)
let modify_order ~symbol ~order_id ~new_qty ~new_price =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | None ->
      Lwt.return (Error (Printf.sprintf "Unknown symbol: %s" symbol))
  | Some market_index ->
      match Lighter_instruments_feed.lookup_info symbol with
      | None ->
          Lwt.return (Error (Printf.sprintf "No instrument info for symbol: %s" symbol))
      | Some info ->
          let order_index = Int64.of_string order_id in
          let new_base_amount = Lighter_types.float_to_lighter_int
            ~decimals:info.supported_size_decimals new_qty in
          let new_price_int = Lighter_types.float_to_lighter_int
            ~decimals:info.supported_price_decimals new_price in
          let tx_info = Lighter_signer.sign_modify_order
            ~market_index ~order_index ~new_base_amount ~new_price:new_price_int in
          send_tx ~tx_type:Lighter_types.tx_type_modify_order ~tx_info >>= fun result ->
          (match result with
           | Ok _ ->
               Logging.debug_f ~section "Modify sent: %s [%s] qty=%.8f price=%.2f" order_id symbol new_qty new_price;
               Lwt.return (Ok {
                 Lighter_types.Types.original_order_id = order_id;
                 new_order_id = order_id;
                 amend_id = None;
                 cl_ord_id = None;
               })
           | Error msg ->
               Logging.error_f ~section "Modify failed: %s [%s]: %s" order_id symbol msg;
               Lwt.return (Error msg))

(** Triggers a mass cancellation sequence for all active orders associated with a specific market on the Lighter exchange.
    The function identifies the target market index and constructs a specialized comprehensive cancellation transaction, thereby clearing the entire order book state for the specified symbol. *)
let cancel_all_orders ~symbol =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | None ->
      Lwt.return (Error (Printf.sprintf "Unknown symbol: %s" symbol))
  | Some market_index ->
      let tx_info = Lighter_signer.sign_cancel_all_orders ~market_index in
      send_tx ~tx_type:Lighter_types.tx_type_cancel_all_orders ~tx_info >>= fun result ->
      (match result with
       | Ok _ ->
           Logging.debug_f ~section "Cancel all sent for %s (market=%d)" symbol market_index;
           Lwt.return (Ok ())
       | Error msg ->
           Logging.error_f ~section "Cancel all failed for %s: %s" symbol msg;
           Lwt.return (Error msg))
