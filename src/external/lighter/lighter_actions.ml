(** Lighter order actions.
    WebSocket-first order dispatch with REST fallback.
    Signs transactions via [lighter_signer.ml] FFI, then sends via WS
    [jsonapi/sendtx] when connected, falling back to REST [POST /api/v1/sendTx]
    when WebSocket is unavailable. REST fallback routes through the proxy
    when LIGHTER_PROXY is configured. *)

open Lwt.Infix

let section = "lighter_actions"

(** Atomic counter for generating unique client_order_index values.
    Initialized from current epoch milliseconds to avoid collisions with
    indexes used by previous engine runs. Lighter requires client_order_index
    to be globally unique across all markets. *)
let client_order_counter =
  Atomic.make (int_of_float (Unix.gettimeofday () *. 1000.0))

let next_client_order_index () =
  Int64.of_int (Atomic.fetch_and_add client_order_counter 1)

(** Send a signed transaction via REST.
    The Python SDK passes the raw Go signer tx_info output unchanged to the
    REST endpoint (confirmed from signer_client.py create_order → send_tx).
    Lighter expects multipart/form-data with fields:
      tx_type=<int>, tx_info=<json-string> *)
let send_tx ~tx_type ~tx_info =
  let url = Lighter_proxy.api_base_url () ^ "/api/v1/sendTx" in
  Logging.info_f ~section "Sending tx via REST (type=%d, tx_info=%s)" tx_type tx_info;
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
      (* Auto-recover from nonce desync. Ghost modifies during WS disconnect
         consume nonces locally but may fail on-chain, causing the local
         counter to diverge. Re-fetch the correct nonce from the exchange
         so subsequent operations don't get stuck in an "invalid nonce" loop. *)
      let lower = String.lowercase_ascii resp_str in
      if String.length lower > 0 then begin
        let rec find_sub s sub i =
          if i + String.length sub > String.length s then false
          else if String.sub s i (String.length sub) = sub then true
          else find_sub s sub (i + 1)
        in
        if find_sub lower "invalid nonce" 0 then begin
          Logging.warn_f ~section "Nonce desync detected — re-fetching correct nonce from exchange";
          let base_url = Lighter_proxy.api_base_url () in
          Lwt.async (fun () ->
            Lighter_signer.initialize_nonce
              ~base_url
              ~api_key_index:(Lighter_signer.get_api_key_index ())
              ~account_index:(Lighter_signer.get_account_index ()))
        end
      end;
      Lwt.return (Error resp_str)
    end
  ) (fun exn ->
    let err = Printexc.to_string exn in
    Logging.error_f ~section "REST sendTx exception: %s" err;
    Lwt.return (Error err)
  )

(** Place a new order on Lighter. *)
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
          (* Lighter requires GTT (Good Till Time) — no GTC support.
             Pass -1 to let the Go signer compute the default expiry
             (Python SDK: DEFAULT_28_DAY_ORDER_EXPIRY = -1). *)
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
               (* Proactively inject into open orders (client index = cl_ord_id until WS assigns order_index). *)
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

(** Cancel an order on Lighter. *)
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
           Logging.info_f ~section "Cancel sent: %s [%s]" order_id symbol;
           Lwt.return (Ok { Lighter_types.Types.order_id; cl_ord_id = None })
       | Error msg ->
           Logging.error_f ~section "Cancel failed: %s [%s]: %s" order_id symbol msg;
           Lwt.return (Error msg))

(** Modify an existing order on Lighter. *)
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
               Logging.info_f ~section "Modify sent: %s [%s] qty=%.8f price=%.2f" order_id symbol new_qty new_price;
               Lwt.return (Ok {
                 Lighter_types.Types.original_order_id = order_id;
                 new_order_id = order_id;
                 amend_id = None;
                 cl_ord_id = None;
               })
           | Error msg ->
               Logging.error_f ~section "Modify failed: %s [%s]: %s" order_id symbol msg;
               Lwt.return (Error msg))

(** Cancel all orders for a market on Lighter. *)
let cancel_all_orders ~symbol =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | None ->
      Lwt.return (Error (Printf.sprintf "Unknown symbol: %s" symbol))
  | Some market_index ->
      let tx_info = Lighter_signer.sign_cancel_all_orders ~market_index in
      send_tx ~tx_type:Lighter_types.tx_type_cancel_all_orders ~tx_info >>= fun result ->
      (match result with
       | Ok _ ->
           Logging.info_f ~section "Cancel all sent for %s (market=%d)" symbol market_index;
           Lwt.return (Ok ())
       | Error msg ->
           Logging.error_f ~section "Cancel all failed for %s: %s" symbol msg;
           Lwt.return (Error msg))
