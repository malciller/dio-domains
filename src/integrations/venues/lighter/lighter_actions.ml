[@@@ocaml.text
  " Order actions for Lighter: signed transactions go over the private WebSocket\n\
  \    ([jsonapi/sendtx]) when that socket is connected, falling back to REST [sendTx]\n\
  \    through the non-US egress relay otherwise. One writer owns signing and dispatch\n\
  \    ([lighter_outbox.ml]) so nonces reach the exchange in allocation order without\n\
  \    callers waiting on a round-trip. "]

open Lwt.Infix

let section = "lighter_actions"

let tx_timeout_s = 10.0
[@@ocaml.doc
  " Budget for one REST [sendTx] round-trip. The outbox writer is serialised, so an\n\
  \    unanswered request would wedge every queued tx. "]
;;

let client_order_counter = Atomic.make (int_of_float (Unix.gettimeofday () *. 1000.0))
[@@ocaml.doc
  " Client order index counter. Lighter requires a unique client order index per placement\n\
  \    across all markets; seeded from epoch millis so restarts do not collide with\n\
  \    previously issued indices. "]
;;

let next_client_order_index () =
  Int64.of_int (Atomic.fetch_and_add client_order_counter 1)
;;

(** Lighter answers [sendTx] with HTTP 200 whatever the outcome; the body's [code] is the
    real result. A non-200 code is a rejection and must not be reported as a placement. *)
let classify_send_tx_reply body =
  let code_and_message =
    try
      let json = Yojson.Safe.from_string body in
      let open Yojson.Safe.Util in
      let code =
        match member "code" json with
        | `Int n -> Some n
        | `Float f -> Some (int_of_float f)
        | `String s ->
          (try Some (int_of_string (String.trim s)) with
           | _ -> None)
        | _ -> None
      in
      let message =
        match member "message" json with
        | `String s -> s
        | _ -> ""
      in
      Option.map (fun c -> c, message) code
    with
    | _ -> None
  in
  match code_and_message with
  | Some (code, _) when code = 200 -> Ok ()
  | Some (code, message) ->
    let err =
      if String.equal message ""
      then Printf.sprintf "sendTx rejected by the venue (code=%d)" code
      else Printf.sprintf "sendTx rejected by the venue (code=%d): %s" code message
    in
    Logging.error_f ~section "%s (body=%s)" err body;
    Error err
  | None -> Ok ()
;;

let send_tx ~tx_type ~tx_info =
  let url = Lighter_endpoints.api_base_url ^ "/api/v1/sendTx" in
  Logging.debug_f
    ~section
    "Sending tx via REST (type=%d, tx_info=%s, connect_host=%s)"
    tx_type
    tx_info
    (Lighter_endpoints.private_rest_connect_host ());
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
      let __ppx_lwt_0 =
        Cohttp_lwt_unix.Client.post
          ~connect_host:(Lighter_endpoints.private_rest_connect_host ())
          ~headers
          ~body
          uri
      in
      Lwt.backtrace_bind
        "src/integrations/venues/lighter/lighter_actions.ml"
        48
        (fun exn ->
          try Lwt.reraise exn with
          | exn -> exn)
        __ppx_lwt_0
        (fun (resp, resp_body) ->
          let __ppx_lwt_0 = Cohttp_lwt.Body.to_string resp_body in
          Lwt.backtrace_bind
            "src/integrations/venues/lighter/lighter_actions.ml"
            49
            (fun exn ->
              try Lwt.reraise exn with
              | exn -> exn)
            __ppx_lwt_0
            (fun resp_str ->
              Network_latency.record_rest
                "lighter"
                (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) rest_start));
              let status = Cohttp.Response.status resp in
              if Cohttp.Code.is_success (Cohttp.Code.code_of_status status)
              then (
                Logging.info_f ~section "REST sendTx success: %s" resp_str;
                Lwt.return
                  (match classify_send_tx_reply resp_str with
                   | Ok () -> Ok resp_str
                   | Error _ as e -> e))
              else (
                Logging.error_f
                  ~section
                  "REST sendTx failed (status=%s): %s"
                  (Cohttp.Code.string_of_status status)
                  resp_str;
                Lwt.return (Error resp_str)))))
    (fun exn ->
      let err = Printexc.to_string exn in
      Logging.error_f ~section "REST sendTx exception: %s" err;
      Lwt.return (Error err))
[@@ocaml.doc
  " Submits a signed transaction over REST as multipart form data, matching the Python\n\
  \    SDK: fields [tx_type] (integer) and [tx_info] (JSON string produced by the Go \
   signer).\n\
  \    Callers must go through [submit]: signing allocates the nonce and Lighter requires\n\
  \    nonces to reach the exchange in ascending order. "]
;;

(** Lighter rejects any tx whose nonce is not strictly greater than the last one it
    accepted, so the nonce must be allocated in the order the requests reach the exchange.
    Every submission goes through [outbox]: one writer signs and sends, so allocation
    order is wire order and queued txs do not wait on a round-trip. *)
type tx =
  { tx_type : int
  ; id : string option
  ; sign : unit -> string
  }

let is_nonce_error msg =
  Error_handling.string_contains (String.lowercase_ascii msg) "invalid nonce"
;;

let resync_nonce () =
  Lighter_signer.initialize_nonce
    ~timeout_s:2.0
    ~base_url:Lighter_endpoints.api_base_url
    ~api_key_index:(Lighter_signer.get_api_key_index ())
    ~account_index:(Lighter_signer.get_account_index ())
    ()
;;

(** Signs and dispatches queued txs: WS when the private socket is up, REST otherwise. A
    WS frame that fails on a now-dead socket falls through to REST. REST rejections are
    retried once after a nonce resync; a nonce rejection means the tx was never applied,
    so the replay is safe. There is no WS equivalent — the frame either left or it did
    not. *)
let max_nonce_attempts = 2

(* Lighter's API reference gives the batch cap as both 15 and 50; stay on the lower bound
   rather than risk 21514 rejecting a whole ladder. *)
let max_tx_batch = 15

let rec dispatch_rest ~tx_type ~sign n =
  let tx_info = sign () in
  Lwt.catch
    (fun () -> Lwt_unix.with_timeout tx_timeout_s (fun () -> send_tx ~tx_type ~tx_info))
    (fun exn ->
      let err =
        match exn with
        | Lwt_unix.Timeout -> Printf.sprintf "sendTx timed out after %.1fs" tx_timeout_s
        | exn -> Printexc.to_string exn
      in
      Logging.error_f ~section "REST sendTx exception: %s" err;
      Lwt.return (Error err))
  >>= function
  | Error msg when n < max_nonce_attempts && is_nonce_error msg ->
    Logging.warn_f
      ~section
      "Nonce rejected (attempt %d/%d), resyncing: %s"
      n
      max_nonce_attempts
      msg;
    resync_nonce () >>= fun () -> dispatch_rest ~tx_type ~sign (n + 1)
  | result -> Lwt.return result
;;

let dispatch_batch (txs : tx list) =
  let rest_all () =
    Lwt_list.map_s (fun (t : tx) -> dispatch_rest ~tx_type:t.tx_type ~sign:t.sign 1) txs
  in
  match txs with
  | [] -> Lwt.return []
  | _ :: _ when not (Lighter_ws.private_connected ()) -> rest_all ()
  | _ ->
    let signed = List.map (fun (t : tx) -> t, t.sign ()) txs in
    let frames = List.map (fun ((t : tx), info) -> t.tx_type, info, t.id) signed in
    Lighter_ws.send_txs_ws frames
    >>= fun results ->
    (* A failed write drops the socket, so a result list carrying errors and no live
       private leg means the frame never left: replay over REST. A venue *rejection* on a
       connected socket is final and must not be replayed. *)
    if (not (Lighter_ws.private_connected ())) && List.exists Result.is_error results
    then (
      Logging.warn_f
        ~section
        "WS tx frame failed on a dropped socket, falling back to REST";
      rest_all ())
    else Lwt.return results
;;

let outbox = Lighter_outbox.create ~max_batch:max_tx_batch ~send:dispatch_batch ()

let submit ?id ~tx_type ~sign () = Lighter_outbox.submit outbox { tx_type; id; sign }
[@@ocaml.doc " Enqueues a signed-tx thunk on the ordered outbox. "]
;;

let place_order
  ~symbol
  ~is_buy
  ~qty
  ~price
  ?(order_type = Lighter_types.Types.Limit)
  ?(tif = Lighter_types.Types.GTC)
  ?(post_only = false)
  ?(reduce_only = false)
  ?order_userref
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
       let client_order_id = Int64.to_string client_order_index in
       (match order_userref with
        | Some u -> Lighter_executions_feed.register_userref ~client_order_id ~userref:u
        | None -> ());
       let base_amount =
         Lighter_types.float_to_lighter_int ~decimals:info.supported_size_decimals qty
       in
       let price_int =
         Lighter_types.float_to_lighter_int ~decimals:info.supported_price_decimals price
       in
       let lighter_ot = Lighter_types.lighter_order_type_int order_type in
       let lighter_tif = Lighter_types.lighter_tif_int ~post_only tif in
       let expiry = Int64.of_int (-1) in
       let sign () =
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
       (* Registered before submission so a venue rejection can retire the order locally
          even when it arrives after this call has returned. *)
       Lighter_ws.register_create_order_tx ~id:client_order_id;
       submit ~id:client_order_id ~tx_type:Lighter_types.tx_type_create_order ~sign ()
       >>= fun result ->
       (match result with
        | Ok _ ->
          let order_id = client_order_id in
          Logging.info_f
            ~section
            "Order placed: %s [%s] %s %.8f @ %.2f (market=%d)"
            order_id
            symbol
            (if is_buy then "BUY" else "SELL")
            qty
            price
            market_index;
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
            ?order_userref
            ();
          Lwt.return
            (Ok { Lighter_types.Types.order_id; cl_ord_id = None; order_userref })
        | Error msg ->
          Lighter_executions_feed.forget_userref client_order_id;
          Logging.error_f
            ~section
            "Order failed: [%s] %s %.8f @ %.2f: %s"
            symbol
            (if is_buy then "BUY" else "SELL")
            qty
            price
            msg;
          Lwt.return (Error msg)))
[@@ocaml.doc
  " Places a new order: converts params to Lighter's integer formats, signs the\n\
  \    create-order tx, and pre-registers the order in the local execution feed keyed by\n\
  \    client order index until the exchange assigns its own order id. "]
;;

let cancel_order ~symbol ~order_id =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | None -> Lwt.return (Error (Printf.sprintf "Unknown symbol: %s" symbol))
  | Some market_index ->
    let order_index = Int64.of_string order_id in
    let sign () = Lighter_signer.sign_cancel_order ~market_index ~order_index in
    (* Namespaced so a cancel reply can never be mistaken for the create-order tx that
       shares the order id. *)
    submit ~id:(order_id ^ ":cancel") ~tx_type:Lighter_types.tx_type_cancel_order ~sign ()
    >>= fun result ->
    (match result with
     | Ok _ ->
       Logging.debug_f ~section "Cancel sent: %s [%s]" order_id symbol;
       Lwt.return (Ok { Lighter_types.Types.order_id; cl_ord_id = None })
     | Error msg ->
       Logging.error_f ~section "Cancel failed: %s [%s]: %s" order_id symbol msg;
       Lwt.return (Error msg))
[@@ocaml.doc
  " Cancels an order: resolves market/order indices, signs, and submits the tx. "]
;;

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
       let sign () =
         Lighter_signer.sign_modify_order
           ~market_index
           ~order_index
           ~new_base_amount
           ~new_price:new_price_int
       in
       (* Namespaced so a modify ack is never confused with the create-order tx that
          shares the order id; only create-order tx ids may retire an order on rejection. *)
       submit
         ~id:(order_id ^ ":modify")
         ~tx_type:Lighter_types.tx_type_modify_order
         ~sign
         ()
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
[@@ocaml.doc
  " Modifies an order's qty/price: converts to Lighter integers, signs, and submits while\n\
  \    keeping the original order id. "]
;;

let cancel_all_orders ~symbol =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | None -> Lwt.return (Error (Printf.sprintf "Unknown symbol: %s" symbol))
  | Some market_index ->
    let sign () = Lighter_signer.sign_cancel_all_orders ~market_index in
    submit ~tx_type:Lighter_types.tx_type_cancel_all_orders ~sign ()
    >>= fun result ->
    (match result with
     | Ok _ ->
       Logging.debug_f ~section "Cancel all sent for %s (market=%d)" symbol market_index;
       Lwt.return (Ok ())
     | Error msg ->
       Logging.error_f ~section "Cancel all failed for %s: %s" symbol msg;
       Lwt.return (Error msg))
[@@ocaml.doc
  " Cancels all open orders in one market: resolves the market index and submits a signed\n\
  \    cancel-all tx. "]
;;

(** A snapshot-confirmed miss must be cancelled at the venue, not just dropped from local
    tracking: if the snapshot was partial the order is still live, and an orphaned
    reduce-only close blocks every replacement for that position. *)
let () =
  Lighter_executions_feed.set_venue_cancel_hook (fun symbol order_id ->
    Lwt.async (fun () ->
      cancel_order ~symbol ~order_id
      >|= fun result ->
      match result with
      | Ok _ ->
        Logging.info_f
          ~section
          "Reconcile cancel sent at the venue: %s [%s]"
          order_id
          symbol
      | Error msg ->
        Logging.warn_f
          ~section
          "Reconcile cancel failed at the venue: %s [%s]: %s"
          order_id
          symbol
          msg))
;;
