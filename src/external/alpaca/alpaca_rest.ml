(** Alpaca REST API Client. Handles authenticated HTTP requests to paper/live endpoints. *)

open Lwt.Infix
open Alpaca_types

let section = "alpaca_rest"

(** H7: Alpaca has no retry layer of its own (the executor now delegates all
    retries to the exchange modules, a single policy). Connection-level HTTP
    exceptions are retried here with a short backoff so transient network
    failures don't fail the order; exchange-level rejections (HTTP 4xx) are
    returned to the caller untouched. *)
let retry_http_exceptions ~f =
  Error_handling.retry_with_backoff
    ~section
    ~config:Error_handling.default_retry_config
    ~f
    ~is_retriable_override:(fun e ->
      match Error_handling.classify e with
      | Error_handling.Connection -> true
      | _ -> false)
    ()
;;

let make_headers () =
  let key = Config.api_key () in
  let secret = Config.api_secret () in
  Cohttp.Header.of_list
    [ "APCA-API-KEY-ID", key
    ; "APCA-API-SECRET-KEY", secret
    ; "Content-Type", "application/json"
    ; "Accept", "application/json"
    ]
;;

(** Records an Alpaca REST round trip in the "alpaca" venue profiler. *)
let record_rest_span span = Network_latency.record_rest "alpaca" span

let json_to_float = function
  | `Float f -> f
  | `Int i -> float_of_int i
  | `String s ->
    (try float_of_string s with
     | _ -> 0.0)
  | _ -> 0.0
;;

let json_to_float_opt = function
  | `Float f -> Some f
  | `Int i -> Some (float_of_int i)
  | `String s ->
    (try Some (float_of_string s) with
     | _ -> None)
  | _ -> None
;;

let parse_order_json json =
  let open Yojson.Safe.Util in
  let id = json |> member "id" |> to_string_option |> Option.value ~default:"" in
  let client_order_id = json |> member "client_order_id" |> to_string_option in
  let symbol = json |> member "symbol" |> to_string_option |> Option.value ~default:"" in
  let side_str =
    json |> member "side" |> to_string_option |> Option.value ~default:"buy"
  in
  let side =
    try side_of_string side_str with
    | _ -> Buy
  in
  let qty = json |> member "qty" |> json_to_float in
  let filled_qty = json |> member "filled_qty" |> json_to_float in
  let type_str = json |> member "type" |> to_string_option |> Option.value ~default:"" in
  let status_str =
    json |> member "status" |> to_string_option |> Option.value ~default:""
  in
  let status = status_of_string status_str in
  let limit_price = json |> member "limit_price" |> json_to_float_opt in
  let created_at =
    json |> member "created_at" |> to_string_option |> Option.value ~default:""
  in
  { id
  ; client_order_id
  ; symbol
  ; side
  ; qty
  ; filled_qty
  ; type_str
  ; side_str
  ; status
  ; limit_price
  ; created_at
  }
;;

(** Compute the effective time-in-force and extended-hours eligibility for an
    order given the market session. Returns (tif_str, mark_extended).

    Alpaca session rules for US equities:
    - Regular hours (9:30 AM - 4:00 PM ET): GTC/IOC/FOK/DAY are accepted. The
      requested TIF is preserved and no [extended_hours] flag is sent (IOC/FOK
      cannot carry [extended_hours], and GTC + [extended_hours] requires account
      enablement).
    - Extended (pre/post-market) and overnight sessions: only [limit] orders are
      accepted and only with TIF [day] (or [gtc] when GTC-for-extended is enabled
      on the account). Every requested TIF is downgraded to [day] with
      [extended_hours=true] so the order is accepted unconditionally, executes in
      the current session, carries through the upcoming sessions, and cancels at
      8:00 PM ET.
    - Crypto (24/7) is never marked extended-hours eligible. *)
let effective_tif_and_extended
      ~is_crypto
      ~is_fractional
      ~order_type
      ~time_in_force
      ~in_extended_session
      ~use_extended
  =
  let type_str =
    match order_type with
    | "limit" -> "limit"
    | _ -> "market"
  in
  let mark_extended =
    (not is_crypto) && use_extended && in_extended_session && type_str = "limit"
  in
  let tif_str =
    if mark_extended
    then "day"
    else if is_fractional && not is_crypto
    then "day"
    else (
      match time_in_force with
      | Some "GTC" | Some "gtc" -> "gtc"
      | Some "IOC" | Some "ioc" -> "ioc"
      | Some "FOK" | Some "fok" -> "fok"
      | Some "DAY" | Some "day" -> "day"
      | _ -> "gtc")
  in
  tif_str, mark_extended
;;

let place_order
      ~symbol
      ~qty
      ~side
      ~order_type
      ?limit_price
      ?time_in_force
      ?cl_ord_id
      ?extended_hours
      ()
  =
  let base_url = Config.rest_base_url () in
  let url = Uri.of_string (base_url ^ "/v2/orders") in
  let is_crypto = String.contains symbol '/' in
  let is_fractional = Float.floor qty <> qty in
  let use_extended =
    match extended_hours with
    | Some b -> b
    | None -> !Config.extended_hours
  in
  (* Session-aware: outside the regular session (pre/after-market, overnight, or
     closed) orders must be day + extended_hours to execute in the current
     session; during regular hours the requested TIF is preserved. *)
  let in_extended_session =
    (not is_crypto) && not (Alpaca_market_hours.is_regular_market_open ())
  in
  let tif_str, mark_extended =
    effective_tif_and_extended
      ~is_crypto
      ~is_fractional
      ~order_type
      ~time_in_force
      ~in_extended_session
      ~use_extended
  in
  let side_str =
    match side with
    | Buy -> "buy"
    | Sell -> "sell"
  in
  let type_str =
    match order_type with
    | "limit" -> "limit"
    | _ -> "market"
  in
  let assoc =
    [ "symbol", `String symbol
    ; "qty", `String (Printf.sprintf "%.9f" qty)
    ; "side", `String side_str
    ; "type", `String type_str
    ; "time_in_force", `String tif_str
    ]
  in
  let assoc = if mark_extended then ("extended_hours", `Bool true) :: assoc else assoc in
  let assoc =
    match limit_price with
    | Some p -> ("limit_price", `String (Printf.sprintf "%.4f" p)) :: assoc
    | None -> assoc
  in
  let assoc =
    match cl_ord_id with
    | Some cid -> ("client_order_id", `String cid) :: assoc
    | None -> assoc
  in
  let req_body = `Assoc assoc |> Yojson.Safe.to_string in
  let headers = make_headers () in
  Logging.debug_f
    ~section
    "Placing Alpaca order: %s %s %.6f %s (TIF=%s%s)"
    side_str
    symbol
    qty
    (match limit_price with
     | Some p -> Printf.sprintf "@ %.4f" p
     | None -> "MKT")
    tif_str
    (if mark_extended then ", extended_hours=true" else "");
  retry_http_exceptions ~f:(fun () ->
    Lwt.catch
      (fun () ->
         let rest_start = Mtime_clock.now_ns () in
         Cohttp_lwt_unix.Client.post
           ~headers
           ~body:(Cohttp_lwt.Body.of_string req_body)
           url
         >>= fun (resp, body) ->
         let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
         Cohttp_lwt.Body.to_string body
         >>= fun body_str ->
         record_rest_span
           (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) rest_start));
         if status_code >= 200 && status_code < 300
         then (
           try
             let json = Yojson.Safe.from_string body_str in
             let ord = parse_order_json json in
             Logging.debug_f
               ~section
               "Placed Alpaca order %s [%s %s %.6f]: status=%s"
               ord.id
               ord.symbol
               ord.side_str
               ord.qty
               (string_of_status ord.status);
             let userref =
               match ord.client_order_id with
               | Some cid ->
                 (try Some (int_of_string cid) with
                  | _ -> None)
               | None -> None
             in
             Lwt.return
               (Ok
                  { order_id = ord.id
                  ; cl_ord_id = ord.client_order_id
                  ; order_userref = userref
                  })
           with
           | exn ->
             let err =
               Printf.sprintf
                 "Failed to parse place_order response: %s"
                 (Printexc.to_string exn)
             in
             Logging.error_f ~section "%s (body: %s)" err body_str;
             Lwt.return (Error err))
         else (
           Logging.error_f ~section "Place order failed HTTP %d: %s" status_code body_str;
           Lwt.return (Error (Printf.sprintf "HTTP %d: %s" status_code body_str))))
      (fun exn ->
         (* Connection-class exceptions are re-raised so the retry layer
              above handles them; anything else converts to an Error. *)
         let exn_str = Printexc.to_string exn in
         match Error_handling.classify exn_str with
         | Error_handling.Connection | Error_handling.Timeout -> Lwt.fail exn
         | _ ->
           let err = Printf.sprintf "Place order HTTP exception: %s" exn_str in
           Logging.error_f ~section "%s" err;
           Lwt.return (Error err)))
;;

let amend_order ~order_id ?qty ?limit_price ?cl_ord_id () =
  let base_url = Config.rest_base_url () in
  let url = Uri.of_string (Printf.sprintf "%s/v2/orders/%s" base_url order_id) in
  let assoc = [] in
  let assoc =
    match qty with
    | Some q when Float.floor q = q -> ("qty", `String (Printf.sprintf "%.9f" q)) :: assoc
    | _ -> assoc
  in
  let assoc =
    match limit_price with
    | Some p -> ("limit_price", `String (Printf.sprintf "%.4f" p)) :: assoc
    | None -> assoc
  in
  let assoc =
    match cl_ord_id with
    | Some cid -> ("client_order_id", `String cid) :: assoc
    | None -> assoc
  in
  let req_body = `Assoc assoc |> Yojson.Safe.to_string in
  let headers = make_headers () in
  Logging.debug_f ~section "Amending Alpaca order %s" order_id;
  retry_http_exceptions ~f:(fun () ->
    Lwt.catch
      (fun () ->
         let rest_start = Mtime_clock.now_ns () in
         Cohttp_lwt_unix.Client.patch
           ~headers
           ~body:(Cohttp_lwt.Body.of_string req_body)
           url
         >>= fun (resp, body) ->
         let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
         Cohttp_lwt.Body.to_string body
         >>= fun body_str ->
         record_rest_span
           (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) rest_start));
         if status_code >= 200 && status_code < 300
         then (
           try
             let json = Yojson.Safe.from_string body_str in
             let ord = parse_order_json json in
             Logging.debug_f
               ~section
               "Amended Alpaca order %s -> %s [%s]"
               order_id
               ord.id
               ord.symbol;
             Lwt.return
               (Ok
                  { original_order_id = order_id
                  ; new_order_id = ord.id
                  ; amend_id = Some ord.id
                  ; cl_ord_id = ord.client_order_id
                  })
           with
           | exn ->
             let err =
               Printf.sprintf
                 "Failed to parse amend_order response: %s"
                 (Printexc.to_string exn)
             in
             Logging.error_f ~section "%s (body: %s)" err body_str;
             Lwt.return (Error err))
         else (
           Logging.error_f
             ~section
             "Amend order failed HTTP %d for %s: %s"
             status_code
             order_id
             body_str;
           Lwt.return (Error (Printf.sprintf "HTTP %d: %s" status_code body_str))))
      (fun exn ->
         let exn_str = Printexc.to_string exn in
         match Error_handling.classify exn_str with
         | Error_handling.Connection | Error_handling.Timeout -> Lwt.fail exn
         | _ ->
           let err = Printf.sprintf "Amend order HTTP exception: %s" exn_str in
           Logging.error_f ~section "%s" err;
           Lwt.return (Error err)))
;;

let cancel_order order_id =
  let base_url = Config.rest_base_url () in
  let url = Uri.of_string (Printf.sprintf "%s/v2/orders/%s" base_url order_id) in
  let headers = make_headers () in
  Logging.debug_f ~section "Cancelling Alpaca order %s" order_id;
  retry_http_exceptions ~f:(fun () ->
    Lwt.catch
      (fun () ->
         let rest_start = Mtime_clock.now_ns () in
         Cohttp_lwt_unix.Client.delete ~headers url
         >>= fun (resp, body) ->
         let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
         Cohttp_lwt.Body.to_string body
         >>= fun body_str ->
         record_rest_span
           (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) rest_start));
         if status_code >= 200 && status_code < 300
         then (
           Logging.debug_f ~section "Cancelled Alpaca order %s" order_id;
           Lwt.return (Ok [ { order_id; cl_ord_id = None } ]))
         else (
           Logging.error_f
             ~section
             "Cancel order failed HTTP %d for %s: %s"
             status_code
             order_id
             body_str;
           Lwt.return
             (Error
                (Printf.sprintf "HTTP %d cancelling %s: %s" status_code order_id body_str))))
      (fun exn ->
         let exn_str = Printexc.to_string exn in
         match Error_handling.classify exn_str with
         | Error_handling.Connection | Error_handling.Timeout -> Lwt.fail exn
         | _ ->
           let err = Printf.sprintf "Cancel order HTTP exception: %s" exn_str in
           Logging.error_f ~section "%s" err;
           Lwt.return (Error err)))
;;

let get_open_orders () =
  let base_url = Config.rest_base_url () in
  let headers = make_headers () in
  let rec fetch_all acc until_id =
    let url_str =
      match until_id with
      | None -> base_url ^ "/v2/orders?status=open&nested=false&limit=500&direction=asc"
      | Some uid ->
        Printf.sprintf
          "%s/v2/orders?status=open&nested=false&limit=500&direction=asc&after=%s"
          base_url
          uid
    in
    let url = Uri.of_string url_str in
    let rest_start = Mtime_clock.now_ns () in
    Cohttp_lwt_unix.Client.get ~headers url
    >>= fun (resp, body) ->
    let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
    Cohttp_lwt.Body.to_string body
    >>= fun body_str ->
    record_rest_span
      (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) rest_start));
    if status_code >= 200 && status_code < 300
    then (
      try
        let json = Yojson.Safe.from_string body_str in
        let items =
          match json with
          | `List l -> l
          | _ -> []
        in
        let orders = List.map parse_order_json items in
        let count = List.length orders in
        let combined = acc @ orders in
        if count >= 500
        then (
          match List.nth_opt orders (count - 1) with
          | Some last_ord -> fetch_all combined (Some last_ord.created_at)
          | None -> Lwt.return (Ok combined))
        else (
          Logging.debug_f
            ~section
            "Retrieved %d open orders from Alpaca"
            (List.length combined);
          Lwt.return (Ok combined))
      with
      | exn ->
        let err =
          Printf.sprintf
            "Failed to parse open orders response: %s"
            (Printexc.to_string exn)
        in
        Logging.error_f ~section "%s (body: %s)" err body_str;
        Lwt.return (Error err))
    else (
      Logging.error_f ~section "Get open orders failed HTTP %d: %s" status_code body_str;
      Lwt.return
        (Error (Printf.sprintf "HTTP %d getting open orders: %s" status_code body_str)))
  in
  Lwt.catch
    (fun () -> fetch_all [] None)
    (fun exn ->
       let err =
         Printf.sprintf "Get open orders HTTP exception: %s" (Printexc.to_string exn)
       in
       Logging.error_f ~section "%s" err;
       Lwt.return (Error err))
;;

let get_account () =
  let base_url = Config.rest_base_url () in
  let url = Uri.of_string (base_url ^ "/v2/account") in
  let headers = make_headers () in
  Lwt.catch
    (fun () ->
       let rest_start = Mtime_clock.now_ns () in
       Cohttp_lwt_unix.Client.get ~headers url
       >>= fun (resp, body) ->
       let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
       Cohttp_lwt.Body.to_string body
       >>= fun body_str ->
       record_rest_span
         (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) rest_start));
       if status_code >= 200 && status_code < 300
       then (
         try
           let json = Yojson.Safe.from_string body_str in
           let open Yojson.Safe.Util in
           let id = json |> member "id" |> to_string_option |> Option.value ~default:"" in
           let status =
             json |> member "status" |> to_string_option |> Option.value ~default:""
           in
           let currency =
             json |> member "currency" |> to_string_option |> Option.value ~default:""
           in
           let buying_power = json |> member "buying_power" |> json_to_float in
           let cash = json |> member "cash" |> json_to_float in
           let portfolio_value = json |> member "portfolio_value" |> json_to_float in
           let equity = json |> member "equity" |> json_to_float in
           Lwt.return
             (Ok { id; status; currency; buying_power; cash; portfolio_value; equity })
         with
         | exn ->
           let err =
             Printf.sprintf
               "Failed to parse account response: %s"
               (Printexc.to_string exn)
           in
           Logging.error_f ~section "%s (body: %s)" err body_str;
           Lwt.return (Error err))
       else (
         Logging.error_f ~section "Get account failed HTTP %d: %s" status_code body_str;
         Lwt.return
           (Error (Printf.sprintf "HTTP %d getting account: %s" status_code body_str))))
    (fun exn ->
       let err =
         Printf.sprintf "Get account HTTP exception: %s" (Printexc.to_string exn)
       in
       Logging.error_f ~section "%s" err;
       Lwt.return (Error err))
;;

let get_positions () =
  let base_url = Config.rest_base_url () in
  let url = Uri.of_string (base_url ^ "/v2/positions") in
  let headers = make_headers () in
  Lwt.catch
    (fun () ->
       let rest_start = Mtime_clock.now_ns () in
       Cohttp_lwt_unix.Client.get ~headers url
       >>= fun (resp, body) ->
       let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
       Cohttp_lwt.Body.to_string body
       >>= fun body_str ->
       record_rest_span
         (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) rest_start));
       if status_code >= 200 && status_code < 300
       then (
         try
           let json = Yojson.Safe.from_string body_str in
           let open Yojson.Safe.Util in
           let items =
             match json with
             | `List l -> l
             | _ -> []
           in
           let positions =
             List.map
               (fun j ->
                  { asset_id =
                      j
                      |> member "asset_id"
                      |> to_string_option
                      |> Option.value ~default:""
                  ; symbol =
                      j |> member "symbol" |> to_string_option |> Option.value ~default:""
                  ; exchange =
                      j
                      |> member "exchange"
                      |> to_string_option
                      |> Option.value ~default:""
                  ; qty = j |> member "qty" |> json_to_float
                  ; market_value = j |> member "market_value" |> json_to_float
                  ; avg_entry_price = j |> member "avg_entry_price" |> json_to_float
                  ; current_price = j |> member "current_price" |> json_to_float
                  ; side =
                      j |> member "side" |> to_string_option |> Option.value ~default:""
                  })
               items
           in
           Lwt.return (Ok positions)
         with
         | exn ->
           let err =
             Printf.sprintf
               "Failed to parse positions response: %s"
               (Printexc.to_string exn)
           in
           Logging.error_f ~section "%s (body: %s)" err body_str;
           Lwt.return (Error err))
       else (
         Logging.error_f ~section "Get positions failed HTTP %d: %s" status_code body_str;
         Lwt.return
           (Error (Printf.sprintf "HTTP %d getting positions: %s" status_code body_str))))
    (fun exn ->
       let err =
         Printf.sprintf "Get positions HTTP exception: %s" (Printexc.to_string exn)
       in
       Logging.error_f ~section "%s" err;
       Lwt.return (Error err))
;;
