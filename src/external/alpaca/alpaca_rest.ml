(** Alpaca REST API Client. Handles authenticated HTTP requests to paper/live endpoints. *)

open Lwt.Infix
open Alpaca_types

let section = "alpaca_rest"

let make_headers () =
  let key = Config.api_key () in
  let secret = Config.api_secret () in
  Cohttp.Header.of_list [
    ("APCA-API-KEY-ID", key);
    ("APCA-API-SECRET-KEY", secret);
    ("Content-Type", "application/json");
    ("Accept", "application/json");
  ]

let json_to_float = function
  | `Float f -> f
  | `Int i -> float_of_int i
  | `String s -> (try float_of_string s with _ -> 0.0)
  | _ -> 0.0

let json_to_float_opt = function
  | `Float f -> Some f
  | `Int i -> Some (float_of_int i)
  | `String s -> (try Some (float_of_string s) with _ -> None)
  | _ -> None

let parse_order_json json =
  let open Yojson.Safe.Util in
  let id = json |> member "id" |> to_string_option |> Option.value ~default:"" in
  let client_order_id = json |> member "client_order_id" |> to_string_option in
  let symbol = json |> member "symbol" |> to_string_option |> Option.value ~default:"" in
  let side_str = json |> member "side" |> to_string_option |> Option.value ~default:"buy" in
  let side = (try side_of_string side_str with _ -> Buy) in
  let qty = json |> member "qty" |> json_to_float in
  let filled_qty = json |> member "filled_qty" |> json_to_float in
  let type_str = json |> member "type" |> to_string_option |> Option.value ~default:"" in
  let status_str = json |> member "status" |> to_string_option |> Option.value ~default:"" in
  let status = status_of_string status_str in
  let limit_price = json |> member "limit_price" |> json_to_float_opt in
  let created_at = json |> member "created_at" |> to_string_option |> Option.value ~default:"" in
  {
    id;
    client_order_id;
    symbol;
    side;
    qty;
    filled_qty;
    type_str;
    side_str;
    status;
    limit_price;
    created_at;
  }

let place_order
    ~symbol
    ~qty
    ~side
    ~order_type
    ?limit_price
    ?time_in_force
    ?cl_ord_id
    ?extended_hours
    () =
  let base_url = Config.rest_base_url () in
  let url = Uri.of_string (base_url ^ "/v2/orders") in
  let is_crypto = String.contains symbol '/' in
  let is_fractional = Float.floor qty <> qty in
  let use_extended =
    match extended_hours with
    | Some b -> b
    | None -> !Config.extended_hours
  in
  let tif_str =
    match time_in_force with
    | Some "GTC" | Some "gtc" -> "gtc"
    | Some "IOC" | Some "ioc" -> if use_extended then "day" else "ioc"
    | Some "FOK" | Some "fok" -> if use_extended then "day" else "fok"
    | Some "DAY" | Some "day" -> "day"
    | _ -> if use_extended then "day" else if is_fractional && not is_crypto then "day" else "gtc"
  in
  let side_str = match side with Buy -> "buy" | Sell -> "sell" in
  let type_str = match order_type with "limit" -> "limit" | _ -> "market" in
  let assoc = [
    ("symbol", `String symbol);
    ("qty", `String (Printf.sprintf "%.9f" qty));
    ("side", `String side_str);
    ("type", `String type_str);
    ("time_in_force", `String tif_str);
  ] in
  let assoc =
    if use_extended then ("extended_hours", `Bool true) :: assoc
    else assoc
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
  Logging.debug_f ~section "Placing Alpaca order: %s %s %.6f %s (TIF=%s%s)"
    side_str symbol qty (match limit_price with Some p -> Printf.sprintf "@ %.4f" p | None -> "MKT") tif_str
    (if use_extended then ", extended_hours=true" else "");
  Lwt.catch (fun () ->
    Cohttp_lwt_unix.Client.post ~headers ~body:(Cohttp_lwt.Body.of_string req_body) url >>= (fun (resp, body) ->
      let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
      Cohttp_lwt.Body.to_string body >>= (fun body_str ->
        if status_code >= 200 && status_code < 300 then
          (try
            let json = Yojson.Safe.from_string body_str in
            let ord = parse_order_json json in
            Logging.debug_f ~section "Placed Alpaca order %s [%s %s %.6f]: status=%s"
              ord.id ord.symbol ord.side_str ord.qty (string_of_status ord.status);
            let userref =
              match ord.client_order_id with
              | Some cid -> (try Some (int_of_string cid) with _ -> None)
              | None -> None
            in
            Lwt.return (Ok { order_id = ord.id; cl_ord_id = ord.client_order_id; order_userref = userref })
          with exn ->
            let err = Printf.sprintf "Failed to parse place_order response: %s" (Printexc.to_string exn) in
            Logging.error_f ~section "%s (body: %s)" err body_str;
            Lwt.return (Error err))
        else begin
          Logging.error_f ~section "Place order failed HTTP %d: %s" status_code body_str;
          Lwt.return (Error (Printf.sprintf "HTTP %d: %s" status_code body_str))
        end
      )
    )
  ) (fun exn ->
    let err = Printf.sprintf "Place order HTTP exception: %s" (Printexc.to_string exn) in
    Logging.error_f ~section "%s" err;
    Lwt.return (Error err))

let amend_order
    ~order_id
    ?qty
    ?limit_price
    ?cl_ord_id
    () =
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
  Lwt.catch (fun () ->
    Cohttp_lwt_unix.Client.patch ~headers ~body:(Cohttp_lwt.Body.of_string req_body) url >>= (fun (resp, body) ->
      let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
      Cohttp_lwt.Body.to_string body >>= (fun body_str ->
        if status_code >= 200 && status_code < 300 then
          (try
            let json = Yojson.Safe.from_string body_str in
            let ord = parse_order_json json in
            Logging.debug_f ~section "Amended Alpaca order %s -> %s [%s]" order_id ord.id ord.symbol;
            Lwt.return (Ok {
              original_order_id = order_id;
              new_order_id = ord.id;
              amend_id = Some ord.id;
              cl_ord_id = ord.client_order_id;
            })
          with exn ->
            let err = Printf.sprintf "Failed to parse amend_order response: %s" (Printexc.to_string exn) in
            Logging.error_f ~section "%s (body: %s)" err body_str;
            Lwt.return (Error err))
        else begin
          Logging.error_f ~section "Amend order failed HTTP %d for %s: %s" status_code order_id body_str;
          Lwt.return (Error (Printf.sprintf "HTTP %d: %s" status_code body_str))
        end
      )
    )
  ) (fun exn ->
    let err = Printf.sprintf "Amend order HTTP exception: %s" (Printexc.to_string exn) in
    Logging.error_f ~section "%s" err;
    Lwt.return (Error err))

let cancel_order order_id =
  let base_url = Config.rest_base_url () in
  let url = Uri.of_string (Printf.sprintf "%s/v2/orders/%s" base_url order_id) in
  let headers = make_headers () in
  Logging.debug_f ~section "Cancelling Alpaca order %s" order_id;
  Lwt.catch (fun () ->
    Cohttp_lwt_unix.Client.delete ~headers url >>= (fun (resp, body) ->
      let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
      Cohttp_lwt.Body.to_string body >>= (fun body_str ->
        if status_code >= 200 && status_code < 300 then begin
          Logging.debug_f ~section "Cancelled Alpaca order %s" order_id;
          Lwt.return (Ok [{ order_id; cl_ord_id = None }])
        end else begin
          Logging.error_f ~section "Cancel order failed HTTP %d for %s: %s" status_code order_id body_str;
          Lwt.return (Error (Printf.sprintf "HTTP %d cancelling %s: %s" status_code order_id body_str))
        end
      )
    )
  ) (fun exn ->
    let err = Printf.sprintf "Cancel order HTTP exception: %s" (Printexc.to_string exn) in
    Logging.error_f ~section "%s" err;
    Lwt.return (Error err))

let get_open_orders () =
  let base_url = Config.rest_base_url () in
  let headers = make_headers () in
  let rec fetch_all acc until_id =
    let url_str =
      match until_id with
      | None -> base_url ^ "/v2/orders?status=open&nested=false&limit=500&direction=asc"
      | Some uid -> Printf.sprintf "%s/v2/orders?status=open&nested=false&limit=500&direction=asc&after=%s" base_url uid
    in
    let url = Uri.of_string url_str in
    Cohttp_lwt_unix.Client.get ~headers url >>= (fun (resp, body) ->
      let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
      Cohttp_lwt.Body.to_string body >>= (fun body_str ->
        if status_code >= 200 && status_code < 300 then
          (try
            let json = Yojson.Safe.from_string body_str in
            let items = match json with `List l -> l | _ -> [] in
            let orders = List.map parse_order_json items in
            let count = List.length orders in
            let combined = acc @ orders in
            if count >= 500 then
              (match List.nth_opt orders (count - 1) with
               | Some last_ord -> fetch_all combined (Some last_ord.created_at)
               | None -> Lwt.return (Ok combined))
            else begin
              Logging.debug_f ~section "Retrieved %d open orders from Alpaca" (List.length combined);
              Lwt.return (Ok combined)
            end
          with exn ->
            let err = Printf.sprintf "Failed to parse open orders response: %s" (Printexc.to_string exn) in
            Logging.error_f ~section "%s (body: %s)" err body_str;
            Lwt.return (Error err))
        else begin
          Logging.error_f ~section "Get open orders failed HTTP %d: %s" status_code body_str;
          Lwt.return (Error (Printf.sprintf "HTTP %d getting open orders: %s" status_code body_str))
        end
      )
    )
  in
  Lwt.catch
    (fun () -> fetch_all [] None)
    (fun exn ->
      let err = Printf.sprintf "Get open orders HTTP exception: %s" (Printexc.to_string exn) in
      Logging.error_f ~section "%s" err;
      Lwt.return (Error err))

let get_account () =
  let base_url = Config.rest_base_url () in
  let url = Uri.of_string (base_url ^ "/v2/account") in
  let headers = make_headers () in
  Lwt.catch (fun () ->
    Cohttp_lwt_unix.Client.get ~headers url >>= (fun (resp, body) ->
      let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
      Cohttp_lwt.Body.to_string body >>= (fun body_str ->
        if status_code >= 200 && status_code < 300 then
          (try
            let json = Yojson.Safe.from_string body_str in
            let open Yojson.Safe.Util in
            let id = json |> member "id" |> to_string_option |> Option.value ~default:"" in
            let status = json |> member "status" |> to_string_option |> Option.value ~default:"" in
            let currency = json |> member "currency" |> to_string_option |> Option.value ~default:"" in
            let buying_power = json |> member "buying_power" |> json_to_float in
            let cash = json |> member "cash" |> json_to_float in
            let portfolio_value = json |> member "portfolio_value" |> json_to_float in
            let equity = json |> member "equity" |> json_to_float in
            Lwt.return (Ok { id; status; currency; buying_power; cash; portfolio_value; equity })
          with exn ->
            let err = Printf.sprintf "Failed to parse account response: %s" (Printexc.to_string exn) in
            Logging.error_f ~section "%s (body: %s)" err body_str;
            Lwt.return (Error err))
        else begin
          Logging.error_f ~section "Get account failed HTTP %d: %s" status_code body_str;
          Lwt.return (Error (Printf.sprintf "HTTP %d getting account: %s" status_code body_str))
        end
      )
    )
  ) (fun exn ->
    let err = Printf.sprintf "Get account HTTP exception: %s" (Printexc.to_string exn) in
    Logging.error_f ~section "%s" err;
    Lwt.return (Error err))

let get_positions () =
  let base_url = Config.rest_base_url () in
  let url = Uri.of_string (base_url ^ "/v2/positions") in
  let headers = make_headers () in
  Lwt.catch (fun () ->
    Cohttp_lwt_unix.Client.get ~headers url >>= (fun (resp, body) ->
      let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
      Cohttp_lwt.Body.to_string body >>= (fun body_str ->
        if status_code >= 200 && status_code < 300 then
          (try
            let json = Yojson.Safe.from_string body_str in
            let open Yojson.Safe.Util in
            let items = match json with `List l -> l | _ -> [] in
            let positions = List.map (fun j ->
              {
                asset_id = j |> member "asset_id" |> to_string_option |> Option.value ~default:"";
                symbol = j |> member "symbol" |> to_string_option |> Option.value ~default:"";
                exchange = j |> member "exchange" |> to_string_option |> Option.value ~default:"";
                qty = j |> member "qty" |> json_to_float;
                market_value = j |> member "market_value" |> json_to_float;
                avg_entry_price = j |> member "avg_entry_price" |> json_to_float;
                current_price = j |> member "current_price" |> json_to_float;
                side = j |> member "side" |> to_string_option |> Option.value ~default:"";
              }
            ) items in
            Lwt.return (Ok positions)
          with exn ->
            let err = Printf.sprintf "Failed to parse positions response: %s" (Printexc.to_string exn) in
            Logging.error_f ~section "%s (body: %s)" err body_str;
            Lwt.return (Error err))
        else begin
          Logging.error_f ~section "Get positions failed HTTP %d: %s" status_code body_str;
          Lwt.return (Error (Printf.sprintf "HTTP %d getting positions: %s" status_code body_str))
        end
      )
    )
  ) (fun exn ->
    let err = Printf.sprintf "Get positions HTTP exception: %s" (Printexc.to_string exn) in
    Logging.error_f ~section "%s" err;
    Lwt.return (Error err))

let parse_iso_timestamp s =
  if String.length s < 19 then 0.0
  else
    try
      let y = int_of_string (String.sub s 0 4) in
      let m = int_of_string (String.sub s 5 2) in
      let d = int_of_string (String.sub s 8 2) in
      let hh = int_of_string (String.sub s 11 2) in
      let mm = int_of_string (String.sub s 14 2) in
      let ss = int_of_string (String.sub s 17 2) in
      let sub_sec =
        if String.length s > 20 && s.[19] = '.' then
          let end_idx = try String.index_from s 20 'Z' with _ -> String.length s in
          let frac_str = String.sub s 19 (end_idx - 19) in
          try float_of_string frac_str with _ -> 0.0
        else 0.0
      in
      let days_before_month = [| 0; 31; 59; 90; 120; 151; 181; 212; 243; 273; 304; 334 |] in
      let is_leap_year y = (y mod 4 = 0 && y mod 100 <> 0) || (y mod 400 = 0) in
      let y_off = y - 1970 in
      let leap_years = (y - 1969) / 4 - (y - 1901) / 100 + (y - 1601) / 400 in
      let m_days = days_before_month.(m - 1) + (if m > 2 && is_leap_year y then 1 else 0) in
      let total_days = (y_off * 365) + leap_years + m_days + (d - 1) in
      let total_secs = (total_days * 86400) + (hh * 3600) + (mm * 60) + ss in
      float_of_int total_secs +. sub_sec
    with _ -> 0.0

let get_snapshot ~symbol () =
  let data_base_url = Config.data_rest_url () in
  let fetch feed_name =
    let url = Uri.of_string (Printf.sprintf "%s/v2/stocks/%s/snapshot?feed=%s" data_base_url symbol feed_name) in
    let headers = make_headers () in
    Cohttp_lwt_unix.Client.get ~headers url >>= (fun (resp, body) ->
      let status_code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
      Cohttp_lwt.Body.to_string body >>= (fun body_str ->
        if status_code >= 200 && status_code < 300 then
          (try
            let json = Yojson.Safe.from_string body_str in
            let open Yojson.Safe.Util in
            let lq = json |> member "latestQuote" in
            let bp = lq |> member "bp" |> json_to_float in
            let bs = lq |> member "bs" |> json_to_float in
            let ap = lq |> member "ap" |> json_to_float in
            let as_val = lq |> member "as" |> json_to_float in
            let lq_t_str = lq |> member "t" |> to_string_option |> Option.value ~default:"" in
            let lq_ts = parse_iso_timestamp lq_t_str in

            let lt = json |> member "latestTrade" in
            let tp = lt |> member "p" |> json_to_float in
            let ts = lt |> member "s" |> json_to_float in
            let lt_t_str = lt |> member "t" |> to_string_option |> Option.value ~default:"" in
            let lt_ts = parse_iso_timestamp lt_t_str in

            let mb = json |> member "minuteBar" in
            let mp = mb |> member "c" |> json_to_float in
            let ms = mb |> member "v" |> json_to_float in
            let mb_t_str = mb |> member "t" |> to_string_option |> Option.value ~default:"" in
            let mb_ts = parse_iso_timestamp mb_t_str in

            let valid_quote = bp > 0.0 && ap > 0.0 && ap >= bp in
            let valid_trade = tp > 0.0 in
            let valid_bar = mp > 0.0 in

            if valid_quote && lq_ts >= lt_ts && lq_ts >= mb_ts then
              Lwt.return (Ok (bp, bs, ap, as_val))
            else if valid_trade && lt_ts >= lq_ts && lt_ts >= mb_ts then
              Lwt.return (Ok (tp, ts, tp, ts))
            else if valid_bar && mb_ts >= lq_ts && mb_ts >= lt_ts then
              Lwt.return (Ok (mp, ms, mp, ms))
            else if valid_quote then
              Lwt.return (Ok (bp, bs, ap, as_val))
            else if valid_trade then
              Lwt.return (Ok (tp, ts, tp, ts))
            else if valid_bar then
              Lwt.return (Ok (mp, ms, mp, ms))
            else if bp > 0.0 then
              Lwt.return (Ok (bp, bs, bp, bs))
            else if ap > 0.0 then
              Lwt.return (Ok (ap, as_val, ap, as_val))
            else
              Lwt.return (Error (Printf.sprintf "No valid quote/trade/bar price for %s in %s snapshot" symbol feed_name))
          with exn ->
            let err = Printf.sprintf "Failed to parse %s snapshot response: %s" feed_name (Printexc.to_string exn) in
            Lwt.return (Error err))
        else begin
          Lwt.return (Error (Printf.sprintf "HTTP %d getting %s snapshot" status_code feed_name))
        end
      )
    )
  in
  let is_reg = Alpaca_market_hours.is_regular_market_open () in
  let primary_feed = !Config.data_feed in
  Lwt.catch (fun () ->
    if is_reg then
      fetch primary_feed
    else
      fetch primary_feed >>= function
      | Ok res -> Lwt.return (Ok res)
      | Error _ ->
          if primary_feed <> "overnight" then
            fetch "overnight"
          else
            Lwt.return (Error "Overnight snapshot failed")
  ) (fun exn ->
    let err = Printf.sprintf "Get snapshot HTTP exception: %s" (Printexc.to_string exn) in
    Logging.error_f ~section "%s" err;
    Lwt.return (Error err))
