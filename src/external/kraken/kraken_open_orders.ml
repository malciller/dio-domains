open Lwt.Infix
open Cohttp_lwt_unix
open Yojson.Safe.Util

let section = "kraken_open_orders"
let endpoint = "https://api.kraken.com"

(** Maps WS feed style side strings to the module's types. *)
let side_of_string s =
  match s with
  | "buy" -> Kraken_executions_feed.Buy
  | "sell" -> Kraken_executions_feed.Sell
  | _ -> Kraken_executions_feed.Buy

let status_of_string s =
  match s with
  | "pending" -> Kraken_executions_feed.PendingNewStatus
  | "open" -> Kraken_executions_feed.NewStatus
  | "closed" -> Kraken_executions_feed.FilledStatus
  | "canceled" -> Kraken_executions_feed.CanceledStatus
  | "expired" -> Kraken_executions_feed.ExpiredStatus
  | "rejected" -> Kraken_executions_feed.RejectedStatus
  | s -> Kraken_executions_feed.UnknownStatus s

let get_open_orders () : Kraken_executions_feed.execution_event list Lwt.t =
  Lwt.catch (fun () ->
    Kraken_get_fee.get_api_credentials_from_env () >>= fun (api_key, api_secret) ->
    let path = "/0/private/OpenOrders" in
    let nonce = Kraken_common_types.nonce () in
    let encoded_body = Uri.encoded_of_query [("nonce", [nonce]); ("trades", ["true"])] in
    let signature = Kraken_common_types.sign ~secret:api_secret ~path ~body:encoded_body ~nonce in
    let headers = Cohttp.Header.of_list [("API-Key", api_key); ("API-Sign", signature); ("Content-Type", "application/x-www-form-urlencoded")] in
    
    Logging.debug ~section "Fetching REST OpenOrders payload to bypass WS 50-order snapshot limit";
    
    Client.post ~headers ~body:(Cohttp_lwt.Body.of_string encoded_body) 
      (Uri.of_string (endpoint ^ path)) >>= fun (resp, body) ->
      
    Cohttp_lwt.Body.to_string body >>= fun body_str ->
    let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
    if status <> 200 then begin
      Logging.error_f ~section "OpenOrders HTTP %d: %s" status body_str;
      Lwt.return []
    end else begin
      let json = Yojson.Safe.from_string body_str in
      match member "error" json with
      | `List (_ :: _ as errs) ->
          Logging.error_f ~section "OpenOrders API error: %s" (errs |> filter_string |> String.concat "; ");
          Lwt.return []
      | _ ->
          let result = member "result" json in
          let open_obj = member "open" result in
          let orders_assoc = try to_assoc open_obj with _ -> [] in
          
          let parsed_orders = List.filter_map (fun (order_id, order_json) ->
            try
              let desc = member "descr" order_json in
              let symbol = member "pair" desc |> to_string in
              let side_str = member "type" desc |> to_string in
              let status_str = member "status" order_json |> to_string in
              
              let vol = member "vol" order_json |> to_string |> float_of_string in
              let vol_exec = member "vol_exec" order_json |> to_string |> float_of_string in
              let cost = member "cost" order_json |> to_string |> float_of_string in
              let limit_price = member "price" desc |> to_string |> float_of_string in
              let avg_price = member "price" order_json |> to_string |> float_of_string in
              
              let userref_opt = match member "userref" order_json with
                | `Int i -> Some i
                | `Intlit s -> int_of_string_opt s
                | `String s -> int_of_string_opt s
                | _ -> None
              in
              
              let cl_ord_id_opt = match member "clordid" order_json with
                | `String s -> Some s
                | _ -> None
              in
              
              Some {
                Kraken_executions_feed.order_id;
                symbol;
                exec_type = Kraken_executions_feed.Restated;
                order_status = status_of_string status_str;
                side = side_of_string side_str;
                order_qty = vol;
                cum_qty = vol_exec;
                cum_cost = cost;
                avg_price;
                limit_price = Some limit_price;
                last_qty = None;
                last_price = None;
                fee = None;
                trade_id = None;
                order_userref = userref_opt;
                cl_ord_id = cl_ord_id_opt;
                timestamp = Unix.gettimeofday ();
              }
            with e ->
              Logging.warn_f ~section "Failed to parse open order %s: %s" order_id (Printexc.to_string e);
              None
          ) orders_assoc in
          
          Logging.info_f ~section "Retrieved %d total open orders via REST (Bypassed WS 50-order cap)" (List.length parsed_orders);
          Lwt.return parsed_orders
    end
  ) (fun exn ->
    Logging.error_f ~section "Failed to fetch OpenOrders: %s" (Printexc.to_string exn);
    Lwt.return []
  )
