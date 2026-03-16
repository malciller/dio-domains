(** Hyperliquid action packing and translation *)

open Hyperliquid_types
module ExTypes = Dio_exchange.Exchange_intf.Types
open Lwt.Infix

(** Convert Exchange standard order type to Hyperliquid order type (with TIF) *)
let hl_order_type (ot : ExTypes.order_type) (tif_opt : ExTypes.time_in_force option) : order_type_wire =
  let tif = match tif_opt with
    | Some ExTypes.GTC -> Gtc
    | Some ExTypes.IOC -> Ioc
    | Some ExTypes.FOK -> Ioc (* HL doesn't support FOK natively in the same way, best effort IOC *)
    | None -> Gtc
  in
  match ot with
  | ExTypes.Limit -> Limit { tif = Alo }
  | ExTypes.Market -> Limit { tif = Ioc } (* Market orders are usually IOC limits in HL *)
  | ExTypes.StopLoss -> Trigger { triggerPx = "0.0"; isMarket = true; tpsl = Sl }
  | ExTypes.TakeProfit -> Trigger { triggerPx = "0.0"; isMarket = true; tpsl = Tp }
  | ExTypes.StopLossLimit -> Trigger { triggerPx = "0.0"; isMarket = false; tpsl = Sl }
  | ExTypes.TakeProfitLimit -> Trigger { triggerPx = "0.0"; isMarket = false; tpsl = Tp }
  | _ -> Limit { tif } (* Fallback *)

(** Format float as a string rounded to a precision suitable for Hyperliquid.
    HL expects maximum 8 decimals, but crucially it must be normalized (no trailing zeros).
    This matches Python's Decimal(rounded).normalize() representation. *)
let format_number f =
  let rounded = Printf.sprintf "%.8f" f in
  (* Convert back to float and then to string to let OCaml handle trailing zeros,
     but OCaml's string_of_float can use scientific notation for very small/large numbers.
     Hyperliquid expects fixed-point strings. *)
  let rec strip_zeros s =
    if String.length s > 1 && s.[String.length s - 1] = '0' && String.contains s '.' then
      strip_zeros (String.sub s 0 (String.length s - 1))
    else if String.length s > 1 && s.[String.length s - 1] = '.' then
      String.sub s 0 (String.length s - 1)
    else s
  in
  let s = strip_zeros rounded in
  if s = "-0" then "0" else s

(** Convert generic order parameters into Hyperliquid's order_wire format *)
let to_hl_order_wire ~qty ~symbol:_ ~asset_index ~ot ~side ~limit_price ~reduce_only ~cl_ord_id : order_wire =
  let b = match side with
    | ExTypes.Buy -> true
    | ExTypes.Sell -> false
  in
  let p = match limit_price with
    | Some price -> format_number price
    | None -> "0.0" (* Market orders or triggers where price is handled differently *)
  in
  let s = format_number qty in
  let r = match reduce_only with
    | Some v -> v
    | None -> false
  in
  let a = asset_index in
  {
    a; b; p; s; r; t = ot; c = cl_ord_id
  }

let to_int64_opt = function
  | `Int i -> Some (Int64.of_int i)
  | `Intlit s -> Some (Int64.of_string s)
  | `String s -> (try Some (Int64.of_string s) with _ -> None)
  | _ -> None

(** --- REST API Actions --- *)

type place_order_result = {
  order_id: int64;
}

type amend_order_result = {
  amend_id: int64;
  order_id: int64;
}

let get_credentials () =
  let pkey = match Sys.getenv_opt "HYPERLIQUID_PRIVATE_KEY" |> Option.map String.trim with
    | Some k -> k
    | None -> failwith "Missing environment variable: HYPERLIQUID_PRIVATE_KEY"
  in
  let wallet = match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim with
    | Some w -> w
    | None -> failwith "Missing environment variable: HYPERLIQUID_WALLET_ADDRESS"
  in
  (pkey, wallet)

let last_nonce = ref 0L
let nonce_mutex = Mutex.create ()

let get_next_nonce () =
  Mutex.lock nonce_mutex;
  let current_time_ms = Int64.of_float (Unix.gettimeofday () *. 1000.0) in
  let next_nonce = if current_time_ms <= !last_nonce then
    Int64.add !last_nonce 1L
  else
    current_time_ms
  in
  last_nonce := next_nonce;
  Mutex.unlock nonce_mutex;
  next_nonce

let post_exchange ~testnet ~action_json ~action_msgpack ~is_mainnet =
  let (pkey, _wallet) = get_credentials () in
  let nonce = get_next_nonce () in
  let (r, s, v) = Hyperliquid_signer.sign_l1_action
    ~private_key_hex:pkey
    ~action_msgpack
    ~nonce
    ~is_mainnet
    ~vault_address:None
    ()
  in
  
  let base_url = if testnet then "https://api.hyperliquid-testnet.xyz" else "https://api.hyperliquid.xyz" in
  let url = Uri.of_string (base_url ^ "/exchange") in
  
  let req_body = `Assoc [
    "action", action_json;
    "nonce", `Intlit (Int64.to_string nonce);
    "signature", `Assoc [
      "r", `String r;
      "s", `String s;
      "v", `Int v
    ]
  ] in
  
  let body_str = Yojson.Safe.to_string req_body in
  let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
  
  Cohttp_lwt_unix.Client.post ~headers ~body:(Cohttp_lwt.Body.of_string body_str) url >>= fun (resp, resp_body) ->
  Cohttp_lwt.Body.to_string resp_body >>= fun body_str ->
  let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
  if status >= 200 && status < 300 then
    Lwt.return (Ok (Yojson.Safe.from_string body_str))
  else
    Lwt.return (Error (Printf.sprintf "HTTP Error %d: %s" status body_str))

(** Place order *)
let place_order ~symbol ~is_buy ~sz ~px ~is_limit:_ ?post_only ?reduce_only ?cl_ord_id ~testnet () =
  let asset_index = match Hyperliquid_instruments_feed.get_asset_index symbol with
    | Some idx -> idx
    | None -> failwith (Printf.sprintf "Unknown symbol: %s" symbol)
  in
  
  let tif = if Option.value post_only ~default:true then Alo else Gtc in
  let ot = Limit { tif } in
  
  let order_wire = {
    a = asset_index;
    b = is_buy;
    p = format_number px;
    s = format_number sz;
    r = Option.value reduce_only ~default:false;
    t = ot;
    c = cl_ord_id;
  } in
  
  let action = pack_order_action ~orders:[order_wire] ~grouping:"na" in
  let action_msgpack = serialize_action action in
  
  let order_json = [
    "a", `Int asset_index;
    "b", `Bool is_buy;
    "p", `String (format_number px);
    "s", `String (format_number sz);
    "r", `Bool (Option.value reduce_only ~default:false);
    "t", `Assoc ["limit", `Assoc ["tif", `String (tif_to_string tif)]]
  ] in
  let order_json = match cl_ord_id with
    | Some c -> order_json @ ["c", `String c]
    | None -> order_json
  in
  
  let action_json = `Assoc [
    "type", `String "order";
    "orders", `List [
      `Assoc order_json
    ];
    "grouping", `String "na"
  ] in
  
  let is_mainnet = not testnet in
  
  post_exchange ~testnet ~action_json ~action_msgpack ~is_mainnet >|= function
  | Ok res ->
      let open Yojson.Safe.Util in
      (try
        (* Check for error status first - response may be a string, not an object *)
        let status = member "status" res |> to_string_option in
        match status with
        | Some "err" ->
            let err_msg = match member "response" res with
              | `String s -> s
              | other -> Yojson.Safe.to_string other
            in
            Error (Printf.sprintf "HL Order Rejected: %s" err_msg)
        | _ ->
            let response = member "response" res in
            let data = member "data" response in
            let statuses = member "statuses" data |> to_list in
            match statuses with
            | s :: _ ->
                let resting = member "resting" s in
                if resting <> `Null then
                  match resting |> member "oid" |> to_int64_opt with
                  | Some oid -> Ok { order_id = oid }
                  | None -> Error (Printf.sprintf "Failed to find oid in resting status: %s" (Yojson.Safe.to_string s))
                else
                  (match member "error" s |> to_string_option with
                   | Some err -> Error (Printf.sprintf "HL Order Rejected: %s" err)
                   | None -> Error (Printf.sprintf "Failed to find oid or error in HL response: %s" (Yojson.Safe.to_string res)))
            | [] -> Error "Empty status list in response"
      with exn -> Error (Printf.sprintf "Failed to parse HL response: %s (Raw: %s)" (Printexc.to_string exn) (Yojson.Safe.to_string res)))
  | Error e -> Error e

(** Amend order *)
let amend_order ~symbol ~order_id ~is_buy ~px ~sz ?cl_ord_id ~testnet () =
  let asset_index = match Hyperliquid_instruments_feed.get_asset_index symbol with
    | Some idx -> idx
    | None -> failwith (Printf.sprintf "Unknown symbol: %s" symbol)
  in
  
  let order_wire = {
    a = asset_index;
    b = is_buy;
    p = format_number px;
    s = format_number sz;
    r = false;
    t = Limit { tif = Gtc };
    c = cl_ord_id;
  } in
  
  let action = pack_modify_action ~oid:order_id ~order:order_wire in
  let action_msgpack = serialize_action action in
  
  let order_json = [
    "a", `Int asset_index;
    "b", `Bool is_buy;
    "p", `String (format_number px);
    "s", `String (format_number sz);
    "r", `Bool false;
    "t", `Assoc ["limit", `Assoc ["tif", `String "Gtc"]]
  ] in
  let order_json = match cl_ord_id with
    | Some c -> order_json @ ["c", `String c]
    | None -> order_json
  in
  
  let action_json = `Assoc [
    "type", `String "modify";
    "oid", `Intlit (Int64.to_string order_id);
    "order", `Assoc order_json
  ] in
  
  let is_mainnet = not testnet in
  
  post_exchange ~testnet ~action_json ~action_msgpack ~is_mainnet >|= function
  | Ok res ->
      let open Yojson.Safe.Util in
      (try
        (* Check for error status first - response may be a string, not an object *)
        let status = member "status" res |> to_string_option in
        match status with
        | Some "err" ->
            let err_msg = match member "response" res with
              | `String s -> s
              | other -> Yojson.Safe.to_string other
            in
            Error (Printf.sprintf "HL Amendment Rejected: %s" err_msg)
        | _ ->
            let response = member "response" res in
            (* Cancel-replace returns {"type":"default"} with no data/statuses.
               The new order ID arrives via WebSocket orderUpdates. *)
            let response_type = member "type" response |> to_string_option in
            (match response_type with
             | Some "default" ->
                 Ok { amend_id = order_id; order_id = order_id }
             | _ ->
                 let data = member "data" response in
                 let statuses = member "statuses" data |> to_list in
                 match statuses with
                 | s :: _ ->
                     let resting = member "resting" s in
                     if resting <> `Null then
                       match resting |> member "oid" |> to_int64_opt with
                       | Some new_oid -> Ok { amend_id = new_oid; order_id = order_id }
                       | None -> Error (Printf.sprintf "Failed to find new oid in resting status: %s" (Yojson.Safe.to_string s))
                     else
                       (match member "error" s |> to_string_option with
                        | Some err -> Error (Printf.sprintf "HL Amendment Rejected: %s" err)
                        | None -> Error (Printf.sprintf "Failed to find oid or error in HL amend response: %s" (Yojson.Safe.to_string res)))
                 | [] -> Error "Empty status list in response")
      with exn -> Error (Printf.sprintf "Failed to parse HL response: %s (Raw: %s)" (Printexc.to_string exn) (Yojson.Safe.to_string res)))
  | Error e -> Error e

(** Cancel order *)
let cancel_orders ~symbol ~order_ids ~testnet =
  let asset_index = match Hyperliquid_instruments_feed.get_asset_index symbol with
    | Some idx -> idx
    | None -> failwith (Printf.sprintf "Unknown symbol: %s" symbol)
  in
  
  let action = pack_cancel_action ~cancels:(List.map (fun oid -> { a = asset_index; o = oid }) order_ids) in
  let action_msgpack = serialize_action action in
  
  let action_json = `Assoc [
    "type", `String "cancel";
    "cancels", `List (List.map (fun oid -> `Assoc ["a", `Int asset_index; "o", `Intlit (Int64.to_string oid)]) order_ids)
  ] in
  
  let is_mainnet = not testnet in
  post_exchange ~testnet ~action_json ~action_msgpack ~is_mainnet >|= function
  | Ok _ -> Ok ()
  | Error e -> Error e