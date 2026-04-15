(** Action packing, signing, and submission for the Hyperliquid exchange API. *)

open Hyperliquid_types
module ExTypes = Dio_exchange.Exchange_intf.Types
open Lwt.Infix

let section = "hyperliquid_actions"

(** Maps an Exchange_intf order type and optional time-in-force to an order_type_wire.
    FOK is not natively supported by Hyperliquid; it is mapped to IOC as a best-effort fallback. *)
let hl_order_type (ot : ExTypes.order_type) (tif_opt : ExTypes.time_in_force option) : order_type_wire =
  let tif = match tif_opt with
    | Some ExTypes.GTC -> Alo
    | Some ExTypes.IOC -> Ioc
    | Some ExTypes.FOK -> Ioc (* FOK unsupported natively; mapped to IOC *)
    | None -> Alo
  in
  match ot with
  | ExTypes.Limit -> Limit { tif = Alo }
  | ExTypes.Market -> Limit { tif = Ioc } (* Market orders submitted as IOC limit orders *)
  | ExTypes.StopLoss -> Trigger { triggerPx = "0.0"; isMarket = true; tpsl = Sl }
  | ExTypes.TakeProfit -> Trigger { triggerPx = "0.0"; isMarket = true; tpsl = Tp }
  | ExTypes.StopLossLimit -> Trigger { triggerPx = "0.0"; isMarket = false; tpsl = Sl }
  | ExTypes.TakeProfitLimit -> Trigger { triggerPx = "0.0"; isMarket = false; tpsl = Tp }
  | _ -> Limit { tif } (* Default fallback for unmatched order types *)

(** Formats a float as a fixed-point string with at most 8 decimal places.
    Strips trailing zeros and the decimal point if unnecessary.
    Hyperliquid requires normalized numeric strings without trailing zeros. *)
let format_number f =
  let rounded = Printf.sprintf "%.8f" f in
  (* Iteratively strip trailing '0' characters and a dangling decimal point. *)
  let rec strip_zeros s =
    if String.length s > 1 && s.[String.length s - 1] = '0' && String.contains s '.' then
      strip_zeros (String.sub s 0 (String.length s - 1))
    else if String.length s > 1 && s.[String.length s - 1] = '.' then
      String.sub s 0 (String.length s - 1)
    else s
  in
  let s = strip_zeros rounded in
  if s = "-0" then "0" else s

(** Constructs an order_wire record from generic order parameters.
    Maps side to a boolean, formats price and size as normalized strings,
    and defaults reduce_only to false when unspecified. *)
let to_hl_order_wire ~qty ~symbol:_ ~asset_index ~ot ~side ~limit_price ~reduce_only ~cl_ord_id : order_wire =
  let b = match side with
    | ExTypes.Buy -> true
    | ExTypes.Sell -> false
  in
  let p = match limit_price with
    | Some price -> format_number price
    | None -> "0.0" (* Placeholder for market or trigger orders *)
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

(** REST API action types and credential management. *)

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

(** Monotonically increasing nonce derived from wall-clock milliseconds.
    Protected by a mutex to ensure uniqueness across concurrent callers. *)
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

(** Submits a signed action to the Hyperliquid REST /exchange endpoint.
    Signs the msgpack-serialized action with the configured private key,
    constructs the JSON request envelope, and performs an HTTP POST.
    Returns Ok(json) on 2xx or Error(string) otherwise. *)
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

(** Returns true if [substr] occurs anywhere within [str].
    Delegates to centralized [Error_handling.string_contains]. *)
let string_contains = Error_handling.string_contains

(** Retry configuration — re-exported from centralized [Error_handling]. *)
type retry_config = Error_handling.retry_config = {
  max_attempts: int;
  base_delay_ms: float;
  max_delay_ms: float;
  backoff_factor: float;
}

let default_retry_config = Error_handling.default_retry_config

(** Classifies an error string as retriable.
    Delegates to centralized [Error_handling.is_retriable_error]. *)
let is_retriable_error = Error_handling.is_retriable_error

(** Atomic counter for WebSocket request IDs. *)

let ws_req_id_counter = Atomic.make 1

let next_ws_req_id () =
  let id = Atomic.fetch_and_add ws_req_id_counter 1 in
  id

(** Submits a signed action via the WebSocket "post" channel.
    Returns Error immediately if the WebSocket is not connected.
    The response payload is extracted from the nested WS frame structure
    (data.response.payload) to match the REST response shape, allowing
    shared downstream parsing logic. *)
let post_exchange_ws ~testnet:_ ~action_json ~action_msgpack ~is_mainnet =
  if not (Hyperliquid_ws.is_connected ()) then
    Lwt.return (Error "Hyperliquid WS not connected - order rejected")
  else begin
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
    let req_id = next_ws_req_id () in
    let ws_frame = `Assoc [
      "method",  `String "post";
      "id",      `Int req_id;
      "request", `Assoc [
        "type",    `String "action";
        "payload", `Assoc [
          "action",    action_json;
          "nonce",     `Intlit (Int64.to_string nonce);
          "signature", `Assoc [
            "r", `String r;
            "s", `String s;
            "v", `Int v
          ]
        ]
      ]
    ] in
    Logging.debug_f ~section "Sending WS post action: req_id=%d" req_id;
    Lwt.catch
      (fun () ->
        Hyperliquid_ws.send_request ~json:ws_frame ~req_id ~timeout_ms:10000 >>= fun resp ->
        (* Extract data.response.payload from the WS broadcast frame. *)
        let open Yojson.Safe.Util in
        let payload =
          try resp |> member "data" |> member "response" |> member "payload"
          with _ -> resp
        in
        Logging.debug_f ~section "Hyperliquid order raw WS response: %s" (Yojson.Safe.to_string payload);
        Lwt.return (Ok payload)
      )
      (fun exn ->
        let err = Printexc.to_string exn in
        Logging.error_f ~section "WS post failed for req_id=%d: %s" req_id err;
        Lwt.return (Error (Printf.sprintf "WS order failed: %s" err))
      )
  end

(** Places a single order on Hyperliquid via WebSocket with exponential backoff retry.
    Constructs both the JSON and msgpack representations, signs and submits them,
    then parses the response for a resting or filled order ID. *)
let place_order ~symbol ~is_buy ~sz ~px ~is_limit:_ ?post_only:_ ?reduce_only ?cl_ord_id ?time_in_force ~testnet () =
  let place_order_once () =
    let asset_index = match Hyperliquid_instruments_feed.get_asset_index symbol with
      | Some idx -> idx
      | None -> failwith (Printf.sprintf "Unknown symbol: %s" symbol)
    in
    
    let tif = Option.value time_in_force ~default:Gtc in
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
    
    post_exchange_ws ~testnet ~action_json ~action_msgpack ~is_mainnet >>= function
    | Ok res ->
        let open Yojson.Safe.Util in
        (try
          let status = member "status" res |> to_string_option in
          match status with
          | Some "err" ->
              let err_msg = match member "response" res with
                | `String s -> s
                | other -> Yojson.Safe.to_string other
              in
              Lwt.return (Error (Printf.sprintf "HL Order Rejected: %s" err_msg))
          | _ ->
              let response = member "response" res in
              let data = member "data" response in
              let statuses = member "statuses" data |> to_list in
              match statuses with
              | s :: _ ->
                  let resting = member "resting" s in
                  let filled = member "filled" s in
                  if resting <> `Null then
                    match resting |> member "oid" |> to_int64_opt with
                    | Some oid -> Lwt.return (Ok { order_id = oid })
                    | None -> Lwt.return (Error (Printf.sprintf "Failed to find oid in resting status: %s" (Yojson.Safe.to_string s)))
                  else if filled <> `Null then
                    match filled |> member "oid" |> to_int64_opt with
                    | Some oid -> Lwt.return (Ok { order_id = oid })
                    | None -> Lwt.return (Error (Printf.sprintf "Failed to find oid in filled status: %s" (Yojson.Safe.to_string s)))
                  else
                    (match member "error" s |> to_string_option with
                     | Some err -> Lwt.return (Error (Printf.sprintf "HL Order Rejected: %s" err))
                     | None -> Lwt.return (Error (Printf.sprintf "Failed to find oid or error in HL response: %s" (Yojson.Safe.to_string res))))
              | [] -> Lwt.return (Error "Empty status list in response")
        with exn -> Lwt.return (Error (Printf.sprintf "Failed to parse HL response: %s (Raw: %s)" (Printexc.to_string exn) (Yojson.Safe.to_string res))))
    | Error e -> Lwt.return (Error e)
  in
  Error_handling.retry_with_backoff ~section ~config:default_retry_config ~f:place_order_once ()

(** Amends an existing order by order_id on Hyperliquid via WebSocket with retry.
    Submits a "modify" action with updated price, size, and side.
    Returns the original and potentially new order ID on success. *)
let amend_order ~symbol ~order_id ~is_buy ~px ~sz ?cl_ord_id ~testnet () =
  let amend_order_once () =
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
      t = Limit { tif = Alo };
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
      "t", `Assoc ["limit", `Assoc ["tif", `String "Alo"]]
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
    
    post_exchange_ws ~testnet ~action_json ~action_msgpack ~is_mainnet >>= function
    | Ok res ->
        let open Yojson.Safe.Util in
        (try
          let status = member "status" res |> to_string_option in
          match status with
          | Some "err" ->
              let err_msg = match member "response" res with
                | `String s -> s
                | other -> Yojson.Safe.to_string other
              in
              Lwt.return (Error (Printf.sprintf "HL Amendment Rejected: %s" err_msg))
          | _ ->
              let response = member "response" res in
              let response_type = member "type" response |> to_string_option in
              (match response_type with
               | Some "default" ->
                   Lwt.return (Ok { amend_id = order_id; order_id = order_id })
               | _ ->
                   let data = member "data" response in
                   let statuses = member "statuses" data |> to_list in
                   match statuses with
                   | s :: _ ->
                       let resting = member "resting" s in
                       if resting <> `Null then
                         match resting |> member "oid" |> to_int64_opt with
                         | Some new_oid -> Lwt.return (Ok { amend_id = new_oid; order_id = order_id })
                         | None -> Lwt.return (Error (Printf.sprintf "Failed to find new oid in resting status: %s" (Yojson.Safe.to_string s)))
                       else
                         (match member "error" s |> to_string_option with
                          | Some err -> Lwt.return (Error (Printf.sprintf "HL Amendment Rejected: %s" err))
                          | None -> Lwt.return (Error (Printf.sprintf "Failed to find oid or error in HL amend response: %s" (Yojson.Safe.to_string res))))
                   | [] -> Lwt.return (Error "Empty status list in response"))
        with exn -> Lwt.return (Error (Printf.sprintf "Failed to parse HL response: %s (Raw: %s)" (Printexc.to_string exn) (Yojson.Safe.to_string res))))
    | Error e -> Lwt.return (Error e)
  in
  Error_handling.retry_with_backoff ~section ~config:default_retry_config ~f:amend_order_once ()

(** Cancels one or more orders by their order IDs for a given symbol.
    Constructs a batch cancel action and submits via WebSocket with retry. *)
let cancel_orders ~symbol ~order_ids ~testnet =
  let cancel_orders_once () =
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
    post_exchange_ws ~testnet ~action_json ~action_msgpack ~is_mainnet >>= function
    | Ok _ -> Lwt.return (Ok ())
    | Error e -> Lwt.return (Error e)
  in
  Error_handling.retry_with_backoff ~section ~config:default_retry_config ~f:cancel_orders_once ()

(** Transfers USDC between spot and perpetual wallets via the usdClassTransfer action.
    Uses the REST endpoint (not WebSocket) with exponential backoff retry. *)
let usd_class_transfer ~amount ~to_perp ~testnet =
  let transfer_once () =
    let amount_str = format_number amount in
    let action = pack_usd_class_transfer_action ~amount:amount_str ~to_perp in
    let action_msgpack = serialize_action action in

    let action_json = `Assoc [
      "type", `String "usdClassTransfer";
      "amount", `String amount_str;
      "toPerp", `Bool to_perp;
    ] in

    let is_mainnet = not testnet in

    post_exchange ~testnet ~action_json ~action_msgpack ~is_mainnet >|= function
    | Ok res ->
        let open Yojson.Safe.Util in
        (try
          let status = member "status" res |> to_string_option in
          match status with
          | Some "err" ->
              let err_msg = match member "response" res with
                | `String s -> s
                | other -> Yojson.Safe.to_string other
              in
              Error (Printf.sprintf "HL Transfer Rejected: %s" err_msg)
          | _ -> Ok ()
        with exn -> Error (Printf.sprintf "Failed to parse HL transfer response: %s (Raw: %s)" (Printexc.to_string exn) (Yojson.Safe.to_string res)))
    | Error e -> Error e
  in
  Error_handling.retry_with_backoff ~section ~config:default_retry_config ~f:transfer_once ()