(** WebSocket trade execution stream for Alpaca. Manages open order state and execution event ring buffers. *)

open Lwt.Infix
open Dio_exchange.Exchange_intf.Types

let section = "alpaca_executions"

type open_order_internal = {
  order_id: string;
  symbol: string;
  side: order_side;
  qty: float;
  cum_qty: float;
  remaining_qty: float;
  limit_price: float option;
  status: order_status;
  user_ref: int option;
  cl_ord_id: string option;
}

type execution_event_internal = {
  order_id: string;
  symbol: string;
  order_status: order_status;
  limit_price: float option;
  side: order_side;
  remaining_qty: float;
  filled_qty: float;
  avg_price: float;
  timestamp: float;
  is_amended: bool;
  cl_ord_id: string option;
}

let status_of_alpaca_status = function
  | Alpaca_types.New -> New
  | Alpaca_types.PartiallyFilled -> PartiallyFilled
  | Alpaca_types.Filled -> Filled
  | Alpaca_types.Canceled | Alpaca_types.DoneForDay | Alpaca_types.Stopped -> Canceled
  | Alpaca_types.Expired -> Expired
  | Alpaca_types.Rejected -> Rejected
  | Alpaca_types.PendingNew | Alpaca_types.PendingCancel | Alpaca_types.PendingReplace -> Pending
  | Alpaca_types.Accepted | Alpaca_types.AcceptedForBidding | Alpaca_types.Calculated -> New
  | Alpaca_types.Replaced -> Canceled
  | Alpaca_types.Suspended -> Unknown "suspended"
  | Alpaca_types.Unknown s -> Unknown s

module SymbolExecStore = struct
  type t = {
    symbol: string;
    buffer: execution_event_internal array;
    capacity: int;
    mutable write_pos: int;
    open_orders: (string, open_order_internal) Hashtbl.t;
    mutable initial_data_received: bool;
    mutex: Mutex.t;
  }

  let create symbol capacity = {
    symbol;
    buffer = Array.make capacity {
      order_id = ""; symbol = ""; order_status = Pending; limit_price = None;
      side = Buy; remaining_qty = 0.0; filled_qty = 0.0; avg_price = 0.0;
      timestamp = 0.0; is_amended = false; cl_ord_id = None;
    };
    capacity;
    write_pos = 0;
    open_orders = Hashtbl.create 32;
    initial_data_received = false;
    mutex = Mutex.create ();
  }

  let push_event t (e : execution_event_internal) =
    Mutex.lock t.mutex;
    let idx = t.write_pos mod t.capacity in
    t.buffer.(idx) <- e;
    t.write_pos <- t.write_pos + 1;
    t.initial_data_received <- true;

    (* Update open order store *)
    (match e.order_status with
     | New | Pending | PartiallyFilled | Unknown _ ->
         let existing_order = Hashtbl.find_opt t.open_orders e.order_id in
         let qty = match existing_order with Some o -> o.qty | None -> e.filled_qty +. e.remaining_qty in
         let user_ref = match e.cl_ord_id with Some cid -> (try Some (int_of_string cid) with _ -> None) | None -> None in
         let oo = {
           order_id = e.order_id;
           symbol = t.symbol;
           side = e.side;
           qty;
           cum_qty = e.filled_qty;
           remaining_qty = e.remaining_qty;
           limit_price = e.limit_price;
           status = e.order_status;
           user_ref;
           cl_ord_id = e.cl_ord_id;
         } in
         Hashtbl.replace t.open_orders e.order_id oo
     | Filled | Canceled | Expired | Rejected ->
         Hashtbl.remove t.open_orders e.order_id);

    Mutex.unlock t.mutex;
    Concurrency.Exchange_wakeup.signal_all ()

  let set_open_orders_snapshot t orders =
    Mutex.lock t.mutex;
    Hashtbl.clear t.open_orders;
    List.iter (fun (o : Alpaca_types.order_record) ->
      let user_ref = match o.client_order_id with Some cid -> (try Some (int_of_string cid) with _ -> None) | None -> None in
      let oo = {
        order_id = o.id;
        symbol = t.symbol;
        side = (match o.side with Alpaca_types.Buy -> Buy | Sell -> Sell);
        qty = o.qty;
        cum_qty = o.filled_qty;
        remaining_qty = max 0.0 (o.qty -. o.filled_qty);
        limit_price = o.limit_price;
        status = status_of_alpaca_status o.status;
        user_ref;
        cl_ord_id = o.client_order_id;
      } in
      Hashtbl.replace t.open_orders o.id oo
    ) orders;
    t.initial_data_received <- true;
    Mutex.unlock t.mutex;
    Concurrency.Exchange_wakeup.signal_all ()
  let mark_ready t =
    Mutex.lock t.mutex;
    t.initial_data_received <- true;
    Mutex.unlock t.mutex;
    Concurrency.Exchange_wakeup.signal_all ()
end

let stores : (string, SymbolExecStore.t) Hashtbl.t = Hashtbl.create 16
let stores_mutex = Mutex.create ()

let get_or_create_store symbol =
  Mutex.lock stores_mutex;
  let store =
    match Hashtbl.find_opt stores symbol with
    | Some s -> s
    | None ->
        let s = SymbolExecStore.create symbol 1024 in
        Hashtbl.replace stores symbol s;
        s
  in
  Mutex.unlock stores_mutex;
  store

let active_conn : Websocket_lwt_unix.conn option ref = ref None

let get_open_order symbol order_id =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
      Mutex.lock store.mutex;
      let res = Hashtbl.find_opt store.open_orders order_id in
      Mutex.unlock store.mutex;
      res
  | None -> None

let remove_open_order symbol order_id =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
      Mutex.lock store.mutex;
      Hashtbl.remove store.open_orders order_id;
      Mutex.unlock store.mutex
  | None -> ()

let get_open_orders symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
      Mutex.lock store.mutex;
      let res = Hashtbl.fold (fun _ o acc -> o :: acc) store.open_orders [] in
      Mutex.unlock store.mutex;
      res
  | None -> []

let get_all_symbols () =
  Mutex.lock stores_mutex;
  let syms = Hashtbl.fold (fun k _ acc -> k :: acc) stores [] in
  Mutex.unlock stores_mutex;
  syms

let get_current_position symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
      Mutex.lock store.mutex;
      let pos = store.write_pos in
      Mutex.unlock store.mutex;
      pos
  | None -> 0

let get_current_position_fast symbol =
  let store = get_or_create_store symbol in
  (fun () ->
    Mutex.lock store.mutex;
    let pos = store.write_pos in
    Mutex.unlock store.mutex;
    pos)

let has_execution_data symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
      Mutex.lock store.mutex;
      let res = store.initial_data_received in
      Mutex.unlock store.mutex;
      res
  | None -> false

let has_execution_data_fast symbol =
  let store = get_or_create_store symbol in
  (fun () ->
    Mutex.lock store.mutex;
    let res = store.initial_data_received in
    Mutex.unlock store.mutex;
    res)

let read_execution_events symbol start_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
      Mutex.lock store.mutex;
      let current_pos = store.write_pos in
      let start_idx = max start_pos (current_pos - store.capacity) in
      let events = ref [] in
      for i = current_pos - 1 downto start_idx do
        let idx = i mod store.capacity in
        events := store.buffer.(idx) :: !events
      done;
      Mutex.unlock store.mutex;
      !events
  | None -> []

let iter_execution_events symbol start_pos f =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
      Mutex.lock store.mutex;
      let current_pos = store.write_pos in
      let start_idx = max start_pos (current_pos - store.capacity) in
      for i = start_idx to current_pos - 1 do
        let idx = i mod store.capacity in
        f store.buffer.(idx)
      done;
      Mutex.unlock store.mutex;
      current_pos
  | None -> start_pos

let fold_open_orders symbol ~init ~f =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
      Mutex.lock store.mutex;
      let res = Hashtbl.fold (fun _ o acc -> f acc o) store.open_orders init in
      Mutex.unlock store.mutex;
      res
  | None -> init

let initialize symbols =
  List.iter (fun sym ->
    let store = get_or_create_store sym in
    SymbolExecStore.mark_ready store
  ) symbols

let bootstrap_open_orders () =
  Alpaca_rest.get_open_orders () >>= function
  | Ok orders ->
      let grouped = Hashtbl.create 8 in
      List.iter (fun (o : Alpaca_types.order_record) ->
        ignore (get_or_create_store o.symbol);
        let existing = try Hashtbl.find grouped o.symbol with _ -> [] in
        Hashtbl.replace grouped o.symbol (o :: existing)
      ) orders;
      Mutex.lock stores_mutex;
      Hashtbl.iter (fun sym store ->
        let sym_orders = try Hashtbl.find grouped sym with _ -> [] in
        SymbolExecStore.set_open_orders_snapshot store sym_orders
      ) stores;
      Mutex.unlock stores_mutex;
      Logging.debug_f ~section "Bootstrapped %d open orders across symbols" (List.length orders);
      Lwt.return_unit
  | Error err ->
      Logging.error_f ~section "Failed to bootstrap open orders: %s" err;
      Mutex.lock stores_mutex;
      Hashtbl.iter (fun _store_sym store ->
        SymbolExecStore.mark_ready store
      ) stores;
      Mutex.unlock stores_mutex;
      Lwt.return_unit

let handle_trade_update json =
  let open Yojson.Safe.Util in
  let event = json |> member "event" |> to_string_option |> Option.value ~default:"" in
  let order_json = json |> member "order" in
  let ord = Alpaca_rest.parse_order_json order_json in
  let side = match ord.side with Alpaca_types.Buy -> Buy | Sell -> Sell in
  let is_amended = false in
  let price =
    match json |> member "price" with
    | `Float f -> f
    | `Int i -> float_of_int i
    | `String s -> (try float_of_string s with _ -> Option.value ord.limit_price ~default:0.0)
    | _ -> Option.value ord.limit_price ~default:0.0
  in
  let exec_event = {
    order_id = ord.id;
    symbol = ord.symbol;
    order_status = status_of_alpaca_status ord.status;
    limit_price = ord.limit_price;
    side;
    remaining_qty = max 0.0 (ord.qty -. ord.filled_qty);
    filled_qty = ord.filled_qty;
    avg_price = price;
    timestamp = Unix.time ();
    is_amended;
    cl_ord_id = ord.client_order_id;
  } in
  Logging.debug_f ~section "Trade update [%s]: order %s %s %s %.4f @ %.4f (filled: %.4f, remaining: %.4f)"
    event ord.id ord.symbol (match side with Buy -> "BUY" | Sell -> "SELL") ord.qty price ord.filled_qty exec_event.remaining_qty;
  let store = get_or_create_store ord.symbol in
  SymbolExecStore.push_event store exec_event;

  (* Publish to centralized fill event bus for Discord notifications if live trading is enabled *)
  (if not !(Alpaca_types.Config.is_paper) && (event = "fill" || exec_event.order_status = Filled) then begin
    let fill_value = ord.filled_qty *. price in
    let maker_fee_rate =
      match Dio_exchange.Exchange_intf.Registry.get "alpaca" with
      | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
          (match Ex.get_fees ~symbol:ord.symbol with (Some f, _) -> f | _ -> 0.0)
      | None -> 0.0
    in
    let fee = fill_value *. maker_fee_rate in
    Concurrency.Fill_event_bus.publish_fill {
      venue = "alpaca";
      symbol = ord.symbol;
      side = (if side = Buy then "buy" else "sell");
      amount = ord.filled_qty;
      fill_price = price;
      value = fill_value;
      fee;
      timestamp = Unix.time ();
      order_id = ord.id;
      trade_id = ord.id;
    }
  end);

  if event = "fill" || event = "partial_fill" || event = "canceled" || event = "rejected" then
    Lwt.async (fun () -> Alpaca_balances.update_balances ())

let handle_message_str content =
  let trimmed = String.trim content in
  if trimmed <> "" then try
    let json = Yojson.Safe.from_string trimmed in
    let items = match json with `List l -> l | _ -> [json] in
    List.iter (fun item ->
      let open Yojson.Safe.Util in
      let stream = item |> member "stream" |> to_string_option |> Option.value ~default:"" in
      match stream with
      | "trade_updates" ->
          let data = item |> member "data" in
          handle_trade_update data
      | "authorization" ->
          let data = item |> member "data" in
          let status = data |> member "status" |> to_string_option |> Option.value ~default:"" in
          let action = data |> member "action" |> to_string_option |> Option.value ~default:"" in
          Logging.debug_f ~section "Alpaca Trading WS authorization status: %s (action: %s)" status action
      | "listening" ->
          let data = item |> member "data" in
          let streams = data |> member "streams" |> to_list |> List.filter_map to_string_option in
          Logging.debug_f ~section "Alpaca Trading WS listening on streams: [%s]" (String.concat ", " streams)
      | "error" ->
          let msg = item |> member "data" |> member "message" |> to_string_option |> Option.value ~default:"" in
          Logging.error_f ~section "Alpaca Trading WS error: %s" msg
      | other ->
          Logging.debug_f ~section "Alpaca Trading WS frame (%s): %s" other (Yojson.Safe.to_string item)
    ) items
  with exn ->
    Logging.error_f ~section "Failed to parse Alpaca trading WS frame: %s (content: %s)"
      (Printexc.to_string exn) content

let connect_and_monitor ~on_failure ~on_connected ~on_heartbeat =
  let url_str = Alpaca_types.Config.trading_ws_url () in
  let uri = Uri.of_string url_str in
  let host = Uri.host uri |> Option.value ~default:"paper-api.alpaca.markets" in
  let port = Uri.port uri |> Option.value ~default:443 in

  Lwt.catch (fun () ->
    Lwt_unix.getaddrinfo host (string_of_int port) [ Unix.AI_FAMILY Unix.PF_INET ] >>= fun addresses ->
    let ip =
      match addresses with
      | { Unix.ai_addr = Unix.ADDR_INET (addr, _); _ } :: _ -> Ipaddr_unix.of_inet_addr addr
      | _ -> failwith ("Failed to resolve host " ^ host)
    in
    let client = `TLS (`Hostname host, `IP ip, `Port port) in
    let ctx = Lazy.force Conduit_lwt_unix.default_ctx in
    Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->
    active_conn := Some conn;
    Logging.debug_f ~section "Connected to Alpaca Trading WS at %s" url_str;

    (* Authenticate *)
    let auth_msg = `Assoc [
      ("action", `String "authenticate");
      ("data", `Assoc [
        ("key_id", `String (Alpaca_types.Config.api_key ()));
        ("secret_key", `String (Alpaca_types.Config.api_secret ()));
      ]);
    ] |> Yojson.Safe.to_string in
    Logging.debug ~section "Sending Alpaca Trading WS authentication...";
    Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:auth_msg ()) >>= fun () ->

    (* Listen on trade_updates *)
    let listen_msg = `Assoc [
      ("action", `String "listen");
      ("data", `Assoc [
        ("streams", `List [`String "trade_updates"]);
      ]);
    ] |> Yojson.Safe.to_string in
    Logging.debug ~section "Sending Alpaca Trading WS listen request for trade_updates...";
    Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:listen_msg ()) >>= fun () ->

    on_connected ();
    bootstrap_open_orders () >>= fun () ->

    let rec read_loop () =
      Websocket_lwt_unix.read conn >>= fun frame ->
      on_heartbeat ();
      (match frame.Websocket.Frame.opcode with
       | Websocket.Frame.Opcode.Ping ->
           let pong_frame = Websocket.Frame.create ~opcode:Websocket.Frame.Opcode.Pong ~content:frame.Websocket.Frame.content () in
           Websocket_lwt_unix.write conn pong_frame
       | Websocket.Frame.Opcode.Close ->
           Lwt.fail (Failure "Alpaca Trading WS received Close frame from server")
       | _ ->
           let content = String.trim frame.Websocket.Frame.content in
           if content <> "" then handle_message_str content;
           Lwt.return_unit) >>= fun () ->
      read_loop ()
    in
    read_loop ()
  ) (fun exn ->
    active_conn := None;
    let err = Printexc.to_string exn in
    Logging.error_f ~section "Alpaca trading WS disconnected: %s" err;
    on_failure err;
    Lwt.return_unit
  )
