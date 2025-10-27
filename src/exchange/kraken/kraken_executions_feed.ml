(** Kraken Executions Feed - WebSocket v2 authenticated executions subscription with lock-free ring buffers 
 *  Tracks open orders and order completions per asset using event-driven, lock-free architecture
 *)

open Lwt.Infix

let section = "kraken_executions"
let ring_buffer_size = 512

(** Execution event types from Kraken WebSocket *)
type exec_type =
  | PendingNew
  | New
  | Trade
  | PartiallyFilled
  | Filled
  | Canceled
  | Expired
  | Amended
  | Restated
  | Rejected
  | Unknown of string

let string_of_exec_type = function
  | PendingNew -> "pending_new"
  | New -> "new"
  | Trade -> "trade"
  | PartiallyFilled -> "partially_filled"
  | Filled -> "filled"
  | Canceled -> "canceled"
  | Expired -> "expired"
  | Amended -> "amended"
  | Restated -> "restated"
  | Rejected -> "rejected"
  | Unknown s -> s

let exec_type_of_string = function
  | "pending_new" -> PendingNew
  | "new" -> New
  | "trade" -> Trade
  | "partially_filled" -> PartiallyFilled
  | "filled" -> Filled
  | "canceled" -> Canceled
  | "expired" -> Expired
  | "amended" -> Amended
  | "restated" -> Restated
  | "rejected" -> Rejected
  | s -> Unknown s

(** Order side *)
type side = Buy | Sell

let string_of_side = function
  | Buy -> "buy"
  | Sell -> "sell"

let side_of_string = function
  | "buy" -> Buy
  | "sell" -> Sell
  | _ -> Buy

(** Order status *)
type order_status =
  | PendingNewStatus
  | NewStatus
  | PartiallyFilledStatus
  | FilledStatus
  | CanceledStatus
  | ExpiredStatus
  | RejectedStatus
  | UnknownStatus of string

let string_of_order_status = function
  | PendingNewStatus -> "pending_new"
  | NewStatus -> "new"
  | PartiallyFilledStatus -> "partially_filled"
  | FilledStatus -> "filled"
  | CanceledStatus -> "canceled"
  | ExpiredStatus -> "expired"
  | RejectedStatus -> "rejected"
  | UnknownStatus s -> s

let order_status_of_string = function
  | "pending_new" -> PendingNewStatus
  | "new" -> NewStatus
  | "partially_filled" -> PartiallyFilledStatus
  | "filled" -> FilledStatus
  | "canceled" -> CanceledStatus
  | "expired" -> ExpiredStatus
  | "rejected" -> RejectedStatus
  | s -> UnknownStatus s

(** Execution event - represents an order status change or fill *)
type execution_event = {
  order_id: string;
  symbol: string;
  exec_type: exec_type;
  order_status: order_status;
  side: side;
  order_qty: float;
  cum_qty: float;
  cum_cost: float;
  avg_price: float;
  limit_price: float option;
  last_qty: float option;
  last_price: float option;
  fee: float option;
  trade_id: int64 option;
  order_userref: int option;
  cl_ord_id: string option;
  timestamp: float;
}

(** Open order information *)
type open_order = {
  order_id: string;
  symbol: string;
  side: side;
  order_qty: float;
  cum_qty: float;
  remaining_qty: float;
  limit_price: float option;
  avg_price: float;
  cum_cost: float;
  order_status: order_status;
  order_userref: int option;  (* Optional numeric identifier for strategy filtering *)
  cl_ord_id: string option;   (* Client order ID for additional tracking *)
  last_updated: float;
}

(** Lock-free ring buffer for execution events using atomics *)
module RingBuffer = struct
  type 'a t = {
    data: 'a option Atomic.t array;
    write_pos: int Atomic.t;
    size: int;
  }

  let create size =
    {
      data = Array.init size (fun _ -> Atomic.make None);
      write_pos = Atomic.make 0;
      size;
    }

  (** Lock-free write - producer side *)
  let write buffer value =
    let pos = Atomic.get buffer.write_pos in
    Atomic.set buffer.data.(pos) (Some value);
    let new_pos = (pos + 1) mod buffer.size in
    Atomic.set buffer.write_pos new_pos;
    Telemetry.inc_counter (Telemetry.counter "execution_events_produced" ()) ()

  (** Wait-free read - consumer side *)
  let read_latest buffer =
    let pos = Atomic.get buffer.write_pos in
    let read_pos = if pos = 0 then buffer.size - 1 else pos - 1 in
    Atomic.get buffer.data.(read_pos)

  (** Read events from last known position - for domain consumers *)
  let read_since buffer last_pos =
    let current_pos = Atomic.get buffer.write_pos in
    if last_pos = current_pos then
      []
    else
      let rec collect acc pos =
        if pos = current_pos then
          List.rev acc
        else
          match Atomic.get buffer.data.(pos) with
          | Some event -> collect (event :: acc) ((pos + 1) mod buffer.size)
          | None -> collect acc ((pos + 1) mod buffer.size)
      in
      collect [] last_pos

  (** Read all events currently in the buffer *)
  let read_all buffer =
    let rec collect_all acc i =
      if i >= buffer.size then
        List.rev acc
      else
        match Atomic.get buffer.data.(i) with
        | Some event -> collect_all (event :: acc) (i + 1)
        | None -> collect_all acc (i + 1)
    in
    collect_all [] 0
end

(** Per-symbol execution store *)
type symbol_store = {
  events_buffer: execution_event RingBuffer.t;
  open_orders: (string, open_order) Hashtbl.t;
  ready: bool Atomic.t;
  last_event_time: float Atomic.t;
}

(** Global stores per symbol *)
let symbol_stores : (string, symbol_store) Hashtbl.t = Hashtbl.create 64

(** Global order_id to symbol mapping for handling minimal events *)
let order_to_symbol : (string, string) Hashtbl.t = Hashtbl.create 128
let global_orders_mutex = Mutex.create ()

let initialization_mutex = Mutex.create ()
let ready_condition = Lwt_condition.create ()

(** Get or create store for a symbol - wait-free after init *)
let get_symbol_store symbol =
  match Hashtbl.find_opt symbol_stores symbol with
  | Some store -> store
  | None ->
      Mutex.lock initialization_mutex;
      let store = 
        match Hashtbl.find_opt symbol_stores symbol with
        | Some s -> s
        | None ->
            let s = {
              events_buffer = RingBuffer.create ring_buffer_size;
              open_orders = Hashtbl.create 32;
              ready = Atomic.make false;
              last_event_time = Atomic.make 0.0;
            } in
            Hashtbl.add symbol_stores symbol s;
            Logging.debug_f ~section "Created execution store for %s" symbol;
            s
      in
      Mutex.unlock initialization_mutex;
      store

(** Notify that execution data is ready *)
let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try
      Lwt_condition.broadcast ready_condition ()
    with Invalid_argument _ ->
      (* Ignore - some waiters may have timed out or been cancelled *)
      ())
  end

(** Check if we have execution data for a symbol *)
let has_execution_data symbol =
  try
    let store = get_symbol_store symbol in
    Atomic.get store.last_event_time > 0.0
  with _ -> false

(** Wait until execution data is available for symbols *)
let wait_for_execution_data_lwt symbols timeout_seconds =
  let deadline = Unix.gettimeofday () +. timeout_seconds in
  let rec loop () =
    if List.for_all has_execution_data symbols then
      Lwt.return_true
    else
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0 then
        Lwt.return_false
      else
        Lwt.pick [
          (Lwt_condition.wait ready_condition >|= fun () -> `Again);
          (Lwt_unix.sleep remaining >|= fun () -> `Timeout)
        ] >>= function
        | `Again -> loop ()
        | `Timeout -> Lwt.return (List.for_all has_execution_data symbols)
  in
  loop ()

let wait_for_execution_data = wait_for_execution_data_lwt

(** Get open orders for a symbol - hot path *)
let[@inline always] get_open_orders symbol =
  let store = get_symbol_store symbol in
  Mutex.lock global_orders_mutex;
  let orders = Hashtbl.fold (fun _id order acc -> order :: acc) store.open_orders [] in
  Mutex.unlock global_orders_mutex;
  orders

(** Get specific open order by ID *)
let[@inline always] get_open_order symbol order_id =
  let store = get_symbol_store symbol in
  Mutex.lock global_orders_mutex;
  let order_opt = Hashtbl.find_opt store.open_orders order_id in
  Mutex.unlock global_orders_mutex;
  order_opt

(** Check if order exists *)
let[@inline always] has_open_order symbol order_id =
  let store = get_symbol_store symbol in
  Mutex.lock global_orders_mutex;
  let exists = Hashtbl.mem store.open_orders order_id in
  Mutex.unlock global_orders_mutex;
  exists

(** Get all symbols that have execution stores (initialized symbols) *)
let get_all_symbols () =
  Mutex.lock global_orders_mutex;
  let symbols = ref [] in
  Hashtbl.iter (fun symbol _store -> symbols := symbol :: !symbols) symbol_stores;
  let result = !symbols in
  Mutex.unlock global_orders_mutex;
  result

(** Get count of open orders for a symbol *)
let[@inline always] count_open_orders symbol =
  let store = get_symbol_store symbol in
  Mutex.lock global_orders_mutex;
  let count = Hashtbl.length store.open_orders in
  Mutex.unlock global_orders_mutex;
  count

(** Get count of open orders by side for a symbol *)
let[@inline always] count_open_orders_by_side symbol =
  let store = get_symbol_store symbol in
  Mutex.lock global_orders_mutex;
  let buys = ref 0 in
  let sells = ref 0 in
  Hashtbl.iter (fun _id order ->
    match order.side with
    | Buy -> incr buys
    | Sell -> incr sells
  ) store.open_orders;
  Mutex.unlock global_orders_mutex;
  (!buys, !sells)

(** Read latest execution events for a symbol since last position *)
let[@inline always] read_execution_events symbol last_pos =
  let store = get_symbol_store symbol in
  RingBuffer.read_since store.events_buffer last_pos

(** Get current write position for tracking consumption *)
let[@inline always] get_current_position symbol =
  let store = get_symbol_store symbol in
  Atomic.get store.events_buffer.write_pos

(** Update open orders based on execution event *)
let update_open_orders store (event : execution_event) =
  Mutex.lock global_orders_mutex;
  
  (* Determine if order should be in open orders based on order_status (most reliable) *)
  let is_terminal_status = match event.order_status with
    | FilledStatus | CanceledStatus | ExpiredStatus | RejectedStatus -> true
    | PendingNewStatus | NewStatus | PartiallyFilledStatus | UnknownStatus _ -> false
  in
  
  let raw_remaining_qty = event.order_qty -. event.cum_qty in
  let remaining_qty =
    if raw_remaining_qty <= 0.0 then 0.0 else raw_remaining_qty
  in
  let epsilon =
    let abs_order_qty = abs_float event.order_qty in
    if abs_order_qty = 0.0 then 1e-12 else abs_order_qty *. 1e-6
  in
  let is_effectively_filled = remaining_qty <= epsilon in
  
  if is_terminal_status || is_effectively_filled then begin
    (* Terminal state - remove from open orders if present *)
    if Hashtbl.mem store.open_orders event.order_id then begin
      Hashtbl.remove store.open_orders event.order_id;
      
      (* Remove from global order mapping *)
      Hashtbl.remove order_to_symbol event.order_id;
      
      if is_terminal_status then
        Logging.info_f ~section "Removed order: %s [%s] status=%s exec_type=%s"
          event.order_id event.symbol 
          (string_of_order_status event.order_status)
          (string_of_exec_type event.exec_type)
      else
        Logging.info_f ~section "Removed order with zero remaining qty: %s [%s] status=%s exec_type=%s remaining=%.12f qty=%.12f"
          event.order_id event.symbol
          (string_of_order_status event.order_status)
          (string_of_exec_type event.exec_type)
          remaining_qty event.order_qty
    end else begin
      (* Terminal event for order we never tracked (e.g., canceled before we saw it) *)
      if is_terminal_status then
        Logging.debug_f ~section "Terminal event for untracked order: %s [%s] status=%s"
          event.order_id event.symbol (string_of_order_status event.order_status)
      else
        Logging.debug_f ~section "Ignored effectively filled order for untracked id: %s [%s] status=%s"
          event.order_id event.symbol (string_of_order_status event.order_status)
    end
  end else begin
    (* Non-terminal state - add or update in open orders *)
    let order = {
      order_id = event.order_id;
      symbol = event.symbol;
      side = event.side;
      order_qty = event.order_qty;
      cum_qty = event.cum_qty;
      remaining_qty;
      limit_price = event.limit_price;
      avg_price = event.avg_price;
      cum_cost = event.cum_cost;
      order_status = event.order_status;
      order_userref = event.order_userref;
      cl_ord_id = event.cl_ord_id;
      last_updated = event.timestamp;
    } in
    
    let was_present = Hashtbl.mem store.open_orders event.order_id in
    Hashtbl.replace store.open_orders event.order_id order;
    
    (* Add to global order mapping *)
    Hashtbl.replace order_to_symbol event.order_id event.symbol;
    
    if was_present then begin
      Logging.info_f ~section "Updated open order: %s [%s] %.8f@%.2f (filled: %.8f/%.8f) status=%s"
        event.order_id event.symbol remaining_qty 
        (Option.value event.limit_price ~default:0.0) 
        event.cum_qty event.order_qty
        (string_of_order_status event.order_status)
    end else begin
      Logging.debug_f ~section "Added new open order: %s [%s] %s side %.8f@%.2f status=%s"
        event.order_id event.symbol 
        (string_of_side event.side)
        event.order_qty
        (Option.value event.limit_price ~default:0.0)
        (string_of_order_status event.order_status)
    end;
    
    (* Log trade fills separately for visibility *)
    if event.exec_type = Trade then begin
      Logging.info_f ~section "Trade fill: %s [%s] qty=%.8f price=%.2f (total filled: %.8f/%.8f)"
        event.order_id event.symbol 
        (Option.value event.last_qty ~default:0.0)
        (Option.value event.last_price ~default:0.0)
        event.cum_qty event.order_qty
    end
  end;
  
  Mutex.unlock global_orders_mutex

(** Parse float from JSON *)
let parse_float_opt json field =
  try
    let open Yojson.Safe.Util in
    match member field json with
    | `Float f -> Some f
    | `Int i -> Some (float_of_int i)
    | `String s -> (try Some (float_of_string s) with _ -> None)
    | `Intlit s -> (try Some (float_of_string s) with _ -> None)
    | _ -> None
  with _ -> None

(** Parse int64 from JSON *)
let parse_int64_opt json field =
  try
    let open Yojson.Safe.Util in
    match member field json with
    | `Int i -> Some (Int64.of_int i)
    | `Intlit s -> (try Some (Int64.of_string s) with _ -> None)
    | `String s -> (try Some (Int64.of_string s) with _ -> None)
    | _ -> None
  with _ -> None

(** Parse int from JSON *)
let parse_int_opt json field =
  try
    let open Yojson.Safe.Util in
    match member field json with
    | `Int i -> Some i
    | `Intlit s -> (try Some (int_of_string s) with _ -> None)
    | `String s -> (try Some (int_of_string s) with _ -> None)
    | _ -> None
  with _ -> None

(** Parse execution event from JSON data - handles both full and minimal events *)
let parse_execution_event json =
  try
    let open Yojson.Safe.Util in
    let order_id = member "order_id" json |> to_string in
    let exec_type_str = member "exec_type" json |> to_string in
    let order_status_str = member "order_status" json |> to_string in
    
    (* Symbol may be missing in minimal status updates (e.g., cancellations) *)
    let symbol_opt = member "symbol" json |> to_string_option in

    (* For minimal events, look up symbol from our mapping *)
    let symbol = match symbol_opt with
      | Some s -> s
      | None ->
          Mutex.lock global_orders_mutex;
          let s = Hashtbl.find_opt order_to_symbol order_id in
          Mutex.unlock global_orders_mutex;
          (match s with
           | Some sym -> sym
           | None ->
               (* If we don't have the symbol, we can't process this event *)
               Logging.warn_f ~section "Cannot process minimal event for order %s: symbol not found in mapping" order_id;
               raise (Failure "missing_symbol"))
    in
    
    (* Get existing order to fill in missing fields for minimal events *)
    let store = get_symbol_store symbol in
    Mutex.lock global_orders_mutex;
    let existing_order = Hashtbl.find_opt store.open_orders order_id in
    Mutex.unlock global_orders_mutex;
    
    (* Extract fields from JSON, using existing order as fallback *)
    let side_str = 
      match member "side" json |> to_string_option with
      | Some s -> s
      | None ->
          (match existing_order with
           | Some order -> string_of_side order.side
           | None -> "buy")  (* Default if we have no existing data *)
    in
    
    let order_qty = 
      match parse_float_opt json "order_qty" with
      | Some q -> q
      | None ->
          (match existing_order with
           | Some order -> order.order_qty
           | None -> 0.0)
    in
    
    let cum_qty = 
      match parse_float_opt json "cum_qty" with
      | Some q -> q
      | None ->
          (match existing_order with
           | Some order -> order.cum_qty
           | None -> 0.0)
    in
    
    let cum_cost = 
      match parse_float_opt json "cum_cost" with
      | Some c -> c
      | None ->
          (match existing_order with
           | Some order -> order.cum_cost
           | None -> 0.0)
    in
    
    let avg_price = 
      match parse_float_opt json "avg_price" with
      | Some p -> p
      | None ->
          (match existing_order with
           | Some order -> order.avg_price
           | None -> 0.0)
    in
    
    let limit_price = 
      match parse_float_opt json "limit_price" with
      | Some _ as p -> p
      | None ->
          (match existing_order with
           | Some order -> order.limit_price
           | None -> None)
    in
    
    let last_qty = parse_float_opt json "last_qty" in
    let last_price = parse_float_opt json "last_price" in
    
    (* Parse fees - handle both single fee and fees array *)
    let fee = 
      match parse_float_opt json "fee_usd_equiv" with
      | Some f -> Some f
      | None ->
          try
            let fees = member "fees" json |> to_list in
            match fees with
            | fee_obj :: _ ->
                parse_float_opt fee_obj "qty"
            | [] -> None
          with _ -> None
    in
    
    let trade_id = parse_int64_opt json "trade_id" in
    
    let order_userref = 
      match parse_int_opt json "order_userref" with
      | Some _ as u -> u
      | None ->
          (match existing_order with
           | Some order -> order.order_userref
           | None -> None)
    in
    
    let cl_ord_id = 
      match member "cl_ord_id" json |> to_string_option with
      | Some _ as c -> c
      | None ->
          (match existing_order with
           | Some order -> order.cl_ord_id
           | None -> None)
    in
    
    let _timestamp_str = member "timestamp" json |> to_string in
    (* Use current time for event timestamp - proper RFC3339 parsing can be added later if needed *)
    let timestamp = Unix.gettimeofday () in
    
    (* Log when we're processing a minimal event *)
    if symbol_opt = None then
      Logging.debug_f ~section "Processing minimal event for order %s [%s]: status=%s exec_type=%s"
        order_id symbol order_status_str exec_type_str;
    
    Some {
      order_id;
      symbol;
      exec_type = exec_type_of_string exec_type_str;
      order_status = order_status_of_string order_status_str;
      side = side_of_string side_str;
      order_qty;
      cum_qty;
      cum_cost;
      avg_price;
      limit_price;
      last_qty;
      last_price;
      fee;
      trade_id;
      order_userref;
      cl_ord_id;
      timestamp;
    }
  with exn ->
    Logging.warn_f ~section "Failed to parse execution event: %s | JSON: %s" 
      (Printexc.to_string exn)
      (Yojson.Safe.to_string json);
    None

(** Handle execution snapshot *)
let handle_snapshot json on_heartbeat =
  try
    let open Yojson.Safe.Util in
    let data = member "data" json |> to_list in
    
    Logging.debug_f ~section "Processing execution snapshot with %d items" (List.length data);
    
    List.iter (fun item ->
      match parse_execution_event item with
      | Some event ->
          let store = get_symbol_store event.symbol in
          RingBuffer.write store.events_buffer event;
          update_open_orders store event;
          Atomic.set store.last_event_time event.timestamp;
          notify_ready store;
          Telemetry.inc_counter (Telemetry.counter "execution_snapshot_items" ()) ();
          (* Update connection heartbeat *)
          on_heartbeat ()
      | None -> ()
    ) data;
    
    Logging.debug_f ~section "Execution snapshot processed"
  with exn ->
    Logging.error_f ~section "Failed to process execution snapshot: %s"
      (Printexc.to_string exn)

(** Handle execution update *)
let handle_update json on_heartbeat =
  try
    let open Yojson.Safe.Util in
    let data = member "data" json |> to_list in
    
    List.iter (fun item ->
      match parse_execution_event item with
      | Some event ->
          let store = get_symbol_store event.symbol in
          RingBuffer.write store.events_buffer event;
          update_open_orders store event;
          Atomic.set store.last_event_time event.timestamp;
          notify_ready store;
          Telemetry.inc_counter (Telemetry.counter "execution_updates" ()) ();
          (* Update connection heartbeat *)
          on_heartbeat ()
      | None -> ()
    ) data
  with exn ->
    Logging.error_f ~section "Failed to process execution update: %s"
      (Printexc.to_string exn)

(** WebSocket message handler *)
let handle_message message on_heartbeat =
  try
    let json = Yojson.Safe.from_string message in
    let open Yojson.Safe.Util in
    let channel = member "channel" json |> to_string_option in
    let msg_type = member "type" json |> to_string_option in
    let method_type = member "method" json |> to_string_option in
    
    (match channel with
    | Some "heartbeat" -> ()
    | _ ->
        Logging.debug_f ~section "Executions message received: channel=%s, type=%s, method=%s"
          (Option.value channel ~default:"none")
          (Option.value msg_type ~default:"none")
          (Option.value method_type ~default:"none"));
    
    match channel, msg_type, method_type with
    | Some "executions", Some "snapshot", _ ->
        handle_snapshot json on_heartbeat
    | Some "executions", Some "update", _ ->
        handle_update json on_heartbeat
    | Some "heartbeat", _, _ ->
        on_heartbeat () (* Update connection heartbeat *)
    | _, _, Some "subscribe" ->
        let success = member "success" json |> to_bool_option in
        (match success with
        | Some true -> 
            Logging.info ~section "Subscribed to executions feed";
            Logging.debug_f ~section "Subscription response: %s" message;
            Telemetry.inc_counter (Telemetry.counter "executions_subscribed" ()) ()
        | Some false -> 
            let error = member "error" json |> to_string_option in
            Logging.error_f ~section "Subscription failed: %s" 
              (Option.value error ~default:"Unknown error")
        | None -> ())
    | Some "status", _, _ ->
        Logging.debug ~section "Status message received"
    | _ ->
        Logging.info_f ~section "Unhandled execution message: %s" message
  with exn ->
    Logging.error_f ~section "Error handling message: %s - %s" 
      (Printexc.to_string exn) message


(** Message handling loop - runs in background *)
let start_message_handler conn token on_failure on_heartbeat =
  (* Subscribe to executions with snapshot *)
  let subscribe_msg = `Assoc [
    ("method", `String "subscribe");
    ("params", `Assoc [
      ("channel", `String "executions");
      ("token", `String token);
      ("snap_orders", `Bool true);
      ("snap_trades", `Bool true);
      ("order_status", `Bool true)
    ])
  ] in
  let msg_str = Yojson.Safe.to_string subscribe_msg in
  Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg_str ()) >>= fun () ->

  (* Message loop *)
  let rec msg_loop () =
    Lwt.catch (fun () ->
      Websocket_lwt_unix.read conn >>= function
      | {Websocket.Frame.opcode = Websocket.Frame.Opcode.Close; _} ->
          Logging.warn ~section "WebSocket connection closed by server";
          on_failure "Connection closed by server";
          Lwt.return_unit
      | frame ->
          let content = frame.Websocket.Frame.content in
          handle_message content on_heartbeat;
          msg_loop ()
    ) (function
      | End_of_file ->
          Logging.warn ~section "Executions WebSocket connection closed unexpectedly (End_of_file)";
          (* Notify supervisor of connection failure *)
          on_failure "Connection closed unexpectedly (End_of_file)";
          Lwt.return_unit
      | exn ->
          Logging.error_f ~section "Executions WebSocket error during read: %s" (Printexc.to_string exn);
          (* Notify supervisor of connection failure *)
          on_failure (Printf.sprintf "WebSocket error: %s" (Printexc.to_string exn));
          Lwt.return_unit
    )
  in
  msg_loop ()

(** WebSocket connection to Kraken authenticated endpoint - establishes connection and starts message handler *)
let connect_and_subscribe token ~on_failure ~on_heartbeat =
  let uri = Uri.of_string "wss://ws-auth.kraken.com/v2" in

  Logging.info_f ~section "Connecting to Kraken authenticated WebSocket for executions...";

  (* Resolve hostname to IP *)
  Lwt_unix.getaddrinfo "ws-auth.kraken.com" "443" [Unix.AI_FAMILY Unix.PF_INET] >>= fun addresses ->
  let ip = match addresses with
    | {Unix.ai_addr = Unix.ADDR_INET (addr, _); _} :: _ ->
        Ipaddr_unix.of_inet_addr addr
    | _ -> failwith "Failed to resolve ws-auth.kraken.com"
  in
  let client = `TLS (`Hostname "ws-auth.kraken.com", `IP ip, `Port 443) in
  let ctx = Lazy.force Conduit_lwt_unix.default_ctx in
  Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->

    Logging.info ~section "Authenticated WebSocket established, subscribing to executions";
    Lwt.async (fun () -> start_message_handler conn token on_failure on_heartbeat);
    Logging.info ~section "Executions WebSocket connection established";
    Lwt.return_unit

(** Initialize executions feed data stores *)
let initialize symbols =
  Logging.info_f ~section "Initializing executions feed for %d symbols" (List.length symbols);
  
  (* Pre-create all symbol stores during initialization *)
  List.iter (fun symbol ->
    let _store = get_symbol_store symbol in
    Logging.debug_f ~section "Created lock-free execution store for %s" symbol
  ) symbols;
  
  Logging.info ~section "Execution stores initialized - now operating lock-free"

