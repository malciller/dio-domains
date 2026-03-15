(** Hyperliquid Executions Feed *)

open Lwt.Infix

let section = "hyperliquid_executions_feed"

type side = Buy | Sell
type order_status = PendingNewStatus | NewStatus | PartiallyFilledStatus | FilledStatus | CanceledStatus | ExpiredStatus | RejectedStatus | UnknownStatus of string

type open_order = {
  order_id: string; 
  symbol: string; 
  side: side; 
  order_qty: float; 
  cum_qty: float;
  remaining_qty: float; 
  limit_price: float option; 
  order_status: order_status;
  order_userref: int option; 
  cl_ord_id: string option;
  last_updated: float;
}

type execution_event = {
  order_id: string; 
  symbol: string;
  order_status: order_status; 
  limit_price: float option; 
  side: side;
  order_qty: float; 
  cum_qty: float; 
  timestamp: float;
  trade_id: int64 option;
  last_qty: float option;
  last_price: float option;
  fee: float option;
  cl_ord_id: string option;
}

(** Event bus for order updates - publishes individual order changes *)
module OrderUpdateEventBus = Concurrency.Event_bus.Make(struct
  type t = execution_event
end)

(** Global order update event bus instance *)
let order_update_event_bus = OrderUpdateEventBus.create "hyperliquid_order_update"

(** Lock-free ring buffer for execution data *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol execution storage with readiness signalling *)
type store = {
  events_buffer: execution_event RingBuffer.t;
  open_orders: (string, open_order) Hashtbl.t;
  ready: bool Atomic.t;
  processed_tids: (int64, float) Hashtbl.t; (* trade_id -> arrival_time for deduplication *)
  orders_mutex: Mutex.t;
  tids_mutex: Mutex.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

(** Get or create store for a symbol - thread-safe initialization *)
let get_symbol_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      Mutex.lock initialization_mutex;
      let store = match Hashtbl.find_opt stores symbol with
        | Some store -> store
        | None ->
            let store = {
              events_buffer = RingBuffer.create 128;
              open_orders = Hashtbl.create 32;
              ready = Atomic.make false;
              processed_tids = Hashtbl.create 128;
              orders_mutex = Mutex.create ();
              tids_mutex = Mutex.create ();
            } in
            Hashtbl.add stores symbol store;
            store
      in
      Mutex.unlock initialization_mutex;
      store

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

let get_open_order symbol order_id =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let order = Hashtbl.find_opt store.open_orders order_id in
  Mutex.unlock store.orders_mutex;
  order

let get_open_orders symbol =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let orders = Hashtbl.fold (fun _ o acc -> o :: acc) store.open_orders [] in
  Mutex.unlock store.orders_mutex;
  orders

let get_current_position symbol =
  let store = get_symbol_store symbol in
  RingBuffer.get_position store.events_buffer

let read_execution_events symbol last_pos =
  let store = get_symbol_store symbol in
  RingBuffer.read_since store.events_buffer last_pos

let has_execution_data symbol =
  let store = get_symbol_store symbol in
  Atomic.get store.ready

let wait_for_execution_data symbols timeout_seconds =
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

(** Internal helper to update open orders and generate an event - adopts Kraken's "update_open_orders" pattern *)
let update_orders_internal ?user_ref store (event : execution_event) =
  let now = Unix.gettimeofday () in
  Mutex.lock store.orders_mutex;
  
  let is_terminal = match event.order_status with
    | FilledStatus | CanceledStatus | RejectedStatus | ExpiredStatus -> true
    | _ -> false
  in
  
  let existing_order = 
    match Hashtbl.find_opt store.open_orders event.order_id with
    | Some o -> Some o
    | None -> 
        (match event.cl_ord_id with
         | Some clid -> 
             Hashtbl.fold (fun _ (o : open_order) acc -> 
               if o.cl_ord_id = Some clid then Some o else acc
             ) store.open_orders None
         | None -> None)
  in

  if is_terminal then begin
    (match existing_order with
     | Some existing when existing.order_id <> event.order_id ->
         Hashtbl.remove store.open_orders existing.order_id
     | _ -> ());
    Hashtbl.remove store.open_orders event.order_id;
  end else begin
    (* UserRef Recovery Logic:
       1. Use explicitly provided user_ref (from proactive inject_order)
       2. Use existing tracked user_ref
       3. Recover from cloid string (Hyperliquid encodes UserRef in cloid hex)
    *)
    let recovered_user_ref = match user_ref with
      | Some _ -> user_ref
      | None -> 
          (match existing_order with 
           | Some o -> o.order_userref 
           | None -> (match event.cl_ord_id with
               | Some clid ->
                   (try
                      let clean_clid = if String.starts_with ~prefix:"0x" clid then 
                        String.sub clid 2 (String.length clid - 2)
                      else clid in
                      
                      if String.length clean_clid >= 16 then
                        let last_part = String.sub clean_clid (String.length clean_clid - 16) 16 in
                        let uref = Int64.to_int (Int64.of_string ("0x" ^ last_part)) in
                        Logging.debug_f ~section "Recovered userref %d from cloid %s" uref clid;
                        Some uref
                      else
                        (try Some (int_of_string clid) with _ -> None)
                    with _ -> 
                      Logging.debug_f ~section "Failed to recover userref from cloid %s" clid;
                      None)
               | _ -> None))
    in
    
    let order : open_order = {
      order_id = event.order_id;
      symbol = event.symbol;
      side = event.side;
      order_qty = event.order_qty;
      cum_qty = event.cum_qty;
      remaining_qty = event.order_qty -. event.cum_qty;
      limit_price = event.limit_price;
      order_status = event.order_status;
      order_userref = recovered_user_ref;
      cl_ord_id = event.cl_ord_id;
      last_updated = now;
    } in
    
    (match existing_order with
     | Some existing when existing.order_id <> event.order_id ->
         Hashtbl.remove store.open_orders existing.order_id
     | _ -> ());
    Hashtbl.replace store.open_orders event.order_id order;
  end;
  Mutex.unlock store.orders_mutex;
  
  RingBuffer.write store.events_buffer event;
  OrderUpdateEventBus.publish order_update_event_bus event;
  notify_ready store

let inject_order ~symbol ~order_id ~side ~qty ~price ?user_ref ?cl_ord_id () =
  let store = get_symbol_store symbol in
  let now = Unix.gettimeofday () in
  let event : execution_event = {
    order_id;
    symbol;
    order_status = NewStatus;
    limit_price = Some price;
    side;
    order_qty = qty;
    cum_qty = 0.0;
    timestamp = now;
    trade_id = None;
    last_qty = None;
    last_price = None;
    fee = None;
    cl_ord_id;
  } in
  
  update_orders_internal ?user_ref store event;
  Logging.info_f ~section "Proactively injected open order: %s [%s] %s side %.8f @ %.2f (cloid: %s, userref: %s)"
    order_id symbol (if side = Buy then "buy" else "sell") qty price
    (Option.value cl_ord_id ~default:"none")
    (match user_ref with Some u -> string_of_int u | None -> "none")

let find_registered_symbol coin =
  match Hyperliquid_instruments_feed.resolve_symbol coin with
  | Some symbol -> 
      Mutex.lock initialization_mutex;
      let res = if Hashtbl.mem stores symbol then Some symbol else None in
      Mutex.unlock initialization_mutex;
      res
  | None ->
      let result = ref None in
      Mutex.lock initialization_mutex;
      Hashtbl.iter (fun registered_symbol _ ->
        if registered_symbol = coin then result := Some registered_symbol
        else if String.starts_with ~prefix:(coin ^ "/") registered_symbol then result := Some registered_symbol
      ) stores;
      Mutex.unlock initialization_mutex;
      !result

let process_order_updates data_json =
  let open Yojson.Safe.Util in
  match data_json with
  | `List orders ->
      List.iter (fun order_update ->
        let order_obj = member "order" order_update in
        let coin = member "coin" order_obj |> to_string in
        match find_registered_symbol coin with
        | Some symbol ->
            let order_id = (match member "oid" order_obj with `Int i -> string_of_int i | `String s -> s | _ -> "0") in
            let status = member "status" order_update |> to_string in
            let price = (match member "limitPx" order_obj with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0) in
            let qty = (match member "sz" order_obj with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0) in
            let side = if (member "side" order_obj |> to_string) = "B" then Buy else Sell in
            let cl_ord_id = member "cloid" order_obj |> to_string_option in
            
            let store = get_symbol_store symbol in
            let now = Unix.gettimeofday () in
            
            let order_status = match status with
              | "open" -> NewStatus
              | "filled" -> FilledStatus
              | "canceled" -> CanceledStatus
              | "rejected" -> RejectedStatus
              | "marginCanceled" -> CanceledStatus
              | s -> UnknownStatus s
            in
            
            let event : execution_event = {
              order_id;
              symbol;
              order_status;
              limit_price = Some price;
              side;
              order_qty = qty;
              cum_qty = 0.0;
              timestamp = now;
              trade_id = None;
              last_qty = None;
              last_price = None;
              fee = None;
              cl_ord_id;
            } in
            
            update_orders_internal store event;
            
            (match order_status with
             | FilledStatus | CanceledStatus | RejectedStatus ->
                 Logging.info_f ~section "Order %s: %s [%s]" (String.uppercase_ascii status) order_id symbol
             | NewStatus ->
                 Logging.debug_f ~section "Order OPEN: %s [%s] %.8f @ %.2f" order_id symbol qty price
             | _ -> ());
        | None -> ()
      ) orders
  | _ -> ()

let process_user_events data_json =
  let open Yojson.Safe.Util in
  (* Handle fills *)
  let fills = try member "fills" data_json |> to_list with _ -> [] in
  List.iter (fun fill ->
    let coin = member "coin" fill |> to_string in
    match find_registered_symbol coin with
    | Some symbol ->
        let order_id = (match member "oid" fill with `Int i -> string_of_int i | `String s -> s | _ -> "0") in
        let price = member "px" fill |> to_string |> float_of_string in
        let size = member "sz" fill |> to_string |> float_of_string in
        let side = if member "side" fill |> to_string = "B" then Buy else Sell in
        let tid = (match member "tid" fill with `Int i -> Int64.of_int i | `String s -> Int64.of_string s | _ -> 0L) in
        let fee = try member "fee" fill |> to_string |> float_of_string with _ -> 0.0 in
        let store = get_symbol_store symbol in
        
        (* Deduplicate trade fills *)
        let now = Unix.gettimeofday () in
        Mutex.lock store.tids_mutex;
        let already_processed = Hashtbl.mem store.processed_tids tid in
        if not already_processed then
          Hashtbl.add store.processed_tids tid now;
        Mutex.unlock store.tids_mutex;

        if not already_processed then begin
          Mutex.lock store.tids_mutex;
          let to_remove = ref [] in
          Hashtbl.iter (fun tid arrival_time ->
            if now -. arrival_time > 3600.0 then to_remove := tid :: !to_remove
          ) store.processed_tids;
          List.iter (Hashtbl.remove store.processed_tids) !to_remove;
          Mutex.unlock store.tids_mutex;

          Mutex.lock store.orders_mutex;
          let (existing_order : open_order option) = Hashtbl.find_opt store.open_orders order_id in
          let cum_qty = match existing_order with Some o -> o.cum_qty +. size | None -> size in
          let order_qty = match existing_order with Some o -> o.order_qty | None -> size in
          let is_filled = cum_qty >= (order_qty -. 1e-6) in
          let status = if is_filled then FilledStatus else PartiallyFilledStatus in
          let limit_price = match existing_order with Some o -> o.limit_price | None -> Some price in
          let cl_ord_id = match existing_order with Some o -> o.cl_ord_id | None -> None in
          Mutex.unlock store.orders_mutex;
          
          let event : execution_event = {
            order_id; symbol; order_status = status; limit_price; side;
            order_qty; cum_qty; timestamp = now; trade_id = Some tid;
            last_qty = Some size; last_price = Some price; fee = Some fee; cl_ord_id;
          } in
          
          update_orders_internal store event;
          
          if is_filled then
            Logging.info_f ~section "Order FILLED: %s [%s] %.8f @ %.2f (trade_id: %Ld)" order_id symbol size price tid
          else
            Logging.info_f ~section "Order PARTIALLY FILLED: %s [%s] %.8f @ %.2f (filled: %.8f/%.8f)" order_id symbol size price cum_qty order_qty
        end
    | None -> ()
  ) fills;

  (* Handle nonUserCancel *)
  let non_user_cancels = try member "nonUserCancel" data_json |> to_list with _ -> [] in
  List.iter (fun nuc ->
    let coin = member "coin" nuc |> to_string in
    match find_registered_symbol coin with
    | Some symbol ->
        let order_id = (match member "oid" nuc with `Int i -> string_of_int i | `String s -> s | _ -> "0") in
        let store = get_symbol_store symbol in
        let now = Unix.gettimeofday () in
        
        Mutex.lock store.orders_mutex;
        let existing_opt = Hashtbl.find_opt store.open_orders order_id in
        Mutex.unlock store.orders_mutex;
        
        (match existing_opt with
         | Some current_order ->
            let event : execution_event = {
              order_id; symbol; order_status = CanceledStatus;
              limit_price = current_order.limit_price; side = current_order.side;
              order_qty = current_order.order_qty; cum_qty = current_order.cum_qty;
              timestamp = now; trade_id = None; last_qty = None; last_price = None; fee = None;
              cl_ord_id = current_order.cl_ord_id;
            } in
            update_orders_internal store event;
            Logging.info_f ~section "Order NON-USER CANCEL: %s [%s]" order_id symbol
         | None -> ())
    | None -> ()
  ) non_user_cancels

let process_market_data json =
  let open Yojson.Safe.Util in
  let channel = member "channel" json |> to_string_option in
  try
    match channel with
    | Some "orderUpdates" ->
        let data = member "data" json in
        process_order_updates data
    | Some "userEvents" ->
        let data = member "data" json in
        process_user_events data
    | Some "webData2" ->
        let data = member "data" json in
        
        let fills = try member "fills" data |> to_list with _ -> [] in
        if List.length fills > 0 then begin
           process_user_events (`Assoc [("fills", `List fills)])
        end;

        let open_orders_json = match member "openOrders" data with
          | `List l -> l
          | _ -> []
        in
        
        Logging.debug_f ~section "Processing webData2 snapshot with %d orders" (List.length open_orders_json);
        
        let new_orders_map = Hashtbl.create 16 in
        List.iter (fun o ->
          let coin = member "coin" o |> to_string in
          match find_registered_symbol coin with
          | Some symbol ->
              let side = if (member "side" o |> to_string) = "B" then Buy else Sell in
              let order_id = (match member "oid" o with `Int i -> string_of_int i | `String s -> s | _ -> "0") in
              let cl_ord_id = member "cloid" o |> to_string_option in
              let limit_price = (match member "limitPx" o with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0) in
              let qty = (match member "sz" o with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0) in
              Hashtbl.replace new_orders_map order_id (symbol, side, limit_price, qty, cl_ord_id)
          | None -> ()
        ) open_orders_json;

        let now = Unix.gettimeofday () in
        let events_to_process = ref [] in

        Hashtbl.iter (fun _ symbol_store ->
          Mutex.lock symbol_store.orders_mutex;
          Hashtbl.iter (fun order_id (current_order : open_order) ->
            if not (Hashtbl.mem new_orders_map order_id) then begin
              if now -. current_order.last_updated > 5.0 then begin
                let reconciled = ref false in
                Hashtbl.iter (fun _ (_, _, _, _, clid_opt) ->
                   if clid_opt = current_order.cl_ord_id && Option.is_some clid_opt then reconciled := true
                ) new_orders_map;

                if not !reconciled then begin
                  let event : execution_event = {
                    order_id; symbol = current_order.symbol; order_status = CanceledStatus; 
                    limit_price = current_order.limit_price; side = current_order.side;
                    order_qty = current_order.order_qty; cum_qty = current_order.cum_qty;
                    timestamp = now; trade_id = None; last_qty = None; last_price = None; fee = None;
                    cl_ord_id = current_order.cl_ord_id;
                  } in
                  events_to_process := (symbol_store, event) :: !events_to_process;
                end
              end
            end
          ) symbol_store.open_orders;
          Mutex.unlock symbol_store.orders_mutex;
        ) stores;

        List.iter (fun (store, event) -> 
          update_orders_internal store event;
          Logging.debug_f ~section "Removing order %s from %s (missing from snapshot and >5s old)" event.order_id event.symbol
        ) !events_to_process;

        Hashtbl.iter (fun order_id (symbol, side, limit_price, qty, cl_ord_id) ->
          let store = get_symbol_store symbol in
          
          let existing_order = 
            Mutex.lock store.orders_mutex;
            let o = match Hashtbl.find_opt store.open_orders order_id with
              | Some o -> Some o
              | None -> 
                  (match cl_ord_id with
                   | Some clid -> 
                       Hashtbl.fold (fun _ (o: open_order) acc -> 
                         if o.cl_ord_id = Some clid then Some o else acc
                       ) store.open_orders None
                   | None -> None)
            in
            Mutex.unlock store.orders_mutex;
            o
          in

          if Option.is_none existing_order || (match existing_order with Some o -> o.order_id <> order_id | None -> false) then begin
            let user_ref = match existing_order with Some o -> o.order_userref | None -> None in
            let event : execution_event = {
              order_id; symbol; order_status = NewStatus; limit_price = Some limit_price;
              side; order_qty = qty; cum_qty = 0.0; timestamp = now; trade_id = None;
              last_qty = None; last_price = None; fee = None; cl_ord_id;
            } in
            update_orders_internal ?user_ref store event;
            Logging.debug_f ~section "Order ADDED from webData2: %s [%s] %.8f @ %.2f%s" 
              order_id symbol qty limit_price
              (if Option.is_some user_ref then " (tagged)" else "")
          end;
        ) new_orders_map;
        
        Mutex.lock initialization_mutex;
        Hashtbl.iter (fun _ store -> notify_ready store) stores;
        Mutex.unlock initialization_mutex;
        Logging.debug_f ~section "Synced %d open orders from Hyperliquid webData2 snapshot" (List.length open_orders_json)
    | _ -> ()
  with exn -> 
    Logging.error_f ~section "Failed to process Hyperliquid executions data: %s" (Printexc.to_string exn)

let _processor_task =
  let sub = Hyperliquid_ws.subscribe_market_data () in
  Lwt.async (fun () ->
    Logging.info ~section "Starting Hyperliquid executions processor task";
    Lwt.catch (fun () ->
      Lwt_stream.iter process_market_data sub.stream
    ) (fun exn ->
      Logging.error_f ~section "Hyperliquid executions processor task crashed: %s" (Printexc.to_string exn);
      Lwt.return_unit
    )
  )

let initialize symbols =
  Logging.info ~section "Initializing Hyperliquid executions feed";
  List.iter (fun symbol ->
    let _ = get_symbol_store symbol in
    Logging.debug_f ~section "Created Hyperliquid executions buffer for %s" symbol
  ) symbols
