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
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let global_orders_mutex = Mutex.create ()

let ensure_store symbol =
  Mutex.lock global_orders_mutex;
  let store = match Hashtbl.find_opt stores symbol with
    | Some store -> store
    | None ->
        let store = {
          events_buffer = RingBuffer.create 128;
          open_orders = Hashtbl.create 32;
          ready = Atomic.make false;
          processed_tids = Hashtbl.create 128;
        } in
        Hashtbl.add stores symbol store;
        store
  in
  Mutex.unlock global_orders_mutex;
  store

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

let get_open_order symbol order_id =
  Mutex.lock global_orders_mutex;
  let order = match Hashtbl.find_opt stores symbol with
    | Some store -> Hashtbl.find_opt store.open_orders order_id
    | None -> None
  in
  Mutex.unlock global_orders_mutex;
  order

let get_open_orders symbol =
  Mutex.lock global_orders_mutex;
  let orders = match Hashtbl.find_opt stores symbol with
    | Some store -> Hashtbl.fold (fun _ o acc -> o :: acc) store.open_orders []
    | None -> []
  in
  Mutex.unlock global_orders_mutex;
  orders

let get_current_position symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.get_position store.events_buffer
  | None -> 0

let read_execution_events symbol last_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_since store.events_buffer last_pos
  | None -> []

let has_execution_data symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> Atomic.get store.ready
  | None -> false

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

let inject_order ~symbol ~order_id ~side ~qty ~price ?user_ref ?cl_ord_id () =
  let store = ensure_store symbol in
  let now = Unix.gettimeofday () in
  let order : open_order = {
    order_id;
    symbol;
    side;
    order_qty = qty;
    cum_qty = 0.0;
    remaining_qty = qty;
    limit_price = Some price;
    order_status = NewStatus;
    order_userref = user_ref;
    cl_ord_id;
    last_updated = now;
  } in
  
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
  } in
  
  Mutex.lock global_orders_mutex;
  Hashtbl.replace store.open_orders order_id order;
  RingBuffer.write store.events_buffer event;
  Mutex.unlock global_orders_mutex;
  
  OrderUpdateEventBus.publish order_update_event_bus event;
  
  notify_ready store;
  Logging.info_f ~section "Proactively injected open order: %s [%s] %s side %.8f @ %.2f (cloid: %s, userref: %s)"
    order_id symbol (if side = Buy then "buy" else "sell") qty price
    (Option.value cl_ord_id ~default:"none")
    (match user_ref with Some u -> string_of_int u | None -> "none")

let find_registered_symbol coin =
  match Hyperliquid_instruments_feed.resolve_symbol coin with
  | Some symbol -> 
      if Hashtbl.mem stores symbol then Some symbol else None
  | None ->
      let result = ref None in
      Hashtbl.iter (fun registered_symbol _ ->
        if registered_symbol = coin then result := Some registered_symbol
        else if String.starts_with ~prefix:(coin ^ "/") registered_symbol then result := Some registered_symbol
      ) stores;
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
            
            let store = ensure_store symbol in
            let now = Unix.gettimeofday () in
            
            let order_status = match status with
              | "open" -> NewStatus
              | "filled" -> FilledStatus
              | "canceled" -> CanceledStatus
              | "rejected" -> RejectedStatus
              | "marginCanceled" -> CanceledStatus
              | s -> UnknownStatus s
            in
            
            let is_terminal = match order_status with
              | FilledStatus | CanceledStatus | RejectedStatus -> true
              | _ -> false
            in

            (* Check for existing order for reconciliation (by oid or cloid) *)
            Mutex.lock global_orders_mutex;
            let existing_order = 
              match Hashtbl.find_opt store.open_orders order_id with
              | Some o -> Some o
              | None -> 
                  (match cl_ord_id with
                   | Some clid -> 
                       Hashtbl.fold (fun _ o acc -> 
                         if o.cl_ord_id = Some clid then Some o else acc
                       ) store.open_orders None
                   | None -> None)
            in
            
            let event : execution_event = {
              order_id;
              symbol;
              order_status;
              limit_price = Some price;
              side;
              order_qty = qty;
              cum_qty = 0.0; (* Fills will update the cumulative quantity separately *)
              timestamp = now;
              trade_id = None;
              last_qty = None;
              last_price = None;
              fee = None;
            } in
            
            RingBuffer.write store.events_buffer event;
            
            if is_terminal then begin
              (* Remove by original ID and also by the potentially new OID if reconciled *)
              (match existing_order with
               | Some existing when existing.order_id <> order_id ->
                   Hashtbl.remove store.open_orders existing.order_id
               | _ -> ());
              Hashtbl.remove store.open_orders order_id;
              Logging.info_f ~section "Order %s: %s [%s]" 
                (String.uppercase_ascii status) order_id symbol
            end else if order_status = NewStatus then begin
              let order_userref = match existing_order with Some o -> o.order_userref | None -> None in
              let order : open_order = {
                order_id; symbol; side; order_qty = qty; cum_qty = 0.0; 
                remaining_qty = qty; limit_price = Some price; 
                order_status; order_userref; 
                cl_ord_id; last_updated = now
              } in
              
              (* Reconcile: If we found by cloid but order_id changed, remove old one *)
              (match existing_order with
               | Some existing when existing.order_id <> order_id ->
                   Logging.info_f ~section "Reconciled order via cloid: %s -> %s [%s]" 
                     existing.order_id order_id symbol;
                   Hashtbl.remove store.open_orders existing.order_id
               | _ -> ());

              Hashtbl.replace store.open_orders order_id order;
              Logging.debug_f ~section "Order OPEN: %s [%s] %.8f @ %.2f%s" 
                order_id symbol qty price
                (if Option.is_some order_userref then " (tagged)" else "")
            end;
            Mutex.unlock global_orders_mutex;
            OrderUpdateEventBus.publish order_update_event_bus event;
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
        
        let store = ensure_store symbol in
        Mutex.lock global_orders_mutex;
        
        (* Deduplicate trade fills across channels *)
        if Hashtbl.mem store.processed_tids tid then begin
          Mutex.unlock global_orders_mutex
        end else begin
          let now = Unix.gettimeofday () in
          Hashtbl.replace store.processed_tids tid now;
          
          (* Periodically prune old TIDs (keep last 1 hour) *)
          if Hashtbl.length store.processed_tids > 500 then begin
            let to_prune = ref [] in
            Hashtbl.iter (fun k t -> if now -. t > 3600.0 then to_prune := k :: !to_prune) store.processed_tids;
            List.iter (Hashtbl.remove store.processed_tids) !to_prune
          end;

          let existing_order = Hashtbl.find_opt store.open_orders order_id in
          
          let cum_qty = match existing_order with
            | Some o -> o.cum_qty +. size
            | None -> size
          in
          let order_qty = match existing_order with
            | Some o -> o.order_qty
            | None -> size (* Fallback if we don't know the total qty *)
          in
          
          let is_filled = cum_qty >= (order_qty -. 1e-6) in
          let status = if is_filled then FilledStatus else PartiallyFilledStatus in
          
          let event : execution_event = {
            order_id;
            symbol;
            order_status = status;
            limit_price = (match existing_order with Some o -> o.limit_price | None -> Some price);
            side;
            order_qty;
            cum_qty;
            timestamp = now;
            trade_id = Some tid;
            last_qty = Some size;
            last_price = Some price;
            fee = Some fee;
          } in
          
          RingBuffer.write store.events_buffer event;
          
          if is_filled then begin
            Hashtbl.remove store.open_orders order_id;
            Logging.info_f ~section "Order FILLED: %s [%s] %.8f @ %.2f (trade_id: %Ld)" 
              order_id symbol size price tid
          end else begin
            let updated_order = match existing_order with
              | Some o -> { o with cum_qty; remaining_qty = o.order_qty -. cum_qty; order_status = status; last_updated = now }
              | None -> {
                  order_id; symbol; side; order_qty; cum_qty; 
                  remaining_qty = 0.0; limit_price = Some price; 
                  order_status = status; order_userref = None; 
                  cl_ord_id = None; last_updated = now
                }
            in
            Hashtbl.replace store.open_orders order_id updated_order;
            Logging.info_f ~section "Order PARTIALLY FILLED: %s [%s] %.8f @ %.2f (filled: %.8f/%.8f)" 
              order_id symbol size price cum_qty order_qty
          end;
          Mutex.unlock global_orders_mutex;
          OrderUpdateEventBus.publish order_update_event_bus event
        end
    | None -> ()
  ) fills;

  (* Handle nonUserCancel (liquidations, system cancels) *)
  let non_user_cancels = try member "nonUserCancel" data_json |> to_list with _ -> [] in
  List.iter (fun nuc ->
    let coin = member "coin" nuc |> to_string in
    match find_registered_symbol coin with
    | Some symbol ->
        let order_id = (match member "oid" nuc with `Int i -> string_of_int i | `String s -> s | _ -> "0") in
        let store = ensure_store symbol in
        let now = Unix.gettimeofday () in
        
        Mutex.lock global_orders_mutex;
        let existing_opt = Hashtbl.find_opt store.open_orders order_id in
        if Option.is_some existing_opt then begin
          let current_order = Option.get existing_opt in
          Hashtbl.remove store.open_orders order_id;
          
          let event : execution_event = {
            order_id;
            symbol;
            order_status = CanceledStatus;
            limit_price = current_order.limit_price;
            side = current_order.side;
            order_qty = current_order.order_qty;
            cum_qty = current_order.cum_qty;
            timestamp = now;
            trade_id = None;
            last_qty = None;
            last_price = None;
            fee = None;
          } in
          
          RingBuffer.write store.events_buffer event;
          Mutex.unlock global_orders_mutex;
          OrderUpdateEventBus.publish order_update_event_bus event;
          Logging.info_f ~section "Order NON-USER CANCEL: %s [%s]" order_id symbol
        end else begin
          Mutex.unlock global_orders_mutex
        end
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
        
        (* 1. Handle Fills from webData2 first to ensure we don't mark as canceled if just filled *)
        let fills = try member "fills" data |> to_list with _ -> [] in
        if List.length fills > 0 then begin
           process_user_events (`Assoc [("fills", `List fills)])
        end;

        (* 2. Handle Open Orders Snapshot *)
        let open_orders_json = match member "openOrders" data with
          | `List l -> l
          | _ -> []
        in
        
        Logging.debug_f ~section "Processing webData2 snapshot with %d orders" (List.length open_orders_json);
        
        (* Build a map of the new open orders by order_id *)
        let new_orders = Hashtbl.create 16 in
        List.iter (fun o ->
          let coin = member "coin" o |> to_string in
          match find_registered_symbol coin with
          | Some symbol ->
              let side = if (member "side" o |> to_string) = "B" then Buy else Sell in
              let order_id = (match member "oid" o with `Int i -> string_of_int i | `String s -> s | _ -> "0") in
              let cl_ord_id = member "cloid" o |> to_string_option in
              let limit_price = (match member "limitPx" o with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0) in
              let qty = (match member "sz" o with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0) in
              Hashtbl.replace new_orders order_id (symbol, side, limit_price, qty, cl_ord_id)
          | None -> ()
        ) open_orders_json;

        let events_to_publish = ref [] in
        let now = Unix.gettimeofday () in

        (* Compute diff, update store.open_orders, and emit events *)
        Mutex.lock global_orders_mutex;
        Hashtbl.iter (fun _ symbol_store ->
          let to_remove = ref [] in
          Hashtbl.iter (fun order_id (current_order : open_order) ->
            if not (Hashtbl.mem new_orders order_id) then begin
              (* Only remove if order is NOT in the new snapshot AND its grace period has expired (5.0s) *)
              (* This prevents proactively injected orders from being removed before they appear in the exchange feed *)
              if now -. current_order.last_updated > 5.0 then begin
                (* Check if cloid reconcile might save it *)
                let reconciled = ref false in
                Hashtbl.iter (fun _ (_, _, _, _, clid_opt) ->
                   if clid_opt = current_order.cl_ord_id && Option.is_some clid_opt then reconciled := true
                ) new_orders;

                if not !reconciled then begin
                  to_remove := order_id :: !to_remove;
                  let event : execution_event = {
                    order_id;
                    symbol = current_order.symbol;
                    order_status = CanceledStatus; 
                    limit_price = current_order.limit_price;
                    side = current_order.side;
                    order_qty = current_order.order_qty;
                    cum_qty = current_order.cum_qty;
                    timestamp = now;
                    trade_id = None;
                    last_qty = None;
                    last_price = None;
                    fee = None;
                  } in
                  RingBuffer.write symbol_store.events_buffer event;
                  events_to_publish := event :: !events_to_publish;
                  Logging.debug_f ~section "Removing order %s from %s (missing from snapshot and >5s old)" order_id current_order.symbol
                end
              end
            end
          ) symbol_store.open_orders;
          
          List.iter (fun order_id -> Hashtbl.remove symbol_store.open_orders order_id) !to_remove;
        ) stores;

        (* Add new orders that were not previously in store.open_orders *)
        Hashtbl.iter (fun order_id (symbol, side, limit_price, qty, cl_ord_id) ->
          let store = match Hashtbl.find_opt stores symbol with Some s -> s | None ->
            let s = {
              events_buffer = RingBuffer.create 128;
              open_orders = Hashtbl.create 32;
              ready = Atomic.make false;
              processed_tids = Hashtbl.create 128;
            } in
            Hashtbl.add stores symbol s;
            s
          in
          
          (* Check if we already have this order (by oid or cloid) *)
          let existing_order = 
            match Hashtbl.find_opt store.open_orders order_id with
            | Some o -> Some o
            | None -> 
                (match cl_ord_id with
                 | Some clid -> 
                     Hashtbl.fold (fun _ o acc -> 
                       if o.cl_ord_id = Some clid then Some o else acc
                     ) store.open_orders None
                 | None -> None)
          in

          if Option.is_none existing_order || (match existing_order with Some o -> o.order_id <> order_id | None -> false) then begin
            let user_ref = match existing_order with Some o -> o.order_userref | None -> None in
            let order : open_order = {
              order_id;
              symbol;
              side;
              order_qty = qty;
              cum_qty = 0.0;
              remaining_qty = qty;
              limit_price = Some limit_price;
              order_status = NewStatus;
              order_userref = user_ref;
              cl_ord_id;
              last_updated = now;
            } in
            
            (* Reconcile: If we found by cloid but order_id changed, remove old one *)
            (match existing_order with
             | Some existing when existing.order_id <> order_id ->
                 Logging.info_f ~section "Reconciled order via cloid in webData2: %s -> %s [%s]" 
                   existing.order_id order_id symbol;
                 Hashtbl.remove store.open_orders existing.order_id
             | _ -> ());

            Hashtbl.replace store.open_orders order_id order;
            
            let event : execution_event = {
              order_id;
              symbol;
              order_status = NewStatus;
              limit_price = Some limit_price;
              side;
              order_qty = qty;
              cum_qty = 0.0;
              timestamp = now;
              trade_id = None;
              last_qty = None;
              last_price = None;
              fee = None;
            } in
            RingBuffer.write store.events_buffer event;
            events_to_publish := event :: !events_to_publish;
            Logging.debug_f ~section "Order ADDED from webData2: %s [%s] %.8f @ %.2f%s" 
              order_id symbol qty limit_price
              (if Option.is_some user_ref then " (tagged)" else "")
          end;
        ) new_orders;
        Mutex.unlock global_orders_mutex;

        (* Publish events outside lock *)
        List.iter (OrderUpdateEventBus.publish order_update_event_bus) !events_to_publish;
        
        (* Signal readiness for all stores *)
        Hashtbl.iter (fun _ store -> notify_ready store) stores;
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
  Logging.info_f ~section "Initializing Hyperliquid executions feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created Hyperliquid executions buffer for %s" symbol
  ) symbols
