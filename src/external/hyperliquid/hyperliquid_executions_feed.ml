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
}

(** Lock-free ring buffer for execution data *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol execution storage with readiness signalling *)
type store = {
  events_buffer: execution_event RingBuffer.t;
  open_orders: (string, open_order) Hashtbl.t;
  ready: bool Atomic.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let global_orders_mutex = Mutex.create ()

let ensure_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      let store = {
        events_buffer = RingBuffer.create 128;
        open_orders = Hashtbl.create 32;
        ready = Atomic.make false;
      } in
      Hashtbl.add stores symbol store;
      store

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

let get_open_order symbol order_id =
  match Hashtbl.find_opt stores symbol with
  | Some store -> Hashtbl.find_opt store.open_orders order_id
  | None -> None

let get_open_orders symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> Hashtbl.fold (fun _ o acc -> o :: acc) store.open_orders []
  | None -> []

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

let find_registered_symbol coin =
  (* Check if the coin matches any registered symbol (e.g., "HYPE" matches "HYPE/USDC") *)
  let result = ref None in
  Logging.info_f ~section "Looking for registered symbol for coin: %s (active stores: %d)" 
    coin (Hashtbl.length stores);
  Hashtbl.iter (fun registered_symbol _ ->
    if registered_symbol = coin then result := Some registered_symbol
    else if String.starts_with ~prefix:(coin ^ "/") registered_symbol then result := Some registered_symbol
  ) stores;
  (match !result with
   | Some s -> Logging.info_f ~section "Matched %s to registered symbol: %s" coin s
   | None -> Logging.info_f ~section "No registered symbol found for coin: %s" coin);
  !result

let process_market_data json =
  let open Yojson.Safe.Util in
  let channel = member "channel" json |> to_string_option in
  Logging.debug_f ~section "Executions feed received message on channel: %s" (Option.value channel ~default:"<none>");
  try
    let channel = member "channel" json |> to_string_option in
    match channel with
    | Some "webData2" ->
        let data = member "data" json in
        let open_orders_json = match member "openOrders" data with
          | `List l -> l
          | _ -> []
        in
        
        Logging.debug_f ~section "Processing webData2 with %d orders" (List.length open_orders_json);
        
        (* Build a map of the new open orders by order_id *)
        let new_orders = Hashtbl.create (List.length open_orders_json) in
        List.iter (fun o ->
          let coin = member "coin" o |> to_string in
          match find_registered_symbol coin with
          | Some symbol ->
              let side = if (member "side" o |> to_string) = "B" then Buy else Sell in
              let order_id = (match member "oid" o with `Int i -> string_of_int i | `String s -> s | _ -> "0") in
              let limit_price = (match member "limitPx" o with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0) in
              let qty = (match member "sz" o with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0) in
              Hashtbl.replace new_orders order_id (symbol, side, limit_price, qty)
          | None -> ()
        ) open_orders_json;

        let now = Unix.gettimeofday () in

        (* Compute diff, update store.open_orders, and emit events *)
        Hashtbl.iter (fun _ store ->
          let to_remove = ref [] in
          Hashtbl.iter (fun order_id (current_order : open_order) ->
            if not (Hashtbl.mem new_orders order_id) then begin
              (* Order is no longer open - emit cancellation/fill event *)
              to_remove := order_id :: !to_remove;
              let event : execution_event = {
                order_id;
                symbol = current_order.symbol;
                order_status = CanceledStatus; (* Assume canceled if it disappears *)
                limit_price = current_order.limit_price;
                side = current_order.side;
                order_qty = current_order.order_qty;
                cum_qty = current_order.cum_qty;
                timestamp = now;
              } in
              RingBuffer.write store.events_buffer event
            end
          ) store.open_orders;
          
          List.iter (fun order_id -> Hashtbl.remove store.open_orders order_id) !to_remove;
        ) stores;

        (* Add new orders that were not previously in store.open_orders *)
        Hashtbl.iter (fun order_id (symbol, side, limit_price, qty) ->
          let store = ensure_store symbol in
          if not (Hashtbl.mem store.open_orders order_id) then begin
            let order : open_order = {
              order_id;
              symbol;
              side;
              order_qty = qty;
              cum_qty = 0.0;
              remaining_qty = qty;
              limit_price = Some limit_price;
              order_status = NewStatus;
              order_userref = None;
              cl_ord_id = None;
              last_updated = now;
            } in
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
            } in
            RingBuffer.write store.events_buffer event
          end
        ) new_orders;
        
        (* Signal readiness for all stores *)
        Hashtbl.iter (fun _ store -> notify_ready store) stores;
        Logging.debug_f ~section "Synced %d open orders from Hyperliquid webData2" (List.length open_orders_json)
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
