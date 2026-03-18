(**
  Standalone Auto-Hedging Module
  
  Intercepts spot fills on Hyperliquid and automatically fires offset derivative orders
  to Hyperliquid Perps to neutralize delta risk. Ensures strict 1x (1:1 ratio) offset.
*)

open Strategy_common

let section = "auto_hedger"

(* Unique ring buffer for hedging orders *)
let order_buffer = OrderRingBuffer.create 2048
let order_buffer_mutex = Mutex.create ()

let get_pending_orders limit =
  Mutex.lock order_buffer_mutex;
  let orders = OrderRingBuffer.read_batch order_buffer limit in
  Mutex.unlock order_buffer_mutex;
  orders

let push_order order =
  Mutex.lock order_buffer_mutex;
  let write_result = OrderRingBuffer.write order_buffer order in
  Mutex.unlock order_buffer_mutex;
  match write_result with
  | None -> Logging.error_f ~section "Failed to write hedge order to ring buffer (buffer full)"
  | Some () -> ()

let handle_order_filled exchange hedge_symbol side filled_qty =
  if exchange <> "hyperliquid" then ()
  else begin
    Logging.info_f ~section "Intercepted spot fill, preparing hedge on %s" hedge_symbol;
    let hedge_side = match side with 
      | Buy -> Sell 
      | Sell -> Buy 
    in
    
    let order = {
      operation = Place;
      order_id = None;
      symbol = hedge_symbol;
      exchange = "hyperliquid";
      side = hedge_side;
      order_type = "market";
      qty = filled_qty;
      price = None;
      time_in_force = "IOC";
      post_only = false;
      userref = Some 3; (* strategy_userref_hedge *)
      strategy = "Hedger";
      duplicate_key = generate_duplicate_key hedge_symbol (string_of_order_side hedge_side) filled_qty None ^ "_" ^ string_of_float (Unix.gettimeofday ());
    } in
    
    push_order order;
    Logging.info_f ~section "Offset hedge order (%s %.8f %s) pushed to execution ring-buffer" 
      (string_of_order_side hedge_side) filled_qty hedge_symbol
  end
