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

let handle_order_filled testnet exchange hedge_symbol side filled_qty =
  if exchange <> "hyperliquid" then ()
  else begin
    Logging.info_f ~section "Intercepted spot fill, preparing hedge on %s" hedge_symbol;
    let hedge_side = match side with 
      | Buy -> Sell 
      | Sell -> Buy 
    in
    
    let is_market, target_price, tif, hedge_qty = 
      if testnet then
        (* On testnet, spot and perps can be mispriced and books are thin. 
           Place an Alo limit order at the perp mid price to avoid immediate rejection. *)
        let ticker = match Dio_exchange.Exchange_intf.Registry.get exchange with
          | Some (module Ex) -> Ex.get_ticker ~symbol:hedge_symbol
          | None -> None
        in
        match ticker with
        | Some (bid, ask) ->
            let exec_price = match hedge_side with
              | Buy -> ask
              | Sell -> bid
            in
            (* Testnet perps can be mispriced vs spot; ensure we meet HL's $10 minimum order value *)
            let min_value = 10.01 in
            let order_value = filled_qty *. exec_price in
            let qty =
              if order_value < min_value then begin
                (* Ceil to next 0.01 to avoid fp rounding landing just under $10 *)
                let min_qty = ceil (min_value /. exec_price *. 100.0) /. 100.0 in
                Logging.info_f ~section "Testnet: bumping hedge qty from %.8f to %.8f to meet $%.0f min (price=%.2f)"
                  filled_qty min_qty min_value exec_price;
                min_qty
              end else
                filled_qty
            in
            (false, Some exec_price, "IOC", qty)
        | None -> 
            Logging.warn_f ~section "No ticker for %s on testnet, defaulting to market IOC" hedge_symbol;
            (true, None, "IOC", filled_qty)
      else
        (* Live environment uses market order (IOC) *)
        (true, None, "IOC", filled_qty)
    in
    
    let order = {
      operation = Place;
      order_id = None;
      symbol = hedge_symbol;
      exchange = "hyperliquid";
      side = hedge_side;
      order_type = if is_market then "market" else "limit";
      qty = hedge_qty;
      price = target_price;
      time_in_force = tif;
      post_only = (tif = "Alo");
      userref = Some 3; (* strategy_userref_hedge *)
      strategy = "Hedger";
      duplicate_key = generate_duplicate_key hedge_symbol (string_of_order_side hedge_side) filled_qty target_price ^ "_" ^ string_of_float (Unix.gettimeofday ());
    } in
    
    push_order order;
    OrderSignal.broadcast ();
    Logging.info_f ~section "Offset hedge order (%s %.8f %s @ %s) pushed to execution ring-buffer" 
      (string_of_order_side hedge_side) hedge_qty hedge_symbol 
      (match target_price with Some p -> Printf.sprintf "%.4f" p | None -> "market")
  end
