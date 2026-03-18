(**
  Delta-Neutral Auto-Hedging Module
  
  Intercepts spot fills on Hyperliquid and automatically fires offset derivative orders
  to Hyperliquid Perps. Every spot fill is hedged 1:1 on the perp side:
  
  - Spot Buy  → Perp Short (same qty)
  - Spot Sell → Perp Long  (same qty)
  
  This ensures the portfolio is always delta-neutral. The grid strategy's spread
  capture is the sole source of profit; the hedge eliminates directional risk.
  
  Fires market orders on the perp side for immediate execution.
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

(** Build a hedge market order and push it to the ring buffer.
    Returns the hedge quantity on success, 0.0 on skip. *)
let build_and_push_hedge testnet hedge_symbol hedge_side filled_qty fill_price =
  if fill_price <= 0.0 then begin
    Logging.warn_f ~section "Fill price is 0 for %s, skipping hedge" hedge_symbol;
    0.0
  end else begin
    let hedge_qty_final =
      if testnet then begin
        let min_value = 10.01 in
        let order_value = filled_qty *. fill_price in
        if order_value < min_value then begin
          let min_qty = ceil (min_value /. fill_price *. 100.0) /. 100.0 in
          Logging.info_f ~section "Testnet: bumping hedge qty from %.8f to %.8f to meet $%.0f min (price=%.2f)"
            filled_qty min_qty min_value fill_price;
          min_qty
        end else
          filled_qty
      end else
        filled_qty
    in
    
    let order = {
      operation = Place;
      order_id = None;
      symbol = hedge_symbol;
      exchange = "hyperliquid";
      side = hedge_side;
      order_type = "market";
      qty = hedge_qty_final;
      price = None;
      time_in_force = "IOC";
      post_only = false;
      userref = Some 3;
      strategy = "Hedger";
      duplicate_key = generate_duplicate_key hedge_symbol (string_of_order_side hedge_side) hedge_qty_final None ^ "_" ^ string_of_float (Unix.gettimeofday ());
    } in
    
    push_order order;
    OrderSignal.broadcast ();
    Logging.info_f ~section "Hedge market order (%s %.8f %s @ spot_price=%.4f) pushed to execution ring-buffer" 
      (string_of_order_side hedge_side) hedge_qty_final hedge_symbol fill_price;
    hedge_qty_final
  end

(* Track net hedge short position so we never open naked longs *)
let hedge_short_qty = ref 0.0
let hedge_short_mutex = Mutex.create ()

let get_hedge_short_qty () =
  Mutex.lock hedge_short_mutex;
  let qty = !hedge_short_qty in
  Mutex.unlock hedge_short_mutex;
  qty

let adjust_hedge_short_qty delta =
  Mutex.lock hedge_short_mutex;
  hedge_short_qty := Float.max 0.0 (!hedge_short_qty +. delta);
  let new_qty = !hedge_short_qty in
  Mutex.unlock hedge_short_mutex;
  new_qty

(** Handle a spot fill:
    - Spot Buy  → open/increase perp short (protect against downside)
    - Spot Sell → close/reduce existing perp short (unwind hedge), never open naked long *)
let handle_order_filled testnet exchange hedge_symbol side filled_qty fill_price =
  if exchange <> "hyperliquid" then ()
  else begin
    match side with
    | Buy ->
      Logging.info_f ~section "Spot buy fill: %.8f %s @ %.4f → hedging with perp short"
        filled_qty hedge_symbol fill_price;
      let qty = build_and_push_hedge testnet hedge_symbol Sell filled_qty fill_price in
      if qty > 0.0 then begin
        let new_pos = adjust_hedge_short_qty qty in
        Logging.info_f ~section "Hedge short position: %.8f (added %.8f)" new_pos qty
      end
    | Sell ->
      let current_short = get_hedge_short_qty () in
      if current_short > 0.0 then begin
        let close_qty = Float.min filled_qty current_short in
        Logging.info_f ~section "Spot sell fill: %.8f %s @ %.4f → closing %.8f of %.8f perp short"
          filled_qty hedge_symbol fill_price close_qty current_short;
        let qty = build_and_push_hedge testnet hedge_symbol Buy close_qty fill_price in
        if qty > 0.0 then begin
          let new_pos = adjust_hedge_short_qty (-.qty) in
          Logging.info_f ~section "Hedge short position: %.8f (closed %.8f)" new_pos qty
        end
      end else
        Logging.info_f ~section "Spot sell fill: %.8f %s @ %.4f → no short to close, skipping hedge"
          filled_qty hedge_symbol fill_price
  end

(** Reset state — kept for test compatibility *)
let reset_state () =
  Mutex.lock hedge_short_mutex;
  hedge_short_qty := 0.0;
  Mutex.unlock hedge_short_mutex
