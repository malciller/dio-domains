(**
   Single-short-per-cycle auto-hedging module.

   Intercepts spot fills on Hyperliquid and manages one perp short per grid cycle:

   - Spot buy -> open a perp short only if no hedge is currently open.
   - Spot sell -> close the entire open hedge short.

   During drawdowns the single short (opened at the first buy price) rides the
   full move, accumulating unrealized profit. Additional buys do not stack more
   shorts. On grid recovery sell, the short is closed, realizing the hedge
   profit and freeing the slot for the next cycle.

   Places IOC limit orders at perp top-of-book (best bid for sells, best ask
   for buys) to minimize slippage. Falls back to market orders when orderbook
   data is unavailable.
*)

open Strategy_common

let section = "auto_hedger"

(* Ring buffer dedicated to hedging orders. *)
let order_buffer = LockFreeQueue.create ()

let get_pending_orders limit =
  LockFreeQueue.read_batch order_buffer limit

let push_order order =
  let write_result = LockFreeQueue.write order_buffer order in
  match write_result with
  | None -> Logging.error_f ~section "Failed to write hedge order to ring buffer (buffer full)"
  | Some () -> ()

(** Build a hedge order and push it to the ring buffer.
    When [perp_tob] is available, places an IOC limit at best bid (sell) or
    best ask (buy) to reduce slippage. Falls back to market order otherwise.
    Returns the hedge quantity on success, or 0.0 on skip. *)
let build_and_push_hedge testnet hedge_symbol hedge_side filled_qty fill_price perp_tob =
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
    
    (* Determine order type and price from perp top-of-book.
       Sell hedge: limit at best bid. Buy hedge: limit at best ask.
       Falls back to market order when no orderbook data is available. *)
    let (order_type, limit_price) = match perp_tob with
      | Some (best_bid, _bid_sz, best_ask, _ask_sz) ->
          let px = match hedge_side with
            | Sell -> best_bid
            | Buy  -> best_ask
          in
          Logging.info_f ~section "Using perp TOB for %s %s: bid=%.4f ask=%.4f -> limit=%.4f"
            (string_of_order_side hedge_side) hedge_symbol best_bid best_ask px;
          ("limit", Some px)
      | None ->
          Logging.info_f ~section "No perp orderbook for %s, falling back to market order" hedge_symbol;
          ("market", None)
    in
    
    let order = {
      operation = Place;
      order_id = None;
      symbol = hedge_symbol;
      exchange = "hyperliquid";
      side = hedge_side;
      order_type;
      qty = hedge_qty_final;
      price = limit_price;
      time_in_force = "GTC";
      post_only = false;
      userref = Some 3;
      strategy = Hedger;
      duplicate_key = generate_duplicate_key hedge_symbol (string_of_order_side hedge_side) hedge_qty_final limit_price ^ "_" ^ string_of_float (Unix.gettimeofday ());
    } in
    
    push_order order;
    OrderSignal.broadcast ();
    let price_str = match limit_price with Some p -> Printf.sprintf "limit=%.4f" p | None -> "market" in
    Logging.info_f ~section "Hedge %s order (%s %.8f %s %s) pushed to ring-buffer" 
      order_type (string_of_order_side hedge_side) hedge_qty_final hedge_symbol price_str;
    hedge_qty_final
  end

(* Mutable state tracking whether a hedge is open and its quantity. *)
let hedge_open = ref false
let hedge_qty = ref 0.0
let hedge_mutex = Mutex.create ()

let is_hedge_open () =
  Mutex.lock hedge_mutex;
  let open_ = !hedge_open in
  Mutex.unlock hedge_mutex;
  open_

let get_hedge_qty () =
  Mutex.lock hedge_mutex;
  let qty = !hedge_qty in
  Mutex.unlock hedge_mutex;
  qty

let set_hedge_open qty =
  Mutex.lock hedge_mutex;
  hedge_open := true;
  hedge_qty := qty;
  Mutex.unlock hedge_mutex

let clear_hedge () =
  Mutex.lock hedge_mutex;
  hedge_open := false;
  hedge_qty := 0.0;
  Mutex.unlock hedge_mutex

(** Handle a spot fill under the single-short-per-cycle model.
    Spot buy: open perp short only if no hedge is currently open.
    Spot sell: close the full open hedge short.
    [perp_tob]: optional (bid_price, bid_size, ask_price, ask_size) from the perp orderbook. *)
let handle_order_filled testnet exchange hedge_symbol side filled_qty fill_price perp_tob =
  if exchange <> "hyperliquid" then ()
  else begin
    match side with
    | Buy ->
      if is_hedge_open () then
        Logging.info_f ~section "Spot buy fill: %.8f %s @ %.4f -> hedge already open (%.8f short), skipping"
          filled_qty hedge_symbol fill_price (get_hedge_qty ())
      else begin
        Logging.info_f ~section "Spot buy fill: %.8f %s @ %.4f -> opening hedge short"
          filled_qty hedge_symbol fill_price;
        let qty = build_and_push_hedge testnet hedge_symbol Sell filled_qty fill_price perp_tob in
        if qty > 0.0 then begin
          set_hedge_open qty;
          Logging.info_f ~section "Hedge short opened: %.8f %s" qty hedge_symbol
        end
      end
    | Sell ->
      if is_hedge_open () then begin
        let close_qty = get_hedge_qty () in
        Logging.info_f ~section "Spot sell fill: %.8f %s @ %.4f -> closing full hedge short (%.8f)"
          filled_qty hedge_symbol fill_price close_qty;
        let qty = build_and_push_hedge testnet hedge_symbol Buy close_qty fill_price perp_tob in
        if qty > 0.0 then begin
          clear_hedge ();
          Logging.info_f ~section "Hedge short closed: %.8f %s" qty hedge_symbol
        end
      end else
        Logging.info_f ~section "Spot sell fill: %.8f %s @ %.4f -> no hedge open, skipping"
          filled_qty hedge_symbol fill_price
  end

(** Reset hedge state. Retained for test compatibility. *)
let reset_state () =
  Mutex.lock hedge_mutex;
  hedge_open := false;
  hedge_qty := 0.0;
  Mutex.unlock hedge_mutex
