(**
  Standalone Auto-Hedging Module
  
  Intercepts spot fills on Hyperliquid and automatically fires offset derivative orders
  to Hyperliquid Perps to neutralize delta risk. Ensures strict 1x (1:1 ratio) offset.
  
  Uses the perp ticker price as the limit, but only hedges when the spot-perp basis
  is within a configurable tolerance (max_basis_bps). This ensures each hedge leg
  is at most max_basis_bps worse than the spot fill, keeping the round-trip net positive
  when grid_spread > 2 * max_basis_bps.
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

let handle_order_filled testnet exchange hedge_symbol side filled_qty fill_price max_basis_bps =
  if exchange <> "hyperliquid" then ()
  else begin
    Logging.info_f ~section "Intercepted spot fill, preparing hedge on %s" hedge_symbol;
    let hedge_side = match side with 
      | Buy -> Sell 
      | Sell -> Buy 
    in
    
    (* Get the perp ticker to check basis *)
    let perp_ticker = match Dio_exchange.Exchange_intf.Registry.get exchange with
      | Some (module Ex) -> Ex.get_ticker ~symbol:hedge_symbol
      | None -> None
    in
    
    (* Calculate the hedge limit price based on basis tolerance:
       - For sell hedge (spot buy filled): use perp bid, check it's within tolerance below fill_price
       - For buy hedge (spot sell filled): use perp ask, check it's within tolerance above fill_price
       The tolerance = fill_price * max_basis_bps / 10000 *)
    let max_basis_pct = max_basis_bps /. 10000.0 in
    
    let hedge_result =
      if fill_price <= 0.0 then begin
        Logging.warn_f ~section "Fill price is 0 for %s, skipping hedge" hedge_symbol;
        None
      end else
        match perp_ticker with
        | Some (bid, ask) ->
            let perp_price, basis_pct = match hedge_side with
              | Sell ->
                  (* Spot buy filled → short perp. Best perp price = bid *)
                  let basis = (fill_price -. bid) /. fill_price in
                  (bid, basis)
              | Buy ->
                  (* Spot sell filled → long perp. Best perp price = ask *)
                  let basis = (ask -. fill_price) /. fill_price in
                  (ask, basis)
            in
            
            if basis_pct <= max_basis_pct then begin
              (* Basis is within tolerance — hedge at the perp price *)
              let direction = if basis_pct <= 0.0 then "favorable" else "within tolerance" in
              Logging.info_f ~section "Basis %s: spot=%.4f perp=%.4f basis=%.2fbps (max=%.0fbps)"
                direction fill_price perp_price (basis_pct *. 10000.0) max_basis_bps;
              Some perp_price
            end else begin
              (* Basis exceeds tolerance — use the worst acceptable price *)
              let capped_price = match hedge_side with
                | Sell -> fill_price *. (1.0 -. max_basis_pct)  (* floor for sell *)
                | Buy  -> fill_price *. (1.0 +. max_basis_pct)  (* ceiling for buy *)
              in
              Logging.info_f ~section "Basis exceeds tolerance: spot=%.4f perp=%.4f basis=%.2fbps > max=%.0fbps, capping at %.4f"
                fill_price perp_price (basis_pct *. 10000.0) max_basis_bps capped_price;
              Some capped_price
            end
        | None ->
            if testnet then begin
              Logging.warn_f ~section "No perp ticker for %s on testnet, using fill price %.4f" hedge_symbol fill_price;
              Some fill_price
            end else begin
              (* No ticker = can't assess basis. Use fill price as limit (conservative). *)
              Logging.warn_f ~section "No perp ticker for %s, using fill price %.4f as limit" hedge_symbol fill_price;
              Some fill_price
            end
    in
    
    match hedge_result with
    | None -> ()
    | Some target_price ->
        (* Testnet: ensure we meet HL's $10 minimum order value *)
        let hedge_qty =
          if testnet then begin
            let min_value = 10.01 in
            let order_value = filled_qty *. target_price in
            if order_value < min_value then begin
              let min_qty = ceil (min_value /. target_price *. 100.0) /. 100.0 in
              Logging.info_f ~section "Testnet: bumping hedge qty from %.8f to %.8f to meet $%.0f min (price=%.2f)"
                filled_qty min_qty min_value target_price;
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
          order_type = "limit";
          qty = hedge_qty;
          price = Some target_price;
          time_in_force = "IOC";
          post_only = false;
          userref = Some 3; (* strategy_userref_hedge *)
          strategy = "Hedger";
          duplicate_key = generate_duplicate_key hedge_symbol (string_of_order_side hedge_side) filled_qty (Some target_price) ^ "_" ^ string_of_float (Unix.gettimeofday ());
        } in
        
        push_order order;
        OrderSignal.broadcast ();
        Logging.info_f ~section "Offset hedge order (%s %.8f %s @ %.4f) pushed to execution ring-buffer" 
          (string_of_order_side hedge_side) hedge_qty hedge_symbol target_price
  end
