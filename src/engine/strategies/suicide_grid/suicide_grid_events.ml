(* Suicide Grid - Order Lifecycle Event Handlers & Persistence *)

open Strategy_common
open Suicide_grid_types
open Suicide_grid_config
open Suicide_grid_reservation
open Suicide_grid_orders

(** Flushes deferred accumulation state to disk when the dirty flag is set. *)
let flush_persistence asset_symbol =
  let state = get_strategy_state asset_symbol in
  if state.persistence_dirty then begin
    let snapshot_to_save =
      Mutex.lock state.mutex;
      Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
        if state.persistence_dirty then begin
          state.persistence_dirty <- false;
          Some (
            state.reserved_base,
            state.accumulated_profit,
            state.last_fill_oid,
            state.last_buy_fill_price,
            state.last_sell_fill_price,
            state.persisted_sell_levels
          )
        end else None)
    in
    match snapshot_to_save with
    | Some (reserved_base, accumulated_profit, last_fill_oid, last_buy_fill_price, last_sell_fill_price, persisted_sell_levels) ->
        Dio_persistence.State_persistence.save_async ~symbol:asset_symbol
          ~reserved_base
          ~accumulated_profit
          ~last_fill_oid
          ~last_buy_fill_price
          ~last_sell_fill_price
          ~persisted_sell_levels ()
    | None -> ()
  end

(** Handles order placement acknowledgment. *)
let handle_order_acknowledged ~now asset_symbol order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    state.pending_orders <- List.filter (fun (pending_id, s, p, _) ->
      let is_placement_prefix = String.starts_with ~prefix:"pending_buy_" pending_id ||
                                String.starts_with ~prefix:"pending_sell_" pending_id in
      let matches_side_placement = is_placement_prefix && s = side in
      let matches_fallback = not (String.starts_with ~prefix:"pending_" pending_id) &&
                            s = side && abs_float (p -. price) < 0.01 in
      not (matches_side_placement || matches_fallback)
    ) state.pending_orders;

    (match side with
     | Buy ->
         state.last_buy_order_id <- Some order_id;
         state.last_buy_order_price <- Some price;
         state.inflight_buy <- false;
         state.inflight_amend_buy <- false;
         ()
     | Sell ->
         state.inflight_sell <- false;
         state.recently_injected_sells <- (order_id, price, now) :: state.recently_injected_sells;
         let replaced = ref false in
         state.open_sell_orders <- List.map (fun (oid, p, q) ->
           if not !replaced && String.starts_with ~prefix:"pending_sell_" oid && abs_float (p -. price) < (price *. 0.01) then begin
             replaced := true;
             (order_id, p, q)
           end else (oid, p, q)
         ) state.open_sell_orders;
         ());

    ()
  )

(** Handles order placement failure. *)
let handle_order_failed ~now asset_symbol side reason =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    state.pending_orders <- List.filter (fun (_, s, _, _) -> s <> side) state.pending_orders;
    (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);

    (match side with
     | Buy -> set_asset_reserved_quote state 0.0
     | Sell ->
         state.open_sell_orders <- List.filter (fun (oid, _, _) ->
           not (String.starts_with ~prefix:"pending_sell_" oid)
         ) state.open_sell_orders);

    let duplicate_key = (match side with Buy -> state.duplicate_key_buy | Sell -> state.duplicate_key_sell) in
    ignore (InFlightOrders.remove_in_flight_order duplicate_key);

    let lower_reason = String.lowercase_ascii reason in
    let is_rate_limit = contains_fragment lower_reason "too many cumulative requests" || contains_fragment lower_reason "rate limit" in
    let is_wash_trade = contains_fragment lower_reason "wash trade" in
    let is_insufficient_balance = contains_fragment lower_reason "insufficient funds"
      || contains_fragment lower_reason "insufficient spot balance"
      || contains_fragment lower_reason "not enough asset balance"
      || contains_fragment lower_reason "insufficient qty" in
    let cooldown = if is_rate_limit || is_wash_trade then 10.0 else 2.0 in

    (match side with
     | Buy when is_insufficient_balance ->
         if not state.capital_low then begin
           state.capital_low <- true;
           state.capital_low_logged <- true;
           state.capital_low_at_balance <- (-1.0);
           Logging.warn_f ~section "Exchange rejected buy for %s with insufficient funds - setting capital_low flag"
             asset_symbol
         end
     | Sell when is_insufficient_balance ->
          let ecfg = get_exchange_config state.exchange_id in
          if ecfg.sell_failure_sets_asset_low then begin
            if not state.asset_low then begin
              state.asset_low <- true;
              Logging.warn_f ~section "Exchange rejected sell for %s with insufficient balance - setting asset_low flag"
                asset_symbol
            end
          end else
            Logging.warn_f ~section "Exchange rejected sell for %s with insufficient balance (ignored, sell is fire-and-forget)"
              asset_symbol
     | _ -> ());

    (match side with
     | Buy ->
         if is_wash_trade then
           Logging.warn_f ~section "Buy rejected for %s due to wash trade conflict - cooling down buy placement for 10s" asset_symbol;
         Hashtbl.replace state.amend_cooldowns "place_Buy" (now +. cooldown)
     | Sell -> ());

    Logging.warn_f ~section "Order failed for %s (%s): %s. Cleared in-flight tracker."
      asset_symbol (string_of_order_side side) reason
  )

(** Handles order rejection. *)
let handle_order_rejected ~now:_ asset_symbol side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    state.pending_orders <- List.filter (fun (pending_id, s, p, _) ->
      let is_placement_prefix = String.starts_with ~prefix:"pending_buy_" pending_id ||
                                String.starts_with ~prefix:"pending_sell_" pending_id in
      let matches_side_placement = is_placement_prefix && s = side in
      let matches_amend_prefix = String.starts_with ~prefix:"pending_amend_" pending_id in
      let matches_fallback = (not (String.starts_with ~prefix:"pending_" pending_id) || matches_amend_prefix) &&
                            s = side && abs_float (p -. price) < 0.01 in
      not (matches_side_placement || matches_fallback)
    ) state.pending_orders;

    (match side with
     | Buy -> state.inflight_buy <- false
     | Sell ->
         state.inflight_sell <- false;
         state.open_sell_orders <- List.filter (fun (oid, _, _) ->
           not (String.starts_with ~prefix:"pending_sell_" oid)
         ) state.open_sell_orders);

    let duplicate_key = (match side with Buy -> state.duplicate_key_buy | Sell -> state.duplicate_key_sell) in
    ignore (InFlightOrders.remove_in_flight_order duplicate_key);

    (match side with
     | Buy ->
         let now = Unix.time () in
         let new_expiry = now +. 2.0 in
         let existing_expiry = match Hashtbl.find_opt state.amend_cooldowns "place_Buy" with
           | Some t -> t
           | None -> 0.0
         in
         if new_expiry > existing_expiry then
           Hashtbl.replace state.amend_cooldowns "place_Buy" new_expiry
     | Sell -> ());

    ()
  )

let buy_tracking_matches_exchange_event tracked order_id cl_ord_id =
  tracked = order_id
  || match cl_ord_id with Some c -> tracked = c | None -> false

let add_processed_fill state order_id =
  if not (Hashtbl.mem state.processed_fills order_id) then begin
    Queue.push order_id state.processed_fills_queue;
    Hashtbl.replace state.processed_fills order_id ();
    if Hashtbl.length state.processed_fills > 2000 then begin
      try
        let oldest = Queue.pop state.processed_fills_queue in
        Hashtbl.remove state.processed_fills oldest
      with _ -> ()
    end
  end

(** Handles order fill. *)
let handle_order_filled ~now:_ asset_symbol order_id side ~fill_price cl_ord_id =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    let acc_qty = venue_lot_qty state.grid_qty state.exchange_id state in

    let is_persisted_fill =
      Hashtbl.mem state.processed_fills order_id
      || match state.last_fill_oid with
         | Some persisted_oid ->
             (try Int64.compare (Int64.of_string order_id) (Int64.of_string persisted_oid) <= 0
              with _ -> order_id = persisted_oid)
         | None -> state.startup_replay
    in
    let skip_fill = is_persisted_fill in

    if skip_fill then begin
      add_processed_fill state order_id;
      Logging.debug_f ~section "Skipping already processed fill: %s (side=%s)" order_id (match side with Buy -> "buy" | Sell -> "sell");
      if state.startup_replay then begin
        let dominated = match state.highest_startup_oid with
          | None -> true
          | Some prev ->
              (try Int64.compare (Int64.of_string order_id) (Int64.of_string prev) > 0
               with _ -> false)
        in
        if dominated then
          state.highest_startup_oid <- Some order_id
      end
    end else begin
      add_processed_fill state order_id;
      state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
        not (String.starts_with ~prefix:"pending_amend_" pending_id &&
             String.length pending_id > 14 &&
             String.sub pending_id 14 (String.length pending_id - 14) = order_id)
      ) state.pending_orders;

      let sell_fill_price = match List.find_opt (fun (id, _, _) -> id = order_id) state.open_sell_orders with
        | Some (_, p, _) -> p
        | None -> fill_price
      in

      state.open_sell_orders <- List.filter (fun (sell_id, _, _) ->
        sell_id <> order_id
      ) state.open_sell_orders;

      let _was_tracked_buy = match side, state.last_buy_order_id with
        | Buy, Some id when buy_tracking_matches_exchange_event id order_id cl_ord_id -> true
        | _ -> false
      in
      if side = Buy then begin
        state.last_buy_fill_price <- Some fill_price;
        state.last_sell_fill_price <- None;
        if persistence_accumulation_exchange state.exchange_id then
          state.persistence_dirty <- true;
        if hl_like_spot_fee_exchange state.exchange_id && acc_qty > 0.0 then begin
          let buy_fee_quote = acc_qty *. fill_price *. state.maker_fee in
          state.accumulated_profit <- state.accumulated_profit -. buy_fee_quote;
          Logging.info_f ~section "Deducted buy fee from accumulated_profit for %s by %.6f (quote fee), now %.6f"
            asset_symbol buy_fee_quote state.accumulated_profit
        end;

        if (Exchange.Types.exchange_of_string state.exchange_id = Alpaca) && acc_qty > 0.0 then begin
          let sell_mult = state.cached_sell_mult in
          let base_increment = acc_qty -. (sell_mult *. acc_qty) in
          if base_increment > 0.0 then begin
            state.reserved_base <- state.reserved_base +. base_increment;
            state.persistence_dirty <- true;
            Logging.info_f ~section "Reserving base for %s on buy fill %s: +%.8f (sell_mult %.4f, total reserved_base now %.8f)"
              asset_symbol order_id base_increment sell_mult state.reserved_base
          end
        end;

        if acc_qty > 0.0 && not state.startup_replay then begin
          state.anticipated_base_credit <- state.anticipated_base_credit +. acc_qty;
          Logging.info_f ~section "Anticipated base credit for %s: +%.8f (total: %.8f) from buy fill %s"
            asset_symbol acc_qty state.anticipated_base_credit order_id
        end;

        state.inflight_sell <- false;
        state.recently_injected_sells <- [];
        ignore (InFlightOrders.remove_in_flight_order state.duplicate_key_sell);

        state.last_buy_order_id <- None;
        state.last_buy_order_price <- None;
        if not state.startup_replay then
          state.just_filled_buy <- true;
      end;

      if state.startup_replay then begin
        let dominated = match state.highest_startup_oid with
          | None -> true
          | Some prev ->
              (try Int64.compare (Int64.of_string order_id) (Int64.of_string prev) > 0
               with _ -> false)
        in
        if dominated then
          state.highest_startup_oid <- Some order_id
      end;
      (match side with
       | Sell ->
            if state.persisted_sell_levels <> [] then begin
              let rec remove_one acc found = function
                | [] -> List.rev acc
                | (sp, _sq) :: rest when not found && abs_float (sp -. sell_fill_price) <= (sell_fill_price *. 0.01) ->
                    remove_one acc true rest
                | item :: rest -> remove_one (item :: acc) found rest
              in
              state.persisted_sell_levels <- remove_one [] false state.persisted_sell_levels;
              state.persistence_dirty <- true
            end;
            if acc_qty > 0.0 then
              state.anticipated_base_credit <- Float.max 0.0 (state.anticipated_base_credit -. acc_qty);
            let cost_basis = match state.last_sell_fill_price with
              | Some prev_sell when prev_sell > 0.0 && prev_sell < sell_fill_price -> Some prev_sell
              | _ -> state.last_buy_fill_price
            in
            (match cost_basis with
             | Some base_price when sell_fill_price > base_price ->
                let qty = acc_qty in
                let gross = (sell_fill_price -. base_price) *. qty in
                let fees =
                  if state.exchange_id = "ibkr" then
                    ibkr_commission ~qty ~price:base_price
                    +. ibkr_commission ~qty ~price:sell_fill_price
                  else if hl_like_spot_fee_exchange state.exchange_id then
                    sell_fill_price *. qty *. state.maker_fee
                  else
                    (sell_fill_price *. qty *. state.maker_fee)
                    +. (base_price *. qty *. state.maker_fee)
                in
                let net_profit = gross -. fees in
                if net_profit > 0.0 then begin
                  state.accumulated_profit <- state.accumulated_profit +. net_profit;
                  Logging.debug_f ~section "Realized profit for %s: %.6f (gross %.6f - fees %.6f, sell@%.4f base@%.4f x %.8f), accumulated: %.6f"
                    asset_symbol net_profit gross fees sell_fill_price base_price qty state.accumulated_profit
                end;
                state.last_sell_fill_price <- Some sell_fill_price;
                state.last_buy_fill_price <- None
            | _ ->
                state.last_sell_fill_price <- Some sell_fill_price;
                state.last_buy_fill_price <- None)
       | Buy -> ());

      let should_update_oid = match state.last_fill_oid with
        | Some prev_oid ->
            (try Int64.compare (Int64.of_string order_id) (Int64.of_string prev_oid) > 0
             with _ -> order_id <> prev_oid)
        | None -> true
      in
      if should_update_oid then
        state.last_fill_oid <- Some order_id;
      if persistence_accumulation_exchange state.exchange_id then
        state.persistence_dirty <- true;

      (match side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);
      ignore (InFlightOrders.remove_in_flight_order ((match side with Buy -> state.duplicate_key_buy | Sell -> state.duplicate_key_sell)));

      if side = Buy then begin
        set_asset_reserved_quote state 0.0
      end;

      if side = Buy then begin
        ignore (InFlightOrders.remove_in_flight_order (state.duplicate_key_sell));
        state.inflight_sell <- false
      end;

      ()
    end
  )

(** Handles order cancellation. *)
let handle_order_cancelled ~now:_ asset_symbol order_id side cl_ord_id =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    let is_being_amended =
      InFlightAmendments.is_in_flight order_id ||
      state.inflight_amend_buy ||
      List.exists (fun (pending_id, _, _, _) ->
        String.starts_with ~prefix:"pending_amend_" pending_id &&
        String.length pending_id > 14 &&
        String.sub pending_id 14 (String.length pending_id - 14) = order_id
      ) state.pending_orders
    in

    state.pending_orders <- List.filter (fun (pending_id, _, _, _) ->
      let matches = pending_id = order_id ||
                   (String.starts_with ~prefix:"pending_amend_" pending_id &&
                    String.length pending_id > 14 &&
                    String.sub pending_id 14 (String.length pending_id - 14) = order_id) in
      not matches
    ) state.pending_orders;

    if not is_being_amended then begin
      Hashtbl.remove state.amend_cooldowns order_id;
      Hashtbl.remove state.evicted_orders order_id;
      ignore (InFlightAmendments.remove_in_flight_amendment order_id);

      let cancelled_side = side in

      let was_tracked_buy = match state.last_buy_order_id with
        | Some id when buy_tracking_matches_exchange_event id order_id cl_ord_id -> true
        | _ -> false
      in

      if was_tracked_buy then begin
        state.last_buy_order_id <- None;
        state.last_buy_order_price <- None;
        ()
      end;

      if cancelled_side = Buy then begin
        state.inflight_cancel_buy <- false;
        state.inflight_amend_buy <- false;
        Hashtbl.remove state.amend_cooldowns "place_Buy"
      end;

      state.open_sell_orders <- List.filter (fun (sell_id, _, _) ->
        sell_id <> order_id
      ) state.open_sell_orders;

      (match cancelled_side with Buy -> state.inflight_buy <- false | Sell -> state.inflight_sell <- false);
      ignore (InFlightOrders.remove_in_flight_order ((match cancelled_side with Buy -> state.duplicate_key_buy | Sell -> state.duplicate_key_sell)))
    end else begin
      Logging.info_f ~section "Order cancellation for %s (%s) ignored for tracking reset because order amendment is in progress"
        asset_symbol order_id
    end
  )

(** Handles order amendment. *)
let handle_order_amended ~now asset_symbol old_order_id new_order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         (String.sub pending_id 14 (String.length pending_id - 14) = old_order_id ||
                          String.sub pending_id 14 (String.length pending_id - 14) = new_order_id) in
      not matches_amend
    ) state.pending_orders;

    (match side with
     | Buy ->
          (match state.last_buy_order_id with
           | Some target_id when target_id = old_order_id ->
               state.last_buy_order_id <- Some new_order_id;
               state.last_buy_order_price <- Some price;
               if old_order_id = new_order_id then
                 ()
               else
                 Logging.info_f ~section "Amended buy order ID in tracking: %s -> %s @ %.2f for %s"
                   old_order_id new_order_id price asset_symbol;

               state.inflight_amend_buy <- false
             | _ ->
                state.inflight_amend_buy <- false)
     | Sell ->
         let original_sell_count = List.length state.open_sell_orders in
         let old_entry = List.find_opt (fun (id, _, _) -> id = old_order_id) state.open_sell_orders in
         let old_qty = match old_entry with
           | Some (_, _, q) -> q
           | None -> venue_lot_qty state.grid_qty state.exchange_id state in
         Logging.info_f ~section "SELL_AMEND [%s] %s -> %s @ %.2f: old_entry=%s old_qty=%.8f sells_before=%d"
           asset_symbol old_order_id new_order_id price
           (match old_entry with Some (_, p, q) -> Printf.sprintf "%.2f/%.8f" p q | None -> "NOT_FOUND")
           old_qty original_sell_count;
         state.open_sell_orders <- (new_order_id, price, old_qty) ::
            List.filter (fun (sell_id, _, _) -> sell_id <> old_order_id) state.open_sell_orders;
         state.recently_injected_sells <- (new_order_id, price, now) :: state.recently_injected_sells;
         Logging.info_f ~section "SELL_AMEND [%s] result: sells_after=%d"
           asset_symbol (List.length state.open_sell_orders));

    let cooldown = if old_order_id = new_order_id then 2.0 else 10.0 in
    Hashtbl.replace state.amend_cooldowns old_order_id (now +. cooldown);
    Hashtbl.replace state.amend_cooldowns new_order_id (now +. cooldown);

    if side = Buy then
      state.inflight_amend_buy <- false;

    ()
  )

(** Handles skipped order amendment. *)
let handle_order_amendment_skipped ~now:_ asset_symbol order_id _ _ =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         String.sub pending_id 14 (String.length pending_id - 14) = order_id in
      not matches_amend
    ) state.pending_orders;

    ()
  )

(** Handles order amendment failure. *)
let handle_order_amendment_failed ~now asset_symbol order_id side reason =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock state.mutex) (fun () ->
    state.pending_orders <- List.filter (fun (pending_id, _s, _p, _) ->
      let matches_amend = String.starts_with ~prefix:"pending_amend_" pending_id &&
                         String.length pending_id > 14 &&
                         String.sub pending_id 14 (String.length pending_id - 14) = order_id in
      not matches_amend
    ) state.pending_orders;

    let lower_reason = String.lowercase_ascii reason in
    let is_cache_miss = contains_fragment lower_reason "order not found" || contains_fragment lower_reason "not found for amendment" || contains_fragment lower_reason "unknown order" in
    let is_cannot_modify = contains_fragment lower_reason "cannot modify canceled or filled" in
    let is_margin_error = contains_fragment lower_reason "insufficient" || contains_fragment lower_reason "margin" in
    let is_rate_limit = contains_fragment lower_reason "too many cumulative requests" || contains_fragment lower_reason "rate limit" in

    let cooldown_duration =
      if is_rate_limit then 10.0
      else if is_cache_miss || is_cannot_modify || is_margin_error then 0.5
      else 2.0
    in

    Hashtbl.replace state.amend_cooldowns order_id (now +. cooldown_duration);

    let is_order_gone = is_cache_miss || is_cannot_modify || is_margin_error in

    if is_order_gone then begin
      let cancel_order = create_cancel_order order_id asset_symbol Grid state.exchange_id in
      ignore (push_order ~now ~state cancel_order);

      if side = Buy then
        state.inflight_cancel_buy <- true;
      (match side with
       | Buy ->
           (match state.last_buy_order_id with
            | Some target_id when target_id = order_id ->
                state.last_buy_order_id <- None;
                state.last_buy_order_price <- None;
                Hashtbl.remove state.amend_cooldowns "place_Buy";
                Logging.info_f ~section "Amendment failed for buy order %s: %s. Order is gone, cleared tracking." order_id reason
            | _ ->
                Hashtbl.remove state.amend_cooldowns "place_Buy";
                ())
       | Sell ->
           let original_sell_count = List.length state.open_sell_orders in
           state.open_sell_orders <- List.filter (fun (sell_id, _, _) -> sell_id <> order_id) state.open_sell_orders;
           if List.length state.open_sell_orders < original_sell_count then
             Logging.info_f ~section "Amendment failed for sell order %s: %s. Order is gone, cleared tracking." order_id reason)
    end else begin
      (match side with
       | Buy ->
           (match state.last_buy_order_id with
            | Some target_id when target_id = order_id ->
                Logging.info_f ~section "Amendment failed for buy order %s: %s. Applying %.1fs cooldown (keeping tracking)." order_id reason cooldown_duration
            | _ -> ())
       | Sell ->
           Logging.info_f ~section "Amendment failed for sell order %s: %s. Applying %.1fs cooldown (keeping tracking)." order_id reason cooldown_duration);
    end;

    ignore (InFlightOrders.remove_in_flight_order ((match side with Buy -> state.duplicate_key_buy | Sell -> state.duplicate_key_sell)));
  )

(** No-op shim for pending cancellation cleanup. *)
let cleanup_pending_cancellation _asset_symbol _order_id = ()
