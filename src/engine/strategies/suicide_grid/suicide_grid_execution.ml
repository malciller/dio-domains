(* Suicide Grid - Strategy Execution Engine *)

open Strategy_common
open Suicide_grid_types
open Suicide_grid_config
open Suicide_grid_reservation
open Suicide_grid_orders

(** Performs 1-to-1 multiset matching between persisted sell levels and open sell orders.
    Returns (open_levels, missing_levels). *)
let partition_persisted_sell_levels persisted open_orders =
  let open_matched = Array.make (List.length open_orders) false in
  let open_orders_arr = Array.of_list open_orders in
  let open_acc = ref [] in
  let missing_acc = ref [] in
  List.iter
    (fun ((target_p, _target_q) as level) ->
       let found = ref None in
       for i = 0 to Array.length open_orders_arr - 1 do
         if !found = None && not open_matched.(i)
         then (
           let _, open_p, _open_q = open_orders_arr.(i) in
           if
             abs_float (open_p -. target_p) <= target_p *. 0.0001
             || abs_float (open_p -. target_p) <= 1e-4
           then found := Some i)
       done;
       match !found with
       | Some idx ->
         open_matched.(idx) <- true;
         open_acc := level :: !open_acc
       | None -> missing_acc := level :: !missing_acc)
    persisted;
  List.rev !open_acc, List.rev !missing_acc
;;

(** Evaluates asset balance recovery and clears asset_low when available balance is restored. *)
let evaluate_asset_low_recovery
      ~state
      ~ecfg
      ~(asset : trading_config)
      ~asset_balance
      ~lot_qty
  =
  if not (Float.is_nan asset_balance)
  then (
    let asset_bal = asset_balance in
    if asset_bal > state.last_seen_asset_balance && state.anticipated_base_credit > 0.0
    then (
      let delta = asset_bal -. state.last_seen_asset_balance in
      state.anticipated_base_credit <- max 0.0 (state.anticipated_base_credit -. delta));
    let qty_f = lot_qty in
    let asset_needed_fast =
      if ecfg.sell_uses_mult then qty_f *. state.cached_sell_mult else qty_f
    in
    let locked_in_sells =
      if ecfg.use_reserved_base_guard
      then List.fold_left (fun acc (_, _, qty) -> acc +. qty) 0.0 state.open_sell_orders
      else 0.0
    in
    let available_asset =
      asset_bal -. state.reserved_base +. state.anticipated_base_credit -. locked_in_sells
    in
    let balance_actually_changed = asset_bal > state.last_seen_asset_balance in
    state.last_seen_asset_balance <- asset_bal;
    let is_sell_on_cooldown = Hashtbl.mem state.amend_cooldowns "place_Sell" in
    let should_clear =
      if ecfg.asset_low_requires_balance_change
      then available_asset >= asset_needed_fast && balance_actually_changed
      else available_asset >= asset_needed_fast && not is_sell_on_cooldown
    in
    if state.asset_low && should_clear
    then (
      state.asset_low <- false;
      state.inflight_sell <- false;
      state.resuming_after_balance_flag <- true;
      Hashtbl.remove state.amend_cooldowns "place_Sell";
      ignore (InFlightOrders.remove_in_flight_order state.duplicate_key_sell);
      Logging.info_f
        ~section
        "Asset balance restored for %s (have %.8f, reserved %.8f, anticipated_credit \
         %.8f, locked_sells %.8f, available %.8f, need %.8f) - resuming sell+buy \
         placement"
        asset.symbol
        asset_bal
        state.reserved_base
        state.anticipated_base_credit
        locked_in_sells
        available_asset
        asset_needed_fast))
;;

(** Evaluates capital (quote) balance recovery and clears capital_low flag. *)
let evaluate_capital_low_recovery
      ~state
      ~(asset : trading_config)
      ~quote_balance
      ~current_price
      ~lot_qty
  =
  if not (Float.is_nan quote_balance)
  then (
    let quote_bal = quote_balance in
    let qty_f = lot_qty in
    let quote_needed_fast =
      if not (Float.is_nan current_price) then current_price *. qty_f else 0.0
    in
    let total_reserved = get_total_reserved_quote state in
    let available_quote = quote_bal -. total_reserved in
    if state.capital_low && state.capital_low_at_balance < 0.0
    then state.capital_low_at_balance <- quote_bal;
    if state.capital_low && available_quote < quote_needed_fast
    then ()
    else if
      state.capital_low
      && state.capital_low_at_balance >= 0.0
      && quote_bal > state.capital_low_at_balance
    then (
      let was_at = state.capital_low_at_balance in
      state.capital_low <- false;
      state.capital_low_logged <- false;
      state.capital_low_at_balance <- 0.0;
      state.resuming_after_balance_flag <- true;
      Hashtbl.remove state.amend_cooldowns "place_Buy";
      state.inflight_buy <- false;
      ignore (InFlightOrders.remove_in_flight_order state.duplicate_key_buy);
      Logging.info_f
        ~section
        "Capital restored for %s (available %.2f, need %.2f, total_reserved %.2f, was_at \
         %.2f) - resuming buy placement"
        asset.symbol
        available_quote
        quote_needed_fast
        total_reserved
        was_at))
;;

(** Performs periodic cleanup of stale pending orders and expired amend cooldowns. *)
let cleanup_pending_and_cooldowns ~state ~now ~(asset : trading_config) =
  let needs_pending_cleanup =
    let rec check_stale count = function
      | [] -> count > 50
      | (_, _, _, ts) :: rest ->
        if now -. ts > 5.0 then true else check_stale (count + 1) rest
    in
    check_stale 0 state.pending_orders
  in
  if needs_pending_cleanup
  then (
    let kept_rev, _, _ =
      List.fold_left
        (fun (acc, kept, removed) ((order_id, side, _, timestamp) as entry) ->
           let age = now -. timestamp in
           if age > 5.0
           then (
             Logging.warn_f
               ~section
               "Removing stale pending order %s for %s (age: %.1fs)"
               order_id
               asset.symbol
               age;
             if String.starts_with ~prefix:"pending_amend_" order_id
             then (
               let target_oid = String.sub order_id 14 (String.length order_id - 14) in
               ignore (InFlightAmendments.remove_in_flight_amendment target_oid))
             else (
               let duplicate_key =
                 match side with
                 | Buy -> state.duplicate_key_buy
                 | Sell -> state.duplicate_key_sell
               in
               ignore (InFlightOrders.remove_in_flight_order duplicate_key);
               match side with
               | Buy -> state.inflight_buy <- false
               | Sell -> state.inflight_sell <- false);
             acc, kept, removed + 1)
           else if kept >= 50
           then acc, kept, removed + 1
           else entry :: acc, kept + 1, removed)
        ([], 0, 0)
        state.pending_orders
    in
    state.pending_orders <- List.rev kept_rev);
  if Hashtbl.length state.amend_cooldowns > 0
  then (
    let to_remove = ref [] in
    Hashtbl.iter
      (fun k v -> if now > v then to_remove := k :: !to_remove)
      state.amend_cooldowns;
    List.iter (Hashtbl.remove state.amend_cooldowns) !to_remove;
    if Hashtbl.length state.amend_cooldowns > 100
    then (
      Hashtbl.reset state.amend_cooldowns;
      Logging.warn_f
        ~section
        "amend_cooldowns exceeded 100 entries for %s, reset"
        asset.symbol));
  if Hashtbl.length state.evicted_orders > 0
  then (
    let to_remove = ref [] in
    Hashtbl.iter
      (fun k v -> if now > v then to_remove := k :: !to_remove)
      state.evicted_orders;
    List.iter (Hashtbl.remove state.evicted_orders) !to_remove)
;;

(** Scans open orders feed, updates local sell tracking, and debounces ghost buy orders. *)
let sync_open_orders
      ~state
      ~now
      ~(asset : trading_config)
      ~bid_price:_
      ~lot_qty
      ~iter_open_orders
      ~ecfg
  =
  let now_time = now in
  let needs_sells_cleanup =
    let rec check_injected count = function
      | [] -> count > 20
      | (_, _, ts) :: rest ->
        if now_time -. ts >= 10.0 then true else check_injected (count + 1) rest
    in
    check_injected 0 state.recently_injected_sells
  in
  if needs_sells_cleanup
  then (
    state.recently_injected_sells
    <- List.filter (fun (_, _, ts) -> now_time -. ts < 10.0) state.recently_injected_sells;
    if List.length state.recently_injected_sells > 20
    then state.recently_injected_sells <- take 20 state.recently_injected_sells);
  let preserved_sells = state.recently_injected_sells in
  state.open_sell_orders <- [];
  let best_buy_price = ref 0.0 in
  let best_buy_id = ref None in
  let best_buy_qty = ref 0.0 in
  let open_buy_count_from_scan = ref 0 in
  let has_recent_amend_buy = ref false in
  let locked_in_buys = ref 0.0 in
  let locked_in_sells = ref 0.0 in
  let closest_sell_order = ref None in
  let matched_persisted_indices = Hashtbl.create 16 in
  iter_open_orders (fun oid price qty side_str userref_opt ->
    let is_our_strategy =
      match userref_opt with
      | Some ref_val -> ref_val <> strategy_userref_mm
      | None -> true
    in
    if qty > 0.0 && is_our_strategy && not (Hashtbl.mem state.evicted_orders oid)
    then
      if side_str = "buy"
      then (
        incr open_buy_count_from_scan;
        locked_in_buys := !locked_in_buys +. (price *. qty);
        if price > !best_buy_price && price > 0.0
        then (
          best_buy_price := price;
          best_buy_id := Some oid;
          best_buy_qty := qty);
        match Hashtbl.find_opt state.amend_cooldowns oid with
        | Some expiry when now_time < expiry -> has_recent_amend_buy := true
        | _ -> ())
      else if side_str = "sell"
      then (
        state.open_sell_orders <- (oid, price, qty) :: state.open_sell_orders;
        locked_in_sells := !locked_in_sells +. qty;
        if ecfg.remaintain_expired_sells
        then (
          let match_idx = ref None in
          List.iteri
            (fun idx (p, _q) ->
               if !match_idx = None && not (Hashtbl.mem matched_persisted_indices idx)
               then
                 if
                   abs_float (p -. price) <= price *. 0.0001
                   || abs_float (p -. price) <= 1e-4
                 then match_idx := Some idx)
            state.persisted_sell_levels;
          match !match_idx with
          | Some idx ->
            Hashtbl.add matched_persisted_indices idx ();
            let _existing_p, existing_q = List.nth state.persisted_sell_levels idx in
            if abs_float (existing_q -. qty) > 1e-6
            then (
              state.persisted_sell_levels
              <- List.mapi
                   (fun i item -> if i = idx then price, qty else item)
                   state.persisted_sell_levels;
              state.persistence_dirty <- true;
              Logging.info_f
                ~section
                "Updated persisted sell level quantity for %s @ %.4f: %.8f -> %.8f"
                asset.symbol
                price
                existing_q
                qty)
          | None ->
            state.persisted_sell_levels
            <- List.sort
                 (fun (p1, _) (p2, _) -> Float.compare p2 p1)
                 ((price, qty) :: state.persisted_sell_levels);
            state.persistence_dirty <- true;
            Logging.info_f
              ~section
              "Adopted open exchange sell order for %s @ %.4f (qty %.8f) into persistent \
               tracking"
              asset.symbol
              price
              qty);
        match !closest_sell_order with
        | None -> closest_sell_order := Some (oid, price)
        | Some (_, best_p) ->
          if price < best_p then closest_sell_order := Some (oid, price)));
  if
    !open_buy_count_from_scan = 0
    && (not state.inflight_cancel_buy)
    && not state.inflight_buy
  then (
    match !best_buy_id with
    | Some oid ->
      Logging.warn_f
        ~section
        "GHOST_BUY_DETECTED [%s] order %s @ %.2f in memory, but not in open orders feed. \
         Clearing."
        asset.symbol
        oid
        !best_buy_price;
      state.last_buy_order_id <- None;
      state.last_buy_order_price <- None;
      set_asset_reserved_quote state 0.0
    | None -> ())
  else if
    (not state.inflight_cancel_buy)
    && (not state.inflight_buy)
    && not state.inflight_amend_buy
  then (
    match !best_buy_id with
    | Some best_order_id ->
      let best_price = !best_buy_price in
      let recent_amend =
        match Hashtbl.find_opt state.amend_cooldowns best_order_id with
        | Some expiry -> now -. expiry < 5.0
        | None -> false
      in
      if not recent_amend
      then (
        state.last_buy_order_price <- Some best_price;
        state.last_buy_order_id <- Some best_order_id;
        set_asset_reserved_quote state (best_price *. lot_qty))
    | None -> ());
  if ecfg.merge_preserved_sells
  then
    List.iter
      (fun (preserved_id, _preserved_price, _) ->
         let already_present =
           List.exists (fun (id, _, _) -> id = preserved_id) state.open_sell_orders
         in
         if not already_present then ())
      preserved_sells;
  ( !open_buy_count_from_scan
  , !has_recent_amend_buy
  , !locked_in_buys
  , !locked_in_sells
  , !closest_sell_order
  , !best_buy_qty )
;;

let compute_buy_ref_price ~bid_price ~ask_price =
  if bid_price > 0.0 then bid_price else ask_price
;;

(** Evaluates buy placement, multi-buy cancellation, and buy trailing. *)
let evaluate_buy_leg
      ~state
      ~now
      ~(asset : trading_config)
      ~bid_price
      ~ask_price
      ~quote_balance
      ~cycle
      ~iter_open_orders
      ~open_buy_count_from_scan
      ~has_recent_amend_buy
      ~locked_in_buys
      ~closest_sell_order_initial
      ~pending_buy_qty_from_scan
  =
  let buy_attempted = ref false in
  let buy_order_pending =
    List.exists (fun (_, side, _, _) -> side = Buy) state.pending_orders
  in
  let has_tracked_buy = state.last_buy_order_id <> None in
  let open_buy_count = open_buy_count_from_scan in
  let effective_buy_count =
    if has_tracked_buy && open_buy_count = 0 then 1 else open_buy_count
  in
  let suppress_duplicate_buys = has_recent_amend_buy in
  let qty = venue_lot_qty state.grid_qty asset.exchange state in
  let grid_interval = asset.grid_interval in
  let quote_needed = ask_price *. qty in
  if buy_order_pending
  then ()
  else if
    effective_buy_count > 1
    && (not state.inflight_cancel_buy)
    && (not state.inflight_amend_buy)
    && not suppress_duplicate_buys
  then (
    Logging.info_f
      ~section
      "Found %d buy orders for %s, cancelling all buy orders to maintain single buy \
       order policy"
      effective_buy_count
      asset.symbol;
    iter_open_orders (fun order_id _ _ side_str userref_opt ->
      let is_our_strategy =
        match userref_opt with
        | Some ref_val -> ref_val <> strategy_userref_mm
        | None -> true
      in
      if is_our_strategy && side_str = "buy"
      then (
        let cancel_order =
          create_cancel_order order_id asset.symbol Grid asset.exchange
        in
        ignore (push_order ~now ~state cancel_order);
        Logging.info_f
          ~section
          "Cancelling excess buy order: %s for %s"
          order_id
          asset.symbol));
    state.last_buy_order_id <- None;
    state.last_buy_order_price <- None;
    state.last_cycle <- cycle)
  else if effective_buy_count = 0 && not buy_order_pending
  then (
    let ref_price = compute_buy_ref_price ~bid_price ~ask_price in
    let raw_buy_price = calculate_grid_price ref_price grid_interval false state in
    let buy_price =
      if bid_price > 0.0 then min raw_buy_price bid_price else raw_buy_price
    in
    let buy_cooldown_key = "place_Buy" in
    let is_buy_on_cooldown = Hashtbl.mem state.amend_cooldowns buy_cooldown_key in
    let has_crossing_sell =
      List.exists
        (fun (_, price, _) ->
           price <= buy_price || (bid_price > 0.0 && price <= bid_price))
        state.open_sell_orders
      || Hashtbl.length state.evicted_orders > 0
    in
    if state.capital_low
    then
      Logging.debug_f
        ~section
        "Buy placement skipped for %s: capital_low flag is set"
        asset.symbol
    else if has_crossing_sell
    then
      Logging.debug_f
        ~section
        "Buy placement deferred for %s: active or evicted sell order price <= \
         buy_price/bid (wash trade protection)"
        asset.symbol
    else if not (Float.is_nan quote_balance)
    then
      if is_buy_on_cooldown || state.inflight_buy
      then
        Logging.debug_f
          ~section
          "Buy placement skipped for %s (cooldown=%B, inflight=%B)"
          asset.symbol
          is_buy_on_cooldown
          state.inflight_buy
      else (
        let quote_bal = quote_balance in
        let available_quote_balance = quote_bal -. locked_in_buys in
        let balance_ok = available_quote_balance >= buy_price *. qty in
        if balance_ok
        then (
          let order =
            create_order
              state.duplicate_key_buy
              asset.symbol
              Buy
              qty
              (Some buy_price)
              true
              asset.exchange
          in
          if push_order ~now ~state order
          then (
            buy_attempted := true;
            state.last_buy_order_price <- Some buy_price;
            Logging.info_f
              ~section
              "Placed buy order for %s: %.8f @ %.4f"
              asset.symbol
              qty
              buy_price))
        else (
          let cooldown_key = "place_Buy" in
          if not (Hashtbl.mem state.amend_cooldowns cooldown_key)
          then (
            Logging.warn_f
              ~section
              "Local balance low for %s buy (need %.2f, available %.2f) - attempting \
               anyway, exchange will reject if truly insufficient"
              asset.symbol
              quote_needed
              available_quote_balance;
            Hashtbl.replace state.amend_cooldowns cooldown_key (now +. 2.0);
            let order =
              create_order
                state.duplicate_key_buy
                asset.symbol
                Buy
                qty
                (Some buy_price)
                true
                asset.exchange
            in
            if push_order ~now ~state order
            then (
              buy_attempted := true;
              state.last_buy_order_price <- Some buy_price))))
    else
      Logging.warn_f
        ~section
        "No quote balance data available for %s buy order"
        asset.symbol;
    state.last_cycle <- cycle)
  else if effective_buy_count > 0
  then (
    let closest_sell_ref = ref closest_sell_order_initial in
    let update_closest_pending oid price =
      match !closest_sell_ref with
      | None -> closest_sell_ref := Some (oid, price)
      | Some (_, best_p) -> if price < best_p then closest_sell_ref := Some (oid, price)
    in
    List.iter
      (fun (oid, side, price, _) -> if side = Sell then update_closest_pending oid price)
      state.pending_orders;
    let closest_sell_order_val = !closest_sell_ref in
    let is_alpaca = Exchange.Types.exchange_of_string asset.exchange = Alpaca in
    let qty_mismatch =
      is_alpaca
      && pending_buy_qty_from_scan > 0.0
      && abs_float (pending_buy_qty_from_scan -. qty) > 1e-6
    in
    if closest_sell_order_val <> None
    then (
      match
        closest_sell_order_val, state.last_buy_order_price, state.last_buy_order_id
      with
      | Some (_sell_order_id, sell_price), Some current_buy_price, Some buy_order_id ->
        let double_grid_interval = bid_price *. (2.0 *. grid_interval /. 100.0) in
        let ref_price = compute_buy_ref_price ~bid_price ~ask_price in
        let grid_buy_from_ref =
          calculate_grid_price ref_price grid_interval false state
        in
        let grid_buy_capped =
          if bid_price > 0.0 then min grid_buy_from_ref bid_price else grid_buy_from_ref
        in
        let exact_target =
          state.cached_round_price (sell_price -. double_grid_interval)
        in
        let proposed_buy_price = grid_buy_capped in
        let target_buy_price = min proposed_buy_price exact_target in
        let current_buy_price_rounded = state.cached_round_price current_buy_price in
        let price_diff_rounded =
          state.cached_round_price
            (abs_float (target_buy_price -. current_buy_price_rounded))
        in
        let min_move_threshold = get_min_move_threshold bid_price grid_interval state in
        if target_buy_price > current_buy_price || qty_mismatch
        then (
          let allow =
            if qty_mismatch
            then (
              let is_being_amended =
                List.exists
                  (fun (id, _, _, _) ->
                     String.starts_with ~prefix:"pending_amend_" id
                     && String.sub id 14 (String.length id - 14) = buy_order_id)
                  state.pending_orders
              in
              let is_in_flight = InFlightAmendments.is_in_flight buy_order_id in
              let is_on_cooldown = Hashtbl.mem state.amend_cooldowns buy_order_id in
              (not is_being_amended) && (not is_in_flight) && not is_on_cooldown)
            else
              amend_allowed
                ~state
                ~order_id:buy_order_id
                ~target_price:target_buy_price
                ~current_price_rounded:current_buy_price_rounded
                ~price_diff:price_diff_rounded
                ~min_move_threshold
          in
          if allow
          then (
            let quote_bal = quote_balance in
            if
              (not (Float.is_nan quote_balance))
              && can_place_buy_order qty quote_bal quote_needed
            then (
              let order =
                create_amend_order
                  buy_order_id
                  asset.symbol
                  Buy
                  qty
                  (Some target_buy_price)
                  true
                  Grid
                  asset.exchange
              in
              ignore (push_order ~now ~state order);
              state.last_buy_order_price <- Some target_buy_price;
              if qty_mismatch
              then
                Logging.info_f
                  ~section
                  "Alpaca pending buy order %s qty (%.8f) differs from config (%.8f) - \
                   amending price to config target %.4f and qty to %.8f"
                  buy_order_id
                  pending_buy_qty_from_scan
                  qty
                  target_buy_price
                  qty;
              ())
            else if not (Float.is_nan quote_balance)
            then
              Logging.warn_f
                ~section
                "Insufficient quote balance for %s trailing: need %.2f, have %.2f"
                asset.symbol
                quote_needed
                quote_bal
            else Logging.warn_f ~section "No quote balance for %s trailing" asset.symbol))
      | _ -> ())
    else (
      match state.last_buy_order_price, state.last_buy_order_id with
      | Some current_buy_price, Some buy_order_id ->
        let ref_price = compute_buy_ref_price ~bid_price ~ask_price in
        let raw_target = calculate_grid_price ref_price grid_interval false state in
        let target_buy_price =
          if bid_price > 0.0 then min raw_target bid_price else raw_target
        in
        if target_buy_price > current_buy_price || qty_mismatch
        then (
          let min_move_threshold = get_min_move_threshold bid_price grid_interval state in
          let current_buy_price_rounded = state.cached_round_price current_buy_price in
          let price_diff_rounded =
            state.cached_round_price
              (abs_float (target_buy_price -. current_buy_price_rounded))
          in
          let allow =
            if qty_mismatch
            then (
              let is_being_amended =
                List.exists
                  (fun (id, _, _, _) ->
                     String.starts_with ~prefix:"pending_amend_" id
                     && String.sub id 14 (String.length id - 14) = buy_order_id)
                  state.pending_orders
              in
              let is_in_flight = InFlightAmendments.is_in_flight buy_order_id in
              let is_on_cooldown = Hashtbl.mem state.amend_cooldowns buy_order_id in
              (not is_being_amended) && (not is_in_flight) && not is_on_cooldown)
            else
              amend_allowed
                ~state
                ~order_id:buy_order_id
                ~target_price:target_buy_price
                ~current_price_rounded:current_buy_price_rounded
                ~price_diff:price_diff_rounded
                ~min_move_threshold
          in
          if allow
          then (
            let quote_bal = quote_balance in
            if
              (not (Float.is_nan quote_balance))
              && can_place_buy_order qty quote_bal quote_needed
            then (
              let order =
                create_amend_order
                  buy_order_id
                  asset.symbol
                  Buy
                  qty
                  (Some target_buy_price)
                  true
                  Grid
                  asset.exchange
              in
              ignore (push_order ~now ~state order);
              state.last_buy_order_price <- Some target_buy_price;
              if qty_mismatch
              then
                Logging.info_f
                  ~section
                  "Alpaca pending buy order %s qty (%.8f) differs from config (%.8f) - \
                   amending price to config target %.4f and qty to %.8f"
                  buy_order_id
                  pending_buy_qty_from_scan
                  qty
                  target_buy_price
                  qty;
              ())
            else if not (Float.is_nan quote_balance)
            then
              Logging.warn_f
                ~section
                "Insufficient quote balance to trail buy: need %.2f, have %.2f"
                quote_needed
                quote_bal
            else Logging.warn_f ~section "No quote balance for buy trailing"))
      | _ -> ());
    state.last_cycle <- cycle)
  else state.last_cycle <- cycle;
  !buy_attempted
;;

(** Evaluates buy-triggered and Alpaca-exclusive inventory-maintenance sell placement leg. *)
let evaluate_sell_leg
      ~state
      ~now
      ~(asset : trading_config)
      ~bid_price
      ~ask_price
      ~asset_balance
      ~buy_attempted
      ~ecfg
      ~locked_in_sells
  =
  let qty = venue_lot_qty state.grid_qty asset.exchange state in
  let available_base =
    if Float.is_nan asset_balance
    then 0.0
    else
      asset_balance
      +. state.anticipated_base_credit
      -. state.reserved_base
      -. locked_in_sells
  in
  if
    ecfg.remaintain_expired_sells
    && state.persisted_sell_levels <> []
    && not (Float.is_nan asset_balance)
  then (
    let available_for_missing_sells =
      max
        0.0
        (asset_balance
         +. state.anticipated_base_credit
         -. state.reserved_base
         -. locked_in_sells)
    in
    let open_levels, missing_levels =
      partition_persisted_sell_levels state.persisted_sell_levels state.open_sell_orders
    in
    let missing_desc =
      List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) missing_levels
    in
    let rem_avail = ref available_for_missing_sells in
    let kept_missing = ref [] in
    let pruned_missing = ref [] in
    List.iter
      (fun ((_target_p, target_q) as level) ->
         if !rem_avail >= target_q -. 1e-6
         then (
           kept_missing := level :: !kept_missing;
           rem_avail := max 0.0 (!rem_avail -. target_q))
         else pruned_missing := level :: !pruned_missing)
      missing_desc;
    let new_persisted = open_levels @ List.rev !kept_missing in
    state.persisted_sell_levels
    <- List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) new_persisted;
    if !pruned_missing <> []
    then (
      state.persistence_dirty <- true;
      List.iter
        (fun (p, q) ->
           Logging.info_f
             ~section
             "Reconciled offline sell fill for %s @ %.4f (qty %.8f) - balance consumed \
              while offline"
             asset.symbol
             p
             q;
           state.last_sell_fill_price <- Some p)
        !pruned_missing));
  let is_alpaca = Exchange.Types.exchange_of_string asset.exchange = Alpaca in
  let min_needed_base =
    if is_alpaca
    then round_qty (qty *. state.cached_sell_mult) asset.symbol asset.exchange
    else qty
  in
  let missing_alpaca_sell_grid =
    if ecfg.remaintain_expired_sells
    then (
      let _, missing_lvl_check =
        partition_persisted_sell_levels state.persisted_sell_levels state.open_sell_orders
      in
      (not (has_active_sell state))
      && available_base >= min_needed_base
      && (state.just_filled_buy
          || buy_attempted
          || state.resuming_after_balance_flag
          || missing_lvl_check <> []
          || (state.open_sell_orders = [] && Option.is_some state.last_buy_fill_price)))
    else false
  in
  let should_trigger_sell =
    if ecfg.remaintain_expired_sells
    then missing_alpaca_sell_grid
    else state.just_filled_buy || buy_attempted
  in
  let is_sell_on_cooldown = Hashtbl.mem state.amend_cooldowns "place_Sell" in
  if
    should_trigger_sell
    && (not (Float.is_nan asset_balance))
    && (not (has_active_sell state))
    && (not state.asset_low)
    && not is_sell_on_cooldown
  then (
    let asset_bal = asset_balance in
    let grid_interval = asset.grid_interval in
    let qty = venue_lot_qty state.grid_qty asset.exchange state in
    let sell_mult = state.cached_sell_mult in
    (* Determine target price & qty for sell placement *)
    let target_sell_price_opt, target_sell_qty_override =
      if ecfg.remaintain_expired_sells && state.persisted_sell_levels <> []
      then (
        let _, missing_levels =
          partition_persisted_sell_levels
            state.persisted_sell_levels
            state.open_sell_orders
        in
        let missing_sorted_desc =
          List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) missing_levels
        in
        match missing_sorted_desc with
        | (tp, tq) :: _ -> Some tp, Some tq
        | [] -> None, None)
      else None, None
    in
    let sell_price =
      match target_sell_price_opt with
      | Some tp -> tp
      | None ->
        let base_price_for_sell =
          if ecfg.remaintain_expired_sells
          then (
            (* Alpaca: Strictly use buy fill price to prevent selling at a loss during price drops *)
            match state.last_buy_fill_price with
            | Some fill_p -> fill_p
            | None -> bid_price)
          else (
            (* Non-Alpaca venues: untouched existing re-anchoring behavior *)
            match state.last_buy_fill_price with
            | Some fill_p
              when (not state.resuming_after_balance_flag)
                   && abs_float (bid_price -. fill_p)
                      <= bid_price *. (grid_interval /. 100.0) -> fill_p
            | Some fill_p ->
              Logging.debug_f
                ~section
                "Re-anchoring sell base price for %s to bid %.4f (last fill %.4f drifted \
                 or resuming_after_balance=%B)"
                asset.symbol
                bid_price
                fill_p
                state.resuming_after_balance_flag;
              bid_price
            | None -> bid_price)
        in
        let raw_sell_price =
          calculate_grid_price base_price_for_sell grid_interval true state
        in
        if ask_price > 0.0 then max raw_sell_price ask_price else raw_sell_price
    in
    let sell_qty, is_accumulation_sell, required_profit =
      match target_sell_qty_override with
      | Some tq when ecfg.remaintain_expired_sells ->
        let target_q =
          if ecfg.sell_uses_mult
          then Float.min tq (round_qty (qty *. sell_mult) asset.symbol asset.exchange)
          else tq
        in
        target_q, false, 0.0
      | _ ->
        compute_sell_qty
          ~ecfg
          ~state
          ~asset
          ~qty
          ~sell_price
          ~sell_mult
          ~symbol:asset.symbol
          ~exchange:asset.exchange
    in
    let accumulation_ok_on_recovery =
      accumulation_sell_allowed_on_recovery ~ecfg ~state ~is_accumulation_sell ~sell_qty
    in
    if not accumulation_ok_on_recovery
    then
      Logging.info_f
        ~section
        "Sell deferred for %s on balance recovery: accumulated_profit %.4f < required \
         %.4f (buffer %.4f)"
        asset.symbol
        state.accumulated_profit
        required_profit
        asset.accumulation_buffer;
    let locked_in_sells_local = locked_in_sells in
    let available =
      asset_bal
      +. state.anticipated_base_credit
      -. state.reserved_base
      -. locked_in_sells_local
    in
    let effective_sell_qty, balance_ok =
      if is_alpaca
      then (
        match target_sell_qty_override with
        | Some tq ->
          let rounded_tq = round_qty tq asset.symbol asset.exchange in
          if available >= rounded_tq -. 1e-6 && rounded_tq > 0.0
          then rounded_tq, true
          else (
            Logging.debug_f
              ~section
              "Sell order blocked for Alpaca %s: available %.8f < target_q %.8f"
              asset.symbol
              available
              rounded_tq;
            0.0, false)
        | None ->
          let avail_rounded = round_qty available asset.symbol asset.exchange in
          let sell_q = avail_rounded in
          if sell_q > 0.0
          then sell_q, true
          else (
            Logging.debug_f
              ~section
              "Sell order blocked for Alpaca %s: available %.8f (bal %.8f + anticipated \
               %.8f - reserved %.8f - locked_sells %.8f) <= 0"
              asset.symbol
              available
              asset_bal
              state.anticipated_base_credit
              state.reserved_base
              locked_in_sells_local;
            0.0, false))
      else if ecfg.use_accumulation_sells && ecfg.use_reserved_base_guard
      then
        if available >= sell_qty
        then sell_qty, true
        else if available > 0.0
        then (
          let rounded_avail = round_qty available asset.symbol asset.exchange in
          if rounded_avail > 0.0
          then rounded_avail, true
          else (
            Logging.debug_f
              ~section
              "Sell order blocked for %s: available %.8f (bal %.8f + anticipated %.8f - \
               reserved %.8f - locked_sells %.8f) < sell_qty %.8f"
              asset.symbol
              available
              asset_bal
              state.anticipated_base_credit
              state.reserved_base
              locked_in_sells_local
              sell_qty;
            0.0, false))
        else (
          Logging.debug_f
            ~section
            "Sell order blocked for %s: available %.8f (bal %.8f + anticipated %.8f - \
             reserved %.8f - locked_sells %.8f) < sell_qty %.8f"
            asset.symbol
            available
            asset_bal
            state.anticipated_base_credit
            state.reserved_base
            locked_in_sells_local
            sell_qty;
          0.0, false)
      else if ecfg.use_reserved_base_guard
      then
        if available >= sell_qty
        then sell_qty, true
        else (
          Logging.debug_f
            ~section
            "Sell order blocked for %s: available %.8f (bal %.8f + anticipated %.8f - \
             reserved %.8f - locked_sells %.8f) < sell_qty %.8f"
            asset.symbol
            available
            asset_bal
            state.anticipated_base_credit
            state.reserved_base
            locked_in_sells_local
            sell_qty;
          0.0, false)
      else sell_qty, true
    in
    if accumulation_ok_on_recovery && sell_qty = 0.0 && is_accumulation_sell
    then (
      let actual_cost = qty *. sell_price in
      state.accumulated_profit <- state.accumulated_profit -. actual_cost;
      state.reserved_base <- state.reserved_base +. qty;
      state.persistence_dirty <- true;
      Logging.info_f
        ~section
        "Retained full share of %s (profit %.4f covered cost %.4f, reserved_base now \
         %.0f)"
        asset.symbol
        (state.accumulated_profit +. actual_cost)
        actual_cost
        state.reserved_base)
    else if accumulation_ok_on_recovery && balance_ok
    then (
      let sell_order =
        create_order
          state.duplicate_key_sell
          asset.symbol
          Sell
          effective_sell_qty
          (Some sell_price)
          true
          asset.exchange
      in
      if push_order ~now ~state sell_order
      then (
        state.asset_low <- false;
        if ecfg.remaintain_expired_sells && target_sell_price_opt = None
        then (
          state.persisted_sell_levels
          <- List.sort
               (fun (p1, _) (p2, _) -> Float.compare p2 p1)
               ((sell_price, effective_sell_qty) :: state.persisted_sell_levels);
          state.persistence_dirty <- true);
        if is_accumulation_sell
        then (
          let rounded_sell = effective_sell_qty in
          let rounding_diff = qty -. rounded_sell in
          let actual_cost = rounding_diff *. sell_price in
          state.accumulated_profit <- state.accumulated_profit -. actual_cost;
          let base_increment = qty -. rounded_sell in
          state.reserved_base <- state.reserved_base +. base_increment;
          state.persistence_dirty <- true;
          Logging.info_f
            ~section
            "Accumulation sell for %s: %.8f (sell_mult, profit %.4f covered cost %.4f, \
             reserved_base now %.8f)"
            asset.symbol
            rounded_sell
            (state.accumulated_profit +. actual_cost)
            actual_cost
            state.reserved_base)
        else if ecfg.sell_uses_mult && persistence_accumulation_exchange state.exchange_id
        then
          if target_sell_qty_override = None
          then (
            let base_increment = qty -. effective_sell_qty in
            if base_increment > 0.0
            then (
              state.reserved_base <- state.reserved_base +. base_increment;
              state.persistence_dirty <- true;
              Logging.info_f
                ~section
                "Reserving base for %s: +%.8f (sell_mult %.4f, total reserved_base now \
                 %.8f)"
                asset.symbol
                base_increment
                sell_mult
                state.reserved_base));
        Logging.info_f
          ~section
          "Placed sell order for %s: %.8f @ %.4f"
          asset.symbol
          effective_sell_qty
          sell_price)));
  state.resuming_after_balance_flag <- false;
  state.just_filled_buy <- false
;;

(** Main strategy execution loop. *)
let execute_strategy
      ?cached_state
      ~now
      (asset : trading_config)
      (current_price : float)
      (top_bid : float)
      (top_ask : float)
      (asset_balance : float)
      (quote_balance : float)
      (_open_buy_count : int)
      (_open_sell_count : int)
      (iter_open_orders :
        (string -> float -> float -> string -> int option -> unit) -> unit)
      (cycle : int)
  =
  let state =
    match cached_state with
    | Some s -> s
    | None -> get_strategy_state asset.symbol
  in
  if state.exchange_id = ""
  then (
    state.exchange_id <- asset.exchange;
    state.cached_ecfg <- get_exchange_config asset.exchange;
    state.cached_round_price <- get_round_price_fn asset.symbol asset.exchange;
    state.cached_price_increment <- get_price_increment asset.symbol asset.exchange;
    state.cached_qty_increment <- get_qty_increment_val asset.symbol asset.exchange;
    state.cached_qty_min
    <- (match get_exchange_module asset.exchange with
        | Some (module Ex : Exchange.S) ->
          Option.value (Ex.get_qty_min ~symbol:asset.symbol) ~default:1.0
        | None -> 1.0);
    state.exchange_reserved_atomic <- Some (get_exchange_reserved_atomic asset.exchange));
  let ecfg = state.cached_ecfg in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
       let lot_qty = venue_lot_qty state.grid_qty asset.exchange state in
       evaluate_asset_low_recovery ~state ~ecfg ~asset ~asset_balance ~lot_qty;
       evaluate_capital_low_recovery ~state ~asset ~quote_balance ~current_price ~lot_qty;
       if Float.is_nan current_price
       then (
         if state.last_cycle <> cycle
         then
           Logging.info_f
             ~section
             "Waiting for price data for %s (no ticker received yet)"
             asset.symbol)
       else (
         let bid_price, ask_price =
           if
             (not (Float.is_nan top_bid))
             && top_bid > 0.0
             && (not (Float.is_nan top_ask))
             && top_ask > 0.0
           then top_bid, top_ask
           else current_price, current_price
         in
         cleanup_pending_and_cooldowns ~state ~now ~asset;
         let ( open_buy_count_from_scan
             , has_recent_amend_buy
             , locked_in_buys
             , locked_in_sells
             , closest_sell_order
             , pending_buy_qty_from_scan )
           =
           sync_open_orders ~state ~now ~asset ~bid_price ~lot_qty ~iter_open_orders ~ecfg
         in
         state.maker_fee
         <- (match asset.maker_fee with
             | Some f -> f
             | None ->
               (match
                  Fee_cache.get_maker_fee ~exchange:asset.exchange ~symbol:asset.symbol
                with
                | Some cached -> cached
                | None -> 0.0));
         let is_stale =
           ecfg.check_stale_balance
           && (Float.is_nan asset_balance || Float.is_nan quote_balance)
         in
         if is_stale
         then (
           state.last_cycle <- cycle;
           ())
         else (
           let buy_attempted =
             evaluate_buy_leg
               ~state
               ~now
               ~asset
               ~bid_price
               ~ask_price
               ~quote_balance
               ~cycle
               ~iter_open_orders
               ~open_buy_count_from_scan
               ~has_recent_amend_buy
               ~locked_in_buys
               ~closest_sell_order_initial:closest_sell_order
               ~pending_buy_qty_from_scan
           in
           evaluate_sell_leg
             ~state
             ~now
             ~asset
             ~bid_price
             ~ask_price
             ~asset_balance
             ~buy_attempted
             ~ecfg
             ~locked_in_sells)))
;;
