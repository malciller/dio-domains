(* Strategy decision procedures (strategy-agnostic).

   Owed-sell pricing and the buy leg (fact computation, excess cancel, initial placement,
   trailing amend, excess sweep). Extracted from jacobs_ladder_execution; the grid and the
   config interpreter both call these. Policy here is to be ported into the strategy file. *)

open Strategy_common
open Strategy_state
open Strategy_venue
open Strategy_reservation
open Strategy_orders
open Strategy_lifecycle
module Sell_orders = Strategy_sell_orders

(** Price at which a newly-owed sell would be placed, given the current book; the single
    source of truth shared by [evaluate_sell_leg] (which places the sell) and
    [evaluate_buy_leg] (which pre-clamps a fresh buy against the companion sell's
    restricted zone in the same tick).

    The buy leg runs first, so the companion sell is not yet in the open-order feed when
    the fresh buy is clamped. On a fill tick the sell leg then places a sell one rung
    lower than the closest sell the buy was clamped against, so the buy lands inside the
    new sell's [2*gi] zone and is amended back down the next tick. Clamping against this
    prospective price up front makes the placed buy equal to what the amend would have
    produced, without the round trip.

    Changes here must keep [evaluate_sell_leg]'s placement and this anticipation in
    lockstep; both callers pass identical inputs. *)
let owed_sell_price
  ~(state : strategy_state)
  ~(asset : trading_config)
  ~(ecfg : exchange_config)
  ~bid_price
  ~ask_price
  ~(capital_exhausted : bool)
  =
  let is_alpaca = Exchange.Types.exchange_of_string asset.exchange = Alpaca in
  let grid_interval = asset.grid_interval in
  let base_price_for_sell =
    if ecfg.remaintain_expired_sells
    then (
      match state.last_buy_fill_price with
      | Some fill_p -> fill_p
      | None -> bid_price)
    else (
      match state.last_buy_fill_price with
      | Some fill_p
        when capital_exhausted
             || ((not state.resuming_after_balance_flag)
                 && abs_float (bid_price -. fill_p)
                    <= bid_price *. (grid_interval /. 100.0)) -> fill_p
      | Some _ -> bid_price
      | None -> bid_price)
  in
  let raw_sell_price =
    calculate_grid_price base_price_for_sell grid_interval true state
  in
  if is_alpaca
  then raw_sell_price
  else if ask_price > 0.0
  then max raw_sell_price ask_price
  else raw_sell_price
;;

(** Evaluates buy placement, multi-buy cancellation, and buy trailing. *)
let buy_leg_facts ~state ~open_buy_count_from_scan ~has_recent_amend_buy =
  let buy_order_pending =
    List.exists (fun (_, side, _, _) -> side = Buy) state.pending_orders
  in
  let has_tracked_buy = state.last_buy_order_id <> None in
  let effective_buy_count =
    if has_tracked_buy && open_buy_count_from_scan = 0
    then 1
    else open_buy_count_from_scan
  in
  let should_cancel =
    effective_buy_count > 1
    && (not state.inflight_cancel_buy)
    && (not state.inflight_amend_buy)
    && not has_recent_amend_buy
  in
  buy_order_pending, effective_buy_count, should_cancel
;;

(** Branch: cancel every resting buy to restore the single-buy policy. *)
let buy_cancel_excess
  ~state
  ~now
  ~(asset : trading_config)
  ~iter_open_orders
  ~cycle
  ~effective_buy_count
  =
  (* A cancel+replace amendment (Hyperliquid/Alpaca) transiently lists BOTH the old id and
     the replacement, which trips the ">1 buys" branch. The old id is covered by the
     registry's [Pending]/[Replaced] phase; the replacement id is not registered at all,
     so without [is_replacement_target] it would be treated as a duplicate and cancelled -
     racing the amend (the amend then fails "order not found") and leaving buy tracking
     pointed at a dead order. Track what we actually cancelled so tracking is only cleared
     for orders that really left the book. *)
  let cancelled = ref [] in
  iter_open_orders (fun order_id _ _ side_str userref_opt ->
    let is_our_strategy =
      match userref_opt with
      | Some ref_val -> ref_val <> strategy_userref_mm
      | None -> true
    in
    if is_our_strategy && side_str = "buy"
    then (
      let is_mid_amend =
        InFlightAmendments.is_amend_lifecycle_active order_id
        || InFlightAmendments.is_replacement_target order_id
        || List.exists
             (fun (id, _, _, _) ->
               String.starts_with ~prefix:"pending_amend_" id
               && String.length id > 14
               && String.sub id 14 (String.length id - 14) = order_id)
             state.pending_orders
      in
      if is_mid_amend
      then
        Logging.debug_f
          ~section
          "Skipping cancel of mid-amendment buy order %s for %s (amend will replace it)"
          order_id
          asset.symbol
      else (
        let cancel_order =
          create_cancel_order order_id asset.symbol Ladder asset.exchange
        in
        ignore (push_order ~now ~state cancel_order);
        cancelled := order_id :: !cancelled;
        Logging.info_f
          ~section
          "Cancelling excess buy order: %s for %s"
          order_id
          asset.symbol)));
  if !cancelled <> []
  then
    Logging.info_f
      ~section
      "Found %d buy orders for %s, cancelled all cancellable buy orders to maintain \
       single buy order policy"
      effective_buy_count
      asset.symbol
  else
    Logging.debug_f
      ~section
      "Found %d buy orders for %s, all mid-amendment - no cancellable excess"
      effective_buy_count
      asset.symbol;
  (* Only clear tracking when the tracked order itself was cancelled. If every candidate
     was mid-amendment (or the tracked id was the protected one), the resting buy (or its
     in-flight amend) is still live; clearing here would orphan it until the next feed
     scan adopts the best buy. *)
  (match state.last_buy_order_id with
   | Some tracked when List.exists (String.equal tracked) !cancelled ->
     state.last_buy_order_id <- None;
     state.last_buy_order_price <- None
   | _ -> ());
  state.last_cycle <- cycle
;;

(** The buy-placement plan: the sizing and price computed for a fresh buy, plus the branch
    facts derived from the current state. Pure (only stamps [last_cycle]). *)
type buy_plan =
  { bp_qty : float
  ; bp_price : float
  ; bp_quote_needed : float
  ; bp_available : float
  ; bp_balance_ok : bool
  ; bp_capital_low : bool
  ; bp_crossing : bool
  ; bp_quote_nan : bool
  ; bp_cooldown : bool
  ; bp_inflight : bool
  }

(** Computes the fresh-buy price (grid rung clamped to the bid and to the closest or
    prospective sell's 2*gi zone) and the branch facts. *)
let buy_place_plan
  ~state
  ~now:_
  ~(asset : trading_config)
  ~bid_price
  ~ask_price
  ~quote_balance
  ~oracle_halted
  ~cycle
  ~locked_in_buys
  ~closest_sell_order_initial
  =
  let qty = venue_lot_qty state.grid_qty asset.exchange state in
  let grid_interval = asset.grid_interval in
  let quote_needed = ask_price *. qty in
  let _bp_price = Strategy_state.sub_start state in
  let ref_price = compute_buy_ref_price ~bid_price ~ask_price in
  let raw_buy_price = calculate_grid_price ref_price grid_interval false state in
  let buy_price =
    if bid_price > 0.0 then min raw_buy_price bid_price else raw_buy_price
  in
  let floor_for_sell sell_price =
    sell_price -. (sell_price *. (2.0 *. grid_interval /. 100.0))
  in
  let buy_price =
    match closest_sell_order_initial with
    | Some (_, sell_price) -> min buy_price (floor_for_sell sell_price)
    | None -> buy_price
  in
  let buy_price =
    if state.just_filled_buy || state.resuming_after_balance_flag
    then (
      let companion_sell_price =
        owed_sell_price
          ~state
          ~asset
          ~ecfg:state.cached_ecfg
          ~bid_price
          ~ask_price
          ~capital_exhausted:(oracle_halted || state.capital_low)
      in
      min buy_price (floor_for_sell companion_sell_price))
    else buy_price
  in
  Strategy_state.sub_stop state Strategy_state.Bplan_price _bp_price;
  let is_buy_on_cooldown = Hashtbl.mem state.amend_cooldowns "place_Buy" in
  let _bp_sells = Strategy_state.sub_start state in
  let has_crossing_sell =
    Sell_orders.exists_price_leq
      state.open_sell_orders
      (if bid_price > 0.0 then Float.max buy_price bid_price else buy_price)
    (* Only an evicted *sell* is a wash-trade hazard. An evicted buy is a venue-confirmed
       terminal buy (filled/canceled amend failure); counting it here deferred every
       replacement buy for the full eviction TTL. *)
    || Hashtbl.fold
         (fun _ (_expiry, side) acc -> acc || side = Sell)
         state.evicted_orders
         false
  in
  Strategy_state.sub_stop state Strategy_state.Bplan_sells _bp_sells;
  let quote_nan = Float.is_nan quote_balance in
  let available = quote_balance -. locked_in_buys in
  let balance_ok = (not quote_nan) && available >= buy_price *. qty in
  state.last_cycle <- cycle;
  { bp_qty = qty
  ; bp_price = buy_price
  ; bp_quote_needed = quote_needed
  ; bp_available = available
  ; bp_balance_ok = balance_ok
  ; bp_capital_low = state.capital_low
  ; bp_crossing = has_crossing_sell
  ; bp_quote_nan = quote_nan
  ; bp_cooldown = is_buy_on_cooldown
  ; bp_inflight = state.inflight_buy
  }
;;

(** Branch action: send the balanced fresh buy. *)
let buy_place_send ~state ~now ~(asset : trading_config) ~qty ~buy_price =
  state.last_buy_attempted_insufficient <- false;
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
    state.last_buy_order_price <- Some buy_price;
    state.tif_recovery_pending <- false;
    state.force_buy_reanchor <- false;
    Logging.info_f
      ~section
      "Placed buy order for %s: %.8f @ %.4f"
      asset.symbol
      qty
      buy_price;
    true)
  else false
;;

(** Branch action: the balance snapshot is stale - attempt anyway (the exchange's verdict
    is the truth), marking the knowingly-underfunded attempt so a rejection does not latch
    [capital_low]. *)
let buy_place_send_insufficient
  ~state
  ~now
  ~(asset : trading_config)
  ~qty
  ~buy_price
  ~quote_needed
  ~available_quote_balance
  =
  Logging.warn_f
    ~section
    "Local balance low for %s buy (need %.2f, available %.2f, balance snapshot stale) - \
     attempting anyway, exchange will reject if truly insufficient"
    asset.symbol
    quote_needed
    available_quote_balance;
  state.last_buy_attempted_insufficient <- true;
  Hashtbl.replace state.amend_cooldowns "place_Buy" (now +. 2.0);
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
    state.last_buy_order_price <- Some buy_price;
    true)
  else false
;;

(** Branch action: fresh balance genuinely cannot fund the buy - latch [capital_low] until
    available quote covers the next buy, and set the retry cooldown. *)
let buy_place_latch_capital_low
  ~state
  ~now
  ~(asset : trading_config)
  ~quote_needed
  ~available_quote_balance
  =
  if not state.capital_low
  then (
    state.capital_low <- true;
    state.capital_low_logged <- true;
    state.capital_low_at_balance <- -1.0;
    Logging.warn_f
      ~section
      "Local balance insufficient for %s buy (need %.2f, available %.2f) - skipping \
       placement until balance recovers"
      asset.symbol
      quote_needed
      available_quote_balance);
  Hashtbl.replace state.amend_cooldowns "place_Buy" (now +. 2.0)
;;

(** Branch action: no quote-balance data available. *)
let buy_place_warn_quote ~(asset : trading_config) =
  Logging.warn_f ~section "No quote balance data available for %s buy order" asset.symbol
;;

(** Branch: no resting buy - place the initial (or resumed) buy. Returns whether an order
    was pushed ([buy_attempted]). Reference recombination of the plan/branch actions. *)
let buy_place_initial
  ~state
  ~now
  ~(asset : trading_config)
  ~bid_price
  ~ask_price
  ~quote_balance
  ~quote_balance_stale
  ~oracle_halted
  ~cycle
  ~locked_in_buys
  ~closest_sell_order_initial
  =
  let p =
    buy_place_plan
      ~state
      ~now
      ~asset
      ~bid_price
      ~ask_price
      ~quote_balance
      ~oracle_halted
      ~cycle
      ~locked_in_buys
      ~closest_sell_order_initial
  in
  let attempted = ref false in
  if p.bp_capital_low
  then
    Logging.debug_f
      ~section
      "Buy placement skipped for %s: capital_low flag is set"
      asset.symbol
  else if p.bp_crossing
  then
    Logging.debug_f
      ~section
      "Buy placement deferred for %s: active or evicted sell order price <= \
       buy_price/bid (wash trade protection)"
      asset.symbol
  else if not p.bp_quote_nan
  then
    if p.bp_cooldown || p.bp_inflight
    then
      Logging.debug_f
        ~section
        "Buy placement skipped for %s (cooldown=%B, inflight=%B)"
        asset.symbol
        p.bp_cooldown
        p.bp_inflight
    else if p.bp_balance_ok
    then
      attempted := buy_place_send ~state ~now ~asset ~qty:p.bp_qty ~buy_price:p.bp_price
    else if not (Hashtbl.mem state.amend_cooldowns "place_Buy")
    then
      if quote_balance_stale
      then
        attempted
        := buy_place_send_insufficient
             ~state
             ~now
             ~asset
             ~qty:p.bp_qty
             ~buy_price:p.bp_price
             ~quote_needed:p.bp_quote_needed
             ~available_quote_balance:p.bp_available
      else
        buy_place_latch_capital_low
          ~state
          ~now
          ~asset
          ~quote_needed:p.bp_quote_needed
          ~available_quote_balance:p.bp_available
    else buy_place_warn_quote ~asset;
  !attempted
;;

(** True when a resting or pending sell is tracked (the 2*gi clamp applies). *)
let buy_amend_has_sell ~state ~closest_sell_order_initial =
  let closest = ref closest_sell_order_initial in
  let update oid price =
    match !closest with
    | None -> closest := Some (oid, price)
    | Some (_, best_p) -> if price < best_p then closest := Some (oid, price)
  in
  List.iter
    (fun (oid, side, price, _) -> if side = Sell then update oid price)
    state.pending_orders;
  !closest <> None
;;

(** Branch: a sell is tracked - trail the buy, or amend it down to clear the sell's 2*gi
    restricted zone. *)
let buy_amend_with_sell
  ~state
  ~now
  ~(asset : trading_config)
  ~bid_price
  ~ask_price
  ~quote_balance
  ~cycle
  ~locked_in_buys
  ~closest_sell_order_initial
  =
  let qty = venue_lot_qty state.grid_qty asset.exchange state in
  let grid_interval = asset.grid_interval in
  let quote_needed = ask_price *. qty in
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
  (match closest_sell_order_val, state.last_buy_order_price, state.last_buy_order_id with
   | Some (_sell_order_id, sell_price), Some current_buy_price, Some buy_order_id ->
     (* The 2*gi separation from the closest sell is anchored on the sell order and is
        price-independent: while a sell is tracked by order management, the buy never
        trails above sell - 2*gi, no matter where the perceived top of book sits. Price
        can dislocate above a resting sell without filling it; the ladder must not let the
        buy cross a sell that still exists. The clamp is released only when the sell is
        removed from tracking (fill/cancel/expiry); only then does the buy trail the top
        of book at the grid interval. [sell_price] is always a positive resting-order
        price, so there is no zero-reference hazard. *)
     let double_grid_interval = sell_price *. (2.0 *. grid_interval /. 100.0) in
     let ref_price = compute_buy_ref_price ~bid_price ~ask_price in
     let grid_buy_from_ref = calculate_grid_price ref_price grid_interval false state in
     let grid_buy_capped =
       if bid_price > 0.0 then min grid_buy_from_ref bid_price else grid_buy_from_ref
     in
     let exact_target = state.cached_round_price (sell_price -. double_grid_interval) in
     let proposed_buy_price = grid_buy_capped in
     let target_buy_price = min proposed_buy_price exact_target in
     let current_buy_price_rounded = state.cached_round_price current_buy_price in
     let min_move_threshold = get_min_move_threshold state.cached_price_increment in
     (* A sizing re-anchor (the capital oracle published a changed grid interval - flagged
        by the domain worker on [force_buy_reanchor]) used to amend the resting buy in
        both directions. A downward amendment is warranted only by a sell-spacing
        violation (see below); a widened grid interval no longer snaps a valid resting buy
        down to the market rung. The ladder spacing is enforced where it matters: fresh
        placements clamp below the closest sell, and this leg corrects real intrusions
        into a sell's restricted zone. A qty-only oracle change does not re-anchor the
        price: the grid adopts the new size (Alpaca qty mismatch amend) or on the next
        placement, and the resting price only trails up. *)
     let reanchor_buy = state.force_buy_reanchor in
     (* Downward movement of the buy is initiated to correct an actual violation of the 2x
        grid_interval restricted zone below the closest sell (above sell - 2*gi). This
        clamp is price-independent and enforces the threshold whenever a resting buy sits
        inside the restricted zone (e.g. after an external upward amendment, book
        dislocation, or grid interval widening). A resting buy already outside the
        restricted zone is never snapped down. *)
     let trail_up = target_buy_price > current_buy_price in
     let zone_violation = (not trail_up) && current_buy_price_rounded > exact_target in
     let should_amend = trail_up || zone_violation in
     (* Release reanchor latch if no action is needed. *)
     if reanchor_buy && not should_amend then state.force_buy_reanchor <- false;
     if should_amend
     then (
       let effective_amend_price =
         if zone_violation then exact_target else target_buy_price
       in
       let effective_price_diff =
         state.cached_round_price
           (abs_float (effective_amend_price -. current_buy_price_rounded))
       in
       let allow =
         amend_allowed
           ~state
           ~order_id:buy_order_id
           ~target_price:effective_amend_price
           ~current_price_rounded:current_buy_price_rounded
           ~price_diff:effective_price_diff
           ~min_move_threshold
       in
       if allow
       then (
         let quote_bal = quote_balance in
         (* An amend replaces the resting buy (cancel+create on Alpaca): the capital
            committed to that buy is released and re-committed at the new price, so the
            affordability check must add the committed notional ([locked_in_buys], sum of
            price*qty over open buys) back to the available balance. Without this the grid
            falsely reports "Insufficient quote balance" when trailing a funded buy up on
            committed capital. *)
         let available_for_amend = quote_bal +. locked_in_buys in
         if (not (Float.is_nan quote_balance))
            && can_place_buy_order qty available_for_amend quote_needed
         then (
           let order =
             create_amend_order
               buy_order_id
               asset.symbol
               Buy
               qty
               (Some effective_amend_price)
               true
               Ladder
               asset.exchange
           in
           ignore (push_order ~now ~state order);
           state.last_buy_order_price <- Some effective_amend_price;
           state.force_buy_reanchor <- false;
           ())
         else if not (Float.is_nan quote_balance)
         then
           Logging.warn_f
             ~section
             "Insufficient quote balance for %s trailing: need %.2f, have %.2f (incl. \
              committed %.2f)"
             asset.symbol
             quote_needed
             available_for_amend
             locked_in_buys
         else Logging.warn_f ~section "No quote balance for %s trailing" asset.symbol
         (* The re-anchor target is already where the buy sits (within the min-move
            threshold): nothing to amend, the sizing is applied. *))
       else if reanchor_buy && effective_price_diff < min_move_threshold
       then state.force_buy_reanchor <- false)
   | _ -> ());
  state.last_cycle <- cycle
;;

(** Branch: no sell tracked - trail-up only (no downward warrant). *)
let buy_amend_no_sell
  ~state
  ~now
  ~(asset : trading_config)
  ~bid_price
  ~ask_price
  ~quote_balance
  ~cycle
  ~locked_in_buys
  ~closest_sell_order_initial:_
  =
  let qty = venue_lot_qty state.grid_qty asset.exchange state in
  let grid_interval = asset.grid_interval in
  let quote_needed = ask_price *. qty in
  (match state.last_buy_order_price, state.last_buy_order_id with
   | Some current_buy_price, Some buy_order_id ->
     let ref_price = compute_buy_ref_price ~bid_price ~ask_price in
     let raw_target = calculate_grid_price ref_price grid_interval false state in
     let target_buy_price =
       if bid_price > 0.0 then min raw_target bid_price else raw_target
     in
     let min_move_threshold = get_min_move_threshold state.cached_price_increment in
     let current_buy_price_rounded = state.cached_round_price current_buy_price in
     (* No resting sell on this symbol, so a downwards amendment has no warrant at all:
        the re-anchor contributes nothing beyond normal trail-up. A resting buy already
        within one grid interval of the reference is left alone. *)
     let reanchor_buy = state.force_buy_reanchor in
     let trail_up = target_buy_price > current_buy_price in
     (* Nothing warranted: release the latch so the sizing counts as adopted without
        moving the book. *)
     if reanchor_buy && not trail_up then state.force_buy_reanchor <- false;
     if trail_up
     then (
       let effective_amend_price = target_buy_price in
       let effective_price_diff =
         state.cached_round_price
           (abs_float (effective_amend_price -. current_buy_price_rounded))
       in
       let allow =
         amend_allowed
           ~state
           ~order_id:buy_order_id
           ~target_price:effective_amend_price
           ~current_price_rounded:current_buy_price_rounded
           ~price_diff:effective_price_diff
           ~min_move_threshold
       in
       if allow
       then (
         let quote_bal = quote_balance in
         (* See the with-sell branch: an amend releases the committed capital of the
            resting buy it replaces, so that committed notional is added back to the
            available balance before the affordability check (fixes the false
            "insufficient quote balance" warning when trailing a funded buy up). *)
         let available_for_amend = quote_bal +. locked_in_buys in
         if (not (Float.is_nan quote_balance))
            && can_place_buy_order qty available_for_amend quote_needed
         then (
           let order =
             create_amend_order
               buy_order_id
               asset.symbol
               Buy
               qty
               (Some effective_amend_price)
               true
               Ladder
               asset.exchange
           in
           ignore (push_order ~now ~state order);
           state.last_buy_order_price <- Some effective_amend_price;
           state.force_buy_reanchor <- false;
           ())
         else if not (Float.is_nan quote_balance)
         then
           Logging.warn_f
             ~section
             "Insufficient quote balance to trail buy: need %.2f, have %.2f (incl. \
              committed %.2f)"
             quote_needed
             available_for_amend
             locked_in_buys
         else Logging.warn_f ~section "No quote balance for buy trailing"
         (* The re-anchor target is already where the buy sits: done. *))
       else if reanchor_buy && effective_price_diff < min_move_threshold
       then state.force_buy_reanchor <- false)
   | _ -> ());
  state.last_cycle <- cycle
;;

(** Branch: exactly one resting buy - trail it up, or amend down to clear a sell's 2*gi
    restricted zone. Reference recombination of the two branches. *)
let buy_amend
  ~state
  ~now
  ~(asset : trading_config)
  ~bid_price
  ~ask_price
  ~quote_balance
  ~cycle
  ~locked_in_buys
  ~closest_sell_order_initial
  =
  if buy_amend_has_sell ~state ~closest_sell_order_initial
  then
    buy_amend_with_sell
      ~state
      ~now
      ~asset
      ~bid_price
      ~ask_price
      ~quote_balance
      ~cycle
      ~locked_in_buys
      ~closest_sell_order_initial
  else
    buy_amend_no_sell
      ~state
      ~now
      ~asset
      ~bid_price
      ~ask_price
      ~quote_balance
      ~cycle
      ~locked_in_buys
      ~closest_sell_order_initial
;;

let evaluate_buy_leg
  ~oracle_halted
  ~state
  ~now
  ~(asset : trading_config)
  ~bid_price
  ~ask_price
  ~quote_balance
  ~quote_balance_stale
  ~cycle
  ~iter_open_orders
  ~open_buy_count_from_scan
  ~has_recent_amend_buy
  ~locked_in_buys
  ~closest_sell_order_initial
  =
  let buy_order_pending, effective_buy_count, should_cancel =
    buy_leg_facts ~state ~open_buy_count_from_scan ~has_recent_amend_buy
  in
  if buy_order_pending
  then false
  else if should_cancel
  then (
    buy_cancel_excess ~state ~now ~asset ~iter_open_orders ~cycle ~effective_buy_count;
    false)
  else if effective_buy_count = 0
  then
    buy_place_initial
      ~state
      ~now
      ~asset
      ~bid_price
      ~ask_price
      ~quote_balance
      ~quote_balance_stale
      ~oracle_halted
      ~cycle
      ~locked_in_buys
      ~closest_sell_order_initial
  else if effective_buy_count > 0
  then (
    buy_amend
      ~state
      ~now
      ~asset
      ~bid_price
      ~ask_price
      ~quote_balance
      ~cycle
      ~locked_in_buys
      ~closest_sell_order_initial;
    false)
  else (
    state.last_cycle <- cycle;
    false)
;;

(** Alpaca excess-inventory sweep.

    The persisted sell-level file is the ladder of record: missing rungs are restored at
    their recorded price/qty first. Only once the ladder is complete (caller gates on no
    missing rungs, no owed sell, nothing in flight) is the remaining sellable base excess.
    Rather than leaving it idle or dumping the whole balance into one order, the excess
    routes to the top of the ladder: the highest-priced rung absorbs it, so surplus is
    offered only at the best price.

    [reserved_base] is never part of the sweep: [available] already excludes it (the
    caller passes the min of the venue free figure and the local ledger headroom
    [position_total - reserved_base - committed_sell_base]).

    The top rung is amended to a larger quantity (qty-only at the same price, so its fill
    anchor is unchanged). The amend's delta is armed into the un-netted-hold overlay so a
    stale venue snapshot cannot authorize the next sweep against base the venue has not
    yet netted. *)
let evaluate_excess_sweep
  ~state
  ~now
  ~(asset : trading_config)
  ~(available : float)
  ~(min_notional : float)
  ~(ecfg : exchange_config)
  =
  match state.persisted_sell_levels with
  | (top_price, _) :: _ when top_price > 0.0 ->
    let min_order_size =
      if state.cached_qty_increment > 0.0 then state.cached_qty_increment else 1e-8
    in
    (* The whole sellable excess routes to the top rung in one amend. There is
       deliberately no per-invocation lot cap: [available] is already the
       reserve-excluded, ledger-headroom-capped sellable base (see the caller), so the
       rung it grows to can never include [reserved_base], and routing it in one pass
       avoids the slow per-cycle ratchet that concentrated the book over many executions. *)
    let excess = Float.max 0.0 available in
    if excess >= min_order_size -. 1e-9
    then (
      let top_open =
        Sell_orders.find_first state.open_sell_orders (fun oid p _q ->
          (not (String.starts_with ~prefix:"pending" oid))
          && (abs_float (p -. top_price) <= top_price *. 0.0001
              || abs_float (p -. top_price) <= 1e-4))
      in
      match top_open with
      | None -> ()
      | Some (oid, top_open_price, top_open_qty) ->
        let target_q = round_qty (top_open_qty +. excess) asset.symbol asset.exchange in
        let delta = target_q -. top_open_qty in
        if delta > 1e-9
           && delta >= min_order_size -. 1e-9
           && (min_notional <= 0.0 || delta *. top_open_price >= min_notional -. 1e-9)
           && (not (InFlightAmendments.is_in_flight oid))
           && (not (Hashtbl.mem state.amend_cooldowns oid))
           && not (has_active_sell state)
        then (
          let order =
            create_amend_order
              oid
              asset.symbol
              Sell
              target_q
              (Some top_open_price)
              true
              Ladder
              asset.exchange
          in
          let pushed = push_order ~now ~state order in
          if pushed && ecfg.use_unnetted_sell_hold && delta > 0.0
          then arm_sell_hold ~state ~qty:delta ~now;
          (* A real qty increase is a meaningful execution event; a rounding-dust sweep
             that leaves the top rung unchanged is internal churn and belongs at DEBUG,
             not in the INFO stream. *)
          (if delta > 1e-9 then Logging.info_f else Logging.debug_f)
            ~section
            "Excess inventory sweep for %s: amended top rung @ %.4f %.8f -> %.8f (+%.8f \
             excess)"
            asset.symbol
            top_open_price
            top_open_qty
            target_q
            delta))
  | _ -> ()
;;

(** Derived sell-leg state shared between the prepare/place/finalize phases.
    ([evaluate_sell_leg] below recombines the three phases, so the reference entry point
    is unchanged. The trigger/latch/sizing contract is documented on its sub-phases.) *)
type sell_pre =
  { sp_is_alpaca : bool
  ; sp_ledger_balance : float
  ; sp_alpaca_available : float
  ; sp_available_base : float
  ; sp_committed_sell : float
  ; sp_reserve_headroom : float
  ; sp_inventory_basis : float
  ; sp_inventory_ok : bool
  ; sp_min_notional : float
  ; sp_base_ref_price : float
  ; sp_capital_exhausted : bool
  ; sp_halt_inventory_check : bool
  ; sp_should_trigger_sell : bool
  ; sp_is_sell_on_cooldown : bool
  ; sp_missing_after_reconcile : (float * float) list ref
  ; sp_sell_pushed : bool ref
  ; sp_nothing_placeable : bool ref
  }

(** Sell leg phase 1: derived sizing/gate facts and the persisted-level reconcile. *)

let sell_leg_prepare
  ~persisted_reconcile
  ~state
  ~now
  ~(asset : trading_config)
  ~bid_price
  ~ask_price
  ~asset_balance
  ~buy_attempted
  ~(oracle_halted : bool)
  ~ecfg
  ~locked_in_sells
  ~base_balance_age
  =
  let is_alpaca = Exchange.Types.exchange_of_string asset.exchange = Alpaca in
  let asset_available = venue_available_base ~asset in
  (* Placed-sell holds the balance feed may not yet be netting: until a balance message
     newer than a placement arrives, the venue's [total - hold] figure still counts that
     base as free, and sizing against it is the reserved_base leak under volatility. *)
  let _sp_ov = Strategy_state.sub_start state in
  let unnetted_hold = unnetted_sell_hold ~state ~ecfg ~now ~base_balance_age in
  (* The base to subtract from the venue's reported holding: the ledger total on
     gross-balance venues, and the ledger's excess over the venue feed (never below the
     unnetted overlay) on net-balance venues. See [effective_committed_sell_base]. *)
  let committed_sell =
    effective_committed_sell_base
      ~ecfg
      ~ledger_total:locked_in_sells
      ~feed_total:state.feed_locked_sell_base
      ~unnetted_hold
  in
  (* Sizing reads the in-memory position ledger, not the raw venue snapshot: the ledger is
     the venue figure plus the timestamp-windowed buy credits the feed has not netted (see
     [unreflected_buy_credit]), so the 1:1 sell is placeable on the fill tick without
     waiting for the balance feed. Fall back to the snapshot when the ledger has not been
     seeded yet (direct callers/tests). *)
  let unreflected_credit = unreflected_buy_credit ~state ~base_balance_age ~now in
  let ledger_balance =
    (if state.position_initialized then state.position_base else asset_balance)
    +. unreflected_credit
  in
  (* Alpaca's sellable base: venue [qty_available] (free of resting holds) plus un-polled
     buy credits, minus the reserve. Moved to Platform_accounting (milestone 2). *)
  let alpaca_available =
    Platform_accounting.alpaca_available_base
      ~venue_available:asset_available
      ~ledger_balance
      ~unreflected_credit
      ~reserved_base:state.reserved_base
      ~committed_sell
      ~unnetted_hold
  in
  let available_base =
    Platform_accounting.available_base
      ~is_venue_authoritative:is_alpaca
      ~asset_balance_nan:(Float.is_nan asset_balance)
      ~venue_available:asset_available
      ~ledger_balance
      ~unreflected_credit
      ~reserved_base:state.reserved_base
      ~committed_sell
      ~unnetted_hold
  in
  (* Total base committed to sells, taking the larger of the in-flight ledger and the live
     open-order list. The ledger is authoritative in the running system (the scan rebuilds
     it each execution); the open-order sum keeps the reserve headroom honest for direct
     callers that populate only [open_sell_orders], and when the ledger transiently
     undercounts a dropped order. Taking the max avoids double-counting the overlap. *)
  let committed_total =
    let open_sum = Sell_orders.sum_qty state.open_sell_orders in
    Float.max (committed_sell_base state) open_sum
  in
  (* Reserve-bounded headroom from the local ledger: base we hold that is not already
     committed to a resting/in-flight sell. Independent of the venue snapshot, so a stale
     [qty_available] cannot authorize committing [reserved_base]. *)
  let reserve_headroom =
    Float.max 0.0 (ledger_balance -. state.reserved_base -. committed_total)
  in
  Strategy_state.sub_stop state Strategy_state.Splan_overlays _sp_ov;
  (* The persisted-sell grid is reconciled once per execution and the result is reused by
     the three persisted-sell branches below. After the pruning below rebuilds
     [persisted_sell_levels] to [open_levels @ kept_missing], a re-partition against the
     same open orders yields exactly [kept_missing] as the missing set, so later branches
     reuse it.

     [sync_open_orders] already computed this exact (open_levels, missing_levels) split
     during its scan (each open sell consumes one persisted level), so the missing set
     falls out in O(m). Only direct [evaluate_sell_leg] callers (tests) fall back to the
     partition. *)
  let missing_after_reconcile = ref [] in
  let pruned_missing = ref [] in
  let _sp_rec = Strategy_state.sub_start state in
  if ecfg.remaintain_expired_sells && state.persisted_sell_levels <> []
  then (
    let open_levels, missing_levels = persisted_reconcile in
    if not (Float.is_nan asset_balance)
    then (
      (* Fundable base comes from the LOCAL ledger, not the venue's [qty_available]:
         [position_total - reserved_base - committed_total] is base we hold that is not
         already committed to a resting/in-flight sell. The venue figure lags hold
         reconstruction (and freezes during REST poll timeouts), so sizing a restore
         against it can re-commit [reserved_base] that is already sitting inside a resting
         order. After a manual cancel the order leaves the committed set but its base
         stays in [position_total], so the headroom still funds the rung - by exactly
         [target_q - reserved_base]. *)
      let available_for_missing_sells =
        if is_alpaca then reserve_headroom else Float.max 0.0 available_base
      in
      let missing_desc =
        List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) missing_levels
      in
      let rem_avail = ref available_for_missing_sells in
      let kept_missing = ref [] in
      let pruned = ref [] in
      List.iter
        (fun ((target_p, target_q) as level) ->
          let min_order_size =
            if state.cached_qty_increment > 0.0 then state.cached_qty_increment else 1e-8
          in
          if target_q > 0.0
             && target_q >= min_order_size -. 1e-9
             && !rem_avail >= target_q -. 1e-6
          then (
            kept_missing := level :: !kept_missing;
            rem_avail := max 0.0 (!rem_avail -. target_q))
          else if (* Under-funded but still fundable: re-place the rung for the available
                     remainder instead of pruning it. Headroom is the local ledger's
                     [position - reserved_base - committed], so an offline FILL (base
                     actually gone) leaves ~no headroom and still prunes, while a
                     cancelled or partially-filled rung restores the remaining sellable
                     base - including when the only shortfall was [reserved_base]. *)
                  is_alpaca
                  && target_q >= min_order_size -. 1e-9
                  && !rem_avail >= min_order_size -. 1e-9
          then (
            kept_missing := (target_p, !rem_avail) :: !kept_missing;
            rem_avail := 0.0)
          else pruned := level :: !pruned)
        missing_desc;
      let new_persisted = open_levels @ List.rev !kept_missing in
      state.persisted_sell_levels <- dedupe_persisted_sell_levels new_persisted;
      (* After the rebuild the missing set is exactly the fundable [kept_missing] subset
         (descending, as [missing_desc] was). *)
      missing_after_reconcile := List.rev !kept_missing;
      pruned_missing := !pruned)
    else
      (* Balance unknown: persisted list is left untouched, so the later branches
         reconcile against it unchanged and see the full missing set from this single
         partition. *)
      missing_after_reconcile := missing_levels);
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
      !pruned_missing);
  Strategy_state.sub_stop state Strategy_state.Splan_reconcile _sp_rec;
  (* Balance basis per venue: [locked_in_sells] (the in-flight sell ledger total) is
     subtracted on EVERY venue so base committed to a resting or in-flight sell is never
     considered free, regardless of whether the venue's reported figure nets open-order
     holds. The venue difference is only the spot figure: accumulation venues use
     [ledger_balance], Alpaca uses its venue-authoritative [qty_available]. This MUST be
     the same basis the sizing branch below uses ([available]), or the inventory gate and
     the sizing disagree. *)
  let is_accumulation_basis = ecfg.use_accumulation_sells in
  let inventory_basis =
    if Float.is_nan asset_balance
    then 0.0
    else if is_accumulation_basis
    then ledger_balance -. state.reserved_base -. committed_sell
    else available_base
  in
  (* Inventory gate for sell placement: available non-accrued inventory must cover the
     VENUE MINIMUM accepted order size. The venue minimum is the exchange's floor -
     entirely separate from the grid's configured order [qty]. Venues express it two ways:
     - quote-notional venues (Alpaca) enforce a DOLLAR minimum order value
       ([cached_venue_min_notional] = $1): the available base must be worth at least that
       in the quote currency, so the comparison is in VALUE.
     - base-quantity venues enforce a base-amount floor ([cached_venue_min_qty]). *)
  let base_ref_price =
    if bid_price > 0.0
    then bid_price
    else (
      match state.last_buy_fill_price with
      | Some p when p > 0.0 -> p
      | Some _ -> ask_price
      | None -> ask_price)
  in
  let min_notional =
    if state.cached_venue_min_notional > 0.0
    then state.cached_venue_min_notional
    else if is_alpaca
    then 1.0
    else 0.0
  in
  let inventory_ok =
    inventory_basis > 0.0
    &&
    if is_alpaca
    then inventory_basis *. base_ref_price >= min_notional -. 1e-9
    else inventory_basis >= state.cached_venue_min_qty -. 1e-9
  in
  let missing_alpaca_sell_grid =
    if ecfg.remaintain_expired_sells
    then (
      let missing_lvl_check = !missing_after_reconcile in
      (not (has_active_sell state))
      && inventory_ok
      && (state.just_filled_buy
          || buy_attempted
          || state.resuming_after_balance_flag
          || missing_lvl_check <> []
          || (Sell_orders.is_empty state.open_sell_orders
              && Option.is_some state.last_buy_fill_price)))
    else false
  in
  (* Oracle-inactive: buys are halted but the sell leg always runs (sells need inventory,
     not quote). When the asset is first placed inactive, check whether placeable
     inventory exists and attempt it - this is what lets a startup with held base, or a
     cascading buy execution that consumed all quote, resume trading as quickly as
     possible. An existing resting sell does NOT suppress the check: free inventory
     ladders a SECOND sell alongside it, and the sizing clamps to what is still tradeable
     once that hold nets out of the balance. The block guards below (NaN balance,
     cooldown, retry latch) keep this from spamming; after placement the hold leaves the
     tradeable figure, so the next tick naturally finds nothing left to sell. *)
  (* Capital exhaustion: either the oracle has published an INACTIVE decision for this
     asset, or the buy leg latched [capital_low] locally because the available quote no
     longer covers the next buy (its balance was fresh, so it skipped a guaranteed-reject
     placement). Both mean "no more quote to commit" and must still place the bottom-rung
     sell for the inventory we already own, on EVERY venue - otherwise a buy fill that
     consumed the last quote leaves the filled base unsold and the strategy paused longer
     than necessary (and over-accumulating). Placement is inventory-gated and clamped to
     non-reserved base, so it can never dip into reserved_base. *)
  let capital_exhausted = oracle_halted || state.capital_low in
  let halt_inventory_check = capital_exhausted && inventory_ok in
  let should_trigger_sell =
    if ecfg.remaintain_expired_sells
    then missing_alpaca_sell_grid || halt_inventory_check
    else state.just_filled_buy || buy_attempted || halt_inventory_check
  in
  let is_sell_on_cooldown = Hashtbl.mem state.amend_cooldowns "place_Sell" in
  (* Surface every path that ends a triggered sell leg without placing, so sell placement
     cannot stay silently wedged (dust persisted levels, wiped position snapshots) while
     buys keep flowing.

     Not surfaced: the verified nothing-to-sell resting state (remaintain trigger armed,
     fresh balance below the venue floor). That state is not a wedge - the owed sell
     cannot place until inventory recovers, and the recovery paths (buy-fill event, the
     persistent grid-maintenance clause) re-trigger on their own - so the latch is
     consumed at the end of the leg with a one-time info line instead of a per-tick warn.
     A per-tick warn interpolating the live ref price would defeat the dedup window and
     spam the log for the life of a dust balance. *)
  let skip_reason =
    if should_trigger_sell
    then
      if Float.is_nan asset_balance
      then Some "asset balance snapshot unavailable (NaN) - inventory not evaluable"
      else if has_active_sell state
      then Some "a sell placement is in flight (has_active_sell)"
      else if state.asset_low
      then Some "asset_low flag latched"
      else if is_sell_on_cooldown
      then Some "sell placement cooldown active"
      else None
    else if Float.is_nan asset_balance
    then Some "asset balance snapshot unavailable (NaN) - inventory not evaluable"
    else None
  in
  (match skip_reason with
   | Some reason -> log_sell_block ~state ~now ~symbol:asset.symbol (fun () -> reason)
   | None -> ());
  let sell_pushed = ref false in
  let nothing_placeable = ref false in
  { sp_is_alpaca = is_alpaca
  ; sp_ledger_balance = ledger_balance
  ; sp_alpaca_available = alpaca_available
  ; sp_available_base = available_base
  ; sp_committed_sell = committed_sell
  ; sp_reserve_headroom = reserve_headroom
  ; sp_inventory_basis = inventory_basis
  ; sp_inventory_ok = inventory_ok
  ; sp_min_notional = min_notional
  ; sp_base_ref_price = base_ref_price
  ; sp_capital_exhausted = capital_exhausted
  ; sp_halt_inventory_check = halt_inventory_check
  ; sp_should_trigger_sell = should_trigger_sell
  ; sp_is_sell_on_cooldown = is_sell_on_cooldown
  ; sp_missing_after_reconcile = missing_after_reconcile
  ; sp_sell_pushed = sell_pushed
  ; sp_nothing_placeable = nothing_placeable
  }
;;

(** True when a fresh sell is placeable this cycle. *)
let sell_place_should ~state ~asset_balance ~pre =
  pre.sp_should_trigger_sell
  && (not (Float.is_nan asset_balance))
  && (not (has_active_sell state))
  && (not state.asset_low)
  && not pre.sp_is_sell_on_cooldown
;;

(** Branch: place the owed or restored sell. *)
let sell_place_body
  ~buy_attempted
  ~state
  ~now
  ~(asset : trading_config)
  ~bid_price
  ~ask_price
  ~ecfg
  ~pre
  =
  let is_alpaca = pre.sp_is_alpaca in
  let alpaca_available = pre.sp_alpaca_available in
  let ledger_balance = pre.sp_ledger_balance in
  let committed_sell = pre.sp_committed_sell in
  let capital_exhausted = pre.sp_capital_exhausted in
  let halt_inventory_check = pre.sp_halt_inventory_check in
  let missing_after_reconcile = pre.sp_missing_after_reconcile in
  let sell_pushed = pre.sp_sell_pushed in
  let nothing_placeable = pre.sp_nothing_placeable in
  let asset_bal = ledger_balance in
  let qty =
    match state.last_buy_fill_qty with
    | Some q when q > 0.0 -> q
    | _ -> venue_lot_qty state.grid_qty asset.exchange state
  in
  (* Sell sizing ownership. An owed sell (a buy fill's 1:1 sell, a buy placement
     companion, inventory recovery after a balance/oracle event, the uncommitted-inventory
     fallback) is strategy-sized on every venue: price anchored on the last buy fill +
     grid interval, qty 1:1 with the fill, clamped to sellable inventory. The persisted
     level file never dictates the price or sizing of a new sell. Its only role is
     restoration: re-placing a rung the venue dropped (Alpaca fractional orders are forced
     to day TIF, so resting rungs die at the session boundary) at its recorded price/qty.
     Restoration is selected only when no new sell is owed, so the two obligations cannot
     hijack each other; a restoration level below the venue's real minimum is pruned at
     the venue-minimum gate below instead of wedging the maintenance path. *)
  let new_sell_owed =
    state.just_filled_buy
    || buy_attempted
    || state.resuming_after_balance_flag
    || halt_inventory_check
    || (Sell_orders.is_empty state.open_sell_orders
        && Option.is_some state.last_buy_fill_price)
  in
  let target_sell_price_opt, target_sell_qty_override =
    if ecfg.remaintain_expired_sells
       && (not new_sell_owed)
       && state.persisted_sell_levels <> []
    then (
      let missing_sorted_desc =
        List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) !missing_after_reconcile
      in
      match missing_sorted_desc with
      | (tp, tq) :: _ when tq > 0.0 ->
        (* A missing rung is restored at its OWN recorded price/qty. Excess inventory is
           never folded into a restore: it is swept to the top of the ladder only once
           every rung is back (see [evaluate_excess_sweep]), so a restore can never
           resurrect a dust level or balloon a rung into the whole balance. *)
        Some tp, Some tq
      | _ -> None, None)
    else None, None
  in
  let sell_price =
    match target_sell_price_opt with
    | Some tp -> tp
    | None ->
      (* The owed sell price (fill/bid anchor + gi, lifted to the ask on non-Alpaca
         venues) lives in [owed_sell_price] so the fresh-buy leg can anticipate the exact
         same price when it clamps in this tick. Re-anchoring to the bid (rather than the
         drifted fill) is decided there, including the capital-exhaustion exception that
         keeps the recovery rung at fill + gi. *)
      owed_sell_price ~state ~asset ~ecfg ~bid_price ~ask_price ~capital_exhausted
  in
  (* Sell quantity: every filled buy owes exactly one 1:1 sell of what it bought (the
     persisted replacement level restores its own qty). All sizing beyond this is
     inventory clamping below - no profit gating, no sell_mult sizing. *)
  let sell_qty =
    match target_sell_qty_override with
    | Some tq -> tq
    | None -> qty
  in
  (* Non-accrued, uncommitted sellable inventory. Base committed to a resting or in-flight
     sell is subtracted on EVERY venue via [locked_in_sells] (the in-flight sell ledger),
     so it can never be offered again - not even when the venue reports a gross figure or
     its open-order feed momentarily drops a live order. Alpaca uses the venue's own
     [qty_available], which is already free of resting holds. *)
  let is_accumulation = ecfg.use_accumulation_sells in
  let available =
    if is_accumulation
    then Float.max 0.0 (asset_bal -. state.reserved_base -. committed_sell)
    else if is_alpaca
    then alpaca_available
    else Float.max 0.0 (asset_bal -. state.reserved_base -. committed_sell)
  in
  let min_order_size =
    if state.cached_qty_increment > 0.0 then state.cached_qty_increment else 1e-8
  in
  let target_q = round_qty sell_qty asset.symbol asset.exchange in
  let effective_sell_qty, balance_ok =
    if ecfg.use_reserved_base_guard
    then (
      let rounded_avail = round_qty available asset.symbol asset.exchange in
      if available >= target_q -. 1e-6 && target_q > 0.0
      then
        if (not is_alpaca)
           && target_sell_qty_override = None
           && (not ecfg.remaintain_expired_sells)
           && rounded_avail >= target_q
           && rounded_avail >= min_order_size -. 1e-9
        then (
          Logging.debug_f
            ~section
            "Sell order sized for %s: target_q %.8f + non-reserved surplus (available \
             %.8f) -> sized to %.8f (min_order_size %.8f)"
            asset.symbol
            target_q
            available
            rounded_avail
            min_order_size;
          rounded_avail, true)
        else if target_q >= min_order_size -. 1e-9
        then target_q, true
        else (
          log_sell_block
            ~state
            ~now
            ~symbol:asset.symbol
            ~kind:"sell qty below min_order_size"
            (fun () ->
               Printf.sprintf
                 "sell qty %.8f below min_order_size %.8f"
                 target_q
                 min_order_size);
          0.0, false)
      else if available >= min_order_size -. 1e-9
      then
        if rounded_avail >= min_order_size -. 1e-9 && rounded_avail > 0.0
        then (
          Logging.debug_f
            ~section
            "Sell order clamped for %s: available %.8f (bal %.8f - reserved %.8f) < \
             target_q %.8f -> clamped to %.8f (min_order_size %.8f)"
            asset.symbol
            available
            asset_bal
            state.reserved_base
            target_q
            rounded_avail
            min_order_size;
          rounded_avail, true)
        else (
          log_sell_block
            ~state
            ~now
            ~symbol:asset.symbol
            ~kind:"available below min_order_size (rounded)"
            (fun () ->
               Printf.sprintf
                 "available %.8f (bal %.8f - reserved %.8f) rounds below min_order_size \
                  %.8f"
                 available
                 asset_bal
                 state.reserved_base
                 min_order_size);
          0.0, false)
      else (
        log_sell_block
          ~state
          ~now
          ~symbol:asset.symbol
          ~kind:"available below min_order_size"
          (fun () ->
             Printf.sprintf
               "available %.8f (bal %.8f - reserved %.8f) is below min_order_size %.8f"
               available
               asset_bal
               state.reserved_base
               min_order_size);
        0.0, false))
    else if target_q >= min_order_size -. 1e-9 && target_q > 0.0
    then target_q, true
    else (
      log_sell_block
        ~state
        ~now
        ~symbol:asset.symbol
        ~kind:"sell qty below min_order_size"
        (fun () ->
           Printf.sprintf
             "sell qty %.8f below min_order_size %.8f"
             target_q
             min_order_size);
      0.0, false)
  in
  if balance_ok
  then (
    (* The NOTIONAL gate: enforce both venue minimum quantity / increment floor and venue
       quote-notional minimum floor. *)
    let min_notional =
      if state.cached_venue_min_notional > 0.0
      then state.cached_venue_min_notional
      else if is_alpaca
      then 1.0
      else 0.0
    in
    let venue_min_ok q =
      q > 0.0
      && q >= min_order_size -. 1e-9
      && (min_notional <= 0.0 || q *. sell_price >= min_notional -. 1e-9)
    in
    if venue_min_ok effective_sell_qty
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
        sell_pushed := true;
        state.asset_low <- false;
        state.last_sell_block_reason <- "";
        (* Arm the unnetted-hold guard for venues that opt in: until a value-changing
           balance adoption or a tradeable drop nets it, the venue's [total - hold] figure
           may still count this base as free, and sizing against it is the reserved_base
           leak under bursts. *)
        if ecfg.use_unnetted_sell_hold && effective_sell_qty > 0.0
        then (
          arm_sell_hold ~state ~qty:effective_sell_qty ~now;
          Logging.debug_f
            ~section
            "Sell hold guard armed for %s: +%.8f (release on next balance message or \
             %.0fs grace)"
            asset.symbol
            effective_sell_qty
            sell_hold_netting_grace_s);
        if ecfg.remaintain_expired_sells
           && target_sell_price_opt = None
           && effective_sell_qty > 0.0
        then (
          state.persisted_sell_levels
          <- dedupe_persisted_sell_levels
               (state.persisted_sell_levels @ [ sell_price, effective_sell_qty ]);
          state.persistence_dirty <- true);
        Logging.info_f
          ~section
          "Placed sell order for %s: %.8f @ %.4f"
          asset.symbol
          effective_sell_qty
          sell_price)
      else
        log_sell_block ~state ~now ~symbol:asset.symbol (fun () ->
          "order dispatch rejected (duplicate in-flight placement key)"))
    else (
      (match target_sell_qty_override, target_sell_price_opt with
       | Some _, Some tp ->
         (* A restoration level below the venue's real minimum can never be placed -
            notional floors are static per level, so retrying is a permanent wedge (legacy
            dust from clamped sizing). Prune it so the file keeps being a faithful record
            of what CAN be on the system. *)
         let rec remove_one acc found = function
           | [] -> List.rev acc
           | (sp, _sq) :: rest
             when (not found)
                  && (abs_float (sp -. tp) <= tp *. 0.0001 || abs_float (sp -. tp) <= 1e-4)
             -> remove_one acc true rest
           | item :: rest -> remove_one (item :: acc) found rest
         in
         let new_levels = remove_one [] false state.persisted_sell_levels in
         if new_levels <> state.persisted_sell_levels
         then (
           state.persisted_sell_levels <- new_levels;
           state.persistence_dirty <- true);
         log_sell_block ~state ~now ~symbol:asset.symbol (fun () ->
           Printf.sprintf
             "persisted sell level %.4f x %.8f below venue minimum $%.2f - pruned from \
              sell_levels_state.json (unplaceable)"
             sell_price
             effective_sell_qty
             min_notional)
       | _ ->
         log_sell_block
           ~state
           ~now
           ~symbol:asset.symbol
           ~kind:"sellable inventory below the quote-notional minimum"
           (fun () ->
              Printf.sprintf
                "sellable inventory below the quote-notional minimum (venue min $%.2f, \
                 sell_price %.4f, sell qty %.8f)"
                min_notional
                sell_price
                effective_sell_qty));
      nothing_placeable := true))
  else nothing_placeable := true
;;

(** Sell leg phase 2: place the owed or restored sell. Reference recombination. *)
let sell_leg_place
  ~state
  ~now
  ~(asset : trading_config)
  ~bid_price
  ~ask_price
  ~asset_balance
  ~buy_attempted
  ~ecfg
  ~pre
  =
  if sell_place_should ~state ~asset_balance ~pre
  then sell_place_body ~state ~now ~asset ~bid_price ~ask_price ~buy_attempted ~ecfg ~pre
;;

(** Sell finalize phase 1: retry-latch / nothing-to-sell bookkeeping. *)
let sell_leg_finalize_latch
  ~state
  ~now:_
  ~(asset : trading_config)
  ~asset_balance
  ~buy_attempted
  ~ecfg
  ~pre
  =
  let inventory_ok = pre.sp_inventory_ok in
  let inventory_basis = pre.sp_inventory_basis in
  let min_notional = pre.sp_min_notional in
  let base_ref_price = pre.sp_base_ref_price in
  let sell_pushed = pre.sp_sell_pushed in
  let nothing_placeable = pre.sp_nothing_placeable in
  if !sell_pushed || !nothing_placeable
  then state.just_filled_buy <- false
  else if buy_attempted && not state.just_filled_buy
  then (
    state.just_filled_buy <- true;
    state.last_sell_block_reason <- "");
  if ecfg.remaintain_expired_sells
     && state.just_filled_buy
     && (not (Float.is_nan asset_balance))
     && not inventory_ok
  then (
    state.just_filled_buy <- false;
    Logging.info_f
      ~section
      "Sell trigger consumed for %s: sellable %.8f below venue minimum %.4f notional at \
       ref price %.4f (verified nothing-to-sell - a later buy fill re-arms it)"
      asset.symbol
      (Float.max 0.0 inventory_basis)
      min_notional
      base_ref_price)
;;

(** Sell finalize phase 2: Alpaca excess-inventory sweep onto the top rung. The file gates
    when this runs; this only performs the sweep. *)
let sell_excess_sweep_phase ~state ~now ~(asset : trading_config) ~pre ~ecfg =
  let is_alpaca = pre.sp_is_alpaca in
  let min_notional = pre.sp_min_notional in
  let available_base = pre.sp_available_base in
  let reserve_headroom = pre.sp_reserve_headroom in
  let sweep_available =
    let venue = Float.max 0.0 available_base in
    if is_alpaca then Float.min venue reserve_headroom else venue
  in
  evaluate_excess_sweep ~state ~now ~asset ~available:sweep_available ~min_notional ~ecfg
;;

(** Sell finalize phase 3: clear the one-cycle resume marker. *)
let sell_finalize_end ~state = state.resuming_after_balance_flag <- false

(** Sell finalize: retry-latch bookkeeping, excess sweep, resume-marker clear. Reference
    recombination. *)
let sell_leg_finalize
  ~state
  ~now
  ~(asset : trading_config)
  ~asset_balance
  ~buy_attempted
  ~ecfg
  ~base_balance_age
  ~pre
  =
  sell_leg_finalize_latch ~state ~now ~asset ~asset_balance ~buy_attempted ~ecfg ~pre;
  let missing_after_reconcile = pre.sp_missing_after_reconcile in
  let sell_pushed = pre.sp_sell_pushed in
  if ecfg.remaintain_expired_sells
     && !missing_after_reconcile = []
     && (not state.just_filled_buy)
     && (not state.resuming_after_balance_flag)
     && (not buy_attempted)
     && (not !sell_pushed)
     && (not (has_active_sell state))
     && (not (Float.is_nan asset_balance))
     &&
     match base_balance_age with
     | Some age -> age <= sweep_max_balance_age_s
     | None -> true
  then sell_excess_sweep_phase ~state ~now ~asset ~pre ~ecfg;
  sell_finalize_end ~state
;;

let evaluate_sell_leg
  ~persisted_reconcile
  ~state
  ~now
  ~(asset : trading_config)
  ~bid_price
  ~ask_price
  ~asset_balance
  ~buy_attempted
  ~(oracle_halted : bool)
  ~ecfg
  ~locked_in_sells
  ~base_balance_age
  =
  let pre =
    sell_leg_prepare
      ~persisted_reconcile
      ~state
      ~now
      ~asset
      ~bid_price
      ~ask_price
      ~asset_balance
      ~buy_attempted
      ~oracle_halted
      ~ecfg
      ~locked_in_sells
      ~base_balance_age
  in
  sell_leg_place
    ~state
    ~now
    ~asset
    ~bid_price
    ~ask_price
    ~asset_balance
    ~buy_attempted
    ~ecfg
    ~pre;
  sell_leg_finalize
    ~state
    ~now
    ~asset
    ~asset_balance
    ~buy_attempted
    ~ecfg
    ~base_balance_age
    ~pre
;;

(** Main strategy execution loop. [quote_balance_stale] is set by the caller (domain
    worker) when the quote-balance snapshot is older than the staleness threshold: a stale
    snapshot is not authoritative, so an under-funded buy is still attempted (the
    exchange's verdict is the truth); a fresh snapshot that cannot fund the buy is skipped
    instead of sent to be rejected.

    [oracle_halted] (the capital oracle published this asset INACTIVE) gates only the buy
    leg - no new buy placement, no buy trailing/amending. The sell leg always runs: a sell
    needs only inventory, not quote, so the sell for a just-filled buy is placed even when
    capital is exhausted and the asset is halted - the account's capital-recovery path.

    Exception - TIF recovery ([state.tif_recovery_pending]): a TIF/ALO/ post-only reject
    or transient placement failure that killed a previously-approved resting buy arms a
    recovery window during which the buy leg re-attempts through the halt. The window
    expires 900s after the last armed kill - each failed re-attempt re-arms and refreshes
    it by design, so recovery keeps re-attempting (2s cooldown) while the venue rejects,
    and the latch decays once kill events stop. Without it, the halt's "no open buy" rule
    turns a transient reject into an indefinite buyless gap. Re-placing the
    already-approved buy at the fresh price restores prior commitment (safer after a price
    drop), not new sizing: the halt still blocks new commitments and capital_low still
    gates every attempt. *)
let execute_strategy
  ?cached_state
  ?(quote_balance_stale = false)
  ?(oracle_halted = false)
  ?(get_open_orders_generation = fun () -> -1)
  ?(drain_open_order_changes = fun ~symbol:_ -> [], true)
  ~base_balance_age
  ~now
  (asset : trading_config)
  (current_price : float)
  (top_bid : float)
  (top_ask : float)
  (asset_balance : float)
  (quote_balance : float)
  (_open_buy_count : int)
  (_open_sell_count : int)
  (iter_open_orders : (string -> float -> float -> string -> int option -> unit) -> unit)
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
    (* Register the full persistence store key and the per-strategy opt-in flags now that
       strategy name + venue are known. *)
    state.persistence_key
    <- Some
         (Dio_persistence.Base_accumulation_store.key_of
            ~strategy:asset.strategy
            ~symbol:asset.symbol
            ~venue:asset.exchange);
    state.base_accumulation_enabled <- asset.base_accumulation;
    state.sell_levels_enabled <- asset.sell_levels_persistence;
    state.cached_ecfg <- get_exchange_config asset.exchange;
    state.cached_round_price <- get_round_price_fn asset.symbol asset.exchange;
    state.cached_price_increment <- get_price_increment asset.symbol asset.exchange;
    state.cached_qty_increment <- get_qty_increment_val asset.symbol asset.exchange;
    (* Venue minimums (the exchange's minimum accepted order size), resolved once at init:
       the base-quantity floor [cached_venue_min_qty] and the quote-notional floor
       [cached_venue_min_notional]. These are the floors every order must clear - separate
       from the grid's configured [qty]. *)
    state.cached_venue_min_qty
    <- (match get_exchange_module asset.exchange with
        | Some (module Ex : Exchange.S) ->
          Option.value (Ex.get_qty_min ~symbol:asset.symbol) ~default:1.0
        | None -> 1.0);
    state.cached_venue_min_notional <- get_min_notional_val asset.symbol asset.exchange;
    state.exchange_reserved_atomic <- Some (get_exchange_reserved_atomic asset.exchange));
  if state.cached_venue_min_notional <= 0.0
  then state.cached_venue_min_notional <- get_min_notional_val asset.symbol asset.exchange;
  let ecfg = state.cached_ecfg in
  (* Realtime accumulation buffer (fear-and-greed resolved upstream); refreshed every
     cycle so fill-time reserve decisions see the latest value. *)
  state.accumulation_buffer <- asset.accumulation_buffer;
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
      (* Reset phase attribution so a short-circuit return (NaN price, stale balance)
         reports zeros rather than the previous execution's values. *)
      state.time_preamble_ns <- 0;
      state.time_cleanup_ns <- 0;
      state.time_sync_ns <- 0;
      state.time_sync_scan_ns <- 0;
      state.time_sync_rec_ns <- 0;
      state.sync_orders_seen <- 0;
      state.time_buy_ns <- 0;
      state.time_sell_ns <- 0;
      let t_preamble_start = Monotonic_clock.now_ns () in
      let lot_qty = venue_lot_qty state.grid_qty asset.exchange state in
      let unnetted_hold = unnetted_sell_hold ~state ~ecfg ~now ~base_balance_age in
      evaluate_asset_low_recovery
        ~state
        ~now
        ~base_balance_age
        ~ecfg
        ~asset
        ~asset_balance
        ~lot_qty
        ~unnetted_hold;
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
          if (not (Float.is_nan top_bid))
             && top_bid > 0.0
             && (not (Float.is_nan top_ask))
             && top_ask > 0.0
          then top_bid, top_ask
          else current_price, current_price
        in
        (* Per-sub-phase attribution: minor-word allocation (cheap [Gc.minor_words] reads)
           and wall time ([Monotonic_clock], a 0-alloc immediate-int C stub). Read by the
           domain on its max cycle to localize where a wide-grid STRAT burst is produced. *)
        state.time_preamble_ns <- Monotonic_clock.now_ns () - t_preamble_start;
        let a_cleanup_start = Gc.minor_words () in
        let t_cleanup_start = Monotonic_clock.now_ns () in
        cleanup_pending_and_cooldowns ~state ~now ~asset;
        state.alloc_cleanup_words <- int_of_float (Gc.minor_words () -. a_cleanup_start);
        state.time_cleanup_ns <- Monotonic_clock.now_ns () - t_cleanup_start;
        let a_sync_start = Gc.minor_words () in
        let t_sync_start = Monotonic_clock.now_ns () in
        let ( open_buy_count_from_scan
            , has_recent_amend_buy
            , locked_in_buys
            , locked_in_sells
            , closest_sell_order
            , open_persisted_levels
            , missing_persisted_levels )
          =
          sync_open_orders
            ~state
            ~now
            ~asset
            ~bid_price
            ~lot_qty
            ~iter_open_orders
            ~get_open_orders_generation
            ~drain_open_order_changes
            ~ecfg
        in
        state.alloc_sync_words <- int_of_float (Gc.minor_words () -. a_sync_start);
        state.time_sync_ns <- Monotonic_clock.now_ns () - t_sync_start;
        if state.maker_fee <= 0.0 || cycle land 0x3ff = 0
        then
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
          (* Oracle-halted: no buy placement, no buy trailing/amending - a halted asset
             must not commit more quote capital. The sell leg still runs (see the
             [oracle_halted] doc on execute_strategy). Exception - TIF recovery: a
             TIF/ALO/post-only reject (or a transient placement failure) killed a
             previously-approved resting buy. Re-attempting it at the fresh price is not a
             new capital commitment (after a price drop it improves survival margin), and
             without it the asset sits buyless for the entire oracle-inactive window. The
             latch expires (900s) so it cannot pin buys through a genuine, persistent
             capital halt. *)
          let recovery_expired =
            state.tif_recovery_pending && now -. state.tif_recovery_since >= 900.0
          in
          if recovery_expired
          then (
            state.tif_recovery_pending <- false;
            Logging.info_f
              ~section
              "TIF recovery window expired for %s - resuming normal oracle-gated buying"
              asset.symbol);
          let tif_recovery_active =
            state.tif_recovery_pending && now -. state.tif_recovery_since < 900.0
          in
          let a_buy_start = Gc.minor_words () in
          let t_buy_start = Monotonic_clock.now_ns () in
          let buy_attempted =
            if oracle_halted && not tif_recovery_active
            then false
            else
              evaluate_buy_leg
                ~oracle_halted
                ~state
                ~now
                ~asset
                ~bid_price
                ~ask_price
                ~quote_balance
                ~quote_balance_stale
                ~cycle
                ~iter_open_orders
                ~open_buy_count_from_scan
                ~has_recent_amend_buy
                ~locked_in_buys
                ~closest_sell_order_initial:closest_sell_order
          in
          state.alloc_buy_words <- int_of_float (Gc.minor_words () -. a_buy_start);
          state.time_buy_ns <- Monotonic_clock.now_ns () - t_buy_start;
          let a_sell_start = Gc.minor_words () in
          let t_sell_start = Monotonic_clock.now_ns () in
          evaluate_sell_leg
            ~persisted_reconcile:(open_persisted_levels, missing_persisted_levels)
            ~state
            ~now
            ~asset
            ~bid_price
            ~ask_price
            ~asset_balance
            ~buy_attempted
            ~oracle_halted
            ~ecfg
            ~locked_in_sells
            ~base_balance_age;
          state.alloc_sell_words <- int_of_float (Gc.minor_words () -. a_sell_start);
          state.time_sell_ns <- Monotonic_clock.now_ns () - t_sell_start)))
;;
