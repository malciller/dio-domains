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
  Logging.info_f
    ~section
    "Found %d buy orders for %s, cancelling all buy orders to maintain single buy order \
     policy"
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
      (* An amend on Alpaca is cancel+create under the hood: while it is in flight the
         open-order scan transiently lists both the old id and the replacement, which
         trips the ">1 buys" branch below. Cancelling the old id then races the amend (the
         cancel is ignored or bounced), so skip orders that are mid-amendment - the amend
         replaces them. *)
      let is_mid_amend =
        InFlightAmendments.is_in_flight order_id
        || List.exists
             (fun (id, _, _, _) ->
               String.starts_with ~prefix:"pending_amend_" id
               && String.length id > 14
               && String.sub id 14 (String.length id - 14) = order_id)
             state.pending_orders
      in
      if is_mid_amend
      then
        Logging.info_f
          ~section
          "Skipping cancel of mid-amendment buy order %s for %s (amend will replace it)"
          order_id
          asset.symbol
      else (
        let cancel_order =
          create_cancel_order order_id asset.symbol Ladder asset.exchange
        in
        ignore (push_order ~now ~state cancel_order);
        Logging.info_f
          ~section
          "Cancelling excess buy order: %s for %s"
          order_id
          asset.symbol)));
  state.last_buy_order_id <- None;
  state.last_buy_order_price <- None;
  state.last_cycle <- cycle
;;

(** Branch: no resting buy - place the initial (or resumed) buy. Returns whether an order
    was pushed ([buy_attempted]). *)
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
  let buy_attempted = ref false in
  let qty = venue_lot_qty state.grid_qty asset.exchange state in
  let grid_interval = asset.grid_interval in
  let quote_needed = ask_price *. qty in
  let ref_price = compute_buy_ref_price ~bid_price ~ask_price in
  let raw_buy_price = calculate_grid_price ref_price grid_interval false state in
  let buy_price =
    if bid_price > 0.0 then min raw_buy_price bid_price else raw_buy_price
  in
  (* A fresh buy must respect the same 2*gi spacing below the closest resting sell that
     the trailing leg enforces via [exact_target] (sell_price - 2*gi of the sell):
     otherwise a buy placed after a fill can sit too close to the lowest sell. As in the
     trailing leg the clamp is price-independent while a sell is tracked.

     The companion sell the sell leg will place later in this same tick is not yet visible
     here, and in a falling market it lands a rung below the closest existing sell.
     Clamping only against the feed let the fresh buy pass, then the companion sell's zone
     caught it and the next tick amended it down. The prospective sell is computed with
     the shared [owed_sell_price] and folded into the same clamp. *)
  let floor_for_sell sell_price =
    sell_price -. (sell_price *. (2.0 *. grid_interval /. 100.0))
  in
  let buy_price =
    match closest_sell_order_initial with
    | Some (_, sell_price) -> min buy_price (floor_for_sell sell_price)
    | None -> buy_price
  in
  (* Only anticipate the companion sell when the sell leg is actually owed one WITH
     inventory behind it - a just-filled buy or a balance recovery. There the sell will
     rest at [owed_sell_price] and the 2*gi clamp is real. Gating on those signals keeps a
     below-market buy from being pulled down against a sell that will never place (e.g. no
     sellable inventory), which would only add an up-amend on the next tick. *)
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
  let buy_cooldown_key = "place_Buy" in
  let is_buy_on_cooldown = Hashtbl.mem state.amend_cooldowns buy_cooldown_key in
  let has_crossing_sell =
    Sell_orders.exists_price_leq
      state.open_sell_orders
      (if bid_price > 0.0 then Float.max buy_price bid_price else buy_price)
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
          buy_attempted := true;
          state.last_buy_order_price <- Some buy_price;
          (* The re-attempt landed - any pending TIF-recovery is satisfied (the ack will
             confirm it as the resting buy). *)
          state.tif_recovery_pending <- false;
          (* A fresh buy is placed at the current sizing target, so any pending re-anchor
             is satisfied. *)
          state.force_buy_reanchor <- false;
          Logging.info_f
            ~section
            "Placed buy order for %s: %.8f @ %.4f"
            asset.symbol
            qty
            buy_price))
      else (
        let cooldown_key = "place_Buy" in
        if not (Hashtbl.mem state.amend_cooldowns cooldown_key)
        then
          if quote_balance_stale
          then (
            (* The balance snapshot is stale: the local figure may be wrong, so the
               attempt is still worthwhile - the exchange's verdict is the truth. Mark the
               attempt as knowingly under-funded so the (expected) rejection does not
               latch capital_low on a foreordained outcome. *)
            Logging.warn_f
              ~section
              "Local balance low for %s buy (need %.2f, available %.2f, balance snapshot \
               stale) - attempting anyway, exchange will reject if truly insufficient"
              asset.symbol
              quote_needed
              available_quote_balance;
            state.last_buy_attempted_insufficient <- true;
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
              state.last_buy_order_price <- Some buy_price))
          else (
            (* Fresh balance, genuinely insufficient: do not send an order that is
               guaranteed to be rejected. Pause buying via capital_low until available
               quote covers the next buy. *)
            if not state.capital_low
            then (
              state.capital_low <- true;
              state.capital_low_logged <- true;
              state.capital_low_at_balance <- -1.0;
              Logging.warn_f
                ~section
                "Local balance insufficient for %s buy (need %.2f, available %.2f) - \
                 skipping placement until balance recovers"
                asset.symbol
                quote_needed
                available_quote_balance);
            Hashtbl.replace state.amend_cooldowns cooldown_key (now +. 2.0))))
  else
    Logging.warn_f
      ~section
      "No quote balance data available for %s buy order"
      asset.symbol;
  state.last_cycle <- cycle;
  !buy_attempted
;;

(** Branch: exactly one resting buy - trail it up, or amend down to clear a sell's 2*gi
    restricted zone. *)
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
  if closest_sell_order_val <> None
  then (
    match closest_sell_order_val, state.last_buy_order_price, state.last_buy_order_id with
    | Some (_sell_order_id, sell_price), Some current_buy_price, Some buy_order_id ->
      (* The 2*gi separation from the closest sell is anchored on the sell order and is
         price-independent: while a sell is tracked by order management, the buy never
         trails above sell - 2*gi, no matter where the perceived top of book sits. Price
         can dislocate above a resting sell without filling it; the ladder must not let
         the buy cross a sell that still exists. The clamp is released only when the sell
         is removed from tracking (fill/cancel/expiry); only then does the buy trail the
         top of book at the grid interval. [sell_price] is always a positive resting-order
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
      (* A sizing re-anchor (the capital oracle published a changed grid interval -
         flagged by the domain worker on [force_buy_reanchor]) used to amend the resting
         buy in both directions. A downward amendment is warranted only by a sell-spacing
         violation (see below); a widened grid interval no longer snaps a valid resting
         buy down to the market rung. The ladder spacing is enforced where it matters:
         fresh placements clamp below the closest sell, and this leg corrects real
         intrusions into a sell's restricted zone. A qty-only oracle change does not
         re-anchor the price: the grid adopts the new size (Alpaca qty mismatch amend) or
         on the next placement, and the resting price only trails up. *)
      let reanchor_buy = state.force_buy_reanchor in
      (* Downward movement of the buy is initiated to correct an actual violation of the
         2x grid_interval restricted zone below the closest sell (above sell - 2*gi). This
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
             price*qty over open buys) back to the available balance. Without this the
             grid falsely reports "Insufficient quote balance" when trailing a funded buy
             up on committed capital. *)
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
    | _ -> ())
  else (
    match state.last_buy_order_price, state.last_buy_order_id with
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
