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
   | Some reason -> log_sell_block ~state ~now ~symbol:asset.symbol reason
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

(** Sell leg phase 2: the gated placement block. *)
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
  let is_alpaca = pre.sp_is_alpaca in
  let alpaca_available = pre.sp_alpaca_available in
  let ledger_balance = pre.sp_ledger_balance in
  let committed_sell = pre.sp_committed_sell in
  let capital_exhausted = pre.sp_capital_exhausted in
  let halt_inventory_check = pre.sp_halt_inventory_check in
  let should_trigger_sell = pre.sp_should_trigger_sell in
  let is_sell_on_cooldown = pre.sp_is_sell_on_cooldown in
  let missing_after_reconcile = pre.sp_missing_after_reconcile in
  let sell_pushed = pre.sp_sell_pushed in
  let nothing_placeable = pre.sp_nothing_placeable in
  if should_trigger_sell
     && (not (Float.is_nan asset_balance))
     && (not (has_active_sell state))
     && (not state.asset_low)
     && not is_sell_on_cooldown
  then (
    let asset_bal = ledger_balance in
    let qty =
      match state.last_buy_fill_qty with
      | Some q when q > 0.0 -> q
      | _ -> venue_lot_qty state.grid_qty asset.exchange state
    in
    (* Sell sizing ownership. An owed sell (a buy fill's 1:1 sell, a buy placement
       companion, inventory recovery after a balance/oracle event, the
       uncommitted-inventory fallback) is strategy-sized on every venue: price anchored on
       the last buy fill + grid interval, qty 1:1 with the fill, clamped to sellable
       inventory. The persisted level file never dictates the price or sizing of a new
       sell. Its only role is restoration: re-placing a rung the venue dropped (Alpaca
       fractional orders are forced to day TIF, so resting rungs die at the session
       boundary) at its recorded price/qty. Restoration is selected only when no new sell
       is owed, so the two obligations cannot hijack each other; a restoration level below
       the venue's real minimum is pruned at the venue-minimum gate below instead of
       wedging the maintenance path. *)
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
           venues) lives in [owed_sell_price] so the fresh-buy leg can anticipate the
           exact same price when it clamps in this tick. Re-anchoring to the bid (rather
           than the drifted fill) is decided there, including the capital-exhaustion
           exception that keeps the recovery rung at fill + gi. *)
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
    (* Non-accrued, uncommitted sellable inventory. Base committed to a resting or
       in-flight sell is subtracted on EVERY venue via [locked_in_sells] (the in-flight
       sell ledger), so it can never be offered again - not even when the venue reports a
       gross figure or its open-order feed momentarily drops a live order. Alpaca uses the
       venue's own [qty_available], which is already free of resting holds. *)
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
              (Printf.sprintf
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
              (Printf.sprintf
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
            (Printf.sprintf
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
          (Printf.sprintf
             "sell qty %.8f below min_order_size %.8f"
             target_q
             min_order_size);
        0.0, false)
    in
    if balance_ok
    then (
      (* The NOTIONAL gate: enforce both venue minimum quantity / increment floor and
         venue quote-notional minimum floor. *)
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
             balance adoption or a tradeable drop nets it, the venue's [total - hold]
             figure may still count this base as free, and sizing against it is the
             reserved_base leak under bursts. *)
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
          log_sell_block
            ~state
            ~now
            ~symbol:asset.symbol
            "order dispatch rejected (duplicate in-flight placement key)")
      else (
        (match target_sell_qty_override, target_sell_price_opt with
         | Some _, Some tp ->
           (* A restoration level below the venue's real minimum can never be placed -
              notional floors are static per level, so retrying is a permanent wedge
              (legacy dust from clamped sizing). Prune it so the file keeps being a
              faithful record of what CAN be on the system. *)
           let rec remove_one acc found = function
             | [] -> List.rev acc
             | (sp, _sq) :: rest
               when (not found)
                    && (abs_float (sp -. tp) <= tp *. 0.0001
                        || abs_float (sp -. tp) <= 1e-4) -> remove_one acc true rest
             | item :: rest -> remove_one (item :: acc) found rest
           in
           let new_levels = remove_one [] false state.persisted_sell_levels in
           if new_levels <> state.persisted_sell_levels
           then (
             state.persisted_sell_levels <- new_levels;
             state.persistence_dirty <- true);
           log_sell_block
             ~state
             ~now
             ~symbol:asset.symbol
             (Printf.sprintf
                "persisted sell level %.4f x %.8f below venue minimum $%.2f - pruned \
                 from sell_levels_state.json (unplaceable)"
                sell_price
                effective_sell_qty
                min_notional)
         | _ ->
           log_sell_block
             ~state
             ~now
             ~symbol:asset.symbol
             ~kind:"sellable inventory below the quote-notional minimum"
             (Printf.sprintf
                "sellable inventory below the quote-notional minimum (venue min $%.2f, \
                 sell_price %.4f, sell qty %.8f)"
                min_notional
                sell_price
                effective_sell_qty));
        nothing_placeable := true))
    else nothing_placeable := true)
;;

(** Sell leg phase 3: retry-latch bookkeeping, nothing-to-sell consumption, excess sweep. *)
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
  let is_alpaca = pre.sp_is_alpaca in
  let inventory_ok = pre.sp_inventory_ok in
  let inventory_basis = pre.sp_inventory_basis in
  let min_notional = pre.sp_min_notional in
  let base_ref_price = pre.sp_base_ref_price in
  let missing_after_reconcile = pre.sp_missing_after_reconcile in
  let available_base = pre.sp_available_base in
  let reserve_headroom = pre.sp_reserve_headroom in
  let sell_pushed = pre.sp_sell_pushed in
  let nothing_placeable = pre.sp_nothing_placeable in
  (* Retry semantics: the sell for a completed buy (or a buy placement) is OWED until it
     is actually placed. Transient blockers (sell cooldown, asset_low, a NaN balance
     snapshot, an in-flight sell placement) do NOT consume the trigger, so the leg retries
     on the next tick - with or without a replacement buy (capital exhausted /
     oracle-halted). Only a placed sell or a verified nothing-to-sell (known balance below
     the venue floor) clears the latch; a later fill or placement re-arms it. This is what
     keeps the last filled buy's inventory sellable when there is no capital to replace
     the buy. *)
  if !sell_pushed || !nothing_placeable
  then state.just_filled_buy <- false
  else if buy_attempted && not state.just_filled_buy
  then (
    state.just_filled_buy <- true;
    (* A freshly armed obligation must always surface its first blocker immediately, even
       if the same blocker kind logged inside the dedup window for a previous obligation. *)
    state.last_sell_block_reason <- "");
  (* Verified nothing-to-sell (remaintain venues - Alpaca): the trigger is armed but the
     fresh balance figure is below the venue's order floor, so the owed sell cannot place
     in this state. Consume the latch here - the placement block above can never reach it
     for remaintain venues because [missing_alpaca_sell_grid] itself requires
     [inventory_ok]. Leaving the latch armed re-fires the inventory-gate warn every tick
     for the life of a dust balance. Recovery paths re-arm placement on their own: a
     buy-fill event re-arms this latch, and the persistent (open_sell_orders = [] /\
     last_buy_fill_price) grid-maintenance clause re-places the fill-anchored sell once
     inventory clears the floor. *)
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
      base_ref_price);
  (* Alpaca excess-inventory sweep: the ladder is refilled first (every rung restored from
     the tracker); only once NO rung is missing do we route the leftover sellable base
     onto the TOP rung as a qty-only amend, so the surplus is offered at the best price
     instead of sitting idle. Runs after the retry block so a blocked owed sell keeps its
     reservation and never races the amend. The sweep allowance is the min of the venue
     free figure and the local ledger headroom ([position - reserved_base - committed]) so
     a stale/hung balance snapshot cannot size it into reserved_base, and a stale snapshot
     blocks the sweep outright. *)
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
  then (
    let sweep_available =
      let venue = Float.max 0.0 available_base in
      if is_alpaca then Float.min venue reserve_headroom else venue
    in
    evaluate_excess_sweep
      ~state
      ~now
      ~asset
      ~available:sweep_available
      ~min_notional
      ~ecfg);
  state.resuming_after_balance_flag <- false
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
