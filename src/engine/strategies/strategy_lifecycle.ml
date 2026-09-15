(* Platform/lifecycle accounting actions (strategy-agnostic).

   Sell-hold overlay, persisted-level reconcile, position ledger reconcile, low-flag
   recovery and cooldown expiry. Moved out of jacobs_ladder_execution; the grid and the
   config interpreter both call these. *)

open Strategy_common
open Strategy_state
open Strategy_venue
open Strategy_reservation
module Sell_orders = Strategy_sell_orders

(* Moved to Platform_accounting (milestone 2). *)
let price_key = Platform_accounting.price_key
let partition_persisted_sell_levels = Platform_accounting.partition_persisted_sell_levels
let price_within_tolerance = Platform_accounting.price_within_tolerance
let dedupe_persisted_sell_levels = Platform_accounting.dedupe_persisted_sell_levels

(** [persisted_rebuild_needed ~feed persisted] is true iff the [remaintain_expired_sells]
    rebuild would change [persisted] given [feed] (the scan's live open-sell store).

    It duplicates the rebuild's change condition exactly - no rung added, dropped,
    re-quantified, re-priced, or de-duplicated, and the list already price-descending with
    one rung per tolerance bucket - but without building the rebuilt list. The common
    cycle therefore skips the rebuild's two sorts, [List.map], and fold, which is the bulk
    of the sync stage's minor-word budget; a genuine change still takes the original path.

    Written with explicit recursion and index reads (no list conversion, no closures, no
    [Some] boxes) so the check itself is allocation-free. *)
let persisted_rebuild_needed ~(feed : Sell_orders.t) persisted =
  let flen = Sell_orders.length feed in
  let rec ordered has_prev prev_p = function
    | [] -> true
    | (p, _) :: rest ->
      let ok =
        (not has_prev)
        || (Float.compare prev_p p > 0 && not (price_within_tolerance ~reference:prev_p p))
      in
      ok && ordered true p rest
  in
  (* Max live qty whose price is within tolerance of [p]. Max is order-independent, so a
     right fold matches the original left fold. *)
  let rec live_qty p j =
    if j >= flen
    then 0.0
    else (
      let fp = Sell_orders.get_price feed j in
      let fq = Sell_orders.get_qty feed j in
      let acc = live_qty p (j + 1) in
      if fq > acc && price_within_tolerance ~reference:p fp then fq else acc)
  in
  let rec any_qty_changed = function
    | [] -> false
    | (p, q) :: rest ->
      let live = live_qty p 0 in
      if live > 0.0 && live <> q then true else any_qty_changed rest
  in
  let rec matches_persisted fp = function
    | [] -> false
    | (p, _) :: rest ->
      price_within_tolerance ~reference:p fp || matches_persisted fp rest
  in
  let rec any_adopted j =
    if j >= flen
    then false
    else (
      let fp = Sell_orders.get_price feed j in
      let fq = Sell_orders.get_qty feed j in
      if fq > 0.0 && not (matches_persisted fp persisted)
      then true
      else any_adopted (j + 1))
  in
  (not (ordered false 0.0 persisted)) || any_qty_changed persisted || any_adopted 0
;;

(** Reconciles the persisted-sell grid (Alpaca offline fill recovery). Computed once per
    execution and reused by the three persisted-sell branches. *)
let reconcile_persisted_sell_levels ~state =
  partition_persisted_sell_levels
    state.persisted_sell_levels
    (Sell_orders.to_list state.open_sell_orders)
;;

(* Pure overlay constants and the freshness cutoff moved to Platform_accounting (milestone
   2). Aliased here so behavior is unchanged while the state-coupled overlays migrate
   next. *)
let sell_hold_netting_grace_s = Platform_accounting.sell_hold_netting_grace_s
let unreflected_cutoff = Platform_accounting.unreflected_cutoff
let sweep_max_balance_age_s = Platform_accounting.sweep_max_balance_age_s

(* Moved to Platform_accounting (milestone 2). Thin adapter keeps the grid's state field
   as the store; behavior is unchanged. *)
let unnetted_sell_hold ~state ~ecfg ~now ~base_balance_age =
  let holds, amount =
    Platform_accounting.unnetted_sell_hold
      ~use_unnetted:ecfg.use_unnetted_sell_hold
      ~holds:state.sell_holds_since_balance
      ~last_balance_delta:state.last_balance_delta
      ~now
      ~base_balance_age
  in
  state.sell_holds_since_balance <- holds;
  amount
;;

(* Moved to Platform_accounting (milestone 2). *)
let consume_sell_hold_netting ~state ~amount =
  state.sell_holds_since_balance
  <- Platform_accounting.consume_sell_hold_netting
       ~holds:state.sell_holds_since_balance
       ~amount
;;

(* Moved to Platform_accounting (milestone 2). *)
let arm_sell_hold ~state ~qty ~now =
  state.sell_holds_since_balance
  <- Platform_accounting.arm_sell_hold ~holds:state.sell_holds_since_balance ~qty ~now
;;

(** Surfaces a sell-placement blocker at warn level. The same blocker re-fires every
    strategy tick (dust persisted level below the venue notional floor, a wiped position
    snapshot, a latched flag); deduplicated per reason with a 60s repeat window, and a
    reason change always logs immediately. [~kind] overrides the dedup key for reasons
    that interpolate tick-varying figures (a live ref price, a moving balance) so the
    window holds while the logged text keeps full detail. *)
let log_sell_block ?(kind = "") ~state ~now ~symbol reason =
  let key = if kind = "" then reason else kind in
  if key <> state.last_sell_block_reason || now -. state.last_sell_block_log_at >= 60.0
  then (
    state.last_sell_block_reason <- key;
    state.last_sell_block_log_at <- now;
    Logging.warn_f ~section "Sell placement blocked for %s: %s" symbol reason)
;;

(** Reconciles the in-memory position ledger to the venue balance feed.

    A new venue balance message is authoritative for the base it reports, so
    [position_base] is adopted outright (replacement, never a running sum - the failure
    mode of the removed anticipated-credit overlay, which added fills on top of the venue
    figure and could size a sell past [reserved_base]). Any buy credit the message's
    generation time already covers is dropped from the overlay; newer credits stay, so a
    just-filled buy remains sellable until the feed nets it. *)
let reconcile_position ~state ~now ~base_balance_age ~asset_balance =
  if not (Float.is_nan asset_balance)
  then (
    let venue_ts =
      match base_balance_age with
      | Some age -> now -. age
      | None -> now
    in
    (* A new message advanced the feed timestamp (preferred), or - when the feed exposes
       no freshness - changed the value. The epsilon absorbs the wall-clock jitter between
       capturing [now] and evaluating the age. *)
    let is_new_message =
      match base_balance_age with
      | Some _ -> venue_ts > state.position_venue_ts +. 0.001
      | None -> asset_balance <> state.last_seen_asset_balance
    in
    if (not state.position_initialized) || is_new_message
    then (
      (* Track how far the adopted figure moved since the last adoption. A positive move
         can already contain buy fills whose execution events have not arrived yet
         (executions and balances are independent feeds), so buy fills draw this down
         before entering the overlay - otherwise a balance message that arrives before its
         fill event is counted twice, once in [position_base] and once in the overlay, and
         a sell can size past the holdings. Deposits that arrive before their
         (nonexistent) fill merely under-credit, which is the safe direction. *)
      let delta = asset_balance -. state.position_base in
      let was_initialized = state.position_initialized in
      if was_initialized
      then (
        state.attributed_balance_increase
        <- Float.max 0.0 (state.attributed_balance_increase +. delta);
        (* A tradeable DROP is the venue netting holds (or base leaving): retire the
           oldest outstanding sell holds by that amount. Buys only raise tradeable, so
           they never consume a hold. *)
        if delta < 0.0 then consume_sell_hold_netting ~state ~amount:(-.delta))
      else state.attributed_balance_increase <- 0.0;
      (* Direction of this message, consumed by [unnetted_sell_hold]: only a flat/down
         move can have applied an outstanding sell hold. A buy-fill increase bumps the
         same per-asset freshness timestamp but nets no hold, so it must not retire the
         guard. First adoption seeds 0.0. *)
      state.last_balance_delta <- (if was_initialized then delta else 0.0);
      state.position_base <- asset_balance;
      state.position_initialized <- true;
      state.position_venue_ts <- venue_ts;
      (* Retire overlay credits the adopted increase already covers, oldest first, then
         drop anything older than the message generation (belt and suspenders for a dead
         feed). *)
      let pool = ref state.attributed_balance_increase in
      let consumed =
        List.filter_map
          (fun (ts, q) ->
            if !pool <= 0.0
            then Some (ts, q)
            else (
              let take = Float.min !pool q in
              pool := !pool -. take;
              let left = q -. take in
              if left > 1e-12 then Some (ts, left) else None))
          state.buy_credits_since_balance
      in
      state.attributed_balance_increase <- !pool;
      let cutoff = unreflected_cutoff ~now ~base_balance_age in
      state.buy_credits_since_balance
      <- List.filter (fun (ts, _) -> ts >= cutoff) consumed))
;;

(* Moved to Platform_accounting (milestone 2). Thin adapter keeps the grid's state field
   as the store; behavior is unchanged. *)
(** Sum of buy-fill credits the balance feed has not yet netted: fills at/after the newest
    balance message (or within the grace when the feed is silent). Entries are pruned so
    the overlay cannot grow without bound, and the sum is added to [position_base] for
    sizing. *)
let unreflected_buy_credit ~state ~base_balance_age ~now =
  let credits, sum =
    Platform_accounting.unreflected_credit
      ~credits:state.buy_credits_since_balance
      ~now
      ~base_balance_age
  in
  state.buy_credits_since_balance <- credits;
  sum
;;

(** Venue-authoritative immediately-sellable base for [asset], read straight from the
    venue module (Alpaca's [qty_available]: total minus base held by resting open orders).
    Returns NaN when the venue exposes no such figure or the lookup fails, so callers fall
    back to the local gross-minus-reconstructed-holds basis. Only Alpaca needs this today;
    the other venues' tradeable accessor is already hold-netted. *)
let venue_available_base ~(asset : trading_config) =
  if Exchange.Types.exchange_of_string asset.exchange <> Alpaca
  then Float.nan
  else (
    match get_exchange_module asset.exchange with
    | Some (module Ex : Exchange.S) ->
      (try Ex.get_available_balance_fast ~asset:asset.symbol () with
       | _ -> Float.nan)
    | None -> Float.nan)
;;

(** Evaluates asset balance recovery and clears asset_low when available balance is
    restored. *)
let evaluate_asset_low_recovery
  ~state
  ~now
  ~base_balance_age
  ~ecfg
  ~(asset : trading_config)
  ~asset_balance
  ~lot_qty
  ~unnetted_hold
  =
  if not (Float.is_nan asset_balance)
  then (
    reconcile_position ~state ~now ~base_balance_age ~asset_balance;
    let unreflected = unreflected_buy_credit ~state ~base_balance_age ~now in
    let asset_bal = state.position_base +. unreflected in
    let qty_f = lot_qty in
    let asset_needed_fast = qty_f in
    let is_alpaca = Exchange.Types.exchange_of_string asset.exchange = Alpaca in
    let asset_available = venue_available_base ~asset in
    let committed_sell =
      if ecfg.use_reserved_base_guard
      then
        effective_committed_sell_base
          ~ecfg
          ~ledger_total:(committed_sell_base state)
          ~feed_total:state.feed_locked_sell_base
          ~unnetted_hold
      else 0.0
    in
    let available_asset =
      if is_alpaca && not (Float.is_nan asset_available)
      then
        (* Alpaca: the venue's own [qty_available] is authoritative for what is free of
           resting holds. The stale poll is bridged with the un-polled buy credit overlay;
           the ledger is NOT subtracted here (it is the eventually-consistent
           reconstruction this path exists to avoid). *)
        Float.max
          0.0
          (asset_available +. unreflected -. state.reserved_base -. unnetted_hold)
      else asset_bal -. state.reserved_base -. committed_sell
    in
    let balance_actually_changed = asset_balance > state.last_seen_asset_balance in
    state.last_seen_asset_balance <- asset_balance;
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
        "Asset balance restored for %s (have %.8f, reserved %.8f, locked_sells %.8f, \
         available %.8f, need %.8f) - resuming sell+buy placement"
        asset.symbol
        asset_bal
        state.reserved_base
        committed_sell
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
    (* Recovery is affordability-based, matching the replay model (Grid_core clears once
       the quote can fund the next buy). Gating the clear on a balance increase latched
       the pause forever when a falling price made the same balance sufficient again, or
       when another asset's reclaim released reserved quote. The stamp below is for the
       log line only. *)
    if state.capital_low && available_quote < quote_needed_fast
    then ()
    else if state.capital_low
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

(** Expires rate-limit cooldowns and ghost-order markers.

    Pending order/amendment tokens are deliberately not swept here. Their lifecycle is
    purely event-driven: every dispatched place/amend produces one guaranteed terminal
    event (Ack/Failed, Amended/Amendment_skipped/ Amendment_failed, or a recognized
    cancel), and each handler removes the token, the in-flight flag, and the registry
    entry. An age-based sweep resolved state while the exchange could still be executing
    the request (Alpaca amends exceed 5s under SSL degradation), so a mid-flight cancel
    event was no longer recognized as the amend's side effect and wrongly reset buy
    tracking. *)
let cleanup_pending_and_cooldowns ~state ~now ~(asset : trading_config) =
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
