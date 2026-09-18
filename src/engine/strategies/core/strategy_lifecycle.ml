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
      ~allow_message_certify:(not state.balance_uses_gross)
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
  (* Dedup window: re-log only when the reason changes or 60s have passed. When [kind] is
     supplied the key is free, so the (allocating) [reason] closure is not called at all
     unless the line is actually emitted; otherwise it is built exactly once (the previous
     code built it a second time on every emitted line). *)
  let due = now -. state.last_sell_block_log_at >= 60.0 in
  if kind <> ""
  then (
    if kind <> state.last_sell_block_reason || due
    then (
      state.last_sell_block_reason <- kind;
      state.last_sell_block_log_at <- now;
      Logging.warn_f ~section "Sell placement blocked for %s: %s" symbol (reason ())))
  else (
    let key = reason () in
    if key <> state.last_sell_block_reason || due
    then (
      state.last_sell_block_reason <- key;
      state.last_sell_block_log_at <- now;
      Logging.warn_f ~section "Sell placement blocked for %s: %s" symbol key))
;;

(** Reconciles the in-memory position ledger to the venue balance feed.

    A new venue balance message is authoritative for the base it reports, so
    [position_base] is adopted outright (replacement, never a running sum - the failure
    mode of the removed anticipated-credit overlay, which added fills on top of the venue
    figure and could size a sell past [reserved_base]). Any buy credit the message's
    generation time already covers is dropped from the overlay; newer credits stay, so a
    just-filled buy remains sellable until the feed nets it. *)
let reconcile_position ~state ~now ~base_balance_age ~asset_balance ~asset_gross =
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
      (* The venue gross read (module lookup + two balance atomics) is paid only when a
         message is actually adopted, not every cycle. *)
      let asset_gross = asset_gross () in
      (* Track how far the adopted figure moved since the last adoption. A positive move
         can already contain buy fills whose execution events have not arrived yet
         (executions and balances are independent feeds), so buy fills draw this down
         before entering the overlay - otherwise a balance message that arrives before its
         fill event is counted twice, once in [position_base] and once in the overlay, and
         a sell can size past the holdings. Deposits that arrive before their
         (nonexistent) fill merely under-credit, which is the safe direction. *)
      let delta = asset_balance -. state.position_base in
      let was_initialized = state.position_initialized in
      (* Gross change of this message, when the venue exposes a gross total. [None] on the
         first gross-bearing message (nothing to diff against) and when no gross is
         available. *)
      let gross_delta =
        match asset_gross with
        | Some g when was_initialized && not (Float.is_nan state.position_gross) ->
          Some (g -. state.position_gross)
        | _ -> None
      in
      if was_initialized
      then (
        match gross_delta with
        | Some d_gross ->
          (* Gross basis: a buy fill raises gross, so attribute against gross - a
             same-window sell hold cannot mask it. *)
          state.attributed_balance_increase
          <- Float.max 0.0 (state.attributed_balance_increase +. Float.max 0.0 d_gross);
          (* A genuine hold move is the gross/tradeable gap changing: an application (hold
             up) or a fill (hold down) both mean the venue has accounted for the committed
             base. Retire armed holds by that magnitude. A flat tradeable with a matching
             gross rise (buy fill + sell hold) therefore neither leaves the buy credit
             unabsorbed nor the sell hold netted. *)
          let d_hold = d_gross -. delta in
          if abs_float d_hold > 1e-12
          then consume_sell_hold_netting ~state ~amount:(abs_float d_hold)
        | None ->
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
      (match asset_gross with
       | Some g ->
         state.position_gross <- g;
         state.balance_uses_gross <- true
       | None -> ());
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

(** Gross per-asset base ([total] minus staked/delegated) for hold-netted venues that
    expose it, or [None] when unavailable. This is the basis the feed-lag overlays need to
    tell a buy fill (raises gross) apart from a sell hold (raises hold, not gross): the
    tradeable figure alone cannot, because a buy fill and a same-window sell hold cancel
    in it. Returns [None] on venues that do not net holds from their own state, and when
    the gross figure is missing/zero or inconsistent (below the reported tradeable), so
    callers fall back to the legacy net-delta reconciliation. *)
let venue_gross_base ~(hold_netted : bool) ~(asset : trading_config) ~spendable =
  if not hold_netted
  then None
  else (
    match get_exchange_module asset.exchange with
    | Some (module Ex : Exchange.S) ->
      let total =
        try Ex.get_total_balance ~asset:asset.symbol with
        | _ -> Float.nan
      in
      let staked =
        try Ex.get_staked_balance ~asset:asset.symbol with
        | _ -> Float.nan
      in
      if Float.is_nan total || Float.is_nan staked
      then None
      else (
        let gross = Float.max 0.0 (total -. staked) in
        if gross +. 1e-9 < spendable then None else Some gross)
    | None -> None)
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
    let asset_gross () =
      venue_gross_base
        ~hold_netted:ecfg.hold_netted_from_venue_state
        ~asset
        ~spendable:asset_balance
    in
    reconcile_position ~state ~now ~base_balance_age ~asset_balance ~asset_gross;
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
    (* Compare the fill-aware ledger, not the raw venue snapshot. A just-filled buy raises
       [asset_bal] through the un-reflected credit before the balance feed nets it, and
       that fill is exactly the balance recovery this flag is waiting for. Testing the raw
       snapshot made the credit invisible, so a latched [asset_low] stayed set until the
       feed caught up (by then the credit had been consumed, and available could have
       re-crossed the threshold the other way). [last_seen_asset_balance] still tracks the
       raw snapshot for [reconcile_position], so the increase is measured against the
       venue's last confirmed figure and a still-pending credit keeps the flag clearable. *)
    let balance_actually_changed = asset_bal > state.last_seen_asset_balance in
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
let expire_amend_cooldowns ~state ~now ~(asset : trading_config) =
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
        asset.symbol))
;;

let evict_ghost_orders ~state ~now =
  if Hashtbl.length state.evicted_orders > 0
  then (
    let to_remove = ref [] in
    Hashtbl.iter
      (fun k (v, _side) -> if now > v then to_remove := k :: !to_remove)
      state.evicted_orders;
    List.iter (Hashtbl.remove state.evicted_orders) !to_remove)
;;

let cleanup_pending_and_cooldowns ~state ~now ~(asset : trading_config) =
  expire_amend_cooldowns ~state ~now ~asset;
  evict_ghost_orders ~state ~now
;;

(** True when [order_id] is known terminal to the strategy regardless of what the venue's
    open-order feed reports: evicted (a venue-terminal amend failure) or already filled
    ([processed_fills]). Such an id must never be adopted as the resting buy. *)
let is_terminal_order state order_id =
  Hashtbl.mem state.evicted_orders order_id || Hashtbl.mem state.processed_fills order_id
;;

(** Recompute the cached best buy from the persistent buy index. Called when the current
    best leaves, via a delta retraction or a terminal-order purge. *)
let recompute_cached_best_buy state =
  let best_id = ref "" in
  let best_p = Array.make 1 0.0 in
  Hashtbl.iter
    (fun id (p, _) ->
      if p > best_p.(0)
      then (
        best_id := id;
        best_p.(0) <- p))
    state.feed_buy_index;
  state.cached_best_buy_id <- !best_id;
  state.cached_best_buy_price <- best_p.(0)
;;

(** Drop indexed buys the strategy already knows are terminal ([is_terminal_order]). The
    venue's REST snapshot and its trade stream are independent feeds, so a snapshot taken
    before a fill propagates can resurrect a filled order in the local open-order cache -
    and the generation-skip path republishes the cached best buy without rescanning.
    Without this purge the stale id is re-adopted as the resting buy and re-amended every
    cycle: a doomed venue-terminal amend whose failure re-evicts and clears tracking,
    forever, while the buy leg never places a replacement. O(open buys) per cycle. Returns
    the number purged. *)
let purge_terminal_feed_buys ~state =
  let to_drop =
    Hashtbl.fold
      (fun id (price, qty) acc ->
        if is_terminal_order state id then (id, price, qty) :: acc else acc)
      state.feed_buy_index
      []
  in
  if to_drop = []
  then 0
  else (
    let best_dropped = ref false in
    List.iter
      (fun (id, price, qty) ->
        Hashtbl.remove state.feed_buy_index id;
        state.cached_open_buy_count <- state.cached_open_buy_count - 1;
        state.cached_locked_in_buys <- state.cached_locked_in_buys -. (price *. qty);
        if state.cached_best_buy_id = id then best_dropped := true)
      to_drop;
    if !best_dropped then recompute_cached_best_buy state;
    List.length to_drop)
;;

(** Apply a drained per-order delta (venue change log) to the persistent feed indexes, the
    cached scan outputs ([cached_open_buy_count], [cached_locked_in_buys],
    [cached_feed_total], [cached_closest_sell_order], [cached_best_buy_*]),
    [cached_feed_sell_orders] and the sell-commitment ledger, in O(changes).

    This is the incremental replacement for the O(open-orders) [sync_open_orders] scan:
    the scan's per-order contribution (buy count/sum, sell list/total/closest, commitment
    upsert) is retracted and re-applied only for the ids the venue reported as changed.
    When the current top buy or closest sell leaves, the new extreme is recomputed from
    the corresponding index (O(index), but only on an extreme leaving - not per changed
    id). [changes] is [(id, snapshot option)] in chronological order ([None] = removed). *)
let apply_open_order_delta ~state ~now_time ~ecfg ~changes =
  let trust_feed = ecfg.hold_netted_from_venue_state in
  let evicted_empty = Hashtbl.length state.evicted_orders = 0 in
  let sells_dirty = ref false in
  (* Set whenever a sell is retracted or added; resolved once after the whole delta by
     validating the cached closest against the final index (O(1)) and recomputing (O(n))
     only if it genuinely left. An amend that re-adds the closest at the same price - the
     common trailing case - therefore skips the scan entirely. *)
  let closest_dirty = ref false in
  let recompute_closest () =
    (* [float array] accumulator keeps this allocation-free; runs only when the closest
       sell genuinely leaves (see [closest_dirty] handling below). *)
    let best_id = ref "" in
    let best_p = Array.make 1 Float.infinity in
    Hashtbl.iter
      (fun id (p, _) ->
        if p < best_p.(0)
        then (
          best_id := id;
          best_p.(0) <- p))
      state.feed_sell_index;
    state.cached_closest_sell_order
    <- (if !best_id = "" then None else Some (!best_id, best_p.(0)))
  in
  let recompute_recent_amend () =
    let found = ref false in
    Hashtbl.iter
      (fun id _ ->
        match Hashtbl.find_opt state.amend_cooldowns id with
        | Some expiry when now_time < expiry -> found := true
        | _ -> ())
      state.feed_buy_index;
    state.cached_has_recent_amend_buy <- !found
  in
  List.iter
    (fun (oid, snap) ->
      (match Hashtbl.find_opt state.feed_buy_index oid with
       | Some (p, q) ->
         state.cached_open_buy_count <- state.cached_open_buy_count - 1;
         state.cached_locked_in_buys <- state.cached_locked_in_buys -. (p *. q);
         Hashtbl.remove state.feed_buy_index oid;
         if state.cached_best_buy_id = oid then recompute_cached_best_buy state;
         if state.cached_has_recent_amend_buy then recompute_recent_amend ()
       | None -> ());
      (match Hashtbl.find_opt state.feed_sell_index oid with
       | Some (_, q) ->
         state.cached_feed_total <- state.cached_feed_total -. q;
         ignore (Sell_orders.remove_by_id state.cached_feed_sell_orders oid);
         Hashtbl.remove state.feed_sell_index oid;
         sells_dirty := true;
         closest_dirty := true
       | None -> ());
      let is_our =
        match snap with
        | Some (_, _, _, userref) ->
          (match userref with
           | Some r -> r <> strategy_userref_mm
           | None -> true)
        | None -> true
      in
      let keep =
        match snap with
        | Some (_, qty, _, _) ->
          qty > 0.0
          && is_our
          && (evicted_empty || not (Hashtbl.mem state.evicted_orders oid))
          && not (Hashtbl.mem state.processed_fills oid)
        | None -> false
      in
      if keep
      then (
        match snap with
        | Some (price_opt, qty, side_str, _) ->
          let price = Option.value price_opt ~default:0.0 in
          if side_str = "buy"
          then (
            Hashtbl.replace state.feed_buy_index oid (price, qty);
            state.cached_open_buy_count <- state.cached_open_buy_count + 1;
            state.cached_locked_in_buys <- state.cached_locked_in_buys +. (price *. qty);
            if price > state.cached_best_buy_price && price > 0.0
            then (
              state.cached_best_buy_id <- oid;
              state.cached_best_buy_price <- price);
            match Hashtbl.find_opt state.amend_cooldowns oid with
            | Some expiry when now_time < expiry ->
              state.cached_has_recent_amend_buy <- true
            | _ -> ())
          else if side_str = "sell"
          then (
            add_tracked_order_id state oid;
            Hashtbl.replace state.feed_sell_index oid (price, qty);
            Sell_orders.push state.cached_feed_sell_orders oid price qty;
            state.cached_feed_total <- state.cached_feed_total +. qty;
            upsert_sell_commitment ~state ~id:oid ~price ~qty ~seen:true ~acked:true;
            (* A newly-added sell at a price below the cached closest must become the
               cached closest immediately. The post-loop [valid] check only detects the
               cached closest LEAVING (or changing price); it does not see a new lower
               sell, so without this the buy leg keeps clamping against the stale higher
               sell and trails into the true closest sell's 2*gi zone. Mirrors the
               best-buy update on the buy-add branch above. *)
            (match state.cached_closest_sell_order with
             | Some (_, best) when price > 0.0 && price >= best -> ()
             | _ -> state.cached_closest_sell_order <- Some (oid, price));
            sells_dirty := true;
            closest_dirty := true)
        | None -> ())
      else (
        match Hashtbl.find_opt state.sell_commitments oid with
        | Some c when c.sc_seen || c.sc_acked ->
          if trust_feed
          then (
            Hashtbl.remove state.sell_commitments oid;
            touch_commitments state)
          else (
            c.sc_listed <- false;
            state.sell_commitments_clean <- false)
        | _ -> ()))
    changes;
  (* Resolve the closest sell once, after every change is applied. The cached binding is
     validated against the final index in O(1); the O(n) rescan runs only when the closest
     genuinely left (an amend that re-adds it at the same price is the common case and
     keeps the binding valid). *)
  if !closest_dirty
  then (
    let valid =
      match state.cached_closest_sell_order with
      | Some (cid, cp) ->
        (match Hashtbl.find_opt state.feed_sell_index cid with
         | Some (fp, _) -> fp = cp
         | None -> false)
      | None -> false
    in
    if not valid then recompute_closest ());
  if !sells_dirty
  then (
    Sell_orders.blit ~src:state.cached_feed_sell_orders ~dst:state.open_sell_orders;
    state.sync_orders_seen <- List.length changes)
;;

(* [drain_open_order_changes] supplies the venue's per-order change log for the O(changes)
   incremental path; callers that have none pass [(fun ~symbol:_ -> [], true)] (or
   [Strategy_venue.no_open_order_changes]) to fall back to the full scan. *)
let sync_open_orders
  ~state
  ~now
  ~(asset : trading_config)
  ~bid_price:_
  ~lot_qty
  ~iter_open_orders
  ~get_open_orders_generation
  ~drain_open_order_changes
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
  (* Reconcile the persistent buy index against orders the strategy already knows are
     terminal before any cache-skip/delta publication. A venue snapshot can re-list a
     filled order, and the skip path republishes [cached_best_buy_id] without rescanning,
     so the purge must run every cycle, not only on a full scan. *)
  let purged = purge_terminal_feed_buys ~state in
  if purged > 0
  then
    Logging.debug_f
      ~section
      "Purged %d terminal buy order(s) still listed in the %s open-orders feed"
      purged
      asset.symbol;
  let best_buy_price = ref 0.0 in
  let best_buy_id = ref "" in
  let open_buy_count_from_scan = ref 0 in
  let has_recent_amend_buy = ref false in
  let locked_in_buys = ref 0.0 in
  let locked_in_sells = ref 0.0 in
  let feed_total = ref 0.0 in
  (* Deferred optionization: building [Some (id, price)] on every closer sell/list
     allocates ~5 words per order per scan; keep the running best in plain refs and build
     the option once at the end. Empty id = "no candidate". *)
  let closest_sell_id = ref "" in
  let closest_sell_price = ref nan in
  (* Rescan gate. The scan below is O(open orders) and dominated by string-keyed hashtable
     work; when the venue exposes an open-orders generation ([get_open_orders_generation])
     that has not moved since the last scan, reuse the last scan's outputs and skip the
     scan. This applies to every venue (including Alpaca/remaintain): an unchanged
     generation means the feed sells are identical, so the persisted-ladder rebuild below
     is a no-op and the ledger reconcile still runs. *)
  let generation = get_open_orders_generation () in
  (* Drain the venue's per-order change log (O(changes)). [has_changes] is true only when
     the venue reported mutations since the last drain; an overflow (or an unsupported
     venue, which returns [[], true]) forces the full scan. Draining on every cycle (even
     a skip) is deliberate: the feed appends to the log before it bumps the generation, so
     a mutation can be visible here while [generation] is unchanged - applying it now
     avoids losing it until the next publish. *)
  let changes, changes_overflow = drain_open_order_changes ~symbol:asset.symbol in
  let has_changes =
    match changes with
    | [] -> false
    | _ :: _ -> true
  in
  let generation_changed =
    (not state.open_orders_scan_valid)
    || generation < 0
    || state.open_orders_scan_generation <> generation
  in
  let can_skip = (not generation_changed) && not has_changes in
  if can_skip
  then (
    Sell_orders.blit ~src:state.cached_feed_sell_orders ~dst:state.open_sell_orders;
    open_buy_count_from_scan := state.cached_open_buy_count;
    has_recent_amend_buy := state.cached_has_recent_amend_buy;
    locked_in_buys := state.cached_locked_in_buys;
    (* Publish the cached best buy too: the adoption block below re-tracks a resting buy
       when tracking was cleared (cancel/amend clearance). Leaving these empty on a skip
       cycle meant a quiet symbol whose buy was untracked never got adopted - the buy leg
       then saw [open_buy_count > 0] with no [last_buy_order_id] and could neither amend
       nor replace it until the next order delta. *)
    best_buy_id := state.cached_best_buy_id;
    best_buy_price := state.cached_best_buy_price;
    (match state.cached_closest_sell_order with
     | Some (id, p) ->
       closest_sell_id := id;
       closest_sell_price := p
     | None -> ());
    feed_total := state.cached_feed_total)
  else if state.feed_index_valid && not changes_overflow
  then (
    (* Incremental path: the persistent indexes are a complete snapshot and the venue gave
       us exactly the ids that moved, so apply O(changes) instead of rescanning every open
       order + walking every commitment. *)
    let t_scan_start = Monotonic_clock.now_ns () in
    if has_changes then apply_open_order_delta ~state ~now_time ~ecfg ~changes;
    state.time_sync_scan_ns <- Monotonic_clock.now_ns () - t_scan_start;
    open_buy_count_from_scan := state.cached_open_buy_count;
    has_recent_amend_buy := state.cached_has_recent_amend_buy;
    locked_in_buys := state.cached_locked_in_buys;
    best_buy_id := state.cached_best_buy_id;
    best_buy_price := state.cached_best_buy_price;
    feed_total := state.cached_feed_total;
    (match state.cached_closest_sell_order with
     | Some (id, p) ->
       closest_sell_id := id;
       closest_sell_price := p
     | None -> ());
    state.open_orders_scan_generation <- generation;
    state.open_orders_scan_valid <- true)
  else (
    (* Hot-path invariant (enforced): reaching the O(open-orders) scan means the
       persistent index was not a complete snapshot, which is only ever true before the
       first scan or after the venue's delta reported an overflow (clear/reconnect). A
       steady-state cycle must never get here - if it does, the delta/index contract has
       regressed and the cycle silently becomes O(n). *)
    assert ((not state.feed_index_valid) || changes_overflow);
    (* Mark every commitment "not listed" for this scan. The scan's
       [upsert_sell_commitment ~seen:true] flips it back on, and the reconcile below tests
       the flag instead of a separate [(string, unit)] membership table. That removes a
       whole hashtable plus a string-hash mem+replace per open sell and a string-hash
       lookup per ledger entry. [execute_strategy] holds [state.mutex], so this is
       single-writer. O(ledger), no allocation. *)
    Hashtbl.iter (fun _ c -> c.sc_listed <- false) state.sell_commitments;
    (* A full scan rebuilds the commitment [sc_listed] flags and the persistent indexes
       from scratch, so the ledger is no longer known clean and the indexes are
       re-derived. *)
    state.sell_commitments_clean <- false;
    Hashtbl.reset state.feed_sell_index;
    Hashtbl.reset state.feed_buy_index;
    Sell_orders.clear state.open_sell_orders;
    state.sync_orders_seen <- 0;
    (* Loop-invariant: [evicted_orders] does not change during the scan. *)
    let evicted_empty = Hashtbl.length state.evicted_orders = 0 in
    let t_scan_start = Monotonic_clock.now_ns () in
    iter_open_orders (fun oid price qty side_str userref_opt ->
      state.sync_orders_seen <- state.sync_orders_seen + 1;
      let is_our_strategy =
        match userref_opt with
        | Some ref_val -> ref_val <> strategy_userref_mm
        | None -> true
      in
      if qty > 0.0
         && is_our_strategy
         && (evicted_empty || not (Hashtbl.mem state.evicted_orders oid))
         && not (Hashtbl.mem state.processed_fills oid)
      then
        if side_str = "buy"
        then (
          incr open_buy_count_from_scan;
          locked_in_buys := !locked_in_buys +. (price *. qty);
          Hashtbl.replace state.feed_buy_index oid (price, qty);
          if price > !best_buy_price && price > 0.0
          then (
            best_buy_price := price;
            best_buy_id := oid);
          match Hashtbl.find_opt state.amend_cooldowns oid with
          | Some expiry when now_time < expiry -> has_recent_amend_buy := true
          | _ -> ())
        else if side_str = "sell"
        then (
          add_tracked_order_id state oid;
          Sell_orders.push state.open_sell_orders oid price qty;
          Hashtbl.replace state.feed_sell_index oid (price, qty);
          (* A snapshot lists each order id at most once, so accumulate directly;
             [upsert_sell_commitment] flips [sc_listed] for the reconcile below. *)
          feed_total := !feed_total +. qty;
          (* Refresh the in-flight ledger with the venue's live remaining qty; a sell
             adopted straight from the feed (no prior local arm) is entered here so it is
             reserved from now on. *)
          upsert_sell_commitment ~state ~id:oid ~price ~qty ~seen:true ~acked:true;
          if Float.is_nan !closest_sell_price || price < !closest_sell_price
          then (
            closest_sell_id := oid;
            closest_sell_price := price)));
    state.time_sync_scan_ns <- Monotonic_clock.now_ns () - t_scan_start;
    state.cached_feed_total <- !feed_total;
    state.cached_open_buy_count <- !open_buy_count_from_scan;
    state.cached_has_recent_amend_buy <- !has_recent_amend_buy;
    state.cached_locked_in_buys <- !locked_in_buys;
    state.cached_closest_sell_order
    <- (if !closest_sell_id = ""
        then None
        else Some (!closest_sell_id, !closest_sell_price));
    (* Snapshot the feed sells for the generation-skip path. Kept for every venue: a
       remaintain venue skips on an unchanged generation just like the others, and the
       persisted rebuild reads [open_sell_orders] (blitted from here on a skip). *)
    Sell_orders.blit ~src:state.open_sell_orders ~dst:state.cached_feed_sell_orders;
    state.cached_best_buy_id <- !best_buy_id;
    state.cached_best_buy_price <- !best_buy_price;
    state.feed_index_valid <- true;
    state.open_orders_scan_generation <- generation;
    state.open_orders_scan_valid <- true);
  (* Rebuild the persisted ladder from the feed deterministically: one rung per price
     (within the same tolerance the matcher uses), its qty the live order qty at that
     price; levels with no live order keep their recorded (missing) qty; feed prices
     absent from the ladder are adopted. The per-price qty is the MAX across order ids so
     an Alpaca amend (cancel+replace) window - which transiently lists the old id and its
     replacement at the same price - or a historical duplicate cannot flap the recorded
     qty. Replacing the old per-order 1-to-1 match/adopt is what removes the SMH/REMX
     "Updated ... -> ..." / "Adopted ..." churn: with two ids at one price the old code
     consumed the single level with the first id and re-adopted the second every scan.
     Runs on the incremental path too (the [remaintain_expired_sells] venue's ladder must
     track the feed's live prices between snapshots), hence outside the scan branches. *)
  if (not can_skip)
     && ecfg.remaintain_expired_sells
     && persisted_rebuild_needed ~feed:state.open_sell_orders state.persisted_sell_levels
  then (
    let feed = Sell_orders.to_list state.open_sell_orders in
    let live_qty_at p =
      List.fold_left
        (fun acc (_, fp, fq) ->
          if fq > acc && price_within_tolerance ~reference:p fp then fq else acc)
        0.0
        feed
    in
    let base =
      List.map
        (fun (p, recorded_q) ->
          let live_q = live_qty_at p in
          if live_q > 0.0 then p, live_q else p, recorded_q)
        (dedupe_persisted_sell_levels state.persisted_sell_levels)
    in
    (* Adopt feed orders whose price is not already represented, deduped within tolerance
       by the running accumulated list. *)
    let rebuilt_rev =
      List.fold_left
        (fun acc (_, fp, fq) ->
          if fq > 0.0
             && not
                  (List.exists (fun (p, _) -> price_within_tolerance ~reference:p fp) acc)
          then (fp, fq) :: acc
          else acc)
        (List.rev base)
        feed
    in
    let rebuilt = List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) rebuilt_rev in
    if rebuilt <> state.persisted_sell_levels
    then (
      state.persisted_sell_levels <- rebuilt;
      state.persistence_dirty <- true;
      Logging.debug_f
        ~section
        "Persisted sell ladder for %s rebuilt from feed: %d level(s)"
        asset.symbol
        (List.length rebuilt)));
  let t_rec_start = Monotonic_clock.now_ns () in
  (* Reconcile the in-flight sell ledger with this scan's feed. The feed refreshes an
     order's remaining qty while it lists it. What an absence means is venue-specific (see
     [hold_netted_from_venue_state]):
     - Venues whose balance nets holds from their own state (Hyperliquid): the feed is
       authoritative and independent of hold netting, so an acked/seen order absent from
       the feed has truly left the book (fill / cancel / amend-away) and is dropped.
       Keeping it would strand a phantom sell and double-subtract base the venue already
       excludes.
     - Venues deriving holds from the same feed (Kraken): an acked/seen order absent from
       the feed stays reserved until its terminal event, so a truncated snapshot/reconnect
       cannot silently free live base. An order never listed and never acked is kept only
       within the dispatch window (a lost placement). Local-only commitments are merged
       back into [open_sell_orders] so the buy leg's wash-trade and 2*gi clamps still see
       them. [locked_in_sells] is then the ledger total. *)
  let trust_feed = ecfg.hold_netted_from_venue_state in
  (* A cache-hit cycle whose ledger is fully feed-listed has nothing to reconcile:
     [open_sell_orders] was just blitted from the cache (the feed's sells) and every
     commitment is listed, so no local-only entry is missing and none is stale. Skip the
     O(commitments) walk entirely. Otherwise one walk both pushes unlisted local-only
     commitments back into [open_sell_orders] (so the buy leg's wash-trade / 2*gi clamps
     still see them) and drops the commitments the feed no longer lists (trust_feed venue)
     or that aged out of the dispatch window, then records whether any unlisted commitment
     remains. This is the union of the previous "needs rebuild" and no-op paths: the old
     guard scan short- circuited on the first unlisted commitment, so both paths together
     already visited exactly these cases. *)
  if state.sell_commitments_clean
  then ()
  else (
    let any_unlisted = ref false in
    let to_remove = ref [] in
    Hashtbl.iter
      (fun id c ->
        if c.sc_listed
        then
          (* [upsert_sell_commitment] already wrote the feed's live price/qty onto this
             commitment during the scan, so the stored values are the feed values. *)
          ()
        else (
          any_unlisted := true;
          if c.sc_seen || c.sc_acked
          then
            if trust_feed
            then to_remove := id :: !to_remove
            else Sell_orders.push state.open_sell_orders id c.sc_price c.sc_qty
          else if now_time -. c.sc_armed <= sell_commitment_in_flight_timeout_s
          then Sell_orders.push state.open_sell_orders id c.sc_price c.sc_qty
          else to_remove := id :: !to_remove))
      state.sell_commitments;
    List.iter (Hashtbl.remove state.sell_commitments) !to_remove;
    if !to_remove <> [] then state.sell_commitments_gen <- state.sell_commitments_gen + 1;
    state.sell_commitments_clean <- not !any_unlisted);
  state.time_sync_rec_ns <- Monotonic_clock.now_ns () - t_rec_start;
  state.feed_locked_sell_base <- !feed_total;
  locked_in_sells := committed_sell_base state;
  let is_amend_active =
    state.inflight_amend_buy
    || InFlightOrders.is_in_flight state.duplicate_key_buy
    ||
    match state.last_buy_order_id with
    | Some oid ->
      InFlightAmendments.is_in_flight oid
      || InFlightAmendments.is_amend_lifecycle_active oid
      ||
        (match Hashtbl.find_opt state.amend_cooldowns oid with
        | Some expiry -> now_time < expiry
        | None -> false)
    | None -> false
  in
  (* Ghost-buy grace predicate moved to Platform_accounting (milestone 2). A freshly acked
     buy is not listed by the open-orders feed yet; after the grace, a buy still absent
     with no terminal event is recovered. *)
  if Platform_accounting.is_ghost_buy
       ~open_buy_count:!open_buy_count_from_scan
       ~inflight_cancel_buy:state.inflight_cancel_buy
       ~inflight_buy:state.inflight_buy
       ~is_amend_active
       ~now:now_time
       ~last_buy_ack_ts:state.last_buy_ack_ts
  then (
    if Option.is_some state.last_buy_order_id || Option.is_some state.last_buy_order_price
    then (
      let oid = Option.value state.last_buy_order_id ~default:"none" in
      let p = Option.value state.last_buy_order_price ~default:0.0 in
      Logging.warn_f
        ~section
        "GHOST_BUY_DETECTED [%s] order %s @ %.2f in memory, but not in open orders feed. \
         Clearing."
        asset.symbol
        oid
        p;
      state.last_buy_order_id <- None;
      state.last_buy_order_price <- None;
      set_asset_reserved_quote state 0.0))
  else if (not state.inflight_cancel_buy)
          && (not state.inflight_buy)
          && not state.inflight_amend_buy
  then
    if !best_buy_id <> ""
    then (
      let best_order_id = !best_buy_id in
      let best_price = !best_buy_price in
      let recent_amend =
        match Hashtbl.find_opt state.amend_cooldowns best_order_id with
        | Some expiry -> now < expiry
        | None -> false
      in
      (* Defense-in-depth: the purge above should already have dropped any terminal id
         from the index/cache, but never adopt one even if a future path republishes it -
         the adopt -> doomed amend -> evict loop is what wedged the symbol. *)
      if (not recent_amend) && not (is_terminal_order state best_order_id)
      then (
        add_tracked_order_id state best_order_id;
        state.last_buy_order_price <- Some best_price;
        state.last_buy_order_id <- Some best_order_id;
        state.tif_recovery_pending <- false;
        set_asset_reserved_quote state (best_price *. lot_qty)));
  (* Split the persisted ladder into open/missing by whether a live order rests at each
     rung's price (within the matcher tolerance). The rebuild above already collapsed the
     list to one entry per price, so a membership partition is exact (no per-price count
     drain). Only the [remaintain_expired_sells] (Alpaca GTC) reconcile
     ([evaluate_sell_leg]) reads it, so other venues skip the O(m) work. *)
  let open_persisted_levels, missing_persisted_levels =
    if ecfg.remaintain_expired_sells && state.persisted_sell_levels <> []
    then (
      (* Index walk (no per-rung closure/predicate allocation). O(m) per rung is fine: m
         is the open-order count and this is the remaintain path only. *)
      let feedm = state.open_sell_orders in
      let flen = Sell_orders.length feedm in
      let feed_open p =
        let rec go j =
          if j >= flen
          then false
          else if price_within_tolerance ~reference:p (Sell_orders.get_price feedm j)
          then true
          else go (j + 1)
        in
        go 0
      in
      let missing_acc = ref [] in
      List.iter
        (fun ((p, _) as level) ->
          if not (feed_open p) then missing_acc := level :: !missing_acc)
        state.persisted_sell_levels;
      (* All rungs open (the common steady state): reuse the persisted list by pointer so
         the sell leg's [open_levels @ ...] and its dedupe stay allocation-free. Only a
         genuinely missing rung forces a fresh split. *)
      if !missing_acc = []
      then state.persisted_sell_levels, []
      else (
        let open_acc = ref [] in
        List.iter
          (fun ((p, _) as level) -> if feed_open p then open_acc := level :: !open_acc)
          state.persisted_sell_levels;
        List.rev !open_acc, List.rev !missing_acc))
    else [], []
  in
  ( !open_buy_count_from_scan
  , !has_recent_amend_buy
  , !locked_in_buys
  , !locked_in_sells
  , (if !closest_sell_id = "" then None else Some (!closest_sell_id, !closest_sell_price))
  , open_persisted_levels
  , missing_persisted_levels )
;;

let compute_buy_ref_price ~bid_price ~ask_price =
  if bid_price > 0.0 then bid_price else ask_price
;;
