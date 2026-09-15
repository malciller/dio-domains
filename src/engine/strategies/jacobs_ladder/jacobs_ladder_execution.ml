(* Jacobs Ladder: strategy execution engine. *)

open Strategy_common
open Jacobs_ladder_types
open Strategy_venue
open Strategy_reservation
open Strategy_orders
include Strategy_lifecycle
include Strategy_decision

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
