(* Suicide Grid - Strategy Execution Engine *)

open Strategy_common
open Suicide_grid_types
open Suicide_grid_config
open Suicide_grid_reservation
open Suicide_grid_orders

(** M16: price-key helper (rounded price*10000 as int) shared by the
    persisted-sell matching in [sync_open_orders] and the reconcile threading
    into [evaluate_sell_leg]. The int key keeps two within-tolerance prices in
    the same (or an adjacent) bucket without allocating a string per lookup. *)
let price_key p = int_of_float (Float.round (p *. 10000.0))

(** Performs 1-to-1 multiset matching between persisted sell levels and open sell orders.
    Returns (open_levels, missing_levels).
    M15: the previous implementation was O(n·m); for each persisted level it
    linearly rescanned the open-order list and allocated a match array. This
    version buckets open orders by a tolerance-rounded price key (Hashtbl) and
    verifies the original tolerance before consuming a candidate, so matching
    is ~O(n+m) with identical semantics.

    The bucket key is an int (price scaled to 4 decimals and rounded), NOT a
    [Printf.sprintf "%.4f"] string: the string key allocated a fresh string
    for every open order and every persisted level on every strategy
    execution, which dominated the Alpaca persisted-sell hotpath for assets
    with large sell grids (e.g. SPCX's 42 open sells). The rounded int keeps
    the same tolerance bucket (2 prices within the 1e-4 tolerance never span
    more than one rounded-decimal bucket), and the per-candidate tolerance
    check below preserves the original matching semantics exactly.

    M16: this partition is now only the fallback for direct [evaluate_sell_leg]
    callers; the strategy hot path builds the same open/missing split during
    [sync_open_orders]' scan and threads it through, so the per-tick reconcile
    is O(m) instead of this O(n+m) re-partition. *)
let partition_persisted_sell_levels persisted open_orders =
  (* Index open orders by rounded price key -> list of (price, remaining
     count). The tolerance check is preserved per candidate. *)
  let by_price : (int, (float * int) list) Hashtbl.t =
    Hashtbl.create (List.length open_orders)
  in
  List.iter
    (fun (_id, open_p, _open_q) ->
       let k = price_key open_p in
       let bucket = Option.value (Hashtbl.find_opt by_price k) ~default:[] in
       let rec bump acc = function
         | [] -> (open_p, 1) :: acc
         | (p, n) :: rest when p = open_p -> ((p, n + 1) :: rest) @ acc
         | item :: rest -> item :: bump acc rest
       in
       Hashtbl.replace by_price k (bump [] bucket))
    open_orders;
  let open_acc = ref [] in
  let missing_acc = ref [] in
  List.iter
    (fun ((target_p, _target_q) as level) ->
       let k = price_key target_p in
       (* Probe the bucket and its neighbors: the old string key used
          printf's %.4f rounding (half-even) while the int key uses
          [Float.round] (half-away), so a price sitting exactly on a
          4-decimal boundary can land in either adjacent bucket. The
          per-candidate tolerance check below is the authoritative gate. *)
       let matched =
         let rec try_buckets = function
           | [] -> None
           | bk :: rest ->
             (match Hashtbl.find_opt by_price bk with
              | Some bucket ->
                (* Verify against the original tolerance, not just the bucket key. *)
                let rec consume acc = function
                  | [] -> None
                  | (p, n) :: rest
                    when abs_float (p -. target_p) <= target_p *. 0.0001
                         || abs_float (p -. target_p) <= 1e-4 ->
                    if n > 1 then Some (((p, n - 1) :: rest) @ acc) else Some (rest @ acc)
                  | item :: rest -> consume (item :: acc) rest
                in
                (match consume [] bucket with
                 | Some nbucket -> Some (bk, nbucket)
                 | None -> try_buckets rest)
              | None -> try_buckets rest)
         in
         try_buckets [ k - 1; k; k + 1 ]
       in
       match matched with
       | Some (bk, nbucket) ->
         Hashtbl.replace by_price bk nbucket;
         open_acc := level :: !open_acc
       | None -> missing_acc := level :: !missing_acc)
    persisted;
  List.rev !open_acc, List.rev !missing_acc
;;

(** Reconciles the persisted-sell grid (Alpaca offline fill recovery). Computed
    once per execution and reused by the three persisted-sell branches (M15). *)
let reconcile_persisted_sell_levels ~state =
  partition_persisted_sell_levels state.persisted_sell_levels state.open_sell_orders
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
      | (_, _, _, ts) :: rest ->
        if now_time -. ts >= 10.0 then true else check_injected (count + 1) rest
    in
    check_injected 0 state.recently_injected_sells
  in
  if needs_sells_cleanup
  then (
    state.recently_injected_sells
    <- List.filter
         (fun (_, _, _, ts) -> now_time -. ts < 10.0)
         state.recently_injected_sells;
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
  (* M15: index the persisted sell levels by a rounded price key so each open
     sell order's match lookup is O(1) instead of rescanning the whole list.
     The previous [List.iteri] scan was O(n·m) per strategy execution (n open
     sell orders x m persisted levels), the dominant cost for assets with
     large sell grids like SPCX's 42 open sells. Buckets store
     (index, price, qty) so a 1-to-1 match consumes the entry and the
     original tolerance check and qty-update semantics are preserved. *)
  (* M16: matched persisted levels keyed by their price key -> count. Built
     during the scan (each open sell consumes exactly one persisted level, so
     a multiset of per-price counts accumulates), the open/missing split for
     the virtual-GTC reconcile falls out in O(m) at the end of the scan
     instead of re-partitioning the whole persisted-vs-open multiset
     ([partition_persisted_sell_levels]) a second time per execution. *)
  let matched_level_counts : (int, int) Hashtbl.t = Hashtbl.create 16 in
  let persisted_idx : (int, (int * float * float) list) Hashtbl.t = Hashtbl.create 16 in
  let build_persisted_idx () =
    Hashtbl.reset persisted_idx;
    List.iteri
      (fun idx (p, q) ->
         let k = price_key p in
         let bucket = Option.value (Hashtbl.find_opt persisted_idx k) ~default:[] in
         Hashtbl.replace persisted_idx k ((idx, p, q) :: bucket))
      state.persisted_sell_levels
  in
  build_persisted_idx ();
  let record_matched pk =
    Hashtbl.replace
      matched_level_counts
      pk
      (1 + Option.value (Hashtbl.find_opt matched_level_counts pk) ~default:0)
  in
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
          let k = price_key price in
          let match_entry =
            (* Probe the price's bucket and its immediate neighbors: the
               original linear scan matched any persisted level within
               tolerance, but grid levels are 0.25%+ apart while the
               tolerance is 0.01% (price*0.0001) or 1e-4 absolute, so a
               within-tolerance candidate is always the SAME grid level -
               the neighbor probes only absorb float rounding at the
               4-decimal bucket boundary. Pick the lowest-index candidate,
               mirroring the original scan order. *)
            let best = ref None in
            let consider_bucket bk =
              match Hashtbl.find_opt persisted_idx bk with
              | None -> ()
              | Some bucket ->
                List.iter
                  (fun (idx, p, _q) ->
                     if
                       (not (Hashtbl.mem matched_persisted_indices idx))
                       && (abs_float (p -. price) <= price *. 0.0001
                           || abs_float (p -. price) <= 1e-4)
                     then (
                       match !best with
                       | None -> best := Some (bk, (idx, p, _q))
                       | Some (_, (b_idx, _, _)) when idx < b_idx ->
                         best := Some (bk, (idx, p, _q))
                       | _ -> ()))
                  bucket
            in
            List.iter consider_bucket [ k - 1; k; k + 1 ];
            match !best with
            | Some (bk, (idx, _p, _q)) ->
              let remaining =
                match Hashtbl.find_opt persisted_idx bk with
                | Some b -> List.filter (fun (i, _, _) -> i <> idx) b
                | None -> []
              in
              Some (bk, idx, _p, _q, remaining)
            | None -> None
          in
          match match_entry with
          | Some (bk, idx, _existing_p, existing_q, remaining_bucket) ->
            Hashtbl.add matched_persisted_indices idx ();
            Hashtbl.replace persisted_idx bk remaining_bucket;
            (* M16: count the persisted level (keyed by ITS price) as matched. *)
            record_matched (price_key _existing_p);
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
            (* M16: the adopted level was matched by this open sell by
               construction - count it so the end-of-scan split keeps it on
               the open side. *)
            record_matched (price_key price);
            (* The list was re-sorted with a new level: rebuild the price
               index so later orders in this scan match against the current
               list (O(m), only on the rare adoption path). *)
            build_persisted_idx ();
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
      (fun (preserved_id, preserved_price, preserved_qty, _) ->
         let already_present =
           List.exists (fun (id, _, _) -> id = preserved_id) state.open_sell_orders
         in
         if not already_present
         then (
           state.open_sell_orders
           <- (preserved_id, preserved_price, preserved_qty) :: state.open_sell_orders;
           locked_in_sells := !locked_in_sells +. preserved_qty;
           if ecfg.remaintain_expired_sells
           then (
             let k = price_key preserved_price in
             record_matched k)))
      preserved_sells;
  (* M16: split the final persisted list into open/missing by draining the
     per-price-key match counts (multiset semantics - duplicate levels at the
     same price each consume one count, exactly mirroring
     [partition_persisted_sell_levels]' 1-to-1 matching). The result is what
     [evaluate_sell_leg]'s reconcile used to re-derive with a full O(n+m)
     partition over the open orders; here it is O(m) on data this scan already
     touched. *)
  let open_levels_acc = ref [] in
  let missing_levels_acc = ref [] in
  List.iter
    (fun ((p, q) as level) ->
       let k = price_key p in
       let rounded_q = round_qty q asset.symbol asset.exchange in
       if rounded_q <= 0.0
       then ()
       else (
         match Hashtbl.find_opt matched_level_counts k with
         | Some n when n > 0 ->
           Hashtbl.replace matched_level_counts k (n - 1);
           open_levels_acc := level :: !open_levels_acc
         | _ -> missing_levels_acc := level :: !missing_levels_acc))
    state.persisted_sell_levels;
  ( !open_buy_count_from_scan
  , !has_recent_amend_buy
  , !locked_in_buys
  , !locked_in_sells
  , !closest_sell_order
  , !best_buy_qty
  , List.rev !open_levels_acc
  , List.rev !missing_levels_acc )
;;

let compute_buy_ref_price ~bid_price ~ask_price =
  if bid_price > 0.0 then bid_price else ask_price
;;

(** Evaluates buy-triggered and Alpaca-exclusive inventory-maintenance sell
    placement leg.
    [persisted_reconcile] is the (open_levels, missing_levels) split that
    [sync_open_orders] computed during its open-order scan (M16), so the
    Alpaca virtual-GTC reconcile never re-partitions the persisted-vs-open
    multiset a second time per execution.

    Sell trigger semantics: a sell is attempted when a buy is placed
    ([buy_attempted]) or filled ([just_filled_buy] - the 1-buy x multi-sell
    ladder), and the trigger is OWED until the sell is actually placed. Only a
    placed sell or a verified nothing-to-sell (known balance below the venue
    floor) consumes the trigger - transient blockers (cooldown, asset_low, a
    NaN balance snapshot, an in-flight sell placement) do not, so the sell
    retries every tick even when there is no capital to replace the buy
    (capital exhausted / oracle-halted) and even when the buy placement tick
    itself was blocked.

    Sell sizing: accumulation venues (Hyperliquid/Lighter/IBKR) size the sell
    PURELY by the non-accrued inventory = available balance - reserved_base.
    The venue's available balance is tradeable (total - hold from open
    orders), so resting-sell holds are already netted and locked_in_sells is
    NOT subtracted again - subtracting it double-counted the hold and
    understated the inventory below the floor, blocking the sell. Non-
    accumulation venues size by qty * sell_mult (Kraken), clamped to the
    sellable inventory. The result must clear the VENUE MINIMUM
    ([cached_venue_min_qty] and [cached_venue_min_notional]; Alpaca's minimum
    is a dollar notional). The venue minimum is the exchange's minimum
    accepted order size - entirely separate from the grid's configured order
    [qty]. *)
let evaluate_sell_leg
      ~persisted_reconcile
      ~state
      ~now
      ~(asset : trading_config)
      ~bid_price
      ~ask_price:_
      ~asset_balance
      ~buy_attempted
      ~ecfg
      ~locked_in_sells
  =
  let missing_after_reconcile = ref [] in
  let pruned_missing = ref [] in
  if state.persisted_sell_levels <> []
  then (
    let open_levels, missing_levels = persisted_reconcile in
    if not (Float.is_nan asset_balance)
    then (
      let available_for_missing_sells =
        max
          0.0
          (asset_balance
           +. state.anticipated_base_credit
           -. state.reserved_base
           -. locked_in_sells)
      in
      let missing_desc =
        List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) missing_levels
      in
      let rem_avail = ref available_for_missing_sells in
      let kept_missing = ref [] in
      let pruned = ref [] in
      List.iter
        (fun ((_target_p, target_q) as level) ->
           let rounded_q = round_qty target_q asset.symbol asset.exchange in
           if rounded_q > 0.0 && !rem_avail >= rounded_q -. 1e-6
           then (
             kept_missing := level :: !kept_missing;
             rem_avail := max 0.0 (!rem_avail -. rounded_q))
           else pruned := level :: !pruned)
        missing_desc;
      let new_persisted = open_levels @ List.rev !kept_missing in
      state.persisted_sell_levels
      <- List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) new_persisted;
      missing_after_reconcile := List.rev !kept_missing;
      pruned_missing := !pruned)
    else missing_after_reconcile := missing_levels);
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
  let missing_lvl_check =
    List.filter
      (fun (_p, q) -> round_qty q asset.symbol asset.exchange > 0.0)
      !missing_after_reconcile
  in
  let active_sell = has_active_sell state in
  let should_trigger_sell =
    (not active_sell)
    && (state.just_filled_buy
        || buy_attempted
        || state.resuming_after_balance_flag
        || missing_lvl_check <> []
        || (state.open_sell_orders = [] && Option.is_some state.last_buy_fill_price))
  in
  let is_sell_on_cooldown = Hashtbl.mem state.amend_cooldowns "place_Sell" in
  let sell_pushed = ref false in
  let nothing_placeable = ref false in
  if not should_trigger_sell
  then (
    if buy_attempted || state.just_filled_buy
    then
      Logging.info_f
        ~section
        "Sell placement not triggered for %s (active_sell=%B, just_filled_buy=%B, \
         buy_attempted=%B, resuming=%B, missing_levels=%d, open_sells=%d, \
         has_last_buy_fill=%B)"
        asset.symbol
        active_sell
        state.just_filled_buy
        buy_attempted
        state.resuming_after_balance_flag
        (List.length missing_lvl_check)
        (List.length state.open_sell_orders)
        (Option.is_some state.last_buy_fill_price))
  else if Float.is_nan asset_balance
  then
    Logging.debug_f
      ~section
      "Sell placement skipped for %s: asset_balance is NaN"
      asset.symbol
  else if active_sell
  then
    Logging.debug_f
      ~section
      "Sell placement skipped for %s: active sell in flight (inflight_sell=%B, \
       in_flight_orders=%B)"
      asset.symbol
      state.inflight_sell
      (InFlightOrders.is_in_flight state.duplicate_key_sell)
  else if state.asset_low
  then
    Logging.debug_f
      ~section
      "Sell placement skipped for %s: asset_low is true"
      asset.symbol
  else if is_sell_on_cooldown
  then
    Logging.debug_f
      ~section
      "Sell placement skipped for %s: place_Sell is on cooldown"
      asset.symbol
  else (
    let asset_bal = asset_balance in
    let grid_interval = asset.grid_interval in
    let oracle_qty = venue_lot_qty state.grid_qty asset.exchange state in
    let sell_mult = state.cached_sell_mult in
    (* Determine target price & qty for sell placement *)
    let target_sell_price_opt, target_sell_qty_override =
      if state.persisted_sell_levels <> []
      then (
        let missing_sorted_desc =
          List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) missing_lvl_check
        in
        match missing_sorted_desc with
        | (tp, tq) :: _ ->
          let rounded_tq = round_qty tq asset.symbol asset.exchange in
          if rounded_tq > 0.0 then Some tp, Some tq else None, None
        | [] -> None, None)
      else None, None
    in
    let sell_price =
      match target_sell_price_opt with
      | Some tp -> tp
      | None ->
        let base_price_for_sell =
          match state.last_buy_fill_price with
          | Some fill_p -> fill_p
          | None -> bid_price
        in
        calculate_grid_price base_price_for_sell grid_interval true state
    in
    let available =
      if ecfg.use_accumulation_sells
      then
        Float.max 0.0 (asset_bal +. state.anticipated_base_credit -. state.reserved_base)
      else
        Float.max
          0.0
          (asset_bal
           +. state.anticipated_base_credit
           -. state.reserved_base
           -. locked_in_sells)
    in
    let effective_sell_qty, balance_ok =
      match target_sell_qty_override with
      | Some tq when tq > 0.0 ->
        let target_q =
          if ecfg.sell_uses_mult
          then
            Float.min tq (round_qty (oracle_qty *. sell_mult) asset.symbol asset.exchange)
          else tq
        in
        let rounded_tq = round_qty target_q asset.symbol asset.exchange in
        if available >= rounded_tq -. 1e-6 && rounded_tq > 0.0
        then rounded_tq, true
        else (
          Logging.info_f
            ~section
            "Sell order blocked for %s: available %.8f < target_q %.8f (bal %.8f, \
             reserved %.8f, locked %.8f)"
            asset.symbol
            available
            rounded_tq
            asset_bal
            state.reserved_base
            locked_in_sells;
          0.0, false)
      | _ ->
        (* Only genuine accumulation venues (Hyperliquid/Lighter/IBKR,
           [use_accumulation_sells]) size the sell to the ENTIRE remaining
           non-accrued inventory - that is the accumulation-ladder rung and it
           is bounded by the venue's tradeable balance (already net of holds)
           minus [reserved_base].

           Alpaca (and other non-accumulation venues) must NOT take this path:
           Alpaca is a 1:1 venue ([sell_uses_mult] false, no sell_mult
           retention), and treating it as an accumulation venue sized the sell
           to the whole non-accrued inventory here, placing a stack of
           full-position sell orders (one per buy fill) that together
           represented the ENTIRE holding, leaving only [reserved_base] - i.e.
           it "sold the entire holdings" instead of one grid lot.
           Non-accumulation venues size to one grid lot (Alpaca 1:1, Kraken
           qty*sell_mult), clamped to the sellable inventory. *)
        if ecfg.use_accumulation_sells
        then (
          let rounded = round_qty available asset.symbol asset.exchange in
          if rounded > 0.0
          then rounded, true
          else (
            Logging.info_f
              ~section
              "Sell order blocked for %s: non-accrued inventory %.8f (bal %.8f + \
               anticipated %.8f - reserved %.8f) rounds below one lot"
              asset.symbol
              available
              asset_bal
              state.anticipated_base_credit
              state.reserved_base;
            0.0, false))
        else (
          let desired_qty =
            if ecfg.sell_uses_mult
            then round_qty (oracle_qty *. sell_mult) asset.symbol asset.exchange
            else round_qty oracle_qty asset.symbol asset.exchange
          in
          if available >= desired_qty -. 1e-6 && desired_qty > 0.0
          then desired_qty, true
          else if available > 0.0
          then (
            let rounded_avail = round_qty available asset.symbol asset.exchange in
            if rounded_avail > 0.0
            then rounded_avail, true
            else (
              Logging.info_f
                ~section
                "Sell order blocked for %s: available %.8f < sell_qty %.8f (bal %.8f, \
                 reserved %.8f, locked %.8f)"
                asset.symbol
                available
                desired_qty
                asset_bal
                state.reserved_base
                locked_in_sells;
              0.0, false))
          else (
            Logging.info_f
              ~section
              "Sell order blocked for %s: available %.8f <= 0 (bal %.8f, reserved %.8f, \
               locked %.8f)"
              asset.symbol
              available
              asset_bal
              state.reserved_base
              locked_in_sells;
            0.0, false))
    in
    if balance_ok
    then (
      let venue_min_ok q =
        q >= state.cached_venue_min_qty -. 1e-9
        && (state.cached_venue_min_notional <= 0.0
            || q *. sell_price >= state.cached_venue_min_notional -. 1e-9)
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
          if ecfg.remaintain_expired_sells && target_sell_price_opt = None
          then (
            state.persisted_sell_levels
            <- List.sort
                 (fun (p1, _) (p2, _) -> Float.compare p2 p1)
                 ((sell_price, effective_sell_qty) :: state.persisted_sell_levels);
            state.persistence_dirty <- true);
          Logging.info_f
            ~section
            "Placed sell order for %s: %.8f @ %.4f"
            asset.symbol
            effective_sell_qty
            sell_price))
      else (
        Logging.info_f
          ~section
          "Sell order blocked for %s: no sellable inventory clears the venue minimum \
           (venue_min_qty %.8f, venue_min_notional %.4f, sell_price %.4f, \
           effective_sell_qty %.8f, available %.8f)"
          asset.symbol
          state.cached_venue_min_qty
          state.cached_venue_min_notional
          sell_price
          effective_sell_qty
          available;
        nothing_placeable := true))
    else nothing_placeable := true);
  if !sell_pushed || !nothing_placeable
  then state.just_filled_buy <- false
  else if buy_attempted && not state.just_filled_buy
  then state.just_filled_buy <- true;
  state.resuming_after_balance_flag <- false
;;

(** Evaluates buy placement, multi-buy cancellation, and buy trailing. *)
let evaluate_buy_leg
      ~persisted_reconcile
      ~asset_balance
      ~ecfg
      ~locked_in_sells
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
        (* An amend on Alpaca is cancel+create under the hood: while it is in
           flight the open-order scan transiently lists both the old id and
           the replacement, which trips the ">1 buys" branch below. Cancelling
           the old id then races the amend (the cancel is ignored or bounced),
           so skip orders that are mid-amendment - the amend replaces them. *)
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
            create_cancel_order order_id asset.symbol Grid asset.exchange
          in
          ignore (push_order ~now ~state cancel_order);
          Logging.info_f
            ~section
            "Cancelling excess buy order: %s for %s"
            order_id
            asset.symbol)));
    state.last_buy_order_id <- None;
    state.last_buy_order_price <- None;
    state.last_cycle <- cycle)
  else if effective_buy_count = 0 && not buy_order_pending
  then (
    let ref_price = compute_buy_ref_price ~bid_price ~ask_price in
    let raw_buy_price = calculate_grid_price ref_price grid_interval false state in
    let buy_price =
      let price =
        if bid_price > 0.0 then min raw_buy_price bid_price else raw_buy_price
      in
      state.cached_round_price price
    in
    (* A fresh buy must respect the same 2x-grid-interval spacing below the
       closest resting sell that the trailing leg enforces via [exact_target]
       (sell_price - 2*gi of the SELL): without it a buy placed after a fill
       can sit too close to the lowest sell (a ~1x rung the grid never allows
       when it amends). As in the trailing leg the clamp is PRICE-INDEPENDENT:
       while a sell is tracked by order management the fresh buy is kept at
       least 2*gi below it; the clamp is released only when the sell is
       removed from tracking. *)
    let buy_price =
      match closest_sell_order_initial with
      | Some (_, sell_price) ->
        state.cached_round_price
          (min buy_price (sell_price -. (sell_price *. (2.0 *. grid_interval /. 100.0))))
      | None -> buy_price
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
            (* A fresh buy is placed at the current sizing target, so any
               pending re-anchor is satisfied. *)
            state.force_buy_reanchor <- false;
            Logging.info_f
              ~section
              "Placed buy order for %s: %.8f @ %.4f"
              asset.symbol
              qty
              buy_price;
            evaluate_sell_leg
              ~persisted_reconcile
              ~state
              ~now
              ~asset
              ~bid_price
              ~ask_price
              ~asset_balance
              ~buy_attempted:true
              ~ecfg
              ~locked_in_sells))
        else (
          let cooldown_key = "place_Buy" in
          if not (Hashtbl.mem state.amend_cooldowns cooldown_key)
          then
            if quote_balance_stale
            then (
              (* The balance snapshot is stale: the local figure may be
                 wrong, so the attempt is still worthwhile - the exchange's
                 verdict is the truth. Mark the attempt as knowingly
                 under-funded so the (expected) rejection does not latch
                 capital_low on a foreordained outcome. *)
              Logging.warn_f
                ~section
                "Local balance low for %s buy (need %.2f, available %.2f, balance \
                 snapshot stale) - attempting anyway, exchange will reject if truly \
                 insufficient"
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
                state.last_buy_order_price <- Some buy_price;
                evaluate_sell_leg
                  ~persisted_reconcile
                  ~state
                  ~now
                  ~asset
                  ~bid_price
                  ~ask_price
                  ~asset_balance
                  ~buy_attempted:true
                  ~ecfg
                  ~locked_in_sells))
            else (
              (* Fresh balance, genuinely insufficient: do not send an order
                 that is guaranteed to be rejected. Pause buying via
                 capital_low until the quote balance recovers (the recovery
                 path clears it on a balance increase). *)
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
    (* A qty mismatch is judged with a RELATIVE tolerance (0.1% of the
       config qty), not an absolute 1e-6: the capital oracle re-derives
       the qty from the live pool every pass, and pool drift means
       successive passes publish micro-different qtys (e.g. 0.03877239 ->
       0.03877509 -> 0.03876709). An absolute 1e-6 tolerance treated each
       micro change as a re-size and amended the resting buy every pass -
       an Alpaca amend is a cancel+create, so this churned order ids and
       raced fills/executions (stacking at grid levels). Only a material
       (>0.1%) qty difference amends now; micro drift is left alone. *)
    let qty_mismatch =
      is_alpaca
      && pending_buy_qty_from_scan > 0.0
      && abs_float (pending_buy_qty_from_scan -. qty) > max (qty *. 0.001) 1e-6
    in
    if closest_sell_order_val <> None
    then (
      match
        closest_sell_order_val, state.last_buy_order_price, state.last_buy_order_id
      with
      | Some (_sell_order_id, sell_price), Some current_buy_price, Some buy_order_id ->
        (* The 2*gi separation from the closest sell is anchored on the SELL
           order and is PRICE-INDEPENDENT: while a sell is tracked by order
           management, the buy never trails above sell - 2*gi (it never enters
           the restricted zone below that sell), no matter where the perceived
           top of book sits. The price can dislocate randomly above a resting
           sell without filling it - the ladder must not let the buy cross a
           sell that still exists. The clamp is released only when the sell is
           removed from tracking (an order-management fill/cancel/expiry);
           only then does the buy trail the top of book at the grid interval.
           ([sell_price] is always a positive resting-order price, so there is
           no zero-reference hazard.) *)
        let double_grid_interval = sell_price *. (2.0 *. grid_interval /. 100.0) in
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
        let min_move_threshold = get_min_move_threshold state.cached_price_increment in
        (* A sizing re-anchor (the capital oracle published a changed grid
           interval - flagged by the domain worker on [force_buy_reanchor])
           amends the resting buy to the new spacing in BOTH directions.
           Normal trailing only moves the buy UP, so a widened grid interval
           would otherwise leave the book running at the old, tighter spacing
           (buy too close to the market, rungs not 2*gi apart) until the
           market happens to rise. A qty-only oracle change does NOT re-anchor
           the price: the grid adopts the new size (Alpaca qty mismatch
           amend) or on the next placement, and the resting price only trails
           up. *)
        let reanchor_buy = state.force_buy_reanchor in
        if target_buy_price > current_buy_price || qty_mismatch || reanchor_buy
        then (
          (* A qty-only mismatch (the oracle re-derived the size from a
             churning pool; the spacing is unchanged) corrects the QTY and
             keeps the resting PRICE - the buy only ever trails up, exactly
             like normal trailing. Only a target above the resting price (a
             genuine trail-up) or a real gi re-anchor moves the price, and the
             min-move deadband still applies. This stops the grid and the
             oracle from fighting over the resting buy every pass (cancel+
             create churn on Alpaca). *)
          let effective_amend_price =
            if qty_mismatch && not reanchor_buy
            then Float.max current_buy_price target_buy_price
            else target_buy_price
          in
          let effective_price_rounded = state.cached_round_price effective_amend_price in
          let effective_price_diff =
            state.cached_round_price
              (abs_float (effective_amend_price -. current_buy_price_rounded))
          in
          let price_moves = effective_price_rounded <> current_buy_price_rounded in
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
              (* A pure qty correction (price unchanged) is always worth
                 sending; a qty correction that also trails the price up
                 respects the min-move deadband like normal trailing. *)
              (not is_being_amended)
              && (not is_in_flight)
              && (not is_on_cooldown)
              && ((not price_moves) || effective_price_diff >= min_move_threshold))
            else
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
            (* An amend REPLACES the resting buy (cancel+create on Alpaca):
               the capital already committed to that buy is released and
               re-committed at the new price, so the affordability check
               must add the committed notional (locked_in_buys, the sum of
               price*qty over the open buys) back to the available balance.
               Without this the grid falsely reports "Insufficient quote
               balance" when trailing a funded buy up on committed capital
               (e.g. HYPE pool $13.75 + committed $18.90 vs need $20.38). *)
            let available_for_amend = quote_bal +. locked_in_buys in
            if
              (not (Float.is_nan quote_balance))
              && can_place_buy_order qty available_for_amend quote_needed
            then (
              let order =
                create_amend_order
                  buy_order_id
                  asset.symbol
                  Buy
                  qty
                  (Some effective_price_rounded)
                  true
                  Grid
                  asset.exchange
              in
              ignore (push_order ~now ~state order);
              state.last_buy_order_price <- Some effective_price_rounded;
              state.force_buy_reanchor <- false;
              if qty_mismatch
              then
                Logging.info_f
                  ~section
                  "Alpaca pending buy order %s qty (%.8f) differs from config (%.8f) - \
                   amending to qty %.8f%s"
                  buy_order_id
                  pending_buy_qty_from_scan
                  qty
                  qty
                  (if price_moves
                   then
                     Printf.sprintf
                       " and trailing price up to %.4f"
                       effective_price_rounded
                   else " (price unchanged)");
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
            (* The re-anchor target is already where the buy sits (within the
             min-move threshold): nothing to amend, the sizing is applied. *))
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
        (* Sizing re-anchor (see the with-sell branch): amend the resting buy
           to the oracle's target in both directions. *)
        let reanchor_buy = state.force_buy_reanchor in
        if target_buy_price > current_buy_price || qty_mismatch || reanchor_buy
        then (
          (* A qty-only mismatch corrects the QTY and keeps the resting PRICE
             (the buy only ever trails up, like normal trailing); only a
             target above the resting price or a real gi re-anchor moves the
             price, and the min-move deadband still applies (see the with-sell
             branch). *)
          let effective_amend_price =
            if qty_mismatch && not reanchor_buy
            then Float.max current_buy_price target_buy_price
            else target_buy_price
          in
          let effective_price_rounded = state.cached_round_price effective_amend_price in
          let effective_price_diff =
            state.cached_round_price
              (abs_float (effective_amend_price -. current_buy_price_rounded))
          in
          let price_moves = effective_price_rounded <> current_buy_price_rounded in
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
              (* A pure qty correction (price unchanged) is always worth
                 sending; a qty correction that also trails the price up
                 respects the min-move deadband like normal trailing. *)
              (not is_being_amended)
              && (not is_in_flight)
              && (not is_on_cooldown)
              && ((not price_moves) || effective_price_diff >= min_move_threshold))
            else
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
            (* See the with-sell branch: an amend releases the committed
               capital of the resting buy it replaces, so that committed
               notional is added back to the available balance before the
               affordability check (fixes the false "insufficient quote
               balance" warning when trailing a funded buy up). *)
            let available_for_amend = quote_bal +. locked_in_buys in
            if
              (not (Float.is_nan quote_balance))
              && can_place_buy_order qty available_for_amend quote_needed
            then (
              let order =
                create_amend_order
                  buy_order_id
                  asset.symbol
                  Buy
                  qty
                  (Some effective_price_rounded)
                  true
                  Grid
                  asset.exchange
              in
              ignore (push_order ~now ~state order);
              state.last_buy_order_price <- Some effective_price_rounded;
              state.force_buy_reanchor <- false;
              if qty_mismatch
              then
                Logging.info_f
                  ~section
                  "Alpaca pending buy order %s qty (%.8f) differs from config (%.8f) - \
                   amending to qty %.8f%s"
                  buy_order_id
                  pending_buy_qty_from_scan
                  qty
                  qty
                  (if price_moves
                   then
                     Printf.sprintf
                       " and trailing price up to %.4f"
                       effective_price_rounded
                   else " (price unchanged)");
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
    state.last_cycle <- cycle)
  else state.last_cycle <- cycle;
  !buy_attempted
;;

(** Main strategy execution loop. [quote_balance_stale] is set by the caller
    (domain worker) when the quote-balance snapshot is older than the
    staleness threshold: a stale snapshot is not authoritative, so an
    under-funded buy is still attempted (the exchange's verdict is the
    truth); a fresh snapshot that cannot fund the buy is skipped outright
    instead of being sent to be rejected.

    [oracle_halted] (the capital oracle published this asset INACTIVE) gates
    ONLY the buy leg - no new buy placement, no buy trailing/amending (a
    halted asset must not commit more quote capital). The SELL leg always
    runs: a sell needs only inventory, not quote, so the sell for a
    just-filled buy is placed even when capital is exhausted and the asset is
    halted - the account's capital-recovery path. Without this the last
    fill's inventory would sit unreclaimable. *)
let execute_strategy
      ?cached_state
      ?(quote_balance_stale = false)
      ?(oracle_halted = false)
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
    (* Venue minimums (the exchange's minimum accepted order size), resolved
       once at init: the base-quantity floor [cached_venue_min_qty] and the
       quote-notional floor [cached_venue_min_notional]. These are the floors
       every order must clear - separate from the grid's configured [qty]. *)
    state.cached_venue_min_qty
    <- (match get_exchange_module asset.exchange with
        | Some (module Ex : Exchange.S) ->
          Option.value (Ex.get_qty_min ~symbol:asset.symbol) ~default:1.0
        | None -> 1.0);
    state.cached_venue_min_notional <- get_min_notional_val asset.symbol asset.exchange;
    state.cached_sell_mult
    <- (try float_of_string asset.sell_mult with
        | _ -> 1.0);
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
             , pending_buy_qty_from_scan
             , open_persisted_levels
             , missing_persisted_levels )
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
           (* Oracle-halted: no buy placement, no buy trailing/amending - a
               halted asset must not commit more quote capital. The sell leg
               still runs (see the [oracle_halted] doc on execute_strategy). *)
           let buy_attempted =
             if oracle_halted
             then false
             else
               evaluate_buy_leg
                 ~persisted_reconcile:(open_persisted_levels, missing_persisted_levels)
                 ~asset_balance
                 ~ecfg
                 ~locked_in_sells
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
                 ~pending_buy_qty_from_scan
           in
           evaluate_sell_leg
             ~persisted_reconcile:(open_persisted_levels, missing_persisted_levels)
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
