(* Capital reservation and accumulation tracking (strategy-agnostic). *)

open Strategy_common
open Strategy_state

(* Moved to Platform_accounting (milestone 2). *)
let total_reserved_by_exchange = Platform_accounting.total_reserved_by_exchange
let get_exchange_reserved_atomic = Platform_accounting.get_exchange_reserved_atomic

let get_total_reserved_quote state =
  let a =
    match state.exchange_reserved_atomic with
    | Some a -> a
    | None ->
      let atm = get_exchange_reserved_atomic state.exchange_id in
      state.exchange_reserved_atomic <- Some atm;
      atm
  in
  Atomic.get a
;;

(* Moved to Platform_accounting (milestone 2). *)
let atomic_add = Platform_accounting.atomic_add

(** Sets [state.reserved_quote]; applies the delta to the exchange atomic. *)
let set_asset_reserved_quote state v =
  let diff = v -. state.reserved_quote in
  state.reserved_quote <- v;
  if state.exchange_id <> ""
  then (
    let a =
      match state.exchange_reserved_atomic with
      | Some a -> a
      | None ->
        let atm = get_exchange_reserved_atomic state.exchange_id in
        state.exchange_reserved_atomic <- Some atm;
        atm
    in
    atomic_add a diff)
;;

(** Atomically reserves [reserve_amount] when available quote >= [quote_needed]. Returns
    (balance_ok, available_quote, total_reserved). *)
let atomic_check_and_reserve state quote_bal quote_needed reserve_amount =
  let a =
    match state.exchange_reserved_atomic with
    | Some a -> a
    | None ->
      let atm = get_exchange_reserved_atomic state.exchange_id in
      state.exchange_reserved_atomic <- Some atm;
      atm
  in
  let diff = reserve_amount -. state.reserved_quote in
  let rec attempt () =
    let total_reserved = Atomic.get a in
    let available = quote_bal -. total_reserved in
    if available >= quote_needed
    then
      if state.exchange_id <> ""
      then
        if Atomic.compare_and_set a total_reserved (total_reserved +. diff)
        then (
          state.reserved_quote <- reserve_amount;
          true, available, total_reserved)
        else attempt ()
      else (
        state.reserved_quote <- reserve_amount;
        true, available, total_reserved)
    else false, available, total_reserved
  in
  attempt ()
;;

(** True iff [quote_balance] >= [quote_needed]. *)
let can_place_buy_order (_qty : float) quote_balance quote_needed =
  quote_balance >= quote_needed
;;

(** True if an amendment of [order_id] is permitted. *)
let amend_allowed
  ~state
  ~order_id
  ~target_price
  ~current_price_rounded
  ~price_diff
  ~min_move_threshold
  =
  let is_being_amended =
    List.exists
      (fun (id, _, _, _) ->
        String.starts_with ~prefix:"pending_amend_" id
        && String.sub id 14 (String.length id - 14) = order_id)
      state.pending_orders
  in
  let is_in_flight = InFlightAmendments.is_in_flight order_id in
  let is_on_cooldown = Hashtbl.mem state.amend_cooldowns order_id in
  (not is_being_amended)
  && (not is_in_flight)
  && (not is_on_cooldown)
  && price_diff >= min_move_threshold
  && target_price <> current_price_rounded
;;

(** True iff [asset_balance] >= [asset_needed]. *)
let can_place_sell_order (_qty : float) asset_balance asset_needed =
  asset_balance >= asset_needed
;;

(** True if a sell placement is in flight or registered in InFlightOrders.
    [handle_order_acknowledged] releases the duplicate key on completion, so a resting
    sell does not report active here; the inventory gate (sellable base >= sell qty)
    prevents duplicate sells. *)
let has_active_sell state =
  state.inflight_sell || InFlightOrders.is_in_flight state.duplicate_key_sell
;;

(* ------------------------------------------------------------------ *)
(* Sell-commitment ledger *)
(* *)
(* Single source of truth for base committed to a sell on any venue. *)
(* Sellable base = spot_holding - reserved_base - committed_sell_base. *)
(* Only [spot_holding] is venue-specific (on Alpaca, the venue's own *)
(* free figure). A resting or in-flight sell's base is never offered *)
(* again regardless of the open-order feed, so a dropped live order *)
(* (reconnect/snapshot truncation) cannot make its base look free. *)
(* ------------------------------------------------------------------ *)

(** Seconds a dispatched sell with no venue confirmation is kept as committed base.
    Applies only to a never-acked, never-seen (lost) dispatch; an acked order is kept
    until its terminal event. *)
let sell_commitment_in_flight_timeout_s = 120.0

(** Bumps the commitment generation, invalidating the [committed_sell_base] fold cache.
    Called on every [sell_commitments] mutation. *)
let[@inline] touch_commitments state =
  state.sell_commitments_gen <- state.sell_commitments_gen + 1
;;

(** Inserts or updates a commitment, preserving the earliest arm time. Id-keyed hashtable:
    [sync_open_orders] calls this once per open sell per execution with the feed's live
    price/qty and seen=acked=true; lookup/update is O(1) and a steady-state no-op writes
    nothing. *)
let upsert_sell_commitment ~state ~id ~price ~qty ~seen ~acked =
  let dirty =
    match Hashtbl.find_opt state.sell_commitments id with
    | Some c ->
      let changed =
        c.sc_price <> price
        || c.sc_qty <> qty
        || (seen && not c.sc_seen)
        || (acked && not c.sc_acked)
      in
      if changed
      then (
        c.sc_price <- price;
        c.sc_qty <- qty;
        c.sc_seen <- c.sc_seen || seen;
        c.sc_acked <- c.sc_acked || acked);
      if seen then c.sc_listed <- true;
      changed
    | None ->
      Hashtbl.replace
        state.sell_commitments
        id
        { sc_price = price
        ; sc_qty = qty
        ; sc_seen = seen
        ; sc_acked = acked
        ; sc_listed = seen
        ; sc_armed = Unix.gettimeofday ()
        };
      true
  in
  (* A locally-armed (unseen) commitment is unlisted until a scan adopts it, so the
     reconcile in [sync_open_orders] must run. Invalidate the skip flag; a full scan
     re-proves it. *)
  if not seen then state.sell_commitments_clean <- false;
  (* Only invalidate the [committed_sell_base] cache when the sum could have changed. A
     steady-state delta re-reports the same price/qty for a resting sell; bumping the
     generation unconditionally forced that O(commitments) fold on every delta cycle. *)
  if dirty then touch_commitments state
;;

(** Records a just-dispatched sell (keyed by its temporary pending id). *)
let arm_sell_commitment ~state ~id ~price ~qty =
  if qty > 0.0 then upsert_sell_commitment ~state ~id ~price ~qty ~seen:false ~acked:false
;;

(** Rekeys a commitment from [old_id] to [new_id] on ack/amend. An unknown [old_id] (order
    adopted from the feed) inserts the new id. [acked] marks the order venue-accepted, so
    the dispatch window never expires it. *)
let rekey_sell_commitment ~state ~old_id ~new_id ~price ~qty ~acked =
  let q = if qty > 0.0 then qty else 0.0 in
  if old_id = new_id
  then (
    match Hashtbl.find_opt state.sell_commitments old_id with
    | Some c ->
      upsert_sell_commitment
        ~state
        ~id:new_id
        ~price
        ~qty:(if q > 0.0 then q else c.sc_qty)
        ~seen:c.sc_seen
        ~acked:(c.sc_acked || acked)
    | None -> upsert_sell_commitment ~state ~id:new_id ~price ~qty:q ~seen:false ~acked)
  else (
    match Hashtbl.find_opt state.sell_commitments old_id with
    | Some c ->
      Hashtbl.remove state.sell_commitments old_id;
      Hashtbl.remove state.sell_commitments new_id;
      c.sc_price <- price;
      c.sc_qty <- (if q > 0.0 then q else c.sc_qty);
      c.sc_acked <- c.sc_acked || acked;
      Hashtbl.replace state.sell_commitments new_id c
    | None -> upsert_sell_commitment ~state ~id:new_id ~price ~qty:q ~seen:false ~acked);
  touch_commitments state
;;

(** Terminal event: the sell no longer holds base. *)
let remove_sell_commitment ~state ~id =
  Hashtbl.remove state.sell_commitments id;
  touch_commitments state
;;

(** Terminal event for a placement that never got a venue id. *)
let remove_pending_sell_commitments ~state =
  let pending =
    Hashtbl.fold
      (fun id _ acc ->
        if String.starts_with ~prefix:"pending_sell_" id then id :: acc else acc)
      state.sell_commitments
      []
  in
  List.iter (Hashtbl.remove state.sell_commitments) pending;
  if pending <> [] then touch_commitments state
;;

(** Total base committed to live sells (the ledger). Cached across calls; the fold is
    O(commitments) and this is read several times per cycle. *)
let committed_sell_base state =
  if state.committed_sell_base_gen = state.sell_commitments_gen
  then state.committed_sell_base_cache
  else (
    let t = Hashtbl.fold (fun _ c acc -> acc +. c.sc_qty) state.sell_commitments 0.0 in
    state.committed_sell_base_cache <- t;
    state.committed_sell_base_gen <- state.sell_commitments_gen;
    t)
;;

(* Moved to Platform_accounting (milestone 2). *)
let effective_committed_sell_base ~ecfg ~ledger_total ~feed_total ~unnetted_hold =
  Platform_accounting.effective_committed_sell_base
    ~balance_nets_open_order_holds:ecfg.balance_nets_open_order_holds
    ~hold_netted_from_venue_state:ecfg.hold_netted_from_venue_state
    ~ledger_total
    ~feed_total
    ~unnetted_hold
;;
