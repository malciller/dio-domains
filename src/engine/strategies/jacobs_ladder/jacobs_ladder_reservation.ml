(* Jacobs Ladder - Capital Reservation & Accumulation Tracking *)

open Strategy_common
open Jacobs_ladder_types

(** Tracking total reserved quote per exchange to avoid O(N) strategy_states locking. *)
let total_reserved_by_exchange =
  Atomic.make
    (List.fold_left
       (fun acc ex -> Strategy_common.StringMap.add ex (Atomic.make 0.0) acc)
       Strategy_common.StringMap.empty
       [ "kraken"; "hyperliquid"; "lighter"; "ibkr" ])
;;

(** Gets the cached total reserved quote atomic for [exchange]. *)
let rec get_exchange_reserved_atomic exchange =
  let map = Atomic.get total_reserved_by_exchange in
  match Strategy_common.StringMap.find_opt exchange map with
  | Some a -> a
  | None ->
    let a3 = Atomic.make 0.0 in
    let new_map = Strategy_common.StringMap.add exchange a3 map in
    if Atomic.compare_and_set total_reserved_by_exchange map new_map
    then a3
    else get_exchange_reserved_atomic exchange
;;

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

let rec atomic_add a diff =
  let old_val = Atomic.get a in
  if not (Atomic.compare_and_set a old_val (old_val +. diff)) then atomic_add a diff
;;

(** Sets this asset's reserved_quote safely. *)
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

(** Atomically checks available quote balance and reserves for a buy if sufficient.
    Returns (balance_ok, available_quote, total_reserved). *)
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

(** Returns true if quote_balance >= quote_needed. *)
let can_place_buy_order (_qty : float) quote_balance quote_needed =
  quote_balance >= quote_needed
;;

(** Returns true if an amendment is permitted for [order_id]. *)
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

(** Returns true if asset_balance >= asset_needed. *)
let can_place_sell_order (_qty : float) asset_balance asset_needed =
  asset_balance >= asset_needed
;;

(** Returns true if a sell order placement is currently in-flight or registered
    in InFlightOrders. The marker now means exactly "a sell placement is in
    flight": [handle_order_acknowledged] releases the duplicate key when the
    placement completes, so a RESTING sell no longer reports as active here -
    the inventory gate (sellable base >= sell qty) is what prevents duplicate
    sells, and the sell for a new fill is placed while earlier sells rest (the
    1-buy x multi-sell ladder). The old [just_filled_buy] bypass existed to
    defeat the latch leak and is gone with it. *)
let has_active_sell state =
  state.inflight_sell || InFlightOrders.is_in_flight state.duplicate_key_sell
;;

(* ------------------------------------------------------------------ *)
(* Sell-commitment ledger                                              *)
(*                                                                     *)
(* The single source of truth for base already committed to a sell on   *)
(* ANY venue. Sellable base is always:                                  *)
(*                                                                     *)
(*   spot_holding - reserved_base - committed_sell_base                 *)
(*                                                                     *)
(* The venue-specific part is only [spot_holding] (and, on Alpaca, the  *)
(* venue's own free figure). A resting or in-flight sell's base is      *)
(* NEVER offered again, no matter what the venue's open-order feed      *)
(* reports. This is what makes in-flight inventory correct under high   *)
(* order volume and reconciles the local ledger to the exchange: the    *)
(* feed updates quantities while it lists an order, but a feed that     *)
(* drops a live order (reconnect / snapshot truncation) can no longer   *)
(* make its base look free.                                             *)
(* ------------------------------------------------------------------ *)

(** How long a dispatched sell with no venue confirmation is kept as committed
    base. Only applies to an order that was never acked and never seen in the
    feed (a lost dispatch); an acked order is real base the venue holds and is
    kept until its terminal event. *)
let sell_commitment_in_flight_timeout_s = 120.0

(** Inserts or updates a commitment, preserving the earliest arm time.

    Backed by an id-keyed hashtable: [sync_open_orders] calls this once for
    every open sell on every execution with the feed's live price/qty and
    seen=acked=true, and the lookup/update is O(1). The previous association
    list made one scan O(n^2) (a full-list scan per open sell); on a wide grid
    that scan was the dominant [strat[sync=]] cost. The steady-state no-op
    writes nothing. *)
let upsert_sell_commitment ~state ~id ~price ~qty ~seen ~acked =
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
    if seen then c.sc_listed <- true
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
      }
;;

(** Records a just-dispatched sell (keyed by its temporary pending id). *)
let arm_sell_commitment ~state ~id ~price ~qty =
  if qty > 0.0 then upsert_sell_commitment ~state ~id ~price ~qty ~seen:false ~acked:false
;;

(** Moves a commitment to the venue order id once ack/amend supplies it. If
    the old id is unknown (the order was adopted straight from the feed), the
    new id is inserted. [acked] marks the order as accepted by the venue, so
    it is never expired by the dispatch window. *)
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
    | None -> upsert_sell_commitment ~state ~id:new_id ~price ~qty:q ~seen:false ~acked)
;;

(** Terminal event: the sell no longer holds base. *)
let remove_sell_commitment ~state ~id = Hashtbl.remove state.sell_commitments id

(** Terminal event for a placement that never got a venue id. *)
let remove_pending_sell_commitments ~state =
  let pending =
    Hashtbl.fold
      (fun id _ acc ->
         if String.starts_with ~prefix:"pending_sell_" id then id :: acc else acc)
      state.sell_commitments
      []
  in
  List.iter (Hashtbl.remove state.sell_commitments) pending
;;

(** Total base committed to live sells (the ledger). *)
let committed_sell_base state =
  Hashtbl.fold (fun _ c acc -> acc +. c.sc_qty) state.sell_commitments 0.0
;;

(** The base to subtract from the venue's reported holding:

      spot_holding - reserved_base - committed_sell_base

    - Net-balance venues ([balance_nets_open_order_holds]):
      - [hold_netted_from_venue_state] (Hyperliquid): the venue's tradeable
        figure nets holds from its OWN state (spotState [hold]), independent of
        our executions feed. It is authoritative even when our feed drops a
        live order, so subtracting the ledger's excess over the feed would
        DOUBLE-count (the observed HYPE under-count after a fill). Only the
        short [unnetted_hold] dispatch overlay is subtracted.
      - otherwise (Kraken): the venue derives holds from the SAME open-order
        feed the ledger tracks, so a feed that drops a live order frees that
        base; the ledger's EXCESS over the feed is the compensation and IS
        subtracted (never below [unnetted_hold]).
    - Gross-balance venues: the venue removed nothing, so the WHOLE ledger is
      subtracted. *)
let effective_committed_sell_base ~ecfg ~ledger_total ~feed_total ~unnetted_hold =
  if ecfg.balance_nets_open_order_holds
  then
    if ecfg.hold_netted_from_venue_state
    then unnetted_hold
    else Float.max (Float.max 0.0 (ledger_total -. feed_total)) unnetted_hold
  else ledger_total
;;
