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
