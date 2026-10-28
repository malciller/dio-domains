(* Order lifecycle event handlers and persistence (strategy-agnostic). *)

open Strategy_common
open Strategy_state
open Strategy_venue
open Strategy_reservation
open Strategy_orders
module Sell_orders = Strategy_sell_orders

(* Per-symbol lock-free lifecycle event queue. The Lwt supervisor thread (REST callbacks,
   supervisor_orders.ml) enqueues events instead of calling handlers directly; the domain
   worker drains the queue at the top of every cycle, so all handler execution stays on
   the domain thread and the per-symbol state mutex is never contended across threads.
   Each queue has a single consumer (its symbol's domain); LockFreeQueue is MPSC-safe, and
   enqueue signals Exchange_wakeup so an idle domain wakes promptly. *)

type lifecycle_event =
  | Ack of
      { now : float
      ; order_id : string
      ; side : order_side
      ; price : float
      }
  | Failed of
      { now : float
      ; side : order_side
      ; reason : string
      }
  | Rejected of
      { now : float
      ; side : order_side
      ; price : float
      }
  | Amended of
      { now : float
      ; old_id : string
      ; new_id : string
      ; side : order_side
      ; price : float
      }
  | Amendment_skipped of
      { now : float
      ; order_id : string
      ; side : order_side
      ; price : float
      }
  | Amendment_failed of
      { now : float
      ; order_id : string
      ; side : order_side
      ; reason : string
      }
  | Cancel_cleanup of { order_id : string }

(* Per-symbol lifecycle-event queues. Read at the top of every strategy cycle from every
   trading Domain and inserted from the Lwt domain, so this was a Hashtbl written under a
   mutex and read without one — on the hottest path in the engine. A Cow_table makes the
   read lock-free and allocation-free; the mutex stays so two Domains cannot both build a
   queue for the same symbol and orphan one of them. *)
let event_queues : (string, lifecycle_event LockFreeQueue.t) Ds.Cow_table.t =
  Ds.Cow_table.create ~shard_count:16 ()
;;

let event_queues_mutex = Mutex.create ()

let get_event_queue symbol =
  match Ds.Cow_table.find_opt event_queues symbol with
  | Some q -> q
  | None ->
    Mutex.lock event_queues_mutex;
    let q =
      match Ds.Cow_table.find_opt event_queues symbol with
      | Some q -> q
      | None ->
        let q = LockFreeQueue.create () in
        Ds.Cow_table.set event_queues symbol q;
        q
    in
    Mutex.unlock event_queues_mutex;
    q
;;

(** Enqueues a lifecycle event from any thread (supervisor REST path). Lock-free push plus
    a per-symbol wakeup so the domain drains promptly. *)
let enqueue_event symbol (ev : lifecycle_event) =
  ignore (LockFreeQueue.write (get_event_queue symbol) ev);
  Concurrency.Exchange_wakeup.signal ~symbol
;;

(** Flushes deferred accumulation state to disk when the dirty flag is set. *)
let flush_persistence asset_symbol =
  let state = get_strategy_state asset_symbol in
  if state.persistence_dirty
  then (
    let snapshot_to_save =
      Mutex.lock state.mutex;
      Fun.protect
        ~finally:(fun () -> Mutex.unlock state.mutex)
        (fun () ->
          if state.persistence_dirty
          then (
            state.persistence_dirty <- false;
            Some
              ( state.reserved_base
              , state.accumulated_profit
              , state.carry_credit_seq
              , state.last_fill_oid
              , state.last_buy_fill_price
              , state.last_sell_fill_price
              , state.last_buy_fill_qty
              , state.last_sell_fill_qty
              , state.persisted_sell_levels ))
          else None)
    in
    match snapshot_to_save with
    | Some
        ( reserved_base
        , accumulated_profit
        , carry_credit_seq
        , last_fill_oid
        , last_buy_fill_price
        , last_sell_fill_price
        , last_buy_fill_qty
        , last_sell_fill_qty
        , persisted_sell_levels ) ->
      let key =
        match state.persistence_key with
        | Some k -> k
        | None ->
          (* Full key not registered yet; fall back to a symbol-only scan. *)
          (match
             Dio_persistence.Base_accumulation_store.resolve_key_for_symbol
               ~symbol:asset_symbol
           with
           | Some k -> k
           | None -> "migrated:" ^ asset_symbol)
      in
      if state.base_accumulation_enabled
      then
        Dio_persistence.Base_accumulation_store.save_async
          ~key
          { Dio_persistence.Base_accumulation_store.reserved_base
          ; accumulated_profit
          ; carry_credit_seq
          ; last_fill_oid
          ; last_buy_fill_price
          ; last_buy_fill_qty
          ; last_sell_fill_price
          ; last_sell_fill_qty
          };
      if state.sell_levels_enabled
      then
        Dio_persistence.Sell_levels_store.save_async
          ~key
          (List.map
             (fun (price, qty) -> { Dio_persistence.Sell_levels_store.price; qty })
             persisted_sell_levels)
    | None -> ())
;;

(** [filter_keep_if_needed keep xs] returns [xs] physically unchanged when [keep] drops
    nothing, else [List.filter keep xs]. Handlers usually find no matching pending token,
    and an unconditional [List.filter] would copy the whole list on every event -
    allocation that fills the minor heap and can force a stop-the-world minor collection
    inside the event drain. Sharing is safe: the lists are immutable and only replaced
    wholesale. *)
let filter_keep_if_needed keep xs =
  let rec all_kept = function
    | [] -> true
    | x :: rest -> keep x && all_kept rest
  in
  if all_kept xs then xs else List.filter keep xs
;;

(** Non-allocating equivalent of
    [String.starts_with s ~prefix:"pending_amend_" && String.sub s 14 (String.length s - 14) = target].
    Avoids allocating a fresh substring per pending entry scanned. *)
let pending_amend_for s target =
  String.starts_with ~prefix:"pending_amend_" s
  && String.length s = 14 + String.length target
  &&
  let rec eq i = i >= String.length target || (s.[14 + i] = target.[i] && eq (i + 1)) in
  eq 0
;;

(** Handles order placement acknowledgment. *)
let handle_order_acknowledged ~now asset_symbol order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
      add_tracked_order_id state order_id;
      state.pending_orders
      <- filter_keep_if_needed
           (fun (pending_id, s, p, _) ->
             let is_placement_prefix =
               String.starts_with ~prefix:"pending_buy_" pending_id
               || String.starts_with ~prefix:"pending_sell_" pending_id
             in
             let matches_side_placement = is_placement_prefix && s = side in
             let matches_fallback =
               (not (String.starts_with ~prefix:"pending_" pending_id))
               && s = side
               && abs_float (p -. price) < 0.01
             in
             not (matches_side_placement || matches_fallback))
           state.pending_orders;
      (match side with
       | Buy ->
         add_tracked_order_id state order_id;
         (* A venue ack outranks a feed absence: un-evict a ghosted id. *)
         Hashtbl.remove state.evicted_orders order_id;
         state.last_buy_order_id <- Some order_id;
         state.last_buy_order_price <- Some price;
         state.inflight_buy <- false;
         state.inflight_amend_buy <- false;
         state.last_buy_attempted_insufficient <- false;
         (* Start the open-orders feed-lag grace: the venue lists the acked buy a moment
            later; [sync_open_orders] must not purge it as a ghost in that window. *)
         state.last_buy_ack_ts <- now;
         (* The buy is resting again - any pending TIF-recovery re-attempt is satisfied. *)
         state.tif_recovery_pending <- false;
         ()
       | Sell ->
         state.inflight_sell <- false;
         (* Release the in-flight marker when the placement completes. The key is added by
            [push_order] at dispatch and removed here, not held until a
            fill/cancel/failure: while held, [has_active_sell] reported true for the
            entire time a sell rested on the book, gating every later sell attempt behind
            a buy fill. The marker now means "a sell placement is in flight", so a resting
            sell no longer blocks the next sell for new inventory - the inventory gate
            (available >= sell qty) prevents duplicates. *)
         ignore (InFlightOrders.remove_in_flight_order state.duplicate_key_sell);
         state.recently_injected_sells
         <- (order_id, price, now) :: state.recently_injected_sells;
         (* Re-key the in-flight commitment from the temporary pending id to the venue
            order id. Matching by price is exact enough: only one sell placement is in
            flight at a time (the dedup key is released when this ack lands). *)
         let pending_sell_id =
           Hashtbl.fold
             (fun i c acc ->
               match acc with
               | Some _ -> acc
               | None ->
                 if String.starts_with ~prefix:"pending_sell_" i
                    && abs_float (c.sc_price -. price) < price *. 0.01
                 then Some i
                 else None)
             state.sell_commitments
             None
         in
         (match pending_sell_id with
          | Some pid ->
            rekey_sell_commitment
              ~state
              ~old_id:pid
              ~new_id:order_id
              ~price
              ~qty:0.0
              ~acked:true
          | None -> ());
         Sell_orders.replace_first
           state.open_sell_orders
           (fun oid p _ ->
             String.starts_with ~prefix:"pending_sell_" oid
             && abs_float (p -. price) < price *. 0.01)
           (fun _ p q -> order_id, p, q);
         ());
      ())
;;

(** Handles order placement failure. *)
let handle_order_failed ~now asset_symbol side reason =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
      let failed_sell_price_opt =
        if side = Sell
        then
          List.find_map
            (fun (id, s, p, _) ->
              if s = Sell && String.starts_with ~prefix:"pending_sell_" id
              then Some p
              else None)
            state.pending_orders
        else None
      in
      state.pending_orders
      <- filter_keep_if_needed (fun (_, s, _, _) -> s <> side) state.pending_orders;
      (match side with
       | Buy ->
         state.inflight_buy <- false;
         (* A placement failure can only concern an order that never acked, so it must not
            retire a tracked buy; [Rejected] retires by price. *)
         if state.last_buy_order_id = None
         then (
           state.last_buy_order_price <- None;
           set_asset_reserved_quote state 0.0)
       | Sell -> state.inflight_sell <- false);
      (match side with
       | Buy -> ()
       | Sell ->
         remove_pending_sell_commitments ~state;
         Sell_orders.remove_prefix state.open_sell_orders "pending_sell_");
      let duplicate_key =
        match side with
        | Buy -> state.duplicate_key_buy
        | Sell -> state.duplicate_key_sell
      in
      ignore (InFlightOrders.remove_in_flight_order duplicate_key);
      let lower_reason = String.lowercase_ascii reason in
      let is_rate_limit =
        contains_fragment lower_reason "too many cumulative requests"
        || contains_fragment lower_reason "rate limit"
      in
      let is_wash_trade = contains_fragment lower_reason "wash trade" in
      let is_insufficient_balance =
        contains_fragment lower_reason "insufficient funds"
        || contains_fragment lower_reason "insufficient spot balance"
        || contains_fragment lower_reason "not enough asset balance"
        || contains_fragment lower_reason "insufficient qty"
        || contains_fragment lower_reason "insufficient balance"
        || contains_fragment lower_reason "qty available"
        || contains_fragment lower_reason "position not found"
        || contains_fragment lower_reason "asset not held"
      in
      let cooldown = if is_rate_limit || is_wash_trade then 10.0 else 2.0 in
      (match side with
       | Buy when not is_insufficient_balance ->
         (* A terminal placement loss (TIF/ALO/post-only, transient venue errors) must not
            leave the asset buyless while the capital oracle is INACTIVE: arm TIF recovery
            so the buy leg re-attempts (repriced, after the normal cooldown) through the
            halt. Insufficient-balance failures are excluded - capital_low owns those and
            recovery must not fight a real capital drought. *)
         state.tif_recovery_pending <- true;
         state.tif_recovery_since <- Unix.gettimeofday ();
         Logging.info_f
           ~section
           "Buy placement for %s failed (%s) - TIF recovery armed (window 900s, \
            refreshes on each kill)"
           asset_symbol
           reason
       | _ -> ());
      (match side with
       | Buy when is_insufficient_balance ->
         if state.last_buy_attempted_insufficient
         then
           (* The buy was knowingly placed against a stale, under-funded balance snapshot:
              the rejection is foreordained, so latching capital_low would pause buying on
              a snapshot about to be replaced. The fresh store value governs; the
              fresh-insufficient placement path latches capital_low itself when the
              shortage is real. *)
           Logging.info_f
             ~section
             "Exchange rejected buy for %s with insufficient funds (foreordained: stale \
              balance snapshot) - not latching capital_low"
             asset_symbol
         else if not state.capital_low
         then (
           state.capital_low <- true;
           state.capital_low_logged <- true;
           state.capital_low_at_balance <- -1.0;
           state.capital_low_last_log_at <- now;
           let venue_quote =
             venue_quote_balance ~exchange:state.exchange_id ~symbol:asset_symbol
           in
           let overlay_reserved = overlay_reserved_quote ~exchange:state.exchange_id in
           let grid_reserved = get_total_reserved_quote state in
           Logging.warn_f
             ~section
             "Exchange rejected buy for %s with insufficient funds - setting capital_low \
              flag (venue quote %.2f, overlays reserved %.2f, grids reserved %.2f)"
             asset_symbol
             venue_quote
             overlay_reserved
             grid_reserved)
       | Sell when is_insufficient_balance ->
         let ecfg = get_exchange_config state.exchange_id in
         (* Local ledger has the base but the venue does not: stale/missed balance
            snapshot, not sizing. Request an authoritative spot snapshot; [asset_low]
            latches below until the refreshed balance is adopted. *)
         (match get_exchange_module state.exchange_id with
          | Some (module Ex) ->
            (try Ex.resync_market_state ~symbol:asset_symbol () with
             | _ -> ())
          | None -> ());
         if ecfg.remaintain_expired_sells && state.persisted_sell_levels <> []
         then (
           match failed_sell_price_opt with
           | Some p ->
             let rec remove_one acc found = function
               | [] -> List.rev acc
               | (sp, sq) :: rest
                 when (not found)
                      && (abs_float (sp -. p) <= p *. 0.0001
                          || abs_float (sp -. p) <= 1e-4) ->
                 Logging.warn_f
                   ~section
                   "Purged un-fundable persisted sell level for %s @ %.4f (qty %.8f) \
                    after exchange rejection: %s"
                   asset_symbol
                   sp
                   sq
                   reason;
                 remove_one acc true rest
               | item :: rest -> remove_one (item :: acc) found rest
             in
             let new_levels = remove_one [] false state.persisted_sell_levels in
             if new_levels <> state.persisted_sell_levels
             then (
               state.persisted_sell_levels <- new_levels;
               state.persistence_dirty <- true)
           | None -> ());
         if ecfg.sell_failure_sets_asset_low
         then (
           if not state.asset_low
           then (
             state.asset_low <- true;
             Logging.warn_f
               ~section
               "Exchange rejected sell for %s with insufficient balance - setting \
                asset_low flag"
               asset_symbol))
         else
           Logging.warn_f
             ~section
             "Exchange rejected sell for %s with insufficient balance (ignored, sell is \
              fire-and-forget)"
             asset_symbol
       | _ -> ());
      (match side with
       | Buy ->
         if is_wash_trade
         then
           Logging.warn_f
             ~section
             "Buy rejected for %s due to wash trade conflict - cooling down buy \
              placement for 10s"
             asset_symbol;
         Hashtbl.replace state.amend_cooldowns "place_Buy" (now +. cooldown)
       | Sell -> Hashtbl.replace state.amend_cooldowns "place_Sell" (now +. cooldown));
      Logging.warn_f
        ~section
        "Order failed for %s (%s): %s. Cleared in-flight tracker."
        asset_symbol
        (string_of_order_side side)
        reason)
;;

(** Handles order rejection. *)
let handle_order_rejected ~now:_ asset_symbol side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
      state.pending_orders
      <- filter_keep_if_needed
           (fun (pending_id, s, p, _) ->
             let is_placement_prefix =
               String.starts_with ~prefix:"pending_buy_" pending_id
               || String.starts_with ~prefix:"pending_sell_" pending_id
             in
             let matches_side_placement = is_placement_prefix && s = side in
             let matches_amend_prefix =
               String.starts_with ~prefix:"pending_amend_" pending_id
             in
             let matches_fallback =
               ((not (String.starts_with ~prefix:"pending_" pending_id))
                || matches_amend_prefix)
               && s = side
               && abs_float (p -. price) < 0.01
             in
             not (matches_side_placement || matches_fallback))
           state.pending_orders;
      (match side with
       | Buy ->
         state.inflight_buy <- false;
         (* Match by price: a rejected duplicate must not detach the acked buy. *)
         let retires_tracked_buy =
           match state.last_buy_order_id, state.last_buy_order_price with
           | None, _ -> true
           | Some _, Some tracked -> abs_float (tracked -. price) < 0.01
           | Some _, None -> false
         in
         if retires_tracked_buy
         then (
           state.last_buy_order_id <- None;
           state.last_buy_order_price <- None;
           set_asset_reserved_quote state 0.0)
       | Sell ->
         state.inflight_sell <- false;
         remove_pending_sell_commitments ~state;
         Sell_orders.remove_prefix state.open_sell_orders "pending_sell_");
      let duplicate_key =
        match side with
        | Buy -> state.duplicate_key_buy
        | Sell -> state.duplicate_key_sell
      in
      ignore (InFlightOrders.remove_in_flight_order duplicate_key);
      (match side with
       | Buy ->
         let now = Unix.time () in
         let new_expiry = now +. 2.0 in
         let existing_expiry =
           match Hashtbl.find_opt state.amend_cooldowns "place_Buy" with
           | Some t -> t
           | None -> 0.0
         in
         if new_expiry > existing_expiry
         then Hashtbl.replace state.amend_cooldowns "place_Buy" new_expiry
       | Sell -> ());
      ())
;;

let buy_tracking_matches_exchange_event tracked order_id cl_ord_id =
  tracked = order_id
  ||
  match cl_ord_id with
  | Some c -> tracked = c
  | None -> false
;;

let add_processed_fill state order_id =
  if not (Hashtbl.mem state.processed_fills order_id)
  then (
    Queue.push order_id state.processed_fills_queue;
    Hashtbl.replace state.processed_fills order_id ();
    if Hashtbl.length state.processed_fills > 2000
    then (
      try
        let oldest = Queue.pop state.processed_fills_queue in
        Hashtbl.remove state.processed_fills oldest
      with
      | _ -> ()))
;;

(** Handles order fill. *)
let handle_order_filled ~now asset_symbol order_id side ~fill_price ~fill_qty cl_ord_id =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
      let acc_qty =
        if fill_qty > 0.0
        then fill_qty
        else venue_lot_qty state.grid_qty state.exchange_id state
      in
      let is_persisted_fill =
        Hashtbl.mem state.processed_fills order_id
        ||
        match state.last_fill_oid with
        | Some persisted_oid ->
          (try
             Int64.compare (Int64.of_string order_id) (Int64.of_string persisted_oid) <= 0
           with
           | _ -> order_id = persisted_oid)
        | None -> state.startup_replay
      in
      let skip_fill = is_persisted_fill in
      if skip_fill
      then (
        add_processed_fill state order_id;
        state.skipped_fill_streak <- state.skipped_fill_streak + 1;
        state.skipped_fills_total <- state.skipped_fills_total + 1;
        if state.skipped_fill_streak >= 50
        then
          Logging.warn_f
            ~section
            "Replay guard skipped %d consecutive fills (total %d) for %s; \
             last_fill_oid=%s"
            state.skipped_fill_streak
            state.skipped_fills_total
            asset_symbol
            (Option.value state.last_fill_oid ~default:"none")
        else
          Logging.debug_f
            ~section
            "Skipping already processed fill: %s (side=%s)"
            order_id
            (match side with
             | Buy -> "buy"
             | Sell -> "sell");
        if state.startup_replay
        then (
          let dominated =
            match state.highest_startup_oid with
            | None -> true
            | Some prev ->
              (try
                 Int64.compare (Int64.of_string order_id) (Int64.of_string prev) > 0
               with
               | _ -> false)
          in
          if dominated then state.highest_startup_oid <- Some order_id)
        else if state.skipped_fill_streak >= 50
        then (
          (* A monotonic high-water skip guard can permanently eat every real fill if the
             persisted mark lands above the venue's live id space (state restored across
             accounts/venues, id reset). After 50 consecutive skips with zero processed
             fills this is the only consistent explanation (ordinary duplicate redelivery
             is bounded and covered by processed_fills). Reset the mark so real fills flow
             again; processed_fills still guards in-window redelivery. *)
          Logging.critical_async_f
            ~section
            "Fill replay guard stuck: %d consecutive skips for %s (last_fill_oid=%s) \
             indicates the persisted high-water mark is ahead of the venue's live id \
             space. Resetting last_fill_oid to None so fills process again."
            state.skipped_fill_streak
            asset_symbol
            (Option.value state.last_fill_oid ~default:"none");
          state.last_fill_oid <- None;
          state.skipped_fill_streak <- 0))
      else (
        add_processed_fill state order_id;
        add_tracked_order_id state order_id;
        state.skipped_fill_streak <- 0;
        (* A buy fill means a buy is working again: pending TIF recovery is satisfied and
           consumed (the re-anchor places the next pair through the normal cycle). *)
        if side = Buy then state.tif_recovery_pending <- false;
        state.pending_orders
        <- filter_keep_if_needed
             (fun (pending_id, _, _, _) -> not (pending_amend_for pending_id order_id))
             state.pending_orders;
        let sell_fill_price =
          match
            Sell_orders.find_first state.open_sell_orders (fun id _ _ -> id = order_id)
          with
          | Some (_, p, _) -> p
          | None -> fill_price
        in
        ignore (Sell_orders.remove_by_id state.open_sell_orders order_id);
        let _was_tracked_buy =
          match side, state.last_buy_order_id with
          | Buy, Some id when buy_tracking_matches_exchange_event id order_id cl_ord_id ->
            true
          | _ -> false
        in
        (* A fill is terminal for the placement as well. The placement Ack normally
           removes the [pending_buy_] token first, but if that Ack is lost or delayed past
           a fast fill the token would make [buy_leg_facts] report an in-flight buy
           forever, blocking every later buy placement and amendment. Drop it here, only
           for our tracked buy (an external/untracked fill must not clear a concurrent
           placement's token). The token is keyed by price, so this side-scoped clear is
           the reliable match. *)
        if side = Buy && _was_tracked_buy
        then
          state.pending_orders
          <- filter_keep_if_needed
               (fun (pending_id, s, _, _) ->
                 not (s = Buy && String.starts_with ~prefix:"pending_buy_" pending_id))
               state.pending_orders;
        (* A fill for the OLD id of a just-replaced order (Hyperliquid/Alpaca
           cancel+create can fill the old order at the moment of replacement): the fill is
           real and its accounting below stands, but the RESTING buy is the replacement -
           clearing buy tracking here would make the grid place a second resting buy. *)
        let is_superseded_old_fill =
          side = Buy
          && (not _was_tracked_buy)
          && InFlightAmendments.is_superseded order_id
        in
        if side = Buy
        then (
          (* Route buy-fill bookkeeping through the base-accumulation store's pure
             decision logic (updates the last-buy reference for the next sell's
             profitability check). *)
          let updated =
            Dio_persistence.Base_accumulation_store.apply_buy_fill
              { Dio_persistence.Base_accumulation_store.reserved_base =
                  state.reserved_base
              ; accumulated_profit = state.accumulated_profit
              ; carry_credit_seq = state.carry_credit_seq
              ; last_fill_oid = state.last_fill_oid
              ; last_buy_fill_price = state.last_buy_fill_price
              ; last_buy_fill_qty = state.last_buy_fill_qty
              ; last_sell_fill_price = state.last_sell_fill_price
              ; last_sell_fill_qty = state.last_sell_fill_qty
              }
              ~price:fill_price
              ~qty:acc_qty
              ~oid:order_id
          in
          state.last_buy_fill_price
          <- updated.Dio_persistence.Base_accumulation_store.last_buy_fill_price;
          state.last_buy_fill_qty
          <- updated.Dio_persistence.Base_accumulation_store.last_buy_fill_qty;
          (* Profit-gated lot skim for 1:1 venues. The order cannot withhold the
             [(1 - sell_mult)] slice (below one lot), so split the buy fill's own base:
             once quote profit covers a lot plus the buffer, move that lot into
             [reserved_base] and reduce the sellable reference. The reserve is backed by
             this buy. Runs once per buy, never on the sell path. *)
          if (not state.startup_replay)
             && state.base_accumulation_enabled
             && state.cached_ecfg.use_accumulation_sells
             && (not state.cached_ecfg.sell_uses_mult)
             && fill_price > 0.0
             && acc_qty > 0.0
          then (
            let sell_mult = Float.min 1.0 (Float.max 0.0 state.cached_sell_mult) in
            let lot =
              if state.cached_qty_increment > 0.0
              then state.cached_qty_increment
              else 1e-8
            in
            let retained = acc_qty *. (1.0 -. sell_mult) in
            let skim =
              round_qty_up (Float.max retained lot) asset_symbol state.exchange_id
            in
            let cost = skim *. fill_price in
            if skim > 0.0
               && acc_qty -. skim >= lot -. 1e-9
               && state.accumulated_profit >= cost +. state.accumulation_buffer
            then (
              state.reserved_base <- state.reserved_base +. skim;
              state.accumulated_profit <- Float.max 0.0 (state.accumulated_profit -. cost);
              state.last_buy_fill_qty <- Some (acc_qty -. skim);
              state.persistence_dirty <- true;
              Logging.info_f
                ~section
                "Skimmed base for %s: +%.8f into reserved_base (cost $%.4f, sellable lot \
                 %.8f)"
                asset_symbol
                skim
                cost
                (acc_qty -. skim)));
          (* Persistence dirty-marking follows the per-strategy config opt-in
             (base_accumulation), not a hardcoded venue list. *)
          if state.base_accumulation_enabled then state.persistence_dirty <- true;
          (* Spec-aligned buy fill: only the reference info for the next sell's
             profitability check is updated (done above via apply_buy_fill). Per-fill
             slice retention into reserved_base is gone - accumulation happens at
             sell-fill time when the profit window exceeds the buffer (see below). *)
          if acc_qty > 0.0 && not state.startup_replay
          then (
            (* Record the buy credit for the fill-aware sizing bridge rather than mutating
               a running ledger: on Hyperliquid-like spot venues the buy fee is subtracted
               from the received base, so crediting the raw fill qty would overstate
               inventory by the fee on every fill and let a sell dip into reserved_base.

               Draw down [attributed_balance_increase] first: the balance feed may have
               already adopted this fill (independent feeds, message before execution
               event), and adding the credit would then double-count it. Only the
               unmatched remainder is an unreflected credit. *)
            let credit_qty =
              if hl_like_spot_fee_exchange state.exchange_id && state.maker_fee > 0.0
              then Float.max 0.0 (acc_qty -. (state.maker_fee *. acc_qty))
              else acc_qty
            in
            let already_reflected =
              Float.min credit_qty state.attributed_balance_increase
            in
            state.attributed_balance_increase
            <- Float.max 0.0 (state.attributed_balance_increase -. already_reflected);
            let pending = credit_qty -. already_reflected in
            if pending > 1e-12
            then Strategy_lifecycle.arm_buy_credit ~state ~qty:pending ~now;
            Logging.debug_f
              ~section
              "Pending base credit for %s: +%.8f (already reflected: %.8f, unreflected \
               credits: %d) from buy fill %s"
              asset_symbol
              pending
              already_reflected
              (List.length state.buy_credits_since_balance)
              order_id);
          (* A buy fill does not complete a sell placement: the sell's own ack/fill/cancel
             events own the sell in-flight lifecycle, so the sell markers
             ([inflight_sell], the duplicate-key latch, the recently-injected debounce)
             are left untouched here. Clearing them on buy fills was a workaround for the
             sell-ack latch leak (see handle_order_acknowledged) and let a fill tick push
             a new sell while the previous sell placement was still in flight. *)
          state.last_buy_attempted_insufficient <- false;
          if is_superseded_old_fill
          then
            Logging.debug_f
              ~section
              "Buy fill for superseded order %s for %s (replaced by amendment); \
               preserving resting buy tracking"
              order_id
              asset_symbol
          else (
            state.last_buy_order_id <- None;
            state.last_buy_order_price <- None;
            if not state.startup_replay then state.just_filled_buy <- true));
        if state.startup_replay
        then (
          let dominated =
            match state.highest_startup_oid with
            | None -> true
            | Some prev ->
              (try
                 Int64.compare (Int64.of_string order_id) (Int64.of_string prev) > 0
               with
               | _ -> false)
          in
          if dominated then state.highest_startup_oid <- Some order_id);
        (match side with
         | Sell ->
           (* A [Filled] event is terminal for the order: release the whole commitment by
              id, never merely the reported fill qty. On Hyperliquid the orderUpdates
              "filled" event retires the order from the open-order feed (and is filtered
              out of the strategy stream), so the userEvents Trade that reaches us can be
              left computing from an untracked order and report only the LAST partial
              size; subtracting that would strand the earlier fills' base in the ledger
              forever (the retained stale sell / negative closest-sell distance, and the
              under-counted sellable balance that then re-buys). Partial fills arrive as
              [PartiallyFilled] and never reach this handler. *)
           remove_sell_commitment ~state ~id:order_id;
           let known_open_sell =
             Sell_orders.find_first state.open_sell_orders (fun oid _ _ -> oid = order_id)
           in
           ignore (Sell_orders.remove_by_id state.open_sell_orders order_id);
           if state.persisted_sell_levels <> []
           then (
             let matched_by_limit =
               match known_open_sell with
               | Some (_, limit_p, _) ->
                 let rec remove_limit acc found = function
                   | [] -> if found then Some (List.rev acc) else None
                   | (sp, _sq) :: rest
                     when (not found)
                          && (abs_float (sp -. limit_p) <= limit_p *. 0.0001
                              || abs_float (sp -. limit_p) <= 1e-4) ->
                     remove_limit acc true rest
                   | item :: rest -> remove_limit (item :: acc) found rest
                 in
                 remove_limit [] false state.persisted_sell_levels
               | None -> None
             in
             let final_levels =
               match matched_by_limit with
               | Some updated -> updated
               | None ->
                 let rec remove_exact acc found = function
                   | [] -> if found then Some (List.rev acc) else None
                   | (sp, _sq) :: rest
                     when (not found)
                          && (abs_float (sp -. sell_fill_price)
                              <= sell_fill_price *. 0.0001
                              || abs_float (sp -. sell_fill_price) <= 1e-4) ->
                     remove_exact acc true rest
                   | item :: rest -> remove_exact (item :: acc) found rest
                 in
                 (match remove_exact [] false state.persisted_sell_levels with
                  | Some updated -> updated
                  | None ->
                    (* Price improvement: candidate with sp <= sell_fill_price closest to
                       the fill price. *)
                    let candidate =
                      List.fold_left
                        (fun best (sp, sq) ->
                          if sp <= sell_fill_price +. 1e-4
                          then (
                            match best with
                            | None -> Some (sp, sq)
                            | Some (b_sp, _) -> if sp > b_sp then Some (sp, sq) else best)
                          else best)
                        None
                        state.persisted_sell_levels
                    in
                    (match candidate with
                     | Some (cand_p, _) ->
                       let rec remove_cand acc found = function
                         | [] -> List.rev acc
                         | (sp, _sq) :: rest
                           when (not found)
                                && (abs_float (sp -. cand_p) <= cand_p *. 0.0001
                                    || abs_float (sp -. cand_p) <= 1e-4) ->
                           remove_cand acc true rest
                         | item :: rest -> remove_cand (item :: acc) found rest
                       in
                       remove_cand [] false state.persisted_sell_levels
                     | None -> state.persisted_sell_levels))
             in
             if final_levels <> state.persisted_sell_levels
             then (
               state.persisted_sell_levels <- final_levels;
               state.persistence_dirty <- true));
           if acc_qty > 0.0
           then
             if (* Accumulation venues report tradeable = total - open-order hold: when a
                   resting sell fills, the hold release offsets the total drop, so the
                   venue figure does not fall and the ledger must not decrement here (that
                   would double-count against [unnetted_hold]); venue reconciliation
                   absorbs the netting. Gross-balance venues fall by the sold qty at fill. *)
                not state.cached_ecfg.use_accumulation_sells
             then state.position_base <- Float.max 0.0 (state.position_base -. acc_qty);
           (* Base that just sold may have been riding the unreflected-buy bridge: retire
              it FIFO so the overlay cannot keep offering base that has already left the
              account. *)
           if acc_qty > 0.0 && not state.startup_replay
           then ignore (Strategy_lifecycle.consume_buy_credits ~state ~qty:acc_qty);
           state.last_sell_fill_qty <- Some acc_qty;
           (* Spec-aligned sell fill: profit is measured against the LAST BUY fill (single
              local buy/sell cycle pair). The prior-sell cost basis and grid-spread
              fallback are removed. All fees (both legs) are folded into one [fees] figure
              passed to the store's pure decision logic, which:
              - accrues net profit into accumulated_profit when > 0,
              - when accumulated_profit >= base_cost + buffer (realtime F&G), adds
                oracle_qty * (1 - sell_mult) to reserved_base and debits
                accumulated_profit by base_cost, preserving the buffer and surplus profit
                in the quote ledger. *)
           let store_t =
             { Dio_persistence.Base_accumulation_store.reserved_base = state.reserved_base
             ; accumulated_profit = state.accumulated_profit
             ; carry_credit_seq = state.carry_credit_seq
             ; last_fill_oid = state.last_fill_oid
             ; last_buy_fill_price = state.last_buy_fill_price
             ; last_buy_fill_qty = state.last_buy_fill_qty
             ; last_sell_fill_price = state.last_sell_fill_price
             ; last_sell_fill_qty = state.last_sell_fill_qty
             }
           in
           let qty = acc_qty in
           let fees =
             if state.exchange_id = "ibkr"
             then (
               match state.last_buy_fill_price with
               | Some bp ->
                 ibkr_commission ~qty ~price:bp
                 +. ibkr_commission ~qty ~price:sell_fill_price
               | None -> 0.0)
             else if hl_like_spot_fee_exchange state.exchange_id
             then sell_fill_price *. qty *. state.maker_fee
             else (
               (* Both legs at maker fee, paired against the last buy ref. *)
               match state.last_buy_fill_price with
               | Some bp ->
                 (sell_fill_price *. qty *. state.maker_fee)
                 +. (bp *. qty *. state.maker_fee)
               | None -> sell_fill_price *. qty *. state.maker_fee)
           in
           let updated =
             Dio_persistence.Base_accumulation_store.apply_sell_fill
               store_t
               ~price:sell_fill_price
               ~qty
               ~oid:order_id
               ~buffer:state.accumulation_buffer
               ~sell_mult:state.cached_sell_mult
               ~oracle_qty:state.grid_qty
               ~sell_uses_mult:state.cached_ecfg.sell_uses_mult
               ~fees
               ()
           in
           state.reserved_base
           <- updated.Dio_persistence.Base_accumulation_store.reserved_base;
           state.accumulated_profit
           <- updated.Dio_persistence.Base_accumulation_store.accumulated_profit;
           state.last_fill_oid
           <- updated.Dio_persistence.Base_accumulation_store.last_fill_oid;
           state.last_sell_fill_price
           <- updated.Dio_persistence.Base_accumulation_store.last_sell_fill_price;
           state.last_sell_fill_qty
           <- updated.Dio_persistence.Base_accumulation_store.last_sell_fill_qty
         | Buy -> ());
        let should_update_oid =
          match state.last_fill_oid with
          | Some prev_oid ->
            (try
               Int64.compare (Int64.of_string order_id) (Int64.of_string prev_oid) > 0
             with
             | _ -> order_id <> prev_oid)
          | None -> true
        in
        if should_update_oid then state.last_fill_oid <- Some order_id;
        if state.base_accumulation_enabled then state.persistence_dirty <- true;
        (match side with
         | Buy -> state.inflight_buy <- false
         | Sell -> state.inflight_sell <- false);
        ignore
          (InFlightOrders.remove_in_flight_order
             (match side with
              | Buy -> state.duplicate_key_buy
              | Sell -> state.duplicate_key_sell));
        if side = Buy then set_asset_reserved_quote state 0.0;
        if side = Buy
        then (
          ignore (InFlightOrders.remove_in_flight_order state.duplicate_key_sell);
          state.inflight_sell <- false);
        ()))
;;

(** Handles order cancellation. *)
let handle_order_cancelled ~now:_ asset_symbol order_id side cl_ord_id =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
      (* Defer the tracking reset only when THIS order is the subject of an in-flight
         amendment (the amend handler later re-keys the tracking to the replacement id).
         [state.inflight_amend_buy] must not be consulted here: it is a per-symbol flag
         that also covers amends of other orders, so a cancel for a different order would
         be swallowed and its reset lost forever. The registry and the pending_amend_
         entries are keyed by the amended (old) order id, exactly the id the exchange's
         mid-amend cancel event carries. [is_amend_lifecycle_active] also covers the
         window after the exchange confirmed a replace (Hyperliquid/Alpaca): the old
         order's cancel event can arrive after the amend response and is still the
         replace's side effect, not a real cancellation. *)
      let is_being_amended =
        InFlightAmendments.is_amend_lifecycle_active order_id
        || List.exists
             (fun (pending_id, _, _, _) -> pending_amend_for pending_id order_id)
             state.pending_orders
      in
      (* A "ghost" placement is an order the venue reports as canceled whose id we never
         tracked (lost ack, TIF/post-only reject after dispatch). Only then may the cancel
         purge in-flight placement tokens of the same side. If the canceled order IS known
         (a resting buy/sell we track), its cancel is a legitimate cancel of a different
         order - purging the side's placement tokens here would free the dedup key / clear
         the in-flight guard of a concurrently dispatched placement and allow a duplicate
         order. *)
      let is_known_order =
        (match state.last_buy_order_id with
         | Some id -> buy_tracking_matches_exchange_event id order_id cl_ord_id
         | None -> false)
        || Sell_orders.exists_id state.open_sell_orders order_id
      in
      (* A STALE cancel references an order this strategy tracked long ago
         (acked/adopted/filled) that is no longer current - e.g. the late WS cancel for a
         buy whose cancel REST call timed out but still landed venue-side, arriving after
         a replacement placement was dispatched. Such a cancel must not purge placement
         tokens (the ghost purge below would kill the concurrent placement's token) and
         must not clobber the placement's guards. A GHOST cancel (never-acked placement,
         id absent from the tracked set) is the opposite: purge and reset are required,
         otherwise the side wedges forever. *)
      let is_stale_order_cancel =
        (not is_known_order) && Hashtbl.mem state.tracked_order_ids order_id
      in
      if is_stale_order_cancel
      then
        Logging.debug_f
          ~section
          "Late cancel for previously tracked order %s (%s) on %s - preserving in-flight \
           placement guards"
          order_id
          (match side with
           | Buy -> "buy"
           | Sell -> "sell")
          asset_symbol;
      state.pending_orders
      <- (if is_stale_order_cancel
          then state.pending_orders
          else (
            (* A WS kill (reject/cancel/expired) of an in-flight placement surfaces here
               as a ghost: the pending token is about to be purged. If that placement was
               our BUY, arm TIF recovery so the buy leg re-attempts promptly rather than
               leaving the asset buyless while the capital oracle is INACTIVE. *)
            let buy_placement_died =
              (not is_known_order)
              && side = Buy
              && List.exists
                   (fun (pending_id, s, _, _) ->
                     s = Buy && String.starts_with ~prefix:"pending_buy_" pending_id)
                   state.pending_orders
            in
            if buy_placement_died
            then (
              state.tif_recovery_pending <- true;
              state.tif_recovery_since <- Unix.gettimeofday ();
              Logging.info_f
                ~section
                "Buy placement for %s died unacked (WS kill) - TIF recovery armed \
                 (window 900s)"
                asset_symbol);
            filter_keep_if_needed
              (fun (pending_id, s, _, _) ->
                (* A cancel for the old id during a cancel+replace amendment is the
                   amendment's side effect, not a real cancellation. Keep its
                   [pending_amend_] marker so the rung stays covered until the amendment's
                   own terminal event (ack/skip/failure) retires it; the persisted-rung
                   reconcile uses that marker to avoid restoring a rung the venue's
                   replacement still holds. *)
                let matches =
                  pending_id = order_id
                  || (pending_amend_for pending_id order_id && not is_being_amended)
                in
                let is_ghost_placement =
                  (not is_known_order)
                  && s = side
                  && (String.starts_with ~prefix:"pending_buy_" pending_id
                      || String.starts_with ~prefix:"pending_sell_" pending_id)
                in
                not (matches || is_ghost_placement))
              state.pending_orders));
      if not is_being_amended
      then (
        Hashtbl.remove state.amend_cooldowns order_id;
        (* A cancelled buy is terminal. Evict it so a stale open-order snapshot cannot
           re-adopt it as the resting buy and issue a doomed amend against a dead id.
           Sells are not evicted: an evicted sell arms the wash-trade crossing guard and
           would block fresh buys for the eviction TTL. *)
        (match side with
         | Buy ->
           Hashtbl.replace
             state.evicted_orders
             order_id
             (Unix.gettimeofday () +. 900.0, Buy)
         | Sell -> Hashtbl.remove state.evicted_orders order_id);
        ignore (InFlightAmendments.remove_in_flight_amendment order_id);
        let cancelled_side = side in
        let was_tracked_buy =
          match state.last_buy_order_id with
          | Some id when buy_tracking_matches_exchange_event id order_id cl_ord_id -> true
          | _ -> false
        in
        if was_tracked_buy
        then (
          state.last_buy_order_id <- None;
          state.last_buy_order_price <- None;
          ());
        (* The canceled order's own cancel/amend markers always clear: they describe THIS
           cancel's lifecycle. But the side's placement guards (reserved quote, inflight
           flag, dedup key) may already belong to a NEWER placement dispatched after this
           cancel was believed failed - wipe them only when no same-side placement token
           is alive, i.e. no concurrent placement can be clobbered. *)
        let placement_in_flight =
          List.exists
            (fun (pending_id, s, _, _) ->
              s = cancelled_side
              && (String.starts_with ~prefix:"pending_buy_" pending_id
                  || String.starts_with ~prefix:"pending_sell_" pending_id))
            state.pending_orders
        in
        (match cancelled_side with
         | Buy ->
           state.inflight_cancel_buy <- false;
           state.inflight_amend_buy <- false;
           Hashtbl.remove state.amend_cooldowns "place_Buy";
           if not placement_in_flight
           then (
             set_asset_reserved_quote state 0.0;
             state.inflight_buy <- false;
             ignore (InFlightOrders.remove_in_flight_order state.duplicate_key_buy))
         | Sell ->
           if not placement_in_flight
           then (
             state.inflight_sell <- false;
             ignore (InFlightOrders.remove_in_flight_order state.duplicate_key_sell)));
        ignore (Sell_orders.remove_by_id state.open_sell_orders order_id);
        remove_sell_commitment ~state ~id:order_id)
      else
        Logging.debug_f
          ~section
          "Order cancellation for %s (%s) is the cancel half of a cancel/replace \
           amendment; keeping the rung's tracking until the amendment's terminal event"
          asset_symbol
          order_id)
;;

(** Handles order amendment. *)
let handle_order_amended ~now asset_symbol old_order_id new_order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
      state.pending_orders
      <- filter_keep_if_needed
           (fun (pending_id, _s, _p, _) ->
             not
               (pending_amend_for pending_id old_order_id
                || pending_amend_for pending_id new_order_id))
           state.pending_orders;
      (match side with
       | Buy ->
         add_tracked_order_id state new_order_id;
         (match state.last_buy_order_id with
          | Some target_id when target_id = old_order_id ->
            state.last_buy_order_id <- Some new_order_id;
            state.last_buy_order_price <- Some price;
            if old_order_id = new_order_id
            then
              Logging.debug_f
                ~section
                "Amended buy order price in tracking: %s @ %.4f for %s"
                old_order_id
                price
                asset_symbol
            else
              Logging.debug_f
                ~section
                "Amended buy order ID in tracking: %s -> %s @ %.4f for %s"
                old_order_id
                new_order_id
                price
                asset_symbol;
            state.inflight_amend_buy <- false
          | _ ->
            (* An unresolved HL amend ack returns the superseded id; don't let it displace
               a live tracked buy. *)
            if state.last_buy_order_id = None
            then (
              state.last_buy_order_id <- Some new_order_id;
              state.last_buy_order_price <- Some price;
              Logging.debug_f
                ~section
                "External buy order amendment in tracking: %s @ %.4f for %s"
                new_order_id
                price
                asset_symbol);
            state.inflight_amend_buy <- false)
       | Sell ->
         add_tracked_order_id state new_order_id;
         let original_sell_count = Sell_orders.length state.open_sell_orders in
         let old_entry =
           Sell_orders.find_first state.open_sell_orders (fun id _ _ -> id = old_order_id)
         in
         let old_qty =
           match old_entry with
           | Some (_, _, q) -> q
           | None -> venue_lot_qty state.grid_qty state.exchange_id state
         in
         rekey_sell_commitment
           ~state
           ~old_id:old_order_id
           ~new_id:new_order_id
           ~price
           ~qty:old_qty
           ~acked:true;
         Logging.debug_f
           ~section
           "SELL_AMEND [%s] %s -> %s @ %.2f: old_entry=%s old_qty=%.8f sells_before=%d"
           asset_symbol
           old_order_id
           new_order_id
           price
           (match old_entry with
            | Some (_, p, q) -> Printf.sprintf "%.2f/%.8f" p q
            | None -> "NOT_FOUND")
           old_qty
           original_sell_count;
         ignore (Sell_orders.remove_by_id state.open_sell_orders old_order_id);
         Sell_orders.push state.open_sell_orders new_order_id price old_qty;
         state.recently_injected_sells
         <- (new_order_id, price, now) :: state.recently_injected_sells;
         (match old_entry with
          | Some (_, old_p, _) when state.persisted_sell_levels <> [] ->
            let rec remove_one acc found = function
              | [] -> List.rev acc
              | (sp, _sq) :: rest
                when (not found)
                     && (abs_float (sp -. old_p) <= old_p *. 0.0001
                         || abs_float (sp -. old_p) <= 1e-4) -> remove_one acc true rest
              | item :: rest -> remove_one (item :: acc) found rest
            in
            let filtered = remove_one [] false state.persisted_sell_levels in
            let updated = (price, old_qty) :: filtered in
            state.persisted_sell_levels
            <- List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) updated;
            state.persistence_dirty <- true
          | _ -> ());
         Logging.debug_f
           ~section
           "SELL_AMEND [%s] result: sells_after=%d"
           asset_symbol
           (Sell_orders.length state.open_sell_orders));
      let cooldown = if old_order_id = new_order_id then 2.0 else 10.0 in
      Hashtbl.replace state.amend_cooldowns old_order_id (now +. cooldown);
      Hashtbl.replace state.amend_cooldowns new_order_id (now +. cooldown);
      (* On a cancel+replace venue, a same-id ack means the exchange did not name the
         replacement. The ordinary cooldown is keyed to the old id, so it cannot cover the
         replacement id until the feed binds it. Suppress the excess-buy sweep for the
         symbol during that window; [sync_open_orders] folds this marker into
         [has_recent_amend_buy], and [buy_cancel_excess] checks it as a backstop. *)
      (match side with
       | Buy ->
         if old_order_id = new_order_id
         then (
           if String.equal state.exchange_id "hyperliquid"
           then Hashtbl.replace state.amend_cooldowns "replacement_pending" (now +. 30.0))
         else Hashtbl.remove state.amend_cooldowns "replacement_pending"
       | Sell -> ());
      if side = Buy then state.inflight_amend_buy <- false;
      ())
;;

(** Handles skipped order amendment (suppressed as a no-op by the executor).

    Clears the pending-amend entry and applies a short cooldown so the strategy does not
    re-push the same suppressed amendment every cycle (previously this produced a silent
    retry loop that looked like a hang). *)
let handle_order_amendment_skipped ~now asset_symbol order_id side _ =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
      state.pending_orders
      <- filter_keep_if_needed
           (fun (pending_id, _s, _p, _) -> not (pending_amend_for pending_id order_id))
           state.pending_orders;
      (* Throttle re-issue of a suppressed amendment. *)
      Hashtbl.replace state.amend_cooldowns order_id (now +. 5.0);
      (* A suppressed amendment is a terminal outcome of the amend lifecycle: clear the
         in-flight flag or it stays set forever, silently blocking every later qty-only
         amendment on this symbol (the qty_mismatch gate requires
         [not state.inflight_amend_buy]). *)
      (match side with
       | Buy -> state.inflight_amend_buy <- false
       | Sell -> ());
      ())
;;

(** Handles order amendment failure. *)
let handle_order_amendment_failed ~now asset_symbol order_id side reason =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
      state.pending_orders
      <- filter_keep_if_needed
           (fun (pending_id, _s, _p, _) -> not (pending_amend_for pending_id order_id))
           state.pending_orders;
      let lower_reason = String.lowercase_ascii reason in
      let is_cache_miss =
        contains_fragment lower_reason "order not found"
        || contains_fragment lower_reason "not found for amendment"
        || contains_fragment lower_reason "unknown order"
      in
      let is_cannot_modify =
        contains_fragment lower_reason "cannot modify canceled or filled"
      in
      (* Alpaca reports a terminal order on the amend/fallback-cancel path as
         [order is already in "filled" state] (JSON-escaped, so the embedded quotes carry
         backslashes). Match the stable words rather than the quoted fragment. Such an
         order is gone: keeping it tracked re-issues the same doomed cancel+replace on
         every cooldown. *)
      let is_already_terminal =
        (contains_fragment lower_reason "already in"
         && (contains_fragment lower_reason "filled"
             || contains_fragment lower_reason "canceled"
             || contains_fragment lower_reason "cancelled"))
        || contains_fragment lower_reason "already filled"
        || contains_fragment lower_reason "already canceled"
        || contains_fragment lower_reason "already cancelled"
      in
      let is_rate_limit =
        contains_fragment lower_reason "too many cumulative requests"
        || contains_fragment lower_reason "rate limit"
      in
      let cooldown_duration =
        if is_rate_limit
        then 10.0
        else if is_cache_miss || is_cannot_modify || is_already_terminal
        then 0.5
        else 2.0
      in
      Hashtbl.replace state.amend_cooldowns order_id (now +. cooldown_duration);
      (* Terminal outcome of the amend lifecycle: clear the in-flight flag so later
         qty-only amendments are not silently blocked. *)
      (match side with
       | Buy -> state.inflight_amend_buy <- false
       | Sell -> ());
      let is_tif_rejection =
        contains_fragment lower_reason "time in force"
        || contains_fragment lower_reason "alo"
        || contains_fragment lower_reason "post-only"
        || contains_fragment lower_reason "post only"
        || contains_fragment lower_reason "cross"
      in
      (* An "insufficient qty" amendment failure deliberately does NOT mean the order is
         gone. On the Alpaca fractional cancel-and-replace path that message comes from
         the *replacement* placement ("insufficient qty available for order ...
         held_for_orders ..."), which fails precisely because the original order is still
         resting. Dropping tracking for it released the sell commitment while the venue
         still held the shares, and the next cycle re-issued the same doomed amend - the
         source of the repeating 403/422 storm. Keep tracking and cool down instead; the
         order's own terminal event or the cancel path will retire it. *)
      let is_order_gone =
        is_cache_miss || is_cannot_modify || is_tif_rejection || is_already_terminal
      in
      (* A venue-terminal order can linger in the local open-order cache if the terminal
         trade-update was missed. Evict it from the open-orders scan so the adoption path
         cannot re-adopt the stale entry as the resting buy and re-issue the same failed
         cancel+replace. The TTL mirrors the cleanup window used by [sync_open_orders];
         the entry is harmless once the order also leaves the venue cache. Apply this to
         every "order is gone" verdict, not just the explicit terminal statuses: a
         cache-miss amend is just as dead, and re-adopting it from a stale feed snapshot
         reproduces the same doomed-amend loop. *)
      if is_order_gone
      then Hashtbl.replace state.evicted_orders order_id (now +. 900.0, side);
      if is_order_gone
      then (
        let cancel_order =
          create_cancel_order order_id asset_symbol Ladder state.exchange_id
        in
        ignore (push_order ~now ~state cancel_order);
        if side = Buy then state.inflight_cancel_buy <- true;
        (match side with
         | Buy when is_tif_rejection ->
           (* The trailing/amended buy died to a TIF/ALO/post-only reject - typically a
              violent move making the target momentarily cross. Arm TIF recovery so the
              buy leg re-attempts promptly at a fresh (re-validated) price instead of
              leaving the asset buyless while the capital oracle is INACTIVE. *)
           state.tif_recovery_pending <- true;
           state.tif_recovery_since <- Unix.gettimeofday ();
           Logging.info_f
             ~section
             "TIF recovery armed for %s - buy leg will re-attempt through the oracle \
              halt (window 900s)"
             asset_symbol
         | _ -> ());
        match side with
        | Buy ->
          (match state.last_buy_order_id with
           | Some target_id when target_id = order_id ->
             Hashtbl.remove state.amend_cooldowns "place_Buy";
             (* The amend target is dead. If the venue already listed the replacement (or
                another live grid buy), re-track it instead of clearing tracking and
                placing a duplicate. This is the recovery path when the amend ack did not
                carry the replacement oid. *)
             let replacement =
               let best_id = state.cached_best_buy_id in
               let best_price = state.cached_best_buy_price in
               if best_id <> ""
                  && best_id <> order_id
                  && best_price > 0.0
                  && (not (Hashtbl.mem state.evicted_orders best_id))
                  && not (Hashtbl.mem state.processed_fills best_id)
               then Some (best_id, best_price)
               else None
             in
             (match replacement with
              | Some (replacement_id, replacement_price) ->
                add_tracked_order_id state replacement_id;
                state.last_buy_order_id <- Some replacement_id;
                state.last_buy_order_price <- Some replacement_price;
                Logging.info_f
                  ~section
                  "Amendment failed for buy order %s: %s. Adopted live replacement %s @ \
                   %.4f instead of replacing."
                  order_id
                  reason
                  replacement_id
                  replacement_price
              | None ->
                state.last_buy_order_id <- None;
                state.last_buy_order_price <- None;
                Logging.info_f
                  ~section
                  "Amendment failed for buy order %s: %s. Order is gone, cleared \
                   tracking."
                  order_id
                  reason)
           | _ ->
             Hashtbl.remove state.amend_cooldowns "place_Buy";
             ())
        | Sell ->
          let original_sell_count = Sell_orders.length state.open_sell_orders in
          ignore (Sell_orders.remove_by_id state.open_sell_orders order_id);
          remove_sell_commitment ~state ~id:order_id;
          if Sell_orders.length state.open_sell_orders < original_sell_count
          then
            Logging.info_f
              ~section
              "Amendment failed for sell order %s: %s. Order is gone, cleared tracking."
              order_id
              reason)
      else (
        match side with
        | Buy ->
          (match state.last_buy_order_id with
           | Some target_id when target_id = order_id ->
             Logging.info_f
               ~section
               "Amendment failed for buy order %s: %s. Applying %.1fs cooldown (keeping \
                tracking)."
               order_id
               reason
               cooldown_duration
           | _ -> ())
        | Sell ->
          Logging.info_f
            ~section
            "Amendment failed for sell order %s: %s. Applying %.1fs cooldown (keeping \
             tracking)."
            order_id
            reason
            cooldown_duration);
      ignore
        (InFlightOrders.remove_in_flight_order
           (match side with
            | Buy -> state.duplicate_key_buy
            | Sell -> state.duplicate_key_sell)))
;;

(** Cleans up in-flight cancellation markers on cancellation failure. Runs on the domain
    thread (drained as a [Cancel_cleanup] lifecycle event), so the flag writes below are
    mutex-synchronized like every other handler - the supervisor's REST callbacks enqueue
    this event instead of mutating strategy state cross-domain. *)
let cleanup_pending_cancellation asset_symbol _order_id =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
      state.inflight_cancel_buy <- false;
      state.inflight_amend_buy <- false)
;;

(* Drain lifecycle events queued by the supervisor REST path and dispatch them to the
   handlers. Runs on the domain thread at the top of every cycle, so every handler
   invocation (REST- or WS-sourced) executes on the domain thread and the strategy mutex
   is never shared across threads. *)
(** Behavioral-equivalence event capture: normalize a lifecycle or execution event into a
    serializable {!Strategy_trace.event_obs}, and re-apply a recorded one through the same
    handlers. The reference handlers remain the single source of behavior; this only adds
    a record/replay seam so the differential harness can feed the exact event stream. *)
let side_to_str (s : order_side) =
  match s with
  | Buy -> "buy"
  | Sell -> "sell"
;;

let side_of_str = function
  | "sell" -> Sell
  | _ -> Buy
;;

let blank_event : Strategy_trace.event_obs =
  { ev_kind = ""
  ; ev_now = 0.0
  ; ev_order_id = ""
  ; ev_new_order_id = ""
  ; ev_side = ""
  ; ev_price = 0.0
  ; ev_qty = 0.0
  ; ev_cl_ord_id = None
  ; ev_reason = ""
  }
;;

let event_to_obs = function
  | Ack { now; order_id; side; price } ->
    { blank_event with
      ev_kind = "acknowledged"
    ; ev_now = now
    ; ev_order_id = order_id
    ; ev_side = side_to_str side
    ; ev_price = price
    }
  | Failed { now; side; reason } ->
    { blank_event with
      ev_kind = "failed"
    ; ev_now = now
    ; ev_side = side_to_str side
    ; ev_reason = reason
    }
  | Rejected { now; side; price } ->
    { blank_event with
      ev_kind = "rejected"
    ; ev_now = now
    ; ev_side = side_to_str side
    ; ev_price = price
    }
  | Amended { now; old_id; new_id; side; price } ->
    { blank_event with
      ev_kind = "amended"
    ; ev_now = now
    ; ev_order_id = old_id
    ; ev_new_order_id = new_id
    ; ev_side = side_to_str side
    ; ev_price = price
    }
  | Amendment_skipped { now; order_id; side; price } ->
    { blank_event with
      ev_kind = "amendment_skipped"
    ; ev_now = now
    ; ev_order_id = order_id
    ; ev_side = side_to_str side
    ; ev_price = price
    }
  | Amendment_failed { now; order_id; side; reason } ->
    { blank_event with
      ev_kind = "amendment_failed"
    ; ev_now = now
    ; ev_order_id = order_id
    ; ev_side = side_to_str side
    ; ev_reason = reason
    }
  | Cancel_cleanup { order_id } ->
    { blank_event with ev_kind = "cancel_cleanup"; ev_order_id = order_id }
;;

(** Record an execution-path event (called from the domain loop before dispatching it to a
    handler). *)
let record_exec_event
  symbol
  ~kind
  ~now
  ?(order_id = "")
  ?(new_order_id = "")
  ?(side = "")
  ?(price = 0.0)
  ?(qty = 0.0)
  ?(cl_ord_id = None)
  ?(reason = "")
  ()
  =
  Strategy_event_recorder.record_event_if_active
    symbol
    { ev_kind = kind
    ; ev_now = now
    ; ev_order_id = order_id
    ; ev_new_order_id = new_order_id
    ; ev_side = side
    ; ev_price = price
    ; ev_qty = qty
    ; ev_cl_ord_id = cl_ord_id
    ; ev_reason = reason
    }
;;

(** Re-apply a recorded event through the reference handlers. Used by the replay driver. *)
let apply_event symbol (e : Strategy_trace.event_obs) =
  match e.ev_kind with
  | "filled" ->
    handle_order_filled
      ~now:e.ev_now
      symbol
      e.ev_order_id
      (side_of_str e.ev_side)
      ~fill_price:e.ev_price
      ~fill_qty:e.ev_qty
      e.ev_cl_ord_id
  | "cancelled" ->
    handle_order_cancelled
      ~now:e.ev_now
      symbol
      e.ev_order_id
      (side_of_str e.ev_side)
      e.ev_cl_ord_id
  | "acknowledged" ->
    handle_order_acknowledged
      ~now:e.ev_now
      symbol
      e.ev_order_id
      (side_of_str e.ev_side)
      e.ev_price
  | "amended" ->
    handle_order_amended
      ~now:e.ev_now
      symbol
      e.ev_order_id
      e.ev_new_order_id
      (side_of_str e.ev_side)
      e.ev_price
  | "failed" ->
    handle_order_failed ~now:e.ev_now symbol (side_of_str e.ev_side) e.ev_reason
  | "rejected" ->
    handle_order_rejected ~now:e.ev_now symbol (side_of_str e.ev_side) e.ev_price
  | "amendment_skipped" ->
    handle_order_amendment_skipped
      ~now:e.ev_now
      symbol
      e.ev_order_id
      (side_of_str e.ev_side)
      e.ev_price
  | "amendment_failed" ->
    handle_order_amendment_failed
      ~now:e.ev_now
      symbol
      e.ev_order_id
      (side_of_str e.ev_side)
      e.ev_reason
  | "cancel_cleanup" -> cleanup_pending_cancellation symbol e.ev_order_id
  | _ -> ()
;;

let dispatch_event symbol (ev : lifecycle_event) =
  Strategy_event_recorder.record_event_if_active symbol (event_to_obs ev);
  match ev with
  | Ack { now; order_id; side; price } ->
    handle_order_acknowledged ~now symbol order_id side price
  | Failed { now; side; reason } -> handle_order_failed ~now symbol side reason
  | Rejected { now; side; price } -> handle_order_rejected ~now symbol side price
  | Amended { now; old_id; new_id; side; price } ->
    handle_order_amended ~now symbol old_id new_id side price
  | Amendment_skipped { now; order_id; side; price } ->
    handle_order_amendment_skipped ~now symbol order_id side price
  | Amendment_failed { now; order_id; side; reason } ->
    handle_order_amendment_failed ~now symbol order_id side reason
  | Cancel_cleanup { order_id } -> cleanup_pending_cancellation symbol order_id
;;

let drain_events symbol =
  let q = get_event_queue symbol in
  let n = ref 0 in
  let rec loop () =
    match LockFreeQueue.read q with
    | Some ev ->
      incr n;
      dispatch_event symbol ev;
      loop ()
    | None -> ()
  in
  loop ();
  !n
;;

(** Like [drain_events] but hands each event to [f] instead of dispatching it. Used when a
    file-bound asset routes lifecycle events through the interpreter. *)
let drain_events_with symbol (f : lifecycle_event -> unit) =
  let q = get_event_queue symbol in
  let n = ref 0 in
  let rec loop () =
    match LockFreeQueue.read q with
    | Some ev ->
      incr n;
      f ev;
      loop ()
    | None -> ()
  in
  loop ();
  !n
;;

(** Normalize a lifecycle event to an interpreter event (kind + fields) so a strategy file
    can own the lifecycle surface. Field names mirror {!Strategy_trace.event_obs}. *)
let runtime_event_of_lifecycle (ev : lifecycle_event) : Strategy_runtime.event =
  let open Strategy_expr in
  let side_v s = V_string (side_to_str s) in
  match ev with
  | Ack { now; order_id; side; price } ->
    Strategy_runtime.make_event
      "acknowledged"
      [ "now", V_float now
      ; "order_id", V_string order_id
      ; "side", side_v side
      ; "price", V_float price
      ]
  | Failed { now; side; reason } ->
    Strategy_runtime.make_event
      "failed"
      [ "now", V_float now; "side", side_v side; "reason", V_string reason ]
  | Rejected { now; side; price } ->
    Strategy_runtime.make_event
      "rejected"
      [ "now", V_float now; "side", side_v side; "price", V_float price ]
  | Amended { now; old_id; new_id; side; price } ->
    Strategy_runtime.make_event
      "amended"
      [ "now", V_float now
      ; "order_id", V_string old_id
      ; "new_order_id", V_string new_id
      ; "side", side_v side
      ; "price", V_float price
      ]
  | Amendment_skipped { now; order_id; side; price } ->
    Strategy_runtime.make_event
      "amendment_skipped"
      [ "now", V_float now
      ; "order_id", V_string order_id
      ; "side", side_v side
      ; "price", V_float price
      ]
  | Amendment_failed { now; order_id; side; reason } ->
    Strategy_runtime.make_event
      "amendment_failed"
      [ "now", V_float now
      ; "order_id", V_string order_id
      ; "side", side_v side
      ; "reason", V_string reason
      ]
  | Cancel_cleanup { order_id } ->
    Strategy_runtime.make_event "cancel_cleanup" [ "order_id", V_string order_id ]
;;
