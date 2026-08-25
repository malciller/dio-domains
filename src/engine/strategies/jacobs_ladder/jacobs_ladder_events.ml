(* Jacobs Ladder - Order Lifecycle Event Handlers & Persistence *)

open Strategy_common
open Jacobs_ladder_types
open Jacobs_ladder_config
open Jacobs_ladder_reservation
open Jacobs_ladder_orders

(* per-symbol lock-free lifecycle event queue. The Lwt supervisor thread
   (REST callbacks, supervisor_orders.ml) enqueues lifecycle events instead of
   calling the handlers directly; the domain worker drains the queue at the
   top of every cycle, so ALL handler execution happens on the domain thread.
   The per-symbol state mutex is then never contended across threads (the
   supervisor never blocks on it, the domain never blocks on a REST batch).
   Each queue is single-consumer (its symbol's domain); LockFreeQueue is
   MPSC-safe, and enqueue signals Exchange_wakeup so an idle domain wakes to
   drain promptly. *)

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

let event_queues : (string, lifecycle_event LockFreeQueue.t) Hashtbl.t = Hashtbl.create 16
let event_queues_mutex = Mutex.create ()

let get_event_queue symbol =
  match Hashtbl.find_opt event_queues symbol with
  | Some q -> q
  | None ->
    Mutex.lock event_queues_mutex;
    let q =
      match Hashtbl.find_opt event_queues symbol with
      | Some q -> q
      | None ->
        let q = LockFreeQueue.create () in
        Hashtbl.replace event_queues symbol q;
        q
    in
    Mutex.unlock event_queues_mutex;
    q
;;

(** Enqueue a lifecycle event from any thread (supervisor REST path). Lock-free
    push plus a per-symbol wakeup so the domain drains it promptly. *)
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

(** Handles order placement acknowledgment. *)
let handle_order_acknowledged ~now asset_symbol order_id side price =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
       state.pending_orders
       <- List.filter
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
          state.last_buy_order_id <- Some order_id;
          state.last_buy_order_price <- Some price;
          state.inflight_buy <- false;
          state.inflight_amend_buy <- false;
          state.last_buy_attempted_insufficient <- false;
          ()
        | Sell ->
          state.inflight_sell <- false;
          (* Release the in-flight marker when the placement completes. The
             key is added by [push_order] at dispatch and must be removed here
             (the placement is acknowledged), not left latched until a fill/
             cancel/failure: while it stayed set, [has_active_sell] reported
             true for the entire time a sell rested on the book, which gated
             every later sell attempt behind a buy fill (the only event that
             force-cleared it). The marker now means "a sell placement is in
             flight", so a resting sell no longer blocks the next sell for new
             inventory - the inventory gate (available >= sell qty) is what
             prevents duplicates. *)
          ignore (InFlightOrders.remove_in_flight_order state.duplicate_key_sell);
          state.recently_injected_sells
          <- (order_id, price, now) :: state.recently_injected_sells;
          let replaced = ref false in
          state.open_sell_orders
          <- List.map
               (fun (oid, p, q) ->
                  if
                    (not !replaced)
                    && String.starts_with ~prefix:"pending_sell_" oid
                    && abs_float (p -. price) < price *. 0.01
                  then (
                    replaced := true;
                    order_id, p, q)
                  else oid, p, q)
               state.open_sell_orders;
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
       state.pending_orders
       <- List.filter (fun (_, s, _, _) -> s <> side) state.pending_orders;
       (match side with
        | Buy -> state.inflight_buy <- false
        | Sell -> state.inflight_sell <- false);
       (match side with
        | Buy -> set_asset_reserved_quote state 0.0
        | Sell ->
          state.open_sell_orders
          <- List.filter
               (fun (oid, _, _) -> not (String.starts_with ~prefix:"pending_sell_" oid))
               state.open_sell_orders);
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
       in
       let cooldown = if is_rate_limit || is_wash_trade then 10.0 else 2.0 in
       (match side with
        | Buy when is_insufficient_balance ->
          if state.last_buy_attempted_insufficient
          then
            (* The buy was knowingly placed against a stale, under-funded
               balance snapshot: the rejection is foreordained, so latching
               capital_low would pause buying on a snapshot that is about to
               be replaced by the fresh balance. The fresh store value on the
               next update governs (and the fresh-insufficient placement path
               latches capital_low itself when the shortage is real). *)
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
            Logging.warn_f
              ~section
              "Exchange rejected buy for %s with insufficient funds - setting \
               capital_low flag"
              asset_symbol)
        | Sell when is_insufficient_balance ->
          let ecfg = get_exchange_config state.exchange_id in
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
       <- List.filter
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
        | Buy -> state.inflight_buy <- false
        | Sell ->
          state.inflight_sell <- false;
          state.open_sell_orders
          <- List.filter
               (fun (oid, _, _) -> not (String.starts_with ~prefix:"pending_sell_" oid))
               state.open_sell_orders);
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
let handle_order_filled ~now:_ asset_symbol order_id side ~fill_price ~fill_qty cl_ord_id =
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
              Int64.compare (Int64.of_string order_id) (Int64.of_string persisted_oid)
              <= 0
            with
            | _ -> order_id = persisted_oid)
         | None -> state.startup_replay
       in
       let skip_fill = is_persisted_fill in
       if skip_fill
       then (
         add_processed_fill state order_id;
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
           if dominated then state.highest_startup_oid <- Some order_id))
       else (
         add_processed_fill state order_id;
         state.pending_orders
         <- List.filter
              (fun (pending_id, _, _, _) ->
                 not
                   (String.starts_with ~prefix:"pending_amend_" pending_id
                    && String.length pending_id > 14
                    && String.sub pending_id 14 (String.length pending_id - 14) = order_id
                   ))
              state.pending_orders;
         let sell_fill_price =
           match
             List.find_opt (fun (id, _, _) -> id = order_id) state.open_sell_orders
           with
           | Some (_, p, _) -> p
           | None -> fill_price
         in
         state.open_sell_orders
         <- List.filter
              (fun (sell_id, _, _) -> sell_id <> order_id)
              state.open_sell_orders;
         let _was_tracked_buy =
           match side, state.last_buy_order_id with
           | Buy, Some id when buy_tracking_matches_exchange_event id order_id cl_ord_id
             -> true
           | _ -> false
         in
         (* A fill for the OLD id of a just-replaced order (Hyperliquid/Alpaca
            cancel+create can fill the old order at the moment of
            replacement): the fill is real and its accounting below stands,
            but the RESTING buy is the replacement - clearing the buy
            tracking here would make the grid place a second resting buy. *)
         let is_superseded_old_fill =
           side = Buy
           && (not _was_tracked_buy)
           && InFlightAmendments.is_superseded order_id
         in
         if side = Buy
         then (
           (* Route the buy-fill bookkeeping through the base-accumulation
             store's pure decision logic (updates the last-buy reference for
             the next sell's profitability check). *)
           let updated =
             Dio_persistence.Base_accumulation_store.apply_buy_fill
               { Dio_persistence.Base_accumulation_store.reserved_base =
                   state.reserved_base
               ; accumulated_profit = state.accumulated_profit
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
           state.last_sell_fill_price <- None;
           state.last_sell_fill_qty <- None;
           (* Persistence dirty-marking follows the per-strategy config
              opt-in (base_accumulation), not a hardcoded venue list - all
              venues track identically now. *)
           if state.base_accumulation_enabled then state.persistence_dirty <- true;
           (* Spec-aligned buy fill: only the reference info for the next
              sell's profitability check is updated (done above via
              apply_buy_fill). The legacy per-fill slice retention into
              reserved_base is gone - accumulation happens at sell-fill time
              when the profit window exceeds the buffer (see below). *)
           if acc_qty > 0.0 && not state.startup_replay
           then (
             (* The anticipated credit mirrors what the venue actually
                 credits: on Hyperliquid-like spot venues the buy fee is
                 subtracted from the received BASE, so crediting the raw
                 fill qty would overstate inventory by the fee on every
                 fill - exactly the balance-vs-credit drift that lets a
                 sell dip into reserved_base. *)
             let credit_qty =
               if hl_like_spot_fee_exchange state.exchange_id && state.maker_fee > 0.0
               then Float.max 0.0 (acc_qty -. (state.maker_fee *. acc_qty))
               else acc_qty
             in
             state.anticipated_base_credit <- state.anticipated_base_credit +. credit_qty;
             Logging.info_f
               ~section
               "Anticipated base credit for %s: +%.8f (total: %.8f) from buy fill %s"
               asset_symbol
               credit_qty
               state.anticipated_base_credit
               order_id);
           (* A buy fill does not complete a sell placement: the sell's own
               ack/fill/cancel events own the sell in-flight lifecycle, so the
               sell markers ([inflight_sell], the duplicate-key latch, the
               recently-injected debounce) are left untouched here. Clearing
               them on buy fills was a workaround for the sell-ack latch leak
               (see handle_order_acknowledged) and let a fill tick push a new
               sell while the previous sell placement was still in flight. *)
           state.last_buy_attempted_insufficient <- false;
           if is_superseded_old_fill
           then
             Logging.info_f
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
            if state.persisted_sell_levels <> []
            then (
              let rec remove_one acc found = function
                | [] -> List.rev acc
                | (sp, _sq) :: rest
                  when (not found)
                       && (abs_float (sp -. sell_fill_price) <= sell_fill_price *. 0.0001
                           || abs_float (sp -. sell_fill_price) <= 1e-4) ->
                  remove_one acc true rest
                | item :: rest -> remove_one (item :: acc) found rest
              in
              state.persisted_sell_levels
              <- remove_one [] false state.persisted_sell_levels;
              state.persistence_dirty <- true);
            if acc_qty > 0.0
            then
              state.anticipated_base_credit
              <- Float.max 0.0 (state.anticipated_base_credit -. acc_qty);
            state.last_sell_fill_qty <- Some acc_qty;
            (* Spec-aligned sell fill: profit is measured against the LAST
               BUY fill (single local buy/sell cycle pair). The legacy
               prior-sell cost basis and grid-spread fallback are removed.
               All fees (both legs) are folded into a single [fees] figure
               passed to the store's pure decision logic, which:
                 - accrues net profit into accumulated_profit when > 0,
                 - when accumulated_profit >= base_cost + buffer (realtime F&G),
                   adds oracle_qty * (1 - sell_mult) to reserved_base and
                   debits accumulated_profit by base_cost, preserving the
                   buffer and surplus profit in the quote ledger. *)
            let store_t =
              { Dio_persistence.Base_accumulation_store.reserved_base =
                  state.reserved_base
              ; accumulated_profit = state.accumulated_profit
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
            <- updated.Dio_persistence.Base_accumulation_store.last_sell_fill_qty;
            if state.open_sell_orders = [] && state.persisted_sell_levels = []
            then state.last_buy_fill_price <- None
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
       (* The tracking reset is deferred only when THIS order is the subject
          of an in-flight amendment (the amend handler later re-keys the
          tracking to the replacement id). [state.inflight_amend_buy] must
          NOT be consulted here: it is a per-symbol flag that also covers
          amends of other orders, so a cancel for a different order would be
          swallowed and its reset lost forever. The registry and the
          pending_amend_ entries are keyed by the amended (old) order id,
          which is exactly the id the exchange's mid-amend cancel event
          carries. [is_amend_lifecycle_active] also covers the window AFTER
          the exchange confirmed a replace (Hyperliquid/Alpaca): the old
          order's cancel event can arrive after the amend response, and it is
          still the replace's side effect, not a real cancellation. *)
       let is_being_amended =
         InFlightAmendments.is_amend_lifecycle_active order_id
         || List.exists
              (fun (pending_id, _, _, _) ->
                 String.starts_with ~prefix:"pending_amend_" pending_id
                 && String.length pending_id > 14
                 && String.sub pending_id 14 (String.length pending_id - 14) = order_id)
              state.pending_orders
       in
       state.pending_orders
       <- List.filter
            (fun (pending_id, _, _, _) ->
               let matches =
                 pending_id = order_id
                 || (String.starts_with ~prefix:"pending_amend_" pending_id
                     && String.length pending_id > 14
                     && String.sub pending_id 14 (String.length pending_id - 14)
                        = order_id)
               in
               not matches)
            state.pending_orders;
       if not is_being_amended
       then (
         Hashtbl.remove state.amend_cooldowns order_id;
         Hashtbl.remove state.evicted_orders order_id;
         ignore (InFlightAmendments.remove_in_flight_amendment order_id);
         let cancelled_side = side in
         let was_tracked_buy =
           match state.last_buy_order_id with
           | Some id when buy_tracking_matches_exchange_event id order_id cl_ord_id ->
             true
           | _ -> false
         in
         if was_tracked_buy
         then (
           state.last_buy_order_id <- None;
           state.last_buy_order_price <- None;
           ());
         if cancelled_side = Buy
         then (
           state.inflight_cancel_buy <- false;
           state.inflight_amend_buy <- false;
           Hashtbl.remove state.amend_cooldowns "place_Buy");
         state.open_sell_orders
         <- List.filter
              (fun (sell_id, _, _) -> sell_id <> order_id)
              state.open_sell_orders;
         (match cancelled_side with
          | Buy -> state.inflight_buy <- false
          | Sell -> state.inflight_sell <- false);
         ignore
           (InFlightOrders.remove_in_flight_order
              (match cancelled_side with
               | Buy -> state.duplicate_key_buy
               | Sell -> state.duplicate_key_sell)))
       else
         Logging.info_f
           ~section
           "Order cancellation for %s (%s) ignored for tracking reset because order \
            amendment is in progress"
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
       <- List.filter
            (fun (pending_id, _s, _p, _) ->
               let matches_amend =
                 String.starts_with ~prefix:"pending_amend_" pending_id
                 && (String.sub pending_id 14 (String.length pending_id - 14)
                     = old_order_id
                     || String.sub pending_id 14 (String.length pending_id - 14)
                        = new_order_id)
               in
               not matches_amend)
            state.pending_orders;
       (match side with
        | Buy ->
          (match state.last_buy_order_id with
           | Some target_id when target_id = old_order_id ->
             state.last_buy_order_id <- Some new_order_id;
             state.last_buy_order_price <- Some price;
             if old_order_id = new_order_id
             then
               Logging.info_f
                 ~section
                 "Amended buy order price in tracking: %s @ %.4f for %s"
                 old_order_id
                 price
                 asset_symbol
             else
               Logging.info_f
                 ~section
                 "Amended buy order ID in tracking: %s -> %s @ %.4f for %s"
                 old_order_id
                 new_order_id
                 price
                 asset_symbol;
             state.inflight_amend_buy <- false
           | _ ->
             state.last_buy_order_id <- Some new_order_id;
             state.last_buy_order_price <- Some price;
             Logging.info_f
               ~section
               "External buy order amendment in tracking: %s @ %.4f for %s"
               new_order_id
               price
               asset_symbol;
             state.inflight_amend_buy <- false)
        | Sell ->
          let original_sell_count = List.length state.open_sell_orders in
          let old_entry =
            List.find_opt (fun (id, _, _) -> id = old_order_id) state.open_sell_orders
          in
          let old_qty =
            match old_entry with
            | Some (_, _, q) -> q
            | None -> venue_lot_qty state.grid_qty state.exchange_id state
          in
          Logging.info_f
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
          state.open_sell_orders
          <- (new_order_id, price, old_qty)
             :: List.filter
                  (fun (sell_id, _, _) -> sell_id <> old_order_id)
                  state.open_sell_orders;
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
          Logging.info_f
            ~section
            "SELL_AMEND [%s] result: sells_after=%d"
            asset_symbol
            (List.length state.open_sell_orders));
       let cooldown = if old_order_id = new_order_id then 2.0 else 10.0 in
       Hashtbl.replace state.amend_cooldowns old_order_id (now +. cooldown);
       Hashtbl.replace state.amend_cooldowns new_order_id (now +. cooldown);
       if side = Buy then state.inflight_amend_buy <- false;
       ())
;;

(** Handles skipped order amendment (suppressed as a no-op by the executor).

    Clears the pending-amend entry and applies a short cooldown so the
    strategy does not re-push the same suppressed amendment every cycle
    (previously this produced a silent retry loop that looked like a hang). *)
let handle_order_amendment_skipped ~now asset_symbol order_id side _ =
  let state = get_strategy_state asset_symbol in
  Mutex.lock state.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock state.mutex)
    (fun () ->
       state.pending_orders
       <- List.filter
            (fun (pending_id, _s, _p, _) ->
               let matches_amend =
                 String.starts_with ~prefix:"pending_amend_" pending_id
                 && String.sub pending_id 14 (String.length pending_id - 14) = order_id
               in
               not matches_amend)
            state.pending_orders;
       (* Throttle re-issue of a suppressed amendment. *)
       Hashtbl.replace state.amend_cooldowns order_id (now +. 5.0);
       (* A suppressed amendment is a terminal outcome of the amend lifecycle:
          clear the in-flight flag or it stays set forever, silently blocking
          every later qty-only amendment on this symbol (the qty_mismatch gate
          requires [not state.inflight_amend_buy]). *)
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
       <- List.filter
            (fun (pending_id, _s, _p, _) ->
               let matches_amend =
                 String.starts_with ~prefix:"pending_amend_" pending_id
                 && String.length pending_id > 14
                 && String.sub pending_id 14 (String.length pending_id - 14) = order_id
               in
               not matches_amend)
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
       let is_margin_error =
         contains_fragment lower_reason "insufficient"
         || contains_fragment lower_reason "margin"
       in
       let is_rate_limit =
         contains_fragment lower_reason "too many cumulative requests"
         || contains_fragment lower_reason "rate limit"
       in
       let cooldown_duration =
         if is_rate_limit
         then 10.0
         else if is_cache_miss || is_cannot_modify || is_margin_error
         then 0.5
         else 2.0
       in
       Hashtbl.replace state.amend_cooldowns order_id (now +. cooldown_duration);
       (* Terminal outcome of the amend lifecycle: clear the in-flight flag
          so later qty-only amendments are not silently blocked. *)
       (match side with
        | Buy -> state.inflight_amend_buy <- false
        | Sell -> ());
       let is_order_gone = is_cache_miss || is_cannot_modify || is_margin_error in
       if is_order_gone
       then (
         let cancel_order =
           create_cancel_order order_id asset_symbol Ladder state.exchange_id
         in
         ignore (push_order ~now ~state cancel_order);
         if side = Buy then state.inflight_cancel_buy <- true;
         match side with
         | Buy ->
           (match state.last_buy_order_id with
            | Some target_id when target_id = order_id ->
              state.last_buy_order_id <- None;
              state.last_buy_order_price <- None;
              Hashtbl.remove state.amend_cooldowns "place_Buy";
              Logging.info_f
                ~section
                "Amendment failed for buy order %s: %s. Order is gone, cleared tracking."
                order_id
                reason
            | _ ->
              Hashtbl.remove state.amend_cooldowns "place_Buy";
              ())
         | Sell ->
           let original_sell_count = List.length state.open_sell_orders in
           state.open_sell_orders
           <- List.filter
                (fun (sell_id, _, _) -> sell_id <> order_id)
                state.open_sell_orders;
           if List.length state.open_sell_orders < original_sell_count
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

(** No-op shim for pending cancellation cleanup. *)
let cleanup_pending_cancellation _asset_symbol _order_id = ()

(* drain lifecycle events queued by the supervisor REST path and dispatch
   them to the handlers. Runs on the domain thread at the top of every cycle,
   so every handler invocation (REST- or WS-sourced) executes on the domain
   thread, so the strategy mutex is never shared across threads. *)
let dispatch_event symbol (ev : lifecycle_event) =
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
;;

let drain_events symbol =
  let q = get_event_queue symbol in
  let rec loop () =
    match LockFreeQueue.read q with
    | Some ev ->
      dispatch_event symbol ev;
      loop ()
    | None -> ()
  in
  loop ()
;;
