(* Jacobs Ladder: strategy execution engine. *)

open Jacobs_ladder_types
open Strategy_venue
open Strategy_reservation
include Strategy_lifecycle
include Strategy_decision

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
