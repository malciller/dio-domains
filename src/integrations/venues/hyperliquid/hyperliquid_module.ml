(** Exchange_intf implementation for the Hyperliquid L1 DEX. Bridges Hyperliquid-specific
    WebSocket feeds, REST actions, and instrument metadata into the exchange-agnostic
    interface consumed by strategies. *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

module Hyperliquid_impl = struct
  let name = "hyperliquid"
  let section = "hyperliquid_module"

  (* Testnet/mainnet toggle. Atomic for safe concurrent reads from feed threads. Defaults
     to mainnet: the engine sets this from config before any feed starts, but a reader
     that races that call must not silently point at the testnet endpoint, where a mainnet
     wallet's staking summary parses to nothing and the staked balance silently reads 0. *)
  let is_testnet = Atomic.make false

  let set_testnet testnet =
    Atomic.set is_testnet testnet;
    Logging.debug_f
      ~section
      "Hyperliquid module configured for %s"
      (if testnet then "testnet" else "mainnet")
  ;;

  (* Per-process counter for unique client order ids. *)
  let cloid_nonce_counter = Atomic.make 0

  (** Builds a unique client order id tagged with the strategy userref. Trailing 16 hex
      digits (low 64 bits, read by the executions feed's userref recovery): bits 63..56 =
      userref tag, bits 55..0 = nonce (unix seconds masked to 36 bits << 20 | per-process
      counter, 20 bits). The time component makes the cloid unique across restarts, so the
      placement retry loop is idempotent: every attempt of one logical order reuses the
      same cloid, and two orders never share one. *)
  let next_unique_cloid (uref : int) : string =
    let counter =
      Int64.of_int (Atomic.fetch_and_add cloid_nonce_counter 1 land 0xF_FFFF)
    in
    let time_component =
      Int64.logand (Int64.of_float (Unix.gettimeofday ())) 0xF_FFFF_FFFFL
    in
    let nonce =
      Int64.logor
        (Int64.shift_left (Int64.logand time_component 0xF_FFFF_FFFFL) 20)
        counter
    in
    let low64 =
      Int64.logor
        (Int64.shift_left (Int64.of_int (uref land 0xFF)) 56)
        (Int64.logand nonce 0x00FF_FFFF_FFFF_FFFFL)
    in
    Printf.sprintf "0x%032Lx" low64
  ;;

  (* Per-symbol fee cache. Maps symbol to (perp_maker, perp_taker, spot_maker,
     spot_taker). Populated once during initialize_fees and read on every get_fees call. *)
  let fee_cache : (string, float * float * float * float) Hashtbl.t = Hashtbl.create 16

  (* Type conversion helpers: Hyperliquid-specific variants to Exchange_intf.Types *)
  let string_of_order_type = function
    | Types.Limit -> "limit"
    | Types.Market -> "market"
    | Types.Other s -> s
    | _ -> "limit" (* Fallback: unsupported order types default to limit *)
  ;;

  let string_of_side = function
    | Types.Buy -> "buy"
    | Types.Sell -> "sell"
  ;;

  let status_of_hyperliquid_status = function
    | Hyperliquid_executions_feed.PendingNewStatus -> Types.New
    | Hyperliquid_executions_feed.NewStatus -> Types.New
    | Hyperliquid_executions_feed.PartiallyFilledStatus -> Types.PartiallyFilled
    | Hyperliquid_executions_feed.FilledStatus -> Types.Filled
    | Hyperliquid_executions_feed.CanceledStatus -> Types.Canceled
    | Hyperliquid_executions_feed.ExpiredStatus -> Types.Expired
    | Hyperliquid_executions_feed.RejectedStatus -> Types.Rejected
    | Hyperliquid_executions_feed.UnknownStatus s -> Types.Unknown s
  ;;

  let side_of_hyperliquid_side = function
    | Hyperliquid_executions_feed.Buy -> Types.Buy
    | Hyperliquid_executions_feed.Sell -> Types.Sell
  ;;

  (** Place a new order on Hyperliquid. Market orders are emulated as aggressive IOC limit
      orders with 5% slippage. Prices and quantities are rounded to the instrument's tick
      and lot size before submission. *)
  let place_order
    ~token:_
    (* Unused: Hyperliquid authenticates via wallet address and private key from env *)
    ~order_type
    ~side
    ~qty
    ~symbol
    ?limit_price
    ?time_in_force
    ?post_only
    ?reduce_only
    ?order_userref
    ?cl_ord_id
    ?trigger_price:_
    ?display_qty:_
    ?retry_config:_
    ()
    =
    let is_limit =
      match order_type with
      | Types.Limit -> true
      | _ -> false
    in
    let side_bool =
      match side with
      | Types.Buy -> true
      | Types.Sell -> false
    in
    let px =
      match limit_price with
      | Some p -> p
      | None ->
        (match order_type with
         | Types.Market ->
           (* Hyperliquid has no native market order type. Emulate via IOC limit order
              with 5% slippage from current BBO. *)
           (match Hyperliquid_orderbook_feed.get_best_bid_ask symbol with
            | Some (bid, _, ask, _) ->
              if side_bool
              then ask *. 1.05 (* Buy: 5% above ask *)
              else bid *. 0.95 (* Sell: 5% below bid *)
            | None ->
              Logging.warn_f
                ~section
                "No orderbook data available for market order on %s, using 0.0"
                symbol;
              0.0)
         | _ -> 0.0)
    in
    let hl_tif =
      if Option.value post_only ~default:false
      then Hyperliquid_types.Alo
      else (
        match time_in_force with
        | Some Types.IOC -> Hyperliquid_types.Ioc
        | Some Types.GTC -> Hyperliquid_types.Gtc
        | Some Types.FOK -> Hyperliquid_types.Ioc
        | None -> Hyperliquid_types.Gtc)
    in
    let px_rounded =
      Hyperliquid_instruments_feed.round_price_to_tick_for_symbol symbol px
    in
    let sz_rounded = Hyperliquid_instruments_feed.round_qty_to_lot symbol qty in
    let constructed_cl_ord_id =
      match order_userref with
      | Some uref ->
        (match cl_ord_id with
         | Some explicit -> Some explicit
         | None ->
           (* Unique per-order cloid makes the placement retry loop idempotent (a
              post-acceptance timeout replays the same cloid rather than landing a second
              order) and keeps concurrent orders distinct. *)
           Some (next_unique_cloid uref))
      | None -> cl_ord_id
    in
    Hyperliquid_actions.place_order
      ~symbol
      ~is_buy:side_bool
      ~sz:sz_rounded
      ~px:px_rounded
      ~is_limit
      ?post_only
      ?reduce_only
      ?cl_ord_id:constructed_cl_ord_id
      ~time_in_force:hl_tif
      ~testnet:(Atomic.get is_testnet)
      ()
    >|= function
    | Ok res ->
      let order_id_str = Int64.to_string res.Hyperliquid_actions.order_id in
      (* Inject into open_orders so immediate amendments can resolve the order; otherwise
         it is invisible until the next webData2 push (~1.5s). *)
      let hl_side =
        match side with
        | Types.Buy -> Hyperliquid_executions_feed.Buy
        | _ -> Hyperliquid_executions_feed.Sell
      in
      Hyperliquid_executions_feed.inject_order
        ~symbol
        ~order_id:order_id_str
        ~side:hl_side
        ~qty:sz_rounded
        ~price:px_rounded
        ?user_ref:
          (match order_userref with
           | Some u -> Some u
           | None -> None)
        ?cl_ord_id:constructed_cl_ord_id
        ();
      Ok { Types.order_id = order_id_str; cl_ord_id = None; order_userref = None }
    | Error e -> Error e
  ;;

  (** Amend an existing order (cancel-replace on Hyperliquid). Looks up the current order
      state from the executions feed to fill in any parameters not explicitly provided by
      the caller. *)
  let amend_order
    ~token:_
    ~order_id
    ?cl_ord_id
    ?qty
    ?limit_price
    ?post_only
    ?reduce_only
    ?time_in_force
    ?trigger_price:_
    ?display_qty:_
    ?symbol
    ?retry_config:_
    ()
    =
    match symbol with
    | None -> Lwt.return (Error "Symbol is required for Hyperliquid amendment")
    | Some sym ->
      (* Resolve current order state for side, price, and qty defaults *)
      (match Hyperliquid_executions_feed.find_order_everywhere order_id with
       | None ->
         Lwt.return (Error (Printf.sprintf "Order not found for amendment: %s" order_id))
       | Some existing ->
         let is_buy =
           match existing.side with
           | Hyperliquid_executions_feed.Buy -> true
           | Hyperliquid_executions_feed.Sell -> false
         in
         let px =
           match limit_price with
           | Some p -> p
           | None -> Option.value existing.limit_price ~default:0.0
         in
         let sz = Option.value qty ~default:existing.order_qty in
         let px_rounded =
           Hyperliquid_instruments_feed.round_price_to_tick_for_symbol sym px
         in
         let sz_rounded = Hyperliquid_instruments_feed.round_qty_to_lot sym sz in
         let hl_tif =
           if Option.value post_only ~default:false
           then Hyperliquid_types.Alo
           else (
             match time_in_force with
             | Some Types.IOC -> Hyperliquid_types.Ioc
             | Some Types.GTC -> Hyperliquid_types.Gtc
             | Some Types.FOK -> Hyperliquid_types.Ioc
             | None -> Hyperliquid_types.Gtc)
         in
         let constructed_cl_ord_id =
           match existing.order_userref with
           | Some uref ->
             (match cl_ord_id with
              | Some explicit -> Some explicit
              | None ->
                (* New cancel-replace leg gets its own unique cloid. *)
                Some (next_unique_cloid uref))
           | None -> cl_ord_id
         in
         Hyperliquid_actions.amend_order
           ~symbol:sym
           ~order_id:(Int64.of_string order_id)
           ~is_buy
           ~px:px_rounded
           ~sz:sz_rounded
           ~reduce_only:(Option.value reduce_only ~default:false)
           ~tif:hl_tif
           ?cl_ord_id:constructed_cl_ord_id
           ~testnet:(Atomic.get is_testnet)
           ()
         >|= (function
          | Ok res ->
            let new_order_id_str = Int64.to_string res.amend_id in
            (* Hyperliquid replaces the order with a new OID, so the old OID is retired
               here (which blacklists a late WS "open" event for it) and the replacement
               is injected with the new price/size. Without the injection the open-orders
               cache has no entry for this instrument until the WS "open" event lands, and
               [order.replace] - which matches against that cache - falls through to a
               fresh Place. That duplicate covers the same reduce-only position, so the
               venue cancels the amended order with "reduceOnlyCanceled" and every tick of
               drift cancels and re-creates the take-profit. Mirror [place_order]'s
               injection so the cache reflects the venue the moment the amend is acked. *)
            if new_order_id_str <> order_id
            then (
              Hyperliquid_executions_feed.remove_open_order
                ~symbol:(Hyperliquid_executions_feed.store_symbol_for_order sym)
                ~order_id;
              Hyperliquid_executions_feed.inject_order
                ~symbol:sym
                ~order_id:new_order_id_str
                ~side:
                  (if is_buy
                   then Hyperliquid_executions_feed.Buy
                   else Hyperliquid_executions_feed.Sell)
                ~qty:sz_rounded
                ~price:px_rounded
                ?user_ref:existing.order_userref
                ?cl_ord_id:constructed_cl_ord_id
                ());
            Ok
              { Types.original_order_id = Int64.to_string res.order_id
              ; Types.new_order_id = new_order_id_str
              ; Types.amend_id = None
              ; Types.cl_ord_id = constructed_cl_ord_id
              }
          | Error e -> Error e))
  ;;

  (** Cancel one or more orders by order ID. Orders are grouped by symbol for
      Hyperliquid's per-coin cancel endpoint. Each symbol group is processed sequentially
      to avoid shared-state races. *)
  let cancel_orders
    ~token:_
    ?order_ids
    ?cl_ord_ids:_
    ?order_userrefs:_
    ?symbol:_
    ?retry_config:_
    ()
    =
    match order_ids with
    | None -> Lwt.return (Ok [])
    | Some ids ->
      (* Group order IDs by symbol; Hyperliquid requires per-coin cancel requests *)
      let symbol_map = Hashtbl.create 4 in
      List.iter
        (fun id ->
          match Hyperliquid_executions_feed.find_order_everywhere id with
          | Some o ->
            let existing =
              try Hashtbl.find symbol_map o.symbol with
              | _ -> []
            in
            Hashtbl.replace symbol_map o.symbol (id :: existing)
          | None -> ())
        ids;
      (* Process symbol groups sequentially to avoid concurrent mutation of shared state *)
      let symbol_groups =
        Hashtbl.fold (fun sym ids acc -> (sym, ids) :: acc) symbol_map []
      in
      Lwt_list.fold_left_s
        (fun (results_acc, errors_acc) (symbol, ids_for_symbol) ->
          Hyperliquid_actions.cancel_orders
            ~symbol
            ~order_ids:(List.map Int64.of_string ids_for_symbol)
            ~testnet:(Atomic.get is_testnet)
          >|= function
          | Ok () ->
            (* Proactively remove cancelled orders from open_orders. Prevents ghost
               entries if the orderUpdates WS message is delayed or lost. *)
            List.iter
              (fun id ->
                Hyperliquid_executions_feed.remove_open_order
                  ~symbol:(Hyperliquid_executions_feed.store_symbol_for_order symbol)
                  ~order_id:id)
              ids_for_symbol;
            let new_results =
              List.map
                (fun id -> { Types.order_id = id; cl_ord_id = None })
                ids_for_symbol
            in
            new_results @ results_acc, errors_acc
          | Error e -> results_acc, e :: errors_acc)
        ([], [])
        symbol_groups
      >>= fun (results, errors) ->
      if errors <> []
      then Lwt.return (Error (String.concat "; " errors))
      else Lwt.return (Ok results)
  ;;

  (** Market data accessors. Delegate to the corresponding feed modules. *)

  let subscribe_orderbook ~symbols =
    Lwt_list.iter_s
      (fun symbol ->
        let _ = Hyperliquid_orderbook_feed.ensure_store symbol in
        let _ = Hyperliquid_executions_feed.get_symbol_store symbol in
        let coin = Hyperliquid_instruments_feed.get_subscription_coin symbol in
        Hyperliquid_ws.subscribe
          (`Assoc
            [ "method", `String "subscribe"
            ; "subscription", `Assoc [ "type", `String "l2Book"; "coin", `String coin ]
            ]))
      symbols
  ;;

  let get_top_of_book ~symbol =
    match Hyperliquid_orderbook_feed.get_best_bid_ask symbol with
    | Some (bp, bs, ap, as_val) -> Some (bp, bs, ap, as_val)
    | None -> None
  ;;

  let get_top_of_book_fast ~symbol =
    Hyperliquid_orderbook_feed.get_best_bid_ask_fast symbol
  ;;

  let has_orderbook_data ~symbol = Hyperliquid_orderbook_feed.has_orderbook_data symbol
  let get_tradeable_balance ~asset = Hyperliquid_balances.get_balance asset

  let get_tradeable_balance_fast ~asset =
    let store = Hyperliquid_balances.get_balance_store asset in
    fun () -> Hyperliquid_balances.BalanceStore.get_balance store
  ;;

  (* Hyperliquid's tradeable figure is already hold-netted. *)
  let get_available_balance_fast = get_tradeable_balance_fast

  (** Age in seconds of the balance snapshot for [asset], or [None] before the first
      update. Keyed on spendable wallets: the store-wide timestamp is also bumped by the
      ~10s staking poller, which cannot change the tradeable figure. Falls back to the
      store-wide timestamp when no spendable record exists. *)
  let get_balance_age_fast ~asset =
    let store = Hyperliquid_balances.get_balance_store asset in
    fun () ->
      let spendable =
        Hyperliquid_balances.BalanceStore.get_spendable_last_updated store
      in
      let last =
        if spendable > 0.0
        then spendable
        else Hyperliquid_balances.BalanceStore.get_last_updated store
      in
      if last > 0.0 then Some (Unix.gettimeofday () -. last) else None
  ;;

  (** Venue time at which the tradeable figure for [asset] last changed, or [None] before
      the first update. Keyed on spendable wallets to match [get_balance_age_fast]: the
      store-wide timestamp is also bumped by the ~10s staking poller, which cannot move
      the tradeable figure. A caller comparing this against an event time learns whether
      the venue has processed that fill or cancel, without reading a clock. *)
  let get_balance_updated_at ~asset =
    let store = Hyperliquid_balances.get_balance_store asset in
    let spendable = Hyperliquid_balances.BalanceStore.get_spendable_last_updated store in
    let last =
      if spendable > 0.0
      then spendable
      else Hyperliquid_balances.BalanceStore.get_last_updated store
    in
    if last > 0.0 then Some last else None
  ;;

  let get_balance_updated_at_fast ~asset =
    let store = Hyperliquid_balances.get_balance_store asset in
    fun () ->
      let spendable =
        Hyperliquid_balances.BalanceStore.get_spendable_last_updated store
      in
      let last =
        if spendable > 0.0
        then spendable
        else Hyperliquid_balances.BalanceStore.get_last_updated store
      in
      if last > 0.0 then Some last else None
  ;;

  let get_total_balance ~asset = Hyperliquid_balances.get_total_balance asset
  let get_staked_balance ~asset = Hyperliquid_balances.get_staked_balance asset

  (** Installed after the snapshot fetchers below, which live at file scope. *)
  let resync_hook : (unit -> unit) ref = ref (fun () -> ())

  let resync_market_state ~symbol:_ () = !resync_hook ()

  let get_all_balances () =
    let assets = Hyperliquid_balances.get_all_assets () in
    List.filter_map
      (fun asset ->
        let bal = Hyperliquid_balances.get_total_balance asset in
        if bal > 0.0 then Some (asset, bal) else None)
      assets
  ;;

  let get_open_order ~symbol ~order_id =
    match Hyperliquid_executions_feed.get_open_order symbol order_id with
    | Some o ->
      Some
        { Types.order_id = o.order_id
        ; symbol = o.symbol
        ; side = side_of_hyperliquid_side o.side
        ; qty = o.order_qty
        ; cum_qty = o.cum_qty
        ; remaining_qty = o.remaining_qty
        ; limit_price = o.limit_price
        ; status = status_of_hyperliquid_status o.order_status
        ; user_ref = o.order_userref
        ; cl_ord_id = o.cl_ord_id
        }
    | None -> None
  ;;

  let open_orders_for_symbol ~symbol =
    if String.contains symbol '/'
    then Hyperliquid_executions_feed.get_spot_open_orders symbol
    else Hyperliquid_executions_feed.get_perp_open_orders ~coin:symbol
  ;;

  let fold_orders_for_symbol ~symbol ~init ~f =
    if String.contains symbol '/'
    then Hyperliquid_executions_feed.fold_spot_open_orders symbol ~init ~f
    else
      List.fold_left
        (fun acc o -> f acc o)
        init
        (Hyperliquid_executions_feed.get_perp_open_orders ~coin:symbol)
  ;;

  let get_open_orders ~symbol =
    let orders = open_orders_for_symbol ~symbol in
    List.map
      (fun (o : Hyperliquid_executions_feed.open_order) ->
        { Types.order_id = o.order_id
        ; symbol = o.symbol
        ; side = side_of_hyperliquid_side o.side
        ; qty = o.order_qty
        ; cum_qty = o.cum_qty
        ; remaining_qty = o.remaining_qty
        ; limit_price = o.limit_price
        ; status = status_of_hyperliquid_status o.order_status
        ; user_ref = o.order_userref
        ; cl_ord_id = o.cl_ord_id
        })
      orders
  ;;

  let get_all_orders_for_asset ~asset =
    let prefix = asset ^ "/" in
    let all_symbols = Hyperliquid_executions_feed.get_all_symbols () in
    let matching = List.filter (fun sym -> String.starts_with ~prefix sym) all_symbols in
    List.concat_map (fun symbol -> get_open_orders ~symbol) matching
  ;;

  let fold_open_orders ~symbol ~init ~f =
    fold_orders_for_symbol
      ~symbol
      ~init
      ~f:(fun acc (o : Hyperliquid_executions_feed.open_order) ->
        f
          acc
          { Types.order_id = o.order_id
          ; symbol = o.symbol
          ; side = side_of_hyperliquid_side o.side
          ; qty = o.order_qty
          ; cum_qty = o.cum_qty
          ; remaining_qty = o.remaining_qty
          ; limit_price = o.limit_price
          ; status = status_of_hyperliquid_status o.order_status
          ; user_ref = o.order_userref
          ; cl_ord_id = o.cl_ord_id
          })
  ;;

  let iter_open_orders_fast ~symbol f =
    fold_orders_for_symbol
      ~symbol
      ~init:()
      ~f:(fun () (o : Hyperliquid_executions_feed.open_order) ->
        let limit_price =
          match o.limit_price with
          | Some p -> p
          | None -> 0.0
        in
        let side_str =
          match o.side with
          | Hyperliquid_executions_feed.Buy -> "buy"
          | Sell -> "sell"
        in
        f o.order_id limit_price o.remaining_qty side_str o.order_userref)
  ;;

  let get_open_orders_generation ~symbol =
    Hyperliquid_executions_feed.get_orders_generation_for_symbol symbol
  ;;

  let drain_open_order_changes ~symbol =
    Hyperliquid_executions_feed.drain_open_order_changes ~symbol
  ;;

  let get_execution_feed_position ~symbol =
    Hyperliquid_executions_feed.get_current_position symbol
  ;;

  let get_execution_feed_position_fast ~symbol =
    Hyperliquid_executions_feed.get_current_position_fast symbol
  ;;

  (** Return [true] once the initial execution snapshot has been ingested for [symbol]. *)
  let has_execution_data ~symbol = Hyperliquid_executions_feed.has_execution_data symbol

  let has_execution_data_fast ~symbol =
    Hyperliquid_executions_feed.has_execution_data_fast symbol
  ;;

  let read_execution_events ~symbol ~start_pos =
    let events = Hyperliquid_executions_feed.read_execution_events symbol start_pos in
    List.filter_map
      (fun (e : Hyperliquid_executions_feed.execution_event) ->
        match e.exec_type with
        | Hyperliquid_executions_feed.Filled ->
          None (* Deduplicate: rely on userEvents Trade event *)
        | _ ->
          Some
            { Types.order_id = e.order_id
            ; order_status = status_of_hyperliquid_status e.order_status
            ; limit_price = e.limit_price
            ; side = side_of_hyperliquid_side e.side
            ; remaining_qty = max 0.0 (e.order_qty -. e.cum_qty)
            ; filled_qty = e.cum_qty
            ; avg_price = e.avg_price
            ; timestamp = e.timestamp
            ; venue_time = e.venue_time
            ; is_amended = e.exec_type = Hyperliquid_executions_feed.Amended
            ; cl_ord_id = e.cl_ord_id
            })
      events
  ;;

  let iter_execution_events ~symbol ~start_pos f =
    Hyperliquid_executions_feed.iter_execution_events
      symbol
      start_pos
      (fun (e : Hyperliquid_executions_feed.execution_event) ->
         match e.exec_type with
         | Hyperliquid_executions_feed.Filled ->
           () (* Deduplicate: rely on userEvents Trade event *)
         | _ ->
           f
             { Types.order_id = e.order_id
             ; order_status = status_of_hyperliquid_status e.order_status
             ; limit_price = e.limit_price
             ; side = side_of_hyperliquid_side e.side
             ; remaining_qty = max 0.0 (e.order_qty -. e.cum_qty)
             ; filled_qty = e.cum_qty
             ; avg_price = e.avg_price
             ; timestamp = e.timestamp
             ; venue_time = e.venue_time
             ; is_amended = e.exec_type = Hyperliquid_executions_feed.Amended
             ; cl_ord_id = e.cl_ord_id
             })
  ;;

  let get_orderbook_position ~symbol =
    Hyperliquid_orderbook_feed.get_current_position symbol
  ;;

  let get_orderbook_position_fast ~symbol =
    Hyperliquid_orderbook_feed.get_current_position_fast symbol
  ;;

  let read_orderbook_events ~symbol ~start_pos =
    let events = Hyperliquid_orderbook_feed.read_orderbook_events symbol start_pos in
    List.map
      (fun (ob : Hyperliquid_orderbook_feed.orderbook) ->
        let map_levels levels =
          Array.map (fun (l : Hyperliquid_orderbook_feed.level) -> l.price, l.size) levels
        in
        { Types.bids = map_levels ob.bids
        ; asks = map_levels ob.asks
        ; timestamp = ob.timestamp
        })
      events
  ;;

  let iter_orderbook_events ~symbol ~start_pos f =
    Hyperliquid_orderbook_feed.iter_orderbook_events
      symbol
      start_pos
      (fun (ob : Hyperliquid_orderbook_feed.orderbook) ->
         let map_levels levels =
           Array.map
             (fun (l : Hyperliquid_orderbook_feed.level) -> l.price, l.size)
             levels
         in
         f
           { Types.bids = map_levels ob.bids
           ; asks = map_levels ob.asks
           ; timestamp = ob.timestamp
           })
  ;;

  (** Zero-allocation top-of-book iterator. Reads price/size directly from Hyperliquid
      level records without building converted arrays. *)
  let iter_top_of_book_events ~symbol ~start_pos f =
    Hyperliquid_orderbook_feed.iter_orderbook_events
      symbol
      start_pos
      (fun (ob : Hyperliquid_orderbook_feed.orderbook) ->
         if Array.length ob.bids > 0 && Array.length ob.asks > 0
         then (
           let bid = ob.bids.(0) in
           let ask = ob.asks.(0) in
           f bid.price bid.size ask.price ask.size))
  ;;

  (** Instrument metadata and fee accessors *)

  let get_price_increment ~symbol =
    Hyperliquid_instruments_feed.get_price_increment symbol
  ;;

  let get_qty_increment ~symbol = Hyperliquid_instruments_feed.get_qty_increment symbol
  let get_qty_min ~symbol = Hyperliquid_instruments_feed.get_qty_min symbol

  let round_price ~symbol ~price =
    Hyperliquid_instruments_feed.round_price_to_tick_for_symbol symbol price
  ;;

  let get_fees ~symbol =
    match Hashtbl.find_opt fee_cache symbol with
    | Some (pm, pt, _sm, _st) -> Some pm, Some pt
    | None -> None, None
  ;;

  let get_spot_fees ~symbol =
    match Hashtbl.find_opt fee_cache symbol with
    | Some (_pm, _pt, sm, st) -> Some sm, Some st
    | None -> None, None
  ;;

  let initialize_fees symbols =
    if symbols = []
    then Lwt.return_unit
    else
      Hyperliquid_get_fee.get_fee_info ~testnet:(Atomic.get is_testnet) ()
      >|= function
      | Some info ->
        let maker = Option.value info.maker_fee ~default:0.0 in
        let taker = Option.value info.taker_fee ~default:0.0 in
        let spot_m = Option.value info.spot_maker_fee ~default:0.0 in
        let spot_t = Option.value info.spot_taker_fee ~default:0.0 in
        List.iter
          (fun symbol -> Hashtbl.replace fee_cache symbol (maker, taker, spot_m, spot_t))
          symbols
      | None -> ()
  ;;

  (** Transfer USDC between spot and perp sub-accounts *)
  let transfer_usd ~amount ~to_perp =
    Hyperliquid_actions.usd_class_transfer
      ~amount
      ~to_perp
      ~testnet:(Atomic.get is_testnet)
  ;;

  (* Uniform decode entry: l2Book frames go through the zero-alloc TOB parser. Control
     channels (pong/responses/subscriber fan-out) touch Lwt and stay in the WS layer. *)
  let decode_frame content =
    if String.starts_with ~prefix:"{\"channel\":\"l2Book\"," content
    then Hyperliquid_orderbook_feed.process_raw_market_data content
  ;;
end

(** WebSocket-based initialization routines. These helpers coordinate between the WS
    transport layer and the domain-specific feed modules during startup. *)
let wait_for_ws_connected () =
  (* Delegates to the Lwt_mvar-based wait in hyperliquid_ws. No polling. *)
  Hyperliquid_ws.wait_for_connected ()
;;

(** Populate the instrument cache from REST when the WS payload left it empty, then signal
    readiness. Without this an empty/malformed WS meta frame marked the feed ready and the
    domains placed orders the signer could not resolve ("Unknown symbol"). *)
let ensure_instruments_loaded () =
  let section = "hyperliquid_startup" in
  if Hyperliquid_instruments_feed.has_instruments ()
  then Lwt.return_unit
  else (
    Logging.warn_f
      ~section
      "Hyperliquid instrument cache empty after WS meta; falling back to REST meta";
    Lwt.catch
      (fun () ->
        Hyperliquid_instruments_feed.fetch_meta_from_rest
          ~testnet:(Atomic.get Hyperliquid_impl.is_testnet)
          ()
        >>= fun () ->
        Hyperliquid_instruments_feed.notify_ready ();
        Lwt.return_unit)
      (fun exn ->
        Logging.error_f
          ~section
          "Hyperliquid REST instrument fallback failed: %s"
          (Printexc.to_string exn);
        Hyperliquid_instruments_feed.notify_ready ();
        Lwt.return_unit))
;;

let initialize_instruments_ws () =
  let section = "hyperliquid_startup" in
  Lwt.catch
    (fun () ->
      wait_for_ws_connected ()
      >>= fun () ->
      let open Yojson.Safe.Util in
      let req_id_perp = Hyperliquid_actions.next_ws_req_id () in
      let ws_frame_perp =
        `Assoc
          [ "method", `String "post"
          ; "id", `Int req_id_perp
          ; ( "request"
            , `Assoc
                [ "type", `String "info"; "payload", `Assoc [ "type", `String "meta" ] ] )
          ]
      in
      let extract_ws_payload resp =
        try
          let p = resp |> member "data" |> member "response" |> member "payload" in
          let d = member "data" p in
          if d = `Null then p else d
        with
        | _ -> resp
      in
      Hyperliquid_ws.send_request ~json:ws_frame_perp ~req_id:req_id_perp ~timeout_ms:5000
      >>= fun resp_perp ->
      let payload_perp = extract_ws_payload resp_perp in
      let req_id_spot = Hyperliquid_actions.next_ws_req_id () in
      let ws_frame_spot =
        `Assoc
          [ "method", `String "post"
          ; "id", `Int req_id_spot
          ; ( "request"
            , `Assoc
                [ "type", `String "info"
                ; "payload", `Assoc [ "type", `String "spotMeta" ]
                ] )
          ]
      in
      Hyperliquid_ws.send_request ~json:ws_frame_spot ~req_id:req_id_spot ~timeout_ms:5000
      >>= fun resp_spot ->
      let payload_spot = extract_ws_payload resp_spot in
      Hyperliquid_instruments_feed.process_meta_response payload_perp payload_spot
      >>= fun () ->
      ensure_instruments_loaded ())
    (fun exn ->
      Logging.error_f
        ~section
        "Failed to fetch instruments via WS: %s"
        (Printexc.to_string exn);
      ensure_instruments_loaded ())
;;

let parse_json_float json =
  match json with
  | `String s ->
    (try float_of_string s with
     | _ -> 0.0)
  | `Float f -> f
  | `Int i -> float_of_int i
  | _ -> 0.0
;;

(** Staking figures reported at INFO by the last successful poll, as
    [(delegated, undelegated, pendingWithdrawal)]. The poll runs every 10s and the figures
    rarely move, so [None] (nothing logged yet) and a change log at INFO and an unchanged
    repeat stays at DEBUG. Without this the steady state is one INFO line per poll. *)
let last_staking_reported : (float * float * float) option Atomic.t = Atomic.make None

let same_staking_figures (a, b, c) (a', b', c') =
  Float.equal a a' && Float.equal b b' && Float.equal c c'
;;

let poll_staking_balance () =
  let section = "hyperliquid_startup" in
  let testnet = Atomic.get Hyperliquid_impl.is_testnet in
  let base_url =
    if testnet
    then "https://api.hyperliquid-testnet.xyz"
    else "https://api.hyperliquid.xyz"
  in
  let wallet =
    match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim with
    | Some w -> w
    | None -> ""
  in
  if wallet = ""
  then Lwt.return_unit
  else (
    let fetch_once () =
      Lwt.catch
        (fun () ->
          let url = Uri.of_string (base_url ^ "/info") in
          let req_body =
            `Assoc [ "type", `String "delegatorSummary"; "user", `String wallet ]
          in
          let body_str = Yojson.Safe.to_string req_body in
          let body = Cohttp_lwt.Body.of_string body_str in
          let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
          Lwt_unix.with_timeout 10.0 (fun () ->
            Cohttp_lwt_unix.Client.post ~headers ~body url)
          >>= fun (resp, resp_body) ->
          Cohttp_lwt.Body.to_string resp_body
          >>= fun body_str ->
          let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
          if status >= 200 && status < 300
          then Lwt.return (Ok body_str)
          else
            Lwt.return
              (Error
                 (Printf.sprintf
                    "Hyperliquid staking request HTTP %d: %s"
                    status
                    body_str)))
        (fun exn -> Lwt.return (Error (Printexc.to_string exn)))
    in
    Error_handling.retry_with_backoff
      ~section
      ~config:Error_handling.default_retry_config
      ~f:fetch_once
      ()
    >>= function
    | Ok body_str ->
      Logging.debug_f ~section "Raw delegator summary response: %s" body_str;
      (try
         let json = Yojson.Safe.from_string body_str in
         let open Yojson.Safe.Util in
         (* delegatorSummary:
            [{delegated, undelegated, totalPendingWithdrawal, nPendingWithdrawals}]
            (info-endpoint docs). *)
         let delegated = parse_json_float (member "delegated" json) in
         let undelegated = parse_json_float (member "undelegated" json) in
         let pending_withdrawal =
           parse_json_float (member "totalPendingWithdrawal" json)
         in
         let store = Hyperliquid_balances.get_balance_store "HYPE" in
         Hyperliquid_balances.BalanceStore.update_wallet
           store
           ~available:delegated
           ~total:delegated
           "staking"
           "delegated";
         Hyperliquid_balances.BalanceStore.update_wallet
           store
           ~available:undelegated
           ~total:undelegated
           "staking"
           "undelegated";
         Hyperliquid_balances.BalanceStore.update_wallet
           store
           ~available:pending_withdrawal
           ~total:pending_withdrawal
           "staking"
           "pending_withdrawal";
         let observed = delegated, undelegated, pending_withdrawal in
         let first_or_changed =
           match Atomic.get last_staking_reported with
           | Some prev -> not (same_staking_figures prev observed)
           | None -> true
         in
         Atomic.set last_staking_reported (Some observed);
         let total = Hyperliquid_balances.get_total_balance "HYPE" in
         let staked = Hyperliquid_balances.get_staked_balance "HYPE" in
         if first_or_changed
         then
           Logging.info_f
             ~section
             "Hyperliquid HYPE staking balance: delegated=%.6f, undelegated=%.6f, \
              pendingWithdrawal=%.6f (total=%.6f, staked=%.6f)"
             delegated
             undelegated
             pending_withdrawal
             total
             staked
         else
           Logging.debug_f
             ~section
             "Hyperliquid HYPE staking balance unchanged: delegated=%.6f, staked=%.6f \
              (total=%.6f)"
             delegated
             staked
             total;
         Lwt.return_unit
       with
       | exn ->
         Logging.error_f
           ~section
           "Failed to parse Hyperliquid staking response: %s"
           (Printexc.to_string exn);
         Lwt.return_unit)
    | Error err ->
      Logging.error_f ~section "Failed to fetch staking balances via REST: %s" err;
      Lwt.return_unit)
;;

let start_staking_balance_poller () =
  let rec loop () =
    Lwt.catch
      (fun () -> poll_staking_balance ())
      (fun exn ->
        Logging.error_f
          ~section:"hyperliquid_startup"
          "Staking poller loop exception: %s"
          (Printexc.to_string exn);
        Lwt.return_unit)
    >>= fun () -> Lwt_unix.sleep 10.0 >>= fun () -> loop ()
  in
  Lwt.async loop
;;

(** Reads the account abstraction mode once. In [unifiedAccount] / [portfolioMargin] the
    spot USDC balance is the perp collateral; the per-DEX clearinghouse summary is not
    meaningful. A failure leaves the mode [Unknown], which falls back to the per-DEX
    summary (correct for standard accounts). *)
let fetch_account_mode_ws () =
  let section = "hyperliquid_startup" in
  let wallet =
    match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim with
    | Some w -> w
    | None -> ""
  in
  if wallet = ""
  then Lwt.return_unit
  else (
    let req_id = Hyperliquid_actions.next_ws_req_id () in
    let ws_frame =
      `Assoc
        [ "method", `String "post"
        ; "id", `Int req_id
        ; ( "request"
          , `Assoc
              [ "type", `String "info"
              ; ( "payload"
                , `Assoc [ "type", `String "userAbstraction"; "user", `String wallet ] )
              ] )
        ]
    in
    Lwt.catch
      (fun () ->
        wait_for_ws_connected ()
        >>= fun () ->
        Hyperliquid_ws.send_request ~json:ws_frame ~req_id ~timeout_ms:5000
        >>= fun resp_frame ->
        let open Yojson.Safe.Util in
        let payload =
          try resp_frame |> member "data" |> member "response" |> member "payload" with
          | _ -> `Null
        in
        let raw =
          match payload with
          | `String s -> Some s
          | `Assoc _ ->
            (try payload |> member "data" |> to_string_option with
             | _ -> None)
          | _ -> None
        in
        (match raw with
         | Some s ->
           let mode = Hyperliquid_account_mode.of_string s in
           Hyperliquid_account_mode.set mode;
           if mode = Hyperliquid_account_mode.Unknown
           then
             Logging.warn_f
               ~section
               "Unrecognised account abstraction %S; using per-DEX margin"
               s
         | None ->
           Logging.warn_f
             ~section
             "Could not parse account abstraction response: %s"
             (Yojson.Safe.to_string resp_frame));
        Lwt.return_unit)
      (fun exn ->
        Logging.error_f
          ~section
          "Failed to fetch account abstraction via WS: %s"
          (Printexc.to_string exn);
        Lwt.return_unit))
;;

let fetch_spot_balances_ws () =
  let section = "hyperliquid_startup" in
  let wallet =
    match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim with
    | Some w -> w
    | None -> ""
  in
  if wallet = ""
  then Lwt.return_unit
  else (
    let req_id = Hyperliquid_actions.next_ws_req_id () in
    let ws_frame =
      `Assoc
        [ "method", `String "post"
        ; "id", `Int req_id
        ; ( "request"
          , `Assoc
              [ "type", `String "info"
              ; ( "payload"
                , `Assoc
                    [ "type", `String "spotClearinghouseState"; "user", `String wallet ] )
              ] )
        ]
    in
    Lwt.catch
      (fun () ->
        wait_for_ws_connected ()
        >>= fun () ->
        Hyperliquid_ws.send_request ~json:ws_frame ~req_id ~timeout_ms:5000
        >>= fun resp_frame ->
        let open Yojson.Safe.Util in
        let response_node =
          try resp_frame |> member "data" |> member "response" with
          | _ -> `Null
        in
        let payload =
          try response_node |> member "payload" with
          | _ -> `Null
        in
        let balances_node =
          try payload |> member "data" |> member "balances" with
          | _ -> `Null
        in
        if balances_node = `Null
        then (
          Logging.warn_f
            ~section
            "Balances node is null! Raw WS response: %s"
            (Yojson.Safe.to_string resp_frame);
          Hyperliquid_balances.notify_ready ();
          Lwt.return_unit)
        else (
          let balances = balances_node |> to_list in
          let entries = ref [] in
          let parse_failed = ref false in
          List.iter
            (fun item ->
              try
                let raw_coin = member "coin" item |> to_string in
                let coin = Hyperliquid_balances.canonicalize_coin raw_coin in
                let total = parse_json_float (member "total" item) in
                (* Same available/total semantics as the spotState subscription: the grid
                   can only spend total - hold. *)
                let hold = parse_json_float (member "hold" item) in
                let available = max 0.0 (total -. hold) in
                entries := (coin, available, total) :: !entries;
                Logging.debug_f
                  ~section
                  "Initial Spot %s balance: %.6f avail / %.6f total"
                  coin
                  available
                  total
              with
              | exn ->
                parse_failed := true;
                Logging.warn_f
                  ~section
                  "Failed to parse spot balance entry: %s"
                  (Printexc.to_string exn))
            balances;
          (* [spotClearinghouseState] is the authoritative full set. Apply it as such so
             tokens that have gone to zero are cleared from the store; a partial parse
             falls back to a merge rather than risking a false zero. *)
          Hyperliquid_balances.apply_spot_snapshot
            ~full:(not !parse_failed)
            (List.rev !entries);
          Hyperliquid_balances.notify_spot_update ();
          Hyperliquid_balances.notify_ready ();
          Lwt.return_unit))
      (fun exn ->
        Logging.error_f
          ~section
          "Failed to fetch spot balances via WS: %s"
          (Printexc.to_string exn);
        Hyperliquid_balances.notify_ready ();
        Lwt.return_unit))
;;

let fetch_open_orders_ws () =
  let section = "hyperliquid_startup" in
  let wallet =
    match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim with
    | Some w -> w
    | None -> ""
  in
  if wallet = ""
  then Lwt.return_unit
  else (
    let req_id = Hyperliquid_actions.next_ws_req_id () in
    let ws_frame =
      `Assoc
        [ "method", `String "post"
        ; "id", `Int req_id
        ; ( "request"
          , `Assoc
              [ "type", `String "info"
              ; "payload", `Assoc [ "type", `String "openOrders"; "user", `String wallet ]
              ] )
        ]
    in
    Lwt.catch
      (fun () ->
        wait_for_ws_connected ()
        >>= fun () ->
        Hyperliquid_ws.send_request ~json:ws_frame ~req_id ~timeout_ms:5000
        >>= fun resp_frame ->
        Logging.debug_f
          ~section
          "Raw open orders response: %s"
          (Yojson.Safe.to_string resp_frame);
        let open Yojson.Safe.Util in
        (* Extract payload from data.response.payload.data, falling back to
           data.response.payload *)
        let response_node =
          try resp_frame |> member "data" |> member "response" with
          | _ -> `Null
        in
        let payload =
          try response_node |> member "payload" with
          | _ -> `Null
        in
        let data_node =
          try payload |> member "data" with
          | _ -> `Null
        in
        let orders_json = if data_node <> `Null then data_node else payload in
        Hyperliquid_executions_feed.inject_open_orders orders_json;
        (* Signal that the open-order snapshot has been fully injected. Domain workers
           block on this event instead of a fixed wall-clock delay. *)
        Hyperliquid_executions_feed.set_startup_snapshot_done ();
        Lwt.return_unit)
      (fun exn ->
        Logging.error_f
          ~section
          "Failed to fetch open orders via WS: %s"
          (Printexc.to_string exn);
        Lwt.return_unit))
;;

(** On-demand user-state resync, single-flight: balances first, then open orders. Failures
    are logged and the next trigger retries. *)
let resync_in_flight = Atomic.make false

let resync_user_state () =
  if not (Atomic.exchange resync_in_flight true)
  then
    Lwt.async (fun () ->
      Lwt.finalize
        (fun () ->
          Lwt.catch
            (fun () ->
              fetch_account_mode_ws ()
              >>= fun () ->
              fetch_spot_balances_ws () >>= fun () -> fetch_open_orders_ws ())
            (fun exn ->
              Logging.warn_f
                ~section:"hyperliquid_startup"
                "User-state resync failed: %s"
                (Printexc.to_string exn);
              Lwt.return_unit))
        (fun () ->
          Atomic.set resync_in_flight false;
          Lwt.return_unit))
  else (
    (* The in-flight fetch may predate this loss; keep the latch so the next pass runs
       again. *)
    Hyperliquid_ws.note_user_resync_needed ();
    Logging.debug_f
      ~section:"hyperliquid_startup"
      "User-state resync already in flight; coalescing request")
;;

let () = Hyperliquid_impl.resync_hook := resync_user_state

(** Consume the user-stream loss flag and resync. The dedicated stream should never fill;
    this is the fallback if it does, or if the venue never sent a frame. *)
let () =
  let rec watch () =
    if Hyperliquid_ws.get_user_resync_needed ()
    then (
      Logging.warn_f
        ~section:"hyperliquid_startup"
        "User-channel frame loss detected; requesting a full balance/open-orders resync";
      resync_user_state ());
    Lwt_unix.sleep 0.25 >>= watch
  in
  Lwt.async watch
;;

(* Register Hyperliquid_impl with the exchange registry at module load time *)
let () = Exchange.Registry.register (module Hyperliquid_impl)

(* Register the uniform venue decoder for the parse-domain offload route. *)

let () =
  Concurrency.Parse_worker.register_venue_decoder
    ~venue:"hyperliquid"
    Hyperliquid_impl.decode_frame
;;

(* Register the oracle data-venue adapter (historical candles, fees, spot balances,
   instruments for the capital oracle) at load time. *)
let () = Exchange.Oracle.Registry.register (module Hyperliquid_oracle)

(** Register the optional engine-facing capabilities when this module loads. The engine
    references [Hyperliquid.Module], so this runs in production; the aliases in
    [hyperliquid.ml] are convenience paths, not the registration trigger.

    Also force-link the two user-stream processors. They publish funding and position
    snapshots into the market-context capability; without a value reference from this
    module the OCaml linker can drop them and the overlay sees no context. *)
let () =
  let module Registry = Dio_exchange.Exchange_intf.Registry in
  Registry.register_market_context (module Hyperliquid_market_context.Impl);
  Registry.register_capital_reservation (module Hyperliquid_capital_reservation.Impl);
  ignore Hyperliquid_asset_ctx._processor_task;
  ignore Hyperliquid_positions._processor_task;
  Logging.info
    ~section:"hyperliquid_module"
    "registered market-context/capital-reservation capabilities and started \
     asset-context/position feeds"
;;
