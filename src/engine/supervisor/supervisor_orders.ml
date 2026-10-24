[@@@ocaml.text
  " Central order processing loop. Drains pending orders from all strategy ring buffers\n\
  \    (grid, market maker, hedger) and dispatches them to [Order_executor] via \
   [Lwt.async].\n\
  \    Blocks on [OrderSignal] when idle.\n\n\
  \    Uses a unified dispatch pipeline with strategy-specific callbacks to avoid \
   duplicated\n\
  \    order handling. "]

open Lwt.Infix
open Dio_strategies.Strategy_common
open Supervisor_types
open Supervisor_connection

let section = "order_processor"

type strategy_callbacks =
  { on_place_ok : strategy_order -> string -> unit
  ; on_place_fail : strategy_order -> string -> unit
  ; on_amend_ok : strategy_order -> string -> string -> unit
  ; on_amend_skipped : strategy_order -> string -> unit
  ; on_amend_fail : strategy_order -> string -> string -> unit
  ; on_cancel_ok : strategy_order -> string -> unit
  ; on_cancel_fail : strategy_order -> string -> unit
  }
[@@ocaml.doc
  " Per-strategy callbacks invoked during order lifecycle events. Each strategy supplies\n\
  \    its own implementation to preserve unique behavior. "]

let side_str order =
  match order.side with
  | Buy -> "buy"
  | Sell -> "sell"
;;

let price_str order =
  match order.price with
  | Some p -> Printf.sprintf "%.2f" p
  | None -> "market"
;;

let contains_fragment s fragment =
  let sl = String.length s
  and fl = String.length fragment in
  let rec loop i = i + fl <= sl && (String.sub s i fl = fragment || loop (i + 1)) in
  loop 0
;;

let is_inventory_rejection err =
  let lower = String.lowercase_ascii err in
  contains_fragment lower "not allowed to short"
  || contains_fragment lower "insufficient funds"
  || contains_fragment lower "insufficient balance"
  || contains_fragment lower "insufficient spot balance"
  || contains_fragment lower "insufficient qty"
  || contains_fragment lower "not enough asset balance"
  || contains_fragment lower "qty available"
  || contains_fragment lower "position not found"
  || contains_fragment lower "asset not held"
[@@ocaml.doc
  " True when a placement error means the strategy's inventory view diverged from the\n\
  \    venue: a shorting/capacity rejection proves the venue held the funds (a resting \
   order\n\
  \    the local view missed), or the position was already consumed (an unobserved fill).\n\
  \    Retrying against a stale view re-sells inventory the account no longer has. "]
;;

let reconciliation_in_flight = Atomic.make false

let reconciliation_pending = Atomic.make false
[@@ocaml.doc
  " Set when a rejection arrives during an in-flight reconcile; the running reconcile then\n\
  \    re-runs once after finishing. Dropping it silently is unsafe because its desync may\n\
  \    predate the running reconcile's balance snapshot. "]
;;

let refresh_inventory_state_on_rejection (order : strategy_order) err =
  if String.equal order.exchange "alpaca" && is_inventory_rejection err
  then (
    let rec run ~with_open_orders =
      match Atomic.compare_and_set reconciliation_in_flight false true with
      | false ->
        Atomic.set reconciliation_pending true;
        Logging.warn_f
          ~section
          "Inventory rejection for %s arrived during an in-flight reconcile - queued \
           re-run (%s)"
          order.symbol
          err
      | true ->
        Lwt.async (fun () ->
          Lwt.catch
            (fun () ->
              Logging.warn_f
                ~section
                "Reconciling venue state after inventory-class rejection: %s %s (%s)"
                (side_str order)
                order.symbol
                err;
              let jobs =
                [ Alpaca.Balances.update_balances () ]
                @
                if with_open_orders
                then [ Alpaca.Executions.bootstrap_open_orders () ]
                else []
              in
              Lwt.join jobs)
            (fun exn ->
              Logging.warn_f
                ~section
                "Post-rejection reconciliation failed for %s: %s"
                order.symbol
                (Printexc.to_string exn);
              Lwt.return_unit)
          >>= fun () ->
          Atomic.set reconciliation_in_flight false;
          if Atomic.get reconciliation_pending
          then (
            Atomic.set reconciliation_pending false;
            run ~with_open_orders:true;
            Lwt.return_unit)
          else Lwt.return_unit)
    in
    run ~with_open_orders:(order.side = Sell))
  else ()
[@@ocaml.doc
  " Event-driven reconciliation after an inventory-class placement rejection: re-bootstrap\n\
  \    the venue's open orders and balances; their completion wakeups re-run the symbol's\n\
  \    strategy against fresh state.\n\n\
  \    Alpaca only: its balances are gross (open-order holds are not netted), so a missed\n\
  \    fill leaves the ladder believing it still holds sellable inventory. Other venues\n\
  \    report tradeable (hold-netted) balances. "]
;;

(** Carry recovery is event-driven. A rejected amend can leave the venue order gone
    (Hyperliquid replaces by cancel+create) while the strategy still sees it in the
    open-order cache. Refresh the venue snapshot and wake the owning domain so the overlay
    re-places the entry on this event instead of waiting for the next unrelated tick. *)
let nudge_carry_recovery (order : strategy_order) =
  (match Dio_engine.Order_executor.get_exchange order.exchange with
   | Ok (module Ex : Dio_exchange.Exchange_intf.S) ->
     (try Ex.resync_market_state ~symbol:order.symbol () with
      | _ -> ())
   | Error _ -> ());
  Concurrency.Exchange_wakeup.signal ~symbol:order.symbol;
  if not (String.contains order.symbol '/')
  then Concurrency.Exchange_wakeup.signal ~symbol:(order.symbol ^ "/USDC")
;;

let grid_callbacks : strategy_callbacks =
  { on_place_ok =
      (fun order order_id ->
        Logging.info_f
          ~section
          "\226\156\147 Order placed successfully: %s [%s] %s %.8f @ %s (Order ID: %s)"
          (side_str order)
          (Dio_strategies.Strategy_common.string_of_market order.market)
          order.symbol
          order.qty
          (price_str order)
          order_id;
        (match order.price with
         | Some price ->
           Dio_strategies.Strategy_api.Strategy.enqueue_event
             order.symbol
             (Ack { now = Unix.gettimeofday (); order_id; side = order.side; price })
         | None -> ());
        (* Only the 60s TTL sweep freed this before, so every place for the rung was
           dropped as a duplicate until then. *)
        ignore
          (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
             order.duplicate_key))
  ; on_place_fail =
      (fun order err ->
        Logging.error_f
          ~section
          "\226\156\151 Order placement failed: %s [%s] %s %.8f @ %s - %s"
          (side_str order)
          (Dio_strategies.Strategy_common.string_of_market order.market)
          order.symbol
          order.qty
          (price_str order)
          err;
        refresh_inventory_state_on_rejection order err;
        Dio_strategies.Strategy_api.Strategy.enqueue_event
          order.symbol
          (Failed { now = Unix.gettimeofday (); side = order.side; reason = err });
        (match order.price with
         | Some price ->
           Dio_strategies.Strategy_api.Strategy.enqueue_event
             order.symbol
             (Rejected { now = Unix.gettimeofday (); side = order.side; price })
         | None -> ());
        (* Free the latch even if the domain thread that consumes [Failed] is stalled. *)
        ignore
          (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
             order.duplicate_key))
  ; on_amend_ok =
      (fun order target_order_id new_order_id ->
        Logging.info_f
          ~section
          "\226\156\147 Order amended successfully: %s [%s] %s %.8f @ %s New Order ID: %s"
          (side_str order)
          (Dio_strategies.Strategy_common.string_of_market order.market)
          order.symbol
          order.qty
          (price_str order)
          new_order_id;
        match order.price with
        | Some price ->
          Dio_strategies.Strategy_api.Strategy.enqueue_event
            order.symbol
            (Amended
               { now = Unix.gettimeofday ()
               ; old_id = target_order_id
               ; new_id = new_order_id
               ; side = order.side
               ; price
               })
        | None ->
          Logging.warn_f
            ~section
            "Amendment acknowledged but no price available for strategy update: %s"
            new_order_id)
  ; on_amend_skipped =
      (fun order target_order_id ->
        match order.price with
        | Some price ->
          Dio_strategies.Strategy_api.Strategy.enqueue_event
            order.symbol
            (Amendment_skipped
               { now = Unix.gettimeofday ()
               ; order_id = target_order_id
               ; side = order.side
               ; price
               })
        | None ->
          Logging.warn_f
            ~section
            "Amendment skipped but no price available for strategy update")
  ; on_amend_fail =
      (fun order target_order_id err ->
        Logging.error_f
          ~section
          "\226\156\151 Order amendment failed: %s %s %.8f @ %s - %s"
          (side_str order)
          order.symbol
          order.qty
          (price_str order)
          err;
        Dio_strategies.Strategy_api.Strategy.enqueue_event
          order.symbol
          (Amendment_failed
             { now = Unix.gettimeofday ()
             ; order_id = target_order_id
             ; side = order.side
             ; reason = err
             }))
  ; on_cancel_ok =
      (fun order target_order_id ->
        (* The dispatch already logged the venue result; this is only the state-cleanup
           acknowledgement. *)
        Logging.debug_f ~section "Cancel cleanup enqueued for %s" target_order_id;
        Dio_strategies.Strategy_api.Strategy.enqueue_event
          order.symbol
          (Cancel_cleanup { order_id = target_order_id }))
  ; on_cancel_fail =
      (fun order target_order_id ->
        Dio_strategies.Strategy_api.Strategy.enqueue_event
          order.symbol
          (Cancel_cleanup { order_id = target_order_id }))
  }
;;

(** Rebuild the full per-instrument in-flight key a carry amend belongs to. The amend
    request carries the logical kind (["carry:entry"], ["carry:recycle:close|ro"]), while
    the registry keys tracked orders by [kind|exchange|instrument(|ro)]. Without this the
    new id would not suppress a duplicate while the venue's open-orders scan catches up. *)
let carry_amend_inflight_key (order : strategy_order) =
  let kind = order.duplicate_key in
  let root =
    let n = String.length kind in
    if n > 3 && String.equal (String.sub kind (n - 3) 3) "|ro"
    then String.sub kind 0 (n - 3)
    else kind
  in
  root
  ^ "|"
  ^ order.exchange
  ^ "|"
  ^ order.symbol
  ^ if order.reduce_only then "|ro" else ""
;;

let hedger_callbacks : strategy_callbacks =
  { on_place_ok =
      (fun order order_id ->
        Logging.info_f
          ~section
          "\226\156\147 Hedger order placed successfully: %s [%s] %s %.8f @ %s (Order \
           ID: %s)"
          (side_str order)
          (Dio_strategies.Strategy_common.string_of_market order.market)
          order.symbol
          order.qty
          (price_str order)
          order_id;
        Dio_strategies.Strategy_common.bind_carry_order
          ~dedup_key:order.duplicate_key
          ~order_id;
        (* Release the placement key on ack, like the grid. The durable duplicate guard is
           the acked id -> kind mapping in [carry_orders] ([has_tracked_carry_order],
           bounded by the ack->feed grace), not this short-lived latch. *)
        ignore
          (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
             order.duplicate_key))
  ; on_place_fail =
      (fun order err ->
        Logging.error_f
          ~section
          "\226\156\151 Hedger order placement failed: %s [%s] %s %.8f @ %s - %s"
          (side_str order)
          (Dio_strategies.Strategy_common.string_of_market order.market)
          order.symbol
          order.qty
          (price_str order)
          err;
        Dio_strategies.Strategy_common.retire_pending_carry_order
          ~dedup_key:order.duplicate_key;
        (* A terminal failure must free the key too, otherwise a single ALO/post-only
           rejection blocks the next retry until the TTL sweep. *)
        ignore
          (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
             order.duplicate_key))
  ; on_amend_ok =
      (fun order target_id new_id ->
        if order.strategy = Dio_strategies.Strategy_common.Carry
           && not (String.equal order.duplicate_key "")
        then (
          (* A venue amend lands as a new order id. Retire the old mapping first: if the
             old id's terminal event is lost, its full-key mapping would suppress every
             later replace for this kind forever (no entry, no take-profit). *)
          Dio_strategies.Strategy_common.retire_carry_order ~order_id:target_id;
          Dio_strategies.Strategy_common.register_carry_order_id
            ~order_id:new_id
            ~market:
              (match order.market with
               | Dio_strategies.Strategy_common.Spot ->
                 Dio_strategies.Strategy_common.Carry_spot
               | Dio_strategies.Strategy_common.Perp ->
                 Dio_strategies.Strategy_common.Carry_perp)
            ~maker:order.post_only
            ~kind:order.duplicate_key
            ~key:(carry_amend_inflight_key order)))
  ; on_amend_skipped = (fun _order _target_id -> ())
  ; on_amend_fail =
      (fun order _target_id _err ->
        if order.strategy = Carry then nudge_carry_recovery order)
  ; on_cancel_ok =
      (fun _order target_order_id ->
        (* Retire the canceled carry order so a tracked-order check does not suppress the
           replacement forever, and release its placement latch. The amend path
           re-registers the new id on its ack. *)
        (match
           Dio_strategies.Strategy_common.retire_carry_order_key ~order_id:target_order_id
         with
         | Some key ->
           ignore
             (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order key)
         | None -> ());
        ignore
          (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
             ("overlay:cancel|" ^ target_order_id)))
  ; on_cancel_fail =
      (fun _order target_order_id ->
        ignore
          (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
             ("overlay:cancel|" ^ target_order_id)))
  }
;;

let callbacks_for_strategy (order : strategy_order) =
  match order.strategy with
  | Ladder -> grid_callbacks
  | MM -> grid_callbacks
  | Hedger | Carry -> hedger_callbacks
[@@ocaml.doc " Resolves the callbacks for [order]. "]
;;

let dispatch_deadline_s = 120.0
[@@ocaml.doc
  " Deadline after which a dispatched Place/Amend that has produced no terminal callback\n\
  \    is declared failed. Must exceed the worst-case legitimate REST duration including \
   the\n\
  \    venue retry budget (3 attempts with up to 30s backoff); 120s keeps the\n\
  \    zombie-completion window small while guaranteeing in-flight guards cannot wedge\n\
  \    forever - e.g. the Alpaca placement REST has no HTTP timeout, so a black-holed\n\
  \    connection produces no terminal event on its own. A REST completion after the \
   deadline\n\
  \    fires is a zombie and is suppressed, so exactly one terminal event reaches the\n\
  \    strategy; a request that landed venue-side before the deadline is adopted by the \
   next\n\
  \    open-orders scan. "]
;;

let once_only resolved f () =
  if Atomic.compare_and_set resolved false true then f () else Lwt.return_unit
[@@ocaml.doc
  " Atomic first-wins latch: runs [f] only if no terminal event has been delivered yet\n\
  \    ([resolved] unset), setting [resolved] on the way. "]
;;

let is_post_only_rejection err =
  let lower = String.lowercase_ascii err in
  contains_fragment lower "post only" || contains_fragment lower "post-only"
;;

(** Parse a leading decimal number, ignoring trailing punctuation. *)
let float_prefix s =
  let n = String.length s in
  let rec stop i =
    if i >= n
    then i
    else (
      match s.[i] with
      | '0' .. '9' | '.' -> stop (i + 1)
      | _ -> i)
  in
  let rec of_len len =
    if len <= 0
    then None
    else (
      match Float.of_string_opt (String.sub s 0 len) with
      | Some f -> Some f
      | None -> of_len (len - 1))
  in
  of_len (stop 0)
;;

(** BBO reported in a venue rejection, e.g. Hyperliquid's "...post only order would have
    immediately matched, bbo was 80.892@80.893". The rejection-time BBO is fresher than
    the local cache, so retries use it when present. *)
let rejection_bbo err =
  let marker = "bbo was " in
  let marker_len = String.length marker in
  let rec find i =
    if i + marker_len > String.length err
    then None
    else if String.equal (String.sub err i marker_len) marker
    then Some (i + marker_len)
    else find (i + 1)
  in
  match find 0 with
  | None -> None
  | Some start ->
    (match String.index_from_opt err start '@' with
     | None -> None
     | Some at ->
       let bid = float_prefix (String.sub err start (at - start)) in
       let ask = float_prefix (String.sub err (at + 1) (String.length err - at - 1)) in
       (match bid, ask with
        | Some b, Some a when b > 0.0 && a > 0.0 -> Some (b, a)
        | _ -> None))
;;

(** Maker price one spread away from [bbo], with a small tick cushion so the retry cannot
    land on the touch if the market moved again. *)
let maker_retry_price ~side ~price ~bbo =
  let bid, ask = bbo in
  let spread = Float.max (ask -. bid) (Float.max 1e-5 (price *. 1e-5)) in
  match side with
  | Sell -> ask +. spread
  | Buy -> bid -. spread
;;

(** Fresh maker price for a rejected carry post-only order.

    The strategy prices at the top of book it saw on the book event. By the time the REST
    placement lands the market can have moved through that price, so HL rejects the
    post-only order. Prefer the BBO in the rejection itself (the exact book at rejection
    time); fall back to the local cache, then quote one spread away from the new touch: a
    sell at [ask + spread] and a buy at [bid − spread] remain maker orders with a cushion
    for the next move. The retry is driven by the rejection event, not a timer. *)
let post_only_retry_price ?err (order : strategy_order) =
  match order.strategy with
  | Carry when order.post_only ->
    let bbo =
      match err with
      | Some e -> rejection_bbo e
      | None -> None
    in
    let bbo =
      match bbo with
      | Some _ as r -> r
      | None ->
        (match Dio_engine.Order_executor.get_exchange order.exchange with
         | Error _ -> None
         | Ok (module Ex) ->
           (match Ex.get_top_of_book ~symbol:order.symbol with
            | Some (bid, _, ask, _) when bid > 0.0 && ask > 0.0 -> Some (bid, ask)
            | _ -> None))
    in
    (match bbo with
     | None -> None
     | Some bbo ->
       let price = Option.value order.price ~default:0.0 in
       let px = maker_retry_price ~side:order.side ~price ~bbo in
       if px <= 0.0
       then None
       else (
         match Dio_engine.Order_executor.get_exchange order.exchange with
         | Error _ -> Some px
         | Ok (module Ex) ->
           (try Some (Ex.round_price ~symbol:order.symbol ~price:px) with
            | _ -> Some px)))
  | _ -> None
;;

let place_request_of_order (order : strategy_order) =
  { Dio_engine.Order_executor.order_type = order.order_type
  ; side = side_str order
  ; quantity = order.qty
  ; symbol = order.symbol
  ; limit_price = order.price
  ; time_in_force = Some order.time_in_force
  ; post_only = Some order.post_only
  ; margin = None
  ; reduce_only = Some order.reduce_only
  ; order_userref = order.userref
  ; cl_ord_id = None
  ; trigger_price = None
  ; trigger_price_type = None
  ; display_qty = None
  ; fee_preference = None
  ; duplicate_key = order.duplicate_key
  ; exchange = order.exchange
  }
;;

let amend_request_of_order (order : strategy_order) target_order_id =
  { Dio_engine.Order_executor.order_id = target_order_id
  ; cl_ord_id = None
  ; new_quantity = Some order.qty
  ; new_limit_price = order.price
  ; limit_price_type = None
  ; post_only = Some order.post_only
  ; reduce_only = Some order.reduce_only
  ; time_in_force = Some order.time_in_force
  ; new_trigger_price = None
  ; trigger_price_type = None
  ; new_display_qty = None
  ; deadline = None
  ; symbol = Some order.symbol
  ; exchange = order.exchange
  }
;;

let dispatch_place ~auth_token ~orders_placed ~cb (order : strategy_order) =
  Lwt.async (fun () ->
    let __ppx_lwt_0 = Lwt.pause () in
    Lwt.backtrace_bind
      "src/engine/supervisor/supervisor_orders.ml"
      345
      (fun exn ->
        try Lwt.reraise exn with
        | exn -> exn)
      __ppx_lwt_0
      (fun () ->
        let resolved = Atomic.make false in
        let rec attempt retries (o : strategy_order) =
          Dio_engine.Order_executor.place_order
            ~token:auth_token
            ~check_duplicate:false
            (place_request_of_order o)
          >>= function
          | Ok result ->
            once_only
              resolved
              (fun () ->
                Atomic.incr orders_placed;
                cb.on_place_ok o result.order_id;
                Lwt.return_unit)
              ()
          | Error err ->
            let retry =
              if retries < 3 && is_post_only_rejection err
              then post_only_retry_price ~err o
              else None
            in
            (match retry with
             | Some px when Some px <> o.price ->
               Logging.warn_f
                 ~section
                 "Carry post-only placement rejected (%s); retrying %s %s %.8f @ %.2f \
                  (attempt %d/3)"
                 err
                 (side_str o)
                 o.symbol
                 o.qty
                 px
                 (retries + 1);
               attempt (retries + 1) { o with price = Some px }
             | _ ->
               once_only
                 resolved
                 (fun () ->
                   cb.on_place_fail o err;
                   Lwt.return_unit)
                 ())
        in
        Lwt.catch
          (fun () ->
            Lwt.choose
              [ attempt 0 order
              ; (Lwt_unix.sleep dispatch_deadline_s
                 >>= fun () ->
                 once_only
                   resolved
                   (fun () ->
                     Logging.error_f
                       ~section
                       "\226\143\177 Place dispatch deadline (%.0fs) exceeded for %s %s \
                        - synthesizing terminal failure"
                       dispatch_deadline_s
                       (side_str order)
                       order.symbol;
                     cb.on_place_fail order "dispatch deadline exceeded";
                     Lwt.return_unit)
                   ())
              ])
          (fun exn ->
            let err = Printexc.to_string exn in
            Logging.error_f
              ~section
              "\226\156\151 Exception placing order %s %s: %s"
              (side_str order)
              order.symbol
              err;
            once_only
              resolved
              (fun () ->
                cb.on_place_fail order err;
                Lwt.return_unit)
              ())))
[@@ocaml.doc " Dispatches a Place order asynchronously via Order_executor. "]
;;

let dispatch_amend ~auth_token ~orders_placed ~cb (order : strategy_order) target_order_id
  =
  Lwt.async (fun () ->
    let __ppx_lwt_0 = Lwt.pause () in
    Lwt.backtrace_bind
      "src/engine/supervisor/supervisor_orders.ml"
      421
      (fun exn ->
        try Lwt.reraise exn with
        | exn -> exn)
      __ppx_lwt_0
      (fun () ->
        let resolved = Atomic.make false in
        let rec attempt retries (o : strategy_order) =
          Dio_engine.Order_executor.amend_order
            ~token:auth_token
            (amend_request_of_order o target_order_id)
          >>= function
          | Ok result ->
            if result.Dio_exchange.Exchange_intf.Types.amend_id = Some "skipped_no_change"
            then
              once_only
                resolved
                (fun () ->
                  cb.on_amend_skipped o target_order_id;
                  Lwt.return_unit)
                ()
            else
              once_only
                resolved
                (fun () ->
                  Atomic.incr orders_placed;
                  let amend_id_str =
                    match result.Dio_exchange.Exchange_intf.Types.amend_id with
                    | Some id -> id
                    | None -> "none"
                  in
                  Logging.debug_f
                    ~section
                    "\226\156\147 Order amended (Amend ID: %s)"
                    amend_id_str;
                  cb.on_amend_ok
                    o
                    target_order_id
                    result.Dio_exchange.Exchange_intf.Types.new_order_id;
                  Lwt.return_unit)
                ()
          | Error err ->
            let retry =
              if retries < 3 && is_post_only_rejection err
              then post_only_retry_price ~err o
              else None
            in
            (match retry with
             | Some px when Some px <> o.price ->
               Logging.warn_f
                 ~section
                 "Carry post-only amend rejected (%s); retrying %s %s %.8f @ %.2f \
                  (attempt %d/3)"
                 err
                 (side_str o)
                 o.symbol
                 o.qty
                 px
                 (retries + 1);
               attempt (retries + 1) { o with price = Some px }
             | _ ->
               once_only
                 resolved
                 (fun () ->
                   cb.on_amend_fail o target_order_id err;
                   Lwt.return_unit)
                 ())
        in
        Lwt.catch
          (fun () ->
            Lwt.choose
              [ attempt 0 order
              ; (Lwt_unix.sleep dispatch_deadline_s
                 >>= fun () ->
                 once_only
                   resolved
                   (fun () ->
                     Logging.error_f
                       ~section
                       "\226\143\177 Amend dispatch deadline (%.0fs) exceeded for %s %s \
                        (%s) - synthesizing terminal failure"
                       dispatch_deadline_s
                       (side_str order)
                       order.symbol
                       target_order_id;
                     cb.on_amend_fail order target_order_id "dispatch deadline exceeded";
                     Lwt.return_unit)
                   ())
              ])
          (fun exn ->
            let err = Printexc.to_string exn in
            Logging.error_f
              ~section
              "\226\156\151 Exception amending order %s %s: %s"
              (side_str order)
              order.symbol
              err;
            once_only
              resolved
              (fun () ->
                cb.on_amend_fail order target_order_id err;
                Lwt.return_unit)
              ())))
[@@ocaml.doc " Dispatches an Amend order asynchronously via Order_executor. "]
;;

let dispatch_cancel
  ~auth_token
  ~orders_placed
  ~cb
  (order : strategy_order)
  target_order_id
  =
  Lwt.async (fun () ->
    let __ppx_lwt_0 = Lwt.pause () in
    Lwt.backtrace_bind
      "src/engine/supervisor/supervisor_orders.ml"
      507
      (fun exn ->
        try Lwt.reraise exn with
        | exn -> exn)
      __ppx_lwt_0
      (fun () ->
        Lwt.catch
          (fun () ->
            let request : Dio_engine.Order_executor.cancel_request =
              { exchange = order.exchange
              ; order_ids = Some [ target_order_id ]
              ; cl_ord_ids = None
              ; order_userrefs = None
              ; symbol = Some order.symbol
              }
            in
            Dio_engine.Order_executor.cancel_orders ~token:auth_token request
            >>= function
            | Ok results ->
              let count = List.length results in
              Atomic.set orders_placed (Atomic.get orders_placed + count);
              (* An empty result means the venue/local cache no longer held the id. That
                 is a successful reconciliation for cleanup purposes, but it is not a
                 venue-confirmed cancellation - say so instead of claiming "Cancelled 0
                 order(s) successfully". *)
              if count = 0
              then
                Logging.info_f
                  ~section
                  "\226\156\147 Cancel reconciled for %s (order already absent)"
                  target_order_id
              else
                Logging.info_f
                  ~section
                  "\226\156\147 Cancelled %d order(s) successfully: %s"
                  count
                  target_order_id;
              cb.on_cancel_ok order target_order_id;
              Lwt.return_unit
            | Error err ->
              Logging.error_f
                ~section
                "\226\156\151 Order cancellation failed: %s - %s"
                target_order_id
                err;
              cb.on_cancel_fail order target_order_id;
              Lwt.return_unit)
          (fun exn ->
            Logging.error_f
              ~section
              "\226\156\151 Exception cancelling order %s: %s"
              target_order_id
              (Printexc.to_string exn);
            cb.on_cancel_fail order target_order_id;
            Lwt.return_unit)))
[@@ocaml.doc " Dispatches a Cancel order asynchronously via Order_executor. "]
;;

let process_single_order
  ~orders_placed
  ~order_mutex
  ~is_connected
  (order : strategy_order)
  =
  if Atomic.get shutdown_requested then () else Mutex.lock order_mutex;
  try
    let auth_token =
      match Token_store.get () with
      | Some token -> token
      | None ->
        Logging.warn ~section "No auth token available for order operations";
        raise (Failure "No auth token")
    in
    let cb = callbacks_for_strategy order in
    let process_fn () =
      match order.operation with
      | Place -> dispatch_place ~auth_token ~orders_placed ~cb order
      | Amend ->
        (match order.order_id with
         | Some target_order_id ->
           dispatch_amend ~auth_token ~orders_placed ~cb order target_order_id
         | None ->
           Logging.error_f
             ~section
             "Amendment request missing target order ID for %s %s - failing fast"
             (side_str order)
             order.symbol;
           cb.on_amend_fail order "<missing order id>" "amend without target order id")
      | Cancel ->
        (match order.order_id with
         | Some target_order_id ->
           dispatch_cancel ~auth_token ~orders_placed ~cb order target_order_id
         | None ->
           Logging.error_f
             ~section
             "Cancel request missing target order ID for %s"
             order.symbol)
    in
    let reject_fn err =
      match order.operation with
      | Place -> cb.on_place_fail order err
      | Amend ->
        (match order.order_id with
         | Some target_order_id -> cb.on_amend_fail order target_order_id err
         | None -> ())
      | Cancel ->
        (match order.order_id with
         | Some target_order_id -> cb.on_cancel_fail order target_order_id
         | None -> ())
    in
    let connected = is_connected order in
    if connected
    then process_fn ()
    else (
      Logging.warn_f
        ~section
        "Exchange %s not connected, dropping order %s %s"
        order.exchange
        (side_str order)
        order.symbol;
      reject_fn "Exchange not connected");
    Mutex.unlock order_mutex
  with
  | exn ->
    Mutex.unlock order_mutex;
    Logging.error_f
      ~section
      "Error processing order %s %s: %s"
      (side_str order)
      order.symbol
      (Printexc.to_string exn)
[@@ocaml.doc
  " Processes a single order: checks connectivity, resolves auth token, and dispatches via\n\
  \    the appropriate operation handler. "]
;;

let order_processing_loop () =
  let cycle_count = ref 0 in
  let orders_placed = Atomic.make 0 in
  let order_mutex = Mutex.create () in
  let rec loop () =
    if Atomic.get shutdown_requested
    then Lwt.return_unit
    else (
      let kraken_connected = Kraken.Kraken_trading_client.is_connected () in
      let is_hyperliquid_connected =
        try
          let hl_conn = Hashtbl.find connections "hyperliquid_ws" in
          get_state hl_conn = Connected
        with
        | Not_found -> false
      in
      let is_ibkr_connected =
        try
          let ibkr_conn = Hashtbl.find connections "ibkr_gateway" in
          get_state ibkr_conn = Connected
        with
        | Not_found -> false
      in
      let is_lighter_connected =
        try
          let lt_conn = Hashtbl.find connections "lighter_ws" in
          get_state lt_conn = Connected
        with
        | Not_found -> false
      in
      let is_alpaca_connected =
        try
          let alpaca_conn = Hashtbl.find connections "alpaca_trading_ws" in
          let st = get_state alpaca_conn in
          st = Connected || st = Connecting
        with
        | Not_found -> false
      in
      let is_connected (order : strategy_order) =
        match Dio_exchange.Exchange_intf.Types.exchange_of_string order.exchange with
        | Kraken -> kraken_connected
        | Hyperliquid -> is_hyperliquid_connected
        | Ibkr -> is_ibkr_connected
        | Lighter -> is_lighter_connected
        | Alpaca -> is_alpaca_connected
        | Custom _ -> true
      in
      let pending_grid_orders =
        Dio_strategies.Strategy_api.Strategy.get_pending_orders 100
      in
      if pending_grid_orders = []
      then (
        OrderSignal.wait ()
        >>= fun () ->
        Lwt.async loop;
        Lwt.return_unit)
      else (
        incr cycle_count;
        try
          List.iter
            (process_single_order ~orders_placed ~order_mutex ~is_connected)
            pending_grid_orders;
          Lwt.async loop;
          Lwt.return_unit
        with
        | exn ->
          Logging.error_f
            ~section
            "Exception in order processing loop: %s"
            (Printexc.to_string exn);
          Lwt.async loop;
          Lwt.return_unit))
  in
  Lwt.async loop
[@@ocaml.doc " The main order processing loop. "]
;;
