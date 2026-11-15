(** FIX order entry: NewOrderSingle, OrderCancelReplaceRequest, OrderCancelRequest.

    Mirrors {!Kraken_actions} one call site at a time (same arguments, same result types,
    same eager cache bookkeeping is left to the caller), so the dimension adapter can swap
    transports without changing a strategy. `Error` carries the same human-readable text
    the WebSocket path returns: the FIX text (tag [58]), the business reject reason, or a
    timeout notice.

    Differences that follow from the protocol, not from a missing feature:

    - A single-order cancel ([35=F]) reaches only orders placed on this FIX session.
      Orders placed by another session require [35=q] (OrderMassCancelRequest), which this
      adapter does not expose because the engine's cancel path is per order.
    - FIX has no [order_userref]; attribution is kept in {!Kraken_fix_order_ids} and
      reattached to the execution reports.
    - [validate] is a WebSocket-only dry-run flag and is ignored here, as is
      [fee_preference] (Kraken charges per account settings over FIX) and
      [trigger_price_type] ([388 DiscretionInst] defaults to last trade, which is what the
      engine's stop/take-profit ladders assume). *)

open Lwt.Infix
open Kraken_fix_codec

let section = "kraken_fix_actions"
let string_contains = Error_handling.string_contains

(** [40 OrdType] for a Kraken order-type string. [None] for types FIX does not carry. *)
let ord_type_of = function
  | "market" -> Some '1'
  | "limit" -> Some '2'
  | "stop-loss" -> Some '3'
  | "stop-loss-limit" -> Some '4'
  | "take-profit" -> Some 'R'
  | "take-profit-limit" -> Some 'T'
  | "trailing-stop" -> Some 'U'
  | "trailing-stop-limit" -> Some 'V'
  | _ -> None
;;

(** [54 Side]. *)
let side_code = function
  | "buy" -> Some '1'
  | "sell" -> Some '2'
  | _ -> None
;;

(** [59 TimeInForce]. *)
let tif_code = function
  | "GTC" -> Some '1'
  | "IOC" -> Some '3'
  | "FOK" -> Some '4'
  | "GTD" -> Some '6'
  | _ -> None
;;

let precision_for symbol ~price =
  match Kraken_instruments_feed.get_precision_info symbol with
  | Some (price_precision, qty_precision) ->
    if price then price_precision else qty_precision
  | None -> 8
;;

(* Write the shared order fields once: every place/replace message carries the same
   instrument, side and quantity block. *)
let write_instrument b ~symbol ~side ~order_type ~qty =
  str b 55 symbol;
  char b 54 side;
  char b 40 order_type;
  float b ~precision:(precision_for symbol ~price:false) 38 qty
;;

(** Copies a [ClOrdID] from the caller when supplied so a strategy can key its own state
    on it; otherwise allocates one. Kraken rejects a reused ClOrdID, so the generated
    values are monotonic microseconds. *)
let resolve_cl_ord_id = function
  | Some id when String.trim id <> "" -> id
  | _ -> Kraken_fix_order_ids.next ()
;;

(** Places an order. The result resolves on the first execution report for the ClOrdID, so
    the returned [order_id] is the exchange-assigned one (tag [37]). *)
let place_order
  ~token:_
  ~order_type
  ~side
  ~order_qty
  ~symbol
  ?limit_price
  ?time_in_force
  ?post_only
  ?margin
  ?reduce_only
  ?order_userref
  ?cl_ord_id
  ?trigger_price
  ?trigger_price_type:_
  ?display_qty
  ?fee_preference:_
  ?validate
  ?retry_config:_
  ()
  : (Kraken_common_types.add_order_result, string) result Lwt.t
  =
  match ord_type_of order_type, side_code side with
  | None, _ ->
    Lwt.return
      (Error (Printf.sprintf "Kraken FIX does not support order type %S" order_type))
  | _, None -> Lwt.return (Error (Printf.sprintf "Unknown order side %S" side))
  | Some ord_type, Some side_code ->
    let tid = resolve_cl_ord_id cl_ord_id in
    let limit_price =
      Option.map
        (fun p -> Kraken_actions.truncate_price_to_precision p symbol)
        limit_price
    in
    let tif =
      match time_in_force with
      | Some tif ->
        (match tif_code tif with
         | Some code -> code
         | None -> '1')
      | None -> '1'
    in
    let exec_inst =
      List.filter_map
        (fun (enabled, code) -> if enabled then Some code else None)
        [ Option.value post_only ~default:false, 'P'
        ; Option.value reduce_only ~default:false, 'E'
        ]
    in
    (match validate with
     | Some true ->
       Logging.debug ~section "FIX has no dry-run mode; `validate` is ignored"
     | _ -> ());
    (match limit_price, ord_type with
     | None, ('2' | '4' | 'T' | 'V') ->
       Logging.warn_f
         ~section
         "Kraken FIX %s order for %s sent without a limit price"
         order_type
         symbol
     | _ -> ());
    let now = Kraken_fix_codec.utc_timestamp_of_unix (Unix.gettimeofday ()) in
    Kraken_fix_client.submit_place
      ?order_userref
      ~cl_ord_id:tid
      ~timeout_ms:(Kraken_fix_client.request_timeout_ms ())
      ~fill:(fun b ->
        str b 11 tid;
        write_instrument b ~symbol ~side:side_code ~order_type:ord_type ~qty:order_qty;
        (match limit_price with
         | Some price -> float b ~precision:(precision_for symbol ~price:true) 44 price
         | None -> ());
        (match trigger_price with
         | Some price ->
           float
             b
             ~precision:(precision_for symbol ~price:true)
             99
             (Kraken_actions.truncate_price_to_precision price symbol)
         | None -> ());
        (match display_qty with
         | Some qty -> float b ~precision:(precision_for symbol ~price:false) 1138 qty
         | None -> ());
        char b 59 tif;
        str b 60 now;
        if exec_inst <> []
        then (
          write_tag b 18;
          List.iteri
            (fun index code ->
              if index > 0 then Buffer.add_char b.body ' ';
              Buffer.add_char b.body code)
            exec_inst;
          Buffer.add_char b.body soh);
        match margin with
        | Some true -> str b 5001 "1"
        | Some false -> str b 5001 "0"
        | None -> ())
      ()
;;

(** Orders placed by another session (a previous run, the WebSocket transport, the UI)
    cannot be reached by a single-order FIX cancel or replace: Kraken scopes those to the
    session that placed the order. They stay fully cancellable over the WebSocket, which
    accepts any origin, so those requests are handed to the retail transport instead of
    bouncing off a business reject. *)
let delegate_to_websocket ~what f =
  match Kraken_order_transport.websocket () with
  | Some transport ->
    Logging.debug_f
      ~section
      "%s: order was not placed on this FIX session, delegating to the WebSocket \
       transport"
      what;
    f transport
  | None ->
    Lwt.return
      (Error
         (Printf.sprintf
            "%s: order belongs to another Kraken session and no WebSocket transport \
             is              available to reach it"
            what))
;;

(** Amends a working order ([35=G], Spot only). Side, symbol and original ClOrdID come
    from the order cache: the message requires all three and the caller only supplies the
    new price/quantity. *)
let amend_order
  ~token
  ~order_id
  ?cl_ord_id
  ?order_qty
  ?limit_price
  ?limit_price_type:_
  ?post_only
  ?trigger_price
  ?trigger_price_type:_
  ?display_qty
  ?deadline:_
  ?validate:_
  ?symbol
  ?retry_config:_
  ()
  : (Kraken_common_types.amend_order_result, string) result Lwt.t
  =
  match Kraken_executions_feed.find_order_everywhere order_id with
  | None ->
    Lwt.return (Error (Printf.sprintf "Order not found for amendment: %s" order_id))
  | Some existing ->
    (* OrderCancelReplaceRequest carries the execution model (tag 40) and has no way to
       express "keep the order type I was given". Sending 99 (StopPx) on a message that
       also says 40=2 would silently convert a resting stop into a limit order, so a
       trigger amend is refused and the caller falls back to cancel + place, which can
       state the type explicitly. *)
    (match trigger_price with
     | Some _ ->
       Lwt.return
         (Error
            "Amending a trigger price over FIX is not supported: \
             OrderCancelReplaceRequest cannot preserve the stop/take-profit order type. \
             Cancel and place instead.")
     | None ->
       let owned_here =
         match existing.Kraken_executions_feed.cl_ord_id with
         | Some id -> Kraken_fix_order_ids.placed_here id
         | None -> false
       in
       if not owned_here
       then
         delegate_to_websocket ~what:"amend_order" (fun transport ->
           transport.Kraken_order_transport.amend_order
             ~token
             ~order_id
             ?cl_ord_id
             ?order_qty
             ?limit_price
             ?post_only
             ?trigger_price
             ?display_qty
             ?symbol
             ())
       else (
         let symbol =
           Option.value symbol ~default:existing.Kraken_executions_feed.symbol
         in
         let orig_cl_ord_id =
           match existing.cl_ord_id, cl_ord_id with
           | Some id, _ -> Some id
           | None, Some id -> Some id
           | None, None -> None
         in
         (match orig_cl_ord_id with
          | Some _ -> ()
          | None ->
            Logging.warn_f
              ~section
              "Order %s has no ClOrdID; the replace message cannot reference it"
              order_id);
         let qty =
           Option.value order_qty ~default:existing.Kraken_executions_feed.order_qty
         in
         let side_code =
           match existing.side with
           | Kraken_executions_feed.Buy -> '1'
           | Kraken_executions_feed.Sell -> '2'
         in
         let tid = Kraken_fix_order_ids.next () in
         let now = Kraken_fix_codec.utc_timestamp_of_unix (Unix.gettimeofday ()) in
         Kraken_fix_client.submit_amend
           ~cl_ord_id:tid
           ~timeout_ms:(Kraken_fix_client.request_timeout_ms ())
           ~fill:(fun b ->
             str b 11 tid;
             str b 37 order_id;
             (match orig_cl_ord_id with
              | Some id -> str b 41 id
              | None -> ());
             write_instrument b ~symbol ~side:side_code ~order_type:'2' ~qty;
             (* A replace keeps a working order working: GTC is the only time-in-force the
                engine amends. *)
             char b 59 '1';
             (match limit_price with
              | Some price ->
                float
                  b
                  ~precision:(precision_for symbol ~price:true)
                  44
                  (Kraken_actions.truncate_price_to_precision price symbol)
              | None -> ());
             (match trigger_price with
              | Some price ->
                float
                  b
                  ~precision:(precision_for symbol ~price:true)
                  99
                  (Kraken_actions.truncate_price_to_precision price symbol)
              | None -> ());
             (match display_qty with
              | Some qty ->
                float b ~precision:(precision_for symbol ~price:false) 1138 qty
              | None -> ());
             str b 60 now;
             match post_only with
             | Some true -> char b 18 'P'
             | _ -> ())
           ()))
;;

(** Looks up a working order by any of the identifiers the engine's cancel path accepts.
    Symbols are scanned only when the order id does not resolve: cancels are rare next to
    the hot order path, and the cache holds at most the account's working orders. *)
let find_order ~cl_ord_ids ~userrefs order_id =
  match Kraken_executions_feed.find_order_everywhere order_id with
  | Some order -> Some order
  | None ->
    let matches (order : Kraken_executions_feed.open_order) =
      List.exists
        (fun id -> Option.equal String.equal order.cl_ord_id (Some id))
        cl_ord_ids
      || List.exists
           (fun ref_ -> Option.equal Int.equal order.order_userref (Some ref_))
           userrefs
    in
    Kraken_executions_feed.get_all_symbols ()
    |> List.find_map (fun symbol ->
      List.find_opt matches (Kraken_executions_feed.get_open_orders symbol))
;;

(** Cancels orders, one request per order, in parallel. Each request carries its own
    ClOrdID so the reports correlate per order. *)
let cancel_orders ~token ?order_ids ?cl_ord_ids ?order_userrefs ?retry_config:_ ()
  : (Kraken_common_types.cancel_order_result list, string) result Lwt.t
  =
  let order_ids = Option.value order_ids ~default:[] in
  let cl_ord_ids = Option.value cl_ord_ids ~default:[] in
  let userrefs = Option.value order_userrefs ~default:[] in
  let now_ts () = Kraken_fix_codec.utc_timestamp_of_unix (Unix.gettimeofday ()) in
  let cancel_one
    (target : [ `Order_id of string | `Cl_ord_id of string | `Userref of int ])
    =
    let label, described =
      match target with
      | `Order_id id -> "order_id", id
      | `Cl_ord_id id -> "cl_ord_id", id
      | `Userref r -> "order_userref", string_of_int r
    in
    let lookup_id =
      match target with
      | `Order_id id -> id
      | _ -> ""
    in
    let target_cl_ord_ids =
      match target with
      | `Cl_ord_id id -> [ id ]
      | _ -> []
    in
    let target_userrefs =
      match target with
      | `Userref r -> [ r ]
      | _ -> []
    in
    match
      find_order ~cl_ord_ids:target_cl_ord_ids ~userrefs:target_userrefs lookup_id
    with
    | None ->
      (* OrderCancelRequest needs the side and symbol, which only the cache knows. A
         WebSocket cancel needs just the identifier, so a missing cache entry means
         "delegate", not "give up": otherwise the order keeps working. *)
      Logging.warn_f
        ~section
        "Cannot cancel %s %s over FIX: order is not in the local cache; delegating to \
         the WebSocket transport"
        label
        described;
      delegate_to_websocket ~what:"cancel_orders" (fun transport ->
        match target with
        | `Order_id id ->
          transport.Kraken_order_transport.cancel_orders ~token ~order_ids:[ id ] ()
        | `Cl_ord_id id ->
          transport.Kraken_order_transport.cancel_orders ~token ~cl_ord_ids:[ id ] ()
        | `Userref r ->
          transport.Kraken_order_transport.cancel_orders ~token ~order_userrefs:[ r ] ())
      >|= (function
       | Ok (result :: _) -> Ok result
       | Ok [] -> Error (Printf.sprintf "Unknown order: %s" described)
       | Error e -> Error e)
    | Some order ->
      let owned_here =
        match order.Kraken_executions_feed.cl_ord_id with
        | Some id -> Kraken_fix_order_ids.placed_here id
        | None -> false
      in
      if not owned_here
      then
        delegate_to_websocket ~what:"cancel_orders" (fun transport ->
          transport.Kraken_order_transport.cancel_orders
            ~token
            ~order_ids:[ order.Kraken_executions_feed.order_id ]
            ())
        >|= function
        | Ok (result :: _) -> Ok result
        | Ok [] -> Error (Printf.sprintf "Unknown order: %s" described)
        | Error e -> Error e
      else (
        let tid = Kraken_fix_order_ids.next () in
        let symbol = order.Kraken_executions_feed.symbol in
        let side_code =
          match order.side with
          | Kraken_executions_feed.Buy -> '1'
          | Kraken_executions_feed.Sell -> '2'
        in
        Kraken_fix_client.submit_cancel
          ~cl_ord_id:tid
          ~timeout_ms:(Kraken_fix_client.request_timeout_ms ())
          ~fill:(fun b ->
            str b 11 tid;
            str b 37 order.order_id;
            (match order.cl_ord_id with
             | Some orig -> str b 41 orig
             | None -> ());
            char b 54 side_code;
            str b 55 symbol;
            str b 60 (now_ts ()))
          ())
  in
  let targets =
    List.map (fun id -> `Order_id id) order_ids
    @ List.map (fun id -> `Cl_ord_id id) cl_ord_ids
    @ List.map (fun ref_ -> `Userref ref_) userrefs
  in
  Lwt_list.map_p cancel_one targets
  >|= fun results ->
  let results =
    List.filter_map
      (function
        | Ok r -> Some r
        | Error e ->
          Logging.error_f ~section "FIX cancel failed: %s" e;
          None)
      results
  in
  if results <> [] then Ok results else Error "Failed to cancel orders"
;;
