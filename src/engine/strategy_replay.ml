(** Live replay driver (design §8.3).

    Re-runs the grid strategy against the inputs recorded in a {!Strategy_trace.t} — the
    per-cycle market/account facts and the venue open-order feed — and captures the
    emitted order intents. Diffing the result (emitted-only) against the recorded emitted
    intents proves the input record is sufficient to reproduce the strategy's decisions.

    Approximations: the effective grid interval and accumulation buffer are taken from the
    config entry, not the live oracle blend. If a replay diverges, those are the first
    inputs to add to the record. *)

module Trace = Dio_strategies.Strategy_trace
module Expr = Dio_strategies.Strategy_expr
module Jac = Dio_strategies.Jacobs_ladder
module Order = Dio_strategies.Strategy_common

let f_entry entries key default =
  match List.assoc_opt key entries with
  | Some (Expr.V_float f) -> f
  | Some (Expr.V_int i) -> float_of_int i
  | _ -> default
;;

let i_entry entries key default =
  match List.assoc_opt key entries with
  | Some (Expr.V_int i) -> i
  | Some (Expr.V_float f) -> int_of_float f
  | _ -> default
;;

let b_entry entries key default =
  match List.assoc_opt key entries with
  | Some (Expr.V_bool b) -> b
  | _ -> default
;;

let age_entry entries =
  match List.assoc_opt "balance_age" entries with
  | Some (Expr.V_float a) -> Some a
  | _ -> None
;;

(** Split a cycle's observations into its input state entries and its venue open orders. *)
let split_cycle (c : Trace.cycle) =
  let state = ref []
  and orders = ref [] in
  List.iter
    (fun o ->
      match o with
      | Trace.State es -> state := es
      | Trace.Order_intent oi -> orders := oi :: !orders
      | _ -> ())
    c.c_obs;
  !state, List.rev !orders
;;

let emitted_of_order (o : Order.strategy_order) : Trace.emitted =
  { em_op =
      (match o.operation with
       | Order.Place -> "place"
       | Order.Amend -> "amend"
       | Order.Cancel -> "cancel")
  ; em_symbol = o.symbol
  ; em_side =
      (match o.side with
       | Order.Buy -> "buy"
       | Order.Sell -> "sell")
  ; em_qty = o.qty
  ; em_price = Option.value o.price ~default:nan
  ; em_post_only = o.post_only
  ; em_order_id = o.order_id
  }
;;

(** Replay [trace] through the reference strategy for [asset], returning an emitted-only
    trace. *)
let replay ~(asset : Jac.trading_config) ~(trace : Trace.t) : Trace.t =
  let state = Jac.get_strategy_state asset.symbol in
  (* Drain any pre-existing buffer so only this replay's intents are captured. *)
  ignore (Jac.get_pending_orders 1_000_000 : Order.strategy_order list);
  List.mapi
    (fun idx (c : Trace.cycle) ->
      let entries, open_orders = split_cycle c in
      let price = f_entry entries "price" nan in
      let bid = f_entry entries "bid" nan in
      let ask = f_entry entries "ask" nan in
      let abal = f_entry entries "asset_balance" nan in
      let qbal = f_entry entries "quote_balance" nan in
      let now = f_entry entries "now" 0.0 in
      let cycle = i_entry entries "cycle" idx in
      let oracle_halted = b_entry entries "oracle_halted" false in
      let quote_stale = b_entry entries "quote_balance_stale" false in
      let base_age = age_entry entries in
      let grid_qty = f_entry entries "grid_qty" nan in
      if Float.is_finite grid_qty then state.grid_qty <- grid_qty;
      let orders = Array.of_list open_orders in
      let iter f =
        Array.iter
          (fun (oi : Trace.order_intent) ->
            f "replay" oi.oi_price oi.oi_qty oi.oi_side None)
          orders
      in
      Jac.Strategy.execute
        ~cached_state:state
        ~quote_balance_stale:quote_stale
        ~oracle_halted
        ~get_open_orders_generation:(fun () -> 0)
        ~base_balance_age:base_age
        ~now
        asset
        price
        bid
        ask
        abal
        qbal
        0
        0
        iter
        cycle;
      let pending = Jac.get_pending_orders 1_000_000 in
      { Trace.c_index = idx
      ; c_obs = List.map (fun e -> Trace.Emitted e) (List.map emitted_of_order pending)
      })
    trace
;;

let asset_of (tc : Config.trading_config) : Jac.trading_config =
  { Jac.exchange = tc.exchange
  ; symbol = tc.symbol
  ; qty = tc.qty
  ; grid_interval = fst tc.grid_interval
  ; sell_mult = tc.sell_mult
  ; strategy = tc.strategy
  ; maker_fee = tc.maker_fee
  ; taker_fee = tc.taker_fee
  ; accumulation_buffer = fst tc.accumulation_buffer
  ; base_accumulation = tc.base_accumulation
  ; sell_levels_persistence = tc.sell_levels
  }
;;

let symbol_of_trace (trace : Trace.t) : string option =
  let rec go = function
    | [] -> None
    | (c : Trace.cycle) :: rest ->
      (match
         List.find_map
           (function
             | Trace.Emitted e -> Some e.em_symbol
             | Trace.Order_intent oi -> Some oi.oi_symbol
             | _ -> None)
           c.c_obs
       with
       | Some s -> Some s
       | None -> go rest)
  in
  go trace
;;

(** Load a recorded trace, replay it through the reference grid, and report equivalence
    between the recorded and replayed emitted intents. Writes [<path>.replayed.json]. *)
let run (path : string) : int =
  match Trace.load path with
  | exception exn ->
    Printf.eprintf "replay: cannot load %s: %s\n" path (Printexc.to_string exn);
    1
  | trace ->
    (match symbol_of_trace trace with
     | None ->
       Printf.eprintf "replay: no symbol found in %s\n" path;
       1
     | Some symbol ->
       let config = Config.read_config () in
       (match
          List.find_opt
            (fun (tc : Config.trading_config) -> String.equal tc.symbol symbol)
            config.trading
        with
        | None ->
          Printf.eprintf "replay: no config.json entry for %s\n" symbol;
          1
        | Some tc ->
          let asset = asset_of tc in
          let replayed = replay ~asset ~trace in
          Trace.save (path ^ ".replayed.json") replayed;
          (match Trace.compare (Trace.emitted_only trace) replayed with
           | None ->
             Printf.printf
               "replay equivalent: %s (%d cycles, %d emitted cycles)\n"
               path
               (List.length trace)
               (List.length replayed);
             0
           | Some msg ->
             Printf.printf "replay divergence: %s\n" msg;
             1)))
;;
