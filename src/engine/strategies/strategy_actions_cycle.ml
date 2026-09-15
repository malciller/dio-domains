(** Strategy-file cycle action handlers.

    Maps strategy-file actions to the generic decision/accounting functions
    (Strategy_decision / Strategy_lifecycle / Strategy_venue): each handler performs the
    same work the reference cycle does, so the config-driven strategy replicates by
    construction. *)

open Strategy_expr

let float_arg args key =
  match List.assoc_opt key args with
  | Some (V_float f) -> f
  | Some (V_int i) -> float_of_int i
  | Some (V_string s) ->
    (try float_of_string s with
     | _ -> nan)
  | _ -> nan
;;

let bool_arg args key =
  match List.assoc_opt key args with
  | Some (V_bool b) -> b
  | _ -> false
;;

let state_float_opt t key =
  match Strategy_runtime.get_state t key with
  | Some (V_float f) -> Some f
  | _ -> None
;;

let state_bool t key =
  match Strategy_runtime.get_state t key with
  | Some (V_bool b) -> b
  | _ -> false
;;

let platform_float t key default =
  match Hashtbl.find_opt t.Strategy_runtime.platform key with
  | Some (V_float f) -> f
  | _ -> default
;;

(** The buy-leg reference price: the last bid, else the ask
    ([Strategy_lifecycle.compute_buy_ref_price]). *)
let compute_buy_ref_price args =
  [ ( "price"
    , V_float
        (Strategy_lifecycle.compute_buy_ref_price
           ~bid_price:(float_arg args "bid")
           ~ask_price:(float_arg args "ask")) )
  ]
;;

(** Price at which a newly-owed sell would be placed. Ports
    [Strategy_decision.owed_sell_price]: the same base-price selection, the shared
    [grid_price] formula, and the ask-side floor (non-Alpaca). Inputs the reference takes
    from the per-asset config/caps: [grid_interval] (live blended value) from the platform
    fact table; [exchange] / [remaintain_expired_sells] / [round_price] from
    {!Strategy_runtime.caps}. *)
let owed_sell_price t args =
  let bid = float_arg args "bid" in
  let ask = float_arg args "ask" in
  let capital_exhausted = bool_arg args "capital_exhausted" in
  let grid_interval = platform_float t "grid_interval" 0.0 in
  let last_fill = state_float_opt t "last_buy_fill_price" in
  let resuming = state_bool t "resuming_after_balance_flag" in
  let is_alpaca = String.equal t.Strategy_runtime.caps.exchange "alpaca" in
  let base_price_for_sell =
    if t.Strategy_runtime.caps.remaintain_expired_sells
    then (
      match last_fill with
      | Some fill_p -> fill_p
      | None -> bid)
    else (
      match last_fill with
      | Some fill_p
        when capital_exhausted
             || ((not resuming)
                 && Float.abs (bid -. fill_p) <= bid *. (grid_interval /. 100.0)) ->
        fill_p
      | Some _ -> bid
      | None -> bid)
  in
  let raw_sell_price =
    Strategy_venue.grid_price
      ~round_price:t.Strategy_runtime.caps.round_price
      ~current:base_price_for_sell
      ~grid_interval_pct:grid_interval
      ~is_above:true
  in
  let price =
    if is_alpaca
    then raw_sell_price
    else if ask > 0.0
    then Float.max raw_sell_price ask
    else raw_sell_price
  in
  [ "price", V_float price ]
;;

(** Sellable base without dipping into the reserve (venue vs ledger basis). Ports
    {!Platform_accounting.available_base}. *)
let available_base _t args =
  [ ( "available"
    , V_float
        (Platform_accounting.available_base
           ~is_venue_authoritative:(bool_arg args "venue_authoritative")
           ~asset_balance_nan:(bool_arg args "asset_balance_nan")
           ~venue_available:(float_arg args "venue_available")
           ~ledger_balance:(float_arg args "ledger_balance")
           ~unreflected_credit:(float_arg args "unreflected_credit")
           ~reserved_base:(float_arg args "reserved_base")
           ~committed_sell:(float_arg args "committed_sell")
           ~unnetted_hold:(float_arg args "unnetted_hold")) )
  ]
;;

(** Grid price for the buy/sell legs: [current] moved by [grid_interval_pct] percent (up
    when [is_above]), snapped by the engine clock round_price. Ports
    [Strategy_venue.grid_price] (the buy leg passes [is_above] = false). *)
let grid_price t args =
  [ ( "price"
    , V_float
        (Strategy_venue.grid_price
           ~round_price:t.Strategy_runtime.caps.round_price
           ~current:(float_arg args "current")
           ~grid_interval_pct:(float_arg args "grid_interval_pct")
           ~is_above:(bool_arg args "is_above")) )
  ]
;;

let run t name args =
  match name with
  | "compute_buy_ref_price" -> compute_buy_ref_price args
  | "owed_sell_price" -> owed_sell_price t args
  | "available_base" -> available_base t args
  | "rung_price" -> grid_price t args
  | _ -> []
;;

let handler : Strategy_runtime.handler = { run }

(* Coarse cycle operations (hybrid: coarse now, decompose later).

   The engine provides a per-instance context whose functions call the generic cycle
   functions ([execute_strategy] / [sync_open_orders] / [evaluate_buy_leg] /
   [evaluate_sell_leg]), so a coarse port replicates behavior by construction. The
   strategy file orchestrates them; the bodies are split into fine actions in a later
   pass, verified by the harness. *)

(** Coarse phases the cycle engine attributes time/allocation to (dashboard STRAT
    breakdown). *)
type phase =
  | Preamble
  | Cleanup
  | Sync
  | Buy
  | Sell

module type ENGINE = sig
  type ctx

  val prepare : ctx -> unit
  val prepare_init : ctx -> unit
  val prepare_recovery : ctx -> unit
  val resolve_book : ctx -> bool
  val cleanup : ctx -> unit
  val expire_amend_cooldowns : ctx -> unit
  val evict_ghost_orders : ctx -> unit
  val sync : ctx -> unit
  val refresh_fee : ctx -> unit
  val guard : ctx -> bool
  val buy_gate : ctx -> bool
  val expire_tif_recovery : ctx -> unit
  val cycle_facts : ctx -> (string * Strategy_expr.value) list
  val mark_stale : ctx -> unit
  val buy_cancel : ctx -> unit
  val buy_place : ctx -> unit
  val buy_place_plan : ctx -> (string * Strategy_expr.value) list
  val buy_place_send : ctx -> unit
  val buy_place_send_insufficient : ctx -> unit
  val buy_place_latch_capital_low : ctx -> unit
  val buy_place_warn_quote : ctx -> unit
  val buy_amend : ctx -> unit
  val buy_amend_has_sell : ctx -> bool
  val buy_amend_with_sell : ctx -> unit
  val buy_amend_no_sell : ctx -> unit
  val sell_prepare : ctx -> unit
  val sell_place : ctx -> unit
  val sell_place_should : ctx -> bool
  val sell_place_body : ctx -> unit
  val sell_finalize : ctx -> unit
  val sell_finalize_facts : ctx -> (string * Strategy_expr.value) list
  val sell_finalize_latch : ctx -> unit
  val sell_excess_sweep_phase : ctx -> unit
  val sell_finalize_end : ctx -> unit
  val on_event : ctx -> Strategy_runtime.event -> unit

  (** [measure ctx phase f] runs [f] and attributes its time/allocation to [phase] when
      per-cycle profiling is enabled. *)
  val measure : ctx -> phase -> (unit -> unit) -> unit
end

module Make (E : ENGINE) = struct
  let handler (ctx : E.ctx) : Strategy_runtime.handler =
    let ph phase f =
      E.measure ctx phase f;
      []
    in
    { run =
        (fun t name _args ->
          match name with
          | "cycle_prepare" -> ph Preamble (fun () -> ignore (E.prepare ctx))
          | "init_venue_state" -> ph Preamble (fun () -> E.prepare_init ctx)
          | "prepare_recovery" -> ph Preamble (fun () -> E.prepare_recovery ctx)
          | "resolve_book" -> ph Preamble (fun () -> ignore (E.resolve_book ctx))
          | "cycle_cleanup" -> ph Cleanup (fun () -> E.cleanup ctx)
          | "expire_amend_cooldowns" ->
            ph Cleanup (fun () -> E.expire_amend_cooldowns ctx)
          | "evict_ghost_orders" -> ph Cleanup (fun () -> E.evict_ghost_orders ctx)
          | "scan_open_orders" -> ph Sync (fun () -> E.sync ctx)
          | "refresh_maker_fee" ->
            E.refresh_fee ctx;
            []
          | "cycle_guard" ->
            let cont = E.guard ctx in
            Strategy_runtime.set_platform t "engine:continue" (V_bool cont);
            []
          | "buy_gate" ->
            let active = E.buy_gate ctx in
            Strategy_runtime.set_platform t "engine:buy_active" (V_bool active);
            []
          | "expire_tif_recovery" ->
            E.expire_tif_recovery ctx;
            []
          | "cycle_facts" ->
            ph Preamble (fun () ->
              List.iter
                (fun (k, v) -> Strategy_runtime.set_platform t k v)
                (E.cycle_facts ctx))
          | "mark_stale_cycle" ->
            E.mark_stale ctx;
            []
          | "cancel_excess_buys" -> ph Buy (fun () -> E.buy_cancel ctx)
          | "buy_place" -> ph Buy (fun () -> E.buy_place ctx)
          | "buy_place_plan" ->
            ph Buy (fun () ->
              List.iter
                (fun (k, v) -> Strategy_runtime.set_platform t k v)
                (E.buy_place_plan ctx))
          | "buy_place_send" -> ph Buy (fun () -> E.buy_place_send ctx)
          | "buy_place_send_insufficient" ->
            ph Buy (fun () -> E.buy_place_send_insufficient ctx)
          | "buy_place_latch_capital_low" ->
            ph Buy (fun () -> E.buy_place_latch_capital_low ctx)
          | "buy_place_warn_quote" -> ph Buy (fun () -> E.buy_place_warn_quote ctx)
          | "buy_amend" -> ph Buy (fun () -> E.buy_amend ctx)
          | "buy_amend_has_sell" ->
            ph Buy (fun () ->
              Strategy_runtime.set_platform
                t
                "amend_has_sell"
                (V_bool (E.buy_amend_has_sell ctx)))
          | "buy_amend_with_sell" -> ph Buy (fun () -> E.buy_amend_with_sell ctx)
          | "buy_amend_no_sell" -> ph Buy (fun () -> E.buy_amend_no_sell ctx)
          | "plan_sell_order" -> ph Sell (fun () -> E.sell_prepare ctx)
          | "sell_place" -> ph Sell (fun () -> E.sell_place ctx)
          | "sell_place_should" ->
            ph Sell (fun () ->
              Strategy_runtime.set_platform
                t
                "sell_place_should"
                (V_bool (E.sell_place_should ctx)))
          | "sell_place_body" -> ph Sell (fun () -> E.sell_place_body ctx)
          | "sell_finalize" -> ph Sell (fun () -> E.sell_finalize ctx)
          | "sell_finalize_facts" ->
            ph Sell (fun () ->
              List.iter
                (fun (k, v) -> Strategy_runtime.set_platform t k v)
                (E.sell_finalize_facts ctx))
          | "sell_finalize_latch" -> ph Sell (fun () -> E.sell_finalize_latch ctx)
          | "sell_excess_sweep_phase" -> ph Sell (fun () -> E.sell_excess_sweep_phase ctx)
          | "sell_finalize_end" -> ph Sell (fun () -> E.sell_finalize_end ctx)
          | "apply_order_event" ->
            (match Strategy_runtime.current_event t with
             | Some ev -> E.on_event ctx ev
             | None -> ());
            []
          | _ -> [])
    }
  ;;
end
