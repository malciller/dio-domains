(** Concrete strategy-agnostic engine context.

    Binds {!Strategy_actions_trade} to the domain: reads come from feed snapshots,
    emission pushes order intents (the domain supplies the submit callbacks that write the
    lock-free order buffer), tracking lives in {!Account_state}, and
    venue/accounting/price specifics are injected as callbacks so this module imports no
    strategy. A concrete strategy is the config file; the domain wires the callbacks once
    at startup. *)

open Strategy_expr

type price_math =
  { pm_sell : base:float -> mult:float -> float
  ; pm_amend : ref:float -> lo:float -> hi:float option -> float
  ; pm_grid :
      ref:float -> lo:float -> hi:float -> side:string -> snap:float option -> float
  ; pm_buy_ref : bid:float -> ask:float -> float
  ; pm_owed_sell : bid:float -> ask:float -> capital_exhausted:bool -> float
  }

type ctx =
  { se_symbol : string
  ; se_exchange : string
  ; se_account : Account_state.t
  ; se_now : unit -> float
  ; se_tob : unit -> (float * float * float * float) option
  ; se_capacity : asset:string -> (string * value) list
  ; se_iter_open_orders :
      (string -> float -> float -> string -> int option -> unit) -> unit
  ; se_submit_place :
      side:string
      -> qty:float
      -> price:float
      -> post_only:bool
      -> dedup_key:string
      -> string option
  ; se_submit_amend : token:string -> price:float -> qty:float -> dedup_key:string -> unit
  ; se_submit_cancel : token:string -> dedup_key:string -> unit
  ; se_submit_cancel_all : side:string -> dedup_key:string -> unit
  ; se_gate_balance : unit -> bool
  ; se_gate_capital_halted : unit -> bool
  ; se_is_ghost : string -> bool
  ; se_reconcile_position : unit -> unit
  ; se_reconcile_persisted : unit -> unit
  ; se_price : price_math
  }

let create
  ~symbol
  ~exchange
  ~account
  ~now
  ~tob
  ~capacity
  ~iter_open_orders
  ~submit_place
  ~submit_amend
  ~submit_cancel
  ~submit_cancel_all
  ~gate_balance
  ~gate_capital_halted
  ~is_ghost
  ~reconcile_position
  ~reconcile_persisted
  ~price
  =
  { se_symbol = symbol
  ; se_exchange = exchange
  ; se_account = account
  ; se_now = now
  ; se_tob = tob
  ; se_capacity = capacity
  ; se_iter_open_orders = iter_open_orders
  ; se_submit_place = submit_place
  ; se_submit_amend = submit_amend
  ; se_submit_cancel = submit_cancel
  ; se_submit_cancel_all = submit_cancel_all
  ; se_gate_balance = gate_balance
  ; se_gate_capital_halted = gate_capital_halted
  ; se_is_ghost = is_ghost
  ; se_reconcile_position = reconcile_position
  ; se_reconcile_persisted = reconcile_persisted
  ; se_price = price
  }
;;

let open_order_stats c =
  let buys = ref 0
  and sells = ref 0
  and closest_sell = ref nan in
  c.se_iter_open_orders (fun _ price _ side _ ->
    if String.equal side "buy"
    then incr buys
    else (
      incr sells;
      if Float.is_nan !closest_sell || price < !closest_sell then closest_sell := price));
  !buys, !sells, !closest_sell
;;

module Impl = Strategy_actions_trade.Make (struct
    type nonrec ctx = ctx

    let read_book c =
      match c.se_tob () with
      | None -> [ "bid", V_none; "ask", V_none; "mid", V_none ]
      | Some (bid, _bsize, ask, _asize) ->
        [ "bid", V_float bid; "ask", V_float ask; "mid", V_float ((bid +. ask) /. 2.0) ]
    ;;

    let read_capacity c ~asset = c.se_capacity ~asset

    let read_open_orders c =
      let buys, sells, closest = open_order_stats c in
      [ "open_buys", V_int buys
      ; "open_sells", V_int sells
      ; ("closest_sell", if Float.is_nan closest then V_none else V_float closest)
      ]
    ;;

    let place c ~side ~qty ~price ~post_only ~dedup_key =
      match c.se_submit_place ~side ~qty ~price ~post_only ~dedup_key with
      | Some token -> [ "token", V_string token ]
      | None -> [ "token", V_none ]
    ;;

    let amend c ~token ~price ~qty ~dedup_key =
      c.se_submit_amend ~token ~price ~qty ~dedup_key;
      []
    ;;

    let cancel c ~token ~dedup_key =
      c.se_submit_cancel ~token ~dedup_key;
      []
    ;;

    let cancel_all c ~side ~dedup_key =
      c.se_submit_cancel_all ~side ~dedup_key;
      []
    ;;

    let track_buy c ~token ~price = Account_state.track_buy c.se_account ~token ~price
    let track_sell c ~token ~price = Account_state.track_sell c.se_account ~token ~price

    let set_cooldown c ~name ~seconds =
      Account_state.set_cooldown c.se_account ~name ~seconds ~now:(c.se_now ())
    ;;

    let update_reserved_base c ~qty = Account_state.update_reserved_base c.se_account ~qty
    let accumulate c ~qty ~profit = Account_state.accumulate c.se_account ~qty ~profit
    let set_time _ _ = ()
    let notify_oracle _ _ = ()
    let gate_balance c = c.se_gate_balance ()
    let gate_capital_halted c = c.se_gate_capital_halted ()
    let is_ghost c ~token = c.se_is_ghost token
    let reconcile_position c = c.se_reconcile_position ()
    let reconcile_persisted_sell_levels c = c.se_reconcile_persisted ()
    let compute_sell_price c ~base ~mult = c.se_price.pm_sell ~base ~mult
    let compute_amend_price c ~ref ~lo ~hi = c.se_price.pm_amend ~ref ~lo ~hi

    let compute_grid_price c ~ref ~lo ~hi ~side ~snap =
      c.se_price.pm_grid ~ref ~lo ~hi ~side ~snap
    ;;

    let compute_buy_ref_price c ~bid ~ask = c.se_price.pm_buy_ref ~bid ~ask

    let owed_sell_price c ~bid ~ask ~capital_exhausted =
      c.se_price.pm_owed_sell ~bid ~ask ~capital_exhausted
    ;;
  end)

let handler (c : ctx) : Strategy_runtime.handler = Impl.handler c
