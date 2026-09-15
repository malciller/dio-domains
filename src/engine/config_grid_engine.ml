(** Config-driven grid engine context (milestone 3, coarse wrapper).

    Holds the per-cycle strategy inputs and calls the reference [execute_strategy] with
    them, so a config-driven grid replicates by construction. Enabled only when
    [config_strategy] is set; the mutable fields are updated in place each cycle (no
    per-cycle allocation). Shared by the domain loop and the offline candidate replay, so
    the candidate interpreter is exercised on exactly the wiring the live loop uses. *)

module Strategy = Dio_strategies.Jacobs_ladder.Strategy

type ctx =
  { mutable cg_asset : Dio_strategies.Jacobs_ladder_types.trading_config option
  ; mutable cg_state : Dio_strategies.Jacobs_ladder_types.strategy_state option
  ; mutable cg_price : float
  ; mutable cg_bid : float
  ; mutable cg_ask : float
  ; mutable cg_abal : float
  ; mutable cg_qbal : float
  ; mutable cg_now : float
  ; mutable cg_cycle : int
  ; mutable cg_quote_stale : bool
  ; mutable cg_oracle_halted : bool
  ; mutable cg_base_age : float option
  ; mutable cg_gen : int
  ; mutable cg_iter : (string -> float -> float -> string -> int option -> unit) -> unit
  }

let create () =
  { cg_asset = None
  ; cg_state = None
  ; cg_price = nan
  ; cg_bid = nan
  ; cg_ask = nan
  ; cg_abal = nan
  ; cg_qbal = nan
  ; cg_now = 0.0
  ; cg_cycle = 0
  ; cg_quote_stale = false
  ; cg_oracle_halted = false
  ; cg_base_age = None
  ; cg_gen = -1
  ; cg_iter = (fun _ -> ())
  }
;;

let run_cycle c =
  match c.cg_asset, c.cg_state with
  | Some asset, Some state ->
    Strategy.execute
      ~cached_state:state
      ~quote_balance_stale:c.cg_quote_stale
      ~oracle_halted:c.cg_oracle_halted
      ~get_open_orders_generation:(fun () -> c.cg_gen)
      ~base_balance_age:c.cg_base_age
      ~now:c.cg_now
      asset
      c.cg_price
      c.cg_bid
      c.cg_ask
      c.cg_abal
      c.cg_qbal
      0
      0
      c.cg_iter
      c.cg_cycle
  | _ -> ()
;;

let sync_open_orders (_ : ctx) = ()
let evaluate_buy_leg (_ : ctx) = ()
let evaluate_sell_leg (_ : ctx) = ()
