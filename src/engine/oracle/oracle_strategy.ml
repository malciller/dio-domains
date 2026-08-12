(* Oracle_strategy - the strategy-model interface of the capital oracle.

   The deployment engine (Oracle_deploy.Engine) and the generic sizing
   inversions (Oracle_replay.Sizing) operate against this signature instead of
   a concrete strategy: a strategy model supplies the survival-relevant
   mechanics -

     - how capital is consumed through a drawdown (cost_at,
       fills_for_drawdown, drawdown_of_fills): the funding function the pool
       inversion and the static runway are built on,
     - how a historical path replays (replay): the actual D_surv the strategy
       achieves on the asset's own history,
     - the per-cycle economics (profit_proxy),
     - the venue order floors (min_qty, round_qty).

   The tuned parameter is abstract: for the grid it is the grid interval
   (%), for another strategy it would be its own density/spacing parameter.
   [bounds] are not part of the model - the caller passes the config range
   (e.g. config.json's grid_interval [lo, hi]) into deploy_asset.

   The only implementation today is [Grid], backed by Grid_core (the pure
   mirror of the live Suicide_grid semantics). Adding a second strategy model
   is a matter of implementing this signature; the oracle (blend, sizing,
   deployment, allocation) is shared as-is. *)

open Oracle_types

module type S = sig
  type config

  (** The survival event of a replayed path: [d_surv] is the price drawdown at
      first exhaustion (1.0 when the strategy never ran dry), [halt_cause] the
      reason the first capital-low event fired. *)
  type outcome =
    { d_surv : float
    ; exhausted : bool
    ; halt_cause : [ `Capital | `Not_placeable ] option
    ; min_quote_drawdown : float
    ; buy_fills : int
    ; sell_fills : int
    }

  (** Current value of the tuned parameter (grid: grid interval in %). *)
  val parameter : config -> float

  (** Config with the tuned parameter set to [v]. *)
  val set_parameter : config -> float -> config

  (** Config with the order qty set to [q]. *)
  val set_qty : config -> float -> config

  (** Config with the quote budget (start_quote) set to [q]. *)
  val set_start_quote : config -> float -> config

  (** Template order qty (the deployment ceiling basis). *)
  val design_qty : config -> float

  (** Quote budget carried by the config (the pool the replay is funded
      with). *)
  val start_quote : config -> float

  (** Smallest placeable order qty (venue qty_min / lot floor). *)
  val min_qty : config -> float

  (** Lot-rounding (floor) to the venue's qty increment, never below
      [min_qty]. *)
  val round_qty : config -> float -> float

  (** Capital the strategy consumes through [n_fills] drawdown steps at [qty]
      (floor-aware: venue notional floors up-size the per-step quantity). *)
  val cost_at : config -> qty:float -> n_fills:int -> float

  (** Smallest number of drawdown steps whose cumulative runway reaches [d]
      (at least one). *)
  val fills_for_drawdown : config -> d:float -> int

  (** Static drawdown survived by [n_fills] steps. *)
  val drawdown_of_fills : config -> n_fills:int -> float

  (** Replay the strategy over a historical path (pessimistic ordering). *)
  val replay : config -> Oracle_types.series -> outcome

  (** Net profit of one cycle per unit of deployed capital. Advisory tuning
      metric: the actual deployed capital is used so a binding floor's drag
      shows up. *)
  val profit_proxy : config -> qty:float -> deployed:float -> float
end

(** The grid-ladder strategy model: the live Suicide_grid semantics as
    replayed by Grid_core (see Grid_core for the documented simplifications).
    The tuned parameter is the grid interval in %, the funding function the
    geometric ladder with dynamic buy up-sizing, the replay the pessimistic
    Buy_first Grid_core path replay. *)
module Grid : S with type config = Dio_strategies.Grid_core.config = struct
  module G = Dio_strategies.Grid_core

  type config = G.config

  type outcome =
    { d_surv : float
    ; exhausted : bool
    ; halt_cause : [ `Capital | `Not_placeable ] option
    ; min_quote_drawdown : float
    ; buy_fills : int
    ; sell_fills : int
    }

  let parameter (g : config) = g.G.grid_interval_pct
  let set_parameter g p = { g with G.grid_interval_pct = p }
  let set_qty g q = { g with G.qty = q }
  let set_start_quote g q = { g with G.start_quote = q }
  let design_qty (g : config) = g.G.qty
  let start_quote (g : config) = g.G.start_quote
  let min_qty (g : config) = Float.max g.G.qty_min g.G.qty_increment

  let round_qty (g : config) q =
    let inv = 1.0 /. g.G.qty_increment in
    Float.max (min_qty g) (Float.floor ((q *. inv) +. 1e-9) /. inv)
  ;;

  let cost_at (g : config) ~qty ~n_fills =
    Oracle_mfd.floor_aware_runway_cost
      ~qty
      ~grid_interval_pct:g.G.grid_interval_pct
      ~fee:g.G.maker_fee
      ~start_price:g.G.start_price
      ~min_notional:g.G.min_notional
      ~price_increment:g.G.price_increment
      ~qty_increment:g.G.qty_increment
      ~n_fills
  ;;

  (** Grid interval as a fraction, capped to stay clear of the log singularity
      at gi >= 1.0 (shared by the fill-count and drawdown inverses so they can
      never disagree). *)
  let grid_interval_frac (g : config) = Float.min (g.G.grid_interval_pct /. 100.0) 0.99

  let fills_for_drawdown (g : config) ~d =
    let gi = grid_interval_frac g in
    if d <= 0.0
    then 1
    else max 1 (int_of_float (Float.ceil (Float.log (1.0 -. d) /. Float.log (1.0 -. gi))))
  ;;

  let drawdown_of_fills (g : config) ~n_fills =
    let gi = grid_interval_frac g in
    1.0 -. ((1.0 -. gi) ** float_of_int n_fills)
  ;;

  let d_surv_of_result (r : G.result) =
    match r.G.first_exhaustion_price_drawdown with
    | Some d -> d
    (* The grid never ran dry: it survived every drawdown the history
       produced, so the survival threshold is 100% and F_h(1.0) = 1.0. This
       keeps coverage monotone in capital/qty, which the inverse-sizing binary
       searches rely on; the realized quote dip is still reported as
       min_quote_drawdown. *)
    | None -> 1.0
  ;;

  let replay (g : config) (s : Oracle_types.series) : outcome =
    let bars =
      s.bars
      |> Oracle_calendar.sort_bars
      |> Oracle_calendar.dedup
      |> Array.map (fun (b : Oracle_types.bar) ->
        Dio_strategies.Grid_core_types.{ high = b.high; low = b.low; close = b.close })
    in
    (* Anchor the ladder at the path's START (the earliest bar), not the last
       close: the replay simulates the strategy as if it had been running
       since the beginning of the available history, so the anchor and the
       path are time-consistent. Anchoring at the last close (today's price)
       would grind the ladder down through the whole historical range below
       it on any net-uptrend history - buying hundreds of levels in the first
       bars toward prices the strategy would never have seen - burning the
       capital on a phantom drawdown (the Grid_core ladder trails the market
       up, it never starts above it). The static funding math keeps the
       config start_price (today's price); only the replay anchor changes. *)
    let g =
      if Array.length bars = 0 then g else { g with G.start_price = bars.(0).close }
    in
    let r = G.replay g ~bars ~ordering:Dio_strategies.Grid_core_types.Buy_first in
    { d_surv = d_surv_of_result r
    ; exhausted = r.exhausted
    ; halt_cause = r.halt_cause
    ; min_quote_drawdown = r.min_quote_drawdown
    ; buy_fills = r.buy_fills
    ; sell_fills = r.sell_fills
    }
  ;;

  let profit_proxy (g : config) ~qty ~deployed =
    if deployed <= 0.0
    then 0.0
    else (
      let gi = g.G.grid_interval_pct /. 100.0 in
      let p_buy = g.G.start_price *. (1.0 -. gi) in
      let cycle_net = qty *. p_buy *. (gi -. (g.G.maker_fee *. (2.0 +. gi))) in
      cycle_net /. deployed)
  ;;
end
