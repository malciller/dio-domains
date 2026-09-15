(** Central accounting module (milestone 2).

    Home for venue-fighting integrity logic that was previously embedded in the grid
    strategy, so every strategy and venue inherits it uniformly. This file currently
    carries the pure, state-free pieces extracted from [jacobs_ladder_execution.ml]; the
    state-coupled overlays migrate here next, with the differential harness proving
    behavior preservation. *)

(** Seconds after a sell placement during which its venue-side hold is treated as not yet
    reflected in the balance feed when the feed provides no freshness signal.
    Hyperliquid's spotState hold update trails the placement ack by up to seconds; sizing
    against the un-netted figure in that window lets a sell dip into reserved_base. *)
let sell_hold_netting_grace_s = 15.0

(** Freshness cutoff shared by the two feed-lag overlays (un-netted sell holds and
    un-netted buy credits): the newest balance message's wall-clock time, but never older
    than the grace - a dead feed must not keep an overlay alive forever. Entries at/after
    the cutoff belong to the lag window; older entries are retired. *)
let unreflected_cutoff ~now ~base_balance_age =
  match base_balance_age with
  | Some age -> Float.max (now -. age) (now -. sell_hold_netting_grace_s)
  | None -> now -. sell_hold_netting_grace_s
;;

(** Seconds after a buy ack during which the open-orders feed may not yet list the order.
    The venue's snapshot/stream lags the ack by up to seconds; a scan in that window sees
    zero open buys and would declare the acked buy a ghost, purging and re-placing it
    (grid churn plus stacked sell obligations). A buy that genuinely left the book is
    cleared by its terminal event (fill/cancel) or by this grace once the feed has listed
    it. *)
let buy_ack_ghost_grace_s = 15.0

(** Tolerance for [last_balance_delta] direction tests. Balances are base quantities
    accumulated by repeated float addition, so an unchanged venue figure can land a few
    ULPs above or below the adopted value; without this a spurious tiny positive delta
    would read as an "increase" and wedge an outstanding sell hold. *)
let balance_delta_epsilon = 1e-9

(** Max base-balance snapshot age for the excess sweep to run. The sweep sizes against the
    venue's [qty_available]; while a poll is hung (REST timeout) or the venue is still
    reconstructing an amend's hold, that figure is stale-HIGH and the sweep ratchets the
    top rung past [reserved_base]. A hung poll ages the snapshot past this bound and the
    sweep waits it out. *)
let sweep_max_balance_age_s = 10.0
