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

(** Sell-hold overlays, parameterized over the hold list and the last balance delta so
    this module stays independent of strategy state. [holds] is oldest-first:
    [(placed_at, qty)]. Callers persist the returned list; these are pure transformations. *)

(** Portion of placed-sell base the balance feed may not yet be netting. Applies to every
    accumulation venue (Hyperliquid, Kraken, IBKR, Lighter): all report a tradeable figure
    with open-order holds removed, and that figure trails a placement (or the adopting
    balance message trails the order feed), so sizing against it can dip into
    reserved_base. Gating on [track_pending_sells = false] (Hyperliquid only) left the
    others exposed: under a burst several sells ack before the balance adopts the hold and
    each sizes against a stale-high tradeable.

    A hold is outstanding only while the newest balance message for this asset still
    predates its placement. A message generated after the placement retires it only when
    it did not raise the tradeable figure ([last_balance_delta <= 0]): a buy fill raises
    the figure and bumps the same per-asset freshness timestamp without netting a sell
    hold, so trusting a positive delta re-offered committed base as free and produced an
    oversized sell the venue rejected. Positive-delta messages keep the hold until a
    flat/down message or the grace retires it. The caller supplies per-asset freshness: a
    fill on another coin must not advance this asset's timestamp (see
    Hyperliquid_balances.BalanceStore.update_wallet). The grace bounds a dead feed;
    [consume_sell_hold_netting] additionally retires holds on an observed drop.

    Returns [(remaining_holds, unnetted_qty)]. *)
let unnetted_sell_hold ~use_unnetted ~holds ~last_balance_delta ~now ~base_balance_age =
  if (not use_unnetted) || holds = []
  then holds, 0.0
  else (
    let cutoff = unreflected_cutoff ~now ~base_balance_age in
    let grace_cutoff = now -. sell_hold_netting_grace_s in
    (* A message may certify netting only if its move was flat or down. An increase (buy
       fill) cannot have applied a sell hold. The tolerance absorbs the float jitter
       between an adopted venue figure and the same figure recomputed by the venue model,
       which otherwise reads as a tiny positive "increase" and wedges the hold. *)
    let message_may_certify = last_balance_delta <= balance_delta_epsilon in
    let rec go unnetted acc = function
      | [] -> List.rev acc, unnetted
      | (placed_at, qty) :: rest ->
        let grace_expired = placed_at < grace_cutoff in
        let released_by_message = message_may_certify && placed_at < cutoff in
        if grace_expired || released_by_message
        then go unnetted acc rest
        else go (unnetted +. qty) ((placed_at, qty) :: acc) rest
    in
    go 0.0 [] holds)
;;

(** Retires the OLDEST outstanding sell holds against an observed tradeable drop of
    [amount] (the venue netting applied holds). FIFO ordering matters: consuming by
    per-hold baseline let an older hold's netting release a newer, un-netted hold and
    over-offer a full lot. *)
let consume_sell_hold_netting ~holds ~amount =
  if amount > 0.0 && holds <> []
  then (
    let budget = ref amount in
    let rec go acc = function
      | [] -> List.rev acc
      | (placed_at, qty) :: rest when !budget <= 1e-12 ->
        List.rev_append acc ((placed_at, qty) :: rest)
      | (placed_at, qty) :: rest ->
        let take = Float.min !budget qty in
        budget := !budget -. take;
        let left = qty -. take in
        if left > 1e-12
        then List.rev_append acc ((placed_at, left) :: rest)
        else go acc rest
    in
    go [] holds)
  else holds
;;

(** Records a placed sell's hold. Kept oldest-first; retired FIFO by
    [consume_sell_hold_netting] on a tradeable drop, or by the grace. *)
let arm_sell_hold ~holds ~qty ~now = holds @ [ now, qty ]
