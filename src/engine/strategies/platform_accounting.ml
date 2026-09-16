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

(* Shared results for the empty case: returning a freshly-built [(holds, sum)] tuple on
   every no-op call allocated 3 words per cycle; these constants are reused instead. *)

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
let no_unnetted_hold = [], 0.0

let no_unreflected_credit = [], 0.0

let unnetted_sell_hold ~use_unnetted ~holds ~last_balance_delta ~now ~base_balance_age =
  if (not use_unnetted) || holds = []
  then no_unnetted_hold
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

(** Un-reflected buy credits (fills not yet reflected in the balance feed), mirroring
    {!unnetted_sell_hold} for the credit direction. [credits] is [(ts, qty)]; entries at
    or after the freshness cutoff are summed and kept. Returns [(remaining, sum)]. *)
let unreflected_credit ~credits ~now ~base_balance_age =
  if credits = []
  then no_unreflected_credit
  else (
    let cutoff = unreflected_cutoff ~now ~base_balance_age in
    let rec go sum acc = function
      | [] -> List.rev acc, sum
      | (ts, q) :: rest when ts >= cutoff -> go (sum +. q) ((ts, q) :: acc) rest
      | _ :: rest -> go sum acc rest
    in
    go 0.0 [] credits)
;;

(** Base to subtract from the venue's reported holding:
    [spot_holding - reserved_base - committed_sell_base].

    - Net-balance venues ([balance_nets_open_order_holds]):
      - [hold_netted_from_venue_state] (Hyperliquid): the venue nets holds from its own
        state (spotState [hold]), independent of our feed. Authoritative even when our
        feed drops a live order, so subtracting the ledger's excess over the feed would
        double-count. Only the short [unnetted_hold] dispatch overlay is subtracted.
      - otherwise (Kraken): holds derive from the same open-order feed the ledger tracks,
        so the ledger's excess over the feed compensates a dropped order and is
        subtracted, never below [unnetted_hold].
    - Gross-balance venues: the venue removes nothing, so the whole ledger is subtracted. *)
let effective_committed_sell_base
  ~balance_nets_open_order_holds
  ~hold_netted_from_venue_state
  ~ledger_total
  ~feed_total
  ~unnetted_hold
  =
  if balance_nets_open_order_holds
  then
    if hold_netted_from_venue_state
    then unnetted_hold
    else Float.max (Float.max 0.0 (ledger_total -. feed_total)) unnetted_hold
  else ledger_total
;;

(** Alpaca's sellable base: venue [qty_available] (free of resting holds) plus un-polled
    buy credits, minus the reserve and the un-netted hold overlay; falls back to the
    ledger basis when the venue exposes no figure. *)
let alpaca_available_base
  ~venue_available
  ~ledger_balance
  ~unreflected_credit
  ~reserved_base
  ~committed_sell
  ~unnetted_hold
  =
  if Float.is_nan venue_available
  then ledger_balance -. reserved_base -. committed_sell
  else
    Float.max 0.0 (venue_available +. unreflected_credit -. reserved_base -. unnetted_hold)
;;

(** Base available to offer without dipping into the reserve. A NaN venue balance yields
    [0.0]; an authoritative venue (Alpaca) uses {!alpaca_available_base}; otherwise the
    ledger basis (ledger minus reserve and committed sells). *)
let available_base
  ~is_venue_authoritative
  ~asset_balance_nan
  ~venue_available
  ~ledger_balance
  ~unreflected_credit
  ~reserved_base
  ~committed_sell
  ~unnetted_hold
  =
  if asset_balance_nan
  then 0.0
  else if is_venue_authoritative
  then
    alpaca_available_base
      ~venue_available
      ~ledger_balance
      ~unreflected_credit
      ~reserved_base
      ~committed_sell
      ~unnetted_hold
  else ledger_balance -. reserved_base -. committed_sell
;;

(** Persisted-sell-level matching. Pure helpers shared by the grid's [sync_open_orders]
    and [evaluate_sell_leg] reconciles. *)

(** Price key: [price * 10000] rounded to int. An int key keeps within-tolerance prices in
    the same (or an adjacent) bucket without allocating a string per lookup. *)
let price_key p = int_of_float (Float.round (p *. 10000.0))

(** Tolerance used to treat two persisted rung prices as the same level: 1 bp of price or
    1e-4 absolute. *)
let price_within_tolerance ~reference p =
  Float.abs (p -. reference) <= reference *. 0.0001 || Float.abs (p -. reference) <= 1e-4
;;

(** 1-to-1 multiset match between persisted sell levels and open sell orders. Returns
    (open_levels, missing_levels). Buckets open orders by tolerance-rounded price key and
    verifies the original tolerance before consuming a candidate: ~O(n+m).

    [open_orders] entries are [(id, price, qty)]; persisted entries are [(price, qty)]. *)
let partition_persisted_sell_levels persisted open_orders =
  let by_price : (int, (float * int) list) Hashtbl.t =
    Hashtbl.create (List.length open_orders)
  in
  List.iter
    (fun (_id, open_p, _open_q) ->
      let k = price_key open_p in
      let bucket = Option.value (Hashtbl.find_opt by_price k) ~default:[] in
      let rec bump acc = function
        | [] -> (open_p, 1) :: acc
        | (p, n) :: rest when p = open_p -> ((p, n + 1) :: rest) @ acc
        | item :: rest -> item :: bump acc rest
      in
      Hashtbl.replace by_price k (bump [] bucket))
    open_orders;
  let open_acc = ref [] in
  let missing_acc = ref [] in
  List.iter
    (fun ((target_p, _target_q) as level) ->
      let k = price_key target_p in
      (* Probe the bucket and its neighbors: [Float.round] (half-away) can place a price
         exactly on a 4-decimal boundary in either adjacent bucket. The per-candidate
         tolerance check below is authoritative. *)
      let matched =
        let rec try_buckets = function
          | [] -> None
          | bk :: rest ->
            (match Hashtbl.find_opt by_price bk with
             | Some bucket ->
               let rec consume acc = function
                 | [] -> None
                 | (p, n) :: rest when price_within_tolerance ~reference:target_p p ->
                   if n > 1 then Some (((p, n - 1) :: rest) @ acc) else Some (rest @ acc)
                 | item :: rest -> consume (item :: acc) rest
               in
               (match consume [] bucket with
                | Some nbucket -> Some (bk, nbucket)
                | None -> try_buckets rest)
             | None -> try_buckets rest)
        in
        try_buckets [ k - 1; k; k + 1 ]
      in
      match matched with
      | Some (bk, nbucket) ->
        Hashtbl.replace by_price bk nbucket;
        open_acc := level :: !open_acc
      | None -> missing_acc := level :: !missing_acc)
    persisted;
  List.rev !open_acc, List.rev !missing_acc
;;

(** Collapse persisted levels whose prices fall within the same tolerance bucket to a
    single rung, price-descending, keeping the FIRST occurrence. Callers pass a list with
    the live (feed-matched) entries ahead of the stale/missing ones, so the surviving rung
    carries the live qty. Returns the input list physically unchanged when there is
    nothing to drop. *)
let dedupe_persisted_sell_levels levels =
  let rec already_deduped_desc prev = function
    | [] -> true
    | (p, _) :: rest ->
      (match prev with
       | Some pp -> Float.compare pp p > 0 && not (price_within_tolerance ~reference:pp p)
       | None -> true)
      && already_deduped_desc (Some p) rest
  in
  if already_deduped_desc None levels
  then levels
  else (
    let sorted = List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) levels in
    let changed = ref false in
    let rec go acc = function
      | [] -> List.rev acc
      | (p, q) :: rest ->
        (match acc with
         | (ap, _) :: _ when price_within_tolerance ~reference:ap p ->
           changed := true;
           go acc rest
         | _ -> go ((p, q) :: acc) rest)
    in
    let merged = go [] sorted in
    if !changed then merged else levels)
;;

(** Per-exchange total reserved-quote atomics. The reservation ledger is platform-owned;
    the map is seeded with the accumulation venues and grows lazily for any other
    exchange. Avoids O(N) strategy_states locking. *)
let total_reserved_by_exchange =
  Atomic.make
    (List.fold_left
       (fun acc ex -> Strategy_common.StringMap.add ex (Atomic.make 0.0) acc)
       Strategy_common.StringMap.empty
       [ "kraken"; "hyperliquid"; "lighter"; "ibkr" ])
;;

(** Cached total-reserved-quote atomic for [exchange]. *)
let rec get_exchange_reserved_atomic exchange =
  let map = Atomic.get total_reserved_by_exchange in
  match Strategy_common.StringMap.find_opt exchange map with
  | Some a -> a
  | None ->
    let a3 = Atomic.make 0.0 in
    let new_map = Strategy_common.StringMap.add exchange a3 map in
    if Atomic.compare_and_set total_reserved_by_exchange map new_map
    then a3
    else get_exchange_reserved_atomic exchange
;;

(** Lock-free compare-and-set add on a reserved-quote atomic. *)
let rec atomic_add a diff =
  let old_val = Atomic.get a in
  if not (Atomic.compare_and_set a old_val (old_val +. diff)) then atomic_add a diff
;;

(** Ghost-buy grace predicate: the open-orders feed listed no buy, nothing is in flight or
    amend-active, and the ack grace has elapsed. A freshly acked buy is not listed by the
    feed yet - within the grace that lag is not a ghost; after it, a buy still absent with
    no terminal event is recovered. *)
let is_ghost_buy
  ~open_buy_count
  ~inflight_cancel_buy
  ~inflight_buy
  ~is_amend_active
  ~now
  ~last_buy_ack_ts
  =
  open_buy_count = 0
  && (not inflight_cancel_buy)
  && (not inflight_buy)
  && (not is_amend_active)
  && now -. last_buy_ack_ts >= buy_ack_ghost_grace_s
;;
