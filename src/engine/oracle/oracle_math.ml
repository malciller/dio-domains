(* Oracle math - dependency-free numerical helpers shared by MFD and stats.

   A percentile of an empty sample is indistinguishable from a legitimate
   "zero drawdown" observation, so both percentile estimators raise
   [Invalid_argument] on empty (or zero-total-weight) input instead of
   returning 0.0: an empty distribution must never masquerade as "this asset
   never drew down" or feed false precision into a percentile table.

   Also hosts the peak-to-valley drawdown reference ([range_stats_of] for
   display context, [peak_to_valley_stats_of] for the sizing drawdown) so the
   deployment engine and the sizing inversions share the same reference
   without a dependency cycle. *)

open Oracle_types

let percentile xs p =
  let n = Array.length xs in
  if n = 0
  then
    invalid_arg
      "Oracle_math.percentile: empty distribution (no samples); refusing to fabricate a \
       percentile"
  else (
    let arr = Array.copy xs in
    Array.sort Float.compare arr;
    if p <= 0.0
    then arr.(0)
    else if p >= 100.0
    then arr.(n - 1)
    else (
      let rank = p /. 100.0 *. float_of_int (n - 1) in
      let lo = int_of_float (Float.floor rank) in
      let hi = min (lo + 1) (n - 1) in
      let frac = rank -. float_of_int lo in
      arr.(lo) +. (frac *. (arr.(hi) -. arr.(lo)))))
;;

(** Standard deviation (sample by default). *)
let std ?(sample = true) xs =
  let n = Array.length xs in
  if n < 2
  then 0.0
  else (
    let mean = Array.fold_left ( +. ) 0.0 xs /. float_of_int n in
    let var =
      Array.fold_left (fun acc x -> acc +. ((x -. mean) *. (x -. mean))) 0.0 xs
      /. float_of_int (if sample then n - 1 else n)
    in
    sqrt var)
;;

(** Percentile of a weighted empirical distribution by weighted linear
    interpolation - the Type 7 analog of [percentile]. Each sample is anchored
    at the end of its cumulative weight mass, and the quantile position
    h = (total - 1) * p/100 + 1 is linearly interpolated between the two
    samples bracketing it. With unit weights this reduces EXACTLY to the
    unweighted [percentile] estimator, so the class and asset percentiles
    report consistent values on the same underlying distribution (a step
    function can sit off a linear interpolation and make the two estimators
    disagree at the same percentile). Weights must be positive; values are
    sorted internally. Used to invert the blended (asset + kappa-weighted
    class) CDF. *)
let weighted_percentile (pairs : (float * float) array) p =
  if Array.length pairs = 0
  then
    invalid_arg
      "Oracle_math.weighted_percentile: empty distribution (no samples); refusing to \
       fabricate a percentile"
  else (
    let arr = Array.copy pairs in
    Array.sort (fun (a, _) (b, _) -> Float.compare a b) arr;
    let total = Array.fold_left (fun acc (_, w) -> acc +. w) 0.0 arr in
    if total <= 0.0
    then
      invalid_arg
        "Oracle_math.weighted_percentile: zero total weight; refusing to fabricate a \
         percentile"
    else if p <= 0.0
    then fst arr.(0)
    else if p >= 100.0
    then fst arr.(Array.length arr - 1)
    else (
      let h = ((total -. 1.0) *. (p /. 100.0)) +. 1.0 in
      let n = Array.length arr in
      (* [j] = largest index whose cumulative weight (including its own) still
         ends at or below [h]; [acc] ends holding the cumulative weight
         through [j + 1]. *)
      let acc = ref 0.0 in
      let j = ref (-1) in
      let i = ref 0 in
      while !i < n && !acc <= h do
        acc := !acc +. snd arr.(!i);
        if !acc <= h then j := !i;
        incr i
      done;
      if !j < 0
      then fst arr.(0)
      else if !j >= n - 1
      then fst arr.(n - 1)
      else (
        let v_lo = fst arr.(!j) in
        let v_hi = fst arr.(!j + 1) in
        let w_hi = snd arr.(!j + 1) in
        let frac = Float.max 0.0 (Float.min 1.0 ((h -. !acc +. w_hi) /. w_hi)) in
        v_lo +. (frac *. (v_hi -. v_lo)))))
;;

(** Per-asset historical price-range reference from the (deepened) series:
    ATH = highest high, all-time low = lowest low, price = last close.
    [None] on an empty history. Informational context for the report: the
    ATH-to-ATL span is NOT the sizing drawdown (a 1000x run-up makes it read
    like a 99.9% drawdown even though no such fall ever took place); the
    sizing reference is the largest actual peak-to-valley drawdown (see
    [peak_to_valley_stats_of]). *)
let range_stats_of (asset : series) : range_stats option =
  let n = Array.length asset.bars in
  if n = 0
  then None
  else (
    let ath = ref 0.0 in
    let low = ref max_float in
    Array.iter
      (fun (b : bar) ->
         ath := Float.max !ath b.high;
         low := Float.min !low b.low)
      asset.bars;
    let ath = !ath in
    let low = !low in
    let price = asset.bars.(n - 1).close in
    let range_span = if ath > 0.0 then (ath -. low) /. ath else 0.0 in
    let d_from_ath = if ath > 0.0 then (ath -. price) /. ath else 0.0 in
    let d_to_low = if price > 0.0 then (price -. low) /. price else 0.0 in
    Some { ath; all_time_low = low; price; d_from_ath; d_to_low; range_span })
;;

(** The largest drawdown the asset has ACTUALLY taken, peak to valley, over
    its whole (deepened) history. Each bar's drawdown is measured from the
    running peak of closes (the highest close seen so far) down to that bar's
    low - a real peak-to-valley fall, so a 1000x run-up only registers the
    falls that actually happened, never the ATH-to-ATL span. The maximum is
    the anchor of the sizing drawdown: the grid must fund the fall the asset
    has really experienced from wherever the price sits today (the funded
    amount is the ATH-scaled remainder, see [sizing_reference_of]).

    The event's [recovered] flag (a later close >= the peak) decides whether
    it anchors the ATH-scaled survival reference at all: a downtrend that
    never ended in recovery means the asset is still living in it and the
    measured floor-overshoot policy funds it instead.

    The bars are sorted chronologically first (and de-duplicated by date) -
    the same order every other consumer works in. This is not optional: the
    venue feeds can return bars newest-first (the Hyperliquid pagination
    reverses its pages), and a backwards series would fabricate "peak -> "
    events whose valley PREDATES the peak (e.g. "peak 74.51 on 2026-06-03 ->
    valley 2.00 on 2024-11-29").

    Bars with non-finite or non-positive close/low are skipped (same guard as
    the MFD windows). [None] when the history is empty or no drawdown ever
    occurred (a strictly monotone rising series: every close is its own
    peak). *)
let peak_to_valley_stats_of (asset : series) : p2v_stats option =
  let bars = Oracle_calendar.sort_bars asset.bars |> Oracle_calendar.dedup in
  let n = Array.length bars in
  if n = 0
  then None
  else (
    let peak = ref 0.0 in
    let peak_idx = ref 0 in
    let best_dd = ref 0.0 in
    let best_peak = ref 0.0 in
    let best_peak_idx = ref 0 in
    let best_peak_date = ref "" in
    let best_valley = ref 0.0 in
    let best_valley_idx = ref 0 in
    let best_valley_date = ref "" in
    Array.iteri
      (fun i (b : bar) ->
         if
           Float.is_finite b.close
           && b.close > 0.0
           && Float.is_finite b.low
           && b.low > 0.0
         then (
           (* The running peak of closes up to and including this bar: the
              drawdown is measured from the highest close the market has
              actually reached before this bar's low. *)
           if b.close > !peak
           then (
             peak := b.close;
             peak_idx := i);
           let dd = 1.0 -. (b.low /. !peak) in
           if dd > !best_dd
           then (
             best_dd := dd;
             best_peak := !peak;
             best_peak_idx := !peak_idx;
             best_peak_date := bars.(!peak_idx).date;
             best_valley := b.low;
             best_valley_idx := i;
             best_valley_date := b.date)))
      bars;
    if !best_dd <= 0.0
    then None
    else (
      (* Recovery: any later close at or above the peak - the downtrend ended
         in a full retrace, not just a bounce. *)
      let recovered =
        let rec go i = i < n && (bars.(i).close >= !best_peak || go (i + 1)) in
        go (!best_valley_idx + 1)
      in
      Some
        { max_drawdown = Float.min 0.999999 !best_dd
        ; peak = !best_peak
        ; peak_date = !best_peak_date
        ; peak_idx = !best_peak_idx
        ; valley = !best_valley
        ; valley_date = !best_valley_date
        ; valley_idx = !best_valley_idx
        ; price = bars.(n - 1).close
        ; recovered
        }))
;;

(** Measured floor overshoot: how far the price fell below an ESTABLISHED
    floor before recovering. A floor is established when the price bottoms
    (a new running-minimum low) and then bounces above that low; a floor
    BREAK is a later low below the established floor, measured as
    (floor - breach_low) / floor. Only breaks that were followed by a
    recovery (a close back at or above the episode's peak) count - a floor
    that broke and never recovered has no proof of recovery. The 90th
    percentile of the breaks is the demonstrated "how much further can it
    fall past the floor" reference that funds the at-floor / unrecovered
    regimes (see [sizing_drawdown_of]).

    A continuous fall never breaks a floor (no floor was established - there
    was no bounce), so the deepest crash itself contributes nothing; the
    distribution comes from floors that held, bounced, and were later broken
    (e.g. the pre-crash dip floor that the crash broke through).

    [None] when no recovered floor-break exists (or no floor ever held):
    there is no floor-break evidence to measure - callers fund the 0.15
    fallback constant. *)
let floor_overshoot_p90_of (asset : series) : float option =
  let bars = Oracle_calendar.sort_bars asset.bars |> Oracle_calendar.dedup in
  let n = Array.length bars in
  if n = 0
  then None
  else (
    (* Pending floor-breaks of the in-progress episode, committed to the
       distribution only when the episode recovers. *)
    let pending = ref [] in
    let breaks = ref [] in
    let peak = ref 0.0 in
    let run_min = ref max_float in
    let run_min_close = ref 0.0 in
    let floor = ref None in
    let commit () =
      breaks := List.rev_append !pending !breaks;
      pending := []
    in
    let discard () = pending := [] in
    Array.iter
      (fun (b : bar) ->
         if
           Float.is_finite b.close
           && b.close > 0.0
           && Float.is_finite b.low
           && b.low > 0.0
         then
           if !peak = 0.0
           then peak := b.close
           else if b.close >= !peak
           then (
             (* Recovery (a close back at the episode's peak): the episode's
                breaks are proven - commit them. A new high also starts a
                new episode from here. *)
             commit ();
             if b.close > !peak then peak := b.close)
           else if b.low < !run_min
           then (
             (* A new running-minimum low: an established floor below is
                broken (a pending break), otherwise this is the start of a
                fresh fall. *)
             (match !floor with
              | Some f -> pending := ((f -. b.low) /. f) :: !pending
              | None -> ());
             run_min := b.low;
             run_min_close := b.close;
             floor := None)
           else if b.close > !run_min_close && !run_min < max_float
           then floor := Some !run_min
           else ())
      bars;
    (* The series end is not a recovery: unproven breaks are discarded. *)
    discard ();
    match !breaks with
    | [] -> None
    | bs -> Some (percentile (Array.of_list bs) 90.0))
;;

(** The ATH-scaled sizing drawdown for one asset (mature / authoritative
    assets). [ath] is the all-time high of the deepened history (see
    [range_stats_of]); [p] the largest actual peak-to-valley event; [fallback]
    is the immature-history flag - fallback assets keep the raw event
    drawdown, the discount is a matured-regime feature. [overshoot_p90] is
    [floor_overshoot_p90_of]'s measurement (None = nothing measured, the 0.15
    fallback funds instead).

    Regimes:
    - fallback: [d_cover = max_drawdown] (raw, unchanged).
    - deepest event never recovered ([outlier]): no recovered anchor - fund
      the measured floor overshoot (0.15 when nothing was measured).
    - recovered, price above [floor_ref = ATH * (1 - max_drawdown)]: fund the
      remaining drop to the floor, (price - floor_ref) / price. This never
      exceeds [max_drawdown] (price <= ATH), so the worst-ever drop is
      automatically the cap - a maturing asset is not expected to repeat it.
    - recovered, price at/below the floor ([at_floor], "living in the max
      drawdown"): the remainder is exhausted - fund the measured floor
      overshoot. *)
let sizing_drawdown_of
      ~(ath : float)
      ~(fallback : bool)
      ~(overshoot_p90 : float option)
      (p : p2v_stats)
  : sizing_reference
  =
  let overshoot = Option.value overshoot_p90 ~default:0.15 in
  if fallback
  then
    { d_cover = p.max_drawdown
    ; floor_ref = None
    ; at_floor = false
    ; outlier = false
    ; overshoot_p90
    }
  else if not p.recovered
  then
    { d_cover = overshoot
    ; floor_ref = None
    ; at_floor = true
    ; outlier = true
    ; overshoot_p90
    }
  else (
    let floor_ref = ath *. (1.0 -. p.max_drawdown) in
    let at_floor =
      not
        (p.price > 0.0
         && Float.is_finite p.price
         && floor_ref > 0.0
         && p.price > floor_ref)
    in
    if at_floor
    then
      { d_cover = overshoot
      ; floor_ref = Some floor_ref
      ; at_floor = true
      ; outlier = false
      ; overshoot_p90
      }
    else
      { d_cover = (p.price -. floor_ref) /. p.price
      ; floor_ref = Some floor_ref
      ; at_floor = false
      ; outlier = false
      ; overshoot_p90
      })
;;

(** Compose the ATH-scaled survival reference for an asset: the deepest
    actual peak-to-valley event, the ATH and the measured floor overshoot,
    fed through [sizing_drawdown_of]. [None] when the history never drew down
    (monotone rising) - callers fall back to the statistical governing
    drawdown. *)
let sizing_reference_of ~(fallback : bool) (asset : series) : sizing_reference option =
  match peak_to_valley_stats_of asset with
  | None -> None
  | Some p ->
    let ath =
      match range_stats_of asset with
      | Some r -> r.ath
      | None -> p.peak
    in
    let overshoot_p90 = floor_overshoot_p90_of asset in
    Some (sizing_drawdown_of ~ath ~fallback ~overshoot_p90 p)
;;
