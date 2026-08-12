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
    the sizing drawdown: the grid must fund the worst peak-to-valley fall the
    asset has really experienced from wherever the price sits today.

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
    else
      Some
        { max_drawdown = Float.min 0.999999 !best_dd
        ; peak = !best_peak
        ; peak_date = !best_peak_date
        ; peak_idx = !best_peak_idx
        ; valley = !best_valley
        ; valley_date = !best_valley_date
        ; valley_idx = !best_valley_idx
        ; price = bars.(n - 1).close
        })
;;
