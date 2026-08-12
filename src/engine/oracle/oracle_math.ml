(* Oracle math - dependency-free numerical helpers shared by MFD and stats.

   A percentile of an empty sample is indistinguishable from a legitimate
   "zero drawdown" observation, so both percentile estimators raise
   [Invalid_argument] on empty (or zero-total-weight) input instead of
   returning 0.0: an empty distribution must never masquerade as "this asset
   never drew down" or feed false precision into a percentile table. *)

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
