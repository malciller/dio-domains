(* Survival math - dependency-free numerical helpers shared by MFD and stats.

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
      "Survival_math.percentile: empty distribution (no samples); refusing to fabricate \
       a percentile"
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

(** Percentile of a weighted empirical distribution: the smallest value [v]
    whose cumulative weight reaches [p]/100 of the total. Weights must be
    positive; values are sorted internally. Used to invert the blended
    (asset + kappa-weighted class) CDF. *)
let weighted_percentile (pairs : (float * float) array) p =
  if Array.length pairs = 0
  then
    invalid_arg
      "Survival_math.weighted_percentile: empty distribution (no samples); refusing to \
       fabricate a percentile"
  else (
    let arr = Array.copy pairs in
    Array.sort (fun (a, _) (b, _) -> Float.compare a b) arr;
    let total = Array.fold_left (fun acc (_, w) -> acc +. w) 0.0 arr in
    if total <= 0.0
    then
      invalid_arg
        "Survival_math.weighted_percentile: zero total weight; refusing to fabricate a \
         percentile"
    else (
      let target = p /. 100.0 *. total in
      let acc = ref 0.0 in
      let found = ref None in
      Array.iter
        (fun (v, w) ->
           if !found = None
           then (
             acc := !acc +. w;
             if !acc >= target then found := Some v))
        arr;
      Option.value !found ~default:(fst arr.(Array.length arr - 1))))
;;
