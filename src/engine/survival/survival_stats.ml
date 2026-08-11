(* Survival stats - realized volatility, normalized MFD, and the asset/class
   blend.

   No-lookahead invariant: trailing volatility at start [s] uses only closes
   from [s-W, s] (bars <= s). z(s,h) = MFD(s,h) / (sigma_s * sqrt(h)). *)

let percentile = Survival_math.percentile
let std = Survival_math.std

(** Realized per-session volatility at session [s] over the trailing [w]-session
    window: std of log-returns of closes [s-w .. s]. Needs s >= w. *)
let trailing_vol ~closes ~s ~w =
  let n = Array.length closes in
  if n = 0 || s < 0 || s >= n || s < w || w < 1
  then None
  else (
    let rets = Array.make w 0.0 in
    for i = 1 to w do
      let prev = closes.(s - w + i - 1) in
      let cur = closes.(s - w + i) in
      if prev > 0.0 && cur > 0.0
      then rets.(i - 1) <- log (cur /. prev)
      else rets.(i - 1) <- 0.0
    done;
    Some (std rets))
;;

(** Volatility-normalized MFD at session [s] over [horizon], using the trailing
    [w]-session window (no lookahead). *)
let z_mfd ~closes ~lows ~s ~horizon ~w =
  match Survival_mfd.mfd ~closes ~lows ~start:s ~horizon with
  | None -> None
  | Some m ->
    (match trailing_vol ~closes ~s ~w with
     | Some v when v > 0.0 -> Some (m /. (v *. sqrt (float_of_int horizon)))
     | _ -> None)
;;

(** Empirical CDF of z over all valid starts. *)
let z_f_h ~closes ~lows ~horizon ~threshold ~w ~warmup =
  let n = Array.length closes in
  let hits = ref 0 in
  let total = ref 0 in
  for s = warmup to n - 1 do
    match z_mfd ~closes ~lows ~s ~horizon ~w with
    | Some z ->
      incr total;
      if z <= threshold then incr hits
    | None -> ()
  done;
  if !total = 0 then 0.0 else float_of_int !hits /. float_of_int !total
;;

(** Blend an asset's empirical CDF with its class CDF using kappa:
    F_blend = (n_a * F_asset + kappa * F_class) / (n_a + kappa). *)
let blend ~n_asset ~asset_f ~kappa ~class_f =
  ((n_asset *. asset_f) +. (kappa *. class_f)) /. (n_asset +. kappa)
;;
