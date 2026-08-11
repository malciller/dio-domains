(* Survival stats - realized volatility, normalized MFD, and the asset/class
   blend.

   No-lookahead invariant: trailing volatility at start [s] uses only closes
   from [s-W, s] (bars <= s). z(s,h) = MFD(s,h) / (sigma_s * sqrt(h)).

   The z-blend reads through [asset_regime_of] (the asset's per-start MFD and
   trailing vol) plus the class's pooled z-CDF (Survival_classes.z_index_of):
   the class contribution is evaluated at each asset start's own vol regime. *)

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

(** Per-start (MFD, trailing-vol) pairs over [warmup, n-horizon-1], shared by
    the z-blend: F_asset stays the raw MFD CDF while the class contribution is
    evaluated at the asset's own vol regime, tau_s(d) = d / (sigma_s * sqrt h).
    A start whose trailing vol is unavailable (or exactly zero, i.e. a flat
    window) gets sigma = 0.0, which maps to tau = +infinity -> class coverage
    1.0. [stride] mirrors Survival_mfd.samples. *)
type asset_regime =
  { mfd : float array
  ; sigma : float array
  ; n : int
  }

let asset_regime_of ~closes ~lows ~horizon ~w ~warmup ?(stride = 1) () : asset_regime =
  let n = Array.length closes in
  let stride = max 1 stride in
  let ms = ref [] in
  let ss = ref [] in
  let s = ref warmup in
  while !s <= n - 1 do
    (match Survival_mfd.mfd ~closes ~lows ~start:!s ~horizon with
     | Some m ->
       ms := m :: !ms;
       let sigma =
         match trailing_vol ~closes ~s:!s ~w with
         | Some v when v > 0.0 -> v
         | _ -> 0.0
       in
       ss := sigma :: !ss
     | None -> ());
    s := !s + stride
  done;
  let mfd = Array.of_list (List.rev !ms) in
  let sigma = Array.of_list (List.rev !ss) in
  { mfd; sigma; n = Array.length mfd }
;;

(** Blend an asset's empirical CDF with its class CDF using kappa:
    F_blend = (n_a * F_asset + kappa * F_class) / (n_a + kappa).
    kappa is a pseudocount of class pseudo-sessions: the class pull is ~
    kappa / n_a and decays naturally as the asset's own history grows. *)
let blend ~n_asset ~asset_f ~kappa ~class_f =
  ((n_asset *. asset_f) +. (kappa *. class_f)) /. (n_asset +. kappa)
;;
