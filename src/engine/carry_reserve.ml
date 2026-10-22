(** Per-overlay liquidation reserve for the carry overlays.

    In unified mode the perp liquidation is driven by the total spot USDC collateral, not
    the per-venue initial margin. Each trading entry with an overlay owns its own reserve
    budget: the free USDC that must remain so that, for a chosen liquidation multiple of
    the mark, that entry's short position still covers maintenance. The domain publishes
    its budget as [min_liq_reserve] and its overlay reserves it; the account's total carry
    reservation is the sum of the per-entry budgets. *)

let section = "carry_reserve"
let base_coin_of_symbol symbol = Config.base_coin_of_symbol symbol

(** Case-insensitive equality without allocating an uppercased copy. *)
let ci_equal a b =
  let n = String.length a in
  n = String.length b
  &&
  let rec go i =
    i >= n || (Char.uppercase_ascii a.[i] = Char.uppercase_ascii b.[i] && go (i + 1))
  in
  go 0
;;

(** Hyperliquid base-tier maintenance rates for the carry assets. Unknown coins use the
    conservative HYPE rate. *)
let maintenance_rate ~coin =
  if ci_equal coin "BTC" then 0.0125 else if ci_equal coin "HYPE" then 0.05 else 0.05
;;

type leg =
  { qty : float (** signed; negative is short *)
  ; entry : float
  ; mark : float
  ; maintenance : float
  }

let pos_leg ~exchange ~coin =
  match Dio_exchange.Market_context.get_position ~exchange ~coin with
  | None -> None
  | Some p ->
    let mark =
      match Dio_exchange.Market_context.get_funding ~exchange ~coin with
      | Some f when f.mark > 0.0 -> f.mark
      | _ -> p.entry
    in
    if Float.is_finite p.qty && Float.is_finite p.entry && p.entry > 0.0 && mark > 0.0
    then Some { qty = p.qty; entry = p.entry; mark; maintenance = maintenance_rate ~coin }
    else None
;;

(** [C_min] for one short at [L = mult * mark]. Each overlay's budget covers only its own
    position, so the per-entry reserves add up to the account's total. Returns [None] when
    the target is not a short. *)
let c_min_for_target ~target ~mult =
  if target.qty >= -0.0000001
  then None
  else (
    let s = -.target.qty in
    let l = mult *. target.mark in
    Some (Float.max 0.0 ((s *. l *. (1.0 +. target.maintenance)) -. (s *. target.entry))))
;;

(** Per-entry reserve for [symbol] on [exchange]: that entry's short position at its own
    [min_liq_mult]. Zero when no carry short is open. *)
let compute ~exchange ~symbol =
  let coin = base_coin_of_symbol symbol in
  match pos_leg ~exchange ~coin with
  | None -> 0.0
  | Some target ->
    let p = Config.get_overlay_params ~exchange ~symbol in
    Option.value (c_min_for_target ~target ~mult:p.min_liq_mult) ~default:0.0
;;

(** As {!compute}, but with the controller's already-resolved coin and liquidation
    multiple. The domain loop knows both for the life of the controller, so the per-cycle
    reserve does no config lookup, string normalization, or symbol split. *)
let compute_with ~exchange ~coin ~min_liq_mult =
  match pos_leg ~exchange ~coin with
  | None -> 0.0
  | Some target -> Option.value (c_min_for_target ~target ~mult:min_liq_mult) ~default:0.0
;;
