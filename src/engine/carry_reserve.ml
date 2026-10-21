(** Per-overlay liquidation reserve for the carry overlays.

    In unified mode the perp liquidation is driven by the total spot USDC collateral, not
    the per-venue initial margin. Each trading entry with an overlay owns its own reserve
    budget: the free USDC that must remain so that, for a chosen liquidation multiple of
    the mark, that entry's short position still covers maintenance. The domain publishes
    its budget as [min_liq_reserve] and its overlay reserves it; the account's total carry
    reservation is the sum of the per-entry budgets. *)

let section = "carry_reserve"

let base_coin_of_symbol symbol =
  match String.split_on_char '/' symbol with
  | b :: _ -> b
  | [] -> symbol
;;

(** Hyperliquid base-tier maintenance rates for the carry assets. Unknown coins use the
    conservative HYPE rate. *)
let maintenance_rate ~coin =
  match String.uppercase_ascii coin with
  | "BTC" -> 0.0125
  | "HYPE" -> 0.05
  | _ -> 0.05
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
