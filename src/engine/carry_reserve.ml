(** Portfolio-level liquidation reserve for the carry overlays.

    In unified mode the perp liquidation is driven by the total spot USDC collateral, not
    the per-venue initial margin. The reserve is the free USDC that must remain so that,
    for a chosen liquidation multiple of the mark, the account equity still covers
    maintenance if one carry position is shocked and the others stay at their current
    marks.

    The reserve is computed once per exchange and assigned to the first configured overlay
    owner; the other overlays reserve zero for perp collateral. *)

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

(** [C_min] for a short target [i] at [L_i = mult_i * mark_i], with every other open carry
    position held at its current mark. Returns [None] when target [i] is not a short. *)
let c_min_for_target ~target ~others ~mult =
  if target.qty >= -0.0000001
  then None
  else (
    let s = -.target.qty in
    let l = mult *. target.mark in
    let other_pnl =
      List.fold_left (fun acc l -> acc +. (l.qty *. (l.mark -. l.entry))) 0.0 others
    in
    let other_maint =
      List.fold_left
        (fun acc l -> acc +. (Float.abs l.qty *. l.mark *. l.maintenance))
        0.0
        others
    in
    Some
      (Float.max
         0.0
         ((s *. l *. (1.0 +. target.maintenance))
          -. (s *. target.entry)
          -. other_pnl
          +. other_maint)))
;;

(** Portfolio reserve for [exchange]: the max single-position-shock [C_min] over the
    configured carry overlays. Zero when no carry short is open. *)
let compute ~exchange =
  let entries = Config.get_overlay_entries ~exchange in
  let legs =
    List.filter_map
      (fun (e : Config.overlay_entry) ->
        let coin = base_coin_of_symbol e.oe_symbol in
        match pos_leg ~exchange ~coin with
        | Some leg -> Some (leg, e.oe_params.min_liq_mult)
        | None -> None)
      entries
  in
  let rec loop best = function
    | [] -> best
    | (target, mult) :: rest ->
      let others =
        List.filter_map (fun (leg, _) -> if leg == target then None else Some leg) legs
        |> List.map (fun leg -> leg)
      in
      let best =
        match c_min_for_target ~target ~others ~mult with
        | Some c -> Float.max best c
        | None -> best
      in
      loop best rest
  in
  loop 0.0 legs
;;
