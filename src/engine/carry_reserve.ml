(** Account-level carry reserve for the unified short book.

    Each controller publishes its gross requirement: initial margin at the terminal ladder
    rung plus the ladder's unrealized loss there, maxed with the maintenance-stress leg.
    The pool nets the gross requirements against same-venue resting sells and the posted
    perp margin, then caps at free quote. The domain allocates the residual; the grid
    sizes buys from the remainder. The overlay's add gate uses the next rung's initial
    margin. *)

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

(** One overlay's gross requirement at the top of its ladder.

    [p_top] is the terminal rung, so the loss uses the average entry after the remaining
    rungs fill, not the live entry on the whole max size. [sells_top]/[sells_stress] are
    same-venue resting-sell proceeds that fill before each price; the caller nets posted
    margin and caps at free quote. *)
type requirement =
  { rq_short_qty : float
  ; rq_max_qty : float
  ; rq_p_top : float
  ; rq_entry : float
  ; rq_entry_max : float
  ; rq_leverage : float
  ; rq_im_top : float
  ; rq_loss_top : float
  ; rq_stress_price : float
  ; rq_mm_stress : float
  ; rq_loss_stress : float
  ; rq_sells_top : float
  ; rq_sells_stress : float
  ; rq_sells_applied : float
  ; rq_gross_top : float
  ; rq_gross_stress : float
  ; rq_gross : float
  ; rq_required_top : float
  ; rq_required_stress : float
  ; rq_required : float
  }

let no_requirement =
  { rq_short_qty = 0.0
  ; rq_max_qty = 0.0
  ; rq_p_top = 0.0
  ; rq_entry = 0.0
  ; rq_entry_max = 0.0
  ; rq_leverage = 0.0
  ; rq_im_top = 0.0
  ; rq_loss_top = 0.0
  ; rq_stress_price = 0.0
  ; rq_mm_stress = 0.0
  ; rq_loss_stress = 0.0
  ; rq_sells_top = 0.0
  ; rq_sells_stress = 0.0
  ; rq_sells_applied = 0.0
  ; rq_gross_top = 0.0
  ; rq_gross_stress = 0.0
  ; rq_gross = 0.0
  ; rq_required_top = 0.0
  ; rq_required_stress = 0.0
  ; rq_required = 0.0
  }
;;

(** Average entry at the top of the ladder: the live short plus the remaining rungs at
    their derived prices, weighted by the geometric add sizes. *)
let laddered_entry ~(target : leg) ~total_qty ~anchor ~p_top ~next_add ~rungs ~add_growth =
  let short = -.target.qty in
  let remaining = Float.max 0.0 (total_qty -. short) in
  let e_live =
    if target.entry > 0.0 then target.entry else if anchor > 0.0 then anchor else p_top
  in
  if remaining <= 1e-12 || rungs <= 0 || p_top <= 0.0
  then e_live
  else (
    let growth =
      if Float.is_finite add_growth && add_growth > 0.0 then add_growth else 1.0
    in
    let first = if next_add > 0.0 && next_add < p_top then next_add else p_top in
    let step = if rungs > 1 then (p_top -. first) /. float_of_int (rungs - 1) else 0.0 in
    let sum_w = ref 0.0 in
    let sum_wp = ref 0.0 in
    let weight = ref 1.0 in
    for k = 0 to rungs - 1 do
      let price = first +. (float_of_int k *. step) in
      sum_w := !sum_w +. !weight;
      sum_wp := !sum_wp +. (!weight *. price);
      let next_weight = !weight *. growth in
      if Float.is_finite next_weight then weight := next_weight
    done;
    let e_add = if !sum_w > 0.0 then !sum_wp /. !sum_w else p_top in
    ((short *. e_live) +. (remaining *. e_add)) /. total_qty)
;;

(** [max_trail_pct] is the trail from [anchor] to the terminal rung; [next_add]/[rungs]/
    [add_growth] describe the remaining rungs. Flat or unpriceable inputs yield
    {!no_requirement}. *)
let compute_requirement
  ~(target : leg)
  ~max_qty
  ~leverage
  ~min_liq_mult
  ~anchor
  ~max_trail_pct
  ~next_add
  ~rungs
  ~add_growth
  ~sells_top
  ~sells_stress
  =
  let short = -.target.qty in
  let mark = if target.mark > 0.0 then target.mark else target.entry in
  let anchor = if anchor > 0.0 then anchor else mark in
  let p_top =
    if anchor > 0.0 && Float.is_finite max_trail_pct
    then anchor *. (1.0 +. (max_trail_pct /. 100.0))
    else mark
  in
  (* The ladder's terminal rung is the target while it is still adding, but a market that
     has already run through it must be covered at the live mark, not the stale schedule. *)
  let p_top = if mark > 0.0 && mark > p_top then mark else p_top in
  if short <= 1e-9 || leverage <= 0.0 || p_top <= 0.0 || not (Float.is_finite p_top)
  then no_requirement
  else (
    let stress_price = if min_liq_mult > 0.0 then min_liq_mult *. p_top else p_top in
    let qty = Float.max max_qty short in
    let entry =
      if target.entry > 0.0 then target.entry else if anchor > 0.0 then anchor else p_top
    in
    let entry_max =
      laddered_entry ~target ~total_qty:qty ~anchor ~p_top ~next_add ~rungs ~add_growth
    in
    let im_top = qty *. p_top /. leverage in
    let loss_top = qty *. Float.max 0.0 (p_top -. entry_max) in
    let mm_stress = qty *. stress_price *. target.maintenance in
    let loss_stress = qty *. Float.max 0.0 (stress_price -. entry_max) in
    let sells_top = Float.max 0.0 sells_top in
    let sells_stress = Float.max 0.0 sells_stress in
    let gross_top = im_top +. loss_top in
    let gross_stress = mm_stress +. loss_stress in
    let required_top = Float.max 0.0 (gross_top -. sells_top) in
    let required_stress = Float.max 0.0 (gross_stress -. sells_stress) in
    let sells_applied =
      if required_top >= required_stress then sells_top else sells_stress
    in
    { rq_short_qty = short
    ; rq_max_qty = max_qty
    ; rq_p_top = p_top
    ; rq_entry = entry
    ; rq_entry_max = entry_max
    ; rq_leverage = leverage
    ; rq_im_top = im_top
    ; rq_loss_top = loss_top
    ; rq_stress_price = stress_price
    ; rq_mm_stress = mm_stress
    ; rq_loss_stress = loss_stress
    ; rq_sells_top = sells_top
    ; rq_sells_stress = sells_stress
    ; rq_sells_applied = sells_applied
    ; rq_gross_top = gross_top
    ; rq_gross_stress = gross_stress
    ; rq_gross = Float.max gross_top gross_stress
    ; rq_required_top = required_top
    ; rq_required_stress = required_stress
    ; rq_required = Float.max required_top required_stress
    })
;;

(** One controller's last requirement, shared with the account pool. *)
type diag =
  { dg_key : string
  ; dg_exchange : string
  ; dg_at : float
  ; dg_gross : float
  ; dg_sells : float
  ; dg_required : float
  ; dg_margin : float
  }

let diagnostics : (string * diag) list Atomic.t = Atomic.make []
let diagnostics_gen = Atomic.make 0
let diag_key ~exchange ~symbol = exchange ^ "|" ^ String.uppercase_ascii symbol

(* [dg_at] refreshes every cycle, so it must not count as a change: the generation is the
   pool cache's invalidation key and a timestamp-only write has nothing new to publish. *)
let same_requirement (a : diag) (b : diag) =
  a.dg_exchange = b.dg_exchange
  && a.dg_gross = b.dg_gross
  && a.dg_sells = b.dg_sells
  && a.dg_required = b.dg_required
  && a.dg_margin = b.dg_margin
;;

let update_diag (d : diag) =
  let rec loop () =
    let old = Atomic.get diagnostics in
    match List.assoc_opt d.dg_key old with
    | Some prev when same_requirement prev d ->
      let next =
        (d.dg_key, { prev with dg_at = d.dg_at }) :: List.remove_assoc d.dg_key old
      in
      if not (Atomic.compare_and_set diagnostics old next) then loop ()
    | _ ->
      let next = (d.dg_key, d) :: List.remove_assoc d.dg_key old in
      if Atomic.compare_and_set diagnostics old next
      then Atomic.incr diagnostics_gen
      else loop ()
  in
  loop ()
;;

let diag_snapshot ~exchange ~now ~max_age =
  List.filter_map
    (fun (_, d) ->
      if String.equal d.dg_exchange exchange && now -. d.dg_at <= max_age
      then Some d
      else None)
    (Atomic.get diagnostics)
;;

(** Account roll-up. [dg_gross] is before sells, [dg_sells] is the winning leg's credit,
    [margin_used] is posted perp margin (not spot order holds), and the reserve is the
    shared shortfall capped at free quote. [pl_own_net] is the no-sharing view. *)
type pool =
  { pl_count : int
  ; pl_gross : float
  ; pl_sells : float
  ; pl_margin : float
  ; pl_required : float
  ; pl_own_net : float
  ; pl_reserve : float
  }

let pool_of ~margin_used ~free diags =
  let fold f init = List.fold_left (fun acc d -> acc +. f d) init diags in
  let gross = fold (fun d -> d.dg_gross) 0.0 in
  let sells = fold (fun d -> d.dg_sells) 0.0 in
  let required = fold (fun d -> d.dg_required) 0.0 in
  let own_net = fold (fun d -> Float.max 0.0 (d.dg_required -. d.dg_margin)) 0.0 in
  let shared = Float.max 0.0 (gross -. sells -. margin_used) in
  let reserve = if Float.is_finite free then Float.min free shared else shared in
  { pl_count = List.length diags
  ; pl_gross = gross
  ; pl_sells = sells
  ; pl_margin = margin_used
  ; pl_required = required
  ; pl_own_net = own_net
  ; pl_reserve = reserve
  }
;;

(** Split the account reserve in proportion to gross requirement; shares sum to
    [pl_reserve]. *)
let allocation (p : pool) ~gross =
  if p.pl_gross > 0.0 then p.pl_reserve *. (gross /. p.pl_gross) else 0.0
;;

(** One account roll-up per controller generation, shared by every controller on the venue
    so their split comes from the same diag set. [free] is caller-scoped (each grid venue
    caps its own quote), so it is part of the key; the TTL bounds how long a cached pool
    can outlive the [max_age] filter. *)
type pool_snapshot =
  { ps_exchange : string
  ; ps_gen : int
  ; ps_at : float
  ; ps_free : float
  ; ps_pool : pool
  }

let pool_cache : pool_snapshot option Atomic.t = Atomic.make None
let pool_ttl_s = 5.0

let pool_for ~exchange ~now ~max_age ~margin_used ~free =
  let gen = Atomic.get diagnostics_gen in
  match Atomic.get pool_cache with
  | Some ps
    when String.equal ps.ps_exchange exchange
         && ps.ps_gen = gen
         && Float.equal ps.ps_free free
         && now -. ps.ps_at <= pool_ttl_s -> ps.ps_pool
  | _ ->
    let diags = diag_snapshot ~exchange ~now ~max_age in
    let margin_used =
      if Float.is_finite margin_used
      then margin_used
      else List.fold_left (fun acc d -> acc +. d.dg_margin) 0.0 diags
    in
    let pool = pool_of ~margin_used ~free diags in
    Atomic.set
      pool_cache
      (Some
         { ps_exchange = exchange
         ; ps_gen = gen
         ; ps_at = now
         ; ps_free = free
         ; ps_pool = pool
         });
    pool
;;
