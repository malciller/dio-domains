(* Oracle_reclaim - priority capital reclamation planning.

   When a higher-priority asset cannot fund its first buy after a fill (its
   committed buy was consumed by the fill) while lower-priority assets still
   hold committed buy capital, the account is under-deployed in priority
   order: the lower-priority capital should be reclaimed so the priority
   asset can resume. This module decides WHICH lower-priority orders to
   cancel to close the funding gap - choosing the FEWEST cancellations, and
   among equal-size sets the lowest-priority orders (keeping the
   highest-priority strategies alive).

   The caller feeds each asset's first-buy cost, the pass-down budget it was
   sized against, and its committed buy value (the capital a cancel returns
   to the available pool) in account priority order; this returns the
   [(reclaimed_symbol, funded_target_symbol)] plan. The actual cancellation
   is executed by the domain worker of the reclaimed asset (the oracle only
   plans).

   Each asset's funding gate is evaluated against its OWN pass-down budget
   (the pool remaining after higher-priority assets consumed their share),
   not the raw account pool: a priority asset can be starved of its first
   buy by a fully-funded higher-priority sibling even when the raw account
   pool reads above its first-buy cost, and the plan must still recognize it
   as a target (and a reclaim of its lower-priority capital must fund it).

   Pure: no IO, unit-testable. *)

type reclaim_input =
  { symbol : string
    (** Asset symbol; the input list is in account priority order (highest
          first). *)
  ; first_buy_cost : float
    (** Cost of the first buy at the minimum order size (grid interval =
          config max) - the same funding gate the deployment uses. *)
  ; committed_value : float
    (** Capital locked by this asset's resting buy orders (what a cancel
          returns to the available pool). Any asset holding committed buy
          capital is eligible to be reclaimed - committed capital always
          flows toward the highest-priority asset that needs it. *)
  ; pool : float
    (** The asset's effective pass-down budget - the pool remaining after
          higher-priority assets consumed their sized share. This is the
          budget the asset's first-buy gate was evaluated against (see the
          module doc); the gap is measured from it, not from the raw account
          pool. *)
  ; capital_blocked : bool
    (** The asset is INACTIVE for capital reasons only (its first-buy gate
          failed) and would run once funded. A target must be capital-blocked:
          reclaiming lower-priority ladders for an asset that would stay
          inactive anyway (no usable history, D_surv below the active floor)
          cancels capital for nothing. *)
  }

(** Best covering subset of [xs] for a funding gap [gap]: the FEWEST elements
    whose committed values sum to at least [gap], breaking ties toward the
    lowest-priority elements (largest index in the candidate list, which is
    ordered highest-priority first). Returns the picked mask as a bool array
    in the same order, or [None] when no subset covers. Exact enumeration
    while the candidate set is small (the usual account shape); above the
    enumeration bound it falls back to a greedy cover (largest committed
    values first) that still finds a covering when one exists - it is never
    a silent no-op. *)
let best_subset ~(gap : float) (xs : reclaim_input list) : bool array option =
  let n = List.length xs in
  if n <= 0
  then None
  else if n < 20
  then (
    let arr = Array.of_list xs in
    let best = ref None in
    for mask = 1 to (1 lsl n) - 1 do
      let sum = ref 0.0 in
      let cnt = ref 0 in
      for i = 0 to n - 1 do
        if mask land (1 lsl i) <> 0
        then (
          sum := !sum +. arr.(i).committed_value;
          incr cnt)
      done;
      if !sum +. 1e-9 >= gap
      then (
        let prio = ref 0 in
        for i = 0 to n - 1 do
          if mask land (1 lsl i) <> 0 then prio := !prio + (i + 1)
        done;
        match !best with
        | Some (_, bc, bp) when !cnt > bc || (!cnt = bc && !prio <= bp) -> ()
        | _ -> best := Some (mask, !cnt, !prio))
    done;
    match !best with
    | None -> None
    | Some (mask, _, _) -> Some (Array.init n (fun i -> mask land (1 lsl i) <> 0)))
  else (
    (* Large candidate set: exact enumeration is exponential; greedily pick
       the largest committed values until the gap closes. Always covers when
       a subset exists (it accumulates toward the total), just not the FEWEST
       - an acceptable degradation for an account too large to enumerate. *)
    let picks = Array.make n false in
    let order =
      List.mapi (fun i (c : reclaim_input) -> i, c) xs
      |> List.sort (fun (_, a) (_, b) ->
        Float.compare b.committed_value a.committed_value)
    in
    let sum = ref 0.0 in
    List.iter
      (fun (i, c) ->
         if !sum +. 1e-9 < gap
         then (
           picks.(i) <- true;
           sum := !sum +. c.committed_value))
      order;
    if !sum +. 1e-9 >= gap then Some picks else None)
;;

(** The per-account reclamation plan. [assets] is the account's assets in
    priority order (highest first); each carries the pass-down budget it was
    sized against. Targets are higher-priority, capital-blocked assets with
    no committed buy of their own whose first-buy cost exceeds THEIR budget.
    Each target is funded from its lower-priority committed candidates: the
    plan cancels the FEWEST orders that close the gap (min cardinality, then
    lowest priority), stopping as soon as the target is covered. Released
    capital accumulates across targets (it also funds the next target).
    Returns [(reclaimed, funded_target)] in reclaim order; [] when no
    deallocation can fund anything - the lower-priority assets stay active. *)
let plan (assets : reclaim_input list) : (string * string) list =
  let n = List.length assets in
  let arr = Array.of_list assets in
  let targets =
    List.filter
      (fun (a : reclaim_input) ->
         a.committed_value <= 0.0 && a.pool < a.first_buy_cost && a.capital_blocked)
      assets
  in
  let reclaimed = ref [] in
  let released = ref 0.0 in
  List.iter
    (fun (t : reclaim_input) ->
       let gap = t.first_buy_cost -. t.pool -. !released in
       if gap > 1e-9
       then (
         let idx = ref 0 in
         Array.iteri
           (fun i (a : reclaim_input) -> if a.symbol = t.symbol then idx := i)
           arr;
         let candidates =
           List.filter
             (fun (c : reclaim_input) ->
                c.committed_value > 0.0
                && not (List.exists (fun (s, _) -> s = c.symbol) !reclaimed))
             (Array.to_list (Array.sub arr (!idx + 1) (n - !idx - 1)))
         in
         if candidates <> []
         then (
           match best_subset ~gap candidates with
           | None -> ()
           | Some picks ->
             let picked_val = ref 0.0 in
             List.iteri
               (fun i (c : reclaim_input) ->
                  if picks.(i)
                  then (
                    reclaimed := (c.symbol, t.symbol) :: !reclaimed;
                    picked_val := !picked_val +. c.committed_value))
               candidates;
             released := !released +. !picked_val)))
    targets;
  List.rev !reclaimed
;;
