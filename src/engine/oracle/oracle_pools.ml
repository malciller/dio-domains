(* Pooling, priority & cascades - the pure capital-allocation engine.

   Pools are PER VENUE: one quote pool and one base pool shared by that
   venue's strategies; pools never cross venues (two strategies may share a
   symbol on a venue). Pool == exchange balance: quantities tied up in
   resting orders are part of the pool but unavailable for new allocation;
   placement/fill/cancel moves quantity in and out of availability. This
   module owns only the arithmetic - the caller feeds balances minus tied
   amounts and applies the returned actions to live orders.

   Allocation walks strategies in CONFIG PRESENTATION ORDER (first =
   highest priority), per venue: each strategy is funded iff its need fits
   the remaining availability; unfundable strategies are skipped and their
   capacity passes down. A lower-priority strategy is never starved while
   quote for it exists.

   The cancellation cascade fires per event when a higher-priority need
   cannot fit available quote: lower-priority RESTING BUYS are cancelled -
   many lesser orders may be cancelled to satisfy one greater - until it
   fits. If no combination fits, resolution proceeds to the next-highest
   priority. Every cancelled strategy is re-evaluated on that event and
   resumes iff quote covers its buy. *)

(** A strategy's claim on its venue pools, in config presentation order. *)
type claim =
  { id : string (** Stable strategy identity (venue + symbol + kind). *)
  ; priority : int (** Config presentation order; smaller = higher priority. *)
  ; need_quote : float (** Quote required for the next buy: buy_qty * current. *)
  ; resting_buy_quote : float (** Quote currently tied in resting buys. *)
  }

(** One venue's allocatable quote: exchange balance minus everything tied
    in resting buys across the venue's strategies. *)
type venue_quote =
  { available : float (** Free quote right now. *)
  ; claims : claim list (** All of the venue's strategies, any order. *)
  }

(** Allocation result for one pass over a venue's claims. *)
type allocation =
  { funded_ids : string list
    (** Strategies whose next buy fits (in presentation order). *)
  ; starved_ids : string list
    (** Skipped strategies: need exceeds remaining availability at their
        turn - they stay inactive awaiting reactivation and keep receiving
        computed parameters. *)
  }

let allocate (vq : venue_quote) : allocation =
  let claims = List.sort (fun a b -> compare a.priority b.priority) vq.claims in
  let remaining = ref vq.available in
  let funded, starved =
    List.fold_left
      (fun (funded, starved) (c : claim) ->
         if c.need_quote <= !remaining +. 1e-9 && c.need_quote >= 0.0
         then (
           remaining := !remaining -. c.need_quote;
           c.id :: funded, starved)
         else funded, c.id :: starved)
      ([], [])
      claims
  in
  { funded_ids = List.rev funded; starved_ids = List.rev starved }
;;

(** The cancellation cascade: which resting buys to cancel so [need] fits
    [available]. [trigger_id] is the strategy whose need could not fit - its
    own resting buys are NEVER cancelled (only lower-priority ones are).
    Cancels walk from the LOWEST priority upward; stops as soon as the need
    fits. Returns [] when nothing needs cancelling or no combination of
    others' orders can satisfy the need (the caller then proceeds to the
    next-highest priority). *)
let cascade
      ~(available : float)
      ~(need : float)
      ~(trigger_id : string)
      ~(claims : claim list)
  : string list
  =
  if need <= available +. 1e-9
  then [] (* Already fits: no cascade. *)
  else (
    let deficit = need -. available in
    let candidates =
      List.filter
        (fun (c : claim) ->
           c.resting_buy_quote > 0.0 && not (String.equal c.id trigger_id))
        claims
      |> List.sort (fun a b ->
        compare b.priority a.priority
        (* Lowest priority first; stable on id for determinism. *))
    in
    let rec gather deficit_left acc = function
      | [] -> if deficit_left <= 1e-9 then Some (List.rev acc) else None
      | _ :: _ when deficit_left <= 1e-9 -> Some (List.rev acc)
      | (c : claim) :: rest ->
        gather (deficit_left -. c.resting_buy_quote) (c.id :: acc) rest
    in
    match gather deficit [] candidates with
    | Some cancels -> cancels
    | None -> [] (* No combination fits: give up this round. *))
;;

(** Sell sizing from the venue base pool: base balance minus reserved_base
    (the execution layer's available_trading_balance already excludes it)
    minus base tied in resting sells. Only this value may drive sell
    actions; it is never capital-gated. *)
let sell_qty_of
      ~(base_balance : float)
      ~(reserved_base : float)
      ~(resting_sell_base : float)
  : float
  =
  Float.max 0.0 (base_balance -. reserved_base -. resting_sell_base)
;;
