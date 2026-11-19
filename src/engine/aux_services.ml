(** Off-hot-path overlay services: the reserve-share pool and persistence snapshots.

    Both are cross-book/IO work that does not belong on a trading domain's cycle. The
    trading domain publishes a small input snapshot and reads one atomic; a background
    domain owns the expensive half and is woken by an input delta, never by a poll. *)

let section = "aux_services"

(* Two detached coordination domains, not a per-request fan-out. *)
[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

let same_float a b = a = b || (Float.is_nan a && Float.is_nan b)

(* ------------------------------------------------------------------ *)
(* Reserve share *)
(* ------------------------------------------------------------------ *)

(** The requirement inputs, plus the store generations that move it. Written under
    [reserve_mutex] only when one of them changes, and read under the same mutex by the
    reserve domain, so a snapshot is always consistent. The no-change path is a plain
    comparison: no lock, no allocation. *)
type reserve_input =
  { mutable target : float
  ; mutable pending : float
  ; mutable free : float
  ; mutable positions_at : float
  ; mutable margin_at : float
  }

type reservation =
  { exchange : string
  ; coin : string
  ; key : string
  ; diag_key : string
  ; expected : int
  ; stress : float
  ; maintenance : float
  ; input : reserve_input
  ; share : float Atomic.t
  }

let reservations : reservation list Atomic.t = Atomic.make []
let reserve_mutex = Mutex.create ()
let reserve_wake = Condition.create ()
let reserve_epoch = Atomic.make 0
let reserve_started = Atomic.make false

(** Store generations the requirement depends on. Both are keyed lookups, no allocation. *)
let generations ~exchange =
  let positions_at =
    try
      Option.value
        (Dio_exchange.Market_context.get_positions_updated_at ~exchange)
        ~default:(-1.0)
    with
    | _ -> -1.0
  in
  let margin_at =
    match Dio_exchange.Market_context.get_margin ~exchange with
    | Some m -> m.updated_at
    | None -> -1.0
  in
  positions_at, margin_at
;;

(** Publish this book's inputs and return its share. A delta wakes the reserve domain; an
    unchanged cycle costs two store reads and five float comparisons. *)
let publish_share r ~target ~pending ~free =
  let i = r.input in
  let positions_at, margin_at = generations ~exchange:r.exchange in
  if not
       (same_float i.target target
        && same_float i.pending pending
        && same_float i.free free
        && same_float i.positions_at positions_at
        && same_float i.margin_at margin_at)
  then (
    Mutex.lock reserve_mutex;
    i.target <- target;
    i.pending <- pending;
    i.free <- free;
    i.positions_at <- positions_at;
    i.margin_at <- margin_at;
    Atomic.incr reserve_epoch;
    Condition.signal reserve_wake;
    Mutex.unlock reserve_mutex);
  Atomic.get r.share
;;

let compute_reservation r ~target ~pending ~free =
  let now = Unix.gettimeofday () in
  let exchange = r.exchange in
  let key = r.key in
  let publish_zero () =
    Overlay_reserve.update_diag
      { Overlay_reserve.dg_key = r.diag_key
      ; dg_exchange = exchange
      ; dg_at = Overlay_reserve.live_at
      ; dg_gross = 0.0
      ; dg_sells = 0.0
      ; dg_required = 0.0
      ; dg_margin = 0.0
      };
    0.0
  in
  let funding_mark =
    match Dio_exchange.Market_context.get_funding_key key with
    | Some f when f.mark > 0.0 -> f.mark
    | _ -> 0.0
  in
  let open_short, entry, mark =
    match Overlay_reserve.pos_leg_key ~coin:r.coin ~key with
    | Some leg ->
      ( Float.max 0.0 (-.leg.Overlay_reserve.qty)
      , (if leg.Overlay_reserve.entry > 0.0
         then leg.Overlay_reserve.entry
         else leg.Overlay_reserve.mark)
      , leg.Overlay_reserve.mark )
    | None -> 0.0, funding_mark, funding_mark
  in
  let total_qty =
    if open_short > 1e-9 || pending > 1e-9 then open_short +. pending else target
  in
  if total_qty <= 1e-9 || mark <= 0.0
  then publish_zero ()
  else (
    let position = Dio_exchange.Market_context.get_position_key key in
    let position_leverage =
      match position with
      | Some p -> Option.value p.leverage ~default:0.0
      | None -> 0.0
    in
    let leverage =
      if position_leverage > 0.0
      then position_leverage
      else (
        match Dio_exchange.Exchange_intf.Registry.get_market_context exchange with
        | Some (module Mc : Dio_exchange.Exchange_intf.Market_context.S) ->
          (match Mc.get_risk ~coin:r.coin with
           | Some { Dio_exchange.Exchange_intf.Market_context.max_leverage = Some l; _ }
             -> l
           | _ -> 0.0)
        | None -> 0.0)
    in
    if leverage <= 0.0
    then publish_zero ()
    else (
      let target_leg : Overlay_reserve.leg =
        { qty = -.total_qty
        ; entry = (if entry > 0.0 then entry else mark)
        ; mark
        ; maintenance = r.maintenance
        }
      in
      let req =
        Overlay_reserve.compute_requirement
          ~target:target_leg
          ~max_qty:total_qty
          ~leverage
          ~min_liq_mult:(1.0 +. (r.stress /. 100.0))
          ~anchor:target_leg.entry
          ~max_trail_pct:0.0
          ~next_add:0.0
          ~rungs:0
          ~add_growth:1.0
          ~sells_top:0.0
          ~sells_stress:0.0
      in
      let margin = Dio_exchange.Market_context.get_margin ~exchange in
      let account_margin_used =
        match margin with
        | Some m -> m.margin_used
        | None -> nan
      in
      let position_margin_used =
        match position with
        | Some p -> p.margin_used
        | None -> 0.0
      in
      if not
           (Float.is_finite req.Overlay_reserve.rq_gross
            && Float.is_finite req.Overlay_reserve.rq_required)
      then publish_zero ()
      else (
        Overlay_reserve.update_diag
          { Overlay_reserve.dg_key = r.diag_key
          ; dg_exchange = exchange
          ; dg_at = Overlay_reserve.live_at
          ; dg_gross = req.Overlay_reserve.rq_gross
          ; dg_sells = 0.0
          ; dg_required = req.Overlay_reserve.rq_required
          ; dg_margin = position_margin_used
          };
        if not (Float.is_finite free)
        then 0.0
        else (
          let own_share =
            if Float.is_finite req.Overlay_reserve.rq_required
            then
              Float.min
                free
                (Float.max 0.0 (req.Overlay_reserve.rq_required -. position_margin_used))
            else 0.0
          in
          let pool =
            Overlay_reserve.pool_for
              ~exchange
              ~now
              ~max_age:180.0
              ~margin_used:account_margin_used
              ~free
          in
          if pool.Overlay_reserve.pl_count >= r.expected
          then Overlay_reserve.allocation pool ~gross:req.Overlay_reserve.rq_gross
          else own_share))))
;;

(* ------------------------------------------------------------------ *)
(* Carry controllers *)
(* ------------------------------------------------------------------ *)

(** Everything a controller's requirement is a function of. The controller resolves these
    from the ladder state and the venue; the arithmetic and the pool split run here. *)
type controller_input =
  { target_qty : float
  ; target_entry : float
  ; target_mark : float
  ; maintenance : float
  ; max_qty : float
  ; leverage : float
  ; min_liq_mult : float
  ; anchor : float
  ; max_trail_pct : float
  ; next_add : float
  ; rungs : int
  ; add_growth : float
  ; p_top_guess : float
  ; stress_price : float
  ; position_margin : float
  ; account_margin : float
  }

type controller_reservation =
  { ctl_exchange : string
  ; ctl_diag_key : string
  ; ctl_expected : int
  ; ctl_legs : Config.overlay_leg list
  ; ctl_caches : Overlay_inventory.open_sell_cache array
  ; ctl_quote_free : unit -> float
  ; mutable ctl_input : controller_input
  ; mutable ctl_gen : float * float
  ; mutable ctl_ready : bool
  ; ctl_share : float Atomic.t
  }

let controller_reservations : controller_reservation list Atomic.t = Atomic.make []

let requirement_of_input (i : controller_input) ~sells_top ~sells_stress =
  let target : Overlay_reserve.leg =
    { qty = i.target_qty
    ; entry = i.target_entry
    ; mark = i.target_mark
    ; maintenance = i.maintenance
    }
  in
  Overlay_reserve.compute_requirement
    ~target
    ~max_qty:i.max_qty
    ~leverage:i.leverage
    ~min_liq_mult:i.min_liq_mult
    ~anchor:i.anchor
    ~max_trail_pct:i.max_trail_pct
    ~next_add:i.next_add
    ~rungs:i.rungs
    ~add_growth:i.add_growth
    ~sells_top
    ~sells_stress
;;

(** Resting-sell proceeds under the top and stress prices. This worker owns its own cache
    array: the trading domain's aggregate pass refreshes a different one. *)
let controller_sells r (i : controller_input) =
  let cached idx =
    if idx < Array.length r.ctl_caches
    then Some (Array.unsafe_get r.ctl_caches idx)
    else None
  in
  let i_leg = ref 0 in
  List.fold_left
    (fun (a, b) (l : Config.overlay_leg) ->
      let cache = cached !i_leg in
      incr i_leg;
      if Config.is_hyperliquid l.ol_exchange
      then (
        let x, y =
          Overlay_inventory.open_sell_proceeds_below2
            ?cache
            ~exchange:l.ol_exchange
            ~symbol:l.ol_symbol
            ~price_a:i.p_top_guess
            ~price_b:i.stress_price
            ()
        in
        a +. x, b +. y)
      else a, b)
    (0.0, 0.0)
    r.ctl_legs
;;

let share_of_input r (i : controller_input) =
  let sells_top, sells_stress = controller_sells r i in
  let free =
    try r.ctl_quote_free () with
    | _ -> nan
  in
  let req = requirement_of_input i ~sells_top ~sells_stress in
  let publish_zero () =
    Overlay_reserve.update_diag
      { Overlay_reserve.dg_key = r.ctl_diag_key
      ; dg_exchange = r.ctl_exchange
      ; dg_at = Overlay_reserve.live_at
      ; dg_gross = 0.0
      ; dg_sells = 0.0
      ; dg_required = 0.0
      ; dg_margin = 0.0
      };
    0.0
  in
  if not
       (Float.is_finite req.Overlay_reserve.rq_gross
        && Float.is_finite req.Overlay_reserve.rq_required)
  then publish_zero ()
  else (
    Overlay_reserve.update_diag
      { Overlay_reserve.dg_key = r.ctl_diag_key
      ; dg_exchange = r.ctl_exchange
      ; dg_at = Overlay_reserve.live_at
      ; dg_gross = req.Overlay_reserve.rq_gross
      ; dg_sells = req.Overlay_reserve.rq_sells_applied
      ; dg_required = req.Overlay_reserve.rq_required
      ; dg_margin = i.position_margin
      };
    let own_share =
      if Float.is_finite i.position_margin && Float.is_finite free
      then
        Float.min
          free
          (Float.max 0.0 (req.Overlay_reserve.rq_required -. i.position_margin))
      else 0.0
    in
    let pool =
      Overlay_reserve.pool_for
        ~exchange:r.ctl_exchange
        ~now:(Unix.gettimeofday ())
        ~max_age:180.0
        ~margin_used:i.account_margin
        ~free
    in
    if pool.Overlay_reserve.pl_count >= r.ctl_expected
    then Overlay_reserve.allocation pool ~gross:req.Overlay_reserve.rq_gross
    else own_share)
;;

(** Publish the controller's inputs and return its share. The first call computes inline
    so the first cycle after a restart does not size against a zero reserve; every later
    cycle reads the atomic unless an input moved. *)
let publish_controller
  r
  ~target_qty
  ~target_entry
  ~target_mark
  ~maintenance
  ~max_qty
  ~leverage
  ~min_liq_mult
  ~anchor
  ~max_trail_pct
  ~next_add
  ~rungs
  ~add_growth
  ~p_top_guess
  ~stress_price
  ~position_margin
  ~account_margin
  =
  let i = r.ctl_input in
  let first = not r.ctl_ready in
  (* A balance can move on a clearinghouse snapshot that leaves every published field
     alone, so the store generations count as inputs too. *)
  let gen = generations ~exchange:r.ctl_exchange in
  let changed =
    first
    || gen <> r.ctl_gen
    || not
         (same_float i.target_qty target_qty
          && same_float i.target_entry target_entry
          && same_float i.target_mark target_mark
          && same_float i.maintenance maintenance
          && same_float i.max_qty max_qty
          && same_float i.leverage leverage
          && same_float i.min_liq_mult min_liq_mult
          && same_float i.anchor anchor
          && same_float i.max_trail_pct max_trail_pct
          && same_float i.next_add next_add
          && i.rungs = rungs
          && same_float i.add_growth add_growth
          && same_float i.p_top_guess p_top_guess
          && same_float i.stress_price stress_price
          && same_float i.position_margin position_margin
          && same_float i.account_margin account_margin)
  in
  if changed
  then (
    Mutex.lock reserve_mutex;
    r.ctl_input
    <- { target_qty
       ; target_entry
       ; target_mark
       ; maintenance
       ; max_qty
       ; leverage
       ; min_liq_mult
       ; anchor
       ; max_trail_pct
       ; next_add
       ; rungs
       ; add_growth
       ; p_top_guess
       ; stress_price
       ; position_margin
       ; account_margin
       };
    r.ctl_gen <- gen;
    r.ctl_ready <- true;
    Atomic.incr reserve_epoch;
    Condition.signal reserve_wake;
    Mutex.unlock reserve_mutex);
  if first
  then (
    let share = share_of_input r r.ctl_input in
    Atomic.set r.ctl_share share;
    share)
  else Atomic.get r.ctl_share
;;

(** Sleeps until an input moves. A pass that changes the pool recomputes every book once
    more before sleeping, so a book's share never waits on another book's next event. *)
let start_reserve_service () =
  if Atomic.compare_and_set reserve_started false true
  then (
    Logging.info_f ~section "reserve domain started";
    ignore
      (Domain.spawn (fun () ->
         Thread_affinity.set_self_other ();
         Thread_affinity.pin_self_background ();
         Gc_config.apply ();
         let seen_epoch = ref 0 in
         let seen_gen = ref (Overlay_reserve.diagnostics_generation ()) in
         let backlog = ref false in
         let rec loop () =
           Mutex.lock reserve_mutex;
           let epoch = Atomic.get reserve_epoch in
           if epoch = !seen_epoch && not !backlog
           then (
             Condition.wait reserve_wake reserve_mutex;
             Mutex.unlock reserve_mutex)
           else (
             let work =
               List.map
                 (fun r ->
                   let i = r.input in
                   r, i.target, i.pending, i.free)
                 (Atomic.get reservations)
             in
             let ctl_work =
               List.filter_map
                 (fun r -> if r.ctl_ready then Some (r, r.ctl_input) else None)
                 (Atomic.get controller_reservations)
             in
             seen_epoch := epoch;
             let extra = !backlog in
             backlog := false;
             Mutex.unlock reserve_mutex;
             List.iter
               (fun (r, target, pending, free) ->
                 (* One bad book must not kill this domain: its share would then freeze at
                    the last value for the rest of the process. *)
                 try
                   Atomic.set r.share (compute_reservation r ~target ~pending ~free)
                 with
                 | exn ->
                   Logging.error_f
                     ~section
                     "reserve compute failed for %s: %s (keeping the last share)"
                     r.diag_key
                     (Printexc.to_string exn))
               work;
             List.iter
               (fun (r, input) ->
                 try Atomic.set r.ctl_share (share_of_input r input) with
                 | exn ->
                   Logging.error_f
                     ~section
                     "reserve compute failed for %s: %s (keeping the last share)"
                     r.ctl_diag_key
                     (Printexc.to_string exn))
               ctl_work;
             let gen = Overlay_reserve.diagnostics_generation () in
             if gen <> !seen_gen
             then (
               seen_gen := gen;
               (* One extra pass when this pass moved the pool. Never re-arm it: a
                  diagnostic that keeps changing would spin this domain forever. *)
               if not extra then backlog := true));
           loop ()
         in
         loop ())))
;;

let register_reservation
  ~exchange
  ~symbol
  ~coin
  ~key
  ~expected
  ~stress
  ~maintenance
  ~free
  ~target
  ~pending
  =
  let r =
    { exchange
    ; coin
    ; key
    ; diag_key = Overlay_reserve.diag_key ~exchange ~symbol
    ; expected
    ; stress
    ; maintenance
    ; input = { target; pending; free; positions_at = -1.0; margin_at = -1.0 }
    ; share = Atomic.make 0.0
    }
  in
  (* One inline pass so a restart with a live short does not open its first cycle with a
     zero share. *)
  Atomic.set r.share (compute_reservation r ~target ~pending ~free);
  Mutex.lock reserve_mutex;
  let rec add () =
    let old = Atomic.get reservations in
    if not (Atomic.compare_and_set reservations old (r :: old)) then add ()
  in
  add ();
  Atomic.incr reserve_epoch;
  Condition.signal reserve_wake;
  Mutex.unlock reserve_mutex;
  start_reserve_service ();
  r
;;

let register_controller ~exchange ~diag_key ~expected ~legs ~caches ~quote_free =
  let r =
    { ctl_exchange = exchange
    ; ctl_diag_key = diag_key
    ; ctl_expected = expected
    ; ctl_legs = legs
    ; ctl_caches = caches
    ; ctl_quote_free = quote_free
    ; ctl_input =
        { target_qty = 0.0
        ; target_entry = 0.0
        ; target_mark = 0.0
        ; maintenance = 0.0
        ; max_qty = 0.0
        ; leverage = 0.0
        ; min_liq_mult = 0.0
        ; anchor = 0.0
        ; max_trail_pct = 0.0
        ; next_add = 0.0
        ; rungs = 0
        ; add_growth = 1.0
        ; p_top_guess = 0.0
        ; stress_price = 0.0
        ; position_margin = nan
        ; account_margin = nan
        }
    ; ctl_gen = -1.0, -1.0
    ; ctl_ready = false
    ; ctl_share = Atomic.make nan
    }
  in
  Mutex.lock reserve_mutex;
  let rec add () =
    let old = Atomic.get controller_reservations in
    if not (Atomic.compare_and_set controller_reservations old (r :: old)) then add ()
  in
  add ();
  Mutex.unlock reserve_mutex;
  start_reserve_service ();
  r
;;

(* ------------------------------------------------------------------ *)
(* Persistence snapshots *)
(* ------------------------------------------------------------------ *)

type save_job =
  { owner_exchange : string
  ; owner_symbol : string
  ; id : string
  ; names : string array
  ; values : Dio_strategies.Strategy_expr.value array
  ; previous : Yojson.Basic.t ref
  }

let save_jobs : save_job Queue.t = Queue.create ()

(** One-shot work handed to the maintenance domain: JSON trace snapshots and other
    callbacks that must never run I/O on a trading domain. Bounded so a slow disk cannot
    grow the queue without limit; the oldest entry is dropped and counted. *)
let task_jobs : (string * (unit -> unit)) Queue.t = Queue.create ()

let task_capacity = 8
let tasks_dropped = Atomic.make 0
let tasks_dropped_count () = Atomic.get tasks_dropped
let save_mutex = Mutex.create ()
let save_wake = Condition.create ()
let save_started = Atomic.make false

(** Non-persistent work multiplexed onto the same low-priority background domain. The
    registry is a callback list so this module never depends on its users. *)
let maintenance_tasks : (string * (unit -> unit)) list Atomic.t = Atomic.make []

(** Bumped by [signal_maintenance]; the worker compares it under [save_mutex] so a
    notification that lands just before [Condition.wait] cannot be lost. *)
let maintenance_epoch = Atomic.make 0

let save_one (j : save_job) =
  let json =
    `Assoc
      (Array.to_list
         (Array.map2
            (fun name v -> name, Dio_strategies.Overlay_instance_runtime.json_of_value v)
            j.names
            j.values))
  in
  if json <> !(j.previous)
  then (
    j.previous := json;
    Dio_persistence.Overlay_instance_store.save_async
      ~owner_exchange:j.owner_exchange
      ~owner_symbol:j.owner_symbol
      ~id:j.id
      json)
;;

let start_save_service () =
  if Atomic.compare_and_set save_started false true
  then (
    Logging.info_f ~section "maintenance domain started";
    ignore
      (Domain.spawn (fun () ->
         Thread_affinity.set_self_other ();
         Thread_affinity.pin_self_background ();
         Gc_config.apply ();
         let seen_maintenance = ref (-1) in
         while true do
           Mutex.lock save_mutex;
           let has_task = not (Queue.is_empty task_jobs) in
           let has_save = not (Queue.is_empty save_jobs) in
           let has_jobs = has_task || has_save in
           let epoch = Atomic.get maintenance_epoch in
           if (not has_jobs) && epoch = !seen_maintenance
           then (
             Condition.wait save_wake save_mutex;
             Mutex.unlock save_mutex)
           else (
             let task = if has_task then Some (Queue.pop task_jobs) else None in
             let job = if has_save then Some (Queue.pop save_jobs) else None in
             seen_maintenance := epoch;
             Mutex.unlock save_mutex;
             (* Maintenance first: a slow persistence write must not delay a risk
                snapshot. One save job per pass keeps the two interleaved. *)
             List.iter
               (fun (name, run) ->
                 try run () with
                 | exn ->
                   Logging.error_f
                     ~section
                     "maintenance task '%s' failed: %s"
                     name
                     (Printexc.to_string exn))
               (Atomic.get maintenance_tasks);
             (match task with
              | Some (name, f) ->
                (try f () with
                 | exn ->
                   Logging.error_f
                     ~section
                     "background task '%s' failed: %s"
                     name
                     (Printexc.to_string exn))
              | None -> ());
             match job with
             | Some j -> save_one j
             | None -> ())
         done)))
;;

(** Wake the shared maintenance domain. Safe from any domain; the epoch closes the
    check-then-wait race. *)
let signal_maintenance () =
  Atomic.incr maintenance_epoch;
  Mutex.lock save_mutex;
  Condition.signal save_wake;
  Mutex.unlock save_mutex
;;

let register_maintenance ~name ~run =
  let rec add () =
    let old = Atomic.get maintenance_tasks in
    if List.mem_assoc name old
    then ()
    else if Atomic.compare_and_set maintenance_tasks old ((name, run) :: old)
    then ()
    else add ()
  in
  add ();
  start_save_service ();
  signal_maintenance ()
;;

let enqueue_save ~owner_exchange ~owner_symbol ~id ~names ~values ~previous =
  Mutex.lock save_mutex;
  Queue.push { owner_exchange; owner_symbol; id; names; values; previous } save_jobs;
  Condition.signal save_wake;
  Mutex.unlock save_mutex;
  start_save_service ()
;;

(** Hand one-shot work to the maintenance domain. [name] identifies the task in an error
    log. Called from a trading domain, so it must stay a queue push and a signal. *)
let enqueue_background ~name f =
  Mutex.lock save_mutex;
  if Queue.length task_jobs >= task_capacity
  then (
    ignore (Queue.pop task_jobs);
    ignore (Atomic.fetch_and_add tasks_dropped 1));
  Queue.push (name, f) task_jobs;
  Condition.signal save_wake;
  Mutex.unlock save_mutex;
  start_save_service ()
;;

(** Start both workers. Called once from the main domain at engine startup; the lazy
    starts stay as a safety net for tests and embedded callers. *)
let start () =
  start_save_service ();
  start_reserve_service ()
;;
