(* Survival_portfolio - multi-asset capital model over shared budgets.

   A portfolio is a set of positions. Each position is a (venue, asset) with
   one shared budget pool and one or more subgrids (Grid_core configs) trading
   that asset. All subgrids of a position draw their buy capital from the same
   pool and credit it on sells, which is exactly the "ven-diagram merge"
   semantics: when the pool cannot fund one subgrid's resting buy, every other
   subgrid on that position is starved too, and the position's survival is the
   pool's survival.

   Budgets are spend + recover only (no inflation), matching the live grid:
   - buys:   pool -= qty * price * (1 + maker_fee)
   - sells:  pool += proceeds
   - transfers: manual budget moves between positions at session boundaries
     (user re-allocations), applied before that session's bars.

   Pure: takes OHLC bars per position and runs Grid_core in lockstep. No
   network. *)

open Survival_types

type pool_key =
  { venue : string
  ; asset : string
  }

type transfer =
  { session : int (** Session index at which the transfer is applied. *)
  ; from : pool_key
  ; to_ : pool_key
  ; amount : float
  }

type subgrid =
  { id : string
  ; grid : Dio_strategies.Grid_core.config
  ; state : Dio_strategies.Grid_core.state
  }

type position_input =
  { venue : string
  ; asset : string
  ; pool : float (** Initial shared budget (quote). *)
  ; bars : Survival_types.bar array (** This position's OHLC history. *)
  ; subgrids : Dio_strategies.Grid_core.config list
    (** One config per subgrid; start_price/start_quote are per-subgrid. *)
  }

type aligned_position_input =
  { venue : string
  ; asset : string
  ; pool : float
  ; initial_base : float
  ; bars : Survival_types.bar option array
  ; subgrids : Dio_strategies.Grid_core.config list
  }

type pos_rt =
  { venue : string
  ; asset : string
  ; initial_pool : float
  ; bars : Survival_types.bar option array
  ; pool : float ref
  ; subgrids : subgrid list
  ; buy_fills : int ref
  ; sell_fills : int ref
  ; pool_min_dd : float ref
  ; first_cl_session : int option ref
  ; first_cl_dd : float option ref
  }

type position_outcome =
  { venue : string
  ; asset : string
  ; pool_min_drawdown : float
    (** Worst pool drawdown over the replay: the position-level survival metric
        (all subgrids share one pool). *)
  ; first_exhausted_drawdown : float option
    (** Pool drawdown at the first session a subgrid was capital-low. *)
  ; first_exhausted_session : int option
  ; capital_low : bool
  ; d_surv : float (** [pool_min_drawdown] when exhausted, else 1.0. *)
  ; buy_fills : int
  ; sell_fills : int
  ; final_pool : float
  ; final_base : float
  }

type result =
  { n_sessions : int
  ; positions : position_outcome list
  ; exhausted : bool
  ; first_exhausted_session : int option
  }

let key_of_position (p : position_input) : pool_key = { venue = p.venue; asset = p.asset }

let to_grid_bar (b : Survival_types.bar) : Dio_strategies.Grid_core_types.bar =
  Dio_strategies.Grid_core_types.{ high = b.high; low = b.low; close = b.close }
;;

(** Run the portfolio replay. Each position advances over its own bars; the
    session count is the longest series. Transfers are applied at their
    session before any position trades that session. *)
let simulate_aligned
      ~(timeline : string array)
      ~(positions : aligned_position_input list)
      ~(transfers : transfer list)
      ?(ordering = Dio_strategies.Grid_core_types.Buy_first)
      ()
  : result
  =
  let module G = Dio_strategies.Grid_core in
  let module GT = Dio_strategies.Grid_core_types in
  let n_sessions = Array.length timeline in
  let transfers = List.sort (fun a b -> Int.compare a.session b.session) transfers in
  (* Build position runtime state: a shared pool hook for all its subgrids. *)
  let pools : (string * string, float ref) Hashtbl.t = Hashtbl.create 8 in
  let pool_of (key : pool_key) =
    match Hashtbl.find_opt pools (key.venue, key.asset) with
    | Some r -> r
    | None ->
      let r = ref 0.0 in
      Hashtbl.add pools (key.venue, key.asset) r;
      r
  in
  let position_runtime =
    List.map
      (fun (p : aligned_position_input) ->
         let pool = pool_of { venue = p.venue; asset = p.asset } in
         pool := !pool +. p.pool;
         let hook =
           Some
             { G.balance = (fun () -> !pool)
             ; spend = (fun a -> pool := !pool -. a)
             ; recover = (fun a -> pool := !pool +. a)
             }
         in
         let subgrids =
           List.mapi
             (fun i cfg ->
                let grid = { cfg with G.cash_hook = hook } in
                { id = Printf.sprintf "%s/%s#%d" p.venue p.asset i
                ; grid
                ; state = G.create grid
                })
             p.subgrids
         in
         List.iter
           (fun sg ->
              sg.state.G.base
              <- p.initial_base /. float_of_int (max 1 (List.length subgrids)))
           subgrids;
         { venue = p.venue
         ; asset = p.asset
         ; initial_pool = p.pool
         ; bars = p.bars
         ; pool
         ; subgrids
         ; buy_fills = ref 0
         ; sell_fills = ref 0
         ; pool_min_dd = ref 0.0
         ; first_cl_session = ref None
         ; first_cl_dd = ref None
         })
      positions
  in
  let first_cl_global = ref None in
  let pool_drawdown (pool : float ref) (init : float) =
    if init > 0.0 then 1.0 -. (!pool /. init) else 1.0
  in
  (* Even with no bars at all we run session 0 so that transfers scheduled for
     it are applied. *)
  let n_iter = if n_sessions = 0 then 1 else n_sessions in
  for i = 0 to n_iter - 1 do
    (* Transfers scheduled for this session: move budget between pools. *)
    List.iter
      (fun t ->
         if t.session = i
         then (
           let from_pool = pool_of t.from in
           let to_pool = pool_of t.to_ in
           let amt = Float.max 0.0 (Float.min t.amount (Float.max 0.0 !from_pool)) in
           from_pool := !from_pool -. amt;
           to_pool := !to_pool +. amt;
           List.iter
             (fun (rt : pos_rt) ->
                if rt.pool == from_pool || rt.pool == to_pool
                then
                  rt.pool_min_dd
                  := Float.max !(rt.pool_min_dd) (pool_drawdown rt.pool rt.initial_pool))
             position_runtime))
      transfers;
    (* Advance every position over its own session bar. *)
    List.iter
      (fun (rt : pos_rt) ->
         if i < Array.length rt.bars
         then (
           match rt.bars.(i) with
           | None -> ()
           | Some source_bar ->
             let bar = to_grid_bar source_bar in
             let start_pool = !(rt.pool) in
             let trough = ref start_pool in
             let running = ref start_pool in
             List.iter
               (fun sg ->
                  let fs = G.on_bar sg.grid ~state:sg.state ~bar ~ordering in
                  List.iter
                    (fun f ->
                       running := !running +. f.GT.quote_delta;
                       trough := Float.min !trough !running;
                       if f.GT.side = `Buy then incr rt.buy_fills else incr rt.sell_fills)
                    fs;
                  if sg.state.G.ever_capital_low
                  then if !first_cl_global = None then first_cl_global := Some i)
               rt.subgrids;
             rt.pool_min_dd
             := Float.max
                  !(rt.pool_min_dd)
                  (if rt.initial_pool > 0.0
                   then 1.0 -. (!trough /. rt.initial_pool)
                   else 1.0);
             (* First exhaustion: first session any subgrid of this position was
              capital-low. *)
             if
               !(rt.first_cl_session) = None
               && List.exists (fun sg -> sg.state.G.ever_capital_low) rt.subgrids
             then (
               rt.first_cl_session := Some i;
               rt.first_cl_dd := Some (pool_drawdown rt.pool rt.initial_pool))))
      position_runtime
  done;
  let positions_out =
    List.map
      (fun (rt : pos_rt) ->
         let capital_low = !(rt.first_cl_session) <> None in
         { venue = rt.venue
         ; asset = rt.asset
         ; pool_min_drawdown = !(rt.pool_min_dd)
         ; first_exhausted_drawdown = !(rt.first_cl_dd)
         ; first_exhausted_session = !(rt.first_cl_session)
         ; capital_low
         ; d_surv = (if capital_low then !(rt.pool_min_dd) else 1.0)
         ; buy_fills = !(rt.buy_fills)
         ; sell_fills = !(rt.sell_fills)
         ; final_pool = !(rt.pool)
         ; final_base =
             List.fold_left (fun acc sg -> acc +. sg.state.G.base) 0.0 rt.subgrids
         })
      position_runtime
  in
  { n_sessions
  ; positions = positions_out
  ; exhausted = List.exists (fun (o : position_outcome) -> o.capital_low) positions_out
  ; first_exhausted_session = !first_cl_global
  }
;;

let simulate
      ~(positions : position_input list)
      ~(transfers : transfer list)
      ?(ordering = Dio_strategies.Grid_core_types.Buy_first)
      ()
  : result
  =
  let n_sessions =
    List.fold_left
      (fun acc (p : position_input) -> max acc (Array.length p.bars))
      0
      positions
  in
  let timeline = Array.make n_sessions "" in
  let positions =
    List.map
      (fun (p : position_input) ->
         { venue = p.venue
         ; asset = p.asset
         ; pool = p.pool
         ; initial_base = 0.0
         ; bars = Array.map (fun bar -> Some bar) p.bars
         ; subgrids = p.subgrids
         })
      positions
  in
  simulate_aligned ~timeline ~positions ~transfers ~ordering ()
;;
