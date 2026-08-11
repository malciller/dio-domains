(* Oracle_portfolio - multi-asset capital model over per-venue budgets.

   The top level of the capital survival model is the venue: capital is pooled
   per venue (account: venue + quote + testnet), never per asset and never at
   the whole-system level. Every position on the same venue draws its buy
   capital from one shared venue pool and credits it on sells, which is the
   venue-locked runway semantics: quote that exists on an exchange cannot fund
   positions on another exchange, so each venue is an independent runway. When
   the pool cannot fund one subgrid's resting buy, every other subgrid on that
   venue is starved too, and the venue's survival is the pool's survival.

   Budgets are spend + recover only (no inflation), matching the live grid:
   - buys:   pool -= qty * price * (1 + maker_fee)
   - sells:  pool += proceeds
   - transfers: manual budget moves between venue pools at session boundaries
     (user re-allocations), applied before that session's bars.

   Pure: takes OHLC bars per position and runs Grid_core in lockstep. No
   network. *)

open Oracle_types

(** Identity of a venue account pool. Positions on the same account share one
    pool. *)
type pool_key =
  { venue : string
  ; quote : string
  ; testnet : bool
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
  ; quote : string
  ; testnet : bool
  ; pool : float
    (** This position's share of the venue pool; all shares on a venue are
        summed into one pool before the replay. *)
  ; bars : Oracle_types.bar array (** This position's OHLC history. *)
  ; subgrids : Dio_strategies.Grid_core.config list
    (** One config per subgrid; start_price/start_quote are per-subgrid. *)
  }

type aligned_position_input =
  { venue : string
  ; asset : string
  ; quote : string
  ; testnet : bool
  ; pool : float
  ; initial_base : float
  ; bars : Oracle_types.bar option array
  ; subgrids : Dio_strategies.Grid_core.config list
  }

type pos_rt =
  { key : pool_key
  ; asset : string
  ; initial_pool : float (** The venue pool at session 0, shared by all positions. *)
  ; bars : Oracle_types.bar option array
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
    (** Worst drawdown of the shared venue pool over the replay: the
        position-level survival metric (all positions on the venue share one
        pool). *)
  ; first_exhausted_drawdown : float option
    (** Venue pool drawdown at the first session a subgrid was capital-low. *)
  ; first_exhausted_session : int option
  ; capital_low : bool
  ; d_surv : float (** [pool_min_drawdown] when exhausted, else 1.0. *)
  ; buy_fills : int
  ; sell_fills : int
  ; final_pool : float (** The shared venue pool at the end of the replay. *)
  ; final_base : float
  }

(** Venue-level headline: one row per venue account pool. *)
type venue_outcome =
  { venue : string
  ; quote : string
  ; testnet : bool
  ; assets : string list
  ; initial_pool : float
  ; final_pool : float
  ; pool_min_drawdown : float
  ; first_exhausted_drawdown : float option
  ; first_exhausted_session : int option
  ; capital_low : bool
  ; d_surv : float
  ; buy_fills : int
  ; sell_fills : int
  ; final_base : float
  }

type result =
  { n_sessions : int
  ; positions : position_outcome list
  ; venues : venue_outcome list
  ; exhausted : bool
  ; first_exhausted_session : int option
  }

let key_of_position (p : aligned_position_input) : pool_key =
  { venue = p.venue; quote = p.quote; testnet = p.testnet }
;;

let to_grid_bar (b : Oracle_types.bar) : Dio_strategies.Grid_core_types.bar =
  Dio_strategies.Grid_core_types.{ high = b.high; low = b.low; close = b.close }
;;

(** Run the portfolio replay. Each position advances over its own bars; the
    session count is the longest series. All positions on the same venue share
    one pool, seeded by the sum of their [pool] shares. Transfers are applied
    at their session before any position trades that session. *)
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
  (* One shared pool per venue account, plus its running trough for the current
     session. The trough tracks the ACTUAL shared pool minimum: it is updated by
     every position's cash hook (and by transfers), so a position's reported
     pool_min_drawdown reflects the joint path of all positions on the venue -
     one sibling's buys draw the same pool the other's sells credit. *)
  let pools : (string * string * bool, float ref) Hashtbl.t = Hashtbl.create 8 in
  let troughs : (string * string * bool, float ref) Hashtbl.t = Hashtbl.create 8 in
  let pool_of (key : pool_key) =
    match Hashtbl.find_opt pools (key.venue, key.quote, key.testnet) with
    | Some r -> r
    | None ->
      let r = ref 0.0 in
      Hashtbl.add pools (key.venue, key.quote, key.testnet) r;
      r
  in
  let trough_of (key : pool_key) =
    match Hashtbl.find_opt troughs (key.venue, key.quote, key.testnet) with
    | Some r -> r
    | None ->
      let r = ref 0.0 in
      Hashtbl.add troughs (key.venue, key.quote, key.testnet) r;
      r
  in
  (* Seed every venue pool with the sum of its positions' shares. *)
  List.iter
    (fun (p : aligned_position_input) ->
       let pool = pool_of (key_of_position p) in
       pool := !pool +. p.pool)
    positions;
  let position_runtime =
    List.map
      (fun (p : aligned_position_input) ->
         let key = key_of_position p in
         let pool = pool_of key in
         let trough = trough_of key in
         let hook =
           Some
             { G.balance = (fun () -> !pool)
             ; spend =
                 (fun a ->
                   pool := !pool -. a;
                   trough := Float.min !trough !pool)
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
         { key
         ; asset = p.asset
         ; initial_pool = !pool
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
  (* Even with no bars at all we run session 0 so that transfers scheduled for
     it are applied. *)
  let n_iter = if n_sessions = 0 then 1 else n_sessions in
  for i = 0 to n_iter - 1 do
    (* Transfers scheduled for this session: move budget between venue pools. *)
    List.iter
      (fun t ->
         if t.session = i
         then (
           let from_pool = pool_of t.from in
           let to_pool = pool_of t.to_ in
           let amt = Float.max 0.0 (Float.min t.amount (Float.max 0.0 !from_pool)) in
           from_pool := !from_pool -. amt;
           to_pool := !to_pool +. amt;
           let from_trough = trough_of t.from in
           let to_trough = trough_of t.to_ in
           from_trough := Float.min !from_trough !from_pool;
           to_trough := Float.min !to_trough !to_pool))
      transfers;
    (* Reset each venue pool's trough to its session-start level (after
       transfers), so the session's drawdown is measured against the level the
       venue actually started from. *)
    List.iter
      (fun (rt : pos_rt) ->
         let trough = trough_of rt.key in
         trough := !(rt.pool))
      position_runtime;
    (* Advance every position over its own session bar. *)
    List.iter
      (fun (rt : pos_rt) ->
         if i < Array.length rt.bars
         then (
           match rt.bars.(i) with
           | None -> ()
           | Some source_bar ->
             let bar = to_grid_bar source_bar in
             List.iter
               (fun sg ->
                  let fs = G.on_bar sg.grid ~state:sg.state ~bar ~ordering in
                  List.iter
                    (fun f ->
                       if f.GT.side = `Buy then incr rt.buy_fills else incr rt.sell_fills)
                    fs;
                  if sg.state.G.ever_capital_low
                  then if !first_cl_global = None then first_cl_global := Some i)
               rt.subgrids))
      position_runtime;
    (* Record the venue trough: every position on the venue shares the one
       pool, so they all report the same pool_min_drawdown for this session.
       Running this after ALL positions have traded keeps the metric on the
       actual pool path (sibling buys and sells included). *)
    List.iter
      (fun (rt : pos_rt) ->
         let trough = trough_of rt.key in
         let dd =
           if rt.initial_pool > 0.0 then 1.0 -. (!trough /. rt.initial_pool) else 1.0
         in
         rt.pool_min_dd := Float.max !(rt.pool_min_dd) dd;
         (* First exhaustion: first session any subgrid of this position was
            capital-low. *)
         if
           !(rt.first_cl_session) = None
           && List.exists (fun sg -> sg.state.G.ever_capital_low) rt.subgrids
         then (
           rt.first_cl_session := Some i;
           rt.first_cl_dd := Some dd))
      position_runtime
  done;
  let positions_out =
    List.map
      (fun (rt : pos_rt) ->
         let capital_low = !(rt.first_cl_session) <> None in
         { venue = rt.key.venue
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
  let venues_order =
    List.fold_left
      (fun acc (rt : pos_rt) ->
         if List.exists (fun key -> key = rt.key) acc then acc else acc @ [ rt.key ])
      []
      position_runtime
  in
  let venues_out =
    List.map
      (fun (key : pool_key) ->
         let rts = List.filter (fun (rt : pos_rt) -> rt.key = key) position_runtime in
         let initial_pool =
           match rts with
           | rt :: _ -> rt.initial_pool
           | [] -> 0.0
         in
         let pool_min_dd =
           List.fold_left
             (fun acc (rt : pos_rt) -> Float.max acc !(rt.pool_min_dd))
             0.0
             rts
         in
         let first_dd =
           match List.filter_map (fun (rt : pos_rt) -> !(rt.first_cl_dd)) rts with
           | [] -> None
           | values -> Some (List.fold_left Float.min Float.infinity values)
         in
         let first_session =
           match List.filter_map (fun (rt : pos_rt) -> !(rt.first_cl_session)) rts with
           | [] -> None
           | values -> Some (List.fold_left min max_int values)
         in
         let capital_low =
           List.exists (fun (rt : pos_rt) -> !(rt.first_cl_session) <> None) rts
         in
         { venue = key.venue
         ; quote = key.quote
         ; testnet = key.testnet
         ; assets = List.map (fun (rt : pos_rt) -> rt.asset) rts
         ; initial_pool
         ; final_pool = !(pool_of key)
         ; pool_min_drawdown = pool_min_dd
         ; first_exhausted_drawdown = first_dd
         ; first_exhausted_session = first_session
         ; capital_low
         ; d_surv = (if capital_low then pool_min_dd else 1.0)
         ; buy_fills =
             List.fold_left (fun acc (rt : pos_rt) -> acc + !(rt.buy_fills)) 0 rts
         ; sell_fills =
             List.fold_left (fun acc (rt : pos_rt) -> acc + !(rt.sell_fills)) 0 rts
         ; final_base =
             List.fold_left
               (fun acc (rt : pos_rt) ->
                  acc
                  +. List.fold_left (fun acc sg -> acc +. sg.state.G.base) 0.0 rt.subgrids)
               0.0
               rts
         })
      venues_order
  in
  { n_sessions
  ; positions = positions_out
  ; venues = venues_out
  ; exhausted = List.exists (fun (o : venue_outcome) -> o.capital_low) venues_out
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
         ; quote = p.quote
         ; testnet = p.testnet
         ; pool = p.pool
         ; initial_base = 0.0
         ; bars = Array.map (fun bar -> Some bar) p.bars
         ; subgrids = p.subgrids
         })
      positions
  in
  simulate_aligned ~timeline ~positions ~transfers ~ordering ()
;;
