(** Live replay driver (design §8.3).

    Re-runs the grid strategy against the inputs recorded in a {!Strategy_trace.t} — the
    per-cycle market/account facts and the venue open-order feed — and captures the
    emitted order intents. Diffing the result (emitted-only) against the recorded emitted
    intents proves the input record is sufficient to reproduce the strategy's decisions.

    Approximations: the effective grid interval and accumulation buffer are taken from the
    config entry, not the live oracle blend. If a replay diverges, those are the first
    inputs to add to the record. *)

module Trace = Dio_strategies.Strategy_trace
module Expr = Dio_strategies.Strategy_expr
module Jac = Dio_strategies.Jacobs_ladder
module Order = Dio_strategies.Strategy_common
module Strategy_file = Dio_strategies.Strategy_file
module Strategy_actions_grid = Dio_strategies.Strategy_actions_grid
module Strategy_actions_builtin = Dio_strategies.Strategy_actions_builtin
module Strategy_compile = Dio_strategies.Strategy_compile
module Strategy_runtime = Dio_strategies.Strategy_runtime

let f_entry entries key default =
  match List.assoc_opt key entries with
  | Some (Expr.V_float f) -> f
  | Some (Expr.V_int i) -> float_of_int i
  | _ -> default
;;

let i_entry entries key default =
  match List.assoc_opt key entries with
  | Some (Expr.V_int i) -> i
  | Some (Expr.V_float f) -> int_of_float f
  | _ -> default
;;

let b_entry entries key default =
  match List.assoc_opt key entries with
  | Some (Expr.V_bool b) -> b
  | _ -> default
;;

let age_entry entries =
  match List.assoc_opt "balance_age" entries with
  | Some (Expr.V_float a) -> Some a
  | _ -> None
;;

(** Split a cycle's observations into its input state entries and its venue open orders. *)
let split_cycle (c : Trace.cycle) =
  let state = ref []
  and orders = ref [] in
  List.iter
    (fun o ->
      match o with
      | Trace.State es -> state := es
      | Trace.Order_intent oi -> orders := oi :: !orders
      | _ -> ())
    c.c_obs;
  !state, List.rev !orders
;;

let emitted_of_order (o : Order.strategy_order) : Trace.emitted =
  { em_op =
      (match o.operation with
       | Order.Place -> "place"
       | Order.Amend -> "amend"
       | Order.Cancel -> "cancel")
  ; em_symbol = o.symbol
  ; em_side =
      (match o.side with
       | Order.Buy -> "buy"
       | Order.Sell -> "sell")
  ; em_qty = o.qty
  ; em_price = Option.value o.price ~default:nan
  ; em_post_only = o.post_only
  ; em_order_id = o.order_id
  }
;;

(** Snapshot the decision-relevant scalar/option strategy-state fields for replay seeding,
    recorded as ["s_"-prefixed] entries. *)
let snapshot_entries (st : Dio_strategies.Jacobs_ladder_types.strategy_state)
  : (string * Expr.value) list
  =
  let optf = function
    | Some f -> Expr.V_float f
    | None -> Expr.V_none
  in
  let opts = function
    | Some s -> Expr.V_string s
    | None -> Expr.V_none
  in
  [ "s_last_buy_order_id", opts st.last_buy_order_id
  ; "s_last_buy_order_price", optf st.last_buy_order_price
  ; "s_reserved_base", Expr.V_float st.reserved_base
  ; "s_reserved_quote", Expr.V_float st.reserved_quote
  ; "s_accumulated_profit", Expr.V_float st.accumulated_profit
  ; "s_position_base", Expr.V_float st.position_base
  ; "s_position_initialized", Expr.V_bool st.position_initialized
  ; "s_last_buy_fill_price", optf st.last_buy_fill_price
  ; "s_last_sell_fill_price", optf st.last_sell_fill_price
  ; "s_last_buy_fill_qty", optf st.last_buy_fill_qty
  ; "s_last_sell_fill_qty", optf st.last_sell_fill_qty
  ; "s_last_fill_oid", opts st.last_fill_oid
  ; "s_feed_locked_sell_base", Expr.V_float st.feed_locked_sell_base
  ; "s_cached_qty_increment", Expr.V_float st.cached_qty_increment
  ; "s_duplicate_key_buy", Expr.V_string st.duplicate_key_buy
  ; "s_duplicate_key_sell", Expr.V_string st.duplicate_key_sell
  ; "s_last_buy_ack_ts", Expr.V_float st.last_buy_ack_ts
  ; "s_last_seen_asset_balance", Expr.V_float st.last_seen_asset_balance
  ; "s_last_balance_delta", Expr.V_float st.last_balance_delta
  ; "s_cached_sell_mult", Expr.V_float st.cached_sell_mult
  ; "s_asset_low", Expr.V_bool st.asset_low
  ; "s_capital_low", Expr.V_bool st.capital_low
  ; "s_resuming_after_balance_flag", Expr.V_bool st.resuming_after_balance_flag
  ; "s_just_filled_buy", Expr.V_bool st.just_filled_buy
  ; "s_force_buy_reanchor", Expr.V_bool st.force_buy_reanchor
  ; "s_tif_recovery_pending", Expr.V_bool st.tif_recovery_pending
  ; "s_inflight_buy", Expr.V_bool st.inflight_buy
  ; "s_inflight_sell", Expr.V_bool st.inflight_sell
  ; "s_inflight_amend_buy", Expr.V_bool st.inflight_amend_buy
  ; "s_inflight_cancel_buy", Expr.V_bool st.inflight_cancel_buy
  ; "s_last_buy_attempted_insufficient", Expr.V_bool st.last_buy_attempted_insufficient
  ]
;;

(** Apply a {!snapshot_entries} snapshot to [st] before replay. *)
let seed
  (st : Dio_strategies.Jacobs_ladder_types.strategy_state)
  (entries : (string * Expr.value) list)
  =
  let getf k d =
    match List.assoc_opt k entries with
    | Some (Expr.V_float f) -> f
    | Some (Expr.V_int i) -> float_of_int i
    | _ -> d
  in
  let getb k d =
    match List.assoc_opt k entries with
    | Some (Expr.V_bool b) -> b
    | _ -> d
  in
  let gets k =
    match List.assoc_opt k entries with
    | Some (Expr.V_string s) -> Some s
    | _ -> None
  in
  let getfo k =
    match List.assoc_opt k entries with
    | Some (Expr.V_float f) -> Some f
    | _ -> None
  in
  st.last_buy_order_id <- gets "s_last_buy_order_id";
  st.last_buy_order_price <- getfo "s_last_buy_order_price";
  st.reserved_base <- getf "s_reserved_base" st.reserved_base;
  st.reserved_quote <- getf "s_reserved_quote" st.reserved_quote;
  st.accumulated_profit <- getf "s_accumulated_profit" st.accumulated_profit;
  st.position_base <- getf "s_position_base" st.position_base;
  st.position_initialized <- getb "s_position_initialized" st.position_initialized;
  st.last_buy_fill_price <- getfo "s_last_buy_fill_price";
  st.last_sell_fill_price <- getfo "s_last_sell_fill_price";
  st.last_buy_fill_qty <- getfo "s_last_buy_fill_qty";
  st.last_sell_fill_qty <- getfo "s_last_sell_fill_qty";
  st.last_fill_oid <- gets "s_last_fill_oid";
  st.feed_locked_sell_base <- getf "s_feed_locked_sell_base" st.feed_locked_sell_base;
  st.cached_qty_increment <- getf "s_cached_qty_increment" st.cached_qty_increment;
  (match List.assoc_opt "s_duplicate_key_buy" entries with
   | Some (Expr.V_string s) -> st.duplicate_key_buy <- s
   | _ -> ());
  (match List.assoc_opt "s_duplicate_key_sell" entries with
   | Some (Expr.V_string s) -> st.duplicate_key_sell <- s
   | _ -> ());
  st.last_buy_ack_ts <- getf "s_last_buy_ack_ts" st.last_buy_ack_ts;
  st.last_seen_asset_balance
  <- getf "s_last_seen_asset_balance" st.last_seen_asset_balance;
  st.last_balance_delta <- getf "s_last_balance_delta" st.last_balance_delta;
  st.cached_sell_mult <- getf "s_cached_sell_mult" st.cached_sell_mult;
  st.asset_low <- getb "s_asset_low" st.asset_low;
  st.capital_low <- getb "s_capital_low" st.capital_low;
  st.resuming_after_balance_flag
  <- getb "s_resuming_after_balance_flag" st.resuming_after_balance_flag;
  st.just_filled_buy <- getb "s_just_filled_buy" st.just_filled_buy;
  st.force_buy_reanchor <- getb "s_force_buy_reanchor" st.force_buy_reanchor;
  st.tif_recovery_pending <- getb "s_tif_recovery_pending" st.tif_recovery_pending;
  st.inflight_buy <- getb "s_inflight_buy" st.inflight_buy;
  st.inflight_sell <- getb "s_inflight_sell" st.inflight_sell;
  st.inflight_amend_buy <- getb "s_inflight_amend_buy" st.inflight_amend_buy;
  st.inflight_cancel_buy <- getb "s_inflight_cancel_buy" st.inflight_cancel_buy;
  st.last_buy_attempted_insufficient
  <- getb "s_last_buy_attempted_insufficient" st.last_buy_attempted_insufficient
;;

(** Snapshot the collection state (JSON-encoded in string entries) for replay seeding. *)
let snapshot_collections (st : Dio_strategies.Jacobs_ladder_types.strategy_state)
  : (string * Expr.value) list
  =
  let module S = Dio_strategies.Jacobs_ladder_sell_orders in
  let open_sells =
    `List
      (List.map
         (fun (id, p, q) -> `List [ `String id; `Float p; `Float q ])
         (S.to_list st.open_sell_orders))
  in
  let commitments =
    `List
      (Hashtbl.fold
         (fun id (c : Dio_strategies.Jacobs_ladder_types.sell_commitment) acc ->
           `List
             [ `String id
             ; `Float c.sc_price
             ; `Float c.sc_qty
             ; `Bool c.sc_seen
             ; `Bool c.sc_acked
             ; `Bool c.sc_listed
             ; `Float c.sc_armed
             ]
           :: acc)
         st.sell_commitments
         [])
  in
  let pending =
    `List
      (List.map
         (fun (id, side, p, ts) ->
           `List
             [ `String id
             ; `String
                 (match side with
                  | Order.Buy -> "buy"
                  | Order.Sell -> "sell")
             ; `Float p
             ; `Float ts
             ])
         st.pending_orders)
  in
  let persisted =
    `List (List.map (fun (p, q) -> `List [ `Float p; `Float q ]) st.persisted_sell_levels)
  in
  let cooldowns =
    `List
      (Hashtbl.fold
         (fun id v acc -> `List [ `String id; `Float v ] :: acc)
         st.amend_cooldowns
         [])
  in
  let pairs xs = `List (List.map (fun (a, b) -> `List [ `Float a; `Float b ]) xs) in
  [ "s_open_sell_orders", Expr.V_string (Yojson.Basic.to_string open_sells)
  ; "s_sell_commitments", Expr.V_string (Yojson.Basic.to_string commitments)
  ; "s_pending_orders", Expr.V_string (Yojson.Basic.to_string pending)
  ; "s_persisted_sell_levels", Expr.V_string (Yojson.Basic.to_string persisted)
  ; "s_amend_cooldowns", Expr.V_string (Yojson.Basic.to_string cooldowns)
  ; ( "s_sell_holds_since_balance"
    , Expr.V_string (Yojson.Basic.to_string (pairs st.sell_holds_since_balance)) )
  ; ( "s_buy_credits_since_balance"
    , Expr.V_string (Yojson.Basic.to_string (pairs st.buy_credits_since_balance)) )
  ]
;;

(** Restore the collection state from a {!snapshot_collections} snapshot. *)
let seed_collections
  (st : Dio_strategies.Jacobs_ladder_types.strategy_state)
  (entries : (string * Expr.value) list)
  =
  let module S = Dio_strategies.Jacobs_ladder_sell_orders in
  let j k =
    match List.assoc_opt k entries with
    | Some (Expr.V_string s) -> Some (Yojson.Basic.from_string s)
    | _ -> None
  in
  (match j "s_open_sell_orders" with
   | Some (`List l) ->
     S.clear st.open_sell_orders;
     List.iter
       (function
         | `List [ `String id; `Float p; `Float q ] -> S.push st.open_sell_orders id p q
         | _ -> ())
       l
   | _ -> ());
  (match j "s_sell_commitments" with
   | Some (`List l) ->
     Hashtbl.reset st.sell_commitments;
     List.iter
       (function
         | `List
             [ `String id
             ; `Float p
             ; `Float q
             ; `Bool seen
             ; `Bool acked
             ; `Bool listed
             ; `Float armed
             ] ->
           Hashtbl.replace
             st.sell_commitments
             id
             { Dio_strategies.Jacobs_ladder_types.sc_price = p
             ; sc_qty = q
             ; sc_seen = seen
             ; sc_acked = acked
             ; sc_listed = listed
             ; sc_armed = armed
             }
         | _ -> ())
       l
   | _ -> ());
  (match j "s_pending_orders" with
   | Some (`List l) ->
     st.pending_orders
     <- List.filter_map
          (function
            | `List [ `String id; `String side; `Float p; `Float ts ] ->
              Some
                ( id
                , (match side with
                   | "sell" -> Order.Sell
                   | _ -> Order.Buy)
                , p
                , ts )
            | _ -> None)
          l
   | _ -> ());
  (match j "s_persisted_sell_levels" with
   | Some (`List l) ->
     st.persisted_sell_levels
     <- List.filter_map
          (function
            | `List [ `Float p; `Float q ] -> Some (p, q)
            | _ -> None)
          l
   | _ -> ());
  (match j "s_amend_cooldowns" with
   | Some (`List l) ->
     Hashtbl.reset st.amend_cooldowns;
     List.iter
       (function
         | `List [ `String id; `Float v ] -> Hashtbl.replace st.amend_cooldowns id v
         | _ -> ())
       l
   | _ -> ());
  let pairs_of l =
    List.filter_map
      (function
        | `List [ `Float a; `Float b ] -> Some (a, b)
        | _ -> None)
      l
  in
  (match j "s_sell_holds_since_balance" with
   | Some (`List l) -> st.sell_holds_since_balance <- pairs_of l
   | _ -> ());
  match j "s_buy_credits_since_balance" with
  | Some (`List l) -> st.buy_credits_since_balance <- pairs_of l
  | _ -> ()
;;

(** Seed the strategy state from the recorded snapshot in the first cycle. *)
let seed_from_trace (state : Dio_strategies.Jacobs_ladder_types.strategy_state) trace =
  match trace with
  | (c : Trace.cycle) :: _ ->
    let entries =
      List.concat_map
        (function
          | Trace.State es -> es
          | _ -> [])
        c.c_obs
    in
    seed state entries;
    seed_collections state entries
  | [] -> ()
;;

(** Drive [trace] through [execute], applying the recorded per-cycle inputs (oracle/F&G
    blend, domain knobs, venue-available snapshot) to [state] first, and capture the
    emitted intents per cycle. Shared by the reference and candidate replays. *)
let drive
  ~(asset : Jac.trading_config)
  ~(state : Dio_strategies.Jacobs_ladder_types.strategy_state)
  ~(set_venue_available : (string -> float -> unit) option)
  ~(execute :
      price:float
      -> bid:float
      -> ask:float
      -> abal:float
      -> qbal:float
      -> now:float
      -> cycle:int
      -> oracle_halted:bool
      -> quote_stale:bool
      -> base_age:float option
      -> iter:((string -> float -> float -> string -> int option -> unit) -> unit)
      -> unit)
  (trace : Trace.t)
  : Trace.t
  =
  (* Drain any pre-existing buffer so only this replay's intents are captured. *)
  ignore (Jac.get_pending_orders 1_000_000 : Order.strategy_order list);
  List.mapi
    (fun idx (c : Trace.cycle) ->
      let entries, open_orders = split_cycle c in
      let price = f_entry entries "price" nan in
      let bid = f_entry entries "bid" nan in
      let ask = f_entry entries "ask" nan in
      let abal = f_entry entries "asset_balance" nan in
      let qbal = f_entry entries "quote_balance" nan in
      let now = f_entry entries "now" 0.0 in
      let cycle = i_entry entries "cycle" idx in
      let oracle_halted = b_entry entries "oracle_halted" false in
      let quote_stale = b_entry entries "quote_balance_stale" false in
      let base_age = age_entry entries in
      let grid_qty = f_entry entries "grid_qty" nan in
      if Float.is_finite grid_qty then state.grid_qty <- grid_qty;
      (* Effective grid interval comes from the capital-oracle blend, not config. *)
      let gi = f_entry entries "grid_interval" nan in
      let asset =
        if Float.is_finite gi then { asset with grid_interval = gi } else asset
      in
      (* Accumulation buffer comes from the resolved Fear & Greed blend, not config. *)
      let ab = f_entry entries "accumulation_buffer" nan in
      let asset =
        if Float.is_finite ab then { asset with accumulation_buffer = ab } else asset
      in
      (* Domain-provided knobs the oracle re-sets outside the strategy call. *)
      if b_entry entries "force_buy_reanchor" false then state.force_buy_reanchor <- true;
      if b_entry entries "capital_low" false then state.capital_low <- true;
      (* Restore the venue's immediately-sellable base snapshot for this cycle. *)
      (match set_venue_available with
       | Some f ->
         let va = f_entry entries "venue_available" nan in
         if Float.is_finite va then f asset.symbol va
       | None -> ());
      let orders = Array.of_list open_orders in
      let iter (f : string -> float -> float -> string -> int option -> unit) =
        Array.iter
          (fun (oi : Trace.order_intent) ->
            f
              (Option.value oi.oi_order_id ~default:"replay")
              oi.oi_price
              oi.oi_qty
              oi.oi_side
              None)
          orders
      in
      execute
        ~price
        ~bid
        ~ask
        ~abal
        ~qbal
        ~now
        ~cycle
        ~oracle_halted
        ~quote_stale
        ~base_age
        ~iter;
      let pending = Jac.get_pending_orders 1_000_000 in
      { Trace.c_index = idx
      ; c_obs = List.map (fun e -> Trace.Emitted e) (List.map emitted_of_order pending)
      })
    trace
;;

(** Replay [trace] through the reference grid, returning an emitted-only trace. *)
let replay
  ~(asset : Jac.trading_config)
  ~(set_venue_available : (string -> float -> unit) option)
  ~(trace : Trace.t)
  : Trace.t
  =
  let state = Jac.get_strategy_state asset.symbol in
  seed_from_trace state trace;
  drive
    ~asset
    ~state
    ~set_venue_available
    ~execute:
      (fun
        ~price
        ~bid
        ~ask
        ~abal
        ~qbal
        ~now
        ~cycle
        ~oracle_halted
        ~quote_stale
        ~base_age
        ~iter
      ->
      Jac.Strategy.execute
        ~cached_state:state
        ~quote_balance_stale:quote_stale
        ~oracle_halted
        ~get_open_orders_generation:(fun () -> 0)
        ~base_balance_age:base_age
        ~now
        asset
        price
        bid
        ask
        abal
        qbal
        0
        0
        iter
        cycle)
    trace
;;

(** Replay [trace] through the config-driven interpreter (candidate) — the coarse
    [grid_cycle] handler for the shipped file, fine actions once decomposed. *)
let replay_candidate
  ~(asset : Jac.trading_config)
  ~(set_venue_available : (string -> float -> unit) option)
  ~(file : Strategy_file.t)
  ~(trace : Trace.t)
  : Trace.t
  =
  let state = Jac.get_strategy_state asset.symbol in
  seed_from_trace state trace;
  let ctx = Config_grid_engine.create () in
  ctx.cg_state <- Some state;
  let module Handlers = Strategy_actions_grid.Make (Config_grid_engine) in
  let rt = Strategy_runtime.create ~handlers:(Handlers.handler ctx) file in
  drive
    ~asset
    ~state
    ~set_venue_available
    ~execute:
      (fun
        ~price
        ~bid
        ~ask
        ~abal
        ~qbal
        ~now
        ~cycle
        ~oracle_halted
        ~quote_stale
        ~base_age
        ~iter
      ->
      ctx.cg_asset <- Some asset;
      ctx.cg_price <- price;
      ctx.cg_bid <- bid;
      ctx.cg_ask <- ask;
      ctx.cg_abal <- abal;
      ctx.cg_qbal <- qbal;
      ctx.cg_now <- now;
      ctx.cg_cycle <- cycle;
      ctx.cg_quote_stale <- quote_stale;
      ctx.cg_oracle_halted <- oracle_halted;
      ctx.cg_base_age <- base_age;
      ctx.cg_gen <- 0;
      ctx.cg_iter <- iter;
      ignore
        (Strategy_runtime.run_cycle
           rt
           ~price
           ~now
           ~event:(Strategy_runtime.make_event "book_update" [])))
    trace
;;

let asset_of (tc : Config.trading_config) : Jac.trading_config =
  { Jac.exchange = tc.exchange
  ; symbol = tc.symbol
  ; qty = tc.qty
  ; grid_interval = fst tc.grid_interval
  ; sell_mult = tc.sell_mult
  ; strategy = tc.strategy
  ; maker_fee = tc.maker_fee
  ; taker_fee = tc.taker_fee
  ; accumulation_buffer = fst tc.accumulation_buffer
  ; base_accumulation = tc.base_accumulation
  ; sell_levels_persistence = tc.sell_levels
  }
;;

let symbol_of_trace (trace : Trace.t) : string option =
  let rec go = function
    | [] -> None
    | (c : Trace.cycle) :: rest ->
      (match
         List.find_map
           (function
             | Trace.Emitted e -> Some e.em_symbol
             | Trace.Order_intent oi -> Some oi.oi_symbol
             | _ -> None)
           c.c_obs
       with
       | Some s -> Some s
       | None -> go rest)
  in
  go trace
;;

(** Load a recorded trace, replay it (through the reference grid, or the config-driven
    interpreter when [candidate]), and report equivalence between the recorded and
    replayed emitted intents. Writes [<path>.replayed.json]. *)
let run
  ~(set_venue_available : (string -> float -> unit) option)
  ~(candidate : bool)
  (path : string)
  : int
  =
  match Trace.load path with
  | exception exn ->
    Printf.eprintf "replay: cannot load %s: %s\n" path (Printexc.to_string exn);
    1
  | trace ->
    (match symbol_of_trace trace with
     | None ->
       Printf.eprintf "replay: no symbol found in %s\n" path;
       1
     | Some symbol ->
       let config = Config.read_config () in
       (match
          List.find_opt
            (fun (tc : Config.trading_config) -> String.equal tc.symbol symbol)
            config.trading
        with
        | None ->
          Printf.eprintf "replay: no config.json entry for %s\n" symbol;
          1
        | Some tc ->
          let asset = asset_of tc in
          let replayed =
            if not candidate
            then replay ~asset ~set_venue_available ~trace
            else (
              Strategy_actions_builtin.register_all ();
              let path_file = Printf.sprintf "strategies/%s.json" asset.strategy in
              match Strategy_file.parse_file path_file with
              | Error msg ->
                Printf.eprintf "replay: cannot load %s: %s\n" path_file msg;
                exit 1
              | Ok file ->
                let diags = Strategy_compile.validate file in
                if Strategy_compile.has_errors diags
                then (
                  Printf.eprintf
                    "replay: %s has errors: %s\n"
                    path_file
                    (Strategy_compile.format diags);
                  exit 1)
                else replay_candidate ~asset ~set_venue_available ~file ~trace)
          in
          Trace.save (path ^ ".replayed.json") replayed;
          (match Trace.compare (Trace.emitted_only trace) replayed with
           | None ->
             Printf.printf
               "replay equivalent (%s): %s (%d cycles, %d emitted cycles)\n"
               (if candidate then "candidate" else "reference")
               path
               (List.length trace)
               (List.length replayed);
             0
           | Some msg ->
             Printf.printf
               "replay divergence (%s): %s\n"
               (if candidate then "candidate" else "reference")
               msg;
             1)))
;;
