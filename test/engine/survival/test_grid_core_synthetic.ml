(* Synthetic deterministic fixtures for the Grid_core survival model (audit
   test plan Section F):

   Scenario A - Flat market:      no fills, D_surv = 1.0 (never exhausted).
   Scenario B - Monotonic decline: exactly N fills, capital = initial minus
       exact ladder costs, exhaustion exactly when Quote < Cost(N+1).
   Scenario C - Dynamic scaling:  the floor is breached on the base qty, the
       order qty is up-sized to clear min_notional, capital drains faster per
       rung and capital_low fires against the up-sized cost.
   Scenario D - Fee drain:        oscillation between two grid levels; capital
       drains purely through fees (gi < ~2*fee) until exhaustion.
   Invariants:                    F_blend monotone non-decreasing; default
       stride = horizon; empty distributions raise instead of defaulting. *)
let lot = 1e-9

(* Must use the same float arithmetic as Grid_core.round_price / ceil_lot
   (multiply by 1e9, then divide), otherwise 1-ulp differences accumulate. *)
let round_tick x = Float.round (x *. 1e9) /. 1e9
let near ?(eps = 1e-6) a b = Alcotest.(check (float eps)) "approx" a b

let cfg
      ?(qty = 1.0)
      ?(sell_mult = 1.0)
      ?(grid_interval_pct = 1.0)
      ?(min_notional = 0.0)
      ?(start_price = 100.0)
      ?(start_quote = 10_000.0)
      ?(fee = 0.0)
      ?(model = Dio_strategies.Grid_core_types.Hyperliquid)
      ()
  =
  let open Dio_strategies.Grid_core in
  { qty
  ; sell_mult
  ; grid_interval_pct
  ; maker_fee = fee
  ; accumulation_buffer = 0.0
  ; price_increment = lot
  ; qty_increment = lot
  ; qty_min = 0.0
  ; min_notional
  ; exchange_model = model
  ; start_price
  ; start_quote
  ; cash_hook = None
  }
;;

let bar ?(high = -1.0) ?(low = -1.0) ?(close = -1.0) () =
  let low = if low < 0.0 then high else low in
  let high = if high < 0.0 then low else high in
  let close = if close < 0.0 then low else close in
  Dio_strategies.Grid_core_types.{ high; low; close }
;;

let series_of ~symbol bars : Dio_survival.Survival_types.series =
  { Dio_survival.Survival_types.symbol
  ; calendar_kind = Dio_survival.Survival_types.Crypto
  ; bars =
      Array.mapi
        (fun i (b : Dio_strategies.Grid_core_types.bar) ->
           ({ date = Printf.sprintf "2023-%03d" i
            ; open_ = b.close
            ; high = b.high
            ; low = b.low
            ; close = b.close
            ; volume = 1000.0
            }
            : Dio_survival.Survival_types.bar))
        bars
  ; gaps = []
  }
;;

let replay c bars =
  Dio_strategies.Grid_core.replay
    c
    ~bars
    ~ordering:Dio_strategies.Grid_core_types.Buy_first
;;

(* ---- Scenario A: flat market ---- *)

let test_scenario_a_flat_market () =
  let c = cfg ~start_quote:5_000.0 () in
  let bars = Array.init 20 (fun _ -> bar ~high:100.0 ~low:100.0 ~close:100.0 ()) in
  let res = replay c bars in
  let out = Dio_survival.Survival_replay.replay_series c (series_of ~symbol:"A" bars) in
  Alcotest.(check int) "zero buys" 0 res.buy_fills;
  Alcotest.(check int) "zero sells" 0 res.sell_fills;
  Alcotest.(check bool) "never exhausted" (not res.exhausted) true;
  Alcotest.(check bool) "no halt cause" (res.halt_cause = None) true;
  near 5_000.0 res.final_quote;
  near 1.0 out.d_surv
;;

(* ---- Scenario B: monotonic geometric decline ---- *)

let test_scenario_b_monotonic_decline () =
  (* One ladder step per bar, no sells: fills while quote can fund the exact
     ladder cost. start_quote 500 funds 5 fills; level 6 (94.15) is the first
     unaffordable rung. *)
  let c = cfg ~start_quote:500.0 () in
  let n_bars = 10 in
  let level k = round_tick (100.0 *. (0.99 ** float_of_int k)) in
  let bars =
    Array.init n_bars (fun i ->
      let low = level (i + 1) in
      bar ~high:(low *. 1.005) ~low ())
  in
  let res = replay c bars in
  let cost_of n = 99.0 *. (1.0 -. (0.99 ** float_of_int n)) /. 0.01 in
  let expected_n = ref 0 in
  while cost_of (!expected_n + 1) <= 500.0 do
    incr expected_n
  done;
  Alcotest.(check int) "fill count" !expected_n res.buy_fills;
  Alcotest.(check int) "no sells" 0 res.sell_fills;
  (* Capital equals the initial quote minus the exact ladder costs. *)
  near (500.0 -. cost_of !expected_n) res.final_quote;
  near (500.0 -. cost_of !expected_n) res.min_quote;
  Alcotest.(check bool) "exhausted" true res.exhausted;
  Alcotest.(check bool) "halt cause is capital" (res.halt_cause = Some `Capital) true;
  (* Exhaustion fired exactly when Quote < Cost(N+1). *)
  let halt_level = level (!expected_n + 1) in
  (match res.first_capital_low_drawdown with
   | Some dd -> near (1.0 -. (halt_level /. 100.0)) dd
   | None -> Alcotest.fail "expected capital-low drawdown");
  let out = Dio_survival.Survival_replay.replay_series c (series_of ~symbol:"B" bars) in
  near (1.0 -. (halt_level /. 100.0)) out.d_surv
;;

(* ---- Scenario C: dynamic scaling & exhaustion ---- *)

(** Independent ladder oracle with dynamic buy sizing: fills while quote can
    fund q = max(qty, ceil_lot(min_notional / level)) at the level price.
    Levels are walked with the state machine's own level helpers (buy_level /
    sell_level / trail_buy_level, contract-tested against the live grid) so
    the expected path is bit-exact; q, cost and the affordability gate are
    recomputed independently here. *)
let ladder_oracle (c : Dio_strategies.Grid_core.config) ~start_quote ~max_levels =
  let open Dio_strategies.Grid_core in
  let b = ref (buy_level c ~ref:c.start_price) in
  let quote = ref start_quote in
  let n = ref 0 in
  let finished = ref false in
  while (not !finished) && !n < max_levels do
    let floor_q =
      if c.min_notional > 0.0
      then Float.ceil ((c.min_notional /. !b *. 1e9) -. 1e-9) /. 1e9
      else 0.0
    in
    let q = Float.max c.qty floor_q in
    let cost = q *. !b in
    if cost <= !quote
    then (
      quote := !quote -. cost;
      incr n;
      b := trail_buy_level c ~bid:!b ~sell:(sell_level c ~ref:!b))
    else finished := true
  done;
  !n, !quote, !b
;;

let test_scenario_c_dynamic_scaling () =
  (* qty 0.5 with the $10 floor: while 0.5 * level >= 10 the rung costs
     ~0.5 * level; below it the qty is up-sized to ~10/level so each rung
     costs ~$10 and capital burns ~20x faster per price step. *)
  let min_notional = 10.0 in
  let c = cfg ~qty:0.5 ~min_notional ~start_quote:4_200.0 () in
  let n_bars = 240 in
  let bars =
    let open Dio_strategies.Grid_core in
    let b = ref (buy_level c ~ref:c.start_price) in
    Array.init n_bars (fun _ ->
      let b_ = !b in
      b := trail_buy_level c ~bid:b_ ~sell:(sell_level c ~ref:b_);
      bar ~high:(b_ *. 1.005) ~low:b_ ())
  in
  let res = replay c bars in
  let n, quote, halt_level = ladder_oracle c ~start_quote:4_200.0 ~max_levels:n_bars in
  Alcotest.(check int) "oracle fills" n res.buy_fills;
  Alcotest.(check bool) "no sells" (res.sell_fills = 0) true;
  near quote res.final_quote;
  Alcotest.(check bool) "exhausted" true res.exhausted;
  Alcotest.(check bool) "halt cause is capital" (res.halt_cause = Some `Capital) true;
  (match res.first_capital_low_drawdown with
   | Some dd -> near (1.0 -. (halt_level /. 100.0)) dd
   | None -> Alcotest.fail "expected capital-low drawdown");
  (* The floor was breached and orders were up-sized to clear it. *)
  Alcotest.(check bool)
    "up-sized fills exist"
    (List.exists
       (fun (f : Dio_strategies.Grid_core_types.fill) -> f.side = `Buy && f.qty > 0.5)
       res.fills)
    true;
  List.iter
    (fun (f : Dio_strategies.Grid_core_types.fill) ->
       if f.side = `Buy
       then
         Alcotest.(check bool)
           "notional clears the floor"
           (f.qty *. f.price >= min_notional -. 1e-9)
           true)
    res.fills
;;

(* ---- Step 2 regression: empirical min capital converges under a floor ---- *)

let test_empirical_min_capital_converges_with_floor () =
  (* The P0 bug: a fixed-qty notional floor capped D_surv at the blocked level
     (~11% here), so no amount of capital cleared the target and
     empirical_min_capital returned 1e9 unreachable. With dynamic buy sizing
     the replay's D_surv scales with capital, so the empirical search must
     converge to a finite reachable value. (Note: under a binding floor the
     empirical number can exceed the closed-form static runway, whose
     geometric ladder ignores floor up-sizing - the floor makes the path burn
     capital faster, not slower.) *)
  let min_notional = 90.0 in
  let start_price = 100.0 in
  let c = cfg ~min_notional ~start_price () in
  let n_bars = 240 in
  let bars =
    let open Dio_strategies.Grid_core in
    let b = ref (buy_level c ~ref:start_price) in
    Array.init n_bars (fun _ ->
      let b_ = !b in
      b := trail_buy_level c ~bid:b_ ~sell:(sell_level c ~ref:b_);
      bar ~high:(b_ *. 1.005) ~low:b_ ())
  in
  let series = series_of ~symbol:"F" bars in
  let horizon =
    { Dio_survival.Survival_types.label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_survival.Survival_replay.blend_model_of
      ~horizon
      ~asset:series
      ~class_members:[ series ]
      ~kappa:2
      ~warmup:10
      ()
  in
  let open Dio_survival.Survival_replay in
  let target = 0.8 in
  let emp = empirical_min_capital ~grid:c ~model ~target_survival:target () in
  Alcotest.(check bool) "empirical reachable" true emp.reachable;
  Alcotest.(check bool) "finite, not the 1e9 sentinel" (emp.value < 1e6) true;
  Alcotest.(check bool) "coverage clears the target" (emp.coverage >= target) true
;;

(* ---- Scenario D: fee drain / oscillation ---- *)

(** Oracle for the oscillation cycle: buy at the trailing buy level, sell at
    the resting sell level, then trail down one grid step. The net per cycle
    is b * (gi - fee*(2+gi)) in percent terms, so with gi <= ~2*fee the quote
    drains purely through fees. Uses the state machine's own level helpers so
    the expected path is exact. *)
let oscillation_oracle (c : Dio_strategies.Grid_core.config) ~start_quote ~max_cycles =
  let open Dio_strategies.Grid_core in
  let b = ref (buy_level c ~ref:c.start_price) in
  let quote = ref start_quote in
  let cycles = ref 0 in
  let finished = ref false in
  while (not !finished) && !cycles < max_cycles do
    let cost = !b *. (1.0 +. c.maker_fee) in
    if cost <= !quote
    then (
      quote := !quote -. cost;
      let s = sell_level c ~ref:!b in
      quote := !quote +. (s *. (1.0 -. c.maker_fee));
      b := trail_buy_level c ~bid:!b ~sell:s;
      incr cycles)
    else finished := true
  done;
  !cycles, !quote
;;

let test_scenario_d_fee_drain () =
  (* gi 0.1% < 2 * fee 0.2%: every buy-sell cycle nets a small loss, so the
     quote drains through fees alone until it can no longer fund the next buy.
     Each completed cycle must be a buy followed by a sell at the same level
     pair. *)
  let c = cfg ~grid_interval_pct:0.1 ~fee:0.002 ~start_quote:150.0 () in
  let max_cycles = 400 in
  let bars =
    let open Dio_strategies.Grid_core in
    let b = ref (buy_level c ~ref:c.start_price) in
    let arr =
      Array.init max_cycles (fun _ ->
        let s = sell_level c ~ref:!b in
        let b_ = !b in
        b := trail_buy_level c ~bid:b_ ~sell:s;
        Dio_strategies.Grid_core_types.{ high = s; low = b_; close = b_ })
    in
    arr
  in
  let res = replay c bars in
  let cycles, quote = oscillation_oracle c ~start_quote:150.0 ~max_cycles in
  Alcotest.(check int) "buy fills" cycles res.buy_fills;
  Alcotest.(check int) "sell fills" cycles res.sell_fills;
  near quote res.final_quote;
  Alcotest.(check bool) "capital drained" (res.final_quote < 150.0) true;
  Alcotest.(check bool) "quote never negative" (res.min_quote >= 0.0) true;
  Alcotest.(check bool) "exhausted" true res.exhausted;
  Alcotest.(check bool) "halt cause is capital" (res.halt_cause = Some `Capital) true
;;

(* ---- Statistical invariants ---- *)

let mk_series ~symbol ~sc () =
  let closes =
    Array.init 400 (fun i ->
      let x = float_of_int i in
      100.0 *. exp (sc *. ((0.001 *. x) +. (0.05 *. sin (x /. 9.0)))))
  in
  let lows =
    Array.mapi
      (fun i c -> c *. (1.0 -. (sc *. (0.02 +. (0.01 *. sin (float_of_int i /. 5.0))))))
      closes
  in
  series_of
    ~symbol
    (Array.mapi
       (fun i close ->
          Dio_strategies.Grid_core_types.{ high = close; low = lows.(i); close })
       closes)
;;

(** Flat path: every close and low is identical, so every trailing-vol window
    is exactly zero (std of identical log-returns) and the class z-index is
    empty - a class pool with no volatility information. Used to construct a
    real coverage gap: the blended max coverage collapses to n/(n+kappa). *)
let mk_flat_series ~symbol () = mk_series ~symbol ~sc:0.0 ()

(* ---- Scenario E: V-shaped recovery ---- *)

(** Ladder levels via the state machine's own level helpers (buy_level /
    sell_level / trail_buy_level, contract-tested against the live grid) so
    the fixture bars agree with the replay bit-for-bit. *)
let ladder_levels (c : Dio_strategies.Grid_core.config) ~n =
  let open Dio_strategies.Grid_core in
  let levels = Array.make n 0.0 in
  let b = ref (buy_level c ~ref:c.start_price) in
  Array.iteri
    (fun i _ ->
       let b_ = !b in
       levels.(i) <- b_;
       b := trail_buy_level c ~bid:b_ ~sell:(sell_level c ~ref:b_))
    levels;
  levels, !b
;;

let test_scenario_v_shaped_recovery_ample_capital () =
  (* Crash then recovery: with ample capital the grid buys down the crash and
     the recovery bar fills the resting sell, never exhausting: D_surv = 1.0
     (it survived every drawdown the history produced). *)
  let c = cfg ~start_quote:10_000.0 () in
  let n_crash = 30 in
  let levels, next = ladder_levels c ~n:n_crash in
  let crash =
    Array.init n_crash (fun i -> bar ~high:(levels.(i) *. 1.005) ~low:levels.(i) ())
  in
  let sell_level = Dio_strategies.Grid_core.sell_level c ~ref:levels.(n_crash - 1) in
  let recovery = [| bar ~high:(sell_level *. 1.001) ~low:(next +. 0.5) () |] in
  let bars = Array.append crash recovery in
  let res = replay c bars in
  let out = Dio_survival.Survival_replay.replay_series c (series_of ~symbol:"V" bars) in
  Alcotest.(check int) "30 buys down the crash" n_crash res.buy_fills;
  Alcotest.(check int) "recovery sell" 1 res.sell_fills;
  Alcotest.(check bool) "never exhausted" (not res.exhausted) true;
  near 1.0 out.d_surv;
  Alcotest.(check bool)
    "quote recovers above the trough"
    (res.final_quote > res.min_quote)
    true
;;

let test_scenario_v_shaped_recovery_tight_capital () =
  (* Same crash with 500 in capital: the ladder exhausts at the 6th level
     (D_surv = 1 - level6/start), then the recovery sell replenishes the
     quote, capital_low clears and buying resumes. *)
  let c = cfg ~start_quote:500.0 () in
  let n_crash = 30 in
  let levels, _next = ladder_levels c ~n:n_crash in
  let crash =
    Array.init n_crash (fun i -> bar ~high:(levels.(i) *. 1.005) ~low:levels.(i) ())
  in
  (* sell resting after the 5th buy (the last affordable rung) *)
  let sell_level = Dio_strategies.Grid_core.sell_level c ~ref:levels.(4) in
  (* recovery bar: low stays above the resting buy (level 6) *)
  let recovery = [| bar ~high:(sell_level *. 1.001) ~low:(levels.(5) +. 0.1) () |] in
  let resume = [| bar ~high:(levels.(5) *. 1.005) ~low:levels.(5) () |] in
  let bars = Array.concat [ crash; recovery; resume ] in
  let res = replay c bars in
  let out = Dio_survival.Survival_replay.replay_series c (series_of ~symbol:"V2" bars) in
  Alcotest.(check bool) "exhausted during the crash" true res.exhausted;
  Alcotest.(check bool) "halt cause is capital" (res.halt_cause = Some `Capital) true;
  near (1.0 -. (levels.(5) /. c.start_price)) out.d_surv;
  Alcotest.(check int) "5 buys + 1 resumed" 6 res.buy_fills;
  Alcotest.(check int) "recovery sell" 1 res.sell_fills;
  (* exact accounting: 500 - ladder(5) + sell proceeds - resumed buy *)
  let ladder_5 = Array.fold_left ( +. ) 0.0 (Array.sub levels 0 5) in
  near (500.0 -. ladder_5 +. sell_level -. levels.(5)) res.final_quote
;;

(* ---- Scenario F: extreme multi-level crash ---- *)

let test_scenario_extreme_multi_level_crash () =
  (* One bar whose low crosses ~68 ladder steps: the grid ladders down within
     a single bar, filling exactly the levels above the low (quote ample), and
     no sell (the bar's high sits below the resting sell of the last buy). *)
  let c = cfg ~start_quote:10_000.0 () in
  let bars = [| bar ~high:50.98 ~low:50.0 () |] in
  let res = replay c bars in
  let cost_of n = 99.0 *. (1.0 -. (0.99 ** float_of_int n)) /. 0.01 in
  Alcotest.(check int) "68 ladder fills" 68 res.buy_fills;
  Alcotest.(check int) "no sells" 0 res.sell_fills;
  Alcotest.(check bool) "never exhausted (quote ample)" (not res.exhausted) true;
  Alcotest.(check bool) "quote never negative" (res.min_quote >= 0.0) true;
  near (10_000.0 -. cost_of 68) res.final_quote
;;

(* ---- Scenario G: insufficient capital ---- *)

let test_scenario_insufficient_capital () =
  (* Capital below the first buy cost: exhaustion at the first level, zero
     fills, D_surv = the grid interval. *)
  let c = cfg ~start_quote:50.0 () in
  let bars = [| bar ~high:100.0 ~low:99.0 () |] in
  let res = replay c bars in
  let out = Dio_survival.Survival_replay.replay_series c (series_of ~symbol:"H" bars) in
  Alcotest.(check int) "no buys" 0 res.buy_fills;
  Alcotest.(check bool) "exhausted" true res.exhausted;
  Alcotest.(check bool) "halt cause is capital" (res.halt_cause = Some `Capital) true;
  near 0.01 out.d_surv
;;

(* ---- Invariants and edge cases ---- *)

let test_impossible_target_explicitly_unreachable () =
  (* A class pool with no volatility information (every member window is flat,
     so all class starts are excluded) leaves the blended max coverage at
     n_eff/(n_eff + kappa) = 0.04: the 0.9 target sits in a coverage gap. No
     capital clears it, and both sizing directions must report reachable =
     false instead of a bogus number. *)
  let asset = mk_series ~symbol:"G" ~sc:1.0 () in
  let flat = mk_flat_series ~symbol:"FLAT" () in
  let horizon =
    { Dio_survival.Survival_types.label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_survival.Survival_replay.blend_model_of
      ~horizon
      ~asset
      ~class_members:[ flat ]
      ~kappa:200
      ~warmup:60
      ()
  in
  let c = cfg () in
  let open Dio_survival.Survival_replay in
  let r = find_min_capital ~grid:c ~model ~target_survival:0.9 () in
  Alcotest.(check bool) "capital unreachable" (not r.reachable) true;
  let q = max_qty ~grid:c ~model ~target_survival:0.9 () in
  Alcotest.(check bool) "qty unreachable" (not q.reachable) true;
  (* A target inside the achievable band is still sized. *)
  let r2 = find_min_capital ~grid:c ~model ~target_survival:0.02 () in
  Alcotest.(check bool) "low target reachable" true r2.reachable;
  (* An impossible bound is reported unreachable too. *)
  let r3 = find_min_capital ~grid:c ~model ~target_survival:0.9 ~hi:1.0 () in
  Alcotest.(check bool) "bound-limited unreachable" (not r3.reachable) true
;;

let test_blend_weight_is_effective_sample () =
  (* The kappa blend weights the asset CDF by the window count on the model's
     own sampling basis: with the default non-overlapping stride the weight is
     n_eff, so kappa shrinks a thin sample toward the class instead of
     pretending every overlapping start is an independent observation. *)
  let series = mk_series ~symbol:"W" ~sc:1.0 () in
  let horizon =
    { Dio_survival.Survival_types.label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_survival.Survival_replay.blend_model_of
      ~horizon
      ~asset:series
      ~class_members:[ series ]
      ~kappa:1
      ~warmup:60
      ()
  in
  let n_eff = Array.length model.index.sigma in
  Alcotest.(check int) "blend weight = n_eff" n_eff model.index.n_asset;
  let d = 0.20 in
  let c = Dio_survival.Survival_replay.blended_coverage model ~d_surv:d in
  let expected =
    ((float_of_int n_eff *. c.asset) +. (1.0 *. c.class_)) /. (float_of_int n_eff +. 1.0)
  in
  near expected c.blended;
  (* An explicitly overlapping basis keeps its (larger) overlapping count. *)
  let overlapping =
    Dio_survival.Survival_replay.blend_model_of
      ~horizon
      ~asset:series
      ~class_members:[ series ]
      ~kappa:1
      ~warmup:60
      ~stride:1
      ()
  in
  Alcotest.(check bool)
    "stride-1 weight larger than n_eff"
    (overlapping.index.n_asset > n_eff)
    true
;;

let test_default_capital_is_viable () =
  (* The CLI's default replay capital: max over horizons of the model's own
     static min capital. It must be positive, finite, and replay to a viable
     grid (fills, D_surv beyond the first level) - the audit's P0 capital
     initialization defect. *)
  let series = mk_series ~symbol:"M" ~sc:1.0 () in
  let h30 =
    { Dio_survival.Survival_types.label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let h90 =
    { Dio_survival.Survival_types.label = "90d"; sessions = 90; calendar_days = 90 }
  in
  let models =
    List.map
      (fun h ->
         Dio_survival.Survival_replay.blend_model_of
           ~horizon:h
           ~asset:series
           ~class_members:[ series ]
           ~kappa:200
           ~warmup:60
           ())
      [ h30; h90 ]
  in
  let c = cfg () in
  let open Dio_survival.Survival_replay in
  (match min_capital_for_horizons ~grid:c ~models ~target_survival:0.9 () with
   | None -> Alcotest.fail "expected a default capital"
   | Some capital ->
     Alcotest.(check bool)
       "positive and finite"
       (capital > 0.0 && Float.is_finite capital)
       true;
     let out = replay_series { c with start_quote = capital } series in
     Alcotest.(check bool) "fills occurred" (out.buy_fills > 0) true;
     Alcotest.(check bool) "survives beyond the first level" (out.d_surv > 0.01) true);
  (* An impossible target yields no default (caller must require --capital). *)
  let flat = mk_flat_series ~symbol:"M2" () in
  let m =
    Dio_survival.Survival_replay.blend_model_of
      ~horizon:h30
      ~asset:flat
      ~class_members:[ flat ]
      ~kappa:200
      ~warmup:60
      ()
  in
  Alcotest.(check bool)
    "no default on an impossible target"
    (min_capital_for_horizons ~grid:c ~models:[ m ] ~target_survival:0.9 () = None)
    true
;;

let test_f_blend_monotone () =
  (* F_blend is an empirical CDF on non-overlapping windows: monotone
     non-decreasing in d, bounded in [0, 1], so the inverse-sizing bisection
     is sound. *)
  let series = mk_series ~symbol:"I" ~sc:1.0 () in
  let horizon =
    { Dio_survival.Survival_types.label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_survival.Survival_replay.blend_model_of
      ~horizon
      ~asset:series
      ~class_members:[ series ]
      ~kappa:200
      ~warmup:60
      ()
  in
  let prev = ref (-1.0) in
  for i = 0 to 40 do
    let d = float_of_int i /. 40.0 in
    let f = Dio_survival.Survival_replay.blended_f model ~d in
    Alcotest.(check bool) "in [0,1]" (f >= 0.0 && f <= 1.0) true;
    Alcotest.(check bool) "non-decreasing" (f >= !prev -. 1e-12) true;
    prev := f
  done
;;

let test_default_stride_is_horizon () =
  (* The audit's P1 fix: coverage CDFs default to non-overlapping windows
     (stride = horizon), so a single crash is counted once, not once per
     rolling start. *)
  let series = mk_series ~symbol:"S" ~sc:1.0 () in
  let horizon =
    { Dio_survival.Survival_types.label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_survival.Survival_replay.blend_model_of
      ~horizon
      ~asset:series
      ~class_members:[ series ]
      ~kappa:200
      ~warmup:60
      ()
  in
  Alcotest.(check int) "default stride = horizon" 30 model.stride;
  let overlapping =
    Dio_survival.Survival_replay.blend_model_of
      ~horizon
      ~asset:series
      ~class_members:[ series ]
      ~kappa:200
      ~warmup:60
      ~stride:1
      ()
  in
  Alcotest.(check int) "explicit stride 1" 1 overlapping.stride
;;

let test_empty_distribution_raises () =
  (* A 100-bar series with a 100-session warmup hosts no MFD start at all:
     coverage evaluation must raise explicitly instead of defaulting to 0.0
     and feeding a bogus inverse-size. *)
  let series =
    series_of
      ~symbol:"E"
      (Array.init 100 (fun _ -> bar ~high:100.0 ~low:100.0 ~close:100.0 ()))
  in
  let horizon =
    { Dio_survival.Survival_types.label = "90d"; sessions = 90; calendar_days = 90 }
  in
  let model =
    Dio_survival.Survival_replay.blend_model_of
      ~horizon
      ~asset:series
      ~class_members:[ series ]
      ~kappa:200
      ~warmup:100
      ()
  in
  (try
     ignore (Dio_survival.Survival_replay.blended_f model ~d:0.5);
     Alcotest.fail "blended_f: expected Invalid_argument"
   with
   | Invalid_argument _ -> ());
  try
    ignore (Dio_survival.Survival_replay.blended_coverage model ~d_surv:0.5);
    Alcotest.fail "blended_coverage: expected Invalid_argument"
  with
  | Invalid_argument _ -> ()
;;

let () =
  Alcotest.run
    "grid_core_synthetic"
    [ ( "scenarios"
      , [ Alcotest.test_case "A flat market" `Quick test_scenario_a_flat_market
        ; Alcotest.test_case
            "B monotonic decline"
            `Quick
            test_scenario_b_monotonic_decline
        ; Alcotest.test_case "C dynamic scaling" `Quick test_scenario_c_dynamic_scaling
        ; Alcotest.test_case "D fee drain" `Quick test_scenario_d_fee_drain
        ; Alcotest.test_case
            "empirical min capital converges with floor"
            `Quick
            test_empirical_min_capital_converges_with_floor
        ; Alcotest.test_case
            "V-shaped recovery (ample capital)"
            `Quick
            test_scenario_v_shaped_recovery_ample_capital
        ; Alcotest.test_case
            "V-shaped recovery (tight capital)"
            `Quick
            test_scenario_v_shaped_recovery_tight_capital
        ; Alcotest.test_case
            "extreme multi-level crash"
            `Quick
            test_scenario_extreme_multi_level_crash
        ; Alcotest.test_case
            "insufficient capital"
            `Quick
            test_scenario_insufficient_capital
        ] )
    ; ( "invariants"
      , [ Alcotest.test_case "F_blend monotone" `Quick test_f_blend_monotone
        ; Alcotest.test_case
            "default stride is horizon"
            `Quick
            test_default_stride_is_horizon
        ; Alcotest.test_case
            "empty distribution raises"
            `Quick
            test_empty_distribution_raises
        ; Alcotest.test_case
            "impossible target explicitly unreachable"
            `Quick
            test_impossible_target_explicitly_unreachable
        ; Alcotest.test_case
            "blend weight is effective sample"
            `Quick
            test_blend_weight_is_effective_sample
        ; Alcotest.test_case
            "default capital is viable"
            `Quick
            test_default_capital_is_viable
        ] )
    ]
;;
