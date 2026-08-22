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
   All scenarios run through Grid_core.replay directly. *)
let lot = 1e-9

(* Must use the same float arithmetic as Grid_core.round_price / ceil_lot
   (multiply by 1e9, then divide), otherwise 1-ulp differences accumulate. *)
let round_tick x = Float.round (x *. 1e9) /. 1e9
let near ?(eps = 1e-6) a b = Alcotest.(check (float eps)) "approx" a b

let cfg
      ?(qty = 1.0)
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
  ; grid_interval_pct
  ; maker_fee = fee
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

let series_of ~symbol bars : Dio_oracle.Oracle_types.series =
  { Dio_oracle.Oracle_types.symbol
  ; calendar_kind = Dio_oracle.Oracle_types.Crypto
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
            : Dio_oracle.Oracle_types.bar))
        bars
  ; gaps = []
  }
;;

(* Bars-flavor path: raw Grid_core result over an inline bar array. *)
let replay_result c bars =
  Dio_strategies.Grid_core.replay
    c
    ~bars
    ~ordering:Dio_strategies.Grid_core_types.Buy_first
;;

(* Series-flavor adapter (the keeper behavior of the old strategy model):
   sort + dedup the bars, anchor the ladder at the path's first close, run
   Grid_core Buy_first, and report d_surv as 1.0 when the grid never
   exhausted. *)
type outcome =
  { d_surv : float
  ; exhausted : bool [@warning "-69"]
  ; halt_cause : [ `Capital | `Not_placeable ] option [@warning "-69"]
  ; buy_fills : int [@warning "-69"]
  ; sell_fills : int [@warning "-69"]
  }

let replay
      ?seed
      (g : Dio_strategies.Grid_core.config)
      (s : Dio_oracle.Oracle_types.series)
  : outcome
  =
  let module GC = Dio_oracle.Oracle_calendar in
  let bars =
    s.bars
    |> GC.sort_bars
    |> GC.dedup
    |> Array.map (fun (b : Dio_oracle.Oracle_types.bar) ->
      Dio_strategies.Grid_core_types.{ high = b.high; low = b.low; close = b.close })
  in
  let g =
    if Array.length bars = 0
    then g
    else { g with Dio_strategies.Grid_core.start_price = bars.(0).close }
  in
  let r =
    Dio_strategies.Grid_core.replay
      ?seed
      g
      ~bars
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  let d_surv =
    match r.Dio_strategies.Grid_core.first_exhaustion_price_drawdown with
    | Some d -> d
    | None -> 1.0
  in
  { d_surv
  ; exhausted = r.exhausted
  ; halt_cause = r.halt_cause
  ; buy_fills = r.buy_fills
  ; sell_fills = r.sell_fills
  }
;;

let test_scenario_a_flat_market () =
  let c = cfg ~start_quote:5_000.0 () in
  let bars = Array.init 20 (fun _ -> bar ~high:100.0 ~low:100.0 ~close:100.0 ()) in
  let res = replay_result c bars in
  let out = replay c (series_of ~symbol:"A" bars) in
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
  let res = replay_result c bars in
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
  (match res.first_exhaustion_price_drawdown with
   | Some dd -> near (1.0 -. (halt_level /. 100.0)) dd
   | None -> Alcotest.fail "expected capital-low drawdown");
  let out = replay c (series_of ~symbol:"B" bars) in
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
  let res = replay_result c bars in
  let n, quote, halt_level = ladder_oracle c ~start_quote:4_200.0 ~max_levels:n_bars in
  Alcotest.(check int) "oracle fills" n res.buy_fills;
  Alcotest.(check bool) "no sells" (res.sell_fills = 0) true;
  near quote res.final_quote;
  Alcotest.(check bool) "exhausted" true res.exhausted;
  Alcotest.(check bool) "halt cause is capital" (res.halt_cause = Some `Capital) true;
  (match res.first_exhaustion_price_drawdown with
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
  let res = replay_result c bars in
  let cycles, quote = oscillation_oracle c ~start_quote:150.0 ~max_cycles in
  Alcotest.(check int) "buy fills" cycles res.buy_fills;
  Alcotest.(check int) "sell fills" cycles res.sell_fills;
  near quote res.final_quote;
  Alcotest.(check bool) "capital drained" (res.final_quote < 150.0) true;
  Alcotest.(check bool) "quote never negative" (res.min_quote >= 0.0) true;
  Alcotest.(check bool) "exhausted" true res.exhausted;
  Alcotest.(check bool) "halt cause is capital" (res.halt_cause = Some `Capital) true
;;

(* ---- Scenario H: fee sensitivity on an infinite oscillation ---- *)

let test_scenario_h_fee_sensitivity () =
  (* The same oscillating path, two fee schedules: with zero fee every
     buy-sell cycle nets +gi (> 0), so the quote never drains and the grid
     runs forever; with a fee above gi/2 the identical path drains through
     fees alone and exhausts (scenario D). This pins the fee as the survival
     driver on flat-ish oscillations: a zero-fee grid must never run dry on
     an infinite oscillation. *)
  let mk_bars (c : Dio_strategies.Grid_core.config) ~cycles =
    let open Dio_strategies.Grid_core in
    let b = ref (buy_level c ~ref:c.start_price) in
    Array.init cycles (fun _ ->
      let s = sell_level c ~ref:!b in
      let b_ = !b in
      b := trail_buy_level c ~bid:b_ ~sell:s;
      Dio_strategies.Grid_core_types.{ high = s; low = b_; close = b_ })
  in
  let cycles = 300 in
  let free = cfg ~grid_interval_pct:0.1 ~fee:0.0 ~start_quote:150.0 () in
  let res_free = replay_result free (mk_bars free ~cycles) in
  Alcotest.(check int) "zero-fee buys" cycles res_free.buy_fills;
  Alcotest.(check int) "zero-fee sells" cycles res_free.sell_fills;
  Alcotest.(check bool) "zero-fee never exhausts" (not res_free.exhausted) true;
  Alcotest.(check bool)
    "zero-fee quote grows (positive grid carry)"
    (res_free.final_quote > 150.0)
    true;
  let priced = cfg ~grid_interval_pct:0.1 ~fee:0.002 ~start_quote:150.0 () in
  let res_priced = replay_result priced (mk_bars priced ~cycles) in
  Alcotest.(check bool) "fee grid drains to exhaustion" true res_priced.exhausted;
  Alcotest.(check bool)
    "fee grid's halt cause is capital"
    (res_priced.halt_cause = Some `Capital)
    true
;;

(* ---- Scenario I: grid spacing vs survival depth ---- *)

let test_scenario_i_grid_spacing () =
  (* Same capital and qty, two grid spacings, on an identical monotonic
     decline: a wider grid takes larger price steps per rung, so the same
     capital funds more price distance before running dry - the exhaustion
     drawdown (and the fill count) must be strictly deeper for the wider
     grid. *)
  let replay_with gi =
    let open Dio_strategies.Grid_core in
    (* 800 funds ~8 rungs at gi 1% but ~10 at gi 5%: the wider ladder's cost
       sum converges slower, so the same capital buys more price distance. *)
    let c = cfg ~grid_interval_pct:gi ~start_quote:800.0 () in
    let n_bars = 200 in
    let b = ref (buy_level c ~ref:c.start_price) in
    let bars =
      Array.init n_bars (fun _ ->
        let b_ = !b in
        b := trail_buy_level c ~bid:b_ ~sell:(sell_level c ~ref:b_);
        bar ~high:(b_ *. 1.005) ~low:b_ ())
    in
    ( c
    , bars
    , Dio_strategies.Grid_core.replay
        c
        ~bars
        ~ordering:Dio_strategies.Grid_core_types.Buy_first )
  in
  let c_narrow, bars_n, res_narrow = replay_with 1.0 in
  let c_wide, bars_w, res_wide = replay_with 5.0 in
  Alcotest.(check bool)
    "both exhaust on the monotonic decline"
    (res_narrow.exhausted && res_wide.exhausted)
    true;
  Alcotest.(check bool)
    "wider grid fills more rungs"
    (res_wide.buy_fills > res_narrow.buy_fills)
    true;
  let dd (res : Dio_strategies.Grid_core.result) =
    match res.first_exhaustion_price_drawdown with
    | Some d -> d
    | None -> 1.0
  in
  Alcotest.(check bool)
    "wider grid survives a deeper price drop"
    (dd res_wide > dd res_narrow)
    true;
  (* The same ordering holds through the replay's D_surv extraction. *)
  let out_narrow = replay c_narrow (series_of ~symbol:"N" bars_n) in
  let out_wide = replay c_wide (series_of ~symbol:"W" bars_w) in
  Alcotest.(check bool)
    "replay D_surv deeper for the wider grid"
    (out_wide.d_surv > out_narrow.d_surv)
    true
;;


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
  let res = replay_result c bars in
  let out = replay c (series_of ~symbol:"V" bars) in
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
  let res = replay_result c bars in
  let out = replay c (series_of ~symbol:"V2" bars) in
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
  let res = replay_result c bars in
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
     fills, D_surv = the grid interval. The ladder anchors at the path's
     first close (the strategy starts where the history starts), so the
     second bar dips below the first rung and triggers the unaffordable
     first buy. *)
  let c = cfg ~start_quote:50.0 () in
  let bars = [| bar ~high:100.0 ~low:99.0 (); bar ~high:98.5 ~low:95.0 () |] in
  let res = replay_result c bars in
  let out = replay c (series_of ~symbol:"H" bars) in
  Alcotest.(check int) "no buys" 0 res.buy_fills;
  Alcotest.(check bool) "exhausted" true res.exhausted;
  Alcotest.(check bool) "halt cause is capital" (res.halt_cause = Some `Capital) true;
  near 0.01 out.d_surv
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
        ; Alcotest.test_case "H fee sensitivity" `Quick test_scenario_h_fee_sensitivity
        ; Alcotest.test_case "I grid spacing" `Quick test_scenario_i_grid_spacing
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
    ]
;;
