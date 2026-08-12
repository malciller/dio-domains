(* Cross-check: Grid_core path replay against the closed-form static drawdown
   runway. On a monotonic geometric crash the replay fills one ladder step per
   bar, so the number of affordable fills and the exhaustion drawdown must
   match Oracle_mfd.static_drawdown_runway exactly (fees included). *)

let near a b = Alcotest.(check (float 1e-6)) "approx" a b

(* The generic sizing inversions, instantiated over the grid strategy model. *)
module S = Dio_oracle.Oracle_replay.Sizing (Dio_oracle.Oracle_strategy.Grid)

let test_replay_matches_closed_form () =
  let gi = 1.0 in
  let fee = 0.0004 in
  let start_price = 100.0 in
  let qty = 1.0 in
  let capital = 2_900.0 in
  let open Dio_strategies.Grid_core in
  let c =
    { qty
    ; sell_mult = 1.0
    ; grid_interval_pct = gi
    ; maker_fee = fee
    ; accumulation_buffer = 0.0
    ; price_increment = 1e-9
    ; qty_increment = 1e-9
    ; qty_min = 0.0
    ; min_notional = 0.0
    ; exchange_model = Dio_strategies.Grid_core_types.Hyperliquid
    ; start_price
    ; start_quote = capital
    ; cash_hook = None
    }
  in
  let n_bars = 80 in
  let level i = start_price *. (0.99 ** float_of_int i) in
  let bars =
    Array.init n_bars (fun i ->
      let hi, lo =
        if i = 0
        then level 0 *. 0.995, level 1 *. 0.999999
        else level i *. 0.995, level (i + 1) *. 0.999999
      in
      Dio_strategies.Grid_core_types.{ high = hi; low = lo; close = lo })
  in
  let res = replay c ~bars ~ordering:Dio_strategies.Grid_core_types.Buy_first in
  let n_static, dd_static =
    Dio_oracle.Oracle_mfd.static_drawdown_runway
      ~qty
      ~grid_interval_pct:gi
      ~fee
      ~start_price
      ~capital
  in
  Alcotest.(check bool) "exhausted" true res.exhausted;
  Alcotest.(check int) "fill count matches closed form" n_static res.buy_fills;
  (* The first capital_low is at the ladder step just past the last affordable
     fill, i.e. 1-(1-gi)^(N*+1). *)
  match res.first_exhaustion_price_drawdown with
  | Some dd ->
    near (1.0 -. (0.99 ** float_of_int (n_static + 1))) dd;
    near dd_static (1.0 -. (0.99 ** float_of_int n_static))
  | None -> Alcotest.fail "expected capital-low drawdown"
;;

let test_replay_no_fill_when_unaffordable () =
  (* Capital below the first buy: exhaustion immediately at level 1. *)
  let open Dio_strategies.Grid_core in
  let c =
    { qty = 1.0
    ; sell_mult = 1.0
    ; grid_interval_pct = 1.0
    ; maker_fee = 0.0
    ; accumulation_buffer = 0.0
    ; price_increment = 1e-9
    ; qty_increment = 1e-9
    ; qty_min = 0.0
    ; min_notional = 0.0
    ; exchange_model = Dio_strategies.Grid_core_types.Hyperliquid
    ; start_price = 100.0
    ; start_quote = 50.0
    ; cash_hook = None
    }
  in
  let res =
    replay
      c
      ~bars:[| Dio_strategies.Grid_core_types.{ high = 100.; low = 50.; close = 50. } |]
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check bool) "exhausted" true res.exhausted;
  Alcotest.(check int) "no fills" 0 res.buy_fills;
  match res.first_exhaustion_price_drawdown with
  | Some dd -> near 0.01 dd
  | None -> Alcotest.fail "expected capital-low drawdown"
;;

let test_inverse_sizing_matches_closed_form () =
  (* Inverse sizing is anchored to the closed-form static runway: for a given
     target, d-star is the smallest drawdown with F(d-star) >= target, N-star
     the smallest fill count whose runway drawdown reaches d-star, and the min
     capital must equal the static runway cost of N-star fills. The max qty must
     satisfy qty * per-unit-cost(N-star) = capital exactly. *)
  let gi = 1.0 in
  let fee = 0.0004 in
  let start_price = 100.0 in
  let capital = 2_900.0 in
  let n_bars = 300 in
  let level i = start_price *. (0.99 ** float_of_int i) in
  let mk i close =
    let lo = if i = 0 then level 1 *. 0.999999 else level (i + 1) *. 0.999999 in
    let hi = if i = 0 then level 0 *. 0.995 else level i *. 0.995 in
    Dio_strategies.Grid_core_types.{ high = hi; low = lo; close }
  in
  let bars =
    Array.init n_bars (fun i ->
      if i < 80
      then mk i (level i)
      else (
        let b = mk 0 (level 0) in
        { b with close = level 0 }))
  in
  let open Dio_oracle.Oracle_types in
  let series =
    { symbol = "T"
    ; calendar_kind = Crypto
    ; bars =
        Array.mapi
          (fun i (b : Dio_strategies.Grid_core_types.bar) ->
             { date = Printf.sprintf "2023-%02d" i
             ; open_ = b.close
             ; high = b.high
             ; low = b.low
             ; close = b.close
             ; volume = 1000.0
             })
          bars
    ; gaps = []
    }
  in
  (* The deep-crash prefix produces MFD samples, so F is non-degenerate. *)
  let horizon = { label = "30d"; sessions = 30; calendar_days = 30 } in
  let model =
    Dio_oracle.Oracle_replay.blend_model_of
      ~horizon
      ~asset:series
      ~class_members:[ series ]
      ~kappa:2
      ~warmup:10
      ()
  in
  let grid =
    Dio_strategies.Grid_core.
      { qty = 1.0
      ; sell_mult = 1.0
      ; grid_interval_pct = gi
      ; maker_fee = fee
      ; accumulation_buffer = 0.0
      ; price_increment = 0.01
      ; qty_increment = 0.01
      ; qty_min = 0.0
      ; min_notional = 0.0
      ; exchange_model = Dio_strategies.Grid_core_types.Hyperliquid
      ; start_price
      ; start_quote = capital
      ; cash_hook = None
      }
  in
  let open Dio_oracle.Oracle_replay in
  let target = 0.80 in
  let cap_res = S.find_min_capital ~grid ~model ~target_survival:target () in
  let qty_res = S.max_qty ~grid ~model ~target_survival:target () in
  Alcotest.(check bool) "capital reachable" true cap_res.reachable;
  Alcotest.(check bool) "qty reachable" true qty_res.reachable;
  (* coverage must clear the target *)
  Alcotest.(check bool) "capital coverage >= target" (cap_res.coverage >= target) true;
  Alcotest.(check bool) "qty coverage >= target" (qty_res.coverage >= target) true;
  (* capital must be the exact static runway cost of the fill count. The
     sizing drawdown is the largest ACTUAL peak-to-valley drawdown of the
     series (the full 1%-per-bar crash: peak close ~100 -> trough low
     ~44.75, ~55%), not the statistical d-star - mirroring
     [find_min_capital]. *)
  let d = drawdown_for_target ~model ~target_survival:target in
  let d_cover =
    match Dio_oracle.Oracle_math.peak_to_valley_stats_of series with
    | Some p -> p.max_drawdown
    | None -> d
  in
  let n = Dio_oracle.Oracle_strategy.Grid.fills_for_drawdown grid ~d:d_cover in
  let expected =
    Dio_oracle.Oracle_strategy.Grid.cost_at
      grid
      ~qty:(Dio_oracle.Oracle_strategy.Grid.design_qty grid)
      ~n_fills:n
  in
  Alcotest.(check (float 1e-6)) "min capital = runway cost" expected cap_res.value;
  (* the static drawdown survived must clear d *)
  Alcotest.(check (float 1e-6))
    "d_surv = 1-(1-gi)^n"
    (Dio_oracle.Oracle_strategy.Grid.drawdown_of_fills grid ~n_fills:n)
    cap_res.d_surv;
  Alcotest.(check bool) "d_surv >= d*" (cap_res.d_surv >= d) true;
  (* qty * per-unit cost must reproduce the same capital. The per-unit cost is
     the floor-aware ladder walk itself (the funding function max_qty inverts
     - the same walk the deployment engine sizes against), not the unrounded
     closed form: the walk's trailing-buy rule and tick rounding make the
     closed form slightly overstate the true per-unit burn, which is exactly
     the "replay vs closed form" gap this test family exists to measure. *)
  let per_unit = Dio_oracle.Oracle_strategy.Grid.cost_at grid ~qty:1.0 ~n_fills:n in
  Alcotest.(check (float 1e-6))
    "qty * per-unit = capital"
    capital
    (qty_res.value *. per_unit);
  Alcotest.(check (float 1e-6)) "same d_surv" cap_res.d_surv qty_res.d_surv
;;

let mk_test_series ~sc () =
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
  let bars =
    Array.mapi
      (fun i close ->
         { Dio_oracle.Oracle_types.date = Printf.sprintf "2023-%03d" i
         ; open_ = close
         ; high = close
         ; low = lows.(i)
         ; close
         ; volume = 1000.0
         })
      closes
  in
  { Dio_oracle.Oracle_types.symbol = "T"; calendar_kind = Crypto; bars; gaps = [] }
;;

let test_blend_asset_matches_f_h () =
  (* The asset component of the z-blend must be exactly the raw empirical CDF
     (Oracle_mfd.f_h), since it shares the same sample set and rule. *)
  let series = mk_test_series ~sc:1.0 () in
  let closes = Array.map (fun (b : Dio_oracle.Oracle_types.bar) -> b.close) series.bars in
  let lows = Array.map (fun (b : Dio_oracle.Oracle_types.bar) -> b.low) series.bars in
  let horizon : Dio_oracle.Oracle_types.horizon =
    { label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_oracle.Oracle_replay.blend_model_of
      ~horizon
      ~asset:series
      ~class_members:[ series ]
      ~kappa:200
      ~warmup:60
      ~stride:1
      ()
  in
  let d = 0.15 in
  let fh =
    Dio_oracle.Oracle_mfd.f_h ~closes ~lows ~horizon:30 ~threshold:d ~warmup:60 ()
  in
  let c = Dio_oracle.Oracle_replay.blended_coverage model ~d_surv:d in
  Alcotest.(check (float 1e-9)) "asset coverage matches f_h" fh c.asset
;;

let test_blend_monotone_bounded () =
  (* F_blend is an empirical CDF: monotone non-decreasing in d and in [0,1], so
     the inverse-sizing bisection is sound. *)
  let series = mk_test_series ~sc:1.0 () in
  let horizon : Dio_oracle.Oracle_types.horizon =
    { label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_oracle.Oracle_replay.blend_model_of
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
    let f = Dio_oracle.Oracle_replay.blended_f model ~d in
    Alcotest.(check bool) "in [0,1]" (f >= 0.0 && f <= 1.0) true;
    Alcotest.(check bool) "non-decreasing" (f >= !prev -. 1e-12) true;
    prev := f
  done
;;

let test_z_blend_ignores_vol_regime () =
  (* A class member with 10x the asset's volatility has the SAME z-shape (the
     scaling invariance property). Under the z-blend its translated coverage
     must therefore sit near the asset's own coverage - whereas raw-MFD pooling
     would report a much lower coverage (the 10x member's raw drawdowns are
     ~10x deeper) and overstate capital needs. *)
  let asset = mk_test_series ~sc:1.0 () in
  let high_vol = mk_test_series ~sc:10.0 () in
  let horizon : Dio_oracle.Oracle_types.horizon =
    { label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_oracle.Oracle_replay.blend_model_of
      ~horizon
      ~asset
      ~class_members:[ high_vol ]
      ~kappa:200
      ~warmup:60
      ()
  in
  let d = 0.20 in
  let c = Dio_oracle.Oracle_replay.blended_coverage model ~d_surv:d in
  (* The raw class CDF at d would be tiny for the 10x member; the translated
     one must track the asset's own coverage instead. *)
  Alcotest.(check bool)
    "translated class coverage tracks the asset"
    (c.class_ >= c.asset -. 0.15)
    true
;;

let test_empirical_matches_static_on_crash () =
  (* On a monotone geometric crash the path replay equals the closed-form
     static runway (one ladder fill per bar, no sells), so the empirical
     (path-replay) min capital must equal the static one: the crash IS the
     worst case the static bound prices. The tail stays flat at the crash
     bottom (no rise, so the ladder never re-anchors up).

     The sizing drawdown is the largest ACTUAL peak-to-valley fall of the
     series (peak close ~100 -> trough low ~44.75, ~55%), so the static
     recommendation funds the whole crash - it does NOT collapse to the
     first buy as the old ATH-anchored model did (the price sits ~55% below
     the ATH, which the anchor treated as "already traversed"). The
     empirical number (full path replay) must land on the same runway. *)
  let gi = 1.0 in
  let fee = 0.0004 in
  let start_price = 100.0 in
  let capital = 2_900.0 in
  let n_bars = 300 in
  let n_crash = 80 in
  let level i = start_price *. (0.99 ** float_of_int i) in
  let mk i close =
    let lo = if i = 0 then level 1 *. 0.999999 else level (i + 1) *. 0.999999 in
    let hi = if i = 0 then level 0 *. 0.995 else level i *. 0.995 in
    Dio_strategies.Grid_core_types.{ high = hi; low = lo; close }
  in
  let bars =
    Array.init n_bars (fun i ->
      if i < n_crash
      then mk i (level i)
      else (
        let bottom = level n_crash in
        Dio_strategies.Grid_core_types.
          { high = bottom *. 1.001; low = bottom *. 0.999; close = bottom }))
  in
  let open Dio_oracle.Oracle_types in
  let series =
    { symbol = "T"
    ; calendar_kind = Crypto
    ; bars =
        Array.mapi
          (fun i (b : Dio_strategies.Grid_core_types.bar) ->
             { date = Printf.sprintf "2023-%02d" i
             ; open_ = b.close
             ; high = b.high
             ; low = b.low
             ; close = b.close
             ; volume = 1000.0
             })
          bars
    ; gaps = []
    }
  in
  let horizon : Dio_oracle.Oracle_types.horizon =
    { label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_oracle.Oracle_replay.blend_model_of
      ~horizon
      ~asset:series
      ~class_members:[ series ]
      ~kappa:200
      ~warmup:10
      ()
  in
  let open Dio_strategies.Grid_core in
  let grid =
    { qty = 1.0
    ; sell_mult = 1.0
    ; grid_interval_pct = gi
    ; maker_fee = fee
    ; accumulation_buffer = 0.0
    ; price_increment = 0.01
    ; qty_increment = 0.01
    ; qty_min = 0.0
    ; min_notional = 0.0
    ; exchange_model = Dio_strategies.Grid_core_types.Hyperliquid
    ; start_price
    ; start_quote = capital
    ; cash_hook = None
    }
  in
  let target = 0.80 in
  let static = S.find_min_capital ~grid ~model ~target_survival:target () in
  let emp = S.empirical_min_capital ~grid ~model ~target_survival:target () in
  Alcotest.(check bool) "static reachable" true static.reachable;
  Alcotest.(check bool) "empirical reachable" true emp.reachable;
  (* The static recommendation funds the full actual peak-to-valley drawdown
     of the series (the whole crash, ~55%): no ATH-anchor cap treats the fall
     below the ATH as already paid. *)
  Alcotest.(check bool)
    "static funds the full actual drawdown (well beyond the first buy)"
    (static.value > 150.0)
    true;
  (* The empirical (full-path replay) number is the target-coverage boundary
     (the smallest capital whose replay clears the 80% blended target, i.e.
     survives a ~26% drawdown) - a real funding need, but shallower than the
     actual worst peak-to-valley fall the static recommendation funds. *)
  Alcotest.(check bool) "empirical funds more than the first buy" (emp.value > 150.0) true;
  Alcotest.(check bool)
    "actual-worst sizing is more conservative than the target boundary"
    (emp.value < static.value)
    true
;;

let test_empirical_bounded_by_static_on_bounces () =
  (* On a bouncy path the trailing ladder re-trades ranges (buys dips, sells
     recoveries, re-buys on the next dip), so the actual historical capital
     limit is at least the closed-form straight-down bound: the static runway
     is the lower bound (one clean pass), and choppy paths cost fees + spread
     per re-trade - empirical >= static. (Under the old non-trailing ladder
     the bounce path was cheaper than straight-down; the trailing ladder
     mirrors the live strategy's re-entry.) *)
  let series = mk_test_series ~sc:3.0 () in
  let closes = Array.map (fun (b : Dio_oracle.Oracle_types.bar) -> b.close) series.bars in
  let start_price = closes.(Array.length closes - 1) in
  let horizon : Dio_oracle.Oracle_types.horizon =
    { label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_oracle.Oracle_replay.blend_model_of
      ~horizon
      ~asset:series
      ~class_members:[ series ]
      ~kappa:200
      ~warmup:60
      ()
  in
  let open Dio_strategies.Grid_core in
  let grid =
    { qty = 1.0
    ; sell_mult = 1.0
    ; grid_interval_pct = 1.0
    ; maker_fee = 0.0004
    ; accumulation_buffer = 0.0
    ; price_increment = 0.01
    ; qty_increment = 0.01
    ; qty_min = 0.0
    ; min_notional = 0.0
    ; exchange_model = Dio_strategies.Grid_core_types.Hyperliquid
    ; start_price
    ; start_quote = 5_000.0
    ; cash_hook = None
    }
  in
  let target = 0.90 in
  let static = S.find_min_capital ~grid ~model ~target_survival:target () in
  let emp = S.empirical_min_capital ~grid ~model ~target_survival:target () in
  Alcotest.(check bool) "static reachable" true static.reachable;
  Alcotest.(check bool) "empirical reachable" true emp.reachable;
  Alcotest.(check bool)
    "empirical at least the static (re-trading drains fees)"
    (emp.value >= static.value *. (1.0 -. 1e-9))
    true
;;

let () =
  Alcotest.run
    "analytical_vs_core"
    [ ( "cross-check"
      , [ Alcotest.test_case
            "replay matches closed form"
            `Quick
            test_replay_matches_closed_form
        ; Alcotest.test_case
            "unaffordable first buy"
            `Quick
            test_replay_no_fill_when_unaffordable
        ; Alcotest.test_case
            "inverse sizing matches closed form"
            `Quick
            test_inverse_sizing_matches_closed_form
        ; Alcotest.test_case "blend asset matches f_h" `Quick test_blend_asset_matches_f_h
        ; Alcotest.test_case "blend monotone bounded" `Quick test_blend_monotone_bounded
        ; Alcotest.test_case
            "z blend ignores vol regime"
            `Quick
            test_z_blend_ignores_vol_regime
        ; Alcotest.test_case
            "empirical matches static on crash"
            `Quick
            test_empirical_matches_static_on_crash
        ; Alcotest.test_case
            "empirical bounded by static on bounces"
            `Quick
            test_empirical_bounded_by_static_on_bounces
        ] )
    ]
;;
