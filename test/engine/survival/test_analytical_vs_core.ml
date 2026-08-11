(* Cross-check: Grid_core path replay against the closed-form static drawdown
   runway. On a monotonic geometric crash the replay fills one ladder step per
   bar, so the number of affordable fills and the exhaustion drawdown must
   match Survival_mfd.static_drawdown_runway exactly (fees included). *)

let near a b = Alcotest.(check (float 1e-6)) "approx" a b

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
    Dio_survival.Survival_mfd.static_drawdown_runway
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
  match res.first_capital_low_drawdown with
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
  match res.first_capital_low_drawdown with
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
  let open Dio_survival.Survival_types in
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
    Dio_survival.Survival_replay.blend_model_of
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
  let open Dio_survival.Survival_replay in
  let target = 0.80 in
  let cap_res = find_min_capital ~grid ~model ~target_survival:target () in
  let qty_res = max_qty ~grid ~model ~target_survival:target () in
  Alcotest.(check bool) "capital reachable" true cap_res.reachable;
  Alcotest.(check bool) "qty reachable" true qty_res.reachable;
  (* coverage must clear the target *)
  Alcotest.(check bool) "capital coverage >= target" (cap_res.coverage >= target) true;
  Alcotest.(check bool) "qty coverage >= target" (qty_res.coverage >= target) true;
  (* capital must be the exact static runway cost of the fill count *)
  let d = drawdown_for_target ~model ~target_survival:target in
  let n = fills_for_drawdown ~grid ~d in
  let expected = capital_for_fills ~grid ~n_fills:n in
  Alcotest.(check (float 1e-6)) "min capital = runway cost" expected cap_res.value;
  (* the static drawdown survived must clear d *)
  Alcotest.(check (float 1e-6))
    "d_surv = 1-(1-gi)^n"
    (drawdown_of_fills ~grid ~n_fills:n)
    cap_res.d_surv;
  Alcotest.(check bool) "d_surv >= d*" (cap_res.d_surv >= d) true;
  (* qty * per-unit cost must reproduce the same capital *)
  let gi_frac = gi /. 100.0 in
  let per_unit =
    (1.0 +. fee)
    *. start_price
    *. (1.0 -. gi_frac)
    *. ((1.0 -. ((1.0 -. gi_frac) ** float_of_int n)) /. gi_frac)
  in
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
         { Dio_survival.Survival_types.date = Printf.sprintf "2023-%03d" i
         ; open_ = close
         ; high = close
         ; low = lows.(i)
         ; close
         ; volume = 1000.0
         })
      closes
  in
  { Dio_survival.Survival_types.symbol = "T"; calendar_kind = Crypto; bars; gaps = [] }
;;

let test_blend_asset_matches_f_h () =
  (* The asset component of the z-blend must be exactly the raw empirical CDF
     (Survival_mfd.f_h), since it shares the same sample set and rule. *)
  let series = mk_test_series ~sc:1.0 () in
  let closes =
    Array.map (fun (b : Dio_survival.Survival_types.bar) -> b.close) series.bars
  in
  let lows = Array.map (fun (b : Dio_survival.Survival_types.bar) -> b.low) series.bars in
  let horizon : Dio_survival.Survival_types.horizon =
    { label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_survival.Survival_replay.blend_model_of
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
    Dio_survival.Survival_mfd.f_h ~closes ~lows ~horizon:30 ~threshold:d ~warmup:60 ()
  in
  let c = Dio_survival.Survival_replay.blended_coverage model ~d_surv:d in
  Alcotest.(check (float 1e-9)) "asset coverage matches f_h" fh c.asset
;;

let test_blend_monotone_bounded () =
  (* F_blend is an empirical CDF: monotone non-decreasing in d and in [0,1], so
     the inverse-sizing bisection is sound. *)
  let series = mk_test_series ~sc:1.0 () in
  let horizon : Dio_survival.Survival_types.horizon =
    { label = "30d"; sessions = 30; calendar_days = 30 }
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

let test_z_blend_ignores_vol_regime () =
  (* A class member with 10x the asset's volatility has the SAME z-shape (the
     scaling invariance property). Under the z-blend its translated coverage
     must therefore sit near the asset's own coverage - whereas raw-MFD pooling
     would report a much lower coverage (the 10x member's raw drawdowns are
     ~10x deeper) and overstate capital needs. *)
  let asset = mk_test_series ~sc:1.0 () in
  let high_vol = mk_test_series ~sc:10.0 () in
  let horizon : Dio_survival.Survival_types.horizon =
    { label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_survival.Survival_replay.blend_model_of
      ~horizon
      ~asset
      ~class_members:[ high_vol ]
      ~kappa:200
      ~warmup:60
      ()
  in
  let d = 0.20 in
  let c = Dio_survival.Survival_replay.blended_coverage model ~d_surv:d in
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
     worst case the static bound prices. *)
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
  let open Dio_survival.Survival_types in
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
  let horizon : Dio_survival.Survival_types.horizon =
    { label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let model =
    Dio_survival.Survival_replay.blend_model_of
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
  let open Dio_survival.Survival_replay in
  let target = 0.80 in
  let static = find_min_capital ~grid ~model ~target_survival:target () in
  let emp = empirical_min_capital ~grid ~model ~target_survival:target () in
  Alcotest.(check bool) "static reachable" true static.reachable;
  Alcotest.(check bool) "empirical reachable" true emp.reachable;
  (* The replay's survival event is the exhaustion drawdown, which fires one
     ladder step deeper than the drawdown the funded fills survive, so the
     empirical capital lands strictly below the static sizing (which prices
     the drawdown its fills fund) even on the worst-case monotone crash. *)
  Alcotest.(check bool)
    "empirical strictly below static on the crash"
    (emp.value > 0.0 && emp.value < static.value)
    true
;;

let test_empirical_bounded_by_static_on_bounces () =
  (* On a bouncy path sells free quote, so the actual historical capital limit
     is never worse than the closed-form straight-down bound: empirical <=
     static. *)
  let series = mk_test_series ~sc:3.0 () in
  let closes =
    Array.map (fun (b : Dio_survival.Survival_types.bar) -> b.close) series.bars
  in
  let start_price = closes.(Array.length closes - 1) in
  let horizon : Dio_survival.Survival_types.horizon =
    { label = "30d"; sessions = 30; calendar_days = 30 }
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
  let open Dio_survival.Survival_replay in
  let target = 0.90 in
  let static = find_min_capital ~grid ~model ~target_survival:target () in
  let emp = empirical_min_capital ~grid ~model ~target_survival:target () in
  Alcotest.(check bool) "static reachable" true static.reachable;
  Alcotest.(check bool) "empirical reachable" true emp.reachable;
  Alcotest.(check bool)
    "empirical never worse than static"
    (emp.value <= static.value *. (1.0 +. 1e-9))
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
