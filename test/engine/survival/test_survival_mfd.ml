(* Survival MFD math tests. *)

let near a b = Alcotest.(check (float 1e-9)) "approx" a b

let test_mfd_basic () =
  let closes = [| 100.; 100.; 100.; 100. |] in
  let lows = [| 100.; 98.; 96.; 94. |] in
  (* MFD(0,3) = 1 - min(lows[1..3])/close[0] = 1 - 94/100 *)
  near 0.06 (Option.get (Dio_survival.Survival_mfd.mfd ~closes ~lows ~start:0 ~horizon:3));
  (* Half-open: MFD(0,1) = 1 - lows[1]/100 = 0.02 *)
  near 0.02 (Option.get (Dio_survival.Survival_mfd.mfd ~closes ~lows ~start:0 ~horizon:1))
;;

let test_mfd_uses_only_lows () =
  let closes = [| 100.; 100.; 100.; 100. |] in
  let lows = [| 100.; 100.; 99.9; 100. |] in
  near
    0.001
    (Option.get (Dio_survival.Survival_mfd.mfd ~closes ~lows ~start:0 ~horizon:3))
;;

let test_f_h_and_survival () =
  (* Two starts (s=0 and s=1); each has MFD exactly 0.10 over its 3-session
     window (min low 90 / close 100). So F(0.05)=0 / S=1, and F(0.10)=1. *)
  let closes = [| 100.; 100.; 100.; 100. |] in
  let lows = [| 100.; 99.; 90.; 95. |] in
  near
    0.0
    (Dio_survival.Survival_mfd.f_h ~closes ~lows ~horizon:3 ~threshold:0.05 ~warmup:0 ());
  near
    1.0
    (Dio_survival.Survival_mfd.survival
       ~closes
       ~lows
       ~horizon:3
       ~threshold:0.05
       ~warmup:0
       ());
  near
    1.0
    (Dio_survival.Survival_mfd.f_h ~closes ~lows ~horizon:3 ~threshold:0.10 ~warmup:0 ())
;;

let test_surface () =
  let closes = Array.make 120 100.0 in
  let lows = Array.make 120 100.0 in
  for i = 0 to 119 do
    lows.(i) <- 100.0 -. (float_of_int i *. 0.01)
  done;
  let h =
    { Dio_survival.Survival_types.label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let s : Dio_survival.Survival_types.survival_surface =
    Dio_survival.Survival_mfd.surface
      ~closes
      ~lows
      ~horizon:h
      ~thresholds_pct:[ 5.; 10.; 30. ]
      ~warmup:60
  in
  let row =
    List.find
      (fun (r : Dio_survival.Survival_types.surface_row) -> r.drawdown_pct = 30.0)
      s.rows
  in
  (* Low falls ~0.01/session; over 30 sessions worst is ~0.30, so F(30%) ~ 1. *)
  Alcotest.(check bool) "30% covered" (row.coverage > 0.9) true
;;

let test_stride_sampling () =
  let closes = Array.make 100 100.0 in
  let lows = Array.make 100 100.0 in
  (* Valid stride-1 starts are s in [10, 98] = 89 windows (s=99 has no room
     for the half-open window). *)
  Alcotest.(check int)
    "stride 1 windows"
    89
    (Dio_survival.Survival_mfd.n_starts ~closes ~lows ~horizon:30 ~warmup:10 ());
  (* stride 30 -> starts 10, 40, 70 -> 3 non-overlapping windows. *)
  Alcotest.(check int)
    "stride 30 windows"
    3
    (Dio_survival.Survival_mfd.n_starts
       ~closes
       ~lows
       ~horizon:30
       ~warmup:10
       ~stride:30
       ());
  (* Percentile tables estimate from the non-overlapping basis and report both
     the raw (overlapping) and effective window counts. *)
  let h =
    { Dio_survival.Survival_types.label = "30d"; sessions = 30; calendar_days = 30 }
  in
  let t : Dio_survival.Survival_types.percentile_table =
    Dio_survival.Survival_mfd.percentile_table
      ~closes
      ~lows
      ~horizon:h
      ~percentiles:[ 50.; 99. ]
      ~warmup:10
  in
  Alcotest.(check int) "table n_starts" 89 t.n_starts;
  Alcotest.(check int) "table n_eff" 3 t.n_eff
;;

let test_static_runway () =
  (* gi=1%, price=100, qty=1, no fee, capital=1000.
     First buy at 99 -> 10 ladder steps * ~95 avg ~ 950 < 1000, 11th crosses. *)
  let n, dd =
    Dio_survival.Survival_mfd.static_drawdown_runway
      ~qty:1.0
      ~grid_interval_pct:1.0
      ~fee:0.0
      ~start_price:100.0
      ~capital:1_000.0
  in
  Alcotest.(check bool) "within range" (n >= 8 && n <= 12) true;
  (* drawdown = 1 - (0.99)^n *)
  near (1.0 -. (0.99 ** float_of_int n)) dd
;;

let test_runway_cost_accumulates () =
  let c1 =
    Dio_survival.Survival_mfd.static_runway_cost
      ~qty:1.0
      ~grid_interval_pct:1.0
      ~fee:0.0
      ~start_price:100.0
      ~n_fills:1
  in
  near 99.0 c1;
  let c2 =
    Dio_survival.Survival_mfd.static_runway_cost
      ~qty:1.0
      ~grid_interval_pct:1.0
      ~fee:0.0
      ~start_price:100.0
      ~n_fills:2
  in
  near (99.0 +. 98.01) c2
;;

let test_f_h_raises_on_no_valid_starts () =
  (* A 100-session series with a 100-session warmup hosts no MFD start: F_h
     must raise, not return 0.0 - a coverage of 0.0 from zero observations is
     meaningless and must not masquerade as "every window drew down". *)
  let closes = Array.make 100 100.0 in
  let lows = Array.make 100 100.0 in
  (try
     ignore
       (Dio_survival.Survival_mfd.f_h
          ~closes
          ~lows
          ~horizon:90
          ~threshold:0.1
          ~warmup:100
          ());
     Alcotest.fail "f_h: expected Invalid_argument"
   with
   | Invalid_argument _ -> ());
  (* The percentile table on the same short history raises too (its samples
     are empty). *)
  let h =
    { Dio_survival.Survival_types.label = "90d"; sessions = 90; calendar_days = 90 }
  in
  try
    ignore
      (Dio_survival.Survival_mfd.percentile_table
         ~closes
         ~lows
         ~horizon:h
         ~percentiles:[ 50. ]
         ~warmup:100);
    Alcotest.fail "percentile_table: expected Invalid_argument"
  with
  | Invalid_argument _ -> ()
;;

let test_runway_monotone_in_capital () =
  (* Invariant: more capital cannot reduce the affordable fill count or the
     static drawdown the grid survives. *)
  let rec loop c =
    if c <= 2000
    then (
      let n1, dd1 =
        Dio_survival.Survival_mfd.static_drawdown_runway
          ~qty:1.0
          ~grid_interval_pct:1.0
          ~fee:0.0
          ~start_price:100.0
          ~capital:(float_of_int c)
      in
      let n2, dd2 =
        Dio_survival.Survival_mfd.static_drawdown_runway
          ~qty:1.0
          ~grid_interval_pct:1.0
          ~fee:0.0
          ~start_price:100.0
          ~capital:(float_of_int (c + 1))
      in
      Alcotest.(check bool) "fills non-decreasing in capital" (n1 <= n2) true;
      Alcotest.(check bool)
        "drawdown non-decreasing in capital"
        (dd1 <= dd2 +. 1e-12)
        true;
      loop (c + 37))
  in
  loop 10
;;

let test_higher_fees_cannot_improve () =
  (* Invariant: a higher fee schedule cannot make the grid survive more
     fills at the same capital. *)
  List.iter
    (fun capital ->
       let n_cheap, _ =
         Dio_survival.Survival_mfd.static_drawdown_runway
           ~qty:1.0
           ~grid_interval_pct:1.0
           ~fee:0.0
           ~start_price:100.0
           ~capital
       in
       let n_priced, _ =
         Dio_survival.Survival_mfd.static_drawdown_runway
           ~qty:1.0
           ~grid_interval_pct:1.0
           ~fee:0.01
           ~start_price:100.0
           ~capital
       in
       Alcotest.(check bool) "more fees never help" (n_cheap >= n_priced) true)
    [ 99.0; 500.0; 2_000.0; 10_000.0 ]
;;

let test_floor_aware_matches_closed_form_without_floor () =
  (* No notional floor: the floor-aware walk reduces to the closed-form
     geometric sum exactly (to the walk's per-level price rounding, ~1e-6). *)
  let c =
    Dio_survival.Survival_mfd.floor_aware_runway_cost
      ~qty:1.0
      ~grid_interval_pct:1.0
      ~fee:0.0
      ~start_price:100.0
      ~min_notional:0.0
      ~price_increment:1e-9
      ~qty_increment:1e-9
      ~n_fills:10
  in
  let expected = 99.0 *. (1.0 -. (0.99 ** 10.0)) /. 0.01 in
  Alcotest.(check (float 1e-6)) "floor-aware ~= closed form" expected c
;;

let test_floor_aware_exceeds_closed_form_when_floor_binds () =
  (* qty 0.5 with a $10 floor: below ~$20/level the per-rung qty up-sizes to
     ceil(10/level), so the true ladder cost exceeds the fixed-qty closed
     form - the closed form is unconservative exactly when the floor binds
     (the P1 audit finding). *)
  let n = 220 in
  let closed =
    Dio_survival.Survival_mfd.static_runway_cost
      ~qty:0.5
      ~grid_interval_pct:1.0
      ~fee:0.0
      ~start_price:100.0
      ~n_fills:n
  in
  let aware =
    Dio_survival.Survival_mfd.floor_aware_runway_cost
      ~qty:0.5
      ~grid_interval_pct:1.0
      ~fee:0.0
      ~start_price:100.0
      ~min_notional:10.0
      ~price_increment:1e-9
      ~qty_increment:1e-9
      ~n_fills:n
  in
  Alcotest.(check bool) "floor-aware exceeds closed form" (aware > closed) true
;;

let test_grid_spacing_sensitivity () =
  (* Surviving a fixed drawdown takes fewer (and, per rung, more expensive)
     ladder steps with a wider grid: the capital needed to survive the same
     drawdown must fall as the grid spacing grows. *)
  let fills_for d gi =
    max 1 (int_of_float (Float.ceil (Float.log (1.0 -. d) /. Float.log (1.0 -. gi))))
  in
  let cost gi =
    let n = fills_for 0.30 (gi /. 100.0) in
    Dio_survival.Survival_mfd.floor_aware_runway_cost
      ~qty:1.0
      ~grid_interval_pct:gi
      ~fee:0.0004
      ~start_price:100.0
      ~min_notional:0.0
      ~price_increment:0.01
      ~qty_increment:0.01
      ~n_fills:n
  in
  Alcotest.(check bool)
    "wider grid needs fewer fills"
    (fills_for 0.30 0.01 > fills_for 0.30 0.05)
    true;
  Alcotest.(check bool) "wider grid needs less capital" (cost 5.0 < cost 1.0) true
;;

let () =
  Alcotest.run
    "survival_mfd"
    [ ( "mfd"
      , [ Alcotest.test_case "mfd basic" `Quick test_mfd_basic
        ; Alcotest.test_case "mfd uses lows only" `Quick test_mfd_uses_only_lows
        ; Alcotest.test_case "f_h and survival" `Quick test_f_h_and_survival
        ; Alcotest.test_case "surface" `Quick test_surface
        ; Alcotest.test_case "stride sampling" `Quick test_stride_sampling
        ; Alcotest.test_case "static runway" `Quick test_static_runway
        ; Alcotest.test_case "runway cost accumulates" `Quick test_runway_cost_accumulates
        ; Alcotest.test_case
            "f_h raises on no valid starts"
            `Quick
            test_f_h_raises_on_no_valid_starts
        ; Alcotest.test_case
            "runway monotone in capital"
            `Quick
            test_runway_monotone_in_capital
        ; Alcotest.test_case
            "higher fees cannot improve"
            `Quick
            test_higher_fees_cannot_improve
        ; Alcotest.test_case
            "floor-aware matches closed form without floor"
            `Quick
            test_floor_aware_matches_closed_form_without_floor
        ; Alcotest.test_case
            "floor-aware exceeds closed form when floor binds"
            `Quick
            test_floor_aware_exceeds_closed_form_when_floor_binds
        ; Alcotest.test_case
            "grid spacing sensitivity"
            `Quick
            test_grid_spacing_sensitivity
        ] )
    ]
;;
