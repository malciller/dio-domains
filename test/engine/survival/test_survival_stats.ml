(* Survival stats tests: percentiles, no-lookahead volatility, z, blend. *)

let near a b = Alcotest.(check (float 1e-6)) "approx" a b

let test_percentile () =
  let xs = [| 1.; 2.; 3.; 4.; 5. |] in
  near 3.0 (Dio_survival.Survival_stats.percentile xs 50.);
  near 1.0 (Dio_survival.Survival_stats.percentile xs 0.);
  near 5.0 (Dio_survival.Survival_stats.percentile xs 100.)
;;

let test_trailing_vol_constant () =
  let closes = Array.make 100 100.0 in
  near 0.0 (Option.get (Dio_survival.Survival_stats.trailing_vol ~closes ~s:80 ~w:60))
;;

let test_trailing_vol_no_lookahead () =
  (* Window [s-w, s]; at s=5, w=60 there are not enough prior bars. *)
  let closes = Array.make 100 100.0 in
  Alcotest.(check bool)
    "no lookahead"
    (Dio_survival.Survival_stats.trailing_vol ~closes ~s:5 ~w:60 = None)
    true
;;

let test_trailing_vol_known () =
  (* Log returns of 1%/session -> vol = std over those returns. *)
  let closes = Array.init 61 (fun i -> 100.0 *. (1.01 ** float_of_int i)) in
  let v = Option.get (Dio_survival.Survival_stats.trailing_vol ~closes ~s:60 ~w:60) in
  Alcotest.(check bool) "positive vol" (v > 0.0) true
;;

let test_z_mfd () =
  let closes = Array.init 200 (fun i -> 100.0 *. (1.001 ** float_of_int i)) in
  let lows = Array.map (fun c -> c *. 0.95) closes in
  let z =
    Option.get (Dio_survival.Survival_stats.z_mfd ~closes ~lows ~s:180 ~horizon:30 ~w:60)
  in
  Alcotest.(check bool) "finite" (Float.is_finite z) true
;;

let test_blend () =
  (* F_blend = (n*F_a + kappa*F_c)/(n+kappa) *)
  near
    0.5
    (Dio_survival.Survival_stats.blend
       ~n_asset:100.0
       ~asset_f:0.4
       ~kappa:100.0
       ~class_f:0.6);
  (* kappa -> infinity pulls toward class: (100*0.4 + 1e6*0.6)/(100+1e6). *)
  let expected = ((100.0 *. 0.4) +. (1e6 *. 0.6)) /. (100.0 +. 1e6) in
  near
    expected
    (Dio_survival.Survival_stats.blend
       ~n_asset:100.0
       ~asset_f:0.4
       ~kappa:1_000_000.0
       ~class_f:0.6)
;;

let () =
  Alcotest.run
    "survival_stats"
    [ ( "stats"
      , [ Alcotest.test_case "percentile" `Quick test_percentile
        ; Alcotest.test_case "trailing vol constant" `Quick test_trailing_vol_constant
        ; Alcotest.test_case
            "trailing vol no lookahead"
            `Quick
            test_trailing_vol_no_lookahead
        ; Alcotest.test_case "trailing vol known" `Quick test_trailing_vol_known
        ; Alcotest.test_case "z mfd" `Quick test_z_mfd
        ; Alcotest.test_case "blend" `Quick test_blend
        ] )
    ]
;;
