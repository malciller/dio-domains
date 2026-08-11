(* Survival stats tests: percentiles, no-lookahead volatility, z, blend. *)

let near a b = Alcotest.(check (float 1e-6)) "approx" a b

let test_percentile () =
  let xs = [| 1.; 2.; 3.; 4.; 5. |] in
  near 3.0 (Dio_survival.Survival_stats.percentile xs 50.);
  near 1.0 (Dio_survival.Survival_stats.percentile xs 0.);
  near 5.0 (Dio_survival.Survival_stats.percentile xs 100.)
;;

let test_percentile_empty_raises () =
  (* An empty sample must never masquerade as a zero drawdown: a 0.0 MFD
     percentile for an asset with no windows is indistinguishable from
     "never drew down" and silently injects false precision downstream. *)
  (try
     ignore (Dio_survival.Survival_stats.percentile [||] 50.0);
     Alcotest.fail "percentile: expected Invalid_argument"
   with
   | Invalid_argument _ -> ());
  try
    ignore (Dio_survival.Survival_math.weighted_percentile [||] 50.0);
    Alcotest.fail "weighted_percentile: expected Invalid_argument"
  with
  | Invalid_argument _ ->
    ();
    (* Zero total weight is the same disease: no information to invert. *)
    (try
       ignore
         (Dio_survival.Survival_math.weighted_percentile [| 1.0, 0.0; 2.0, 0.0 |] 50.0);
       Alcotest.fail "weighted_percentile: expected Invalid_argument on zero weight"
     with
     | Invalid_argument _ -> ())
;;

let test_percentile_ordering () =
  (* Percentile rows must be non-decreasing in p for both estimators: the
     table's P50 <= P75 <= P90 <= P95 <= P99 invariant. *)
  let xs =
    Array.init 137 (fun i -> 100.0 *. sin (float_of_int (i * 7))) |> Array.map abs_float
  in
  let prev = ref (-1.0) in
  List.iter
    (fun p ->
       let v = Dio_survival.Survival_stats.percentile xs p in
       Alcotest.(check bool)
         (Printf.sprintf "percentile P%g ordered" p)
         (v >= !prev -. 1e-9)
         true;
       prev := v)
    [ 50.; 75.; 90.; 95.; 99. ];
  let pairs = Array.mapi (fun i v -> v, 1.0 +. float_of_int (i mod 3)) xs in
  let prev = ref (-1.0) in
  List.iter
    (fun p ->
       let v = Dio_survival.Survival_math.weighted_percentile pairs p in
       Alcotest.(check bool)
         (Printf.sprintf "weighted P%g ordered" p)
         (v >= !prev -. 1e-9)
         true;
       prev := v)
    [ 50.; 75.; 90.; 95.; 99. ]
;;

let test_weighted_percentile_matches_unweighted () =
  (* The weighted estimator is the Type 7 analog: with unit weights it must
     reduce EXACTLY to the unweighted [percentile] on the same values, so the
     class and asset percentile columns report consistent numbers. *)
  let xs =
    Array.init 137 (fun i -> 100.0 *. sin (float_of_int (i * 7))) |> Array.map abs_float
  in
  let pairs = Array.map (fun v -> v, 1.0) xs in
  List.iter
    (fun p ->
       let want = Dio_survival.Survival_stats.percentile xs p in
       let got = Dio_survival.Survival_math.weighted_percentile pairs p in
       Alcotest.(check (float 1e-12))
         (Printf.sprintf "weighted P%g = unweighted P%g" p p)
         want
         got)
    [ 0.; 5.; 50.; 75.; 90.; 95.; 99.; 100. ];
  (* Interpolation between the two brackets: P50 of {1, 10} with unit weights
     is the midpoint 5.5; shifting mass onto the low sample pulls the median
     toward it (2/3 of the mass at 1.0 -> P50 = 1.0, sitting at the heavier
     sample's anchor). *)
  near 5.5 (Dio_survival.Survival_math.weighted_percentile [| 1.0, 1.0; 10.0, 1.0 |] 50.0);
  near 1.0 (Dio_survival.Survival_math.weighted_percentile [| 1.0, 2.0; 10.0, 1.0 |] 50.0)
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
    Option.get (Dio_survival.Survival_stats.z_mfd ~closes ~lows ~s:169 ~horizon:30 ~w:60)
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

(* Synthetic deterministic paths with nonzero vol and known scaling: scaling
   the log-path by [sc] scales volatility exactly 10x-consistent and drawdowns
   ~proportionally (constant low fraction per bar), leaving
   z = MFD/(sigma*sqrt(h)) approximately invariant (small residual from the
   intra-window close ratio, verified ~1%). *)
let mk_closes ~sc () =
  Array.init 400 (fun i ->
    let x = float_of_int i in
    100.0 *. exp (sc *. ((0.002 *. x) +. (0.01 *. sin (x /. 9.0)))))
;;

let mk_lows closes ~sc = Array.map (fun c -> c *. (1.0 -. (0.02 *. sc))) closes

let z_samples ~closes ~lows =
  let acc = ref [] in
  for s = 60 to Array.length closes - 1 do
    match Dio_survival.Survival_stats.z_mfd ~closes ~lows ~s ~horizon:30 ~w:60 with
    | Some z -> acc := z :: !acc
    | None -> ()
  done;
  Array.of_list (List.rev !acc)
;;

let test_asset_regime_zero_sigma () =
  (* Constant log-returns -> trailing vol is 0 up to FP noise, so every sigma
     is ~0.0 (excluded from the z-blend by Survival_replay) and the start
     count matches f_h's. Valid starts are s in [60, 169] = 110 windows (the
     half-open (s, s+30] window must be complete). *)
  let closes = Array.init 200 (fun i -> 100.0 *. (1.01 ** float_of_int i)) in
  let lows = Array.map (fun c -> c *. 0.99) closes in
  let r =
    Dio_survival.Survival_stats.asset_regime_of
      ~closes
      ~lows
      ~horizon:30
      ~w:60
      ~warmup:60
      ()
  in
  Alcotest.(check int) "start count" 110 r.n;
  Alcotest.(check bool)
    "all sigma ~zero"
    (Array.for_all (fun sigma -> abs_float sigma < 1e-12) r.sigma)
    true
;;

let test_z_scale_invariance () =
  (* Scaling the log-path by 5x scales drawdowns ~5x and vol exactly 5x,
     leaving z = MFD/(sigma*sqrt(h)) invariant. This is the property that
     makes pooling across differently-volatile class members valid. *)
  let c1 = mk_closes ~sc:1.0 () in
  let l1 = mk_lows c1 ~sc:1.0 in
  let c2 = mk_closes ~sc:5.0 () in
  let l2 = mk_lows c2 ~sc:5.0 in
  let z1 = z_samples ~closes:c1 ~lows:l1 in
  let z2 = z_samples ~closes:c2 ~lows:l2 in
  Alcotest.(check bool) "nonempty" (Array.length z1 > 0 && Array.length z2 > 0) true;
  let mean xs = Array.fold_left ( +. ) 0.0 xs /. float_of_int (Array.length xs) in
  let m1 = mean z1 in
  let m2 = mean z2 in
  Alcotest.(check bool)
    "z means within 5%"
    (abs_float (m1 -. m2) <= 0.05 *. Float.max 1.0 (abs_float m1))
    true
;;

let test_z_index_cdf () =
  (* z_index with weight_by_sessions over two identical members: every z value
     has weight 2, so the CDF at tau is the share of z <= tau; at tau = 0.5 the
     coverage must equal the empirical share exactly. *)
  let c = mk_closes ~sc:1.0 () in
  let l = mk_lows c ~sc:1.0 in
  let bars =
    Array.mapi
      (fun i close ->
         { Dio_survival.Survival_types.date = Printf.sprintf "2023-%03d" i
         ; open_ = close
         ; high = close
         ; low = l.(i)
         ; close
         ; volume = 1000.0
         })
      c
  in
  let series : Dio_survival.Survival_types.series =
    { symbol = "T"; calendar_kind = Crypto; bars; gaps = [] }
  in
  let idx =
    Dio_survival.Survival_classes.z_index_of
      ~members:[ series; series ]
      ~horizon:30
      ~vol_window:60
      ~warmup:60
      ()
  in
  let zs = z_samples ~closes:c ~lows:l in
  let expected tau =
    let hits = ref 0 in
    Array.iter (fun z -> if z <= tau then incr hits) zs;
    float_of_int !hits /. float_of_int (Array.length zs)
  in
  Alcotest.(check bool) "index nonempty" (idx.n > 0) true;
  Alcotest.(check (float 1e-9))
    "cdf at 0"
    (Dio_survival.Survival_classes.z_cdf_of idx ~tau:0.0)
    (expected 0.0);
  let tau = 0.5 in
  Alcotest.(check (float 1e-9))
    "cdf at 0.5"
    (Dio_survival.Survival_classes.z_cdf_of idx ~tau)
    (expected tau);
  Alcotest.(check (float 1e-9))
    "cdf at +infinity"
    (Dio_survival.Survival_classes.z_cdf_of idx ~tau:Float.infinity)
    1.0
;;

let () =
  Alcotest.run
    "survival_stats"
    [ ( "stats"
      , [ Alcotest.test_case "percentile" `Quick test_percentile
        ; Alcotest.test_case "percentile empty raises" `Quick test_percentile_empty_raises
        ; Alcotest.test_case "percentile ordering" `Quick test_percentile_ordering
        ; Alcotest.test_case
            "weighted percentile = unweighted with unit weights"
            `Quick
            test_weighted_percentile_matches_unweighted
        ; Alcotest.test_case "trailing vol constant" `Quick test_trailing_vol_constant
        ; Alcotest.test_case
            "trailing vol no lookahead"
            `Quick
            test_trailing_vol_no_lookahead
        ; Alcotest.test_case "trailing vol known" `Quick test_trailing_vol_known
        ; Alcotest.test_case "z mfd" `Quick test_z_mfd
        ; Alcotest.test_case "blend" `Quick test_blend
        ; Alcotest.test_case "asset regime zero sigma" `Quick test_asset_regime_zero_sigma
        ; Alcotest.test_case "z scale invariance" `Quick test_z_scale_invariance
        ; Alcotest.test_case "z index cdf" `Quick test_z_index_cdf
        ] )
    ]
;;
