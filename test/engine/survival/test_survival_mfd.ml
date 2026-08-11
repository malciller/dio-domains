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
    (Dio_survival.Survival_mfd.f_h ~closes ~lows ~horizon:3 ~threshold:0.05 ~warmup:0);
  near
    1.0
    (Dio_survival.Survival_mfd.survival
       ~closes
       ~lows
       ~horizon:3
       ~threshold:0.05
       ~warmup:0);
  near
    1.0
    (Dio_survival.Survival_mfd.f_h ~closes ~lows ~horizon:3 ~threshold:0.10 ~warmup:0)
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

let () =
  Alcotest.run
    "survival_mfd"
    [ ( "mfd"
      , [ Alcotest.test_case "mfd basic" `Quick test_mfd_basic
        ; Alcotest.test_case "mfd uses lows only" `Quick test_mfd_uses_only_lows
        ; Alcotest.test_case "f_h and survival" `Quick test_f_h_and_survival
        ; Alcotest.test_case "surface" `Quick test_surface
        ; Alcotest.test_case "static runway" `Quick test_static_runway
        ; Alcotest.test_case "runway cost accumulates" `Quick test_runway_cost_accumulates
        ] )
    ]
;;
