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
        ] )
    ]
;;
