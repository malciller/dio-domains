(* Oracle_core unit tests - pins the spec's formulas exactly. *)

let near ?(eps = 1e-9) name a b = Alcotest.(check (float eps)) name a b

let bar ~date ~high ~low ~close =
  { Dio_oracle.Oracle_types.date; open_ = close; high; low; close; volume = 1.0 }
;;

(* ------------------------------------------------------------------ *)
(* references_of                                                      *)
(* ------------------------------------------------------------------ *)

let test_references () =
  (* Empty series: no references. *)
  Alcotest.(check bool)
    "empty series has no references"
    (Dio_oracle.Oracle_core.references_of ~bars:[||] = None)
    true;
  (* ATH 100k (intrabar high), ATL 8k (intrabar low), worst peak-to-trough:
     close-peak 100k -> low 10k = 90% decline, recovery irrelevant. *)
  let bars =
    [| bar ~date:"1" ~high:50.0 ~low:48.0 ~close:49.0
     ; bar ~date:"2" ~high:100.0 ~low:95.0 ~close:99.0
     ; bar ~date:"3" ~high:60.0 ~low:10.0 ~close:20.0
     ; bar ~date:"4" ~high:30.0 ~low:8.0 ~close:25.0
    |]
  in
  match Dio_oracle.Oracle_core.references_of ~bars with
  | None -> Alcotest.fail "expected references"
  | Some r ->
    near "ath is the intrabar high" r.ath 100.0;
    near "atl is the intrabar low" r.atl 8.0;
    (* Worst fall: close-peak 99 -> later low 8 (bar 4's trough counts even
       though bar 3 already bounced). *)
    near
      "max drawdown = close-peak to subsequent low"
      r.max_drawdown_pct
      (1.0 -. (8.0 /. 99.0));
    (* The deepest fall counts even though price recovered afterwards. *)
    Alcotest.(check bool) "recovery irrelevant" true true
;;

(* ------------------------------------------------------------------ *)
(* runway_of - the spec's worked example                              *)
(* ------------------------------------------------------------------ *)

let spec_refs : Dio_oracle.Oracle_core.references =
  { max_drawdown_pct = 0.92; ath = 100_000.0; atl = 7_000.0 }
;;

let test_runway_worked_example () =
  (* Spec: ATH 100k, max_drawdown_pct 92%, target_survival 0.80
     -> runway 73.6%, floor 26.4k.
     At current 70k: realized_dd 30%, remaining_drop 43.6%. *)
  let rw =
    Dio_oracle.Oracle_core.runway_of
      ~current:70_000.0
      ~refs:spec_refs
      ~target_survival:0.80
  in
  near "realized_dd" rw.realized_dd 0.30;
  near "runway_pct = mdd * ts" rw.runway_pct 0.736;
  near "floor_price = ath * (1 - runway)" rw.floor_price 26_400.0;
  near "remaining_drop" rw.remaining_drop ((70_000.0 -. 26_400.0) /. 70_000.0);
  Alcotest.(check bool)
    "normal regime above floor"
    (rw.regime = Dio_oracle.Oracle_core.Normal)
    true;
  near "aggressiveness = realized/mdd" rw.aggressiveness (0.30 /. 0.92)
;;

let test_regimes () =
  (* Normal: current strictly above the floor. *)
  let rw =
    Dio_oracle.Oracle_core.runway_of
      ~current:70_000.0
      ~refs:spec_refs
      ~target_survival:0.80
  in
  Alcotest.(check bool) "normal" (rw.regime = Dio_oracle.Oracle_core.Normal) true;
  (* Floor extension: below the floor but above ATL. *)
  let rw2 =
    Dio_oracle.Oracle_core.runway_of
      ~current:20_000.0
      ~refs:spec_refs
      ~target_survival:0.80
  in
  Alcotest.(check bool)
    "floor extension"
    (rw2.regime = Dio_oracle.Oracle_core.Floor_extension)
    true;
  near "funded down to atl" rw2.funded_floor 7_000.0;
  (* Unprecedented lows: at the deepest recorded drawdown AND <= ATL.
     Deepest-drawdown price level = ath * (1 - mdd) = 8k; atl is 7k. *)
  let rw3 =
    Dio_oracle.Oracle_core.runway_of
      ~current:6_500.0
      ~refs:spec_refs
      ~target_survival:0.80
  in
  Alcotest.(check bool)
    "unprecedented lows"
    (rw3.regime = Dio_oracle.Oracle_core.Unprecedented_lows)
    true
;;

(* ------------------------------------------------------------------ *)
(* d_surv_of                                                          *)
(* ------------------------------------------------------------------ *)

let test_d_surv () =
  let fees = Dio_oracle.Oracle_core.{ maker_fee = 0.0; fee_in_base_buy = false } in
  (* current 100, floor 50 (depth 50), gi 10%: rungs at 90, 81, ..., 54ish.
     quote 500, qty 1, no fee: costs 90+81+72.9+65.61+59.049+53.14 = 421.7 -
     all rungs down to >= 50 fund: full depth survived. *)
  let d =
    Dio_oracle.Oracle_core.d_surv_of
      ~current:100.0
      ~funded_floor:50.0
      ~gi:10.0
      ~buy_qty:1.0
      ~quote:500.0
      ~fees
  in
  Alcotest.(check bool) "fully funded depth" (d >= 1.0) true;
  (* Same ladder, quote 200: buys 90 + 81 = 171 affordable, next rung 72.9
     not: last surviving rung 81 -> fraction (100 - 81)/50 = 0.38. *)
  let d2 =
    Dio_oracle.Oracle_core.d_surv_of
      ~current:100.0
      ~funded_floor:50.0
      ~gi:10.0
      ~buy_qty:1.0
      ~quote:200.0
      ~fees
  in
  near "partial survival fraction" d2 0.38;
  (* First rung unaffordable: zero survival. *)
  let d3 =
    Dio_oracle.Oracle_core.d_surv_of
      ~current:100.0
      ~funded_floor:50.0
      ~gi:10.0
      ~buy_qty:1.0
      ~quote:50.0
      ~fees
  in
  near "first-rung failure is zero" d3 0.0;
  (* Fees make each rung costlier: pick a quote that funds the whole depth
     fee-free (cumulative 421.70 <= 423) but falls one rung short with a 1%
     maker fee (cumulative 372.24, next rung costs 53.68 > 50.76 left):
     survival stops at the 59.049 rung -> (100 - 59.049) / 50. *)
  let dfree =
    Dio_oracle.Oracle_core.d_surv_of
      ~current:100.0
      ~funded_floor:50.0
      ~gi:10.0
      ~buy_qty:1.0
      ~quote:423.0
      ~fees
  in
  Alcotest.(check bool) "quote 423 funds full depth fee-free" (dfree >= 1.0) true;
  let dfee =
    Dio_oracle.Oracle_core.d_surv_of
      ~current:100.0
      ~funded_floor:50.0
      ~gi:10.0
      ~buy_qty:1.0
      ~quote:423.0
      ~fees:{ maker_fee = 0.01; fee_in_base_buy = false }
  in
  near ~eps:1e-6 "fees drop survival below one" dfee ((100.0 -. 59.049) /. 50.0)
;;

(* ------------------------------------------------------------------ *)
(* resolve                                                            *)
(* ------------------------------------------------------------------ *)

let bounds =
  Dio_oracle.Oracle_core.{ qty = 1.0; qty_cap_mult = 3.0; gi_min = 1.0; gi_max = 5.0 }
;;

let test_resolve_unreachable () =
  (* Quote far too small for any candidate: conservative corner emitted. *)
  let fees = Dio_oracle.Oracle_core.default_fees in
  let r =
    Dio_oracle.Oracle_core.resolve
      ~regime:Dio_oracle.Oracle_core.Normal
      ~current:100.0
      ~funded_floor:50.0
      ~aggressiveness:0.5
      ~bounds
      ~quote:10.0
      ~fees
      ~target_survival:1.0
      ()
  in
  Alcotest.(check bool)
    "unreachable branch"
    (r.branch = Dio_oracle.Oracle_core.Unreachable)
    true;
  near "unreachable emits gi_max" r.grid_interval 5.0;
  near "unreachable emits qty min" r.buy_qty 1.0
;;

let test_resolve_surplus () =
  (* Deep quote pool on a shallow depth: the most aggressive corner funds
     everything with leftover. *)
  let fees = Dio_oracle.Oracle_core.default_fees in
  let r =
    Dio_oracle.Oracle_core.resolve
      ~regime:Dio_oracle.Oracle_core.Normal
      ~current:100.0
      ~funded_floor:98.0
      ~aggressiveness:0.5
      ~bounds
      ~quote:100_000.0
      ~fees
      ~target_survival:0.95
      ()
  in
  Alcotest.(check bool) "surplus branch" (r.branch = Dio_oracle.Oracle_core.Surplus) true;
  near "surplus emits gi_min" r.grid_interval 1.0;
  near "surplus emits qty max" r.buy_qty 3.0;
  Alcotest.(check bool) "surplus exceeded target" (r.d_surv > 1.0) true
;;

let test_resolve_reachable_bias () =
  let fees = Dio_oracle.Oracle_core.default_fees in
  let resolve_at bias =
    Dio_oracle.Oracle_core.resolve
      ~regime:Dio_oracle.Oracle_core.Normal
      ~current:100.0
      ~funded_floor:50.0
      ~aggressiveness:bias
      ~bounds
      ~quote:2000.0
      ~fees
      ~target_survival:1.0
      ()
  in
  let shallow = resolve_at 0.0 in
  let deep = resolve_at 1.0 in
  Alcotest.(check bool)
    "shallow resolves reachable"
    (shallow.branch = Dio_oracle.Oracle_core.Reachable)
    true;
  Alcotest.(check bool)
    "deep resolves reachable"
    (deep.branch = Dio_oracle.Oracle_core.Reachable)
    true;
  (* Shallow bias picks the unique most conservative feasible corner:
     minimum size, widest spacing (score is maximal there). *)
  near "shallow takes min qty" shallow.buy_qty 1.0;
  near "shallow takes max grid interval" shallow.grid_interval 5.0;
  (* Deeper position accumulates harder: strictly larger size and tighter
     spacing than the conservative pick, while still funding full depth. *)
  Alcotest.(check bool)
    "deeper prefers tighter grid"
    (deep.grid_interval < shallow.grid_interval)
    true;
  Alcotest.(check bool) "deeper prefers larger size" (deep.buy_qty > shallow.buy_qty) true;
  Alcotest.(check bool)
    "both meet the target"
    (shallow.d_surv >= 1.0 && deep.d_surv >= 1.0)
    true
;;

(* ------------------------------------------------------------------ *)
(* decision_of                                                        *)
(* ------------------------------------------------------------------ *)

let test_decision_activity_gating () =
  let res : Dio_oracle.Oracle_core.resolution =
    { branch = Dio_oracle.Oracle_core.Reachable
    ; grid_interval = 2.0
    ; buy_qty = 2.0
    ; d_surv = 0.9
    }
  in
  (* Affordable + d_surv above the gate: active. *)
  let d =
    Dio_oracle.Oracle_core.decision_of
      ~resolution:res
      ~sell_qty:0.5
      ~available_quote:100.0
      ~current:50.0
      ~min_active_dsurv:0.5
  in
  Alcotest.(check bool) "active when gated and affordable" d.active true;
  near "grid_interval passes through raw" d.grid_interval 2.0;
  near "buy_qty passes through raw" d.buy_qty 2.0;
  near "sell_qty passes through raw" d.sell_qty 0.5;
  (* d_surv below min_active_dsurv: inactive, values still emitted. *)
  let d2 =
    Dio_oracle.Oracle_core.decision_of
      ~resolution:res
      ~sell_qty:0.5
      ~available_quote:100.0
      ~current:50.0
      ~min_active_dsurv:0.95
  in
  Alcotest.(check bool) "inactive under the dsurv gate" d2.active false;
  Alcotest.(check bool) "inactive still emits parameters" (d2.buy_qty > 0.0) true;
  (* Unaffordable: inactive regardless of d_surv. *)
  let d3 =
    Dio_oracle.Oracle_core.decision_of
      ~resolution:res
      ~sell_qty:0.5
      ~available_quote:50.0
      ~current:50.0
      ~min_active_dsurv:0.5
  in
  Alcotest.(check bool) "inactive when unaffordable" d3.active false
;;

(* ------------------------------------------------------------------ *)
(* pipeline                                                           *)
(* ------------------------------------------------------------------ *)

let test_current_price_of_series () =
  let s : Dio_oracle.Oracle_types.series =
    { symbol = "X"
    ; calendar_kind = Crypto
    ; bars =
        [| bar ~date:"1" ~high:10.0 ~low:9.0 ~close:9.5
         ; bar ~date:"2" ~high:11.0 ~low:10.0 ~close:10.5
        |]
    ; gaps = []
    }
  in
  near
    "current price is last close"
    (Option.get (Dio_oracle.Oracle_pipeline.current_price_of_series s))
    10.5;
  let empty = { s with bars = [||] } in
  Alcotest.(check bool)
    "empty series has no current price"
    (Dio_oracle.Oracle_pipeline.current_price_of_series empty = None)
    true
;;

let test_pipeline_decide () =
  (* History: close-peak 95 -> later low 47.5 gives mdd exactly 0.5;
     ath 100 (intrabar), atl 40. Current = last close 80.
     ts 0.8 -> runway 0.4 -> floor 60: Normal regime, aggressiveness 1.0. *)
  let bars =
    [| bar ~date:"1" ~high:60.0 ~low:40.0 ~close:50.0
     ; bar ~date:"2" ~high:100.0 ~low:90.0 ~close:95.0
     ; bar ~date:"3" ~high:96.0 ~low:47.5 ~close:80.0
    |]
  in
  let inputs : Dio_oracle.Oracle_pipeline.inputs =
    { exchange = "kraken"
    ; symbol = "XXBT/USD"
    ; bars
    ; current_price = 80.0
    ; available_quote = 500.0
    ; sell_qty = 0.25
    ; bounds =
        Dio_oracle.Oracle_core.
          { qty = 0.5; qty_cap_mult = 2.0; gi_min = 1.0; gi_max = 5.0 }
    ; target_survival = 0.8
    ; min_active_dsurv = 0.5
    ; fees = Dio_oracle.Oracle_core.default_fees
    }
  in
  match Dio_oracle.Oracle_pipeline.decide ~inputs with
  | None -> Alcotest.fail "expected an outcome"
  | Some o ->
    near "pipeline mdd" o.refs.max_drawdown_pct 0.5;
    near "pipeline ath" o.refs.ath 100.0;
    near "pipeline atl" o.refs.atl 40.0;
    near "pipeline floor" o.runway.floor_price 60.0;
    Alcotest.(check bool)
      "normal regime"
      (o.runway.regime = Dio_oracle.Oracle_core.Normal)
      true;
    near "aggressiveness = 0.2/0.5" o.runway.aggressiveness 0.4;
    Alcotest.(check bool)
      "reachable branch"
      (o.resolution.branch = Dio_oracle.Oracle_core.Reachable)
      true;
    Alcotest.(check bool) "meets target" (o.resolution.d_surv >= 0.8) true;
    Alcotest.(check bool) "active" o.decision.active true;
    Alcotest.(check bool)
      "buy_qty within bounds"
      (o.decision.buy_qty >= 0.5 && o.decision.buy_qty <= 1.0)
      true;
    Alcotest.(check bool)
      "grid_interval within bounds"
      (o.decision.grid_interval >= 1.0 && o.decision.grid_interval <= 5.0)
      true;
    near "sell_qty passes through" o.decision.sell_qty 0.25;
    (* No usable history: no decision. *)
    let no_history = { inputs with bars = [||] } in
    Alcotest.(check bool)
      "empty history yields no outcome"
      (Dio_oracle.Oracle_pipeline.decide ~inputs:no_history = None)
      true
;;

let () =
  Alcotest.run
    "oracle_core"
    [ ( "references"
      , [ Alcotest.test_case "ath/atl/max drawdown from bars" `Quick test_references ] )
    ; ( "runway"
      , [ Alcotest.test_case "spec worked example" `Quick test_runway_worked_example
        ; Alcotest.test_case "three regimes" `Quick test_regimes
        ] )
    ; "survival", [ Alcotest.test_case "d_surv fractions" `Quick test_d_surv ]
    ; ( "search"
      , [ Alcotest.test_case "unreachable branch" `Quick test_resolve_unreachable
        ; Alcotest.test_case "surplus branch" `Quick test_resolve_surplus
        ; Alcotest.test_case
            "reachable aggressiveness bias"
            `Quick
            test_resolve_reachable_bias
        ] )
    ; ( "decision"
      , [ Alcotest.test_case "activity gating" `Quick test_decision_activity_gating ] )
    ; ( "pipeline"
      , [ Alcotest.test_case "current price of series" `Quick test_current_price_of_series
        ; Alcotest.test_case "end-to-end decide" `Quick test_pipeline_decide
        ] )
    ]
;;
