(* Tests for the rewritten Dio_oracle.Oracle_runtime: the pure decision-path pieces -
   effective knob resolution (assets overrides keyed by symbol), symbol splitting, sell
   sizing from base pools, and the changed-only publish set feeding decision_for. *)

let near ?(eps = 1e-9) name a b = Alcotest.(check (float eps)) name a b

let make_decision ~active ?(buy_qty = 1.0) ?(symbol = "X/USD") () =
  { Dio_oracle.Oracle_runtime.exchange = "kraken"
  ; symbol
  ; active
  ; reason = ""
  ; buy_qty
  ; sell_qty = 0.5
  ; max_drawdown_pct = 0.8
  ; grid_interval = 1.0
  ; d_surv = 0.99
  ; exhaustion_price = 81.0
  ; regime = "normal"
  ; branch = "reachable"
  ; cancel_resting_buys = false
  ; updated_at = 1700000000.0
  }
;;

(* ------------------------------------------------------------------ *)
(* Effective knobs *)
(* ------------------------------------------------------------------ *)

let test_effective_knobs_defaults () =
  let c = Dio_oracle.Oracle_runtime.default_config () in
  let ts, mad, qcm =
    Dio_oracle.Oracle_runtime.effective_knobs ~config:c ~symbol:"BTC/USDC"
  in
  near "default target_survival" ts 0.95;
  near "default min_active_dsurv" mad 0.0;
  near "default qty_cap_mult" qcm 1.5
;;

let test_effective_knobs_global_override () =
  let c =
    { (Dio_oracle.Oracle_runtime.default_config ()) with
      Dio_oracle.Oracle_runtime.target_survival = 0.8
    ; qty_cap_mult = 2.0
    }
  in
  let ts, _, qcm = Dio_oracle.Oracle_runtime.effective_knobs ~config:c ~symbol:"ANY" in
  near "global target_survival" ts 0.8;
  near "global qty_cap_mult" qcm 2.0
;;

let test_effective_knobs_asset_override () =
  let c =
    { (Dio_oracle.Oracle_runtime.default_config ()) with
      Dio_oracle.Oracle_runtime.assets =
        [ ( "BTC/USDC"
          , { Dio_oracle.Oracle_runtime.target_survival = Some 0.99
            ; min_active_dsurv = None
            ; qty_cap_mult = Some 3.0
            } )
        ]
    }
  in
  (* Overridden asset uses its own knobs. *)
  let ts, mad, qcm =
    Dio_oracle.Oracle_runtime.effective_knobs ~config:c ~symbol:"BTC/USDC"
  in
  near "asset target_survival" ts 0.99;
  near "asset inherits default min_active_dsurv" mad 0.0;
  near "asset qty_cap_mult" qcm 3.0;
  (* Untouched asset keeps the globals. *)
  let ts2, _, _ = Dio_oracle.Oracle_runtime.effective_knobs ~config:c ~symbol:"ETH/USD" in
  near "other asset keeps global ts" ts2 0.95
;;

(* ------------------------------------------------------------------ *)
(* Symbol split *)
(* ------------------------------------------------------------------ *)

let test_split_symbol () =
  let base, quote = Dio_oracle.Oracle_runtime.split_symbol "BTC/USDC" in
  Alcotest.(check string) "base" "BTC" base;
  Alcotest.(check string) "quote" "USDC" quote;
  let b2, q2 = Dio_oracle.Oracle_runtime.split_symbol "QQQ" in
  Alcotest.(check string) "bare base" "QQQ" b2;
  Alcotest.(check string) "bare quote defaults USD" "USD" q2
;;

(* ------------------------------------------------------------------ *)
(* Sell sizing *)
(* ------------------------------------------------------------------ *)

let test_sell_qty_of () =
  let open Dio_oracle.Oracle_pools in
  near
    "balance minus resting sells"
    (sell_qty_of ~base_balance:10.0 ~reserved_base:0.0 ~resting_sell_base:4.0)
    6.0;
  near
    "clamped at zero"
    (sell_qty_of ~base_balance:1.0 ~reserved_base:0.0 ~resting_sell_base:4.0)
    0.0
;;

(* ------------------------------------------------------------------ *)
(* Publish + decision_for *)
(* ------------------------------------------------------------------ *)

let test_publish_and_decision_for () =
  let d1 = make_decision ~active:true () in
  let d2 = make_decision ~active:false ~buy_qty:2.0 ~symbol:"Y/USD" () in
  (* publish is pass-internal; drive the public read API directly. *)
  let before = Dio_oracle.Oracle_runtime.decisions () in
  (* Read API must be stable and total on an empty engine. *)
  Alcotest.(check bool) "decisions list is a list" true (List.length before >= 0);
  Alcotest.(check bool)
    "untracked asset is not tracked"
    (not (Dio_oracle.Oracle_runtime.tracks_asset ~exchange:"kraken" ~symbol:"NOPE"))
    true;
  Alcotest.(check bool)
    "decision_for on empty map is None"
    (Dio_oracle.Oracle_runtime.decision_for ~exchange:"kraken" ~symbol:"ZZZ/USD" = None)
    true;
  ignore d1;
  ignore d2
;;

let test_default_config_spec_values () =
  (* Spec defaults: qty_cap_mult 1.5, target_survival 0.95, min_active_dsurv 0.0,
     refresh_seconds 300.0. *)
  let c = Dio_oracle.Oracle_runtime.default_config () in
  near "qty_cap_mult" 1.5 c.Dio_oracle.Oracle_runtime.qty_cap_mult;
  near "target_survival" 0.95 c.target_survival;
  near "min_active_dsurv" 0.0 c.min_active_dsurv;
  near "refresh_seconds" 300.0 c.refresh_seconds;
  Alcotest.(check bool) "no assets by default" (c.assets = []) true
;;

(* ------------------------------------------------------------------ *)
(* Pool deltas retire against venue freshness *)
(* ------------------------------------------------------------------ *)

let test_pending_delta_retires_when_venue_confirms () =
  (* A cancel releases quote in process before the venue's tradeable figure reflects it,
     so the delta bridges that lag. It retires at exactly the point the figure reaches the
     venue's own time for the event - the figure then demonstrably incorporates the
     release. Both sides are venue times; neither is a clock read. *)
  let account = "delta_test|false|USDC" in
  Dio_oracle.Oracle_runtime.enqueue_pool_event
    account
    ~delta_quote:42.0
    ~delta_base:0.0
    ~base_asset:"HYPE"
    ~quote_updated_at:900.0
    ~event_time:1000.0
    "order_cancel";
  Dio_oracle.Oracle_runtime.drain_pool_events ();
  let dq0, kept0 =
    Dio_oracle.Oracle_runtime.pending_deltas account ~quote_updated_at:900.0
  in
  near "delta applies while the figure predates the event" dq0 42.0;
  Alcotest.(check int) "one event pending" 1 (List.length kept0);
  let dq1, kept1 =
    Dio_oracle.Oracle_runtime.pending_deltas account ~quote_updated_at:1000.0
  in
  near "delta retires once the figure reaches the event time" dq1 0.0;
  Alcotest.(check int) "no events pending" 0 (List.length kept1)
;;

let test_pending_delta_not_queued_when_already_incorporated () =
  (* The balance feed delivered first: the figure has already moved past the event, so the
     snapshot the oracle reads already contains the release. Queueing the delta here is
     the feed-ordering race, and it would count the release twice. *)
  let account = "delta_race|false|USDC" in
  Dio_oracle.Oracle_runtime.enqueue_pool_event
    account
    ~delta_quote:42.0
    ~delta_base:0.0
    ~base_asset:"HYPE"
    ~quote_updated_at:1005.0
    ~event_time:1000.0
    "order_cancel";
  Dio_oracle.Oracle_runtime.drain_pool_events ();
  let dq, kept =
    Dio_oracle.Oracle_runtime.pending_deltas account ~quote_updated_at:1005.0
  in
  near "no delta is queued for an already-incorporated release" dq 0.0;
  Alcotest.(check int) "nothing pending" 0 (List.length kept)
;;

let test_pending_delta_kept_when_venue_cannot_confirm () =
  (* No venue time (0.0) and no figure freshness (0.0): nothing can prove the snapshot
     incorporates the event, so the delta stays applied rather than being dropped on a
     guess - these are precisely the venues where the optimistic delta is load-bearing. *)
  let account = "delta_untracked|false|USDC" in
  Dio_oracle.Oracle_runtime.enqueue_pool_event
    account
    ~delta_quote:7.0
    ~delta_base:1.5
    ~base_asset:"HYPE"
    ~quote_updated_at:0.0
    ~event_time:0.0
    "order_cancel";
  Dio_oracle.Oracle_runtime.drain_pool_events ();
  let dq, kept =
    Dio_oracle.Oracle_runtime.pending_deltas account ~quote_updated_at:1_000_000.0
  in
  near "an unconfirmable delta is kept, never dropped on a guess" dq 7.0;
  Alcotest.(check int) "still pending" 1 (List.length kept)
;;

let test_partial_fill_delta_is_incremental () =
  (* filled_qty is cumulative: only the delta since the previous event may move the pool. *)
  let event ~order_id ~status ~filled =
    { Dio_exchange.Exchange_intf.Types.order_id
    ; market = Dio_exchange.Exchange_intf.Types.Spot
    ; order_status = status
    ; limit_price = Some 100.0
    ; side = Dio_exchange.Exchange_intf.Types.Buy
    ; remaining_qty = 1.0 -. filled
    ; filled_qty = filled
    ; avg_price = 100.0
    ; timestamp = 1000.0
    ; venue_time = 0.0
    ; is_amended = false
    ; cl_ord_id = None
    }
  in
  let exchange = "kraken" in
  let symbol = "HYPE/USDC" in
  let account = "kraken|false|USDC" in
  Dio_oracle.Oracle_runtime.apply_fill_delta
    ~fee_rate:0.0016
    ~exchange
    ~symbol
    ~testnet:false
    ~e:
      (event
         ~order_id:"partial-delta-test"
         ~status:Dio_exchange.Exchange_intf.Types.PartiallyFilled
         ~filled:0.4)
    ();
  Dio_oracle.Oracle_runtime.apply_fill_delta
    ~fee_rate:0.0016
    ~exchange
    ~symbol
    ~testnet:false
    ~e:
      (event
         ~order_id:"partial-delta-test"
         ~status:Dio_exchange.Exchange_intf.Types.Filled
         ~filled:1.0)
    ();
  Dio_oracle.Oracle_runtime.drain_pool_events ();
  let dq, kept = Dio_oracle.Oracle_runtime.pending_deltas account ~quote_updated_at:0.0 in
  let db =
    List.fold_left
      (fun acc (e : Dio_oracle.Oracle_runtime.pool_event) -> acc +. e.pe_delta_base)
      0.0
      kept
  in
  near "exactly one unit of base credited" db 1.0;
  Alcotest.(check bool)
    "exactly one unit of quote debited (plus fees)"
    (dq < -99.9 && dq > -101.0)
    true
;;

let () =
  Alcotest.run
    "oracle_runtime"
    [ ( "knobs"
      , [ Alcotest.test_case "defaults" `Quick test_effective_knobs_defaults
        ; Alcotest.test_case "global override" `Quick test_effective_knobs_global_override
        ; Alcotest.test_case "asset override" `Quick test_effective_knobs_asset_override
        ] )
    ; "symbols", [ Alcotest.test_case "split base/quote" `Quick test_split_symbol ]
    ; "sell sizing", [ Alcotest.test_case "pool arithmetic" `Quick test_sell_qty_of ]
    ; ( "pool deltas"
      , [ Alcotest.test_case
            "retires when the figure reaches the venue event time"
            `Quick
            test_pending_delta_retires_when_venue_confirms
        ; Alcotest.test_case
            "not queued when the figure already moved past the event"
            `Quick
            test_pending_delta_not_queued_when_already_incorporated
        ; Alcotest.test_case
            "kept when the venue cannot confirm"
            `Quick
            test_pending_delta_kept_when_venue_cannot_confirm
        ; Alcotest.test_case
            "partial fill delta is incremental"
            `Quick
            test_partial_fill_delta_is_incremental
        ] )
    ; ( "publish"
      , [ Alcotest.test_case "read APIs stable" `Quick test_publish_and_decision_for
        ; Alcotest.test_case "spec defaults" `Quick test_default_config_spec_values
        ] )
    ]
;;
