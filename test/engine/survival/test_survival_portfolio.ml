(* Survival_portfolio per-venue shared-budget tests.

   Capital is pooled per venue account (venue + quote + testnet): every
   position on the same venue draws from one shared pool. Bars are chosen so
   high < every resting sell level (98.99 is the lowest after the second buy)
   and the ladder stops mid-grid: with low = 97.5 a qty-1 grid buys at 99 and
   98.01 then stops (97.03 not traded). A pool of 200 exactly funds that
   ladder (197.01), leaving 2.99. *)

let near a b = Alcotest.(check (float 1e-6)) "approx" a b

let grid_cfg ?(qty = 1.0) ?(start_price = 100.0) ?(fee = 0.0) ()
  : Dio_strategies.Grid_core.config
  =
  let open Dio_strategies.Grid_core in
  { qty
  ; sell_mult = 1.0
  ; grid_interval_pct = 1.0
  ; maker_fee = fee
  ; accumulation_buffer = 0.0
  ; price_increment = 1e-9
  ; qty_increment = 1e-9
  ; qty_min = 0.0
  ; min_notional = 0.0
  ; exchange_model = Dio_strategies.Grid_core_types.Hyperliquid
  ; start_price
  ; start_quote = 0.0 (* budget lives in the venue pool, not the grid *)
  ; cash_hook = None (* portfolio injects the shared pool hook *)
  }
;;

let pbar ?(low = 97.5) ?(high = 98.5) () : Dio_survival.Survival_types.bar =
  { date = "d"; open_ = 100.0; high; low; close = low; volume = 1.0 }
;;

let run ?(transfers = []) ~positions () =
  Dio_survival.Survival_portfolio.simulate ~positions ~transfers ()
;;

let position ?(quote = "USD") ?(testnet = false) venue asset pool bars subgrids
  : Dio_survival.Survival_portfolio.position_input
  =
  { Dio_survival.Survival_portfolio.venue; asset; quote; testnet; pool; bars; subgrids }
;;

let find_o venue asset (r : Dio_survival.Survival_portfolio.result) =
  List.find
    (fun (o : Dio_survival.Survival_portfolio.position_outcome) ->
       o.venue = venue && o.asset = asset)
    r.positions
;;

let find_v venue quote (r : Dio_survival.Survival_portfolio.result) =
  List.find
    (fun (v : Dio_survival.Survival_portfolio.venue_outcome) ->
       v.venue = venue && v.quote = quote)
    r.venues
;;

let test_shared_pool_merge () =
  (* Three qty-1 subgrids share a 200 venue pool. A single subgrid's ladder
     costs 197.01 and just fits; three would cost 591.03. The pool funds only
     the first subgrid's ladder and starves the rest: the venue is
     capital-low. *)
  let subgrids = List.init 3 (fun _ -> grid_cfg ()) in
  let r = run ~positions:[ position "hype" "HYPE" 200.0 [| pbar () |] subgrids ] () in
  let o = find_o "hype" "HYPE" r in
  let v = find_v "hype" "USD" r in
  Alcotest.(check int) "one ladder funded" 2 o.buy_fills;
  Alcotest.(check bool) "siblings starved" true o.capital_low;
  Alcotest.(check bool) "venue capital low" true v.capital_low;
  Alcotest.(check (option int))
    "exhausted at session 0"
    (Some 0)
    o.first_exhausted_session;
  near 0.98505 o.pool_min_drawdown;
  near 0.98505 o.d_surv;
  near 0.98505 v.pool_min_drawdown;
  near 2.99 o.final_pool;
  near 2.0 o.final_base
;;

let test_separate_venues_all_survive () =
  (* The same three grids split into three positions on three venues each with
     its own 200 pool all survive: the venue-pool distinction between merged
     and split. *)
  let positions =
    List.init 3 (fun i ->
      position
        (Printf.sprintf "v%d" i)
        (Printf.sprintf "A%d" i)
        200.0
        [| pbar () |]
        [ grid_cfg () ])
  in
  let r = run ~positions () in
  Alcotest.(check bool) "no venue exhausted" false r.exhausted;
  List.iter
    (fun (o : Dio_survival.Survival_portfolio.position_outcome) ->
       Alcotest.(check int) "ladder funded" 2 o.buy_fills;
       near 1.0 o.d_surv)
    r.positions
;;

let test_shared_pool_merge_ok_with_capital () =
  (* Two subgrids on a 400 venue pool: both ladders (197.01 x2) fit, no
     starvation. *)
  let subgrids = List.init 2 (fun _ -> grid_cfg ()) in
  let r = run ~positions:[ position "hype" "HYPE" 400.0 [| pbar () |] subgrids ] () in
  let o = find_o "hype" "HYPE" r in
  Alcotest.(check int) "both ladders funded" 4 o.buy_fills;
  Alcotest.(check bool) "no starvation" false o.capital_low;
  near 1.0 o.d_surv;
  near 5.98 o.final_pool
;;

let test_same_venue_assets_share_one_pool () =
  (* Two assets on the SAME venue share one pool: BTC's 200-pool is the venue
     pool, so BTC's own ladder (197.01) fits, but HYPE's ladder on the same
     venue is starved. *)
  let positions =
    [ position "kraken" "BTC" 200.0 [| pbar () |] [ grid_cfg () ]
    ; position "kraken" "HYPE" 0.0 [| pbar () |] [ grid_cfg () ]
    ]
  in
  let r = run ~positions () in
  let bt = find_o "kraken" "BTC" r in
  let hy = find_o "kraken" "HYPE" r in
  let v = find_v "kraken" "USD" r in
  Alcotest.(check int) "btc ladder funded" 2 bt.buy_fills;
  Alcotest.(check int) "hype ladder starved" 0 hy.buy_fills;
  Alcotest.(check bool) "btc not capital low" false bt.capital_low;
  Alcotest.(check bool) "hype capital low" true hy.capital_low;
  Alcotest.(check bool) "venue capital low" true v.capital_low;
  near 0.98505 v.pool_min_drawdown;
  Alcotest.(check int) "venue assets" 2 (List.length v.assets)
;;

let test_same_venue_assets_share_one_pool_enough_capital () =
  (* With a 400 pool both assets' ladders fit from the shared venue pool. *)
  let positions =
    [ position "kraken" "BTC" 400.0 [| pbar () |] [ grid_cfg () ]
    ; position "kraken" "HYPE" 0.0 [| pbar () |] [ grid_cfg () ]
    ]
  in
  let r = run ~positions () in
  let bt = find_o "kraken" "BTC" r in
  let hy = find_o "kraken" "HYPE" r in
  Alcotest.(check int) "btc ladder funded" 2 bt.buy_fills;
  Alcotest.(check int) "hype ladder funded" 2 hy.buy_fills;
  Alcotest.(check bool) "no starvation" false r.exhausted;
  near 1.0 bt.d_surv;
  near 5.98 bt.final_pool
;;

let test_independent_venue_pools () =
  (* Pools are per venue account: exhausting one venue's pool must not touch
     the other's budget. *)
  let positions =
    [ position "kraken" "BTC" 200.0 [| pbar () |] (List.init 3 (fun _ -> grid_cfg ()))
    ; position "hype" "HYPE" 200.0 [| pbar () |] [ grid_cfg () ] ~quote:"USDC"
    ]
  in
  let r = run ~positions () in
  let kl = find_o "kraken" "BTC" r in
  let hy = find_o "hype" "HYPE" r in
  Alcotest.(check bool) "kraken exhausted" true kl.capital_low;
  Alcotest.(check bool) "hype survives" false hy.capital_low;
  near 1.0 hy.d_surv;
  Alcotest.(check int) "hype ladder funded" 2 hy.buy_fills;
  Alcotest.(check int) "kraken one ladder funded" 2 kl.buy_fills;
  Alcotest.(check bool) "portfolio exhausted" true r.exhausted;
  Alcotest.(check int) "two venues" 2 (List.length r.venues)
;;

let test_transfer_rescues_position () =
  (* B's 200-pool runs down to 2.99 after session 0. An idle position A on
     another venue donates 300 at session 1 before B's second bar, funding the
     97.03 buy that would otherwise be blocked (capital-low). *)
  let a_key =
    Dio_survival.Survival_portfolio.{ venue = "kraken"; quote = "USD"; testnet = false }
  in
  let b_key =
    Dio_survival.Survival_portfolio.{ venue = "hype"; quote = "USDC"; testnet = false }
  in
  let bars = [| pbar (); pbar ~low:96.5 ~high:97.8 () |] in
  let with_transfer =
    let positions =
      [ position "kraken" "USD" 500.0 [||] []
      ; position "hype" "HYPE" 200.0 bars [ grid_cfg () ] ~quote:"USDC"
      ]
    in
    let transfers =
      [ { Dio_survival.Survival_portfolio.session = 1
        ; from = a_key
        ; to_ = b_key
        ; amount = 300.0
        }
      ]
    in
    run ~positions ~transfers ()
  in
  let hy = find_o "hype" "HYPE" with_transfer in
  Alcotest.(check bool) "rescued before blocked buy" false hy.capital_low;
  near 1.0 hy.d_surv;
  Alcotest.(check int) "three buys" 3 hy.buy_fills;
  near 205.9601 hy.final_pool;
  Alcotest.(check int) "two sessions" 2 with_transfer.n_sessions;
  near 200.0 (find_o "kraken" "USD" with_transfer).final_pool;
  (* Without the transfer the 97.03 buy is blocked at session 1. *)
  let without =
    run ~positions:[ position "hype" "HYPE" 200.0 bars [ grid_cfg () ] ~quote:"USDC" ] ()
  in
  let hy' = find_o "hype" "HYPE" without in
  Alcotest.(check bool) "blocked without rescue" true hy'.capital_low;
  near 0.98505 hy'.d_surv
;;

let test_transfer_capped_at_pool () =
  (* A transfer larger than the source pool is capped at the source balance.
     Applied even though no position has any bars (n_sessions = 0). *)
  let a_key =
    Dio_survival.Survival_portfolio.{ venue = "a"; quote = "USD"; testnet = false }
  in
  let b_key =
    Dio_survival.Survival_portfolio.{ venue = "b"; quote = "USD"; testnet = false }
  in
  let positions = [ position "a" "x" 100.0 [||] []; position "b" "y" 100.0 [||] [] ] in
  let transfers =
    [ { Dio_survival.Survival_portfolio.session = 0
      ; from = a_key
      ; to_ = b_key
      ; amount = 500.0
      }
    ]
  in
  let r = run ~positions ~transfers () in
  near 0.0 (find_o "a" "x" r).final_pool;
  near 200.0 (find_o "b" "y" r).final_pool;
  Alcotest.(check int) "no sessions" 0 r.n_sessions
;;

let test_aligned_missing_bar_does_not_trade () =
  let timeline = [| "d1"; "d2"; "d3" |] in
  let positions : Dio_survival.Survival_portfolio.aligned_position_input list =
    [ { venue = "hype"
      ; asset = "HYPE"
      ; quote = "USDC"
      ; testnet = false
      ; pool = 1_000.0
      ; initial_base = 0.0
      ; bars = [| Some (pbar ()); None; Some (pbar ~low:96.5 ~high:97.8 ()) |]
      ; subgrids = [ grid_cfg () ]
      }
    ]
  in
  let result =
    Dio_survival.Survival_portfolio.simulate_aligned ~timeline ~positions ~transfers:[] ()
  in
  let outcome = find_o "hype" "HYPE" result in
  Alcotest.(check int) "timeline retained" 3 result.n_sessions;
  Alcotest.(check int) "only present bars trade" 3 outcome.buy_fills
;;

let test_venue_pool_trough_is_joint () =
  (* Two positions on the SAME venue each buy 99 from a shared 200 pool. The
     actual pool hits 2 (drawdown 0.99); per-position own-delta tracking would
     report only 0.495 for each. Every position and the venue headline must
     report the joint trough. *)
  let bars = [| pbar ~low:98.5 () |] in
  let r =
    run
      ~positions:
        [ position "hype" "A" 100.0 bars [ grid_cfg () ]
        ; position "hype" "B" 100.0 bars [ grid_cfg () ]
        ]
      ()
  in
  let a = find_o "hype" "A" r in
  let b = find_o "hype" "B" r in
  let v = find_v "hype" "USD" r in
  Alcotest.(check int) "A buys once" 1 a.buy_fills;
  Alcotest.(check int) "B buys once" 1 b.buy_fills;
  Alcotest.(check bool) "no position capital low" false r.exhausted;
  near 0.99 a.pool_min_drawdown;
  near 0.99 b.pool_min_drawdown;
  near 0.99 v.pool_min_drawdown;
  near 2.0 a.final_pool;
  near 2.0 v.final_pool
;;

let () =
  Alcotest.run
    "survival_portfolio"
    [ ( "portfolio"
      , [ Alcotest.test_case
            "shared pool merge (venue pool)"
            `Quick
            test_shared_pool_merge
        ; Alcotest.test_case
            "separate venues all survive"
            `Quick
            test_separate_venues_all_survive
        ; Alcotest.test_case
            "shared pool merge ok with capital"
            `Quick
            test_shared_pool_merge_ok_with_capital
        ; Alcotest.test_case
            "same venue assets share one pool"
            `Quick
            test_same_venue_assets_share_one_pool
        ; Alcotest.test_case
            "same venue assets share one pool with enough capital"
            `Quick
            test_same_venue_assets_share_one_pool_enough_capital
        ; Alcotest.test_case "independent venue pools" `Quick test_independent_venue_pools
        ; Alcotest.test_case
            "transfer rescues a position"
            `Quick
            test_transfer_rescues_position
        ; Alcotest.test_case
            "transfer capped at source pool"
            `Quick
            test_transfer_capped_at_pool
        ; Alcotest.test_case
            "aligned missing bar does not trade"
            `Quick
            test_aligned_missing_bar_does_not_trade
        ; Alcotest.test_case
            "venue pool trough is joint"
            `Quick
            test_venue_pool_trough_is_joint
        ] )
    ]
;;
