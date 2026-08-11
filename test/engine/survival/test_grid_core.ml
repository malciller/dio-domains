(* Grid_core state machine behaviour tests. *)

let cfg
      ?(qty = 1.0)
      ?(sell_mult = 1.0)
      ?(grid_interval_pct = 1.0)
      ?(price_increment = 1e-9)
      ?(qty_increment = 1e-9)
      ?(qty_min = 0.0)
      ?(min_notional = 0.0)
      ?(start_price = 100.0)
      ?(start_quote = 10_000.0)
      ?(fee = 0.0)
      ?(model = Dio_strategies.Grid_core_types.Hyperliquid)
      ()
  =
  let open Dio_strategies.Grid_core in
  { qty
  ; sell_mult
  ; grid_interval_pct
  ; maker_fee = fee
  ; accumulation_buffer = 0.0
  ; price_increment
  ; qty_increment
  ; qty_min
  ; min_notional
  ; exchange_model = model
  ; start_price
  ; start_quote
  ; cash_hook = None
  }
;;

let bar ?(high = -1.0) ?(low = -1.0) ?(close = -1.0) () =
  let low = if low < 0.0 then high else low in
  let high = if high < 0.0 then low else high in
  let close = if close < 0.0 then low else close in
  Dio_strategies.Grid_core_types.{ high; low; close }
;;

let near a b = Alcotest.(check (float 1e-6)) "approx" a b

let test_initial_buy_level () =
  let c = cfg () in
  let st = Dio_strategies.Grid_core.create c in
  match st.resting_buy with
  | Some b -> near 99.0 b
  | None -> Alcotest.fail "expected resting buy"
;;

let test_buy_ladders_down_single_bar () =
  let c = cfg () in
  let st = Dio_strategies.Grid_core.create c in
  let fs =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~low:98.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  (* Buys at 99.0 then 98.01 (geometric ladder). *)
  Alcotest.(check int) "two buys" 2 st.buy_fills;
  Alcotest.(check int) "two fills" 2 (List.length fs);
  near 98.01 (List.nth fs 1).price;
  (* Quote consumed: 99 + 98.01 = 197.01 *)
  near 9_802.99 st.quote
;;

let test_sell_fills_after_buy () =
  let c = cfg () in
  let st = Dio_strategies.Grid_core.create c in
  let _ =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~low:99.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  (* Buy at 99.0 -> sell resting at 99.99. *)
  (match st.resting_sell with
   | Some s -> near 99.99 s
   | None -> Alcotest.fail "expected resting sell");
  let fs =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~high:101.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check int) "one sell fill" 1 st.sell_fills;
  Alcotest.(check int) "one fill" 1 (List.length fs);
  near 99.99 (List.hd fs).price;
  (* Quote replenished: 10000 - 99 + 99.99 (fee 0). *)
  near 10_000.99 st.quote
;;

let test_capital_low_exhaustion () =
  (* One buy affordable; second ladder step is not. *)
  let c = cfg ~start_quote:(99.0 +. 1e-3) () in
  let st = Dio_strategies.Grid_core.create c in
  let _ =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~low:50.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check bool) "exhausted" true st.ever_capital_low;
  (match st.first_capital_low_buy with
   | Some b -> near 98.01 b
   | None -> Alcotest.fail "expected first capital-low buy level");
  let res =
    Dio_strategies.Grid_core.replay
      c
      ~bars:[| bar ~low:50.0 () |]
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check bool) "replay exhausted" true res.exhausted;
  match res.first_capital_low_drawdown with
  | Some dd -> near (1.0 -. (98.01 /. 100.0)) dd
  | None -> Alcotest.fail "expected capital-low drawdown"
;;

let test_capital_low_recovers () =
  (* Small quote: the ladder exhausts, then the resting sell replenishes the
     quote and buying resumes on the next bar. *)
  let c = cfg ~start_quote:500.0 () in
  let bars =
    [| bar ~low:99.0 () (* buy 99 -> quote 401, sell 99.99 resting, not filled *)
     ; bar ~high:102.0 ~low:90.0 ()
       (* ladder 98.01/97.03/96.06/95.10 then blocked at 94.15;
          sell 96.051 fills -> quote ~110.85 *)
     ; bar ~high:95.0 ~low:92.0 () (* buy resumes at 93.21 -> quote 17.64 *)
    |]
  in
  let res =
    Dio_strategies.Grid_core.replay
      c
      ~bars
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check bool) "exhausted at some point" true res.exhausted;
  Alcotest.(check int) "buys (1 + 4 + 1)" 6 res.buy_fills;
  Alcotest.(check int) "sells" 1 res.sell_fills;
  match res.first_capital_low_drawdown with
  | Some dd -> near (1.0 -. (0.99 ** 6.0)) dd
  | None -> Alcotest.fail "expected capital-low drawdown"
;;

let test_ordering_affects_trough () =
  (* Same V-bar: buy-first consumes before the sell replenishes, so the quote
     trough is deeper than sell-first. *)
  let c = cfg () in
  let bars = [| bar ~low:99.0 (); bar ~high:103.0 ~low:90.0 () |] in
  let r_bf =
    Dio_strategies.Grid_core.replay
      c
      ~bars
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  let r_sf =
    Dio_strategies.Grid_core.replay
      c
      ~bars
      ~ordering:Dio_strategies.Grid_core_types.Sell_first
  in
  Alcotest.(check bool) "buy-first trough deeper" (r_bf.min_quote < r_sf.min_quote) true;
  Alcotest.(check bool) "same buy count" (r_bf.buy_fills = r_sf.buy_fills) true
;;

let test_no_sell_without_buy () =
  (* A rising bar (low = high = 200) never crosses the resting buy at 99 and
     there is no resting sell, so nothing fills. *)
  let c = cfg () in
  let st = Dio_strategies.Grid_core.create c in
  let fs =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~high:200.0 ~low:200.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check int) "no fills" 0 (List.length fs);
  Alcotest.(check int) "no sells" 0 st.sell_fills
;;

let test_min_notional_stops_ladder () =
  (* Hyperliquid spot floor: with min_notional = 90 and qty = 1.0, the ladder
     fills at 99.0 .. 90.45 (notional >= 90) and is blocked at 89.54 even
     though capital is ample. *)
  let c = cfg ~min_notional:90.0 ~start_quote:10_000.0 () in
  let res =
    Dio_strategies.Grid_core.replay
      c
      ~bars:[| bar ~low:5.0 () |]
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check int) "buys down to 90.45" 10 res.buy_fills;
  Alcotest.(check bool) "exhausted on notional floor" true res.exhausted;
  (* Blocked level 99*0.99^10 = 89.53 < 90 -> drawdown 1 - 0.99^11. *)
  match res.first_capital_low_drawdown with
  | Some dd -> near (1.0 -. (0.99 ** 11.0)) dd
  | None -> Alcotest.fail "expected blocked level"
;;

let test_min_notional_blocks_reduced_sell () =
  (* Kraken sells qty * sell_mult = 0.05; with min_notional = 50 the reduced
     sell notional 0.05 * 99.99 < 50 is not placeable and no sell fills. *)
  let c =
    cfg ~sell_mult:0.05 ~min_notional:50.0 ~model:Dio_strategies.Grid_core_types.Kraken ()
  in
  let st = Dio_strategies.Grid_core.create c in
  let _ =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~low:99.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  let fs =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~high:101.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check int) "no sells below min notional" 0 st.sell_fills;
  Alcotest.(check int) "no fills" 0 (List.length fs)
;;

let test_min_notional_full_sell_fills () =
  (* Same Kraken setup but with a low floor: the reduced sell notional
     0.05 * 99.99 >= 0.05 so it fills. *)
  let c =
    cfg ~sell_mult:0.05 ~min_notional:0.05 ~model:Dio_strategies.Grid_core_types.Kraken ()
  in
  let st = Dio_strategies.Grid_core.create c in
  let _ =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~low:99.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  let fs =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~high:101.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check int) "sell fills" 1 st.sell_fills;
  Alcotest.(check int) "one fill" 1 (List.length fs);
  near 0.05 (List.hd fs).qty
;;

let test_qty_min_blocks_under_min_qty () =
  (* qty 0.05 < qty_min 0.1: the buy order is never placeable and the grid is
     exhausted at the first level. *)
  let c = cfg ~qty:0.05 ~qty_min:0.1 () in
  let res =
    Dio_strategies.Grid_core.replay
      c
      ~bars:[| bar ~low:99.0 () |]
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check int) "no buys" 0 res.buy_fills;
  Alcotest.(check bool) "exhausted" true res.exhausted
;;

let test_qty_min_allows_at_least_min () =
  (* qty = qty_min: the buy places (notional floor also clear). *)
  let c = cfg ~qty:0.1 ~qty_min:0.1 () in
  let st = Dio_strategies.Grid_core.create c in
  let fs =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~low:99.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check int) "one buy" 1 (List.length fs);
  Alcotest.(check int) "buy_fills" 1 st.buy_fills
;;

let cash_hook pool =
  { Dio_strategies.Grid_core.balance = (fun () -> !pool)
  ; spend = (fun a -> pool := !pool -. a)
  ; recover = (fun a -> pool := !pool +. a)
  }
;;

let test_cash_hook_spends_and_recovers () =
  (* With a cash hook, buys spend the ledger and sells recover it. *)
  let pool = ref 10_000.0 in
  let c = { (cfg ()) with cash_hook = Some (cash_hook pool) } in
  let st = Dio_strategies.Grid_core.create c in
  let _ =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~low:99.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  near 9_901.0 !pool;
  let _ =
    Dio_strategies.Grid_core.on_bar
      c
      ~state:st
      ~bar:(bar ~high:101.0 ())
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  near 10_000.99 !pool
;;

let test_cash_hook_shared_pool_merge () =
  (* Two subgrids on the same asset draw from one shared pool (ven-diagram
     merge): total buying is bounded by the pool, so when one grid's ladder
     exhausts it, the other cannot place a buy either. *)
  let pool = ref 150.0 in
  let hook = cash_hook pool in
  let mk gi = { (cfg ~grid_interval_pct:gi ()) with cash_hook = Some hook } in
  let g1 = mk 1.0 in
  let g2 = mk 2.0 in
  let s1 = Dio_strategies.Grid_core.create g1 in
  let s2 = Dio_strategies.Grid_core.create g2 in
  let crash = bar ~low:50.0 () in
  let _ =
    Dio_strategies.Grid_core.on_bar
      g1
      ~state:s1
      ~bar:crash
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  let _ =
    Dio_strategies.Grid_core.on_bar
      g2
      ~state:s2
      ~bar:crash
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  (* g1: buy 99 (pool 51); its next level 98.01 is unaffordable. g2: 98 not
     affordable from the exhausted pool. *)
  Alcotest.(check int) "g1 one buy" 1 s1.buy_fills;
  Alcotest.(check bool) "g1 capital low" true s1.ever_capital_low;
  Alcotest.(check int) "g2 no buys (pool shared)" 0 s2.buy_fills;
  Alcotest.(check bool) "g2 capital low from pool" true s2.ever_capital_low;
  near 51.0 !pool
;;

let test_cash_hook_replay_tracks_pool () =
  (* Grid_core.replay with a hook tracks the ledger the same way a
     self-contained grid tracks its own quote. *)
  let pool = ref 10_000.0 in
  let hook = cash_hook pool in
  let c = { (cfg ()) with cash_hook = Some hook } in
  let bars = [| bar ~low:99.0 (); bar ~high:103.0 ~low:90.0 () |] in
  let with_hook =
    Dio_strategies.Grid_core.replay
      c
      ~bars
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  let plain =
    Dio_strategies.Grid_core.replay
      (cfg ())
      ~bars
      ~ordering:Dio_strategies.Grid_core_types.Buy_first
  in
  Alcotest.(check int) "same buy fills" plain.buy_fills with_hook.buy_fills;
  Alcotest.(check int) "same sell fills" plain.sell_fills with_hook.sell_fills;
  near plain.min_quote with_hook.min_quote;
  near plain.min_quote_drawdown with_hook.min_quote_drawdown;
  near plain.final_quote with_hook.final_quote
;;

let () =
  Alcotest.run
    "grid_core"
    [ ( "state machine"
      , [ Alcotest.test_case "initial buy level" `Quick test_initial_buy_level
        ; Alcotest.test_case "buy ladders down" `Quick test_buy_ladders_down_single_bar
        ; Alcotest.test_case "sell fills after buy" `Quick test_sell_fills_after_buy
        ; Alcotest.test_case "capital_low exhaustion" `Quick test_capital_low_exhaustion
        ; Alcotest.test_case "capital_low recovery" `Quick test_capital_low_recovers
        ; Alcotest.test_case "ordering affects trough" `Quick test_ordering_affects_trough
        ; Alcotest.test_case "no sell without buy" `Quick test_no_sell_without_buy
        ; Alcotest.test_case
            "min notional stops ladder"
            `Quick
            test_min_notional_stops_ladder
        ; Alcotest.test_case
            "min notional blocks reduced sell"
            `Quick
            test_min_notional_blocks_reduced_sell
        ; Alcotest.test_case
            "min notional allows full sell"
            `Quick
            test_min_notional_full_sell_fills
        ; Alcotest.test_case
            "qty_min blocks under-min qty"
            `Quick
            test_qty_min_blocks_under_min_qty
        ; Alcotest.test_case
            "qty_min allows at least min"
            `Quick
            test_qty_min_allows_at_least_min
        ; Alcotest.test_case
            "cash hook spends and recovers"
            `Quick
            test_cash_hook_spends_and_recovers
        ; Alcotest.test_case
            "cash hook shared pool merge"
            `Quick
            test_cash_hook_shared_pool_merge
        ; Alcotest.test_case
            "cash hook replay tracks pool"
            `Quick
            test_cash_hook_replay_tracks_pool
        ] )
    ]
;;
