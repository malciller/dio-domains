(* Oracle_pools unit tests - pins allocation order, pass-down, cascades,
   and sell sizing. *)

let claim ~id ~priority ~need_quote ?(resting = 0.0) () =
  { Dio_oracle.Oracle_pools.id; priority; need_quote; resting_buy_quote = resting }
;;

let near ?(eps = 1e-9) name a b = Alcotest.(check (float eps)) name a b
let has_id lst id = List.mem id lst

(* ------------------------------------------------------------------ *)
(* allocate                                                           *)
(* ------------------------------------------------------------------ *)

let test_allocate_pass_down () =
  (* Available 100: A(60) funded -> 40 left; B(50) skipped; C(30) funded
     from what B could not use - capacity passes down. *)
  let vq =
    { Dio_oracle.Oracle_pools.available = 100.0
    ; claims =
        [ claim ~id:"A" ~priority:0 ~need_quote:60.0 ()
        ; claim ~id:"B" ~priority:1 ~need_quote:50.0 ()
        ; claim ~id:"C" ~priority:2 ~need_quote:30.0 ()
        ]
    }
  in
  let a = Dio_oracle.Oracle_pools.allocate vq in
  Alcotest.(check bool) "A funded" (has_id a.funded_ids "A") true;
  Alcotest.(check bool) "B starved" (has_id a.starved_ids "B") true;
  Alcotest.(check bool) "lower-priority C not starved" (has_id a.funded_ids "C") true
;;

let test_allocate_exact_fit_and_order () =
  (* Presentation order wins ties on scarce quote: exact fit funds. *)
  let vq =
    { Dio_oracle.Oracle_pools.available = 50.0
    ; claims =
        [ claim ~id:"late" ~priority:5 ~need_quote:25.0 ()
        ; claim ~id:"early" ~priority:1 ~need_quote:50.0 ()
        ]
    }
  in
  let a = Dio_oracle.Oracle_pools.allocate vq in
  Alcotest.(check bool) "early exact-fit funded" (has_id a.funded_ids "early") true;
  Alcotest.(check bool) "late starved" (has_id a.starved_ids "late") true
;;

let test_allocate_resting_counts_against_availability () =
  (* The caller passes availability net of resting buys - the engine treats
     that number as the whole truth. *)
  let vq =
    { Dio_oracle.Oracle_pools.available = 20.0
    ; claims = [ claim ~id:"A" ~priority:0 ~need_quote:21.0 () ]
    }
  in
  let a = Dio_oracle.Oracle_pools.allocate vq in
  Alcotest.(check bool) "just-short need starves" (a.funded_ids = []) true
;;

(* ------------------------------------------------------------------ *)
(* cascade                                                            *)
(* ------------------------------------------------------------------ *)

let test_cascade_noop_when_fits () =
  let cs = [ claim ~id:"low" ~priority:2 ~need_quote:10.0 ~resting:30.0 () ] in
  Alcotest.(check bool)
    "fitting need cancels nothing"
    (Dio_oracle.Oracle_pools.cascade
       ~available:100.0
       ~need:50.0
       ~trigger_id:"high"
       ~claims:cs
     = [])
    true
;;

let test_cascade_single_cancel () =
  let cs =
    [ claim ~id:"mid" ~priority:1 ~need_quote:10.0 ~resting:40.0 ()
    ; claim ~id:"low" ~priority:2 ~need_quote:10.0 ~resting:70.0 ()
    ]
  in
  (* Deficit 30: lowest priority first - cancelling low's 70 covers it and
     the engine stops there (mid keeps its order). *)
  Alcotest.(check (list string))
    "single sufficient cancel"
    [ "low" ]
    (Dio_oracle.Oracle_pools.cascade
       ~available:70.0
       ~need:100.0
       ~trigger_id:"high"
       ~claims:cs)
;;

let test_cascade_many_lesser_orders () =
  let cs =
    [ claim ~id:"high" ~priority:0 ~need_quote:100.0 ~resting:80.0 ()
    ; claim ~id:"m1" ~priority:1 ~need_quote:10.0 ~resting:40.0 ()
    ; claim ~id:"m2" ~priority:2 ~need_quote:10.0 ~resting:30.0 ()
    ; claim ~id:"low" ~priority:3 ~need_quote:10.0 ~resting:200.0 ()
    ]
  in
  (* Available 50, need 100 -> deficit 50. Lowest first: low alone covers
     it; the engine stops at the FIRST fit. *)
  Alcotest.(check (list string))
    "stops at first fit"
    [ "low" ]
    (Dio_oracle.Oracle_pools.cascade
       ~available:50.0
       ~need:100.0
       ~trigger_id:"high"
       ~claims:cs);
  (* Without the deep pool, two lesser orders together satisfy one greater -
     and the trigger's own resting buy is never cancelled. *)
  let cs2 = List.filter (fun (c : Dio_oracle.Oracle_pools.claim) -> c.id <> "low") cs in
  Alcotest.(check (list string))
    "many lesser satisfy one greater, trigger spared"
    [ "m2"; "m1" ]
    (Dio_oracle.Oracle_pools.cascade
       ~available:50.0
       ~need:100.0
       ~trigger_id:"high"
       ~claims:cs2)
;;

let test_cascade_impossible_gives_up () =
  let cs = [ claim ~id:"low" ~priority:2 ~need_quote:10.0 ~resting:5.0 () ] in
  Alcotest.(check bool)
    "no combination fits -> empty plan"
    (Dio_oracle.Oracle_pools.cascade
       ~available:10.0
       ~need:100.0
       ~trigger_id:"high"
       ~claims:cs
     = [])
    true
;;

(* ------------------------------------------------------------------ *)
(* sell_qty_of                                                        *)
(* ------------------------------------------------------------------ *)

let test_sell_qty () =
  near
    "base minus reserved minus resting sells"
    (Dio_oracle.Oracle_pools.sell_qty_of
       ~base_balance:10.0
       ~reserved_base:2.0
       ~resting_sell_base:3.0)
    5.0;
  near
    "clamped at zero"
    (Dio_oracle.Oracle_pools.sell_qty_of
       ~base_balance:1.0
       ~reserved_base:2.0
       ~resting_sell_base:3.0)
    0.0
;;

let () =
  Alcotest.run
    "oracle_pools"
    [ ( "allocate"
      , [ Alcotest.test_case
            "presentation order with pass-down"
            `Quick
            test_allocate_pass_down
        ; Alcotest.test_case
            "exact fit and order"
            `Quick
            test_allocate_exact_fit_and_order
        ; Alcotest.test_case
            "availability is the caller's number"
            `Quick
            test_allocate_resting_counts_against_availability
        ] )
    ; ( "cascade"
      , [ Alcotest.test_case "noop when it fits" `Quick test_cascade_noop_when_fits
        ; Alcotest.test_case "single cancel" `Quick test_cascade_single_cancel
        ; Alcotest.test_case "many lesser orders" `Quick test_cascade_many_lesser_orders
        ; Alcotest.test_case "impossible gives up" `Quick test_cascade_impossible_gives_up
        ] )
    ; "sell qty", [ Alcotest.test_case "pool arithmetic" `Quick test_sell_qty ]
    ]
;;
