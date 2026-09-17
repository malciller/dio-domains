(* Hot-path O(1) invariants.

   The engine is advertised as high-frequency: per-cycle work must be O(1) in the number
   of resting orders, not O(n). These tests enforce that by comparing per-cycle minor-word
   allocation for a small vs a large open-order set and asserting the difference does not
   scale with n.

   This is deliberately NOT an absolute allocation budget: absolute word counts move with
   the compiler profile and any legitimate behavior change, which would make a threshold
   flaky or (if loosened) useless. Comparing two configurations of the same compiled code
   isolates the n-scaling term, which is exactly the contract we care about (a
   re-introduced per-order fold shows up as ~10 words/order x n).

   A reference-model test also guards the incremental [Sell_orders] aggregates ([sum_qty],
   [exists_price_leq]) against the linear definitions they replaced. *)

module Sell_orders = Dio_strategies.Strategy_sell_orders
open Dio_strategies.Strategy_api

(* ---- reference model for the maintained aggregates ------------------------------ *)

let model_sum (m : (string * (float * float)) list) =
  List.fold_left (fun a (_, (_, q)) -> a +. q) 0.0 m
;;

let model_exists_leq (m : (string * (float * float)) list) x =
  List.exists (fun (_, (p, _)) -> p <= x) m
;;

let test_sell_orders_aggregates_correct () =
  (* Randomized mutation stream validated against the linear reference, including the
     min-removal recompute path. *)
  let t = Sell_orders.create 8 in
  let m = ref [] in
  let push id p q =
    Sell_orders.push t id p q;
    m := (id, (p, q)) :: !m
  in
  let remove id =
    ignore (Sell_orders.remove_by_id t id);
    m := List.filter (fun (i, _) -> i <> id) !m
  in
  let replace id p q =
    Sell_orders.replace_first t (fun i _ _ -> i = id) (fun i _ _ -> i, p, q);
    m := List.map (fun (i, (op, oq)) -> if i = id then i, (p, q) else i, (op, oq)) !m
  in
  let ok () =
    Alcotest.(check (float 1e-9))
      "sum matches reference"
      (model_sum !m)
      (Sell_orders.sum_qty t);
    Alcotest.(check int)
      "length matches reference"
      (List.length !m)
      (Sell_orders.length t);
    List.iter
      (fun x ->
        Alcotest.(check bool)
          (Printf.sprintf "exists_price_leq %.4f matches reference" x)
          (model_exists_leq !m x)
          (Sell_orders.exists_price_leq t x))
      [ -1.0; 0.0; 99.0; 100.0; 100.5; 105.0; 1e9 ]
  in
  push "a" 100.0 1.0;
  push "b" 101.0 2.0;
  push "c" 99.0 0.5;
  ok ();
  (* Remove the current minimum: exercises the O(n) recompute. *)
  remove "c";
  ok ();
  push "d" 98.5 3.0;
  ok ();
  (* Replace (re-key) an existing order's qty and price. *)
  replace "a" 102.0 4.0;
  ok ();
  (* Remove the new minimum again. *)
  remove "d";
  ok ();
  remove "a";
  remove "b";
  ok ();
  (* Empty: sum 0, no price <= anything. *)
  Alcotest.(check (float 1e-9)) "empty sum" 0.0 (Sell_orders.sum_qty t);
  Alcotest.(check bool) "empty leq" false (Sell_orders.exists_price_leq t 1e9);
  (* Prefix removal recompute. *)
  push "pending_sell_1" 97.0 1.0;
  push "live" 103.0 2.0;
  Sell_orders.remove_prefix t "pending_sell_";
  Alcotest.(check (float 1e-9)) "after prefix remove sum" 2.0 (Sell_orders.sum_qty t);
  Alcotest.(check bool)
    "after prefix remove leq"
    false
    (Sell_orders.exists_price_leq t 100.0)
;;

let measure_words f k =
  let b = Gc.minor_words () in
  for _ = 1 to k do
    f ()
  done;
  (Gc.minor_words () -. b) /. float k
;;

let test_measurement_is_sensitive () =
  (* The instrument must actually detect n-scaling allocation, otherwise the O(1)
     assertions below could pass vacuously. [List.init n] allocates ~n cons cells; the
     measured per-call allocation must rise with n. This is the positive control for the
     whole file. *)
  let alloc n = measure_words (fun () -> ignore (List.init n (fun i -> i))) 2000 in
  let a2 = alloc 2 in
  let a64 = alloc 64 in
  Alcotest.(check bool)
    (Printf.sprintf
       "measure_words detects n-scaling allocation (n=2 %.2f, n=64 %.2f)"
       a2
       a64)
    true
    (a64 -. a2 > 50.0)
;;

let test_sell_orders_reads_allocate_constant () =
  (* The two per-cycle reads must allocate nothing, and nothing that scales with n. *)
  let build n =
    let t = Sell_orders.create n in
    for i = 1 to n do
      Sell_orders.push t (Printf.sprintf "s-%d" i) (100.0 +. float i) 1.0
    done;
    t
  in
  let read_alloc n x =
    let t = build n in
    measure_words
      (fun () ->
        ignore (Sell_orders.sum_qty t);
        ignore (Sell_orders.exists_price_leq t x))
      1000
  in
  let small = read_alloc 2 101.0 in
  let large = read_alloc 256 101.0 in
  Alcotest.(check bool)
    "sum_qty/exists_price_leq allocate ~0 words at n=2"
    true
    (small < 1.0);
  Alcotest.(check bool)
    (Printf.sprintf "read allocation is n-independent (n=2 %.3f, n=256 %.3f)" small large)
    true
    (large -. small < 1.0)
;;

(* ---- sync_open_orders allocation must not scale with open-order count ------------- *)

let make_case ~symbol n =
  let state = get_strategy_state symbol in
  state.exchange_id <- "kraken";
  state.cached_ecfg <- get_exchange_config "kraken";
  Hashtbl.clear state.sell_commitments;
  Sell_orders.clear state.open_sell_orders;
  Sell_orders.clear state.cached_feed_sell_orders;
  Hashtbl.reset state.feed_sell_index;
  Hashtbl.reset state.feed_buy_index;
  state.feed_index_valid <- false;
  state.open_orders_scan_valid <- false;
  state.sell_commitments_clean <- false;
  let asset =
    { exchange = "kraken"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = None
    ; accumulation_buffer = 0.0
    ; base_accumulation = false
    ; sell_levels_persistence = false
    }
  in
  let ecfg = get_exchange_config "kraken" in
  let feed =
    ref
      (List.init n (fun i ->
         Printf.sprintf "%s-s%d" symbol i, 100.0 +. float i, 1.0, "sell", None))
  in
  let iter f = List.iter (fun (id, p, q, s, u) -> f id p q s u) !feed in
  let gen = ref 1 in
  let pending = ref [] in
  let overflow = ref false in
  let drain ~symbol:_ =
    let c = !pending in
    pending := [];
    c, !overflow
  in
  let sync () =
    sync_open_orders
      ~state
      ~now:100.0
      ~asset
      ~bid_price:100.0
      ~lot_qty:1.0
      ~iter_open_orders:iter
      ~get_open_orders_generation:(fun () -> !gen)
      ~drain_open_order_changes:drain
      ~ecfg
  in
  (* Prime the persistent index with a full scan. *)
  ignore (sync ());
  state, sync, gen, pending, feed
;;

let test_sync_skip_alloc_n_independent () =
  let run n =
    let _state, sync, _gen, _pending, _feed =
      make_case ~symbol:("SKIP" ^ string_of_int n) n
    in
    measure_words (fun () -> ignore (sync ())) 2000
  in
  let w2 = run 8 in
  let w64 = run 512 in
  Alcotest.(check bool)
    (Printf.sprintf
       "skip-cycle allocation is n-independent (n=8 %.2f, n=512 %.2f)"
       w2
       w64)
    true
    (abs_float (w64 -. w2) < 16.0)
;;

let delta_alloc ~symbol ~n ~pick ~k =
  let _state, sync, gen, pending, feed = make_case ~symbol n in
  let id, p, q, s, u = pick !feed in
  let change = [ id, Some (Some p, q, s, u) ] in
  measure_words
    (fun () ->
      incr gen;
      pending := change;
      ignore (sync ()))
    k
;;

let test_primitives_alloc_n_independent () =
  (* The Sell_orders mutators the delta path uses must not allocate by n. [remove_by_id]
     is exercised on an existing (and the minimum) id so the min-removal recompute runs. *)
  let store n =
    let t = Sell_orders.create n in
    for i = 1 to n do
      Sell_orders.push t (Printf.sprintf "s-%d" i) (100.0 +. float i) 1.0
    done;
    t
  in
  let per_call n f =
    let t = store n in
    measure_words (fun () -> f t) 2000
  in
  let rem2 = per_call 2 (fun t -> ignore (Sell_orders.remove_by_id t "s-1")) in
  let rem64 = per_call 64 (fun t -> ignore (Sell_orders.remove_by_id t "s-1")) in
  let push2 = per_call 2 (fun t -> Sell_orders.push t "x" 1.0 1.0) in
  let push64 = per_call 64 (fun t -> Sell_orders.push t "x" 1.0 1.0) in
  let blit n =
    let s = store n in
    let d = store n in
    measure_words (fun () -> Sell_orders.blit ~src:s ~dst:d) 2000
  in
  let b2 = blit 2 in
  let b64 = blit 64 in
  Alcotest.(check bool)
    (Printf.sprintf "remove_by_id (min) flat (n=2 %.2f, n=64 %.2f)" rem2 rem64)
    true
    (abs_float (rem64 -. rem2) < 2.0);
  Alcotest.(check bool)
    (Printf.sprintf "push flat (n=2 %.2f, n=64 %.2f)" push2 push64)
    true
    (abs_float (push64 -. push2) < 2.0);
  Alcotest.(check bool)
    (Printf.sprintf "blit flat (n=2 %.2f, n=64 %.2f)" b2 b64)
    true
    (abs_float (b64 -. b2) < 2.0)
;;

let test_sync_delta_alloc_n_independent () =
  (* A single-order delta on a large book must cost the same as on a small one, whether
     the changed order is the current closest sell (retract+readd plus the min recompute)
     or a middle rung (pure O(1) retract+readd). *)
  let first l = List.hd l in
  let last l = List.nth l (List.length l - 1) in
  let check_pair label a2 a64 =
    Alcotest.(check bool)
      (Printf.sprintf "%s is n-independent (n=8 %.2f, n=512 %.2f)" label a2 a64)
      true
      (abs_float (a64 -. a2) < 16.0)
  in
  check_pair
    "delta amend (closest)"
    (delta_alloc ~symbol:"DELTA_A8" ~n:8 ~pick:first ~k:2000)
    (delta_alloc ~symbol:"DELTA_A512" ~n:512 ~pick:first ~k:2000);
  check_pair
    "delta amend (middle)"
    (delta_alloc ~symbol:"DELTA_M8" ~n:8 ~pick:last ~k:2000)
    (delta_alloc ~symbol:"DELTA_M512" ~n:512 ~pick:last ~k:2000)
;;

let time_delta_us ~symbol ~n ~pick ~k =
  let _state, sync, gen, pending, feed = make_case ~symbol n in
  let id, p, q, s, u = pick !feed in
  let change = [ id, Some (Some p, q, s, u) ] in
  (* Warm. *)
  for i = 1 to 500 do
    incr gen;
    pending := change;
    ignore (sync ());
    ignore i
  done;
  let t0 = Sys.time () in
  for _ = 1 to k do
    incr gen;
    pending := change;
    ignore (sync ())
  done;
  (Sys.time () -. t0) /. float k *. 1e6
;;

let test_delta_min_recompute_cost () =
  (* Quantify the O(n) min-recompute that fires when the closest sell leaves: a
     closest-amend delta vs a middle-amend delta at n=64. If this delta is tiny the scan
     is not worth a heap/index. Informational, not an assertion. *)
  let first l = List.hd l in
  let last l = List.nth l (List.length l - 1) in
  let k = 50_000 in
  let closest = time_delta_us ~symbol:"TIME_C" ~n:64 ~pick:first ~k in
  let middle = time_delta_us ~symbol:"TIME_M" ~n:64 ~pick:last ~k in
  Printf.printf
    "probe-time n=64: closest-amend=%.3fus middle-amend=%.3fus (min-recompute cost \
     ~%.3fus)\n\
     %!"
    closest
    middle
    (closest -. middle)
;;

let () =
  test_delta_min_recompute_cost ();
  Alcotest.run
    "hot_path_invariants"
    [ ( "measurement sensitivity"
      , [ Alcotest.test_case
            "measure_words detects n-scaling allocation"
            `Quick
            test_measurement_is_sensitive
        ] )
    ; ( "sell_orders aggregates"
      , [ Alcotest.test_case
            "matches reference model"
            `Quick
            test_sell_orders_aggregates_correct
        ; Alcotest.test_case
            "reads are allocation-free and n-independent"
            `Quick
            test_sell_orders_reads_allocate_constant
        ] )
    ; ( "sync_open_orders O(1)"
      , [ Alcotest.test_case
            "skip-cycle allocation is n-independent"
            `Quick
            test_sync_skip_alloc_n_independent
        ; Alcotest.test_case
            "primitives do not allocate by n"
            `Quick
            test_primitives_alloc_n_independent
        ; Alcotest.test_case
            "delta-cycle allocation is n-independent"
            `Quick
            test_sync_delta_alloc_n_independent
        ] )
    ]
;;
