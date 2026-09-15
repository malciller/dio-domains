(* Allocation profile for [Strategy_api.sync_open_orders].

   Build with the release profile (`dune build --profile release
   test/engine/perf/sync_alloc_profile.exe`). Under the dev profile there is no
   cross-module inlining, so reading a [float] back across the [Strategy_sell_orders]
   module boundary boxes it (2 words/call) and the per-order index walks look
   quadratically allocating; release (-O3) inlines them unboxed, which is what production
   builds do.

   The production engine attributes a per-cycle minor-word budget to the sync stage
   ([alloc_sync_words]); this harness drives the *real* sync with a synthetic, prebuilt
   open-orders feed so the allocation can be attributed to the persisted-level rebuild vs
   the scan/reconcile, with no venue or I/O. The rebuild is now allocation-free in the
   steady no-change state, so the remaintain=T minus remaintain=F delta is the
   open/missing split only.

   The feed is precomputed so [iter_open_orders] allocates nothing per call (matching the
   venue iterators, which yield stored strings/literals). Each scenario is warmed up so
   commitment upserts and id tracking are steady-state, then measured over many calls.
   [remaintain_expired_sells] is the Alpaca flag that gates the persisted-ladder
   reconcile. *)

module JL = Dio_strategies.Strategy_api
module Sell_orders = Dio_strategies.Strategy_sell_orders

let alpaca_asset symbol =
  { JL.exchange = "alpaca"
  ; symbol
  ; qty = "1.0"
  ; grid_interval = 1.0
  ; sell_mult = "1.0"
  ; strategy = "Ladder"
  ; maker_fee = Some 0.0004
  ; taker_fee = None
  ; accumulation_buffer = 0.05
  ; base_accumulation = true
  ; sell_levels_persistence = true
  }
;;

let words_per_call ~warmup ~iters f =
  for _ = 1 to warmup do
    f ()
  done;
  let before = Gc.minor_words () in
  for _ = 1 to iters do
    f ()
  done;
  let after = Gc.minor_words () in
  (after -. before) /. float iters
;;

(* One scenario: [n_levels] persisted levels and [n_sells] feed sells. When the two differ
   the scenario is not aligned, so a rebuild/adoption may occur; the [aligned] rows below
   keep them equal to stay in the steady no-change regime. *)
let profile ~n_levels ~n_sells ~remaintain =
  let symbol = Printf.sprintf "PROF_%d_%d_%b/USD" n_levels n_sells remaintain in
  let state = JL.get_strategy_state symbol in
  state.persisted_sell_levels <- List.init n_levels (fun i -> 100.0 -. float i, 1.0);
  Sell_orders.clear state.open_sell_orders;
  let orders =
    List.init n_sells (fun i ->
      Printf.sprintf "s%d" i, 100.0 -. float i, 1.0, "sell", Some 1)
    @ [ "b1", 90.0, 1.0, "buy", Some 1 ]
  in
  let iter_orders f = List.iter (fun (id, p, q, s, u) -> f id p q s u) orders in
  let ecfg =
    let base = JL.get_exchange_config "alpaca" in
    { base with remaintain_expired_sells = remaintain }
  in
  let asset = alpaca_asset symbol in
  let call () =
    ignore
      (JL.sync_open_orders
         ~state
         ~now:1000.0
         ~asset
         ~bid_price:100.0
         ~lot_qty:1.0
         ~iter_open_orders:iter_orders
         ~get_open_orders_generation:(fun () -> -1)
         ~ecfg)
  in
  words_per_call ~warmup:300 ~iters:3000 call
;;

let () =
  Printf.printf
    "sync_open_orders allocation profile (minor words/call; 1 word = 8 bytes)\n\n";
  Printf.printf "%-22s %13s %13s %8s\n" "scenario" "remaintain=F" "remaintain=T" "delta";
  List.iter
    (fun n ->
      let f = profile ~n_levels:n ~n_sells:n ~remaintain:false in
      let t = profile ~n_levels:n ~n_sells:n ~remaintain:true in
      Printf.printf
        "%-22s %13.1f %13.1f %8.1f\n"
        (Printf.sprintf "levels=%d sells=%d" n n)
        f
        t
        (t -. f))
    [ 4; 8; 16; 32; 64 ];
  Printf.printf "\nfeed-scaling (levels=0, remaintain=F)\n";
  List.iter
    (fun m ->
      let w = profile ~n_levels:0 ~n_sells:m ~remaintain:false in
      Printf.printf "%-22s %13.1f\n" (Printf.sprintf "levels=0 sells=%d" m) w)
    [ 4; 8; 16; 32; 64 ]
;;
