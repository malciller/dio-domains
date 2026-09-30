(* Demonstrates the cross-domain stall a mutex-guarded intern table causes on the strategy
   hot path, and that the lock-free [Ds.Cow_table] used by
   [Strategy_expr.intern_key] removes it.

   "Worst" alone is not a usable signal — over millions of calls a single OS scheduling
   hiccup or minor GC swamps the tail and every variant looks identical. So the run also
   reports how many calls exceeded a latency threshold, which is what actually separates
   contention from noise, plus total elapsed for throughput.

   Build/run: dune build test/engine/perf/intern_contention.exe
   ./_build/default/test/engine/perf/intern_contention.exe *)

[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

(* A call slower than this means the domain was descheduled or blocked on a lock held by
   another domain. Tuned to sit well above the few-hundred-ns cost of the lookup itself
   and well below a scheduler quantum. *)
let slow_call_threshold_ns = 50_000

let keys =
  [ "price_nan"
  ; "oracle_halted"
  ; "buy_active"
  ; "buy_pending"
  ; "sell_place_should"
  ; "open_buy_count"
  ; "fee_refresh_due"
  ; "balance_fresh"
  ]
;;

(* Mirror of the previous implementation: a mutable Hashtbl guarded by one process-wide
   mutex, with the lock taken on every lookup (hit or miss). *)
module Old = struct
  let tbl : (string, int) Hashtbl.t = Hashtbl.create 256
  let mtx = Mutex.create ()
  let next = ref 0

  let intern s =
    Mutex.lock mtx;
    let i =
      match Hashtbl.find tbl s with
      | i -> i
      | exception Not_found ->
        let i = !next in
        incr next;
        Hashtbl.replace tbl s i;
        i
    in
    Mutex.unlock mtx;
    i
  ;;
end

let run ~label ~intern ~domains ~iters =
  List.iter (fun k -> ignore (intern k)) keys;
  let start = Atomic.make false in
  let max_gap = Array.make domains 0 in
  let slow = Array.make domains 0 in
  let elapsed = Array.make domains 0 in
  let worker d () =
    while not (Atomic.get start) do
      Domain.cpu_relax ()
    done;
    let t0 = Monotonic_clock.now_ns () in
    let prev = ref (Monotonic_clock.now_ns ()) in
    for i = 1 to iters do
      ignore (intern (List.nth keys (i land 7)));
      let now = Monotonic_clock.now_ns () in
      let gap = now - !prev in
      if gap > max_gap.(d) then max_gap.(d) <- gap;
      if gap > slow_call_threshold_ns then slow.(d) <- slow.(d) + 1;
      prev := now
    done;
    elapsed.(d) <- Monotonic_clock.now_ns () - t0
  in
  let doms = List.init domains (fun d -> Domain.spawn (worker d)) in
  Atomic.set start true;
  List.iter Domain.join doms;
  let worst_gap = Array.fold_left max 0 max_gap in
  let slow_calls = Array.fold_left ( + ) 0 slow in
  let total_ns = Array.fold_left ( + ) 0 elapsed in
  let total_calls = domains * iters in
  Printf.printf
    "%-11s domains=%d iters=%d  worst=%.1fus  slow=%d (%.4f%%)  ns/call=%.1f\n%!"
    label
    domains
    iters
    (float worst_gap /. 1000.0)
    slow_calls
    (100.0 *. float slow_calls /. float total_calls)
    (float total_ns /. float total_calls)
;;

let () =
  let domains = 8
  and iters = 2_000_000 in
  run ~label:"OLD(mutex)" ~intern:Old.intern ~domains ~iters;
  run ~label:"NEW(cow)" ~intern:Dio_strategies.Strategy_expr.intern_key ~domains ~iters
;;
