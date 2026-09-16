(* Demonstrates the cross-domain stall a mutex-guarded intern table causes on the strategy
   hot path, and that the lock-free snapshot used by [Strategy_expr.intern_key] removes
   it.

   Build/run: dune build test/engine/perf/intern_contention.exe
   ./_build/default/test/engine/perf/intern_contention.exe *)

[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

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
  let worker d () =
    while not (Atomic.get start) do
      Domain.cpu_relax ()
    done;
    let prev = ref (Monotonic_clock.now_ns ()) in
    for i = 1 to iters do
      ignore (intern (List.nth keys (i land 7)));
      let now = Monotonic_clock.now_ns () in
      let gap = now - !prev in
      if gap > max_gap.(d) then max_gap.(d) <- gap;
      prev := now
    done
  in
  let doms = List.init domains (fun d -> Domain.spawn (worker d)) in
  Atomic.set start true;
  List.iter Domain.join doms;
  let worst_gap = Array.fold_left max 0 max_gap in
  Printf.printf
    "%-11s domains=%d iters=%d  worst per-call stall=%.1fus\n%!"
    label
    domains
    iters
    (float worst_gap /. 1000.0)
;;

let () =
  let domains = 8
  and iters = 2_000_000 in
  run ~label:"OLD(mutex)" ~intern:Old.intern ~domains ~iters;
  run ~label:"NEW(atomic)" ~intern:Dio_strategies.Strategy_expr.intern_key ~domains ~iters
;;
