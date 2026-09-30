(* Compares the read path of the copy-on-write instrument cache against the two things it
   replaced, on the workload that matters: many domains hammering the same symbol-keyed
   table while it is being rewritten.

   - HASHTBL_UNSYNCED: the previous state of affairs for
     kraken_instruments_feed.pair_cache — a plain Hashtbl written under a mutex by the
     warm-up while the hot path read it without one. Correct only by luck, and the reason
     the Cow_table exists. Included to show the read is not the expensive part; the hazard
     is the race, not the cost.
   - HASHTBL_MUTEXED: same table read under the mutex, i.e. what the fix would cost if the
     simple repair (lock the reads) had been taken. This is the number the Cow_table has
     to beat, and it is the one that shows up on every strategy decision cycle.
   - COW_TABLE: what shipped. Lock-free, allocation-free.

   Build/run: dune build test/engine/perf/cow_read.exe &&
   ./_build/default/test/engine/perf/cow_read.exe *)

[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

module Cow = Ds.Cow_table

(* Kraken lists several hundred pairs; use a representative slice. *)
let symbols = Array.init 400 (fun i -> Printf.sprintf "XBT/USD-%d" i)
let key n = symbols.(n mod Array.length symbols)
let iters = 300_000

(* A plain Hashtbl sized so it is already at steady-state capacity, i.e. reads hit the
   fast path and no resize is in flight. That isolates read cost from the race. *)
let plain : (string, int) Hashtbl.t = Hashtbl.create 1024
let cow = Cow.create ~shard_count:64 ()
let mtx = Mutex.create ()

let () =
  for i = 0 to Array.length symbols - 1 do
    let s = symbols.(i) in
    Hashtbl.replace plain s i;
    Cow.set cow s i
  done
;;

let unsynced_read () = ignore (Hashtbl.find_opt plain (key (Random.int 400)))
let cow_read () = ignore (Cow.find_opt cow (key (Random.int 400)))

let mutexed_read () =
  Mutex.lock mtx;
  ignore (Hashtbl.find_opt plain (key (Random.int 400)));
  Mutex.unlock mtx
;;

(* Rewrite the table underneath the readers, the way the instrument warm-up does. *)
let rewriting = Atomic.make true

let run ~label ~read ~domains =
  Random.init 12345;
  let per_domain = iters / domains in
  let elapsed = Array.make domains 0 in
  let writer =
    Domain.spawn (fun () ->
      let n = ref 0 in
      while Atomic.get rewriting do
        let s = key !n in
        Hashtbl.replace plain s !n;
        Cow.set cow s !n;
        incr n
      done)
  in
  let workers =
    List.init domains (fun d ->
      Domain.spawn (fun () ->
        let t0 = Monotonic_clock.now_ns () in
        for _ = 1 to per_domain do
          read ()
        done;
        elapsed.(d) <- Monotonic_clock.now_ns () - t0))
  in
  List.iter Domain.join workers;
  Atomic.set rewriting false;
  Domain.join writer;
  let total = Array.fold_left ( + ) 0 elapsed in
  Printf.printf
    "%-20s domains=%d  ns/read=%.1f  total=%.0fms\n%!"
    label
    domains
    (float total /. float (domains * per_domain))
    (float total /. 1e6)
;;

let () =
  Printf.printf
    "\n%d symbols, %d reads per domain, concurrent rewrite of the table\n\n%!"
    (Array.length symbols)
    (iters / 8);
  List.iter
    (fun domains ->
      run ~label:"HASHTBL_UNSYNCED" ~read:unsynced_read ~domains;
      run ~label:"HASHTBL_MUTEXED" ~read:mutexed_read ~domains;
      run ~label:"COW_TABLE" ~read:cow_read ~domains;
      print_newline ())
    [ 1; 8 ]
;;
