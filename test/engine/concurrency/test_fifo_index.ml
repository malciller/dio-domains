(* Coverage for the bounded FIFO index used by the per-venue order-id maps.
   - capacity is finite from the first insert, and lock_cap only tunes it
   - an id removed out of band and then re-inserted is NOT evicted early (the duplicate
     queue-entry bug this type exists to fix)
   - trim_queue drops superseded records and preserves eviction order *)

(* [Fifo_index] enforces a [min_cap] floor of 32, so every cap here is at least that. *)
let cap_min = Concurrency.Fifo_index.min_cap

let test_bounded_from_first_insert () =
  (* The cap must exist before any tuning call. A feed whose startup snapshot never
     completes used to sit at max_int forever. *)
  let t = Concurrency.Fifo_index.create ~label:"test" ~cap:cap_min () in
  Alcotest.(check int) "initial cap" cap_min (Concurrency.Fifo_index.cap t);
  for i = 1 to 1000 do
    Concurrency.Fifo_index.set t ~key:(string_of_int i) ~value:i
  done;
  Alcotest.(check int) "never exceeds cap" cap_min (Concurrency.Fifo_index.length t);
  (* Oldest-first: the survivors are the most recent ids. *)
  Alcotest.(check (option int))
    "newest kept"
    (Some 1000)
    (Concurrency.Fifo_index.find_opt t "1000");
  Alcotest.(check (option int))
    "oldest evicted"
    None
    (Concurrency.Fifo_index.find_opt t "1")
;;

let test_cap_floor () =
  let t = Concurrency.Fifo_index.create ~label:"test" ~cap:8 () in
  Alcotest.(check int) "tiny cap clamped up" cap_min (Concurrency.Fifo_index.cap t);
  Concurrency.Fifo_index.lock_cap t ~observed:1 ~floor:1;
  Alcotest.(check int) "tiny floor clamped up" cap_min (Concurrency.Fifo_index.cap t)
;;

let test_lock_cap_tunes_and_evicts () =
  let t = Concurrency.Fifo_index.create ~label:"test" ~cap:65_536 () in
  for i = 1 to 100 do
    Concurrency.Fifo_index.set t ~key:(string_of_int i) ~value:i
  done;
  Alcotest.(check int) "pre-lock size" 100 (Concurrency.Fifo_index.length t);
  Concurrency.Fifo_index.lock_cap t ~observed:100 ~floor:32;
  Alcotest.(check int) "cap = observed*1.5+1" 151 (Concurrency.Fifo_index.cap t);
  Alcotest.(check int)
    "no eviction when growing cap"
    100
    (Concurrency.Fifo_index.length t);
  (* Tightening below current size evicts immediately rather than waiting for an insert. *)
  Concurrency.Fifo_index.lock_cap t ~observed:10 ~floor:32;
  Alcotest.(check int) "cap floored" 32 (Concurrency.Fifo_index.cap t);
  Alcotest.(check int) "evicted down to cap" 32 (Concurrency.Fifo_index.length t);
  Alcotest.(check (option int))
    "oldest gone after tighten"
    None
    (Concurrency.Fifo_index.find_opt t "1");
  Alcotest.(check (option int))
    "newest kept after tighten"
    (Some 100)
    (Concurrency.Fifo_index.find_opt t "100");
  (* Observed volume above the hard ceiling is clamped, not honoured. *)
  Concurrency.Fifo_index.lock_cap t ~observed:10_000_000 ~floor:32;
  Alcotest.(check int)
    "clamped to default_cap"
    Concurrency.Fifo_index.default_cap
    (Concurrency.Fifo_index.cap t)
;;

(* The regression this module was written for.

   Old behaviour: `remove` deleted the map row but left the id in the eviction queue.
   Re-inserting then pushed a SECOND copy, because membership was inferred from the map.
   Popping that duplicate later deleted the live re-inserted row, evicting an order that
   was still open. *)
let test_readmit_is_not_evicted_early () =
  let t = Concurrency.Fifo_index.create ~label:"test" ~cap:cap_min () in
  (* Fill to one below the cap. *)
  for i = 0 to cap_min - 2 do
    Concurrency.Fifo_index.set t ~key:(string_of_int i) ~value:i
  done;
  Alcotest.(check int) "filled" (cap_min - 1) (Concurrency.Fifo_index.length t);
  (* Terminal event retires id 0 out of band. *)
  Concurrency.Fifo_index.remove t "0";
  Alcotest.(check (option int)) "0 gone" None (Concurrency.Fifo_index.find_opt t "0");
  (* Same order id comes back. It must be admitted as new work at the BACK. *)
  Concurrency.Fifo_index.set t ~key:"0" ~value:1000;
  Alcotest.(check (option int))
    "0 re-admitted"
    (Some 1000)
    (Concurrency.Fifo_index.find_opt t "0");
  (* Two more inserts push past the cap so eviction runs more than once. *)
  Concurrency.Fifo_index.set t ~key:"n1" ~value:2001;
  Concurrency.Fifo_index.set t ~key:"n2" ~value:2002;
  Alcotest.(check int) "back at cap" cap_min (Concurrency.Fifo_index.length t);
  Alcotest.(check (option int))
    "re-admitted id survives eviction"
    (Some 1000)
    (Concurrency.Fifo_index.find_opt t "0");
  Alcotest.(check (option int))
    "oldest retired id evicted instead"
    None
    (Concurrency.Fifo_index.find_opt t "1");
  Alcotest.(check int)
    "the stale queue entry was skipped, not applied"
    1
    (Concurrency.Fifo_index.stats t).stale_skips
;;

let test_trim_queue () =
  let t = Concurrency.Fifo_index.create ~label:"test" ~cap:65_536 () in
  for i = 1 to 10 do
    Concurrency.Fifo_index.set t ~key:(string_of_int i) ~value:i
  done;
  (* Retire three ids out of band; their queue records linger. *)
  List.iter (Concurrency.Fifo_index.remove t) [ "2"; "5"; "9" ];
  let before = Concurrency.Fifo_index.stats t in
  Alcotest.(check int) "queue holds superseded records" 10 before.pending_queue;
  Alcotest.(check int) "map already shrank" 7 before.entries;
  Alcotest.(check int) "admissions dropped too" 7 before.queued;
  let dropped = Concurrency.Fifo_index.trim_queue t in
  Alcotest.(check int) "dropped 3" 3 dropped;
  let after = Concurrency.Fifo_index.stats t in
  Alcotest.(check int) "queue now matches admissions" 7 after.pending_queue;
  Alcotest.(check int) "map untouched" 7 after.entries;
  Alcotest.(check int) "trim is idempotent" 0 (Concurrency.Fifo_index.trim_queue t);
  (* Order preserved: overflow evicts the oldest SURVIVORS (1 then 3 then 4 ...), never
     the retired ids that used to sit in the queue. *)
  Concurrency.Fifo_index.lock_cap t ~observed:7 ~floor:cap_min;
  Alcotest.(check int) "at cap" cap_min (Concurrency.Fifo_index.cap t);
  for i = 11 to cap_min + 5 do
    Concurrency.Fifo_index.set t ~key:(string_of_int i) ~value:i
  done;
  Alcotest.(check int) "still at cap" cap_min (Concurrency.Fifo_index.length t);
  Alcotest.(check (option int))
    "oldest survivor evicted first"
    None
    (Concurrency.Fifo_index.find_opt t "1");
  Alcotest.(check (option int))
    "next survivor also evicted"
    None
    (Concurrency.Fifo_index.find_opt t "3");
  Alcotest.(check (option int))
    "10 survived"
    (Some 10)
    (Concurrency.Fifo_index.find_opt t "10")
;;

let test_clear () =
  let t = Concurrency.Fifo_index.create ~label:"test" ~cap:65_536 () in
  for i = 1 to 20 do
    Concurrency.Fifo_index.set t ~key:(string_of_int i) ~value:i
  done;
  Concurrency.Fifo_index.clear t;
  Alcotest.(check int) "cleared" 0 (Concurrency.Fifo_index.length t);
  Alcotest.(check int) "queue cleared" 0 (Concurrency.Fifo_index.stats t).pending_queue;
  Alcotest.(check int) "admissions cleared" 0 (Concurrency.Fifo_index.stats t).queued;
  (* Usable again after a reconnect. *)
  Concurrency.Fifo_index.set t ~key:"z" ~value:26;
  Alcotest.(check (option int))
    "reusable"
    (Some 26)
    (Concurrency.Fifo_index.find_opt t "z")
;;

let () =
  Alcotest.run
    "fifo_index"
    [ ( "capacity"
      , [ Alcotest.test_case
            "bounded from first insert"
            `Quick
            test_bounded_from_first_insert
        ; Alcotest.test_case "cap floor" `Quick test_cap_floor
        ; Alcotest.test_case
            "lock_cap tunes and evicts"
            `Quick
            test_lock_cap_tunes_and_evicts
        ] )
    ; ( "correctness"
      , [ Alcotest.test_case
            "re-admitted id not evicted early"
            `Quick
            test_readmit_is_not_evicted_early
        ; Alcotest.test_case "trim_queue" `Quick test_trim_queue
        ; Alcotest.test_case "clear" `Quick test_clear
        ] )
    ]
;;
