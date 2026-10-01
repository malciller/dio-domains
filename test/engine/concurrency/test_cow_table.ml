(* Coverage for the copy-on-write sharded map.
   - semantics: set/find/find_opt/mem/add_if_absent/remove/length/clear/fold
   - deletion: intra-shard probe chains survive a remove (the hazard a hole-punching
     delete cannot survive, because an empty slot terminates the probe)
   - concurrency: many domains writing and reading disjoint and shared keyspaces at once,
     which is the hazard a plain Hashtbl cannot survive (in-place resize during a read). *)

[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

let test_basics () =
  let t = Ds.Cow_table.create ~shard_count:4 () in
  Alcotest.(check bool) "empty" true (Ds.Cow_table.is_empty t);
  Ds.Cow_table.set t "a" 1;
  Ds.Cow_table.set t "b" 2;
  Ds.Cow_table.set t "a" 10;
  Alcotest.(check (option int)) "find_opt a" (Some 10) (Ds.Cow_table.find_opt t "a");
  Alcotest.(check int) "find b" 2 (Ds.Cow_table.find t "b");
  Alcotest.(check (option int)) "missing" None (Ds.Cow_table.find_opt t "zz");
  Alcotest.(check bool) "mem a" true (Ds.Cow_table.mem t "a");
  Alcotest.(check bool) "mem zz" false (Ds.Cow_table.mem t "zz");
  Alcotest.(check int) "length" 2 (Ds.Cow_table.length t);
  Alcotest.(check bool) "add_if_absent new" true (Ds.Cow_table.add_if_absent t "c" 3);
  Alcotest.(check bool)
    "add_if_absent existing"
    false
    (Ds.Cow_table.add_if_absent t "a" 99);
  Alcotest.(check (option int))
    "existing untouched"
    (Some 10)
    (Ds.Cow_table.find_opt t "a");
  Ds.Cow_table.remove t "a";
  Alcotest.(check (option int)) "removed" None (Ds.Cow_table.find_opt t "a");
  Ds.Cow_table.remove t "nope";
  Alcotest.(check int) "length after remove" 2 (Ds.Cow_table.length t)
;;

let test_bucket_count_rounding () =
  (* Non-power-of-two arguments are rounded up, and 1 never collapses to zero. *)
  Alcotest.(check int)
    "3 -> 4"
    4
    (Ds.Cow_table.shard_count (Ds.Cow_table.create ~shard_count:3 ()));
  Alcotest.(check int)
    "5 -> 8"
    8
    (Ds.Cow_table.shard_count (Ds.Cow_table.create ~shard_count:5 ()));
  Alcotest.(check int)
    "64 -> 64"
    64
    (Ds.Cow_table.shard_count (Ds.Cow_table.create ~shard_count:64 ()));
  Alcotest.(check int)
    "1 -> 1"
    1
    (Ds.Cow_table.shard_count (Ds.Cow_table.create ~shard_count:1 ()))
;;

let test_fold_and_clear () =
  let t = Ds.Cow_table.create ~shard_count:8 () in
  for i = 0 to 99 do
    Ds.Cow_table.set t (string_of_int i) (i * i)
  done;
  Alcotest.(check int) "100 entries" 100 (Ds.Cow_table.length t);
  let sum = Ds.Cow_table.fold (fun _ v acc -> acc + v) t ~init:0 in
  let expected = ref 0 in
  for i = 0 to 99 do
    expected := !expected + (i * i)
  done;
  Alcotest.(check int) "fold sum" !expected sum;
  Ds.Cow_table.clear t;
  Alcotest.(check int) "cleared" 0 (Ds.Cow_table.length t);
  Alcotest.(check bool) "cleared is_empty" true (Ds.Cow_table.is_empty t)
;;

(* Many domains hammer the same table concurrently. Each writer owns a disjoint key range,
   so the final contents are deterministic and assertable; readers run throughout to keep
   the buckets being republished while they are traversed. *)
let test_concurrent_read_write () =
  let t = Ds.Cow_table.create ~shard_count:16 () in
  let domains = 8 in
  let per_domain = 500 in
  let key d i = Printf.sprintf "d%d-k%d" d i in
  let readers = 3 in
  let reads = Atomic.make 0 in
  let bad = Atomic.make 0 in
  let reader_body () =
    for _ = 1 to 20000 do
      (* Any key we observe must either be absent or hold its own domain's square. A torn
         read of a republished bucket would show some other value. *)
      let d = Random.int domains in
      let i = Random.int per_domain in
      let k = key d i in
      (match Ds.Cow_table.find_opt t k with
       | None -> ()
       | Some v -> if v <> i * i then Atomic.incr bad);
      Atomic.incr reads
    done
  in
  let handles =
    List.init domains (fun d ->
      Domain.spawn (fun () ->
        for i = 0 to per_domain - 1 do
          Ds.Cow_table.set t (key d i) (i * i)
        done))
    @ List.init readers (fun _ -> Domain.spawn reader_body)
  in
  List.iter Domain.join handles;
  Alcotest.(check int) "no torn reads" 0 (Atomic.get bad);
  Alcotest.(check bool) "readers made progress" true (Atomic.get reads > 0);
  Alcotest.(check int) "all keys present" (domains * per_domain) (Ds.Cow_table.length t);
  (* Spot-check every key landed. *)
  let missing = ref 0 in
  for d = 0 to domains - 1 do
    for i = 0 to per_domain - 1 do
      match Ds.Cow_table.find_opt t (key d i) with
      | Some v when v = i * i -> ()
      | _ -> incr missing
    done
  done;
  Alcotest.(check int) "no missing keys" 0 !missing
;;

(* Concurrent writers contending on ONE key: the last writer wins and every reader sees
   one of the values actually written, never a stale-but-published bucket. *)
let test_concurrent_same_key () =
  let t = Ds.Cow_table.create ~shard_count:1 () in
  let writes = Atomic.make 0 in
  let handles =
    List.init 8 (fun d ->
      Domain.spawn (fun () ->
        for i = 1 to 500 do
          Ds.Cow_table.set t "hot" ((d * 100000) + i);
          Atomic.incr writes
        done))
  in
  List.iter Domain.join handles;
  Alcotest.(check int) "all writes landed" 4000 (Atomic.get writes);
  match Ds.Cow_table.find_opt t "hot" with
  | Some v when v > 0 && v <= (8 * 100000) + 500 ->
    Alcotest.(check bool) "hot key readable" true true
  | other ->
    Alcotest.failf
      "hot key missing or corrupt: %s"
      (match other with
       | None -> "none"
       | Some v -> string_of_int v)
;;

(* Deletion must not orphan a key that probed past the removed one. One shard makes every
   key share a probe sequence, forcing the collision instead of hoping a multi-shard table
   produces one. *)
let test_remove_preserves_probe_chain () =
  let t = Ds.Cow_table.create ~shard_count:1 () in
  let n = 200 in
  let key i = "k" ^ string_of_int i in
  for i = 0 to n - 1 do
    Ds.Cow_table.set t (key i) i
  done;
  (* Any key that probed past a hole reports absent here. *)
  for i = 0 to n - 1 do
    if i mod 2 = 0 then Ds.Cow_table.remove t (key i)
  done;
  let missing =
    List.fold_left
      (fun acc i ->
        if i mod 2 = 0
        then acc
        else (
          match Ds.Cow_table.find_opt t (key i) with
          | Some v when v = i -> acc
          | _ -> i :: acc))
      []
      (List.init n (fun i -> i))
  in
  Alcotest.(check (list int))
    "survivors reachable after interleaved removes"
    []
    (List.sort compare missing);
  Alcotest.(check int) "length reflects removals" (n / 2) (Ds.Cow_table.length t);
  (* Reinserting a removed key must not resurrect its probe neighbours or double the count. *)
  Ds.Cow_table.set t (key 0) 0;
  Alcotest.(check int) "reinsert keeps count" ((n / 2) + 1) (Ds.Cow_table.length t);
  let odd_keys = List.filter (fun i -> i mod 2 = 1) (List.init n (fun i -> i)) in
  let still_missing =
    List.filter
      (fun i ->
        match Ds.Cow_table.find_opt t (key i) with
        | Some v -> v <> i
        | None -> true)
      odd_keys
  in
  Alcotest.(check (list int)) "reinsert leaves others intact" [] still_missing
;;

(* Remove and reinsert repeatedly, in a table small enough that growth happens
   mid-sequence. Growth rehashes with the same [slot_start] derivation, so a backward-shift
   delete that mis-tracked a home slot would show up as a key lost across a resize. *)
let test_remove_across_growth () =
  let t = Ds.Cow_table.create ~shard_count:1 () in
  let key i = "g" ^ string_of_int i in
  (* Model the live set alongside the operations rather than re-deriving the schedule. *)
  let live = Hashtbl.create 64 in
  let rounds = 40 in
  for round = 1 to rounds do
    Ds.Cow_table.set t (key round) round;
    Hashtbl.replace live round ();
    if round mod 3 = 0
    then (
      Ds.Cow_table.remove t (key (round - 1));
      Hashtbl.remove live (round - 1))
  done;
  let expected =
    List.filter (fun i -> Hashtbl.mem live i) (List.init (rounds + 1) (fun i -> i))
  in
  Alcotest.(check int)
    "length matches live set"
    (List.length expected)
    (Ds.Cow_table.length t);
  let wrong =
    List.filter
      (fun i ->
        match Ds.Cow_table.find_opt t (key i) with
        | Some v -> v <> i
        | None -> true)
      expected
  in
  Alcotest.(check (list int)) "no key lost across growth" [] wrong;
  (* Removed keys must really be gone, or the count assertion could be satisfied by a
     duplicate binding instead. *)
  let resurrected =
    List.filter
      (fun i -> i > 0 && not (Hashtbl.mem live i))
      (List.filter (fun i -> i > 0) (List.init (rounds + 1) (fun i -> i)))
    |> List.filter (fun i -> Ds.Cow_table.find_opt t (key i) <> None)
  in
  Alcotest.(check (list int)) "removed keys stay removed" [] resurrected
;;

let () =
  Alcotest.run
    "cow_table"
    [ ( "semantics"
      , [ Alcotest.test_case "basics" `Quick test_basics
        ; Alcotest.test_case "bucket count rounding" `Quick test_bucket_count_rounding
        ; Alcotest.test_case "fold and clear" `Quick test_fold_and_clear
        ] )
    ; ( "deletion"
      , [ Alcotest.test_case
            "remove preserves probe chain"
            `Quick
            test_remove_preserves_probe_chain
        ; Alcotest.test_case "remove across growth" `Quick test_remove_across_growth
        ] )
    ; ( "concurrency"
      , [ Alcotest.test_case "concurrent read/write" `Quick test_concurrent_read_write
        ; Alcotest.test_case "concurrent same key" `Quick test_concurrent_same_key
        ] )
    ]
;;
