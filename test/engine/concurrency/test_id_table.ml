(* Coverage for the paged int-keyed table.
   - semantics: set/find/find_opt/find_default/mem/add_if_absent/remove/length/clear/fold
   - the property that motivates the type: removal is a single store, so it cannot orphan
     a neighbour the way a linear-probe delete does
   - paged addressing: sparse and large ids, growth of the page directory
   - concurrency: many domains writing and removing disjoint id ranges at once, which is
     the hazard a plain array or Hashtbl cannot survive. *)

[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

let test_basics () =
  let t = Ds.Id_table.create () in
  Alcotest.(check bool) "empty" true (Ds.Id_table.is_empty t);
  Alcotest.(check (option int)) "missing" None (Ds.Id_table.find_opt t 7);
  Ds.Id_table.set t 7 70;
  Alcotest.(check (option int)) "find_opt" (Some 70) (Ds.Id_table.find_opt t 7);
  Alcotest.(check int) "find" 70 (Ds.Id_table.find t 7);
  Alcotest.(check int) "find_default hit" 70 (Ds.Id_table.find_default t 7 ~default:0);
  Alcotest.(check int) "find_default miss" 0 (Ds.Id_table.find_default t 8 ~default:0);
  Alcotest.(check bool) "mem" true (Ds.Id_table.mem t 7);
  Alcotest.(check int) "length" 1 (Ds.Id_table.length t);
  Ds.Id_table.set t 7 71;
  Alcotest.(check int) "replace" 71 (Ds.Id_table.find t 7);
  Alcotest.(check int) "replace keeps length" 1 (Ds.Id_table.length t);
  Ds.Id_table.remove t 7;
  Alcotest.(check (option int)) "removed" None (Ds.Id_table.find_opt t 7);
  Alcotest.(check int) "length after remove" 0 (Ds.Id_table.length t);
  Ds.Id_table.remove t 7;
  Alcotest.(check int) "double remove safe" 0 (Ds.Id_table.length t)
;;

let test_add_if_absent () =
  let t = Ds.Id_table.create () in
  Alcotest.(check bool) "installs" true (Ds.Id_table.add_if_absent t 3 "a");
  Alcotest.(check bool) "refuses existing" false (Ds.Id_table.add_if_absent t 3 "b");
  Alcotest.(check (option string)) "first wins" (Some "a") (Ds.Id_table.find_opt t 3);
  Alcotest.(check int) "one entry" 1 (Ds.Id_table.length t)
;;

(* The whole reason this type exists. In a linear-probe table, removing an id can leave a
   hole that hides every id which probed past it, so a test that removes ids in a
   different order from their insertion would lose bindings. Here removal is a single
   store into a slot addressed directly by the id, so the survivors must all survive
   regardless. *)
let test_remove_cannot_orphan_neighbours () =
  let t = Ds.Id_table.create () in
  let n = 500 in
  for i = 0 to n - 1 do
    Ds.Id_table.set t i (i * 3)
  done;
  for i = 0 to n - 1 do
    if i mod 3 = 0 then Ds.Id_table.remove t i
  done;
  let lost =
    List.filter
      (fun i ->
        if i mod 3 = 0
        then Ds.Id_table.mem t i
        else Ds.Id_table.find_default t i ~default:(-1) <> i * 3)
      (List.init n (fun i -> i))
  in
  Alcotest.(check (list int)) "no id lost or resurrected" [] lost;
  let expected_live =
    List.length (List.filter (fun i -> i mod 3 <> 0) (List.init n (fun i -> i)))
  in
  Alcotest.(check int) "length exact" expected_live (Ds.Id_table.length t)
;;

(* Interleaved insert/remove across the 256-slot page boundary, where a delete that
   mishandled its offset would corrupt the neighbouring page. *)
let test_remove_across_page_boundary () =
  let t = Ds.Id_table.create () in
  let ids = List.init 1024 (fun i -> i) in
  List.iter (fun i -> Ds.Id_table.set t i i) ids;
  List.iter (fun i -> if i mod 2 = 0 then Ds.Id_table.remove t i) ids;
  let wrong =
    List.filter
      (fun i ->
        if i mod 2 = 0
        then Ds.Id_table.mem t i
        else Ds.Id_table.find_default t i ~default:(-1) <> i)
      ids
  in
  Alcotest.(check (list int)) "ids survive page neighbours" [] wrong
;;

(* Venue order ids are large and sparse, so the directory must grow by page rather than
   allocate max_id entries. *)
let test_sparse_large_ids () =
  let t = Ds.Id_table.create () in
  let ids = [ 0; 1; 255; 256; 257; 65_536; 1_000_000; 1_073_741_824; 2_000_000_000 ] in
  List.iter (fun i -> Ds.Id_table.set t i (i * 2)) ids;
  let wrong =
    List.filter (fun i -> Ds.Id_table.find_default t i ~default:(-1) <> i * 2) ids
  in
  Alcotest.(check (list int)) "large sparse ids addressable" [] wrong;
  Ds.Id_table.remove t 1_000_000;
  Alcotest.(check (option int)) "large id removed" None (Ds.Id_table.find_opt t 1_000_000);
  Alcotest.(check int)
    "neighbours of a large id intact"
    (2_000_000_000 * 2)
    (Ds.Id_table.find_default t 2_000_000_000 ~default:0);
  Alcotest.(check int) "length" (List.length ids - 1) (Ds.Id_table.length t)
;;

(* Ids at or below zero are not representable; they must be ignored rather than index
   backwards into another page. *)
let test_negative_ids_rejected () =
  let t = Ds.Id_table.create () in
  Ds.Id_table.set t 5 "ok";
  Ds.Id_table.set t (-1) "bad";
  Alcotest.(check bool)
    "negative add_if_absent refused"
    false
    (Ds.Id_table.add_if_absent t (-7) "bad");
  Alcotest.(check (option string))
    "id 5 unaffected"
    (Some "ok")
    (Ds.Id_table.find_opt t 5);
  Alcotest.(check (option string))
    "negative reads absent"
    None
    (Ds.Id_table.find_opt t (-1));
  Alcotest.(check int) "negative add_if_absent refused" 0 (Ds.Id_table.length t - 1);
  Alcotest.(check int) "dropped negative counted" 1 (Ds.Id_table.dropped_negative t);
  Ds.Id_table.remove t (-1);
  Alcotest.(check int) "negative remove is a no-op" 1 (Ds.Id_table.length t)
;;

let test_fold_and_clear () =
  let t = Ds.Id_table.create () in
  let n = 300 in
  for i = 0 to n - 1 do
    Ds.Id_table.set t i (i * i)
  done;
  Alcotest.(check int) "300 entries" n (Ds.Id_table.length t);
  let sum = Ds.Id_table.fold (fun _ v acc -> acc + v) t ~init:0 in
  let expected = ref 0 in
  for i = 0 to n - 1 do
    expected := !expected + (i * i)
  done;
  Alcotest.(check int) "fold sum" !expected sum;
  (* fold must report the id alongside the value, so a caller can rebuild its own index. *)
  let round_trip =
    Ds.Id_table.fold (fun id v acc -> (id, v) :: acc) t ~init:[] |> List.sort compare
  in
  Alcotest.(check int) "fold recovers every id" n (List.length round_trip);
  Alcotest.(check (list (pair int int)))
    "fold id/value pairs"
    (List.init n (fun i -> i, i * i))
    round_trip;
  Ds.Id_table.clear t;
  Alcotest.(check int) "cleared" 0 (Ds.Id_table.length t);
  Alcotest.(check bool) "cleared is_empty" true (Ds.Id_table.is_empty t)
;;

(* Many domains hammer the table concurrently. Each writer owns a disjoint id range so the
   final state is deterministic; readers run throughout to keep pages being republished
   under them. A published page is never mutated, so a reader sees whole values only. *)
let test_concurrent_read_write () =
  let t = Ds.Id_table.create () in
  let domains = 8 in
  let per_domain = 400 in
  let id d i = (d * per_domain) + i in
  let readers = 3 in
  let reads = Atomic.make 0 in
  let bad = Atomic.make 0 in
  let reader_body () =
    for _ = 1 to 20000 do
      let d = Random.int domains in
      let i = Random.int per_domain in
      (* Any value we observe must be some domain's own square, never a torn mixture. *)
      (match Ds.Id_table.find_opt t (id d i) with
       | None -> ()
       | Some v -> if v <> i * i then Atomic.incr bad);
      Atomic.incr reads
    done
  in
  let handles =
    List.init domains (fun d ->
      Domain.spawn (fun () ->
        for i = 0 to per_domain - 1 do
          Ds.Id_table.set t (id d i) (i * i)
        done))
    @ List.init readers (fun _ -> Domain.spawn reader_body)
  in
  List.iter Domain.join handles;
  Alcotest.(check int) "no torn reads" 0 (Atomic.get bad);
  Alcotest.(check bool) "readers made progress" true (Atomic.get reads > 0);
  Alcotest.(check int) "all ids present" (domains * per_domain) (Ds.Id_table.length t);
  let missing =
    List.length
      (List.filter
         (fun d ->
           List.exists
             (fun i -> Ds.Id_table.find_default t (id d i) ~default:(-1) <> i * i)
             (List.init per_domain (fun i -> i)))
         (List.init domains (fun d -> d)))
  in
  Alcotest.(check int) "no missing ids" 0 missing
;;

(* Domains interleaving set and remove on the *same* ids. The final state is
   nondeterministic — they interleave per operation, not per round — so there is no "last
   round" to compare against. What must hold is that an id is either absent or holds its
   own value, never another id's, and the count matches what is present. A lost update or a
   page published from a stale snapshot breaks both. *)
let test_concurrent_set_remove_same_ids () =
  let t = Ds.Id_table.create () in
  let domains = 6 in
  let n = 300 in
  let corrupt = Atomic.make 0 in
  let handles =
    List.init domains (fun d ->
      Domain.spawn (fun () ->
        for round = 0 to 60 do
          for i = 0 to n - 1 do
            if (i + round + d) mod 2 = 0
            then Ds.Id_table.set t i i
            else Ds.Id_table.remove t i
          done
        done;
        (* Reads concurrent with the other domains' writes must still see whole values. *)
        for _ = 1 to 2000 do
          let i = Random.int n in
          match Ds.Id_table.find_opt t i with
          | Some v when v <> i -> Atomic.incr corrupt
          | Some _ | None -> ()
        done))
  in
  List.iter Domain.join handles;
  Alcotest.(check int) "no id ever held a foreign value" 0 (Atomic.get corrupt);
  let ids = List.init n (fun i -> i) in
  let present = List.filter (fun i -> Ds.Id_table.mem t i) ids in
  Alcotest.(check int)
    "every present id holds its own value"
    0
    (List.length
       (List.filter (fun i -> Ds.Id_table.find_default t i ~default:(-1) <> i) present));
  (* Quiesced, the count must be exact: the increment follows the CAS that published the
     transition, so no update can be dropped or double-counted. *)
  Alcotest.(check int) "count exact once quiesced" (List.length present) (Ds.Id_table.length t)
;;

let () =
  Alcotest.run
    "id_table"
    [ ( "semantics"
      , [ Alcotest.test_case "basics" `Quick test_basics
        ; Alcotest.test_case "add_if_absent" `Quick test_add_if_absent
        ; Alcotest.test_case "fold and clear" `Quick test_fold_and_clear
        ; Alcotest.test_case "negative ids rejected" `Quick test_negative_ids_rejected
        ] )
    ; ( "deletion"
      , [ Alcotest.test_case
            "remove cannot orphan neighbours"
            `Quick
            test_remove_cannot_orphan_neighbours
        ; Alcotest.test_case
            "remove across page boundary"
            `Quick
            test_remove_across_page_boundary
        ] )
    ; "paging", [ Alcotest.test_case "sparse large ids" `Quick test_sparse_large_ids ]
    ; ( "concurrency"
      , [ Alcotest.test_case "concurrent read/write" `Quick test_concurrent_read_write
        ; Alcotest.test_case
            "concurrent set/remove same ids"
            `Quick
            test_concurrent_set_remove_same_ids
        ] )
    ]
;;
