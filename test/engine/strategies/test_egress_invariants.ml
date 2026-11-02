(* Egress-path invariants introduced by the HFT performance work.

   These guard the pieces that are easy to break silently:

   - [LockFreeQueue]'s publish protocol (no torn payload, no lost item, FIFO under
     multi-producer contention, bounded).
   - [InFlightAmendments.is_replacement_target], now an O(1) reverse index instead of a
     64-shard fold. The index must stay consistent with the registry across
     replace/remove/evict, because a false negative makes order management cancel a live
     replacement order. *)

module SC = Dio_strategies.Strategy_common

let test_queue_fifo_bounded () =
  let q = SC.LockFreeQueue.create ~size:4 () in
  Alcotest.(check bool) "write 1" true (SC.LockFreeQueue.write q 1 <> None);
  Alcotest.(check bool) "write 2" true (SC.LockFreeQueue.write q 2 <> None);
  Alcotest.(check int) "size 2" 2 (SC.LockFreeQueue.size q);
  Alcotest.(check (option int)) "read 1" (Some 1) (SC.LockFreeQueue.read q);
  Alcotest.(check (option int)) "read 2" (Some 2) (SC.LockFreeQueue.read q);
  Alcotest.(check (option int)) "empty" None (SC.LockFreeQueue.read q)
;;

let test_queue_full_is_reported () =
  let q = SC.LockFreeQueue.create ~size:2 () in
  ignore (SC.LockFreeQueue.write q 'a');
  ignore (SC.LockFreeQueue.write q 'b');
  Alcotest.(check bool) "third write refused" true (SC.LockFreeQueue.write q 'c' = None)
;;

let test_queue_multi_producer () =
  let q = SC.LockFreeQueue.create ~size:1024 () in
  let producers = 4 in
  let per = 5_000 in
  let total = producers * per in
  let seen = Array.make total 0 in
  let received = ref 0 in
  let consume () =
    let rec drain () =
      match SC.LockFreeQueue.read q with
      | None -> ()
      | Some v ->
        seen.(v) <- seen.(v) + 1;
        incr received;
        drain ()
    in
    drain ()
  in
  let threads =
    List.init producers (fun p ->
      Thread.create
        (fun () ->
          for i = 0 to per - 1 do
            let v = (p * per) + i in
            let rec push tries =
              match SC.LockFreeQueue.write q v with
              | Some () -> ()
              | None ->
                (* Bounded: spinning forever would deadlock the join below on a full
                   queue, turning a real regression into a hung suite. *)
                if tries > 1_000_000
                then failwith "queue stalled"
                else (
                  Thread.yield ();
                  push (tries + 1))
            in
            push 0
          done)
        ())
  in
  let deadline = Unix.gettimeofday () +. 20.0 in
  while !received < total && Unix.gettimeofday () < deadline do
    consume ();
    Thread.yield ()
  done;
  List.iter Thread.join threads;
  consume ();
  Alcotest.(check int) "all delivered" total !received;
  Alcotest.(check int)
    "each exactly once"
    0
    (Array.fold_left (fun acc n -> acc + abs (n - 1)) 0 seen)
;;

let test_replacement_index () =
  let old_id = "old-1" in
  let new_id = "new-1" in
  Alcotest.(check bool)
    "start untracked"
    false
    (SC.InFlightAmendments.is_replacement_target new_id);
  Alcotest.(check bool)
    "add old"
    true
    (SC.InFlightAmendments.add_in_flight_amendment old_id);
  Alcotest.(check bool)
    "replacement not visible before ack"
    false
    (SC.InFlightAmendments.is_replacement_target new_id);
  SC.InFlightAmendments.note_amendment_succeeded ~old_id ~new_id;
  Alcotest.(check bool)
    "replacement visible after ack"
    true
    (SC.InFlightAmendments.is_replacement_target new_id);
  Alcotest.(check bool)
    "replacement target with same-id amend"
    true
    (SC.InFlightAmendments.is_replacement_target new_id);
  Alcotest.(check bool)
    "retire old"
    true
    (SC.InFlightAmendments.remove_in_flight_amendment old_id);
  Alcotest.(check bool)
    "index cleared with the entry"
    false
    (SC.InFlightAmendments.is_replacement_target new_id)
;;

let test_same_id_amend_drops () =
  let id = "kraken-1" in
  ignore (SC.InFlightAmendments.add_in_flight_amendment id);
  SC.InFlightAmendments.note_amendment_succeeded ~old_id:id ~new_id:id;
  Alcotest.(check bool)
    "same-id amend leaves no replacement target"
    false
    (SC.InFlightAmendments.is_replacement_target id)
;;

let test_replacement_replaced () =
  (* A second replacement of the same old id must retire the previous new id from the
     index, or the stale id stays "recognized" forever. *)
  let old_id = "old-2" in
  ignore (SC.InFlightAmendments.add_in_flight_amendment old_id);
  SC.InFlightAmendments.note_amendment_succeeded ~old_id ~new_id:"new-2a";
  SC.InFlightAmendments.note_amendment_succeeded ~old_id ~new_id:"new-2b";
  Alcotest.(check bool)
    "latest replacement tracked"
    true
    (SC.InFlightAmendments.is_replacement_target "new-2b");
  Alcotest.(check bool)
    "previous replacement retired"
    false
    (SC.InFlightAmendments.is_replacement_target "new-2a");
  ignore (SC.InFlightAmendments.remove_in_flight_amendment old_id)
;;

let () =
  Alcotest.run
    "egress_invariants"
    [ ( "lock_free_queue"
      , [ Alcotest.test_case "fifo and bound" `Quick test_queue_fifo_bounded
        ; Alcotest.test_case "full is reported" `Quick test_queue_full_is_reported
        ; Alcotest.test_case "multi-producer delivery" `Slow test_queue_multi_producer
        ] )
    ; ( "replacement index"
      , [ Alcotest.test_case "register and clear" `Quick test_replacement_index
        ; Alcotest.test_case "same-id amend" `Quick test_same_id_amend_drops
        ; Alcotest.test_case "superseded replacement" `Quick test_replacement_replaced
        ] )
    ]
;;
