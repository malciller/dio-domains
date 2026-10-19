let test_doorbell_rings_once_per_notify () =
  let wakes = ref 0 in
  Concurrency.Oracle_wakeup.set_wake (fun () -> incr wakes);
  Concurrency.Oracle_wakeup.notify_exec ();
  Concurrency.Oracle_wakeup.notify_balance ();
  Concurrency.Oracle_wakeup.notify_orders ();
  Alcotest.(check int) "one ring per notify" 3 !wakes;
  (* Clearing the bridge must make producers no-ops, not crash. *)
  Concurrency.Oracle_wakeup.clear_wake ();
  Concurrency.Oracle_wakeup.notify_exec ();
  Alcotest.(check int) "no ring after clear" 3 !wakes
;;

let () =
  Alcotest.run
    "oracle_wakeup"
    [ ( "doorbell"
      , [ Alcotest.test_case
            "rings once per notify"
            `Quick
            test_doorbell_rings_once_per_notify
        ] )
    ]
;;
