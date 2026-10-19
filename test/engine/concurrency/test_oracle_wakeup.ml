let test_doorbell_generations () =
  Concurrency.Oracle_wakeup.clear_wake ();
  let exec0 = Concurrency.Oracle_wakeup.exec_gen () in
  let bal0 = Concurrency.Oracle_wakeup.balance_gen () in
  let ord0 = Concurrency.Oracle_wakeup.orders_gen () in
  Concurrency.Oracle_wakeup.notify_exec ();
  Concurrency.Oracle_wakeup.notify_balance ();
  Concurrency.Oracle_wakeup.notify_orders ();
  Alcotest.(check int)
    "exec generation"
    (exec0 + 1)
    (Concurrency.Oracle_wakeup.exec_gen ());
  Alcotest.(check int)
    "balance generation"
    (bal0 + 1)
    (Concurrency.Oracle_wakeup.balance_gen ());
  Alcotest.(check int)
    "orders generation"
    (ord0 + 1)
    (Concurrency.Oracle_wakeup.orders_gen ())
;;

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
      , [ Alcotest.test_case "generations" `Quick test_doorbell_generations
        ; Alcotest.test_case
            "rings once per notify"
            `Quick
            test_doorbell_rings_once_per_notify
        ] )
    ]
;;
