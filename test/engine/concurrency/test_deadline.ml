(* A parked domain has no other wake, so an armed deadline must arrive as an ordinary
   symbol signal. *)

let test_deadline_wakes_a_parked_waiter () =
  Concurrency.Exchange_wakeup.start_deadline_timer ();
  let symbol = "DEADLINE/TEST" in
  let sync = Concurrency.Exchange_wakeup.get_sync_handle symbol in
  let since = Concurrency.Exchange_wakeup.get_generation_fast sync in
  Concurrency.Exchange_wakeup.schedule ~symbol ~at:(Unix.gettimeofday () +. 0.05);
  Concurrency.Exchange_wakeup.wait_since_fast sync ~since;
  Alcotest.(check bool)
    "woken by the timer"
    true
    (Concurrency.Exchange_wakeup.get_generation_fast sync > since)
;;

let () =
  Alcotest.run
    "deadline"
    [ ( "timer"
      , [ Alcotest.test_case
            "wakes a parked waiter"
            `Quick
            test_deadline_wakes_a_parked_waiter
        ] )
    ]
;;
