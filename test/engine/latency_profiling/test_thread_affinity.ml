(* Regression for the background allocator's init order: it used to be built at module
   init, before [Logging.load_dotenv], so DIO_BACKGROUND_CPUS from .env was ignored.

   This test mutates the process environment before configuring, so it opts out of the
   OxCaml environment-mutation alert. *)

[@@@alert "-unsafe_multidomain"]

let check_cpu = Alcotest.(check int)

let test_configure_after_env () =
  Unix.putenv "DIO_BACKGROUND_CPUS" "2,3";
  Thread_affinity.configure ();
  check_cpu "first" 2 (Thread_affinity.next_background_cpu ());
  check_cpu "second" 3 (Thread_affinity.next_background_cpu ());
  check_cpu "wraps" 2 (Thread_affinity.next_background_cpu ())
;;

let test_configure_rebuilds () =
  Unix.putenv "DIO_BACKGROUND_CPUS" "4";
  Thread_affinity.configure ();
  check_cpu "reconfigured" 4 (Thread_affinity.next_background_cpu ())
;;

let () =
  Alcotest.run
    "thread_affinity"
    [ ( "env"
      , [ Alcotest.test_case
            "configure reads the env after module load"
            `Quick
            test_configure_after_env
        ; Alcotest.test_case
            "configure rebuilds the allocator"
            `Quick
            test_configure_rebuilds
        ] )
    ]
;;
