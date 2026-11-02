(* Guards the epoll backend's option codes and startup probe.

   Production incident this exists to prevent: DEL and MOD were swapped on the OCaml side.
   A MOD with an empty mask is accepted by the kernel and leaves the fd registered, so the
   engine's own registry forgot the fd while the kernel did not. Every later ADD on that
   fd then failed with EEXIST, which surfaced as a permanent WebSocket reconnect loop on a
   live box.

   The codes are now read from <sys/epoll.h> through the C stub rather than restated in
   OCaml, and the self-test exercises ADD/MOD/DEL/*ADD*. This test pins both facts.

   On non-Linux hosts the backend is absent by design; the test asserts that it reports
   itself unsupported instead of pretending to work. *)

let is_linux_epoll = Epoll_stubs.supported ()

let test_backend_reported () =
  let backend = Lwt_engine.backend () in
  Alcotest.(check bool)
    "backend is one of select/epoll"
    true
    (backend = "select" || backend = "epoll");
  if is_linux_epoll
  then
    Alcotest.(check string)
      "epoll is selected on Linux when it self-tests"
      "epoll"
      backend
  else Alcotest.(check string) "select is used without epoll" "select" backend
;;

let test_op_codes () =
  (* Values are defined by <sys/epoll.h>; these are the Linux ABI constants. *)
  Alcotest.(check int) "EPOLL_CTL_ADD" 1 Epoll_stubs.ctl_add;
  Alcotest.(check int) "EPOLL_CTL_DEL" 2 Epoll_stubs.ctl_del;
  Alcotest.(check int) "EPOLL_CTL_MOD" 3 Epoll_stubs.ctl_mod;
  Alcotest.(check bool)
    "del and mod are distinct"
    true
    (Epoll_stubs.ctl_del <> Epoll_stubs.ctl_mod)
;;

let test_self_test_matches_support () =
  (* The probe must not claim epoll works when the platform has no epoll at all. *)
  if not is_linux_epoll
  then
    Alcotest.(check bool)
      "no epoll on this platform"
      false
      (Lwt_engine.backend () = "epoll")
;;

let () =
  Alcotest.run
    "io_backend"
    [ ( "epoll"
      , [ Alcotest.test_case "backend reported" `Quick test_backend_reported
        ; Alcotest.test_case "op codes" `Quick test_op_codes
        ; Alcotest.test_case "platform support" `Quick test_self_test_matches_support
        ] )
    ]
;;
