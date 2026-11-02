(* End-to-end test for the out-of-process persistence transport.

   Starts a helper in a child process (the real [dio-persist] serve loop), drives
   [Persist_ipc.send] from the test process, and asserts the file lands atomically with
   the exact bytes sent. This is the contract that lets the trading process skip the fsync
   entirely. *)

let with_temp_dir f =
  let dir =
    Filename.concat
      (Filename.get_temp_dir_name ())
      (Printf.sprintf "dio-persist-%d" (Unix.getpid ()))
  in
  (try Unix.mkdir dir 0o755 with
   | Unix.Unix_error (Unix.EEXIST, _, _) -> ());
  Fun.protect ~finally:(fun () -> ()) (fun () -> f dir)
;;

let read_file path =
  let ic = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in_noerr ic)
    (fun () -> really_input_string ic (in_channel_length ic))
;;

let wait_for_file path =
  let rec loop n =
    if Sys.file_exists path
    then true
    else if n = 0
    then false
    else (
      Thread.delay 0.02;
      loop (n - 1))
  in
  loop 500
;;

(* A helper in this process would share [Persist_ipc]'s module state, so run the real
   serve loop in a forked child. *)
let start_helper socket =
  match Unix.fork () with
  | 0 ->
    (try Dio_persistence.Persist_ipc.serve () with
     | _ -> ());
    exit 0
  | pid ->
    (* Give bind() a moment. *)
    let rec wait n =
      if Sys.file_exists socket
      then ()
      else if n = 0
      then failwith "helper did not bind its socket"
      else (
        Thread.delay 0.02;
        wait (n - 1))
    in
    wait 250;
    pid
;;

let test_roundtrip () =
  with_temp_dir (fun dir ->
    let socket = Filename.concat dir "persist.sock" in
    Dio_persistence.Persist_ipc.set_socket_path socket;
    let pid = start_helper socket in
    Fun.protect
      ~finally:(fun () ->
        (try Unix.kill pid Sys.sigterm with
         | _ -> ());
        try ignore (Unix.waitpid [] pid) with
        | _ -> ())
      (fun () ->
        Dio_persistence.Persist_ipc.init ();
        Alcotest.(check bool)
          "helper attached"
          true
          (Dio_persistence.Persist_ipc.is_enabled ());
        let target = Filename.concat dir "state.json" in
        let contents = "{\"k\":{\"v\":1}}" in
        Dio_persistence.Persist_ipc.send ~filename:target ~contents;
        Alcotest.(check bool) "file written" true (wait_for_file target);
        (* The helper owns the terminator. *)
        Alcotest.(check string) "bytes plus newline" (contents ^ "\n") (read_file target);
        Dio_persistence.Persist_ipc.flush ()))
;;

let () =
  Alcotest.run
    "persist_ipc"
    [ "helper", [ Alcotest.test_case "roundtrip" `Quick test_roundtrip ] ]
;;
