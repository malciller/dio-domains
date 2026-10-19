let run promise =
  let rec loop () =
    match Lwt.poll promise with
    | Some value -> value
    | None ->
      Lwt_engine.iter (Lwt.paused_count () = 0);
      Lwt.wakeup_paused ();
      loop ()
  in
  loop ()
;;
