(** Terminal dashboard for the Dio trading engine.

    Runs as an out-of-process binary for crash isolation. Connects to the
    engine over a Unix domain socket using a length-prefixed JSON protocol
    in watch mode, where the engine pushes snapshot frames on each tick.


    Usage: ./dio-dashboard [--socket /tmp/dio-<pid>.sock]
*)

let () =
  (* Parse command-line arg early to set the socket override if any *)
  let speclist = [
    "--socket", Arg.Set_string Dashboard_ui.App.socket_path, " Path to engine UDS (auto-discovers if not set)";
  ] in
  Arg.parse speclist (fun _ -> ()) "dio-dashboard [--socket /tmp/dio-<pid>.sock]";
  
  Dashboard_ui.App.run ()