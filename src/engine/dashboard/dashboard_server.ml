[@@@ocaml.text
  " Dashboard Unix Domain Socket server.\n\n    Runs as an Lwt fiber in the engine process, exposing engine state to the standalone\n    dashboard binary over a UDS (see {!socket_path}).\n\n    Wire protocol (length-prefixed JSON): Client to Server: 1-byte ASCII command 'S' =\n    request a single state snapshot 'W' = enter watch mode (server pushes state every 500\n    ms) 'P' = heartbeat pong (client confirms it rendered a frame) 'Q' = close the\n    connection Server to Client: 4-byte big-endian length prefix followed by a JSON\n    payload "]
open Lwt.Infix
let section = "dashboard_server"
let socket_dir =
  let preferred = "/var/run/dio" in
  try Unix.mkdir preferred 0o755; preferred
  with | Unix.Unix_error (Unix.EEXIST, _, _) -> preferred
  | Unix.Unix_error _ -> "/tmp/dio"[@@ocaml.doc
                                     " UDS directory for the dashboard socket. /var/run/dio is volume-mountable for access\n    from outside the engine container; falls back to /tmp/dio. "]
let socket_path () = Printf.sprintf "%s/dashboard.sock" socket_dir
let send_response oc json_str =
  let len = String.length json_str in
  let header = Bytes.create 4 in
  Bytes.set_uint8 header 0 ((len lsr 24) land 0xff);
  Bytes.set_uint8 header 1 ((len lsr 16) land 0xff);
  Bytes.set_uint8 header 2 ((len lsr 8) land 0xff);
  Bytes.set_uint8 header 3 (len land 0xff);
  (let header_str = Bytes.to_string header in
   (Lwt_io.write oc header_str) >>=
     (fun () -> (Lwt_io.write oc json_str) >>= (fun () -> Lwt_io.flush oc)))
  [@@ocaml.doc
    " Write a length-prefixed JSON frame to [oc]: [json_str] length as a 4-byte big-endian\n    header, then the payload. Propagates write errors; callers handle client disconnects. "]
let active_clients = ref 0[@@ocaml.doc
                            " Count of currently connected dashboard clients. "]
type watch_entry =
  {
  oc: Lwt_io.output_channel ;
  fd: Lwt_unix.file_descr ;
  cancel: unit Lwt.u ;
  last_pong: float ref }[@@ocaml.doc
                          " Watch client state: output channel, fd for explicit close, a cancel resolver to\n    unblock the reader when the broadcaster detects a dead write, and a heartbeat\n    timestamp updated by pong messages. "]
let watch_clients : watch_entry list ref = ref [][@@ocaml.doc
                                                   " Mutable list of watch-mode client entries. "]
let snapshot_cache_str = ref ""[@@ocaml.doc
                                 " Cached serialized snapshot and its generation timestamp. Reused across broadcast ticks\n    to avoid repeated Yojson serialization; invalidated after [snapshot_cache_max_age]\n    seconds so each full-snapshot tick rebuilds. "]
let snapshot_cache_time = ref 0.0
let snapshot_cache_max_age = 0.45
let full_snapshot_interval = 0.5
let broadcast_tick = 0.05
let last_full_time = ref 0.0
let log_last_seq = ref 0
let refresh_capture () = Logging.set_capture_enabled ((!watch_clients) <> [])
  [@@ocaml.doc
    " Capture engine log lines into the dashboard ring only while a client is attached. "]
let heartbeat_timeout = 8.0
let idle_read_timeout = 10.0
let close_fd fd =
  Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit)[@@ocaml.doc
                                                                    " Close a client fd, ignoring errors if already closed. "]
let rec state_broadcaster () =
  let __ppx_lwt_0 = Lwt_unix.sleep broadcast_tick in
  Lwt.backtrace_bind "src/engine/dashboard/dashboard_server.ml" 98
    (fun exn -> try Lwt.reraise exn with | exn -> exn) __ppx_lwt_0
    (fun () ->
       let current_clients = !watch_clients in
       (if current_clients <> []
        then
          Lwt.catch
            (fun () ->
               let now = Unix.gettimeofday () in
               let __ppx_lwt_0 =
                 Lwt_list.filter_map_s
                   (fun entry ->
                      let age = now -. (!(entry.last_pong)) in
                      if age > heartbeat_timeout
                      then
                        (Logging.info_f ~section
                           "Pruning stale dashboard client (no pong for %.1fs)"
                           age;
                         (try Lwt.wakeup entry.cancel ()
                          with | Invalid_argument _ -> ());
                         (let __ppx_lwt_0 = close_fd entry.fd in
                          Lwt.backtrace_bind
                            "src/engine/dashboard/dashboard_server.ml" 118
                            (fun exn -> try Lwt.reraise exn with | exn -> exn)
                            __ppx_lwt_0 (fun () -> Lwt.return_none)))
                      else Lwt.return (Some entry)) current_clients in
               Lwt.backtrace_bind "src/engine/dashboard/dashboard_server.ml"
                 106 (fun exn -> try Lwt.reraise exn with | exn -> exn)
                 __ppx_lwt_0
                 (fun live_clients ->
                    watch_clients := live_clients;
                    refresh_capture ();
                    (let payload_opt =
                       if
                         (now -. (!last_full_time)) >= full_snapshot_interval
                       then
                         (last_full_time := now;
                          (let s =
                             if
                               ((now -. (!snapshot_cache_time)) <
                                  snapshot_cache_max_age)
                                 && ((!snapshot_cache_str) <> "")
                             then !snapshot_cache_str
                             else
                               (let s = Dashboard_state.snapshot_to_string () in
                                snapshot_cache_str := s;
                                snapshot_cache_time := now;
                                s) in
                           Some s))
                       else
                         (let (latest, fresh_count, json) =
                            Logging.delta ~since:(!log_last_seq) in
                          log_last_seq := latest;
                          if fresh_count > 0
                          then Some (Yojson.Basic.to_string json)
                          else None) in
                     match payload_opt with
                     | None -> Lwt.return_unit
                     | Some json_str ->
                         let __ppx_lwt_0 =
                           Lwt_list.filter_map_p
                             (fun entry ->
                                Lwt.catch
                                  (fun () ->
                                     Lwt.pick
                                       [(send_response entry.oc json_str) >|=
                                          ((fun () -> Some entry));
                                       (let __ppx_lwt_0 = Lwt_unix.sleep 1.0 in
                                        Lwt.backtrace_bind
                                          "src/engine/dashboard/dashboard_server.ml"
                                          160
                                          (fun exn ->
                                             try Lwt.reraise exn
                                             with | exn -> exn) __ppx_lwt_0
                                          (fun () ->
                                             Lwt.fail
                                               (Failure "write timeout")))])
                                  (fun _ ->
                                     (try Lwt.wakeup entry.cancel ()
                                      with | Invalid_argument _ -> ());
                                     (let __ppx_lwt_0 = close_fd entry.fd in
                                      Lwt.backtrace_bind
                                        "src/engine/dashboard/dashboard_server.ml"
                                        166
                                        (fun exn ->
                                           try Lwt.reraise exn
                                           with | exn -> exn) __ppx_lwt_0
                                        (fun () -> Lwt.return_none))))
                             live_clients in
                         Lwt.backtrace_bind
                           "src/engine/dashboard/dashboard_server.ml" 153
                           (fun exn -> try Lwt.reraise exn with | exn -> exn)
                           __ppx_lwt_0
                           (fun surviving ->
                              watch_clients := surviving;
                              refresh_capture ();
                              Lwt.return_unit))))
            (fun exn ->
               Logging.error_f ~section "Error in state_broadcaster: %s"
                 (Printexc.to_string exn);
               Lwt.return_unit)
        else Lwt.return_unit) >>=
         (fun () -> Lwt.async state_broadcaster; Lwt.return_unit))[@@ocaml.doc
                                                                    " Periodic broadcaster: every [broadcast_tick], push either a full state snapshot (on\n    the [full_snapshot_interval] cadence) or a small flow-delta frame carrying only\n    entries newer than [flow_last_seq]. Nothing is sent on a tick with no full snapshot\n    due and no fresh flow entries. On write failure, wake the client's cancel resolver to\n    unblock its reader and close its fd. "]
let read_with_timeout ic buf off len timeout_s =
  Lwt.pick
    [Lwt_io.read_into ic buf off len;
    (let __ppx_lwt_0 = Lwt_unix.sleep timeout_s in
     Lwt.backtrace_bind "src/engine/dashboard/dashboard_server.ml" 191
       (fun exn -> try Lwt.reraise exn with | exn -> exn) __ppx_lwt_0
       (fun () -> Lwt.return 0))][@@ocaml.doc
                                   " Read with a timeout. Returns 0 (simulating EOF) if [timeout_s] elapses before any data\n    arrives. "]
let handle_client ~fd ~cancel_promise (ic, oc) =
  let buf = Bytes.create 1 in
  let rec loop () =
    Lwt.catch
      (fun () ->
         let __ppx_lwt_0 = read_with_timeout ic buf 0 1 idle_read_timeout in
         Lwt.backtrace_bind "src/engine/dashboard/dashboard_server.ml" 205
           (fun exn -> try Lwt.reraise exn with | exn -> exn) __ppx_lwt_0
           (fun n ->
              if n = 0
              then Lwt.return_unit
              else
                (let cmd = Bytes.get buf 0 in
                 match cmd with
                 | 'S' ->
                     let json_str = Dashboard_state.snapshot_to_string () in
                     let __ppx_lwt_0 = send_response oc json_str in
                     Lwt.backtrace_bind
                       "src/engine/dashboard/dashboard_server.ml" 214
                       (fun exn -> try Lwt.reraise exn with | exn -> exn)
                       __ppx_lwt_0 (fun () -> loop ())
                 | 'W' ->
                     let (cancel_promise_inner, cancel_resolver) =
                       Lwt.wait () in
                     let pong_time = ref (Unix.gettimeofday ()) in
                     let entry =
                       {
                         oc;
                         fd;
                         cancel = cancel_resolver;
                         last_pong = pong_time
                       } in
                     (watch_clients := (entry :: (!watch_clients));
                      refresh_capture ();
                      (let wait_for_quit () =
                         let (quit_p, quit_u) = Lwt.wait () in
                         let stream =
                           Lwt_stream.from
                             (fun () ->
                                Lwt.catch
                                  (fun () ->
                                     (Lwt_io.read_into ic buf 0 1) >>=
                                       (fun n ->
                                          if n = 0
                                          then Lwt.return_none
                                          else
                                            Lwt.return_some (Bytes.get buf 0)))
                                  (fun _ -> Lwt.return_none)) in
                         let process_char c =
                           if c = 'Q'
                           then
                             try Lwt.wakeup_later quit_u ()
                             with | Invalid_argument _ -> ()
                           else
                             if c = 'P'
                             then pong_time := (Unix.gettimeofday ()) in
                         Lwt.async
                           (fun () ->
                              (Concurrency.Lwt_util.consume_stream
                                 process_char stream)
                                >>=
                                (fun () ->
                                   (try Lwt.wakeup_later quit_u ()
                                    with | Invalid_argument _ -> ());
                                   Lwt.return_unit));
                         quit_p in
                       let __ppx_lwt_0 =
                         Lwt.pick
                           [wait_for_quit ();
                           cancel_promise_inner;
                           cancel_promise] in
                       Lwt.backtrace_bind
                         "src/engine/dashboard/dashboard_server.ml" 252
                         (fun exn -> try Lwt.reraise exn with | exn -> exn)
                         __ppx_lwt_0
                         (fun () ->
                            watch_clients :=
                              (List.filter (fun e -> e.fd != fd)
                                 (!watch_clients));
                            refresh_capture ();
                            Lwt.return_unit)))
                 | 'Q' -> Lwt.return_unit
                 | _ ->
                     let err =
                       `Assoc [("error", (`String "unknown command"))] in
                     let __ppx_lwt_0 =
                       send_response oc (Yojson.Basic.to_string err) in
                     Lwt.backtrace_bind
                       "src/engine/dashboard/dashboard_server.ml" 263
                       (fun exn -> try Lwt.reraise exn with | exn -> exn)
                       __ppx_lwt_0 (fun () -> loop ()))))
      (fun _exn -> Lwt.return_unit) in
  loop ()[@@ocaml.doc
           " Handle one client connection over [ic]/[oc]/[fd], reading 1-byte commands until\n    disconnect or 'Q'. [cancel_promise] resolves when the broadcaster detects a dead\n    write, interrupting blocked reads. The initial command read uses [idle_read_timeout]\n    so zombie connections that send no command byte are cleaned up. "]
let start ~start_time =
  Dashboard_state.set_start_time start_time;
  (let path = socket_path () in
   (try Unix.mkdir socket_dir 0o755 with | Unix.Unix_error _ -> ());
   (try Unix.unlink path with | Unix.Unix_error _ -> ());
   (let addr = Lwt_unix.ADDR_UNIX path in
    let server_socket = Lwt_unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
    Lwt_unix.setsockopt server_socket Unix.SO_REUSEADDR true;
    (let __ppx_lwt_0 = Lwt_unix.bind server_socket addr in
     Lwt.backtrace_bind "src/engine/dashboard/dashboard_server.ml" 285
       (fun exn -> try Lwt.reraise exn with | exn -> exn) __ppx_lwt_0
       (fun () ->
          Lwt_unix.listen server_socket 4;
          Unix.chmod path 0o600;
          Logging.info_f ~section "Dashboard server listening on %s" path;
          Lwt.async state_broadcaster;
          (let rec accept_loop () =
             let __ppx_lwt_0 = Lwt_unix.accept server_socket in
             Lwt.backtrace_bind "src/engine/dashboard/dashboard_server.ml"
               292 (fun exn -> try Lwt.reraise exn with | exn -> exn)
               __ppx_lwt_0
               (fun (client_fd, _client_addr) ->
                  if (!active_clients) >= 5
                  then
                    (Logging.warn_f ~section
                       "Dashboard client rejected: Max active clients (5) reached";
                     (close_fd client_fd) >>= ((fun () -> accept_loop ())))
                  else
                    (active_clients := ((!active_clients) + 1);
                     Logging.info_f ~section
                       "Dashboard client connected (active: %d)"
                       (!active_clients);
                     Lwt.async
                       (fun () ->
                          let ic =
                            Lwt_io.of_fd ~close:(fun () -> Lwt.return_unit)
                              ~mode:Lwt_io.Input client_fd in
                          let oc =
                            Lwt_io.of_fd ~close:(fun () -> Lwt.return_unit)
                              ~mode:Lwt_io.Output client_fd in
                          let (cancel_promise, _cancel_resolver) =
                            Lwt.wait () in
                          Lwt.finalize
                            (fun () ->
                               handle_client ~fd:client_fd ~cancel_promise
                                 (ic, oc))
                            (fun () ->
                               watch_clients :=
                                 (List.filter (fun e -> e.fd != client_fd)
                                    (!watch_clients));
                               refresh_capture ();
                               active_clients := ((!active_clients) - 1);
                               Logging.info_f ~section
                                 "Dashboard client disconnected (active: %d)"
                                 (!active_clients);
                               (let __ppx_lwt_0 = Lwt_io.abort ic in
                                Lwt.backtrace_bind
                                  "src/engine/dashboard/dashboard_server.ml"
                                  319
                                  (fun exn ->
                                     try Lwt.reraise exn with | exn -> exn)
                                  __ppx_lwt_0
                                  (fun () ->
                                     let __ppx_lwt_0 = Lwt_io.abort oc in
                                     Lwt.backtrace_bind
                                       "src/engine/dashboard/dashboard_server.ml"
                                       320
                                       (fun exn ->
                                          try Lwt.reraise exn
                                          with | exn -> exn) __ppx_lwt_0
                                       (fun () -> close_fd client_fd)))));
                     accept_loop ())) in
           Lwt.catch (fun () -> accept_loop ())
             (fun exn ->
                Logging.info_f ~section "Dashboard server shutting down: %s"
                  (Printexc.to_string exn);
                (Lwt.catch (fun () -> Lwt_unix.close server_socket)
                   (fun _ -> Lwt.return_unit))
                  >>=
                  ((fun () ->
                      (try Unix.unlink path with | _ -> ()); Lwt.return_unit))))))))
  [@@ocaml.doc
    " Start the UDS server as an Lwt fiber: bind, spawn the state broadcaster, and enter an\n    accept loop. Limits concurrent clients to 5. Call after engine initialization. "]
let shutdown () =
  let path = socket_path () in
  (try Unix.unlink path with | _ -> ());
  Logging.info_f ~section "Dashboard socket cleaned up: %s" path[@@ocaml.doc
                                                                  " Removes the UDS socket file. Call during engine shutdown. "]
