(** Dashboard Unix Domain Socket Server

    Runs inside the engine process as an Lwt fiber. Exposes engine state
    to the separate dashboard binary via a UDS at /tmp/dio-{pid}.sock.

    Protocol (length-prefixed JSON):
      Client -> Server: 1-byte command
        'S' = full state snapshot
        'W' = watch mode (continuous push every 500ms)
        'Q' = close connection
      Server -> Client: 4-byte big-endian length + JSON payload
*)

open Lwt.Infix

let section = "dashboard_server"

(** Socket path for this engine instance *)
let socket_path () =
  let pid = Unix.getpid () in
  Printf.sprintf "/tmp/dio-%d.sock" pid

(** Encode length-prefixed message: 4 bytes big-endian + payload *)
let encode_message payload =
  let len = String.length payload in
  let header = Bytes.create 4 in
  Bytes.set_uint8 header 0 ((len lsr 24) land 0xff);
  Bytes.set_uint8 header 1 ((len lsr 16) land 0xff);
  Bytes.set_uint8 header 2 ((len lsr 8) land 0xff);
  Bytes.set_uint8 header 3 (len land 0xff);
  Bytes.to_string header ^ payload

(** Send a length-prefixed JSON response *)
let send_response oc json_str =
  let msg = encode_message json_str in
  Lwt.catch
    (fun () ->
      Lwt_io.write oc msg >>= fun () ->
      Lwt_io.flush oc)
    (fun _exn -> Lwt.return_unit)

(** Handle a single client connection *)
let handle_client (ic, oc) =
  let buf = Bytes.create 1 in
  let rec loop () =
    Lwt.catch
      (fun () ->
        let%lwt n = Lwt_io.read_into ic buf 0 1 in
        if n = 0 then Lwt.return_unit  (* Client disconnected *)
        else
          let cmd = Bytes.get buf 0 in
          match cmd with
          | 'S' ->
              (* Single snapshot *)
              let json_str = Dashboard_state.snapshot_to_string () in
              let%lwt () = send_response oc json_str in
              loop ()
          | 'W' ->
              (* Watch mode: push snapshots every 500ms *)
              let stopped = ref false in
              let watch_loop =
                let rec push () =
                  if !stopped then Lwt.return_unit
                  else
                    Lwt.catch
                      (fun () ->
                        let json_str = Dashboard_state.snapshot_to_string () in
                        let%lwt () = send_response oc json_str in
                        let%lwt () = Lwt_unix.sleep 0.5 in
                        push ())
                      (fun _exn ->
                        stopped := true;
                        Lwt.return_unit)
                in
                push ()
              in
              (* Also listen for 'Q' to stop watch mode *)
              let wait_for_quit =
                let rec wait () =
                  Lwt.catch
                    (fun () ->
                      let%lwt n = Lwt_io.read_into ic buf 0 1 in
                      if n = 0 then (stopped := true; Lwt.return_unit)
                      else if Bytes.get buf 0 = 'Q' then (stopped := true; Lwt.return_unit)
                      else wait ())
                    (fun _exn ->
                      stopped := true;
                      Lwt.return_unit)
                in
                wait ()
              in
              let%lwt () = Lwt.pick [watch_loop; wait_for_quit] in
              Lwt.return_unit
          | 'Q' ->
              Lwt.return_unit  (* Close *)
          | _ ->
              (* Unknown command — send error *)
              let err = `Assoc ["error", `String "unknown command"] in
              let%lwt () = send_response oc (Yojson.Basic.to_string err) in
              loop ())
      (fun _exn ->
        (* Connection error — client disconnected *)
        Lwt.return_unit)
  in
  loop ()

(** Active client count for monitoring *)
let active_clients = Atomic.make 0

(** Start the UDS server as an Lwt fiber.
    Call this from main.ml after engine initialization. *)
let start ~start_time =
  Dashboard_state.set_start_time start_time;
  let path = socket_path () in
  (* Clean up any stale socket *)
  (try Unix.unlink path with Unix.Unix_error _ -> ());
  let addr = Lwt_unix.ADDR_UNIX path in
  let server_socket = Lwt_unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt server_socket Unix.SO_REUSEADDR true;
  let%lwt () = Lwt_unix.bind server_socket addr in
  Lwt_unix.listen server_socket 4;
  (* Set socket permissions so only current user can connect *)
  Unix.chmod path 0o600;
  Logging.info_f ~section "Dashboard server listening on %s" path;

  let rec accept_loop () =
    let%lwt (client_fd, _client_addr) = Lwt_unix.accept server_socket in
    let _count = Atomic.fetch_and_add active_clients 1 in
    Logging.info_f ~section "Dashboard client connected (active: %d)" (Atomic.get active_clients);
    Lwt.async (fun () ->
      let ic = Lwt_io.of_fd ~mode:Lwt_io.Input client_fd in
      let oc = Lwt_io.of_fd ~mode:Lwt_io.Output client_fd in
      Lwt.finalize
        (fun () -> handle_client (ic, oc))
        (fun () ->
          ignore (Atomic.fetch_and_add active_clients (-1));
          Logging.info_f ~section "Dashboard client disconnected (active: %d)" (Atomic.get active_clients);
          Lwt.catch
            (fun () -> Lwt_unix.close client_fd)
            (fun _exn -> Lwt.return_unit)));
    accept_loop ()
  in

  (* Run accept loop, catch shutdown *)
  Lwt.catch
    (fun () -> accept_loop ())
    (fun exn ->
      Logging.info_f ~section "Dashboard server shutting down: %s" (Printexc.to_string exn);
      Lwt.catch
        (fun () -> Lwt_unix.close server_socket)
        (fun _ -> Lwt.return_unit)
      >>= fun () ->
      (try Unix.unlink path with _ -> ());
      Lwt.return_unit)

(** Cleanup socket on shutdown *)
let shutdown () =
  let path = socket_path () in
  (try Unix.unlink path with _ -> ());
  Logging.info_f ~section "Dashboard socket cleaned up: %s" path
