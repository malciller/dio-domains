(** Dashboard Unix Domain Socket Server

    Runs as an Lwt fiber within the engine process. Exposes engine state
    to the standalone dashboard binary over a UDS at /tmp/dio-{pid}.sock.

    Wire protocol (length-prefixed JSON):
      Client to Server: 1-byte ASCII command
        'S' = request a single state snapshot
        'W' = enter watch mode (server pushes state every 500 ms)
        'P' = heartbeat pong (client confirms it rendered a frame)
        'Q' = close the connection
      Server to Client: 4-byte big-endian length prefix followed by a JSON payload
*)

open Lwt.Infix

let section = "dashboard_server"

(** Fixed UDS path for the dashboard socket.
    Lives under /var/run/dio/ so it can be volume-mounted and accessed
    from outside the engine container (e.g. via docker run --rm -it). *)
let socket_dir = "/var/run/dio"
let socket_path () =
  Printf.sprintf "%s/dashboard.sock" socket_dir

(** Writes a length-prefixed JSON frame to [oc].
    Encodes [json_str] length as a 4-byte big-endian header.
    Propagates write errors; callers must handle client disconnects. *)
let send_response oc json_str =
  let len = String.length json_str in
  let header = Bytes.create 4 in
  Bytes.set_uint8 header 0 ((len lsr 24) land 0xff);
  Bytes.set_uint8 header 1 ((len lsr 16) land 0xff);
  Bytes.set_uint8 header 2 ((len lsr 8) land 0xff);
  Bytes.set_uint8 header 3 (len land 0xff);
  let header_str = Bytes.to_string header in
  Lwt_io.write oc header_str >>= fun () ->
  Lwt_io.write oc json_str >>= fun () ->
  Lwt_io.flush oc

(** Count of currently connected dashboard clients. *)
let active_clients = ref 0

(** Each watch client tracks its output channel, the underlying fd
    (for explicit close), a cancel resolver to unblock the reader
    when the broadcaster detects a dead write, and a heartbeat
    timestamp updated by client pong messages. *)
type watch_entry = {
  oc : Lwt_io.output_channel;
  fd : Lwt_unix.file_descr;
  cancel : unit Lwt.u;
  last_pong : float ref;
}

(** Mutable list of watch-mode client entries. *)
let watch_clients : watch_entry list ref = ref []

(** Cached serialized snapshot string and its generation timestamp.
    Reused across 500 ms broadcast ticks to avoid repeated Yojson
    serialization. Invalidated after [snapshot_cache_max_age] seconds
    so displayed data remains within ~1 s of real-time. *)
let snapshot_cache_str = ref ""
let snapshot_cache_time = ref 0.0
let snapshot_cache_max_age = 0.9
let heartbeat_timeout = 3.0  (* seconds without pong before pruning *)
let idle_read_timeout = 10.0 (* seconds waiting for a command before dropping client *)

(** Close a client fd, ignoring errors if already closed. *)
let close_fd fd =
  Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit)

(** Periodic broadcaster that pushes state to all watch-mode clients
    every 500 ms. Uses the snapshot cache when valid; rebuilds otherwise.
    When a write fails, the client's cancel resolver is woken to unblock
    its reader, and its fd is closed. *)
let rec state_broadcaster () =
  let%lwt () = Lwt_unix.sleep 0.5 in
  let current_clients = !watch_clients in
  (if current_clients <> [] then begin
    Lwt.catch
      (fun () ->
        let now = Unix.gettimeofday () in
        (* Prune clients that have not sent a heartbeat pong within the timeout. *)
        let%lwt live_clients = Lwt_list.filter_map_s (fun entry ->
          let age = now -. !(entry.last_pong) in
          if age > heartbeat_timeout then begin
            Logging.info_f ~section "Pruning stale dashboard client (no pong for %.1fs)" age;
            (try Lwt.wakeup entry.cancel () with Invalid_argument _ -> ());
            let%lwt () = close_fd entry.fd in
            Lwt.return_none
          end else
            Lwt.return (Some entry)
        ) current_clients in
        watch_clients := live_clients;
        (* Use cached snapshot if within max age; otherwise regenerate. *)
        let json_str =
          if now -. !snapshot_cache_time < snapshot_cache_max_age && !snapshot_cache_str <> ""
          then !snapshot_cache_str
          else begin
            let s = Dashboard_state.snapshot_to_string () in
            snapshot_cache_str := s;
            snapshot_cache_time := now;
            s
          end
        in
        (* Broadcast in parallel; on write failure or timeout, signal cancel and close fd. *)
        let%lwt surviving = Lwt_list.filter_map_p (fun entry ->
          Lwt.catch
            (fun () ->
              Lwt.pick [
                (send_response entry.oc json_str >|= fun () -> Some entry);
                (let%lwt () = Lwt_unix.sleep 0.2 in Lwt.fail (Failure "write timeout"))
              ]
            )
            (fun _ ->
              (try Lwt.wakeup entry.cancel () with Invalid_argument _ -> ());
              let%lwt () = close_fd entry.fd in
              Lwt.return_none)
        ) live_clients in
        watch_clients := surviving;
        Lwt.return_unit)
      (fun exn ->
        Logging.error_f ~section "Error in state_broadcaster: %s" (Printexc.to_string exn);
        Lwt.return_unit)
  end else Lwt.return_unit)
  >>= fun () ->
  (* Sever promise chain to prevent Forward node accumulation. *)
  Lwt.async state_broadcaster;
  Lwt.return_unit

(** Read with a timeout.  Returns 0 (simulating EOF) if [timeout_s]
    elapses before any data arrives. *)
let read_with_timeout ic buf off len timeout_s =
  Lwt.pick [
    Lwt_io.read_into ic buf off len;
    (let%lwt () = Lwt_unix.sleep timeout_s in Lwt.return 0)
  ]

(** Handles a single client connection over [ic]/[oc]/[fd].
    Reads 1-byte commands in a loop until disconnect or 'Q'.
    [cancel_promise] resolves when the broadcaster detects a dead write,
    allowing blocked reads to be interrupted.
    The initial command read uses [idle_read_timeout] so zombie connections
    that never send a command byte are cleaned up. *)
let handle_client ~fd ~cancel_promise (ic, oc) =
  let buf = Bytes.create 1 in
  let rec loop () =
    Lwt.catch
      (fun () ->
        let%lwt n = read_with_timeout ic buf 0 1 idle_read_timeout in
        if n = 0 then Lwt.return_unit  (* EOF or timeout: client disconnected *)
        else
          let cmd = Bytes.get buf 0 in
          match cmd with
          | 'S' ->
              (* Respond with a single state snapshot. *)
              let json_str = Dashboard_state.snapshot_to_string () in
              let%lwt () = send_response oc json_str in
              loop ()
          | 'W' ->
              (* Register this client for periodic broadcast. *)
              let cancel_promise_inner, cancel_resolver = Lwt.wait () in
              let pong_time = ref (Unix.gettimeofday ()) in
              let entry = { oc; fd; cancel = cancel_resolver; last_pong = pong_time } in
              watch_clients := entry :: !watch_clients;
              (* Block until client sends 'Q', disconnects, or is cancelled
                 by the broadcaster detecting a write failure or heartbeat timeout. *)
              let wait_for_quit () =
                let quit_p, quit_u = Lwt.wait () in
                let rec read_loop () =
                  Lwt.catch
                    (fun () ->
                      let%lwt n = Lwt_io.read_into ic buf 0 1 in
                      if n = 0 || Bytes.get buf 0 = 'Q' then begin
                        (try Lwt.wakeup_later quit_u () with Invalid_argument _ -> ());
                        Lwt.return_unit
                      end else begin
                        if Bytes.get buf 0 = 'P' then
                          pong_time := Unix.gettimeofday ();
                        (* Sever Forward chain: spawn next read independently. *)
                        Lwt.async read_loop;
                        Lwt.return_unit
                      end)
                    (fun _exn ->
                      (try Lwt.wakeup_later quit_u () with Invalid_argument _ -> ());
                      Lwt.return_unit)
                in
                Lwt.async read_loop;
                quit_p
              in
              let%lwt () = Lwt.pick [wait_for_quit (); cancel_promise_inner; cancel_promise] in
              (* Unsubscribe from broadcast list. *)
              watch_clients := List.filter (fun e -> e.fd != fd) !watch_clients;
              Lwt.return_unit
          | 'Q' ->
              Lwt.return_unit  (* Graceful close *)
          | _ ->
              (* Unrecognized command; respond with JSON error. *)
              let err = `Assoc ["error", `String "unknown command"] in
              let%lwt () = send_response oc (Yojson.Basic.to_string err) in
              loop ())
      (fun _exn ->
        (* Read error; treat as client disconnect. *)
        Lwt.return_unit)
  in
  loop ()

(** Starts the UDS server as an Lwt fiber.
    Binds to [/var/run/dio/dashboard.sock], spawns the state broadcaster,
    and enters an accept loop. Limits concurrent clients to 5.
    Call after engine initialization. *)
let start ~start_time =
  Dashboard_state.set_start_time start_time;
  let path = socket_path () in
  (* Ensure socket directory exists. *)
  (try Unix.mkdir socket_dir 0o755 with Unix.Unix_error (Unix.EEXIST, _, _) -> ());
  (* Remove stale socket file from a previous run. *)
  (try Unix.unlink path with Unix.Unix_error _ -> ());
  let addr = Lwt_unix.ADDR_UNIX path in
  let server_socket = Lwt_unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt server_socket Unix.SO_REUSEADDR true;
  let%lwt () = Lwt_unix.bind server_socket addr in
  Lwt_unix.listen server_socket 4;
  (* Restrict socket access to the current user. *)
  Unix.chmod path 0o600;
  Logging.info_f ~section "Dashboard server listening on %s" path;

  Lwt.async state_broadcaster;

  let rec accept_loop () =
    let%lwt (client_fd, _client_addr) = Lwt_unix.accept server_socket in
    if !active_clients >= 5 then begin
      Logging.warn_f ~section "Dashboard client rejected: Max active clients (5) reached";
      close_fd client_fd >>= fun () ->
      accept_loop ()
    end else begin
      active_clients := !active_clients + 1;
      Logging.info_f ~section "Dashboard client connected (active: %d)" !active_clients;
    Lwt.async (fun () ->
      let ic = Lwt_io.of_fd ~close:(fun () -> Lwt.return_unit) ~mode:Lwt_io.Input client_fd in
      let oc = Lwt_io.of_fd ~close:(fun () -> Lwt.return_unit) ~mode:Lwt_io.Output client_fd in
      let cancel_promise, _cancel_resolver = Lwt.wait () in
      Lwt.finalize
        (fun () -> handle_client ~fd:client_fd ~cancel_promise (ic, oc))
        (fun () ->
          (* Remove from watch list in case handle_client didn't clean up. *)
          watch_clients := List.filter (fun e -> e.fd != client_fd) !watch_clients;
          active_clients := !active_clients - 1;
          Logging.info_f ~section "Dashboard client disconnected (active: %d)" !active_clients;
          close_fd client_fd));
      accept_loop ()
    end
  in

  (* Accept connections until the server socket is closed or an error occurs. *)
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

(** Removes the UDS socket file. Call during engine shutdown. *)
let shutdown () =
  let path = socket_path () in
  (try Unix.unlink path with _ -> ());
  Logging.info_f ~section "Dashboard socket cleaned up: %s" path
