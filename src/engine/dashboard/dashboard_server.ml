(** Dashboard Unix Domain Socket Server

    Runs as an Lwt fiber within the engine process. Exposes engine state
    to the standalone dashboard binary over a UDS at /tmp/dio-{pid}.sock.

    Wire protocol (length-prefixed JSON):
      Client to Server: 1-byte ASCII command
        'S' = request a single state snapshot
        'W' = enter watch mode (server pushes state every 500 ms)
        'Q' = close the connection
      Server to Client: 4-byte big-endian length prefix followed by a JSON payload
*)

open Lwt.Infix

let section = "dashboard_server"

(** Returns the UDS path for the current engine process, keyed by PID. *)
let socket_path () =
  let pid = Unix.getpid () in
  Printf.sprintf "/tmp/dio-%d.sock" pid

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

(** Atomic count of currently connected dashboard clients. *)
let active_clients = Atomic.make 0

(** Mutable list of output channels subscribed to watch mode broadcasts. *)
let watch_clients : Lwt_io.output_channel list ref = ref []

(** Cached serialized snapshot string and its generation timestamp.
    Reused across 500 ms broadcast ticks to avoid repeated Yojson
    serialization. Invalidated after [snapshot_cache_max_age] seconds
    so displayed data remains within ~1 s of real-time. *)
let snapshot_cache_str = ref ""
let snapshot_cache_time = ref 0.0
let snapshot_cache_max_age = 0.9

(** Periodic broadcaster that pushes state to all watch-mode clients
    every 500 ms. Uses the snapshot cache when valid; rebuilds otherwise.
    Clients that fail a write are removed from [watch_clients]. *)
let rec state_broadcaster () =
  let%lwt () = Lwt_unix.sleep 0.5 in
  let current_clients = !watch_clients in
  (if current_clients <> [] then begin
    Lwt.catch
      (fun () ->
        (* Use cached snapshot if within max age; otherwise regenerate. *)
        let now = Unix.gettimeofday () in
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
        (* Broadcast sequentially; remove clients whose writes fail. *)
        let%lwt surviving = Lwt_list.filter_map_s (fun oc ->
          Lwt.catch
            (fun () -> send_response oc json_str >|= fun () -> Some oc)
            (fun _ -> Lwt.return_none)
        ) current_clients in
        watch_clients := surviving;
        Lwt.return_unit)
      (fun exn ->
        Logging.error_f ~section "Error in state_broadcaster: %s" (Printexc.to_string exn);
        Lwt.return_unit)
  end else Lwt.return_unit)
  >>= fun () -> state_broadcaster ()

(** Handles a single client connection over [ic]/[oc].
    Reads 1-byte commands in a loop until disconnect or 'Q'. *)
let handle_client (ic, oc) =
  let buf = Bytes.create 1 in
  let rec loop () =
    Lwt.catch
      (fun () ->
        let%lwt n = Lwt_io.read_into ic buf 0 1 in
        if n = 0 then Lwt.return_unit  (* EOF: client disconnected *)
        else
          let cmd = Bytes.get buf 0 in
          match cmd with
          | 'S' ->
              (* Respond with a single state snapshot. *)
              let json_str = Dashboard_state.snapshot_to_string () in
              let%lwt () = send_response oc json_str in
              loop ()
          | 'W' ->
              (* Register this channel for periodic broadcast. *)
              watch_clients := oc :: !watch_clients;
              (* Block until client sends 'Q' or disconnects. *)
              let rec wait_for_quit () =
                Lwt.catch
                  (fun () ->
                    let%lwt n = Lwt_io.read_into ic buf 0 1 in
                    if n = 0 || Bytes.get buf 0 = 'Q' then Lwt.return_unit
                    else wait_for_quit ())
                  (fun _exn -> Lwt.return_unit)
              in
              let%lwt () = wait_for_quit () in
              (* Unsubscribe from broadcast list. *)
              watch_clients := List.filter (fun c -> c != oc) !watch_clients;
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
    Binds to the PID-keyed socket path, spawns the state broadcaster,
    and enters an accept loop. Limits concurrent clients to 5.
    Call after engine initialization. *)
let start ~start_time =
  Dashboard_state.set_start_time start_time;
  let path = socket_path () in
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
    if Atomic.get active_clients >= 5 then begin
      Logging.warn_f ~section "Dashboard client rejected: Max active clients (5) reached";
      Lwt.catch (fun () -> Lwt_unix.close client_fd) (fun _ -> Lwt.return_unit) >>= fun () ->
      accept_loop ()
    end else begin
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
