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

(** Send a length-prefixed JSON response.
    Propagates write errors to the caller. Callers that want to tolerate
    client disconnects must catch exceptions themselves. *)
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

(** Active client count for monitoring *)
let active_clients = Atomic.make 0

(** List of active output channels subscribed to watch mode *)
let watch_clients : Lwt_io.output_channel list ref = ref []

(** Snapshot string cache — reused across 500ms ticks to avoid rebuilding
    the full Yojson AST on every broadcaster cycle. Invalidated after ~900ms
    so the displayed data is never more than ~1s stale. *)
let snapshot_cache_str = ref ""
let snapshot_cache_time = ref 0.0
let snapshot_cache_max_age = 0.9

(** Central ticker that broadcasts state to all watching clients every 500ms.
    Clients that fail a write (disconnected) are pruned from watch_clients.
    The snapshot string is cached for up to snapshot_cache_max_age seconds
    to avoid rebuilding the full Yojson AST on every 500ms tick. *)
let rec state_broadcaster () =
  let%lwt () = Lwt_unix.sleep 0.5 in
  let current_clients = !watch_clients in
  (if current_clients <> [] then begin
    Lwt.catch
      (fun () ->
        (* Reuse cached snapshot if fresh enough; otherwise rebuild and cache. *)
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
        (* Broadcast sequentially; filter out any client whose write fails *)
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
              (* Watch mode: subscribe to central state_broadcaster *)
              watch_clients := oc :: !watch_clients;
              (* Listen for 'Q' or disconnect to stop watch mode *)
              let rec wait_for_quit () =
                Lwt.catch
                  (fun () ->
                    let%lwt n = Lwt_io.read_into ic buf 0 1 in
                    if n = 0 || Bytes.get buf 0 = 'Q' then Lwt.return_unit
                    else wait_for_quit ())
                  (fun _exn -> Lwt.return_unit)
              in
              let%lwt () = wait_for_quit () in
              (* Unsubscribe *)
              watch_clients := List.filter (fun c -> c != oc) !watch_clients;
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
