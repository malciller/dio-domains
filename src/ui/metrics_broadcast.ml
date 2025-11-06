(** TCP Metrics Broadcast Server

    Provides TCP-based broadcasting of metrics data as JSON streams.
    Supports separate streams for telemetry, system stats, balances, and logs.
*)

open Lwt.Infix
open Ui_types
open Dio_ui_cache.Telemetry_cache
open Dio_ui_cache.System_cache
open Dio_ui_cache.Balance_cache

(** Connected client state *)
type client = {
  id: int;
  stream_type: string;
  input_channel: Lwt_io.input_channel;
  output_channel: Lwt_io.output_channel;
  mutable active: bool;
}

(** Server state *)
type server_state = {
  next_client_id: int Atomic.t;
  mutable clients: (int, client) Hashtbl.t;
  client_mutex: Mutex.t;  (* Protect hashtable operations *)
  port: int;
  enable_token_auth: bool;
  enable_ip_whitelist: bool;
  token: string option;
  ip_whitelist: string list option;
}

(** Safely close channels with error handling *)
let safe_close_channels input_channel output_channel =
  Lwt.catch
    (fun () ->
      Lwt_io.close input_channel >>= fun () ->
      Lwt_io.close output_channel
    )
    (fun _ -> Lwt.return_unit)  (* Ignore errors when closing *)

(** Create server state *)
let create_server_state config = {
  next_client_id = Atomic.make 0;
  clients = Hashtbl.create 16;
  client_mutex = Mutex.create ();
  port = config.Dio_engine.Config.port;
  enable_token_auth = config.Dio_engine.Config.enable_token_auth;
  enable_ip_whitelist = config.Dio_engine.Config.enable_ip_whitelist;
  token = config.Dio_engine.Config.token;
  ip_whitelist = config.Dio_engine.Config.ip_whitelist;
}

(** Close channels for inactive clients *)
let close_inactive_client_channels state =
  Mutex.lock state.client_mutex;
  Hashtbl.iter (fun _ client ->
    if not client.active then (
      (* Close channels asynchronously to avoid blocking *)
      Lwt.async (fun () ->
        safe_close_channels client.input_channel client.output_channel
      )
    )
  ) state.clients;
  Mutex.unlock state.client_mutex

(** Remove inactive clients *)
let cleanup_clients state =
  (* First close channels for inactive clients *)
  close_inactive_client_channels state;
  Mutex.lock state.client_mutex;
  let to_remove = ref [] in
  Hashtbl.iter (fun id client ->
    if not client.active then
      to_remove := id :: !to_remove
  ) state.clients;
  List.iter (Hashtbl.remove state.clients) !to_remove;
  Mutex.unlock state.client_mutex;
  Logging.debug_f ~section:"broadcast" "Cleaned up %d inactive clients" (List.length !to_remove)

(** Broadcast message to all clients of a specific stream type *)
let broadcast_to_stream state stream_type json_str =
  let count = ref 0 in
  Mutex.lock state.client_mutex;
  Hashtbl.iter (fun _ client ->
    if client.stream_type = stream_type && client.active then (
      incr count;
      Lwt.async (fun () ->
        Lwt.catch
          (fun () ->
            let%lwt () = Lwt_io.write_line client.output_channel json_str in
            Logging.debug_f ~section:"broadcast" "Sent %s update to client %d" stream_type client.id;
            Lwt.return_unit
          )
          (fun exn ->
            (* Handle EPIPE and other connection errors gracefully *)
            match exn with
            | Unix.Unix_error (Unix.EPIPE, _, _) ->
                Logging.debug_f ~section:"broadcast" "Client %d disconnected (broken pipe)" client.id;
                client.active <- false;
                Lwt.return_unit
            | Unix.Unix_error (err, _, _) ->
                Logging.warn_f ~section:"broadcast" "Unix error sending to client %d: %s" client.id (Unix.error_message err);
                client.active <- false;
                Lwt.return_unit
            | _ ->
                Logging.warn_f ~section:"broadcast" "Failed to send to client %d: %s" client.id (Printexc.to_string exn);
                client.active <- false;
                Lwt.return_unit
          )
      )
    )
  ) state.clients;
  Mutex.unlock state.client_mutex;
  if !count > 0 then
    Logging.info_f ~section:"broadcast" "Broadcast %s update to %d clients" stream_type !count
  else
    Logging.debug_f ~section:"broadcast" "Broadcast %s update but no active clients" stream_type

(** Send current snapshot to a client immediately upon connection *)
let send_current_snapshot client stream_type =
  Lwt.async (fun () ->
    Lwt.catch
      (fun () ->
        let json_str = match stream_type with
          | "telemetry" ->
              let snapshot = Dio_ui_cache.Telemetry_cache.current_telemetry_snapshot () in
              Metrics_json.(telemetry_snapshot_to_json snapshot |> json_to_string)
          | "system" ->
              let stats = Dio_ui_cache.System_cache.current_system_stats () in
              Metrics_json.(system_stats_to_json stats |> json_to_string)
          | "balance" ->
              let snapshot = Dio_ui_cache.Balance_cache.current_balance_snapshot () in
              Metrics_json.(balance_snapshot_to_json snapshot |> json_to_string)
          | "log" ->
              (* Logs are streamed individually, not as snapshots *)
              "{\"type\":\"log\",\"message\":\"Log stream active\"}"
          | _ -> ""
        in
        if json_str <> "" then (
          let%lwt () = Lwt_io.write_line client.output_channel json_str in
          Logging.info_f ~section:"broadcast" "Sent current %s snapshot to client %d" stream_type client.id;
          Lwt.return_unit
        ) else (
          Logging.warn_f ~section:"broadcast" "No current snapshot available for stream type %s" stream_type;
          Lwt.return_unit
        )
      )
      (fun exn ->
        (* Handle EPIPE and other connection errors gracefully *)
        match exn with
        | Unix.Unix_error (Unix.EPIPE, _, _) ->
            Logging.debug_f ~section:"broadcast" "Client %d disconnected before snapshot sent (broken pipe)" client.id;
            client.active <- false;
            Lwt.return_unit
        | Unix.Unix_error (err, _, _) ->
            Logging.warn_f ~section:"broadcast" "Unix error sending snapshot to client %d: %s" client.id (Unix.error_message err);
            client.active <- false;
            Lwt.return_unit
        | _ ->
            Logging.warn_f ~section:"broadcast" "Failed to send current snapshot to client %d: %s" client.id (Printexc.to_string exn);
            client.active <- false;
            Lwt.return_unit
      )
  )

(** Handle TCP connection for a specific stream type *)
let handle_tcp_connection state stream_type client_id input_channel output_channel =
  let client = {
    id = client_id;
    stream_type;
    input_channel;
    output_channel;
    active = true;
  } in
  Mutex.lock state.client_mutex;
  Hashtbl.add state.clients client_id client;
  Mutex.unlock state.client_mutex;

  Logging.info_f ~section:"broadcast" "Client %d connected to %s stream" client_id stream_type;

  (* Send current snapshot immediately *)
  send_current_snapshot client stream_type;

  (* Handle client disconnection *)
  Lwt.catch
    (fun () ->
      (* Read from client until they disconnect *)
      let rec read_loop () =
        Lwt.catch
          (fun () ->
            let%lwt line = Lwt_io.read_line_opt input_channel in
            match line with
            | Some _ -> read_loop ()  (* Ignore any input from client *)
            | None ->
                (* Client disconnected normally *)
                client.active <- false;
                Logging.info_f ~section:"broadcast" "Client %d disconnected from %s stream" client_id stream_type;
                Lwt.return_unit
          )
          (fun exn ->
            (* Handle read errors *)
            match exn with
            | Unix.Unix_error (Unix.EPIPE, _, _) | Unix.Unix_error (Unix.ECONNRESET, _, _) ->
                client.active <- false;
                Logging.debug_f ~section:"broadcast" "Client %d disconnected (connection reset)" client_id;
                Lwt.return_unit
            | _ ->
                client.active <- false;
                Logging.warn_f ~section:"broadcast" "Error reading from client %d: %s" client_id (Printexc.to_string exn);
                Lwt.return_unit
          )
      in
      read_loop ()
    )
    (fun exn ->
      (* Catch-all for any other errors *)
      client.active <- false;
      Logging.warn_f ~section:"broadcast" "Client %d connection error: %s" client_id (Printexc.to_string exn);
      Lwt.return_unit
    )

(** Non-blocking stream consumer with backpressure detection and monitoring *)
let consume_stream_non_blocking stream_type stream process_fn state =
  let consecutive_timeouts = ref 0 in
  let last_success_time = ref (Unix.time ()) in
  let rec consumer_loop () =
    Lwt.catch
      (fun () ->
        (* Use get with timeout to prevent blocking *)
        Lwt.pick [
          (Lwt_stream.get stream >|= fun opt -> opt);
          (Lwt_unix.sleep 0.05 >|= fun () -> None)  (* 50ms timeout *)
        ] >>= fun opt ->
        match opt with
        | Some item ->
            consecutive_timeouts := 0;
            last_success_time := Unix.time ();
            (* Process the item asynchronously to avoid blocking the consumer *)
            Lwt.async (fun () ->
              Lwt.catch
                (fun () -> process_fn item state)
                (fun exn ->
                  Logging.warn_f ~section:"broadcast" "Error processing %s item: %s" stream_type (Printexc.to_string exn);
                  Lwt.return_unit
                )
            );
            consumer_loop ()
        | None ->
            (* No item available within timeout - track for backpressure detection *)
            let new_timeouts = !consecutive_timeouts + 1 in
            consecutive_timeouts := new_timeouts;
            let time_since_success = Unix.time () -. !last_success_time in

            (* Log backpressure warnings *)
            if new_timeouts = 10 then
              Logging.debug_f ~section:"broadcast" "%s stream: 10 consecutive timeouts, possible backpressure" stream_type
            else if new_timeouts mod 100 = 0 && new_timeouts > 0 then
              Logging.warn_f ~section:"broadcast" "%s stream: %d consecutive timeouts (%.1fs since last success), severe backpressure detected"
                stream_type new_timeouts time_since_success;

            consumer_loop ()
      )
      (fun exn ->
        Logging.warn_f ~section:"broadcast" "%s stream consumer failed: %s, restarting..." stream_type (Printexc.to_string exn);
        consecutive_timeouts := 0;  (* Reset on failure *)
        Lwt_unix.sleep 1.0 >>= consumer_loop  (* Restart after delay *)
      )
  in
  consumer_loop ()

(** Set up subscriptions to cache event buses and broadcast updates *)
let setup_broadcast_subscriptions state =
  (* Telemetry subscription *)
  Lwt.async (fun () ->
    Logging.info ~section:"broadcast" "Setting up telemetry broadcast subscription";
    let stream = subscribe_telemetry_snapshot () in
    Logging.info ~section:"broadcast" "Telemetry stream subscribed, waiting for data...";
    let process_telemetry snapshot state =
      Logging.info_f ~section:"broadcast" "Received telemetry snapshot with %d metrics, broadcasting..." (List.length snapshot.metrics);
      let json_str = Metrics_json.(telemetry_snapshot_to_json snapshot |> json_to_string) in
      broadcast_to_stream state "telemetry" json_str;
      Lwt.return_unit
    in
    consume_stream_non_blocking "telemetry" stream process_telemetry state
  );

  (* System stats subscription *)
  Lwt.async (fun () ->
    Logging.info ~section:"broadcast" "Setting up system stats broadcast subscription";
    let stream = subscribe_system_stats () in
    let process_system_stats stats state =
      let json_str = Metrics_json.(system_stats_to_json stats |> json_to_string) in
      broadcast_to_stream state "system" json_str;
      Lwt.return_unit
    in
    consume_stream_non_blocking "system" stream process_system_stats state
  );

  (* Balance subscription *)
  Lwt.async (fun () ->
    Logging.info ~section:"broadcast" "Setting up balance broadcast subscription";
    let stream = subscribe_balance_snapshot () in
    let process_balance snapshot state =
      let json_str = Metrics_json.(balance_snapshot_to_json snapshot |> json_to_string) in
      broadcast_to_stream state "balance" json_str;
      Lwt.return_unit
    in
    consume_stream_non_blocking "balance" stream process_balance state
  );

  (* Logs subscription *)
  Lwt.async (fun () ->
    let rec subscribe_logs () =
      Lwt.catch
        (fun () ->
          Logging.info ~section:"broadcast" "Setting up logs broadcast subscription";
          let stream = Dio_ui_cache.Logs_cache.subscribe_log_entries () in
          let process_log_entry log_entry state =
            let json_str = Metrics_json.(log_entry_to_json log_entry |> json_to_string) in
            broadcast_to_stream state "log" json_str;
            Lwt.return_unit
          in
          consume_stream_non_blocking "log" stream process_log_entry state
        )
        (fun exn ->
          Logging.warn_f ~section:"broadcast" "Logs subscription failed: %s, retrying..." (Printexc.to_string exn);
          Lwt_unix.sleep 1.0 >>= subscribe_logs  (* Retry after short delay *)
        )
    in
    subscribe_logs ()
  )

(** Convert IPv4 string to 32-bit integer *)
let ipv4_to_int ip_str =
  let parts = String.split_on_char '.' ip_str in
  match parts with
  | [a; b; c; d] ->
      let a = int_of_string a lsl 24 in
      let b = int_of_string b lsl 16 in
      let c = int_of_string c lsl 8 in
      let d = int_of_string d in
      a lor b lor c lor d
  | _ -> raise (Invalid_argument "Invalid IPv4 address")

(** Check if IP is within CIDR range *)
let ip_in_cidr ip_str cidr_str =
  try
    let parts = String.split_on_char '/' cidr_str in
    match parts with
    | [network; mask_str] ->
        let mask = int_of_string mask_str in
        let network_int = ipv4_to_int network in
        let ip_int = ipv4_to_int ip_str in
        let mask_int = lnot (1 lsl (32 - mask) - 1) in
        (network_int land mask_int) = (ip_int land mask_int)
    | _ -> false
  with _ -> false

(** Check if IP matches whitelist entry (IP, CIDR, or hostname pattern) *)
let ip_matches_entry client_ip client_hostname whitelist_entry =
  (* Check if it's a CIDR block *)
  if String.contains whitelist_entry '/' then
    ip_in_cidr client_ip whitelist_entry
  (* Check if it's an exact IP match *)
  else if client_ip = whitelist_entry then
    true
  (* Check if hostname matches (exact match or ends with entry) *)
  else
    client_hostname = whitelist_entry ||
    (String.length client_hostname > String.length whitelist_entry &&
     String.ends_with ~suffix:whitelist_entry client_hostname)

(** Get client IP and hostname from sockaddr *)
let get_client_info sockaddr =
  match sockaddr with
  | Unix.ADDR_INET (inet_addr, _) ->
      let ip_str = Unix.string_of_inet_addr inet_addr in
      (* Perform reverse DNS lookup synchronously *)
      let hostname = try
        let nameinfo = Unix.getnameinfo sockaddr [Unix.NI_NAMEREQD] in
        nameinfo.Unix.ni_hostname
      with _ ->
        (* If DNS lookup fails, use IP as hostname *)
        ip_str
      in
      Lwt.return (ip_str, hostname)
  | Unix.ADDR_UNIX _ ->
      (* Unix socket - shouldn't happen for TCP server *)
      Lwt.return ("127.0.0.1", "localhost")

(** Check if whitelist contains any hostname entries (not just IPs/CIDRs) *)
let whitelist_has_hostnames whitelist =
  List.exists (fun entry ->
    (* If it contains '/', it's a CIDR block *)
    if String.contains entry '/' then false
    (* Try to parse as IP - if it fails, assume it's a hostname *)
    else try ignore (ipv4_to_int entry); false with _ -> true
  ) whitelist

(** Check if client is allowed based on IP/hostname whitelist *)
let client_allowed sockaddr whitelist =
  (* Only do reverse DNS if whitelist contains hostname entries *)
  let needs_hostname = whitelist_has_hostnames whitelist in
  (if needs_hostname then
    get_client_info sockaddr
  else
    match sockaddr with
    | Unix.ADDR_INET (inet_addr, _) ->
        let ip_str = Unix.string_of_inet_addr inet_addr in
        Lwt.return (ip_str, ip_str)
    | Unix.ADDR_UNIX _ ->
        Lwt.return ("127.0.0.1", "localhost")
  ) >>= fun (client_ip, client_hostname) ->
  let allowed = List.exists (ip_matches_entry client_ip client_hostname) whitelist in
  if allowed then
    Lwt.return_unit
  else
    Lwt.fail_with (Printf.sprintf "Client IP %s (hostname: %s) not in whitelist" client_ip client_hostname)

(** Start the metrics broadcast server *)
let start_server (config : Dio_engine.Config.metrics_broadcast_config) : unit Lwt.t =
  Logging.info_f ~section:"broadcast" "Starting TCP metrics broadcast server on port %d" config.port;

  (* Log authentication configuration *)
  if config.enable_token_auth then
    Logging.info ~section:"broadcast" "Token authentication enabled"
  else
    Logging.info ~section:"broadcast" "Token authentication disabled";

  if config.enable_ip_whitelist then
    Logging.info ~section:"broadcast" "IP whitelist enabled"
  else
    Logging.info ~section:"broadcast" "IP whitelist disabled";

  let state = create_server_state config in

  (* Set up subscriptions to cache event buses *)
  setup_broadcast_subscriptions state;

  (* Start periodic cleanup of inactive clients and monitoring *)
  Lwt.async (fun () ->
    Logging.info ~section:"broadcast" "Starting periodic client cleanup task";
    let rec cleanup_loop () =
      let%lwt () = Lwt_unix.sleep 5.0 in (* Clean up every 5 seconds *)
      let before_cleanup = Hashtbl.length state.clients in
      cleanup_clients state;
      let after_cleanup = Hashtbl.length state.clients in
      let cleaned_count = before_cleanup - after_cleanup in

      (* Report metrics for monitoring *)
      if cleaned_count > 0 then
        Logging.info_f ~section:"broadcast" "Cleaned up %d inactive clients, %d total clients remaining" cleaned_count after_cleanup;

      (* Report active client counts *)
      let active_clients = ref 0 in
      Hashtbl.iter (fun _ client ->
        if client.active then incr active_clients
      ) state.clients;

      Logging.debug_f ~section:"broadcast" "Broadcast status: %d active clients, %d total clients"
        !active_clients (Hashtbl.length state.clients);

      cleanup_loop ()
    in
    cleanup_loop ()
  );

  (* Create TCP server *)
  let server_socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt server_socket Unix.SO_REUSEADDR true;

  let sockaddr = Unix.ADDR_INET (Unix.inet_addr_any, config.port) in
  Lwt_unix.bind server_socket sockaddr >>= fun () ->
  Lwt_unix.listen server_socket 10;

  Logging.info_f ~section:"broadcast" "TCP metrics broadcast server listening on port %d" config.port;
  Logging.info ~section:"broadcast" "Available streams (connect and send stream name):";
  Logging.info ~section:"broadcast" "  telemetry - Telemetry metrics";
  Logging.info ~section:"broadcast" "  system - System statistics";
  Logging.info ~section:"broadcast" "  balance - Balance and orders";
  Logging.info ~section:"broadcast" "  log - Log entries";

  (* Accept connections in a loop *)
  let rec accept_loop () =
    Lwt_unix.accept server_socket >>= fun (client_socket, sockaddr) ->
    Lwt.async (fun () ->
      Lwt.catch
        (fun () ->
          let input_channel = Lwt_io.of_fd ~mode:Lwt_io.input client_socket in
          let output_channel = Lwt_io.of_fd ~mode:Lwt_io.output client_socket in

          (* Step 1: Check IP whitelist if enabled *)
          (match state.ip_whitelist with
           | Some whitelist when state.enable_ip_whitelist ->
               client_allowed sockaddr whitelist >>= fun () ->
               Lwt.return_unit
           | _ -> Lwt.return_unit
          ) >>= fun () ->

          (* Step 2: Read first line - token if enabled, otherwise stream type *)
          Lwt_io.read_line_opt input_channel >>= fun line_opt ->
          (match line_opt with
           | Some first_line ->
               let trimmed_line = String.trim first_line in

               (* Check if this is an HTTP request *)
               if String.starts_with ~prefix:"GET" trimmed_line || String.starts_with ~prefix:"POST" trimmed_line then (
                 Logging.warn_f ~section:"broadcast" "Received HTTP request, this is a TCP-only server. Use: echo 'telemetry' | nc localhost %d" state.port;
                 let http_response = Printf.sprintf "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\n\r\nThis server accepts TCP connections only.\nConnect and send one of: telemetry, system, balance, log\n" in
                 Lwt_io.write output_channel http_response >>= fun () ->
                 safe_close_channels input_channel output_channel
               ) else (
                 (* Step 3: Handle authentication and stream type *)
                 if state.enable_token_auth then (
                   (* Token auth enabled - first line should be token *)
                   match state.token with
                   | Some expected_token ->
                       if trimmed_line = expected_token then (
                         (* Token valid, read stream type *)
                         Lwt_io.read_line_opt input_channel >>= fun stream_line_opt ->
                         match stream_line_opt with
                         | Some stream_line ->
                             let stream_type = String.trim stream_line in
                             (* Check if it's a valid stream type *)
                             (match stream_type with
                              | "telemetry" | "system" | "balance" | "log" ->
                                  let client_id = Atomic.get state.next_client_id in
                                  Atomic.incr state.next_client_id;
                                  handle_tcp_connection state stream_type client_id input_channel output_channel
                              | _ ->
                                  Logging.warn_f ~section:"broadcast" "Unknown stream type requested: %s" stream_type;
                                  let error_msg = Printf.sprintf "Unknown stream type: %s\nValid types: telemetry, system, balance, log\n" stream_type in
                                  Lwt_io.write output_channel error_msg >>= fun () ->
                                  safe_close_channels input_channel output_channel
                             )
                         | None ->
                             Logging.warn ~section:"broadcast" "Client disconnected before sending stream type";
                             safe_close_channels input_channel output_channel
                       ) else (
                         Logging.warn_f ~section:"broadcast" "Invalid token provided by client";
                         let error_msg = "Invalid token\n" in
                         Lwt_io.write output_channel error_msg >>= fun () ->
                         safe_close_channels input_channel output_channel
                       )
                   | None ->
                       Logging.error ~section:"broadcast" "Token auth enabled but no token configured - this should not happen";
                       safe_close_channels input_channel output_channel
                 ) else (
                   (* Token auth disabled - first line should be stream type *)
                   let stream_type = trimmed_line in
                   (* Check if it's a valid stream type *)
                   match stream_type with
                   | "telemetry" | "system" | "balance" | "log" ->
                       let client_id = Atomic.get state.next_client_id in
                       Atomic.incr state.next_client_id;
                       handle_tcp_connection state stream_type client_id input_channel output_channel
                   | _ ->
                       Logging.warn_f ~section:"broadcast" "Unknown stream type requested: %s" stream_type;
                       let error_msg = Printf.sprintf "Unknown stream type: %s\nValid types: telemetry, system, balance, log\n" stream_type in
                       Lwt_io.write output_channel error_msg >>= fun () ->
                       safe_close_channels input_channel output_channel
                 )
               )
           | None ->
               let msg = if state.enable_token_auth then "token" else "stream type" in
               Logging.warn_f ~section:"broadcast" "Client disconnected before sending %s" msg;
               safe_close_channels input_channel output_channel
          )
        )
        (fun exn ->
          (* Handle any errors during connection setup *)
          match exn with
          | Failure msg when String.starts_with ~prefix:"Client IP" msg ->
              (* IP whitelist rejection *)
              Logging.warn_f ~section:"broadcast" "Connection rejected: %s" msg;
              let error_msg = Printf.sprintf "Access denied: %s\n" msg in
              let input_channel = Lwt_io.of_fd ~mode:Lwt_io.input client_socket in
              let output_channel = Lwt_io.of_fd ~mode:Lwt_io.output client_socket in
              Lwt_io.write output_channel error_msg >>= fun () ->
              safe_close_channels input_channel output_channel
          | _ ->
              Logging.warn_f ~section:"broadcast" "Error handling connection: %s" (Printexc.to_string exn);
              Lwt.return_unit
        )
    );
    accept_loop ()
  in

  accept_loop ()
