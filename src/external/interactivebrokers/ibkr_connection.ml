(** TCP transport for the IB Gateway connection.

    Owns the Unix socket, the TWS handshake, length-prefixed frame
    reading/writing, and reconnection with exponential backoff.
    Single writer, single reader under Lwt; the reader loop runs in the
    background and feeds decoded field lists to a callback. *)

open Lwt.Infix

let section = "ibkr_connection"

(** Connection state: socket, IO channels, negotiated server version,
    order id counter, and the write mutex. *)
type t =
  { mutable socket : Lwt_unix.file_descr option
  ; mutable ic : Lwt_io.input_channel option
  ; mutable oc : Lwt_io.output_channel option
  ; mutable server_version : int
  ; mutable next_order_id : int
  ; mutable account_id : string
  ; mutable connected : bool
  ; host : string
  ; port : int
  ; client_id : int
  ; write_mutex : Lwt_mutex.t
  }

(** Builds a connection record for [host]:[port] with the given client
    id; no socket is opened. *)
let create ~host ~port ~client_id =
  { socket = None
  ; ic = None
  ; oc = None
  ; server_version = 0
  ; next_order_id = 0
  ; account_id = ""
  ; connected = false
  ; host
  ; port
  ; client_id
  ; write_mutex = Lwt_mutex.create ()
  }
;;

(** Connection status: true once the TCP socket is bound and the TWS
    handshake is complete. *)
let is_connected t = t.connected

(** Returns the next order id, incrementing the local counter.
    The starting value is supplied by TWS during the handshake. Single
    threaded use only: no synchronization here. *)
let get_next_order_id t =
  let id = t.next_order_id in
  t.next_order_id <- id + 1;
  id
;;

(** Account id cached from the managedAccounts message. *)
let get_account_id t = t.account_id

(** Server version negotiated during the handshake. *)
let get_server_version t = t.server_version

(* ---- Low-level IO ---- *)

(** Shared 4-byte buffer for length prefixes. Safe because reads are
    strictly sequential on the Lwt stream. *)
let length_buf = Bytes.create 4

(** Growable payload buffer, reallocated when a frame exceeds capacity.
    Safe because [decode_fields] copies eagerly. *)
let msg_buf = ref (Bytes.create 4096)

(** Reads exactly [n] bytes into [buf]. *)
let read_bytes_into ic buf n = Lwt_io.read_into_exactly ic buf 0 n

(** Reads the 4-byte big-endian length prefix. *)
let read_length ic =
  read_bytes_into ic length_buf 4
  >|= fun () ->
  (Bytes.get_uint8 length_buf 0 lsl 24)
  lor (Bytes.get_uint8 length_buf 1 lsl 16)
  lor (Bytes.get_uint8 length_buf 2 lsl 8)
  lor Bytes.get_uint8 length_buf 3
;;

(** Reads one length-prefixed frame into the reusable buffer and decodes
    it. Rejects lengths outside (0, 1_000_000]. *)
let read_message ic =
  read_length ic
  >>= fun len ->
  if len <= 0 || len > 1_000_000
  then Lwt.fail_with (Printf.sprintf "Invalid message length: %d" len)
  else (
    (* Grow the payload buffer if needed *)
    if Bytes.length !msg_buf < len then msg_buf := Bytes.create (len * 2);
    read_bytes_into ic !msg_buf len
    >|= fun () -> Ibkr_codec.decode_fields (Bytes.sub_string !msg_buf 0 len))
;;

(** Writes raw bytes under the write mutex so frames are not
    interleaved. No-op with an error log when disconnected. *)
let write_raw t bytes =
  match t.oc with
  | Some oc ->
    Lwt_mutex.with_lock t.write_mutex (fun () ->
      Lwt_io.write_from_exactly oc bytes 0 (Bytes.length bytes)
      >>= fun () -> Lwt_io.flush oc)
  | None ->
    Logging.error ~section "Cannot write: not connected";
    Lwt.return_unit
;;

(** Encodes [fields] and writes the frame. *)
let send t (fields : string list) = write_raw t (Ibkr_codec.encode_fields fields)

(* ---- Handshake ---- *)

(** TWS handshake:
    1. Send "API\0" plus supported version range.
    2. Receive server version and connection time.
    3. Send startApi with the client id. *)
let handshake t =
  let ic =
    match t.ic with
    | Some ic -> ic
    | None -> failwith "Not connected"
  in
  let handshake_bytes =
    Ibkr_codec.encode_handshake
      ~min_ver:Ibkr_types.api_version_min
      ~max_ver:Ibkr_types.api_version_max
  in
  write_raw t handshake_bytes
  >>= fun () ->
  (* First two fields in the response envelope: version, connection time. *)
  read_message ic
  >>= fun fields ->
  let server_version, fields = Ibkr_codec.read_int fields in
  let connection_time, _fields = Ibkr_codec.read_string fields in
  t.server_version <- server_version;
  Logging.info_f
    ~section
    "Connected to IB Gateway: server_version=%d time=%s"
    server_version
    connection_time;
  let start_api_fields =
    [ string_of_int Ibkr_types.msg_start_api
    ; "2" (* version *)
    ; string_of_int t.client_id
    ; "" (* optionalCapabilities *)
    ]
  in
  send t start_api_fields
;;

(* ---- Connection lifecycle ---- *)

(** Connects the socket and runs the handshake. Does not start the
    reader loop; call [start_reader] separately. On failure the socket is
    closed and the exception re-raised. *)
let connect t =
  Logging.info_f
    ~section
    "Connecting to IB Gateway at %s:%d (clientId=%d)"
    t.host
    t.port
    t.client_id;
  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string t.host, t.port) in
  let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt.catch
    (fun () ->
       Lwt_unix.connect fd addr
       >>= fun () ->
       let ic = Lwt_io.of_fd ~mode:Lwt_io.input fd in
       let oc = Lwt_io.of_fd ~mode:Lwt_io.output fd in
       t.socket <- Some fd;
       t.ic <- Some ic;
       t.oc <- Some oc;
       t.connected <- true;
       handshake t
       >>= fun () ->
       Logging.info ~section "Handshake complete, awaiting nextValidId";
       Lwt.return_unit)
    (fun exn ->
       Logging.error_f ~section "Connection failed: %s" (Printexc.to_string exn);
       Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit)
       >>= fun () ->
       t.socket <- None;
       t.ic <- None;
       t.oc <- None;
       t.connected <- false;
       Lwt.fail exn)
;;

(** Closes the IO channels and socket and marks the connection down.
    Channel/socket cleanup errors are swallowed. *)
let disconnect t =
  t.connected <- false;
  let close_ic =
    match t.ic with
    | Some ic ->
      t.ic <- None;
      Lwt.catch (fun () -> Lwt_io.close ic) (fun _ -> Lwt.return_unit)
    | None -> Lwt.return_unit
  in
  let close_oc =
    match t.oc with
    | Some oc ->
      t.oc <- None;
      Lwt.catch (fun () -> Lwt_io.close oc) (fun _ -> Lwt.return_unit)
    | None -> Lwt.return_unit
  in
  let close_fd =
    match t.socket with
    | Some fd ->
      t.socket <- None;
      Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit)
    | None -> Lwt.return_unit
  in
  Lwt.join [ close_ic; close_oc; close_fd ]
;;

(** Background reader loop. Decodes each frame and invokes [on_message]
    with the message id and remaining fields; handler exceptions are
    logged, not fatal. On EOF, EBADF, or a closed channel the loop ends
    quietly; on other errors it logs and ends. In both cases it clears
    [t.connected] and invokes the caller-supplied [on_disconnect]; it
    never calls [disconnect].

    Per-message handlers run under [Lwt.async] so they are not chained
    onto the stream-consumption promise. Chaining them would make the
    reader await every handler before pulling the next frame and retain
    one pending promise per message for as long as the reader promise is
    awaited. *)
let start_reader t ~on_message ~on_disconnect =
  match t.ic with
  | None -> Logging.error ~section "Cannot start reader: not connected"
  | Some ic ->
    let stream =
      Lwt_stream.from (fun () ->
        if not t.connected
        then Lwt.return_none
        else
          Lwt.catch
            (fun () -> read_message ic >>= fun fields -> Lwt.return_some fields)
            (function
              | End_of_file -> Lwt.return_none
              | Unix.Unix_error (Unix.EBADF, _, _) -> Lwt.return_none
              | Lwt_io.Channel_closed _ -> Lwt.return_none
              | exn -> Lwt.fail exn))
    in
    let process_fields fields =
      match fields with
      | [] -> Logging.warn ~section "Received empty message, skipping"
      | msg_id_str :: rest ->
        let msg_id =
          try int_of_string msg_id_str with
          | _ -> -1
        in
        Lwt.async (fun () ->
          Lwt.catch
            (fun () ->
               on_message ~msg_id ~fields:rest;
               Lwt.return_unit)
            (fun exn ->
               Logging.error_f
                 ~section
                 "Handler error for msg_id=%d: %s"
                 msg_id
                 (Printexc.to_string exn);
               Lwt.return_unit))
    in
    Lwt.async (fun () ->
      Lwt.catch
        (fun () ->
           Concurrency.Lwt_util.consume_stream process_fields stream
           >>= fun () ->
           Logging.warn ~section "Connection closed by gateway (EOF)";
           t.connected <- false;
           on_disconnect "Connection closed by gateway (EOF)";
           Lwt.return_unit)
        (fun exn ->
           Logging.error_f ~section "Reader error: %s" (Printexc.to_string exn);
           t.connected <- false;
           on_disconnect (Printf.sprintf "Reader error: %s" (Printexc.to_string exn));
           Lwt.return_unit))
;;

(** Retries [connect] with exponential backoff until it succeeds or
    [max_attempts] is exhausted, then fails. *)
let connect_with_retry t ~max_attempts =
  let base_delay = Ibkr_types.default_reconnect_base_delay_ms /. 1000.0 in
  let max_delay = Ibkr_types.default_reconnect_max_delay_ms /. 1000.0 in
  let backoff = Ibkr_types.default_reconnect_backoff_factor in
  let rec attempt n delay =
    if n > max_attempts
    then (
      Logging.error_f ~section "Failed to connect after %d attempts" max_attempts;
      Lwt.fail_with "Max reconnection attempts exceeded")
    else (
      Logging.info_f ~section "Connection attempt %d/%d" n max_attempts;
      Lwt.catch
        (fun () -> connect t)
        (fun exn ->
           Logging.warn_f ~section "Attempt %d failed: %s" n (Printexc.to_string exn);
           let next_delay = Float.min (delay *. backoff) max_delay in
           Logging.info_f ~section "Retrying in %.1fs" delay;
           Lwt_unix.sleep delay >>= fun () -> attempt (n + 1) next_delay))
  in
  attempt 1 base_delay
;;
