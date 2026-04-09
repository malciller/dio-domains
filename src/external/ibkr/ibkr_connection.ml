(** TCP socket connection to IB Gateway.

    Manages the low-level transport: TCP connect, TWS API handshake,
    message framing (read/write), and automatic reconnection with
    exponential backoff when the gateway restarts (IBC handles daily
    re-authentication). 

    Single-writer, single-reader design using Lwt cooperative threads.
    The reader loop runs as a background Lwt async thread, dispatching
    decoded messages to a caller-provided callback. *)

open Lwt.Infix

let section = "ibkr_connection"

(** Connection state. *)
type t = {
  mutable socket: Lwt_unix.file_descr option;
  mutable ic: Lwt_io.input_channel option;
  mutable oc: Lwt_io.output_channel option;
  mutable server_version: int;
  mutable next_order_id: int;
  mutable account_id: string;
  mutable connected: bool;
  host: string;
  port: int;
  client_id: int;
  write_mutex: Lwt_mutex.t;
}

(** Create a new (disconnected) connection handle. *)
let create ~host ~port ~client_id =
  {
    socket = None;
    ic = None;
    oc = None;
    server_version = 0;
    next_order_id = 0;
    account_id = "";
    connected = false;
    host;
    port;
    client_id;
    write_mutex = Lwt_mutex.create ();
  }

(** [is_connected t] returns [true] if the connection is established. *)
let is_connected t = t.connected

(** [get_next_order_id t] returns and increments the next valid order ID. *)
let get_next_order_id t =
  let id = t.next_order_id in
  t.next_order_id <- id + 1;
  id

(** [get_account_id t] returns the account identifier received at login. *)
let get_account_id t = t.account_id

(** [get_server_version t] returns the TWS server version from handshake. *)
let get_server_version t = t.server_version

(* ---- Low-level IO ---- *)

(** Pre-allocated 4-byte buffer for reading length prefixes.
    Eliminates a Bytes.create(4) per inbound message.
    Safe: single-reader architecture guarantees sequential access. *)
let length_buf = Bytes.create 4

(** Growable reusable buffer for reading message payloads.
    Avoids allocating a fresh Bytes.t per inbound message. Only
    reallocates when a message exceeds the current capacity.
    Safe: decode_fields copies data into new strings before we reuse. *)
let msg_buf = ref (Bytes.create 4096)

(** Read exactly [n] bytes from the input channel into a provided buffer. *)
let read_bytes_into ic buf n =
  Lwt_io.read_into_exactly ic buf 0 n

(** Read a 4-byte big-endian length prefix using the pre-allocated buffer. *)
let read_length ic =
  read_bytes_into ic length_buf 4 >|= fun () ->
  (Bytes.get_uint8 length_buf 0 lsl 24) lor
  (Bytes.get_uint8 length_buf 1 lsl 16) lor
  (Bytes.get_uint8 length_buf 2 lsl 8) lor
  (Bytes.get_uint8 length_buf 3)

(** Read a single length-prefixed message and decode its fields.
    Uses a growable reusable buffer to minimize allocations. *)
let read_message ic =
  read_length ic >>= fun len ->
  if len <= 0 || len > 1_000_000 then
    Lwt.fail_with (Printf.sprintf "Invalid message length: %d" len)
  else begin
    (* Grow the reusable buffer if needed *)
    if Bytes.length !msg_buf < len then
      msg_buf := Bytes.create (len * 2);
    read_bytes_into ic !msg_buf len >|= fun () ->
    Ibkr_codec.decode_fields (Bytes.sub_string !msg_buf 0 len)
  end

(** Write raw bytes to the output channel under the write mutex. *)
let write_raw t bytes =
  match t.oc with
  | Some oc ->
      Lwt_mutex.with_lock t.write_mutex (fun () ->
        Lwt_io.write_from_exactly oc bytes 0 (Bytes.length bytes) >>= fun () ->
        Lwt_io.flush oc
      )
  | None ->
      Logging.error ~section "Cannot write: not connected";
      Lwt.return_unit

(** Send a message as a list of string fields. *)
let send t (fields : string list) =
  write_raw t (Ibkr_codec.encode_fields fields)

(* ---- Handshake ---- *)

(** Perform the TWS API handshake:
    1. Send "API\x00" + version range
    2. Read server version + connection time
    3. Send startApi with client ID *)
let handshake t =
  let ic = match t.ic with Some ic -> ic | None -> failwith "Not connected" in
  let handshake_bytes = Ibkr_codec.encode_handshake
    ~min_ver:Ibkr_types.api_version_min
    ~max_ver:Ibkr_types.api_version_max
  in
  write_raw t handshake_bytes >>= fun () ->

  (* Read server response: server_version and connection_time come as
     the first two null-delimited fields in a length-prefixed message. *)
  read_message ic >>= fun fields ->
  let server_version, fields = Ibkr_codec.read_int fields in
  let connection_time, _fields = Ibkr_codec.read_string fields in
  t.server_version <- server_version;
  Logging.info_f ~section "Connected to IB Gateway: server_version=%d time=%s"
    server_version connection_time;

  (* Send startApi message *)
  let start_api_fields = [
    string_of_int Ibkr_types.msg_start_api;
    "2";    (* version *)
    string_of_int t.client_id;
    "";     (* optionalCapabilities *)
  ] in
  send t start_api_fields

(* ---- Connection lifecycle ---- *)

(** Establish a TCP connection and perform the handshake.
    Does NOT start the reader loop — call [start_reader] separately. *)
let connect t =
  Logging.info_f ~section "Connecting to IB Gateway at %s:%d (clientId=%d)"
    t.host t.port t.client_id;

  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string t.host, t.port) in
  let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt.catch (fun () ->
    Lwt_unix.connect fd addr >>= fun () ->
    let ic = Lwt_io.of_fd ~mode:Lwt_io.input fd in
    let oc = Lwt_io.of_fd ~mode:Lwt_io.output fd in
    t.socket <- Some fd;
    t.ic <- Some ic;
    t.oc <- Some oc;
    t.connected <- true;
    handshake t >>= fun () ->
    Logging.info ~section "Handshake complete, awaiting nextValidId";
    Lwt.return_unit
  ) (fun exn ->
    Logging.error_f ~section "Connection failed: %s" (Printexc.to_string exn);
    (Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit)) >>= fun () ->
    t.socket <- None;
    t.ic <- None;
    t.oc <- None;
    t.connected <- false;
    Lwt.fail exn
  )

(** Close the TCP connection cleanly. *)
let disconnect t =
  t.connected <- false;
  let close_fd = match t.socket with
    | Some fd ->
        t.socket <- None;
        t.ic <- None;
        t.oc <- None;
        Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit)
    | None -> Lwt.return_unit
  in
  close_fd

(** Start the background reader loop. Reads messages from the socket and
    dispatches each to [on_message ~msg_id ~fields]. The loop exits on
    socket EOF or error, invoking [on_disconnect] with a reason string. *)
let start_reader t ~on_message ~on_disconnect =
  let rec loop ic =
    Lwt.catch (fun () ->
      read_message ic >>= fun fields ->
      match fields with
      | [] ->
          Logging.warn ~section "Received empty message, skipping";
          loop ic
      | msg_id_str :: rest ->
          let msg_id = try int_of_string msg_id_str with _ -> -1 in
          (Lwt.catch
            (fun () -> on_message ~msg_id ~fields:rest; Lwt.return_unit)
            (fun exn ->
              Logging.error_f ~section "Handler error for msg_id=%d: %s"
                msg_id (Printexc.to_string exn);
              Lwt.return_unit)
          ) >>= fun () ->
          loop ic
    ) (function
      | End_of_file ->
          Logging.warn ~section "Connection closed by gateway (EOF)";
          t.connected <- false;
          on_disconnect "Connection closed by gateway (EOF)";
          Lwt.return_unit
      | exn ->
          Logging.error_f ~section "Reader error: %s" (Printexc.to_string exn);
          t.connected <- false;
          on_disconnect (Printf.sprintf "Reader error: %s" (Printexc.to_string exn));
          Lwt.return_unit
    )
  in
  match t.ic with
  | Some ic -> Lwt.async (fun () -> loop ic)
  | None -> Logging.error ~section "Cannot start reader: not connected"

(** Connect with exponential backoff retry. Attempts up to [max_attempts]
    times before giving up. *)
let connect_with_retry t ~max_attempts =
  let base_delay = Ibkr_types.default_reconnect_base_delay_ms /. 1000.0 in
  let max_delay = Ibkr_types.default_reconnect_max_delay_ms /. 1000.0 in
  let backoff = Ibkr_types.default_reconnect_backoff_factor in
  let rec attempt n delay =
    if n > max_attempts then begin
      Logging.error_f ~section "Failed to connect after %d attempts" max_attempts;
      Lwt.fail_with "Max reconnection attempts exceeded"
    end else begin
      Logging.info_f ~section "Connection attempt %d/%d" n max_attempts;
      Lwt.catch (fun () -> connect t)
        (fun exn ->
          Logging.warn_f ~section "Attempt %d failed: %s" n (Printexc.to_string exn);
          let next_delay = Float.min (delay *. backoff) max_delay in
          Logging.info_f ~section "Retrying in %.1fs" delay;
          Lwt_unix.sleep delay >>= fun () ->
          attempt (n + 1) next_delay
        )
    end
  in
  attempt 1 base_delay
