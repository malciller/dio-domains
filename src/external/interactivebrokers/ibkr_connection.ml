(** Advanced TCP socket multiplexer allocated for Interactive Brokers Gateway communication.

    Administrates the low level Unix transport layer including TCP binding operations, the
    synchronous TWS API initialization handshake protocol, binary payload framing mechanics,
    and automatic socket recovery loops parameterized with exponential backoff logic during
    unexpected daemon restarts.

    Implements a strict single writer and single reader cooperative concurrent design
    leveraging the Lwt threading engine. The stream consumption loop inherently runs as localized
    background Lwt async threads, routing the demultiplexed field vectors into the system
    event registry callback target. *)

open Lwt.Infix

let section = "ibkr_connection"

(** Primary state container for the Interactive Brokers TCP connection.
    Encapsulates socket descriptors, IO channels, protocol version metrics,
    and mutual exclusion primitives for thread safe transmission. *)
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

(** Initializes an unlinked connection data structure.
    Prepares the struct with the specified target IP address, port configuration,
    and client identification integer prior to socket allocation. *)
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

(** Evaluates the boolean connection status indicator.
    Returns true strictly when the TCP socket is actively bound and the TWS handshake is complete. *)
let is_connected t = t.connected

(** Retrieves and atomically increments the local order sequence generator.
    The starting integer is initially supplied by the callback during the handshake sequence. *)
let get_next_order_id t =
  let id = t.next_order_id in
  t.next_order_id <- id + 1;
  id

(** Retrieves the alphanumeric account identifier cached from the managed accounts response message. *)
let get_account_id t = t.account_id

(** Returns the TWS API server version integer dictated by the server during the initial protocol negotiation. *)
let get_server_version t = t.server_version

(* ---- Low-level IO ---- *)

(** Static 4 byte buffer allocated for consuming message length prefixes.
    Designed to prevent constant byte string allocations on high frequency message reception.
    Thread safety relies entirely on the strictly sequential Lwt read stream. *)
let length_buf = Bytes.create 4

(** Mutable referenced buffer designated for reading the raw message payloads.
    Functions as a memory pool to minimize garbage collection latency, reallocating
    only when incoming payload frames exceed the current byte array capacity.
    Memory safety is ensured because decode_fields performs eager string copies. *)
let msg_buf = ref (Bytes.create 4096)

(** Block on the asynchronous input channel until the exact byte count is deposited into the buffer. *)
let read_bytes_into ic buf n =
  Lwt_io.read_into_exactly ic buf 0 n

(** Consumes exactly four bytes from the input stream to construct the big endian integer payload size. *)
let read_length ic =
  read_bytes_into ic length_buf 4 >|= fun () ->
  (Bytes.get_uint8 length_buf 0 lsl 24) lor
  (Bytes.get_uint8 length_buf 1 lsl 16) lor
  (Bytes.get_uint8 length_buf 2 lsl 8) lor
  (Bytes.get_uint8 length_buf 3)

(** Orchestrates the reading of a length prefixed frame followed by the payload extraction.
    Interacts with the reusable payload buffer to prevent allocation spikes before passing to the codec. *)
let read_message ic =
  read_length ic >>= fun len ->
  if len <= 0 || len > 1_000_000 then
    Lwt.fail_with (Printf.sprintf "Invalid message length: %d" len)
  else begin
    (* Perform dynamic reallocation if the payload exceeds buffer capacity *)
    if Bytes.length !msg_buf < len then
      msg_buf := Bytes.create (len * 2);
    read_bytes_into ic !msg_buf len >|= fun () ->
    Ibkr_codec.decode_fields (Bytes.sub_string !msg_buf 0 len)
  end

(** Dispatches raw byte sequences to the outbound stream socket.
    Implements a strict mutual exclusion lock to guarantee that interleaved writes do not corrupt the frame. *)
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

(** Serializes a list of string fields into a null delimited byte sequence and executes an asynchronous locked write. *)
let send t (fields : string list) =
  write_raw t (Ibkr_codec.encode_fields fields)

(* ---- Handshake ---- *)

(** Executes the synchronous TWS API connection authentication protocol.
    Step 1. Transmits the API header and the supported minimum and maximum version range.
    Step 2. Awaits the server response containing the negotiated version and system time.
    Step 3. Transmits the startApi payload including the configured integer client identifier. *)
let handshake t =
  let ic = match t.ic with Some ic -> ic | None -> failwith "Not connected" in
  let handshake_bytes = Ibkr_codec.encode_handshake
    ~min_ver:Ibkr_types.api_version_min
    ~max_ver:Ibkr_types.api_version_max
  in
  write_raw t handshake_bytes >>= fun () ->

  (* Extract server details where the version integer and connection time string
     constitute the first two null delimited sections inside the length prefixed envelope. *)
  read_message ic >>= fun fields ->
  let server_version, fields = Ibkr_codec.read_int fields in
  let connection_time, _fields = Ibkr_codec.read_string fields in
  t.server_version <- server_version;
  Logging.info_f ~section "Connected to IB Gateway: server_version=%d time=%s"
    server_version connection_time;

  (* Dispatch the startApi payload *)
  let start_api_fields = [
    string_of_int Ibkr_types.msg_start_api;
    "2";    (* version *)
    string_of_int t.client_id;
    "";     (* optionalCapabilities *)
  ] in
  send t start_api_fields

(* ---- Connection lifecycle ---- *)

(** Initiates the TCP socket binding sequence and proceeds with the synchronous API handshake protocol.
    Note that this function explicitly avoids spawning the continuous reader thread, which must be invoked independently. *)
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

(** Triggers a graceful teardown of the network connections.
    Flags the session boolean false and instructs the input output channels and socket descriptors to terminate inline. *)
let disconnect t =
  t.connected <- false;
  let close_ic = match t.ic with
    | Some ic ->
        t.ic <- None;
        Lwt.catch (fun () -> Lwt_io.close ic) (fun _ -> Lwt.return_unit)
    | None -> Lwt.return_unit
  in
  let close_oc = match t.oc with
    | Some oc ->
        t.oc <- None;
        Lwt.catch (fun () -> Lwt_io.close oc) (fun _ -> Lwt.return_unit)
    | None -> Lwt.return_unit
  in
  let close_fd = match t.socket with
    | Some fd ->
        t.socket <- None;
        Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit)
    | None -> Lwt.return_unit
  in
  Lwt.join [close_ic; close_oc; close_fd]

(** Spawns the central background consumer stream for incoming socket data.
    Decodes packets and transfers them directly to the provided message callback router.
    Gracefully exits on socket exhaustion or internal error states, terminating via the disconnect subroutine.

    Crucially depends on Lwt async delegation to isolate each processing step, thus severing
    the consecutive promise linkage pattern prescribed in the Lwt utilities module.
    Failing to break this promise sequence would inevitably result in severe Forward node leakage
    for every message ingested throughout the runtime. *)
let start_reader t ~on_message ~on_disconnect =
  match t.ic with
  | None -> Logging.error ~section "Cannot start reader: not connected"
  | Some ic ->
      let stream = Lwt_stream.from (fun () ->
        if not t.connected then Lwt.return_none
        else
        Lwt.catch (fun () ->
          read_message ic >>= fun fields -> Lwt.return_some fields
        ) (function
          | End_of_file -> Lwt.return_none
          | Unix.Unix_error(Unix.EBADF, _, _) -> Lwt.return_none
          | Lwt_io.Channel_closed _ -> Lwt.return_none
          | exn -> Lwt.fail exn)
      ) in
      let process_fields fields =
        match fields with
        | [] -> Logging.warn ~section "Received empty message, skipping"
        | msg_id_str :: rest ->
            let msg_id = try int_of_string msg_id_str with _ -> -1 in
            Lwt.async (fun () ->
              Lwt.catch
                (fun () -> on_message ~msg_id ~fields:rest; Lwt.return_unit)
                (fun exn ->
                  Logging.error_f ~section "Handler error for msg_id=%d: %s"
                    msg_id (Printexc.to_string exn);
                  Lwt.return_unit)
            )
      in
      Lwt.async (fun () ->
        Lwt.catch (fun () ->
          Concurrency.Lwt_util.consume_stream process_fields stream >>= fun () ->
          Logging.warn ~section "Connection closed by gateway (EOF)";
          t.connected <- false;
          on_disconnect "Connection closed by gateway (EOF)";
          Lwt.return_unit
        ) (fun exn ->
          Logging.error_f ~section "Reader error: %s" (Printexc.to_string exn);
          t.connected <- false;
          on_disconnect (Printf.sprintf "Reader error: %s" (Printexc.to_string exn));
          Lwt.return_unit
        )
      )

(** Executes the connection initialization with exponential backoff fault tolerance.
    Continuously loops until the maximum attempt threshold is breached or a stable TCP port is acquired. *)
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
