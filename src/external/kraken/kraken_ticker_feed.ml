(**
   Kraken WebSocket v2 ticker feed.
   Subscribes to real-time top-of-book bid/ask, last price, and rolling
   24h volume for configured trading pairs. Ticker snapshots are written
   to per-symbol lock-free ring buffers. Readiness is signalled via
   [Lwt_condition] and per-symbol [Exchange_wakeup] events to notify
   waiting domains without polling.
*)
open Lwt.Infix

let section = "kraken_ticker"

(** Obtain a TLS-capable Conduit context, delegated to [Kraken_common_types]. *)
let get_conduit_ctx = Kraken_common_types.get_conduit_ctx

(** Snapshot of ticker state for a single symbol at a point in time. *)
type ticker = {
  symbol: string;
  bid: float;
  ask: float;
  last: float;
  volume: float;
  timestamp: float;
}

(** Lock-free ring buffer shared with other feeds via [Concurrency.Ring_buffer]. *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol store pairing a ring buffer with an atomic readiness flag. *)
type store = {
  buffer: ticker RingBuffer.t;
  ready: bool Atomic.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()

let ensure_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      let store = {
        buffer = RingBuffer.create 100;
        ready = Atomic.make false;
      } in
      Hashtbl.add stores symbol store;
      store

let store_opt symbol = Hashtbl.find_opt stores symbol

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try
      Lwt_condition.broadcast ready_condition ()
    with _ ->
      (* Broadcast may raise if waiters were cancelled; suppress safely. *)
      ())
  end

(** Return the most recent ticker snapshot for [symbol], or [None] if unavailable. Hot path; always inlined. *)
let[@inline always] get_latest_ticker symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None

(** Return the bid/ask midpoint for [symbol], or [None] if no ticker exists. Hot path; always inlined. *)
let[@inline always] get_latest_price symbol =
  match get_latest_ticker symbol with
  | None -> None
  | Some ticker -> Some ((ticker.bid +. ticker.ask) /. 2.0)

(** Return all ticker snapshots written after [last_pos]. Allocates a list; use [iter_ticker_events] on hot paths. *)
let[@inline always] read_ticker_events symbol last_pos =
  match store_opt symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

(** Iterate [f] over ticker snapshots written after [last_pos] without allocating a list. Returns the new read position. *)
let[@inline always] iter_ticker_events symbol last_pos f =
  match store_opt symbol with
  | Some store -> RingBuffer.iter_since store.buffer last_pos f
  | None -> last_pos

(** Return the current write position of the ring buffer for [symbol]. Used by consumers to initialize or checkpoint their read cursor. *)
let[@inline always] get_current_position symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

(** Return [true] if the store for [symbol] is marked ready and contains at least one ticker entry. *)
let has_price_data symbol =
  match store_opt symbol with
  | Some store when Atomic.get store.ready -> Option.is_some (RingBuffer.read_latest store.buffer)
  | _ -> false

(** Block until [has_price_data] returns [true] for every symbol in [symbols], or [timeout_seconds] elapses. Returns [true] if all symbols are ready, [false] on timeout. Uses [ready_condition] to avoid busy-waiting. *)
(** Reusable hashtable for deduplicating per-symbol readiness notifications
    within a single parse_ticker call. Cleared on each invocation to avoid
    allocating a fresh Hashtbl on every incoming WS message. *)
let notified_symbols_reusable : (string, store) Hashtbl.t = Hashtbl.create 16

let wait_for_price_data_lwt symbols timeout_seconds =
  let deadline = Unix.gettimeofday () +. timeout_seconds in
  let rec loop () =
    if List.for_all has_price_data symbols then
      Lwt.return_true
    else
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0 then
        Lwt.return_false
      else
        Lwt.pick [
          (Lwt_condition.wait ready_condition >|= fun () -> `Again);
          (Lwt_unix.sleep remaining >|= fun () -> `Timeout)
        ] >>= function
        | `Again -> loop ()
        | `Timeout -> Lwt.return (List.for_all has_price_data symbols)
  in
  loop ()

let wait_for_price_data = wait_for_price_data_lwt

(** Parse a "ticker" channel message. Extracts each element of the [data] array, writes a [ticker] record to the corresponding symbol store, and signals readiness. Calls [on_heartbeat] per successfully parsed entry to keep the supervisor connection alive. *)
let parse_ticker json on_heartbeat =
  let open Yojson.Safe.Util in
  match json |> member "data" |> to_list with
  | exception _ ->
      Logging.warn_f ~section "Failed to parse ticker message: %s"
        (Yojson.Safe.to_string json);
      None
  | [] -> Some ()
  | data ->
      (* Collect symbols that wrote successfully so readiness is signalled once per symbol per message. *)
      Hashtbl.clear notified_symbols_reusable;
      let notified_symbols = notified_symbols_reusable in
      List.iter (fun ticker_data ->
        try
          let symbol = ticker_data |> member "symbol" |> to_string in
          let ticker = {
            symbol;
            bid = ticker_data |> member "bid" |> to_float;
            ask = ticker_data |> member "ask" |> to_float;
            last = ticker_data |> member "last" |> to_float;
            volume = ticker_data |> member "volume" |> to_float;
            timestamp = Unix.gettimeofday ();
          } in
          let store = ensure_store symbol in
          RingBuffer.write store.buffer ticker;
          (* Deduplicate: replace ensures at most one notification per symbol. *)
          Hashtbl.replace notified_symbols symbol store;
          Logging.debug_f ~section "Ticker: %s bid=%.2f ask=%.2f last=%.2f"
            symbol ticker.bid ticker.ask ticker.last;
          (* Propagate heartbeat to supervisor connection tracker. *)
          on_heartbeat ()
        with exn ->
          Logging.warn_f ~section "Failed to parse ticker data: %s"
            (Printexc.to_string exn)
      ) data;

      (* Signal readiness and per-symbol Exchange_wakeup for each successfully updated store. *)
      Hashtbl.iter (fun symbol store ->
        notify_ready store;
        Concurrency.Exchange_wakeup.signal ~symbol
      ) notified_symbols;

      Some ()

(** Top-level WebSocket frame dispatcher. Publishes a global tick event, then routes based on the [channel] or [method] field of the JSON payload. *)
let handle_message message on_heartbeat =
  Concurrency.Tick_event_bus.publish_tick ();
  try
    let json = Yojson.Safe.from_string message in
    let open Yojson.Safe.Util in
    match member "channel" json |> to_string_option,
          member "method" json |> to_string_option with
    | Some "ticker", _ ->
        ignore (parse_ticker json on_heartbeat)
    | Some "heartbeat", _ -> on_heartbeat () (* Propagate server heartbeat to supervisor. *)
    | _, Some "subscribe" ->
        let result = member "result" json in
        let symbol = member "symbol" result |> to_string in
        Logging.info_f ~section "Subscribed to %s ticker feed" symbol
    | Some "status", _ ->
        Logging.info ~section "Connected to Kraken WebSocket v2"
    | _ -> ()
  with exn ->
    Logging.error_f ~section "Error handling message: %s - %s"
      (Printexc.to_string exn) message


(** Mutable reference to the current WebSocket connection, used for dynamic subscriptions. *)
let active_conn = ref None

(** Send a subscribe request for [symbol] on the active WebSocket connection. Ensures the local store exists before subscribing. Logs a warning and returns [unit] if no connection is active. *)
let subscribe_ticker symbol =
  match !active_conn with
  | Some conn ->
      let _ = ensure_store symbol in
      let subscribe_msg = `Assoc [
        ("method", `String "subscribe");
        ("params", `Assoc [
          ("channel", `String "ticker");
          ("symbol", `List [`String symbol])
        ])
      ] in
      let msg_str = Yojson.Safe.to_string subscribe_msg in
      Logging.info_f ~section "Dynamically subscribing to %s ticker feed" symbol;
      Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg_str ())
  | None ->
      Logging.warn_f ~section "Cannot dynamically subscribe to %s: ticker WS not connected" symbol;
      Lwt.return_unit

(** Subscribe to all [symbols] on [conn], then enter a background read loop. Resolves the returned promise when the connection is closed or an error occurs. Invokes [on_failure] with a reason string on abnormal termination. Uses [Lwt.async] per frame to avoid stack growth. *)
let start_message_handler conn symbols on_failure on_heartbeat =
  active_conn := Some conn;
  (* Build and send a single batch subscribe message for all symbols. *)
  let subscribe_msg = `Assoc [
    ("method", `String "subscribe");
    ("params", `Assoc [
      ("channel", `String "ticker");
      ("symbol", `List (List.map (fun s -> `String s) symbols))
    ])
  ] in
  let msg_str = Yojson.Safe.to_string subscribe_msg in
  Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg_str ()) >>= fun () ->

  (* Recursive read loop. Each iteration dispatches via Lwt.async to avoid promise chain accumulation. *)
  let stream = Lwt_stream.from (fun () ->
    Lwt.catch (fun () ->
      Websocket_lwt_unix.read conn >>= fun frame ->
      Lwt.return_some frame
    ) (function
      | End_of_file -> Lwt.return_none
      | exn -> Lwt.fail exn)
  ) in

  let process_frame = function
    | {Websocket.Frame.opcode = Websocket.Frame.Opcode.Close; _} ->
        Logging.warn ~section "WebSocket connection closed by server";
        active_conn := None;
        on_failure "Connection closed by server";
        Lwt.fail (Failure "Connection closed by server")
    | frame ->
        Lwt.catch
          (fun () ->
             handle_message frame.Websocket.Frame.content on_heartbeat;
             Lwt.return_unit)
          (fun exn ->
             Logging.error_f ~section "Error handling Kraken ticker frame: %s" (Printexc.to_string exn);
             Lwt.return_unit)
  in

  let done_p =
    Lwt.catch
      (fun () -> Concurrency.Lwt_util.consume_stream (fun frame -> Lwt.async (fun () -> process_frame frame)) stream)
      (fun exn ->
         match exn with
         | Failure msg when msg = "Connection closed by server" ->
             Lwt.return_unit
         | _ ->
             Logging.error_f ~section "Ticker WebSocket error during read: %s" (Printexc.to_string exn);
             active_conn := None;
             on_failure (Printf.sprintf "WebSocket error: %s" (Printexc.to_string exn));
             Lwt.return_unit)
  in
  let final_done_p =
    done_p >>= fun () ->
    (* End_of_file triggers stream completion *)
    if !active_conn <> None then begin
      Logging.warn ~section "Ticker WebSocket connection closed unexpectedly (End_of_file)";
      active_conn := None;
      on_failure "Connection closed unexpectedly (End_of_file)";
      Lwt.return_unit
    end else
      Lwt.return_unit
  in
  final_done_p

(** Resolve the Kraken WebSocket host, establish a TLS connection, invoke [on_connected], then delegate to [start_message_handler]. Returns when the connection terminates. *)
let connect_and_subscribe symbols ~on_failure ~on_heartbeat ~on_connected =
  let uri = Uri.of_string "wss://ws.kraken.com/v2" in

  Logging.info_f ~section "Connecting to Kraken WebSocket...";
  (* DNS resolution: resolve ws.kraken.com to an IPv4 address for Conduit TLS. *)
  Lwt_unix.getaddrinfo "ws.kraken.com" "443" [Unix.AI_FAMILY Unix.PF_INET] >>= fun addresses ->
  let ip = match addresses with
    | {Unix.ai_addr = Unix.ADDR_INET (addr, _); _} :: _ ->
        Ipaddr_unix.of_inet_addr addr
    | _ -> failwith "Failed to resolve ws.kraken.com"
  in
  let client = `TLS (`Hostname "ws.kraken.com", `IP ip, `Port 443) in
  let _ctx = get_conduit_ctx () in
  Websocket_lwt_unix.connect ~ctx:_ctx client uri >>= fun conn ->

    Logging.info ~section "WebSocket established, subscribing to ticker feed";
    (* Notify supervisor of successful connection before entering the read loop. *)
    on_connected ();
    start_message_handler conn symbols on_failure on_heartbeat >>= fun () ->
    Logging.info ~section "Ticker WebSocket connection closed";
    Lwt.return_unit

(** Pre-allocate ring buffer stores for all [symbols] so that writers never race with store creation during the first tick. *)
let initialize symbols =
  Logging.info_f ~section "Initializing ticker feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created ticker buffer for %s" symbol
  ) symbols;
  Logging.info ~section "Ticker feed stores initialized"

