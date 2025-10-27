(** Kraken Ticker Feed - WebSocket v2 ticker subscription with ring buffer *)
open Lwt.Infix

let section = "kraken_ticker"

(** Ticker data *)
type ticker = {
  symbol: string;
  bid: float;
  ask: float;
  last: float;
  volume: float;
  timestamp: float;
}

(** Lock-free ring buffer for ticker data using atomics *)
module RingBuffer = struct
  type 'a t = {
    data: 'a option Atomic.t array;
    write_pos: int Atomic.t;
    size: int;
  }

  let create size =
    {
      data = Array.init size (fun _ -> Atomic.make None);
      write_pos = Atomic.make 0;
      size;
    }

  (* Lock-free write - single writer *)
  let write buffer value =
    let pos = Atomic.get buffer.write_pos in
    Atomic.set buffer.data.(pos) (Some value);
    let new_pos = (pos + 1) mod buffer.size in
    Atomic.set buffer.write_pos new_pos

  (* Wait-free read - multiple readers *)
  let read_latest buffer =
    let pos = Atomic.get buffer.write_pos in
    let read_pos = if pos = 0 then buffer.size - 1 else pos - 1 in
    Atomic.get buffer.data.(read_pos)

  (** Read events from last known position - for domain consumers *)
  let read_since buffer last_pos =
    let current_pos = Atomic.get buffer.write_pos in
    if last_pos = current_pos then
      []
    else
      let rec collect acc pos =
        if pos = current_pos then
          List.rev acc
        else
          match Atomic.get buffer.data.(pos) with
          | Some event -> collect (event :: acc) ((pos + 1) mod buffer.size)
          | None -> collect acc ((pos + 1) mod buffer.size)
      in
      collect [] last_pos
end

(** Per-symbol ticker storage with readiness signalling *)
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
    with Invalid_argument _ ->
      (* Ignore - some waiters may have timed out or been cancelled *)
      ())
  end

(** Get latest ticker for a symbol - hot path, inlined *)
let[@inline always] get_latest_ticker symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None

(** Get latest price (midpoint of bid/ask) - hot path, inlined *)
let[@inline always] get_latest_price symbol =
  match get_latest_ticker symbol with
  | None -> None
  | Some ticker -> Some ((ticker.bid +. ticker.ask) /. 2.0)

(** Read ticker events since last position - for domain consumers *)
let[@inline always] read_ticker_events symbol last_pos =
  match store_opt symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

(** Get current write position for tracking consumption *)
let[@inline always] get_current_position symbol =
  match store_opt symbol with
  | Some store -> Atomic.get store.buffer.write_pos
  | None -> 0

(** Check if we have price data for a symbol *)
let has_price_data symbol =
  match store_opt symbol with
  | Some store when Atomic.get store.ready -> Option.is_some (RingBuffer.read_latest store.buffer)
  | _ -> false

(** Wait until price data is available for all symbols using event signalling *)
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

(** Parse ticker from WebSocket message *)
let parse_ticker json on_heartbeat =
  let open Yojson.Safe.Util in
  match json |> member "data" |> to_list with
  | exception _ ->
      Logging.warn_f ~section "Failed to parse ticker message: %s"
        (Yojson.Safe.to_string json);
      None
  | [] -> Some ()
  | data ->
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
          notify_ready store;
          Logging.debug_f ~section "Ticker: %s bid=%.2f ask=%.2f last=%.2f"
            symbol ticker.bid ticker.ask ticker.last;
          Telemetry.inc_counter (Telemetry.counter "ticker_updates" ()) ();
          (* Update connection heartbeat *)
          on_heartbeat ()
        with exn ->
          Logging.warn_f ~section "Failed to parse ticker data: %s"
            (Printexc.to_string exn)
      ) data;
      Some ()

(** WebSocket message handler *)
let handle_message message on_heartbeat =
  try
    let json = Yojson.Safe.from_string message in
    let open Yojson.Safe.Util in
    match member "channel" json |> to_string_option,
          member "method" json |> to_string_option with
    | Some "ticker", _ ->
        ignore (parse_ticker json on_heartbeat)
    | Some "heartbeat", _ -> on_heartbeat () (* Update connection heartbeat *)
    | _, Some "subscribe" ->
        let result = member "result" json in
        let symbol = member "symbol" result |> to_string in
        Logging.info_f ~section "Subscribed to %s ticker feed" symbol
    | Some "status", _ ->
        Logging.info ~section "Connected to Kraken WebSocket v2"
    | _ ->
        Logging.debug_f ~section "Unhandled: %s" message
  with exn ->
    Logging.error_f ~section "Error handling message: %s - %s"
      (Printexc.to_string exn) message


(** Message handling loop - runs in background *)
let start_message_handler conn symbols on_failure on_heartbeat =
  (* Subscribe to ticker for all symbols *)
  let subscribe_msg = `Assoc [
    ("method", `String "subscribe");
    ("params", `Assoc [
      ("channel", `String "ticker");
      ("symbol", `List (List.map (fun s -> `String s) symbols))
    ])
  ] in
  let msg_str = Yojson.Safe.to_string subscribe_msg in
  Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg_str ()) >>= fun () ->

  (* Message loop *)
  let rec msg_loop () =
    Lwt.catch (fun () ->
      Websocket_lwt_unix.read conn >>= function
      | {Websocket.Frame.opcode = Websocket.Frame.Opcode.Close; _} ->
          Logging.warn ~section "WebSocket connection closed by server";
          on_failure "Connection closed by server";
          Lwt.return_unit
      | frame ->
          handle_message frame.Websocket.Frame.content on_heartbeat;
          msg_loop ()
    ) (function
      | End_of_file ->
          Logging.warn ~section "Ticker WebSocket connection closed unexpectedly (End_of_file)";
          (* Notify supervisor of connection failure *)
          on_failure "Connection closed unexpectedly (End_of_file)";
          Lwt.return_unit
      | exn ->
          Logging.error_f ~section "Ticker WebSocket error during read: %s" (Printexc.to_string exn);
          (* Notify supervisor of connection failure *)
          on_failure (Printf.sprintf "WebSocket error: %s" (Printexc.to_string exn));
          Lwt.return_unit
    )
  in
  msg_loop ()

(** WebSocket connection to Kraken - establishes connection and starts message handler *)
let connect_and_subscribe symbols ~on_failure ~on_heartbeat =
  let uri = Uri.of_string "wss://ws.kraken.com/v2" in

  Logging.info_f ~section "Connecting to Kraken WebSocket...";
  (* Resolve hostname to IP *)
  Lwt_unix.getaddrinfo "ws.kraken.com" "443" [Unix.AI_FAMILY Unix.PF_INET] >>= fun addresses ->
  let ip = match addresses with
    | {Unix.ai_addr = Unix.ADDR_INET (addr, _); _} :: _ ->
        Ipaddr_unix.of_inet_addr addr
    | _ -> failwith "Failed to resolve ws.kraken.com"
  in
  let client = `TLS (`Hostname "ws.kraken.com", `IP ip, `Port 443) in
  let ctx = Lazy.force Conduit_lwt_unix.default_ctx in
  Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->

    Logging.info ~section "WebSocket established, subscribing to ticker feed";
    Lwt.async (fun () -> start_message_handler conn symbols on_failure on_heartbeat);
    Logging.info ~section "Ticker WebSocket connection established";
    Lwt.return_unit

(** Initialize ticker feed data stores *)
let initialize symbols =
  Logging.info_f ~section "Initializing ticker feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created ticker buffer for %s" symbol
  ) symbols;
  Logging.info ~section "Ticker feed stores initialized"

