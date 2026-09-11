(** Alpaca trade execution stream, delivered by the Events API over
    Server-Sent Events ([GET /v2/events/trades]). Manages open order state and
    execution event ring buffers. The legacy v1 [wss://.../stream]
    trade_updates WebSocket is deprecated by Alpaca (HTTP 500) and must not be
    reinstated. *)

open Lwt.Infix
open Dio_exchange.Exchange_intf.Types

let section = "alpaca_executions"

(* Cumulative count of fill-class trade updates dropped because they carried
   unusable money fields (price <= 0, filled_qty <= 0, unparseable side, or
   empty order id/symbol). Surfaced on every drop so a venue API drift that
   silently loses fills is visible rather than folding into a silent
   inventory desync. *)
let dropped_fill_events = Atomic.make 0

type open_order_internal =
  { order_id : string
  ; symbol : string
  ; side : order_side
  ; qty : float
  ; cum_qty : float
  ; remaining_qty : float
  ; limit_price : float option
  ; status : order_status
  ; user_ref : int option
  ; cl_ord_id : string option
  }

type execution_event_internal =
  { order_id : string
  ; symbol : string
  ; order_status : order_status
  ; limit_price : float option
  ; side : order_side
  ; remaining_qty : float
  ; filled_qty : float
  ; avg_price : float
  ; timestamp : float
  ; is_amended : bool
  ; cl_ord_id : string option
  }

let status_of_alpaca_status = function
  | Alpaca_types.New -> New
  | Alpaca_types.PartiallyFilled -> PartiallyFilled
  | Alpaca_types.Filled -> Filled
  | Alpaca_types.Canceled | Alpaca_types.DoneForDay | Alpaca_types.Stopped -> Canceled
  | Alpaca_types.Expired -> Expired
  | Alpaca_types.Rejected -> Rejected
  | Alpaca_types.PendingNew | Alpaca_types.PendingCancel | Alpaca_types.PendingReplace ->
    Pending
  | Alpaca_types.Accepted | Alpaca_types.AcceptedForBidding | Alpaca_types.Calculated ->
    New
  | Alpaca_types.Replaced -> Canceled
  | Alpaca_types.Suspended -> Unknown "suspended"
  | Alpaca_types.Unknown s -> Unknown s
;;

module SymbolExecStore = struct
  type t =
    { symbol : string
    ; buffer : execution_event_internal array
    ; capacity : int
      (* write_pos/initial_data_received live in atomics: the WS writer is the
       single writer and the domain reads them lock-free via the _fast
       closures . The mutex still guards the open_orders Hashtbl and the
       full ring-buffer reads. *)
    ; write_pos : int Atomic.t
    ; open_orders : (string, open_order_internal) Hashtbl.t
    ; open_orders_cache : open_order_internal list Atomic.t
      (** Lock-free atomic snapshot cache of active open orders for the domain hotpath. *)
    ; initial_data_received : bool Atomic.t
    ; mutex : Mutex.t
    }

  let create symbol capacity =
    { symbol
    ; buffer =
        Array.make
          capacity
          { order_id = ""
          ; symbol = ""
          ; order_status = Pending
          ; limit_price = None
          ; side = Buy
          ; remaining_qty = 0.0
          ; filled_qty = 0.0
          ; avg_price = 0.0
          ; timestamp = 0.0
          ; is_amended = false
          ; cl_ord_id = None
          }
    ; capacity
    ; write_pos = Atomic.make 0
    ; open_orders = Hashtbl.create 32
    ; open_orders_cache = Atomic.make []
    ; initial_data_received = Atomic.make false
    ; mutex = Mutex.create ()
    }
  ;;

  let[@inline] publish_open_orders_cache t =
    let snapshot = Hashtbl.fold (fun _ o acc -> o :: acc) t.open_orders [] in
    Atomic.set t.open_orders_cache snapshot
  ;;

  let push_event t (e : execution_event_internal) =
    let idx = Atomic.get t.write_pos mod t.capacity in
    t.buffer.(idx) <- e;
    Atomic.set t.write_pos (Atomic.get t.write_pos + 1);
    Atomic.set t.initial_data_received true;
    (* Update the open order store under the mutex. *)
    Mutex.lock t.mutex;
    (match e.order_status with
     | New | Pending | PartiallyFilled | Unknown _ ->
       let existing_order = Hashtbl.find_opt t.open_orders e.order_id in
       let qty =
         match existing_order with
         | Some o -> o.qty
         | None -> e.filled_qty +. e.remaining_qty
       in
       let user_ref =
         match e.cl_ord_id with
         | Some cid ->
           (try Some (int_of_string cid) with
            | _ -> None)
         | None -> None
       in
       let oo =
         { order_id = e.order_id
         ; symbol = t.symbol
         ; side = e.side
         ; qty
         ; cum_qty = e.filled_qty
         ; remaining_qty = e.remaining_qty
         ; limit_price = e.limit_price
         ; status = e.order_status
         ; user_ref
         ; cl_ord_id = e.cl_ord_id
         }
       in
       Hashtbl.replace t.open_orders e.order_id oo
     | Filled | Canceled | Expired | Rejected -> Hashtbl.remove t.open_orders e.order_id);
    publish_open_orders_cache t;
    Mutex.unlock t.mutex;
    (* per-symbol wakeup - only this symbol's domain consumes its exec events. *)
    Concurrency.Exchange_wakeup.signal ~symbol:t.symbol
  ;;

  let set_open_orders_snapshot t orders =
    Mutex.lock t.mutex;
    Hashtbl.clear t.open_orders;
    List.iter
      (fun (o : Alpaca_types.order_record) ->
         let user_ref =
           match o.client_order_id with
           | Some cid ->
             (try Some (int_of_string cid) with
              | _ -> None)
           | None -> None
         in
         let oo =
           { order_id = o.id
           ; symbol = t.symbol
           ; side =
               (match o.side with
                | Alpaca_types.Buy -> Buy
                | Sell -> Sell)
           ; qty = o.qty
           ; cum_qty = o.filled_qty
           ; remaining_qty = max 0.0 (o.qty -. o.filled_qty)
           ; limit_price = o.limit_price
           ; status = status_of_alpaca_status o.status
           ; user_ref
           ; cl_ord_id = o.client_order_id
           }
         in
         Hashtbl.replace t.open_orders o.id oo)
      orders;
    publish_open_orders_cache t;
    Mutex.unlock t.mutex;
    Atomic.set t.initial_data_received true;
    (* snapshot readiness is per-store; only this symbol's domain gates on it. *)
    Concurrency.Exchange_wakeup.signal ~symbol:t.symbol
  ;;

  let mark_ready t =
    Atomic.set t.initial_data_received true;
    Concurrency.Exchange_wakeup.signal ~symbol:t.symbol
  ;;
end

let stores : (string, SymbolExecStore.t) Hashtbl.t = Hashtbl.create 16
let stores_mutex = Mutex.create ()

let get_or_create_store symbol =
  Mutex.lock stores_mutex;
  let store =
    match Hashtbl.find_opt stores symbol with
    | Some s -> s
    | None ->
      let s = SymbolExecStore.create symbol 1024 in
      Hashtbl.replace stores symbol s;
      s
  in
  Mutex.unlock stores_mutex;
  store
;;

(* Alpaca trade events arrive over the Events API as Server-Sent Events
   (SSE), NOT the legacy WebSocket: the v1 [wss://.../stream] trade_updates
   endpoint is deprecated and currently returns HTTP 500.

   [sse_active] mirrors "the HTTP response stream is open"; [last_activity] is
   the wall-clock of the most recent SSE line (a data frame OR a comment
   heartbeat) and backs the supervisor's ping probe, since SSE has no
   protocol-level ping; [last_event_id] carries the last event ULID so a
   reconnect resumes with [since_id] and no gap. *)
let sse_active = Atomic.make false
let last_activity = ref 0.0
let last_event_id : string option ref = ref None

(** Bounded startup backfill: the first connect of a process asks for events
    since now - 15min so a short restart does not lose fills. Every reconnect
    after that resumes exactly from [last_event_id]. Replayed fills are safe:
    the strategy's per-order high-water guard drops already-applied fills. *)
let trade_events_backfill_s = 900.0

(** Idle time after which the connectivity probe reports a stalled stream.
    SSE servers emit comment heartbeats well inside this; the value is
    deliberately generous so a quiet-but-healthy stream is not torn down by
    the probe's shorter timeout. *)
let sse_idle_failure_s = 60.0

let get_open_order symbol order_id =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
    let orders = Atomic.get store.open_orders_cache in
    List.find_opt (fun (o : open_order_internal) -> o.order_id = order_id) orders
  | None -> None
;;

let remove_open_order symbol order_id =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
    Mutex.lock store.mutex;
    let existed = Hashtbl.mem store.open_orders order_id in
    if existed
    then (
      Hashtbl.remove store.open_orders order_id;
      SymbolExecStore.publish_open_orders_cache store);
    Mutex.unlock store.mutex
  | None -> ()
;;

let get_open_orders symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> Atomic.get store.open_orders_cache
  | None -> []
;;

let get_all_symbols () =
  Mutex.lock stores_mutex;
  let syms = Hashtbl.fold (fun k _ acc -> k :: acc) stores [] in
  Mutex.unlock stores_mutex;
  syms
;;

let get_current_position symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> Atomic.get store.write_pos
  | None -> 0
;;

let get_current_position_fast symbol =
  let store = get_or_create_store symbol in
  fun () -> Atomic.get store.write_pos
;;

let has_execution_data symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> Atomic.get store.initial_data_received
  | None -> false
;;

let has_execution_data_fast symbol =
  let store = get_or_create_store symbol in
  fun () -> Atomic.get store.initial_data_received
;;

let read_execution_events symbol start_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
    let current_pos = Atomic.get store.write_pos in
    let start_idx = max start_pos (current_pos - store.capacity) in
    let events = ref [] in
    for i = current_pos - 1 downto start_idx do
      let idx = i mod store.capacity in
      events := store.buffer.(idx) :: !events
    done;
    !events
  | None -> []
;;

let iter_execution_events symbol start_pos f =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
    let current_pos = Atomic.get store.write_pos in
    let start_idx = max start_pos (current_pos - store.capacity) in
    for i = start_idx to current_pos - 1 do
      let idx = i mod store.capacity in
      f store.buffer.(idx)
    done;
    current_pos
  | None -> start_pos
;;

let fold_open_orders symbol ~init ~f =
  match Hashtbl.find_opt stores symbol with
  | Some store ->
    let snapshot = Atomic.get store.open_orders_cache in
    List.fold_left (fun acc o -> f acc o) init snapshot
  | None -> init
;;

let initialize symbols =
  List.iter
    (fun sym ->
       let store = get_or_create_store sym in
       SymbolExecStore.mark_ready store)
    symbols
;;

let bootstrap_open_orders () =
  Alpaca_rest.get_open_orders ()
  >>= function
  | Ok orders ->
    let grouped = Hashtbl.create 8 in
    List.iter
      (fun (o : Alpaca_types.order_record) ->
         ignore (get_or_create_store o.symbol);
         let existing =
           try Hashtbl.find grouped o.symbol with
           | _ -> []
         in
         Hashtbl.replace grouped o.symbol (o :: existing))
      orders;
    Mutex.lock stores_mutex;
    Hashtbl.iter
      (fun sym store ->
         let sym_orders =
           try Hashtbl.find grouped sym with
           | _ -> []
         in
         SymbolExecStore.set_open_orders_snapshot store sym_orders)
      stores;
    Mutex.unlock stores_mutex;
    Logging.debug_f
      ~section
      "Bootstrapped %d open orders across symbols"
      (List.length orders);
    Lwt.return_unit
  | Error err ->
    Logging.error_f ~section "Failed to bootstrap open orders: %s" err;
    Mutex.lock stores_mutex;
    Hashtbl.iter (fun _store_sym store -> SymbolExecStore.mark_ready store) stores;
    Mutex.unlock stores_mutex;
    Lwt.return_unit
;;

let apply_trade_update json =
  let open Yojson.Safe.Util in
  let event = json |> member "event" |> to_string_option |> Option.value ~default:"" in
  let order_json = json |> member "order" in
  let ord = Alpaca_rest.parse_order_json order_json in
  let side =
    match ord.side with
    | Alpaca_types.Buy -> Buy
    | Sell -> Sell
  in
  let is_amended = false in
  let price =
    match json |> member "price" with
    | `Float f -> f
    | `Int i -> float_of_int i
    | `String s ->
      (try float_of_string s with
       | _ -> Option.value ord.limit_price ~default:0.0)
    | _ -> Option.value ord.limit_price ~default:0.0
  in
  let exec_event =
    { order_id = ord.id
    ; symbol = ord.symbol
    ; order_status = status_of_alpaca_status ord.status
    ; limit_price = ord.limit_price
    ; side
    ; remaining_qty = max 0.0 (ord.qty -. ord.filled_qty)
    ; filled_qty = ord.filled_qty
    ; avg_price = price
    ; timestamp = Unix.time ()
    ; is_amended
    ; cl_ord_id = ord.client_order_id
    }
  in
  (* Fail-closed on money fields for fill-class events: a fill at price<=0,
     qty<=0, with an unparseable side, or a missing order id would corrupt
     inventory direction / P&L if it entered the strategy ledger. Drop it
     loudly (a dropped fill is healed by the balance-based reconcile; a
     wrong-side fill actively corrupts). Non-fill events (new/canceled/...)
     carry no fill money and are unaffected. *)
  let is_fill_class = String.equal event "fill" || String.equal event "partial_fill" in
  let side_ok = String.equal ord.side_str "buy" || String.equal ord.side_str "sell" in
  let money_ok = price > 0.0 && ord.filled_qty > 0.0 in
  let should_drop =
    is_fill_class && ((not side_ok) || (not money_ok) || String.equal ord.id "")
  in
  if should_drop
  then (
    let n = Atomic.fetch_and_add dropped_fill_events 1 in
    Logging.critical_f
      ~section
      "Dropping Alpaca %s event for order %s (side=%s, qty=%.6f, filled=%.6f, \
       price=%.6f) - unusable money fields; cumulative drops=%d. JSON: %s"
      event
      ord.id
      ord.side_str
      ord.qty
      ord.filled_qty
      price
      n
      (Yojson.Safe.to_string json));
  Logging.debug_f
    ~section
    "Trade update [%s]: order %s %s %s %.4f @ %.4f (filled: %.4f, remaining: %.4f)"
    event
    ord.id
    ord.symbol
    (match side with
     | Buy -> "BUY"
     | Sell -> "SELL")
    ord.qty
    price
    ord.filled_qty
    exec_event.remaining_qty;
  if not should_drop
  then (
    let store = get_or_create_store ord.symbol in
    SymbolExecStore.push_event store exec_event;
    (* Publish to centralized fill event bus for Discord notifications if live trading is enabled *)
    if
      (not !Alpaca_types.Config.is_paper)
      && (event = "fill" || exec_event.order_status = Filled)
    then (
      let fill_value = ord.filled_qty *. price in
      let maker_fee_rate =
        match Dio_exchange.Exchange_intf.Registry.get "alpaca" with
        | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
          (match Ex.get_fees ~symbol:ord.symbol with
           | Some f, _ -> f
           | _ -> 0.0)
        | None -> 0.0
      in
      let fee = fill_value *. maker_fee_rate in
      Concurrency.Fill_event_bus.publish_fill
        { venue = "alpaca"
        ; symbol = ord.symbol
        ; side = (if side = Buy then "buy" else "sell")
        ; amount = ord.filled_qty
        ; fill_price = price
        ; value = fill_value
        ; fee
        ; timestamp = Unix.time ()
        ; order_id = ord.id
        ; trade_id = ord.id
        });
    ());
  if event = "fill" || event = "partial_fill" || event = "canceled" || event = "rejected"
  then Lwt.async (fun () -> Alpaca_balances.update_balances ())
;;

(** Primary entry point for one trade event. The Events API delivers the
    [TradeUpdateEventV2] object directly (no legacy [{stream,data}] wrapper):
    [event], [order], and (for fills) [price]/[qty] sit at the top level, which
    is exactly the shape [apply_trade_update] already consumes. Records the
    monotonic [event_id] for resumable reconnects. [trade_bust] /
    [trade_correct] reverse/correct a prior execution; the fill ledger has no
    reversal model, so they are surfaced loudly and left alone. *)
let handle_trade_update json =
  let open Yojson.Safe.Util in
  (match json |> member "event_id" with
   | `String id -> last_event_id := Some id
   | _ -> ());
  let event = json |> member "event" |> to_string_option |> Option.value ~default:"" in
  if event = "trade_bust" || event = "trade_correct"
  then (
    Logging.warn_f
      ~section
      "Alpaca %s event not modeled by the execution ledger (previous_execution_id=%s); \
       leaving inventory as-is, balance reconcile will heal. JSON: %s"
      event
      (json
       |> member "previous_execution_id"
       |> to_string_option
       |> Option.value ~default:"")
      (Yojson.Safe.to_string json);
    Lwt.async (fun () -> Alpaca_balances.update_balances ()))
  else apply_trade_update json
;;

(* -- Events API (Server-Sent Events) transport ------------------------ *)

(** RFC3339 UTC timestamp for the [since] query param. *)
let iso8601_utc (t : float) : string =
  let tm = Unix.gmtime t in
  Printf.sprintf
    "%04d-%02d-%02dT%02d:%02d:%02dZ"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
    tm.Unix.tm_hour
    tm.Unix.tm_min
    tm.Unix.tm_sec
;;

(** Stream URI: resume exactly from [last_event_id] when known, else request a
    bounded startup backfill. *)
let events_uri () =
  let base = Uri.of_string (Alpaca_types.Config.trading_events_url ()) in
  match !last_event_id with
  | Some id -> Uri.with_query' base [ "since_id", id ]
  | None ->
    Uri.with_query'
      base
      [ "since", iso8601_utc (Unix.gettimeofday () -. trade_events_backfill_s) ]
;;

let event_stream_headers () =
  Cohttp.Header.of_list
    [ "APCA-API-KEY-ID", Alpaca_types.Config.api_key ()
    ; "APCA-API-SECRET-KEY", Alpaca_types.Config.api_secret ()
    ; "Accept", "text/event-stream"
    ; "Cache-Control", "no-cache"
    ; "Accept-Encoding", "identity"
    ]
;;

let starts_with ~prefix s =
  String.length s >= String.length prefix
  && String.equal (String.sub s 0 (String.length prefix)) prefix
;;

let strip_trailing_cr s =
  let n = String.length s in
  if n > 0 && Char.equal s.[n - 1] '\r' then String.sub s 0 (n - 1) else s
;;

(** Consumes the SSE body until EOF. A blank line dispatches the accumulated
    [data:] payload; every line (including [:] comment heartbeats) refreshes
    [last_activity] and the supervisor heartbeat. *)
let consume_event_stream ~on_heartbeat body =
  let stream = Cohttp_lwt.Body.to_stream body in
  let line_buf = Buffer.create 1024 in
  let data_buf = Buffer.create 512 in
  let dispatch () =
    let raw = String.trim (Buffer.contents data_buf) in
    Buffer.clear data_buf;
    if raw <> ""
    then (
      match Yojson.Safe.from_string raw with
      | json -> handle_trade_update json
      | exception exn ->
        Logging.error_f
          ~section
          "Failed to parse Alpaca trade event: %s (payload: %s)"
          (Printexc.to_string exn)
          raw)
  in
  let handle_line line =
    last_activity := Unix.gettimeofday ();
    on_heartbeat ();
    if String.equal line ""
    then dispatch ()
    else if Char.equal line.[0] ':'
    then () (* comment / heartbeat *)
    else if starts_with ~prefix:"data:" line
    then (
      let payload = String.sub line 5 (String.length line - 5) in
      let payload =
        if String.length payload > 0 && Char.equal payload.[0] ' '
        then String.sub payload 1 (String.length payload - 1)
        else payload
      in
      if Buffer.length data_buf > 0 then Buffer.add_char data_buf '\n';
      Buffer.add_string data_buf payload)
    else () (* event:/id:/retry: - we read those from the JSON body *)
  in
  let rec scan chunk i start =
    let len = String.length chunk in
    if i >= len
    then (if start < len then Buffer.add_substring line_buf chunk start (len - start))
    else if Char.equal chunk.[i] '\n'
    then (
      Buffer.add_substring line_buf chunk start (i - start);
      let line = strip_trailing_cr (Buffer.contents line_buf) in
      Buffer.clear line_buf;
      handle_line line;
      scan chunk (i + 1) (i + 1))
    else scan chunk (i + 1) start
  in
  let rec loop () =
    Lwt_stream.get stream
    >>= function
    | None -> Lwt.return_unit
    | Some chunk ->
      scan chunk 0 0;
      loop ()
  in
  loop ()
  >>= fun () ->
  dispatch ();
  Lwt.return_unit
;;

(** Opens the Events API SSE stream and consumes it until EOF. Preserves the
    supervised-feed contract: [on_connected] once the response is 2xx,
    [on_failure] on end/error, reconnection owned by the supervisor (which
    resumes from [last_event_id]). *)
let connect_and_monitor ~on_failure ~on_connected ~on_heartbeat =
  Lwt.catch
    (fun () ->
       let uri = events_uri () in
       Logging.debug_f
         ~section
         "Connecting to Alpaca trade events stream at %s"
         (Uri.to_string uri);
       (* Bound only the TLS + response-header phase: the body is a
          long-lived stream and must not be cancelled by a timeout. *)
       Lwt_unix.with_timeout 20.0 (fun () ->
         Cohttp_lwt_unix.Client.get ~headers:(event_stream_headers ()) uri)
       >>= fun (resp, body) ->
       let status = Cohttp.Response.status resp in
       if not (Cohttp.Code.is_success (Cohttp.Code.code_of_status status))
       then
         Lwt.fail
           (Failure
              (Printf.sprintf
                 "Alpaca trade events stream HTTP %s"
                 (Cohttp.Code.string_of_status status)))
       else (
         Atomic.set sse_active true;
         last_activity := Unix.gettimeofday ();
         on_connected ();
         Logging.info_f
           ~section
           "Connected to Alpaca trade events stream at %s"
           (Uri.to_string uri);
         bootstrap_open_orders ()
         >>= fun () ->
         consume_event_stream ~on_heartbeat body
         >>= fun () ->
         Atomic.set sse_active false;
         Logging.warn_f ~section "Alpaca trade events stream ended; reconnecting";
         on_failure "trade events stream ended";
         Lwt.return_unit))
    (fun exn ->
       Atomic.set sse_active false;
       let err = Printexc.to_string exn in
       Logging.error_f ~section "Alpaca trade events stream disconnected: %s" err;
       on_failure err;
       Lwt.return_unit)
;;

(** Connectivity probe for the supervised monitor loop. SSE has no protocol
    ping, so liveness means "the stream is open and produced a line (data or
    comment heartbeat) within [sse_idle_failure_s]". Returns [false] when
    disconnected, preserving the existing monitor/test contract. *)
let send_ping ~req_id ~timeout_ms : bool Lwt.t =
  ignore req_id;
  ignore timeout_ms;
  if not (Atomic.get sse_active)
  then Lwt.return false
  else (
    let idle = Unix.gettimeofday () -. !last_activity in
    if idle <= sse_idle_failure_s
    then (
      Network_latency.record_ping_s "alpaca" idle;
      Lwt.return true)
    else (
      Logging.warn_f
        ~section
        "Alpaca trade events stream idle for %.0fs (no data/heartbeat)"
        idle;
      Lwt.return false))
;;
