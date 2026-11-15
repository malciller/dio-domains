(** Execution reports (FIX [35=8]) translated into the engine's order cache.

    Over FIX the trading session *is* the execution stream: there is no separate
    executions channel. This module maps tag values onto the same
    [Kraken_executions_feed.execution_event] records the WebSocket feed produces, so the
    rest of the engine cannot tell which transport an order used.

    Parsing happens on the parse worker, not the socket fiber: a fill burst must not
    compete with the session's own protocol handling. The worker domain applies the event
    through the feed's domain-safe path (per-symbol mutex, atomics, wakeups). *)

let section = "kraken_fix_executions"

open Kraken_fix_codec

(** [150 ExecType]. Drives lifecycle handling in the feed; [39 OrdStatus] carries the
    resulting state. *)
let exec_type_of = function
  | '0' -> Kraken_executions_feed.New
  | '4' -> Kraken_executions_feed.Canceled
  | '5' -> Kraken_executions_feed.Amended
  | 'A' -> Kraken_executions_feed.PendingNew
  | 'C' -> Kraken_executions_feed.Expired
  | 'D' -> Kraken_executions_feed.Restated
  | 'F' -> Kraken_executions_feed.Trade
  | 'I' -> Kraken_executions_feed.New
  | other -> Kraken_executions_feed.Unknown (String.make 1 other)
;;

(** [39 OrdStatus]. [5 Replaced] and [E PendingReplace] both leave the order working, so
    they map onto [NewStatus] for the cache. *)
let order_status_of = function
  | '0' -> Kraken_executions_feed.NewStatus
  | '1' -> Kraken_executions_feed.PartiallyFilledStatus
  | '2' -> Kraken_executions_feed.FilledStatus
  | '4' -> Kraken_executions_feed.CanceledStatus
  | '5' -> Kraken_executions_feed.NewStatus
  | 'A' -> Kraken_executions_feed.PendingNewStatus
  | 'C' -> Kraken_executions_feed.ExpiredStatus
  | 'E' -> Kraken_executions_feed.PendingNewStatus
  | '8' -> Kraken_executions_feed.RejectedStatus
  | other -> Kraken_executions_feed.UnknownStatus (String.make 1 other)
;;

(** [54 Side]. *)
let side_of = function
  | '1' -> Kraken_executions_feed.Buy
  | '2' -> Kraken_executions_feed.Sell
  | _ -> Kraken_executions_feed.Buy
;;

let side_of_field frame =
  match field_char frame 54 with
  | Some c -> side_of c
  | None -> Kraken_executions_feed.Buy
;;

let is_terminal = function
  | Kraken_executions_feed.FilledStatus
  | Kraken_executions_feed.CanceledStatus
  | Kraken_executions_feed.ExpiredStatus
  | Kraken_executions_feed.RejectedStatus -> true
  | _ -> false
;;

(** Builds the engine-side event from one report. [None] when the frame is missing the
    identifiers every report carries, which means the session is out of sync and the frame
    must not touch the cache. *)
let event_of_frame frame =
  match field frame 37, field frame 55 with
  | Some order_id, Some symbol when order_id <> "" && symbol <> "" ->
    let exec_type =
      match field_char frame 150 with
      | Some c -> exec_type_of c
      | None -> Kraken_executions_feed.Unknown "?"
    in
    let order_status =
      match field_char frame 39 with
      | Some c -> order_status_of c
      | None -> Kraken_executions_feed.UnknownStatus "?"
    in
    let venue_time =
      match field frame 60 with
      | Some raw -> Option.value (unix_of_utc_timestamp raw) ~default:0.0
      | None -> 0.0
    in
    let cl_ord_id = field frame 11 in
    Some
      { Kraken_executions_feed.order_id
      ; symbol
      ; exec_type
      ; order_status
      ; side = side_of_field frame
      ; order_qty = Option.value (field_float frame 38) ~default:0.0
      ; cum_qty = Option.value (field_float frame 14) ~default:0.0
      ; cum_cost = Option.value (field_float frame 381) ~default:0.0
      ; avg_price = Option.value (field_float frame 6) ~default:0.0
      ; limit_price = field_float frame 44
      ; last_qty = field_float frame 32
      ; last_price = field_float frame 31
      ; fee = field_float frame 137
      ; trade_id =
          (match Option.bind (field frame 1003) Int64.of_string_opt with
           | Some id -> Some id
           | None -> None)
      ; order_userref =
          (match cl_ord_id with
           | Some id -> Kraken_fix_order_ids.userref_of_cl_ord_id id
           | None -> None)
      ; cl_ord_id
      ; timestamp = Unix.gettimeofday ()
      ; venue_time
      }
  | _ -> None
;;

(** Parses and applies one report on the calling thread. Domain-safe. *)
let handle_report raw =
  let bytes = Bytes.of_string raw in
  let frame = { buf = bytes; off = 0; len = Bytes.length bytes } in
  match event_of_frame frame with
  | None ->
    Logging.error_f
      ~section
      "Dropping Kraken FIX execution report without OrderID/Symbol: %s"
      (if String.length raw > 200 then String.sub raw 0 200 else raw)
  | Some event ->
    Kraken_executions_feed.apply_external_event event;
    (match event.cl_ord_id with
     | Some id when is_terminal event.order_status -> Kraken_fix_order_ids.forget id
     | _ -> ())
;;

let () = Concurrency.Parse_worker.register "kraken_fix" handle_report

(** Entry point from the session's read loop: copies the frame out of the session buffer
    and hands it to the parse worker, falling back to an inline apply when the worker
    queue is full so a fill is never dropped. *)
let ingest frame =
  let raw = span_string frame 0 (view_length frame) in
  if not (Concurrency.Parse_worker.submit "kraken_fix" raw) then handle_report raw
;;
