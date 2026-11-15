(** Kraken Unified FIX 4.4 trading session.

    Owns one TLS connection to a Kraken FIX gateway: logon, sequence numbers, heartbeat
    negotiation, resend handling, the daily 22:00 UTC rollover, and the reconnect
    lifecycle the supervisor drives. Application messages are built by
    {!Kraken_fix_actions}; execution reports are handed to
    {!Kraken_fix_execution_reports}, which applies them to the shared order cache.

    Design notes that matter for a live account:

    - Sequence numbers are session state, not connection state. Kraken's gateway replays
      execution reports from the number we ask for, so they survive a reconnect here too.
      A rejected logon (disagreeing sequence) arms [reset_next_logon] and the next attempt
      sends [141=Y] to reset both sides.
    - One writer. Every frame is written while holding [state.mutex], so tag [34] on the
      wire is always in the order the numbers were assigned.
    - Outbound frames are journalled so a [ResendRequest] is answered with the original
      messages (tag [43] set) instead of a gap fill, which would silently swallow an order
      the gateway never received.
    - Inbound processing stays on the Lwt domain that owns the socket; only the
      order-cache update is offloaded to the parse domain. Lwt waiters are never woken
      from another domain. *)

open Lwt.Infix
open Kraken_fix_codec

let section = "kraken_fix"

module Pending = Hashtbl.Make (struct
    type t = string

    let equal = String.equal
    let hash = Hashtbl.hash
  end)

module Heartbeat_bus = Concurrency.Event_bus.Make (struct
    type t = unit
  end)

module Connection_bus = Concurrency.Event_bus.Make (struct
    type t =
      [ `Connected
      | `Disconnected of string
      ]
  end)

(** Outbound frames kept for resend. A grid places and amends far fewer messages than this
    inside a reconnect window, and an entry is a few hundred bytes. *)
let journal_capacity = 2048

(** Bound on out-of-order inbound frames held while waiting for a replay. *)
let max_pending_inbound = 512

type connection =
  { ic : Lwt_io.input_channel
  ; oc : Lwt_io.output_channel
  ; framer : Kraken_fix_codec.framer
  ; flow : Conduit_lwt_unix.flow
  ; generation : int
  }

(** An outbound frame, kept verbatim so a resend is byte-identical apart from [43]/[122]. *)
type journal_entry =
  { seq : int
  ; msg_type : char
  ; body : string
  ; sending_time : string
  }

type state =
  { mutex : Lwt_mutex.t
  ; mutable conn : connection option
  ; mutable connection_generation : int
  ; mutable seq_out : int
  ; mutable expected_seq_in : int
  ; mutable logged_on : bool
  ; mutable connecting : bool
  ; mutable reset_next_logon : bool
  ; mutable on_failure : (string -> unit) option
  ; mutable test_request_sent_at : float
  ; mutable resend_requested_for : int option
  ; mutable replay_window : (int * int) option
  (** [(floor, until)] while a reconnect replay is outstanding: replayed frames (tag
      [43]=Y) numbered in [floor, until) are processed instead of discarded, because they
      are the reports the gateway sent while we were disconnected. Each number is accepted
      once and the floor advances past it; the window closes when the floor reaches
      [until], never on the arrival of a newer live frame. *)
  ; connected : bool Atomic.t
  ; logged_on_signal : unit Lwt_condition.t
  ; session_closed : unit Lwt_condition.t
  (** Broadcast whenever [conn] goes away, so a redundant supervisor attempt parks on the
      live session instead of resolving its connect function early. *)
  ; last_inbound : float Atomic.t
  ; last_outbound : float Atomic.t
  ; places : (Kraken_common_types.add_order_result, string) result Lwt.u Pending.t
  ; amends : (Kraken_common_types.amend_order_result, string) result Lwt.u Pending.t
  ; cancels : (Kraken_common_types.cancel_order_result, string) result Lwt.u Pending.t
  ; pings : unit Lwt.u Pending.t
  ; journal : (int, journal_entry) Hashtbl.t
  ; mutable journal_oldest : int
  ; out_of_order : (int, string) Hashtbl.t
  }

let state =
  { mutex = Lwt_mutex.create ()
  ; conn = None
  ; connection_generation = 0
  ; seq_out = 1
  ; expected_seq_in = 1
  ; logged_on = false
  ; connecting = false
  ; reset_next_logon = false
  ; on_failure = None
  ; test_request_sent_at = 0.0
  ; resend_requested_for = None
  ; replay_window = None
  ; connected = Atomic.make false
  ; logged_on_signal = Lwt_condition.create ()
  ; session_closed = Lwt_condition.create ()
  ; last_inbound = Atomic.make 0.0
  ; last_outbound = Atomic.make 0.0
  ; places = Pending.create 64
  ; amends = Pending.create 64
  ; cancels = Pending.create 64
  ; pings = Pending.create 8
  ; journal = Hashtbl.create 256
  ; journal_oldest = 1
  ; out_of_order = Hashtbl.create 32
  }
;;

let heartbeat_bus = Heartbeat_bus.create "kraken_fix_heartbeat"
let connection_bus = Connection_bus.create "kraken_fix_connection"
let heartbeat_stream () = Heartbeat_bus.subscribe heartbeat_bus
let connection_stream () = Connection_bus.subscribe connection_bus
let notify_heartbeat () = Heartbeat_bus.publish heartbeat_bus ()
let notify_connection status = Connection_bus.publish connection_bus status

(** True once the gateway has accepted our logon. This is the readiness signal the order
    path gates on. *)
let is_connected () = Atomic.get state.connected

let config =
  lazy
    (match Kraken_fix_config.load () with
     | Some cfg -> cfg
     | None -> failwith "Kraken FIX session started without FIX credentials")
;;

let config_or_none () =
  try Some (Lazy.force config) with
  | Failure _ -> None
;;

(** How long an order request waits for its acknowledgement. Read from the session
    configuration so an operator can widen it without a rebuild; the default matches the
    WebSocket adapter's per-attempt timeout. *)
let request_timeout_ms () =
  match config_or_none () with
  | Some cfg -> cfg.Kraken_fix_config.request_timeout_ms
  | None -> 10_000
;;

(* -- Waiter bookkeeping ---------------------------------------------- *)

let resolve_generic table key to_result =
  match Pending.find_opt table key with
  | None -> false
  | Some wakener ->
    Pending.remove table key;
    (try Lwt.wakeup_later wakener (to_result ()) with
     | Invalid_argument _ -> ());
    true
;;

let fail_all_pending reason =
  let entries = Pending.fold (fun k w acc -> (k, w) :: acc) state.places [] in
  Pending.reset state.places;
  List.iter
    (fun (_, wakener) ->
      try Lwt.wakeup_later wakener (Error reason) with
      | Invalid_argument _ -> ())
    entries;
  let entries = Pending.fold (fun k w acc -> (k, w) :: acc) state.amends [] in
  Pending.reset state.amends;
  List.iter
    (fun (_, wakener) ->
      try Lwt.wakeup_later wakener (Error reason) with
      | Invalid_argument _ -> ())
    entries;
  let entries = Pending.fold (fun k w acc -> (k, w) :: acc) state.cancels [] in
  Pending.reset state.cancels;
  List.iter
    (fun (_, wakener) ->
      try Lwt.wakeup_later wakener (Error reason) with
      | Invalid_argument _ -> ())
    entries;
  let pings = Pending.fold (fun k w acc -> (k, w) :: acc) state.pings [] in
  Pending.reset state.pings;
  List.iter
    (fun (_, wakener) ->
      try Lwt.wakeup_later_exn wakener (Failure reason) with
      | Invalid_argument _ -> ())
    pings
;;

(* -- Outbound journal ------------------------------------------------ *)

let journal_add st entry =
  Hashtbl.replace st.journal entry.seq entry;
  if Hashtbl.length st.journal > journal_capacity
  then (
    Hashtbl.remove st.journal st.journal_oldest;
    st.journal_oldest <- st.journal_oldest + 1)
;;

(* -- Writing --------------------------------------------------------- *)

let write_raw st conn raw =
  Lwt.catch
    (fun () ->
      Lwt_io.write conn.oc raw
      >>= fun () ->
      Lwt_io.flush conn.oc
      >|= fun () ->
      Atomic.set st.last_outbound (Unix.gettimeofday ());
      Ok ())
    (fun exn -> Lwt.return (Error (Printexc.to_string exn)))
;;

(** Builds and writes one frame. Caller must hold [state.mutex] so the sequence number and
    the bytes on the wire cannot be reordered. *)
let emit_locked st cfg ~msg_type ~(fill : builder -> unit) =
  match st.conn with
  | None -> Lwt.return (Error "FIX session not connected")
  | Some conn ->
    let builder = thread_builder () in
    reset builder;
    fill builder;
    let seq = st.seq_out in
    st.seq_out <- seq + 1;
    let sending_time = utc_timestamp_of_unix (Unix.gettimeofday ()) in
    let body = Buffer.contents builder.body in
    journal_add st { seq; msg_type; body; sending_time };
    let raw =
      finish
        builder
        ~msg_type
        ~seq
        ~sender:cfg.Kraken_fix_config.sender_comp_id
        ~target:cfg.target_comp_id
        ~sending_time
        ()
    in
    write_raw st conn raw
;;

let emit st cfg ~msg_type ~fill ~require_logon =
  Lwt_mutex.with_lock st.mutex (fun () ->
    if require_logon && (not st.logged_on) && not (Char.equal msg_type 'A')
    then Lwt.return (Error "FIX session not logged on")
    else emit_locked st cfg ~msg_type ~fill)
;;

(* -- Session-level messages ------------------------------------------ *)

let send_heartbeat st cfg ~test_req_id =
  emit st cfg ~msg_type:'0' ~require_logon:false ~fill:(fun b ->
    match test_req_id with
    | Some id -> str b 112 id
    | None -> ())
;;

let send_test_request st cfg ~test_req_id =
  emit st cfg ~msg_type:'1' ~require_logon:false ~fill:(fun b -> str b 112 test_req_id)
;;

let send_sequence_reset_locked st cfg ~seq ~new_seq =
  let builder = thread_builder () in
  reset builder;
  bool builder 123 true;
  int builder 36 new_seq;
  let sending_time = utc_timestamp_of_unix (Unix.gettimeofday ()) in
  journal_add
    st
    { seq; msg_type = '4'; body = Buffer.contents builder.body; sending_time };
  let raw =
    finish
      builder
      ~msg_type:'4'
      ~seq
      ~sender:cfg.Kraken_fix_config.sender_comp_id
      ~target:cfg.target_comp_id
      ~sending_time
      ()
  in
  match st.conn with
  | None -> Lwt.return (Error "FIX session not connected")
  | Some conn -> write_raw st conn raw
;;

(** Answers a [ResendRequest]: replay everything still journalled, and close the numbers
    we no longer hold with a gap fill so the gateway does not wait on them. Caller holds
    the mutex. *)
let resend_from_journal_locked st cfg ~begin_seq ~end_seq =
  let last = if end_seq <= 0 then st.seq_out - 1 else min end_seq (st.seq_out - 1) in
  let rec replay seq =
    if seq > last
    then Lwt.return (Ok ())
    else (
      let send_one () =
        match Hashtbl.find_opt st.journal seq with
        | Some entry ->
          let builder = create () in
          Buffer.add_string builder.body entry.body;
          let raw =
            finish
              builder
              ~msg_type:entry.msg_type
              ~seq
              ~sender:cfg.Kraken_fix_config.sender_comp_id
              ~target:cfg.target_comp_id
              ~sending_time:(utc_timestamp_of_unix (Unix.gettimeofday ()))
              ~poss_dup:true
              ~orig_sending_time:entry.sending_time
              ()
          in
          (match st.conn with
           | None -> Lwt.return (Error "FIX session not connected")
           | Some conn -> write_raw st conn raw)
        | None -> send_sequence_reset_locked st cfg ~seq ~new_seq:(seq + 1)
      in
      send_one ()
      >>= function
      | Ok () -> replay (seq + 1)
      | Error e -> Lwt.return (Error e))
  in
  replay begin_seq
;;

let send_logon_locked st cfg ~reset =
  match st.conn with
  | None -> Lwt.return (Error "FIX session not connected")
  | Some _ ->
    if reset
    then (
      (* Numbers restart, so every journalled frame belongs to a stream that no longer
         exists: keeping it would make eviction walk dead keys and would answer a resend
         with an order the gateway has already forgotten. *)
      st.seq_out <- 1;
      st.expected_seq_in <- 1;
      Hashtbl.reset st.journal;
      st.journal_oldest <- 1);
    st.logged_on <- false;
    let seq = st.seq_out in
    let nonce = Kraken_fix_auth.nonce () in
    let password =
      Kraken_fix_auth.password
        ~msg_seq_num:(string_of_int seq)
        ~sender_comp_id:cfg.Kraken_fix_config.sender_comp_id
        ~target_comp_id:cfg.target_comp_id
        ~api_key:cfg.api_key
        ~api_secret:cfg.api_secret
        ~nonce
    in
    emit_locked st cfg ~msg_type:'A' ~fill:(fun b ->
      int b 98 0;
      int b 108 cfg.Kraken_fix_config.heartbeat_s;
      str b 553 cfg.api_key;
      str b 554 password;
      str b 5025 nonce;
      bool b 141 reset;
      int b 8674 (if cfg.cancel_on_disconnect then 0 else 1))
;;

(* -- Inbound dispatch ------------------------------------------------ *)

let reject_text frame = Option.value (field frame 58) ~default:"rejected by Kraken"

(** Resolves the waiter the order path is blocked on. Runs on the reader fiber before the
    order-cache offload, so an acknowledgement wakes the strategy without waiting for the
    parse domain to drain. *)
let resolve_ack frame =
  match field frame 11 with
  | None -> ()
  | Some id ->
    let order_id = Option.value (field frame 37) ~default:"" in
    let rejected = Char.equal (Option.value (field_char frame 39) ~default:' ') '8' in
    let error () = Error (reject_text frame) in
    if Pending.mem state.places id
    then
      ignore
        (resolve_generic state.places id (fun () ->
           if rejected
           then error ()
           else
             Ok
               { Kraken_common_types.order_id
               ; cl_ord_id = Some id
               ; order_userref = Kraken_fix_order_ids.userref_of_cl_ord_id id
               }))
    else if Pending.mem state.amends id
    then
      ignore
        (resolve_generic state.amends id (fun () ->
           if rejected
           then error ()
           else
             Ok
               { Kraken_common_types.amend_id =
                   Option.value (field frame 17) ~default:"fix_replace"
               ; order_id
               ; cl_ord_id = Some id
               }))
    else if Pending.mem state.cancels id
    then
      ignore
        (resolve_generic state.cancels id (fun () ->
           if rejected
           then error ()
           else Ok { Kraken_common_types.order_id; cl_ord_id = field frame 41 }))
    else ()
;;

let rec process_frame st cfg (frame : view) =
  match msg_seq_num frame with
  | None ->
    (* A frame without [34] cannot be sequenced; treat it as a protocol error and move on
       rather than stall the session. *)
    Logging.error ~section "Kraken FIX frame without MsgSeqNum; dropped";
    Lwt.return_unit
  | Some seq ->
    if seq = st.expected_seq_in
    then (
      st.expected_seq_in <- seq + 1;
      st.resend_requested_for <- None;
      (match st.replay_window with
       | Some (floor, until) when floor >= until -> st.replay_window <- None
       | _ -> ());
      handle_message st cfg frame >>= fun () -> drain_out_of_order st cfg)
    else if seq > st.expected_seq_in
    then (
      let expected = st.expected_seq_in in
      (* A logon above our expected number means reports were produced while we were away.
         The logon must be handled now (it is what makes the session usable) and the
         missed range is then requested, with [replay_window] letting those replayed
         frames through even though the expected number has already moved past them. *)
      if Char.equal (Option.value (field_char frame 35) ~default:' ') 'A'
      then (
        st.replay_window <- Some (expected, seq);
        st.expected_seq_in <- seq + 1;
        st.resend_requested_for <- Some expected;
        handle_message st cfg frame
        >>= fun () ->
        Logging.warn_f
          ~section
          "Kraken FIX logon above the expected sequence (%d > %d); requesting the missed \
           range"
          seq
          expected;
        emit st cfg ~msg_type:'2' ~require_logon:false ~fill:(fun b ->
          int b 7 expected;
          int b 16 0)
        >|= ignore)
      else (
        let expected = st.expected_seq_in in
        if Hashtbl.length st.out_of_order < max_pending_inbound
        then Hashtbl.replace st.out_of_order seq (span_string frame 0 (view_length frame))
        else
          Logging.error_f
            ~section
            "Kraken FIX sequence gap buffer full (%d); dropping seq %d"
            max_pending_inbound
            seq;
        match st.resend_requested_for with
        | Some begin_ when begin_ = expected -> Lwt.return_unit
        | _ ->
          st.resend_requested_for <- Some expected;
          Logging.warn_f
            ~section
            "Kraken FIX sequence gap: expected %d, received %d; requesting resend"
            expected
            seq;
          emit st cfg ~msg_type:'2' ~require_logon:false ~fill:(fun b ->
            int b 7 expected;
            int b 16 0)
          >|= ignore))
    else if Char.equal (Option.value (field_char frame 35) ~default:' ') 'A'
    then (
      (* The gateway restarted its numbering (daily 22:00 UTC rollover, or an operator
         reset). Its logon is authoritative: adopt its sequence. *)
      Logging.info_f
        ~section
        "Kraken FIX session renumbered by gateway at seq %d (was %d); adopting"
        seq
        st.expected_seq_in;
      st.expected_seq_in <- seq + 1;
      Hashtbl.reset st.out_of_order;
      st.resend_requested_for <- None;
      handle_message st cfg frame)
    else if poss_dup frame
            &&
            match st.replay_window with
            | Some (floor, until) -> seq >= floor && seq < until
            | None -> false
    then (
      (* Replay of a report missed while disconnected. Processed without moving
         [expected_seq_in], and the floor advances so the same number cannot be applied
         twice if the gateway repeats it. *)
      (match st.replay_window with
       | Some (_, until) -> st.replay_window <- Some (seq + 1, until)
       | None -> ());
      Logging.info_f ~section "Applying replayed Kraken FIX frame seq %d" seq;
      handle_message st cfg frame)
    else if poss_dup frame
    then Lwt.return_unit
    else (
      Logging.debug_f ~section "Dropping duplicate Kraken FIX frame seq %d" seq;
      Lwt.return_unit)

and drain_out_of_order st cfg =
  match Hashtbl.find_opt st.out_of_order st.expected_seq_in with
  | None -> Lwt.return_unit
  | Some raw ->
    Hashtbl.remove st.out_of_order st.expected_seq_in;
    let bytes = Bytes.of_string raw in
    process_frame st cfg { buf = bytes; off = 0; len = Bytes.length bytes }
    >>= fun () -> drain_out_of_order st cfg

and handle_message st cfg (frame : view) =
  match msg_type frame with
  | None -> Lwt.return_unit
  | Some "0" ->
    (* Heartbeat. It doubles as the answer to our TestRequest. *)
    (match field frame 112 with
     | Some id ->
       if Pending.mem st.pings id then ignore (resolve_generic st.pings id (fun () -> ()))
     | None -> ());
    notify_heartbeat ();
    Lwt.return_unit
  | Some "1" ->
    (* TestRequest: the answer must echo TestReqID. *)
    send_heartbeat st cfg ~test_req_id:(field frame 112) >|= ignore
  | Some "2" ->
    let begin_seq = Option.value (field_int frame 7) ~default:0 in
    let end_seq = Option.value (field_int frame 16) ~default:0 in
    Logging.info_f ~section "Kraken FIX requested resend of %d..%d" begin_seq end_seq;
    Lwt_mutex.with_lock st.mutex (fun () ->
      resend_from_journal_locked st cfg ~begin_seq ~end_seq)
    >|= (function
     | Ok () -> ()
     | Error e -> Logging.error_f ~section "Kraken FIX resend failed: %s" e)
  | Some "3" ->
    let text = reject_text frame in
    Logging.error_f
      ~section
      "Kraken FIX session reject (refMsgType=%s refSeqNum=%d): %s"
      (Option.value (field frame 372) ~default:"?")
      (Option.value (field_int frame 45) ~default:0)
      text;
    (* A rejected logon means our sequence disagrees with the gateway; reset on the next
       attempt instead of looping on the same numbers. *)
    if Option.equal String.equal (field frame 372) (Some "A") || not st.logged_on
    then
      (* Either the gateway rejected our logon or we are being rejected before a session
         exists: our sequence numbers are the likely cause, so the retry resets them. *)
      st.reset_next_logon <- true;
    Lwt.return_unit
  | Some "4" ->
    let new_seq = Option.value (field_int frame 36) ~default:st.expected_seq_in in
    let gap_fill = Option.equal Char.equal (field_char frame 123) (Some 'Y') in
    Logging.info_f
      ~section
      "Kraken FIX sequence reset (%s) to %d"
      (if gap_fill then "gap fill" else "hard")
      new_seq;
    if (not gap_fill) || new_seq > st.expected_seq_in then st.expected_seq_in <- new_seq;
    Hashtbl.reset st.out_of_order;
    Lwt.return_unit
  | Some "5" ->
    Logging.warn_f ~section "Kraken FIX logout: %s" (reject_text frame);
    (* The gateway is retiring the session; the socket follows. Clear readiness now so no
       order is attempted against a session that is going away. *)
    st.logged_on <- false;
    Atomic.set st.connected false;
    Lwt.return_unit
  | Some "8" ->
    resolve_ack frame;
    Kraken_fix_execution_reports.ingest frame;
    notify_heartbeat ();
    Lwt.return_unit
  | Some "9" ->
    (match field frame 11 with
     | Some id when Pending.mem st.cancels id ->
       ignore (resolve_generic st.cancels id (fun () -> Error (reject_text frame)))
     | _ -> ());
    Lwt.return_unit
  | Some "j" ->
    let text = reject_text frame in
    (match field frame 379 with
     | Some id ->
       if Pending.mem st.places id
       then ignore (resolve_generic st.places id (fun () -> Error text))
       else if Pending.mem st.amends id
       then ignore (resolve_generic st.amends id (fun () -> Error text))
       else if Pending.mem st.cancels id
       then ignore (resolve_generic st.cancels id (fun () -> Error text))
       else
         Logging.warn_f
           ~section
           "Kraken FIX business reject for unknown ClOrdID %s: %s"
           id
           text
     | None -> Logging.warn_f ~section "Kraken FIX business reject: %s" text);
    Lwt.return_unit
  | Some "A" ->
    st.logged_on <- true;
    Atomic.set st.connected true;
    Lwt_condition.broadcast st.logged_on_signal ();
    notify_connection `Connected;
    Logging.info_f
      ~section
      "Kraken FIX logon accepted (next inbound seq %d, next outbound seq %d)"
      st.expected_seq_in
      st.seq_out;
    Lwt.return_unit
  | Some other ->
    Logging.debug_f ~section "Ignoring Kraken FIX message type %s" other;
    Lwt.return_unit
;;

(* -- Reading --------------------------------------------------------- *)

(** Reads until the socket dies and returns why. Each frame is copied out of the read
    buffer before any suspension, so a concurrent [framer_feed] cannot overwrite it. *)
let read_loop st cfg conn =
  let chunk = Bytes.create 8192 in
  let rec loop () =
    Lwt.catch
      (fun () -> Lwt_io.read_into conn.ic chunk 0 (Bytes.length chunk))
      (fun exn -> Lwt.fail exn)
    >>= fun read ->
    if read = 0
    then Lwt.return "connection closed by Kraken"
    else (
      Atomic.set st.last_inbound (Unix.gettimeofday ());
      Atomic.set st.connected st.logged_on;
      framer_feed conn.framer chunk ~off:0 ~count:read;
      let rec drain () =
        match framer_next conn.framer with
        | None -> Lwt.return_unit
        | Some frame ->
          let bytes = Bytes.sub frame.buf frame.off (view_length frame) in
          let view = { buf = bytes; off = 0; len = Bytes.length bytes } in
          if not (checksum_ok view)
          then
            Logging.error_f
              ~section
              "Kraken FIX checksum mismatch (stored %03d, computed %03d); applying anyway"
              (stored_checksum view)
              (computed_checksum view);
          process_frame st cfg view >>= drain
      in
      drain () >>= loop)
  in
  Lwt.catch loop (fun exn -> Lwt.return (Printexc.to_string exn))
;;

(* -- Heartbeat watchdog ---------------------------------------------- *)

(** Keeps the session demonstrably alive: probes after half again the negotiated interval
    without traffic, and tears the connection down when the gateway goes fully silent.
    Both sides send heartbeats, so silence means the path is broken even if the socket
    still looks open. *)
let start_watchdog st cfg conn =
  let interval = float_of_int cfg.Kraken_fix_config.heartbeat_s in
  let probe_counter = ref 0 in
  let alive () =
    match st.conn with
    | Some c -> c.generation = conn.generation
    | None -> false
  in
  let rec loop () =
    Lwt_unix.sleep (Float.max 1.0 (interval /. 2.0))
    >>= fun () ->
    (if alive () && st.logged_on
     then (
       let now = Unix.gettimeofday () in
       let inbound_silence = now -. Atomic.get st.last_inbound in
       let outbound_silence = now -. Atomic.get st.last_outbound in
       if inbound_silence > interval *. 2.5
       then (
         Logging.error_f
           ~section
           "Kraken FIX silent for %.0fs (>%.0fs); dropping the connection"
           inbound_silence
           (interval *. 2.5);
         Lwt.join
           [ Lwt.catch (fun () -> Lwt_io.abort conn.oc) (fun _ -> Lwt.return_unit)
           ; Lwt.catch (fun () -> Lwt_io.abort conn.ic) (fun _ -> Lwt.return_unit)
           ])
       else if inbound_silence > interval *. 1.5
               && now -. st.test_request_sent_at > interval
       then (
         st.test_request_sent_at <- now;
         incr probe_counter;
         send_test_request st cfg ~test_req_id:(Printf.sprintf "dio-%d" !probe_counter)
         >|= ignore)
       else if outbound_silence > interval
       then send_heartbeat st cfg ~test_req_id:None >|= ignore
       else Lwt.return_unit)
     else Lwt.return_unit)
    >>= fun () -> if alive () then loop () else Lwt.return_unit
  in
  Lwt.async loop
;;

(* -- Dialling -------------------------------------------------------- *)

(** Resolves at connection time, never caches: Kraken's DNS TTL is five minutes and the
    gateways move. Each address gets a slice of one budget so a blackholed A record cannot
    consume the whole attempt. *)
let dial cfg =
  let ctx = Kraken_common_types.get_conduit_ctx () in
  let timeout_s = cfg.Kraken_fix_config.connect_timeout_s in
  let deadline = Unix.gettimeofday () +. timeout_s in
  let failf fmt = Printf.ksprintf (fun msg -> Lwt.fail (Failure msg)) fmt in
  Lwt.catch
    (fun () ->
      Lwt_unix.with_timeout timeout_s (fun () ->
        Lwt_unix.getaddrinfo
          cfg.host
          (string_of_int cfg.port)
          [ Unix.AI_FAMILY Unix.PF_INET ]))
    (function
      | Lwt_unix.Timeout ->
        failf "Kraken FIX connect %s:%d: resolution timed out" cfg.host cfg.port
      | exn -> Lwt.fail exn)
  >>= fun addresses ->
  let ips =
    List.filter_map
      (fun { Unix.ai_addr; _ } ->
        match ai_addr with
        | Unix.ADDR_INET (addr, _) -> Some addr
        | Unix.ADDR_UNIX _ -> None)
      addresses
    |> List.sort_uniq compare
  in
  let total = List.length ips in
  if total = 0
  then failf "Kraken FIX connect %s:%d: no IPv4 address" cfg.host cfg.port
  else (
    let slice =
      if total = 1 then timeout_s else Float.min 5.0 (timeout_s /. float_of_int total)
    in
    let rec try_ips tried = function
      | [] ->
        failf
          "Kraken FIX connect %s:%d failed on all %d address(es) [%s]"
          cfg.host
          cfg.port
          total
          (String.concat ", " (List.rev tried))
      | ip :: rest ->
        let budget = Float.min slice (deadline -. Unix.gettimeofday ()) in
        if budget <= 0.0
        then failf "Kraken FIX connect %s:%d: budget exhausted" cfg.host cfg.port
        else (
          let client =
            `TLS (`Hostname cfg.host, `IP (Ipaddr_unix.of_inet_addr ip), `Port cfg.port)
          in
          Lwt.catch
            (fun () ->
              Lwt_unix.with_timeout budget (fun () ->
                Conduit_lwt_unix.connect ~ctx client)
              >>= fun (flow, ic, oc) ->
              (* Order entry is latency-bound: Nagle must not batch our frames. *)
              let () =
                match flow with
                | Conduit_lwt_unix.TCP { fd; _ } ->
                  Lwt_unix.setsockopt fd Lwt_unix.TCP_NODELAY true
                | Conduit_lwt_unix.Domain_socket _ -> ()
              in
              Lwt.return ((flow, ic, oc), Unix.string_of_inet_addr ip))
            (fun exn ->
              try_ips
                (Printf.sprintf
                   "%s: %s"
                   (Unix.string_of_inet_addr ip)
                   (Printexc.to_string exn)
                 :: tried)
                rest))
    in
    try_ips [] ips)
;;

(* -- Connection lifecycle -------------------------------------------- *)

let await_logon st cfg conn =
  let deadline = Unix.gettimeofday () +. cfg.Kraken_fix_config.logon_timeout_s in
  let rec wait () =
    if st.logged_on
    then Lwt.return (Ok ())
    else if st.conn == None
            ||
            match st.conn with
            | Some c -> c.generation <> conn.generation
            | None -> true
    then Lwt.return (Error "FIX connection dropped during logon")
    else (
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0
      then Lwt.return (Error "FIX logon timed out")
      else
        Lwt.pick
          [ (Lwt_condition.wait st.logged_on_signal >|= fun () -> `Signal)
          ; (Lwt_unix.sleep (Float.min remaining 1.0) >|= fun () -> `Tick)
          ]
        >>= fun _ -> wait ())
  in
  wait ()
;;

(** Tears a session down. [notify] is false when the caller is about to report the failure
    itself: the supervisor must see exactly one failure per attempt, or it schedules two
    reconnects and counts the attempt twice against its breaker. *)
let teardown ?(notify = true) st conn reason =
  Lwt_mutex.with_lock st.mutex (fun () ->
    let current =
      match st.conn with
      | Some c -> c.generation = conn.generation
      | None -> false
    in
    if current
    then (
      st.conn <- None;
      st.logged_on <- false;
      st.connecting <- false;
      st.connection_generation <- st.connection_generation + 1;
      st.resend_requested_for <- None;
      st.replay_window <- None;
      Hashtbl.reset st.out_of_order;
      Atomic.set st.connected false;
      Lwt_condition.broadcast st.logged_on_signal ();
      Lwt_condition.broadcast st.session_closed ());
    Lwt.return current)
  >>= fun current ->
  (* A superseded teardown must not fail the live session's in-flight requests. *)
  if current then fail_all_pending reason;
  Lwt.catch
    (fun () -> Lwt.join [ Lwt_io.close conn.ic; Lwt_io.close conn.oc ])
    (fun _ -> Lwt.return_unit)
  >|= fun () ->
  if current && notify
  then (
    notify_connection (`Disconnected reason);
    match st.on_failure with
    | Some f -> f reason
    | None -> ())
;;

(** Establishes one session and blocks until it dies. The supervisor re-invokes this after
    the returned promise resolves, exactly as it does for the WebSocket transport. *)
let connect_and_monitor ~token:_ ~on_failure ~on_connected =
  match config_or_none () with
  | None ->
    Lwt.fail_with "Kraken FIX credentials are missing; cannot start the FIX session"
  | Some cfg ->
    Lwt_mutex.with_lock state.mutex (fun () ->
      state.on_failure <- Some on_failure;
      if state.conn <> None
      then Lwt.return `Already
      else if state.connecting
      then Lwt.return `Wait
      else (
        state.connecting <- true;
        Lwt.return `Connect))
    >>= (function
     | `Already | `Wait ->
       (* A redundant attempt: a session is already up or being established. Park until it
          goes away rather than resolving the supervisor's connect function now, which it
          would read as an unexpected completion and turn into another restart. *)
       let rec wait_for_close () =
         if state.conn = None && not state.connecting
         then Lwt.return_unit
         else
           Lwt.pick
             [ (Lwt_condition.wait state.session_closed >|= fun () -> `Closed)
             ; (Lwt_unix.sleep 1.0 >|= fun () -> `Tick)
             ]
           >>= fun _ -> wait_for_close ()
       in
       wait_for_close ()
     | `Connect ->
       Lwt.catch
         (fun () ->
           dial cfg
           >>= fun ((flow, ic, oc), dialed_ip) ->
           Logging.info_f
             ~section
             "Kraken FIX connected to %s:%d via %s"
             cfg.host
             cfg.port
             dialed_ip;
           let generation = state.connection_generation + 1 in
           let conn =
             { ic; oc; framer = framer_create ~capacity:16384; flow; generation }
           in
           Lwt_mutex.with_lock state.mutex (fun () ->
             state.connection_generation <- generation;
             state.conn <- Some conn;
             state.connecting <- false;
             state.logged_on <- false;
             state.resend_requested_for <- None;
             state.replay_window <- None;
             Hashtbl.reset state.out_of_order;
             Atomic.set state.last_inbound (Unix.gettimeofday ());
             Atomic.set state.last_outbound (Unix.gettimeofday ());
             let reset = state.reset_next_logon in
             state.reset_next_logon <- false;
             send_logon_locked state cfg ~reset)
           >>= function
           | Error e ->
             teardown ~notify:false state conn e >>= fun () -> Lwt.fail (Failure e)
           | Ok () ->
             start_watchdog state cfg conn;
             await_logon state cfg conn
             >>= (function
              | Error e ->
                teardown ~notify:false state conn e >>= fun () -> Lwt.fail (Failure e)
              | Ok () ->
                on_connected ();
                (* One order cache: FIX reports feed the same store the WebSocket snapshot
                   would have seeded, so seed it from REST now. *)
                Kraken_executions_feed.mark_feed_ready ();
                Kraken_executions_feed.request_open_order_bootstrap ();
                read_loop state cfg conn >>= fun reason -> teardown state conn reason))
         (fun exn ->
           let reason = Printexc.to_string exn in
           Lwt_mutex.with_lock state.mutex (fun () ->
             state.connecting <- false;
             Lwt_condition.broadcast state.logged_on_signal ();
             Lwt_condition.broadcast state.session_closed ();
             Lwt.return_unit)
           >>= fun () ->
           Logging.error_f ~section "Kraken FIX connect failed: %s" reason;
           on_failure reason;
           Lwt.return_unit))
;;

(** Closes the session. Used at shutdown: with [8674=1] the working orders stay on the
    account, matching what a WebSocket disconnect does. *)
let close () =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match state.conn with
    | None -> Lwt.return None
    | Some conn ->
      (* A Logout lets the gateway retire the session cleanly instead of waiting for its
         heartbeat to expire. Best effort: the socket is closed either way. *)
      (match config_or_none () with
       | Some cfg ->
         emit_locked state cfg ~msg_type:'5' ~fill:(fun b -> str b 58 "dio shutdown")
         >|= fun _ -> ()
       | None -> Lwt.return_unit)
      >>= fun () ->
      state.conn <- None;
      state.logged_on <- false;
      state.connecting <- false;
      state.connection_generation <- state.connection_generation + 1;
      Atomic.set state.connected false;
      Lwt_condition.broadcast state.logged_on_signal ();
      Lwt_condition.broadcast state.session_closed ();
      Lwt.return (Some conn))
  >>= function
  | None -> Lwt.return_unit
  | Some conn ->
    fail_all_pending "FIX session closed";
    notify_connection (`Disconnected "client_closed");
    Lwt.catch
      (fun () -> Lwt.join [ Lwt_io.close conn.ic; Lwt_io.close conn.oc ])
      (fun _ -> Lwt.return_unit)
;;

(* -- Request entry points -------------------------------------------- *)

(** Registers the waiter before writing, so an acknowledgement that arrives faster than
    the write completes cannot be lost, and bounds the wait so a dead gateway surfaces to
    the strategy instead of hanging its fiber. *)
let wait_for_resolution ~table ~key ~timeout_ms waiter =
  let timeout_s = float_of_int timeout_ms /. 1000.0 in
  Lwt.pick
    [ (waiter >|= fun resolved -> `Resolved resolved)
    ; (Lwt_unix.sleep timeout_s >|= fun () -> `Timeout)
    ]
  >>= function
  | `Resolved resolved -> Lwt.return resolved
  | `Timeout ->
    Pending.remove table key;
    let message =
      Printf.sprintf
        "Timed out after %dms waiting for the FIX acknowledgement of %s"
        timeout_ms
        key
    in
    Logging.warn_f ~section "%s" message;
    Lwt.return (Error message)
;;

let submit_generic ~table ~key ~msg_type ~timeout_ms ~fill =
  match config_or_none () with
  | None -> Lwt.return (Error "Kraken FIX session is not configured")
  | Some cfg ->
    if Pending.mem table key
    then Lwt.return (Error (Printf.sprintf "Kraken FIX ClOrdID collision for %s" key))
    else (
      let waiter, wakener = Lwt.wait () in
      Pending.replace table key wakener;
      emit state cfg ~msg_type ~require_logon:true ~fill
      >>= function
      | Error e ->
        Pending.remove table key;
        Lwt.return (Error e)
      | Ok () -> wait_for_resolution ~table ~key ~timeout_ms waiter)
;;

(** Places one order and returns once the execution report acknowledges it (or a business
    reject resolves it). *)
let submit_place ?order_userref ~cl_ord_id ~timeout_ms ~fill () =
  Kraken_fix_order_ids.note ?userref:order_userref ~cl_ord_id ();
  submit_generic ~table:state.places ~key:cl_ord_id ~msg_type:'D' ~timeout_ms ~fill
;;

let submit_amend ~cl_ord_id ~timeout_ms ~fill () =
  submit_generic ~table:state.amends ~key:cl_ord_id ~msg_type:'G' ~timeout_ms ~fill
;;

let submit_cancel ~cl_ord_id ~timeout_ms ~fill () =
  submit_generic ~table:state.cancels ~key:cl_ord_id ~msg_type:'F' ~timeout_ms ~fill
;;

(** Session-level liveness probe, mirroring the WebSocket client's ping so the health
    monitor needs no special case: a TestRequest is sent and the echo heartbeat resolves
    it. *)
let send_ping ~req_id ~timeout_ms : Kraken_common_types.ws_response Lwt.t =
  let key = string_of_int req_id in
  let started = Mtime_clock.now_ns () in
  let waiter, wakener = Lwt.wait () in
  Pending.replace state.pings key wakener;
  send_test_request state (Lazy.force config) ~test_req_id:key
  >>= function
  | Error e ->
    Pending.remove state.pings key;
    Lwt.fail_with e
  | Ok () ->
    Lwt.pick
      [ (waiter >|= fun () -> `Pong)
      ; (Lwt_unix.sleep (float_of_int timeout_ms /. 1000.0) >|= fun () -> `Timeout)
      ]
    >>= (function
     | `Timeout ->
       Pending.remove state.pings key;
       Lwt.fail_with (Printf.sprintf "Timeout waiting for FIX heartbeat (%s)" key)
     | `Pong ->
       Network_latency.record_ping
         "kraken"
         (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) started));
       Lwt.return
         { Kraken_common_types.method_ = "ping"
         ; success = true
         ; req_id = Some req_id
         ; time_in = ""
         ; time_out = ""
         ; result = None
         ; error = None
         ; warnings = None
         })
;;
