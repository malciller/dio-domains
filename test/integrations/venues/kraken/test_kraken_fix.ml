(** Unit tests for the Unified FIX workflow: wire codec, framing, logon signing,
    execution-report translation, id allocation and tier selection. No network. *)

module Codec = Kraken.Kraken_fix.Kraken_fix_codec
module Auth = Kraken.Kraken_fix.Kraken_fix_auth
module Reports = Kraken.Kraken_fix.Kraken_fix_execution_reports
module Ids = Kraken.Kraken_fix.Kraken_fix_order_ids
module Actions = Kraken.Kraken_fix.Kraken_fix_actions
module Feed = Kraken.Kraken_executions_feed
module Tier = Kraken.Kraken_account_tier
module Client = Kraken.Kraken_fix.Kraken_fix_client
module Config = Kraken.Kraken_fix.Kraken_fix_config

let soh = Codec.soh

(* Builds a frame the way the session layer does, so the tests exercise the same path a
   live session takes. *)
let build ?(seq = 1) ?(poss_dup = false) ~sender ~target ~msg_type ~sending_time fill =
  let b = Codec.create () in
  fill b;
  Codec.finish b ~msg_type ~seq ~sender ~target ~sending_time ~poss_dup ()
;;

let view_of raw =
  let bytes = Bytes.of_string raw in
  { Codec.buf = bytes; off = 0; len = Bytes.length bytes }
;;

let test_format_float () =
  let check = Alcotest.(check string) in
  check "integral price drops the fraction" "84000" (Codec.format_float 84000.0);
  check "trailing zeros are trimmed" "0.001" (Codec.format_float 0.001);
  check "precision is honoured" "0.12346" (Codec.format_float ~precision:5 0.123456);
  check "zero stays zero" "0" (Codec.format_float 0.0);
  check "no exponent notation" "0.00000001" (Codec.format_float 1e-8)
;;

(** [9 BodyLength]: bytes after the field's own SOH up to and including the SOH that
    precedes [10]. *)
let body_length_of raw =
  let nine = String.index_from raw 0 '9' in
  let body_start = String.index_from raw nine soh + 1 in
  let rec trailer_at i =
    if i + 4 >= String.length raw
    then Alcotest.fail "no trailer in frame"
    else if raw.[i] = soh && raw.[i + 1] = '1' && raw.[i + 2] = '0' && raw.[i + 3] = '='
    then i
    else trailer_at (i + 1)
  in
  trailer_at body_start + 1 - body_start
;;

let test_message_round_trip () =
  let raw =
    build
      ~sender:"DIO"
      ~target:"KRAKEN-TRD"
      ~msg_type:'D'
      ~sending_time:"20260407-14:32:05.000"
      (fun b ->
         Codec.str b 11 "1623448294234000";
         Codec.str b 55 "BTC/USD";
         Codec.char b 54 '1';
         Codec.char b 40 '2';
         Codec.float b 44 84000.0;
         Codec.float b 38 0.001)
  in
  let frame = view_of raw in
  Alcotest.(check (option string)) "magic" (Some "FIX.4.4") (Codec.field frame 8);
  Alcotest.(check (option string)) "msg type" (Some "D") (Codec.msg_type frame);
  Alcotest.(check (option int)) "seq" (Some 1) (Codec.msg_seq_num frame);
  Alcotest.(check (option string)) "sender" (Some "DIO") (Codec.field frame 49);
  Alcotest.(check (option string)) "target" (Some "KRAKEN-TRD") (Codec.field frame 56);
  Alcotest.(check (option string))
    "cl ord id"
    (Some "1623448294234000")
    (Codec.field frame 11);
  Alcotest.(check (option string)) "symbol" (Some "BTC/USD") (Codec.field frame 55);
  Alcotest.(check (option char)) "side" (Some '1') (Codec.field_char frame 54);
  Alcotest.(check (option (float 0.0000001)))
    "price"
    (Some 84000.0)
    (Codec.field_float frame 44);
  Alcotest.(check (option (float 0.0000001)))
    "qty"
    (Some 0.001)
    (Codec.field_float frame 38);
  Alcotest.(check bool) "checksum validates" true (Codec.checksum_ok frame);
  Alcotest.(check int)
    "body length counts header+body, not the trailer"
    (Option.value (Codec.field_int frame 9) ~default:0)
    (body_length_of raw)
;;

(** The published Kraken sample, verbatim, is the ground truth for both byte counts. *)
let test_documented_sample_round_trips () =
  let sample =
    "8=FIX.4.4|9=148|35=D|34=2|49=CLIENT|56=KRAKEN-TRD|52=20260407-14:32:05.000|11=1744036325000000|55=BTC/USD|54=1|40=2|44=84000|38=0.001|59=1|60=20260407-14:32:05.000|10=141|"
  in
  let raw = String.map (fun c -> if c = '|' then soh else c) sample in
  let frame = view_of raw in
  Alcotest.(check bool) "documented checksum" true (Codec.checksum_ok frame);
  Alcotest.(check int)
    "documented body length"
    148
    (Option.value (Codec.field_int frame 9) ~default:0);
  Alcotest.(check int)
    "our body-length definition agrees"
    (Option.value (Codec.field_int frame 9) ~default:0)
    (body_length_of raw)
;;

let test_checksum_detects_corruption () =
  let raw =
    build
      ~sender:"DIO"
      ~target:"KRAKEN-TRD"
      ~msg_type:'0'
      ~sending_time:"20260407-14:32:05.000"
      (fun _ -> ())
  in
  let frame = view_of raw in
  Alcotest.(check bool) "clean frame passes" true (Codec.checksum_ok frame);
  let corrupted = Bytes.of_string raw in
  let index = String.index_from raw 0 '3' in
  Bytes.set corrupted index (Char.chr (Char.code '3' + 1));
  let bad = { Codec.buf = corrupted; off = 0; len = Bytes.length corrupted } in
  Alcotest.(check bool) "corrupted frame fails" false (Codec.checksum_ok bad)
;;

let test_timestamp_round_trip () =
  let t = 1_775_572_325.123 in
  let formatted = Codec.utc_timestamp_of_unix t in
  Alcotest.(check string) "utc format" "20260407-14:32:05.123" formatted;
  match Codec.unix_of_utc_timestamp formatted with
  | None -> Alcotest.fail "timestamp did not parse"
  | Some parsed ->
    Alcotest.(check (float 0.001)) "round trip" t parsed;
    Alcotest.(check (option (float 0.001)))
      "fraction optional"
      (Some (1_775_572_325.0 +. 0.5))
      (Codec.unix_of_utc_timestamp "20260407-14:32:05.500");
    Alcotest.(check bool)
      "garbage rejected"
      true
      (Option.is_none (Codec.unix_of_utc_timestamp "not-a-timestamp"))
;;

let test_framer_splits_and_coalesces () =
  let message seq =
    build
      ~seq
      ~sender:"KRAKEN-TRD"
      ~target:"DIO"
      ~msg_type:'8'
      ~sending_time:"20260407-14:32:05.000"
      (fun b ->
         Codec.str b 37 "ORDER-1";
         Codec.str b 55 "BTC/USD")
  in
  let first = message 1 in
  let second = message 2 in
  (* One frame delivered in fixed-size chunks must stay invisible until it is complete. *)
  let framer = Codec.framer_create ~capacity:256 in
  let rec feed_all raw offset =
    if offset < String.length raw
    then (
      let count = min 37 (String.length raw - offset) in
      Codec.framer_feed
        framer
        (Bytes.of_string (String.sub raw offset count))
        ~off:0
        ~count;
      feed_all raw (offset + count))
  in
  let partial = String.sub first 0 (String.length first - 12) in
  feed_all partial 0;
  Alcotest.(check bool)
    "incomplete frame held back"
    true
    (Option.is_none (Codec.framer_next framer));
  Codec.framer_feed
    framer
    (Bytes.of_string (String.sub first (String.length first - 12) 12))
    ~off:0
    ~count:12;
  let completed = Codec.framer_next framer in
  Alcotest.(check (option int))
    "completed frame seq"
    (Some 1)
    (Option.bind completed Codec.msg_seq_num);
  (* Two frames in one read are yielded separately. *)
  let combined = first ^ second in
  let framer = Codec.framer_create ~capacity:256 in
  Codec.framer_feed
    framer
    (Bytes.of_string combined)
    ~off:0
    ~count:(String.length combined);
  let first_frame = Codec.framer_next framer in
  let second_frame = Codec.framer_next framer in
  Alcotest.(check (option int))
    "first frame seq"
    (Some 1)
    (Option.bind first_frame Codec.msg_seq_num);
  Alcotest.(check (option int))
    "second frame seq"
    (Some 2)
    (Option.bind second_frame Codec.msg_seq_num);
  Alcotest.(check bool) "no third frame" true (Option.is_none (Codec.framer_next framer))
;;

let test_framer_resyncs_after_garbage () =
  let raw =
    build
      ~sender:"KRAKEN-TRD"
      ~target:"DIO"
      ~msg_type:'0'
      ~sending_time:"20260407-14:32:05.000"
      (fun _ -> ())
  in
  let noisy = "garbage" ^ raw in
  let framer = Codec.framer_create ~capacity:256 in
  Codec.framer_feed framer (Bytes.of_string noisy) ~off:0 ~count:(String.length noisy);
  match Codec.framer_next framer with
  | None -> Alcotest.fail "frame after garbage was not found"
  | Some frame ->
    Alcotest.(check (option string)) "resynced" (Some "0") (Codec.msg_type frame)
;;

let test_logon_password_matches_documented_construction () =
  (* Vector computed independently from the construction in Kraken's FIX authentication
     guide: base64(HMAC-SHA512(base64decode(secret), SHA256(input ^ nonce))). *)
  let input =
    Auth.message_input
      ~msg_seq_num:"1"
      ~sender_comp_id:"DIO"
      ~target_comp_id:"KRAKEN-TRD"
      ~api_key:"fix-api-key"
  in
  Alcotest.(check string)
    "signed input"
    (Printf.sprintf
       "35=A%c34=1%c49=DIO%c56=KRAKEN-TRD%c553=fix-api-key%c"
       soh
       soh
       soh
       soh
       soh)
    input;
  let password =
    Auth.password
      ~msg_seq_num:"1"
      ~sender_comp_id:"DIO"
      ~target_comp_id:"KRAKEN-TRD"
      ~api_key:"fix-api-key"
      ~api_secret:"ZGlvLWZpeC1zZWNyZXQ="
      ~nonce:"1700000000000"
  in
  Alcotest.(check string)
    "hmac-sha512 over sha256"
    "RlpUS2M3wT6ADNt2qiqa+cZVywvH1E4IRbpdmdka90U+iAx5wouTanIQxlnE2eJhcZbP5zcJ3cp6wJULpebveQ=="
    password
;;

let test_execution_report_translation () =
  (* A fill, in the shape Kraken documents for ExecType=F. *)
  let raw =
    build
      ~sender:"KRAKEN-TRD"
      ~target:"DIO"
      ~msg_type:'8'
      ~sending_time:"20260407-14:32:05.122"
      (fun b ->
         Codec.str b 11 "1623448294234000";
         Codec.str b 37 "OQNCZM-NVAVC-AVD2LO";
         Codec.str b 55 "BTC/USD";
         Codec.char b 54 '1';
         Codec.char b 39 '2';
         Codec.char b 40 '2';
         Codec.float b 44 84000.0;
         Codec.float b 38 0.001;
         Codec.float b 14 0.001;
         Codec.float b 151 0.0;
         Codec.float b 31 84000.0;
         Codec.float b 32 0.001;
         Codec.float b 6 84000.0;
         Codec.float b 137 0.168;
         Codec.float b 381 84.0;
         Codec.char b 150 'F';
         Codec.str b 60 "20260407-14:32:05.122")
  in
  match Reports.event_of_frame (view_of raw) with
  | None -> Alcotest.fail "execution report did not translate"
  | Some (event : Feed.execution_event) ->
    Alcotest.(check string) "order id" "OQNCZM-NVAVC-AVD2LO" event.order_id;
    Alcotest.(check string) "symbol" "BTC/USD" event.symbol;
    Alcotest.(check (option string)) "cl ord id" (Some "1623448294234000") event.cl_ord_id;
    Alcotest.(check (float 0.0000001)) "order qty" 0.001 event.order_qty;
    Alcotest.(check (float 0.0000001)) "cum qty" 0.001 event.cum_qty;
    Alcotest.(check (float 0.0000001))
      "last px"
      84000.0
      (Option.value event.last_price ~default:0.0);
    Alcotest.(check (float 0.0000001)) "fee" 0.168 (Option.value event.fee ~default:0.0);
    Alcotest.(check bool) "trade exec type" true (event.exec_type = Feed.Trade);
    Alcotest.(check bool) "filled status" true (event.order_status = Feed.FilledStatus);
    Alcotest.(check bool) "buy side" true (event.side = Feed.Buy);
    Alcotest.(check (float 0.001)) "venue time parsed" 1_775_572_325.122 event.venue_time
;;

let test_execution_report_mappings () =
  Alcotest.(check bool) "exec type 0 is new" true (Reports.exec_type_of '0' = Feed.New);
  Alcotest.(check bool)
    "exec type 4 is cancel"
    true
    (Reports.exec_type_of '4' = Feed.Canceled);
  Alcotest.(check bool)
    "exec type 5 is amend"
    true
    (Reports.exec_type_of '5' = Feed.Amended);
  Alcotest.(check bool) "exec type F is trade" true (Reports.exec_type_of 'F' = Feed.Trade);
  Alcotest.(check bool)
    "ord status 1 is partial"
    true
    (Reports.order_status_of '1' = Feed.PartiallyFilledStatus);
  Alcotest.(check bool)
    "ord status 5 stays working"
    true
    (Reports.order_status_of '5' = Feed.NewStatus);
  Alcotest.(check bool)
    "ord status 8 is rejected"
    true
    (Reports.order_status_of '8' = Feed.RejectedStatus);
  Alcotest.(check bool)
    "terminal statuses"
    true
    (Reports.is_terminal Feed.FilledStatus
     && Reports.is_terminal Feed.CanceledStatus
     && not (Reports.is_terminal Feed.PartiallyFilledStatus))
;;

let test_order_id_allocation () =
  let first = Ids.next () in
  let second = Ids.next () in
  Alcotest.(check bool)
    "strictly increasing"
    true
    (Int64.compare (Int64.of_string first) (Int64.of_string second) < 0);
  Alcotest.(check bool) "fits the 18-char limit" true (String.length first <= 18);
  Ids.note ~userref:4242 ~cl_ord_id:first ();
  Alcotest.(check (option int))
    "userref recorded"
    (Some 4242)
    (Ids.userref_of_cl_ord_id first);
  Ids.forget first;
  Alcotest.(check (option int))
    "userref dropped on terminal"
    None
    (Ids.userref_of_cl_ord_id first);
  Alcotest.(check (option int)) "unknown id" None (Ids.userref_of_cl_ord_id "nope")
;;

let test_order_mappings () =
  Alcotest.(check (option char)) "limit" (Some '2') (Actions.ord_type_of "limit");
  Alcotest.(check (option char)) "market" (Some '1') (Actions.ord_type_of "market");
  Alcotest.(check (option char))
    "take profit"
    (Some 'R')
    (Actions.ord_type_of "take-profit");
  Alcotest.(check (option char))
    "settl-position is not expressible"
    None
    (Actions.ord_type_of "settl-position");
  Alcotest.(check (option char)) "buy" (Some '1') (Actions.side_code "buy");
  Alcotest.(check (option char)) "sell" (Some '2') (Actions.side_code "sell");
  Alcotest.(check (option char)) "gtc" (Some '1') (Actions.tif_code "GTC");
  Alcotest.(check (option char)) "ioc" (Some '3') (Actions.tif_code "IOC");
  Alcotest.(check (option char)) "fok" (Some '4') (Actions.tif_code "FOK")
;;

(* -- Session state machine ------------------------------------------- *)

let session_config =
  { Config.host = "localhost"
  ; port = 4001
  ; sender_comp_id = "DIO"
  ; target_comp_id = "KRAKEN-TRD"
  ; api_key = "fix-api-key"
  ; api_secret = "ZGlvLWZpeC1zZWNyZXQ="
  ; heartbeat_s = 30
  ; cancel_on_disconnect = false
  ; connect_timeout_s = 1.0
  ; logon_timeout_s = 1.0
  ; request_timeout_ms = 100
  }
;;

let reset_session () =
  Client.state.expected_seq_in <- 1;
  Client.state.logged_on <- false;
  Client.state.replay_window <- None;
  Client.state.resend_requested_for <- None;
  Hashtbl.reset Client.state.out_of_order
;;

let logon_frame seq =
  build
    ~seq
    ~sender:"KRAKEN-TRD"
    ~target:"DIO"
    ~msg_type:'A'
    ~sending_time:"20260407-14:32:05.000"
    (fun b -> Kraken.Kraken_fix.Kraken_fix_codec.int b 108 30)
;;

(** A gateway that answers the logon above our expected number (reports were produced
    while we were disconnected) must still bring the session up, and must open a replay
    window for the missed range instead of waiting for a gap that can no longer arrive. *)
let test_logon_above_expected_opens_replay_window () =
  reset_session ();
  Lwt_main.run
    (Client.process_frame Client.state session_config (view_of (logon_frame 5)));
  Alcotest.(check bool) "session is logged on" true (Client.is_connected ());
  Alcotest.(check int) "sequence adopted" 6 Client.state.expected_seq_in;
  Alcotest.(check (option (pair int int)))
    "replay window covers the missed range"
    (Some (1, 5))
    Client.state.replay_window
;;

(** A replayed report is applied once and then falls out of the window, so a gateway that
    repeats the same sequence number cannot double-apply a fill. *)
let test_replay_window_accepts_each_frame_once () =
  reset_session ();
  Client.state.replay_window <- Some (2, 5);
  Client.state.expected_seq_in <- 7;
  let heartbeat =
    build
      ~seq:2
      ~poss_dup:true
      ~sender:"KRAKEN-TRD"
      ~target:"DIO"
      ~msg_type:'0'
      ~sending_time:"20260407-14:32:05.000"
      (fun _ -> ())
  in
  Lwt_main.run (Client.process_frame Client.state session_config (view_of heartbeat));
  Alcotest.(check (option (pair int int)))
    "floor advanced past the replayed frame"
    (Some (3, 5))
    Client.state.replay_window;
  (* The same number again is now below the floor and must be ignored. *)
  Lwt_main.run (Client.process_frame Client.state session_config (view_of heartbeat));
  Alcotest.(check (option (pair int int)))
    "floor unchanged on repeat"
    (Some (3, 5))
    Client.state.replay_window;
  Alcotest.(check int)
    "expected sequence untouched by replays"
    7
    Client.state.expected_seq_in
;;

(** A frame far ahead of the expected number is held back until the gap is replayed, and a
    resend is requested exactly once. *)
let test_gap_holds_frame_and_requests_resend () =
  reset_session ();
  let update =
    build
      ~seq:4
      ~sender:"KRAKEN-TRD"
      ~target:"DIO"
      ~msg_type:'0'
      ~sending_time:"20260407-14:32:05.000"
      (fun _ -> ())
  in
  Lwt_main.run (Client.process_frame Client.state session_config (view_of update));
  Alcotest.(check int) "one frame held" 1 (Hashtbl.length Client.state.out_of_order);
  Alcotest.(check (option int))
    "resend requested from the gap"
    (Some 1)
    Client.state.resend_requested_for;
  Alcotest.(check int) "expected sequence not advanced" 1 Client.state.expected_seq_in
;;

let test_tier_resolution () =
  let resolve = Tier.resolve in
  Alcotest.(check bool)
    "empty defaults to standard"
    true
    (resolve ~requested:"" ~has_fix_credentials:true = Tier.Standard);
  Alcotest.(check bool)
    "normal is an alias"
    true
    (resolve ~requested:"normal" ~has_fix_credentials:true = Tier.Standard);
  Alcotest.(check bool)
    "enterprise with credentials"
    true
    (resolve ~requested:"enterprise" ~has_fix_credentials:true = Tier.Enterprise);
  Alcotest.(check bool)
    "enterprise without credentials degrades"
    true
    (resolve ~requested:"enterprise" ~has_fix_credentials:false = Tier.Standard);
  Alcotest.(check bool)
    "unknown tier degrades"
    true
    (resolve ~requested:"platinum" ~has_fix_credentials:true = Tier.Standard);
  Alcotest.(check string) "round trip" "enterprise" (Tier.to_string Tier.Enterprise)
;;

let () =
  Alcotest.run
    "Kraken FIX"
    [ ( "codec"
      , [ Alcotest.test_case "float formatting" `Quick test_format_float
        ; Alcotest.test_case "message round trip" `Quick test_message_round_trip
        ; Alcotest.test_case
            "checksum protects the body"
            `Quick
            test_checksum_detects_corruption
        ; Alcotest.test_case "timestamp round trip" `Quick test_timestamp_round_trip
        ; Alcotest.test_case "documented sample" `Quick test_documented_sample_round_trips
        ] )
    ; ( "framing"
      , [ Alcotest.test_case
            "split and coalesced reads"
            `Quick
            test_framer_splits_and_coalesces
        ; Alcotest.test_case
            "resync after garbage"
            `Quick
            test_framer_resyncs_after_garbage
        ] )
    ; ( "auth"
      , [ Alcotest.test_case
            "logon password"
            `Quick
            test_logon_password_matches_documented_construction
        ] )
    ; ( "execution reports"
      , [ Alcotest.test_case "fill translation" `Quick test_execution_report_translation
        ; Alcotest.test_case "tag mappings" `Quick test_execution_report_mappings
        ] )
    ; ( "order entry"
      , [ Alcotest.test_case "order type/side/tif" `Quick test_order_mappings ] )
    ; ( "identity"
      , [ Alcotest.test_case "clordid allocation" `Quick test_order_id_allocation ] )
    ; "tier", [ Alcotest.test_case "resolution" `Quick test_tier_resolution ]
    ; ( "session"
      , [ Alcotest.test_case
            "logon above expected reopens replay"
            `Quick
            test_logon_above_expected_opens_replay_window
        ; Alcotest.test_case
            "replay applied once"
            `Quick
            test_replay_window_accepts_each_frame_once
        ; Alcotest.test_case
            "gap holds and requests resend"
            `Quick
            test_gap_holds_frame_and_requests_resend
        ] )
    ]
;;
