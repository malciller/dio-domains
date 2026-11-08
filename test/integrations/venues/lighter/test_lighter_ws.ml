let test_connection_state_initially_false () =
  Alcotest.(check bool) "initially disconnected" false (Lighter.Ws.is_connected ())
;;

let test_subscribe_market_data () =
  let sub = Lighter.Ws.subscribe_market_data () in
  (* A fresh subscription yields an empty stream. *)
  let items = Lwt_stream.get_available sub.stream in
  Alcotest.(check int) "initially empty" 0 (List.length items);
  sub.close ()
;;

let test_broadcast_message () =
  let sub = Lighter.Ws.subscribe_market_data () in
  let test_json = `Assoc [ "test", `String "value" ] in
  Lighter.Ws.broadcast_message test_json;
  let items = Lwt_stream.get_available sub.stream in
  Alcotest.(check int) "received 1 message" 1 (List.length items);
  sub.close ()
;;

let test_broadcast_multiple_subscribers () =
  let sub1 = Lighter.Ws.subscribe_market_data () in
  let sub2 = Lighter.Ws.subscribe_market_data () in
  let test_json = `Assoc [ "key", `String "val" ] in
  Lighter.Ws.broadcast_message test_json;
  let items1 = Lwt_stream.get_available sub1.stream in
  let items2 = Lwt_stream.get_available sub2.stream in
  Alcotest.(check int) "sub1 received" 1 (List.length items1);
  Alcotest.(check int) "sub2 received" 1 (List.length items2);
  sub1.close ();
  sub2.close ()
;;

let test_close_unsubscribes () =
  let sub = Lighter.Ws.subscribe_market_data () in
  sub.close ();
  (* Broadcast after close is not delivered. *)
  Lighter.Ws.broadcast_message (`Assoc [ "after", `String "close" ]);
  let items = Lwt_stream.get_available sub.stream in
  Alcotest.(check int) "no messages after close" 0 (List.length items)
;;

let test_unsent_ping_is_not_sent () =
  let state = Lighter.Ws.create_connection_state () in
  let result = Lwt_main.run (Lighter.Ws.ping_side state) in
  Alcotest.(check bool) "no socket" true (result = Lighter.Ws.Ping_not_sent)
;;

let test_first_frame_grace () =
  let overdue ~connected_for ~frames ~grace =
    Lighter.Ws.first_frame_overdue
      ~now:100.0
      ~connected_at:(100.0 -. connected_for)
      ~frames_since_connect:frames
      ~grace_s:grace
  in
  Alcotest.(check bool)
    "inside grace"
    false
    (overdue ~connected_for:5.0 ~frames:0 ~grace:20.0);
  Alcotest.(check bool)
    "frame arrived"
    false
    (overdue ~connected_for:60.0 ~frames:1 ~grace:20.0);
  Alcotest.(check bool)
    "no frame past grace"
    true
    (overdue ~connected_for:21.0 ~frames:0 ~grace:20.0)
;;

let test_tx_frame_single () =
  let open Yojson.Safe.Util in
  let frame = Lighter.Ws.tx_frame [ 1, {|{"Index":42}|}, "id-1" ] in
  Alcotest.(check string) "type" "jsonapi/sendtx" (member "type" frame |> to_string);
  let data = member "data" frame in
  Alcotest.(check string) "id" "id-1" (member "id" data |> to_string);
  Alcotest.(check int) "tx_type" 1 (member "tx_type" data |> to_int);
  Alcotest.(check int)
    "tx_info is an object"
    42
    (member "Index" (member "tx_info" data) |> to_int)
;;

let test_tx_frame_batch () =
  let open Yojson.Safe.Util in
  let frame =
    Lighter.Ws.tx_frame [ 1, {|{"Index":1}|}, "id-1"; 2, {|{"Index":2}|}, "id-2" ]
  in
  Alcotest.(check string) "type" "jsonapi/sendtxbatch" (member "type" frame |> to_string);
  let data = member "data" frame in
  (* Batch fields are JSON strings holding arrays, per the reference client. *)
  let types = member "tx_types" data |> to_string |> Yojson.Safe.from_string |> to_list in
  Alcotest.(check (list int)) "tx_types" [ 1; 2 ] (List.map to_int types);
  let infos = member "tx_infos" data |> to_string |> Yojson.Safe.from_string |> to_list in
  Alcotest.(check int) "one info per tx" 2 (List.length infos);
  Alcotest.(check int) "first info" 1 (member "Index" (List.nth infos 0) |> to_int)
;;

let test_tx_frame_string_matches_ast () =
  let cases =
    [ []
    ; [ 1, {|{"Index":42}|}, "id-1" ]
    ; [ 1, {|{"Index":1,"Hex":"0xab"}|}, "id-1"; 2, {|{"Index":2}|}, "id-2" ]
    ; [ 3, {|{"Nested":{"q":"a\"b"}}|}, "id\"x" ]
    ]
  in
  List.iter
    (fun txs ->
      Alcotest.(check string)
        "raw frame equals serialized AST"
        (Yojson.Safe.to_string (Lighter.Ws.tx_frame txs))
        (Lighter.Ws.tx_frame_string txs))
    cases
;;

let test_parse_tx_ack_single_accepted () =
  let replies =
    Lighter.Ws.parse_tx_ack
      (Yojson.Safe.from_string
         {|{"type":"jsonapi/sendtx","data":{"id":"1793682004876","code":200,"message":""}}|})
  in
  match replies with
  | [ r ] ->
    Alcotest.(check string) "id" "1793682004876" r.Lighter.Ws.reply_id;
    Alcotest.(check int) "code" 200 r.Lighter.Ws.reply_code
  | _ -> Alcotest.fail "expected exactly one reply"
;;

let test_parse_tx_ack_single_rejected () =
  let replies =
    Lighter.Ws.parse_tx_ack
      (Yojson.Safe.from_string
         {|{"type":"jsonapi/sendtx","data":{"id":"7","code":21107,"message":"reduce only would not reduce position"}}|})
  in
  match replies with
  | [ r ] ->
    Alcotest.(check int) "code" 21107 r.Lighter.Ws.reply_code;
    Alcotest.(check string)
      "message"
      "reduce only would not reduce position"
      r.Lighter.Ws.reply_message
  | _ -> Alcotest.fail "expected exactly one reply"
;;

let test_parse_tx_ack_batch_list () =
  let replies =
    Lighter.Ws.parse_tx_ack
      (Yojson.Safe.from_string
         {|{"type":"jsonapi/sendtxbatch","data":[{"code":200},{"code":21108,"message":"nope"}]}|})
  in
  match replies with
  | [ first; second ] ->
    Alcotest.(check int) "first code" 200 first.Lighter.Ws.reply_code;
    Alcotest.(check int) "second code" 21108 second.Lighter.Ws.reply_code;
    Alcotest.(check string) "second reason" "nope" second.Lighter.Ws.reply_message
  | _ -> Alcotest.fail "expected two replies"
;;

let test_parse_tx_ack_batch_encoded_string () =
  let replies =
    Lighter.Ws.parse_tx_ack
      (Yojson.Safe.from_string
         {|{"type":"jsonapi/sendtxbatch","data":"[{\"code\":21107,\"message\":\"rejected\"}]"}|})
  in
  match replies with
  | [ r ] ->
    Alcotest.(check int) "code" 21107 r.Lighter.Ws.reply_code;
    Alcotest.(check string) "reason" "rejected" r.Lighter.Ws.reply_message
  | _ -> Alcotest.fail "expected one reply"
;;

let test_parse_tx_ack_envelope_code () =
  let replies =
    Lighter.Ws.parse_tx_ack
      (Yojson.Safe.from_string
         {|{"type":"jsonapi/sendtxbatch","data":null,"code":21100,"message":"batch rejected"}|})
  in
  match replies with
  | [ r ] ->
    Alcotest.(check int) "rejected" 21100 r.Lighter.Ws.reply_code;
    Alcotest.(check string) "reason" "batch rejected" r.Lighter.Ws.reply_message
  | _ -> Alcotest.fail "expected one reply"
;;

let test_parse_tx_ack_without_result_is_empty () =
  let replies =
    Lighter.Ws.parse_tx_ack (Yojson.Safe.from_string {|{"type":"jsonapi/sendtx"}|})
  in
  Alcotest.(check int) "no attributable result" 0 (List.length replies)
;;

let test_batch_ack_attributed_to_tx_ids () =
  Lighter.Ws.remember_pending_ack [ "batch_a"; "batch_b" ];
  let replies =
    Lighter.Ws.parse_tx_ack
      (Yojson.Safe.from_string
         {|{"type":"jsonapi/sendtxbatch","data":{"codes":[200,21107],"messages":["","nope"]}}|})
    |> Lighter.Ws.attribute_ack_ids
  in
  match replies with
  | [ first; second ] ->
    Alcotest.(check string) "first id" "batch_a" first.Lighter.Ws.reply_id;
    Alcotest.(check string) "second id" "batch_b" second.Lighter.Ws.reply_id;
    Alcotest.(check int) "second code" 21107 second.Lighter.Ws.reply_code
  | _ -> Alcotest.fail "expected two replies"
;;

let test_echoed_id_ack_consumes_frame () =
  Lighter.Ws.remember_pending_ack [ "solo" ];
  ignore
    (Lighter.Ws.parse_tx_ack
       (Yojson.Safe.from_string
          {|{"type":"jsonapi/sendtx","data":{"id":"solo","code":200}}|})
     |> Lighter.Ws.attribute_ack_ids);
  (* The answered frame must not linger and capture the next id-less verdict. *)
  let replies =
    Lighter.Ws.parse_tx_ack
      (Yojson.Safe.from_string
         {|{"type":"jsonapi/sendtxbatch","data":{"codes":[21107],"messages":["nope"]}}|})
    |> Lighter.Ws.attribute_ack_ids
  in
  match replies with
  | [ r ] -> Alcotest.(check string) "no id" "" r.Lighter.Ws.reply_id
  | _ -> Alcotest.fail "expected one reply"
;;

let test_batch_ack_without_ids_is_unattributed () =
  (* Nothing remembered: a positional verdict must not invent an id. *)
  let replies =
    Lighter.Ws.parse_tx_ack
      (Yojson.Safe.from_string
         {|{"type":"jsonapi/sendtxbatch","data":{"codes":[21107],"messages":["nope"]}}|})
    |> Lighter.Ws.attribute_ack_ids
  in
  match replies with
  | [ r ] -> Alcotest.(check string) "no id" "" r.Lighter.Ws.reply_id
  | _ -> Alcotest.fail "expected one reply"
;;

let test_rejected_batch_create_order_retires_phantom () =
  let symbol = "WSBATCH" in
  Lighter.Instruments_feed.initialize [ symbol ];
  Lighter.Executions_feed.initialize [ symbol ];
  List.iter
    (fun order_id ->
      Lighter.Executions_feed.inject_order
        ~symbol
        ~order_id
        ~side:Lighter.Executions_feed.Sell
        ~qty:0.44
        ~price:86.64
        ())
    [ "ws_batch_1"; "ws_batch_2" ];
  (* Only create-order txs may retire an order on rejection; [place_order] registers the
     client order index before submitting. *)
  List.iter
    (fun id -> Lighter.Ws.register_create_order_tx ~id)
    [ "ws_batch_1"; "ws_batch_2" ];
  Lighter.Ws.remember_pending_ack [ "ws_batch_1"; "ws_batch_2" ];
  Lighter.Ws.parse_tx_ack
    (Yojson.Safe.from_string
       {|{"type":"jsonapi/sendtxbatch","data":{"codes":[200,21107],"messages":["","nope"]}}|})
  |> Lighter.Ws.attribute_ack_ids
  |> List.iter Lighter.Ws.apply_tx_reply;
  Alcotest.(check bool)
    "rejected close dropped"
    true
    (Option.is_none (Lighter.Executions_feed.get_open_order symbol "ws_batch_2"))
;;

let test_rejected_amend_retires_only_missing_order () =
  let symbol = "WSAMEND" in
  Lighter.Instruments_feed.initialize [ symbol ];
  Lighter.Executions_feed.initialize [ symbol ];
  List.iter
    (fun order_id ->
      Lighter.Executions_feed.inject_order
        ~symbol
        ~order_id
        ~side:Lighter.Executions_feed.Sell
        ~qty:0.44
        ~price:86.64
        ())
    [ "amend_gone"; "amend_live" ];
  Lighter.Ws.apply_tx_reply
    { Lighter.Ws.reply_id = "amend_gone:modify"
    ; reply_code = 21107
    ; reply_message = "order not found"
    };
  Alcotest.(check bool)
    "missing target dropped"
    true
    (Option.is_none (Lighter.Executions_feed.get_open_order symbol "amend_gone"));
  (* In-place modify: a rejected amend leaves the target resting at its old price. *)
  Lighter.Ws.apply_tx_reply
    { Lighter.Ws.reply_id = "amend_live:modify"
    ; reply_code = 21108
    ; reply_message = "post only order would cross"
    };
  Alcotest.(check bool)
    "live target kept"
    true
    (Option.is_some (Lighter.Executions_feed.get_open_order symbol "amend_live"))
;;

let test_rejected_create_order_retires_phantom () =
  let symbol = "WSREJ" in
  Lighter.Instruments_feed.initialize [ symbol ];
  Lighter.Executions_feed.initialize [ symbol ];
  Lighter.Executions_feed.inject_order
    ~symbol
    ~order_id:"ws_rej_1"
    ~side:Lighter.Executions_feed.Sell
    ~qty:0.44
    ~price:86.64
    ();
  Lighter.Ws.register_create_order_tx ~id:"ws_rej_1";
  Lighter.Ws.apply_tx_reply
    { Lighter.Ws.reply_id = "ws_rej_1"
    ; reply_code = 21107
    ; reply_message = "reduce only would not reduce position"
    };
  Alcotest.(check bool)
    "placed order dropped"
    true
    (Option.is_none (Lighter.Executions_feed.get_open_order symbol "ws_rej_1"))
;;

let test_rejected_cancel_keeps_live_order () =
  let symbol = "WSCANCEL" in
  Lighter.Instruments_feed.initialize [ symbol ];
  Lighter.Executions_feed.initialize [ symbol ];
  Lighter.Executions_feed.inject_order
    ~symbol
    ~order_id:"ws_cancel_1"
    ~side:Lighter.Executions_feed.Sell
    ~qty:0.44
    ~price:86.64
    ();
  (* A rejected cancel targets a live order; only create-order txs may retire it. *)
  Lighter.Ws.apply_tx_reply
    { Lighter.Ws.reply_id = "ws_cancel_1"
    ; reply_code = 21107
    ; reply_message = "order not found"
    };
  Alcotest.(check bool)
    "live order kept"
    true
    (Option.is_some (Lighter.Executions_feed.get_open_order symbol "ws_cancel_1"))
;;

let test_decode_orderbook_frame_uses_span_path () =
  Lighter.Instruments_feed.initialize [ "OBWSPAN" ];
  Lighter.Orderbook_feed.initialize [ "OBWSPAN" ];
  let content =
    {|{"type":"snapshot","channel":"order_book:0","order_book":{"b":[{"price":"100.0","size":"2.0"}],"a":[{"price":"101.0","size":"3.0"}]}}|}
  in
  (match Lighter.Ws.decode_text_frame content with
   | Lighter.Ws.Frame_data -> ()
   | _ -> Alcotest.fail "order-book frame should decode as Frame_data");
  match Lighter.Orderbook_feed.get_best_bid_ask "OBWSPAN" with
  | Some (bid, _, ask, _) ->
    Alcotest.(check (float 0.000001)) "bid" 100.0 bid;
    Alcotest.(check (float 0.000001)) "ask" 101.0 ask
  | None -> Alcotest.fail "span path did not publish a book"
;;

let () =
  Alcotest.run
    "Lighter WebSocket"
    [ ( "state"
      , [ Alcotest.test_case "initial_state" `Quick test_connection_state_initially_false
        ] )
    ; ( "orderbook"
      , [ Alcotest.test_case
            "decode uses span path"
            `Quick
            test_decode_orderbook_frame_uses_span_path
        ] )
    ; ( "pubsub"
      , [ Alcotest.test_case "subscribe_market_data" `Quick test_subscribe_market_data
        ; Alcotest.test_case "broadcast_message" `Quick test_broadcast_message
        ; Alcotest.test_case
            "broadcast_multiple_subscribers"
            `Quick
            test_broadcast_multiple_subscribers
        ; Alcotest.test_case "close_unsubscribes" `Quick test_close_unsubscribes
        ] )
    ; ( "tx_frames"
      , [ Alcotest.test_case "single" `Quick test_tx_frame_single
        ; Alcotest.test_case "batch" `Quick test_tx_frame_batch
        ; Alcotest.test_case
            "raw frame matches AST"
            `Quick
            test_tx_frame_string_matches_ast
        ] )
    ; ( "tx_acks"
      , [ Alcotest.test_case "single accepted" `Quick test_parse_tx_ack_single_accepted
        ; Alcotest.test_case "single rejected" `Quick test_parse_tx_ack_single_rejected
        ; Alcotest.test_case "batch list" `Quick test_parse_tx_ack_batch_list
        ; Alcotest.test_case
            "batch encoded string"
            `Quick
            test_parse_tx_ack_batch_encoded_string
        ; Alcotest.test_case "batch envelope code" `Quick test_parse_tx_ack_envelope_code
        ; Alcotest.test_case
            "unparseable ack"
            `Quick
            test_parse_tx_ack_without_result_is_empty
        ; Alcotest.test_case
            "batch ack attributed to tx ids"
            `Quick
            test_batch_ack_attributed_to_tx_ids
        ; Alcotest.test_case
            "batch ack without ids is unattributed"
            `Quick
            test_batch_ack_without_ids_is_unattributed
        ; Alcotest.test_case
            "echoed id ack consumes frame"
            `Quick
            test_echoed_id_ack_consumes_frame
        ; Alcotest.test_case
            "rejected batch placement retires order"
            `Quick
            test_rejected_batch_create_order_retires_phantom
        ; Alcotest.test_case
            "rejected amend retires only missing order"
            `Quick
            test_rejected_amend_retires_only_missing_order
        ; Alcotest.test_case
            "rejected placement retires order"
            `Quick
            test_rejected_create_order_retires_phantom
        ; Alcotest.test_case
            "rejected cancel keeps order"
            `Quick
            test_rejected_cancel_keeps_live_order
        ] )
    ; ( "ping"
      , [ Alcotest.test_case "unsent_ping_is_not_sent" `Quick test_unsent_ping_is_not_sent
        ; Alcotest.test_case "first_frame_grace" `Quick test_first_frame_grace
        ] )
    ]
;;
