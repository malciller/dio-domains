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

let () =
  Alcotest.run
    "Lighter WebSocket"
    [ ( "state"
      , [ Alcotest.test_case "initial_state" `Quick test_connection_state_initially_false
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
        ] )
    ; ( "ping"
      , [ Alcotest.test_case "unsent_ping_is_not_sent" `Quick test_unsent_ping_is_not_sent
        ; Alcotest.test_case "first_frame_grace" `Quick test_first_frame_grace
        ] )
    ]
;;
