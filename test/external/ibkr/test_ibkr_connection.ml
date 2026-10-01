[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

let test_connection_create () =
  let conn = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4002 ~client_id:0 in
  Alcotest.(check bool)
    "not connected initially"
    false
    (Ibkr.Connection.is_connected conn);
  Alcotest.(check string) "account_id empty" "" (Ibkr.Connection.get_account_id conn);
  Alcotest.(check int) "server_version 0" 0 (Ibkr.Connection.get_server_version conn)
;;

let test_get_next_order_id () =
  let conn = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4002 ~client_id:1 in
  Ibkr.Connection.set_next_order_id conn 100;
  let id1 = Ibkr.Connection.get_next_order_id conn in
  let id2 = Ibkr.Connection.get_next_order_id conn in
  let id3 = Ibkr.Connection.get_next_order_id conn in
  Alcotest.(check bool)
    "order IDs increment from the server-supplied floor"
    true
    (id1 = 100 && id2 = 101 && id3 = 102)
;;

(* Placement runs on per-asset trading domains, so the counter is read-modify-write from
   several domains at once. Non-atomic, two domains get the same id: TWS rejects the
   duplicate and the id->symbol index loses a binding. *)
let test_order_ids_unique_across_domains () =
  let conn = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4002 ~client_id:3 in
  Ibkr.Connection.set_next_order_id conn 1000;
  let domains = 8 in
  let per_domain = 2000 in
  let base = 1000 in
  let total = domains * per_domain in
  let handed_out = Array.make total false in
  let duplicates = Atomic.make 0 in
  (* Release all domains at once. Without this a domain can finish its whole burst before
     the next starts and the read-modify-write never overlaps, which is how the race
     survived the suite in the first place. *)
  let arrived = Atomic.make 0 in
  let go = Atomic.make false in
  let handles =
    List.init domains (fun _ ->
      Domain.spawn (fun () ->
        Atomic.incr arrived;
        while not (Atomic.get go) do
          Domain.cpu_relax ()
        done;
        for _ = 1 to per_domain do
          let id = Ibkr.Connection.get_next_order_id conn in
          if id >= base && id < base + total then
            if handed_out.(id - base) then Atomic.incr duplicates
            else handed_out.(id - base) <- true
        done))
  in
  while Atomic.get arrived < domains do
    Domain.cpu_relax ()
  done;
  Atomic.set go true;
  List.iter Domain.join handles;
  Alcotest.(check int) "no id handed out twice" 0 (Atomic.get duplicates);
  let missing = Array.fold_left (fun acc got -> if got then acc else acc + 1) 0 handed_out in
  Alcotest.(check int) "every id in the window was issued" 0 missing
;;

(* nextValidId is re-sent on every reconnect and can be *lower* than ids this process has
   already used, whose orders may still be working. Resuming from it would reuse a live id,
   so the floor only rises. *)
let test_next_valid_id_never_lowers_the_floor () =
  let conn = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4002 ~client_id:4 in
  Ibkr.Connection.set_next_order_id conn 5000;
  let _ = Ibkr.Connection.get_next_order_id conn in
  let _ = Ibkr.Connection.get_next_order_id conn in
  (* A reconnect reporting a stale lower value must not rewind the counter. *)
  Ibkr.Connection.set_next_order_id conn 100;
  Alcotest.(check int) "counter did not rewind" 5002 (Ibkr.Connection.get_next_order_id conn);
  (* A higher value is still adopted. *)
  Ibkr.Connection.set_next_order_id conn 9000;
  Alcotest.(check int) "higher server value adopted" 9000 (Ibkr.Connection.get_next_order_id conn)
;;

let test_disconnect_when_not_connected () =
  let conn = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4002 ~client_id:2 in
  (* Disconnect is safe when not connected. *)
  let result = Lwt_main.run (Ibkr.Connection.disconnect conn) in
  Alcotest.(check unit) "disconnect ok when not connected" () result;
  Alcotest.(check bool) "still not connected" false (Ibkr.Connection.is_connected conn)
;;

let test_multiple_connections () =
  let conn1 = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4001 ~client_id:10 in
  let conn2 = Ibkr.Connection.create ~host:"127.0.0.1" ~port:4002 ~client_id:20 in
  (* Per-connection order IDs are independent. *)
  let _id1 = Ibkr.Connection.get_next_order_id conn1 in
  let _id2 = Ibkr.Connection.get_next_order_id conn1 in
  let id_from_conn2 = Ibkr.Connection.get_next_order_id conn2 in
  (* conn2 starts at 0. *)
  Alcotest.(check int) "conn2 independent order ID" 0 id_from_conn2
;;

let () =
  Alcotest.run
    "IBKR Connection"
    [ "creation", [ Alcotest.test_case "connection_create" `Quick test_connection_create ]
    ; ( "order IDs"
      , [ Alcotest.test_case "get_next_order_id" `Quick test_get_next_order_id
        ; Alcotest.test_case "multiple_connections" `Quick test_multiple_connections
        ; Alcotest.test_case
            "order IDs unique across domains"
            `Quick
            test_order_ids_unique_across_domains
        ; Alcotest.test_case
            "nextValidId never lowers the floor"
            `Quick
            test_next_valid_id_never_lowers_the_floor
        ] )
    ; ( "lifecycle"
      , [ Alcotest.test_case
            "disconnect_when_not_connected"
            `Quick
            test_disconnect_when_not_connected
        ] )
    ]
;;
