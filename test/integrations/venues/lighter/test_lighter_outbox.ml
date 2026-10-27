open Lwt.Infix
open Alcotest

(* The writer must preserve submission order: Lighter rejects nonces that arrive out of
   order, and signing happens inside the writer, so FIFO enqueue == monotonic nonce. *)
let test_fifo_order () =
  let sent = ref [] in
  let outbox =
    Lighter.Outbox.create
      ~send:(fun (items : int list) ->
        sent := List.rev_append items !sent;
        Lwt.pause () >|= fun () -> List.map (fun i -> Ok (string_of_int i)) items)
      ()
  in
  let n = 64 in
  let results =
    Lwt_main.run
      (Lwt_list.map_p (fun i -> Lighter.Outbox.submit outbox i) (List.init n Fun.id))
  in
  check int "all submitted" n (List.length !sent);
  check (list int) "writer order is fifo" (List.init n Fun.id) (List.rev !sent);
  check int "all resolved" n (List.length (List.filter Result.is_ok results))
;;

(* A queued burst is coalesced into frames of at most [max_batch]; that is what turns a
   ladder re-placement into one transport write. *)
let test_burst_is_batched () =
  let frames = ref [] in
  let outbox =
    Lighter.Outbox.create
      ~max_batch:8
      ~send:(fun (items : int list) ->
        frames := List.length items :: !frames;
        Lwt.pause () >|= fun () -> List.map (fun i -> Ok (string_of_int i)) items)
      ()
  in
  let n = 64 in
  let results =
    Lwt_main.run
      (Lwt_list.map_p (fun i -> Lighter.Outbox.submit outbox i) (List.init n Fun.id))
  in
  check int "all resolved" n (List.length (List.filter Result.is_ok results));
  check int "never exceeds max_batch" 8 (List.fold_left max 0 !frames);
  check bool "burst coalesced" true (List.length !frames < n);
  check bool "full frames used" true (List.mem 8 !frames)
;;

(* A failing send surfaces to its own batch and does not wedge the queue. *)
let test_failure_is_isolated () =
  let outbox =
    Lighter.Outbox.create
      ~send:(fun (items : int list) ->
        Lwt.return (List.map (fun i -> if i = 1 then Error "boom" else Ok "ok") items))
      ()
  in
  let results =
    Lwt_main.run (Lwt_list.map_p (fun i -> Lighter.Outbox.submit outbox i) [ 0; 1; 2 ])
  in
  check
    (list string)
    "per-request outcomes"
    [ "ok"; "boom"; "ok" ]
    (List.map
       (function
         | Ok s -> s
         | Error e -> e)
       results)
;;

let () =
  run
    "lighter_outbox"
    [ ( "ordering"
      , [ test_case "fifo_order" `Quick test_fifo_order
        ; test_case "burst_is_batched" `Quick test_burst_is_batched
        ; test_case "failure_is_isolated" `Quick test_failure_is_isolated
        ] )
    ]
;;
