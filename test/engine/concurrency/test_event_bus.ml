open Lwt.Infix

module EventBus = Concurrency.Event_bus.Make (struct
    type t = string
  end)

let test_event_bus_creation () =
  let bus = EventBus.create "test_bus" in
  Alcotest.(check string) "event bus topic" "test_bus" (EventBus.topic bus)
;;

let test_event_publish_subscribe () =
  let test_async () =
    let bus = EventBus.create "test_bus" in
    let received = ref [] in
    let finished = Lwt_condition.create () in
    let subscription = EventBus.subscribe bus in
    Lwt.async (fun () ->
      let rec loop () =
        Lwt.pick
          [
            (Lwt_stream.get subscription.stream
             >>= function
             | Some event ->
               received := event :: !received;
               Lwt.return `Continue
             | None -> Lwt.return `Stop)
          ;
            (Lwt_condition.wait finished >>= fun () -> Lwt.return `Stop)
          ]
        >>= function
        | `Continue -> loop ()
        | `Stop -> Lwt.return_unit
      in
      loop ());
    EventBus.publish bus "test_event";
    (* Allow async delivery. *)
    Lwt_unix.sleep 0.01
    >>= fun () ->
    Alcotest.(check (list string)) "event received" [ "test_event" ] !received;
    Lwt_condition.signal finished ();
    (* Allow async loop termination. *)
    Lwt_unix.sleep 0.001 >>= fun () -> Lwt.return_unit
  in
  Lwt_main.run (test_async ())
;;

let () =
  Alcotest.run
    "Event Bus"
    [ "creation", [ Alcotest.test_case "bus creation" `Quick test_event_bus_creation ]
    ; ( "pubsub"
      , [ Alcotest.test_case "publish subscribe" `Quick test_event_publish_subscribe ] )
    ]
;;
