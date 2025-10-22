
open Lwt.Infix

module EventBus = Concurrency.Event_bus.Make(struct type t = string end)

let test_event_bus_creation () =
  let bus = EventBus.create "test_bus" in
  Alcotest.(check string) "event bus topic" "test_bus" (EventBus.topic bus)

let test_event_publish_subscribe () =
  let test_async () =
    let bus = EventBus.create "test_bus" in
    let received = ref [] in
    let stream = EventBus.subscribe bus in

    (* Subscribe to events *)
    Lwt.async (fun () ->
      let rec loop () =
        Lwt_stream.get stream >>= function
        | Some event ->
            received := event :: !received;
            loop ()
        | None -> Lwt.return_unit
      in loop ()
    );

    (* Publish an event *)
    EventBus.publish bus "test_event";

    (* Give some time for async processing *)
    Lwt_unix.sleep 0.01 >>= fun () ->
    Alcotest.(check (list string)) "event received" ["test_event"] !received;
    Lwt.return_unit
  in
  Lwt_main.run (test_async ())

let () =
  Alcotest.run "Event Bus" [
    "creation", [
      Alcotest.test_case "bus creation" `Quick test_event_bus_creation;
    ];
    "pubsub", [
      Alcotest.test_case "publish subscribe" `Quick test_event_publish_subscribe;
    ];
  ]
