
open Lwt.Infix

module EventBus = Concurrency.Event_bus.Make(struct type t = string end)

let test_event_bus_creation () =
  let bus = EventBus.create "test_bus" in
  Alcotest.(check string) "event bus topic" "test_bus" (EventBus.topic bus)

let test_event_publish_subscribe () =
  let test_async () =
    let bus = EventBus.create "test_bus" in
    let received = ref [] in
    let finished = Lwt_condition.create () in
    let subscription = EventBus.subscribe bus in

    (* Subscribe to events *)
    Lwt.async (fun () ->
      let rec loop () =
        Lwt.pick [
          (* Try to get an event from the stream *)
          (Lwt_stream.get subscription.stream >>= function
           | Some event ->
               received := event :: !received;
               Lwt.return `Continue
           | None -> Lwt.return `Stop);
          (* Or wait for the finish signal *)
          (Lwt_condition.wait finished >>= fun () -> Lwt.return `Stop)
        ] >>= function
        | `Continue -> loop ()
        | `Stop -> Lwt.return_unit
      in loop ()
    );

    (* Publish an event *)
    EventBus.publish bus "test_event";

    (* Give some time for async processing *)
    Lwt_unix.sleep 0.01 >>= fun () ->
    Alcotest.(check (list string)) "event received" ["test_event"] !received;

    (* Signal the async loop to finish *)
    Lwt_condition.signal finished ();

    (* Give a little more time for the async loop to terminate *)
    Lwt_unix.sleep 0.001 >>= fun () ->
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
