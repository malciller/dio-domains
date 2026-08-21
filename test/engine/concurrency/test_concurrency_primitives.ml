(* Regression tests for the concurrency primitives rewritten in the HFT
   latency/correctness pass:
   - Ring_buffer: absolute monotonic positions, deterministic lap clamping,
     cursor validity across clear.
   - Exchange_wakeup: generation counters make wait_since return immediately
     when a signal raced the work cycle (the lost-wakeup fix).
   - Parse_worker: submit/register round-trip. *)

let test_ring_buffer_basic () =
  let b = Concurrency.Ring_buffer.RingBuffer.create 4 in
  Alcotest.(check int)
    "fresh position"
    0
    (Concurrency.Ring_buffer.RingBuffer.get_position b);
  Alcotest.(check (option int))
    "empty latest"
    None
    (Concurrency.Ring_buffer.RingBuffer.read_latest b);
  Concurrency.Ring_buffer.RingBuffer.write b 10;
  Concurrency.Ring_buffer.RingBuffer.write b 20;
  Concurrency.Ring_buffer.RingBuffer.write b 30;
  Alcotest.(check int)
    "absolute position counts writes"
    3
    (Concurrency.Ring_buffer.RingBuffer.get_position b);
  Alcotest.(check (option int))
    "latest"
    (Some 30)
    (Concurrency.Ring_buffer.RingBuffer.read_latest b);
  let seen = Concurrency.Ring_buffer.RingBuffer.read_since b 0 in
  Alcotest.(check (list int)) "read_since 0 replays all" [ 10; 20; 30 ] seen;
  (* Cursor returned by get_position observes nothing new until next write. *)
  let pos = Concurrency.Ring_buffer.RingBuffer.get_position b in
  let none = Concurrency.Ring_buffer.RingBuffer.read_since b pos in
  Alcotest.(check (list int)) "caught-up reader sees nothing" [] none
;;

let test_ring_buffer_lap_clamps_deterministically () =
  let b = Concurrency.Ring_buffer.RingBuffer.create 2 in
  Concurrency.Ring_buffer.RingBuffer.write b 1;
  Concurrency.Ring_buffer.RingBuffer.write b 2;
  Concurrency.Ring_buffer.RingBuffer.write b 3;
  (* Reader stalled at 0 was lapped: entry 1 is gone. The old modulo design
     aliased positions here (reader saw an empty buffer); the new one
     deterministically resumes at the oldest survivor. *)
  let seen = Concurrency.Ring_buffer.RingBuffer.read_since b 0 in
  Alcotest.(check (list int)) "lapped reader gets survivors" [ 2; 3 ] seen;
  let pos = Concurrency.Ring_buffer.RingBuffer.iter_since b 0 (fun _ -> ()) in
  Alcotest.(check int) "iter_since returns writer position" 3 pos;
  Alcotest.(check (option int))
    "latest survives wraparound"
    (Some 3)
    (Concurrency.Ring_buffer.RingBuffer.read_latest b)
;;

let test_ring_buffer_clear_keeps_cursors_valid () =
  let b = Concurrency.Ring_buffer.RingBuffer.create 4 in
  Concurrency.Ring_buffer.RingBuffer.write b 5;
  let pos_before_clear = Concurrency.Ring_buffer.RingBuffer.get_position b in
  Concurrency.Ring_buffer.RingBuffer.clear b;
  (* A consumer holding the pre-clear cursor must see "no new data", not a
     reset stream it would misinterpret. *)
  let after_clear = Concurrency.Ring_buffer.RingBuffer.read_since b pos_before_clear in
  Alcotest.(check (list int)) "cleared entries invisible" [] after_clear;
  Concurrency.Ring_buffer.RingBuffer.write b 6;
  let seen = Concurrency.Ring_buffer.RingBuffer.read_since b pos_before_clear in
  Alcotest.(check (list int)) "post-clear writes delivered once" [ 6 ] seen
;;

let test_wakeup_generation_immediate_return () =
  let symbol = "TEST/WAKEUP" in
  let g0 = Concurrency.Exchange_wakeup.get_generation ~symbol in
  Concurrency.Exchange_wakeup.signal ~symbol;
  (* A signal that arrived "during the cycle" (after baseline capture) must
     make wait_since return immediately instead of parking - this is the R1
     lost-wakeup fix. If this regressed, the test would hang. *)
  Concurrency.Exchange_wakeup.wait_since ~symbol ~since:g0;
  Alcotest.(check int)
    "generation advanced by one"
    (g0 + 1)
    (Concurrency.Exchange_wakeup.get_generation ~symbol)
;;

let test_wakeup_wait_releases_on_signal () =
  let symbol = "TEST/WAKEUP2" in
  let g0 = Concurrency.Exchange_wakeup.get_generation ~symbol in
  (* Signal from another thread after a short delay; the parked waiter must
     wake promptly rather than sleep forever (a regression here hangs this
     test, which is itself the diagnostic). *)
  let _t =
    Thread.create
      (fun () ->
         Thread.delay 0.05;
         Concurrency.Exchange_wakeup.signal ~symbol)
      ()
  in
  Concurrency.Exchange_wakeup.wait_since ~symbol ~since:g0;
  Alcotest.(check int)
    "parked waiter woke on signal"
    (g0 + 1)
    (Concurrency.Exchange_wakeup.get_generation ~symbol)
;;

let test_parse_worker_roundtrip () =
  let name = "test_handler" in
  let hits = Atomic.make 0 in
  Concurrency.Parse_worker.register name (fun _payload ->
    ignore (Atomic.fetch_and_add hits 1));
  let queued = Concurrency.Parse_worker.submit name "frame" in
  Alcotest.(check bool) "queued ok" true queued;
  (* Worker drains asynchronously; bounded wait for the handler to run. *)
  let rec poll n =
    if Atomic.get hits > 0
    then ()
    else if n <= 0
    then Alcotest.fail "handler never ran"
    else (
      Thread.delay 0.01;
      poll (n - 1))
  in
  poll 500;
  Alcotest.(check bool) "handler executed" true (Atomic.get hits >= 1)
;;

let () =
  Alcotest.run
    "concurrency primitives"
    [ ( "ring_buffer"
      , [ Alcotest.test_case "basic" `Quick test_ring_buffer_basic
        ; Alcotest.test_case
            "lap_clamp"
            `Quick
            test_ring_buffer_lap_clamps_deterministically
        ; Alcotest.test_case
            "clear_keeps_cursors"
            `Quick
            test_ring_buffer_clear_keeps_cursors_valid
        ] )
    ; ( "exchange_wakeup"
      , [ Alcotest.test_case
            "generation_immediate_return"
            `Quick
            test_wakeup_generation_immediate_return
        ; Alcotest.test_case
            "wait_releases_on_signal"
            `Quick
            test_wakeup_wait_releases_on_signal
        ] )
    ; ( "parse_worker"
      , [ Alcotest.test_case "roundtrip" `Quick test_parse_worker_roundtrip ] )
    ]
;;
