(* Regression tests for the concurrency primitives rewritten in the HFT
   latency/correctness pass:
   - Ring_buffer: absolute monotonic positions, deterministic lap clamping,
     cursor validity across clear.
   - Exchange_wakeup: generation counters make wait_since return immediately
     when a signal raced the work cycle (the lost-wakeup fix).
   - Parse_worker: submit/register round-trip. *)

(* These tests spawn a few short-lived helper domains to exercise concurrent
   producers/consumers, then join them. Bounded and test-only, so OxCaml's
   [do_not_spawn_domains]/[unsafe_multidomain] alerts are acknowledged. *)
[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

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

(* The writer assigns value = absolute position, so a reader visiting cursor
   p can only ever be delivered the value p. A lapped mis-delivery (the
   pre-seqlock bug) surfaced the NEWER payload at the OLDER cursor, which
   breaks monotonicity within a single drain. *)
let assert_monotonic_drain b =
  let seen = Concurrency.Ring_buffer.RingBuffer.read_since b 0 in
  let rec check prev = function
    | [] -> ()
    | v :: rest ->
      if v <= prev then Alcotest.failf "non-monotonic drain: %d after %d" v prev;
      check v rest
  in
  check (-1) seen;
  match seen with
  | [] -> ()
  | last :: _ ->
    (* read_latest is called after the drain, so its event position is >=
        the last delivered one. *)
    (match Concurrency.Ring_buffer.RingBuffer.read_latest b with
     | Some v when v < last -> Alcotest.failf "read_latest regressed: %d < %d" v last
     | _ -> ())
;;

let test_ring_buffer_writer_lap_race_cross_domain () =
  let b = Concurrency.Ring_buffer.RingBuffer.create 8 in
  let stop = Atomic.make false in
  let writer =
    Domain.spawn (fun () ->
      let i = ref 0 in
      while not (Atomic.get stop) do
        incr i;
        Concurrency.Ring_buffer.RingBuffer.write b !i
      done)
  in
  (* Reader hammers full drains while the writer laps it live on another
     domain; every delivered payload must be the complete event for its
     cursor (monotonic), never a lapped newer payload at an older cursor. *)
  for _ = 1 to 3000 do
    assert_monotonic_drain b
  done;
  Atomic.set stop true;
  Domain.join writer;
  (* Quiesced exact-replay check: with the writer stopped, the surviving
     window must be exactly the last [size] events in order - no gaps, no
     duplicates, no lapped payloads. *)
  let size = 8 in
  let n = Concurrency.Ring_buffer.RingBuffer.get_position b in
  let expected = List.init (min size n) (fun k -> n - min size n + 1 + k) in
  let seen = Concurrency.Ring_buffer.RingBuffer.read_since b 0 in
  Alcotest.(check (list int)) "quiesced replay is exact" expected seen
;;

(* Record payloads make the clear-vs-reader hazard observable: the pre-seqlock
   [clear] stored [Obj.magic 0] (an immediate) into the payload field BEFORE
   invalidating [seq], so a reader that validated [seq] just before the clear
   then read the payload dereferenced integer 0 as a block pointer - a crash
   for record-typed buffers. With the sentinel protocol that can never be
   delivered; this test fails by crashing if the guarantee regresses. *)
type clear_race_payload =
  { gen : int
  ; tag : int
  }

let test_ring_buffer_clear_vs_reader_cross_domain () =
  let b = Concurrency.Ring_buffer.RingBuffer.create 8 in
  for i = 1 to 8 do
    Concurrency.Ring_buffer.RingBuffer.write b { gen = i; tag = i * 3 }
  done;
  let stop = Atomic.make false in
  let clearer =
    Domain.spawn (fun () ->
      while not (Atomic.get stop) do
        Concurrency.Ring_buffer.RingBuffer.clear b
      done)
  in
  let validate name = function
    | None -> ()
    | Some { gen; tag } ->
      if tag <> gen * 3 then Alcotest.failf "%s: torn payload gen=%d tag=%d" name gen tag
  in
  let validate1 name p =
    if p.tag <> p.gen * 3
    then Alcotest.failf "%s: torn payload gen=%d tag=%d" name p.gen p.tag
  in
  for _ = 1 to 5000 do
    validate "read_latest" (Concurrency.Ring_buffer.RingBuffer.read_latest b);
    List.iter (validate1 "read_since") (Concurrency.Ring_buffer.RingBuffer.read_since b 0);
    Concurrency.Ring_buffer.RingBuffer.iter_since b 0 (validate1 "iter_since") |> ignore;
    List.iter (validate1 "read_all") (Concurrency.Ring_buffer.RingBuffer.read_all b)
  done;
  Atomic.set stop true;
  Domain.join clearer
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

let test_watchdog_staleness () =
  let open Concurrency.Main_loop_watchdog in
  (* A fresh beat survives many beat intervals without tripping. *)
  Alcotest.(check bool)
    "fresh beat healthy"
    (is_stalled ~last_beat:1000.0 ~now:1025.0)
    false;
  (* Exactly at the threshold is still healthy (strictly-greater rule):
     several sequential bounded TLS ops must not false-trigger. *)
  Alcotest.(check bool)
    "threshold not exceeded"
    (is_stalled ~last_beat:1000.0 ~now:1060.0)
    false;
  (* Past the threshold the loop is presumed wedged. *)
  Alcotest.(check bool)
    "past threshold trips"
    (is_stalled ~last_beat:1000.0 ~now:1061.0)
    true;
  Alcotest.(check bool) "long stall trips" (is_stalled ~last_beat:0.0 ~now:600.0) true
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
        ; Alcotest.test_case
            "writer_lap_cross_domain"
            `Slow
            test_ring_buffer_writer_lap_race_cross_domain
        ; Alcotest.test_case
            "clear_vs_reader_cross_domain"
            `Slow
            test_ring_buffer_clear_vs_reader_cross_domain
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
    ; ( "main_loop_watchdog"
      , [ Alcotest.test_case "stall threshold" `Quick test_watchdog_staleness ] )
    ]
;;
