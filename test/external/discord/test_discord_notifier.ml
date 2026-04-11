open Lwt.Infix

(* Aliases for readability *)
module Notifier = Discord.Notifier
module Fill_event_bus = Concurrency.Fill_event_bus

(** Helper to create dummy fill events. *)
let create_fill_event ?(venue="test_venue") ?(symbol="BTC/USD") ?(side="buy")
    ?(amount=1.5) ?(fill_price=50000.0) ?(fee=0.1) timestamp =
  {
    Fill_event_bus.venue;
    symbol;
    side;
    amount;
    fill_price;
    value = amount *. fill_price;
    fee;
    timestamp;
  }

(** Test that formatting a single fill event produces the correct single embed structure. *)
let test_format_single_fill () =
  let ev = create_fill_event ~side:"buy" 1620000000.0 in
  match Notifier.build_webhook_payload [ev] with
  | None -> Alcotest.fail "Expected payload for single fill"
  | Some payload ->
      let open Yojson.Safe.Util in
      let embeds = member "embeds" payload |> to_list in
      Alcotest.(check int) "has 1 embed" 1 (List.length embeds);
      let embed = List.hd embeds in
      let title = member "title" embed |> to_string in
      Alcotest.(check string) "title matches" "Order Fill — test_venue" title;
      let color = member "color" embed |> to_int in
      Alcotest.(check int) "color matches green for buy" 0x2ECC71 color;
      let fields = member "fields" embed |> to_list in
      Alcotest.(check int) "has 6 fields for single message" 6 (List.length fields)

(** Test that formatting multiple fills produces the batched embed structure. *)
let test_format_multiple_fills () =
  let ev1 = create_fill_event ~side:"buy" ~venue:"kraken" 1620000000.0 in
  let ev2 = create_fill_event ~side:"sell" ~venue:"lighter" 1620000010.0 in
  match Notifier.build_webhook_payload [ev1; ev2] with
  | None -> Alcotest.fail "Expected payload for multiple fills"
  | Some payload ->
      let open Yojson.Safe.Util in
      let embeds = member "embeds" payload |> to_list in
      Alcotest.(check int) "has 1 embed" 1 (List.length embeds);
      let embed = List.hd embeds in
      let title = member "title" embed |> to_string in
      Alcotest.(check string) "title matches batched title" "2 Order Fills — kraken, lighter" title;
      let fields = member "fields" embed |> to_list in
      Alcotest.(check int) "has 2 fields (1 per fill) for batched message" 2 (List.length fields);
      let color = member "color" embed |> to_int in
      Alcotest.(check int) "color matches blue for batch" 0x3498DB color

(** Test the rate limiter token consumption logic. *)
let test_rate_limiter_logic _switch () =
  (* Since bucket is global and relies on gettimeofday, this test primarily
     exercises the acquire path to ensure it eventually resolves. *)
  let start_time = Unix.gettimeofday () in
  Notifier.acquire_token () >>= fun () ->
  let end_time = Unix.gettimeofday () in
  Alcotest.(check bool) "acquire token unblocks quickly when bucket is full"
    true ((end_time -. start_time) < 0.1);
  Lwt.return_unit

let suite =
  [
    ("test_format_single_fill", `Quick, test_format_single_fill);
    ("test_format_multiple_fills", `Quick, test_format_multiple_fills);
    ("test_rate_limiter_logic", `Quick, (fun () -> Lwt_main.run (test_rate_limiter_logic () ())));
  ]

let () =
  Alcotest.run "Discord_notifier" [
    ("Notifier", suite);
  ]
