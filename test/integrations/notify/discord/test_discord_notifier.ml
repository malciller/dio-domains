open Lwt.Infix
module Notifier = Discord.Notifier
module Fill_event_bus = Concurrency.Fill_event_bus

(** Fill event with per-field test defaults. *)
let create_fill_event
  ?(venue = "test_venue")
  ?(symbol = "BTC/USD")
  ?(side = "buy")
  ?(amount = 1.5)
  ?(fill_price = 50000.0)
  ?(fee = 0.1)
  ?(order_id = "test_oid")
  ?(trade_id = "test_tid")
  ?(market = "spot")
  ?intent
  ?(strategy = "grid")
  ?realized_pnl
  ?note
  timestamp
  =
  let intent =
    match intent with
    | Some i -> i
    | None -> if side = "buy" then "buy_spot" else "sell_spot"
  in
  { Fill_event_bus.venue
  ; symbol
  ; side
  ; amount
  ; fill_price
  ; value = amount *. fill_price
  ; fee
  ; timestamp
  ; order_id
  ; trade_id
  ; market
  ; intent
  ; strategy
  ; realized_pnl
  ; note
  }
;;

(** Single fill formats to one embed. *)
let test_format_single_fill () =
  let ev = create_fill_event ~side:"buy" 1620000000.0 in
  match Notifier.build_webhook_payload [ ev ] with
  | None -> Alcotest.fail "Expected payload for single fill"
  | Some payload ->
    let open Yojson.Safe.Util in
    let embeds = member "embeds" payload |> to_list in
    Alcotest.(check int) "has 1 embed" 1 (List.length embeds);
    let embed = List.hd embeds in
    let title = member "title" embed |> to_string in
    Alcotest.(check string)
      "title matches"
      "🟢 Bought spot — BTC/USD @ $50000.0000"
      title;
    let color = member "color" embed |> to_int in
    Alcotest.(check int) "color matches green for buy" 0x2ECC71 color;
    let fields = member "fields" embed |> to_list in
    Alcotest.(check int) "has 7 fields for single message" 7 (List.length fields)
;;

(** Multiple fills format to one batched embed. *)
let test_format_multiple_fills () =
  let ev1 = create_fill_event ~side:"buy" ~venue:"kraken" 1620000000.0 in
  let ev2 = create_fill_event ~side:"sell" ~venue:"lighter" 1620000010.0 in
  match Notifier.build_webhook_payload [ ev1; ev2 ] with
  | None -> Alcotest.fail "Expected payload for multiple fills"
  | Some payload ->
    let open Yojson.Safe.Util in
    let embeds = member "embeds" payload |> to_list in
    Alcotest.(check int) "has 1 embed" 1 (List.length embeds);
    let embed = List.hd embeds in
    let title = member "title" embed |> to_string in
    Alcotest.(check string)
      "title matches batched title"
      "2 executions — kraken, lighter"
      title;
    let fields = member "fields" embed |> to_list in
    Alcotest.(check int)
      "has 2 fields (1 per fill) for batched message"
      2
      (List.length fields);
    let color = member "color" embed |> to_int in
    Alcotest.(check int) "color matches blue for batch" 0x3498DB color
;;

(** Perp fills read as short lifecycle actions, not generic buys/sells. *)
let test_short_lifecycle_labels () =
  let opened =
    create_fill_event
      ~symbol:"HYPE"
      ~side:"sell"
      ~market:"perp"
      ~intent:"open_short"
      ~strategy:"carry"
      1620000000.0
  in
  let closed =
    create_fill_event
      ~symbol:"HYPE"
      ~side:"buy"
      ~market:"perp"
      ~intent:"close_short"
      ~strategy:"carry"
      ~realized_pnl:0.42
      1620000010.0
  in
  let title ev =
    match Notifier.build_webhook_payload [ ev ] with
    | None -> Alcotest.fail "Expected payload"
    | Some p ->
      let open Yojson.Safe.Util in
      member "embeds" p |> to_list |> List.hd |> member "title" |> to_string
  in
  Alcotest.(check string)
    "selling a perp opens a short"
    "🔴 Opened short — HYPE @ $50000.0000"
    (title opened);
  Alcotest.(check string)
    "buying a perp closes a short"
    "🟢 Closed short — HYPE @ $50000.0000"
    (title closed);
  match Notifier.build_webhook_payload [ closed ] with
  | None -> Alcotest.fail "Expected payload"
  | Some p ->
    let open Yojson.Safe.Util in
    let fields = member "embeds" p |> to_list |> List.hd |> member "fields" |> to_list in
    let has_pnl =
      List.exists
        (fun f -> member "name" f |> to_string = "Realized PnL")
        fields
    in
    Alcotest.(check bool) "closed short shows realized PnL" true has_pnl
;;

(** An account liquidation gets the warning treatment. *)
let test_liquidation_label () =
  let ev =
    create_fill_event
      ~symbol:"ACCOUNT"
      ~side:""
      ~amount:0.0
      ~fill_price:0.0
      ~fee:0.0
      ~market:"perp"
      ~intent:"liquidation"
      ~strategy:"account"
      ?note:(Some "account_value=$12.00 liquidator=0xabc")
      1620000000.0
  in
  match Notifier.build_webhook_payload [ ev ] with
  | None -> Alcotest.fail "Expected payload"
  | Some p ->
    let open Yojson.Safe.Util in
    let embed = member "embeds" p |> to_list |> List.hd in
    Alcotest.(check string)
      "liquidation title"
      "🚨 LIQUIDATION — ACCOUNT"
      (member "title" embed |> to_string);
    Alcotest.(check int) "liquidation color" 0x8B0000 (member "color" embed |> to_int)
;;

(** Rate limiter token acquisition. *)
let test_rate_limiter_logic _switch () =
  (* Bucket is global and clock-based; exercises the acquire path for eventual resolution. *)
  let start_time = Unix.gettimeofday () in
  Notifier.acquire_token ()
  >>= fun () ->
  let end_time = Unix.gettimeofday () in
  Alcotest.(check bool)
    "acquire token unblocks quickly when bucket is full"
    true
    (end_time -. start_time < 0.1);
  Lwt.return_unit
;;

let suite =
  [ "test_format_single_fill", `Quick, test_format_single_fill
  ; "test_format_multiple_fills", `Quick, test_format_multiple_fills
  ; "test_short_lifecycle_labels", `Quick, test_short_lifecycle_labels
  ; "test_liquidation_label", `Quick, test_liquidation_label
  ; ( "test_rate_limiter_logic"
    , `Quick
    , fun () -> Lwt_main.run (test_rate_limiter_logic () ()) )
  ]
;;

let () = Alcotest.run "Discord_notifier" [ "Notifier", suite ]
