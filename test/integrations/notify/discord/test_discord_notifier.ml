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

let content payload =
  let open Yojson.Safe.Util in
  member "content" payload |> to_string
;;

let embed payload =
  let open Yojson.Safe.Util in
  member "embeds" payload |> to_list |> List.hd
;;

let fields payload =
  let open Yojson.Safe.Util in
  embed payload |> member "fields" |> to_list
;;

let color payload =
  let open Yojson.Safe.Util in
  embed payload |> member "color" |> to_int
;;

let field_value name payload =
  let open Yojson.Safe.Util in
  fields payload
  |> List.find (fun f -> member "name" f |> to_string = name)
  |> fun f -> member "value" f |> to_string
;;

let build = function
  | Some payload -> payload
  | None -> Alcotest.fail "Expected payload"
;;

(** Single fill formats to one concise message plus a detail embed. *)
let test_format_single_fill () =
  let payload =
    build (Notifier.build_webhook_payload [ create_fill_event 1620000000.0 ])
  in
  Alcotest.(check string)
    "content is action, asset, and price"
    "BUY SPOT — BTC/USD @ $50,000.00"
    (content payload);
  Alcotest.(check int) "color matches green for buy" 0x2ECC71 (color payload);
  Alcotest.(check int) "has 7 detail fields" 7 (List.length (fields payload));
  Alcotest.(check string) "price is grouped" "$50,000.00" (field_value "Price" payload);
  Alcotest.(check string) "value is grouped" "$75,000.00" (field_value "Value" payload);
  Alcotest.(check string) "fee is trimmed" "$0.10" (field_value "Fee" payload)
;;

(** A fill without a reported price omits it from the summary. *)
let test_missing_price () =
  let ev = create_fill_event ~fill_price:0.0 ~amount:0.0 1620000000.0 in
  let payload = build (Notifier.build_webhook_payload [ ev ]) in
  Alcotest.(check string) "summary omits price" "BUY SPOT — BTC/USD" (content payload);
  Alcotest.(check string)
    "detail marks price unavailable"
    "n/a"
    (field_value "Price" payload)
;;

(** Multiple fills format to one batch summary plus one full-width field per fill. *)
let test_format_multiple_fills () =
  let ev1 = create_fill_event ~side:"buy" ~venue:"kraken" 1620000000.0 in
  let ev2 = create_fill_event ~side:"sell" ~venue:"lighter" 1620000010.0 in
  let payload = build (Notifier.build_webhook_payload [ ev1; ev2 ]) in
  Alcotest.(check string)
    "content summarizes the batch"
    "2 FILLS — BTC/USD · Kraken, Lighter"
    (content payload);
  Alcotest.(check int) "color matches neutral for mixed batch" 0x5865F2 (color payload);
  let fields = fields payload in
  Alcotest.(check int) "has 2 fields (1 per fill)" 2 (List.length fields);
  let open Yojson.Safe.Util in
  let name i = List.nth fields i |> member "name" |> to_string in
  let inline i = List.nth fields i |> member "inline" |> to_bool in
  Alcotest.(check string) "first fill row" "BUY SPOT — BTC/USD @ $50,000.00" (name 0);
  Alcotest.(check string) "second fill row" "SELL SPOT — BTC/USD @ $50,000.00" (name 1);
  Alcotest.(check bool) "batch rows are full width" false (inline 0)
;;

(** A batch of identical actions leads with the action and asset. *)
let test_batch_summary () =
  let ev symbol = create_fill_event ~side:"buy" ~venue:"kraken" ~symbol 1620000000.0 in
  let payload = build (Notifier.build_webhook_payload [ ev "BTC/USD"; ev "BTC/USD" ]) in
  Alcotest.(check string)
    "uniform batch summary"
    "2× BUY SPOT — BTC/USD · Kraken"
    (content payload)
;;

(** Perp fills read as position lifecycle actions, not generic buys/sells. *)
let test_short_lifecycle_labels () =
  let opened =
    create_fill_event
      ~symbol:"HYPE"
      ~side:"sell"
      ~market:"perp"
      ~intent:"open_short"
      ~fill_price:50.0
      ~strategy:"carry"
      1620000000.0
  in
  let closed =
    create_fill_event
      ~symbol:"HYPE"
      ~side:"buy"
      ~market:"perp"
      ~intent:"close_short"
      ~fill_price:50.0
      ~strategy:"carry"
      ~realized_pnl:0.42
      1620000010.0
  in
  let opened_payload = build (Notifier.build_webhook_payload [ opened ]) in
  let closed_payload = build (Notifier.build_webhook_payload [ closed ]) in
  Alcotest.(check string)
    "selling a perp opens a short"
    "OPEN SHORT — HYPE @ $50.00"
    (content opened_payload);
  Alcotest.(check string)
    "buying a perp closes a short"
    "CLOSE SHORT — HYPE @ $50.00"
    (content closed_payload);
  Alcotest.(check int) "opening a perp is blue" 0x3498DB (color opened_payload);
  Alcotest.(check int) "closing a perp is grey" 0x95A5A6 (color closed_payload);
  Alcotest.(check string)
    "closed short shows realized PnL"
    "+$0.42"
    (field_value "Realized PnL" closed_payload)
;;

(** A batch inherits a single fill's color only when every fill agrees. *)
let test_batch_color () =
  let batch_color fills = build (Notifier.build_webhook_payload fills) |> color in
  let perp_open symbol =
    create_fill_event
      ~symbol
      ~side:"buy"
      ~market:"perp"
      ~intent:"open_long"
      ~strategy:"carry"
      1620000000.0
  in
  let perp_close symbol =
    create_fill_event
      ~symbol
      ~side:"sell"
      ~market:"perp"
      ~intent:"close_long"
      ~strategy:"carry"
      1620000010.0
  in
  Alcotest.(check int)
    "all perp opens keep the open color"
    0x3498DB
    (batch_color [ perp_open "BTC/USD"; perp_open "ETH/USD" ]);
  Alcotest.(check int)
    "mixed perp open/close falls back to neutral"
    0x5865F2
    (batch_color [ perp_open "BTC/USD"; perp_close "ETH/USD" ])
;;

(** An account liquidation gets the warning treatment. *)
let test_liquidation_label () =
  let ev =
    create_fill_event
      ~venue:"hyperliquid"
      ~symbol:"ACCOUNT"
      ~side:""
      ~market:"perp"
      ~intent:"liquidation"
      ~strategy:"account"
      ?note:(Some "account_value=$12.00 liquidator=0xabc")
      1620000000.0
  in
  let payload = build (Notifier.build_webhook_payload [ ev ]) in
  Alcotest.(check string) "liquidation summary" "LIQUIDATION — ACCOUNT" (content payload);
  Alcotest.(check int) "liquidation color" 0x8B0000 (color payload);
  Alcotest.(check string)
    "liquidated notional is readable"
    "$75,000.00"
    (field_value "Liquidated notional" payload)
;;

(** Currency values stay readable across magnitudes. *)
let test_currency_formatting () =
  let check expected value =
    Alcotest.(check string) expected expected (Notifier.format_currency value)
  in
  check "$50,000.00" 50000.0;
  check "$1,234.57" 1234.5678;
  check "$50.00" 50.0;
  check "$0.1234" 0.1234;
  check "$0.00001234" 0.00001234;
  check "$0.0000000012" 0.0000000012;
  check "-$1,234.56" (-1234.56);
  Alcotest.(check string) "positive PnL" "+$0.42" (Notifier.pnl_str 0.42);
  Alcotest.(check string) "negative PnL" "-$12.00" (Notifier.pnl_str (-12.0))
;;

let contains haystack needle =
  let needle_len = String.length needle in
  let rec loop i =
    if i + needle_len > String.length haystack
    then false
    else if String.sub haystack i needle_len = needle
    then true
    else loop (i + 1)
  in
  loop 0
;;

(** Notifications carry no emoji. *)
let test_no_emoji () =
  let perp =
    create_fill_event
      ~symbol:"HYPE"
      ~market:"perp"
      ~intent:"open_long"
      ~fill_price:50.0
      1620000000.0
  in
  let liquidation =
    create_fill_event
      ~symbol:"ACCOUNT"
      ~market:"perp"
      ~intent:"liquidation"
      ~strategy:"account"
      1620000010.0
  in
  let json =
    Notifier.build_webhook_payload [ perp; liquidation ] |> build |> Yojson.Safe.to_string
  in
  List.iter
    (fun emoji -> Alcotest.(check bool) ("no " ^ emoji) false (contains json emoji))
    [ "🟢"; "🔴"; "🚨"; "➕"; "➖"; "🟩"; "🟥" ]
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
  ; "test_missing_price", `Quick, test_missing_price
  ; "test_format_multiple_fills", `Quick, test_format_multiple_fills
  ; "test_batch_summary", `Quick, test_batch_summary
  ; "test_short_lifecycle_labels", `Quick, test_short_lifecycle_labels
  ; "test_batch_color", `Quick, test_batch_color
  ; "test_liquidation_label", `Quick, test_liquidation_label
  ; "test_currency_formatting", `Quick, test_currency_formatting
  ; "test_no_emoji", `Quick, test_no_emoji
  ; ( "test_rate_limiter_logic"
    , `Quick
    , fun () -> Lwt_main.run (test_rate_limiter_logic () ()) )
  ]
;;

let () = Alcotest.run "Discord_notifier" [ "Notifier", suite ]
