(* Tests for the decision-flow telemetry: pure rendering (stable, aligned, gutter-ready)
   and the throttle/dedup state machine. Output capture is avoided — the transport writes
   asynchronously — so the assertions target [render] and [should_emit] directly. *)

let no_colors () = Logging.set_colors false

let test_render_aligned () =
  no_colors ();
  let out =
    Flow_log.render
      ~category:Flow_log.Signal
      ~headline:"⟨BTC/USDC⟩ α=+2.3σ"
      ~fields:[ "book", "bid 100"; "oracle", "ACTIVE" ]
  in
  (* Header = glyph + TAG + two spaces + headline; then one row per field, labels padded
     to the widest label and separated by a single space. No leading indent: the transport
     supplies the [┆] gutter for continuation lines. *)
  Alcotest.(check string)
    "aligned block"
    "◢ SIGNAL  ⟨BTC/USDC⟩ α=+2.3σ\nbook   bid 100\noracle ACTIVE"
    out
;;

let test_render_each_category_has_tag () =
  no_colors ();
  List.iter
    (fun (cat, tag) ->
      let out = Flow_log.render ~category:cat ~headline:"x" ~fields:[] in
      let expected = Printf.sprintf "%s %s  x" (Flow_log.category_glyph cat) tag in
      Alcotest.(check string) (Printf.sprintf "category header %s" tag) expected out)
    [ Flow_log.Signal, "SIGNAL"
    ; Flow_log.Gate, "GATE"
    ; Flow_log.Send, "SEND"
    ; Flow_log.Amend, "AMEND"
    ; Flow_log.Cancel, "CANCEL"
    ; Flow_log.Fill, "FILL"
    ; Flow_log.Risk, "RISK"
    ; Flow_log.Pulse, "PULSE"
    ]
;;

let test_disabled_by_default () =
  Flow_log.configure Flow_log.default_config;
  Alcotest.(check bool) "default config is disabled" false (Flow_log.enabled ())
;;

let test_dedup_and_change () =
  Flow_log.configure
    { Flow_log.enabled = true
    ; min_interval_s = 0.0
    ; heartbeat_s = 1000.0
    ; pulse_s = 0.0
    ; price_ticks = true
    };
  Flow_log.reset ();
  Alcotest.(check bool) "first emit" true (Flow_log.should_emit ~key:"k" ~signature:"a");
  Alcotest.(check bool)
    "identical signature suppressed"
    false
    (Flow_log.should_emit ~key:"k" ~signature:"a");
  Alcotest.(check bool)
    "changed signature emits"
    true
    (Flow_log.should_emit ~key:"k" ~signature:"b");
  Alcotest.(check bool)
    "independent key emits"
    true
    (Flow_log.should_emit ~key:"other" ~signature:"a")
;;

let test_heartbeat () =
  (* heartbeat 0 => any elapsed window re-emits an unchanged signature. *)
  Flow_log.configure
    { Flow_log.enabled = true
    ; min_interval_s = 0.0
    ; heartbeat_s = 0.0
    ; pulse_s = 0.0
    ; price_ticks = true
    };
  Flow_log.reset ();
  ignore (Flow_log.should_emit ~key:"h" ~signature:"x");
  Alcotest.(check bool)
    "heartbeat re-emits unchanged"
    true
    (Flow_log.should_emit ~key:"h" ~signature:"x")
;;

let test_min_interval_floors () =
  (* A high floor suppresses even a changed signature until it elapses. *)
  Flow_log.configure
    { Flow_log.enabled = true
    ; min_interval_s = 1000.0
    ; heartbeat_s = 0.0
    ; pulse_s = 0.0
    ; price_ticks = true
    };
  Flow_log.reset ();
  ignore (Flow_log.should_emit ~key:"m" ~signature:"x");
  Alcotest.(check bool)
    "min interval floors a change"
    false
    (Flow_log.should_emit ~key:"m" ~signature:"y")
;;

let () =
  Alcotest.run
    "flow_log"
    [ ( "render"
      , [ Alcotest.test_case "aligned" `Quick test_render_aligned
        ; Alcotest.test_case "category tags" `Quick test_render_each_category_has_tag
        ] )
    ; ( "config"
      , [ Alcotest.test_case "disabled by default" `Quick test_disabled_by_default ] )
    ; ( "throttle"
      , [ Alcotest.test_case "dedup + change" `Quick test_dedup_and_change
        ; Alcotest.test_case "heartbeat" `Quick test_heartbeat
        ; Alcotest.test_case "min interval floor" `Quick test_min_interval_floors
        ] )
    ]
;;
