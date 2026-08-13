let test_log_levels () =
  (* Test logging at all levels - just ensure no exceptions are raised *)
  Logging.debug ~section:"test_logging" "Debug message";
  Logging.info ~section:"test_logging" "Info message";
  Logging.warn ~section:"test_logging" "Warning message";
  Logging.error ~section:"test_logging" "Error message";
  Logging.critical ~section:"test_logging" "Critical message";
  Alcotest.(check bool) "log levels work" true true
;;

let test_formatted_logging () =
  (* Test formatted logging functions - just ensure no exceptions are raised *)
  Logging.debug_f ~section:"test_logging" "Debug: %d + %d = %d" 1 2 3;
  Logging.info_f ~section:"test_logging" "Info: %s" "formatted";
  Logging.warn_f ~section:"test_logging" "Warning: %.2f" 3.14159;
  Logging.error_f ~section:"test_logging" "Error: %c" 'X';
  Logging.critical_f ~section:"test_logging" "Critical: %b" true;
  Alcotest.(check bool) "formatted logging works" true true
;;

let test_level_conversions () =
  (* Test level to string and string to level conversions *)
  Alcotest.(check string)
    "DEBUG to string"
    "DEBUG"
    (Logging.level_to_string Logging.DEBUG);
  Alcotest.(check string) "INFO to string" "INFO" (Logging.level_to_string Logging.INFO);
  Alcotest.(check string) "WARN to string" "WARN" (Logging.level_to_string Logging.WARN);
  Alcotest.(check string)
    "ERROR to string"
    "ERROR"
    (Logging.level_to_string Logging.ERROR);
  Alcotest.(check string)
    "CRITICAL to string"
    "CRITICAL"
    (Logging.level_to_string Logging.CRITICAL);
  (match Logging.level_of_string "debug" with
   | Some level -> Alcotest.(check bool) "debug to level" true (level = Logging.DEBUG)
   | None -> Alcotest.fail "debug should parse");
  (match Logging.level_of_string "DEBUG" with
   | Some level -> Alcotest.(check bool) "DEBUG to level" true (level = Logging.DEBUG)
   | None -> Alcotest.fail "DEBUG should parse");
  (match Logging.level_of_string "info" with
   | Some level -> Alcotest.(check bool) "info to level" true (level = Logging.INFO)
   | None -> Alcotest.fail "info should parse");
  (match Logging.level_of_string "INFO" with
   | Some level -> Alcotest.(check bool) "INFO to level" true (level = Logging.INFO)
   | None -> Alcotest.fail "INFO should parse");
  (match Logging.level_of_string "warn" with
   | Some level -> Alcotest.(check bool) "warn to level" true (level = Logging.WARN)
   | None -> Alcotest.fail "warn should parse");
  (match Logging.level_of_string "WARN" with
   | Some level -> Alcotest.(check bool) "WARN to level" true (level = Logging.WARN)
   | None -> Alcotest.fail "WARN should parse");
  (match Logging.level_of_string "error" with
   | Some level -> Alcotest.(check bool) "error to level" true (level = Logging.ERROR)
   | None -> Alcotest.fail "error should parse");
  (match Logging.level_of_string "ERROR" with
   | Some level -> Alcotest.(check bool) "ERROR to level" true (level = Logging.ERROR)
   | None -> Alcotest.fail "ERROR should parse");
  (match Logging.level_of_string "critical" with
   | Some level ->
     Alcotest.(check bool) "critical to level" true (level = Logging.CRITICAL)
   | None -> Alcotest.fail "critical should parse");
  (match Logging.level_of_string "CRITICAL" with
   | Some level ->
     Alcotest.(check bool) "CRITICAL to level" true (level = Logging.CRITICAL)
   | None -> Alcotest.fail "CRITICAL should parse");
  Alcotest.(check bool)
    "invalid level returns None"
    true
    (Logging.level_of_string "invalid" = None)
;;

let test_global_level_filtering () =
  (* Test global level filtering - just ensure no exceptions are raised *)
  Logging.set_level Logging.ERROR;
  (* Only ERROR and CRITICAL should log *)

  (* These should be filtered out *)
  Logging.debug ~section:"test_logging" "Filtered debug";
  Logging.info ~section:"test_logging" "Filtered info";
  Logging.warn ~section:"test_logging" "Filtered warn";
  (* These should appear *)
  Logging.error ~section:"test_logging" "Visible error";
  Logging.critical ~section:"test_logging" "Visible critical";
  (* Reset to INFO for other tests *)
  Logging.set_level Logging.INFO;
  Alcotest.(check bool) "global level filtering works" true true
;;

let test_section_level_filtering () =
  (* Test per-section level filtering - just ensure no exceptions are raised *)
  Logging.set_level Logging.DEBUG;
  (* Allow all globally *)
  Logging.set_section_level "strict_section" Logging.ERROR;
  (* But strict for this section *)

  (* Global section should log all *)
  Logging.debug ~section:"test_logging" "Global debug";
  Logging.info ~section:"test_logging" "Global info";
  Logging.warn ~section:"test_logging" "Global warn";
  Logging.error ~section:"test_logging" "Global error";
  (* Strict section should only log ERROR and CRITICAL *)
  Logging.debug ~section:"strict_section" "Filtered debug";
  Logging.info ~section:"strict_section" "Filtered info";
  Logging.warn ~section:"strict_section" "Filtered warn";
  Logging.error ~section:"strict_section" "Visible error";
  Logging.critical ~section:"strict_section" "Visible critical";
  (* Reset *)
  Logging.set_level Logging.INFO;
  Alcotest.(check bool) "section level filtering works" true true
;;

let test_section_enable_filtering () =
  (* Test section enable/disable filtering - just ensure no exceptions are raised *)
  Logging.set_enabled_sections [ "test_logging" ];
  (* Only allow test_logging section *)
  Logging.info ~section:"test_logging" "Visible message";
  Logging.info ~section:"other_section" "Filtered message";
  (* Reset *)
  Logging.set_enabled_sections [];
  Alcotest.(check bool) "section enable filtering works" true true
;;

let test_colors () =
  (* Test color enable/disable - just ensure no exceptions are raised *)
  Logging.set_colors true;
  Logging.info ~section:"test_logging" "Message with colors";
  Logging.set_colors false;
  Logging.info ~section:"test_logging" "Message without colors";
  (* Reset *)
  Logging.set_colors true;
  Alcotest.(check bool) "colors work" true true
;;

let test_output_redirection () =
  (* Test output redirection - basic functionality test *)
  (* Since we can't easily capture output, just test that set_output doesn't crash *)
  let temp_channel = open_out "/dev/null" in
  Logging.set_output temp_channel;
  Logging.info ~section:"test_logging" "Redirected message";
  close_out temp_channel;
  Logging.set_output stderr;
  (* Reset to stderr *)
  Alcotest.(check bool) "output redirection works" true true
;;

let test_section_management () =
  (* Test section creation and management *)
  let section1 = Logging.get_section_level "new_section" in
  let section2 = Logging.get_section_level "new_section" in
  Alcotest.(check bool) "section levels are consistent" true (section1 = section2)
;;

(* ---- New format-line behavior ----
   [Logging.format_line] is pure (no I/O, no queue), so we can assert on the
   exact rendered output. The section column is FIXED at 20 chars, so the
   message column is always 40 (12 timestamp + 1 + 5 level + 1 + 20 section
   + 1). The stability of that column is the whole point of the layout. *)

let msg_col = 40

let test_format_line_no_full_date () =
  Logging.set_colors false;
  let line = Logging.format_line Logging.INFO "main" "hello" in
  (* Timestamp is compact HH:MM:SS.mmm — no YYYY-MM-DD prefix. *)
  Alcotest.(check bool)
    "timestamp is time-only"
    true
    (String.length line >= 12
     && String.get line 2 = ':'
     && String.get line 5 = ':'
     && String.get line 8 = '.')
;;

let test_format_line_alignment () =
  Logging.set_colors false;
  let l1 = Logging.format_line Logging.INFO "main" "first message" in
  let l2 = Logging.format_line Logging.WARN "oracle_runtime" "second message" in
  let col1 = String.index l1 'f' in
  let col2 = String.index l2 's' in
  Alcotest.(check int) "messages align at the same column" col1 col2;
  Alcotest.(check int) "message column is fixed at 40" msg_col col1;
  (* Level column is fixed width (field spans 13..17, so 17 is a pad space). *)
  Alcotest.(check char) "level padded to 5" ' ' (String.get l2 17);
  (* Section column is fixed at 20 (field spans 19..38, so 38 is a pad space). *)
  Alcotest.(check char) "section padded" ' ' (String.get l1 38)
;;

(* Regression: the message column must be the same before AND after a long
   section name appears — it must never shift mid-stream. *)
let test_format_line_stable_column () =
  Logging.set_colors false;
  let a = Logging.format_line Logging.INFO "main" "alpha" in
  let b = Logging.format_line Logging.INFO "hyperliquid_startup" "beta" in
  let c = Logging.format_line Logging.INFO "main" "gamma" in
  Alcotest.(check string) "short, first" "alpha" (String.sub a msg_col 5);
  Alcotest.(check string) "long stays" "beta" (String.sub b msg_col 4);
  Alcotest.(check string) "short, later" "gamma" (String.sub c msg_col 5)
;;

let test_format_line_multiline () =
  Logging.set_colors false;
  let line =
    Logging.format_line Logging.INFO "oracle_runtime" "header line\n      funding: detail"
  in
  let lines = String.split_on_char '\n' line in
  Alcotest.(check int) "two lines rendered" 2 (List.length lines);
  match lines with
  | [ _; cont ] ->
    (* Continuation is indented to the message column (where the header's
       message starts) plus the message's own 6-space relative indent. *)
    let head_msg_col = String.index line 'h' in
    let cont_first = String.index cont 'f' in
    Alcotest.(check int) "header starts at message column" msg_col head_msg_col;
    Alcotest.(check int)
      "continuation indented to message column"
      (head_msg_col + 6)
      cont_first
  | _ -> Alcotest.fail "expected two lines"
;;

let test_format_line_no_color () =
  Logging.set_colors false;
  let line = Logging.format_line Logging.INFO "main" "plain message" in
  Logging.set_colors true;
  Alcotest.(check bool) "no ANSI escapes" true (not (String.contains line '\027'));
  (* Alignment is preserved without colors: "main" padded to 20. *)
  Alcotest.(check char) "section padded without color" ' ' (String.get line 38)
;;

let test_format_line_colors () =
  Logging.set_colors true;
  let line = Logging.format_line Logging.WARN "oracle_runtime" "colored message" in
  Alcotest.(check bool) "level color present" true (String.contains line '\027')
;;

(* Visual demo of the rendered layout (colors on), so the format can be
   eyeballed during development. Not an assertion. *)
let demo_format () =
  Logging.set_colors true;
  let samples =
    [ Logging.INFO, "main", "Starting Dio Trading Engine..."
    ; Logging.INFO, "supervisor", "Loaded 8 trading configuration(s)"
    ; Logging.INFO, "domain_spawner", "Domain kraken/ETH/USD started successfully"
    ; ( Logging.INFO
      , "oracle_runtime"
      , "[2/8] hyperliquid/BTC/USDC ACTIVE — buy 0.0005 BTC every 0.75%\n\
         worst drop 83.6% (peak $19497.40 on 2017-12-16 → valley $3191.30 on 2018-12-15)\n\
        \      funding: drop 67.4% to floor $20688.95 (ATH $126400.00 − 83.6% worst)\n\
        \      sizing: gi 0.7500% (grid max 0.75%) · qty 0.0005 (minimum qty 0.0005)" )
    ; ( Logging.WARN
      , "oracle_replay"
      , "only 3 independent 180-session windows for HYPE/USDC" )
    ; ( Logging.INFO
      , "suicide_grid"
      , "Order cancellation for QQQ ignored for tracking reset" )
    ; ( Logging.INFO
      , "order_processor"
      , "✓ Order amended successfully: buy QQQ 0.03879045 @ 730.99" )
    ]
  in
  List.iter
    (fun (lvl, sec, msg) -> print_endline (Logging.format_line lvl sec msg))
    samples
;;

let () =
  if Array.exists (( = ) "--demo") Sys.argv
  then demo_format ()
  else
    Alcotest.run
      "Logging"
      [ ( "basic"
        , [ Alcotest.test_case "log levels" `Quick test_log_levels
          ; Alcotest.test_case "formatted logging" `Quick test_formatted_logging
          ] )
      ; ( "conversions"
        , [ Alcotest.test_case "level conversions" `Quick test_level_conversions ] )
      ; ( "filtering"
        , [ Alcotest.test_case "global level filtering" `Quick test_global_level_filtering
          ; Alcotest.test_case
              "section level filtering"
              `Quick
              test_section_level_filtering
          ; Alcotest.test_case
              "section enable filtering"
              `Quick
              test_section_enable_filtering
          ] )
      ; ( "features"
        , [ Alcotest.test_case "colors" `Quick test_colors
          ; Alcotest.test_case "output redirection" `Quick test_output_redirection
          ; Alcotest.test_case "section management" `Quick test_section_management
          ] )
      ; ( "format"
        , [ Alcotest.test_case "no full date" `Quick test_format_line_no_full_date
          ; Alcotest.test_case "column alignment" `Quick test_format_line_alignment
          ; Alcotest.test_case "stable column" `Quick test_format_line_stable_column
          ; Alcotest.test_case "multi-line indent" `Quick test_format_line_multiline
          ; Alcotest.test_case "no-color layout" `Quick test_format_line_no_color
          ; Alcotest.test_case "colors enabled" `Quick test_format_line_colors
          ] )
      ]
;;
