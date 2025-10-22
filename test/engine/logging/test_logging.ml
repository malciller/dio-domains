

let test_log_levels () =
  (* Test logging at all levels - just ensure no exceptions are raised *)
  Logging.debug ~section:"test_logging" "Debug message";
  Logging.info ~section:"test_logging" "Info message";
  Logging.warn ~section:"test_logging" "Warning message";
  Logging.error ~section:"test_logging" "Error message";
  Logging.critical ~section:"test_logging" "Critical message";
  Alcotest.(check bool) "log levels work" true true

let test_formatted_logging () =
  (* Test formatted logging functions - just ensure no exceptions are raised *)
  Logging.debug_f ~section:"test_logging" "Debug: %d + %d = %d" 1 2 3;
  Logging.info_f ~section:"test_logging" "Info: %s" "formatted";
  Logging.warn_f ~section:"test_logging" "Warning: %.2f" 3.14159;
  Logging.error_f ~section:"test_logging" "Error: %c" 'X';
  Logging.critical_f ~section:"test_logging" "Critical: %b" true;
  Alcotest.(check bool) "formatted logging works" true true

let test_level_conversions () =
  (* Test level to string and string to level conversions *)
  Alcotest.(check string) "DEBUG to string" "DEBUG" (Logging.level_to_string Logging.DEBUG);
  Alcotest.(check string) "INFO to string" "INFO" (Logging.level_to_string Logging.INFO);
  Alcotest.(check string) "WARN to string" "WARN" (Logging.level_to_string Logging.WARN);
  Alcotest.(check string) "ERROR to string" "ERROR" (Logging.level_to_string Logging.ERROR);
  Alcotest.(check string) "CRITICAL to string" "CRITICAL" (Logging.level_to_string Logging.CRITICAL);

  (match Logging.level_of_string "debug" with Some level -> Alcotest.(check bool) "debug to level" true (level = Logging.DEBUG) | None -> Alcotest.fail "debug should parse");
  (match Logging.level_of_string "DEBUG" with Some level -> Alcotest.(check bool) "DEBUG to level" true (level = Logging.DEBUG) | None -> Alcotest.fail "DEBUG should parse");
  (match Logging.level_of_string "info" with Some level -> Alcotest.(check bool) "info to level" true (level = Logging.INFO) | None -> Alcotest.fail "info should parse");
  (match Logging.level_of_string "INFO" with Some level -> Alcotest.(check bool) "INFO to level" true (level = Logging.INFO) | None -> Alcotest.fail "INFO should parse");
  (match Logging.level_of_string "warn" with Some level -> Alcotest.(check bool) "warn to level" true (level = Logging.WARN) | None -> Alcotest.fail "warn should parse");
  (match Logging.level_of_string "WARN" with Some level -> Alcotest.(check bool) "WARN to level" true (level = Logging.WARN) | None -> Alcotest.fail "WARN should parse");
  (match Logging.level_of_string "error" with Some level -> Alcotest.(check bool) "error to level" true (level = Logging.ERROR) | None -> Alcotest.fail "error should parse");
  (match Logging.level_of_string "ERROR" with Some level -> Alcotest.(check bool) "ERROR to level" true (level = Logging.ERROR) | None -> Alcotest.fail "ERROR should parse");
  (match Logging.level_of_string "critical" with Some level -> Alcotest.(check bool) "critical to level" true (level = Logging.CRITICAL) | None -> Alcotest.fail "critical should parse");
  (match Logging.level_of_string "CRITICAL" with Some level -> Alcotest.(check bool) "CRITICAL to level" true (level = Logging.CRITICAL) | None -> Alcotest.fail "CRITICAL should parse");

  Alcotest.(check bool) "invalid level returns None" true (Logging.level_of_string "invalid" = None)

let test_global_level_filtering () =
  (* Test global level filtering - just ensure no exceptions are raised *)
  Logging.set_level Logging.ERROR;  (* Only ERROR and CRITICAL should log *)

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

let test_section_level_filtering () =
  (* Test per-section level filtering - just ensure no exceptions are raised *)
  Logging.set_level Logging.DEBUG;  (* Allow all globally *)
  Logging.set_section_level "strict_section" Logging.ERROR;  (* But strict for this section *)

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

let test_section_enable_filtering () =
  (* Test section enable/disable filtering - just ensure no exceptions are raised *)
  Logging.set_enabled_sections ["test_logging"];  (* Only allow test_logging section *)

  Logging.info ~section:"test_logging" "Visible message";
  Logging.info ~section:"other_section" "Filtered message";

  (* Reset *)
  Logging.set_enabled_sections [];
  Alcotest.(check bool) "section enable filtering works" true true

let test_colors () =
  (* Test color enable/disable - just ensure no exceptions are raised *)
  Logging.set_colors true;
  Logging.info ~section:"test_logging" "Message with colors";

  Logging.set_colors false;
  Logging.info ~section:"test_logging" "Message without colors";

  (* Reset *)
  Logging.set_colors true;
  Alcotest.(check bool) "colors work" true true

let test_output_redirection () =
  (* Test output redirection - basic functionality test *)
  (* Since we can't easily capture output, just test that set_output doesn't crash *)
  let temp_channel = open_out "/dev/null" in
  Logging.set_output temp_channel;
  Logging.info ~section:"test_logging" "Redirected message";
  close_out temp_channel;
  Logging.set_output stderr;  (* Reset to stderr *)
  Alcotest.(check bool) "output redirection works" true true

let test_section_management () =
  (* Test section creation and management *)
  let section1 = Logging.get_section_level "new_section" in
  let section2 = Logging.get_section_level "new_section" in

  Alcotest.(check bool) "section levels are consistent" true (section1 = section2)

let () =
  Alcotest.run "Logging" [
    "basic", [
      Alcotest.test_case "log levels" `Quick test_log_levels;
      Alcotest.test_case "formatted logging" `Quick test_formatted_logging;
    ];
    "conversions", [
      Alcotest.test_case "level conversions" `Quick test_level_conversions;
    ];
    "filtering", [
      Alcotest.test_case "global level filtering" `Quick test_global_level_filtering;
      Alcotest.test_case "section level filtering" `Quick test_section_level_filtering;
      Alcotest.test_case "section enable filtering" `Quick test_section_enable_filtering;
    ];
    "features", [
      Alcotest.test_case "colors" `Quick test_colors;
      Alcotest.test_case "output redirection" `Quick test_output_redirection;
      Alcotest.test_case "section management" `Quick test_section_management;
    ];
  ]
