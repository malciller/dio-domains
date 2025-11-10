(** Reporter - Generates memory leak reports in human-readable and JSON formats

    Creates comprehensive reports of memory leaks, domain issues, and allocation
    patterns. Integrates with telemetry system for real-time alerts.
*)

open Lwt.Infix

(** Open Allocation_tracker to access its types *)
open Allocation_tracker

(** Helper function to take first n elements from a list *)
let rec take n = function
  | [] -> []
  | x :: xs -> if n <= 0 then [] else x :: take (n - 1) xs

(** Report format *)
type report_format = [`Text | `Json | `Summary]

(** Memory report data structure *)
type memory_report = {
  timestamp: float;
  duration_seconds: float;
  total_allocations: int;
  active_allocations: int;
  total_memory_mb: float;
  tracked_memory_mb: float;  (* Memory tracked by our allocation system *)
  reuse_operations: int;  (* Number of data structure reuse operations *)
  recreation_operations: int;  (* Number of data structure recreation operations *)
  leaks_found: Leak_detector.leak_result list;
  domain_issues: Leak_detector.leak_result list;
  allocation_counts: (string * int) list;
  domain_counts: (int * int) list;
  function_per_domain_counts: (int * (string * int) list) list;  (* (domain_id, [(function_name, count)]) list *)
  allocations: Allocation_tracker.allocation_record list;
  recommendations: string list;
}

(** Generate comprehensive memory report *)
let generate_report () : memory_report Lwt.t =
  let start_time = Unix.gettimeofday () in

  (* Gather all data *)
  let alloc_stats = Allocation_tracker.get_allocation_stats () in
  let allocations : Allocation_tracker.allocation_record list = Allocation_tracker.get_all_active_allocations () in
  let leaks = Leak_detector.get_detailed_leak_report () in
  let domain_issues = Leak_detector.detect_domain_memory_issues () in
  let type_counts = Leak_detector.get_allocation_counts_by_type () in
  let domain_counts = Leak_detector.get_allocation_counts_by_domain () in
  let function_per_domain_counts = Leak_detector.get_allocation_counts_by_function_per_domain () in
  let memory_summary = Domain_profiler.get_memory_summary () in
  let reuse_count, recreation_count = Allocation_tracker.get_reuse_metrics () in

  (* Check for allocation tracking issues *)
  if alloc_stats.total_allocations = 0 && memory_summary.total_heap_mb > 10.0 then
    Logging.warn_f ~section:"memory_report" "WARNING: Memory usage is %.1f MB but no allocations tracked. Memory tracing may not be instrumented properly."
      memory_summary.total_heap_mb;

  (* Generate recommendations *)
  let recommendations = ref [] in

  if alloc_stats.total_allocations > 10000 then
    recommendations := "High allocation volume detected - consider optimizing allocation patterns" :: !recommendations;

  if alloc_stats.total_allocations = 0 && memory_summary.total_heap_mb > 10.0 then
    recommendations := "Allocation tracking shows 0 allocations but memory usage is high - check that tracked data structures are being used" :: !recommendations;

  if List.length leaks > 0 then
    recommendations := "Memory leaks detected - investigate allocation sites" :: !recommendations;

  if List.length domain_issues > 0 then
    recommendations := "Domain memory issues found - check domain-specific data structures" :: !recommendations;

  let critical_leaks = List.filter (fun l -> l.Leak_detector.severity = `Critical) leaks in
  if List.length critical_leaks > 0 then
    recommendations := "CRITICAL: Immediate action required for critical leaks" :: !recommendations;

  (* Check for frequent recreation patterns *)
  let current_time = Unix.gettimeofday () in
  let recent_allocations = List.filter (fun (alloc : Allocation_tracker.allocation_record) ->
    let age_seconds = current_time -. alloc.timestamp in
    age_seconds < 60.0  (* Look at last minute *)
  ) allocations in

  (* Group by allocation site and count frequency *)
  let site_counts = Allocation_tracker.TrackedHashtbl.create 32 in
  List.iter (fun alloc ->
    let key = Printf.sprintf "%s:%s:%d" alloc.site.file alloc.site.function_name alloc.site.line in
    let count = try Allocation_tracker.TrackedHashtbl.find site_counts key with Not_found -> 0 in
    Allocation_tracker.TrackedHashtbl.replace site_counts key (count + 1)
  ) recent_allocations;

  (* Check for sites with high frequency indicating potential improper recreation *)
  Allocation_tracker.TrackedHashtbl.iter (fun site count ->
    if count > 10 then  (* More than 10 allocations from same site in last minute *)
      recommendations := Printf.sprintf "HIGH FREQUENCY: %d allocations from %s in last minute - possible improper data structure recreation" count site :: !recommendations
  ) site_counts;

  let end_time = Unix.gettimeofday () in

  (* Add reuse ratio analysis to recommendations *)
  let total_operations = reuse_count + recreation_count in
  if total_operations > 0 then begin
    let reuse_ratio = float_of_int reuse_count /. float_of_int total_operations *. 100.0 in
    if reuse_ratio < 50.0 then
      recommendations := Printf.sprintf "LOW REUSE RATIO: Only %.1f%% of data structure operations are reusing existing structures (%d reuse, %d recreation)" reuse_ratio reuse_count recreation_count :: !recommendations
    else
      recommendations := Printf.sprintf "GOOD REUSE RATIO: %.1f%% of data structure operations are reusing existing structures (%d reuse, %d recreation)" reuse_ratio reuse_count recreation_count :: !recommendations
  end;

  Lwt.return {
    timestamp = end_time;
    duration_seconds = end_time -. start_time;
    total_allocations = alloc_stats.total_allocations;
    active_allocations = alloc_stats.active_allocations;
    total_memory_mb = memory_summary.total_heap_mb;
    tracked_memory_mb = float_of_int alloc_stats.total_memory_tracked /. 1024.0 /. 1024.0;
    reuse_operations = reuse_count;
    recreation_operations = recreation_count;
    leaks_found = leaks;
    domain_issues;
    allocation_counts = type_counts;
    domain_counts;
    function_per_domain_counts;
    allocations = allocations;
    recommendations = !recommendations;
  }

(** Format severity as string *)
let format_severity = function
  | `Low -> "LOW"
  | `Medium -> "MEDIUM"
  | `High -> "HIGH"
  | `Critical -> "CRITICAL"

(** Format severity with color codes for terminal output *)
let format_severity_colored = function
  | `Low -> "LOW"
  | `Medium -> "\027[33mMEDIUM\027[0m"  (* Yellow *)
  | `High -> "\027[31mHIGH\027[0m"     (* Red *)
  | `Critical -> "\027[35;1mCRITICAL\027[0m"  (* Bright magenta *)

(** Format timestamp as human-readable date/time *)
let format_timestamp timestamp =
  let tm = Unix.localtime timestamp in
  Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d"
    (tm.Unix.tm_year + 1900) (tm.Unix.tm_mon + 1) tm.Unix.tm_mday
    tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec

(** Format memory size in human-readable units *)
let format_memory_size bytes =
  if bytes >= 1048576 then (* 1 MB *)
    Printf.sprintf "%.1f MB" (float_of_int bytes /. 1048576.0)
  else if bytes >= 1024 then (* 1 KB *)
    Printf.sprintf "%.1f KB" (float_of_int bytes /. 1024.0)
  else
    Printf.sprintf "%d bytes" bytes

(** Format age since creation timestamp *)
let format_age timestamp =
  let now = Unix.gettimeofday () in
  let age_seconds = now -. timestamp in
  if age_seconds < 60.0 then
    Printf.sprintf "%.0fs ago" age_seconds
  else if age_seconds < 3600.0 then
    let minutes = age_seconds /. 60.0 in
    Printf.sprintf "%.0fm ago" minutes
  else if age_seconds < 86400.0 then
    let hours = age_seconds /. 3600.0 in
    Printf.sprintf "%.1fh ago" hours
  else
    let days = age_seconds /. 86400.0 in
    Printf.sprintf "%.1fd ago" days

(** Generate human-readable text report *)
let generate_text_report report =
  let buffer = Buffer.create 4096 in

  Buffer.add_string buffer "=======================================\n";
  Buffer.add_string buffer "      MEMORY LEAK DETECTION REPORT      \n";
  Buffer.add_string buffer "=======================================\n\n";

  Buffer.add_string buffer (Printf.sprintf "Report generated: %.0f\n"
    report.timestamp);
  Buffer.add_string buffer (Printf.sprintf "Analysis duration: %.2f seconds\n\n" report.duration_seconds);

  Buffer.add_string buffer "MEMORY OVERVIEW\n";
  Buffer.add_string buffer "---------------\n";
  Buffer.add_string buffer (Printf.sprintf "Total heap memory: %.2f MB (total OCaml heap)\n" report.total_memory_mb);
  Buffer.add_string buffer (Printf.sprintf "Tracked memory: %.2f MB (memory in tracked data structures)\n" report.tracked_memory_mb);
  Buffer.add_string buffer (Printf.sprintf "Total allocations: %d\n" report.total_allocations);
  Buffer.add_string buffer (Printf.sprintf "Active allocations: %d\n" report.active_allocations);
  Buffer.add_string buffer (Printf.sprintf "Reuse operations: %d\n" report.reuse_operations);
  Buffer.add_string buffer (Printf.sprintf "Recreation operations: %d\n" report.recreation_operations);
  Buffer.add_string buffer (Printf.sprintf "Leaks detected: %d\n" (List.length report.leaks_found));
  Buffer.add_string buffer (Printf.sprintf "Domain issues: %d\n\n" (List.length report.domain_issues));

  if report.allocation_counts <> [] then begin
    Buffer.add_string buffer "ALLOCATION COUNTS BY TYPE\n";
    Buffer.add_string buffer "-------------------------\n";
    List.iter (fun (typ, count) ->
      Buffer.add_string buffer (Printf.sprintf "  %s: %d\n" typ count)
    ) report.allocation_counts;
    Buffer.add_string buffer "\n";
  end;

  if report.domain_counts <> [] then begin
    Buffer.add_string buffer "ALLOCATION COUNTS BY DOMAIN\n";
    Buffer.add_string buffer "---------------------------\n";
    List.iter (fun (domain_id, count) ->
      Buffer.add_string buffer (Printf.sprintf "  Domain %d: %d\n" domain_id count)
    ) report.domain_counts;
    Buffer.add_string buffer "\n";
  end;

  if report.function_per_domain_counts <> [] then begin
    Buffer.add_string buffer "ALLOCATION COUNTS BY FUNCTION PER DOMAIN\n";
    Buffer.add_string buffer "----------------------------------------\n";
    Buffer.add_string buffer "Shows aggregated allocation counts per function within each domain\n";
    Buffer.add_string buffer "This gives a clear view of which functions are allocating most frequently.\n\n";

    (* Sort domains by total allocation count (highest first) *)
    let sorted_domains = List.sort (fun (_domain_a, funcs_a) (_domain_b, funcs_b) ->
      let total_a = List.fold_left (fun acc (_, count) -> acc + count) 0 funcs_a in
      let total_b = List.fold_left (fun acc (_, count) -> acc + count) 0 funcs_b in
      compare total_b total_a
    ) report.function_per_domain_counts in

    List.iter (fun (domain_id, function_counts) ->
      let total_in_domain = List.fold_left (fun acc (_, count) -> acc + count) 0 function_counts in
      Buffer.add_string buffer (Printf.sprintf "Domain %d: %d total allocations\n" domain_id total_in_domain);

      (* Sort functions within domain by count (highest first) *)
      let sorted_functions = List.sort (fun (_, count_a) (_, count_b) -> compare count_b count_a) function_counts in

      (* Show top functions, or all if there aren't many *)
      let display_functions = if List.length sorted_functions > 20 then take 20 sorted_functions else sorted_functions in

      Buffer.add_string buffer (Printf.sprintf "  %-50s | %-8s\n" "Function" "Count");
      Buffer.add_string buffer (String.make 62 '-');
      Buffer.add_string buffer "\n";

      List.iter (fun (func_name, count) ->
        Buffer.add_string buffer (Printf.sprintf "  %-50s | %-8d\n" func_name count)
      ) display_functions;

      if List.length sorted_functions > 20 then begin
        Buffer.add_string buffer (Printf.sprintf "  (... and %d more functions)\n" (List.length sorted_functions - 20));
      end;

      Buffer.add_string buffer "\n";
    ) sorted_domains;
  end;

  if report.leaks_found <> [] then begin
    Buffer.add_string buffer "MEMORY LEAKS DETECTED\n";
    Buffer.add_string buffer "--------------------\n";
    List.iteri (fun i (leak : Leak_detector.leak_result) ->
      Buffer.add_string buffer (Printf.sprintf "[%d] %s - %s\n"
        (i + 1) leak.Leak_detector.structure_type (format_severity_colored leak.severity));
      Buffer.add_string buffer (Printf.sprintf "    Location: %s:%d in %s\n"
        leak.allocation_site.file leak.allocation_site.line leak.allocation_site.function_name);
      Buffer.add_string buffer (Printf.sprintf "    Domain: %d\n" leak.domain_id);
      Buffer.add_string buffer (Printf.sprintf "    Growth rate: %.2f bytes/sec over %.1f seconds\n"
        leak.growth_rate leak.time_window);
      Buffer.add_string buffer (Printf.sprintf "    Current size: %d\n" leak.current_size);
      Buffer.add_string buffer (Printf.sprintf "    Description: %s\n" leak.description);
      if leak.recommendations <> [] then begin
        Buffer.add_string buffer "    Recommendations:\n";
        List.iter (fun rec_item -> Buffer.add_string buffer (Printf.sprintf "      - %s\n" rec_item)) leak.recommendations;
      end;
      Buffer.add_string buffer "\n";
    ) report.leaks_found;
  end;

  if report.domain_issues <> [] then begin
    Buffer.add_string buffer "DOMAIN MEMORY ISSUES\n";
    Buffer.add_string buffer "--------------------\n";
    List.iteri (fun i (issue : Leak_detector.leak_result) ->
      Buffer.add_string buffer (Printf.sprintf "[%d] %s - %s\n"
        (i + 1) issue.Leak_detector.structure_type (format_severity_colored issue.severity));
      Buffer.add_string buffer (Printf.sprintf "    Description: %s\n" issue.description);
      if issue.recommendations <> [] then begin
        Buffer.add_string buffer "    Recommendations:\n";
        List.iter (fun rec_item -> Buffer.add_string buffer (Printf.sprintf "      - %s\n" rec_item)) issue.recommendations;
      end;
      Buffer.add_string buffer "\n";
    ) report.domain_issues;
  end;

  if report.recommendations <> [] then begin
    Buffer.add_string buffer "RECOMMENDATIONS\n";
    Buffer.add_string buffer "---------------\n";
    List.iter (fun rec_item ->
      Buffer.add_string buffer (Printf.sprintf "- %s\n" rec_item)
    ) report.recommendations;
    Buffer.add_string buffer "\n";
  end;

  Buffer.contents buffer

(** Generate JSON report *)
let generate_json_report report =

  (** Convert allocation record to JSON *)
  let allocation_to_json (alloc : Allocation_tracker.allocation_record) =
    let age_seconds = Unix.gettimeofday () -. alloc.timestamp in
    `Assoc [
      ("id", `Int alloc.id);
      ("structure_type", `String alloc.structure_type);
      ("size", `Int alloc.size);
      ("domain_id", `Int alloc.domain_id);
      ("timestamp", `Float alloc.timestamp);
      ("age_seconds", `Float age_seconds);
      ("allocation_site", `Assoc [
        ("file", `String alloc.site.file);
        ("function_name", `String alloc.site.function_name);
        ("line", `Int alloc.site.line);
        ("backtrace", `String alloc.site.backtrace);
      ]);
    ]
  in

  let leak_to_json (leak : Leak_detector.leak_result) =
    `Assoc [
      ("structure_type", `String leak.Leak_detector.structure_type);
      ("severity", `String (format_severity leak.severity));
      ("domain_id", `Int leak.domain_id);
      ("growth_rate", `Float leak.growth_rate);
      ("current_size", `Int leak.current_size);
      ("time_window", `Float leak.time_window);
      ("description", `String leak.description);
      ("allocation_site", `Assoc [
        ("file", `String leak.allocation_site.file);
        ("function_name", `String leak.allocation_site.function_name);
        ("line", `Int leak.allocation_site.line);
      ]);
      ("recommendations", `List (List.map (fun r -> `String r) leak.recommendations));
    ]
  in

  let allocation_counts = `Assoc (List.map (fun (typ, count) -> (typ, `Int count)) report.allocation_counts) in
  let domain_counts = `Assoc (List.map (fun (domain_id, count) ->
    (string_of_int domain_id, `Int count)) report.domain_counts) in
  let function_per_domain_counts = `Assoc (List.map (fun (domain_id, function_counts) ->
    (string_of_int domain_id, `Assoc (List.map (fun (func_name, count) -> (func_name, `Int count)) function_counts))
  ) report.function_per_domain_counts) in

  `Assoc [
    ("timestamp", `Float report.timestamp);
    ("duration_seconds", `Float report.duration_seconds);
    ("total_allocations", `Int report.total_allocations);
    ("active_allocations", `Int report.active_allocations);
    ("total_memory_mb", `Float report.total_memory_mb);
    ("tracked_memory_mb", `Float report.tracked_memory_mb);
    ("reuse_operations", `Int report.reuse_operations);
    ("recreation_operations", `Int report.recreation_operations);
    ("leaks_found", `List (List.map leak_to_json report.leaks_found));
    ("domain_issues", `List (List.map leak_to_json report.domain_issues));
    ("allocation_counts", allocation_counts);
    ("domain_counts", domain_counts);
    ("function_per_domain_counts", function_per_domain_counts);
    ("allocations", `List (List.map allocation_to_json report.allocations));
    ("recommendations", `List (List.map (fun r -> `String r) report.recommendations));
  ]

(** Generate summary report (brief overview) *)
let generate_summary_report report =
  let critical_leaks = List.filter (fun l -> l.Leak_detector.severity = `Critical) report.leaks_found in
  let high_leaks = List.filter (fun l -> l.Leak_detector.severity = `High) report.leaks_found in

  Printf.sprintf "Memory Report: %.1f MB heap (%.1f MB tracked), %d active allocations, %d leaks (%d critical, %d high), %d domain issues"
    report.total_memory_mb
    report.tracked_memory_mb
    report.active_allocations
    (List.length report.leaks_found)
    (List.length critical_leaks)
    (List.length high_leaks)
    (List.length report.domain_issues)

(** Ensure report directory exists *)
let ensure_report_directory () =
  let report_dir = Config.get_report_directory () in
  try
    Unix.mkdir report_dir 0o755
  with Unix.Unix_error (Unix.EEXIST, _, _) ->
    ()  (* Directory already exists *)
  | exn ->
    Logging.warn_f ~section:"reporter" "Failed to create report directory %s: %s" report_dir (Printexc.to_string exn)

(** Write report to file *)
let write_report_to_file report format filename =
  try
    (* Ensure directory exists before writing *)
    ensure_report_directory ();

    let content = match format with
      | `Text -> generate_text_report report
      | `Json -> Yojson.Basic.to_string (generate_json_report report)
      | `Summary -> generate_summary_report report
    in
    let oc = open_out filename in
    Fun.protect ~finally:(fun () -> close_out oc)
      (fun () -> output_string oc content);
    Logging.info_f ~section:"reporter" "Memory report written to %s (%s format)" filename
      (match format with `Text -> "text" | `Json -> "json" | `Summary -> "summary");
    true
  with exn ->
    Logging.error_f ~section:"reporter" "Failed to write report to %s: %s" filename (Printexc.to_string exn);
    false

(** Send report via logging system (for real-time monitoring) *)
let log_report report =
  let summary = generate_summary_report report in
  Logging.info_f ~section:"memory_report" "%s" summary;

  (* Log critical issues separately *)
  let critical_leaks = List.filter (fun l -> l.Leak_detector.severity = `Critical) report.leaks_found in
  let critical_domain_issues = List.filter (fun l -> l.Leak_detector.severity = `Critical) report.domain_issues in

  if critical_leaks <> [] || critical_domain_issues <> [] then begin
    Logging.warn_f ~section:"memory_report" "CRITICAL MEMORY ISSUES DETECTED: %d critical leaks, %d critical domain issues"
      (List.length critical_leaks) (List.length critical_domain_issues);

    (* Log details of critical issues *)
    List.iter (fun (leak : Leak_detector.leak_result) ->
      Logging.warn_f ~section:"memory_report" "CRITICAL LEAK: %s at %s:%d - %s"
        leak.structure_type leak.allocation_site.file leak.allocation_site.line leak.description;
    ) critical_leaks;

    List.iter (fun (issue : Leak_detector.leak_result) ->
      Logging.warn_f ~section:"memory_report" "CRITICAL DOMAIN ISSUE: %s - %s"
        issue.structure_type issue.description;
    ) critical_domain_issues;
  end

(** Generate and save periodic reports *)
let generate_periodic_report () =
  generate_report () >>= fun report ->

  (* Always log the summary *)
  log_report report;

  (* Save detailed report if configured *)
  let timestamp_str = Printf.sprintf "%.0f" report.timestamp in
  let report_dir = Config.get_report_directory () in
  let text_filename = Printf.sprintf "%s/%s_%s.txt" report_dir (Config.get_report_file_prefix ()) timestamp_str in
  let json_filename = Printf.sprintf "%s/%s_%s.json" report_dir (Config.get_report_file_prefix ()) timestamp_str in

  (* Write both text and JSON reports *)
  let text_ok = write_report_to_file report `Text text_filename in
  let json_ok = write_report_to_file report `Json json_filename in

  if text_ok && json_ok then
    Logging.debug ~section:"reporter" "Periodic memory reports generated successfully"
  else
    Logging.warn ~section:"reporter" "Some periodic memory reports failed to generate";

  Lwt.return_unit

(** Start periodic reporting *)
let start_periodic_reporting interval_seconds =
  Logging.info_f ~section:"reporter" "Starting periodic memory reporting every %d seconds" interval_seconds;

  Lwt.async (fun () ->
    let rec report_loop () =
      Lwt_unix.sleep (float_of_int interval_seconds) >>= fun () ->
      generate_periodic_report () >>= fun () ->
      report_loop ()
    in
    report_loop ()
  )

(** Generate on-demand report *)
let generate_report_now format =
  generate_report () >>= fun report ->
  match format with
  | `Text -> Lwt.return (generate_text_report report)
  | `Json -> Lwt.return (Yojson.Basic.to_string (generate_json_report report))
  | `Summary -> Lwt.return (generate_summary_report report)

(** Initialize reporter *)
let init () =
  Logging.info ~section:"reporter" "Memory leak reporter initialized"
