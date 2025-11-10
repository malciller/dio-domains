(** Simple test to verify memory reporting enhancements *)

open Lwt.Infix

let () =
  (* Initialize logging *)
  Logging.init ();
  Logging.set_level Logging.INFO;

  (* Initialize memory tracing *)
  let result =
    if Dio_memory_tracing.Memory_tracing.init () then begin
      Printf.printf "Memory tracing initialized\n";

      (* Generate a report *)
      Dio_memory_tracing.Reporter.generate_report () >>= fun report ->
      let json = Dio_memory_tracing.Reporter.generate_json_report report in
      let text = Dio_memory_tracing.Reporter.generate_text_report report in
      let json_str = Yojson.Basic.to_string json in

      (* Write to files *)
      let oc_json = open_out "test_memory_report.json" in
      output_string oc_json json_str;
      close_out oc_json;

      let oc_text = open_out "test_memory_report.txt" in
      output_string oc_text text;
      close_out oc_text;

      Printf.printf "Reports generated and saved to test_memory_report.json and test_memory_report.txt\n";
      Printf.printf "Number of allocations in report: %d\n" (List.length report.allocations);

      (* Print first 500 chars of text report to check if ACTIVE ALLOCATIONS section is present *)
      let preview = if String.length text > 500 then String.sub text 0 500 else text in
      Printf.printf "Text report preview:\n%s\n[...]\n" preview;

      Lwt.return_unit
    end else begin
      Printf.printf "Failed to initialize memory tracing\n";
      Lwt.return_unit
    end
  in
  result
  |> Lwt_main.run
