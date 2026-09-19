(** `dio strategy` commands: `validate <file>` and `diff <a.json> <b.json>`.

    Returns an exit code (0 = ok, 1 = errors/divergence, 2 = usage). [maybe_run] returns
    [None] when the argv does not select a strategy command, so the engine can start
    normally. *)

let usage =
  "Usage:\n\
  \  dio strategy validate <file>\n\
  \  dio strategy compile <file.strategy> [-o out.json]\n\
  \  dio strategy diff <a> <b>\n\
  \  dio strategy lsp"
;;

let print_diagnostic (d : Strategy_compile.diagnostic) =
  Printf.printf "  %-7s %s: %s\n" (Strategy_compile.severity_string d.sev) d.where d.msg
;;

let validate_file (path : string) : int =
  Strategy_actions_builtin.register_all ();
  match Strategy_loader.parse_file ~path with
  | Error msg ->
    Printf.eprintf "strategy: %s: parse failed: %s\n" path msg;
    1
  | Ok f ->
    let diags = Strategy_compile.validate f in
    let errors = Strategy_compile.errors diags
    and warnings = Strategy_compile.warnings diags in
    if diags = []
    then Printf.printf "strategy %S (%s): ok\n" f.name path
    else (
      Printf.printf
        "strategy %S (%s): %d error(s), %d warning(s)\n"
        f.name
        path
        (List.length errors)
        (List.length warnings);
      List.iter print_diagnostic diags);
    if Strategy_compile.has_errors diags then 1 else 0
;;

let compile_file (path : string) (output : string option) : int =
  Strategy_actions_builtin.register_all ();
  match Strategy_loader.parse_file ~path with
  | Error msg ->
    Printf.eprintf "strategy: %s: parse failed: %s\n" path msg;
    2
  | Ok f ->
    let diags = Strategy_compile.validate f in
    if Strategy_compile.has_errors diags
    then (
      Printf.printf
        "strategy %S (%s): %d error(s)\n"
        f.name
        path
        (List.length (Strategy_compile.errors diags));
      List.iter print_diagnostic diags;
      2)
    else (
      let json = Strategy_file.to_json f in
      let rendered = Yojson.Basic.pretty_to_string json in
      (match output with
       | None -> Printf.printf "strategy %S (%s): ok\n%s\n" f.name path rendered
       | Some out ->
         let ch = open_out out in
         output_string ch rendered;
         close_out ch;
         Printf.printf "strategy %S: compiled to %s\n" f.name out);
      0)
;;

let diff_files (a : string) (b : string) : int =
  try
    let ta = Strategy_trace.load a
    and tb = Strategy_trace.load b in
    match Strategy_trace.compare ta tb with
    | None ->
      Printf.printf "equivalent: %s == %s\n" a b;
      0
    | Some msg ->
      Printf.printf "divergence: %s\n" msg;
      1
  with
  | exn ->
    Printf.eprintf "strategy diff failed: %s\n" (Printexc.to_string exn);
    1
;;

let maybe_run (argv : string array) : int option =
  match Array.to_list argv with
  | _ :: "strategy" :: "validate" :: file :: _ -> Some (validate_file file)
  | _ :: "strategy" :: "compile" :: file :: rest ->
    let output =
      match rest with
      | "-o" :: out :: _ -> Some out
      | _ -> None
    in
    Some (compile_file file output)
  | _ :: "strategy" :: "diff" :: a :: b :: _ -> Some (diff_files a b)
  | _ :: "strategy" :: "lsp" :: _ -> Some (Strategy_lsp.run ())
  | _ :: "strategy" :: _ ->
    Printf.eprintf "%s\n" usage;
    Some 2
  | _ -> None
;;
