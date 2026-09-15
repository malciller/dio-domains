(** `dio strategy validate <file>` command.

    Returns an exit code (0 = valid, 1 = errors, 2 = usage). [maybe_run] returns [None]
    when the argv does not select this command, so the engine can start normally. *)

let usage = "Usage: dio strategy validate <strategy.json>"

let print_diagnostic (d : Strategy_compile.diagnostic) =
  Printf.printf "  %-7s %s: %s\n" (Strategy_compile.severity_string d.sev) d.where d.msg
;;

let validate_file (path : string) : int =
  Strategy_actions_builtin.register_all ();
  match Strategy_file.parse_file path with
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

let maybe_run (argv : string array) : int option =
  match Array.to_list argv with
  | _ :: "strategy" :: "validate" :: file :: _ -> Some (validate_file file)
  | _ :: "strategy" :: _ ->
    Printf.eprintf "%s\n" usage;
    Some 2
  | _ -> None
;;
