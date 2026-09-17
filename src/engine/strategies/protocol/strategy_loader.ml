(** Strategy file loader: resolves a strategy binding to a `.strategy` or `.json` file and
    parses it into the shared [Strategy_file.t] AST, dispatching on extension.

    Binding convention: `config.json` names a strategy (["strategy": "jacobs_ladder"]);
    the loader tries [`strategies/<name>.strategy`] first, then [`strategies/<name>.json`]. A
    mismatch between the filename and the declared name fails in the caller (config load)
    via the normal name-mismatch check. *)

let strateg_dir = "strategies"
let strateg_exts = [ ".strategy"; ".json" ]

(** Path of the first existing file for [name], in preference order. *)
let locate ~(name : string) : string option =
  let candidates =
    List.map (fun ext -> Filename.concat strateg_dir (name ^ ext)) strateg_exts
  in
  List.find_opt Sys.file_exists candidates
;;

(** Parse an already-resolved path, dispatching on extension. *)
let parse_file ~(path : string) : (Strategy_file.t, string) result =
  let ext = Filename.extension path in
  if String.equal ext ".strategy"
  then Strategy_script.parse_file path
  else Strategy_file.parse_file path
;;

(** Resolve and parse a strategy by its configured name. Returns the path used and the
    parsed file on success, or an error message. *)
let load ~(name : string) : (string * Strategy_file.t, string) result =
  match locate ~name with
  | None ->
    Error
      (Printf.sprintf
         "no strategy file for %S (tried %s/<name>.strategy and %s/<name>.json)"
         name
         strateg_dir
         strateg_dir)
  | Some path ->
    (match parse_file ~path with
     | Ok f -> Ok (path, f)
     | Error msg -> Error (Printf.sprintf "strategy file %S is invalid: %s" path msg))
;;
