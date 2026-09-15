(** Static validation of a parsed strategy file.

    Checks action existence, arg schemas, reference resolution (state/params/
    events/locals/signals/platform), trigger validity, effectful dedup keys and duplicate
    names. No runtime execution. *)

type severity =
  | Error
  | Warning

type diagnostic =
  { sev : severity
  ; where : string
  ; msg : string
  }

let known_triggers =
  [ "book_update"; "fill"; "order_lifecycle"; "balance_update"; "oracle_publish" ]
;;

let known_platform_caps =
  [ "available_sell"; "balance_age"; "generation"; "is_ghost"; "open_sells"; "pending" ]
;;

let event_fields = function
  | "fill" -> Some [ "fill_price"; "fill_qty"; "side"; "fill_order_id"; "realized" ]
  | "order_lifecycle" -> Some [ "order_id"; "status"; "result" ]
  | "balance_update" -> Some [ "asset"; "total"; "available"; "age" ]
  | "oracle_publish" -> Some [ "signals" ]
  | "book_update" -> Some []
  | _ -> None
;;

type acc =
  { mutable diags : diagnostic list
  ; params : string list
  ; states : string list
  }

let err acc where msg = acc.diags <- { sev = Error; where; msg } :: acc.diags
let warn acc where msg = acc.diags <- { sev = Warning; where; msg } :: acc.diags

let strip_suffix name =
  List.find_map
    (fun suf ->
      let n = String.length name
      and m = String.length suf in
      if n > m && String.equal (String.sub name (n - m) m) suf
      then Some (String.sub name 0 (n - m))
      else None)
    [ "_f"; "_dec"; "_lo"; "_hi" ]
;;

let param_resolves params name =
  List.mem name params
  ||
  match strip_suffix name with
  | Some base -> List.mem base params
  | None -> false
;;

let ref_name prefix path = "$" ^ String.concat "." (prefix :: path)

let resolve_ref acc ~locals ~event ~where (r : Strategy_expr.ref_) =
  match r.scope with
  | Strategy_expr.Price | Strategy_expr.Now -> ()
  | Strategy_expr.State ->
    (match r.path with
     | [] -> err acc where "empty $state reference"
     | head :: _ ->
       if not (List.mem head acc.states)
       then err acc where ("unknown state: " ^ ref_name "state" r.path))
  | Strategy_expr.Param ->
    (match r.path with
     | [] -> err acc where "empty $params reference"
     | [ name ] ->
       if not (param_resolves acc.params name)
       then err acc where ("unknown param: " ^ ref_name "params" r.path)
     | _ -> err acc where "params references take no sub-path")
  | Strategy_expr.Local ->
    (match r.path with
     | [] -> err acc where "empty $local reference"
     | head :: _ ->
       if not (List.mem head locals)
       then err acc where ("unknown local: " ^ ref_name "local" r.path))
  | Strategy_expr.Event ->
    (match event with
     | None -> warn acc where "$event referenced in a step with no event trigger"
     | Some ev ->
       (match event_fields ev with
        | None -> ()
        | Some fields ->
          (match r.path with
           | [] -> err acc where "empty $event reference"
           | field :: _ ->
             if not (List.mem field fields)
             then
               err
                 acc
                 where
                 (Printf.sprintf "unknown event field for %s: $event.%s" ev field))))
  | Strategy_expr.Signal ->
    warn acc where ("unverified signal: " ^ ref_name "signal" r.path)
  | Strategy_expr.Platform ->
    (match r.path with
     | [] -> err acc where "empty $platform reference"
     | head :: _ ->
       if not (List.mem head known_platform_caps)
       then err acc where ("unknown platform capability: " ^ ref_name "platform" r.path))
;;

let check_string_refs acc ~locals ~event ~where s =
  List.iter
    (fun res ->
      match res with
      | Ok r -> resolve_ref acc ~locals ~event ~where r
      | Error m -> err acc where m)
    (Strategy_expr.refs_in s)
;;

let rec guard_event = function
  | Strategy_file.G_event e -> Some e
  | Strategy_file.G_all gs | Strategy_file.G_any gs -> List.find_map guard_event gs
  | Strategy_file.G_not g -> guard_event g
  | _ -> None
;;

let rec check_guard acc ~locals ~event ~where g =
  let open Strategy_file in
  match g with
  | G_event _ | G_side _ | G_pending _ | G_engine _ -> ()
  | G_all gs | G_any gs -> List.iter (check_guard acc ~locals ~event ~where) gs
  | G_not g -> check_guard acc ~locals ~event ~where g
  | G_is_none s | G_is_some s | G_expr s -> check_string_refs acc ~locals ~event ~where s
  | G_capacity kvs | G_order kvs | G_signal kvs ->
    List.iter (fun (_, s) -> check_string_refs acc ~locals ~event ~where s) kvs
  | G_cooldown { since; seconds } ->
    check_string_refs acc ~locals ~event ~where since;
    check_string_refs acc ~locals ~event ~where seconds
;;

let json_kind_ok (kind : Strategy_actions.arg_kind) (v : Yojson.Basic.t) =
  let open Strategy_actions in
  let numeric_string s =
    try
      ignore (float_of_string s);
      true
    with
    | _ -> false
  in
  match kind with
  | A_float ->
    (match v with
     | `Int _ | `Float _ -> true
     | `String s -> numeric_string s || String.contains s '$'
     | _ -> false)
  | A_int ->
    (match v with
     | `Int _ -> true
     | `String s ->
       (try
          ignore (int_of_string s);
          true
        with
        | _ -> String.contains s '$')
     | _ -> false)
  | A_bool ->
    (match v with
     | `Bool _ -> true
     | _ -> false)
  | A_string | A_decimal_str ->
    (match v with
     | `String _ -> true
     | _ -> false)
  | A_enum opts ->
    (match v with
     | `String s -> List.mem s opts
     | _ -> false)
  | A_expr ->
    (match v with
     | `String _ | `Int _ | `Float _ | `Bool _ -> true
     | _ -> false)
;;

let check_bind acc ~where (var, rhs) =
  if not (String.length rhs > 5 && String.equal (String.sub rhs 0 5) "$out.")
  then err acc where (Printf.sprintf "bind %s must be \"$out.<field>\" (got %s)" var rhs)
;;

let check_action acc ~locals ~event ~where (a : Strategy_file.action) =
  match Strategy_actions.find a.a_name with
  | None ->
    err acc where ("unknown action: " ^ a.a_name);
    locals
  | Some act ->
    List.iter
      (fun (se : Strategy_actions.schema_entry) ->
        if se.required && not (List.mem_assoc se.arg a.a_args)
        then err acc where (Printf.sprintf "%s: missing required arg %S" a.a_name se.arg))
      act.schema;
    List.iter
      (fun (k, v) ->
        match
          List.find_opt
            (fun (se : Strategy_actions.schema_entry) -> String.equal se.arg k)
            act.schema
        with
        | None -> warn acc where (Printf.sprintf "%s: unknown arg %S" a.a_name k)
        | Some se ->
          if not (json_kind_ok se.kind v)
          then err acc where (Printf.sprintf "%s: arg %S has the wrong type" a.a_name k);
          (match v with
           | `String s -> check_string_refs acc ~locals ~event ~where s
           | _ -> ()))
      a.a_args;
    if act.class_ = Strategy_actions.Effectful
       && not (List.mem_assoc "dedup_key" a.a_args)
    then err acc where (Printf.sprintf "%s: effectful action requires dedup_key" a.a_name);
    List.iter (check_bind acc ~where) a.a_bind;
    locals @ List.map fst a.a_bind
;;

let rec check_actions acc ~locals ~event ~where = function
  | [] -> locals
  | a :: rest ->
    let locals' = check_action acc ~locals ~event ~where a in
    check_actions acc ~locals:locals' ~event ~where rest
;;

let check_step acc (s : Strategy_file.step) =
  let where = "step " ^ s.st_id in
  let event =
    match s.st_when with
    | Some g -> guard_event g
    | None -> None
  in
  let locals = List.map fst s.st_let in
  List.iter (fun (_, e) -> check_string_refs acc ~locals ~event ~where e) s.st_let;
  (match s.st_when with
   | Some g -> check_guard acc ~locals ~event ~where g
   | None -> ());
  ignore (check_actions acc ~locals ~event ~where s.st_then : string list);
  ignore (check_actions acc ~locals ~event ~where s.st_else : string list)
;;

let check_duplicates acc kind names =
  let seen = Hashtbl.create 16 in
  List.iter
    (fun n ->
      if Hashtbl.mem seen n
      then err acc kind ("duplicate " ^ kind ^ ": " ^ n)
      else Hashtbl.add seen n ())
    names
;;

let validate (f : Strategy_file.t) : diagnostic list =
  let acc =
    { diags = []
    ; params = List.map (fun (p : Strategy_file.param) -> p.p_name) f.params
    ; states = List.map (fun (s : Strategy_file.state_decl) -> s.s_name) f.state
    }
  in
  if f.version <> 1
  then err acc "file" (Printf.sprintf "unsupported version %d" f.version);
  check_duplicates
    acc
    "param"
    (List.map (fun (p : Strategy_file.param) -> p.p_name) f.params);
  check_duplicates
    acc
    "state"
    (List.map (fun (s : Strategy_file.state_decl) -> s.s_name) f.state);
  check_duplicates acc "step" (List.map (fun (s : Strategy_file.step) -> s.st_id) f.steps);
  List.iter
    (fun t ->
      if not (List.mem t known_triggers) then err acc "triggers" ("unknown trigger: " ^ t))
    f.triggers;
  List.iter (check_step acc) f.steps;
  List.rev acc.diags
;;

let errors ds = List.filter (fun d -> d.sev = Error) ds
let warnings ds = List.filter (fun d -> d.sev = Warning) ds
let has_errors ds = List.exists (fun d -> d.sev = Error) ds

let severity_string = function
  | Error -> "error"
  | Warning -> "warning"
;;

let format_diagnostic d =
  Printf.sprintf "%s: %s: %s" (severity_string d.sev) d.where d.msg
;;

let format ds = String.concat "\n" (List.map format_diagnostic ds)
