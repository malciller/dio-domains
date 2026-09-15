(** Guard evaluation for strategy files.

    Evaluates a [Strategy_file.guard] against an expression [env] plus a small set of
    structural facts (current event kind/side, platform capacity and pending queries,
    engine flags, signals, order posture). Guards are side-effect-free. *)

type facts =
  { event_kind : unit -> string option
  ; side : unit -> string option
  ; pending : string -> bool
  ; engine_flag : string -> Strategy_expr.value option
  ; capacity : string -> Strategy_expr.value option
  ; signal : string -> Strategy_expr.value option
  ; order_posture : string -> string -> bool
  }

let eval_ref_string (env : Strategy_expr.env) (s : string) =
  let raw =
    if String.length s > 0 && s.[0] = '$' then String.sub s 1 (String.length s - 1) else s
  in
  match Strategy_expr.parse_ref raw with
  | Ok r -> Strategy_expr.eval_ref env r
  | Error m -> Error m
;;

type op =
  | Gte
  | Lte
  | Gt
  | Lt
  | Eq

let compare_ordered op a b =
  match op with
  | Gte -> a >= b
  | Lte -> a <= b
  | Gt -> a > b
  | Lt -> a < b
  | Eq -> a = b
;;

let parse_capacity_key key =
  match List.rev (String.split_on_char '_' key) with
  | [] -> Error "empty capacity key"
  | op :: rest ->
    let asset = String.concat "_" (List.rev rest) in
    let opinfo =
      match op with
      | "gte" -> Some Gte
      | "lte" -> Some Lte
      | "gt" -> Some Gt
      | "lt" -> Some Lt
      | "eq" -> Some Eq
      | _ -> None
    in
    (match opinfo with
     | Some o when asset <> "" -> Ok (asset, o)
     | Some _ -> Error ("capacity key has no asset: " ^ key)
     | None -> Error ("unknown capacity operator: " ^ key))
;;

(** Guard evaluation.

    [parse_expr] is threaded in so the runtime can supply a memoized parser: expression
    guards carry their source string, and re-parsing it on every cycle is the dominant
    per-tick cost once a file uses many [expr] guards. *)

let rec eval_guard
  ~parse_expr
  (env : Strategy_expr.env)
  (facts : facts)
  (g : Strategy_file.guard)
  : (bool, string) result
  =
  let open Strategy_file in
  match g with
  | G_event e ->
    Ok
      (match facts.event_kind () with
       | Some k -> String.equal k e
       | None -> false)
  | G_side s ->
    Ok
      (match facts.side () with
       | Some k -> String.equal k s
       | None -> false)
  | G_all gs -> eval_all ~parse_expr env facts gs
  | G_any gs -> eval_any ~parse_expr env facts gs
  | G_not g ->
    (match eval_guard ~parse_expr env facts g with
     | Ok b -> Ok (not b)
     | Error m -> Error m)
  | G_is_none s ->
    (match eval_ref_string env s with
     | Ok Strategy_expr.V_none -> Ok true
     | Ok _ -> Ok false
     | Error m -> Error m)
  | G_is_some s ->
    (match eval_ref_string env s with
     | Ok Strategy_expr.V_none -> Ok false
     | Ok _ -> Ok true
     | Error m -> Error m)
  | G_expr s ->
    (match parse_expr s with
     | Error m -> Error m
     | Ok e ->
       (match Strategy_expr.eval env e with
        | Ok (Strategy_expr.V_bool b) -> Ok b
        | Ok v ->
          Error ("expr guard did not yield a bool: " ^ Strategy_expr.string_of_value v)
        | Error m -> Error m))
  | G_capacity kvs ->
    let rec loop = function
      | [] -> Ok true
      | (key, expr_s) :: rest ->
        (match parse_capacity_key key with
         | Error m -> Error m
         | Ok (asset, op) ->
           (match facts.capacity asset with
            | None -> Ok false
            | Some fact ->
              (match Strategy_expr.eval_arg env expr_s with
               | Error m -> Error m
               | Ok ev ->
                 (match Strategy_expr.to_float fact, Strategy_expr.to_float ev with
                  | Ok fv, Ok xv ->
                    if compare_ordered op fv xv then loop rest else Ok false
                  | Error m, _ | _, Error m -> Error m))))
    in
    loop kvs
  | G_engine kvs -> eval_fact_map facts.engine_flag kvs
  | G_signal kvs -> eval_fact_map facts.signal kvs
  | G_pending p -> Ok (facts.pending p)
  | G_order kvs ->
    let rec loop = function
      | [] -> Ok true
      | (side, pred) :: rest ->
        if facts.order_posture side pred then loop rest else Ok false
    in
    loop kvs
  | G_cooldown { since; seconds } ->
    (match eval_ref_string env since with
     | Error m -> Error m
     | Ok Strategy_expr.V_none -> Ok true
     | Ok sv ->
       (match Strategy_expr.to_float sv with
        | Error m -> Error m
        | Ok t0 ->
          (match Strategy_expr.eval_arg env seconds with
           | Error m -> Error m
           | Ok dv ->
             (match Strategy_expr.to_float dv, env.now () with
              | Ok dt, Ok (Strategy_expr.V_float nowv) -> Ok (nowv -. t0 >= dt)
              | _ -> Error "cooldown_elapsed requires numeric seconds"))))

and eval_all ~parse_expr env facts gs =
  let rec loop = function
    | [] -> Ok true
    | g :: rest ->
      (match eval_guard ~parse_expr env facts g with
       | Ok true -> loop rest
       | Ok false -> Ok false
       | Error m -> Error m)
  in
  loop gs

and eval_any ~parse_expr env facts gs =
  let rec loop = function
    | [] -> Ok false
    | g :: rest ->
      (match eval_guard ~parse_expr env facts g with
       | Ok true -> Ok true
       | Ok false -> loop rest
       | Error m -> Error m)
  in
  loop gs

and eval_fact_map lookup kvs =
  let rec loop = function
    | [] -> Ok true
    | (k, v) :: rest ->
      (match lookup k with
       | None -> Ok false
       | Some fact ->
         if Strategy_expr.value_eq fact (Strategy_expr.value_of_literal v)
         then loop rest
         else Ok false)
  in
  loop kvs
;;

let eval ?(parse_expr = Strategy_expr.parse) env facts g =
  eval_guard ~parse_expr env facts g
;;
