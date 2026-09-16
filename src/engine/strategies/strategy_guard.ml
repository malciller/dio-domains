(** Guard evaluation for strategy files.

    Evaluates a [Strategy_file.guard] against an expression [env] plus a small set of
    structural facts (current event kind/side, platform capacity and pending queries,
    engine flags, signals, order posture). Guards are side-effect-free. *)

type facts =
  { event_kind : unit -> string (* "" when no event is in context *)
  ; side : unit -> string (* "" when the event has no side *)
  ; pending : string -> bool
  ; engine_flag : string -> Strategy_expr.value option
  ; capacity : string -> Strategy_expr.value option
  ; signal : string -> Strategy_expr.value option
  ; order_posture : string -> string -> bool
  }

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

(** Direct-style guard evaluation. Guards only need a bool, so the result-returning
    recursion boxes an [Ok] for every guard node in an [all]/[any] list; the hot path
    ([run_cycle]) calls [eval_exn] and pays nothing on success, falling back to [eval]
    (which wraps the same core) only on error. [parse_expr] is threaded in so the runtime
    can supply a memoized parser. *)

let rec eval_guard_exn
  ~parse_expr
  (env : Strategy_expr.env)
  (facts : facts)
  (g : Strategy_file.guard)
  : bool
  =
  let open Strategy_file in
  match g with
  | G_event e -> String.equal (facts.event_kind ()) e
  | G_side s -> String.equal (facts.side ()) s
  | G_all gs -> eval_all_exn ~parse_expr env facts gs
  | G_any gs -> eval_any_exn ~parse_expr env facts gs
  | G_not g -> not (eval_guard_exn ~parse_expr env facts g)
  | G_is_none s ->
    (match eval_ref_value_exn env s with
     | Strategy_expr.V_none -> true
     | _ -> false)
  | G_is_some s ->
    (match eval_ref_value_exn env s with
     | Strategy_expr.V_none -> false
     | _ -> true)
  | G_expr s ->
    (match parse_expr s with
     | Error m -> raise (Strategy_expr.Eval_error m)
     | Ok e -> Strategy_expr.eval_bool env e)
  | G_capacity kvs ->
    let rec loop = function
      | [] -> true
      | (key, expr_s) :: rest ->
        (match parse_capacity_key key with
         | Error m -> raise (Strategy_expr.Eval_error m)
         | Ok (asset, op) ->
           (match facts.capacity asset with
            | None -> false
            | Some fact ->
              (match Strategy_expr.eval_arg env expr_s with
               | Error m -> raise (Strategy_expr.Eval_error m)
               | Ok ev ->
                 (match Strategy_expr.to_float fact, Strategy_expr.to_float ev with
                  | Ok fv, Ok xv -> if compare_ordered op fv xv then loop rest else false
                  | Error m, _ | _, Error m -> raise (Strategy_expr.Eval_error m)))))
    in
    loop kvs
  | G_engine kvs -> eval_fact_map_exn facts.engine_flag kvs
  | G_signal kvs -> eval_fact_map_exn facts.signal kvs
  | G_pending p -> facts.pending p
  | G_order kvs ->
    let rec loop = function
      | [] -> true
      | (side, pred) :: rest -> if facts.order_posture side pred then loop rest else false
    in
    loop kvs
  | G_cooldown { since; seconds } ->
    (match eval_ref_value_exn env since with
     | Strategy_expr.V_none -> true
     | sv ->
       (match Strategy_expr.to_float sv with
        | Error m -> raise (Strategy_expr.Eval_error m)
        | Ok t0 ->
          (match Strategy_expr.eval_arg env seconds with
           | Error m -> raise (Strategy_expr.Eval_error m)
           | Ok dv ->
             (match Strategy_expr.to_float dv, env.now () with
              | Ok dt, Strategy_expr.V_float nowv -> nowv -. t0 >= dt
              | _ ->
                raise
                  (Strategy_expr.Eval_error "cooldown_elapsed requires numeric seconds")))))

and eval_ref_value_exn env s =
  let raw =
    if String.length s > 0 && s.[0] = '$' then String.sub s 1 (String.length s - 1) else s
  in
  match Strategy_expr.parse_ref raw with
  | Error m -> raise (Strategy_expr.Eval_error m)
  | Ok r -> Strategy_expr.eval_ref env r

and eval_all_exn ~parse_expr env facts gs =
  let rec loop = function
    | [] -> true
    | g :: rest -> if eval_guard_exn ~parse_expr env facts g then loop rest else false
  in
  loop gs

and eval_any_exn ~parse_expr env facts gs =
  let rec loop = function
    | [] -> false
    | g :: rest -> if eval_guard_exn ~parse_expr env facts g then true else loop rest
  in
  loop gs

and eval_fact_map_exn lookup kvs =
  let rec loop = function
    | [] -> true
    | (k, v) :: rest ->
      (match lookup k with
       | None -> false
       | Some fact ->
         if Strategy_expr.value_eq fact (Strategy_expr.value_of_literal v)
         then loop rest
         else false)
  in
  loop kvs
;;

(** Direct evaluation for the hot path; raises [Strategy_expr.Eval_error] on a malformed
    or unresolved guard. *)
let eval_exn ?(parse_expr = Strategy_expr.parse) env facts g =
  eval_guard_exn ~parse_expr env facts g
;;

(** Result-returning wrapper (kept for callers that prefer the explicit error channel). *)
let eval ?(parse_expr = Strategy_expr.parse) env facts g =
  try Ok (eval_guard_exn ~parse_expr env facts g) with
  | Strategy_expr.Eval_error m -> Error m
;;

(** Precompiled guard: parse every embedded expression/reference once, at load, and return
    a closure that only walks the parsed structure at runtime. Removes the per-cycle
    [Hashtbl] `guard_cache` lookup and re-walk of the guard AST; on a file whose flat
    steps repeat shared predicates this is the dominant interpreter cost. *)
let parse_ref_opt (s : string) : (Strategy_expr.ref_, string) result =
  let raw =
    if String.length s > 0 && s.[0] = '$' then String.sub s 1 (String.length s - 1) else s
  in
  Strategy_expr.parse_ref raw
;;

let rec all_closures cgs env facts =
  match cgs with
  | [] -> true
  | cg :: rest -> if cg env facts then all_closures rest env facts else false

and any_closures cgs env facts =
  match cgs with
  | [] -> false
  | cg :: rest -> if cg env facts then true else any_closures rest env facts

and order_all kvs facts =
  match kvs with
  | [] -> true
  | (side, pred) :: rest ->
    if facts.order_posture side pred then order_all rest facts else false

and capacity_all compiled env facts =
  match compiled with
  | [] -> true
  | (key_r, expr_r) :: rest ->
    (match key_r with
     | Error m -> raise (Strategy_expr.Eval_error m)
     | Ok (asset, op) ->
       (match facts.capacity asset with
        | None -> false
        | Some fact ->
          (match expr_r with
           | Error m -> raise (Strategy_expr.Eval_error m)
           | Ok e ->
             (match
                ( Strategy_expr.to_float fact
                , Strategy_expr.to_float (Strategy_expr.eval_value env e) )
              with
              | Ok fv, Ok xv ->
                if compare_ordered op fv xv then capacity_all rest env facts else false
              | Error m, _ | _, Error m -> raise (Strategy_expr.Eval_error m)))))
;;

(** Per-cycle memo for compiled [G_expr] guards. The strategy file repeats the same leaf
    expressions across many steps (e.g. [$state.cycle_ok] appears in ~20 guards, and the
    four [buy_place_*] guards share an 11-term prefix), so without this the interpreter
    re-evaluates the same expression up to four times per cycle.

    Keys are serialized to an int slot at load ([memo_slot]) so a lookup is an array read
    with no string hashing; the slot arrays are indexed directly. [memo_gen] is bumped by
    the runtime at each cycle start and on every fact write, so a memoised result is
    reused only while no fact changed since it was computed. *)
type memo =
  { memo_ids : (string, int) Hashtbl.t
  ; mutable memo_next : int
  ; mutable memo_gen : int
  ; mutable memo_gen_arr : int array
  ; mutable memo_val_arr : bool array
  }

(** Resolve [s] to its int slot, assigning one on first use (load time only). *)
let memo_slot m s =
  match Hashtbl.find_opt m.memo_ids s with
  | Some i -> i
  | None ->
    let i = m.memo_next in
    m.memo_next <- i + 1;
    if i >= Array.length m.memo_gen_arr
    then (
      let n = max 16 (Array.length m.memo_gen_arr * 2) in
      m.memo_gen_arr
      <- Array.append m.memo_gen_arr (Array.make (n - Array.length m.memo_gen_arr) (-1));
      m.memo_val_arr
      <- Array.append m.memo_val_arr (Array.make (n - Array.length m.memo_val_arr) false));
    Hashtbl.replace m.memo_ids s i;
    i
;;

let rec compile_exn ?(parse_expr = Strategy_expr.parse) ?memo (g : Strategy_file.guard)
  : Strategy_expr.env -> facts -> bool
  =
  let open Strategy_file in
  match g with
  | G_event e -> fun _ facts -> String.equal (facts.event_kind ()) e
  | G_side s -> fun _ facts -> String.equal (facts.side ()) s
  | G_all gs ->
    let cgs = List.map (compile_exn ~parse_expr ?memo) gs in
    fun env facts -> all_closures cgs env facts
  | G_any gs ->
    let cgs = List.map (compile_exn ~parse_expr ?memo) gs in
    fun env facts -> any_closures cgs env facts
  | G_not g ->
    let cg = compile_exn ~parse_expr ?memo g in
    fun env facts -> not (cg env facts)
  | G_is_none s ->
    (match parse_ref_opt s with
     | Error m -> fun _ _ -> raise (Strategy_expr.Eval_error m)
     | Ok r ->
       fun env _ ->
         (match Strategy_expr.eval_ref env r with
          | Strategy_expr.V_none -> true
          | _ -> false))
  | G_is_some s ->
    (match parse_ref_opt s with
     | Error m -> fun _ _ -> raise (Strategy_expr.Eval_error m)
     | Ok r ->
       fun env _ ->
         (match Strategy_expr.eval_ref env r with
          | Strategy_expr.V_none -> false
          | _ -> true))
  | G_expr s ->
    (match parse_expr s with
     | Error _ -> fun _ _ -> false
     | Ok e ->
       (match memo with
        | None -> fun env _ -> Strategy_expr.eval_bool env e
        | Some m ->
          let slot = memo_slot m s in
          fun env _ ->
            if m.memo_gen_arr.(slot) = m.memo_gen
            then m.memo_val_arr.(slot)
            else (
              let b = Strategy_expr.eval_bool env e in
              m.memo_gen_arr.(slot) <- m.memo_gen;
              m.memo_val_arr.(slot) <- b;
              b)))
  | G_capacity kvs ->
    let compiled =
      List.map (fun (key, expr_s) -> parse_capacity_key key, parse_expr expr_s) kvs
    in
    fun env facts -> capacity_all compiled env facts
  | G_engine kvs -> fun _ facts -> eval_fact_map_exn facts.engine_flag kvs
  | G_signal kvs -> fun _ facts -> eval_fact_map_exn facts.signal kvs
  | G_pending p -> fun _ facts -> facts.pending p
  | G_order kvs -> fun _ facts -> order_all kvs facts
  | G_cooldown { since; seconds } ->
    (match parse_ref_opt since, parse_expr seconds with
     | Ok rs, Ok es ->
       fun env _ ->
         (match Strategy_expr.eval_ref env rs with
          | Strategy_expr.V_none -> true
          | sv ->
            (match Strategy_expr.to_float sv with
             | Error m -> raise (Strategy_expr.Eval_error m)
             | Ok t0 ->
               (match
                  Strategy_expr.to_float (Strategy_expr.eval_value env es), env.now ()
                with
                | Ok dt, Strategy_expr.V_float nowv -> nowv -. t0 >= dt
                | _ ->
                  raise
                    (Strategy_expr.Eval_error "cooldown_elapsed requires numeric seconds"))))
     | Error m, _ | _, Error m -> fun _ _ -> raise (Strategy_expr.Eval_error m))
;;
