(* Executing a compiled strategy program against the engine.

    This is the strategy layer's only execution path: the program's command stream is
    replayed against [Strategy_cycle_engine], with the compiled env as the script's own
    state and the runtime's state kept in step around engine phases. It lives here, not in
    the domain loop, so the replay tool and the tests drive exactly what live trading
    drives. *)

(* Push the file's state assignments into the runtime. Typed setters compare the raw
   payload, so an unchanged slot costs no allocation; a boxed [V_float] per slot per phase
   boundary did. *)
let sync_state program env rt =
  Array.iter
    (fun (key, kind, index) ->
       let slot = Strategy_runtime.slot_of_key key in
       match kind with
       | Strategy_ir.K_float ->
         Strategy_runtime.set_state_float_slot rt slot env.Strategy_ir.floats.(index)
       | K_int -> Strategy_runtime.set_state_int_slot rt slot env.ints.(index)
       | K_bool ->
         Strategy_runtime.set_state_bool_slot rt slot env.Strategy_ir.bools.(index)
       | K_string ->
         Strategy_runtime.set_state_string_slot rt slot env.strings.(index))
    program.Strategy_program.state_slots
;;

(** Run one engine phase from the compiled command stream. The id comes from
    [Strategy_program.engine_phases], so this is the only place a phase name is matched. *)
let run_engine_phase ctx rt id =
  let module E = Strategy_cycle_engine in
  let module P = Strategy_program in
  let module R = Strategy_runtime in
  if id < 0 || id >= Array.length P.engine_phases
  then ()
  else (
    match P.engine_phases.(id) with
    | "apply_order_event" ->
      (match R.current_event rt with
       | Some ev -> E.on_event ctx ev
       | None -> ())
    | "init_venue_state" -> E.prepare_init ctx
    | "prepare_recovery" -> E.prepare_recovery ctx
    | "resolve_book" -> ignore (E.resolve_book ctx : bool)
    | "cycle_cleanup" -> E.cleanup ctx
    | "early_facts" -> E.early_facts ctx rt
    | "cycle_facts" -> E.cycle_facts ctx rt
    | "expire_amend_cooldowns" -> E.expire_amend_cooldowns ctx
    | "evict_ghost_orders" -> E.evict_ghost_orders ctx
    | "scan_open_orders" -> E.sync ctx
    | "refresh_maker_fee" -> E.refresh_fee ctx
    | "expire_tif_recovery" -> E.expire_tif_recovery ctx
    | "cycle_guard" ->
      R.set_platform_slot
        rt
        Strategy_fact_slots.engine_continue
        (Strategy_expr.V_bool (E.guard ctx))
    | "buy_gate" ->
      R.set_platform_slot
        rt
        Strategy_fact_slots.engine_buy_active
        (Strategy_expr.V_bool (E.buy_gate ctx))
    | "cancel_excess_buys" -> E.buy_cancel ctx
    | "mark_stale_cycle" -> E.mark_stale ctx
    | "buy_place_plan" -> E.buy_place_plan ctx rt
    | "buy_place_send" -> E.buy_place_send ctx
    | "buy_place_send_insufficient" -> E.buy_place_send_insufficient ctx
    | "buy_place_latch_capital_low" -> E.buy_place_latch_capital_low ~rt ctx
    | "buy_place_warn_quote" -> E.buy_place_warn_quote ctx
    | "buy_amend" -> E.buy_amend ctx
    | "buy_amend_has_sell" ->
      R.set_platform_slot
        rt
        Strategy_fact_slots.amend_has_sell
        (Strategy_expr.V_bool (E.buy_amend_has_sell ctx))
    | "buy_amend_with_sell" -> E.buy_amend_with_sell ctx
    | "buy_amend_no_sell" -> E.buy_amend_no_sell ctx
    | "plan_sell_order" -> E.sell_prepare ctx
    | "sell_place" -> E.sell_place ctx
    | "sell_place_should" ->
      R.set_platform_slot
        rt
        Strategy_fact_slots.sell_place_should
        (Strategy_expr.V_bool (E.sell_place_should ctx))
    | "sell_place_body" -> E.sell_place_body ctx
    | "sell_finalize" -> E.sell_finalize ctx
    | "sell_finalize_facts" -> E.sell_finalize_facts ctx rt
    | "sell_finalize_latch" -> E.sell_finalize_latch ctx
    | "sell_excess_sweep_phase" -> E.sell_excess_sweep_phase ctx
    | "sell_finalize_end" -> E.sell_finalize_end ctx
    | _ -> ())
;;

(** Run one compiled pass against the engine.

    A grid's engine phases run inside the same command stream, and they both read and write
    the runtime's state and platform facts, so the env is synchronised around phase
    boundaries: state is pushed to the runtime before a phase runs (the phase sees what the
    file just assigned), and both state and facts are pulled back after it (a later guard
    sees what the phase decided). The runtime therefore stays the single source of truth,
    which is what makes the compiled path equivalent by construction. *)
let run_pass
      ctx
      rt
      program
      env
      sink
      resolved
      ~price
      ~now
      ~event
      ~event_kind
      ~event_fields
  =
  rt.Strategy_runtime.event <- Some event;
  Strategy_program.fill_env_resolved
    ~state:true
    resolved
    rt
    env
    ~price
    ~now
    ~event_kind
    ~event_fields;
  Strategy_program.run program env sink;
  let dirty = ref true in
  for c = 0 to sink.Strategy_program.cmd_count - 1 do
    match sink.cmds.(c) with
    | Strategy_program.Cm_state -> dirty := true
    | Cm_phase id ->
      if !dirty
      then (
        sync_state program env rt;
        dirty := false);
      run_engine_phase ctx rt id;
      (* Guards were evaluated before any phase ran, so only state written by the phase
         has a reader left: the final [sync_state]. *)
      Strategy_program.fill_state_resolved resolved rt env
    | Cm_order ->
      () (* A grid places through its engine phases. *)
    | Cm_reserve amount -> Strategy_cycle_engine.quote_reserve ctx ~amount
    | Cm_release -> Strategy_cycle_engine.quote_reserve ctx ~amount:0.0
    | Cm_credit (seq, qty) ->
      Strategy_cycle_engine.credit_profit ctx ~qty ~seq
  done;
  sync_state program env rt
;;
