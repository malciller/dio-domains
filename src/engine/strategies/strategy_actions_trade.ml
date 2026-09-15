(** Strategy-agnostic trade actions.

    A concrete strategy is a config file composed of these actions plus expressions; the
    engine supplies the per-domain context ([ENGINE]) that reads feed snapshots, emits
    order intents to the lock-free buffer, and performs the platform accounting. No
    strategy policy lives here — only the action plumbing and argument/return marshalling.
    See docs/strategy-engine-design.md §9.6 ("generic action inventory" and the
    action-layer contract: inputs are feeds, outputs are intents, persistence is async). *)

open Strategy_expr

(** Everything an action needs from the engine. Implementations read in-memory feed
    snapshots and push intents; none block on the network or touch files. *)
module type ENGINE = sig
  type ctx

  (* Reads (feed snapshots). *)
  val read_book : ctx -> (string * value) list
  val read_capacity : ctx -> asset:string -> (string * value) list
  val read_open_orders : ctx -> (string * value) list

  (* Order emission (push to the lock-free order buffer). *)
  val place
    :  ctx
    -> side:string
    -> qty:float
    -> price:float
    -> post_only:bool
    -> dedup_key:string
    -> (string * value) list

  val amend
    :  ctx
    -> token:string
    -> price:float
    -> qty:float
    -> dedup_key:string
    -> (string * value) list

  val cancel : ctx -> token:string -> dedup_key:string -> (string * value) list
  val cancel_all : ctx -> side:string -> dedup_key:string -> (string * value) list

  (* Tracking / policy state (in-memory; persistence is signalled, not written here). *)
  val track_buy : ctx -> token:string -> price:float -> unit
  val track_sell : ctx -> token:string -> price:float -> unit
  val set_cooldown : ctx -> name:string -> seconds:float -> unit
  val update_reserved_base : ctx -> qty:float -> unit
  val accumulate : ctx -> qty:float -> profit:float -> unit
  val set_time : ctx -> string -> unit
  val notify_oracle : ctx -> string -> unit

  (* Gates (feed-derived booleans). *)
  val gate_balance : ctx -> bool
  val gate_capital_halted : ctx -> bool
  val is_ghost : ctx -> token:string -> bool

  (* Accounting reconciles (platform invariants). *)
  val reconcile_position : ctx -> unit
  val reconcile_persisted_sell_levels : ctx -> unit

  (* Price math (venue rounding + percentage steps). *)
  val compute_sell_price : ctx -> base:float -> mult:float -> float
  val compute_amend_price : ctx -> ref:float -> lo:float -> hi:float option -> float

  val compute_grid_price
    :  ctx
    -> ref:float
    -> lo:float
    -> hi:float
    -> side:string
    -> snap:float option
    -> float

  val compute_buy_ref_price : ctx -> bid:float -> ask:float -> float
  val owed_sell_price : ctx -> bid:float -> ask:float -> capital_exhausted:bool -> float
end

let f_entry args k d =
  match List.assoc_opt k args with
  | Some (V_float f) -> f
  | Some (V_int i) -> float_of_int i
  | _ -> d
;;

let s_entry args k d =
  match List.assoc_opt k args with
  | Some (V_string s) -> s
  | _ -> d
;;

let b_entry args k d =
  match List.assoc_opt k args with
  | Some (V_bool b) -> b
  | _ -> d
;;

let f_opt args k =
  match List.assoc_opt k args with
  | Some (V_float f) -> Some f
  | Some (V_int i) -> Some (float_of_int i)
  | _ -> None
;;

module Make (E : ENGINE) = struct
  let handler (ctx : E.ctx) : Strategy_runtime.handler =
    { run =
        (fun _t name args ->
          match name with
          | "read_book" -> E.read_book ctx
          | "read_capacity" -> E.read_capacity ctx ~asset:(s_entry args "asset" "")
          | "read_open_orders" -> E.read_open_orders ctx
          | "place_buy" ->
            E.place
              ctx
              ~side:"buy"
              ~qty:(f_entry args "qty" 0.0)
              ~price:(f_entry args "price" 0.0)
              ~post_only:(b_entry args "post_only" true)
              ~dedup_key:(s_entry args "dedup_key" "")
          | "place_sell" ->
            E.place
              ctx
              ~side:"sell"
              ~qty:(f_entry args "qty" 0.0)
              ~price:(f_entry args "price" 0.0)
              ~post_only:true
              ~dedup_key:(s_entry args "dedup_key" "")
          | "amend_buy" | "amend_sell" ->
            E.amend
              ctx
              ~token:(s_entry args "token" "")
              ~price:(f_entry args "price" 0.0)
              ~qty:(f_entry args "qty" 0.0)
              ~dedup_key:(s_entry args "dedup_key" "")
          | "cancel_order" ->
            E.cancel
              ctx
              ~token:(s_entry args "token" "")
              ~dedup_key:(s_entry args "dedup_key" "")
          | "cancel_all" ->
            E.cancel_all
              ctx
              ~side:(s_entry args "side" "both")
              ~dedup_key:(s_entry args "dedup_key" "")
          | "track_buy" ->
            E.track_buy
              ctx
              ~token:(s_entry args "token" "")
              ~price:(f_entry args "price" 0.0);
            []
          | "track_sell" ->
            E.track_sell
              ctx
              ~token:(s_entry args "token" "")
              ~price:(f_entry args "price" 0.0);
            []
          | "set_cooldown" ->
            E.set_cooldown
              ctx
              ~name:(s_entry args "name" "")
              ~seconds:(f_entry args "seconds" 0.0);
            []
          | "update_reserved_base" ->
            E.update_reserved_base ctx ~qty:(f_entry args "qty" 0.0);
            []
          | "accumulate" ->
            E.accumulate
              ctx
              ~qty:(f_entry args "qty" 0.0)
              ~profit:(f_entry args "profit" 0.0);
            []
          | "set_time" ->
            E.set_time ctx (s_entry args "state" "");
            []
          | "notify_oracle" ->
            E.notify_oracle ctx (s_entry args "message" "");
            []
          | "gate_balance" -> [ "ok", V_bool (E.gate_balance ctx) ]
          | "gate_capital_halted" -> [ "halted", V_bool (E.gate_capital_halted ctx) ]
          | "is_ghost" ->
            [ "ghost", V_bool (E.is_ghost ctx ~token:(s_entry args "token" "")) ]
          | "reconcile_position" ->
            E.reconcile_position ctx;
            []
          | "reconcile_persisted_sell_levels" ->
            E.reconcile_persisted_sell_levels ctx;
            []
          | "compute_sell_price" ->
            [ ( "price"
              , V_float
                  (E.compute_sell_price
                     ctx
                     ~base:(f_entry args "base" 0.0)
                     ~mult:(f_entry args "mult" 1.0)) )
            ]
          | "compute_amend_price" ->
            [ ( "price"
              , V_float
                  (E.compute_amend_price
                     ctx
                     ~ref:(f_entry args "ref" 0.0)
                     ~lo:(f_entry args "lo" 0.0)
                     ~hi:(f_opt args "hi")) )
            ]
          | "compute_grid_price" ->
            [ ( "price"
              , V_float
                  (E.compute_grid_price
                     ctx
                     ~ref:(f_entry args "ref" 0.0)
                     ~lo:(f_entry args "lo" 0.0)
                     ~hi:(f_entry args "hi" 0.0)
                     ~side:(s_entry args "side" "below")
                     ~snap:(f_opt args "snap")) )
            ]
          | "compute_buy_ref_price" ->
            [ ( "price"
              , V_float
                  (E.compute_buy_ref_price
                     ctx
                     ~bid:(f_entry args "bid" 0.0)
                     ~ask:(f_entry args "ask" 0.0)) )
            ]
          | "owed_sell_price" ->
            [ ( "price"
              , V_float
                  (E.owed_sell_price
                     ctx
                     ~bid:(f_entry args "bid" 0.0)
                     ~ask:(f_entry args "ask" 0.0)
                     ~capital_exhausted:(b_entry args "capital_exhausted" false)) )
            ]
          | _ -> [])
    }
  ;;
end
