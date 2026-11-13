(** Strategy runtime: execute a compiled strategy file against events.

    Owns per-instance state, builds the expression [env], runs steps in file order with
    live-state guards, evaluates action arguments, calls the supplied handler, and applies
    ["bind"] outputs to step-local scope. Handlers are provided by the caller; this layer
    does not talk to an exchange. *)

open Strategy_expr

type event =
  { ev_kind : string
  ; ev_fields : (string * value) list
  }

type action_call =
  { ac_step : string
  ; ac_action : string
  ; ac_args : (string * value) list
  }

(** Engine-provided capabilities for handlers that call reference functions (e.g. price
    rounding, venue identity, venue flag matrix). The engine sets these per instance; the
    defaults are identity/empty so synthetic runs work without an exchange. *)
type caps =
  { round_price : float -> float
  ; exchange : string
  ; remaintain_expired_sells : bool
  }

let default_caps =
  { round_price = (fun x -> x); exchange = ""; remaintain_expired_sells = false }
;;

(** Fact publication target. The interpreter publishes into the runtime's platform table;
    a compiled strategy writes the same facts straight into its typed env slots, which
    skips the per-fact compare, boxing and guard-memo invalidation that only the
    interpreter needs. Slot ids are the interned keys the engine already uses. *)
type facts_io =
  { io_f : int -> float -> unit
  ; io_i : int -> int -> unit
  ; io_b : int -> bool -> unit
  ; io_s : int -> string -> unit
  ; io_get_f : int -> float
  }


let default_of_kind = function
  | Strategy_file.S_float -> V_float 0.0
  | S_float_opt -> V_none
  | S_int -> V_int 0
  | S_bool -> V_bool false
  | S_string -> V_string ""
  | S_buy_intent_opt -> V_none
  | S_sell_intent_opt -> V_none
  | S_reserve_policy -> V_none
;;

let default_string = function
  | Some (`String s) -> s
  | Some (`Int i) -> string_of_int i
  | Some (`Float f) -> string_of_float f
  | _ -> ""
;;

let default_float = function
  | Some (`Float f) -> f
  | Some (`Int i) -> float_of_int i
  | Some (`String s) ->
    (try float_of_string s with
     | _ -> 0.0)
  | _ -> 0.0
;;

(** Growable slot-addressed value table. [V_unset] means "not published"; [V_none] is a
    real value, so a missing fact stays distinguishable from a fact set to none - without
    the [Some] box that a [value option array] allocated on every publish. *)
let set_arr (a : value array) slot (v : value) : value array =
  if slot < Array.length a
  then (
    a.(slot) <- v;
    a)
  else (
    let n = Array.length a in
    let n' = max (slot + 1) (max 8 (n * 2)) in
    let na = Array.make n' V_unset in
    Array.blit a 0 na 0 n;
    na.(slot) <- v;
    na)
;;

let get_arr (a : value array) slot = if slot < Array.length a then a.(slot) else V_unset

let expand_params (file : Strategy_file.t) (overrides : (string * value) list) =
  let arr = ref (Array.make (max 8 (interned_key_count ())) V_unset) in
  let set k v = arr := set_arr !arr (intern_key k) v in
  List.iter
    (fun (p : Strategy_file.param) ->
      let ov = List.assoc_opt p.p_name overrides in
      match p.p_kind with
      | Strategy_file.P_float ->
        set
          p.p_name
          (match ov with
           | Some v -> v
           | None -> V_float (default_float p.p_default))
      | P_int ->
        set
          p.p_name
          (match ov with
           | Some v -> v
           | None ->
             (match p.p_default with
              | Some (`Int i) -> V_int i
              | _ -> V_int 0))
      | P_bool ->
        set
          p.p_name
          (match ov with
           | Some v -> v
           | None ->
             (match p.p_default with
              | Some (`Bool b) -> V_bool b
              | _ -> V_bool false))
      | P_string ->
        set
          p.p_name
          (match ov with
           | Some v -> v
           | None -> V_string (default_string p.p_default))
      | P_decimal_str ->
        let s =
          match ov with
          | Some (V_string s) -> s
          | _ -> default_string p.p_default
        in
        set p.p_name (V_string s);
        set (p.p_name ^ "_dec") (V_string s);
        set
          (p.p_name ^ "_f")
          (V_float
             (try float_of_string s with
              | _ -> 0.0))
      | P_range ->
        let lo, hi =
          match ov with
          | Some (V_float f) -> f, f
          | _ ->
            (match p.p_default with
             | Some (`List [ `Float a; `Float b ]) -> a, b
             | Some (`List [ `Int a; `Int b ]) -> float_of_int a, float_of_int b
             | Some (`List [ `Float a; `Int b ]) -> a, float_of_int b
             | Some (`List [ `Int a; `Float b ]) -> float_of_int a, b
             | _ -> 0.0, 0.0)
        in
        set (p.p_name ^ "_lo") (V_float lo);
        set (p.p_name ^ "_hi") (V_float hi)
      | P_enum _ ->
        set
          p.p_name
          (match ov with
           | Some v -> v
           | None -> V_string (default_string p.p_default)))
    file.params;
  !arr
;;


(** The runtime holds a strategy's state, params and published facts. Execution lives in
    {!Strategy_run}: the compiled program is the only engine, so there is no compiled-step
    form and no handler table here. *)
type t =
  { file : Strategy_file.t
  ; mutable state : value array
  ; mutable params : value array
  ; mutable signals : value array
  ; mutable platform : value array
  ; mutable caps : caps
  ; mutable event : event option
  ; mutable price : float
  ; mutable now : float
  ; mutable facts_io : facts_io option
  }

let create ?(params = []) (file : Strategy_file.t) =
  let state =
    List.fold_left
      (fun a (s : Strategy_file.state_decl) ->
        set_arr a (intern_key s.s_name) (default_of_kind s.s_kind))
      (Array.make (max 8 (interned_key_count ())) V_unset)
      file.state
  in
  { file
  ; state
  ; params = expand_params file params
  ; signals = Array.make (max 8 (interned_key_count ())) V_unset
  ; platform = Array.make (max 8 (interned_key_count ())) V_unset
  ; caps = default_caps
  ; event = None
  ; price = nan
  ; now = 0.0
  ; facts_io = None
  }
;;

(** True when [slot] already holds a value equal to [v], so republishing it is a no-op.
    Most facts (flags, balances, prices) are unchanged cycle to cycle, so this skips the
    store on the common path. Structural equality on the [value] variant allocates
    nothing. *)
let[@inline] slot_unchanged (a : value array) slot (v : value) =
  slot < Array.length a
  &&
  match Array.unsafe_get a slot with
  | V_unset -> false
  | x -> x = v
;;

let set_state t k v =
  let slot = intern_key k in
  if not (slot_unchanged t.state slot v)
  then (
    t.state <- set_arr t.state slot v)
;;

let get_state t k = get_arr t.state (intern_key k)

let set_platform t k v =
  let slot = intern_key k in
  if not (slot_unchanged t.platform slot v)
  then (
    t.platform <- set_arr t.platform slot v)
;;

let get_platform t k = get_arr t.platform (intern_key k)
let get_param t k = get_arr t.params (intern_key k)
let get_signal t k = get_arr t.signals (intern_key k)
let set_signal t k v =
  let slot = intern_key k in
  if not (slot_unchanged t.signals slot v)
  then (
    t.signals <- set_arr t.signals slot v)
;;

(* Slot-addressed publication for the fixed fact/gate set. [Strategy_fact_slots] resolves
   each name to one of these indices at startup, so the per-cycle publish path is an array
   store with no string hashing. *)
let slot_of_key : string -> int = intern_key
let get_state_slot t slot = get_arr t.state slot
let get_param_slot t slot = get_arr t.params slot
let get_platform_slot t slot = get_arr t.platform slot
let get_signal_slot t slot = get_arr t.signals slot

let set_state_slot t slot v =
  if not (slot_unchanged t.state slot v)
  then (
    t.state <- set_arr t.state slot v)
;;

(* Shared immutable bool blocks: an unchanged gate stores nothing, and a changed bool
   reuses one of these instead of allocating a fresh [V_bool]. *)
let v_true = V_bool true
let v_false = V_bool false

let[@inline] set_state_bool_slot t slot b =
  match get_arr t.state slot with
  | V_bool b0 when Bool.equal b0 b -> ()
  | _ ->
    t.state <- set_arr t.state slot (if b then v_true else v_false)
;;

let[@inline] set_state_float_slot t slot f =
  match get_arr t.state slot with
  | V_float f0 when Float.equal f0 f -> ()
  | _ -> t.state <- set_arr t.state slot (V_float f)
;;

let[@inline] set_state_int_slot t slot i =
  match get_arr t.state slot with
  | V_int i0 when Int.equal i0 i -> ()
  | _ -> t.state <- set_arr t.state slot (V_int i)
;;

let[@inline] set_state_string_slot t slot str =
  match get_arr t.state slot with
  | V_string s0 when String.equal s0 str -> ()
  | _ -> t.state <- set_arr t.state slot (V_string str)
;;

let set_platform_slot t slot v =
  if not (slot_unchanged t.platform slot v)
  then (
    t.platform <- set_arr t.platform slot v)
;;

(* Typed platform setters. The caller passes the raw payload, so an unchanged fact is
   rejected by comparing the unboxed value against the slot - no [V_bool]/[V_float] block
   is allocated. Most facts are unchanged cycle to cycle, so this removes the fact-boxing
   allocation that [set_platform_slot t slot (V_bool ..)] paid unconditionally. *)
let[@inline] platform_slot_get t slot =
  if slot < Array.length t.platform then Array.unsafe_get t.platform slot else V_unset
;;

let[@inline] set_platform_bool t slot (b : bool) =
  match platform_slot_get t slot with
  | V_bool b0 when Bool.equal b0 b -> ()
  | _ ->
    t.platform <- set_arr t.platform slot (V_bool b)
;;

let[@inline] set_platform_float t slot (f : float) =
  match platform_slot_get t slot with
  | V_float f0 when Float.equal f0 f -> ()
  | _ ->
    t.platform <- set_arr t.platform slot (V_float f)
;;

let[@inline] set_platform_int t slot (i : int) =
  match platform_slot_get t slot with
  | V_int i0 when Int.equal i0 i -> ()
  | _ ->
    t.platform <- set_arr t.platform slot (V_int i)
;;

(* Raw-payload string setter: compare before allocating the [V_string] block. The symbol
   and base-coin facts are republished every overlay tick but almost never change. *)
let[@inline] set_platform_string t slot (s : string) =
  match platform_slot_get t slot with
  | V_string s0 when String.equal s0 s -> ()
  | _ ->
    t.platform <- set_arr t.platform slot (V_string s)
;;

let rt_facts_io t =
  match t.facts_io with
  | Some io -> io
  | None ->
    let io =
      { io_f = (fun slot v -> set_platform_float t slot v)
      ; io_i = (fun slot v -> set_platform_int t slot v)
      ; io_b = (fun slot v -> set_platform_bool t slot v)
      ; io_s = (fun slot v -> set_platform_string t slot v)
      ; io_get_f =
          (fun slot ->
            match platform_slot_get t slot with
            | V_float f -> f
            | _ -> 0.0)
      }
    in
    t.facts_io <- Some io;
    io
;;

let set_signal_slot t slot v =
  if not (slot_unchanged t.signals slot v)
  then (
    t.signals <- set_arr t.signals slot v)
;;

let set_caps t c = t.caps <- c
let make_event kind fields = { ev_kind = kind; ev_fields = fields }
let current_event t = t.event

