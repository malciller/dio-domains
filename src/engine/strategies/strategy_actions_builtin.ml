(** Declarations for the initial action inventory.

    Metadata only in this slice (no handlers); schemas are what the validator checks user
    strategy files against. *)

let se ?(required = true) arg kind = Strategy_actions.{ arg; kind; required }
let act name class_ schema = Strategy_actions.{ name; class_; schema; handler = None }
let read = Strategy_actions.Read
let decision = Strategy_actions.Decision
let effectful = Strategy_actions.Effectful
let e = Strategy_actions.A_expr
let s = Strategy_actions.A_string
let b = Strategy_actions.A_bool
let enum l = Strategy_actions.A_enum l

let declarations =
  [ act
      "compute_grid_price"
      decision
      [ se "ref" e
      ; se "lo" e
      ; se "hi" e
      ; se "side" (enum [ "below"; "above" ])
      ; se ~required:false "snap" e
      ]
  ; act "compute_buy_ref_price" decision [ se "bid" e; se "ask" e ]
  ; act
      "owed_sell_price"
      decision
      [ se "bid" e; se "ask" e; se ~required:false "capital_exhausted" b ]
  ; act
      "available_base"
      decision
      [ se "venue_authoritative" b
      ; se "asset_balance_nan" b
      ; se "venue_available" e
      ; se "ledger_balance" e
      ; se "unreflected_credit" e
      ; se "reserved_base" e
      ; se "committed_sell" e
      ; se "unnetted_hold" e
      ]
  ; act
      "rung_price"
      decision
      [ se "current" e; se "grid_interval_pct" e; se "is_above" b ]
  ; act "cycle_prepare" decision []
  ; act "init_venue_state" decision []
  ; act "prepare_recovery" decision []
  ; act "resolve_book" read []
  ; act "cycle_cleanup" decision []
  ; act "expire_amend_cooldowns" decision []
  ; act "evict_ghost_orders" decision []
  ; act "scan_open_orders" decision []
  ; act "refresh_maker_fee" decision []
  ; act "cycle_guard" decision []
  ; act "buy_gate" decision []
  ; act "expire_tif_recovery" decision []
  ; act "cycle_facts" read []
  ; act "mark_stale_cycle" decision []
  ; act "buy_facts" decision []
  ; act "cancel_excess_buys" decision []
  ; act "buy_place" decision []
  ; act "buy_place_plan" read []
  ; act "buy_place_send" decision []
  ; act "buy_place_send_insufficient" decision []
  ; act "buy_place_latch_capital_low" decision []
  ; act "buy_place_warn_quote" decision []
  ; act "buy_amend" decision []
  ; act "buy_amend_has_sell" read []
  ; act "buy_amend_with_sell" decision []
  ; act "buy_amend_no_sell" decision []
  ; act "plan_sell_order" decision []
  ; act "sell_place" decision []
  ; act "sell_place_should" read []
  ; act "sell_place_body" decision []
  ; act "sell_finalize" decision []
  ; act "sell_finalize_facts" read []
  ; act "sell_finalize_latch" decision []
  ; act "sell_excess_sweep_phase" decision []
  ; act "sell_finalize_end" decision []
  ; act "apply_order_event" decision []
  ; act "compute_sell_price" decision [ se "base" e; se "mult" e ]
  ; act
      "compute_amend_price"
      decision
      [ se "ref" e; se "lo" e; se ~required:false "hi" e ]
  ; act "read_book" read []
  ; act "read_capacity" read [ se "asset" s ]
  ; act "read_open_orders" read []
  ; act
      "place_buy"
      effectful
      [ se "qty" e; se "price" e; se ~required:false "post_only" b; se "dedup_key" e ]
  ; act "place_sell" effectful [ se "qty" e; se "price" e; se "dedup_key" e ]
  ; act "amend_buy" effectful [ se "token" e; se "price" e; se "dedup_key" e ]
  ; act
      "cancel_all"
      effectful
      [ se "side" (enum [ "buy"; "sell"; "both" ]); se "dedup_key" e ]
  ; act "cancel_order" effectful [ se "token" e; se "dedup_key" e ]
  ; act "accumulate" decision [ se "qty" e; se ~required:false "profit" e ]
  ; act "track_buy" decision [ se "token" e; se "price" e ]
  ; act "track_sell" decision [ se "token" e; se "price" e ]
  ; act "set_time" decision [ se "state" s ]
  ; act "set_cooldown" decision [ se "name" s; se ~required:false "seconds" e ]
  ; act "update_reserved_base" decision [ se "qty" e ]
  ; act "notify_oracle" decision [ se ~required:false "message" s ]
  ; act "gate_balance" read []
  ; act "gate_capital_halted" read []
  ; act "is_ghost" read [ se "token" e ]
  ; act "reconcile_position" read []
  ; act "reconcile_persisted_sell_levels" read []
  ]
;;

(** Register every declaration; idempotent (duplicates are ignored). *)
let register_all () =
  List.iter
    (fun a -> ignore (Strategy_actions.try_register a : (unit, string) result))
    declarations
;;
