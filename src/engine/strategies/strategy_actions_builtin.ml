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
