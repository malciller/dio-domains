(** Strategy-agnostic per-symbol account state.

    The platform-owned treasury a strategy file reads and mutates through actions: base
    and quote reserved against resting orders, order/placement cooldowns, tracked order
    tokens, and accumulated profit. No strategy policy and no I/O — the hot path only
    touches memory; persistence is signalled separately. *)

type t =
  { mutable reserved_base : float
  ; mutable reserved_quote : float
  ; mutable accumulated_profit : float
  ; cooldowns : (string, float) Hashtbl.t (* name -> expiry unix ts *)
  ; mutable tracked_buy : (string * float) option (* (token, price) *)
  ; mutable tracked_sells : (string * float) list
  }

let create () =
  { reserved_base = 0.0
  ; reserved_quote = 0.0
  ; accumulated_profit = 0.0
  ; cooldowns = Hashtbl.create 8
  ; tracked_buy = None
  ; tracked_sells = []
  }
;;

let set_cooldown t ~name ~seconds ~now =
  if seconds > 0.0 then Hashtbl.replace t.cooldowns name (now +. seconds)
;;

let is_on_cooldown t ~name ~now =
  match Hashtbl.find_opt t.cooldowns name with
  | Some expiry -> now < expiry
  | None -> false
;;

let track_buy t ~token ~price = t.tracked_buy <- Some (token, price)
let track_sell t ~token ~price = t.tracked_sells <- (token, price) :: t.tracked_sells
let update_reserved_base t ~qty = t.reserved_base <- Float.max 0.0 (t.reserved_base +. qty)
let accumulate t ~qty:_ ~profit = t.accumulated_profit <- t.accumulated_profit +. profit

let expire_cooldowns t ~now =
  Hashtbl.filter_map_inplace (fun _ e -> if e <= now then None else Some e) t.cooldowns
;;
