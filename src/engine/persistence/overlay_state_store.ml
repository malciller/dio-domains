(* Durable carry-overlay state.

   The overlay's ladder (anchor/ratio/add index/next band), profit-recycle state machine,
   trailing reference/value, and last actual close/spot fills. Separate from the grid's
   accumulation store; the only shared field is the reserved-base ledger. *)

let section = "overlay_state_store"

type fill =
  { price : float
  ; qty : float
  ; realized : float
  ; fee : float
  ; market : string (** ["perp"] or ["spot"]. *)
  ; kind : string
  ; ts : float
  }

type t =
  { entry : float
  ; anchor : float
  ; ladder_ratio : float
  ; add_index : int
  ; next_add : float
  ; add_armed : bool
  ; risk_paused : bool
  ; recycle_pending : bool
  ; pending_close : bool
  ; close_fill_seen : bool
  ; pending_gain : float
  ; pending_base_quote : float
  ; pending_base : float
  ; tp_book_price : float
  ; close_entry : float
  ; close_qty : float
  ; close_started_at : float
  ; close_seq : int
  ; close_seq_seen : int
  ; close_realized_total : float
  ; close_realized_seen : float
  ; recycle_seq : int
  ; recycle_seq_seen : int
  ; recycle_realized_total : float
  ; recycle_realized_seen : float
  ; last_close : fill option
  ; spot_seq : int
  ; spot_seq_seen : int
  ; last_recycle_base : float
  ; last_spot : fill option
  ; last_leverage : float
  ; full_balance_ref : bool
  ; updated_at : float
  }

let default =
  { entry = 0.0
  ; anchor = 0.0
  ; ladder_ratio = 0.0
  ; add_index = 0
  ; next_add = 0.0
  ; add_armed = false
  ; risk_paused = false
  ; recycle_pending = false
  ; pending_close = false
  ; close_fill_seen = false
  ; pending_gain = 0.0
  ; pending_base_quote = 0.0
  ; pending_base = 0.0
  ; tp_book_price = 0.0
  ; close_entry = 0.0
  ; close_qty = 0.0
  ; close_started_at = 0.0
  ; close_seq = 0
  ; close_seq_seen = 0
  ; close_realized_total = 0.0
  ; close_realized_seen = 0.0
  ; recycle_seq = 0
  ; recycle_seq_seen = 0
  ; recycle_realized_total = 0.0
  ; recycle_realized_seen = 0.0
  ; last_close = None
  ; spot_seq = 0
  ; spot_seq_seen = 0
  ; last_recycle_base = 0.0
  ; last_spot = None
  ; last_leverage = 0.0
  ; full_balance_ref = false
  ; updated_at = 0.0
  }
;;

let key_of ~exchange ~base_coin = Printf.sprintf "%s:%s" exchange base_coin

(* -- Serialization --------------------------------------------------- *)

let get_float json field default =
  try Yojson.Basic.Util.(json |> member field |> to_float) with
  | _ -> default
;;

let get_int json field default =
  try Yojson.Basic.Util.(json |> member field |> to_int) with
  | _ -> default
;;

let get_bool json field default =
  try Yojson.Basic.Util.(json |> member field |> to_bool) with
  | _ -> default
;;

let get_string json field default =
  try Yojson.Basic.Util.(json |> member field |> to_string) with
  | _ -> default
;;

let fill_of_json json =
  let f field default = get_float json field default in
  { price = f "price" 0.0
  ; qty = f "qty" 0.0
  ; realized = f "realized" 0.0
  ; fee = f "fee" 0.0
  ; market = get_string json "market" ""
  ; kind = get_string json "kind" ""
  ; ts = f "ts" 0.0
  }
;;

let fill_to_json (x : fill) =
  `Assoc
    [ "price", `Float x.price
    ; "qty", `Float x.qty
    ; "realized", `Float x.realized
    ; "fee", `Float x.fee
    ; "market", `String x.market
    ; "kind", `String x.kind
    ; "ts", `Float x.ts
    ]
;;

let of_json json =
  { entry = get_float json "entry" 0.0
  ; anchor = get_float json "anchor" 0.0
  ; ladder_ratio = get_float json "ladder_ratio" 0.0
  ; add_index = get_int json "add_index" 0
  ; next_add = get_float json "next_add" 0.0
  ; add_armed = get_bool json "add_armed" false
  ; risk_paused = get_bool json "risk_paused" false
  ; recycle_pending = get_bool json "recycle_pending" false
  ; pending_close = get_bool json "pending_close" false
  ; close_fill_seen = get_bool json "close_fill_seen" false
  ; pending_gain = get_float json "pending_gain" 0.0
  ; pending_base_quote = get_float json "pending_base_quote" 0.0
  ; pending_base = get_float json "pending_base" 0.0
  ; tp_book_price = get_float json "tp_book_price" 0.0
  ; close_entry = get_float json "close_entry" 0.0
  ; close_qty = get_float json "close_qty" 0.0
  ; close_started_at = get_float json "close_started_at" 0.0
  ; close_seq = get_int json "close_seq" 0
  ; close_seq_seen = get_int json "close_seq_seen" 0
  ; close_realized_total = get_float json "close_realized_total" 0.0
  ; close_realized_seen =
      get_float json "close_realized_seen" 0.0
      (* Historical files used the old trailing-drawdown names. Read them as a fallback so
         the first deploy after the policy change does not lose the close/split cursors. *)
  ; recycle_seq = get_int json "recycle_seq" (get_int json "drawdown_seq" 0)
  ; recycle_seq_seen =
      get_int json "recycle_seq_seen" (get_int json "drawdown_seq_seen" 0)
  ; recycle_realized_total =
      get_float
        json
        "recycle_realized_total"
        (get_float json "drawdown_realized_total" 0.0)
  ; recycle_realized_seen =
      get_float json "recycle_realized_seen" (get_float json "drawdown_realized_seen" 0.0)
  ; last_close =
      (match Yojson.Basic.Util.member "last_close" json with
       | `Assoc _ as v -> Some (fill_of_json v)
       | _ -> None)
  ; spot_seq = get_int json "spot_seq" 0
  ; spot_seq_seen = get_int json "spot_seq_seen" 0
  ; last_recycle_base = get_float json "last_recycle_base" 0.0
  ; last_spot =
      (match Yojson.Basic.Util.member "last_spot" json with
       | `Assoc _ as v -> Some (fill_of_json v)
       | _ -> None)
  ; last_leverage = get_float json "last_leverage" 0.0
  ; full_balance_ref = get_bool json "full_balance_ref" false
  ; updated_at = get_float json "updated_at" 0.0
  }
;;

let fill_field name = function
  | Some (x : fill) -> [ name, fill_to_json x ]
  | None -> []
;;

let to_json (x : t) =
  `Assoc
    ([ "entry", `Float x.entry
     ; "anchor", `Float x.anchor
     ; "ladder_ratio", `Float x.ladder_ratio
     ; "add_index", `Int x.add_index
     ; "next_add", `Float x.next_add
     ; "add_armed", `Bool x.add_armed
     ; "risk_paused", `Bool x.risk_paused
     ; "recycle_pending", `Bool x.recycle_pending
     ; "pending_close", `Bool x.pending_close
     ; "close_fill_seen", `Bool x.close_fill_seen
     ; "pending_gain", `Float x.pending_gain
     ; "pending_base_quote", `Float x.pending_base_quote
     ; "pending_base", `Float x.pending_base
     ; "tp_book_price", `Float x.tp_book_price
     ; "close_entry", `Float x.close_entry
     ; "close_qty", `Float x.close_qty
     ; "close_started_at", `Float x.close_started_at
     ; "close_seq", `Int x.close_seq
     ; "close_seq_seen", `Int x.close_seq_seen
     ; "close_realized_total", `Float x.close_realized_total
     ; "close_realized_seen", `Float x.close_realized_seen
     ; "recycle_seq", `Int x.recycle_seq
     ; "recycle_seq_seen", `Int x.recycle_seq_seen
     ; "recycle_realized_total", `Float x.recycle_realized_total
     ; "recycle_realized_seen", `Float x.recycle_realized_seen
     ; "spot_seq", `Int x.spot_seq
     ; "spot_seq_seen", `Int x.spot_seq_seen
     ; "last_recycle_base", `Float x.last_recycle_base
     ; "last_leverage", `Float x.last_leverage
     ; "full_balance_ref", `Bool x.full_balance_ref
     ; "updated_at", `Float x.updated_at
     ]
     @ fill_field "last_close" x.last_close
     @ fill_field "last_spot" x.last_spot)
;;

let orchestrator =
  Persistence_orchestrator.create
    ~filename:"carry_state.json"
    ~parse:(fun tree ->
      let open Yojson.Basic.Util in
      try tree |> to_assoc |> List.map (fun (k, v) -> k, of_json v) with
      | _ -> [])
    ~serialize:to_json
;;

let load ~exchange ~base_coin =
  match Persistence_orchestrator.load orchestrator ~key:(key_of ~exchange ~base_coin) with
  | Some t -> t
  | None -> default
;;

let save ~exchange ~base_coin t =
  Persistence_orchestrator.put orchestrator ~key:(key_of ~exchange ~base_coin) t
;;

let save_async ~exchange ~base_coin t =
  Persistence_orchestrator.put_async orchestrator ~key:(key_of ~exchange ~base_coin) t
;;

(* -- Pure state transitions ------------------------------------------ *)

let is_recycle_close_kind kind =
  String.equal kind "carry:recycle:close|ro" || String.equal kind "carry:drawdown|ro"
;;

(** Record an actual close fill. [realized] is net of fees, in quote terms. [recycle_seq]
    / [recycle_realized_total] advance only for the profit-recycle close kind; the other
    reduce-only buys (risk exits, resize) are counted in the all-close fields but ignored
    by the recycle split. *)
let record_close_fill t ~price ~qty ~realized ~fee ~kind ~ts =
  let is_recycle = is_recycle_close_kind kind in
  { t with
    close_seq = t.close_seq + 1
  ; close_realized_total = t.close_realized_total +. realized
  ; recycle_seq = (t.recycle_seq + if is_recycle then 1 else 0)
  ; recycle_realized_total =
      (t.recycle_realized_total +. if is_recycle then realized else 0.0)
  ; last_close = Some { price; qty; realized; fee; market = "perp"; kind; ts }
  ; updated_at = ts
  }
;;

(** Record an actual spot recycle buy. *)
let record_spot_fill t ~price ~qty ~realized ~fee ~kind ~ts =
  { t with
    spot_seq = t.spot_seq + 1
  ; last_spot = Some { price; qty; realized; fee; market = "spot"; kind; ts }
  ; updated_at = ts
  }
;;
