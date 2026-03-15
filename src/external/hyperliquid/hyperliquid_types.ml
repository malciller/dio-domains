(** Hyperliquid specific types and message formats *)

open Msgpck

(** Basic Hyperliquid API types *)
type tif = Alo | Ioc | Gtc
type tpsl = Tp | Sl
type limit_order_type = { tif: tif }
type trigger_order_type = { triggerPx: string; isMarket: bool; tpsl: tpsl }

type order_type_wire = 
  | Limit of limit_order_type
  | Trigger of trigger_order_type

type order_wire = {
  a: int;          (* asset *)
  b: bool;         (* is buy *)
  p: string;       (* limit px *)
  s: string;       (* size *)
  r: bool;         (* reduce only *)
  t: order_type_wire; 
  c: string option;(* cloid *)
}

type cancel_wire = {
  a: int;    (* asset *)
  o: int64;  (* order id *)
}

type cancel_by_cloid_wire = {
  a: int;
  cloid: string;
}

(** Convert TIF to msgpck *)
let tif_string = function
  | Alo -> "Alo"
  | Ioc -> "Ioc"
  | Gtc -> "Gtc"

let tpsl_string = function
  | Tp -> "tp"
  | Sl -> "sl"

let pack_order_type_wire ot =
  match ot with
  | Limit l ->
      Map [ (String "limit", Map [ (String "tif", String (tif_string l.tif)) ]) ]
  | Trigger t ->
      Map [ (String "trigger", 
        Map [ 
          (String "isMarket", Bool t.isMarket);
          (String "triggerPx", String t.triggerPx);
          (String "tpsl", String (tpsl_string t.tpsl))
        ]) 
      ]

let pack_cloid c_opt =
  match c_opt with
  | Some c -> [ (String "c", String c) ]
  | None -> []

(** Convert order_wire to Msgpck map *)
let pack_order_wire (ow : order_wire) =
  let base_fields = [
    (String "a", Int ow.a);
    (String "b", Bool ow.b);
    (String "p", String ow.p);
    (String "s", String ow.s);
    (String "r", Bool ow.r);
    (String "t", pack_order_type_wire ow.t);
  ] in
  Map (base_fields @ pack_cloid ow.c)

let pack_cancel_wire (cw : cancel_wire) =
  Map [
    (String "a", Int cw.a);
    (String "o", Int64 cw.o);
  ]

let pack_cancel_by_cloid_wire (cw : cancel_by_cloid_wire) =
  Map [
    (String "a", Int cw.a);
    (String "cloid", String cw.cloid);
  ]

(** Pack full Action payloads *)

(** Order Action *)
let pack_order_action ~orders ~grouping =
  Map [
    (String "type", String "order");
    (String "orders", List (List.map pack_order_wire orders));
    (String "grouping", String grouping);
  ]

(** Cancel Action *)
let pack_cancel_action ~cancels =
  Map [
    (String "type", String "cancel");
    (String "cancels", List (List.map pack_cancel_wire cancels));
  ]

(** Modify Action (single) - Use batchModify instead for better compatibility *)
let pack_modify_wire ~oid ~order =
  Map [
    (String "oid", Int64 oid);
    (String "order", pack_order_wire order);
  ]

let pack_batch_modify_action ~modifies ~grouping =
  Map [
    (String "type", String "batchModify");
    (String "modifies", List (List.map (fun (oid, order) -> pack_modify_wire ~oid ~order) modifies));
    (String "grouping", String grouping);
  ]

(** Cancel by Cloid Action *)
let pack_cancel_by_cloid_action ~cancels =
  Map [
    (String "type", String "cancelByCloid");
    (String "cancels", List (List.map pack_cancel_by_cloid_wire cancels));
  ]

(** Serialize to string *)
let serialize_action msgpck_val =
  let buf = Buffer.create 128 in
  let _ = Msgpck.StringBuf.write buf msgpck_val in
  Buffer.contents buf

(** Market Data JSON parsing structure (handled easily with Yojson mapping in feeds, 
    but defined here for cleanliness if needed) *)
