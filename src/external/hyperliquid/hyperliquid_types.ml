(** Hyperliquid specific types and message formats *)

open Msgpck

(* ──────────────────────────────────────────────────────────────── *)
(* Core wire types                                                  *)
(* ──────────────────────────────────────────────────────────────── *)

type tif = Alo | Ioc | Gtc
type tpsl = Tp | Sl

type limit_order_type = { tif : tif }
type trigger_order_type = { triggerPx : string; isMarket : bool; tpsl : tpsl }

type order_type_wire = 
  | Limit  of limit_order_type
  | Trigger of trigger_order_type

type order_wire = {
  a : int;                    (* asset index *)
  b : bool;                   (* is buy *)
  p : string;                 (* price *)
  s : string;                 (* size *)
  r : bool;                   (* reduce only *)
  t : order_type_wire;
  c : string option;          (* client order id / cloid *)
}

(** Cancel by order id (numeric oid) *)
type cancel_wire = {
  a : int;     (* asset index *)
  o : int64;   (* order id *)
}

(** Cancel by client order id (cloid string) *)
type cancel_by_cloid_wire = {
  a     : int;
  cloid : string;
}

(* ──────────────────────────────────────────────────────────────── *)
(* Packing helpers                                                  *)
(* ──────────────────────────────────────────────────────────────── *)

let tif_to_string = function Alo -> "Alo" | Ioc -> "Ioc" | Gtc -> "Gtc"
let tpsl_to_string = function Tp -> "tp" | Sl -> "sl"

(** Helper to pack a numeric ID using the most compact integer tag.
    Most Hyperliquid IDs fit in OCaml's 63-bit int. *)
let pack_id id =
  try Int (Int64.to_int id)
  with _ -> Int64 id

let pack_order_type_wire = function
  | Limit l ->
      Map [ String "limit", Map [ String "tif", String (tif_to_string l.tif) ] ]
  | Trigger t ->
      Map [ String "trigger",
            Map [ String "isMarket",   Bool t.isMarket;
                  String "triggerPx",  String t.triggerPx;
                  String "tpsl",       String (tpsl_to_string t.tpsl) ] ]

let pack_order_wire (ow : order_wire) =
  let fields = [
    String "a", Int ow.a;
    String "b", Bool ow.b;
    String "p", String ow.p;
    String "s", String ow.s;
    String "r", Bool ow.r;
    String "t", pack_order_type_wire ow.t;
  ] in
  let fields = match ow.c with
    | Some cloid -> fields @ [ String "c", String cloid ]
    | None       -> fields
  in
  Map fields

(* ──────────────────────────────────────────────────────────────── *)
(* Action packing – signed objects                                  *)
(* ──────────────────────────────────────────────────────────────── *)

let pack_order_action ~orders ~grouping =
  Map [
    String "type",     String "order";
    String "orders",   List (List.map pack_order_wire orders);
    String "grouping", String grouping;
  ]

let pack_modify_action ~oid ~order =
  Map [
    String "type",  String "modify";
    String "oid",   pack_id oid;
    String "order", pack_order_wire order;
  ]

let pack_batch_modify_action ~modifies ~grouping =
  let pack_one (oid, ord) =
    Map [ String "oid", pack_id oid; String "order", pack_order_wire ord ]
  in
  Map [
    String "type",      String "batchModify";
    String "modifies",  List (List.map pack_one modifies);
    String "grouping",  String grouping;
  ]
(* ────────────────────────────────────────────────────────────── *)
(* Cancel packing – type safe                                       *)
(* ────────────────────────────────────────────────────────────── *)

let pack_cancel_action ~(cancels : cancel_wire list) : t =
  Map [
    String "type", String "cancel";
    String "cancels",
      List (
        List.map
          (fun (c : cancel_wire) ->
            Map [
              String "a", Int c.a;
              String "o", pack_id c.o;
            ])
          cancels
      )
  ]

let pack_cancel_by_cloid_action ~(cancels : cancel_by_cloid_wire list) : t =
  Map [
    String "type", String "cancelByCloid";
    String "cancels",
      List (
        List.map
          (fun (c : cancel_by_cloid_wire) ->
            Map [
              String "a", Int c.a;
              String "cloid", String c.cloid;
            ])
          cancels
      )
  ]

let serialize_action (msg : t) : string =
  let buf = Buffer.create 512 in
  ignore (Msgpck.StringBuf.write buf msg);
  Buffer.contents buf