(** Hyperliquid wire types, order structures, and MessagePack serialization. *)

open Msgpck

(* Core wire types *)

(** Time-in-force variants: Add Liquidity Only, Immediate or Cancel, Good Til Cancelled. *)
type tif = Alo | Ioc | Gtc

(** Take-profit / stop-loss discriminator. *)
type tpsl = Tp | Sl

(** Limit order parameters with time-in-force constraint. *)
type limit_order_type = { tif : tif }

(** Trigger order parameters: activation price, market flag, and TP/SL type. *)
type trigger_order_type = { triggerPx : string; isMarket : bool; tpsl : tpsl }

(** Discriminated union of order types on the wire. *)
type order_type_wire = 
  | Limit  of limit_order_type
  | Trigger of trigger_order_type

(** Compact order representation for the Hyperliquid exchange API. *)
type order_wire = {
  a : int;                    (** Asset index. *)
  b : bool;                   (** Buy side flag. *)
  p : string;                 (** Limit price as decimal string. *)
  s : string;                 (** Order size as decimal string. *)
  r : bool;                   (** Reduce-only flag. *)
  t : order_type_wire;        (** Order type: limit or trigger. *)
  c : string option;          (** Optional client order ID (cloid). *)
}

(** Cancel request keyed by numeric order ID. *)
type cancel_wire = {
  a : int;     (** Asset index. *)
  o : int64;   (** Exchange-assigned order ID. *)
}

(** Cancel request keyed by client order ID string. *)
type cancel_by_cloid_wire = {
  a     : int;    (** Asset index. *)
  cloid : string; (** Client-assigned order ID. *)
}

(* Packing helpers *)

(** Convert time-in-force variant to its API string representation. *)
let tif_to_string = function Alo -> "Alo" | Ioc -> "Ioc" | Gtc -> "Gtc"

(** Convert take-profit/stop-loss variant to its API string representation. *)
let tpsl_to_string = function Tp -> "tp" | Sl -> "sl"

(** Pack a numeric ID using the most compact MsgPack integer tag.
    Falls back to Int64 encoding if the value exceeds OCaml int range. *)
let pack_id id =
  try Int (Int64.to_int id)
  with _ -> Int64 id

(** Serialize an order type to its MsgPack map representation.
    Limit orders encode TIF; trigger orders encode price, market flag, and TP/SL. *)
let pack_order_type_wire = function
  | Limit l ->
      Map [ String "limit", Map [ String "tif", String (tif_to_string l.tif) ] ]
  | Trigger t ->
      Map [ String "trigger",
            Map [ String "isMarket",   Bool t.isMarket;
                  String "triggerPx",  String t.triggerPx;
                  String "tpsl",       String (tpsl_to_string t.tpsl) ] ]

(** Serialize an order_wire to a MsgPack map.
    Appends the optional cloid field only when present. *)
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

(* Action packing: signed message payloads *)

(** Pack a batch order placement action with the given grouping mode. *)
let pack_order_action ~orders ~grouping =
  Map [
    String "type",     String "order";
    String "orders",   List (List.map pack_order_wire orders);
    String "grouping", String grouping;
  ]

(** Pack a single order modification action for the given exchange order ID. *)
let pack_modify_action ~oid ~order =
  Map [
    String "type",  String "modify";
    String "oid",   pack_id oid;
    String "order", pack_order_wire order;
  ]

(** Pack a batch order modification action with the given grouping mode.
    Each element is an (oid, order_wire) pair. *)
let pack_batch_modify_action ~modifies ~grouping =
  let pack_one (oid, ord) =
    Map [ String "oid", pack_id oid; String "order", pack_order_wire ord ]
  in
  Map [
    String "type",      String "batchModify";
    String "modifies",  List (List.map pack_one modifies);
    String "grouping",  String grouping;
  ]

(* Cancel action packing *)

(** Pack a cancel action for a list of cancel_wire entries keyed by numeric order ID. *)
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

(** Pack a cancel action for a list of cancel_by_cloid_wire entries keyed by client order ID. *)
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

(** Pack a USD class transfer action between spot and perp sub-accounts.
    [amount] is a decimal string; [to_perp] selects the transfer direction. *)
let pack_usd_class_transfer_action ~amount ~to_perp =
  Map [
    String "type",    String "usdClassTransfer";
    String "amount",  String amount;
    String "toPerp",  Bool to_perp;
  ]

(** Serialize a MsgPack value to a binary string using StringBuf encoding. *)
let serialize_action (msg : t) : string =
  let buf = Buffer.create 512 in
  ignore (Msgpck.StringBuf.write buf msg);
  Buffer.contents buf