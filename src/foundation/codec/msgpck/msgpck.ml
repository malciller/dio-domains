(** In-tree MessagePack encoder.

    Hyperliquid signs the MessagePack encoding of each action, so the canonical integer
    widths and container tags below are part of the wire contract. Only the encoder
    surface the engine uses is implemented. *)

type t =
  | Nil
  | Bool of bool
  | Int of int
  | Uint32 of int32
  | Int32 of int32
  | Uint64 of int64
  | Int64 of int64
  | Float32 of int32
  | Float of float
  | String of string
  | Bytes of string
  | Ext of int * string
  | List of t list
  | Map of (t * t) list

let add_u8 b v = Buffer.add_char b (Char.chr (v land 0xff))

let add_u16_be b v =
  add_u8 b (v lsr 8);
  add_u8 b v
;;

let add_u32_be b v =
  add_u8 b (Int32.to_int (Int32.shift_right_logical v 24));
  add_u8 b (Int32.to_int (Int32.shift_right_logical v 16));
  add_u8 b (Int32.to_int (Int32.shift_right_logical v 8));
  add_u8 b (Int32.to_int v)
;;

let add_u64_be b v =
  for k = 0 to 7 do
    add_u8
      b
      (Int64.to_int (Int64.logand (Int64.shift_right_logical v (56 - (8 * k))) 0xffL))
  done
;;

let write_nil b = add_u8 b 0xc0
let write_bool b v = add_u8 b (if v then 0xc3 else 0xc2)

let write_int b v =
  let i = Int64.of_int v in
  if i >= 0L && i <= 0x7fL
  then add_u8 b v
  else if i >= 0L && i <= 0xffL
  then (
    add_u8 b 0xcc;
    add_u8 b v)
  else if i >= 0L && i <= 0xffffL
  then (
    add_u8 b 0xcd;
    add_u16_be b v)
  else if i >= 0L && i <= 0xffff_ffffL
  then (
    add_u8 b 0xce;
    add_u32_be b (Int32.of_int v))
  else if i >= 0L
  then (
    add_u8 b 0xcf;
    add_u64_be b i)
  else if i >= Int64.sub (Int64.neg 0x1fL) 1L
  then add_u8 b v
  else if i >= Int64.sub (Int64.neg 0x7fL) 1L
  then (
    add_u8 b 0xd0;
    add_u8 b v)
  else if i >= Int64.sub (Int64.neg 0x7fffL) 1L
  then (
    add_u8 b 0xd1;
    add_u16_be b v)
  else if i >= Int64.sub (Int64.neg 0x7fff_ffffL) 1L
  then (
    add_u8 b 0xd2;
    add_u32_be b (Int32.of_int v))
  else (
    add_u8 b 0xd3;
    add_u64_be b i)
;;

let write_uint32 b i =
  add_u8 b 0xce;
  add_u32_be b i
;;

let write_uint64 b i =
  add_u8 b 0xcf;
  add_u64_be b i
;;

let write_int32 b i =
  add_u8 b 0xd2;
  add_u32_be b i
;;

let write_int64 b i =
  add_u8 b 0xd3;
  add_u64_be b i
;;

let write_float32 b i =
  add_u8 b 0xca;
  add_u32_be b i
;;

let write_float b f =
  add_u8 b 0xcb;
  add_u64_be b (Int64.bits_of_float f)
;;

let write_string b s =
  let len = String.length s in
  if len <= 0x1f
  then add_u8 b (0xa0 lor len)
  else if len <= 0xff
  then (
    add_u8 b 0xd9;
    add_u8 b len)
  else if len <= 0xffff
  then (
    add_u8 b 0xda;
    add_u16_be b len)
  else (
    add_u8 b 0xdb;
    add_u32_be b (Int32.of_int len));
  Buffer.add_string b s
;;

let write_bin b s =
  let len = String.length s in
  if len <= 0xff
  then (
    add_u8 b 0xc4;
    add_u8 b len)
  else if len <= 0xffff
  then (
    add_u8 b 0xc5;
    add_u16_be b len)
  else (
    add_u8 b 0xc6;
    add_u32_be b (Int32.of_int len));
  Buffer.add_string b s
;;

let write_ext b typ s =
  let len = String.length s in
  (match len with
   | 1 -> add_u8 b 0xd4
   | 2 -> add_u8 b 0xd5
   | 4 -> add_u8 b 0xd6
   | 8 -> add_u8 b 0xd7
   | 16 -> add_u8 b 0xd8
   | n when n <= 0xff ->
     add_u8 b 0xc7;
     add_u8 b n
   | n when n <= 0xffff ->
     add_u8 b 0xc8;
     add_u16_be b n
   | n ->
     add_u8 b 0xc9;
     add_u32_be b (Int32.of_int n));
  add_u8 b typ;
  Buffer.add_string b s
;;

let rec write b = function
  | Nil -> write_nil b
  | Bool v -> write_bool b v
  | Int v -> write_int b v
  | Uint32 v -> write_uint32 b v
  | Int32 v -> write_int32 b v
  | Uint64 v -> write_uint64 b v
  | Int64 v -> write_int64 b v
  | Float32 v -> write_float32 b v
  | Float v -> write_float b v
  | String s -> write_string b s
  | Bytes s -> write_bin b s
  | Ext (typ, s) -> write_ext b typ s
  | List xs ->
    let len = List.length xs in
    if len <= 0xf
    then add_u8 b (0x90 lor len)
    else if len <= 0xffff
    then (
      add_u8 b 0xdc;
      add_u16_be b len)
    else (
      add_u8 b 0xdd;
      add_u32_be b (Int32.of_int len));
    List.iter (write b) xs
  | Map xs ->
    let len = List.length xs in
    if len <= 0xf
    then add_u8 b (0x80 lor len)
    else if len <= 0xffff
    then (
      add_u8 b 0xde;
      add_u16_be b len)
    else (
      add_u8 b 0xdf;
      add_u32_be b (Int32.of_int len));
    List.iter
      (fun (k, v) ->
        write b k;
        write b v)
      xs
;;

module StringBuf = struct
  let write ?pos:_ b msg =
    let start = Buffer.length b in
    write b msg;
    Buffer.length b - start
  ;;

  let to_string msg =
    let b = Buffer.create 64 in
    ignore (write b msg);
    Buffer.contents b
  ;;
end
