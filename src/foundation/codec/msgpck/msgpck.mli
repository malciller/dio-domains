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

module StringBuf : sig
  val write : ?pos:int -> Buffer.t -> t -> int
  val to_string : t -> string
end
