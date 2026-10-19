module Span : sig
  type t

  val of_uint64_ns : int64 -> t
  val to_uint64_ns : t -> int64
  val to_float_ns : t -> float
  val zero : t
  val add : t -> t -> t
  val sub : t -> t -> t
  val mul : t -> float -> t
  val equal : t -> t -> bool
  val compare : t -> t -> int
end
