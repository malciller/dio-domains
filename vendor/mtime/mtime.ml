(** In-tree monotonic time spans.

    Only the span operations the engine uses are provided. Nanoseconds are stored as an
    [int64]; the module is deliberately allocation-free. *)

module Span = struct
  type t = int64

  let of_uint64_ns ns = ns
  let to_uint64_ns span = span
  let to_float_ns span = Int64.to_float span
  let zero = 0L
  let add a b = Int64.add a b
  let sub a b = Int64.sub a b
  let mul span factor = Int64.of_float (Int64.to_float span *. factor)
  let equal = Int64.equal
  let compare = Int64.compare
end
