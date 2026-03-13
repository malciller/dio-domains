(** Global tick event bus for tick-driven architectures *)

module Payload = struct
  type t = unit
end

include Event_bus.Make(Payload)

let global_bus = create "global_tick"

let publish_tick () = publish global_bus ()
let subscribe_ticks () = subscribe global_bus
