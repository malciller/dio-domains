(** Tick event bus: unit-payload specialization of [Event_bus] for tick-driven scheduling.

    Exposes a singleton [global_bus] and convenience wrappers for publishing
    and subscribing to tick events without constructing or passing a bus handle. *)

module Payload = struct
  (** Unit payload; tick events carry no data, only signal occurrence. *)
  type t = unit
end

include Event_bus.Make(Payload)

(** Singleton bus instance used as the global tick distribution channel. *)
let global_bus = create "global_tick"

(** Publish a single tick event to all active subscribers on [global_bus]. *)
let publish_tick () = publish global_bus ()

(** Return a subscription stream that yields on each tick published to [global_bus]. *)
let subscribe_ticks () = subscribe global_bus
