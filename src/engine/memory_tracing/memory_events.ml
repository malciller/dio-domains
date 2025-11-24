(** Memory Pressure Event System for Event-Driven Cleanup

    Publishes memory pressure events when memory growth is detected,
    allowing subsystems to react with appropriate cleanup actions.
*)

open Concurrency

(** Memory pressure event types *)
type memory_event =
  | MemoryGrowth of {
      heap_growth_mb: int;
      live_growth_mb: int;
      fragmentation_percent: float;
      runtime_seconds: float;
    }
  | MemoryPressure of {
      level: [`Medium | `High];
      heap_mb: int;
      live_mb: int;
      fragmentation_percent: float;
    }
  | CleanupRequested

(** Event bus for memory pressure events *)
module MemoryEventBus = Event_bus.Make(struct
  type t = memory_event
end)

(** Global memory event bus instance *)
let memory_event_bus = MemoryEventBus.create "memory_pressure"

(** Publish memory growth event *)
let publish_memory_growth heap_growth_mb live_growth_mb fragmentation_percent runtime_seconds =
  MemoryEventBus.publish memory_event_bus (MemoryGrowth {
    heap_growth_mb;
    live_growth_mb;
    fragmentation_percent;
    runtime_seconds;
  })

(** Publish memory pressure event *)
let publish_memory_pressure level heap_mb live_mb fragmentation_percent =
  MemoryEventBus.publish memory_event_bus (MemoryPressure {
    level;
    heap_mb;
    live_mb;
    fragmentation_percent;
  })

(** Publish cleanup request event *)
let publish_cleanup_request () =
  MemoryEventBus.publish memory_event_bus CleanupRequested

(** Subscribe to memory pressure events *)
let subscribe_memory_events () =
  MemoryEventBus.subscribe memory_event_bus

(** Get subscriber statistics for monitoring *)
let get_subscriber_stats () =
  MemoryEventBus.get_subscriber_stats memory_event_bus
