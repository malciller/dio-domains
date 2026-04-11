(** Top-level namespace for the Discord integration.
    Re-exports the notifier submodule for external access. *)

(** Discord webhook notifier for order fill events. *)
module Notifier = Discord_notifier
