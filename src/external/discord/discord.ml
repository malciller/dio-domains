(** Top-level namespace for the Discord integration.
    Re-exports the notifier submodule. *)

(** Discord webhook notifier for order fill events. *)
module Notifier = Discord_notifier
