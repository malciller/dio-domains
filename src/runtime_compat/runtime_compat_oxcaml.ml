(* OxCaml toolchain.

   [Sys.Safe.set_signal] requires the handler to be [portable] (free of shared
   mutable state). Our handlers intentionally close over process-wide flags and
   Lwt state (e.g. [shutdown_requested], [quit]) and are installed once from the
   main domain; they cannot be made portable without an architectural change.
   Signals are delivered to one domain and these handlers only touch atomics and
   Lwt state, so we use the standard API and acknowledge the
   [unsafe_multidomain] alert at the call sites that install handlers.

   See runtime_compat_stdlib.ml for the shared description. *)

[@@@alert "-unsafe_multidomain"]

let set_signal = Sys.set_signal
