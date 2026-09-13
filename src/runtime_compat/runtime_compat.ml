(* Compat shims over OCaml runtime APIs whose types differ between the
   classic toolchain and OxCaml.

   OxCaml adds a mode system and marks several legacy [Sys]/[Domain] functions
   as [unsafe_multidomain] because they are not race-safe when used from
   multiple domains. Its replacement is [Sys.Safe]; the originals still exist
   and behave identically. This single implementation is used on every
   toolchain: the alert suppression below is accepted, and is a no-op, on
   classic OCaml, which does not know the [unsafe_multidomain] alert. That
   removes the need for a build-time toolchain selection.

   [set_signal] installs a process-wide signal handler. Handlers intentionally
   close over process-wide flags and Lwt state and are installed once from the
   main domain, so they cannot be made [portable]; signals are delivered to one
   domain and the handlers only touch atomics and Lwt state. We therefore use
   the standard API and acknowledge the [unsafe_multidomain] alert. *)

[@@@alert "-unsafe_multidomain"]

let set_signal = Sys.set_signal
