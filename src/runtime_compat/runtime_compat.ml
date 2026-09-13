(* Compat shims over OCaml runtime APIs whose types differ between the classic
   toolchain and OxCaml.

   OxCaml adds a mode system and marks several legacy [Sys]/[Domain] functions
   as [unsafe_multidomain] because they are not race-safe across domains. Its
   replacement is [Sys.Safe]; the originals remain and behave identically. This
   implementation is used on every toolchain: the alert suppression below is
   valid, and a no-op, on classic OCaml, which does not know the
   [unsafe_multidomain] alert, removing the need for toolchain selection.

   [set_signal] installs a process-wide signal handler. Handlers intentionally
   close over process-wide flags and Lwt state and are installed once from the
   main domain, so they cannot be made [portable]; signals are delivered to one
   domain and the handlers only touch atomics and Lwt state. The standard API
   is therefore used with the [unsafe_multidomain] alert acknowledged. *)

[@@@alert "-unsafe_multidomain"]

let set_signal = Sys.set_signal
