(* Compat shims over OCaml runtime APIs whose types differ between the
   classic-flambda toolchain and OxCaml.

   OxCaml adds a mode system and marks several legacy [Sys]/[Domain] functions
   as [unsafe_multidomain] because they are not race-safe when used from
   multiple domains. It provides [Sys.Safe] variants; this module routes to them
   when available and falls back to the standard functions otherwise, so call
   sites are toolchain-agnostic. See runtime_compat_oxcaml.ml.

   [set_signal] installs a process-wide signal handler. On OxCaml this uses
   [Sys.Safe.set_signal] (safe with multiple domains); on the classic toolchain
   it uses [Sys.set_signal]. Same observable behaviour. *)

let set_signal = Sys.set_signal
