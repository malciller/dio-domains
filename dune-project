(lang dune 3.17)

(name dio)

; One package: every foundation library is an internal component of dio, not a
; separately published opam package. Public library names are namespaced under
; [dio.] (dio.lwt, dio.cohttp, ...), so a stanza never looks like an external
; dependency. dio.opam is maintained by hand because it records the native
; system dependency that dune cannot infer.
(generate_opam_files false)

(source
 (github malciller/dio-domains))

(authors "Cal Miller <malciller@gmail.com>")

(maintainers "Cal Miller <malciller@gmail.com>")

(license LICENSE)

(package
 (name dio)
 (synopsis "Domain-Oriented Trading Engine")
 (description
  "A high-performance, domain-oriented trading engine built with OCaml 5 multicore")
 (depends
  ocaml
  dune
  dune-configurator
  (odoc :with-doc))
 (tags
  ("trading" "finance" "multicore" "ocaml" "domains")))

; Foundation libraries are components of the single [dio] package. Their public
; names live under [dio.] (for example [dio.lwt], [dio.cohttp]) and install as
; part of that package; they are not separately distributable.

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/reference/dune-project/index.html
