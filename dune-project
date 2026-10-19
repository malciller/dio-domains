(lang dune 3.17)

(name dio)

; dio.opam is maintained by hand: the application's OCaml dependencies now live
; in vendor/ and are separate in-tree packages, so auto-generating opam files
; would emit one package per vendored library. No external OCaml library is
; required; libsecp256k1 remains a native system dependency.
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
  (odoc :with-doc))
 (tags
  ("trading" "finance" "multicore" "ocaml" "domains")))

; In-tree replacements for the OCaml libraries the engine used to take from
; opam. The public names are kept identical so no application dune stanza or
; source reference has to change.
(package
 (name lwt))

(package
 (name base64))

(package
 (name ssl))

(package
 (name lwt_ssl))

(package
 (name uri))

(package
 (name ipaddr))

(package
 (name cohttp))

(package
 (name cohttp-lwt))

(package
 (name cohttp-lwt-unix))

(package
 (name conduit-lwt-unix))

(package
 (name websocket))

(package
 (name uutf))

(package
 (name notty-community))

(package
 (name alcotest))

(package
 (name mtime))

(package
 (name secp256k1))

(package
 (name lighter_ffi))

(package
 (name yojson))

(package
 (name msgpck))

(package
 (name digestif))

(package
 (name mirage-crypto-rng))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/reference/dune-project/index.html
