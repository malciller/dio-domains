(lang dune 3.17)

(name dio)

(generate_opam_files true)

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
  (yojson
   (and
    (>= 2.2.2)
    (< 2.2.3)))
  lwt
  lwt_ppx
  lwt_ssl
  mirage-crypto-rng
  (cohttp-lwt-unix
   (< 6.0.0))
  websocket
  base64
  digestif
  uri
  (conduit-lwt-unix
   (< 8.0.0))
  ipaddr
  alcotest
  mtime
  secp256k1
  msgpck
  ctypes
  ctypes-foreign
  notty-community)
 (tags
  ("trading" "finance" "multicore" "ocaml" "domains")))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/reference/dune-project/index.html
