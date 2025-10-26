(lang dune 3.17)

(name dio)

(generate_opam_files true)

(source
 (github malciller/dio))

(authors "Cal Miller <malciller@gmail.com>")

(maintainers "Cal Miller <malciller@gmail.com>")

(license LICENSE)

(documentation https://url/to/documentation)

(package
 (name dio)
 (synopsis "Domain-Oriented Trading Engine")
 (description "A high-performance, domain-oriented trading engine built with OCaml 5 multicore")
 (depends
   ocaml
   dune
   yojson
   lwt
   lwt_ppx
   lwt_ssl
   mirage-crypto
   mirage-crypto-rng
   cohttp-lwt-unix
   websocket-lwt-unix
   dotenv
   base64
   digestif
   domainslib
   uri
   conduit-lwt-unix
   ipaddr
   alcotest
   lwd
   notty
   nottui
   nottui-lwt
   cpuid
   mtime)
 (tags
   ("trading" "finance" "multicore" "ocaml" "domains")))


; See the complete stanza docs at https://dune.readthedocs.io/en/stable/reference/dune-project/index.html
