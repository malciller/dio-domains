# In-tree OCaml dependencies

Everything in this directory is built by this repository. The goal is that the
application links no OCaml library from opam; the only external OCaml library
still linked is `secp256k1` (the audited native signing library), plus the
compiler standard library (`unix`, `threads`, `bigarray`).

The public library names match the upstream packages (`lwt`, `cohttp`,
`websocket`, …), so application source and `(libraries ...)` stanzas did not
change.

## Provenance

| Directory | Upstream | Version | License | Notes |
|---|---|---|---|---|
| `lwt/` | ocaml-lwt | 6.0.0+ox | MIT | OxCaml patch baked into the generated sources; `lwt_ppx` was removed and its expansion baked into call sites |
| `ssl/` | ocaml-ssl | 0.7.0 | LGPL-2.1 + OpenSSL exemption | OpenSSL bindings; `pkg-config openssl` used at build time |
| `lwt_ssl/` | lwt_ssl | 1.2.0 | LGPL-2.1 + OpenSSL exemption | |
| `base64/` | ocaml-base64 | 3.5.2 | ISC | |
| `mtime/` | mtime | 2.2.0 | ISC | |
| `yojson/` | yojson | 2.2.2+ox | BSD-3-Clause | OxCaml `write_intlit` eta-expansion applied |
| `msgpck/` | msgpck | 1.7 | ISC | `ocplib-endian` replaced by local big-endian helpers |
| `notty/` | notty-community | 0.2.4 | ISC | core renderer plus the no-Uucp Unicode tables (Uucp/Uuseg data, ISC); `Notty_unix` is a minimal `winsize` shim |
| `uutf/` | uutf | 1.0.4+ox | ISC | |
| `ctypes/` | ocaml-ctypes | 0.24.0+ox | MIT | OxCaml bigarray patch applied |
| `integers/` | ocaml-integers | 0.8.0 | MIT | `stdlib-shims` dependency dropped |
| `secp256k1` | libsecp256k1 bindings | 0.5.0 | MIT | **not vendored** — kept as the audited native dependency |

## Hand-written replacements

These directories are original code written for this repository, not copies of
the upstream packages. They implement only the API surface the engine uses:

- `digestif/` — SHA-1, SHA-256, SHA-512, HMAC-SHA-512, Keccak-256
- `uri/` — absolute URL parsing, percent-encoding, query construction
- `ipaddr/` — IPv4/IPv6 carrier for resolved addresses
- `conduit-lwt-unix/` — client-side TCP/TLS connect using `ssl`/`lwt_ssl`
- `cohttp/`, `cohttp-lwt/`, `cohttp-lwt-unix/` — HTTP/1.1 client, chunked/SSE streaming
- `websocket/` — RFC 6455 client framing
- `mirage-crypto-rng/` — the engine's RNG initialisation hook
- `alcotest/` — minimal test harness matching the suite's API

## Notes

- `notty/unix` implements only `winsize`, which is the only `Notty_unix` API
  the dashboard and logging code use. Rendering goes through
  `Notty.Render.to_buffer` and direct stdout writes.
- Lwt's `.cppo.ml` templates were expanded once with the pinned
  `OCaml:5.2.0+ox` and are committed as generated sources; the `.cppo.*` inputs
  are kept as `.disabled` for provenance. Regenerate them if the compiler
  version changes.
- `lwt_ppx` was removed. Its expansion is baked into the 14 files that used
  `let%lwt`/`match%lwt`; the transformation is the one the ppx itself emits.

## Updating a vendored library

1. Copy the new upstream source into the directory.
2. Re-apply the OxCaml patch from the matching `~/.opam/<switch>/.opam-switch/packages/<pkg>/files/` directory.
3. Pre-generate any `cppo` sources (Lwt) and move the `.cppo.*` templates out of the build path.
4. Keep the public library name and module names unchanged.
5. Run `dune build @runtest` and `dune build --profile=release bin/main.exe bin/dashboard.exe`.
