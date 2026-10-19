# In-tree OCaml dependencies

Everything in this directory is built by this repository. The application
links no OCaml library from opam; the only remaining external dependency is the
native libsecp256k1, used through the in-tree binding, plus the compiler
standard library (`unix`, `threads`, `bigarray`).

The public library names match the upstream packages (`lwt`, `cohttp`,
`websocket`, …), so application source and `(libraries ...)` stanzas did not
change.

## Provenance

| Directory | Upstream | Version | License | Notes |
|---|---|---|---|---|
| `secp256k1/` | libsecp256k1 | native 0.7.1 | MIT | original OCaml binding; native libsecp256k1 remains external |

## Hand-written replacements

These directories are original code written for this repository, not copies of
the upstream packages. They implement only the API surface the engine uses:

- `lwt/` — promise runtime (`lwt`, `lwt.unix`), stream/condition/mutex/mvar/list helpers
- `digestif/` — SHA-1, SHA-256, SHA-512, HMAC-SHA-512, Keccak-256
- `base64/` — RFC 4648 encode/decode
- `mtime/` — monotonic spans and clock
- `msgpck/` — canonical MessagePack encoder
- `yojson/` — JSON parser, printer and `Safe`/`Basic`/`Util` subset
- `uutf/` — UTF-8 folder used by the terminal renderer
- `lighter_ffi/` — direct dlopen binding to the Go Lighter signer
- `ssl/` — direct OpenSSL binding
- `lwt_ssl/` — Lwt wrapper over the OpenSSL binding
- `notty/` — terminal image/attribute model and ANSI renderer used by the dashboard
- `uri/` — absolute URL parsing, percent-encoding, query construction
- `ipaddr/` — IPv4/IPv6 carrier for resolved addresses
- `conduit-lwt-unix/` — client-side TCP/TLS connect using `ssl`/`lwt_ssl`
- `cohttp/`, `cohttp-lwt/`, `cohttp-lwt-unix/` — HTTP/1.1 client, chunked/SSE streaming
- `websocket/` — RFC 6455 client framing
- `mirage-crypto-rng/` — the engine's RNG initialisation hook
- `alcotest/` — minimal test harness matching the suite's API
- `secp256k1/` — OCaml binding over native libsecp256k1

## Notes

- `notty/unix` implements only `winsize`, which is the only `Notty_unix` API
  the dashboard and logging code use. Rendering goes through
  `Notty.Render.to_buffer` and direct stdout writes.
- The promise type is covariant with its element type erased from the mutable
  cell. That keeps `Lwt.return_none` polymorphic despite the value restriction.
- `lwt_ppx` was removed. Its expansion is baked into the 14 files that used
  `let%lwt`/`match%lwt`; the transformation is the one the ppx itself emits.

## Updating a vendored library

The only third-party source still vendored is the native `libsecp256k1` binding
in `vendor/secp256k1`. To update it:

1. Copy the new binding source into the directory.
2. Keep the public library name and module names unchanged.
3. Run `dune build @runtest` and `dune build --profile=release bin/main.exe bin/dashboard.exe`.
