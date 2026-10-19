# Foundation libraries

Everything under `src/foundation/` is built by this repository and owned by dio. The
engine links no OCaml library from opam. The native libraries reached through
bindings in this tree are OpenSSL, libsecp256k1 (built from source in
`Dockerfile.base`), and the prebuilt Lighter Go signer. jemalloc (the runtime
allocator) and the C/C++ runtime remain system libraries. The compiler standard
library (`unix`, `threads`, `bigarray`) and the OxCaml toolchain are the other
external build inputs.

The directories are grouped by capability, not by upstream package. Every
library is part of the single `dio` package and has a `dio.*` public name
(`dio.lwt`, `dio.cohttp`, `dio.yojson`, …), so a dune stanza always shows that
the dependency is ours. Source-level module names remain upstream-compatible
(`Lwt`, `Cohttp`, `Yojson`, …) because application code refers to them; the
`dio.*` namespace is the ownership boundary, not a second module prefix.

## Layout

| Directory | Public name | Kind |
|---|---|---|
| `async/lwt/` | `dio.lwt`, `dio.lwt.unix` | Original promise runtime and Unix integration |
| `net/cohttp/` | `dio.cohttp` | Original HTTP/1.1 core |
| `net/cohttp-lwt/` | `dio.cohttp-lwt` | Original Lwt adapter |
| `net/cohttp-lwt-unix/` | `dio.cohttp-lwt-unix` | Original Unix client, chunked/SSE streaming |
| `net/conduit-lwt-unix/` | `dio.conduit-lwt-unix` | Original client-side TCP/TLS connect |
| `net/websocket/` | `dio.websocket` | Original RFC 6455 client framing |
| `codec/yojson/` | `dio.yojson` | Original JSON parser/printer, `Safe`/`Basic`/`Util` subset |
| `codec/msgpck/` | `dio.msgpck` | Original canonical MessagePack encoder |
| `codec/base64/` | `dio.base64` | Original RFC 4648 encode/decode |
| `codec/uri/` | `dio.uri` | Original absolute URL parser and query builder |
| `codec/uutf/` | `dio.uutf` | Original UTF-8 folder used by the terminal renderer |
| `codec/json_scan/` | `dio.json_scan` | Original streaming JSON span scanner; no DOM allocation |
| `crypto/digestif/` | `dio.digestif`, `dio.digestif.c` | Original SHA-1/256/512, HMAC-SHA-512, Keccak-256 |
| `crypto/secp256k1/` | `dio.secp256k1` | Original OCaml binding over native libsecp256k1 |
| `crypto/ssl/` | `dio.ssl` | Original OCaml binding over OpenSSL |
| `crypto/lwt_ssl/` | `dio.lwt_ssl` | Original Lwt wrapper over the OpenSSL binding |
| `crypto/mirage-crypto-rng/` | `dio.mirage-crypto-rng`, `dio.mirage-crypto-rng.unix` | Original RNG initialisation hook |
| `crypto/lighter_ffi/` | `dio.lighter_ffi` | Original `dlopen` binding to the Lighter Go signer |
| `system/ipaddr/` | `dio.ipaddr`, `dio.ipaddr.unix` | Original IPv4/IPv6 carrier for resolved addresses |
| `system/mtime/` | `dio.mtime`, `dio.mtime.clock.os` | Original monotonic spans and clock |
| `tui/notty/` | `dio.notty`, `dio.notty.unix` | Original terminal image/attribute model and ANSI renderer |
| `test/alcotest/` | private (`alcotest`) | Original minimal test harness matching the suite's API |

`src/foundation/crypto/lighter_ffi` loads `native/lighter-signer-<os>-<arch>` at
runtime. The shared library is prebuilt and is not covered by this
repository's LICENSE; see `licenses/THIRD_PARTY_LICENSES`.

## Ownership rule

- **`src/foundation/`** — first-party support code (`dio.lwt`, `dio.cohttp`, …).
  It must not depend on application libraries (`dio.engine`, `dio.strategies`,
  `dio.<exchange>`) or on `src/engine`, `src/integrations` or `src/dashboard_ui`;
  it is the bottom of the graph.
- **`src/integrations/`** — first-party adapters that talk to external services.
- **`native/`** — prebuilt binaries we do not build from source.
- **`licenses/`** — provenance and third-party notices. A library that needs a
  notice here is not automatically third-party code.

## Notes

- `notty/unix` implements only `winsize`, which is the only `Notty_unix` API the
  dashboard and logging code use. Rendering goes through
  `Notty.Render.to_buffer` and direct stdout writes.
- The promise type is covariant with its element type erased from the mutable
  cell. That keeps `Lwt.return_none` polymorphic despite the value restriction.
- `lwt_ppx` was removed. Its expansion is baked into the 14 files that used
  `let%lwt`/`match%lwt`; the transformation is the one the ppx itself emits.
- The client wrapper in `src/integrations/transport/ws_lwt` is first-party
  code over `dio.websocket`. It owns the RFC 6455 opening handshake, channel
  lifetime and `TCP_NODELAY`; it does not contain upstream ws-lwt code.

## Updating a foundation library

1. Change the source under the relevant `src/foundation/<group>/<library>/` directory.
2. Keep the `dio.*` public name and module names unchanged; application
   `(libraries ...)` stanzas depend on them.
3. Run `dune build @all @runtest` and, for release-sensitive changes,
   `dune build --profile=release bin/main.exe bin/dashboard.exe`.
4. Update the provenance entry in `licenses/THIRD_PARTY_LICENSES` if the native
   dependency or its version changes.
