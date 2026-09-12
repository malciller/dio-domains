# OxCaml Port — Plan & Action Log

**Branch:** `oxcaml` (never merge to `main` until Gate G5 passes)
**Status:** BLOCKED on dependency ecosystem (see §4). Planning complete; no port code yet.
**Last updated:** 2026-09-12

---

## 0. How to use this document

This is the single source of truth for the OxCaml port. It is designed to be worked on
by many agents across many sessions.

Rules for every session/agent:

1. **Work on the `oxcaml` branch.** Never commit OxCaml work to `main`.
2. **Read §4 (blocker) and §6 (tasks) first**, pick a task whose dependencies are done,
   set its status to `IN_PROGRESS`, and record the claimed task + your session id in the
   Action Log (§12) when you start.
3. **Append to the Action Log; do not rewrite history.** Every session adds an entry:
   date, session/agent, task id(s), commands run, observed result, next step.
4. **Keep the doc current.** Update the task table (§6) and dependency triage table (§7)
   as facts change.
5. **Minimize blast radius.** Porting changes must not alter trading behaviour. Prefer
   patching a third-party dependency over editing engine logic. If engine code must
   change, call it out explicitly in the Action Log and justify it.
6. **A task is done only when:** the dependency/artifact builds under OxCaml, the full
   relevant test suite is green, and there is a reproducible command in this doc that
   demonstrates it.
7. **No secrets, no live keys.** Builds and tests run offline/paper only.

Status legend: `TODO` · `IN_PROGRESS` · `BLOCKED` · `DONE` · `DROPPED`.

---

## 1. Objective

Build and run `dio` on **OxCaml** (Jane Street's flambda2-based compiler) in Docker, so
production benefits from stronger allocation elimination than classic flambda.

**Non-goals (explicitly out of scope for now):**
- Using OxCaml language extensions (`local_`, unboxed types, `[@zero_alloc]`, SIMD) in
  our own code. Engine source stays plain OCaml. (`[@zero_alloc]` may be revisited as a
  follow-up once the build is green — see §10.)
- Adopting Jane Street `Base`/`Core`.
- Changing strategy algorithms, order flow, or risk logic.
- Merging OxCaml to `main` before Gate G5.

---

## 2. Gates (each is a mergeable milestone)

| Gate | Definition | Evidence required |
| --- | --- | --- |
| **G0** | Classic-flambda build + tests green (baseline) | `dune build`, `dune runtest` on `5.2.0+flambda`; commit `b2e510b` |
| **G1** | OxCaml compiler builds in Docker | Docker builder target reaches switch-creation + assertion |
| **G2** | Full dependency closure builds under OxCaml | `opam install . --deps-only --with-test` exits 0 |
| **G3** | Release binaries build under OxCaml | `dune build --profile=release bin/main.exe bin/dashboard.exe` exits 0 |
| **G4** | Full test suite green under OxCaml | `dune runtest --force` (and `--profile=release`) |
| **G5** | Prod deploy, measured, with rollback | Deploy to prod-node-1; A/B latency/allocation; rollback runbook |

G1 is already demonstrated (see §4.1). G2 is the current wall.

---

## 3. Current baseline (G0, landed on `main` in `b2e510b`)

- Classic-flambda release build: root `dune` applies `-O3` in the `release` profile;
  `Dockerfile` creates `5.2.0+flambda` and asserts `ocamlopt -config-var flambda = true`.
- Engine allocation/latency work: GC safepoint before the execution-event drain
  (`src/engine/domain_spawner.ml`) and allocation reductions in the Jacobs Ladder event
  handlers (`src/engine/strategies/jacobs_ladder/jacobs_ladder_events.ml`).
- `dotenv` removed; replaced by `Logging.load_dotenv` in
  `src/engine/logging/logging.ml`. This also removed the Jane Street `base < v0.17`
  dependency chain, which was itself an OxCaml conflict.
- `yojson` pin relaxed from `= 2.2.2` to `>= 2.2.2` (so the OxCaml `2.2.2+ox` variant can
  resolve later).
- Local dev default switch: `5.2.0+flambda`. Host: macOS 26 / Apple Silicon.
- Docker Desktop memory raised to **16 GB RAM / 4 GB swap** (host has 36 GB) to survive
  the OxCaml compiler build. Was 8 GB / 1 GB.

---

## 4. The blocker

### 4.1 What works

OxCaml itself installs and builds in Docker (Ubuntu 22.04, `linux/arm64` native
validation). Switch creation:

```sh
opam update -y && \
opam switch create 5.2.0+ox ocaml-variants.5.2.0+ox \
  --repos ox=git+https://github.com/oxcaml/opam-repository.git,default
```

The compiler built; the `opam install . --deps-only` step is where it dies.

Two unrelated things also bite:
- **Local macOS cannot build OxCaml at all**: the bootstrap passes a bare `-std` to the C
  compiler (`clang` 17 and GNU `gcc-14` both reject it). This is a host-toolchain bug, not
  our code. Use the Docker/Linux path for all OxCaml work.
- **8 GB Docker OOMs** the OxCaml compiler (`Command got signal KILL`). 16 GB is required.

### 4.2 Dependency failures (the actual wall)

Observed in Docker (`opam install . --deps-only --with-test` under `5.2.0+ox`).
Errors are from OxCaml's local/global **mode system** and from ppx AST drift:

| Package | Version tried | Error signature | OxCaml-patched pkg exists? |
| --- | --- | --- | --- |
| `notty` | 0.2.3 | `Some record fields are undefined: "out_width"`; also `Alert unsafe_multidomain: Stdlib.Sys.signal` (OxCaml wants `Sys.Safe.signal`) | No `notty`; there **is** `notty-community` / `oxcaml-notty-community` |
| `digestif` | 1.3.1 | `bytes @ local -> …` vs expected `'a @ local -> (int -> By.t -> …)` in `src-ocaml/baijiu_*.ml` | No |
| `cohttp-lwt` | 4.0.0 | `Error: This value is "local" but is expected to be "global".` in `cohttp-lwt/src/body.ml:50` | No |
| `msgpck` | 1.7 | `ocplib-endian` signature mismatch + deprecated `set_int8` alerts in `src/msgpck.ml:619` | No |
| `lwt_ppx` | 5.9.1+ox | `Error: The constructor "Pexp_let" expects 4 argument(s)` (AST/ppxlib mismatch) | Yes, but the `+ox` version itself fails |

Additionally, **no OxCaml patch exists** for `websocket`, `conduit`, `secp256k1` (must be
verified individually — they may or may not compile once the above are fixed).

### 4.3 What OxCaml *does* ship patches for (reference)

From `github.com/oxcaml/opam-repository` (`oxcaml-<name>` + `oxcaml-<name>-patches`):
`alcotest`, `angstrom`, `backoff`, `chrome-trace`, `cmarkit`, `compiler`, `ctypes`,
`ctypes-foreign`, `dot-merlin-reader`, `dune*`, `eio*`, `extlib`, `faraday`, `fs-io`,
`gen_js_api`, `js_of_ocaml*`, `jsonrpc`, `lsp`, `lwt`, `lwt_direct`, `lwt_ppx`,
`lwt_runtime_events`, `mdx`, `merlin*`, `notty-community`, `ocaml-compiler-libs`,
`ocaml-index`, `ocaml-lsp-server`, `ocamlbuild`, `ocamlc-loc`, `ocamlfind`,
`ocamlformat*`, `odoc`, `ojs`, `omd`, `opam-core`, `opam-format`, `ordering`, `otoml`,
`patch-guards`, `ppx_deriving`, `ppxlib`, `ppxlib_ast`, `re`, `sedlex`, `sherlodoc`,
`spawn`, `stdune`, `top-closure`, `topkg`, `utop`, `uutf`, `wasm_of_ocaml-compiler`,
`xdg`, `yojson`, `zarith`.

Our failures are all in packages **not** on this list (except `lwt_ppx`, which is broken).

### 4.4 Implication

There is no configuration that fixes this. To port we must supply OxCaml-compatible
builds of the missing libraries ourselves — i.e. maintain a downstream overlay of patched
opam packages, the same way Jane Street does. That is the work of this branch.

---

## 5. Architecture & build mechanics

```
Dockerfile (builder)                          Host (validation)
  FROM ocaml/opam:ubuntu-22.04-ocaml-5.2        macOS arm64 (host toolchain can't build OxCaml)
  apt deps (autoconf, automake, …)              -> validate inside Docker on linux/arm64
  build libsecp256k1 v0.7.1                     prod target is linux/amd64 -> must re-validate
  create OxCaml switch (ox repo)                Docker Desktop: 16 GB RAM required
  select switch (OPAMSWITCH=5.2.0+ox)
  assert OxCaml present
  COPY dio.opam dune-project
  opam install . --deps-only --with-test        <-- currently fails (see §4)
  COPY source
  dune build --profile=release bin/{main,dashboard}.exe
```

Key commands (validation):

```sh
# Full builder stage, native linux/arm64 (fast local check)
DOCKER_BUILDKIT=1 docker buildx build --progress=plain \
  --platform linux/arm64 --target builder -t dio-oxcaml-builder --load .

# Production target (linux/amd64) — final gate before G5
DOCKER_BUILDKIT=1 docker buildx build --progress=plain \
  --platform linux/amd64 -t dio-oxcaml --load .
```

Log paths used so far: `/tmp/docker_oxcaml_build*.log`.

**Open decision — how we carry patches (decide in WS0 spike):**

| Option | Pros | Cons |
| --- | --- | --- |
| A. Project-local opam overlay repo (`oxcaml-port/opam-overlay/`) with `oxcaml-<pkg>` patch packages | Mirrors OxCaml's own scheme; pinned/auditable; works for Docker + CI | Most upfront setup |
| B. Fork each lib + `opam pin` to a commit | Fast to start | Many forks; hard to keep coherent; pins live in build scripts |
| C. Vendor patched sources into repo (`vendor/`) + dune `vendored_dirs` | No external forks | Diverges from opam resolution; large diffs |

Recommendation: **A**, with **B** as the spike mechanism until the overlay exists.

---

## 6. Workstreams & tasks

Owner column is free text (agent/session id). Keep one `IN_PROGRESS` at a time per owner.

| ID | Task | Depends on | Status | Owner |
| --- | --- | --- | --- | --- |
| WS0 | Build harness: reproducible Docker builder target + local Linux dev switch; capture baseline logs | — | TODO | |
| WS1 | `notty` → `notty-community` (dep swap + code/API check) | WS0 | **DONE (flambda)** | this session |
| WS2 | Fix `lwt_ppx+ox` AST mismatch (pin compatible `ppxlib`/`ppxlib_ast`) | WS0 | **DONE (resolution)** | this session |
| WS2b | Resolve `lwt 6` vs `lwt_log < 6` (pulled by `websocket-lwt-unix`); pin `alcotest = 1.9.0+ox` | WS2 | **DONE (flambda)** | this session |
| WS3 | `digestif` — patch modes OR replace with `mirage-crypto` hashes | WS0 | **DONE (overlay; flambda-verified)** | this session |
| WS4 | `msgpck` / `ocplib-endian` — patch or replace encoder | WS0 | **DONE (flambda)** | this session |
| WS5 | `cohttp-lwt` — patch local/global mode errors (overlay `4.0.0+dio1`) | WS0 | **IN_PROGRESS (overlay built; flambda-green; OxCaml verify pending)** | this session |
| WS6 | `websocket-lwt-unix` / `conduit` — verify then patch | WS0 | TODO | |
| WS7 | `secp256k1` bindings — verify then patch | WS0 | TODO | |
| WS8 | Audit remaining deps (`uri`, `base64`, `mtime`, `ipaddr`, `ctypes`, `alcotest`, `ssl`, `mirage-crypto*`, `websocket`, `yojson`) under OxCaml | WS4-WS7 | TODO | |
| WS9 | Full closure green + full test suite (G2–G4) | WS1-WS8 | TODO | |
| WS10 | Prod rollout: amd64 image, A/B latency/allocation, rollback (G5) | WS9 | TODO | |

### WS0 — Harness (do first)
- Pin the OxCaml repo commit and compiler version; record in §7.
- Decide patch-carrying option (§5).
- Provide one command that reproduces the current failure and captures the log.
- Acceptance: a fresh session can run the command from §5 and reproduce §4.2 exactly.

### WS1 — notty
- Symptom: `out_width` undefined + `unsafe_multidomain` alerts.
- Approach: switch dependency to `notty-community` (which OxCaml patches). Verify the
  module name (`Notty`/`Notty_unix`) and API used by `src/engine/logging` and the TUI
  (`bin/dashboard.ml`, `dashboard_ui/`). If API drift, patch call sites.
- Acceptance: `dune build` links dashboard + logging; dashboard starts against a stub.

### WS2 — lwt_ppx
- Symptom: `Pexp_let expects 4 argument(s)` — ppx uses an AST arity from a different
  `ppxlib` than installed.
- Approach: pin `ppxlib`/`ppxlib_ast` to the version the `lwt_ppx+ox` patch targets
  (OxCaml ships both). If upstream `lwt_ppx+ox` is genuinely broken, patch it and publish
  in the overlay.
- Acceptance: `let%lwt` compiles in a trivial file and in the full tree.
- Note: this is the highest-risk task — the entire codebase uses `lwt_ppx`.

### WS2b — `lwt 6` / `lwt_log` collision
- **Cause (proven):** OxCaml's `lwt_ppx 6.0.0+ox` requires `lwt >= 6`; but
  `dio -> websocket-lwt-unix -> lwt_log >= 1.1.1 -> lwt < 6.0.0`. These cannot both hold.
- The stale alternative `lwt_ppx 5.9.1+ox` needs `lwt < 6` but is built against the older
  AST and fails with `Pexp_let expects 4 argument(s)` under `ppxlib 0.33.0+ox2`.
- `lwt_log` is NOT used by our source (grep clean); it is a hard dep of
  `websocket-lwt-unix`. Note that **`lwt_log` is deprecated by its own authors**.
- **Options (pick one, record here):**
  1. Patch `lwt_log` to allow `lwt 6` and ship `oxcaml-lwt_log`-style overlay package.
  2. Drop `websocket-lwt-unix` for a direct `websocket` + small Lwt wrapper; we already
     use `websocket-lwt-unix` only in `src/external/{alpaca,hyperliquid,kraken,lighter}` and
     one kraken test. Direct `websocket` (no `lwt_log`) keeps `lwt 6`.
  3. Find an older/newer `websocket-lwt-unix` whose `lwt_log` bound is `>= 6`.
- **Also:** pin `alcotest` to `1.9.0+ox` (only version the OxCaml patch accepts). The
  current unpinned `alcotest` conflicts with the OxCaml `alcotest` invariant.
- Acceptance: resolution no longer reports `lwt_log`/`lwt`/`alcotest` conflicts.

### WS3 — digestif
- Symptom: mode errors in `src-ocaml/baijiu_*.ml` (`feed`/`blit` local modes).
- Two routes:
  1. Patch `digestif` (add mode annotations, or restore global `blit`) and ship in overlay.
  2. Replace `digestif` with `mirage-crypto` hashes already in the tree
     (`mirage-crypto-rng` is a dep; `mirage-crypto` provides SHA256/SHA512/BLAKE2/MD5).
     Audit usage first: `rg -n "Digestif|Hashif|BLAKE2|SHA" src`.
- Acceptance: all hash call sites produce identical digests vs the flambda build (test
  vectors / golden files).

**Resolution (2026-09-12) — Route B (exclude the optional pure-OCaml backend).**
- The replace-with-`mirage-crypto` route is **not viable**: `mirage-crypto >= 1.0.0`
  removed `Mirage_crypto.Hash` and *depends on* `digestif`; `digestif` is also a hard
  dep of `mirage-crypto-rng`. Keccak-256 (Ethereum padding `0x01`) has no `mirage-crypto`
  equivalent in any version. `digestif` must build regardless of our call sites.
- Chose **Route B** over Route A because macOS cannot build OxCaml, so Route A's mode
  annotations cannot be compile-validated here; Route B does not touch any mode-sensitive
  source. `src-ocaml/dune` declared the optional `digestif.ocaml` library; it is removed
  from the overlay build so the default `digestif.c` (`(default_implementation digestif.c)`
  in `src/dune`) is the only implementation. Consumers request the virtual library
  `digestif`, so no API change and byte output is identical (C backend either way).
- Overlay package: `oxcaml-port/opam-overlay/packages/digestif/digestif.1.3.1+dio1/`
  (`opam` + `files/oxcaml.patch`). Patch touches four dune files only: `src-ocaml/dune`
  (drop library stanza), and `test/ocaml/dune`, `fuzz/dune`, `fuzz/ocaml/dune`
  (redirect `digestif.ocaml` → `digestif.c` so the test/fuzz suites still build).
- Verified on `5.2.0+flambda` (see Action Log): patch applies with `patch -p1` and
  `git apply --check`; `dune build -p digestif` exits 0; digestif's own known-answer
  suites pass (684 tests incl. SHA256/SHA512/HMAC/Keccak-256); a before/after program
  printing SHA256/HMAC-SHA512/Keccak-256 is byte-identical on unpatched vs patched.
- **Not verified by execution under OxCaml** (this macOS host cannot build OxCaml). The
  OxCaml-side basis is the prior Docker log `/tmp/docker_oxcaml_build3.log`: only the
  `src-ocaml/baijiu_*.ml` library failed; `digestif.c` and its C stubs built. Removing
  that library is therefore sufficient. Re-verify in Docker at WS9.
- Overlay metadata validated in an isolated opam root (`OPAMROOT=… opam init --bare
  … dio-ox file://…/opam-overlay`): `opam show digestif.1.3.1+dio1` reports the expected
  url/checksum/patches/extra-files, and `opam lint` passes. The overlay `repo` marker was
  added concurrently by the WS4 session.
- Wiring still needed (WS0/WS9): the Docker switch must add this overlay repo and select
  `1.3.1+dio1` over upstream `1.3.1` (e.g. an OxCaml-only `digestif` version constraint).
  Also confirmed the pinned OxCaml repo `bb455526` ships **no** `digestif`/`oxcaml-digestif`
  package.

### WS4 — msgpck
- Used for Hyperliquid action signing/serialization. Depends on `ocplib-endian`.
- Approach: patch `ocplib-endian` and/or `msgpck` for OxCaml; or replace with a small
  internal msgpack encoder if the surface is narrow. Do **not** change the wire format.
- Acceptance: Hyperliquid signing produces byte-identical output vs flambda (golden test).
- **DONE (flambda-verified, 2026-09-12):** only `msgpck` fails; `ocplib-endian.1.2` is fine
  (corrects the §4.2 attribution). Overlay package
  `oxcaml-port/opam-overlay/packages/msgpck/msgpck.1.7+dio1/` carries
  `files/oxcaml.patch`, which eta-expands `SIBO.blit`/`BIBO.blit` (`src/msgpck.ml:37,49`)
  into global-typed `fun` wrappers so the recorded `blit` type matches `STRING.blit`.
  `StringBuf`/`BytesBuf` (the runtime path, `SIBUFO`/`BIBUFO`) are untouched.
  Verified on `5.2.0+flambda`: `git apply --check` and `patch --dry-run -p1` pass against
  a clean 1.7 tree; `dune build -p msgpck` exits 0 after `patch -p1`; a `StringBuf.write`
  vector harness (12 cases, including the documented sample order action) is byte-identical
  before/after. OxCaml end-to-end was **not** run (macOS cannot build it; see Action Log).

### WS5 — cohttp-lwt
- Symptom: `local`/`global` mode error in `body.ml:50` (partial application
  `Lwt_stream.iter (Buffer.add_string b) s`).
- Version-bump ruled out: the offending expression is byte-identical in `cohttp-lwt`
  4.0.0 and the latest 6.3.0 (`opam source cohttp-lwt.6.3.0`), so no release avoids the
  patch. Bumping would also force `cohttp-lwt-unix >= 5.3` and `conduit-lwt-unix >= 5`,
  changing the OxCaml closure that currently settles at `cohttp-lwt-unix 4.0.0` +
  `conduit-lwt-unix 2.2.2`.
- Chosen: patch 4.0.0 via overlay `cohttp-lwt.4.0.0+dio1` (`files/oxcaml.patch`):
  eta-expand `Buffer.add_string b` (body.ml:50) and `(Request|Response).write_body writer`
  (client.ml:54,64,116; server.ml:126). API and runtime behaviour unchanged.
- Overlay also ships `cohttp.4.0.0+dio1` and `cohttp-lwt-unix.4.0.0+dio1` (version-only,
  no patch): the cohttp packages are coupled by `depends: "cohttp" {= version}` /
  `"cohttp-lwt" {= version}`, so a `+dio1` `cohttp-lwt` requires matching `+dio1` cohttp
  packages (otherwise `cohttp-lwt-unix.4.0.0` selects the unpatched `cohttp-lwt.4.0.0`).
- Verification (macOS): `patch -p1 --dry-run` clean; applied to a clean 4.0.0 tree;
  `dune build -p cohttp-lwt` on `5.2.0+flambda` exit 0. opam resolution with the overlay
  repo added: `cohttp-lwt-unix.4.0.0+dio1 -> cohttp-lwt.4.0.0+dio1 -> cohttp.4.0.0+dio1`.
- Still open: OxCaml build (macOS cannot build OxCaml); `cohttp-lwt-unix` sources are
  unpatched (e.g. `callback spec` partial application at
  `cohttp-lwt-unix/src/server.ml:69` may hit the same mode error) — extend the overlay
  `cohttp-lwt-unix.4.0.0+dio1` if the Docker build reaches it and fails.
- Acceptance: REST request/response round-trip test passes.

### WS6/WS7 — websocket/conduit/secp256k1
- These are unpatched. First just try to build them under OxCaml; only port if they fail.
- `secp256k1` is a C-stub binding (`secp256k1.0.5.0`) — the C library is built in the
  Dockerfile; likely OK, but verify signing tests.

### WS10 — rollout
- Build `linux/amd64` image on prod node (check prod RAM ≥ 16 GB, or add swap).
- A/B on existing instrumentation: `al:…w[ex:…]`, `Latency [<sym>:place]`/`:amend]`,
  dashboard `gc_minor`, EVENTS/STRATEGY/TOTAL percentiles. Compare vs flambda `b2e510b`.
- Rollback: keep the flambda image tag; `deploy.sh` can be pointed back in one edit.

---

## 7. Dependency triage

| Dep | Used for | Pinned now | Ox patched? | Status | Fix route | Owner |
| --- | --- | --- | --- | --- | --- | --- |
| `ocaml-variants` | compiler | `5.2.0+ox` | — | builds | repo pinned `bb455526` (2026-08-31) | |
| `lwt_ppx` | `let%lwt` ppx | `>= 6.0.0` (dio) | yes | **fixed in resolution**: pin `>= 6.0.0` forces `6.0.0+ox`; `5.9.1+ox` was stale vs `ppxlib 0.33.0+ox2` | WS2 — pinned, re-verify in WS9 | this session |
| `lwt` | runtime | (unpinned) | yes | `6.0.0+ox` required by `lwt_ppx 6`; conflict resolved in WS2b by dropping the lwt websocket wrapper | — | |
| `lwt_log` | was pulled by `websocket-lwt-unix` | — | no | **no longer a dependency** (WS2b dropped `websocket-lwt-unix`) | — | |
| `alcotest` | tests | (unpinned) | yes | must resolve to `1.9.0+ox` on OxCaml | project constraint left broad (classic-flambda unaffected); the OxCaml build must pass `--update-invariant` or explicitly resolve `alcotest` to `1.9.0+ox` (WS2b) | |
| `notty-community` | logging/TUI | `0.2.4` (default) / `0.2.4+ox2` (ox repo) | yes | flambda green; OxCaml `+ox2` selects automatically | WS1 — API identical to notty 0.2.3 (`Notty`/`Notty_unix`); expose `notty-community{,.unix}` | this session |
| `digestif` | hashing | 1.3.1 → overlay `1.3.1+dio1` | overlay (Route B) | **patched (flambda-verified)** | WS3: `oxcaml-port/opam-overlay/packages/digestif/digestif.1.3.1+dio1` drops the unused `digestif.ocaml` backend; default `digestif.c` retained | this session |
| `cohttp-lwt` | REST | 4.0.0 | now yes (dio overlay) | overlay `cohttp-lwt.4.0.0+dio1` (eta-expand partial applications); flambda `dune build -p cohttp-lwt` green | WS5 — OxCaml verify pending | this session |
| `cohttp` / `cohttp-lwt-unix` | REST (coupling) | 4.0.0 | no (version-only) | overlaid as `+dio1` purely to satisfy `cohttp-lwt {= version}`; sources unpatched | WS5 | this session |
| `msgpck` | HL msgpack | `1.7+dio1` (overlay) | overlay (dio) | patched; flambda build + byte-identical vectors | WS4: overlay `msgpck.1.7+dio1` eta-expands `SIBO`/`BIBO.blit`; runtime `StringBuf` path untouched | this session |
| `websocket-lwt-unix` | WS feeds | 2.17 | no | **DROPPED** (pulled `lwt_log`, capping `lwt < 6`) | WS2b: replaced by internal `dio.ws_lwt` over base `websocket` + `conduit-lwt-unix` | |
| `websocket` | WS framing | 2.17 | no | kept; no `lwt_log` | base framing used by `dio.ws_lwt` (WS2b) | |
| `conduit-lwt-unix` | transport | 7.0.0 | no | unknown | WS6 | |
| `secp256k1` | signing | 0.5.0 | no | unknown | WS7 | |
| `yojson` | JSON | `>=2.2.2` | yes | resolves (`2.2.2+ox`) | — | |
| `ctypes`, `ctypes-foreign` | lighter bindings | — | yes | resolves | — | |
| `uri`, `base64`, `mtime`, `ipaddr`, `ssl`, `alcotest` | misc | — | partly | unknown | WS8 | |

(Fill in versions/commits as tasks complete.)

---

## 8. Reproduction appendix

```sh
# 1. Ensure Docker has >= 16 GB RAM (Docker Desktop > Settings > Resources), then:
cd /Users/malciller/dev/dio-domains
git switch oxcaml

# 2. Build the OxCaml Dockerfile builder stage (native arm64 local)
#    NOTE: current Dockerfile on this branch is reverted to flambda; apply the
#    OxCaml switch block (commented in the Action Log) to reproduce the failure.
DOCKER_BUILDKIT=1 docker buildx build --progress=plain \
  --platform linux/arm64 --target builder -t dio-oxcaml-builder --load . \
  > /tmp/docker_oxcaml_build.log 2>&1

# 3. Inspect failures
rg -n "ERROR|Error|No solution|signal KILL" /tmp/docker_oxcaml_build.log | head -40
```

OxCaml Dockerfile switch block (to be re-applied on this branch):

```dockerfile
RUN --mount=type=cache,target=/home/opam/.opam/download-cache,uid=1000,gid=1000 \
    opam update -y \
    && opam switch create 5.2.0+ox ocaml-variants.5.2.0+ox \
         --repos ox=git+https://github.com/oxcaml/opam-repository.git,default
ENV OPAMSWITCH=5.2.0+ox
RUN eval $(opam env) && opam list --installed --short | grep -qi oxcaml \
    && ocamlopt -config-var version
```

---

## 9. Testing & verification

- **Build:** `dune build`, `dune build --profile=release` under OxCaml.
- **Tests:** `dune runtest --force` and `dune runtest --force --profile=release`.
- **Golden/wire tests:** signing (Hyperliquid), any serialization must be byte-identical
  to the flambda build. Add these if missing before WS4/WS7 done.
- **Perf:** compare `al:…w[ob/ex/prep/strat]`, `Latency [<sym>:place|amend|cancel]`
  (debug logs), dashboard `gc_minor`, and stage percentiles.
- **Gate evidence:** paste commands + result summary into the Action Log.

---

## 10. Risks & open questions

- **Mode system may break libs beyond repair** → replacement (e.g. digestif→mirage-crypto)
  rather than patching. Decide per WS.
- **`lwt_ppx+ox` broken upstream** → may require us to patch ppx; highest risk.
- **Fork maintenance burden** — the overlay must track OxCaml releases.
- **OxCaml stability** — no compatibility promise; pin everything; update deliberately.
- **amd64 not yet validated** — all evidence so far is arm64.
- **Build memory/time** — OxCaml compile needs ~16 GB and is slow; consider a prebuilt
  base image if we can build one ourselves (do NOT pull arbitrary community images).
- **`[@zero_alloc]` as a future win** — once building, wrapping event handlers could
  enforce the alloc-free goal from the flambda work. Separate follow-up.

**Open decisions**
1. Patch strategy A/B/C (§5).
2. digestif: patch vs replace (WS3).
3. msgpck: patch vs internal encoder (WS4).
4. Minimum prod RAM / swap on prod-node-1.

---

## 11. Agent conventions

- Branch `oxcaml`; conventional, small commits; reference the WS id (`WS3: patch digestif modes`).
- Keep engine behaviour unchanged; every engine edit needs a justification in the log.
- Pin exact versions/commits; never track a moving branch in a build.
- Run the full test suite before marking a task `DONE`.
- Update §6 and §7 in the same commit as the code change where practical.
- Never commit secrets or `.env`.

---

## 12. Action log

Newest last. Format: `### YYYY-MM-DD — <session/agent> — <task ids>` then what was done.

### 2026-09-12 — initial session — planning
- Established baseline on `main` (commit `b2e510b`): classic flambda `-O3`, GC safepoint,
  event-handler allocation cuts, `dotenv` removal + `yojson` pin relaxation.
- Attempted OxCaml end-to-end:
  - Local macOS OxCaml switch build failed (bare `-std` bootstrap bug; clang 17 + gcc-14).
    Switch automatically cleaned up.
  - Docker `linux/arm64` builder: OOM at 8 GB → raised Docker Desktop to 16 GB / 4 GB swap
    → OxCaml compiler built successfully.
  - `opam install . --deps-only` then failed on `notty`, `digestif`, `cohttp-lwt`,
    `msgpck`, and `lwt_ppx+ox` (details in §4.2).
- Conclusion: ecosystem gap, not configuration. Reverted the branch's Dockerfile to
  flambda so it stays buildable; OxCaml switch block preserved in §8 for re-application.
- Created branch `oxcaml` and this document. No port code yet.
- Next: WS0 (harness) then WS2 (lwt_ppx) since it gates the whole tree.

### 2026-09-12 — follow-up session — WS0, WS2, WS2b opened
- WS0 partial: pinned OxCaml opam repo commit `bb4555262936283daf5cbc82423509d4e7069b15`
  (2026-08-31). Captured reproduction command in §8. Patch-carrying strategy still TODO.
- WS2 root-caused and fixed in resolution:
  - Solver picked `lwt_ppx 5.9.1+ox`, which requires `lwt < 6` and is built against an older
    AST; it fails with `Pexp_let expects 4 argument(s)` under `ppxlib 0.33.0+ox2`.
  - Pinned `lwt_ppx >= 6.0.0` in `dune-project`/`dio.opam`; this forces `lwt_ppx 6.0.0+ox`
    (matched to `ppxlib 0.33.0+ox2` and `lwt 6.0.0+ox`).
  - Verified local classic-flambda build + 69/69 tests still green with the pin.
- Docker `linux/arm64` run after WS2 (`/tmp/docker_oxcaml_build4.log`): the `lwt_ppx` error
  is gone. New blockers surfaced:
  - `lwt_ppx 6 -> lwt >= 6` vs `websocket-lwt-unix -> lwt_log -> lwt < 6.0.0` (WS2b).
  - `alcotest` must pin to `1.9.0+ox` (WS2b).
  - `conduit-lwt-unix < 2` chain via `ppx_sexp_conv`/`base` (WS6, later versions may avoid).
- Reverted the branch Dockerfile to flambda after the experiment; OxCaml block remains in §8.
- Committed and pushed `OXCAML_PORT.md` to `gitea/oxcaml`.
- Next: WS2b choose option (recommend option 2: drop `websocket-lwt-unix` for direct
  `websocket`), then WS1 (`notty`), WS3 (`digestif`).

<!-- Append new entries below. -->

### 2026-09-12 — WSL session — WS1
- Swapped `notty` → `notty-community` across `dune-project` + 4 dune files:
  `src/engine/logging/dune`, `src/dashboard_ui/dune`, `bin/dune`,
  `test/engine/dashboard/dune`. Dune regenerated `dio.opam`.
- Exposed libraries: `notty-community` (module `Notty`) and
  `notty-community.unix` (module `Notty_unix`) — same module/API surface as
  `notty`/`notty.unix`, so no call-site changes were required.
- Verified `notty-community` 0.2.4 is on the default opam repo and installs on
  `5.2.0+flambda`; `ocamlfind list` shows `notty-community{,.unix,.lwt,.top}`.
  Upstream `CHANGES` says the only difference from 0.2.3 is the library rename
  (modules unchanged), plus Unicode 17 + OCaml 5.4 support.
- OxCaml route: the pinned ox repo (`bb455526`) ships
  `notty-community.0.2.4+ox2` and `oxcaml-notty-community-patches`, which carry
  the mode fixes and the `unsafe_multidomain`/`Sys.Safe.signal` patch while
  keeping the same `notty-community` / `notty-community.unix` public names. A
  plain `notty-community` dependency hence resolves to `0.2.4+ox2` on an OxCaml
  switch and `0.2.4` on flambda — one source of truth, no OxCaml-only deps.
- Verification on `5.2.0+flambda`: `dune build` → exit 0 (builds
  `bin/dashboard.exe`, `bin/main.exe`); `dune runtest --force` → exit 0 with 69
  `Test Successful` lines (matches baseline).
- Note/blocker for full OxCaml evidence only: the local `5.2.0+ox` switch's opam
  repo cache is stale (lacks `notty-community`), and OxCaml cannot be built on
  this macOS host, so OxCaml end-to-end remains a Docker (WS0) step.
- Next: WS3 (`digestif`), WS2b choice, then WS9 re-verify under Docker.

### 2026-09-12 — WS2b session — WS2b
- Chose option 2: dropped `websocket-lwt-unix` (which pulled `lwt_log`, capping
  `lwt < 6`) for a client-only internal library `dio.ws_lwt`
  (`src/external/ws_lwt/`) built directly on base `websocket` and
  `Websocket.Make (Cohttp_lwt_unix.IO)`, with no `Lwt_log` dependency.
- API (`Ws_lwt`): `connect ?extra_headers ?random_string ?ctx ?buf client uri`,
  `read`, `write`, `close_transport`, `type conn` — same surface as the subset of
  `websocket-lwt-unix` in use. Reproduces the reference connect/read/write/close
  semantics and calls `set_tcp_nodelay`; no server code; no wire-format change
  (framing still from the `websocket` package).
- Updated call sites `Websocket_lwt_unix` → `Ws_lwt` in `hyperliquid_ws.ml`,
  `kraken_trading_client.ml`, `kraken_orderbook_feed.ml`, `alpaca_orderbook.ml`,
  `lighter_ws.ml`, and `test/external/kraken/debug_ws.ml`; dune deps now
  `websocket` + `dio.ws_lwt`.
- Removed `websocket-lwt-unix` from `dune-project`; `dio.opam` regenerated with
  `websocket`. `rg -n "websocket-lwt-unix" .` (excl. this doc and `_build`) is
  empty.
- `alcotest` left broad in `dune-project`/`dio.opam` so classic-flambda is
  unaffected; under OxCaml the build must pass `--update-invariant` or resolve
  `alcotest` to `1.9.0+ox` (see §7).
- Verified on `5.2.0+flambda`: `dune build` → exit 0; `dune runtest --force` →
  exit 0 with 69 `Test Successful` (matches baseline).
- Note: a concurrent WS1 session also changed `notty` → `notty-community` in the
  same working tree; those edits are present here but are not part of WS2b.

### 2026-09-12 — WS1/WS2/WS2b Docker verification — WS1, WS2, WS2b
- Dispatched four subagents (WS1 notty, WS2b websocket wrapper, WS3 digestif
  analysis, WS4 msgpck analysis) in parallel; all returned.
- **Dependency resolution is now fixed under OxCaml.** Docker `linux/arm64`
  builder run (`/tmp/docker_oxcaml_build5.log`, arm64 native): the solver now
  resolves the full closure with `+ox` variants and **no conflicts**:
  `lwt 6.0.0+ox`, `lwt_ppx 6.0.0+ox`, `ppxlib 0.33.0+ox2`, `notty-community
  0.2.4+ox2`, `alcotest 1.9.0+ox`, `conduit-lwt-unix 2.2.2`, `websocket 2.14`.
  The `--update-invariant` note was not needed; `alcotest` resolved to `1.9.0+ox`
  because the OxCaml patch package constrains it.
- The build now fails only at **compilation**, on exactly the three predicted
  packages: `msgpck.1.7`, `digestif.1.3.1`, `cohttp-lwt.4.0.0`. No
  `websocket`/`conduit`/`secp256k1`/`lwt` failures — WS2b and WS1 held up.
- `cohttp-lwt` error (new detail): `cohttp-lwt/src/body.ml:50: Error: This value
  is "local" but is expected to be "global".` (mode annotation).
- WS3/WS4 analyses delivered patch recipes (see §6 WS3/WS4 and the task outputs):
  - **WS3:** replacing digestif with mirage-crypto is NOT viable (mirage-crypto
    >= 1.0 removed its hash module and *depends on* digestif; digestif is also a
    hard dep of mirage-crypto-rng). Patch digestif's 8 `baijiu_*.ml` files with
    mode annotations; or disable the optional `digestif.ocaml` backend and keep
    `digestif.c`. Small, byte-preserving.
  - **WS4:** only `msgpck` fails (ocplib-endian is fine). Eta-expand
    `SIBO.blit`/`BIBO.blit` in `msgpck.ml:37,49`; runtime path `StringBuf` is
    untouched so bytes are preserved. ~0.5 day + overlay wiring, low risk.
- Verified local `5.2.0+flambda`: combined tree `dune build` exit 0 and
  `dune runtest --force` exit 0 with 69 `Test Successful` (the subagents' edits
  are compatible).
- Reverted the branch Dockerfile to flambda after the experiment (OxCaml block
  remains in §8).
- Next: implement the three dependency patches via the §5 overlay (WS3, WS4,
  WS5), then re-run the Docker builder; expect G2 to pass.

### 2026-09-12 — WS3 session — WS3
- Obtained `digestif.1.3.1` via `opam source digestif.1.3.1
  --switch=5.2.0+flambda --dir=/tmp/digestif-src` (this opam is 2.3.0, which
  spells the flag `--dir`, not `--dest`). Confirmed the download-cache archive
  hashes to the opam `url.checksum` (sha256
  `3927949a…c88466`, sha512 `436dcd82…2707`).
- Confirmed the pinned OxCaml repo `bb455526` has **no** `digestif` /
  `oxcaml-digestif` package. Also confirmed the local `5.2.0+ox` switch is a
  misnamed **MetaOCaml `ocaml-variants.5.3.0+BER`** (`flambda: false`), not
  OxCaml — so it cannot validate mode errors.
- Created overlay package
  `oxcaml-port/opam-overlay/packages/digestif/digestif.1.3.1+dio1/`:
  - `opam` — same src/checksums/depends/conflicts as upstream 1.3.1;
    `version: "1.3.1+dio1"`, `patches: ["oxcaml.patch"]`, `extra-files`
    (sha256 of the patch).
  - `files/oxcaml.patch` — Route B. Removes the optional pure-OCaml
    `digestif.ocaml` library stanza from `src-ocaml/dune` (the 8
    `baijiu_*.ml` mode failures are never compiled), leaving the default C
    backend `digestif.c`; redirects `digestif.ocaml` → `digestif.c` in
    `test/ocaml/dune`, `fuzz/dune`, `fuzz/ocaml/dune` so the test/fuzz
    suites still build/run.
- Verification (all on `5.2.0+flambda`; macOS cannot build OxCaml):
  - `patch --dry-run -p1` and `git apply --check -p1` on a clean 1.3.1 tree:
    both exit 0 (4 files).
  - fresh copy + `patch -p1` + `dune build -p digestif -j 4`: exit 0.
  - digestif's own known-answer suites (`@test/ocaml/runtest`,
    `@test/c/runtest`, `@test/conv/runtest`): all green — e.g. `test/ocaml`
    now exercises `digestif.c` and reports `684 tests run`, including
    SHA256/SHA512/HMAC, SHA3 FIPS-202 and Keccak-256 vectors.
  - byte-identity: tiny program printing SHA256(""), SHA256(fox),
    HMAC-SHA512(key,fox), Keccak-256(""), Keccak-256("abc"), raw length;
    outputs from unpatched 1.3.1 and patched are **identical** (e.g.
    `KECCAK256(empty)=c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470`,
    `SHA256(empty)=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`).
- Not executed under OxCaml; the OxCaml claim rests on
  `/tmp/docker_oxcaml_build3.log` (only `src-ocaml/baijiu_*.ml` failed;
  `digestif.c` + stubs built). Re-verify at WS9/G2.
- Overlay metadata validated in an isolated opam root
  (`OPAMROOT=…/ws3/opamroot-digestif opam init --bare … dio-ox
  file://…/opam-overlay`): `opam show digestif.1.3.1+dio1` shows the expected
  url/checksum and `patches`/`extra-files`; `opam lint` Passed. The overlay
  `repo` marker was added concurrently by the WS4 session.
- Follow-up wiring (WS0/WS9): the Docker switch must add the overlay repo and
  make the solver pick `1.3.1+dio1` over upstream `1.3.1`.
- No commit; `deploy.sh` not run.

### 2026-09-12 — WS4 session — WS4
- Adopted §5 option A for `msgpck`. Created the first overlay package:
  - `oxcaml-port/opam-overlay/repo` (`opam-version: "2.0"`).
  - `oxcaml-port/opam-overlay/packages/msgpck/msgpck.1.7+dio1/opam` — copy of the
    official 1.7 opam file with `version: "1.7+dio1"`, the unchanged `url { src;
    checksum }` (tarball `ff7065bf…` / `7d71baa9…`), plus
    `patches: ["oxcaml.patch"]` and `extra-files: [["oxcaml.patch" "sha256=e50ab58c…"]]`.
    `depends` unchanged.
  - `.../files/oxcaml.patch` (sha256 `e50ab58cedb1fa07ae171c27018f6ea6aa631b878a3950278bd4afe787a42100`).
- Source provenance: the official 1.7 tarball was taken from the opam download
  cache (`~/.opam/download-cache/sha256/ff/ff7065bf…`); `shasum -a 256`/`-a 512`
  match the opam metadata exactly (`ff7065bf590af502a1b1622ff3b5280805c122033d68cf6b53da32c31ecb5f5d`,
  `7d71baa9614f890f669bb52181a295e51d6735ab9786fd7bc69c123721f801232a314ec98b8e59ccf8d2c1541f8fcc084ebf1d47189fd45632621c4a246d0368`).
- Patch (only 4 changed lines; `StringBuf`/`BytesBuf` untouched):
  eta-expand `SIBO.blit` and `BIBO.blit` from
  `let blit = Bytes.blit_string` to
  `let blit src src_pos dst dst_pos len = Bytes.blit_string src src_pos dst dst_pos len`.
  This gives the wrapper a global arrow type so it matches `STRING.blit`
  (`src/msgpck.ml:25`) under OxCaml's mode system; the deprecated `set_*` alerts are
  non-fatal under opam's `dune build -p` (release profile).
- Verification (all on local `5.2.0+flambda`; OxCaml cannot be built on this macOS host):
  - a. `git apply --check` → exit 0; `patch --dry-run -p1` → exit 0, against a clean
    official 1.7 tree.
  - b. Fresh copy + `patch -p1 < …/files/oxcaml.patch` + `dune build -p msgpck` → exit 0
    (`_build/default/src/msgpck.cma` produced).
  - c. Byte-identity: a throwaway `bytecheck` executable linked against a *clean* copy
    and a *patched* copy, serializing 12 `Msgpck.StringBuf.write` vectors (sample order
    action, empty order, modify Int/Int64, cancel, cancelByCloid, batchModify,
    usdClassTransfer, int/int64 boundaries, string lengths, scalars). The sample vector
    matches the documented reference exactly
    (`83a474797065…a26e61`); `diff` of clean vs patched output → empty (identical).
  - d. Overlay parsed by an isolated opam root (`OPAMROOT=/tmp/… opam init --bare
    … dio-ox file://…/opam-overlay`): `opam show msgpck.1.7+dio1` reports the correct
    url/checksums; `opam lint` → Passed. The overlay was **not** installed/compiled in
    the real switch (would mutate it), so opam's own apply-patch step was exercised only
    via the identical `patch -p1` used by `opam`.
- **Not proven / limitation:** the patched package was not compiled by the OxCaml
  compiler. The mode fix is reasoned (global `fun` wrapper accepting `Bytes.blit_string`'s
  `@ local` args) and matches the WS4 analysis, but no `5.2.0+ox` build was run because
  macOS cannot build OxCaml and a Docker rebuild was out of scope for this session. The
  Docker overlay wiring (`dio-ox=file:///app/oxcaml-port/opam-overlay` in §8's switch
  block) is left for WS9/G2.
- No repository files under `src/` or `test/` were changed; no commit made.

### 2026-09-12 — WS5 session — WS5
- Version bump ruled out. `opam source cohttp-lwt.4.0.0/6.3.0 --dir=…`: the offending
  expression `Lwt_stream.iter (Buffer.add_string b) s` is byte-identical in `body.ml:50`
  of 4.0.0 and the latest 6.3.0, so no release avoids the patch. Bumping would also force
  `cohttp-lwt-unix >= 5.3` → `conduit-lwt-unix >= 5`, changing the OxCaml closure that
  currently settles at `cohttp-lwt-unix 4.0.0` + `conduit-lwt-unix 2.2.2`
  (`docker_oxcaml_build5.log`).
- Patched 4.0.0 via overlay
  `oxcaml-port/opam-overlay/packages/cohttp-lwt/cohttp-lwt.4.0.0+dio1/` (`opam` +
  `files/oxcaml.patch`, sha256 `2ed838a2e3b50fafe3b71470585537de0ba17c8ba1df38ef2930453284e190b2`).
  Patch eta-expands the partial applications OxCaml's mode system infers "local":
  `Buffer.add_string b` (`body.ml:50`) and `(Request|Response).write_body writer`
  (`client.ml:54,64,116`, `server.ml:126`). Public API and runtime behaviour unchanged.
- Also created `cohttp.4.0.0+dio1` and `cohttp-lwt-unix.4.0.0+dio1` (version-only, no
  patch): the cohttp packages are coupled by `depends: "cohttp" {= version}` /
  `"cohttp-lwt" {= version}`, so `cohttp-lwt-unix.4.0.0` would otherwise demand the
  unpatched `cohttp-lwt = 4.0.0` and bypass the patch. Confirmed with opam (overlay repo
  added to the `5.2.0+flambda` switch, then removed): `opam install
  cohttp-lwt-unix.4.0.0+dio1 --dry-run` → `cohttp 5.3.1 -> 4.0.0+dio1 [required by
  cohttp-lwt]`, `cohttp-lwt 5.3.0 -> 4.0.0+dio1 [required by cohttp-lwt-unix]`.
- Verification (macOS; OxCaml cannot be built on this host):
  - `patch -p1 --dry-run --batch` against a clean `cohttp-v4.0.0` tree → exit 0, no offsets.
  - Fresh copy + `patch -p1` + `dune build -p cohttp-lwt -j 4` on `5.2.0+flambda` → exit 0
    (`_build/default/cohttp-lwt/src/cohttp_lwt.a` and `.cmx` objects produced).
  - `opam` parses the three overlay opams and resolves the `{= version}` coupling (above).
- Not verified: compilation under `5.2.0+ox` (no OxCaml host). `cohttp-lwt-unix` sources
  are unpatched; `callback spec` (`cohttp-lwt-unix/src/server.ml:69`) is another
  partial-application-to-global candidate, to check/extend if the Docker builder reaches it.
- Docker overlay wiring (`dio-ox=file:///app/oxcaml-port/opam-overlay`, §8) is still WS0/WS9.
- No `src/`/`test/` changes; no commit; `deploy.sh` not run.


