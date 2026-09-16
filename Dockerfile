# ────────────────────────────────────────────────────────────────────────────────
# Dio – Dockerfile (application image)
#
# Builds the engine on top of a prebuilt OxCaml base image (Dockerfile.base)
# that already contains the OxCaml compiler and every opam dependency. Only the
# engine sources are compiled here, so ordinary code changes build in well under
# a minute; the compiler is never rebuilt.
#
# The base image must exist first:
#   docker build -f Dockerfile.base -t dio-oxcaml-base:5.2.0minus40 .
#
# Then:
#   docker build -t dio .
#
# Stage 1: build  – the base image, plus a native build of the engine
# Stage 2: runtime – minimal Ubuntu with only shared libs + binaries
# ────────────────────────────────────────────────────────────────────────────────

ARG DIO_BASE_IMAGE=dio-oxcaml-base:5.2.0minus40

# ==============================================================================
# STAGE 1 — Build (base image already has the toolchain and dependencies)
# ==============================================================================
FROM ${DIO_BASE_IMAGE} AS builder

USER opam
WORKDIR /app

# 1. Copy the source tree
COPY --chown=opam:opam . .

# 2. Build native executables in parallel with Dune cache
RUN --mount=type=cache,target=/home/opam/.cache/dune,uid=1000,gid=1000 \
    eval $(opam env) && dune build -j $(nproc) --profile=release bin/main.exe bin/dashboard.exe

# 2a. Record the exact installed opam package set (name, version, license) so the
#     released image carries a manifest of every OCaml library it links. Copied
#     into the runtime stage below.
RUN eval $(opam env) \
    && opam list --installed --columns=name,version,license: 2>/dev/null \
       | sort -f > /app/opam-packages.txt

# ==============================================================================
# STAGE 1b — Optional Lighter signer
# ==============================================================================
# The precompiled Go signer (lighter-go) statically bundles go-ethereum,
# gnark-crypto and a Go stdlib; those modules dominate image CVE scans and are
# reported against the published image even though the library is only loaded
# when a Lighter symbol is configured (ctypes dlopen, lazily). It is therefore
# EXCLUDED from the default image. Re-include it for a Lighter deployment with:
#   docker build --build-arg INCLUDE_LIGHTER_SIGNER=1 ...
FROM builder AS lighter_signer
ARG INCLUDE_LIGHTER_SIGNER=0
RUN mkdir -p /app/signer-out \
    && if [ "$INCLUDE_LIGHTER_SIGNER" = "1" ]; then \
         cp /app/lighter-signer-linux-amd64.so /app/signer-out/; \
       fi

# ==============================================================================
# STAGE 2 — Runtime (minimal)
# ==============================================================================
# Pinned ubuntu:24.04. Moving off 22.04 is what clears the CVEs carried by its
# old base packages (glibc 2.35, perl 5.34, tar 1.34, pcre2 10.39, zstd 1.4.8,
# ncurses 6.3, systemd 249, shadow 4.8, gcc-12). 24.04 ships the patched versions
# and keeps a shell + coreutils, which the documented `--entrypoint cp` config
# extraction and deploy.sh rely on (a distroless base would drop those).
FROM ubuntu:24.04@sha256:224a1869083a311ef3f13648a154ba79832fbef6364d31493642ca03082da254 AS runtime

# 3. Runtime shared libraries only. Kept to what the engine actually links:
#    `ldd /usr/local/bin/dio` -> jemalloc, secp256k1, ffi, ssl/crypto, stdc++,
#    gcc_s, m, c. gmp/pcre/pq/zlib are NOT linked and are deliberately omitted
#    here; libpq5 in particular drags in krb5/ldap/gnutls. libssl is named
#    libssl3t64 on 24.04 (the 64-bit time_t rename).
#
#    `apt-get upgrade` applies the security updates the pinned base digest is
#    missing (e.g. glibc 2.39-0ubuntu8.8 -> 8.9, which fixes six CVEs). After
#    this the image has zero *fixable* CVEs; the residual are base-OS packages
#    with no upstream patch (glibc/perl/tar/zlib/passwd/libudev1), present in
#    any glibc image - distroless was measurably worse (Debian glibc carries
#    more unfixed legacy CVEs).
RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       libffi8 \
       libssl3t64 \
       libstdc++6 \
       libgcc-s1 \
       libjemalloc2 \
       ca-certificates \
       netbase \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

# 4. Copy libsecp256k1 from builder
COPY --from=builder /usr/local/lib/libsecp256k1* /usr/local/lib/
RUN ldconfig

# 5. Copy compiled binaries from builder
COPY --from=builder /app/_build/default/bin/main.exe /usr/local/bin/dio
COPY --from=builder /app/_build/default/bin/dashboard.exe /usr/local/bin/dio-dashboard

# 5a. Optional Lighter signer shared library (linux/amd64). The directory is
#     empty unless the image was built with INCLUDE_LIGHTER_SIGNER=1 (see stage
#     1b); the runtime loader only touches it when a Lighter symbol runs.
COPY --from=lighter_signer /app/signer-out/ /opt/lighter-signer/

# 5b. Entrypoint guard. Refuses to start the engine when no config is mounted,
#     instead of failing later inside the config parser.
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

# 5b'. Strategy files. The engine binds each config entry to strategies/<strategy>.json
#      at runtime; without these the domains start but no strategy is loaded and nothing
#      trades. Baked into the image at /app/strategies (the engine's cwd).
COPY --from=builder /app/strategies /app/strategies

# 5c. Third-party license notices. Required by the licenses of the bundled
#     libraries; the opam manifest is generated from the exact build switch.
COPY THIRD_PARTY_LICENSES third_party/ /usr/share/licenses/dio/
COPY --from=builder /app/opam-packages.txt /usr/share/licenses/dio/opam-packages.txt

# 5d. Starter kit. Lets someone with only the image create a working setup
#     without cloning the repository.
COPY config.example.json .env.example compose.yaml /usr/share/doc/dio/

# 6. Setup non-root runtime user (UID/GID 1000, matching the host bind mounts)
#    and directories. Ubuntu 24.04 already ships UID/GID 1000 as `ubuntu`, so
#    reuse it rather than failing; create `dio` only when the slot is free.
#    Run as the numeric UID so the username does not matter.
RUN (getent group 1000 >/dev/null || groupadd -g 1000 dio) \
    && (getent passwd 1000 >/dev/null || useradd -u 1000 -g 1000 -s /bin/false -M dio) \
    && mkdir -p /var/run/dio /app/data \
    && chown -R 1000:1000 /var/run/dio /app

WORKDIR /app

# 7. Use jemalloc to prevent glibc arena fragmentation in OCaml 5
ENV LD_PRELOAD=libjemalloc.so.2

# 8. jemalloc tuning: fast dirty/muzzy page decay, limited arenas for OCaml 5
ENV MALLOC_CONF="dirty_decay_ms:1000,muzzy_decay_ms:1000,narenas:2"

# 9. OCaml runtime GC tuning. Verified against this OxCaml runtime
#    (oxcaml-compiler.5.2.0minus40) with Gc.get ():
#      s = minor_heap_size, in words per domain (OxCaml default 1M words = 8MB)
#      o = space_overhead, major-GC pacing (OxCaml default 80)
#      O = max_overhead, compaction trigger (OxCaml default 500; >=1000000
#          disables compaction)
#    h is not a runtime parameter (silently ignored) and w had no observable
#    effect, so both are omitted. a (allocation_policy) is likewise a no-op in
#    OCaml 5. Sweep s and o with test/engine/perf/sweep_gc_params.sh; do not
#    sweep a.
#    NB: the gc block in config.json is the single source of truth. It is applied
#    in every spawned domain via Gc.set (engine/config.ml apply_gc_config). Do NOT
#    set OCAMLRUNPARAM here: any domain that spawns before/without that call would
#    silently inherit it, and a 256MB minor heap produced multi-ms pauses.

# 9a. Lighter signer library path. The .so is absent from the default image
#     (build with INCLUDE_LIGHTER_SIGNER=1 to include it); the loader warns and
#     only fails if a Lighter symbol is actually signed.
ENV LIGHTER_SIGNER_LIB_PATH=/opt/lighter-signer/lighter-signer-linux-amd64

# 10. Expose metrics broadcast port
EXPOSE 8080

# 11. Run as non-root user (numeric UID 1000; the name is `ubuntu` on 24.04)
USER 1000

# 12. Entrypoint guard + default command
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["dio"]
