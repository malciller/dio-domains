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

# 1a. Mark the toolchain as OxCaml for the build rules. src/external/ws_lwt and
#     src/runtime_compat select their implementation from this flag, because the
#     OxCaml bundle ships Conduit 2 (default_ctx : ctx) and lacks Sys.Safe, while
#     every other toolchain has Conduit >= 3 (default_ctx : ctx Lazy.t) and no
#     Safe module. [flambda] is not a usable discriminator: the default OCaml 5.2
#     switch used by CI is also non-flambda.
ENV DIO_OXCAML=1

# 2. Build native executables in parallel with Dune cache
RUN --mount=type=cache,target=/home/opam/.cache/dune,uid=1000,gid=1000 \
    eval $(opam env) && dune build -j $(nproc) --profile=release bin/main.exe bin/dashboard.exe

# ==============================================================================
# STAGE 2 — Runtime (minimal)
# ==============================================================================
FROM ubuntu:22.04 AS runtime

# 3. Runtime shared libraries only (no compilers, no opam, no git)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libffi8 \
    libgmp10 \
    libpcre3 \
    libssl3 \
    libpq5 \
    zlib1g \
    libjemalloc2 \
    ca-certificates \
    netbase \
    && rm -rf /var/lib/apt/lists/*

# 4. Copy libsecp256k1 from builder
COPY --from=builder /usr/local/lib/libsecp256k1* /usr/local/lib/
RUN ldconfig

# 5. Copy compiled binaries from builder
COPY --from=builder /app/_build/default/bin/main.exe /usr/local/bin/dio
COPY --from=builder /app/_build/default/bin/dashboard.exe /usr/local/bin/dio-dashboard

# 5a. Copy Lighter signer shared library (Go-compiled .so for linux/amd64)
COPY --from=builder /app/lighter-signer-linux-amd64.so /app/lighter-signer-linux-amd64.so

# 6. Setup non-root system user and runtime directories
RUN groupadd -g 1000 dio && useradd -u 1000 -g dio -s /bin/false dio \
    && mkdir -p /var/run/dio /app/data \
    && chown -R dio:dio /var/run/dio /app

WORKDIR /app

# 7. Use jemalloc to prevent glibc arena fragmentation in OCaml 5
ENV LD_PRELOAD=libjemalloc.so.2

# 8. jemalloc tuning: fast dirty/muzzy page decay, limited arenas for OCaml 5
ENV MALLOC_CONF="dirty_decay_ms:1000,muzzy_decay_ms:1000,narenas:2"

# 9. OCaml runtime GC defaults (Forces OCaml 5 minor_heap_size scaling per-domain natively)
ENV OCAMLRUNPARAM="s=33554432,o=120,O=1000000,h=100,w=1"

# 9a. Lighter signer library path (linux/amd64 .so in /app)
ENV LIGHTER_SIGNER_LIB_PATH=./lighter-signer-linux-amd64

# 10. Expose metrics broadcast port
EXPOSE 8080

# 11. Run as non-root user
USER dio

# 12. Default command
CMD ["dio"]
