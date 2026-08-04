# ────────────────────────────────────────────────────────────────────────────────
# Dio – Dockerfile (multi-stage)
# Stage 1: build  – Ubuntu 22.04 + OCaml 5.2.0 + full native build
# Stage 2: runtime – minimal Ubuntu with only shared libs + binaries
# ────────────────────────────────────────────────────────────────────────────────

# ==============================================================================
# STAGE 1 — Builder (Pre-compiled OCaml 5.2 base image for fast cold builds)
# ==============================================================================
FROM ocaml/opam:ubuntu-22.04-ocaml-5.2 AS builder
ENV QEMU_CPU=host

USER root

# 1. System dependencies (build-time C header libraries & tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    m4 \
    g++ \
    make \
    libffi-dev \
    libgmp-dev \
    libpcre3-dev \
    libssl-dev \
    libpq-dev \
    zlib1g-dev \
    autoconf \
    automake \
    libtool \
    && rm -rf /var/lib/apt/lists/*

# 2. Compile libsecp256k1 from source (pinned to v0.7.1, fast parallel build without test suite)
RUN git clone --depth 1 --branch v0.7.1 \
        https://github.com/bitcoin-core/secp256k1.git /tmp/secp256k1 \
    && cd /tmp/secp256k1 \
    && ./autogen.sh \
    && ./configure --enable-module-schnorrsig --enable-module-recovery --disable-tests --disable-benchmark \
    && make -j$(nproc) \
    && make install \
    && ldconfig \
    && rm -rf /tmp/secp256k1

# 3. Setup workdir owned by opam
WORKDIR /app
RUN chown opam:opam /app

USER opam

# 4. Copy project descriptors first (layer-cache friendly)
COPY --chown=opam:opam dio.opam dune-project ./

# 5. Install OCaml dependencies in parallel with BuildKit cache
RUN --mount=type=cache,target=/home/opam/.opam/download-cache,uid=1000,gid=1000 \
    eval $(opam env) && opam install -y -j $(nproc) . --deps-only --with-test --no-depexts

# 6. Copy the rest of the source tree
COPY --chown=opam:opam . .

# 7. Build native executables in parallel with Dune cache
RUN --mount=type=cache,target=/home/opam/.cache/dune,uid=1000,gid=1000 \
    eval $(opam env) && dune build -j $(nproc) --profile=release bin/main.exe bin/dashboard.exe

# ==============================================================================
# STAGE 2 — Runtime (minimal)
# ==============================================================================
FROM ubuntu:22.04 AS runtime

# 9. Runtime shared libraries only (no compilers, no opam, no git)
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

# 10. Copy libsecp256k1 from builder
COPY --from=builder /usr/local/lib/libsecp256k1* /usr/local/lib/
RUN ldconfig

# 11. Copy compiled binaries from builder
COPY --from=builder /app/_build/default/bin/main.exe /usr/local/bin/dio
COPY --from=builder /app/_build/default/bin/dashboard.exe /usr/local/bin/dio-dashboard

# 11a. Copy Lighter signer shared library (Go-compiled .so for linux/amd64)
COPY --from=builder /app/lighter-signer-linux-amd64.so /app/lighter-signer-linux-amd64.so

# 12. Copy config files needed at runtime
COPY --chown=root:root config.json /app/config.json

WORKDIR /app

# 13. Use jemalloc to prevent glibc arena fragmentation in OCaml 5
ENV LD_PRELOAD=libjemalloc.so.2

# 14. jemalloc tuning: fast dirty/muzzy page decay, limited arenas for OCaml 5
ENV MALLOC_CONF="dirty_decay_ms:1000,muzzy_decay_ms:1000,narenas:2"

# 15. OCaml runtime GC defaults (Forces OCaml 5 minor_heap_size scaling per-domain natively)
ENV OCAMLRUNPARAM="s=33554432,o=120,O=1000000,h=100,w=1"

# 15a. Lighter signer library path (linux/amd64 .so in /app)
ENV LIGHTER_SIGNER_LIB_PATH=./lighter-signer-linux-amd64

# 16. Expose metrics broadcast port
EXPOSE 8080

# 17. Default command
CMD ["dio"]