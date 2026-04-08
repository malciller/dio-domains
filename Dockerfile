# ────────────────────────────────────────────────────────────────────────────────
# Dio – Dockerfile (multi-stage)
# Stage 1: build  – Ubuntu 22.04 + OCaml 5.2.0 + full native build
# Stage 2: runtime – minimal Ubuntu with only shared libs + binaries
# ────────────────────────────────────────────────────────────────────────────────

# ==============================================================================
# STAGE 1 — Builder
# ==============================================================================
FROM ubuntu:22.04 AS builder
ENV QEMU_CPU=host

# 1. System dependencies (build-time only)
RUN apt-get update && apt-get install -y --no-install-recommends \
    sudo \
    m4 \
    pkg-config \
    libffi-dev \
    libgmp-dev \
    libpcre3-dev \
    libssl-dev \
    libpq-dev \
    zlib1g-dev \
    make \
    g++ \
    git \
    curl \
    opam \
    ca-certificates \
    netbase \
    autoconf \
    automake \
    libtool \
    && rm -rf /var/lib/apt/lists/*

# 2. Compile libsecp256k1 from source (pinned to v0.7.1 for cache stability)
RUN git clone --depth 1 --branch v0.7.1 \
        https://github.com/bitcoin-core/secp256k1.git /tmp/secp256k1 \
    && cd /tmp/secp256k1 \
    && ./autogen.sh \
    && ./configure --enable-module-schnorrsig --enable-module-recovery \
    && make \
    && make check \
    && make install \
    && ldconfig \
    && rm -rf /tmp/secp256k1

# 3. OPAM + OCaml switch (5.2.0)
RUN opam init --disable-sandboxing --reinit -y \
    && opam switch create 5.2.0 ocaml-base-compiler.5.2.0 \
    && eval $(opam env --switch=5.2.0)

# 4. Workdir
WORKDIR /app

# 5. Copy project descriptors first (layer-cache friendly)
COPY --chown=root:root dio.opam dune-project ./

# 6. Install OCaml dependencies (cached unless opam deps change)
RUN eval $(opam env --switch=5.2.0) \
    && opam install -y . --deps-only --with-test --no-depexts

# 7. Copy the rest of the source tree
COPY --chown=root:root . .

# 8. Build native executables
RUN eval $(opam env --switch=5.2.0) \
    && dune build --profile=release bin/main.exe bin/dashboard.exe

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

# 12. Copy config files needed at runtime
COPY --chown=root:root config.json /app/config.json

WORKDIR /app

# 13. Use jemalloc to prevent glibc arena fragmentation in OCaml 5
ENV LD_PRELOAD=libjemalloc.so.2

# 14. jemalloc tuning: fast dirty/muzzy page decay, limited arenas for OCaml 5
ENV MALLOC_CONF="dirty_decay_ms:1000,muzzy_decay_ms:1000,narenas:2"

# 15. OCaml runtime GC defaults (Forces OCaml 5 minor_heap_size scaling per-domain natively)
ENV OCAMLRUNPARAM="s=4194304,o=2000,O=8000,h=100,w=1"

# 16. Expose metrics broadcast port
EXPOSE 8080

# 17. Default command
CMD ["dio"]