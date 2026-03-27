# ────────────────────────────────────────────────────────────────────────────────
# Dio – Dockerfile
# Ubuntu 22.04 base image + OCaml 5.2.0 + full native build
# Fixes "unknown scheme" by shipping /etc/services (via netbase package)
# ────────────────────────────────────────────────────────────────────────────────

# 1.  Base image
FROM ubuntu:22.04
ENV QEMU_CPU=host

# 2.  System dependencies
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
    libjemalloc2 \
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

# 3. Compile libsecp256k1 from source (Ubuntu package lacks schnorrsig)
RUN git clone https://github.com/bitcoin-core/secp256k1.git /tmp/secp256k1 \
    && cd /tmp/secp256k1 \
    && ./autogen.sh \
    && ./configure --enable-module-schnorrsig --enable-module-recovery \
    && make \
    && make check \
    && make install \
    && ldconfig \
    && rm -rf /tmp/secp256k1

# 4.  OPAM + OCaml switch (5.2.0)
RUN opam init --disable-sandboxing --reinit -y \
    && opam switch create 5.2.0 ocaml-base-compiler.5.2.0 \
    && eval $(opam env --switch=5.2.0)

# 5.  Workdir inside the container
WORKDIR /app

# 6.  Copy project descriptors first (layer-cache friendly)
COPY --chown=root:root dio.opam dune-project ./

# 7.  Install OCaml dependencies
RUN eval $(opam env --switch=5.2.0) \
    && opam install -y . --deps-only --with-test --no-depexts

# 8.  Copy the rest of the source tree
COPY --chown=root:root . .

# 9.  Build native executable
RUN eval $(opam env --switch=5.2.0) \
    && dune build --profile=release bin/main.exe \
    && cp _build/default/bin/main.exe /usr/local/bin/dio

# 10. Runtime PATH (opam binaries + app)
ENV PATH="/usr/local/bin:/root/.opam/5.2.0/bin:${PATH}"

# 11. Use jemalloc to prevent glibc arena fragmentation in OCaml 5
ENV LD_PRELOAD=libjemalloc.so.2

# 12. Expose metrics broadcast port
EXPOSE 8080

# 13. Default command
CMD ["dio"]