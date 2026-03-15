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
    make \
    g++ \
    git \
    curl \
    opam \
    ca-certificates \
    netbase \
 && rm -rf /var/lib/apt/lists/*

# 3.  OPAM + OCaml switch (5.2.0)
RUN opam init --disable-sandboxing --reinit -y \
 && opam switch create 5.2.0 ocaml-base-compiler.5.2.0 \
 && eval $(opam env --switch=5.2.0)

# 4.  Workdir inside the container
WORKDIR /app

# 5.  Copy project descriptors first (layer-cache friendly)
COPY --chown=root:root dio.opam dune-project ./

# 6.  Install OCaml dependencies
RUN eval $(opam env --switch=5.2.0) \
 && opam install -y . --deps-only --with-test

# 7.  Copy the rest of the source tree
COPY --chown=root:root . .

# 8.  Build native executable
RUN eval $(opam env --switch=5.2.0) \
 && dune build --profile=release bin/main.exe \
 && cp _build/default/bin/main.exe /usr/local/bin/dio

# 9.  Runtime PATH (opam binaries + app)
ENV PATH="/usr/local/bin:/root/.opam/5.2.0/bin:${PATH}"

# 10. Expose metrics broadcast port
EXPOSE 8080

# 11. Default command
CMD ["dio"]