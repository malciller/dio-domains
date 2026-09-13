# Deployment

The prebuilt image is private; request access per [ACCESS.md](ACCESS.md).

## Run the prebuilt image

The image contains only the compiled binaries and their runtime libraries. It
**ships without a configuration**, so the engine refuses to start until you
mount one. The example config, the env template, and `compose.yaml` are inside
the image; you do not need the repository.

1. Request access and log in ([ACCESS.md](ACCESS.md)).
2. Extract the templates and edit them:

   ```sh
   IMAGE=ghcr.io/malciller/dio-domains:latest
   docker run --rm -v "$PWD:/out" --entrypoint cp $IMAGE \
     /usr/share/doc/dio/config.example.json /out/config.json
   docker run --rm -v "$PWD:/out" --entrypoint cp $IMAGE \
     /usr/share/doc/dio/.env.example /out/.env
   docker run --rm -v "$PWD:/out" --entrypoint cp $IMAGE \
     /usr/share/doc/dio/compose.yaml /out/compose.yaml
   # edit config.json and .env for your venues and instruments
   ```

3. Start the engine and attach the dashboard:

   ```sh
   docker compose up -d
   docker compose run --rm dashboard
   ```

`compose.yaml` mounts `config.json` and `.env` read-only, keeps state in the
`dio-data` volume, shares the `dio-sock` volume for the dashboard's Unix domain
socket, and applies the same hardening as a manual run: read-only root
filesystem, all capabilities dropped, `no-new-privileges`, and a `noexec` tmpfs
at `/tmp`. The engine exposes metrics on port `8080`.

### Manual `docker run`

If you are not using compose:

```sh
docker run -d --name dio_engine \
  --restart unless-stopped \
  --read-only --cap-drop=ALL --security-opt no-new-privileges:true \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  -v "$PWD/config.json:/app/config.json:ro" \
  -v "$PWD/.env:/app/.env:ro" \
  -v dio-data:/app/data \
  -v dio-sock:/var/run/dio \
  -p 8080:8080 \
  -e DIO_CANARY=0 \
  ghcr.io/malciller/dio-domains:latest

# dashboard, inside the running container:
docker exec -it dio_engine dio-dashboard
```

## Build the images yourself

The image is built in two stages. `Dockerfile.base` compiles the OxCaml toolchain
and installs every opam dependency; it is slow (roughly an hour, and ~16 GB RAM
to bootstrap the compiler) and only changes when dependencies change.
`Dockerfile` compiles the engine on top of it, which takes well under a minute.

```sh
docker build -f Dockerfile.base -t dio-oxcaml-base:5.2.0minus40 .
docker build \
  --build-arg DIO_BASE_IMAGE=dio-oxcaml-base:5.2.0minus40 \
  -t dio .
```

The build is `linux/amd64` only. On Apple Silicon it runs under emulation.

The base image embeds `oxcaml-port/opam-overlay`, a local opam repository overlay
carrying patched builds of the dependencies that do not compile under OxCaml as
released (`msgpck.1.7+dio1`, `digestif.1.3.1+dio1`, and
`cohttp-lwt.4.0.0+dio1` with its version-coupled companions).

## Runtime

The compiled binary is dynamically linked, and the runtime stage provides:
`libffi8`, `libgmp10`, `libpcre3`, `libssl3`, `libpq5`, `zlib1g`,
`libjemalloc2`, `ca-certificates`, and `libsecp256k1`. The Lighter signer
(`lighter-signer-linux-amd64.so`) ships in the image at `/app`.

The image sets these environment defaults:

- `LD_PRELOAD=libjemalloc.so.2` with `MALLOC_CONF` tuned to limit arenas — jemalloc
  prevents glibc arena fragmentation under OCaml 5's per-domain allocation.
- `OCAMLRUNPARAM` GC defaults; `config.json`'s `gc` section is applied on top at
  process start.
- `LIGHTER_SIGNER_LIB_PATH=./lighter-signer-linux-amd64`.

The container runs as the non-root `dio` user (UID/GID 1000).

### Data

`/app/data` holds `accumulation_state.json` and `sell_levels_state.json`. Mount
it to a named volume or host directory; without a mount, state is lost when the
container is replaced.

### Dashboard transport

`dio-dashboard` talks to the engine over a Unix domain socket under
`/var/run/dio`. Mount the same volume into both containers, or run the dashboard
with `docker exec` inside the engine container.

## Remote deployment

The pattern for a server is: copy the source (and your `config.json`/`.env`,
never into the image), build the image on the host, then run the container with
the same mounts and hardening shown above. Keep `config.json` and `.env` on a
persistent path and bind-mount them read-only; treat the `.env` file as
root-only (`chmod 600`).

## Platform support

Only `linux/amd64` images are published. The engine also builds on macOS for
local development (see the top-level README), but the prebuilt image targets
Linux.
