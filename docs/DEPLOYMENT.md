# Deployment

The prebuilt image is public; see [RISK.md](RISK.md) for the terms of use.

## Run the prebuilt image

The image contains only the compiled programs. It **ships without a
configuration**, so it will not start until you give it two files: `config.json`
(what to trade) and `.env` (your exchange keys). Both templates, plus the
`compose.yaml` used to run it, are inside the image, so you do not need the
repository.

You need Docker installed and a terminal. Run each block below and read the note
after it; if something does not look right, stop and check.

### 1. Make a folder to keep everything in

```sh
mkdir -p ~/dio
cd ~/dio
```

`~` means your home folder. All the files for dio live in `~/dio`, and this is
where you run the commands from now on. Every time you open a new terminal,
start with `cd ~/dio`.

### 2. Download the image

The image is public, so no login is needed:

```sh
docker pull ghcr.io/malciller/dio-domains:latest
```

You should see the download finish with a line naming
`ghcr.io/malciller/dio-domains:latest`.

### 3. Copy the starter files out of the image

These three commands write `config.json`, `.env`, and `compose.yaml` into the
folder you are in:

```sh
IMAGE=ghcr.io/malciller/dio-domains:latest
docker run --rm -v "$PWD:/out" --entrypoint cp $IMAGE \
  /usr/share/doc/dio/config.example.json /out/config.json
docker run --rm -v "$PWD:/out" --entrypoint cp $IMAGE \
  /usr/share/doc/dio/.env.example /out/.env
docker run --rm -v "$PWD:/out" --entrypoint cp $IMAGE \
  /usr/share/doc/dio/compose.yaml /out/compose.yaml
```

Check they are there with `ls -a`; you should see `config.json`, `.env`,
`compose.yaml` (the leading dot on `.env` is normal).

### 4. Fill in `.env` with your exchange keys

`.env` is a plain text file of `NAME=value` lines. Rules: one per line, no
spaces around the `=`, no quotation marks, and you only fill in the exchange(s)
you actually use. Leave the others blank.

Open it in a simple editor:

```sh
nano .env
```

Arrow down to the matching lines and type your keys after the `=`. For example,
to trade on Hyperliquid, fill in:

```
HYPERLIQUID_WALLET_ADDRESS=0xYourAccountAddress
HYPERLIQUID_PRIVATE_KEY=0xYourSigningKey
```

Alpaca uses `ALPACA_API_KEY` / `ALPACA_API_SECRET`; Kraken uses
`KRAKEN_API_KEY` / `KRAKEN_API_SECRET`. The full list with what each one is for
is in [CONFIGURATION.md](CONFIGURATION.md#environment-variables).

In `nano`: type your value, then save with **Ctrl-O**, press **Enter**, and exit
with **Ctrl-X**. (Any text editor works; `nano` is just the easiest in a
terminal.) Your keys are private — never paste this file into an issue or chat.

### 5. Edit `config.json` to say what to trade

`config.json` is JSON. The engine is strict: a misspelled or unknown key stops
it from starting, which is deliberate. Open it:

```sh
nano config.json
```

The safest first run is a testnet. Replace the whole file with this, which
trades a tiny amount on Hyperliquid's testnet:

```json
{
  "trading": [
    {
      "symbol": "BTC/USDC",
      "exchange": "hyperliquid",
      "qty": "0.0001",
      "grid_interval": [0.1, 0.5],
      "strategy": "Ladder",
      "testnet": true
    }
  ]
}
```

What the fields mean:

- `symbol` — the instrument, in that exchange's format (`BTC/USDC` on
  Hyperliquid, `BTC/USD` on Kraken, `AAPL` on Alpaca/IBKR).
- `exchange` — `kraken`, `hyperliquid`, `lighter`, `ibkr`, or `alpaca`. It must
  match the keys you filled in `.env`.
- `qty` — order size, in the base asset, written as a string.
- `grid_interval` — two numbers, the smallest and largest gap (in percent)
  between orders. The oracle picks within this range.
- `strategy` — `Ladder` or `MM` (market maker).
- `testnet` — `true` routes to the venue's sandbox/paper account. **Kraken has
  no testnet**, so a Kraken entry is always live.

Every other option (`sell_mult`, `accumulation_buffer`, `data_feed`, fees,
oracle tuning) is documented in [CONFIGURATION.md](CONFIGURATION.md). For Alpaca
paper trading instead, swap in:

```json
{ "symbol": "SPY", "exchange": "alpaca", "qty": "0.01",
  "grid_interval": [0.1, 0.5], "strategy": "Ladder",
  "testnet": true, "data_feed": "iex" }
```

JSON gotchas: strings need double quotes, entries are separated by commas, there
is no comma after the last entry, and comments are not allowed.

### 6. Start it

```sh
docker compose up -d
```

The `-d` means "in the background". Watch it start up:

```sh
docker compose logs -f engine
```

You want to see it connect to the exchange and begin working. Press **Ctrl-C**
to stop watching (this does *not* stop the engine). A denied API key or a
config error shows up here. To attach the dashboard, in a second terminal:

```sh
cd ~/dio
docker compose run --rm dashboard
```

Detach from the dashboard with **Ctrl-p** then **Ctrl-q** (it keeps running).

### 7. Stop

```sh
docker compose down
```

This stops the containers. Your state (accumulated position bookkeeping) stays
in the `dio-data` volume; add `-v` to delete that too.

`compose.yaml` mounts `config.json` and `.env` read-only, keeps state in the
`dio-data` volume, shares the `dio-sock` volume for the dashboard's Unix domain
socket, and applies the same hardening as a manual run: read-only root
filesystem, all capabilities dropped, `no-new-privileges`, and a `noexec` tmpfs
at `/tmp`. The engine exposes metrics on port `8080`.

### If it does not start

- It exits immediately and the log names a config key — that key is misspelled,
  wrongly nested, or not valid for that exchange. Fix `config.json`.
- `docker compose up` fails with a name or port conflict — another `dio_engine`
  container is running, or port 8080 is taken. Stop the other container.
- The log shows an authentication error — the keys in `.env` are wrong, belong to
  a different account, or you set `testnet` differently from the keys you pasted.

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
