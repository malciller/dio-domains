# dio-domains

dio is an OCaml 5 trading engine built for high-frequency trading. Strategies
are authored and extended through a scripting configuration. The engine loads a
strategy, validates it, and runs it over a library of registered actions. The
bundled strategy is Jacobs Ladder, a grid that buys dips and sells into
reversals. It runs against Kraken, Hyperliquid, Lighter, Interactive Brokers,
and Alpaca.

Each traded asset runs in its own OCaml domain. Order intents go through a
lock-free executor, and market data arrives on lock-free ring buffers. A
capital-survival oracle sizes each position from the asset's all-time drawdown
history. A terminal dashboard connects to the running engine over a Unix domain
socket.

[![ci](https://github.com/malciller/dio-domains/actions/workflows/ci.yml/badge.svg)](https://github.com/malciller/dio-domains/actions/workflows/ci.yml)
[![release](https://img.shields.io/github/v/release/malciller/dio-domains)](https://github.com/malciller/dio-domains/releases)
[![stars](https://img.shields.io/github/stars/malciller/dio-domains?style=social&label=Star)](https://github.com/malciller/dio-domains/stargazers)
[![docker pulls](https://img.shields.io/docker/pulls/malciller/dio-domains)](https://hub.docker.com/r/malciller/dio-domains)
[![OCaml](https://img.shields.io/github/languages/top/malciller/dio-domains)](https://ocaml.org)
[![license](https://img.shields.io/github/license/malciller/dio-domains)](LICENSE)

> dio trades real money and ships without warranty. Test on a testnet before
> risking capital.

![dio terminal dashboard](assets/dio-dashboard.gif)

## Quick start

The [deployment guide](https://diophantsolutions.com/dio/DEPLOYMENT/) has the
full walkthrough. Create a working directory, pull the image, extract the
starter files, then fill in `config.json` and `.env`.

The image contains the example config, env template, and compose file:

```sh
IMAGE=ghcr.io/malciller/dio-domains:latest
docker pull $IMAGE
docker run --rm -v "$PWD:/out" --entrypoint cp $IMAGE /usr/share/doc/dio/config.example.json /out/config.json
docker run --rm -v "$PWD:/out" --entrypoint cp $IMAGE /usr/share/doc/dio/.env.example /out/.env
docker run --rm -v "$PWD:/out" --entrypoint cp $IMAGE /usr/share/doc/dio/compose.yaml /out/compose.yaml

docker compose up -d                 # start the engine
docker compose run --rm dashboard    # attach the dashboard (Ctrl-p Ctrl-q to detach)
```

The image is published to GitHub Container Registry
(`ghcr.io/malciller/dio-domains`) and Docker Hub (`malciller/dio-domains`). The
commands use GHCR. Substitute the Docker Hub name to use that registry.

The published image targets `linux/amd64` and runs under emulation on Apple
Silicon. The engine reads `config.json` and `.env` from the working directory and
writes state to the `dio-data` volume.

Full reference: [configuration](https://diophantsolutions.com/dio/CONFIGURATION/)
and [deployment](https://diophantsolutions.com/dio/DEPLOYMENT/).

## Executables

| Executable | Function |
| --- | --- |
| `dio` | The engine. Trades the instruments in `config.json`. |
| `dio-dashboard` | Terminal UI. Connects to a running engine over the Unix domain socket. |
| `dio-oracle` | Runs the sizing pipeline offline and prints the decision surface. |

## Performance

dio targets high-frequency trading. Trading domains pin to performance cores,
background work stays on efficiency cores, and real-time `SCHED_FIFO` priority is
available through `DIO_TRADING_RT_PRIO` on a dedicated host. The strategy
interpreter resolves action handlers at load, prewarms guards, and hoists
per-cycle closures out of the steady state. The OxCaml flambda2 build uses
`local_` allocation mode, and CI fails on any compiler alert.

Latency and allocation are observable through every phase of the cycle:

- A histogram profiler reports p50, p90, p95, p99, and p999, with a nanosecond
  tier for sub-microsecond samples.
- A busy-spin canary detects process-wide stop-the-world pauses and scheduler
  descheduling that per-domain GC counters miss.
- Per-cycle GC counters attribute a spike to collector activity or to CPU work.

## Strategy files

Each entry in `config.json` names a strategy, for example
`"strategy": "jacobs_ladder"`. The engine loads `strategies/<name>.strategy`,
validates it, and compiles it to its internal step/action representation at
startup. The script is a small dotted language. The compiled `.json` form is
also accepted.

```
when book.updates:
  skip.nan.price:
    if $platform.price.nan:
      stop
```

`dio strategy validate <file>` checks a strategy statically.
`dio strategy compile <file.strategy>` emits its JSON form. The
[strategy script guide](https://diophantsolutions.com/dio/STRATEGY/) documents
the language and the action vocabulary.

## Documentation

All documentation lives at **<https://diophantsolutions.com/dio/>**:
[deployment](https://diophantsolutions.com/dio/DEPLOYMENT/),
[configuration](https://diophantsolutions.com/dio/CONFIGURATION/),
[strategy scripts](https://diophantsolutions.com/dio/STRATEGY/),
[capital oracle](https://diophantsolutions.com/dio/ORACLE/),
[risk and terms](https://diophantsolutions.com/dio/RISK/), and the
[specification](https://diophantsolutions.com/dio/SPEC/).

## Support

Bug reports and feature requests go through GitHub issues:
<https://github.com/malciller/dio-domains/issues>. Do not paste API keys or other
secrets into an issue.

## License

MIT. See [LICENSE](LICENSE).
