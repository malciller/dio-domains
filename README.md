# dio-domains

dio is an OCaml 5 trading engine. It runs a **ladder** strategy (buying dips and
selling into reversals) and a top-of-book **market maker** against Kraken,
Hyperliquid, Lighter, Interactive Brokers, and Alpaca.

Each traded asset runs in its own OCaml domain. Order intents funnel through a
lock-free executor, market data lands in lock-free ring buffers, and a
**capital-survival oracle** sizes each position from the asset's all-time
drawdown history. A terminal dashboard attaches to the running engine over a
Unix domain socket.

[![ci](https://github.com/malciller/dio-domains/actions/workflows/ci.yml/badge.svg)](https://github.com/malciller/dio-domains/actions/workflows/ci.yml)

> **This trades real money.** It comes with no warranty. The auto-hedge strategy
> is experimental — test on a testnet before risking capital.

## Quick start

The container image is private; request access first
([docs/ACCESS.md](docs/ACCESS.md)).

```sh
cp config.example.json config.json   # pick your venues and instruments
cp .env.example .env                 # add exchange credentials

docker compose up -d                 # start the engine
docker compose run --rm dashboard    # attach the dashboard (Ctrl-p Ctrl-q to detach)
```

The published image is `linux/amd64`. On Apple Silicon it runs under emulation.
The engine reads `config.json` and `.env` from the working directory and writes
state to the `dio-data` volume.

Full reference: [docs/CONFIGURATION.md](docs/CONFIGURATION.md) and
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Executables

| Executable | Function |
| --- | --- |
| `dio` | The engine. Trades the instruments in `config.json`. |
| `dio-dashboard` | Terminal UI. Connects to a running engine over the Unix domain socket. |
| `dio-oracle` | Runs the sizing pipeline offline and prints the decision surface. |

## Documentation

| Document | Contents |
| --- | --- |
| [docs/SPEC.md](docs/SPEC.md) | The full software product specification (DIO-SPS-001). |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | `config.json` keys and environment variables. |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Docker, compose, building from source, and remote deployment. |
| [docs/ACCESS.md](docs/ACCESS.md) | Requesting access to the prebuilt image. |
| [docs/oracle.md](docs/oracle.md) | The capital-oracle mathematics. |

## License

MIT. See [LICENSE](LICENSE).
