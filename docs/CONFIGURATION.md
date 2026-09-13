# Configuration

The engine reads `config.json` from the working directory. Credentials and
one-off overrides come from the environment; if a `.env` file is present it is
loaded dotenv-style (`KEY=value` per line). Start from
[`config.example.json`](../config.example.json) and `.env.example`.

The `trading` schema is **strict**: an unknown key under `trading`, an unknown
key in the `oracle` section, or a key that does not apply to the selected venue
causes the engine to exit at startup. This is deliberate — a typo should not
silently trade the wrong thing.

## Contents

- [Top-level keys](#top-level-keys)
- [GC tunables](#gc-tunables)
- [Oracle section](#oracle-section)
- [Trading entries](#trading-entries)
- [Environment variables](#environment-variables)

## Top-level keys

| Key | Default | Description |
| --- | --- | --- |
| `logging_level` | `INFO` | One of `DEBUG`, `INFO`, `WARN`, `ERROR`, `CRITICAL`. |
| `logging_sections` | unset | Comma-separated section filters; unset enables all sections. |
| `logging_width` | autodetect | Message column width; autodetected from the terminal or `COLUMNS`. |
| `cycle_mod` | `10000` | Legacy interval for periodic background work; unused by current strategies. |
| `latency_window_seconds` | `5.0` | Rolling window for network latency profiling statistics. |
| `latency_spike_threshold_us` | `10.0` | Per-stage ceiling; a window that breaches it emits one INFO line naming the offending stages, their worst spike, and breach count. |
| `latency_spike_report` | `internal` | Which latency families emit spike logs: `internal` (per-domain pipeline), `network` (`ws_ping`/`ws_feed`/`rest_request`/`signer`), `both`, or `none`. |
| `latency_spike_report_seconds` | `30.0` | Minimum wall-clock seconds between per-domain internal spike log lines; `0` logs every window. |
| `latency_network_spike_threshold_us` | `20000.0` | Ceiling for the network spike family, in microseconds (20 ms); separate from the 10 us internal-operation target. |
| `gc` | see below | OCaml GC tunables applied before the engine starts. |
| `oracle` | see below | Capital-oracle knobs. |
| `trading` | required | One entry per instrument to trade. |
| `fng_check_threshold` | `1.5` | Price movement (percent) from baseline that re-triggers a Fear & Greed check. |
| `theme` | unset | Default dashboard theme identifier; overridable with `dio-dashboard --theme`. |

## GC tunables

Applied at process start through `Gc.set`. Units for `minor_heap_size` and
`major_heap_increment` are OCaml words (8 bytes on 64-bit platforms).

| Key | Default | Repository value |
| --- | --- | --- |
| `minor_heap_size` | `33554432` | `262144` |
| `space_overhead` | `120` | `80` |
| `max_overhead` | `1000000` | `1000000` |
| `window_size` | `10` | `5` |
| `allocation_policy` | `2` | `2` |
| `major_heap_increment` | `100` | `1048576` |

The repository values are tuned for tail latency rather than throughput: a
small minor heap makes minor collections short and frequent instead of one long
pause, and the smaller major-heap increment keeps heap-growth slices fine.
Combined with the allocation reductions on the trading hot path, this targets a
sub-10 us internal-pipeline p99.

## Oracle section

Optional; every key falls back to its default. Unknown keys are rejected.

| Key | Default | Description |
| --- | --- | --- |
| `qty_cap_mult` | `1.5` | Buy-size upper-bound multiplier: the search sizes `buy_qty` within `[qty, qty * qty_cap_mult]`. |
| `target_survival` | `0.95` | Fraction of the historical maximum drawdown the runway covers: `runway_pct = max_drawdown_pct * target_survival`. Drives sizing only, never activity. |
| `min_active_dsurv` | `0.0` | Active gate: a strategy is active if and only if its replayed `d_surv >= min_active_dsurv` (or it has a resting buy to preserve), subject to affordability. |
| `refresh_seconds` | `300.0` | Background fallback poll for history and balances; decisions remain event-driven. |
| `assets` | unset | Per-symbol overrides keyed by symbol, each accepting `{ target_survival, min_active_dsurv, qty_cap_mult }`. |

## Trading entries

Each element of the `trading` array configures one symbol on one exchange.
A representative entry:

```json
{
  "symbol": "BTC/USDC",
  "exchange": "hyperliquid",
  "qty": "0.01",
  "grid_interval": [1.0, 5.0],
  "strategy": "jacobs_ladder",
  "maker_fee": null
}
```

| Key | Applies to | Description |
| --- | --- | --- |
| `symbol` | all | Exchange symbol, e.g. `BTC/USD` (Kraken), `BTC` (Hyperliquid perpetual), `BTC/USDC` (Hyperliquid spot), `AAPL` (IBKR, Alpaca). |
| `exchange` | all | `kraken`, `hyperliquid`, `lighter`, `ibkr`, or `alpaca`. |
| `qty` | all | Base order size in base currency, encoded as a string (e.g. `"0.01"`). |
| `grid_interval` | jacobs_ladder | `[gi_min, gi_max]`: the hardened bounds (in percent) walked by the oracle's parameter search; the strategy reads only the oracle's resolved interval, never these bounds. |
| `min_usd_balance` | MM only | Lower bound on account quote balance; MM pauses buys below this value. |
| `max_exposure` | MM only | Upper bound on quote exposure for one symbol; MM pauses buys above this value. |
| `strategy` | all | `jacobs_ladder` (alias `Ladder`) or market making (`MM`, alias `market_maker`). |
| `maker_fee`, `taker_fee` | all | Explicit fee overrides (fractions, e.g. `0.0016`); `null` selects the venue default or a live fee lookup. |
| `testnet` | HL, Lighter, IBKR, Alpaca | Route to sandbox/paper endpoints; rejected for Kraken. |
| `hedge` | Hyperliquid only | Enable the experimental perpetual-short auto-hedge; rejected elsewhere. |
| `accumulation_buffer` | all | `[min, max]` retained quote profit buffer required before base accumulation; resolved live from Fear & Greed on crypto venues. |
| `data_feed` | Alpaca | `iex` (free) or `sip` (paid, full-market); accepted on any recognized venue, consumed only by Alpaca. |
| `sell_mult` | jacobs_ladder | Fraction of each ladder rung's quantity sold per rung fill (`1.0` sells the full rung; smaller values accrue base). |
| `base_accumulation` | all | Persist accumulated base and profit state for this entry (default `true`). |
| `sell_levels` | jacobs_ladder | Persist pending sell levels for this entry (default `false`). |

Venue-specific restrictions are enforced at startup:

- `hedge` is Hyperliquid-only.
- `testnet` is accepted for Hyperliquid, Lighter, IBKR, and Alpaca; it is rejected for Kraken.
- `testnet`, `hedge`, `data_feed`, and `accumulation_buffer` are rejected for unrecognized (custom) exchanges.

## Environment variables

| Variable | Used by | Notes |
| --- | --- | --- |
| `KRAKEN_API_KEY` | Kraken | |
| `KRAKEN_API_SECRET` | Kraken | |
| `HYPERLIQUID_WALLET_ADDRESS` | Hyperliquid | Account address used for balance and fee queries. |
| `HYPERLIQUID_PRIVATE_KEY` | Hyperliquid | Private key for the EIP-712 order signer. |
| `IBKR_GATEWAY_HOST` | IBKR | Default `127.0.0.1`. |
| `IBKR_GATEWAY_PORT` | IBKR | Default `4002`; live mode forces `4001` when unset. |
| `IBKR_TRADING_MODE` | IBKR | `paper` or `live`; default `paper`. |
| `IBKR_CLIENT_ID` | IBKR | Default `0`. |
| `IBKR_ACCOUNT_ID` | IBKR | Optional; auto-detected when unset. |
| `ALPACA_API_KEY` | Alpaca | |
| `ALPACA_API_SECRET` | Alpaca | |
| `LIGHTER_API_PRIVATE_KEY` | Lighter | |
| `LIGHTER_API_KEY_INDEX` | Lighter | |
| `LIGHTER_ACCOUNT_INDEX` | Lighter | |
| `LIGHTER_SIGNER_LIB_PATH` | Lighter | Path to the signer shared library (`.dylib` on macOS, `.so` on Linux). |
| `LIGHTER_PROXY_URL` | Lighter | Comma-separated list of relay proxy URLs. |
| `CMC_API_KEY` | Fear and Greed | CoinMarketCap API key; a missing key falls back to a neutral value. |
| `DISCORD_WEBHOOK_URL` | Discord notifier | Fill notifications; unset disables Discord. |
| `DIO_BACKTRACE` | Engine | When set, pretty-prints OCaml backtraces on crashes. |
| `DIO_DATA_DIR` | Persistence | State-directory override; defaults to `/app/data` when `/app` exists, otherwise `data`. |
| `DIO_WATCHDOG_OFF` | Engine | When set, disables the main-loop watchdog. |
| `DIO_CANARY` | Engine | `0`/`false`/`off`/`no` disables the stop-the-world canary domain (which busy-spins one core while enabled). |
| `DIO_CANARY_THRESHOLD_US` | Engine | Canary spike threshold in microseconds (default `10`). |
| `DIO_CANARY_WINDOW_S` | Engine | Canary report window in seconds (default `5`). |
| `COLUMNS` | Engine | Fallback log width when stdout is not a TTY (default width `200`). |
| `DIO_MOTION` | Dashboard | `off`/`0`/`false`/`no` disables animations. |
| `DIO_FPS` | Dashboard | Caps the animated frame rate (default `30.0`). |
| `DIO_DAMAGE` | Dashboard | `off`/`0`/`false`/`no` forces full-frame redraws instead of incremental damage rendering. |
| `HOME` | Dashboard | Used for the persisted `~/.dio_theme` theme path. |

## Persistence

State lives in two JSON files under the state directory (`data/`, `$DIO_DATA_DIR`,
or `/app/data` in Docker), written atomically:

- `accumulation_state.json` — accumulated base and realized profit, keyed by
  `{strategy}:{symbol}:{venue}`.
- `sell_levels_state.json` — pending sell levels for entries with
  `sell_levels: true`.

See [SPEC.md § 4.8](SPEC.md#48-persistence) for the full field list. In Docker,
`/app/data` must be mounted to a volume to survive container restarts.
