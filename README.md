[![OCaml](https://img.shields.io/badge/Language-OCaml-blue.svg)](https://ocaml.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

High-performance OCaml 5.2 trading engine for Kraken, Hyperliquid,
Lighter, Interactive Brokers (IBKR), and Alpaca featuring domain-based
parallel strategy execution. Each trading asset runs in its own
isolated domain with lock-free communication, tick-driven event
architecture, and real-time latency profiling. Built for
high-frequency trading with WebSocket data feeds and asynchronous
order execution.

> [!WARNING]
> **Auto-hedge for Hyperliquid is EXPERIMENTAL.** Testing is ongoing; features may be incomplete or unstable.

---

## Table of Contents

- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [IBKR Gateway Setup](#ibkr-gateway-setup)
- [Strategies](#strategies)
- [Architecture](#architecture)
- [Dashboard](#dashboard)
- [Deployment](#deployment)
- [Development](#development)
- [Contributing](#contributing)

---

## Requirements

- OCaml 5.2.0 (opam)
- macOS / Linux / WSL
- At least one exchange configured:
  - **Kraken**: API key and secret
  - **Hyperliquid**: Wallet address, agent address, and private key
  - **Lighter**: API private key, account index, and signer shared library
  - **IBKR**: IB Gateway (Docker) with TWS credentials
  - **Alpaca**: API key and secret (Paper or Live)

---

## Quick Start

```bash
git clone https://github.com/malciller/dio-domains.git
cd dio-domains
opam install . --deps-only
dune build
```

```bash
# Start the engine
./_build/default/bin/main.exe

# In a separate terminal, start the dashboard
./_build/default/bin/dashboard.exe
```

---

## Configuration

### Environment Variables

Create a `.env` file in the project root with credentials for your
exchanges. Only include variables for exchanges you are using.

```bash
# ── Kraken ──
KRAKEN_API_KEY=your_kraken_api_key
KRAKEN_API_SECRET=your_kraken_api_secret

# ── Hyperliquid ──
HYPERLIQUID_WALLET_ADDRESS=your_wallet_address
HYPERLIQUID_AGENT_ADDRESS=your_agent_address
HYPERLIQUID_PRIVATE_KEY=your_private_key

# ── IBKR ──
IBKR_GATEWAY_HOST=127.0.0.1    # Default: 127.0.0.1
IBKR_GATEWAY_PORT=4002          # Default: 4002
IBKR_TRADING_MODE=paper         # "paper" or "live" (default: paper)
IBKR_CLIENT_ID=0                # Default: 0
IBKR_ACCOUNT_ID=                # Auto-detected from gateway if omitted

# ── Alpaca ──
ALPACA_API_KEY=your_alpaca_api_key
ALPACA_API_SECRET=your_alpaca_api_secret

# ── Lighter ──
LIGHTER_API_PRIVATE_KEY=your_lighter_private_key
LIGHTER_API_KEY_INDEX=4                            # API key index from Lighter dashboard
LIGHTER_ACCOUNT_INDEX=123456                       # Account index from Lighter dashboard
LIGHTER_SIGNER_LIB_PATH=./lighter-signer-darwin-arm64  # Path to signer lib (without extension)
LIGHTER_PROXY_URL=https://your-proxy.workers.dev       # Optional: Cloudflare proxy URL

# ── Optional ──
CMC_API_KEY=your_cmc_api_key    # Fear & Greed index (defaults to 50.0 if absent)
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/... # Order fill webhook
DIO_BACKTRACE=                  # Set any value to enable exception backtraces
```

### Trading Config (`config.json`)

Each entry in the `trading` array defines one asset to trade. The
engine spawns an isolated domain per entry.

```json
{
  "logging_level": "info",
  "logging_sections": "",
  "cycle_mod": 10000,
  "fng_check_threshold": 1.5,
  "gc": {
    "minor_heap_size": 8388608,
    "space_overhead": 80,
    "max_overhead": 150,
    "window_size": 10,
    "allocation_policy": 2,
    "major_heap_increment": 100
  },
  "oracle": {
    "qty_cap_mult": 0.0,
    "target_survival": 0.99,
    "fng_weight": 0.5,
    "range_weight": 0.25,
    "min_active_dsurv": 0.0,
    "weight_by_sessions": true,
    "no_deep_history": false,
    "refresh_seconds": 300.0,
    "poll_seconds": 30.0
  },
  "classes": {
    "crypto_core": ["BTC/USD", "ETH/USD"],
    "crypto_alt": ["SOL/USD", "DOGE/USD", "ADA/USD"],
    "equity_etf": ["SPY", "QQQ"],
    "equity_momentum": ["NVDA", "AMD"]
  },
  "trading": [
    {
      "symbol": "BTC/USD",
      "exchange": "kraken",
      "qty": "0.0002",
      "grid_interval": [0.25, 1.25],
      "sell_mult": "0.999",
      "strategy": "Grid",
      "asset_class": "crypto_core"
    },
    {
      "symbol": "HYPE/USDC",
      "exchange": "hyperliquid",
      "qty": "0.35",
      "grid_interval": [0.25, 0.5],
      "sell_mult": "0.999",
      "accumulation_buffer": [0.5, 5.0],
      "strategy": "Grid",
      "testnet": false,
      "hedge": true,
      "asset_class": "crypto_alt"
    },
    {
      "symbol": "ETH/USDC",
      "exchange": "lighter",
      "qty": "0.01",
      "grid_interval": [0.25, 0.75],
      "sell_mult": "0.999",
      "accumulation_buffer": [0.5, 5.0],
      "strategy": "Grid"
    },
    {
      "symbol": "TQQQ",
      "exchange": "ibkr",
      "qty": "1.0",
      "grid_interval": [0.5, 1.0],
      "sell_mult": "0.999",
      "accumulation_buffer": [25.0, 50.0],
      "strategy": "Grid"
    },
    {
      "symbol": "SPY",
      "exchange": "alpaca",
      "qty": "1.0",
      "grid_interval": [0.25, 0.75],
      "sell_mult": "0.999",
      "accumulation_buffer": [10.0, 25.0],
      "strategy": "Grid",
      "testnet": true,
      "data_feed": "iex",
      "asset_class": "equity_etf"
    }
  ]
}
```

#### Config Reference

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | string | Trading pair (e.g. `"BTC/USD"`) or ticker (e.g. `"TQQQ"`, `"SPY"`) |
| `exchange` | string | `"kraken"`, `"hyperliquid"`, `"lighter"`, `"ibkr"`, or `"alpaca"` |
| `qty` | string | Order quantity per grid level |
| `grid_interval` | [min, max] | Grid spacing as `%` of price. A constraint range, never a default: the capital oracle picks within it (crypto blends Fear & Greed in; equities are pure oracle and use the survival-constrained value) |
| `sell_mult` | string | Sell quantity multiplier (`qty × sell_mult`). Values < 1.0 trigger accumulation |
| `accumulation_buffer` | [min, max] | Profit threshold buffer before accumulation triggers (Hyperliquid, Lighter, IBKR, Alpaca) |
| `strategy` | string | `"Grid"` or `"MM"` |
| `min_usd_balance` | string | Minimum USD balance to run (MM only) |
| `max_exposure` | string | Maximum asset exposure before pausing (MM only) |
| `testnet` | bool | Use testnet / paper mode (Hyperliquid, IBKR, Alpaca) |
| `hedge` | bool | Enable auto-hedge (Hyperliquid only) |
| `data_feed` | string | Market data feed channel (`"iex"` or `"sip"`; Alpaca only) |
| `maker_fee` / `taker_fee` | float | Override exchange fee rates |
| `asset_class` | string | Risk class for capital-survival modeling (e.g. `"crypto_core"`); must match a key in the top-level `classes` map, required for `dio-oracle` runs |

The optional top-level `oracle` object configures the capital-oracle runtime
knobs. Every key is optional; absent keys fall back to the runtime defaults
(`Oracle_runtime.default_config`), so a minimal section like
`{"qty_cap_mult": 0.0}` is valid. Unknown keys are rejected at startup.

The live runtime is managed by the supervisor like every other engine module
(`Supervisor.start_oracle`): it registers an `"oracle"` entry in the
connection registry, is started through the standard lifecycle machinery,
is heartbeated on every published pass plus a 10s liveness ticker, and is
auto-restarted by the health monitor if its loop ever dies. It also prefers
the engine's in-process websocket-fed data over standalone HTTP where
semantics match: venue pools come from the live balance stores (Kraken /
Alpaca; Hyperliquid stays REST because its live store mixes perp margin into
the spot USDC entry), and the grid ladder anchors at the live top-of-book
bid (the grid's first order is a resting buy) with the last history close as
fallback. The CLI keeps its pure HTTP paths.

Allocation is two-phase and joint across each venue account: every asset is
analyzed first (each one computes its sizing reservation - the minimum funding
needed to reach its ATH-anchored survival runway at the tightest grid
interval), then assets are sized in config priority order against a budget
equal to the remaining pool minus the lower-priority assets' reserved minimum
funding. A priority asset therefore only grows after the rest of the account
can still fill their drawdowns at minimum qty, and an asset is disabled only
when even its first buy cannot be funded.

Priority reclamation: when a higher-priority asset fills its last buy and the
available pool can no longer fund a replacement while a lower-priority asset
still holds committed buy capital, the oracle publishes the lowest-priority
asset(s) INACTIVE-with-reclaim (canceling the FEWEST resting buys, lowest
priority first, whose committed capital closes the gap) so their domains
cancel the orders and the released capital returns to the pool — only when the
deallocation actually funds the higher-priority first buy, otherwise the
lower-priority asset stays active. Any lower-priority asset holding committed
capital is eligible — committed capital always flows toward the highest-priority
asset that needs it. A grid's sells are never gated on capital: a halted grid
still places the sell for a just-filled buy (the account's capital-recovery
path), so the last fill's inventory is never left unreclaimable.

| Oracle key | Type | Default | Description |
|------------|------|---------|-------------|
| `qty_cap_mult` | float | `0.0` | The qty search range ceiling as a multiple of the config qty: the model sizes the order qty within `[config qty, config qty × mult]` (volume-driven: it grows as large as the survival constraints allow). `0.0` = uncapped (bounded only by the survival replay and the venue floor). A value `> 0` also caps each asset's deployment at `config qty × mult` so surplus capital passes down the config priority order |
| `target_survival` | float | `0.99` | Replay coverage required to accept a sizing (fraction of blended-history paths that must clear the drawdown) |
| `fng_weight` | float | `0.5` | Sentiment weight of the Fear & Greed blend for the grid interval (crypto only): `gi = fng_weight·gi_fng + range_weight·gi_range + (1 − fng_weight − range_weight)·gi_survival`; `0.0` removes the F&G side |
| `range_weight` | float | `0.25` | Weight of the per-asset historical range side in the gi blend. The range side reads the asset's position within its ATH → all-time-low span (the deepened history): near the ATH the potential fall is the whole span, so spacing widens toward `hi` (preserve runway); near the lows the remaining downside is bounded, so spacing tightens toward `lo` (aggressive accumulation zone, working with the F&G contrarian convention). The blend is never tighter than the survival-constrained parameter - runway wins over sentiment and range aggression alike |
| `min_active_dsurv` | float | `0.0` | Minimum achieved D_surv an asset must replay at to be published `active` (a threshold above the achievable runway marks under-funded assets inactive until capital returns) |
| `weight_by_sessions` | bool | `true` | Weight class members by session count when blending class survival curves |
| `no_deep_history` | bool | `false` | Disable the Yahoo deep-history extension |
| `refresh_seconds` | float | `300.0` | Baseline cadence between analysis passes. Passes also wake early (within ~50ms) whenever the engine requests one on a fill or canceled/rejected/expired order, so this is the fallback rhythm, not the only one |
| `poll_seconds` | float | `30.0` | Fast cadence while no asset is active - kept as a safety net: capital that becomes available on sell fills is normally recognized by the event-driven wake (fill → `request_pass`), but a slow poll guarantees a fresh pass even if an event is ever missed |
| `horizons` | [int] | `None` | Session horizons in days (defaults per calendar kind) |
| `max_capital` | float | `None` | Upper bound of the sizing binary search (in quote currency) |

Grid spacing and order size are dynamic, driven by the oracle's drawdown-survival
output, and tuned to be AS AGGRESSIVE AS POSSIBLE within the confined
constraints: the tightest grid interval and the largest order qty (within
`[qty, qty × qty_cap_mult]`) that still fund the survival runway and clear the
target survival on the replayed path. The strategy is volume driven - more
fills and larger sizes are the point - but it must survive volatile markets,
and survival is the constraint that bounds the aggression.

Survival is anchored at the ALL-TIME HIGH. The model computes the governing
drawdown (the deepest horizon drawdown with `F_blend(d) >= target`) and sizes
the grid to cover the fall from the current price down to the absolute target
level `ATH × (1 - d_gov)`. Anchoring at the ATH caps the ladder's scale
absolutely: the grid always covers down to the same price level no matter
where the price sits, so the scale never shrinks endlessly as the market
grinds down (and the deployed capital never grows without bound as the price
approaches the lows). Once the price is at or below the target level, the
whole ATH-to-target span has already been traversed: the required runway is
zero, only the first buy needs funding, and the model can be maximally
aggressive.

The verification replay is funded with the asset's actual pool budget (not the
static ladder cost), so the survival verdict answers the honest question - can
the strategy survive this history with the capital it is entitled to? - and a
well-funded asset grows its qty to the survival boundary instead of being
falsely branded under-funded.

Crypto assets blend the Fear & Greed contrarian signal into the resolved
sizing (fear tightens the grid and up-sizes the qty, greed loosens and pulls
back - never breaking the survival constraint, which always bounds the
aggression); the per-asset historical range position (ATH → all-time low over
the deepened history) joins the grid blend as well. Equities are PURE ORACLE:
no F&G side and no sentiment - the grid interval is exactly the
survival-constrained value and the qty the survival-max, and a live Fear &
Greed reading is never applied to an equity grid.

Sizing signals are never fabricated. The grid domain's startup gate gives
BOTH signals their chance at startup: it opens immediately on a
capital-oracle decision for the asset, or once the oracle's first pass
attempt has finished (or the startup window has elapsed) on a live Fear &
Greed reading alone — one real signal suffices to proceed, but both are
attempted first (event-driven wakeups carry the oracle pass completion; the
F&G cache holds genuinely fetched values only, so a failed fetch is
distinguishable from a neutral reading). Equities are the exception: their
sizing is pure oracle, so a live F&G reading never opens an equity grid —
the equity domain withholds orders until the capital oracle publishes a
decision. With neither signal available the grid withholds orders entirely:
it cannot profitably and accurately size them, so it does not create them.
When only one source is active and the other failed, a one-shot warning
names the failed one.

The top-level `classes` object defines the member pools backing each risk
class's survival curve; `dio-oracle` fetches the listed symbols per venue
(never from hardcoded code lists). Two schemas are accepted:

```json
"classes": {
  "crypto_core": ["BTC/USD", "ETH/USD"],
  "crypto_alt": { "members": ["SOL/USD", "DOGE/USD"], "kappa": 250 }
}
```

- legacy: `"name": [member symbols]`
- extended: `"name": {"members": [...], "kappa": N}` — per-class blend weight

The survival blend pulls the asset's empirical drawdown coverage toward its
class curve: `F_blend = (n_asset·F_asset + kappa·F_class)/(n_asset + kappa)`,
where `kappa` is a pseudocount of class pseudo-sessions (default 200 ≈ 11% of
the vote for a ~1600-session history, decaying as the asset's own history
grows; `--kappa` overrides, config.json per-class kappa wins otherwise). The
class contribution is volatility-normalized (`z = drawdown/(vol·√h)` pooled
across members, then translated back through the asset's own trailing
volatility), so a low-vol asset is not punished for a high-vol classmate's raw
swing size. Percentile tables (asset/class/blended) are estimated from
non-overlapping windows (one window per horizon block) so tail percentiles are
not autocorrelation-dominated; `n_eff` reports the independent-window count
next to the overlapping `n_starts` count. Inverse sizing reports both the
safe closed-form static minimum capital (a straight-down worst case) and an
advisory empirical minimum capital from actual path replay, whose
static/empirical ratio shows the capital buffer the static sizing pays for.

#### Portfolio Oracle

Use `dio-oracle --portfolio` to replay multiple qualified instruments on a
shared date timeline. The model's top level is the venue: capital is pooled per
venue account (venue + quote + testnet), and every asset on the same venue
draws its buy capital from that one pool — quote locked on an exchange cannot
fund positions on another exchange, so each venue is an independent runway.
`--total-capital` is split equally per venue; explicit funding uses repeated
`--allocation VENUE/SYMBOL=AMOUNT` options (summed per venue). Manual quote
transfers between venues use `--transfer SESSION:FROM->TO=AMOUNT`; transfers
within one venue are rejected as no-ops and cross-quote transfers are rejected.
A topology JSON file can define the same positions and transfers:

```json
{
  "positions": [
    { "venue": "hyperliquid", "symbol": "BTC/USDC", "capital": 1000.0 },
    { "venue": "kraken", "symbol": "ETH/USD", "capital": 1000.0 },
    { "venue": "alpaca", "symbol": "QQQ" }
  ],
  "transfers": [
    { "session": 10, "from": "kraken/ETH/USD", "to": "alpaca/QQQ", "amount": 250.0 }
  ]
}
```

When `--capital` is omitted, online runs fetch each venue account's available
quote balance and pool it per venue, splitting it across that venue's
unspecified assets. `--positions-file` and `--save-positions` persist
per-asset pool shares and base checkpoints separately from live strategy state
(an asset's saved share is its venue pool divided by the venue's asset count,
so the shares sum back to the venue pool).

#### GC Tuning

| Field | Default | Description |
|-------|---------|-------------|
| `minor_heap_size` | 8388608 | Words (64MB) — absorbs JSON parsing bursts |
| `space_overhead` | 80 | Major heap spacing |
| `max_overhead` | 150 | Compaction threshold |
| `window_size` | 10 | Smooths major GC pacing |
| `allocation_policy` | 2 | Best-fit (OCaml 5 default) |
| `major_heap_increment` | 100 | Heap growth factor to prevent OS thrashing |

---

## IBKR Gateway Setup

The engine connects to Interactive Brokers via the TWS API. For headless
deployment, this project relies on [ib-gateway-docker](https://github.com/gnzsnz/ib-gateway-docker),
an excellent Docker image created by [@gnzsnz](https://github.com/gnzsnz) that handles
headless authentication and session management.

> [!WARNING]
> Only run one trading mode (paper or live) per container. Do not use the 'both' mode with AUTO_RESTART_TIME enabled, as the dual Java applications will fight for VNC window focus during automation and deadlock the gateway.

### 1. Create gateway directory and credentials

```bash
mkdir -p ~/ibkr && cd ~/ibkr

cat > .env << 'EOF'
TWS_USERID=your_ibkr_username
TWS_PASSWORD=your_ibkr_password
EOF
```

### 2. Create `docker-compose.yml`

This example is configured for Paper mode. To use Live mode, switch TRADING_MODE to "live" and swap the exposed port to the live proxy port (4003).

```yaml
name: ibkr-gateway

services:
  ib-gateway:
    image: ghcr.io/gnzsnz/ib-gateway:stable
    restart: always
    environment:
      # Use paper or live. Do not use both.
      TRADING_MODE: paper
      TWS_USERID: ${TWS_USERID}
      TWS_PASSWORD: ${TWS_PASSWORD}
      
      READ_ONLY_API: "no"
      TWS_ACCEPT_INCOMING: accept
      EXISTING_SESSION_DETECTED_ACTION: primary
      TWOFA_TIMEOUT_ACTION: restart
      RELOGIN_AFTER_TWOFA_TIMEOUT: "yes"
      AUTO_RESTART_TIME: "11:59 PM"
      TIME_ZONE: America/New_York
      ALLOW_BLIND_TRADING: "no"
      VNC_SERVER_PASSWORD: "ibkr"
    ports:
      # Paper proxy API
      - "127.0.0.1:4002:4004"
      
      # Live proxy API (Uncomment if using TRADING_MODE: live)
      # - "127.0.0.1:4001:4003"
      
      # VNC Display
      - "127.0.0.1:5900:5900"
    volumes:
      - ibkr-settings:/home/ibgateway/Jts

volumes:
  ibkr-settings:
```

> [!IMPORTANT]
> The internal port mapping must point to the container's socat proxy (4003 or 4004) rather than the direct Java ports to safely bypass the JVM's "TrustedIPs=127.0.0.1" local network restriction in Docker.

### 3. Start and verify

```bash
cd ~/ibkr
docker compose up -d
docker compose logs -f   # Look for "Market data farm connection is OK"
```

> [!CAUTION]
> **First-time LIVE connection requires 2FA.** When IB Gateway starts for
> the first time (or after credential changes), IBKR will send a 2FA
> challenge to your IBKR mobile app or security device. You must
> approve it within the timeout window or the gateway will fail to
> authenticate. Connect via VNC (see below) to monitor the login
> process. Subsequent reconnections and daily restarts are handled
> automatically by the `TWOFA_TIMEOUT_ACTION: restart` and
> `RELOGIN_AFTER_TWOFA_TIMEOUT: "yes"` settings.

### 4. VNC access (optional)

Connect to `localhost:5900` (password: `ibkr`) via any VNC client to
see the IB Gateway GUI for debugging login or order issues.

> [!NOTE]
> The gateway handles automatic reconnection, daily restarts, and 2FA
> timeouts. Paper accounts receive delayed (15-min) market data; the
> engine sets data type to delayed automatically.

> [!WARNING]
> **Paper Trading Auto-Restart Bug**: In Paper Trading mode, IB Gateway's internal JVM daily restart (triggered by `AUTO_RESTART_TIME`) frequently hangs at an "Unrecognized Username or Password" dialog because paper session tokens expire. The engine will loop `Connection refused` endlessly.
> 
> **Fix**: For Paper Trading, remove `AUTO_RESTART_TIME` from your `docker-compose.yml` and use a host OS crontab (`crontab -e`) to hard-restart the docker container daily before market open (e.g. `59 23 * * * docker restart ib-gateway`). This ensures a completely fresh login sequence, which always succeeds.

---

## Alpaca Setup

The engine supports US equities and crypto trading via Alpaca Markets REST & WebSocket APIs, including full support for extended hours trading (pre-market and post-market sessions).

### 1. Credentials

Generate API keys from your [Alpaca Dashboard](https://app.alpaca.markets/). Add your credentials to `.env`:

```bash
ALPACA_API_KEY=your_alpaca_api_key_id
ALPACA_API_SECRET=your_alpaca_api_secret
```

### 2. Account Mode & Data Feeds

Configure account environment and streaming data feed per asset in `config.json`:

- **Paper vs. Live Trading**: Set `"testnet": true` for paper trading (`https://paper-api.alpaca.markets`) or `"testnet": false` for live trading (`https://api.alpaca.markets`).
- **Data Feed Source**: Set `"data_feed"` to `"iex"` (default, free Investors Exchange feed) or `"sip"` (paid Securities Information Processor feed with consolidated cross-exchange coverage).

### 3. Extended Hours Trading

Alpaca supports extended and overnight sessions for US equities:
- **Pre-Market**: 4:00 AM – 9:30 AM ET
- **Post-Market**: 4:00 PM – 8:00 PM ET
- **Overnight (24/5)**: 8:00 PM ET Sunday – 4:00 AM ET Friday

The engine automatically handles market session transitions:
- Orders submitted during regular market hours (9:30 AM – 4:00 PM ET) preserve the requested `time_in_force` (`GTC`, `IOC`, `FOK`, or `DAY`) and do not set `extended_hours`.
- Orders submitted during extended or overnight sessions automatically use `time_in_force: "day"` and `extended_hours: true` (only limit orders are supported by Alpaca outside regular hours). `GTC`, `IOC`, and `FOK` are all downgraded to `day` so the order is accepted unconditionally and cancels at the end of the session.
- Real-time orderbook snapshots and live quotes continue streaming during pre-market, post-market, and overnight sessions.
- Order amendments (`PATCH /v2/orders/{id}`) automatically track and update replacement order IDs across live session changes.

---

## Strategies

### Grid

Maintains buy/sell limit orders at grid intervals around the current
price. Acts as a market maker with configurable spacing and optional
DCA accumulation via `sell_mult`.

#### Accumulation (Hyperliquid)

Hyperliquid enforces discrete order sizes via `szDecimals`. When
`sell_mult < 1.0`, the sell quantity is floored to the nearest valid
lot, creating a rounding difference (e.g. `0.35 × 0.999 = 0.34965`
floors to `0.34`). The strategy sells 1:1 most cycles, accumulating
profit from the grid spread. When `accumulated_profit` exceeds
`rounding_cost + accumulation_buffer`, a reduced sell is triggered
and the difference is retained as `reserved_base`.

#### Accumulation (Lighter)

Lighter uses the same discrete-size model as Hyperliquid — each
market defines `supported_size_decimals` which determines the minimum
lot step. Quantities are floored to valid precision via the
instruments feed metadata fetched at startup. The accumulation logic
is identical: 1:1 sells most cycles, with reduced sells triggered
when `accumulated_profit` exceeds `rounding_cost + accumulation_buffer`.

- **EdDSA signing**: Orders are signed via a precompiled Go shared library (`lighter-signer`) using BabyJubJub/Poseidon cryptography
- **Instrument metadata**: Price/quantity precision resolved from `GET /api/v1/orderBookDetails` at startup
- **GTC orders**: All Lighter orders use Good-Til-Cancelled time-in-force

#### Accumulation (IBKR)

IBKR's TWS API does not support fractional shares. The same mechanism
adapts for whole shares: any `sell_mult < 1.0` (e.g. `"0.999"`) floors
to 0 shares with the whole-share increment. The strategy trades 1:1,
accumulating USD profit. When `accumulated_profit` exceeds
`share_price + accumulation_buffer`, the sell is skipped entirely and
the share is retained as `reserved_base`. The `reserved_base` guard
prevents retained shares from being sold.

- **No fee erosion**: IBKR commissions are in USD, not shares
- **Position-gated sells**: Pre-checked via `updatePortfolio` to prevent short-selling
- **GTC orders**: All IBKR orders use Good-Til-Cancelled time-in-force

#### Accumulation (Alpaca)

Alpaca supports whole/fractional share equity trading and crypto pairs. DCA accumulation (`sell_mult < 1.0`) calculates valid lot precision and floors sell quantities. The strategy trades 1:1, accumulating USD profit. When `accumulated_profit` exceeds `share_price + accumulation_buffer`, the sell order is reduced or skipped, retaining shares as `reserved_base`.

- **Extended hours execution**: Automatically routes limit orders with `extended_hours=true` and `TIF=day` during pre/post-market sessions
- **Order amendment tracking**: Supports REST order replacement with automatic replacement ID tracking
- **GTC & Day TIF support**: Uses Good-Til-Cancelled (`GTC`) during regular market hours and `DAY` during extended sessions

#### `accumulation_buffer` and Fear & Greed

`accumulation_buffer` is resolved via linear interpolation against the
CoinMarketCap Fear & Greed index (0–100): fear resolves closer to
`min` (accumulate faster), greed resolves closer to `max` (wait
longer). Re-evaluated dynamically when price moves ≥3.5% from
baseline. Applies to Hyperliquid, Lighter, IBKR, and Alpaca. Only a
live F&G reading resolves it - there is no midpoint default, and
without a live F&G reading or a capital-oracle decision the grid does
not place orders.

### MM (Adaptive Market Maker)

Dynamically adapts quoting style based on market fees. Uses greedy
quoting for no-fee markets and conservative profit-guaranteeing
quotes where fees apply.

### Auto-Hedge (Hyperliquid only)

Single-short-per-cycle delta hedge on perps. Spot buy fills open a
perp short; spot sell fills close it. Enable with `"hedge": true`.

---

## Architecture

```
+------------------+     +--------------------+     +--------------------+
|   Main Entry     |---->|  Domain Spawner    |---->|  Strategy Domain   |
|  (CLI, signals)  |     |  (per-asset mgmt)  |     |  (Grid / MM / ..) |
+------------------+     +--------------------+     +--------------------+
                                  |                          |
                                  v                          v
                         +--------------------+     +--------------------+
                         |   Supervisor       |     |  Order Executor    |
                         | (circuit breaker,  |     |  (async placement, |
                         |  heartbeat, health)|     |   dedup, amend)    |
                         +--------------------+     +--------------------+
                                  |                          |
                                  v                          v
               +---------------------------------------------+
               |           Engine Core                       |
               |  - Lock-free tick bus & event registry      |
               |  - Histogram latency profiling (μs)        |
               |  - Structured logging (level + section)     |
               |  - Centralized error handling & backoff     |
               |  - Dashboard UDS server (JSON over socket)  |
               +---------------------------------------------+
                |                   |                   |
                v                   v                   v
    +------------------+  +------------------+  +------------------+  +------------------+  +------------------+
    | Kraken           |  | Hyperliquid      |  | Lighter          |  | IBKR             |  | Alpaca           |
    | WS: ticker,      |  | WS: allMids, L2, |  | WS: ticker, L2,  |  | TWS API: ticker, |  | WS: quote (IEX/  |
    | book, balance,   |  | webData2, spot   |  | exec, balances   |  | orderbook, exec  |  | SIP), executions |
    | exec             |  | REST: L1 sigs    |  | FFI: EdDSA signer|  | portfolio, acct  |  | REST: orders,    |
    | Ring buffer store|  | Global order idx |  | REST: instruments|  | Contract cache   |  | balances, acct   |
    +------------------+  +------------------+  +------------------+  +------------------+  +------------------+
```

---

## Dashboard

Standalone TUI dashboard (`bin/dashboard.exe`) connecting to the
engine via Unix domain socket at `/var/run/dio/dashboard.sock`.
It renders the engine's JSON state stream (~20 FPS) as a live
terminal UI with two views: the **Main Dashboard** (all assets,
portfolio, engine health) and a per-asset **Detail View** (L2 depth,
15-minute price history, and order pins).

### Usage

```bash
# Local
./_build/default/bin/dashboard.exe

# Docker (ties lifetime to SSH session)
docker run --rm -it -v dio-sock:/var/run/dio dio dio-dashboard
```

> [!WARNING]
> Do not use `docker exec -it` for the dashboard — it keeps the PTY
> alive after SSH disconnects, leaving zombie connections.

If the socket is unavailable, the dashboard shows a "Waiting for
engine..." screen and auto-reconnects. When multiple engine sockets
are detected (e.g. stale `/tmp/dio-*.sock` files) it tries the newest
first.

### Controls

| Key | Action |
|-----|--------|
| `↑` / `↓` or `k` / `j` | Move selection in the strategy table |
| `Enter` | Open the selected asset in Detail View |
| `q` / `Q` | Quit |
| `Esc` / `b` / `Backspace` | Return to Main Dashboard (Detail View) / quit (Main) |

In **Detail View**, `←` / `→` (or `↑` / `↓`) cycle to the previous/next
asset, `+` / `=` zoom the chart in around the mid price, and `-` / `_`
zoom back out. Zoom is clamped so at most one order level is visible on
each side of the mid.

### Main Dashboard

The main view is composed of the following sections, top to bottom:

#### KPI Cards

Four summary cards across the top:

| Card | Information |
|------|-------------|
| **Portfolio** | Net worth (holdings + cash) and accumulated value (`ACCUM VAL`) across all exchanges |
| **System Engine** | Active/idle strategy count, uptime, recent fill count, and aggregate strategy execution rate (`strat/s`) |
| **Latency** | Capital-oracle pass p50/p99 (the oracle runtime's per-pass window, replacing the old per-domain cycle aggregate). Green `<5s`, yellow `<30s`, red `≥30s`; dims while no fresh pass window exists |
| **Memory / GC** | Heap size (MB) with usage bar and live ratio (%) vs. the configured `space_overhead` |

#### Live Ticker

A scrolling strip of mid prices for assets that are **not** active
strategies. Entries are grouped by base asset, deduplicated by venue, and
show the mid price plus the bid/ask spread in basis points. The spread
is color-coded by tightness: `<5bp` (tight), `<20bp`, `<50bp`, and
`≥50bp` (extreme).

#### Holdings & Strategy

The main table. One row per configured strategy, followed by a
`balances` subsection (non-strategy assets with a positive balance) and
a `cash` subsection (quote currencies).

| Column | Meaning |
|--------|---------|
| **SYMBOL** | Asset symbol with venue tag |
| **STGY** | Strategy type (`Grid` / `MM`) |
| **ST** | Status: `▶` running, `⏸` paused (low capital or market closed), `⏹` / `$` balance row |
| **PRICE** | Mid price (bracketed) |
| **SPREAD** | Bid/ask spread in basis points |
| **BUY @ / Δ BUY** | Last buy order price and its distance from mid in % |
| **gauge** | Proximity slider showing where price sits between the nearest buy and sell levels |
| **Δ SELL / SELL @** | Closest sell distance (%) and price |
| **SELLS / SELL VAL** | Open sell count and total pending sell value (unrealized) |
| **HOLD QTY / HOLD VAL** | Total base balance and its value at mid |
| **ACCUM QTY / ACCUM VAL** | Accumulated (non-committed) quantity and value — retained DCA/reserved base |

Rows flash green when price is within ~0.25% of the buy level and
magenta near a sell level. A row whose `capital_low` flag is set renders
paused (`⏸`). A summary bar at the bottom shows `Cash`, `Accum Val`,
`Hold Val`, and `Sell Val` totals.

#### Recent Fills

A scrolling feed of the last 50 fills from the engine's fill event
bus, in the form `Xs ago SYMBOL BUY/SELL amount @ price`. Buys render
green, sells red, with venue-colored symbols.

#### Memory & GC

Engine-wide OCaml GC stats:

| Field | Meaning |
|-------|---------|
| **HEAP** | Major heap size (MB) with a bar relative to the max seen this session |
| **LIVE / FREE** | Live and free heap words (KB) |
| **MAJOR / MINOR / COMPACT / FRAGS** | Major/minor collection counts, compactions, and heap fragments |
| **PRESSURE** | Two-row sparkline of live-ratio normalized against the expected ratio from `space_overhead`. Green ≤100%, yellow ≤125%, red above |

#### Engine Latency

Per-domain latency profiling, shown as a two-row-header table with
**p50/p99/p999** (µs, sub-microsecond readings at ns resolution) plus a
**trend** sparkline (EMA-smoothed p99 of the page's primary metric over the
last 15 windows) and the strategy rate in **strat/s**. Columns are grouped
into two pages switched with `←/→`:

| Page | Metric | What it measures | Thresholds (µs) |
|------|--------|------------------|-----------------|
| CORE | **ORACLE** | The capital-oracle per-asset pipeline for that domain's asset (history fetch + survival analysis + sizing), windowed per pass | 1,000,000 / 10,000,000 |
| CORE | **OB** | Orderbook ring-buffer consumption | 10 / 30 |
| CORE | **STRAT** | Grid/MM strategy execution | 30 / 75 |
| CORE | **EXEC** | Execution event consumption | 50 / 150 |
| CORE | **CYCLE** | Full domain cycle span (wake → consume → strategy → exec) | 100 / 1,000 |
| NETWORK | **PING** | Venue WebSocket ping/pong round trip | 20,000 / 100,000 |
| NETWORK | **FEED** | Gap between consecutive venue market-data feed messages | 50,000 / 200,000 |
| NETWORK | **REST** | Venue REST round trip (trading actions + oracle fetches) | 100,000 / 500,000 |
| NETWORK | **SIGN** | Local signature generation time | 1,000 / 10,000 |

A metric with zero samples in the current window keeps showing its last
measured value (dimmed) until a fresh window arrives, so short-lived
measurements like the network metrics stay visible between windows; a metric
that has never published renders `--`. Domains whose windows are stale
(oracle windows age out beyond the pass horizon, ~10 min; the domain stages
after 15s) drop out of the list. The engine-global oracle pass latency also
appears in the LATENCY KPI card (p50/p99).

> [!NOTE]
> The CORE page measures **internal processing latency only** — the CPU time
> the engine spends per stage; time blocked waiting for exchange data
> (`Exchange_wakeup.wait`) is excluded. The NETWORK page measures the wire
> itself: WS ping RTT, feed cadence, REST round trips, and signing cost,
> collected per venue by the engine and attributed to each domain that
> trades there.

#### Footer

Uptime, the Fear & Greed index (colored by sentiment: green ≥60,
yellow 40–60, red <40), and per-exchange connectivity dots (`◉` green
= live bid/ask feed, red = none).

### Asset Detail View

Press `Enter` on a strategy row to drill into a single asset. The view
shows, top to bottom:

1. **Header bar** — asset symbol and venue.
2. **Summary card** — strategy type, bid / mid / ask, holding quantity
   and value, last buy/sell fill prices, quote balance, and accumulated
   quantity/value.
3. **L2 order book sidebar** — venue depth. Your own buy orders render
   cyan and sell orders magenta, each flagged `★MY`. The mid row is a
   green `▶ MID` banner with the spread; the bottom row shows the last
   buy/sell fill. On Alpaca this pane becomes a **recent trades** feed
   (time, price, qty, side).
4. **Price history chart** — a 15-minute rolling window plotted with
   subpixel braille rendering. The mid price traces green; buy order
   levels cyan, sell levels magenta. The live mid is annotated with a
   `◀ LIVE NOW` badge and a warm liquid-depth gradient. Order pins on
   the right summarize qty and distance from mid (`◆ BUY`, `◆ SELL`;
   a synthetic strategy target renders `◇ EST BUY` in yellow).
5. **X-axis** — realtime timeline with 15m/10m/5m/now tick labels and
   zoom indicator (`[Zoom: Nx]`).

### Wire Protocol

4-byte big-endian length-prefixed JSON frames. Max 5 concurrent
clients. Client sends `W` to subscribe (watch), `P` to ping (keeps the
connection alive), and `Q` to close a connection.

---

## Discord Notifications

The engine supports cross-venue real-time order fill notifications delivered to a Discord webhook. 

To enable notifications, provide your webhook URL via the `DISCORD_WEBHOOK_URL` environment variable. The engine operates a background token-bucket rate limiter (2.5 req/sec) to strictly adhere to Discord's `5 requests / 2 seconds` rate limits. 

During bursty periods (e.g. cascading grid fills), the notification module natively batches up to 10 fills into a single Discord embed message to preserve rate limits seamlessly. Fills include precise estimated `maker_fee` deductions for net-value visibility.

---

## Deployment

### Docker

```bash
docker run -v /path/on/host/data:/app/data -v dio-sock:/var/run/dio --env-file .env dio
```

The Docker image uses `jemalloc` (`LD_PRELOAD=libjemalloc.so.2`) to
prevent glibc arena fragmentation, with tuning for fast dirty/muzzy
page decay and limited arenas (`narenas:2`) suited to OCaml 5
multicore workloads.

### Lighter Signer Library

Lighter requires a precompiled Go shared library for EdDSA transaction
signing. The library is loaded via FFI at runtime.

| Platform | File | Source |
|----------|------|--------|
| macOS ARM64 (dev) | `lighter-signer-darwin-arm64.dylib` | [lighter-go releases](https://github.com/elliottech/lighter-go/releases) |
| Linux x86_64 (Docker) | `lighter-signer-linux-amd64.so` | [lighter-go releases](https://github.com/elliottech/lighter-go/releases) |

Place both files in the project root. The engine auto-detects the
correct binary based on OS and architecture. Override with
`LIGHTER_SIGNER_LIB_PATH` (without file extension).

To build from source:

```bash
git clone https://github.com/elliottech/lighter-go.git /tmp/lighter-go
cd /tmp/lighter-go && go mod vendor

# Linux amd64 (via Docker)
docker run --rm --platform linux/amd64 -v ${PWD}:/go/src/sdk -w /go/src/sdk \
  golang:1.23.2-bullseye /bin/sh -c \
  "CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildmode=c-shared -trimpath \
   -o ./build/lighter-signer-linux-amd64.so ./sharedlib"

# macOS ARM64 (native)
go build -buildmode=c-shared -trimpath \
  -o ./build/lighter-signer-darwin-arm64.dylib ./sharedlib/main.go
```

### Lighter Proxy (Cloudflare Worker)

Some exchange APIs serve traffic through CDNs that enforce regional
access policies. The included Cloudflare Worker proxy
(`proxy/cloudflare/`) provides a lightweight forwarding layer that
routes all Lighter HTTP and WebSocket traffic through a Cloudflare
point of presence in a region of your choosing. This can be useful
if your infrastructure is deployed in a region where direct
connectivity to an exchange's upstream CDN is unreliable or
unavailable — a simple configuration change lets you route through
any jurisdiction Cloudflare operates in.

The proxy is a generic forwarder: it does not modify, inspect, or
cache request/response payloads. It simply relocates the network
egress point.

#### Prerequisites

- A [Cloudflare account](https://dash.cloudflare.com/sign-up) (free
  tier is sufficient)
- [Node.js](https://nodejs.org/) 18+ and npm
- Wrangler CLI (`npm install -g wrangler` or use `npx`)

#### 1. Authenticate with Cloudflare

```bash
npx wrangler login
```

This opens a browser window to authorize Wrangler against your
Cloudflare account.

#### 2. Configure the region

Edit `proxy/cloudflare/wrangler.toml` to set the Durable Object
location hint and Worker placement region. The defaults route traffic
through Western Europe:

```toml
# wrangler.toml (excerpt)

# Durable Object location — where upstream connections originate from.
# Options: "wnam", "enam", "weur", "eeur", "apac", "oc", "afr", "me"
# See: https://developers.cloudflare.com/durable-objects/reference/data-location/
[[durable_objects.bindings]]
name = "LIGHTER_PROXY"
class_name = "LighterProxy"

# Worker placement — colocate the Worker near a specific cloud region.
# Choose a region geographically close to your Durable Object hint.
[placement]
mode = "targeted"
region = "gcp:europe-west1"    # or "gcp:asia-east1", "gcp:us-east1", etc.
```

> [!TIP]
> The `locationHint` in `src/index.ts` (default: `"weur"`) controls
> where the Durable Object instance is created. The `[placement]`
> region in `wrangler.toml` controls where the Worker itself runs.
> For lowest latency, set both to the same geographic area. If you
> need traffic to exit from a specific jurisdiction, set the location
> hint to the corresponding region code.

#### 3. Deploy

```bash
cd proxy/cloudflare
npm install
npx wrangler deploy
```

Wrangler will output your proxy URL:

```
Published lighter-proxy (x.xx sec)
  https://lighter-proxy.<your-subdomain>.workers.dev
```

#### 4. Configure the engine

Add the proxy URL(s) to your `.env`:

```bash
LIGHTER_PROXY_URL=https://lighter-proxy.<your-subdomain>.workers.dev
```

**Free Tier Proxy Pooling (Failover)**: To prevent strict daily Cloudflare Durable Object limits (100k requests/day) from pausing your engine, you can deploy the proxy worker from multiple free Cloudflare accounts. Set `LIGHTER_PROXY_URL` to a comma-separated list of your proxy URLs:

```bash
LIGHTER_PROXY_URL=https://proxy1.workers.dev,https://proxy2.workers.dev,https://proxy3.workers.dev
```

When configured with a pool, `dio-domains` will seamlessly rotate through the provided proxies if it encounters an upstream HTTP failure (such as an exhausted quota `500 Internal Server Error`).

> [!TIP]
> **Performance Edge Split**: To aggressively conserve Durable Object quotas, the engine splits your streams. High-frequency public feeds (orderbook/ticker) securely bypass the proxy directly to the origin. Only authenticated private streams and API calls utilize the proxy quota pool.

#### Architecture

The proxy uses a Cloudflare Durable Object to maintain a persistent
egress point:

- **HTTP requests**: Forwarded to the DO, which fetches from Lighter's
  origin (`mainnet.zklighter.elliot.ai`) using its regional IP
- **WebSocket connections**: The Worker creates a client-facing
  WebSocket pair, then instructs the DO to establish the upstream
  connection and relay messages bidirectionally
- **Observability**: Request/response logging is enabled by default
  via Cloudflare's `[observability]` config — view logs in the
  Cloudflare dashboard under Workers → Logs

> [!NOTE]
> The proxy adds minimal latency (~10–30ms) since Cloudflare's
> network is anycast. The Durable Object is pinned to a single
> region, so all upstream connections consistently originate from
> the same jurisdiction regardless of where your client connects
> from.

### State Persistence

The Grid strategy persists accumulation state to disk so
`reserved_base`, `accumulated_profit`, and `last_fill_oid` survive
restarts. Used by Hyperliquid, Lighter, IBKR, and Alpaca.

**State file**: `data/accumulated_state.json` (local) or
`/app/data/accumulated_state.json` (Docker).

```json
{
  "HYPE/USDC": {
    "reserved_base": 0.15,
    "accumulated_profit": 0.482716,
    "last_fill_oid": "355883830672"
  },
  "ETH": {
    "reserved_base": 0.02,
    "accumulated_profit": 1.237,
    "last_fill_oid": "18442"
  },
  "TQQQ": {
    "reserved_base": 3.0,
    "accumulated_profit": 12.45,
    "last_fill_oid": "42"
  },
  "SPY": {
    "reserved_base": 2.0,
    "accumulated_profit": 45.10,
    "last_fill_oid": "6a82b9e1-2c09-411a-8c88-e21ad5813bfb"
  }
}
```

Written atomically (temp file + rename) with a mutex for multi-domain
safety. Missing fields default to `0.0` / `None` on startup.

### Graceful Shutdown

The engine handles `SIGINT` and `SIGTERM` for orderly shutdown. A
second `SIGINT` forces immediate exit. The shutdown sequence:

1. Close all open Hyperliquid hedge positions (if any).
2. Tear down exchange feeds and strategy domains.
3. Clean up the dashboard UDS socket.
4. Force-exit after a 3-second timeout if teardown stalls.

Fatal signals (`SIGSEGV`, `SIGABRT`, `SIGBUS`, `SIGFPE`) emit
diagnostic snapshots (GC stats, domain status) before termination.

---

## Development

```bash
dune build        # Build
dune test         # Tests
dune fmt          # Format
dune build @doc   # Docs
```

### Logging

Structured, domain-safe logging with five severity levels: `DEBUG`,
`INFO`, `WARN`, `ERROR`, `CRITICAL`.

- **Per-section filtering** via `logging_sections` (comma-separated)
- **Domain safety** — serialized through a shared mutex
- **Zero-allocation fast path** — disabled levels skip formatting entirely
- **Scannable, aligned layout** — every line renders as
  `HH:MM:SS.mmm LEVEL SECTION message` with fixed-width level and section
  columns, so the message always starts at the same column:
  ```
  19:17:23.672 INFO  oracle_runtime   [2/8] hyperliquid/BTC/USDC ACTIVE — buy ...
  19:17:23.672 INFO  oracle_runtime   worst drop 83.6% (peak $19497.40 on ...
  19:17:23.672 INFO  oracle_runtime         funding: drop 67.4% to floor ...
  19:17:23.672 WARN  oracle_replay    only 3 independent 180-session windows ...
  ```
- **Compact time-only timestamps** — the date is redundant when live-tailing
  and is available via `docker logs --timestamps`
- **Per-section colors** — each component gets a stable hue (the oracle family
  is bright yellow, `suicide_grid` bright cyan, ...) so its lines can be
  tracked down the screen; the level column is colored by severity
- **Multi-line blocks** (e.g. capital-oracle decision reports) indent their
  continuation lines to the message column, keeping each block grouped

### Error Handling

Centralized under `src/engine/error_handling/`, providing unified classification for network faults, rate limits, and server errors across all integrations. It features:

- **Exponential Backoff**: Canonical retry mechanics for transport errors.
- **Lwt Exception Wrappers**: Boilerplate elimination (`catch_and_log`, `catch_with_recovery`).
- **Semantic Classification**: Maps exchange-specific error strings to common `error_kind` variants.

### Platform Notes

**macOS / Linux**: No additional setup needed.

**Windows (WSL2)**: Run inside WSL2 (Ubuntu 22.04+). Store project
inside the WSL filesystem, not on `/mnt/c/`. You will have to compile your own lighter-signer-windows-amd64.dll.

> [!WARNING]
> Native Windows is not actively tested. WSL2 or Docker is strongly
> recommended.

---

## Contributing

```
1. Fork
2. Create branch  -->  git checkout -b feature/xyz
     - Strategies:       src/engine/strategies/
     - Domain management: src/engine/domain_spawner.ml
     - Concurrency:      src/engine/concurrency/
     - Latency:          src/engine/latency_profiling/
     - Supervisor:       src/engine/supervisor/
     - Dashboard:        src/engine/dashboard/ + bin/dashboard.ml
     - Kraken feeds:     src/external/kraken/
     - Hyperliquid feeds: src/external/hyperliquid/
     - Lighter feeds:    src/external/lighter/
     - IBKR feeds:       src/external/ibkr/
     - Alpaca feeds:     src/external/alpaca/
     - Lighter proxy:    proxy/cloudflare/
     - Exchange interface: src/external/exchange_intf.ml
3. Commit  -->  git commit -m "..."
4. Push    -->  git push origin feature/xyz
5. Open PR
```

Add tests, update docs, keep CI green. See `src/external/README.md`
for the exchange integration guide.

---

## License

MIT. See `LICENSE`.

```
Legal: Provided "as is", without warranty. Trading involves risk.
```
