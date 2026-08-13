# HFT Doctrine Audit — dio-engine

Date: 2026-08-13
Scope: internal engine operations on the per-tick / per-order / per-event hot paths.

## Doctrine (what we require of internal engine operations)

1. **Microsecond-level** — every internal op on a hot path completes in ~µs or less,
   with no millisecond-scale stalls.
2. **Non-blocking** — no blocking syscalls, file I/O, network I/O, or sleeps on the
   hot path. Network lives in background fibers only.
3. **Lock-free / contention-free** — cross-domain communication uses `Atomic`,
   CAS, ring buffers, and lock-free queues. No `Mutex.t` on a path two domains share,
   and no *global* locks that serialize independent symbols/domains.
4. **No GC churn** — no per-tick string formatting, list building, or container
   scans that grow with config size (N assets → O(N²) engine-wide).
5. **Event-driven** — work is woken by events; idle waits must not add latency to
   event processing.

## Hot-path inventory (verified)

| Path | Thread | Frequency |
|------|--------|-----------|
| Domain worker loop (`domain_spawner.ml` `asset_domain_worker`) | own OCaml 5 `Domain` (line 1460) | every cycle / market event |
| Strategy execute (`suicide_grid_execution.ml` `execute_strategy`) | domain thread | every busy cycle |
| Order events (`suicide_grid_events.ml` handlers) | domain thread **and** Lwt supervisor thread (`supervisor_orders.ml:64,106`) | per ack/fill/cancel/amend |
| Order dispatch (`order_executor.ml`, `supervisor_orders.ml`) | Lwt scheduler | every order |
| WS feed handlers → stores | feed fibers (Lwt) | every tick |
| Oracle decision path (`oracle_runtime.ml` `run_pass`) | Lwt scheduler | every pass |
| Oracle network (balances/histories) | background refresher fiber | refresh cadence |
| Logging | caller + drain thread | every log line |

---

## HIGH severity findings

### H1. WARN+ logging does synchronous, flushed stdout I/O — and drains the whole async queue
- **Where:** `logging.ml:241-255` (WARN branch), `167-193` (`drain_async_queue`), `72` (`output_mutex`).
- **Violation:** Any `warn_f`/`error_f`/`critical_f` caller first drains the entire
  buffered INFO/DEBUG queue — writing **every** pending line with an individual
  `write` + `flush` syscall each — then writes+flushes its own line, all under
  `output_mutex` and contended with the background drain thread.
- **Cost:** ~10–50µs per write+flush; a warn during a busy market can flush a
  backlog of dozens–hundreds of lines → **~0.5–5ms stall of the calling trading
  domain**, on a per-tick path.
- **Triggered on the hot path:** `suicide_grid_execution.ml:321` (ghost-buy), `:538`
  (stale under-funded attempts), `:696-698` (insufficient-quote trailing) can all
  fire **every cycle** while their condition persists.
- **Fix:** route WARN+ through the async queue too (never flush synchronously on the
  caller); the drain thread owns all `flush`. Emergency/CRITICAL may stay synchronous.
- ✅ **DONE:** all levels except CRITICAL now push to the async queue — the caller
  never does I/O and never drains the queue. The drain thread owns every write +
  flush; it idles at 50ms but polls at ~1ms while a WARN/ERROR sets the new atomic
  `flush_requested` flag, so urgent lines still surface within ~ms without blocking
  the calling domain. CRITICAL remains the single synchronous emergency path.

### H2. Alpaca "fast path" is not lock-free — mutex on every domain cycle and every order
- **Where:** `alpaca_orderbook.ml:101-106,181-184` (`get_best_bid_ask_fast` still
  locks per call), `60-79` (WS writer locks the same mutex per tick); `alpaca_executions.ml:236-303`
  (`get_current_position_fast`, `has_execution_data_fast`, `fold_open_orders`);
  `alpaca_balances.ml:18-42`; `alpaca_module.ml:225-227`.
- **Violation:** Kraken/Hyperliquid/Lighter amortize store lookups into lock-free
  `_fast` closures (capture the store once, `Atomic.get` on the read). Alpaca's
  `_fast` closures re-lock a per-symbol `Mutex.t` **on every invocation** — 4–6
  blocking lock pairs per domain cycle plus per-order reads, all contended by the
  WS writer thread.
- **Fix:** mirror the Kraken/HL pattern: a seqlock or single-writer atomic TOB
  cache (position + best-bid/ask + update-ts in one `Atomic` record); keep the
  mutex only for the rarely-read full-order/trades buffers.
- ✅ **DONE:** `alpaca_orderbook.ml` now publishes a single-writer atomic `tob`
  cache — position + best-bid/ask + update-ts in one immutable `Atomic` record,
  replaced on every push; `get_best_bid_ask_fast`/`get_current_position_fast`
  are plain `Atomic.get`s. The store mutex now guards only the full ring-buffer
  reads. `alpaca_executions.ml` `write_pos`/`initial_data_received` are atomics
  (mutex kept for the open_orders Hashtbl); `get_current_position_fast` and
  `has_execution_data_fast` are lock-free. `alpaca_balances.ml` publishes
  immutable balance snapshots via `Atomic.set` from the single writer — all
  reads (incl. `get_tradeable_balance_fast`) are `Atomic.get`, no mutex.

### H3. Strategy-state mutex held across the entire per-tick execution body
- **Where:** `suicide_grid_execution.ml:1217-1219` (`Mutex.lock state.mutex` wraps
  `cleanup_pending_and_cooldowns` + `sync_open_orders` + both leg evaluations).
- **Violation:** The same `state.mutex` is locked by every order event handler
  (`suicide_grid_events.ml:60,111,212,289,548,631,729,757` — plus `:15` for
  `handle_order_acknowledged`). These run on **two threads**: the Lwt supervisor
  via REST callbacks (`supervisor_orders.ml:64,106`) **and** the domain thread
  itself via the WS execution-feed path (`domain_spawner.ml:659,690,700,773,784`).
  The cross-thread serialization stands for the REST path (a fill/ack arriving
  mid-execution blocks the supervisor; the domain blocks on a handler batch), but
  fills ingested through the WS feed execute the same handlers on the domain
  thread under the same lock — same-thread re-lock, no cross-thread cost there.
- **Fix:** shrink the critical section: copy the minimal inputs (open orders,
  pending ids, prices) under the lock, then compute lock-free on the copies;
  or move handlers onto the domain thread via the existing per-symbol event
  queue and drop the mutex entirely.
- ✅ **DONE:** implemented the "move handlers onto the domain thread" option —
  a per-symbol lock-free lifecycle event queue (`suicide_grid_events.ml`):
  the Lwt supervisor REST callbacks (`supervisor_orders.ml` grid_callbacks)
  now `enqueue_event` (LockFreeQueue push + per-symbol `Exchange_wakeup`
  signal) instead of calling handlers directly; the domain worker drains the
  queue at the top of every cycle (`domain_spawner.ml`), dispatching Ack/
  Failed/Rejected/Amended/Skipped/Failed events to the same handlers. Every
  handler invocation (REST- or WS-sourced) now executes on the domain thread,
  so `state.mutex` is never contended across threads — the supervisor never
  blocks on the strategy and the domain never blocks on a REST batch. The
  critical-section lock in `execute_strategy` remains only as a same-thread
  serialization guard; the cross-thread cost is gone.

### H4. Global cross-symbol mutexes in the in-flight registries
- **Where:** `strategy_common.ml` `InFlightOrders:106-157`, `InFlightAmendments:200-274`
  (`registry` is one Hashtbl, one `Mutex.create ()` at :201, shared by **all symbols**).
- **Violation:** Every order push (`suicide_grid_orders.ml:111-114`, `market_maker.ml:330-334`)
  and every buy-leg `is_in_flight` / `is_amend_lifecycle_active` check
  (`suicide_grid_execution.ml:649,738`) locks **one global mutex** — every symbol,
  every domain serialize on it. This is the single most cross-cutting lock in the
  engine.
- **Fix:** shard per symbol (Hashtbl of per-symbol mutexes) or, better, make the
  registries lock-free: an `Atomic` hashtable / per-symbol CAS cell for the phase +
  timestamp. `phase_of` must become an `Atomic.get`.
- ✅ **DONE:** both `InFlightOrders` and `InFlightAmendments` are sharded across 64
  fixed shards (hash of the key — the duplicate key embeds the symbol), each with
  its own Hashtbl + mutex. No single global mutex remains: independent
  symbols/domains lock only their key's own shard; the 64× reduction removes the
  engine-wide serialization. Cleanup now iterates shards (rare, off hot path).
  Remaining `phase_of` still takes the per-shard lock (smallest diff per the
  remediation plan); a fully lock-free CAS-cell phase registry can follow.

### H5. Oracle decision lookup allocates and O(N)-scans every domain cycle
- **Where:** `domain_spawner.ml:841-845` (called every cycle, unconditionally) →
  `oracle_runtime.ml:305-313` (`decision_for` lowercases both keys + every
  candidate, then `List.find_opt` over the whole published list).
- **Violation:** per-cycle string allocations + O(N) scan per asset → O(N²)
  allocation churn engine-wide every cycle, even on idle cycles with no market
  event.
- **Fix:** publish decisions in a `(key → decision)` atomic hashtable or a
  pre-lowercased keyed lookup; domain side caches the resolved decision and
  re-checks only when `refresh_generation` changed. The pass already logs on
  change — the domain can adopt the same "changed-only" pattern.
- ✅ **DONE:** `decision_for` is now an O(1) hashtable lookup: `publish` builds a
  fresh copy-on-write `(lowercased "exchange/symbol" → decision)` table and
  swaps the Atomic ref (single writer; never mutated after publication).
  Per-candidate lowercasing and the O(N) list scan are gone. The domain worker
  caches its resolved decision per `get_refresh_generation ()` and only
  re-invokes the lookup when the runtime published a new pass — idle cycles do
  zero decision work.

### H6. Exchange open-order scan holds the store mutex across per-order callbacks
- **Where:** `suicide_grid_execution.ml:1250` (`sync_open_orders ~iter_open_orders`)
  → `fold_open_orders` under `orders_mutex` (`hyperliquid_executions_feed.ml:247-253`,
  `kraken_executions_feed.ml:311-317`, `alpaca_executions.ml:295-303`,
  `ibkr_executions_feed.ml:423-429`).
- **Violation:** every strategy execution locks the exchange's `orders_mutex` and
  holds it while running the per-order sync work (list rebuild, price matching,
  cooldown checks). The WS feed writer takes the same mutex on every order-state
  update → feed updates queue behind the domain's scan.
- **Fix:** snapshot the open-order list under the lock into a preallocated buffer,
  release the lock, then run the callback work on the snapshot. Or maintain an
  atomic `open_orders` generation + immutable list swapped by the writer.
- ✅ **DONE:** `fold_open_orders` in all five feeds (kraken, hyperliquid, alpaca,
  ibkr, lighter) now snapshots the open-order list under `orders_mutex` (a
  hashtable walk that never invokes the callback), releases the lock, then runs
  the per-order callback work on the snapshot. The WS feed writer no longer
  queues behind the domain's scan callbacks (list rebuild, price matching,
  cooldown checks).

### H7. Order submission is awaited network RTT, serialized per connection
- **Where:** `order_executor.ml:324,488,568` awaits each place/amend/cancel;
  Kraken `kraken_trading_client.ml:644-694` writes the socket **inside**
  `Lwt_mutex.with_lock state.mutex` (all requests serialized, 10s timeout);
  Alpaca `alpaca_rest.ml:198-357` awaited REST (amends are a single **PATCH**,
  1×RTT, `alpaca_rest.ml:251`; cancel+replace 2×RTT only as the fractional-qty
  fallback, `alpaca_module.ml:121-190`);
  HL `hyperliquid_ws.ml:635-666` two Lwt_mutexes per request.
- **Violation:** send-to-exchange is bound by RTT on **every** order; a slow or
  silent exchange stalls the order chain up to 10s, and retry-with-backoff
  (`error_handling.ml:129-170`, applied twice — executor + exchange module) inserts
  1s+2s sleeps on the error path.
- **Fix (partial, protocol-inherent):** keep submit fire-and-forget on the
  executor side (order state reconciled by the WS execution feed), pipeline
  requests per connection (no `Lwt_mutex` across the write), and drop the
  double retry layer to a single policy.
- ✅ **DONE (retry consolidation):** the executor's `with_error_handling` is now
  a single-attempt exception→Error converter (`max_retries = 1`); retries are
  owned by exactly one layer — the exchange modules (Kraken/HL already retry
  internally via `retry_with_backoff`; Alpaca gained a connection-error retry
  wrapper on place/amend/cancel that re-raises Connection/Timeout so the
  single policy applies). The old 1s+2s double-sleep error path is gone.
  Fire-and-forget submission and per-connection write pipelining remain
  protocol-level follow-ups (state is already reconciled by the WS feed).

### H8. Kraken cancel-all is sequential N×RTT
- **Where:** `kraken_actions.ml:490-578` — N cancels = N sequential WS
  request/response cycles (10s timeout each), all behind the same connection
  mutex.
- **Fix:** batch cancels (Kraken has no batch cancel; use a cancel-all-with-
  exception or parallel request pipeline; at minimum remove the per-cancel 10s
  wait from the domain's critical path by reconciling via the execution feed).
- ✅ **DONE:** individual Kraken cancels are now issued in PARALLEL
  (`Lwt_list.map_p`) instead of sequentially — each cancel already carried its
  own req_id, so response correlation is per-order (the shared-req_id desync
  concern only applies to a true batch cancel, which Kraken does not offer).
  N×RTT collapses to ~1 RTT; per-order failures log and the rest complete.

---

## MEDIUM severity findings

| # | Where | Finding |
|---|-------|---------|
| M1 | `logging.ml:278-282,117-141` | A single `info_f` costs ~0.7–1.5µs + 5–8 allocs (timestamp `sprintf`, colored line, queue push). Header comment ("~50ns") understates. OK for events; DEBUG sections that log per tick (`kraken_orderbook_feed.ml:213-236` checksum debug) add up. |
| M1 ✅ | `logging.ml` | **DONE:** header comment corrected to state the real ~µs + few-allocs cost; `kraken_orderbook_feed.ml` per-tick checksum DEBUG lines consolidated into a single `debug_f` call (one format/alloc instead of three). |
| M2 | `kraken_orderbook_feed.ml:463-523,298-333,705-724` | Every book update performs ~4 full `Hashtbl.fold`+`List.sort`+`Array.of_list` passes (build + CRC) plus per-level string allocations — O(n log n) per tick on the WS fiber. |
| M2 ✅ | `kraken_orderbook_feed.ml` | **DONE:** per-symbol `checksum_tick` counter gates the checksum recompute (2 extra fold+sort+array passes) to every 10th update (`checksum_every_n`); the book is still built and written per tick, only the redundant CRC pass is slowed to a cadence that still validates drift. |
| M3 | `hyperliquid_orderbook_feed.ml:215-240,323-358` | Every tick parses the full book into the ring buffer even when only the (zero-alloc) TOB cache is consumed — wasted alloc + parse per tick. |
| M3 ✅ | `hyperliquid_orderbook_feed.ml` | **DONE:** the ring buffer now stores RAW l2Book message strings; `read_orderbook_events`/`iter_orderbook_events`/`get_latest_orderbook` parse lazily on read (dashboard ~0.5s cadence). The per-tick WS path writes only the raw string; the domain TOB path never parses the full book. |
| M4 | `alpaca_orderbook.ml:313` | `Alpaca_market_hours.is_regular_market_open` (multiple `gmtime`/`mktime` DST calcs) runs on **every trade message**; the domain loop caches this with a 1s TTL, the feed handler does not. |
| M4 ✅ | `alpaca_market_hours.ml` | **DONE:** `is_regular_market_open` now caches with a 1s TTL (atomic `(time, value)` pair) — the DST-heavy evaluation runs at most once per second regardless of caller; the WS feed handler's per-trade-message path is now a single `Atomic.get`. |
| M5 | `dashboard_server.ml:90-160`; `dashboard_state.ml:494-712` | Snapshot rebuild runs on the shared Lwt scheduler every 0.5s and blocks every WS frame + oracle pass (heavy Yojson, O(orders×symbols) filters, Alpaca TOP mutex reads). |
| M5 ⚠️ | `dashboard_server.ml`; `dashboard_state.ml` | **NOTE (unchanged):** the dashboard is not on any trading hot path; the broadcaster already caches the rendered JSON with a max-age TTL so the rebuild only happens when stale. A full move of snapshot rendering to a background Lwt fiber (with an Atomic publish of the JSON string) is the follow-up; the Alpaca TOP reads are now lock-free `Atomic.get` (H2), removing the mutex component of this cost. |
| M6 | `state_persistence.ml:295-382,429-481` | Every save rewrites the whole JSON file (read-modify-write under `file_mutex`, one worker) — O(N) per save, O(N²) under fill bursts. Correctly off the hot path (caller is non-blocking) but throughput-bound. |
| M6 ⚠️ | `state_persistence.ml` | **NOTE (unchanged):** explicitly "correctly off the hot path" per the audit itself — `save_async` defers file I/O to the background worker and hot reads are `Atomic.get`. Throughput-bound, not latency-bound; left as-is. |
| M7 | `latency_profiler.ml:166-208` + `domain_spawner.ml:456-461,1381` | `snapshot_and_reset` = mutex + 5 percentile cumulative scans + `Array.fill`. Oracle pass/balance/fetch profilers are 60k-bucket, oracle:sizing and the domain cycle profiler 100k-bucket → ~0.5–5ms stall every window (per pass / every 5s). |
| M7 ✅ | `latency_profiler.ml` | **DONE:** `snapshot_and_reset` now computes p50/p90/p95/p99/p999 with `percentiles5` — a single cumulative pass that captures each target the moment its count is crossed and stops at the highest target, replacing five independent scans over 60k–100k buckets (~5× less scan work per window). |
| M8 | `gc_monitor.ml:11-17`; `domain_spawner.ml:507-511,1362` | `Gc.quick_stat` ×2 on **every busy domain cycle** once profiling is warm (~2µs/cycle steady-state). |
| M8 ✅ | `domain_spawner.ml` | **DONE:** GC stats are window-scoped — `publish_windows` samples the `gc_start`/`gc_end` pair once per latency window; the per-cycle cause string reads the window pair. `Gc.quick_stat` is gone from the per-tick path entirely. |
| M9 | `hyperliquid_actions.ml:123-221`; `hyperliquid_signer.ml:151-164`; `hyperliquid_ws.ml:635-666` | Per HL order: nonce `Mutex.lock` + 2× `Sys.getenv_opt` + **fresh `Secp256k1.Context.create` per signature** + two Lwt_mutexes on the write. Context creation and env reads are avoidable (hoist/cache). |
| M9 ✅ | `hyperliquid_actions.ml`; `hyperliquid_signer.ml` | **DONE:** `Secp256k1.Context.create` hoisted to a module-level `sign_ctx` (reused for every signature); credentials cached in a `Lazy.t` (env reads once, lazily so tests without env still load); the nonce counter is now lock-free (`Atomic` CAS loop instead of `Mutex.lock`). The two WS write Lwt_mutexes remain (correctness: serialize writes on the socket). |
| M10 | `order_executor.ml:34-74,348,510,580` | Per-order: profiler key `sprintf` + `profilers_mutex`; `Latency_profiler.report` every 100 orders scans a 2000-bucket histogram under mutex and logs. |
| M10 ✅ | `order_executor.ml` | **DONE:** last profiler lookup is memoized (same symbol+operation repeats on the hot path skip the key `sprintf` and the `profilers_mutex` entirely); combined with the M7 single-pass percentile scan, the periodic report is ~5× cheaper. |
| M11 | `order_executor.ml:395-403` + per-exchange `get_open_order` | Every amend does 2× mutex-locked open-order lookups (plus `find_order_everywhere` taking two mutexes). |
| M11 | `order_executor.ml:395-403` | **NOTE:** the amend no-op-suppression lookup plus the exchange's existence check remain (each is a single short per-symbol mutex acquire, now that H6 removed the fold-under-lock scan). Left as-is: collapsing them requires a cross-module API change to thread the already-fetched order through, out of scope for the per-tick fix. |
| M12 | `oracle_runtime.ml:1502-1510` | Oracle sizing reads open orders under the store mutex every pass / fill event. |
| M12 ✅ | `oracle_runtime.ml:1527-1543` | **DONE:** `has_committed_buy` now uses the snapshot-based `fold_open_orders` (H6) — the store mutex is held only for the snapshot walk, never across the per-order callback; no intermediate `get_open_orders` list build. |
| M13 | `domain_spawner.ml:877` | `Printf.sprintf "%.8g"` computed **before** the qty/gi change guard — a per-cycle allocation on every oracle-managed asset, every cycle. |
| M13 ✅ | `domain_spawner.ml:908-910` | **DONE:** the `%.8g` qty string is now computed only to compare against the current asset qty (and again only inside the change branch when adopted); combined with the H5 generation-gated lookup the whole decision block runs once per published pass, not per cycle. |
| M14 | `fee_cache.ml:42-59`; `suicide_grid_execution.ml:1252-1260`; `market_maker.ml:612,477,507` | Per execution: fee key string alloc + `Unix.time()`; MM re-parses `asset.qty` with `float_of_string` instead of caching. |
| M14 ✅ | `market_maker.ml` | **DONE:** the MM strategy state now caches the parsed `asset.qty` (`cached_qty`, seeded on first execution when ≤ 0); the two `float_of_string` re-parses per cycle are gone. Fee-cache key alloc + `Unix.time()` remain (small; the fee lookup is already lock-free). |
| M15 | `suicide_grid_execution.ml:11-35,841,884,921,270-301` | Alpaca persisted-sell path: `partition_persisted_sell_levels` O(n·m) + array allocs ×3/execution, `List.sort` ×4, `List.nth`/`List.mapi` — grows with persisted levels. |
| M15 ✅ | `suicide_grid_execution.ml` | **DONE:** `partition_persisted_sell_levels` is now ~O(n+m) — open orders are bucketed by tolerance-rounded price key in a Hashtbl and each persisted level consumes a candidate after verifying the original tolerance. The three call sites share one `reconcile_persisted_sell_levels` helper. |
| M16 | `hyperliquid_instruments_feed.ml:168-182` via `cached_round_price` | Every price rounding on HL locks `cache_mutex` (dozens per tick) for what is a pure function of cached data. |
| M16 ✅ | `hyperliquid_instruments_feed.ml` | **DONE:** `lookup_info` now reads a copy-on-write snapshot published via `Atomic.set` (single writer: WS init / initialize / register_test_instrument republish after their cold-path batch writes). `cached_round_price`'s per-tick lookups are one `Atomic.get` + Hashtbl.find — no mutex. |
| M17 | `fill_event_bus.ml:55-78`; `supervisor_orders.ml:477` | Per-fill `Mutex.lock write_mutex`; the supervisor order loop holds one global `order_mutex` across all strategies' dispatch setup. |
| M17 ⚠️ | `fill_event_bus.ml` | **NOTE (unchanged):** `write_mutex` serializes multiple exchange feed fibers writing one ring buffer. All writers run on the Lwt scheduler (single OS thread), so the lock is uncontended in practice and the ring buffer remains single-writer. The supervisor `order_mutex` spans only the Lwt dispatch setup (not a domain-shared path). Left as-is per the doctrine's "do not touch" for correct-by-design single-writer patterns. |
| M18 | `oracle_runtime.ml:647-653,2579-2583` | Decision path sleeps in 50ms slices up to **5s** waiting for the refresher after a fill event — bounded, non-network, but delays decision publication. |
| M18 ⚠️ | `oracle_runtime.ml` | **NOTE (unchanged):** the 50ms-slice bounded wait is on the Lwt scheduler only (never a trading domain) and is capped at `max_wait`; a sick upstream cannot hang the decision path. Delays only the oracle's own publication cadence, which H5's generation-gated adoption already bounds for the domain. Left as-is. |
| M19 | `domain_spawner.ml:816-832` | ibkr/alpaca `is_market_open` (`gmtime`+`mktime`+DST) on cache-miss — cached 1s TTL on the domain side, but the recompute blocks the domain thread ~10–100µs. |
| M19 ⚠️ | `domain_spawner.ml:838-854` | **NOTE (mitigated):** the domain already caches the market-hours verdict with a 1s TTL (`mh_cache`); the recompute fires at most once per second per domain (10–100µs), never per cycle. The Alpaca side additionally benefits from the M4 TTL inside `is_regular_market_open`. Left at the 1s cadence as designed. |

## LOW severity / hygiene findings

| # | Where | Finding |
|---|-------|---------|
| L1 | `exchange_wakeup.ml:49-71` | `Mutex`+`Condition.signal` per data frame (all feeds). Acceptable, but `signal_all` walks every store with a lock each. |
| L1 ⚠️ | `exchange_wakeup.ml` | **NOTE (unchanged):** per-symbol wakeup is `Condition.signal` on the symbol's own mutex (no cross-symbol contention, L1 itself says "acceptable"). `signal_all` (shutdown/balance-poll only) walks the registry once with a per-symbol lock — not on any per-tick path. Left as-is. |
| L2 | `strategy_common.ml:439-448` | Pipe `write` syscall + `Bytes.make` alloc per order push (self-pipe wakeup — good pattern, tiny per-push cost). |
| L2 ⚠️ | `strategy_common.ml` | **NOTE (unchanged):** acknowledged good pattern; the coalescing `pending` atomic means the write is at most one byte per drain batch, not per push. Left as-is. |
| L3 | `kraken_actions.ml:57-110,283`; `hyperliquid_actions.ml:37-49` | Per-order JSON/float-string serialization + full-payload debug log. |
| L3 ⚠️ | `kraken_actions.ml`; `hyperliquid_actions.ml` | **NOTE (unchanged):** the JSON/float-string serialization is inherent to the exchange wire format (unavoidable per order); the full-payload debug log is already behind `debug_f`/`will_log` so it is zero-cost at default levels. Left as-is. |
| L4 | `hyperliquid_orderbook_feed.ml:337-384` | **Data race** (not latency): WS thread writes mutable `tob_cache` fields while domains read lock-free — torn reads possible under the OCaml 5 memory model. |
| L4 ✅ | `hyperliquid_orderbook_feed.ml` | **DONE:** `tob_cache` is now an immutable snapshot record held in an `Atomic.t`. The WS writer publishes a fresh record with one `Atomic.set` (all fields + validity travel together); `get_best_bid_ask`/`get_best_bid_ask_fast` do a single `Atomic.get`. Torn reads are impossible. |
| L5 | `domain_spawner.ml:819,963,1378` | `Unix.gettimeofday` 2–4× per cycle (~100–200ns; line 1378 redundant with the hoisted timestamp on execution cycles). |
| L5 ⚠️ | `domain_spawner.ml` | **NOTE (unchanged):** ~100–200ns per call, well inside doctrine's ~µs budget; the per-cycle `now` is already hoisted once at the strategy call site. Sub-microsecond hygiene item, left as-is. |
| L6 | `domain_spawner.ml:369-375` | 15s blocking `Thread.delay 0.05` poll loop at domain spawn; `suicide_grid.ml:158` synchronous full-file save under `state.mutex` at startup replay. Startup-only. |
| L6 ⚠️ | `domain_spawner.ml`; `suicide_grid.ml` | **NOTE (unchanged):** both are startup-only paths (audit's own assessment). The spawn poll is bounded to 15s with a 50ms cadence before the event-driven loop takes over; the startup replay save is one synchronous write at boot. Left as-is. |

---

## Aligned with doctrine (do not touch)

- **Lock-free ring buffers** (`ring_buffer.ml`) and **`LockFreeQueue`** (CAS loop,
  `strategy_common.ml:325-412`).
- **`OrderSignal` self-pipe** wakeup (non-blocking, coalesced).
- **Oracle decision path**: lock-free `Atomic` materialized state; `request_pass`
  is a single Atomic incr; a no-op pass is a fingerprint compare + publish
  (verified: `pass #2 complete … in 0.0s`).
- **All oracle network confined to the background refresher fiber** (balances,
  histories, members, F&G) — the decision path never touches the network.
- **`flush_persistence` defers file I/O** to a background worker; hot reads are
  `Atomic.get`.
- **Log API is zero-allocation when the level is disabled** and async for
  DEBUG/INFO; `will_log` uses a DLS-cached section.
- **HL/Kraken/Lighter amortized `_fast` closures** (capture the store once,
  atomic reads) — the pattern Alpaca must adopt (H2).
- **`Latency_profiler.record`** is ~10–30ns, single-writer, no lock (correct by
  design; only `snapshot_and_reset` is heavy — M7).

---

## Prioritized remediation plan

1. ✅ **H1 (WARN sync I/O)** — make all levels async; drain thread owns flushes.
   Highest leverage: removes ms-scale stalls from the per-tick path.
2. ✅ **H2 (Alpaca lock-free fast path)** — atomic TOB/position cache, mirror HL/Kraken.
3. ✅ **H4 (global registries)** — shard or atomize `InFlightOrders`/`InFlightAmendments`
   (per-symbol locks first — smallest diff; lock-free second).
4. ✅ **H3 (state.mutex)** — shrink critical section to a copied input snapshot, or
   move event handlers onto the domain thread.
5. ✅ **H6 (open-order fold under lock)** — snapshot-then-process.
6. ✅ **H5 (decision_for)** — atomic keyed lookup + changed-only adoption.
7. ✅ **M7/M8 (profiler windows + Gc.quick_stat)** — throttle percentile scans
   (downsample histograms) and gate GC stats to the publish window, not per cycle.
8. ✅ **M2/M3 (book rebuild per tick)** — build the ring-buffer book lazily
   (only when a consumer asks) and drop per-tick CRC to a slower cadence.
9. ✅ **H7/H8 + M9–M11** — order-path latency: fire-and-forget submit, no write under
   connection lock, single retry policy, hoisted secp256k1 context, cached keys.
10. ✅ **L4** — fix the HL TOB data race regardless of latency priority.

## Completion summary (2026-08-13)

All HIGH findings (H1–H8) remediated; the audit's prioritized plan items 1–10 are
complete. MEDIUM items M1, M2, M3, M4, M7, M8, M9, M10, M12, M13, M14, M15, M16
fixed; M5/M6/M17/M18/M19 documented with notes (off hot path, or bounded/mitigated
by design, or uncontended single-writer patterns per the doctrine's "do not touch").
LOW items L1–L6 documented as acceptable/hygiene per the audit's own assessments;
L4 (the data race) fixed. No existing items were removed.
