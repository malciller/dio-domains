# HFT Performance Remediation Plan

Status: draft for review
Scope: every bottleneck reported in the codebase review
Constraint: no functional or behavioural change

---

## 1. Verdict

Yes — all of it can be done without breaking the system, provided the work is
sequenced by *risk*, not by *size*, and every phase ships behind a switch that
restores the current behaviour exactly.

Three rules govern the whole plan:

1. **No phase bundles a semantics change with a speed change.** A patch either
   makes something faster or changes what it does — never both.
2. **Every phase is independently revertable** via a runtime flag or a single
   commit revert, and the legacy path stays in the tree until the new path has
   run in production for a full reconciling session.
3. **Correctness is proven by differential testing, not by inspection.** Where a
   rewrite is involved, the old and new implementations run side by side on the
   same recorded input and must produce byte-identical output.

---

## 2. Ground truth: what is actually still open

The review contained a mix of live issues, already-fixed issues, and a few
inaccuracies. Correcting the record matters — otherwise we spend days
optimising code that is already optimal.

### 2.1 Confirmed live bottlenecks

| # | Area | Location | Symptom |
|---|------|----------|---------|
| 1 | Event loop | `src/foundation/async/lwt/unix/lwt_engine.ml:121-165` | `Unix.select` + `List.mem` fd dedupe is O(N²) per iteration; `gettimeofday` called twice per loop |
| 2 | Event loop | `lwt_engine.ml:83-94` | `collect_ready` rescans the whole event table with `List.mem` |
| 3 | Event loop | `lwt_engine.ml:108-118` | `next_timer_time` folds the whole table every iteration |
| 4 | WebSocket | `src/foundation/net/websocket/websocket.ml:113-135` | HTTP upgrade headers read one byte at a time through `Lwt_io` |
| 5 | WebSocket | `websocket.ml:258-317` | Fresh `Buffer.create n` per frame, 4–5 promise binds per frame |
| 6 | WebSocket | `websocket.ml:349-356` | Masking is per-byte `Buffer.add_char` with `i mod 4` |
| 7 | WebSocket | `src/integrations/transport/ws_lwt/ws_lwt.ml:130-137` | `Buffer.contents` → `Lwt_io.write` → `Bytes.of_string` → byte-wise blit into bigarray |
| 8 | HTTP | `src/foundation/net/cohttp-lwt-unix/cohttp_lwt_unix.ml:201-257` | Fresh DNS + TCP + TLS handshake per request, `Connection: close` |
| 9 | GC | `src/engine/domain_spawner.ml:2350` | Forced full minor collection on the trading domain, once per cycle with exec events |
| 10 | GC | `domain_spawner.ml:2364,2473,2922,3810,3992,4077` | `Gc.minor_words ()` boxes a float on the C boundary, 6× per measured cycle |
| 11 | GC | all domains | OCaml 5 STW barrier couples background allocation (persistence, dashboard, parse) to pinned P-core trading domains |
| 12 | Egress | `src/engine/strategies/core/strategy_common.ml:947-975` | `Unix.write` syscall from the domain + Lwt wake + `Lwt.pause` + mutex before the order moves |
| 13 | Egress | `src/engine/supervisor/supervisor_orders.ml:578-587` | `Lwt.pause ()` forces a scheduler round-trip per dispatched order |
| 14 | Egress | `hyperliquid_signer.ml:155` | Private key re-parsed from hex and rebuilt into a `Key.t` on every signature |
| 15 | Egress | `src/integrations/venues/hyperliquid/hyperliquid_actions.ml:283-325` | Msgpack tree + Yojson AST built per order, `Printf.sprintf "%.8f"` + recursive `String.sub` |
| 16 | Book | `src/integrations/venues/kraken/kraken_orderbook_feed.ml:620-645` | `float_of_string` then `Printf.sprintf "%.*f"` to build a Hashtbl key, per level |
| 17 | Book | `kraken_orderbook_feed.ml:704-740, 888-910` | Full fold + sort + fresh arrays on every incremental message; a third rebuild for CRC32 |
| 18 | Book | `src/integrations/venues/lighter/lighter_orderbook_feed.ml:90-102` | `Array.init nb (fun i -> List.nth l i)` — O(N²) per publish |
| 19 | Book | `src/integrations/venues/lighter/lighter_ws.ml:482` | Full `Yojson.Safe.from_string` on the Lwt scheduler thread |
| 20 | Book | `src/integrations/venues/hyperliquid/hyperliquid_ws.ml:887-897` | New `Lwt.async` fiber per inbound frame |
| 21 | Lookups | `strategy_common.ml:698-720` | `is_replacement_target` locks and folds all 64 shards |
| 22 | Lookups | `strategy_common.ml:157-169` | `has_tracked_carry_order` folds the whole registry under a global mutex |
| 23 | Lookups | `src/integrations/venues/hyperliquid/hyperliquid_executions_feed.ml:986-997` | `registered_pair_for` iterates all stores under `initialization_mutex` |
| 24 | Alloc | `strategy_common.ml:834-921` | `Atomic.set slot (Some v)` allocates a `Some` block per order |
| 25 | Alloc | `strategy_runtime.ml:601-616` | `List.filter_map` allocates a fresh arg list per non-gate action |
| 26 | Alloc | `strategy_cycle_engine.ml:1293` | `Hashtbl.create 4` per `order_replace`, i.e. per book tick |
| 27 | Alloc | `domain_spawner.ml:3054,3098` | `Printf.sprintf "%.8g"` immediately re-parsed by `float_of_string` |
| 28 | Queues | `src/engine/logging/logging.ml:475` | Unbounded `Queue.t` |
| 29 | Queues | `src/integrations/venues/lighter/lighter_outbox.ml:19` | Unbounded `Queue.t` |
| 30 | Queues | `lwt_engine.ml:58-66`, `lwt.ml:36,428-433` | Unbounded `foreign_jobs`; `pause` cancellation is an O(N) `List.filter` over the paused list |
| 31 | Timers | `domain_spawner.ml` (7 sites) | `Unix.gettimeofday ()` per cycle; wall clock, not monotonic |
| 32 | Signing | `src/integrations/venues/lighter/lighter_signer.ml:157-169` | All FFI sign calls serialised behind one mutex into a cgo/Go runtime |

### 2.2 Already optimised — do not touch

These were reported as problems but the code already solves them. Leaving them
alone is part of the plan.

- **Hyperliquid orderbook ingress.** `hyperliquid_ws.ml:727-730` short-circuits
  `l2Book` before any DOM work, and `hyperliquid_orderbook_feed.ml:300-345`
  runs a zero-allocation `parse_tob_fast` into an `Atomic` cache. Correct and
  fast.
- **Kraken offload.** `kraken_orderbook_feed.ml:1516` already routes raw frames
  through `Parse_worker` with a documented inline fallback on overflow. The
  *parser* is the problem (§2.1 #16–17), not the threading.
- **`eval_args`.** `strategy_runtime.ml:580-616` already pre-serialises literals
  at load time. Only the result-list allocation remains (#25) — the original
  claim of "string hashing and JSON dispatch" is stale.
- **`open_orders_snapshot`.** `strategy_cycle_engine.ml:1153-1183` is already
  generation-cached and only rebuilds when the generation moves. Leave the cache
  strategy intact.
- **Overlay order scan.** `strategy_cycle_engine.ml:1285-1300` already does a
  two-phase scan that materialises only matching orders. The comment there is
  correct.
- **`RingBuffer`** (`src/engine/concurrency/ring_buffer.ml`) is a sound seqlock
  with zero-alloc writes. Do not "improve" it.
- **`Cow_table`**, **`Id_table`**, `LockFreeQueue`'s padded indices — all
  already well designed.

### 2.3 Two out-of-scope system dependencies

`Lighter_signer` (#32) and the Go shared library are external. Optimising them
means changing the signer binary contract. Recommend deferring; the mutex only
matters if order rate per process exceeds the Go sign latency.

---

## 3. New correctness defects found during this review

These were not in the original brief. They are order-path correctness bugs and
should be fixed **before** any performance work, because several planned
optimisations make them easier to hit.

### D1 — Lost wakeup in `OrderSignal` (severity: critical)

`strategy_common.ml:957-975`:

```
wait () =
  Lwt_unix.wait_read fd >>= fun () ->
  drain the pipe (read until EAGAIN) ;
  Atomic.set pending false ;
  return
```

Producer (`broadcast`, line 943) does `if not (Atomic.exchange pending true) then
write a byte`. Interleaving:

1. Consumer drains the pipe.
2. Producer calls `broadcast`; `pending` is still `true`, so it writes **no**
   byte.
3. Consumer sets `pending <- false`.
4. Order sits in the ring buffer with no byte in the pipe. Consumer blocks in
   `wait_read` indefinitely.

The window is small but real, and it is a stall, not a crash — the trading
domain emits and then goes quiet. Fix: clear `pending` **before** draining, then
re-check `LockFreeQueue.size` after the drain and loop if non-empty. Regression
test: deterministic two-thread interleaving harness with an injected sleep at
the drain point.

### D2 — Silent frame drop on Hyperliquid user stream (severity: high)

`hyperliquid_ws.ml:380-394`: a bounded-16 stream whose push is *cancelled* when
full. `Lwt_stream.create_bounded` + cancel means the producer's item is
discarded. For `orderUpdates` / `userFills` a dropped frame means a desynced
execution ledger until the next snapshot. The bounded-16 sizing is defensible;
the silent drop is not. Fix: on overflow, log at WARN with the channel name and
force a reconciliation — never discard silently.

### D3 — Full queue drops orders (severity: medium, by design — make it loud)

`strategy_orders.ml:257-267`: when the 65 536-slot ring is full the order is
dropped with a WARN and the caller returns `false`. That is a defensible
backpressure policy, but under load the WARN itself allocates and the strategy
has already mutated its in-flight ledger. Document the invariant explicitly and
add a counter + circuit breaker rather than leaving it as a log line.

### D4 — Overflow races in `LockFreeQueue.read` (severity: low, verify)

`strategy_common.ml:894-906` returns `None` when a producer has claimed a slot
but not yet stored the payload. Because `broadcast` happens *after* the store,
the current pairing is safe — but only by ordering coincidence, and D1 plus the
planned direct-TX path both remove that coincidence. Add an explicit invariant
comment and a test that asserts "broadcast is issued after slot publication".

---

## 4. Safety contract (invariants that must survive every phase)

Write these down and assert them in tests. If a phase cannot preserve one, the
phase is wrong.

- **I1 — Order lifecycle.** Every emitted order transitions
  `emitted → dispatched → acked|rejected → filled|cancelled`, exactly once per
  transition. Duplicate suppression (`InFlightOrders`, `InFlightAmendments`)
  and the in-flight sell ledger behave identically.
- **I2 — Wire bytes.** For a given `(key, action, nonce, price, size)` the
  signature and serialised payload are byte-identical to today's output.
  Secp256k1 signing here is deterministic, so golden fixtures are valid.
- **I3 — Book state.** For any recorded frame sequence, the resulting level
  array, `sequence` and CRC32 are identical. Kraken's `price_str` canonical
  key must keep collapsing different wire spellings of the same float.
- **I4 — Wakeup.** If the ring buffer is non-empty, the consumer eventually
  reads it. No polling fallback may mask a lost wakeup.
- **I5 — Backpressure.** Full queue / full stream is always observable
  (counter + log), never a silent drop.
- **I6 — Ordering.** Orders from one strategy domain reach the venue in emission
  order.

---

## 5. Phases

Ordering is by dependency and risk. Each phase is a separate PR set, shippable
alone.

### Phase 0 — Safety net (must land first)

Nothing here changes production behaviour.

1. **Golden-trace harness.** Record real inbound frames per venue into
   `test/fixtures/`. Replay drives both the old path and the new path.
   The repo already has 99 test files and a reused `Strategy_common` bench
   harness (`test/engine/perf/bench.ml`), so this is an extension, not a new
   framework.
2. **Order-lifecycle model test.** A fake venue implementing
   `Dio_exchange.Exchange_intf.S` that accepts, acks, rejects, partially fills
   and replaces. Randomised operation sequences, invariant I1 asserted after
   every step. This is the test that catches every egress regression later.
3. **Book differential test.** For each venue, feed the recorded trace into the
   current implementation and snapshot the full level array + CRC. New
   implementations must match exactly.
4. **Signer golden test.** Fixed vectors for Hyperliquid (place, amend, cancel,
   USDC transfer) and Kraken. Byte comparison of the final payload. This makes
   the signer/encoder rewrite in Phase 4 mechanical rather than scary.
5. **Allocation and latency budgets.** Extend `test/engine/perf` with a
   per-phase `words/call` budget using `Gc.minor_words`, wired into `dune test`
   so a regression fails the build. Existing numbers to beat:
   `strategy_run_cycle_real_file` = 38 words/cycle,
   `sync_open_orders_48_orders` = 1 µs p50.
6. **Flake gate.** `dune test --force` must be green three consecutive runs
   before Phase 1 starts. It is green today.

### Phase 1 — Correctness fixes (D1–D4)

Small, targeted, no performance claim. Land, soak, then continue.

- D1 lost wakeup: reorder the clear/drain and add the post-drain size check.
- D2 silent drop: observable overflow + reconciliation trigger.
- D3 full-queue drop: counter, rate-limited log, documented invariant.
- D4: invariant comment + ordering assertion test.

**Verification:** D1 interleaving harness; D2/D3 counter assertions in the
lifecycle model test.

### Phase 2 — Event loop and transport (`lwt_unix`, `websocket`, `cohttp`)

This is where the biggest and safest wins are, because it is *pure plumbing* —
no trading semantics live here.

**2a. `Unix.select` → `epoll`.** `lwt_engine.mli` is a clean 7-function seam, so
this is a drop-in backend swap.

- Deployment target is **Linux only**. Write `epoll` only. Do not write a
  `kqueue` backend — it is dead code until macOS deployment is a real
  requirement, at which point the same `.mli` seam takes it without changes
  elsewhere.
- Keep `lwt_engine.ml` as the reference implementation; add
  `lwt_engine_epoll.ml` behind a `DIO_IO_BACKEND=select|epoll` switch. `select`
  stays as the rollback path and as the CI backend on macOS dev machines.
- Remove the O(N²): maintain `read_fds`/`write_fds` as indexing structures
  updated on `on_readable`/`stop_event`, not rebuilt per iteration.
- `collect_ready`: replace `List.mem` with a `Hashtbl` membership set.
- Timers: keep a binary min-heap; `next_timer_time` becomes O(1).
- Cache one monotonic `now` per iteration instead of two `gettimeofday` calls.

*Why safe:* the `.mli` contract is unchanged; the loop still runs one callback
per ready fd, still processes expired timers, still drains the self-pipe.
*Verify:* all transport tests pass on both backends; a 512-fd stress test
measures loop cost as a function of fd count on both.

**2b. Buffered WebSocket framing.**

- `read_line`: replace `IO.read ic 1` with a persistent read buffer, scan for
  `\r\n`, carry the remainder forward.
- `make_read_frame`: one reusable `Bytes` scratch buffer sized to the largest
  observed frame (grown geometrically, cached per connection); one `read_into`
  bind instead of 4–5.
- Masking: 64-bit XOR with `Bytes.get_int64_ne` / `set_int64_ne` over the
  payload, mask key replicated to 8 bytes via `Int64`. Handle the `len mod 8`
  tail with a byte loop.
- `send_frame`: serialise straight into a preallocated `Bytes`, then
  `Lwt_io.write_from_exactly` (already exposed at `lwt_io.ml:266`). Kills the
  `Buffer.contents` → `Bytes.of_string` → byte-wise blit triple copy.

*Why safe:* framing is fully determined by RFC 6455 and the existing
`Wire.write_frame_to_buf` / `read_exactly` boundary. The new code is validated
by round-trip and by replaying recorded frames.
*Verify:* property test — for random payloads and lengths 0…1 MiB, the old and
new encoders produce identical bytes; the new decoder accepts everything the old
one accepted, plus fragmentation cases already covered by
`test/integrations/transport/ws_lwt`.

**2c. HTTP keep-alive and connection pooling.**

The highest-risk item in this phase, because HTTP connection reuse has subtle
failure modes. Do it last, behind a flag.

- Add a `Connection_pool` keyed by `(host, port, tls)` with idle timeout and a
  max-age, sitting under `Cohttp_lwt_unix.Client.{get,post}`.
- Switch to `Connection: keep-alive` and `HTTP/1.1` with
  `Content-Length`/`chunked` handling (both already parsed in
  `make_body_stream`).
- On a stale reused connection (`End_of_file` / `ECONNRESET` before any response
  bytes are read), transparently open a fresh connection and retry exactly once.
  Never retry after response bytes have arrived.
- Cache DNS with a TTL alongside the pool.

*Why safe:* the request/response surface is unchanged; the retry rule is
restricted to the "no bytes received" case, which cannot duplicate a
non-idempotent request already accepted by the venue.
*Verify:* pool tests against a local stub server covering keep-alive, stale
connection, chunked body, TLS, and premature close. Gate: `DIO_HTTP_POOL=0`
restores today's behaviour exactly.

**Phase 2 wins:** removes the two largest sources of scheduler jitter —
per-frame allocation and per-request handshake — without touching a single line
of trading logic.

### Phase 3 — Hot-path allocation removal (`src/engine`)

Allocation reduction only. Every change here is mechanical and provable.

**3a. Remove the forced `Gc.minor ()`** (`domain_spawner.ml:2350`).

The comment explains it is a *latency-measurement* safepoint, not a
performance optimisation. Correct handling:

1. Confirm with `test/engine/perf/cycle_alloc_profile.ml` that per-cycle
   allocation is near zero once Phase 3b/3c land.
2. Make the safepoint conditional on `latency_active` only — never run it in
   the default path.
3. Re-baseline the latency profiler to count GC events observed via
   `Gc_monitor` instead of forcing a collection to create a clean measurement
   window.

Do this **after** 3b and 3c, otherwise the safepoint is masking real allocation
that would then show up as jitter.

**3b. Clock and counter fixes.**

- `Gc.minor_words ()` → `Monotonic_clock.minor_words ()` at all 6 sites
  (`domain_spawner.ml:2364, 2473, 2922, 3810, 3992, 4077`). The unboxed stub
  already exists.
- Replace per-cycle `Unix.gettimeofday ()` with one cached `Monotonic_clock.now_ns ()`
  converted to float once per cycle, at the 7 sites. Timestamps used for
  *persistence* keep wall-clock; timestamps used for *decisions and profiling*
  move to monotonic.

**3c. Remove per-cycle allocations.**

- `strategy_common.ml` `LockFreeQueue`: replace `'a option Atomic.t array` with
  a slot record `{ mutable payload : 'a; state : int Atomic.t }`
  (`0 = empty`, `1 = claimed`, `2 = ready`). Removes the `Some` allocation per
  order. Must preserve the D4 ordering invariant explicitly.
- `domain_spawner.ml:3054-3102`: delete the `Printf.sprintf "%.8g"` →
  `float_of_string` round-trip; keep the float.
- `strategy_runtime.ml:601-616`: preallocate one `value array` per action at
  load time (arity is known), fill in place, no `filter_map`.
- `strategy_cycle_engine.ml:1293`: hoist the `Hashtbl.create 4` to a per-cycle
  reusable buffer that is cleared, not reallocated.
- `route_exec` / `dispatch_generic_overlay_fill` (`domain_spawner.ml:2305-2317,
  2544-2594`): replace assoc-list construction with field-slot arrays resolved
  at load time, matching how `eval_args` already works.

**3d. Replace linear scans with indexes.**

- `is_replacement_target`: add a per-shard reverse map
  `new_id → old_id`, published under the same shard mutex. Lookup becomes O(1)
  and touches one mutex instead of 64.
- `has_tracked_carry_order`: maintain a secondary `Hashtbl` keyed by
  `(com_key)` with a small FIFO of `com_at`, so the age check is O(1) amortised.
- `registered_pair_for` (`hyperliquid_executions_feed.ml:986`): maintain a
  `coin → pair` `Cow_table` updated on registration, making the common path a
  single lock-free read instead of an all-stores iteration under a mutex.

**3e. Orderbook parsers.**

- **Lighter** (`lighter_orderbook_feed.ml:90-102`): replace
  `Array.init nb (fun i -> List.nth …)` with a single left-to-right pass
  writing into preallocated level arrays. Keep the local list representation if
  it is load-bearing for incremental updates — only the publish path changes.
- **Lighter WS** (`lighter_ws.ml:482`): dispatch channel frames by a prefix scan,
  the way Hyperliquid already does (`hyperliquid_ws.ml:727`), so only frames
  that need a full DOM pay for one. This mirrors an existing, proven pattern in
  the tree — lowest-risk option.
- **Kraken** (`kraken_orderbook_feed.ml:704-740`): the per-message
  fold + sort + array rebuild. Two independent changes:
  1. Keep a *sorted level array* per side, maintained incrementally (find
     insertion point by binary search, shift). Depth is small (10–50), so this
     is cheaper than a full sort and eliminates the three-way rebuild.
  2. Keep string keys. **Do not** switch to float or tick keys: the
     `Printf.sprintf "%.*f"` canonicalisation is what collapses duplicate wire
     spellings, and CRC32 is computed over those strings. Instead **cache** the
     rendered string on the span parser's output so it is computed once per
     level, not once per level per rebuild.
- **Hyperliquid** user stream (`hyperliquid_ws.ml:887-897`): stop spawning a
  fiber per frame. Consume inline when the handler is already non-blocking;
  keep `Lwt.async` only for handlers that genuinely await.

**Phase 3 wins:** removes essentially all per-cycle allocation, which is also
the precondition for attacking STW pauses in Phase 5.

### Phase 4 — Order egress

The largest latency win and the highest functional risk. Do it once Phase 0's
lifecycle model test exists — that test is the entire safety argument.

**4a. Signer key caching (trivial, do first).**
`hyperliquid_signer.ml:155` parses and rebuilds the key per signature. Parse once
at startup into `Secp256k1.Key.secret Secp256k1.Key.t`, keyed by
`(private_key, network)`. Invalidates only on key rotation.

**4b. Hand-written encoders.**
Replace the per-order Msgpack tree + Yojson AST with direct serialisation into a
preallocated `Bytes`:
- Hyperliquid msgpack for `order`/`cancel`/`amend` — fixed schema, no tree.
- `format_number`: fixed-point formatter into a scratch buffer; no
  `Printf.sprintf "%.8f"`, no recursive `String.sub`.
- Kraken `json_to_string_precise` (`kraken_trading_client.ml:136-150`): replace
  `List.init`/`List.map`/`String.concat` with a single-pass escaper.

*Safety:* I2. The golden signer vectors from Phase 0 make this a pure
compile-and-compare loop.

**4c. Direct transmit path (flagged: `DIO_TX_MODE=legacy|direct`).**

Today the order takes: domain → ring → `Unix.write` on self-pipe → Lwt wake →
`Lwt.async` → `Lwt.pause` → `order_mutex` → sign → serialise → socket.

Target: a dedicated TX domain per venue that owns the outbound socket, spins on
the MPSC queue with a short park, signs with the cached key, serialises into a
preallocated buffer, and issues a non-blocking `write(2)`, draining partial
writes from a small out-ring. Acks continue to be read by the Lwt loop —
bidirectional, no shared mutable state beyond the existing atomics.

Preserved without change: duplicate suppression, the in-flight ledger, retry and
backoff, timeouts, the per-venue `order_mutex` semantics (now per TX domain,
which is strictly stronger), and I6 ordering.

*Safety:* the legacy path stays compiled in and the flag flips both ways at
runtime. No shadow/side-by-side mode — the correctness argument is the Phase 0
lifecycle model test plus the golden wire-byte vectors, both of which run
against the direct path directly. Rollout is: run the full test suite against
both modes, then flip the flag on the box; if anything diverges, flip back and
the legacy path is still warm.

**4d. Remove `Lwt.pause` in `dispatch_place`/`dispatch_amend`/`dispatch_cancel`.**
Once 4c lands, this is moot in direct mode; in legacy mode replace
`Lwt.pause ()` with a plain `Lwt.return_unit` bind and confirm no starvation
(starvation is what the pause was presumably defending against).

### Phase 5 — GC and domain isolation

Only worth doing after Phase 3, because most STW pressure is self-inflicted
allocation.

1. **Measure first.** `test/engine/perf/gc_hammer.ml` and `bench.ml` already
   exist. Instrument pause duration per domain, attributed by cause, so we can
   tell whether the remaining pauses come from the parse domain, persistence, the
   dashboard, or the trading domain itself.
2. **Reduce allocation on background domains.** `persistence_orchestrator.ml:78`
   does `Yojson.Basic.pretty_to_string` (full tree + pretty printer) on a
   hot-adjacent path — switch to the tree-free writer already used elsewhere or
   to a preallocated buffer.
3. **Move persistence out of process** (decided, not conditional). Extract
   `persistence_orchestrator`'s writer into a second executable `dio-persist`.
   The engine writes state deltas into a shared-memory ring; `dio-persist`
   drains it, renders JSON, fsyncs, and renames, preserving the existing atomic
   `tmp → flush → fsync → rename` contract
   (`persistence_orchestrator.ml:67-88`).

   Design rules:
   - **Non-blocking ring.** The engine never waits on the helper. A dead helper
     is a loud WARN plus degraded durability, never an engine stall. Trades are
     the thing that must not stop; state is recoverable.
   - **Preserve the flush barrier.** `flush_all` already exists as a
     module-level hook (`drainers`). It becomes "drain the ring and wait for the
     helper's ack" at clean shutdown only — never on the trading path.
   - **Durability is unchanged**, because saves are already async and coalesced
     today. The helper must fsync before acking a barrier.
   - **Liveness.** If the helper is dead at startup, the engine still runs and
     logs degraded-durability mode rather than refusing to trade.
   - Migration order: persistence first, then re-measure. Only then decide
     whether the dashboard/analytics domains also move out, using the pause
     attribution data from step 1 rather than assumption.
4. **Bound the queues** from §2.1 #28–30 (`logging.async_queue`,
   `lighter_outbox.queue`, `lwt_engine.foreign_jobs`). Bounded with a documented
   drop/block policy and a counter — never unbounded.
5. **`Lwt.pause` cancellation** (`lwt.ml:433`): replace the `List.filter` over
   the paused list with a swap-remove or an id-keyed set.

---

## 6. Sequencing summary

| Phase | Risk | Functional change | Gate to proceed |
|-------|------|-------------------|-----------------|
| 0 Safety net | none | none | 3× green `dune test`, golden fixtures committed |
| 1 Correctness (D1–D4) | low | fixes, no feature change | lifecycle model + interleaving test green |
| 2a Event loop | low | none | transport tests on both backends, epoll fd-scaling bench |
| 2b WebSocket | low | none | byte-identical encoder property test |
| 2c HTTP pool | medium | none | stub-server suite, `DIO_HTTP_POOL=0` parity |
| 3a–3e Alloc/index | low | none | `words/call` budgets hold, book diffs identical |
| 4a Signer cache | very low | none | golden signature vectors |
| 4b Encoders | low | none | golden payload vectors |
| 4c Direct TX | high | none (same wire bytes) | lifecycle model + golden wire bytes green in both modes |
| 5 GC isolation | medium | none | pause attribution before/after, `dio-persist` restart test (helper killed → engine still trades) |

Phases 2a, 2b, 3 and 4a can proceed in parallel once Phase 0 lands. Phase 4c
depends on Phase 0 and 4b. Phase 5 depends on Phase 3.

---

## 7. What to explicitly *not* do

- Do not switch Kraken's string keys to float or tick keys. It looks like an
  obvious win, but the string canonicalisation is the duplicate-collapse
  mechanism and the CRC32 input. Cache the string instead.
- Do not remove `Gc.minor ()` before 3b/3c. It is currently hiding allocation
  that would otherwise surface as measurement noise.
- Do not remove `Parse_worker` or the inline fallback. The fallback is a
  correctness requirement, not a pessimisation.
- Do not "improve" `RingBuffer`, `Cow_table`, `Id_table`, `LockFreeQueue`'s
  padding, or the Hyperliquid `parse_tob_fast` path. They are already right.
- Do not batch the `websocket.ml` and `cohttp_lwt_unix.ml` rewrites into one
  PR. They have different failure modes and different test strategies.

---

## 8. Measurement baseline to capture before starting

Commit these numbers so every phase has a before/after:

- `p50/p99` per stage from `test/engine/perf/bench.ml` (already emitted).
- `words/call` for `strategy_run_cycle_real_file` (38 today) and
  `hyperliquid_spot_filter_100_orders` (28 today).
- Event-loop iteration cost at 16 / 64 / 256 / 512 active fds.
- WebSocket decode cost for a 1 KiB and a 64 KiB `l2Book` frame.
- GC pause duration per domain, at p50/p99/max, idle and under Kraken book load.
- End-to-end order latency: emission timestamp on the domain → `write(2)`
  return on the socket, p50/p99, measured with the existing
  `Monotonic_clock` instrumentation.

---

## 9. Decisions

Resolved:

1. **Event loop target — Linux only.** `epoll` only. `kqueue` deferred until
   macOS deployment is a real requirement; the `.mli` seam means it costs
   nothing to add later.
2. **No shadow mode.** Correctness rests on the Phase 0 lifecycle model test and
   golden wire-byte vectors, which run against the direct path directly. The
   `DIO_TX_MODE` flag stays as the rollback mechanism, not as a comparison
   harness.
3. **Persistence out of process — permitted.** Confirmed viable; see §9.1.

### 9.1 Out-of-process persistence — what it means here

The persistence layer already does the expensive things correctly: per-store
background domain (`persistence_orchestrator.ml:399`), latest-wins save
coalescing, and atomic `tmp → flush → fsync → rename` writes
(`persistence_orchestrator.ml:67-88`). The remaining cost is not disk I/O, it is
that `Yojson.Basic.pretty_to_string` per save allocates inside the same OCaml 5
runtime as the pinned trading domains, so its major-GC work becomes a
stop-the-world pause the trading domains also pay.

"Out of process" means moving that work into a second executable
(`dio-persist`): the engine writes state deltas into a shared-memory ring, the
helper drains it, renders JSON, fsyncs and renames. The engine's GC never sees
the helper's allocation, so the pause disappears entirely.

**Durability is unchanged**, because saves are already asynchronous and
coalesced today — the engine already only guarantees "durable shortly after, and
fully flushed on clean shutdown". Preserving that needs exactly one thing from
the helper: a flush barrier the engine blocks on during `flush_all`, which
already exists as a module-level hook (`drainers`).

**Failure policy — decided:** `dio-persist` is not a hard dependency. If it dies
the engine keeps trading, logs a loud WARN, and enters degraded-durability mode;
state is rebuildable from the venue. The alternative — acking a barrier on every
save — would put cross-process latency back on the trading path, which defeats
the point of the change.

### 9.2 Logging — decided

Logging stays **in process**, bounded. An out-of-process logger adds a
dependency precisely when you need it most — during an incident — and the log
path is already off the trading hot path via the async drain. The change is a
bounded queue with drop-oldest, a dropped-line counter, and no new process.

## 10. Implementation status

Landed in this pass. `dune build` and `dune build @runtest` are green.

| Item | Status | Where |
|------|--------|-------|
| D1 lost wakeup | fixed | `strategy_common.ml` `OrderSignal` — clear-then-drain |
| D2 silent user-frame drop | already correct upstream | no change (resync latch + ERROR log already present) |
| D3 silent order drop | fixed | `strategy_orders.ml` `record_dropped_order`, power-of-two rate-limited |
| D4 publish ordering | documented + enforced by slot state | `strategy_common.ml` `LockFreeQueue` |
| 2a epoll backend | done | `lwt_engine.ml`, `epoll_stubs.c/.ml`, `DIO_IO_BACKEND` |
| 2b buffered WS framing | done | `websocket.ml`, `ws_lwt.ml`, `lwt_io.ml`, `buffer_stubs.c` |
| 2c HTTP keep-alive pool | done | `cohttp_lwt_unix.ml` |
| 3b unboxed GC counters | done | `domain_spawner.ml` → `Monotonic_clock.minor_words` |
| 3a forced `Gc.minor` | gated to profiling | `domain_spawner.ml` |
| 3c `Some` per order | done | `LockFreeQueue` slot record |
| 3c sprintf→float round-trip | done | `domain_spawner.ml` |
| 3c `order_replace` hashtable | done, per-domain | `strategy_cycle_engine.ml` (DLS) |
| 3d replacement scan | done | `InFlightAmendments.replacement_index` |
| 3d carry scan | early-exit | `has_tracked_carry_order` |
| 3d `registered_pair_for` | done | `coin_to_pair` Cow_table |
| 3e Lighter O(N²) publish | done | `lighter_orderbook_feed.flush_to_ring` |
| 3e Kraken triple rebuild | 3 → 1 | CRC reuses `make_orderbook` arrays |
| 4a key cache | done | `hyperliquid_signer.cached_key` |
| 4b `format_number` | done | single-pass strip |
| 4b duplicated `format_number` call | done | `hyperliquid_actions` |
| 4d `Lwt.pause` on dispatch | removed | `supervisor_orders.ml` |
| 5 logging bound | done | `logging.log_async`, drop-oldest |
| 5 `pause` cancel O(N) | done | `lwt.ml` cancelled flag |
| 5 out-of-process persistence | done | `persist_ipc.ml`, `bin/dio_persist.ml` |
| 5 deployment wiring | done | `Dockerfile`, `docker-entrypoint.sh`, `docker-sidecar.sh`, `deploy.sh`, `compose.yaml` |
| 0 WS framing test | done | `test_ws_framing.ml` |
| 0 queue/replacement-index tests | done | `test_egress_invariants.ml` |
| 0 persistence helper test | done | `test_persist_ipc.ml` |
| 0 HTTP pool test | done | `cohttp-lwt-unix/test/test_http.ml` |

### Deployment wiring

`dio-persist` runs as a **sidecar inside the engine container**, not as a separate
service. It needs the same `/app/data` mount and the same private `/tmp` (where its
Unix socket lives), and both are per-container — a second container would need a
shared volume just for the socket and would still have to write the same files.

- `Dockerfile`: builds and installs `dio-persist`, plus the `dio-sidecar` launcher.
- `docker-entrypoint.sh`: rewrites the `dio` command to `dio-sidecar dio`, *after* the
  root→uid 1000 drop so both processes own the files they write. The launcher `exec`s
  the engine, so the engine remains PID 1 and still receives `SIGTERM`; its shutdown
  flush completes while the helper is alive to acknowledge it.
- `deploy.sh`: both the normal and Valgrind commands go through `dio-sidecar` (that
  path passes `bash -c`, so the entrypoint's rewrite does not apply to it).
- `compose.yaml`: unchanged command; `DIO_PERSIST=1` documents the switch.
- `DIO_PERSIST=0` restores in-process persistence exactly. If the binary is missing or
  fails to bind, the launcher logs a warning and the engine falls back on its own.

`Dockerfile.base` needs no change: the new stubs use only `sys/epoll.h` from libc6-dev,
which the base image already has for the existing C stubs.

### Post-deploy defect: epoll op codes (fixed)

First Linux deployment failed with a permanent `Unix.EEXIST, "epoll_ctl"` loop on every
WebSocket reconnect.

Cause: `EPOLL_CTL_DEL` and `EPOLL_CTL_MOD` were swapped in the OCaml bindings. Deregistering
an fd therefore issued a MOD with an empty event mask, which the kernel accepts and which
leaves the fd registered. The OCaml side then dropped its own record of the fd, so the next
`ADD` on that fd returned `EEXIST`. Because a TLS/WebSocket handshake alternates read and
write interest on a live socket, this fired on every handshake and never cleared.

Three changes prevent a repeat:

1. The op codes are exported from `<sys/epoll.h>` by the C stub (`dio_epoll_ops`) and are
   never restated in OCaml.
2. `Epoll_poller.self_test` runs ADD → MOD → DEL → **ADD** → DEL on a socketpair at startup.
   The second ADD is the step that catches a DEL/MOD swap, since a MOD'd-away fd still
   accepts nothing but a MOD. If the probe fails, the engine logs and uses `select`.
3. `apply` is self-healing: `ADD` on `EEXIST` falls back to `MOD`, `MOD` on `ENOENT` falls
   back to `ADD`, and `DEL` on `ENOENT` is ignored. A registry/kernel disagreement can no
   longer wedge a live connection.

`test/engine/concurrency/test_io_backend.ml` pins the codes and the selected backend.

Operational note: `DIO_IO_BACKEND=select` remains the instant rollback, and both the IO
backend and the persistence mode are now logged at boot.

### Deliberate deviations

Three items were changed after implementation review. Each is a place where
following the plan literally would have added risk for no measurable gain.

**4c direct-TX domain: not implemented.** Once 4a (key cache) and 4d (no
`Lwt.pause`) are in, the egress chain is: domain enqueues → self-pipe wake →
epoll returns immediately → loop drains → sign → `write(2)`. A dedicated TX
domain would not shorten that: it would still sign once per order and still take
a cross-domain handoff, which is the same self-pipe. The dominant remaining cost
is the ECDSA operation itself, which neither design removes. Building one would
be a large, unverifiable change against a live venue for no latency gain, so it
is dropped rather than half-built. Revisit only if signing moves to a hardware
service or batching becomes useful.

**Kraken incremental sorted book: rejected, rebuild count reduced instead.** The
map is truncated to `orderbook_depth` (≈10) on every message, so
`levels_to_array` sorts ~10 elements — O(depth log depth), not the O(n log n)
hot spot the review assumed. An incremental sorted structure would duplicate the
hashtable and risk drifting from it. What *was* wasteful is building the arrays
three times per message; the CRC now reuses the arrays `make_orderbook` already
produced.

**`eval_args` / `route_exec` assoc lists: left as lists.** The result crosses
the interpreter's handler API (`(string * value) list`), and the allocs are per
*action invocation* and per *fill*, not per book tick. An array rewrite would
touch every handler for a rounding error.

**Lighter WS DOM parse: left in place.** The hot frames (`order_book`,
`account_all_orders`) are parsed *for* the downstream handler, which takes the
JSON tree. A prefix dispatch would only skip parses for frames that are
discarded, which are rare. The real structural fix would be to move Lighter's
parse onto `Parse_worker`, exactly as Kraken already does; that is a separate,
verifiable change and is not folded in here.

Still not done from the plan: the full venue-backed order-lifecycle state-machine
harness (Phase 0 item 2) and the GC pause-attribution instrument (Phase 5 item
1). Both are test/measurement scaffolding rather than production changes.

