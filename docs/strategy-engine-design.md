# Config-Driven Strategy Engine — Design

**Status:** Proposed
**Scope:** Design + implementation mapping. No code changes; this document only.
**Goal:** Let a user (self, future developers, and non-developer traders) define trading strategies without writing or editing OCaml — by authoring a *strategy file* that composes steps over a registered library of *actions*. The engine parses that file and runs its steps. In parallel, venue-specific integrity logic (hold netting, ghost detection, freshness semantics, reserve-dip guards) moves behind a centralized engine-level accounting module so every strategy and venue benefits uniformly.

---

## 1. Vision

Today a strategy is a hardcoded OCaml module (`Jacobs_ladder`, `Market_maker`) dispatched by string comparison inside `domain_spawner.ml`/`supervisor_orders.ml`. Adding a strategy means writing OCaml and rebuilding the engine.

The target model has exactly one code-side extension surface — the **action registry** — and everything user-facing is data:

- **Strategy files** (user-authored, separate from `config.json`) *are* the strategy. They describe triggers, state, params, and steps.
- **Steps** are ordered sequences of **actions** — named, schema-validated capabilities the engine provides.
- The **action registry** is where code is added. A new reusable capability = registering a new action.
- `config.json` only binds a strategy file to a symbol/exchange/instance.
- The engine reads the strategy file, compiles it once to native closures, and steps through it on each trigger.

Adding a new strategy = writing a new file that picks from the action library. No OCaml, no rebuild.

This is a strategy **programming model**: a declarative step/rule interpreter over registered action primitives.

### 1.1 Scope

**In scope:** per-symbol, per-instance decision strategies — Jacobs Ladder, Market Maker, and future user-authored strategies.

**Out of scope:** account-wide, venue-scale logic (e.g. fill interception/hedging). Such behavior does not fit the per-instance model and remains engine-owned; it is not expressed in strategy files. A separate design would be required to make account-scoped policy configurable.

## 2. The four layers

```
┌─────────────────────────────────────────────────────────────────┐
│ 3  STRATEGY FILES   (user-authored JSON, separate from config)  │
│    triggers · params · state · steps (guards → ordered actions) │
└──────────────────────────────┬──────────────────────────────────┘
                               │ compiled once to closures
┌──────────────────────────────▼──────────────────────────────────┐
│  STRATEGY PROTOCOL (interpreter core)                          │
│    parse + validate + compile the file → per-instance runtime,  │
│    ordered step runner, event-phase dispatch                    │
└──────────────────────────────┬──────────────────────────────────┘
                               │ calls registered actions
┌──────────────────────────────▼──────────────────────────────────┐
│ 2  STRATEGY_ACTIONS (registry — the only code extension point)  │
│    venue-agnostic decision primitives: compute_grid_price,      │
│    place_buy, place_sell, amend_buy, cancel_all, accumulate,    │
│    gate_balance, notify, ...                                    │
└──────────────────────────────┬──────────────────────────────────┘
                               │ uniform platform guarantees
┌──────────────────────────────▼──────────────────────────────────┐
│ 1  EXCHANGE PLATFORM  (Exchange_intf.S + central accounting)    │
│    venue facts: book, balances, orders, fees, ticks             │
│    UNIFORM invariants: hold-netted capacity, freshness,         │
│    grace windows, ghost detection, reserve-dip ceiling,         │
│    pending/dedup registry, reservation ledger                   │
└─────────────────────────────────────────────────────────────────┘

   Layer 4 (engine orchestration — domains, supervisor, order
   executor, persistence, fixtures) keeps its structure; the
   strategy dispatch point becomes data-driven, and it gains a
   dual-run (reference ‖ candidate) mode for the equivalence gate.
```

Responsibility split (see §5.2 for the precise ownership rule):

| Layer | Owns | Is authored by |
|---|---|---|
| Strategy files | triggers, params, policy state, steps | the user |
| Strategy protocol | interpreting/compiling/running files | engine code (once) |
| Strategy actions | decision primitives (price math, order intent, accounting policy) | engine code + future contributors |
| Exchange platform | venue facts + integrity invariants + pending/dedup/reservation | engine code + venue adapters (once) |
| Engine orchestration | loops, lifecycle, persistence, supervision, dual-run | engine code (existing) |

### 2.1 Illustrative example — a Jacobs Ladder-style decision as a strategy file

> Illustrative only. The authoritative behavior specification is the reference-action mapping table in §8.2; the syntax, control flow, token bindings, and platform-owned capacity queries shown here are what matter.

```jsonc
// strategies/golden_grid.json  — decision procedure (abridged)
{
  "name": "golden_grid",
  "version": 1,
  "triggers": ["book_update", "fill", "order_lifecycle", "oracle_publish"],
  "params": {
    "qty":              { "type": "decimal_str", "default": "0.00025" },
    "grid_interval":    { "type": "range",       "default": [0.16, 0.16] },
    "sell_mult":        { "type": "range",       "default": [0.98, 0.98] },
    "amend_cooldown_s": { "type": "float",       "default": 30.0 }
  },
  "state": {
    "tracked_buy":        { "type": "buy_intent?", "persist": true },
    "reserved_base":      { "type": "float",       "persist": true },
    "accumulated_profit": { "type": "float",       "persist": true },
    "last_amend_at":      { "type": "float?",      "persist": false }
  },
  "steps": [
    {
      "id": "place_initial_buy",
      "when": { "event": "book_update",
                "all": [
                  { "is_none": "$state.tracked_buy" },
                  { "not": { "pending": "buy" } },
                  { "capacity": { "quote_gte": "$price * $params.qty_f" } },
                  { "not": { "engine": { "capital_halted": true } } }
                ] },
      "then": [
        { "action": "compute_grid_price",
          "args": { "ref": "$price", "lo": "$params.grid_interval_lo",
                    "hi": "$params.grid_interval_hi", "side": "below" },
          "bind": { "buy_px": "$out.price" } },
        { "action": "place_buy",
          "args": { "qty": "$params.qty_dec", "price": "$local.buy_px",
                    "post_only": true, "dedup_key": "buy:initial" },
          "bind": { "tok": "$out.token" } },
        { "action": "track_buy", "args": { "token": "$local.tok", "price": "$local.buy_px" } }
      ],
      "stop": true
    },
    {
      "id": "amend_trailing_buy",
      "when": { "event": "book_update",
                "all": [
                  { "is_some": "$state.tracked_buy" },
                  { "expr": "$price < $state.tracked_buy.price * (1.0 - 0.001)" },
                  { "cooldown_elapsed": { "since": "$state.last_amend_at",
                                          "seconds": "$params.amend_cooldown_s" } }
                ] },
      "then": [
        { "action": "compute_amend_price",
          "args": { "ref": "$price", "lo": "$params.grid_interval_lo" },
          "bind": { "amend_px": "$out.price" } },
        { "action": "amend_buy",
          "args": { "token": "$state.tracked_buy.token", "price": "$local.amend_px",
                    "dedup_key": "buy:amend" } },
        { "action": "set_time", "args": { "state": "last_amend_at" } }
      ]
    },
    {
      "id": "place_sell_on_buy_fill",
      "when": { "event": "fill", "side": "buy" },
      "then": [
        { "action": "compute_sell_price",
          "args": { "base": "$event.fill_price", "mult": "$params.sell_mult_lo" },
          "bind": { "sell_px": "$out.price" } },
        { "action": "place_sell",
          "args": { "qty": "$event.fill_qty", "price": "$local.sell_px",
                    "dedup_key": "sell:$event.fill_order_id" } },
        { "action": "accumulate", "args": { "qty": "$event.fill_qty",
                                            "profit": "$event.realized" } }
      ]
    }
  ]
}
```

And `config.json` shrinks to bindings:

```jsonc
{ "instances": [
    { "strategy_file": "strategies/golden_grid.json",
      "exchange": "hyperliquid", "symbol": "BTC/USDC",
      "params": { "grid_interval": [0.16, 0.16] }, "mode": "live" }
] }
```

A new strategy = a new file picking from the action library.

## 3. Strategy file schema

One file per strategy definition. Distinct from `config.json`, which only binds files to instances.

### 3.1 Triggers and the event model

The engine reads ordered events from ring-buffer cursors; the protocol presents that stream to a compiled instance in event form:

- Each cycle, the protocol hands the instance the **same drained event lists, in the same order**, that the current code reads. No new cursors and no extra allocation beyond the loop's existing reads.
- At compile time, steps are bucketed by trigger into an **array indexed by event kind → step indices**, so dispatch is O(steps-for-that-trigger) with no hashing or JSON work in the hot path.
- The protocol runs trigger groups in a **canonical cycle order** matching the domain loop's read/handle order. The order is a compile-time constant pinned by the differential harness (§8.3).

Known triggers:

| Trigger | Source | `$event` binding |
|---|---|---|
| `fill` | execution ring buffer | fill price, qty, side, order id, realized |
| `order_lifecycle` | execution/order snapshot changes | order id, new status, amend/cancel result |
| `balance_update` | newest balance snapshot | asset, total, available, age |
| `oracle_publish` | capital oracle / F&G | signal values |
| `book_update` | top-of-book snapshot | none (`$price` is the snapshot ref) |

`book_update` is the snapshot phase: it runs once per cycle after ordered events, where the current code re-evaluates the book. Steps filter further with `when` guards. Event payload schemas are fixed by the protocol; `$event.<field>` resolves against the table above. This keeps expression typing tractable and the hot path allocation-free.

### 3.2 Params

Instance-overridable tuning values. Typed kinds:

- `float` / `int` / `bool` / `string`
- `decimal_str` — a string parsed to a float at compile (`qty`) and kept as its string form for wire submission (venue size precision). `$params.<p>_f` is the float, `$params.<p>_dec` is the original string.
- `range` — a `[lo, hi]` pair; exposes `$params.<p>_lo` / `$params.<p>_hi`. Used where a rule has a low action (snap-amend) and a high action (re-anchor).
- `enum [a, b, c]`

Params can be overridden per-instance in `config.json`. All substitutions happen at compile; no runtime string parsing.

### 3.3 State

Declared per-strategy **policy state** (see §5.2 for the ownership rule). Scalars and small intent records:

- `type` — `float`, `float?`, `int`, `bool`, `string`, `buy_intent?` (a tracked order token + price), `sell_intent?`, plus a policy `reserve_policy`.
- `persist` — hooks into the persistence orchestrator (`Base_accumulation_store` and friends); the mapping from declared fields to concrete store records is explicit (§3.3.1).

Composite integrity structures (`sell_stack`, reservation ledger, pending registry) are platform-owned and read via `$platform`; they are not declared as strategy state.

State is scoped per instance and compiled 1:1 to the current strategy state fields so behavior mapping is verifiable (§8).

#### 3.3.1 Persistence mapping

Every persisted field maps to a concrete store key so the equivalence harness can compare persistence writes against the reference:

- `reserved_base` → `Base_accumulation_store.reserved_base`
- `accumulated_profit` → `Base_accumulation_store.accumulated_profit`
- `tracked_buy` → the reference's `last_fill_oid` / tracked-buy slot
- bootstrapping follows the reference's boot sequence (`last_fill_oid`, streak self-heal, reserved_base bootstrap).

A **persistence mapping table** is maintained alongside the reference-action mapping table (§8.2) and is bidirectional: every current store key has a declared field, and every persisted field has a store key. Mismatch fails validation.

### 3.4 Steps and control flow

A strategy runs an **ordered decision procedure** per trigger group. Each step is:

```
step := {
  "id"   : <string>,                  // stable id, used in traces
  "when" : <guard>,                   // evaluated against LIVE state
  "then" : [ <action>, ... ],         // run if guard passes
  "else" : [ <action>, ... ]?,        // run if guard fails (optional)
  "let"  : { <var>: <expr>, ... }?,   // pure derived bindings, evaluated before "when"
  "stop" : <bool>?                    // default false; true ends the cycle
}
```

Semantics (deterministic):

1. Steps run in file order.
2. `let` bindings evaluate first, against live state, and are visible to `when` and to `then`/`else`.
3. `when` sees all mutations made by earlier steps in this cycle (live state), not a snapshot.
4. **All** matching steps run, unless a step sets `stop: true` (or a `stop` action fires), which ends the cycle immediately. This models statement sequencing with early return.
5. `else` gives if/else branching without a separate construct.
6. Guards are side-effect-free. Only `then`/`else` actions mutate state.

Derived conditions (`trail_up`, `zone_violation`) are either expressed directly in `expr` or pre-computed by a registered action bound via `bind` and consulted in a later guard. The grammar stays small; complexity moves into typed, testable actions.

Guard grammar:

```
guard := { "event": <event> } | { "side": "buy"|"sell" }        // match current dispatch
       | { "all": [ <guard>, ... ] } | { "any": [ ... ] } | { "not": <guard> }
       | { "is_none":  <ref> } | { "is_some": <ref> }
       | { "expr": <bool-expr> }                                 // comparisons/arithmetic over refs
       | { "capacity": { <asset>: <pred> } }                     // platform-answered capacity
       | { "pending": <dedup-prefix> }                           // platform-owned pending/dedup query
       | { "order": { "posture": ... } }                         // open-order posture (via platform)
       | { "signal": { <name>: <pred> } }                        // oracle/external signals
       | { "engine": { "capital_halted": <bool> } }              // engine/account-level flags
       | { "cooldown_elapsed": { "since": <ref>, "seconds": <expr> } }
```

`cooldown_elapsed` covers amendment cooldowns; timestamps live in `float?` state set by `set_time`. `$now` is available for direct time expressions.

### 3.5 Expression references

Uniform resolution syntax, compiled once to a closure tree:

- `$price` — current top-of-book reference for the symbol
- `$event.<field>` — field of the triggering event (schema from §3.1)
- `$state.<var>` — instance policy state
- `$params.<p>` / `_f` / `_dec` / `_lo` / `_hi` — effective param forms
- `$signal.<name>` — oracle/external signals
- `$local.<var>` — step-local `let`/`bind` values
- `$now` — current time (seconds)
- `$platform.<cap>` — platform facts: `available_sell`, `balance_age`, `generation`, `is_ghost`, `open_sells`, `pending`

Numeric literals in arithmetic are float-parsed at compile. No JSON walking or dynamic dispatch in the hot path.

## 4. Strategy_actions registry

The single code-side extension surface.

### 4.1 Signature

```ocaml
(* strategy_actions.ml *)

type arg_kind =
  | A_float | A_int | A_bool | A_string | A_decimal_str
  | A_enum of string list
  | A_expr                                     (* resolved at compile; not a literal *)

type schema_entry =
  { arg      : string
  ; kind     : arg_kind
  ; required : bool
  }

type class_ = Decision | Effectful | Read

type t =
  { name    : string
  ; class_  : class_
  ; schema  : schema_entry list
  ; handler : ctx -> args -> out Lwt.t         (* native closure *)
  }

and ctx  = { instance : instance; event : event option; now : float }

(* Args and outputs are SLOT-INDEXED, never string-keyed, on the hot path: the
   compiler rewrites every "$..." reference and every "$out.<field>" to a
   compile-time slot index. The schema below is consulted only by the loader
   and validator. String-keyed access must not appear in a handler's hot path,
   so execution stays allocation-bounded (see §6.4). *)
and args = { slots : value array }             (* positional; resolved at compile *)
and out  = { slots : value array }             (* positional; "$out.<field>" → index *)

val register : t -> unit                        (* rejects duplicate names at startup *)
val find     : string -> t option
val all_names : unit -> string list
```

- **Schema** — validated at load: every invocation's `args` satisfies the action's schema; `A_expr` args must be expressions, literal args must type-check.
- **Handler** — the implementation; existing strategy functions are wrapped to become the initial inventory.
- **Registration** — new capabilities are registered here; all existing strategy files can use them.

### 4.2 Action classes and the async/pending contract

| Class | Examples | Returns | Notes |
|---|---|---|---|
| **Read** | `read_book`, `read_capacity`, `read_open_orders` | immediate value(s) | no side effects |
| **Decision** | `compute_grid_price`, `compute_sell_price`, `compute_amend_price` | immediate value | pure/in-memory |
| **Effectful** | `place_buy`, `place_sell`, `amend_buy`, `cancel_all` | a **pending token** | enqueued via supervisor |

Effectful rules:

1. **Non-blocking, token-returning.** `place_buy` does not block on a venue round-trip and does not return a confirmed venue order id. It allocates a pending token (client id/dedup key), enqueues the request through the supervisor/order-executor path, records the intent in the platform pending registry, and returns `{ token }` immediately. `$out.token` is a token, not a venue id.
2. **Platform-owned dedup.** Every effectful action requires a `dedup_key` expression. The platform rejects a duplicate enqueue for an outstanding dedup key within its window. Guards should also consult `{ "pending": <prefix> }` (§3.4) so a step does not attempt a duplicate.
3. **Reserve-before-place / release-on-failure.** A placement action atomically reserves capacity (platform accounting) before enqueue and releases on rejection/failure, mirroring `atomic_check_and_reserve`. The reservation is platform state, not strategy state.
4. **Failure semantics.** Step execution is best-effort sequential. Each step may declare `"on_error": "stop" | "continue"` (default `stop`): `stop` abandons the rest of that step's action list; the cycle continues with subsequent steps. Prior effects are not rolled back. Terminal errors surface to the health path.
5. **Reentrancy.** Effectful actions are non-blocking, so the next cycle may run while a prior request is in flight. Guards express in-flight awareness via `{ "pending": ... }` and `$state` tokens.

### 4.3 Initial action inventory

Starting inventory (all wrap existing code):

- **Read/derive:** `compute_grid_price`, `compute_sell_price`, `compute_amend_price`, `read_book`, `read_capacity`, `read_open_orders`
- **Effectful:** `place_buy`, `place_sell`, `amend_buy`, `cancel_all`, `cancel_order`
- **Accounting/state:** `accumulate`, `track_buy`, `track_sell`, `set_time`, `set_cooldown`, `update_reserved_base`, `notify_oracle`
- **Posture/guards:** `gate_balance`, `gate_capital_halted`, `is_ghost`
- **Recovery:** `reconcile_position`, `reconcile_persisted_sell_levels`

Finalized during the behavioral mapping (§8.2): every row in the mapping table must resolve to a registered action, and no action may exist that is not referenced by a mapping row or marked as a user-facing extension.

## 5. Platform integrity layer (central accounting module)

**Decision:** venue-fighting invariants are platform guarantees, implemented once at engine level, driven by per-venue **capability descriptors**. Every strategy and venue inherits them.

`Exchange_intf.S` (`src/external/exchange_intf.ml:252`) already exposes much of the raw material:

- `get_tradeable_balance`, `get_available_balance_fast` — `available_balance_fast` is the venue-authoritative immediately-sellable figure; equal to tradeable for hold-netted venues (Hyperliquid), returns venue `qty_available` for gross venues (Alpaca).
- `get_balance_age_fast` — freshness of the balance snapshot.
- `get_open_orders_generation` — changes when the open-orders snapshot changes.
- `place_order` / `amend_order` / `cancel_orders` with retry configs; precision/fee metadata.

What migrates down from the grid strategy:

- **Sell-hold netting grace** — `sell_hold_netting_grace_s = 15.0` (`jacobs_ladder_execution.ml:206`)
- **Buy-ack ghost grace** — `buy_ack_ghost_grace_s = 15.0` (`jacobs_ladder_execution.ml:225`)
- **Ghost placement/cancel detection** — `jacobs_ladder_events.ml:933-1047`
- **Freshness cutoffs** — `jacobs_ladder_execution.ml:208-256`
- **Reserve-dip ceiling** — sizing a sell above `reserved_base` is a platform-enforced ceiling
- **Capability flag matrix** — `jacobs_ladder_config.ml:10-15` (`use_reserved_base_guard`, `use_unnetted_sell_hold`, `balance_nets_open_order_holds`, `hold_netted_from_venue_state`, `merge_preserved_sells`)

### 5.1 Design

```
 strategy / actions
        │  uniform queries: "how much can I truly sell?",
        │  "is my balance fresh?", "is this order a ghost?",
        │  "is this dedup key pending?"
        ▼
 CENTRAL ACCOUNTING MODULE (engine-level, implemented once)
        │  grace windows · ghost detection · freshness semantics
        │  reserve-dip ceiling · capacity derived from descriptors
        │  pending/dedup registry · reservation ledger
        ▼
 per-venue capability descriptors
        │  does the venue net holds? report gross? expose qty_available?
        │  track ack time? expose a generation counter?
        ▼
 venue adapters (hyperliquid, alpaca, lighter, ibkr)   /   raw feed data
```

- The accounting module is a shared engine library: adapters hand it facts; strategies consult it uniformly.
- Per-venue capability descriptors answer "what does this venue actually guarantee?" in one place.
- Strategies and actions never encode venue-specific branches.
- The grid overlay is split: invariant logic moves into the accounting module; decision logic becomes actions (§8.2).
- Behavior preservation is a precondition of the equivalence gate. The accounting module is a new home, not a rewrite: each migrated calculation must reproduce the reference's exact conservativeness, verified by the harness (§8.3).

### 5.2 Ownership rule (single source of truth)

| Concern | Owner | Access |
|---|---|---|
| Capacity / available-to-sell, balance freshness, ghost status | platform | `$platform.*`, enforced in effectful actions |
| Pending/dedup registry, reservation ledger | platform | `{ "pending": ... }`, `$platform.pending`; enforced in effectful actions |
| Sell-stack integrity (netting, nesting, merge) | platform | read-only `$platform.open_sells` |
| `reserved_base` value (policy: how much profit to protect) | strategy | `$state.reserved_base` |
| Enforcement that sells do not dip below the strategy's reserved ceiling | platform | inside `place_sell` via the reserve-dip ceiling |
| Tracked intents, cooldowns, accumulation policy, amend rules | strategy | `$state.*` |

Consequences:

- `sell_stack` is a platform view, not strategy state. The `merge_preserved_sells` flag is a capability descriptor on the instance, not a property of strategy-owned state.
- `reserved_base` is the strategy's number; the platform enforces it. No duplicate copies, no divergence.
- The instance's `flags` block (in `config.json` or the strategy file) selects capability descriptors; it is not strategy logic.

## 6. Engine integration

### 6.1 Dispatch becomes data-driven

Today, dispatch is hardcoded string-matching:

- `domain_spawner.ml:290-308` — `is_grid_strategy` / `is_mm_strategy`
- `domain_spawner.ml:1425-1467` — `Jacobs_ladder.Strategy.execute` / `Market_maker.Strategy.execute`
- `domain_spawner.ml:589-601, 657-714, 756-798` — per-strategy event handler branches
- `domain_spawner.ml:1786-1795` — hardcoded `Strategy.init ()` calls
- `supervisor_orders.ml:151-412, 802-808` — hardcoded grid/mm callbacks and pending-order drains

These become: an instance is compiled from its strategy file once at startup; the loop dispatches to the compiled step runner. The `STRATEGY` protocol (`strategy_common.ml:572`) remains the external contract — execute, lifecycle handlers, pending-order drain — with the compiled instance as its content.

### 6.2 No hot-path interpretation

- Parse, validation, schema checks, guard/expression compilation happen once at startup.
- Compiled form: a guard is a `ctx -> bool`; an action is a partial application of the registered handler with resolved args; each trigger group is an array of compiled steps.
- The per-cycle path stays allocation-bounded (`iter_top_of_book_events`, `iter_open_orders_fast`, generation skips preserved and exposed via `$platform`).

### 6.3 Lifecycle, supervision, and dual-run

- Lifecycle (init/bootstrap, start, health, persist, shutdown) mirrors today's `Strategy.init`; state bootstraps from persistence per §3.3.1.
- Supervisor, order-executor, fixtures, and dashboard observe the same order stream and state.
- Dual-run mode: the loop can run a candidate alongside a frozen reference in the same domain, feeding both the identical drained events and comparing observable outputs. This is a test-gated mode, not a production path.

### 6.4 Preserved loop semantics (HFT invariants)

The compiled instance slots into the existing hot loop without changing when or how often strategy work happens. These are non-negotiable; violating any of them breaks either the HFT profile or equivalence.

- **Single invocation per dirty cycle, at the same call site.** The loop is event-driven via a dirty flag: `should_execute_strategy` (`domain_spawner.ml:185`) is set when new data warrants work (`:618-1193`) and cleared after executing (`:1269`). The compiled instance is invoked exactly once per cycle when `should_execute` holds — `exec_ready && should_execute_strategy && has_exec_fn && not equity_market_closed && oracle_gate_open` (`domain_spawner.ml:1248-1254`) — the same point `execute_strategy` is called today. Trigger grouping (§3.1) is an internal partitioning of that single invocation; it does not change the invocation cadence. Idle cycles still do no decision work.
- **Engine-level gates stay in the loop.** `exec_ready`, `oracle_gate_open` (including the "buy withheld, sell still runs" asymmetry under oracle halt, `domain_spawner.ml:1243-1247`), `equity_market_closed`, `has_exec_data`, and `capital_halted` are account/engine gates applied *around* the strategy call. They remain loop-owned and are surfaced read-only where needed (e.g. `{ "engine": ... }`); they are not strategy state and must not be re-derived in files.
- **Latency and allocation instrumentation preserved.** The loop measures PREP vs STRAT separately with `Monotonic_clock.now_ns` and `Gc.minor_words` (`domain_spawner.ml:1255-1266`). Per-cycle execution must stay allocation-bounded so those budgets hold: arg/output binding is compile-time slot-indexed (§4.1), guards are compiled closures, and the same fast-path iterators (`iter_open_orders_fast`, `iter_top_of_book_events`, generation skips) are used.
- **No new cursor reads.** The instance consumes the event stream the loop already drains; it adds no ring-buffer reads or allocations on the hot path.

The equivalence gate (§8) holds these gates and the invocation cadence identical between reference and candidate, so any divergence is attributable to the strategy file or actions, never to loop timing.

## 7. Validation & safety

- **Static validation** at load (fail-fast): action names registered; args satisfy schemas; `$ref` resolve; guard predicates/expressions type-check; triggers ⊆ known events; dedup keys present on effectful actions; persisted fields have store mappings (§3.3.1); no undeclared state/property.
- **`dio strategy validate <file>`**: static validation plus a compile + single synthetic event, no exchange connection.
- **Paper mode**: step runner executes against a simulated venue using the same platform queries; no real orders.
- **Dry-run on stored history**: replay recorded history through the compiled instance in shadow mode; log would-have-done actions.
- **Shadow/canary**: dual-run vs the frozen reference; compare traces; promote only on equivalence pass.
- **Fail-safe**: parse/validation failure surfaces through the health path and disarms the instance rather than trading unvalidated.

## 8. Behavioral equivalence — 100% replication

**Requirement:** the config-driven engine must reproduce the current trading behavior of the reference strategies for identical input. For identical market input, identical decisions → identical effects.

### 8.1 Equivalence, defined at the observable boundary

Equivalence is defined at the observable boundary and proven by a harness against a frozen reference.

- **Frozen reference** — a snapshot (git tag/branch, modules retained under `reference/`) of the pre-migration `Jacobs_ladder` and `Market_maker`. It is the oracle, retained until its port passes the gate.
- **Candidate** — the compiled strategy file.
- **Observable boundary** — the effects compared:
  1. order-intent messages, compared on semantic fields (side, qty, price, symbol, post_only, reduce_only, time_in_force) — not literal bytes (client ids, nonces, timestamps differ by construction);
  2. end-of-cycle strategy state;
  3. persistence writes (per the mapping table, §3.3.1);
  4. ordering of side effects.
- **Equivalence** — for identical recorded input and identical starting state, reference and candidate produce identical effects on every observable above.

The loop's engine gates and invocation cadence (§6.4) are held identical for reference and candidate, so divergence is attributable only to the strategy file or actions — never to loop timing or gating.

### 8.2 Reference-action mapping table (authoritative behavior spec)

Before porting, each reference's behavior is enumerated exhaustively: every code path → (trigger, guard, action sequence, state effect, persistence effect). Required grid coverage:

| Current path | Trigger | Guard (abridged) | Action sequence |
|---|---|---|---|
| Initial/next buy placement | book_update | no tracked buy, no pending, quote capacity, not capital-halted | compute_grid_price → place_buy → track_buy |
| Buy amendment | book_update | trail_up or zone_violation, cooldown elapsed | compute_amend_price → amend_buy → set_time |
| Buy-fill → sell placement | fill(buy) | — | compute_sell_price → place_sell → track_sell |
| Sell-fill profit accrual | fill(sell) | — | settle_hold → accumulate → update_reserved_base |
| Position reconcile | lifecycle/balance | balance fresh | reconcile_position |
| Ghost detection/recovery | order_lifecycle | ghost grace expired, no ack pending | is_ghost → recover |
| Sell-stack merge/nesting | sell placement fill | merge_preserved_sells (descriptor) | merge_sell_levels |
| Exhaust-buy cancel sweep | order_lifecycle | excess buys | cancel_excess_buys |
| Reserved-base dip guard | sell sizing | capacity ceiling | gate_balance |

This table is a starting skeleton. The grid's execution module is ~2000 lines and every branch must appear. A row with no registered action is a hard blocker; an action not referenced by a row must be marked as a user-facing extension. An equivalent table is produced for Market Maker. The persistence mapping table (§3.3.1) is maintained alongside.

### 8.3 Differential replay harness

- **Recorder**: instrument the loop to capture the ordered drained events + snapshots per cycle (orderbook, execution, balance/cancel/amend lifecycle, sequencing) into a replay log — for live runs and for scenarios.
- **Runner**: replay the identical stream through reference and candidate, identical starting state.
- **Diff**: per cycle, compare the §8.1 observables; any divergence fails, with cycle and field reported.
- **Corpus**: live-collected cases plus adversarial synthetic cases (fill/cancel races, stale-high amend holds, ghost windows, bursts of sells before balance adopts holds, reboot mid-cycle).
- **Acceptance**: a port is done only on a full pass. The harness is the long-term regression suite for the engine itself.

### 8.4 Staged validation and canary gating

The migration is validated in stages, each gated:

1. **Platform migration is validated as a behavior-preserving refactor.** Before any config instance runs, the accounting module must reproduce the reference's observable behavior on the recorded corpus (a refactor equivalence test). This is what makes §8's guarantee hold despite the code moving.
2. **Candidate config is validated by the harness** against the frozen reference (§8.3).
3. **Shadow (dual-run)** in live conditions catches differences the corpus missed; divergence disarms.
4. **Promotion** is per-instance via `use_config_driven_strategies`, default off, reversible.

"100% replication" is a release gate: nothing ships that changes live trading behavior.

## 9. Implementation map

Concrete module and integration work, with per-module status. Milestone 1's first slice (registry + file parser + `validate` CLI) is implemented on branch `composer`; the interpreter runtime and everything downstream are pending.

### 9.1 New modules

| Module | Responsibility | Layer | Status |
|---|---|---|---|
| `strategy_actions.ml` | registry, `t`, schema types, `register`/`find` | protocol | done |
| `strategy_actions_builtin.ml` | declares the §4.3 inventory (schemas) | actions | done (metadata; handlers pending) |
| `strategy_file.ml` | JSON → AST (triggers/params/state/steps) | protocol | done |
| `strategy_expr.ml` | expression engine: `$ref` scanning + tokenizer/parser/evaluator (arithmetic, comparison, boolean) and string templates | protocol | done |
| `strategy_compile.ml` | static validation against the registry | protocol | done (validation); compile-to-closures pending |
| `strategy_cli.ml` | `dio strategy validate <file>` | tooling | done |
| `strategy_guard.ml` | guard evaluation over an expression `env` + facts (event/side/capacity/pending/engine/cooldown) | protocol | done (closure compile pending) |
| `strategy_runtime.ml` | per-instance state, env, cycle runner, action dispatch, step-local bindings | protocol | done (synthetic; exchange handlers pending) |
| `strategy_protocol.ml` | implements `Strategy_common.S` over a compiled instance | protocol | pending |
| `platform_accounting.ml` | central accounting: grace, ghost, freshness, ceilings, pending, reservation | platform | pending |
| `exchange_capabilities.ml` | per-venue capability descriptors (extracted from the grid flag matrix) | platform | done |
| `strategy_event_recorder.ml` | record per-cycle observations (order intents/state/persistence) into a trace | test | skeleton (loop instrumentation pending) |
| `strategy_equivalence.ml` | trace diff / equivalence decision | test | skeleton (ref-vs-ref test passes) |
| `strategy_trace.ml` | observable trace types + comparison | test | done |

### 9.2 Existing modules to touch

| File | Change |
|---|---|
| `config.ml` | instance binding to `strategy_file` + param overrides + descriptor flags; keep `trading_config`, shift `strategy` tag semantics (`config.ml:504` `read_config`) |
| `domain_spawner.ml` | replace `is_grid_strategy`/`is_mm_strategy` and dispatch branches (290-308, 589-798, 1425-1467, 1786-1795) with compiled-instance dispatch; add dual-run mode |
| `supervisor_orders.ml` | generalise callbacks/drains (151-412, 802-808) to the protocol; expose pending/dedup + reservation to `platform_accounting` |
| `strategy_common.ml` | finalize the `S` signature (572) to the compiled-instance contract; retain in-flight caches/ring buffer as platform-owned |
| `order_executor.ml` | expose reservation/pending/dedup hooks used by effectful actions |
| `jacobs_ladder/*` | extract actions into `strategy_actions_builtin`; migrate overlay to `platform_accounting`; retain as frozen reference |
| `market_maker.ml` | extract actions; retain as frozen reference |
| `src/engine/strategies/dune`, `src/engine/dune` | register new modules/libs — **done** for the protocol/tooling modules |
| `bin/main.ml` | load strategy files; wire `dio strategy validate` — **CLI wired** (early intercept before `Arg.parse`); instance loading pending |

### 9.3 Milestones and gates

| # | Milestone | Work | Gate |
|---|---|---|---|
| 0 | Frozen reference + harness skeleton | snapshot reference modules; recorder; trace/diff comparator | harness reproduces reference traces (ref vs ref) |
| 1 | Registry + interpreter | §9.1 protocol modules; builtin inventory stubs; `validate` CLI | synthetic strategy files compile, validate, run deterministically; feature flag off = no live change |
| 2 | Platform accounting + capabilities | extract grid overlay; descriptors; move flag matrix | refactor equivalence: accounting outputs match reference on corpus (§8.4.1) |
| 3 | Grid port | mapping tables (§8.2 + persistence); remaining actions; author grid file; wire dispatch behind flag | full differential pass; STRAT-phase allocation/latency parity within the reference budget (§6.4); shadow parity; canary |
| 4 | Market Maker port | same mapping → actions → file | full differential pass; allocation/latency parity; canary |
| 5 | Tooling | paper mode, dry-run/shadow on stored history, dashboard surfacing | paper/dry-run validated against corpus |

Dependency order is strict: 1 before 3; 2 before 3 (the accounting module is what effectful actions call); 3 before 4.

### 9.4 Status and local dev

**Implemented (branch `composer`), milestone 1 target:**

- `strategy_actions.ml` — action registry; `strategy_actions_builtin.ml` — declarations for the §4.3 inventory (metadata; handlers pending).
- `strategy_file.ml` — JSON → AST; `strategy_compile.ml` — static validation; `strategy_cli.ml` — `dio strategy validate`.
- `strategy_expr.ml` — expression engine (refs, arithmetic, comparison, boolean, string templates).
- `strategy_guard.ml` — guard evaluation; `strategy_runtime.ml` — state, env, cycle runner, action dispatch, bindings; executes a compiled strategy against synthetic events and returns an ordered action trace.
- `bin/main.ml` — CLI intercept before `Arg.parse`, so validation runs without booting the engine.
- Tests: `test_strategy_compose.ml` (parse + validation) and `test_strategy_runtime.ml` (expression/guard evaluation + synthetic cycle execution). Sample file: `strategies/golden_grid.json`.

**Remaining for milestone 1:** the `Strategy_common.S` protocol adapter (`strategy_protocol.ml`) and wiring a compiled instance into the domain loop behind the default-off feature flag. Milestones 0 and 2–5 are not started.

**Milestone 0 skeleton (branch `composer`):** `strategy_trace.ml` (observable trace types + comparison), `strategy_event_recorder.ml` (per-cycle recording), and `strategy_equivalence.ml` (trace diff) are implemented with a ref-vs-ref identity test, a state order-insensitivity test, and divergence-detection tests (`test_strategy_harness.ml`). Live-loop instrumentation and dual-run capture remain.

**Milestone 2 (in progress):** `exchange_capabilities.ml` extracts the grid's per-venue flag matrix (`jacobs_ladder_config.ml`) into capability descriptors for hyperliquid/kraken/ibkr/lighter/alpaca. The central accounting module (`platform_accounting.ml`), migration of the grid overlay, and the refactor-equivalence gate remain.

**Usage:**

```
./_build/default/bin/main.exe strategy validate strategies/golden_grid.json
```

Exit codes: `0` valid, `1` errors, `2` usage.

**Local test config:** `config.json` is gitignored and set to the Alpaca testnet Ladder instances (BOTZ, LIT, REMX, SMH), so local runs do not touch the live engine.

**Tooling:** the `.ocamlformat` `version` pin was removed so the OxCaml-patched `ocamlformat` on the `5.2.0+ox` switch can format the tree (`profile = janestreet`, `comment-check = false`). `dune build @fmt` is clean repo-wide.

## 10. Open items

- Final expression/guard grammar surface — keep minimal; prefer registered predicates over new syntax.
- Capability descriptor encoding (OCaml record vs per-venue static table) and the 1:1 mapping from the current flag matrix.
- Persistence mapping table completion (store keys ↔ declared fields), including boot sequences.
- Canonical cycle-order confirmation against the domain loop, locked by harness tests.
- Event recorder cost in live mode (sampling vs full capture) and replay-log format.
