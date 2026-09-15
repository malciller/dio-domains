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

> Illustrative only. The authoritative behavior specification is the reference-action mapping table in §8.2; the syntax, control flow, token bindings, and platform-owned capacity queries shown here are what matter. The grid's *shipped* file (`strategies/jacobs_ladder.json`) is currently the coarse orchestration (`book_update → grid_cycle`, §9.4); this fine-grained example is the target shape for the later decomposition.

```jsonc
// strategies/example.json  — decision procedure (abridged)
{
  "name": "example",
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
{ "trading": [
    { "exchange": "hyperliquid", "symbol": "BTC/USDC"
    , "strategy": "jacobs_ladder"
    , "strategy_file": "strategies/jacobs_ladder.json"
    , "qty": "0.00025", "grid_interval": [0.16, 0.16] }
] }
```

A new strategy = a new file picking from the action library.

## 3. Strategy file schema

One file per strategy definition. Distinct from `config.json`, which only binds files to instances.

Strategy names are **user-defined and opaque to the engine** — the engine never canonicalizes, aliases, or matches on a built-in name. A `config.json` entry binds a strategy file via `strategy_file`, and at startup the engine requires the entry's `strategy` to equal the file's `name` exactly. A mismatch (or an invalid/unreadable file) logs `CRITICAL` and exits `1` (§7). Keeping the two in sync is the user's responsibility.

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

Two stores, both keyed `"{strategy}:{symbol}:{venue}"` (so the config's strategy name is part of the key):

| Store (file) | Store field | Strategy-file `state` declaration | Notes |
|---|---|---|---|
| `Base_accumulation_store` (`accumulation_state.json`) | `reserved_base : float` | `reserved_base : float, persist` | base accumulated via `sell_mult`; excluded from sellable balance |
| | `accumulated_profit : float` | `accumulated_profit : float, persist` | realized local PnL in quote |
| | `last_fill_oid : string option` | part of `tracked_buy` / last-fill state | most recent fill ref; **boot key** |
| | `last_buy_fill_price/qty : float option` | `tracked_buy` | |
| | `last_sell_fill_price/qty : float option` | `tracked_sell` | |
| `Sell_levels_store` (`sell_levels_state.json`) | `levels : (price, qty) list` | `persisted_sell_levels` | pending-sell levels (Alpaca remaintain) |

Per-entry opt-ins: `base_accumulation` (default true) and `sell_levels` (default false) gate whether these stores are read/written at all.

Bootstrapping must reproduce the reference's boot sequence exactly (`last_fill_oid`, streak self-heal, `reserved_base` bootstrap), since the harness compares persistence writes.

The mapping is **bidirectional**: every current store key has a declared field, and every persisted field has a store key. Mismatch fails validation.

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
- **Config ↔ strategy-file name match** at startup: each `config.json` entry's `strategy` must equal the bound strategy file's `name`. A mismatch (or an invalid/unreadable file) logs `CRITICAL` and exits `1`. Names are user-defined; the engine does not canonicalize them.
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

Before porting, each reference's behavior is enumerated: every code path → (trigger, guard, action sequence, state effect, persistence effect). This is the **function-level inventory** for the grid; per-branch guards are pinned against the frozen reference during porting (the harness catches any omission). A row with no registered action is a hard blocker; an action not referenced by a row must be marked as a user-facing extension.

**Entry / orchestration** (`jacobs_ladder_execution.ml`, `jacobs_ladder_events.ml`)

| Current path | Trigger | Guard (abridged) | Action sequence | State / persistence effect |
|---|---|---|---|---|
| `Strategy.execute` → `execute_strategy` | book_update (snapshot phase) | loop `should_execute` gate | sync → buy leg → sell leg → cleanup | orchestration |
| `sync_open_orders` | order_lifecycle / each execute | open-orders generation changed | read_open_orders → reconcile ledgers → ghost-buy check | open-buy tracking, `sell_commitments`, `feed_locked_sell_base`, position ledger |
| `cleanup_pending_and_cooldowns` | each execute | — | expire cooldowns/in-flight; arm reclaims | clears stale tokens |
| `enqueue_event` / `drain_events` / `dispatch_event` | each execute / fill | — | drain lifecycle queue | — |
| `flush_persistence` | each execute (dirty) | `base_accumulation` / `sell_levels` | persist | `accumulation_state.json`, `sell_levels_state.json` |

**Position ledger** (`jacobs_ladder_execution.ml`)

| Current path | Trigger | Guard (abridged) | Action sequence | State / persistence effect |
|---|---|---|---|---|
| `reconcile_position` | balance_update | non-NaN asset balance | reconcile_position | adopt venue figure; `last_seen_asset_balance`, credit/hold lists |
| `unreflected_buy_credit` | each execute | credits non-empty | (platform) prune/sum credits | `buy_credits_since_balance` |
| `unnetted_sell_hold` / `consume_sell_hold_netting` / `arm_sell_hold` | place / balance drop / place sell | hold outstanding | (platform) | `sell_holds_since_balance` |
| `effective_committed_sell_base` | sell sizing | — | (platform) | committed-sell ceiling |

**Buy leg**

| Current path | Trigger | Guard (abridged) | Action sequence | State / persistence effect |
|---|---|---|---|---|
| `compute_buy_ref_price` | book_update | — | compute_grid_price | (pure) |
| initial / next buy placement | book_update | no tracked buy, no pending, `can_place_buy_order`, not capital-halted | compute_grid_price → place_buy → track_buy | `last_buy_order_id`/`_price`, reserved quote |
| buy amendment | book_update | trail-up / zone-violation, cooldown elapsed | compute_amend_price → create_amend_order → push_order | in-flight amend, `amend_cooldowns` |
| buy capacity | buy placement | `use_reserved_base_guard` | atomic_check_and_reserve / gate_balance | per-exchange reserved atomic |
| ghost-buy detection/recovery | order_lifecycle (sync) | no open buy, not in-flight/amend-active, `buy_ack_ghost_grace_s` elapsed | is_ghost → recover | clear buy tracking, release reserved quote |

**Sell leg** (`evaluate_sell_leg` and result builders)

| Current path | Trigger | Guard (abridged) | Action sequence | State / persistence effect |
|---|---|---|---|---|
| `available_base` / `reserve_headroom` | each execute | venue vs ledger basis | gate_balance | reserve-dip ceiling |
| `owed_sell_price` | fill(buy) | — | compute_sell_price | (pure) |
| sell placement | fill(buy) / execute | `effective_sell_qty` ≥ min, `use_reserved_base_guard` | place_sell → track_sell | `open_sell_orders`, `sell_commitments`, arm hold |
| `can_place_sell_order` | sell placement | — | gate_balance | — |
| persisted-level reconcile (`partition`/`persisted_rebuild_needed`/`dedupe`/`reconcile_persisted_sell_levels`) | execute | `remaintain_expired_sells` | reconcile_persisted_sell_levels | `persisted_sell_levels` |
| sell-stack merge/nesting | sell placement | `merge_preserved_sells` (descriptor) | merge_sell_levels | commitment/feed merge |

**Reservation**

| Current path | Trigger | Guard (abridged) | Action sequence | State / persistence effect |
|---|---|---|---|---|
| `atomic_check_and_reserve` | buy placement | — | reserve | per-exchange reserved atomic |
| `set_asset_reserved_quote` | fills/cancels/recovery | — | update_reserved_base | `reserved_quote` + atomic |
| `upsert`/`arm`/`rekey`/`remove_sell_commitment`, `remove_pending_sell_commitments` | lifecycle | — | track_sell | `sell_commitments` |

**Fills / lifecycle** (`jacobs_ladder_events.ml`)

| Current path | Trigger | Guard (abridged) | Action sequence | State / persistence effect |
|---|---|---|---|---|
| `handle_order_filled` | fill | not already processed (`add_processed_fill`) | accumulate → update_reserved_base → place_sell (sell leg) | position, `reserved_base`, accumulated profit |
| `handle_order_acknowledged` | order_lifecycle (ack) | — | track_buy / track_sell | `last_buy_order_id`, ack timestamp |
| `handle_order_rejected` / `handle_order_failed` | order_lifecycle | — | release reservation | in-flight, reserved quote |
| `handle_order_cancelled` | order_lifecycle (cancel) | ghost semantics (`buy_tracking_matches_exchange_event`) | is_ghost → recover | tracking, commitments |
| `handle_order_amended` / `_skipped` / `_failed` | order_lifecycle | — | amend-result handling | `amend_cooldowns`, in-flight |
| `cleanup_pending_cancellation` | order_lifecycle | — | cleanup | in-flight tokens |

**Support (not behaviors; provided by the engine, no mapping needed):** `Sell_orders` ring buffer, `price_key`, `price_within_tolerance`, `dedupe_persisted_sell_levels` internals, `create_place/amend/cancel_order` constructors.

An equivalent table is produced for Market Maker. The persistence mapping table (§3.3.1) is maintained alongside.

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
| `strategy_actions_grid.ml` | grid decision-action handlers (faithful wrappers of reference grid functions) + coarse `grid_cycle`/`sync_open_orders`/`evaluate_buy_leg`/`evaluate_sell_leg` via `Make` | actions | coarse port wired (`config_strategy`) |
| `strategy_file.ml` | JSON → AST (triggers/params/state/steps) | protocol | done |
| `strategy_expr.ml` | expression engine: `$ref` scanning + tokenizer/parser/evaluator (arithmetic, comparison, boolean) and string templates | protocol | done |
| `strategy_compile.ml` | static validation against the registry | protocol | done (validation); compile-to-closures pending |
| `strategy_cli.ml` | `dio strategy validate <file>` | tooling | done |
| `strategy_guard.ml` | guard evaluation over an expression `env` + facts (event/side/capacity/pending/engine/cooldown) | protocol | done (closure compile pending) |
| `strategy_runtime.ml` | per-instance state, env, cycle runner, action dispatch, step-local bindings, engine caps | protocol | done (synthetic; engine caps + exchange handlers pending) |
| `strategy_protocol.ml` | implements `Strategy_common.S` over a compiled instance | protocol | pending |
| `platform_accounting.ml` | central accounting: grace, ghost, freshness, ceilings, pending, reservation | platform | partial (constants, freshness cutoff, sell-hold overlays; ghost/recovery, reserve-dip ceiling, reservation pending) |
| `exchange_capabilities.ml` | per-venue capability descriptors (extracted from the grid flag matrix) | platform | done |
| `strategy_event_recorder.ml` | record per-cycle observations (venue open orders, emitted intents, state, persistence) into a trace | test | done |
| `strategy_equivalence.ml` | trace diff / equivalence decision + deterministic replay API | test | done |
| `strategy_trace.ml` | observable trace types + comparison + JSON persistence | test | done |

### 9.2 Existing modules to touch

| File | Change |
|---|---|
| `config.ml` | instance binding to `strategy_file` + param overrides + descriptor flags; keep `trading_config`, shift `strategy` tag semantics (`config.ml:504` `read_config`) — **binding + name-match check done**; param overrides/descriptor flags pending |
| `domain_spawner.ml` | replace `is_grid_strategy`/`is_mm_strategy` and dispatch branches (290-308, 589-798, 1425-1467, 1786-1795) with compiled-instance dispatch; add dual-run mode — **⚠ temporary bridge**: the hardcoded name dispatch below stays until milestone 3 removes it (see §9.5) |
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
| 3 | Grid port | mapping tables (§8.2 + persistence); remaining actions; author grid file; **remove the hardcoded name-dispatch bridge (§9.5)**; wire dispatch behind flag | full differential pass; STRAT-phase allocation/latency parity within the reference budget (§6.4); shadow parity; canary |
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
- Tests: `test_strategy_compose.ml` (parse + validation) and `test_strategy_runtime.ml` (expression/guard evaluation + synthetic cycle execution). Sample file: `strategies/jacobs_ladder.json`.

**Remaining for milestone 1:** the `Strategy_common.S` protocol adapter (`strategy_protocol.ml`) and wiring a compiled instance into the domain loop behind the default-off feature flag. Milestones 0 and 2–5 are not started.

**Milestone 0 (in progress):** `strategy_trace.ml` (observable trace types + comparison + JSON persistence), `strategy_event_recorder.ml` (per-cycle recording, including **emitted order intents** via a per-symbol hot-path hook), and `strategy_equivalence.ml` (trace diff + a deterministic **replay API**) are implemented with ref-vs-ref identity, state order-insensitivity, divergence, JSON round-trip, and replay-equivalence/divergence tests (`test_strategy_harness.ml`). The domain loop records per-cycle venue open orders, emitted intents, and state inputs (price, bid/ask, base/quote tradeable balances, base balance age, open-orders generation, cycle) under the default-off `strategy_trace` config flag, persisting every 50 busy cycles to `data/strategy_trace_<exchange>_<symbol>.json`; `dio strategy diff <a.json> <b.json>` compares two traces. A **replay driver** (`dio strategy replay <trace>`, `Strategy_replay`) re-feeds a recorded input trace through the reference grid and diffs emitted intents; verified live on alpaca testnet, it runs but still diverges where the strategy's starting *state* is not captured. Scalar/option state (tracked buy, `reserved_base`/`quote`, position, `last_buy_fill_price`, flags, in-flight bits) and the collection state (`open_sell_orders`, `sell_commitments`, `pending_orders`, `persisted_sell_levels`, `amend_cooldowns`) are now seeded via a recorded pre-run snapshot (`s_`-prefixed entries). Exact replay still diverges (a spurious sell in cycle 0), so the remaining derived inputs — the oracle/accumulation blend and cached values — must also be captured. Remaining: capture those, then dual-run capture.

**Milestone 3 (in progress):** decision handlers ported (`compute_buy_ref_price`, `owed_sell_price`, `available_base`, `grid_price`); a coarse-wrapper seam (`Strategy_actions_grid.Make`) plus the real engine context in `domain_spawner` (`Config_grid_engine`) behind a default-off `config_strategy` flag. The grid's shipped strategy file (`strategies/jacobs_ladder.json`) is now the coarse orchestration (`book_update → grid_cycle`); when `config_strategy` is on, the loop feeds the engine context and dispatches to the interpreter, whose handler calls the same reference `execute_strategy`. Remaining: run on the alpaca testnet with `strategy_trace` for a first differential, then the harness + canary.

**Milestone 2 (in progress):** `exchange_capabilities.ml` extracts the grid's per-venue flag matrix (`jacobs_ladder_config.ml`) into capability descriptors for hyperliquid/kraken/ibkr/lighter/alpaca. `platform_accounting.ml` now holds the pure overlay constants, the freshness cutoff, the sell-hold overlays (`unnetted_sell_hold`, `consume_sell_hold_netting`, `arm_sell_hold`), the credit overlay (`unreflected_credit`), the committed-sell ceiling (`effective_committed_sell_base`), the reserve-dip availability (`available_base`, `alpaca_available_base`), the persisted-sell-level matching helpers (`price_key`, `price_within_tolerance`, `partition_persisted_sell_levels`, `dedupe_persisted_sell_levels`), and the reservation atomics (`total_reserved_by_exchange`, `get_exchange_reserved_atomic`, `atomic_add`), and the ghost-buy grace predicate (`is_ghost_buy`), parameterized over lists/flags so the module is strategy-state-independent; the grid keeps thin adapters and its state fields as the store, so behavior is unchanged (grid suite + `test_platform_accounting.ml` pass). Remaining: ghost-buy recovery (clear/re-track) and the refactor-equivalence gate.

**Usage:**

```
./_build/default/bin/main.exe strategy validate strategies/jacobs_ladder.json
```

Exit codes: `0` valid, `1` errors, `2` usage.

**Local test config:** `config.json` is gitignored and set to the Alpaca testnet Ladder instances (BOTZ, LIT, REMX, SMH), so local runs do not touch the live engine.

**Tooling:** the `.ocamlformat` `version` pin was removed so the OxCaml-patched `ocamlformat` on the `5.2.0+ox` switch can format the tree (`profile = janestreet`, `comment-check = false`). `dune build @fmt` is clean repo-wide.

### 9.5 Temporary bridges (must be removed)

> **⚠ TODO — remove in milestone 3.** These are deliberate, temporary shims that keep the current engine working while the interpreter is unwired. They violate the "names are opaque / code handles any name" rule and **must not survive into the finished engine.**

| Bridge | Where | Remove when | Removal |
|---|---|---|---|
| Hardcoded strategy-name dispatch | `domain_spawner.ml` — `is_grid_strategy` / `is_mm_strategy` (`strategy = "jacobs_ladder" \|\| "Ladder"` / `"market_maker" \|\| "MM"`) and the cleanup dispatch at `domain_spawner.ml:1702-1704` | milestone 3 | dispatch on the compiled strategy instance / strategy file, never on the user's name. A user naming their strategy anything must still run it. |
| `oracle_tasks.default_trading_config` literal `strategy` | `oracle_tasks.ml` — synthetic CLI fallback config | milestone 3 | derive from the actual bound entry, or leave unset; it is not a name match, but should not assert a built-in name. |

Rationale: strategy behaviour will come from the strategy file (steps/actions), so the engine selects an implementation by the file, not by a built-in name. Hardcoded name dispatch is a bridge only.

## 10. Open items

- Final expression/guard grammar surface — keep minimal; prefer registered predicates over new syntax.
- Capability descriptor encoding (OCaml record vs per-venue static table) and the 1:1 mapping from the current flag matrix.
- Persistence mapping table completion (store keys ↔ declared fields), including boot sequences.
- Canonical cycle-order confirmation against the domain loop, locked by harness tests.
- Event recorder cost in live mode (sampling vs full capture) and replay-log format.
