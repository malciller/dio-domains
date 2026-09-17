# Strategy Engine Layout

This directory implements the strategy system from
`docs/strategy-engine-design.md`. The engine is *config-driven*: users author
strategy files (`strategies/<name>.json`), and the OCaml side compiles and runs
them over a registered library of **actions**. The code-side extension surface
is the action registry (`Strategy_actions`), not new strategy modules.

## Physical layout

The `dio.strategies` library is one wrapped library; `(include_subdirs
unqualified)` in `dune` keeps module identity equal to the file basename, so
moving a module between the subdirs below never changes how it is referenced
(`Dio_strategies.Type_X`). Subdirectories must not contain their own `dune`
stanzas. The subdirs are a *physical* organization of the layers; they are not
separate libraries (a layered library split is a deferred follow-up).

| Subdir | Modules | Layer (design doc §2) |
|---|---|---|
| `core/` | `Strategy_common`, `Strategy_state`, `Strategy_venue`, `Strategy_reservation`, `Strategy_orders`, `Strategy_sell_orders`, `Strategy_lifecycle`, `Strategy_decision`, `Strategy_events`, `Strategy_api` | legacy strategy-agnostic decision engine — the reference whose behavior the config-driven engine must reproduce. `Strategy_api` is the aggregation seam used by `domain_spawner.ml`; retire both with the equivalence harness. |
| `platform/` | `Platform_accounting`, `Exchange_capabilities`, `Fee_cache` | central accounting: grace windows, ghost detection, freshness, reserve-dip ceiling, pending/dedup, reservation; per-venue capability descriptors. |
| `actions/` | `Strategy_actions`, `Strategy_actions_builtin`, `Strategy_actions_cycle`, `Strategy_cycle_engine` | the action registry (the only code-side extension point) plus the functor bridge (`Strategy_actions_cycle.Make (Strategy_cycle_engine)`) that keeps the config-driven interpreter behaviorally identical to the reference grid. |
| `protocol/` | `Strategy_file`, `Strategy_compile`, `Strategy_expr`, `Strategy_guard`, `Strategy_runtime`, `Strategy_fact_slots` | strategy-file protocol: parse JSON → validate → compile to closures → run the ordered step runner against events. |
| `harness/` | `Strategy_trace`, `Strategy_event_recorder`, `Strategy_equivalence`, `Strategy_cli` | behavioral-equivalence tooling: recorder, trace diff, and the `dio strategy validate/diff/replay` CLI. |

## Dependency rule

```
strategy files (JSON)  →  protocol (interpret/compile/run)
protocol               →  actions (registered capabilities)
actions                →  core (reference decision/accounting functions)
core, actions          →  platform (central integrity invariants)
```

`domain_spawner.ml`, `supervisor_orders.ml`, and `strategy_replay.ml` consume
the library only through `Dio_strategies.*`; `bin/main.ml` wires the
`dio strategy` CLI through `Strategy_cli`.

## Adding capabilities

New reusable strategy behavior = register a new action in
`Strategy_actions` / `Strategy_actions_builtin`. New strategies = a new JSON file
picking from the action library — no OCaml, no rebuild.