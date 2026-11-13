# Code map

An interactive, static view of the dio-domains source. Open `index.html` in a
browser; there is no server and no build step.

```sh
python3 scripts/codeviz.py site
open docs/code-map/index.html
```

`scripts/codeviz.py` rewrites `graph.js` (the data) and leaves `index.html`
alone. Regenerate after moving modules or changing call sites.

## Views

| View | Edges | Source |
| --- | --- | --- |
| Architecture | library depends on library | `(library ...)` and `(executable ...)` stanzas in every `dune` file |
| Modules | file imports file | `ocamldep -modules` over `src/` and `bin/` |
| Trace | caller calls callee | top-level `let` bindings and their references in the source |

Arrows always point from the thing doing the using to the thing being used:
library → dependency, importer → imported module, caller → callee.

## How to use it

Architecture and Modules are **grouped boards**, not hairballs: one panel per
area or directory, one chip per node, and no edges until you ask for them. The
board is the map; **clicking a chip opens a focused relationship view** with the
node in the middle, importers/dependents on the left, and imports/dependencies
on the right. Short arrows, room to read, and `Overview` or `back` returns.

- **Find an owner.** Search a function or module. Results jump to the right
  view; module results switch the area filter on automatically.
- **Filter modules.** Modules opens on `engine`. Toggle areas in the left rail
  to add `integrations`, `foundation`, `dashboard_ui`, or `bin`.
- **Follow a dependency.** In a focus view, click any chip to recenter on it,
  or click a `file:line` in the inspector. Hubs draw the first 22 per side and
  list the rest in the inspector.
- **Trace a request path.** Trace opens with clickable starters, or search for
  an entry point (`Domain_spawner.asset_domain_worker`,
  `Supervisor_orders.process_single_order`). Callers sit left of the root,
  callees right. Click any node to re-root; *New trace* returns to the start
  list, and back returns to the previous root. Direction and depth
  are in the left rail. A hub shows the nearest 24 per side and says how many
  it left out; the inspector lists all of them.
- **Trace from a module.** Open a module and use *Trace functions in this
  module*: the start screen lists that module's functions, busiest first.

The inspector lists every neighbour with the `file:line` that justifies the
edge. `⌘K` focuses search, the zoom controls sit bottom-right, and the URL hash
(`#path::function`) is shareable.

## Accuracy

The library and module graphs are exact: they come from dune metadata and
`ocamldep`.

The trace graph is a source index, not a type checker. It is deliberately
conservative:

- only top-level `let` bindings that look like functions are nodes (`let f x =`,
  `let f = fun x ->`); value bindings are skipped;
- an unqualified name is an edge only inside its own file — matching it across
  modules would turn record fields and locals into phantom calls;
- a cross-module call is an edge only when the source writes `Module.name`
  (the module name must match a file stem, including wrapped `Dio_*` roots);
- comment-only lines are ignored, inline comments and string literals are not.

If a name is defined in several modules and the target cannot be pinned down,
that edge is dropped rather than guessed. The count of dropped references is
shown at the bottom of the panel.

## Other output

The same generator prints single graphs without writing the site:

```sh
python3 scripts/codeviz.py libs --focus dio.engine
python3 scripts/codeviz.py deps src/engine/strategies --depth 1
python3 scripts/codeviz.py trace Order_executor.place_order --direction callers --evidence
python3 scripts/codeviz.py libs --png viz/libraries.png   # needs graphviz
```

`--format mermaid` (the default) emits a `graph LR` block that renders on GitHub;
`--format dot` emits Graphviz, and `--png` renders it with `dot`.
