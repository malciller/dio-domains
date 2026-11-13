#!/usr/bin/env python3
"""codeviz - generate an interactive code map for dio-domains.

Builds `docs/code-map/graph.js` from three sources, then you open
`docs/code-map/index.html` in a browser. No server, no build step.

  libs   library/executable dependencies, from dune stanzas
  deps   module dependencies, from `ocamldep -modules`
  trace  caller/callee edges, from a source-text index
  site   write all three to docs/code-map/graph.js (the default)

The CLI also prints any one graph as Mermaid or Graphviz:

  python3 scripts/codeviz.py libs --focus dio.engine
  python3 scripts/codeviz.py deps src/engine/strategies --depth 1
  python3 scripts/codeviz.py trace Order_executor.place_order --callers --evidence
  python3 scripts/codeviz.py site
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC_DIRS = ("src", "bin")
SKIP_DIRS = {"_build", "node_modules", "test", ".git"}

# External libraries dio consumes. A reference that is neither declared in a
# dune stanza nor listed here is reported as unresolved rather than drawn.
EXTERNAL_HINTS = {
    "alcotest",
    "base64",
    "bigarray",
    "bigstringaf",
    "bytes",
    "caml",
    "cohttp",
    "domainslib",
    "digestif",
    "dune",
    "findlib",
    "fmt",
    "graphics",
    "lwt",
    "mirage",
    "mtime",
    "ocaml",
    "ppx",
    "re",
    "result",
    "sexplib",
    "str",
    "threads",
    "unix",
    "uutf",
    "yojson",
    "zarith",
    "zstd",
}


# --------------------------------------------------------------------------
# dune s-expressions
# --------------------------------------------------------------------------


def sexp_tokens(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    i = 0
    while i < len(text):
        c = text[i]
        if c == ";":
            j = text.find("\n", i)
            i = len(text) if j == -1 else j
        elif c == '"':
            j = i + 1
            buf = []
            while j < len(text) and text[j] != '"':
                if text[j] == "\\" and j + 1 < len(text):
                    j += 1
                buf.append(text[j])
                j += 1
            out.append(("atom", "".join(buf)))
            i = j + 1
        elif c in "()":
            out.append((c, ""))
            i += 1
        elif c.isspace():
            i += 1
        else:
            j = i
            while j < len(text) and not text[j].isspace() and text[j] not in "();":
                j += 1
            out.append(("atom", text[i:j]))
            i = j
    return out


def parse_sexps(text: str) -> list:
    tokens = sexp_tokens(text)
    pos = 0

    def parse() -> list:
        nonlocal pos
        items: list = []
        while pos < len(tokens):
            kind, value = tokens[pos]
            if kind == "(":
                pos += 1
                items.append(parse())
            elif kind == ")":
                pos += 1
                return items
            else:
                pos += 1
                items.append(value)
        return items

    forms = []
    while pos < len(tokens):
        if tokens[pos][0] == "(":
            pos += 1
            forms.append(parse())
        else:
            pos += 1
    return forms


def atoms(node, key: str) -> list[str]:
    for item in node:
        if isinstance(item, list) and item and item[0] == key:
            return [x for x in item[1:] if isinstance(x, str)]
    return []


# --------------------------------------------------------------------------
# graph model
# --------------------------------------------------------------------------


@dataclass(eq=False)
class Node:
    label: str
    group: str
    kind: str = "lib"


@dataclass
class Graph:
    title: str
    nodes: dict[str, Node] = field(default_factory=dict)
    edges: set[tuple[str, str]] = field(default_factory=set)
    notes: list[str] = field(default_factory=list)

    def add(self, node_id: str, label: str, group: str, kind: str = "lib") -> None:
        self.nodes.setdefault(node_id, Node(label, group, kind))

    def edge(self, a: str, b: str) -> None:
        if a != b and a in self.nodes and b in self.nodes:
            self.edges.add((a, b))

    def mermaid(self) -> str:
        ids = {nid: f"n{i}" for i, nid in enumerate(self.nodes)}
        lines = [f"%% {self.title}", "graph LR"]
        groups: dict[str, list[str]] = {}
        for nid, node in self.nodes.items():
            groups.setdefault(node.group, []).append(nid)
        for group, members in sorted(groups.items()):
            lines.append(f'  subgraph g_{slug(group)}["{group}"]')
            for nid in members:
                label = self.nodes[nid].label.replace('"', "'")
                shape = (
                    f'["{label}"]'
                    if self.nodes[nid].kind != "exe"
                    else f'[["{label}"]]'
                )
                lines.append(f"    {ids[nid]}{shape}")
            lines.append("  end")
        for a, b in sorted(self.edges, key=lambda e: (ids[e[0]], ids[e[1]])):
            lines.append(f"  {ids[a]} --> {ids[b]}")
        for note in self.notes:
            lines.append(f"  %% {note}")
        return "\n".join(lines) + "\n"

    def dot(self) -> str:
        ids = {nid: f"n{i}" for i, nid in enumerate(self.nodes)}
        lines = [
            "digraph codeviz {",
            "  rankdir=LR;",
            '  graph [fontname="Helvetica", labelloc="t"];',
            '  node [shape=box, fontname="Helvetica", fontsize=10];',
            '  edge [color="#666666", arrowsize=0.6];',
            f'  label="{self.title}";',
        ]
        groups: dict[str, list[str]] = {}
        for nid, node in self.nodes.items():
            groups.setdefault(node.group, []).append(nid)
        for gi, (group, members) in enumerate(sorted(groups.items())):
            lines.append(
                f'  subgraph cluster_{gi} {{ label="{group}"; style=dashed; color="#bbbbbb";'
            )
            for nid in members:
                node = self.nodes[nid]
                shape = "box,style=bold" if node.kind == "exe" else "box"
                lines.append(f'    {ids[nid]} [label="{node.label}", shape={shape}];')
            lines.append("  }")
        for a, b in sorted(self.edges, key=lambda e: (ids[e[0]], ids[e[1]])):
            lines.append(f"  {ids[a]} -> {ids[b]};")
        lines.append("}")
        return "\n".join(lines) + "\n"


def slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", text)


def emit(graph: Graph, fmt: str, png: str | None) -> None:
    source = graph.dot() if fmt == "dot" else graph.mermaid()
    if not png:
        sys.stdout.write(source)
        return
    target = Path(png)
    if not target.is_absolute():
        target = ROOT / target
    target.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        ["dot", "-Tpng", "-Gdpi=150", "-o", str(target)],
        input=source,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        sys.exit(f"dot failed:\n{proc.stderr}")
    sys.stdout.write(f"wrote {rel(target)}\n")


# --------------------------------------------------------------------------
# libraries
# --------------------------------------------------------------------------


@dataclass(eq=False)
class Lib:
    name: str
    public_name: str | None
    directory: Path
    libraries: list[str]
    kind: str

    @property
    def ident(self) -> str:
        return self.public_name or self.name


def dune_files() -> list[Path]:
    out = []
    for top in SRC_DIRS:
        base = ROOT / top
        if not base.exists():
            continue
        for path in base.rglob("dune"):
            if any(part in SKIP_DIRS for part in path.parts):
                continue
            out.append(path)
    return sorted(out)


def load_libs() -> list[Lib]:
    libs: list[Lib] = []
    for path in dune_files():
        for form in parse_sexps(path.read_text(errors="replace")):
            if not form or not isinstance(form[0], str):
                continue
            head = form[0]
            if head == "executables":
                for name in atoms(form, "names"):
                    libs.append(
                        Lib(name, name, path.parent, atoms(form, "libraries"), "exe")
                    )
                continue
            if head not in {"library", "executable", "test"}:
                continue
            names = atoms(form, "name")
            if not names:
                continue
            public = atoms(form, "public_name")
            libs.append(
                Lib(
                    name=names[0],
                    public_name=public[0] if public else None,
                    directory=path.parent,
                    libraries=atoms(form, "libraries"),
                    kind="exe" if head in {"executable", "test"} else "lib",
                )
            )
    return libs


def area(directory: Path) -> str:
    try:
        rel = directory.relative_to(ROOT)
    except ValueError:
        return "."
    parts = rel.parts
    if parts and parts[0] == "src" and len(parts) > 1:
        return "/".join(parts[1:3])
    return parts[0] if parts else "."


def lib_index(libs: list[Lib]) -> tuple[dict[str, Lib], dict[str, Lib]]:
    by_public = {lib.public_name: lib for lib in libs if lib.public_name}
    by_name = {lib.name: lib for lib in libs}
    return by_public, by_name


def lib_alias_map(libs: list[Lib]) -> dict[str, Path]:
    """Map `strategies` / `dio.strategies` / `dio_strategies` to its directory."""
    out: dict[str, Path] = {}
    for lib in libs:
        for key in (lib.name, lib.public_name or ""):
            alias = key.lower()
            if alias.startswith("dio."):
                alias = alias[4:]
            elif alias.startswith("dio_"):
                alias = alias[4:]
            if alias:
                out.setdefault(alias, lib.directory)
    return out


def resolve_lib(
    ref: str, by_public: dict[str, Lib], by_name: dict[str, Lib]
) -> Lib | None:
    if ref in by_public:
        return by_public[ref]
    if "." not in ref and ref in by_name:
        return by_name[ref]
    return None


def cmd_libs(args: argparse.Namespace) -> None:
    libs = load_libs()
    by_public, by_name = lib_index(libs)
    graph = Graph("dio-domains libraries")
    for lib in libs:
        graph.add(lib.ident, lib.ident, area(lib.directory), lib.kind)
    for lib in libs:
        for ref in lib.libraries:
            dep = resolve_lib(ref, by_public, by_name)
            if dep is None:
                if not any(hint in ref for hint in EXTERNAL_HINTS):
                    graph.notes.append(f"unresolved dependency: {lib.ident} -> {ref}")
                continue
            graph.edge(lib.ident, dep.ident)

    if args.focus:
        start = resolve_lib(args.focus, by_public, by_name)
        if start is None:
            matches = [l.ident for l in libs if args.focus.lower() in l.ident.lower()]
            sys.exit(
                f"unknown library {args.focus!r}"
                + (f"; try: {', '.join(matches)}" if matches else "")
            )
        keep = {start.ident}
        frontier = {start.ident}
        for _ in range(args.depth):
            nxt: set[str] = set()
            for a, b in graph.edges:
                if args.direction in {"down", "both"} and a in frontier:
                    nxt.add(b)
                if args.direction in {"up", "both"} and b in frontier:
                    nxt.add(a)
            nxt -= keep
            keep |= nxt
            frontier = nxt
        for nid in [n for n in graph.nodes if n not in keep]:
            del graph.nodes[nid]
        graph.edges = {(a, b) for a, b in graph.edges if a in keep and b in keep}
        graph.title += f" (focused on {args.focus})"
    emit(graph, args.format, args.png)


# --------------------------------------------------------------------------
# modules
# --------------------------------------------------------------------------


@dataclass(eq=False)
class Mod:
    path: Path
    ident: str
    lib_dir: Path

    @property
    def label(self) -> str:
        return self.path.stem[:1].upper() + self.path.stem[1:]


def lib_dir_for(path: Path, libs: list[Lib]) -> Path:
    best = ROOT
    for lib in libs:
        try:
            path.relative_to(lib.directory)
        except ValueError:
            continue
        if len(lib.directory.parts) > len(best.parts):
            best = lib.directory
    return best


def project_mlis() -> list[Path]:
    out: list[Path] = []
    for top in SRC_DIRS:
        base = ROOT / top
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.suffix not in {".ml", ".mli"}:
                continue
            if path.name == "discover.ml" or any(
                part in SKIP_DIRS for part in path.parts
            ):
                continue
            out.append(path)
    return sorted(out)


def ocamldep(files: list[Path]) -> dict[Path, list[str]]:
    if not files:
        return {}
    proc = subprocess.run(
        ["ocamldep", "-modules", *map(str, files)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    deps: dict[Path, list[str]] = {}
    for line in proc.stdout.splitlines():
        if ":" not in line:
            continue
        path, mods = line.split(":", 1)
        deps[Path(path.strip()).resolve()] = mods.split()
    return deps


def build_module_edges(libs: list[Lib], files: list[Path]):
    mls = [p for p in files if p.suffix == ".ml"]
    mods = {
        p.resolve(): Mod(p.resolve(), p.stem.lower(), lib_dir_for(p.resolve(), libs))
        for p in files
    }
    by_ident: dict[str, list[Mod]] = {}
    for mod in mods.values():
        by_ident.setdefault(mod.ident, []).append(mod)

    def resolve(ref: str, source: Mod) -> Mod | None:
        cands = sorted(
            by_ident.get(ref.lower()) or [], key=lambda m: m.path.suffix != ".ml"
        )
        if not cands:
            return None
        if len(cands) == 1:
            return cands[0]
        same_lib = [c for c in cands if c.lib_dir == source.lib_dir]
        if len(same_lib) == 1:
            return same_lib[0]
        same_dir = [c for c in cands if c.path.parent == source.path.parent]
        return same_dir[0] if len(same_dir) == 1 else None

    raw = ocamldep(mls)
    edges: dict[Path, set[Path]] = {}
    ambiguous: set[str] = set()
    for path, refs in raw.items():
        src = mods.get(path)
        if src is None:
            continue
        for ref in refs:
            dep = resolve(ref, src)
            if dep is not None:
                edges.setdefault(src.path, set()).add(dep.path)
            elif len(by_ident.get(ref.lower(), [])) > 1:
                ambiguous.add(ref)
    return mods, edges, ambiguous


def select_seeds(target: str, mods: dict[Path, Mod]) -> list[Mod]:
    path = (ROOT / target).resolve()
    if path.is_dir():
        return [m for m in mods.values() if path in m.path.parents]
    if path.is_file():
        return [m for m in mods.values() if m.path == path]
    return [m for m in mods.values() if m.ident == target.lower().replace(".ml", "")]


def cmd_deps(args: argparse.Namespace) -> None:
    if not args.target:
        sys.exit("deps needs a target: a directory, .ml file, or module name")
    libs = load_libs()
    files = project_mlis()
    mods, edges, _ = build_module_edges(libs, files)
    seeds = select_seeds(args.target, mods)
    if not seeds:
        sys.exit(f"no modules under {args.target!r}")

    keep = {m.path for m in seeds}
    frontier = set(keep)
    for _ in range(args.depth):
        nxt: set[Path] = set()
        for a, deps in edges.items():
            if a not in frontier:
                continue
            if args.direction in {"down", "both"}:
                nxt |= deps
            if args.direction in {"up", "both"}:
                nxt |= {b for b, ds in edges.items() if a in ds}
        nxt -= keep
        keep |= nxt
        frontier = nxt

    graph = Graph(f"module dependencies: {args.target} (depth {args.depth})")
    for path in keep:
        graph.add(str(path), mods[path].label, area(path.parent))
    for a, deps in edges.items():
        if a not in keep:
            continue
        for b in deps:
            if b in keep:
                graph.edge(str(a), str(b))
    emit(graph, args.format, args.png)


# --------------------------------------------------------------------------
# symbols
# --------------------------------------------------------------------------

TOP_LET = re.compile(
    r"^(?:let(?:%[a-zA-Z_]+)?(?: rec)?|and)\s+([a-zA-Z_][A-Za-z0-9_']*)"
)
TOKEN = re.compile(r"[a-zA-Z_][a-zA-Z0-9_']*")


@dataclass(eq=False)
class Def:
    path: Path
    line: int
    name: str


def is_callable(lines: list[str], line_no: int, name_end: int) -> bool:
    """True when a top-level `let` binds a function rather than a value.

    `let foo a b =` and `let foo = fun x ->` are functions; `let foo = ...`
    with no parameters is a value and is left out of the call graph. Signatures
    can span several lines, so look far enough ahead for the `=`.
    """
    window = [lines[line_no - 1][name_end:], *lines[line_no : line_no + 10]]
    joined = "\n".join(window)
    before_eq, eq, after = joined.partition("=")
    if not eq:
        return False
    lhs = before_eq.strip()
    if lhs and not lhs.startswith(":"):
        return True
    return bool(re.match(r"\s*(?:fun\b|function\b)", after))


class SourceIndex:
    def __init__(self, files: list[Path], lib_dirs: dict[str, Path] | None = None):
        self.files = files
        self.lib_dirs = lib_dirs or {}
        self.lines: dict[Path, list[str]] = {}
        self.fns: dict[Path, list[tuple[int, str]]] = {}
        self.defs: dict[str, list[Def]] = {}
        self.init_by_path: dict[Path, Def] = {}
        for path in files:
            text = path.read_text(errors="replace").splitlines()
            self.lines[path] = text
            fns: list[tuple[int, str]] = []
            rec_open = False
            for i, line in enumerate(text, 1):
                m = TOP_LET.match(line)
                if not m:
                    if re.match(r"^let (?:\(\)|_) =", line):
                        fns.append((i, "<init>"))
                        defn = Def(path, i, "<init>")
                        self.defs.setdefault("<init>", []).append(defn)
                        self.init_by_path[path] = defn
                    continue
                name = m.group(1)
                if line.startswith("and") and not rec_open:
                    continue  # `and t =` from a type declaration
                rec_open = line.startswith("let rec")
                if name in {"_", "()"}:
                    continue
                fns.append((i, name))
                if is_callable(text, i, m.end()):
                    self.defs.setdefault(name, []).append(Def(path, i, name))
            self.fns[path] = fns

    def enclosing(self, path: Path, line: int) -> str | None:
        best = None
        for ln, name in self.fns[path]:
            if ln <= line:
                best = (ln, name)
            else:
                break
        return best[1] if best else None

    def resolve(self, name: str, from_path: Path) -> Def | None:
        cands = self.defs.get(name)
        if not cands:
            return None
        if len(cands) == 1:
            return cands[0]
        same_file = [c for c in cands if c.path == from_path]
        if len(same_file) == 1:
            return same_file[0]
        same_dir = [c for c in cands if c.path.parent == from_path.parent]
        if len(same_dir) == 1:
            return same_dir[0]
        return None

    def resolve_in_file(self, name: str, path: Path, line: int) -> Def | None:
        cands = [c for c in self.defs.get(name, []) if c.path == path]
        if not cands:
            return None
        before = [c for c in cands if c.line <= line]
        return before[-1] if before else cands[0]

    def resolve_qualified(self, module: str, name: str, from_path: Path) -> Def | None:
        stem = module.lower()
        cands = [c for c in self.defs.get(name, []) if c.path.stem.lower() == stem]
        if not cands and stem.startswith("dio_") and stem[4:] in self.lib_dirs:
            # `Dio_strategies.foo` reaches into a wrapped library whose files
            # are named for the module, not the library.
            lib_dir = self.lib_dirs[stem[4:]]
            cands = [c for c in self.defs.get(name, []) if lib_dir in c.path.parents]
        if len(cands) == 1:
            return cands[0]
        same_dir = [c for c in cands if c.path.parent == from_path.parent]
        return same_dir[0] if len(same_dir) == 1 else None

    def resolve_symbol(self, symbol: str) -> Def:
        if "." in symbol:
            module, name = symbol.rsplit(".", 1)
            module_key = module.lower().replace(".", "_")
            cands = [
                d for d in self.defs.get(name, []) if module_key in d.path.stem.lower()
            ]
        else:
            name = symbol
            cands = self.defs.get(name, [])
        if not cands:
            close = [n for n in self.defs if symbol.lower() in n.lower()][:8]
            sys.exit(
                f"no definition of {symbol!r}"
                + (f"; close: {', '.join(close)}" if close else "")
            )
        if len(cands) > 1:
            spots = "\n".join(
                f"  {d.path.relative_to(ROOT)}:{d.line}" for d in cands[:12]
            )
            sys.exit(
                f"{symbol!r} is defined in several places; qualify it with a module:\n{spots}"
            )
        return cands[0]


def build_symbol_edges(index: SourceIndex):
    """Edges only where the source makes the target unambiguous.

    An unqualified identifier is an edge only within its own file: matching it
    across modules turns record fields and locals into phantom calls. Anything
    cross-module must be written `Module.name` in the source. That is stricter
    than OCaml (which resolves through opens), but every drawn edge is then
    visible at the cited line.
    """
    names = {name for name in index.defs if len(name) > 2}
    def_positions = {
        (d.path, d.line, d.name) for defs in index.defs.values() for d in defs
    }
    out: dict[Def, list[tuple[Def, Path, int]]] = {}
    ambiguous = 0
    for path, text in index.lines.items():
        for i, line in enumerate(text, 1):
            stripped = line.lstrip()
            if stripped.startswith("(*") or stripped.startswith("*"):
                continue
            caller_name = index.enclosing(path, i)
            caller = index.resolve(caller_name, path) if caller_name else None
            if caller is None:
                caller = index.init_by_path.get(path)
            if caller is None:
                continue
            for m in TOKEN.finditer(line):
                token = m.group(0)
                if token not in names or token == caller.name:
                    continue
                if (path, i, token) in def_positions:
                    continue
                after = line[m.end() :]
                if re.match(r"\s*[=:]", after):
                    continue  # record field or type annotation, not a call
                qualifier = re.search(r"([A-Z][A-Za-z0-9_']*)\.$", line[: m.start()])
                if qualifier:
                    callee = index.resolve_qualified(qualifier.group(1), token, path)
                elif m.start() > 0 and line[m.start() - 1] == ".":
                    continue  # field access on a value, e.g. `cfg.field`
                else:
                    callee = index.resolve_in_file(token, path, i)
                if callee is None:
                    if qualifier:
                        same_module = [
                            c
                            for c in index.defs.get(token, [])
                            if c.path.stem.lower() == qualifier.group(1).lower()
                        ]
                        if len(same_module) > 1:
                            ambiguous += 1
                    continue
                if caller != callee:
                    out.setdefault(caller, []).append((callee, path, i))
    return out, ambiguous


def node_label(defn: Def) -> str:
    stem = defn.path.stem
    module = stem[:1].upper() + stem[1:]
    return f"{module}.<init>" if defn.name == "<init>" else f"{module}.{defn.name}"


def cmd_trace(args: argparse.Namespace) -> None:
    files = [p for p in project_mlis() if p.suffix == ".ml"]
    index = SourceIndex(files, lib_alias_map(load_libs()))
    start = index.resolve_symbol(args.symbol)
    edges, ambiguous = build_symbol_edges(index)

    forward: dict[Def, list[tuple[Def, Path, int]]] = {}
    backward: dict[Def, list[tuple[Def, Path, int]]] = {}
    for caller, calls in edges.items():
        for callee, path, line in calls:
            forward.setdefault(caller, []).append((callee, path, line))
            backward.setdefault(callee, []).append((caller, path, line))

    keep = {start}
    frontier = {start}
    evidence: list[tuple[Def, Def, Path, int]] = []
    truncated = False
    for _ in range(args.depth):
        nxt: set[Def] = set()
        for node in frontier:
            if args.direction in {"callees", "both"}:
                for callee, path, line in forward.get(node, []):
                    evidence.append((node, callee, path, line))
                    nxt.add(callee)
            if args.direction in {"callers", "both"}:
                for caller, path, line in backward.get(node, []):
                    evidence.append((caller, node, path, line))
                    nxt.add(caller)
        frontier = nxt - keep
        keep |= frontier
        if len(keep) >= args.limit:
            truncated = True
            keep = {start} | set(list(keep - {start})[: args.limit - 1])
            break

    graph = Graph(f"trace: {node_label(start)} ({args.direction}, depth {args.depth})")
    for defn in keep:
        graph.add(str(defn), node_label(defn), area(defn.path.parent))
    for caller, callee, _, _ in evidence:
        if caller in keep and callee in keep:
            graph.edge(str(caller), str(callee))
    if truncated:
        graph.notes.append(f"stopped at --limit {args.limit}")
    if ambiguous:
        graph.notes.append(f"{ambiguous} ambiguous call sites skipped")
    emit(graph, args.format, args.png)

    if args.evidence:
        seen: set[tuple[Def, Def]] = set()
        rows = []
        for caller, callee, path, line in evidence:
            if (caller, callee) in seen:
                continue
            seen.add((caller, callee))
            rows.append(
                f"| {node_label(caller)} | {node_label(callee)} | `{path.relative_to(ROOT)}:{line}` |"
            )
        if rows:
            sys.stdout.write(
                "\n| caller | callee | evidence |\n| --- | --- | --- |\n"
                + "\n".join(rows)
                + "\n"
            )


# --------------------------------------------------------------------------
# site
# --------------------------------------------------------------------------


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def libs_payload(libs: list[Lib]) -> dict:
    by_public, by_name = lib_index(libs)
    nodes = [
        {
            "id": lib.ident,
            "label": lib.ident,
            "group": area(lib.directory),
            "kind": lib.kind,
            "path": rel(lib.directory / "dune"),
        }
        for lib in libs
    ]
    edges = []
    unresolved = set()
    for lib in libs:
        for ref in lib.libraries:
            dep = resolve_lib(ref, by_public, by_name)
            if dep is None:
                if not any(hint in ref for hint in EXTERNAL_HINTS):
                    unresolved.add(f"{lib.ident} -> {ref}")
                continue
            edges.append([lib.ident, dep.ident, None, None])
    return {"nodes": nodes, "edges": edges, "notes": sorted(unresolved)}


def modules_payload(libs: list[Lib], files: list[Path]) -> dict:
    mods, edges, unresolved = build_module_edges(libs, files)
    nodes = []
    for path, mod in mods.items():
        if path.suffix != ".ml":
            continue
        nodes.append(
            {
                "id": rel(path),
                "label": mod.label,
                "group": area(path.parent),
                "kind": "module",
                "path": rel(path),
            }
        )
    payload_edges = []
    for a, deps in edges.items():
        if a.suffix != ".ml":
            continue
        for b in sorted(deps):
            if b.suffix != ".ml":
                continue
            line = first_reference(a, b.stem)
            payload_edges.append([rel(a), rel(b), rel(a), line])
    return {"nodes": nodes, "edges": payload_edges, "notes": sorted(unresolved)}


def first_reference(path: Path, stem: str) -> int | None:
    pattern = re.compile(rf"\b{re.escape(stem)}\b", re.IGNORECASE)
    try:
        for i, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
            if pattern.search(line):
                return i
    except OSError:
        return None
    return None


def symbols_payload(index: SourceIndex) -> dict:
    edges, ambiguous = build_symbol_edges(index)
    nodes: dict[Def, dict] = {}
    for name, defs in index.defs.items():
        for defn in defs:
            nodes[defn] = {
                "id": f"{rel(defn.path)}::{name}",
                "label": node_label(defn),
                "name": name,
                "module": defn.path.stem,
                "group": area(defn.path.parent),
                "path": rel(defn.path),
                "line": defn.line,
            }
    payload_edges = []
    for caller, calls in edges.items():
        if caller not in nodes:
            continue
        best: dict[Def, tuple[Path, int]] = {}
        for callee, path, line in calls:
            if callee not in nodes:
                continue
            if callee not in best or (path, line) < best[callee]:
                best[callee] = (path, line)
        for callee, (path, line) in best.items():
            payload_edges.append(
                [nodes[caller]["id"], nodes[callee]["id"], rel(path), line]
            )
    notes = [f"{ambiguous} ambiguous call sites skipped"] if ambiguous else []
    return {"nodes": list(nodes.values()), "edges": payload_edges, "notes": notes}


def cmd_site(args: argparse.Namespace) -> None:
    libs = load_libs()
    files = project_mlis()
    index = SourceIndex([p for p in files if p.suffix == ".ml"], lib_alias_map(libs))
    payload = {
        "generated": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "libraries": libs_payload(libs),
        "modules": modules_payload(libs, files),
        "symbols": symbols_payload(index),
    }
    out = Path(args.out)
    if not out.is_absolute():
        out = ROOT / out
    out.mkdir(parents=True, exist_ok=True)
    target = out / "graph.js"
    target.write_text(
        "window.CODEVIZ = " + json.dumps(payload, separators=(",", ":")) + ";\n"
    )
    size = target.stat().st_size / 1024
    print(
        f"wrote {rel(target)} "
        f"({len(payload['libraries']['nodes'])} libs, "
        f"{len(payload['modules']['nodes'])} modules, "
        f"{len(payload['symbols']['nodes'])} symbols, "
        f"{len(payload['symbols']['edges'])} symbol edges) · {size:.0f} KiB"
    )
    print(f"open {rel(target.with_name('index.html'))}")


# --------------------------------------------------------------------------
# cli
# --------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "directions:\n"
            "  libs/deps  down = what the target uses (default), up = who uses it, both\n"
            "  trace      callees = what the symbol calls (default), callers, both\n\n"
            "trace is a source-text index: it skips comment lines and refuses ambiguous\n"
            "names, but it is not a type-checker. Confirm published edges with --evidence.\n"
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--format", choices=["mermaid", "dot"], default="mermaid")
        p.add_argument("--png", metavar="PATH", help="render a graphviz PNG")

    p = sub.add_parser("libs", help="library graph from dune stanzas")
    common(p)
    p.add_argument("--focus", help="library to centre on, e.g. dio.engine")
    p.add_argument("--depth", type=int, default=2)
    p.add_argument("--direction", choices=["down", "up", "both"], default="down")
    p.set_defaults(func=cmd_libs)

    p = sub.add_parser("deps", help="module graph from ocamldep")
    common(p)
    p.add_argument("target", nargs="?", help="directory, .ml file, or module name")
    p.add_argument("--depth", type=int, default=1)
    p.add_argument("--direction", choices=["down", "up", "both"], default="down")
    p.set_defaults(func=cmd_deps)

    p = sub.add_parser("trace", help="caller/callee graph around a symbol")
    common(p)
    p.add_argument("symbol", help="function name, optionally Module.function")
    p.add_argument("--depth", type=int, default=1)
    p.add_argument(
        "--direction", choices=["callees", "callers", "both"], default="callees"
    )
    p.add_argument("--evidence", action="store_true", help="append a file:line table")
    p.add_argument("--limit", type=int, default=40, help="max nodes (default 40)")
    p.set_defaults(func=cmd_trace)

    p = sub.add_parser("site", help="write docs/code-map/graph.js")
    p.add_argument("--out", default="docs/code-map", help="output directory")
    p.set_defaults(func=cmd_site)

    args = parser.parse_args()
    if getattr(args, "png", None):
        args.format = "dot"
    args.func(args)


if __name__ == "__main__":
    main()
