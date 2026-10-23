# Dio Strategy — Zed extension

Syntax highlighting and live validation for `.strategy` files in [Zed](https://zed.dev).

This is the Zed counterpart to `editors/vscode-strategy`. Zed does not support
TextMate grammars, so highlighting comes from a small tree-sitter grammar in
`editors/tree-sitter-strategy` rather than reusing the VS Code `.tmLanguage`
file. Validation talks to the same server, `dio strategy lsp`.

- **Highlighting**: comments, keywords (`strategy`, `when`, `if`, `stop`, …),
  `$ref`s, step labels, types, booleans, numbers, strings, operators.
- **Validation**: diagnostics from `dio strategy lsp` (the same parser and
  validator as `dio strategy validate`), published as you type.

## Install

From the repo root:

```sh
dune build                       # builds _build/default/bin/main.exe
editors/zed-strategy/dev-install.sh
```

The script snapshots the grammar into a throwaway git repo, builds the
extension to `wasm32-wasip2`, compiles the tree-sitter grammar with `wasi-sdk`,
and symlinks the result into Zed's `extensions/installed` directory.

The first run downloads `wasi-sdk` (about 180 MB) unless `WASI_SDK_PATH` is set
or Zed already has one in `extensions/build/wasi-sdk`. Re-run the script after
changing any extension or grammar file, then run `zed: reload extensions` from
the command palette if Zed does not pick the change up automatically.

### Requirements

- Rust with the `wasm32-wasip2` target (the script runs `rustup target add`).
- `python3`, `rsync`, `curl`, and `tar` (present on macOS/Linux).

## Finding the `dio` binary

Zed starts validation with `dio strategy lsp`. The extension resolves the
binary in this order:

1. `dio` on `PATH`.
2. `<worktree>/_build/default/bin/main.exe` (the in-repo `dune build`).

That second fallback means no PATH setup is needed when editing this checkout.
If neither exists, Zed reports the missing path in its language-server error
notification.

## Layout

```
editors/zed-strategy/
  extension.toml              language, grammar, and language-server registration
  src/lib.rs                  supplies the `dio strategy lsp` command to Zed
  languages/strategy/
    config.toml               file suffixes, comments, brackets, indentation
    highlights.scm            tree-sitter highlight query
    brackets.scm              rainbow-bracket pairs
  dev-install.sh              build + install as a dev extension
  tests/lsp_smoke.py          protocol smoke test for `dio strategy lsp`
```

The parser lives in `editors/tree-sitter-strategy` (`grammar.js`, generated
`src/parser.c`, and the canonical `queries/highlights.scm`). Keep the two
`highlights.scm` copies in sync.

## Notes

- The grammar is deliberately **flat**: it tokenizes the source but does not
  model indentation-scoped blocks. Highlighting, bracket matching, and LSP
  diagnostics work; structural outline/indent features are limited. The
  engine's own parser remains the source of truth for what a strategy means.
- Zed's dev extensions are installed from the local filesystem, so nothing is
  published. If this extension is published later, change the `[grammars.strategy]`
  `rev` in `extension.toml` to the commit containing
  `editors/tree-sitter-strategy` (the repository URL already points at this
  repo).

## Development

```sh
# grammar: regenerate parser.c after editing grammar.js
cd editors/tree-sitter-strategy
npx --yes tree-sitter-cli generate
npx --yes tree-sitter-cli parse ../../strategies/*.strategy

# server: protocol-level smoke test
python3 editors/zed-strategy/tests/lsp_smoke.py
```
