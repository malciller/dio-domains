# Dio Strategy — VS Code extension

Syntax highlighting and live validation for `.strategy` files.

- **Highlighting**: a TextMate grammar (`syntaxes/strategy.tmLanguage.json`) colors
  comments, keywords (`strategy`, `when`, `stop`, …), `$ref`s
  (`$platform.price.nan`), step labels, types, booleans, numbers, and strings.
- **Validation**: a dependency-free LSP client in `client/extension.js` spawns
  `dio strategy lsp` (a minimal Language Server Protocol server in
  `src/engine/strategies/harness/strategy_lsp.ml`) and surfaces its diagnostics
  in the editor, using the same parser/validator as `dio strategy validate`.

No `npm install` or build step is required — the client uses only the built-in
VS Code API.

## Setup

1. Build the `dio` binary:
   ```sh
   dune build
   ```
2. Make sure `dio` is on your PATH, or set the extension setting
   `dioStrategy.binary` to the path of the binary (e.g.
   `_build/default/bin/main.exe`).
3. In VS Code, run **Developer: Install Extension from Location…** and pick this
   folder (`editors/vscode-strategy`). It works from source; no packaging needed.
4. Open any `*.strategy` file — it gets colored immediately, and errors/warnings
   (unknown actions, unknown facts, bad argument types, …) show up inline as you
   type.

## Development

- Server: `dio strategy lsp` — hand-testable with framed JSON over stdio.
- To iterate on the OCaml server:
  ```sh
  dune build bin/main.exe && \
  printf 'Content-Length: 75\r\n\r\n{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{}}}' \
    | _build/default/bin/main.exe strategy lsp
  ```
- After changing the extension files you can reload the VS Code window
  (Cmd+Shift+P → *Developer: Reload Window*).