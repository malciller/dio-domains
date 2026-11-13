#!/usr/bin/env bash
# Build the Dio Strategy Zed extension from this checkout and install it as a
# dev extension (a symlink into Zed's `extensions/installed` directory).
#
# Usage: editors/zed-strategy/dev-install.sh
#
# Re-run after changing the grammar, the query files, or `src/lib.rs`. Zed
# reloads the extension automatically when the symlink target changes; if it
# does not, run `zed: reload extensions` from the command palette.

set -euo pipefail

EXT_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$EXT_SRC/../.." && pwd)"
EXTENSION_ID="dio-strategy"

if [[ "$(uname)" == "Darwin" ]]; then
  ZED_DATA="$HOME/Library/Application Support/Zed"
else
  ZED_DATA="${XDG_DATA_HOME:-$HOME/.local/share}/zed"
fi
INSTALLED_DIR="$ZED_DATA/extensions/installed"

# Stage outside the repo: `dune clean` wipes `_build`, which would leave the
# symlink in `extensions/installed` dangling. Zed only reads `installed/`, so a
# sibling `dev/` is ours to use and keeps cargo's `target/` between runs.
BUILD_DIR="$ZED_DATA/extensions/dev"
DEST="$BUILD_DIR/$EXTENSION_ID"
GRAMMAR_SRC="$REPO_ROOT/editors/tree-sitter-strategy"
GRAMMAR_REPO="$BUILD_DIR/tree-sitter-strategy"

echo "==> Preparing build directory"
rm -rf "$GRAMMAR_REPO"
mkdir -p "$DEST"
# Clear generated files but keep `target/` so rebuilds stay incremental.
rm -f "$DEST/extension.toml" "$DEST/Cargo.toml" "$DEST/Cargo.lock" \
  "$DEST/extension.wasm" "$DEST/src" "$DEST/languages"
rm -rf "$DEST/grammars"

# Zed clones grammars from a git repository. Snapshot the working tree into a
# throwaway repo so local, uncommitted grammar edits can be installed too.
echo "==> Snapshotting grammar"
rsync -a \
  --exclude '.git' \
  --exclude 'node_modules' \
  --exclude 'target' \
  --exclude 'build' \
  "$GRAMMAR_SRC/" "$GRAMMAR_REPO/"
(
  cd "$GRAMMAR_REPO"
  git init -q
  git add -A
  git -c user.name="dio-dev-install" -c user.email="dev@localhost" \
    commit -qm "tree-sitter-strategy snapshot"
)
GRAMMAR_REV="$(git -C "$GRAMMAR_REPO" rev-parse HEAD)"

echo "==> Preparing extension"
mkdir -p "$DEST"
ln -s "$EXT_SRC/src" "$DEST/src"
ln -s "$EXT_SRC/languages" "$DEST/languages"
cp "$EXT_SRC/Cargo.toml" "$DEST/Cargo.toml"
cp "$EXT_SRC/Cargo.lock" "$DEST/Cargo.lock"

python3 - "$EXT_SRC/extension.toml" "$DEST/extension.toml" \
  "file://$GRAMMAR_REPO" "$GRAMMAR_REV" <<'PY'
import sys

source, dest, repo, rev = sys.argv[1:]
lines = open(source).read().splitlines()
in_grammar = False
for i, line in enumerate(lines):
    stripped = line.strip()
    if stripped == "[grammars.strategy]":
        in_grammar = True
    elif stripped.startswith("[") and stripped != "[grammars.strategy]":
        in_grammar = False
    elif in_grammar and stripped.startswith("repository = "):
        lines[i] = f'repository = "{repo}"'
    elif in_grammar and stripped.startswith("rev = "):
        lines[i] = f'rev = "{rev}"'
    elif in_grammar and stripped.startswith("path = "):
        lines[i] = 'path = "."'
open(dest, "w").write("\n".join(lines) + "\n")
PY

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required to build the extension" >&2
  exit 1
fi
rustup target add wasm32-wasip2 >/dev/null 2>&1 || true

echo "==> Building extension (wasm32-wasip2)"
(
  cd "$DEST"
  cargo build --release --target wasm32-wasip2
)
cp "$DEST/target/wasm32-wasip2/release/dio_strategy.wasm" "$DEST/extension.wasm"

find_wasi_sdk_clang() {
  if [[ -n "${WASI_SDK_PATH:-}" && -x "$WASI_SDK_PATH/bin/clang" ]]; then
    echo "$WASI_SDK_PATH/bin/clang"
    return 0
  fi
  if [[ -x "$ZED_DATA/extensions/build/wasi-sdk/bin/clang" ]]; then
    echo "$ZED_DATA/extensions/build/wasi-sdk/bin/clang"
    return 0
  fi
  return 1
}

if ! CLANG="$(find_wasi_sdk_clang)"; then
  case "$(uname)-$(uname -m)" in
    Darwin-arm64) WASI_PLATFORM="arm64-macos" ;;
    Darwin-x86_64) WASI_PLATFORM="x86_64-macos" ;;
    Linux-x86_64) WASI_PLATFORM="x86_64-linux" ;;
    Linux-aarch64 | Linux-arm64) WASI_PLATFORM="arm64-linux" ;;
    *)
      echo "error: no wasi-sdk build for $(uname)-$(uname -m); set WASI_SDK_PATH" >&2
      exit 1
      ;;
  esac
  WASI_SDK_DIR="$ZED_DATA/extensions/build/wasi-sdk"
  WASI_TMP="$BUILD_DIR/wasi-sdk-temp"
  WASI_URL="https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-34/wasi-sdk-34.0-$WASI_PLATFORM.tar.gz"

  if [[ ! -d "$WASI_TMP" ]]; then
    echo "==> Downloading wasi-sdk 34 ($WASI_PLATFORM, one-time)"
    mkdir -p "$WASI_TMP"
    curl -fL --progress-bar "$WASI_URL" -o "$BUILD_DIR/wasi-sdk.tar.gz"
    tar -xzf "$BUILD_DIR/wasi-sdk.tar.gz" -C "$WASI_TMP"
    rm -f "$BUILD_DIR/wasi-sdk.tar.gz"
  fi
  mkdir -p "$(dirname "$WASI_SDK_DIR")"
  rm -rf "$WASI_SDK_DIR"
  mv "$WASI_TMP"/wasi-sdk-34.0-* "$WASI_SDK_DIR"
  rm -rf "$WASI_TMP"
  CLANG="$WASI_SDK_DIR/bin/clang"
fi

echo "==> Compiling tree-sitter grammar"
mkdir -p "$DEST/grammars"
"$CLANG" -fPIC -shared -Os \
  -Wl,--export=tree_sitter_strategy \
  -o "$DEST/grammars/strategy.wasm" \
  -I "$GRAMMAR_REPO/src" \
  "$GRAMMAR_REPO/src/parser.c"

echo "==> Installing into $INSTALLED_DIR"
mkdir -p "$INSTALLED_DIR"
if [[ -e "$INSTALLED_DIR/$EXTENSION_ID" && ! -L "$INSTALLED_DIR/$EXTENSION_ID" ]]; then
  echo "error: $INSTALLED_DIR/$EXTENSION_ID exists and is not a dev symlink; move it aside first" >&2
  exit 1
fi
ln -sfn "$DEST" "$INSTALLED_DIR/$EXTENSION_ID"

echo
echo "Installed $EXTENSION_ID -> $DEST"
echo "Zed should pick it up automatically. If it does not: cmd-shift-p -> zed: reload extensions."
