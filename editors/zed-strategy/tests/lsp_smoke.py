#!/usr/bin/env python3
"""Smoke-test `dio strategy lsp` over stdio as a plain LSP client.

Run from anywhere:

    python3 editors/zed-strategy/tests/lsp_smoke.py [_build/default/bin/main.exe]

It exercises the handshake, full and incremental document sync, diagnostics,
and the shutdown/exit sequence. Exits non-zero on the first failed expectation.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def frame(message: dict) -> bytes:
    body = json.dumps(message).encode()
    return b"Content-Length: %d\r\n\r\n%s" % (len(body), body)


class Client:
    def __init__(self, binary: Path):
        self.proc = subprocess.Popen(
            [str(binary), "strategy", "lsp"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.pending = b""
        self.messages: list[dict] = []
        self.cond = threading.Condition()
        self.dead = False
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        stream = self.proc.stdout
        assert stream is not None
        while True:
            chunk = stream.read(1)
            if not chunk:
                with self.cond:
                    self.dead = True
                    self.cond.notify_all()
                return
            self.pending += chunk
            while True:
                sep = self.pending.find(b"\r\n\r\n")
                if sep == -1:
                    break
                header = self.pending[:sep].decode("ascii", "replace")
                length = None
                for line in header.split("\r\n"):
                    if line.lower().startswith("content-length:"):
                        length = int(line.split(":", 1)[1].strip())
                if length is None:
                    self.pending = self.pending[sep + 4 :]
                    continue
                body_start = sep + 4
                if len(self.pending) < body_start + length:
                    break
                body = self.pending[body_start : body_start + length]
                self.pending = self.pending[body_start + length :]
                with self.cond:
                    self.messages.append(json.loads(body))
                    self.cond.notify_all()

    def send(self, message: dict) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(frame(message))
        self.proc.stdin.flush()

    def wait_for(self, predicate, timeout: float = 5.0) -> dict:
        deadline = time.time() + timeout
        with self.cond:
            while True:
                for message in self.messages:
                    if predicate(message):
                        return message
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise AssertionError(
                        f"timed out waiting for message; got {self.messages}"
                    )
                self.cond.wait(remaining)

    def response(self, request_id: int, timeout: float = 5.0) -> dict:
        return self.wait_for(
            lambda m: m.get("id") == request_id and "method" not in m, timeout
        )

    def diagnostics(self, uri: str, timeout: float = 5.0) -> list[dict]:
        message = self.wait_for(
            lambda m: (
                m.get("method") == "textDocument/publishDiagnostics"
                and m.get("params", {}).get("uri") == uri
            ),
            timeout,
        )
        return message["params"].get("diagnostics", [])

    def clear_diagnostics_seen(self) -> None:
        with self.cond:
            self.messages = [
                m
                for m in self.messages
                if m.get("method") != "textDocument/publishDiagnostics"
            ]


def main() -> int:
    binary = (
        Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "_build/default/bin/main.exe"
    )
    if not binary.exists():
        print(f"missing dio binary: {binary}", file=sys.stderr)
        return 2

    client = Client(binary)
    uri = "file:///tmp/dio-strategy-smoke.strategy"
    version = 1

    client.send(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "processId": os.getpid(),
                "rootUri": None,
                "capabilities": {
                    "general": {"positionEncodings": ["utf-16", "utf-8"]},
                    "textDocument": {"publishDiagnostics": {"versionSupport": True}},
                },
            },
        }
    )
    result = client.response(1)["result"]
    caps = result["capabilities"]
    assert caps["textDocumentSync"]["openClose"] is True
    assert caps["textDocumentSync"]["change"] == 1
    assert result["serverInfo"]["name"] == "dio-strategy"
    print("initialize: ok")

    client.send({"jsonrpc": "2.0", "method": "initialized", "params": {}})

    text = (ROOT / "strategies/jacobs_ladder.strategy").read_text()
    client.send(
        {
            "jsonrpc": "2.0",
            "method": "textDocument/didOpen",
            "params": {
                "textDocument": {
                    "uri": uri,
                    "languageId": "strategy",
                    "version": version,
                    "text": text,
                }
            },
        }
    )
    diags = client.diagnostics(uri)
    assert diags == [], f"expected clean diagnostics, got {diags[:3]}"
    print("didOpen (valid file): ok")

    # Point at an action that does not exist; the validator should flag it.
    broken = text.replace("init.venue.state", "init.venue.state_typo", 1)
    client.clear_diagnostics_seen()
    version += 1
    client.send(
        {
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {"uri": uri, "version": version},
                "contentChanges": [{"text": broken}],
            },
        }
    )
    diags = client.diagnostics(uri)
    assert diags, "expected a diagnostic for the unknown action"
    assert any("unknown action" in d["message"] for d in diags), diags[0]
    assert diags[0]["source"] == "dio"
    print(f"didChange (full sync): ok ({len(diags)} diagnostic(s))")

    # Incremental edit: replace `state_typo` with `state`.
    line = next(
        i for i, l in enumerate(broken.splitlines()) if "init.venue.state_typo" in l
    )
    character = broken.splitlines()[line].index("state_typo")
    client.clear_diagnostics_seen()
    version += 1
    client.send(
        {
            "jsonrpc": "2.0",
            "method": "textDocument/didChange",
            "params": {
                "textDocument": {"uri": uri, "version": version},
                "contentChanges": [
                    {
                        "range": {
                            "start": {"line": line, "character": character},
                            "end": {
                                "line": line,
                                "character": character + len("state_typo"),
                            },
                        },
                        "text": "state",
                    }
                ],
            },
        }
    )
    diags = client.diagnostics(uri)
    assert diags == [], f"expected diagnostics to clear, got {diags[:3]}"
    print("didChange (incremental sync): ok")

    client.send(
        {
            "jsonrpc": "2.0",
            "method": "textDocument/didClose",
            "params": {"textDocument": {"uri": uri}},
        }
    )

    client.send({"jsonrpc": "2.0", "id": 2, "method": "shutdown", "params": None})
    assert client.response(2)["result"] is None
    client.send({"jsonrpc": "2.0", "method": "exit", "params": None})

    try:
        rc = client.proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        client.proc.kill()
        raise AssertionError("server did not exit after `exit`")
    assert rc == 0, f"server exited {rc}"
    print("shutdown/exit: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
