'use strict';

// Dio Strategy editor support.
//
//   - Language registration + TextMate grammar give `*.strategy` files syntax
//     highlighting (see package.json / syntaxes/).
//   - This client speaks just enough Language Server Protocol (over stdio) to a
//     spawned `dio strategy lsp` process to get live validation diagnostics,
//     which are shown through the standard VS Code diagnostic UI.
//
// It is deliberately dependency-free so the extension works from source with
// no npm install.

const vscode = require('vscode');
const { spawn } = require('child_process');

let server = null;
let pending = Buffer.alloc(0);
let shuttingDown = false;
let nextId = 0;
let diagnostics = null;

function isStrategyDoc(doc) {
  return doc.languageId === 'strategy' || doc.fileName.endsWith('.strategy');
}

function sendMessage(method, params, id) {
  if (!server || server.killed) return;
  const msg = { jsonrpc: '2.0', method, params };
  if (id !== undefined) msg.id = id;
  const body = JSON.stringify(msg);
  const payload = Buffer.from(body, 'utf8');
  const header = Buffer.from(`Content-Length: ${payload.length}\r\n\r\n`, 'ascii');
  server.stdin.write(Buffer.concat([header, payload]));
}

function toSeverity(sev) {
  switch (sev) {
    case 1: return vscode.DiagnosticSeverity.Error;
    case 2: return vscode.DiagnosticSeverity.Warning;
    case 3: return vscode.DiagnosticSeverity.Information;
    case 4: return vscode.DiagnosticSeverity.Hint;
    default: return vscode.DiagnosticSeverity.Error;
  }
}

function toDiagnostic(d) {
  const r = d.range || { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } };
  const range = new vscode.Range(r.start.line, r.start.character, r.end.line, r.end.character);
  const diag = new vscode.Diagnostic(range, d.message || 'strategy error', toSeverity(d.severity));
  diag.source = d.source || 'dio';
  return diag;
}

function handleServerData(chunk) {
  pending = Buffer.concat([pending, chunk]);
  for (;;) {
    const sep = pending.indexOf('\r\n\r\n');
    if (sep === -1) return;
    const head = pending.slice(0, sep).toString('ascii');
    const match = /Content-Length:\s*(\d+)/i.exec(head);
    const start = sep + 4;
    if (!match) {
      pending = pending.slice(start);
      continue;
    }
    const length = parseInt(match[1], 10);
    if (pending.length < start + length) return;
    const body = pending.slice(start, start + length).toString('utf8');
    pending = pending.slice(start + length);
    let msg = null;
    try {
      msg = JSON.parse(body);
    } catch (e) {
      continue;
    }
    if (msg && msg.method === 'textDocument/publishDiagnostics' && msg.params) {
      const uri = vscode.Uri.parse(msg.params.uri);
      diagnostics.set(uri, (msg.params.diagnostics || []).map(toDiagnostic));
    }
  }
}

function startServer() {
  pending = Buffer.alloc(0);
  const binary = vscode.workspace.getConfiguration('dioStrategy').get('binary', 'dio');
  server = spawn(binary, ['strategy', 'lsp']);
  server.stdout.on('data', (d) => handleServerData(d));
  server.stderr.on('data', (d) => console.error('[dio strategy lsp]', d.toString()));
  server.on('error', (e) => {
    if (!shuttingDown) {
      vscode.window.showWarningMessage(
        `dio strategy lsp: could not start '${binary}'. ` +
        'Syntax highlighting still works; validation is disabled. ' +
        `(${e.message}) Set dioStrategy.binary if dio is not on PATH.`
      );
    }
  });
  server.on('close', () => {
    server = null;
    if (!shuttingDown) startServer();
  });
  sendMessage('initialize', { processId: process.pid, capabilities: {} }, nextId++);
  sendMessage('initialized', {});
  for (const doc of vscode.workspace.textDocuments) {
    if (isStrategyDoc(doc)) sendOpen(doc);
  }
}

function sendOpen(doc) {
  sendMessage('textDocument/didOpen', {
    textDocument: {
      uri: doc.uri.toString(),
      languageId: doc.languageId,
      version: doc.version,
      text: doc.getText()
    }
  });
}

function activate(context) {
  diagnostics = vscode.languages.createDiagnosticCollection('strategy');
  context.subscriptions.push(diagnostics);

  startServer();

  context.subscriptions.push(
    vscode.workspace.onDidOpenTextDocument((doc) => {
      if (isStrategyDoc(doc)) sendOpen(doc);
    }),
    vscode.workspace.onDidChangeTextDocument((e) => {
      if (isStrategyDoc(e.document)) {
        sendMessage('textDocument/didChange', {
          textDocument: { uri: e.document.uri.toString(), version: e.document.version },
          contentChanges: [{ text: e.document.getText() }]
        });
      }
    }),
    vscode.workspace.onDidSaveTextDocument((doc) => {
      if (isStrategyDoc(doc)) {
        sendMessage('textDocument/didSave', { textDocument: { uri: doc.uri.toString() } });
      }
    }),
    vscode.workspace.onDidCloseTextDocument((doc) => {
      if (isStrategyDoc(doc)) {
        sendMessage('textDocument/didClose', { textDocument: { uri: doc.uri.toString() } });
      }
    })
  );
}

function deactivate() {
  shuttingDown = true;
  if (server && !server.killed) {
    try {
      sendMessage('shutdown', null, nextId++);
      server.stdin.end();
    } catch (e) {
      /* ignore */
    }
    setTimeout(() => {
      if (server && !server.killed) server.kill();
    }, 500);
  }
}

module.exports = { activate, deactivate };