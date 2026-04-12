/**
 * Lighter API Proxy — Cloudflare Worker + Durable Object
 *
 * Routes all Lighter traffic through a Durable Object pinned to Asia-East
 * (locationHint: "apac") to bypass CloudFront geo-restrictions.
 *
 * HTTP requests are forwarded to the DO which fetches from Lighter with an
 * appropriate regional IP.
 * WebSocket requests are handled at the entrypoint (where CF recognises the
 * upgrade) and the DO establishes the upstream connection and manages
 * keepalive + transparent reconnection.
 *
 * Deploy:  cd proxy/cloudflare && npx wrangler deploy
 * Usage:   export LIGHTER_PROXY_URL=https://lighter-proxy.malciller.workers.dev
 */

const LIGHTER_ORIGIN = "https://mainnet.zklighter.elliot.ai";
const LOCATION_HINT = "apac";

// ─── Env type ────────────────────────────────────────────────────────────────

interface Env {
  LIGHTER_PROXY: DurableObjectNamespace;
}

interface UpstreamSession {
  id: string;
  upstream: WebSocket | null;
  client: WebSocket | null;
  buffer: string[];
  keepaliveInterval: ReturnType<typeof setInterval> | null;
  expiryTimer: ReturnType<typeof setTimeout> | null;
  subscriptions: Set<string>;
  clientClosed: boolean;
  reconnecting: boolean;
  internalToUpstreamCount: number;
  upstreamToInternalCount: number;
  upstreamConnectCount: number;
}

// ─── Worker entrypoint ───────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const isWebSocket =
      url.pathname === "/stream" ||
      request.headers.get("upgrade")?.toLowerCase() === "websocket";

    if (isWebSocket) {
      return handleWebSocket(url, env);
    }

    // HTTP: Route non-authenticated directly from edge to bypass DO billing
    const isAuth =
      url.searchParams.has("auth") ||
      url.searchParams.has("signature") ||
      request.headers.has("authorization");

    if (!isAuth) {
      const targetUrl = new URL(request.url);
      targetUrl.hostname = "mainnet.zklighter.elliot.ai";
      targetUrl.protocol = "https:";
      targetUrl.port = "443";
      
      const reqHeaders = new Headers(request.headers);
      reqHeaders.set("host", "mainnet.zklighter.elliot.ai");
      reqHeaders.delete("cf-connecting-ip");
      reqHeaders.delete("cf-ipcountry");
      reqHeaders.delete("cf-ray");
      reqHeaders.delete("cf-visitor");
      reqHeaders.delete("cf-worker");

      try {
        const resp = await fetch(targetUrl.toString(), {
          method: request.method,
          headers: reqHeaders,
          body: request.method !== "GET" && request.method !== "HEAD" ? request.body : undefined,
        });
        const respHeaders = new Headers(resp.headers);
        respHeaders.set("access-control-allow-origin", "*");
        respHeaders.set("x-proxy-region", "edge");

        return new Response(resp.body, {
          status: resp.status,
          statusText: resp.statusText,
          headers: respHeaders,
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: String(err) }), {
          status: 502,
          headers: { "content-type": "application/json" },
        });
      }
    }

    // HTTP Auth: forward to DO pinned in Asia-East
    const id = env.LIGHTER_PROXY.idFromName("lighter-apac");
    const stub = env.LIGHTER_PROXY.get(id, { locationHint: LOCATION_HINT });
    return stub.fetch(request);
  },
};

// ─── WebSocket handling ──────────────────────────────────────────────────────
// WebSocket upgrade must be returned from the entrypoint (where CF recognises
// the client upgrade). We create the client-facing pair here, then ask the DO
// (running in Asia-East) to establish the upstream connection and relay
// messages back through a simple message-passing protocol over an internal DO
// WebSocket.

async function handleWebSocket(url: URL, env: Env): Promise<Response> {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();

  // Ask the DO to connect upstream and relay via an internal WebSocket
  const id = env.LIGHTER_PROXY.idFromName("lighter-apac");
  const stub = env.LIGHTER_PROXY.get(id, { locationHint: LOCATION_HINT });

  // Use a special internal endpoint to establish the upstream connection
  const internalUrl = new URL(url.toString());
  internalUrl.pathname = "/__ws_upstream";
  // Pass the original search params (e.g. ?readonly=true)
  internalUrl.search = url.search;

  const doResp = await stub.fetch(internalUrl.toString(), {
    headers: { Upgrade: "websocket" },
  });

  const doSocket = doResp.webSocket;
  if (!doSocket) {
    console.error(`DO WebSocket relay failed: status=${doResp.status}`);
    const body = await doResp.text().catch(() => "");
    console.error(`DO response body: ${body.slice(0, 500)}`);
    server.close(1011, "Failed to connect upstream via DO");
    return new Response("Failed to establish upstream connection", {
      status: 502,
    });
  }

  doSocket.accept();

  // Diagnostic counters
  let clientToDoCount = 0;
  let doToClientCount = 0;

  // Relay: client → DO (→ upstream)
  server.addEventListener("message", (event) => {
    try {
      if (doSocket.readyState === WebSocket.READY_STATE_OPEN) {
        clientToDoCount++;
        if (clientToDoCount <= 5) {
          console.log(`[relay] client→DO #${clientToDoCount}: ${String(event.data).slice(0, 200)}`);
        }
        doSocket.send(event.data);
      }
    } catch {
      // DO socket already closed
    }
  });

  server.addEventListener("close", (event) => {
    console.log(`Client WS closed: code=${event.code} reason="${event.reason}"`);
    try {
      doSocket.close(event.code, event.reason);
    } catch {
      // already closed
    }
  });

  server.addEventListener("error", (event) => {
    console.error("Client-side WebSocket error:", event);
    try { doSocket.close(1011, "client error"); } catch {}
  });

  // Relay: DO (upstream →) → client
  doSocket.addEventListener("message", (event) => {
    try {
      if (server.readyState === WebSocket.READY_STATE_OPEN) {
        doToClientCount++;
        if (doToClientCount <= 3 || doToClientCount % 500 === 0) {
          console.log(`[relay] DO→client #${doToClientCount}: ${String(event.data).slice(0, 120)}`);
        }
        server.send(event.data);
      }
    } catch {
      // server side already closed
    }
  });

  doSocket.addEventListener("close", (event) => {
    console.log(`DO WS closed: code=${event.code} reason="${event.reason}", relayed ${doToClientCount} msgs to client`);
    try {
      server.close(event.code, event.reason);
    } catch {
      // already closed
    }
  });

  doSocket.addEventListener("error", (event) => {
    console.error("DO-side WebSocket error:", event);
    try { server.close(1011, "DO relay error"); } catch {}
  });

  return new Response(null, { status: 101, webSocket: client });
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── Durable Object: LighterProxy ───────────────────────────────────────────
// Runs in Asia-East. All outbound fetch() and WebSocket connections
// exit from an appropriate Cloudflare PoP → CloudFront sees allowed IP → 200.

export class LighterProxy implements DurableObject {
  private state: DurableObjectState;
  private sessions = new Map<string, UpstreamSession>();

  constructor(state: DurableObjectState, _env: Env) {
    this.state = state;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // ── Internal WebSocket upstream relay ──
    if (url.pathname === "/__ws_upstream") {
      return this.handleUpstreamRelay(url);
    }

    // ── HTTP reverse proxy ──
    return this.handleHttp(request, url);
  }

  // ── HTTP proxy ──────────────────────────────────────────────────────────

  private async handleHttp(request: Request, url: URL): Promise<Response> {
    const targetUrl = `${LIGHTER_ORIGIN}${url.pathname}${url.search}`;

    const headers = new Headers(request.headers);
    headers.delete("host");
    headers.set("host", "mainnet.zklighter.elliot.ai");
    // Remove CF-specific headers that could confuse the upstream
    headers.delete("cf-connecting-ip");
    headers.delete("cf-ipcountry");
    headers.delete("cf-ray");
    headers.delete("cf-visitor");
    headers.delete("cf-worker");

    try {
      const resp = await fetch(targetUrl, {
        method: request.method,
        headers,
        body: request.method !== "GET" && request.method !== "HEAD"
          ? request.body
          : undefined,
      });

      const respHeaders = new Headers(resp.headers);
      respHeaders.set("access-control-allow-origin", "*");
      respHeaders.set("x-proxy-region", LOCATION_HINT);

      return new Response(resp.body, {
        status: resp.status,
        statusText: resp.statusText,
        headers: respHeaders,
      });
    } catch (err) {
      return new Response(JSON.stringify({ error: String(err) }), {
        status: 502,
        headers: { "content-type": "application/json" },
      });
    }
  }

  // ── Upstream WebSocket relay ─────────────────────────────────────────────
  // Called internally by the worker entrypoint. Establishes the upstream
  // WebSocket to Lighter from this DO, relays messages over an internal
  // WebSocket pair back to the entrypoint. Manages upstream keepalive and
  // transparent reconnection so the client never sees upstream drops.

  private async handleUpstreamRelay(url: URL): Promise<Response> {
    const sessionId = url.searchParams.get("sessionId") || "default-" + Math.random().toString(36).slice(2);
    
    // Create the internal WebSocket pair (between entrypoint and this DO)
    const [client, server] = Object.values(new WebSocketPair());

    let session = this.sessions.get(sessionId);

    if (session) {
      console.log(`[DO] Resuming session ${sessionId}, flushing buffer (${session.buffer.length} msgs)`);
      if (session.expiryTimer) {
        clearTimeout(session.expiryTimer);
        session.expiryTimer = null;
      }
      
      session.client = server;
      session.clientClosed = false;

      server.accept();

      let flushCount = 0;
      for (const msg of session.buffer) {
        try {
           server.send(msg);
           flushCount++;
        } catch {
           break;
        }
      }
      session.buffer = [];
      console.log(`[DO] Session ${sessionId} flushed ${flushCount} msgs`);
      
      this.wireSessionInternal(session);

      return new Response(null, { status: 101, webSocket: client });
    }

    // ── Create New Session ──
    session = {
      id: sessionId,
      upstream: null,
      client: server,
      buffer: [],
      keepaliveInterval: null,
      expiryTimer: null,
      subscriptions: new Set<string>(),
      clientClosed: false,
      reconnecting: false,
      internalToUpstreamCount: 0,
      upstreamToInternalCount: 0,
      upstreamConnectCount: 0,
    };
    this.sessions.set(sessionId, session);

    // ── Wire up the new session ──
    const upstream = await this.connectUpstream(url, session);
    if (!upstream) {
      this.cleanupSession(session);
      return new Response(
        JSON.stringify({ error: "Failed to connect upstream WebSocket" }),
        { status: 502, headers: { "content-type": "application/json" } },
      );
    }
    session.upstream = upstream;

    server.accept();

    this.wireUpstream(upstream, session, url);
    this.startKeepalive(session);
    this.wireSessionInternal(session);

    return new Response(null, { status: 101, webSocket: client });
  }

  // ── Session Helpers ──────────────────────────────────────────────────────

  private async connectUpstream(url: URL, session: UpstreamSession): Promise<WebSocket | null> {
    const upstreamUrl = `${LIGHTER_ORIGIN}/stream${url.search}`;
    try {
      const upstreamResp = await fetch(upstreamUrl, {
        headers: { Upgrade: "websocket" },
      });

      const ws = upstreamResp.webSocket;
      if (!ws) {
        console.error(`Upstream WS err: status=${upstreamResp.status}`);
        return null;
      }

      ws.accept();
      session.upstreamConnectCount++;
      console.log(`[DO] Upstream connected (#${session.upstreamConnectCount}) for session ${session.id}`);
      return ws;
    } catch (err) {
      console.error(`[DO] Upstream connect error: ${err}`);
      return null;
    }
  }

  private startKeepalive(session: UpstreamSession) {
    this.stopKeepalive(session);
    session.keepaliveInterval = setInterval(() => {
      try {
        if (session.upstream && session.upstream.readyState === WebSocket.READY_STATE_OPEN) {
          session.upstream.send(JSON.stringify({ type: "ping" }));
        }
      } catch {}
    }, 30_000);
  }

  private stopKeepalive(session: UpstreamSession) {
    if (session.keepaliveInterval !== null) {
      clearInterval(session.keepaliveInterval);
      session.keepaliveInterval = null;
    }
  }

  private replaySubscriptions(ws: WebSocket, session: UpstreamSession) {
    if (session.subscriptions.size === 0) return;
    console.log(`[DO] Replaying ${session.subscriptions.size} subscriptions`);
    for (const sub of session.subscriptions) {
      try {
        if (ws.readyState === WebSocket.READY_STATE_OPEN) {
          ws.send(sub);
        }
      } catch {}
    }
  }

  private wireUpstream(ws: WebSocket, session: UpstreamSession, url: URL) {
    ws.addEventListener("message", (event) => {
      try {
        if (session.client && session.client.readyState === WebSocket.READY_STATE_OPEN && !session.clientClosed) {
          session.upstreamToInternalCount++;
          if (session.upstreamToInternalCount <= 3 || session.upstreamToInternalCount % 1000 === 0) {
            console.log(`[DO] upstream→internal #${session.upstreamToInternalCount}: ${String(event.data).slice(0, 120)}`);
          }
          session.client.send(event.data);
        } else {
          // Client is disconnected. Buffer the message!
          session.buffer.push(String(event.data));
          if (session.buffer.length > 5000) {
            session.buffer.shift(); // Drop oldest to avoid OOM
          }
        }
      } catch {}
    });

    ws.addEventListener("close", (event) => {
      console.log(`[DO] Upstream closed: code=${event.code} reason="${event.reason}"`);
      
      // If we are already terminating the session, or reconnecting
      if (session.expiryTimer === null && session.clientClosed) {
        return; // normal disconnect after expiry
      }
      
      if (session.reconnecting) return;
      session.reconnecting = true;

      (async () => {
        console.log(`[DO] Upstream reconnecting for session ${session.id}`);
        const newWs = await this.connectUpstream(url, session);
        if (newWs) {
          session.upstream = newWs;
          this.wireUpstream(newWs, session, url);
          this.replaySubscriptions(newWs, session);
          session.reconnecting = false;
          console.log(`[DO] Upstream reconnected`);
          return;
        }

        // Reconnet failed
        session.reconnecting = false;
        console.error(`[DO] Upstream reconnect failed for ${session.id}`);
        try {
          if (session.client && !session.clientClosed) {
             session.client.close(1011, "upstream reconnect failed");
          }
        } catch {}
      })();
    });

    ws.addEventListener("error", (event) => {
      console.error("[DO] Upstream WebSocket error:", event);
    });
  }

  private wireSessionInternal(session: UpstreamSession) {
    if (!session.client) return;
    const server = session.client;

    server.addEventListener("message", (event) => {
      try {
        const data = String(event.data);

        try {
          const parsed = JSON.parse(data);
          if (parsed.type === "subscribe") {
            session.subscriptions.add(data);
          } else if (parsed.type === "unsubscribe") {
            for (const sub of session.subscriptions) {
              try {
                const s = JSON.parse(sub);
                if (s.channel === parsed.channel) {
                  session.subscriptions.delete(sub);
                  break;
                }
              } catch {}
            }
          }
        } catch {}

        if (session.upstream && session.upstream.readyState === WebSocket.READY_STATE_OPEN) {
          session.internalToUpstreamCount++;
          if (session.internalToUpstreamCount <= 10) {
             console.log(`[DO] internal→upstream #${session.internalToUpstreamCount}: ${data.slice(0, 200)}`);
          }
          session.upstream.send(event.data);
        } else {
          // Upstream unavailable (reconnecting, cold-starting, or null).
          // Always respond to pings so the client doesn't mark the proxied
          // connection as dead while the DO manages upstream recovery.
          try {
            const parsed = JSON.parse(data);
            if (parsed.type === "ping" && server.readyState === WebSocket.READY_STATE_OPEN) {
              server.send(JSON.stringify({ type: "pong" }));
            }
          } catch {}
          if (session.reconnecting) {
            console.warn(`[DO] Upstream reconnecting, dropped client msg`);
          }
        }
      } catch {}
    });

    server.addEventListener("close", (event) => {
      console.log(`[DO] Client disconnected from session ${session.id}: code=${event.code}`);
      session.clientClosed = true;
      session.client = null;
      
      // Start buffering grace period (60s)
      session.expiryTimer = setTimeout(() => {
        console.log(`[DO] Session ${session.id} expired. Cleaning up.`);
        this.cleanupSession(session);
      }, 60_000);
    });

    server.addEventListener("error", (event) => {
      console.error(`[DO] internal server error for ${session.id}:`, event);
      try {
         server.close(1011, "relay error");
      } catch {} // triggers close event
    });
  }

  private cleanupSession(session: UpstreamSession) {
    this.sessions.delete(session.id);
    this.stopKeepalive(session);
    try {
      if (session.upstream && session.upstream.readyState === WebSocket.READY_STATE_OPEN) {
        session.upstream.close(1000, "session expired");
      }
    } catch {}
  }
}
