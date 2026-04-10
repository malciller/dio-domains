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

    // HTTP: forward to DO pinned in Asia-East
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
    // Create the internal WebSocket pair (between entrypoint and this DO)
    const [client, server] = Object.values(new WebSocketPair());

    // ── State for this relay session ──
    let upstream: WebSocket | null = null;
    let keepaliveInterval: ReturnType<typeof setInterval> | null = null;
    let clientClosed = false;
    let reconnecting = false;
    const subscriptions = new Set<string>();

    // Diagnostic counters (cumulative across reconnects)
    let internalToUpstreamCount = 0;
    let upstreamToInternalCount = 0;
    let upstreamConnectCount = 0;

    // ── Connect to Lighter upstream ──
    const connectUpstream = async (): Promise<WebSocket | null> => {
      const upstreamUrl = `${LIGHTER_ORIGIN}/stream${url.search}`;
      try {
        const upstreamResp = await fetch(upstreamUrl, {
          headers: { Upgrade: "websocket" },
        });

        const ws = upstreamResp.webSocket;
        if (!ws) {
          console.error(
            `Upstream WS upgrade failed: status=${upstreamResp.status} ${upstreamResp.statusText}`
          );
          return null;
        }

        ws.accept();
        upstreamConnectCount++;
        console.log(`[DO] Upstream connected (#${upstreamConnectCount})`);
        return ws;
      } catch (err) {
        console.error(`[DO] Upstream connect error: ${err}`);
        return null;
      }
    };

    // ── Start keepalive interval ──
    const startKeepalive = () => {
      stopKeepalive();
      keepaliveInterval = setInterval(() => {
        try {
          if (upstream && upstream.readyState === WebSocket.READY_STATE_OPEN) {
            upstream.send(JSON.stringify({ type: "ping" }));
          }
        } catch {
          // Upstream gone, will be caught by close handler
        }
      }, 30_000);
    };

    // ── Stop keepalive interval ──
    const stopKeepalive = () => {
      if (keepaliveInterval !== null) {
        clearInterval(keepaliveInterval);
        keepaliveInterval = null;
      }
    };

    // ── Replay subscriptions to a new upstream ──
    const replaySubscriptions = (ws: WebSocket) => {
      if (subscriptions.size === 0) return;
      console.log(`[DO] Replaying ${subscriptions.size} subscriptions to new upstream`);
      for (const sub of subscriptions) {
        try {
          if (ws.readyState === WebSocket.READY_STATE_OPEN) {
            ws.send(sub);
          }
        } catch {
          console.warn("[DO] Failed to replay subscription during reconnect");
        }
      }
    };

    // ── Wire event handlers for an upstream WebSocket ──
    const wireUpstream = (ws: WebSocket) => {
      // Relay: upstream (from Lighter) → internal (to entrypoint → client)
      ws.addEventListener("message", (event) => {
        try {
          if (server.readyState === WebSocket.READY_STATE_OPEN) {
            upstreamToInternalCount++;
            if (upstreamToInternalCount <= 3 || upstreamToInternalCount % 1000 === 0) {
              console.log(`[DO] upstream→internal #${upstreamToInternalCount}: ${String(event.data).slice(0, 120)}`);
            }
            server.send(event.data);
          } else {
            console.warn(`[DO] internal not open (state=${server.readyState}), dropped msg #${upstreamToInternalCount + 1}`);
          }
        } catch {
          // server side already closed
        }
      });

      // Upstream close: attempt transparent reconnect
      ws.addEventListener("close", (event) => {
        console.log(`[DO] Upstream closed: code=${event.code} reason="${event.reason}" (relayed ${upstreamToInternalCount} msgs total)`);
        stopKeepalive();

        // If client already disconnected, nothing to reconnect for
        if (clientClosed) {
          console.log("[DO] Client already closed, not reconnecting upstream");
          return;
        }

        // If already reconnecting (from a previous close), skip
        if (reconnecting) {
          console.log("[DO] Already reconnecting, skipping duplicate");
          return;
        }

        // Attempt transparent reconnect
        reconnecting = true;
        (async () => {
          const maxAttempts = 5;
          for (let attempt = 0; attempt < maxAttempts; attempt++) {
            if (clientClosed) {
              console.log("[DO] Client closed during reconnect, aborting");
              return;
            }

            const delay = Math.min(1000 * Math.pow(2, attempt), 16_000);
            console.log(`[DO] Upstream reconnect attempt ${attempt + 1}/${maxAttempts} in ${delay}ms`);
            await sleep(delay);

            if (clientClosed) {
              console.log("[DO] Client closed during reconnect backoff, aborting");
              return;
            }

            const newWs = await connectUpstream();
            if (newWs) {
              upstream = newWs;
              wireUpstream(newWs);
              replaySubscriptions(newWs);
              startKeepalive();
              reconnecting = false;
              console.log(`[DO] Upstream reconnected successfully (attempt ${attempt + 1})`);
              return;
            }
          }

          // All attempts exhausted
          reconnecting = false;
          console.error(`[DO] Upstream reconnect failed after ${maxAttempts} attempts, closing client`);
          try {
            server.close(1011, "upstream reconnect failed");
          } catch {}
        })();
      });

      ws.addEventListener("error", (event) => {
        console.error("[DO] Upstream WebSocket error:", event);
        // The close event will follow and trigger reconnect
      });
    };

    // ── Initial upstream connection ──
    upstream = await connectUpstream();
    if (!upstream) {
      return new Response(
        JSON.stringify({ error: "Failed to connect upstream WebSocket" }),
        { status: 502, headers: { "content-type": "application/json" } },
      );
    }

    // Accept internal server side
    server.accept();

    // Wire upstream event handlers
    wireUpstream(upstream);

    // Start keepalive
    startKeepalive();

    // Relay: internal (from entrypoint/client) → upstream (to Lighter)
    server.addEventListener("message", (event) => {
      try {
        const data = String(event.data);

        // Track subscription messages for replay on reconnect
        try {
          const parsed = JSON.parse(data);
          if (parsed.type === "subscribe") {
            subscriptions.add(data);
          } else if (parsed.type === "unsubscribe") {
            // Remove matching subscription if client unsubscribes
            // Match by channel since the rest of the payload should be identical
            for (const sub of subscriptions) {
              try {
                const s = JSON.parse(sub);
                if (s.channel === parsed.channel) {
                  subscriptions.delete(sub);
                  break;
                }
              } catch {}
            }
          }
        } catch {
          // Not JSON, just relay
        }

        if (upstream && upstream.readyState === WebSocket.READY_STATE_OPEN) {
          internalToUpstreamCount++;
          if (internalToUpstreamCount <= 10) {
            console.log(`[DO] internal→upstream #${internalToUpstreamCount}: ${data.slice(0, 200)}`);
          }
          upstream.send(event.data);
        } else if (reconnecting) {
          // Upstream is reconnecting — drop the message but don't error.
          // Pings and subscription messages will be lost during reconnect,
          // but subscriptions will be replayed and pings will resume.
          console.warn(`[DO] Upstream reconnecting, dropped client msg #${internalToUpstreamCount + 1}`);
        } else {
          console.warn(`[DO] upstream not open (state=${upstream?.readyState}), dropped msg #${internalToUpstreamCount + 1}`);
        }
      } catch {
        // upstream already closed
      }
    });

    server.addEventListener("close", (event) => {
      console.log(`[DO] Internal relay closed: code=${event.code}`);
      clientClosed = true;
      stopKeepalive();
      try {
        if (upstream && upstream.readyState === WebSocket.READY_STATE_OPEN) {
          upstream.close(event.code, event.reason);
        }
      } catch {
        // already closed
      }
    });

    server.addEventListener("error", (event) => {
      console.error("[DO] Internal relay server-side error:", event);
      clientClosed = true;
      stopKeepalive();
      try {
        if (upstream) upstream.close(1011, "relay error");
      } catch {}
    });

    return new Response(null, { status: 101, webSocket: client });
  }
}
