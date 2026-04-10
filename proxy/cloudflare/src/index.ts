/**
 * Lighter API Proxy — Cloudflare Worker + Durable Object
 *
 * Routes all Lighter traffic through a Durable Object pinned to Western Europe
 * (locationHint: "weur") to bypass CloudFront US geo-restrictions.
 *
 * HTTP requests are forwarded to the DO which fetches from Lighter with an EU IP.
 * WebSocket requests are handled at the entrypoint (where CF recognises the
 * upgrade) and the DO establishes the upstream connection from EU.
 *
 * Deploy:  cd proxy/cloudflare && npx wrangler deploy
 * Usage:   export LIGHTER_PROXY_URL=https://lighter-proxy.malciller.workers.dev
 */

const LIGHTER_ORIGIN = "https://mainnet.zklighter.elliot.ai";

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

    // HTTP: forward to DO pinned in Western Europe
    const id = env.LIGHTER_PROXY.idFromName("lighter-eu");
    const stub = env.LIGHTER_PROXY.get(id, { locationHint: "weur" });
    return stub.fetch(request);
  },
};

// ─── WebSocket handling ──────────────────────────────────────────────────────
// WebSocket upgrade must be returned from the entrypoint (where CF recognises
// the client upgrade). We create the client-facing pair here, then ask the DO
// (running in EU) to establish the upstream connection and relay messages back
// through a simple message-passing protocol over an internal DO WebSocket.

async function handleWebSocket(url: URL, env: Env): Promise<Response> {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();

  // Ask the DO to connect upstream and relay via an internal WebSocket
  const id = env.LIGHTER_PROXY.idFromName("lighter-eu");
  const stub = env.LIGHTER_PROXY.get(id, { locationHint: "weur" });

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

// ─── Durable Object: LighterProxy ───────────────────────────────────────────
// Runs in Western Europe. All outbound fetch() and WebSocket connections
// exit from a European Cloudflare PoP → CloudFront sees EU IP → 200 OK.

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
      respHeaders.set("x-proxy-region", "weur");

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
  // WebSocket to Lighter from this EU DO, and relays messages over an
  // internal WebSocket pair back to the entrypoint.

  private async handleUpstreamRelay(url: URL): Promise<Response> {
    // Create the internal WebSocket pair (between entrypoint and this DO)
    const [client, server] = Object.values(new WebSocketPair());

    // Connect upstream to Lighter from this EU Durable Object
    // CF Workers fetch() requires https:// (not wss://) for WebSocket upgrades
    const upstreamUrl = `${LIGHTER_ORIGIN}/stream${url.search}`;

    try {
      // Let CF Workers handle WebSocket negotiation headers automatically.
      // Do NOT set Sec-WebSocket-Version manually — it conflicts with the
      // runtime's auto-negotiation and can cause upstream rejection.
      const upstreamResp = await fetch(upstreamUrl, {
        headers: {
          Upgrade: "websocket",
        },
      });

      const upstream = upstreamResp.webSocket;
      if (!upstream) {
        console.error(
          `Upstream WS upgrade failed: status=${upstreamResp.status} ${upstreamResp.statusText}`
        );
        const body = await upstreamResp.text().catch(() => "");
        console.error(`Upstream response body: ${body.slice(0, 500)}`);
        return new Response(
          `Failed to connect upstream WebSocket (${upstreamResp.status})`,
          { status: 502 }
        );
      }

      // Accept both sides
      upstream.accept();
      server.accept();

      // Diagnostic counters
      let internalToUpstreamCount = 0;
      let upstreamToInternalCount = 0;

      // Relay: internal (from entrypoint) → upstream (to Lighter)
      server.addEventListener("message", (event) => {
        try {
          if (upstream.readyState === WebSocket.READY_STATE_OPEN) {
            internalToUpstreamCount++;
            if (internalToUpstreamCount <= 10) {
              console.log(`[DO] internal→upstream #${internalToUpstreamCount}: ${String(event.data).slice(0, 200)}`);
            }
            upstream.send(event.data);
          } else {
            console.warn(`[DO] upstream not open (state=${upstream.readyState}), dropped msg #${internalToUpstreamCount + 1}`);
          }
        } catch {
          // upstream already closed
        }
      });

      server.addEventListener("close", (event) => {
        console.log(`[DO] Internal relay closed: code=${event.code}`);
        try {
          upstream.close(event.code, event.reason);
        } catch {
          // already closed
        }
      });

      server.addEventListener("error", (event) => {
        console.error("Internal relay server-side error:", event);
        try { upstream.close(1011, "relay error"); } catch {}
      });

      // Relay: upstream (from Lighter) → internal (to entrypoint)
      upstream.addEventListener("message", (event) => {
        try {
          if (server.readyState === WebSocket.READY_STATE_OPEN) {
            upstreamToInternalCount++;
            if (upstreamToInternalCount <= 3 || upstreamToInternalCount % 500 === 0) {
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

      upstream.addEventListener("close", (event) => {
        console.log(`[DO] Upstream WS closed: code=${event.code} reason="${event.reason}", relayed ${upstreamToInternalCount} msgs`);
        try {
          server.close(event.code, event.reason);
        } catch {
          // already closed
        }
      });

      upstream.addEventListener("error", (event) => {
        console.error("Upstream WebSocket error:", event);
        try { server.close(1011, "upstream error"); } catch {}
      });

      return new Response(null, { status: 101, webSocket: client });
    } catch (err) {
      console.error("Upstream WebSocket connection failed:", err);
      return new Response(
        JSON.stringify({ error: String(err) }),
        { status: 502, headers: { "content-type": "application/json" } },
      );
    }
  }
}
