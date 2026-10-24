import http from "node:http";
import crypto from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = process.env.PORT || 10000;
const WEBHOOK_URL = process.env.WEBHOOK_URL || "https://2fast.com.ng/api/device-webhook";
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
const TABLE_NAME = process.env.DEVICE_TABLE || "wallet7"; // 'wallet7' for platform devices or 'devices'

// In-memory registry: socket_id -> { ws, device, version, socketId, connectedAt, lastSeen }
const devicesBySocketId = new Map();
// Pending HTTP requests waiting for immediate device ACK by ref: ref -> resolve()
const pendingAcks = new Map();

// Helper: Sync status to Supabase (wallet7 or devices table)
async function syncDeviceToSupabase(socketId, deviceModel, status = "online") {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) return;

  try {
    const endpoint = `${SUPABASE_URL.replace(/\/+$/, "")}/rest/v1/${TABLE_NAME}`;
    const now = new Date().toISOString();

    // Check if device row already exists with this device_name
    const searchRes = await fetch(
      `${endpoint}?device_name=eq.${encodeURIComponent(deviceModel)}&select=id,device_id`,
      {
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        },
      }
    );
    const existing = await searchRes.json();

    if (Array.isArray(existing) && existing.length > 0) {
      // Update existing device row with new socket_id and status
      await fetch(`${endpoint}?id=eq.${existing[0].id}`, {
        method: "PATCH",
        headers: {
          "content-type": "application/json",
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          Prefer: "return=minimal",
        },
        body: JSON.stringify({
          socket_id: socketId,
          status,
          last_seen: now,
          updated_at: now,
        }),
      });
      console.log(`[DB] Updated ${TABLE_NAME} for device '${deviceModel}' -> socket_id: ${socketId}, status: ${status}`);
    } else {
      // Insert new device row
      const deviceId = crypto.randomUUID();
      const authCode = crypto.randomBytes(4).toString("hex").toUpperCase();
      await fetch(endpoint, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          Prefer: "return=minimal",
        },
        body: JSON.stringify({
          device_name: deviceModel,
          device_id: deviceId,
          authorization_code: authCode,
          socket_id: socketId,
          status,
          last_seen: now,
        }),
      });
      console.log(`[DB] Inserted new ${TABLE_NAME} device row: '${deviceModel}' -> socket_id: ${socketId}`);
    }
  } catch (err) {
    console.error("[DB] Supabase sync error:", err.message);
  }
}

// Helper: Post final operation result to your website webhook
async function forwardResultToWebhook(payload) {
  try {
    console.log(`[Webhook] Forwarding result for ref '${payload.ref}' to ${WEBHOOK_URL}...`);
    const res = await fetch(WEBHOOK_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    const replyText = await res.text();
    console.log(`[Webhook] Webhook response (status ${res.status}):`, replyText.slice(0, 300));
  } catch (err) {
    console.error(`[Webhook] Failed forwarding result for ref '${payload.ref}':`, err.message);
  }
}

// ----------------------------------------------------
// HTTP Server (Health checks, device list & sending transactions)
// ----------------------------------------------------
const server = http.createServer(async (req, res) => {
  // CORS headers
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "content-type, authorization");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  const url = new URL(req.url, `http://${req.headers.host}`);

  // 1. Health check (Render requirement)
  if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/health")) {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(
      JSON.stringify({
        status: "ok",
        service: "2fast-device-websocket",
        connected_devices: devicesBySocketId.size,
        uptime: Math.round(process.uptime()),
      })
    );
  }

  // 2. List currently online devices
  if (req.method === "GET" && url.pathname === "/devices") {
    const list = Array.from(devicesBySocketId.values()).map((d) => ({
      socket_id: d.socketId,
      device: d.device,
      version: d.version,
      connectedAt: d.connectedAt,
      lastSeen: d.lastSeen,
    }));
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ count: list.length, devices: list }));
  }

  // 3. Send transaction command to device: POST /send or POST /send-txn or POST /dial
  if (req.method === "POST" && ["/send", "/send-txn", "/dial"].includes(url.pathname)) {
    let bodyText = "";
    req.on("data", (chunk) => (bodyText += chunk));
    req.on("end", async () => {
      let body;
      try {
        body = JSON.parse(bodyText);
      } catch {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ status: false, message: "Invalid JSON body" }));
      }

      // Extract socket_id and transaction payload
      const socketId = body.socket_id || body.socketId;
      const ref = String(body.ref || "").trim();
      const phone = String(body.phone || "").trim();
      const amount = Number(body.amount);
      const pin = String(body.pin || "");

      if (!socketId) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ status: false, message: "Missing socket_id" }));
      }
      if (!ref || !phone) {
        res.writeHead(400, { "content-type": "application/json" });
        return res.end(JSON.stringify({ status: false, message: "Missing ref or phone" }));
      }

      const client = devicesBySocketId.get(socketId);
      if (!client || client.ws.readyState !== WebSocket.OPEN) {
        res.writeHead(404, { "content-type": "application/json" });
        return res.end(
          JSON.stringify({
            status: false,
            message: `Device with socket_id '${socketId}' is offline or disconnected`,
          })
        );
      }

      // Payload sent to phone app (socket_id excluded as requested)
      const txnPayload = {
        type: "txn",
        ref,
        phone,
        amount,
        pin,
      };

      console.log(`[TXN] Sending to socket_id '${socketId}' (${client.device}):`, txnPayload);

      // Setup promise waiting for device ACK (timeout after 10 seconds)
      const ackPromise = new Promise((resolve) => {
        const timer = setTimeout(() => {
          pendingAcks.delete(ref);
          resolve({ ok: false, message: "Device did not return immediate ACK within 10s" });
        }, 10000);

        pendingAcks.set(ref, (ackData) => {
          clearTimeout(timer);
          pendingAcks.delete(ref);
          resolve({ ok: true, ack: ackData });
        });
      });

      // Send to WebSocket
      client.ws.send(JSON.stringify(txnPayload));

      const ackResult = await ackPromise;
      if (!ackResult.ok) {
        res.writeHead(504, { "content-type": "application/json" });
        return res.end(JSON.stringify({ status: false, message: ackResult.message }));
      }

      res.writeHead(200, { "content-type": "application/json" });
      return res.end(
        JSON.stringify({
          status: true,
          message: "Transaction accepted by device",
          socket_id: socketId,
          ref,
          ack: ackResult.ack,
        })
      );
    });
    return;
  }

  res.writeHead(404, { "content-type": "application/json" });
  res.end(JSON.stringify({ message: "Not found" }));
});

// ----------------------------------------------------
// WebSocket Server
// ----------------------------------------------------
const wss = new WebSocketServer({ server });

wss.on("connection", (ws, req) => {
  // Generate a unique socket_id for this connection
  const socketId = `sock_${crypto.randomBytes(8).toString("hex")}`;
  ws.socketId = socketId;
  ws.isAlive = true;

  console.log(`[WS] New device connection opened: ${socketId} from ${req.socket.remoteAddress}`);

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      console.warn(`[WS] Non-JSON message received from ${socketId}:`, raw.toString());
      return;
    }

    const type = msg.type;

    // 1. Handshake: "hello"
    if (type === "hello") {
      const deviceModel = msg.device || "Unknown Device";
      const version = msg.version || "1.0";

      // Register or update in memory map
      const info = {
        ws,
        socketId,
        device: deviceModel,
        version,
        connectedAt: new Date().toISOString(),
        lastSeen: new Date().toISOString(),
      };
      devicesBySocketId.set(socketId, info);

      console.log(`[WS] Device Hello registered: model='${deviceModel}', version='${version}', socketId='${socketId}'`);

      // Reply with hello_ack to show connected status on phone app
      ws.send(JSON.stringify({ type: "hello_ack", socket_id: socketId }));

      // Sync into Supabase (wallet7 or devices)
      await syncDeviceToSupabase(socketId, deviceModel, "online");
      return;
    }

    // 2. Immediate ACK from device: { "type": "ack", "ref": "TX123" }
    if (type === "ack") {
      const ref = msg.ref;
      console.log(`[WS] Received immediate ACK for ref '${ref}' from ${socketId}`);
      const resolver = pendingAcks.get(ref);
      if (resolver) {
        resolver(msg);
      }
      return;
    }

    // 3. Final operation result: { "type": "result", "ref": "...", "status": "success"|"failed", ... }
    if (type === "result") {
      console.log(`[WS] Received operation RESULT from ${socketId}:`, msg);
      // Forward the exact payload to https://2fast.com.ng/api/device-webhook
      await forwardResultToWebhook(msg);
      return;
    }

    console.log(`[WS] Received other message type '${type}' from ${socketId}:`, msg);
  });

  ws.on("close", async () => {
    console.log(`[WS] Device disconnected: ${socketId}`);
    const client = devicesBySocketId.get(socketId);
    if (client) {
      devicesBySocketId.delete(socketId);
      await syncDeviceToSupabase(socketId, client.device, "offline");
    }
  });

  ws.on("error", (err) => {
    console.error(`[WS] Error on socket ${socketId}:`, err.message);
  });
});

// Periodic ping/pong to prune zombie connections every 30s
const pingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on("close", () => clearInterval(pingInterval));

server.listen(PORT, "0.0.0.0", () => {
  console.log(`===============================================`);
  console.log(`🚀 2fast Device WebSocket Server running on port ${PORT}`);
  console.log(`🔗 Webhook URL: ${WEBHOOK_URL}`);
  console.log(`💾 Database Target Table: ${TABLE_NAME}`);
  console.log(`===============================================`);
});
