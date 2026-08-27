// Black-box contract tests against a running relay.
// Usage: node tests/contract.mjs [base-url]   (default http://127.0.0.1:4000)
// Uses Node's built-in WebSocket so the suite has no dependencies.

import { spawn } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
const BASE = process.argv[2] ?? "http://127.0.0.1:4000";
const WS = BASE.replace(/^http/, "ws");

let passed = 0;
const failures = [];

function check(name, condition, detail = "") {
  if (condition) {
    passed++;
    console.log(`  ok   ${name}`);
  } else {
    failures.push(`${name}${detail ? ` — ${detail}` : ""}`);
    console.log(`  FAIL ${name}${detail ? ` — ${detail}` : ""}`);
  }
}

function eq(name, actual, expected) {
  check(name, Object.is(actual, expected), `expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let counter = 0;
const uniqueServerId = () => `srv_${process.pid}_${++counter}`;

/** Opens a socket and collects messages plus the close code. */
function open(url) {
  const socket = new WebSocket(url);
  const messages = [];
  const state = { socket, messages, closed: null, opened: false };
  state.ready = new Promise((resolve, reject) => {
    socket.addEventListener("open", () => {
      state.opened = true;
      resolve(state);
    });
    socket.addEventListener("close", (event) => {
      state.closed = { code: event.code, reason: event.reason };
      if (!state.opened) reject(new Error(`closed before open: ${event.code}`));
      resolve(state);
    });
    socket.addEventListener("error", () => {});
  });
  socket.addEventListener("message", (event) => messages.push(event.data));
  state.next = async (predicate, timeoutMs = 3000) => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const found = messages.find((m) => {
        try {
          return predicate(JSON.parse(m));
        } catch {
          return false;
        }
      });
      if (found) return JSON.parse(found);
      if (state.closed) return null;
      await sleep(20);
    }
    return null;
  };
  state.waitClose = async (timeoutMs = 3000) => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline && !state.closed) await sleep(20);
    return state.closed;
  };
  return state;
}

const relayUrl = (params) => `${WS}/ws?${new URLSearchParams(params)}`;

async function httpEndpoints() {
  console.log("\nHTTP endpoints");
  const health = await fetch(`${BASE}/health`);
  eq("/health status", health.status, 200);
  eq("/health content-type", health.headers.get("content-type"), "application/json");
  eq("/health body", await health.text(), '{"status":"ok"}');

  const ready = await fetch(`${BASE}/ready`);
  eq("/ready status", ready.status, 200);
  eq("/ready body", await ready.text(), '{"status":"ready"}');

  const metrics = await fetch(`${BASE}/metrics`);
  eq("/metrics status", metrics.status, 200);
  eq("/metrics content-type", metrics.headers.get("content-type"), "text/plain; version=0.0.4");
  const body = await metrics.text();
  check("/metrics exposes readiness", body.includes("paseo_relay_ready 1"));

  const missing = await fetch(`${BASE}/nope`);
  eq("unknown path status", missing.status, 404);
  eq("unknown path body", await missing.text(), "not found\n");

  const plainWs = await fetch(`${BASE}/ws`);
  eq("/ws without upgrade status", plainWs.status, 426);
  eq("/ws without upgrade body", await plainWs.text(), "Expected WebSocket upgrade");
}

/// Sends a real WebSocket handshake so the relay gets past its upgrade check and actually
/// parses the query string. `fetch` cannot do this: Upgrade is a forbidden header.
function rawUpgrade(path) {
  const url = new URL(BASE);
  const port = Number(url.port || 80);
  return new Promise((resolve, reject) => {
    const socket = net.connect(port, url.hostname, () => {
      socket.write(
        `GET ${path} HTTP/1.1\r\n` +
          `Host: ${url.hostname}:${port}\r\n` +
          `Upgrade: websocket\r\n` +
          `Connection: Upgrade\r\n` +
          `Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n` +
          `Sec-WebSocket-Version: 13\r\n\r\n`,
      );
    });
    let buffer = Buffer.alloc(0);
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error("upgrade probe timed out"));
    }, 3000);
    socket.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      const separator = buffer.indexOf("\r\n\r\n");
      if (separator === -1) return;
      const head = buffer.subarray(0, separator).toString();
      const length = Number(/content-length:\s*(\d+)/i.exec(head)?.[1] ?? 0);
      const body = buffer.subarray(separator + 4);
      if (body.length < length) return;
      clearTimeout(timer);
      socket.destroy();
      resolve({ status: Number(head.split(" ")[1]), body: body.subarray(0, length).toString() });
    });
    socket.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
}

async function upgradeIsCheckedFirst() {
  console.log("\nUpgrade check precedes parameter parsing");
  // Matches socket.ex:14-19: a non-upgrade request is refused before the query is inspected,
  // so even a malformed query yields 426 rather than 400.
  const response = await fetch(`${BASE}/ws?role=bogus`);
  eq("non-upgrade request with a bad query still gets 426", response.status, 426);
  eq("  body", await response.text(), "Expected WebSocket upgrade");
}

async function queryValidation() {
  console.log("\nQuery parameter validation");
  const cases = [
    [{}, "Missing or invalid role parameter"],
    [{ role: "peer", serverId: "s" }, "Missing or invalid role parameter"],
    [{ role: " server", serverId: "s" }, "Missing or invalid role parameter"],
    [{ role: "client" }, "Missing serverId parameter"],
    [{ role: "client", serverId: "" }, "Missing serverId parameter"],
    [{ role: "client", serverId: "x".repeat(257) }, "serverId is too long"],
    [{ role: "client", serverId: "s", v: "3" }, "Invalid v parameter (expected 1 or 2)"],
    [{ role: "client", serverId: "s", v: "2", connectionId: "x".repeat(257) }, "connectionId is too long"],
  ];
  for (const [params, expected] of cases) {
    const { status, body } = await rawUpgrade(`/ws?${new URLSearchParams(params)}`);
    eq(`400 for ${JSON.stringify(params).slice(0, 56)}`, status, 400);
    eq(`  body`, body, expected);
  }
}

async function v2HappyPath() {
  console.log("\nv2 routing");
  const serverId = uniqueServerId();
  const control = open(relayUrl({ serverId, role: "server", v: "2" }));
  await control.ready;

  const sync = await control.next((m) => m.type === "sync");
  check("control receives sync immediately on attach", sync !== null);
  check("sync carries a connectionIds array", Array.isArray(sync?.connectionIds));

  const connectionId = "conn_test_1";
  const client = open(relayUrl({ serverId, role: "client", v: "2", connectionId }));
  await client.ready;

  const connected = await control.next((m) => m.type === "connected");
  eq("control is told a client connected", connected?.connectionId, connectionId);

  const data = open(relayUrl({ serverId, role: "server", v: "2", connectionId }));
  await data.ready;
  await sleep(100);

  client.socket.send("from-client");
  await sleep(200);
  check("client frame reaches the data channel", data.messages.includes("from-client"));

  data.socket.send("from-daemon");
  await sleep(200);
  check("data frame reaches the client", client.messages.includes("from-daemon"));

  // A second client on the same connectionId must receive broadcasts too.
  const client2 = open(relayUrl({ serverId, role: "client", v: "2", connectionId }));
  await client2.ready;
  await sleep(100);
  data.socket.send("broadcast");
  await sleep(200);
  check("broadcast reaches first client", client.messages.includes("broadcast"));
  check("broadcast reaches second client", client2.messages.includes("broadcast"));

  control.socket.send(JSON.stringify({ type: "ping" }));
  const pong = await control.next((m) => m.type === "pong");
  check("control ping is answered with pong", pong !== null);
  check("pong carries a numeric ts", typeof pong?.ts === "number");

  // Dropping every client closes the data channel and notifies control.
  client.socket.close();
  client2.socket.close();
  const dataClose = await data.waitClose();
  eq("data channel closes with 1001 when all clients leave", dataClose?.code, 1001);
  eq("  reason", dataClose?.reason, "Client disconnected");
  const disconnected = await control.next((m) => m.type === "disconnected");
  eq("control is told the client disconnected", disconnected?.connectionId, connectionId);

  control.socket.close();
}

async function serverDisconnectCascade() {
  console.log("\nData channel disconnect cascades to clients");
  const serverId = uniqueServerId();
  const connectionId = "conn_cascade";
  const control = open(relayUrl({ serverId, role: "server", v: "2" }));
  await control.ready;
  const client = open(relayUrl({ serverId, role: "client", v: "2", connectionId }));
  await client.ready;
  const data = open(relayUrl({ serverId, role: "server", v: "2", connectionId }));
  await data.ready;
  await sleep(100);

  data.socket.close();
  const closed = await client.waitClose();
  eq("client closes with 1012 when its data channel drops", closed?.code, 1012);
  eq("  reason", closed?.reason, "Server disconnected");
  control.socket.close();
}

async function replacement() {
  console.log("\nSlot replacement");
  const serverId = uniqueServerId();
  const first = open(relayUrl({ serverId, role: "server", v: "2" }));
  await first.ready;
  await sleep(50);
  const second = open(relayUrl({ serverId, role: "server", v: "2" }));
  await second.ready;

  const closed = await first.waitClose();
  eq("previous control channel closes with 1008", closed?.code, 1008);
  eq("  reason", closed?.reason, "Replaced by new connection");
  second.socket.close();
}

async function handshakeValidation() {
  console.log("\nHandshake validation");
  const serverId = uniqueServerId();
  const control = open(relayUrl({ serverId, role: "server", v: "2" }));
  await control.ready;

  const client = open(relayUrl({ serverId, role: "client", v: "2", connectionId: "conn_hs" }));
  await client.ready;
  const allZero = Buffer.alloc(32).toString("base64");
  client.socket.send(JSON.stringify({ type: "hello", key: allZero }));
  const closed = await client.waitClose();
  eq("blocklisted handshake key closes with 1008", closed?.code, 1008);
  eq("  reason", closed?.reason, "Invalid handshake key");

  // A well-formed key is forwarded like any other frame.
  const good = Buffer.alloc(32);
  good[0] = 9;
  const client2 = open(relayUrl({ serverId, role: "client", v: "2", connectionId: "conn_hs2" }));
  await client2.ready;
  const data = open(relayUrl({ serverId, role: "server", v: "2", connectionId: "conn_hs2" }));
  await data.ready;
  await sleep(100);
  const envelope = JSON.stringify({ type: "hello", key: good.toString("base64") });
  client2.socket.send(envelope);
  await sleep(200);
  check("valid handshake is forwarded verbatim", data.messages.includes(envelope));
  check("valid handshake does not close the client", client2.closed === null);

  control.socket.close();
}

async function oversizedMessage() {
  console.log("\nOversized message");
  const serverId = uniqueServerId();
  // The control channel caps payloads at 64 KiB, so this exercises the same code path as the
  // 32 MiB data limit without allocating 32 MiB.
  const control = open(relayUrl({ serverId, role: "server", v: "2" }));
  await control.ready;
  control.socket.send("x".repeat(70 * 1024));
  const closed = await control.waitClose();
  eq("oversized control payload closes with 1009", closed?.code, 1009);
}

async function closeHandshake() {
  console.log("\nClose handshake");
  // tungstenite queues the RFC 6455 echo itself but can only flush it while the stream is
  // still polled. Returning from the read loop on Close truncates the connection and every
  // peer reports 1006 instead of a clean close.
  const serverId = uniqueServerId();
  const cases = [
    ["close() with no code", (socket) => socket.close(), 1005],
    ["close(1000)", (socket) => socket.close(1000), 1000],
  ];
  for (const [label, closer, expected] of cases) {
    const socket = open(relayUrl({ serverId, role: "server", v: "2" }));
    await socket.ready;
    await sleep(50);
    closer(socket.socket);
    const closed = await socket.waitClose();
    eq(`${label} completes the handshake`, closed?.code, expected);
  }

  const withReason = open(relayUrl({ serverId, role: "server", v: "2" }));
  await withReason.ready;
  await sleep(50);
  withReason.socket.close(1000, "bye");
  const closed = await withReason.waitClose();
  eq("close reason is echoed back", closed?.reason, "bye");
}

async function v1Routing() {
  console.log("\nv1 routing");
  const serverId = uniqueServerId();
  const server = open(relayUrl({ serverId, role: "server" }));
  await server.ready;
  const client = open(relayUrl({ serverId, role: "client" }));
  await client.ready;
  await sleep(100);

  client.socket.send("v1-up");
  await sleep(200);
  check("v1 client frame reaches the server", server.messages.includes("v1-up"));

  server.socket.send("v1-down");
  await sleep(200);
  check("v1 server frame reaches the client", client.messages.includes("v1-down"));

  server.socket.close();
  client.socket.close();
}

async function generatedConnectionId() {
  console.log("\nGenerated connection ids");
  const serverId = uniqueServerId();
  const control = open(relayUrl({ serverId, role: "server", v: "2" }));
  await control.ready;
  const client = open(relayUrl({ serverId, role: "client", v: "2" }));
  await client.ready;

  const connected = await control.next((m) => m.type === "connected");
  check("relay assigns a connectionId when the client omits one", /^conn_[0-9a-f]{16}$/.test(connected?.connectionId ?? ""));
  client.socket.close();
  control.socket.close();
}

async function controlWatchdogSurvivesClientReplacement() {
  console.log("\nControl watchdog ignores stale client timers");
  const serverId = uniqueServerId();
  const connectionId = "conn_watchdog";
  const control = open(relayUrl({ serverId, role: "server", v: "2" }));
  await control.ready;

  const firstClient = open(relayUrl({ serverId, role: "client", v: "2", connectionId }));
  await firstClient.ready;
  await sleep(500);
  firstClient.socket.close();
  await sleep(100);

  const secondClient = open(relayUrl({ serverId, role: "client", v: "2", connectionId }));
  await secondClient.ready;
  const data = open(relayUrl({ serverId, role: "server", v: "2", connectionId }));
  await data.ready;
  await sleep(16_000);

  check("control stays alive after stale watchdog expiry", control.closed === null);
  secondClient.socket.close();
  data.socket.close();
  control.socket.close();
}

async function attachWaitRespectsDeliveryBudget() {
  console.log("\nAttach wait respects delivery budget");
  const bin = process.env.PASEO_RELAY_BIN ?? "target/release/paseo-relay";
  if (!fs.existsSync(bin)) {
    check("release binary exists", false, `build with: cargo build --release (missing ${bin})`);
    return;
  }

  const port = 4099;
  const configPath = `${import.meta.dirname}/attach-budget-config.toml`;
  fs.writeFileSync(configPath, `port = ${port}\ndelivery_timeout_ms = 800\ndata_attach_timeout_ms = 15000\n`);
  const child = spawn(bin, [], {
    env: {
      ...process.env,
      PASEO_RELAY_CONFIG: configPath,
    },
    stdio: "ignore",
  });

  try {
    const healthUrl = `http://127.0.0.1:${port}/health`;
    let up = false;
    for (let i = 0; i < 150; i++) {
      try {
        const response = await fetch(healthUrl);
        if (response.status === 200) {
          up = true;
          break;
        }
      } catch {}
      await sleep(20);
    }
    if (!up) throw new Error("second relay instance failed to start within 3s");

    const serverId = uniqueServerId();
    const connectionId = "conn_attach_budget";
    const url = `ws://127.0.0.1:${port}/ws?${new URLSearchParams({
      serverId,
      role: "client",
      v: "2",
      connectionId,
    })}`;
    const client = open(url);
    await client.ready;
    client.socket.send("probe");
    const closed = await client.waitClose(2000);
    eq("attach wait capped by delivery budget closes with 1013", closed?.code, 1013);
    eq("  reason", closed?.reason, "Data route unavailable");
  } finally {
    child.kill();
    fs.rmSync(configPath, { force: true });
  }
}

async function controlQueueNotLimitedByCount() {
  console.log("\nControl channel not limited by message count");
  const serverId = uniqueServerId();
  const control = open(relayUrl({ serverId, role: "server", v: "2" }));
  await control.ready;

  for (let i = 0; i < 1100; i++) {
    control.socket.send(JSON.stringify({ type: "ping" }));
  }
  await sleep(500);
  check("control survives 1100 small pings without 1013 close", control.closed === null);
  control.socket.close();
}

const suites = [
  httpEndpoints,
  upgradeIsCheckedFirst,
  queryValidation,
  v2HappyPath,
  serverDisconnectCascade,
  replacement,
  handshakeValidation,
  oversizedMessage,
  closeHandshake,
  v1Routing,
  generatedConnectionId,
  controlWatchdogSurvivesClientReplacement,
  controlQueueNotLimitedByCount,
  attachWaitRespectsDeliveryBudget,
];

for (const suite of suites) {
  try {
    await suite();
  } catch (error) {
    failures.push(`${suite.name} threw: ${error.message}`);
    console.log(`  FAIL ${suite.name} threw: ${error.message}`);
  }
}

console.log(`\n${passed} passed, ${failures.length} failed`);
if (failures.length) {
  console.log("\nFailures:");
  for (const failure of failures) console.log(`  - ${failure}`);
}
// Sockets left open by the suite would otherwise keep the event loop alive.
process.exit(failures.length ? 1 : 0);
