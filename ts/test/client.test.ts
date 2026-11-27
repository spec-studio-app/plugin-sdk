// The client against the memory example plugin, over stdio and HTTP.
// The tests build the plugin with `go build`, or use $PLUGIN_MEMORY_BIN.
import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { chmodSync, copyFileSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Ajv2020 } from "ajv/dist/2020.js";
import addFormatsModule from "ajv-formats";
import { PluginClient, PluginError, isPluginError, sha256Hex, type MessageObserver } from "../src/index.js";

const here = dirname(fileURLToPath(import.meta.url));
// dist/test -> the repo root
const root = join(here, "..", "..", "..");
const tmp = mkdtempSync(join(tmpdir(), "plugin-sdk-ts-"));

function memoryBin(): string {
  if (process.env.PLUGIN_MEMORY_BIN) return process.env.PLUGIN_MEMORY_BIN;
  const out = join(tmp, "memory");
  execFileSync("go", ["build", "-o", out, "."], { cwd: join(root, "examples", "memory"), stdio: "inherit" });
  return out;
}

// ---- the schema check (R13.S2) ----

const addFormats = addFormatsModule as unknown as (ajv: Ajv2020) => Ajv2020;
const ajv = new Ajv2020({ strict: false, allErrors: true });
addFormats(ajv);
const schemaDir = join(root, "schema");
const validators = new Map<string, ReturnType<Ajv2020["compile"]>>();
for (const file of readdirSync(schemaDir)) {
  validators.set(file.replace(/\.json$/, ""), ajv.compile(JSON.parse(readFileSync(join(schemaDir, file), "utf8"))));
}
const schemaFailures: string[] = [];
let schemaChecked = 0;
/** Set while a test sends params that are not valid on purpose. */
let skipRequestCheck = false;

const observe: MessageObserver = (direction, method, raw) => {
  const msg = JSON.parse(raw);
  if (direction === "sent" && skipRequestCheck) return;
  const name = direction === "sent" ? `${method}.request` : msg.error ? "error" : `${method}.result`;
  const part = direction === "sent" ? msg.params : msg.error ?? msg.result;
  const validate = validators.get(name);
  if (!validate) return;
  schemaChecked++;
  if (!validate(part)) schemaFailures.push(`${name}: ${ajv.errorsText(validate.errors)}`);
};

function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const s = createServer();
    s.listen(0, "127.0.0.1", () => {
      const port = (s.address() as { port: number }).port;
      s.close(() => resolve(port));
    });
  });
}

async function waitFor(check: () => boolean, ms = 10_000): Promise<void> {
  const end = Date.now() + ms;
  while (!check()) {
    if (Date.now() > end) throw new Error("timed out");
    await new Promise((r) => setTimeout(r, 25));
  }
}

async function waitHttp(url: string): Promise<void> {
  const end = Date.now() + 10_000;
  for (;;) {
    try {
      await fetch(url, { method: "POST", body: "{}" });
      return;
    } catch {
      if (Date.now() > end) throw new Error("the HTTP plugin did not start");
      await new Promise((r) => setTimeout(r, 50));
    }
  }
}

let bin = "";
before(() => {
  bin = memoryBin();
});

const host = { kind: "relay" as const, version: "test" };

/** Every call of every capability (R11.S1). */
async function everyCall(c: PluginClient, ns: string): Promise<void> {
  // plugin
  const d = await c.describe();
  assert.equal(d.plugin.name, "memory");
  assert.deepEqual(c.info.capabilities, ["state", "events", "blobs", "watch"]);
  assert.equal((await c.ping()).status, "ok");

  // state
  const col = `${ns}-specs`;
  assert.deepEqual(await c.state.get({ collection: col, key: "abc" }), { exists: false });
  const v1 = await c.state.put({ collection: col, key: "abc", value: { title: "A" }, attributes: { pendingForMac: true }, expectedVersion: null });
  const got = await c.state.get({ collection: col, key: "abc" });
  assert.ok(got.exists);
  assert.equal(got.version, v1.version);
  assert.deepEqual(got.value, { title: "A" });
  const v2 = await c.state.put({ collection: col, key: "abc", value: { title: "B" }, attributes: { pendingForMac: false }, expectedVersion: v1.version });
  await c.state.put({ collection: col, key: "def", value: 1, attributes: { pendingForMac: true }, expectedVersion: null });
  const listed = await c.state.list({ collection: col, filter: { pendingForMac: true }, withValues: true });
  assert.deepEqual(listed.items.map((i) => [i.key, i.value]), [["def", 1]]);
  assert.equal((await c.state.listAll({ collection: col, pageSize: 1 })).length, 2);
  await c.state.delete({ collection: col, key: "abc", expectedVersion: v2.version });
  assert.equal((await c.state.get({ collection: col, key: "abc" })).exists, false);

  // events
  const stream = `${ns}-stream`;
  const appended = await c.events.append(stream, [
    { type: "A", data: 1 },
    { type: "B", data: { b: true }, at: "2026-01-02T10:05:00Z" },
  ]);
  assert.equal(appended.cursors.length, 2);
  await c.events.append(stream, [{ type: "C", data: null }]);
  assert.deepEqual((await c.events.readAll({ stream })).map((e) => e.type), ["A", "B", "C"]);
  assert.deepEqual((await c.events.read({ stream, after: appended.cursors[0] })).events.map((e) => e.type), ["B", "C"]);
  const ranged = await c.events.read({ stream, from: "2026-01-02T10:00:00Z", to: "2026-01-02T10:10:00Z" });
  assert.deepEqual(ranged.events.map((e) => e.type), ["B"]);
  await c.test.continueStream(stream);
  await c.events.append(stream, [{ type: "D", data: 4 }]);
  assert.deepEqual((await c.events.readAll({ stream, after: appended.cursors[1] })).map((e) => e.type), ["C", "D"]);
  assert.deepEqual(await c.events.delete(stream), { deleted: true });
  assert.equal((await c.events.read({ stream })).events.length, 0);

  // blobs
  const data = new Uint8Array(3 * 1024 * 1024 + 7).map((_, i) => (i * 31) % 251);
  const sha = await c.blobs.put(data);
  assert.equal(sha, sha256Hex(data));
  assert.deepEqual(new Uint8Array(await c.blobs.get(sha)), data);
  const missing = sha256Hex(new TextEncoder().encode(`${ns}-missing`));
  assert.deepEqual(await c.blobs.has([sha, missing]), [sha]);
  await assert.rejects(c.blobs.putAs(missing, new Uint8Array([1, 2, 3])), (e) => isPluginError(e, "refused"));
  await assert.rejects(c.blobs.get(missing), (e) => isPluginError(e, "not_found"));
  assert.equal(await c.blobs.delete(sha), true);
  assert.deepEqual(await c.blobs.has([sha]), []);

  // watch
  const w1 = await c.state.put({ collection: col, key: "w", value: 1, expectedVersion: null });
  const waiting = c.watch.wait({ collection: col, key: "w", version: w1.version, timeoutMs: 20_000 });
  await new Promise((r) => setTimeout(r, 200));
  const w2 = await c.state.put({ collection: col, key: "w", value: 2, expectedVersion: w1.version });
  assert.deepEqual(await waiting, { changed: true, version: w2.version });
  assert.deepEqual(await c.watch.wait({ collection: col, key: "w", version: w2.version, timeoutMs: 100 }), { changed: false });
  const s1 = await c.events.append(`${ns}-w`, [{ type: "A", data: 1 }]);
  const streamWait = c.watch.wait({ stream: `${ns}-w`, after: s1.cursors[0], timeoutMs: 20_000 });
  const s2 = await c.events.append(`${ns}-w`, [{ type: "B", data: 2 }]);
  assert.deepEqual(await streamWait, { changed: true, cursor: s2.cursors[0] });
}

/** R11.S2: a conflict is a typed error with the stored version. */
async function typedConflict(c: PluginClient, ns: string): Promise<void> {
  const col = `${ns}-conflict`;
  const v1 = await c.state.put({ collection: col, key: "k", value: 1, expectedVersion: null });
  const v2 = await c.state.put({ collection: col, key: "k", value: 2, expectedVersion: v1.version });
  const err = await c.state.put({ collection: col, key: "k", value: 3, expectedVersion: v1.version }).catch((e: unknown) => e);
  assert.ok(err instanceof PluginError);
  assert.equal(err.code, "conflict");
  assert.equal(err.rpcCode, -32002);
  assert.equal(err.storedVersion, v2.version);
  skipRequestCheck = true;
  try {
    await assert.rejects(c.state.get({ collection: col, key: "../x" }), (e) => isPluginError(e, "refused"));
  } finally {
    skipRequestCheck = false;
  }
}

describe("stdio", () => {
  test("the handshake and every call (R11.S1)", async () => {
    const logs: Array<Record<string, unknown>> = [];
    const c = await PluginClient.startStdio({ command: bin, host, log: (e) => logs.push(e), onMessage: observe });
    try {
      assert.equal(c.info.plugin.name, "memory");
      assert.equal(c.info.protocolVersion, "1.0");
      assert.ok(logs.some((l) => l.event === "plugin_handshake" && l.repo === "https://github.com/spec-studio-app/plugin-sdk"));
      await everyCall(c, "stdio");
      await typedConflict(c, "stdio");
    } finally {
      await c.shutdown();
    }
  });

  test("a call before the handshake is refused, and describe works (R1.S3)", async () => {
    const d = await PluginClient.describeStdio({ command: bin });
    assert.deepEqual(d.capabilities, ["state", "events", "blobs", "watch"]);
    // A raw call before the handshake.
    const child = spawn(bin, [], { stdio: ["pipe", "pipe", "ignore"] });
    const line = await new Promise<string>((resolve) => {
      createInterface({ input: child.stdout! }).once("line", resolve);
      child.stdin!.write(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "state.get", params: { collection: "c", key: "k" } }) + "\n");
    });
    child.kill();
    const msg = JSON.parse(line);
    assert.equal(msg.id, 1);
    assert.equal(msg.error.data.code, "refused");
    assert.equal(msg.error.data.reason, "handshake_required");
  });

  test("a restart that cannot start the plugin tries again (R9.S3)", async () => {
    const copy = join(tmp, "memory-copy");
    copyFileSync(bin, copy);
    chmodSync(copy, 0o755);
    const logs: Array<Record<string, unknown>> = [];
    const c = await PluginClient.startStdio({ command: copy, host, log: (e) => logs.push(e), restart: { minMs: 50, maxMs: 200 } });
    try {
      rmSync(copy);
      process.kill(c.pid!, "SIGKILL");
      await waitFor(() => logs.filter((l) => l.event === "plugin_restart_failed").length >= 2);
      copyFileSync(bin, copy);
      chmodSync(copy, 0o755);
      await waitFor(() => logs.some((l) => l.event === "plugin_restarted"));
      assert.equal((await c.ping()).status, "ok");
    } finally {
      await c.shutdown();
    }
  });

  test("no shared major version (R1.S2)", async () => {
    await assert.rejects(
      PluginClient.startStdio({ command: bin, host, protocolVersions: ["2.0"], restart: false }),
      (e) => isPluginError(e, "refused") && /2\.0/.test(e.message) && /1\.0/.test(e.message),
    );
  });

  test("a bad setting fails the handshake (R2.S2)", async () => {
    await assert.rejects(
      PluginClient.startStdio({ command: bin, host, settings: { nothing: "x" }, restart: false }),
      (e) => isPluginError(e, "bad_settings") && /nothing/.test(e.message),
    );
  });

  test("the plugin exits: the client logs why, fails the call, and restarts (R9.S3)", async () => {
    const logs: Array<Record<string, unknown>> = [];
    const data = join(tmp, "restart.json");
    const c = await PluginClient.startStdio({ command: bin, args: ["--data", data], host, log: (e) => logs.push(e), restart: { minMs: 50 } });
    try {
      const v = await c.state.put({ collection: "r", key: "k", value: "kept", expectedVersion: null });
      const firstPid = c.pid;
      const waiting = c.watch.wait({ collection: "r", key: "k", version: v.version, timeoutMs: 20_000 });
      await new Promise((r) => setTimeout(r, 100));
      process.kill(firstPid!, "SIGKILL");
      await assert.rejects(waiting, (e) => isPluginError(e, "unavailable"));
      await waitFor(() => logs.some((l) => l.event === "plugin_restarted"));
      const exited = logs.find((l) => l.event === "plugin_exited");
      assert.ok(exited);
      assert.equal(exited.signal, "SIGKILL");
      assert.ok(Array.isArray(exited.stderr));
      assert.notEqual(c.pid, firstPid);
      assert.equal((await c.ping()).status, "ok");
      // R9.S4: the data file keeps the document across the restart.
      const got = await c.state.get({ collection: "r", key: "k" });
      assert.ok(got.exists);
      assert.equal(got.version, v.version);
    } finally {
      await c.shutdown();
    }
  });

  test("a line that is not JSON-RPC is ignored", async () => {
    const fake = join(tmp, "fake.mjs");
    writeFileSync(
      fake,
      `import { createInterface } from "node:readline";
process.stdout.write("hello, not json\\n");
createInterface({ input: process.stdin }).on("line", (line) => {
  const m = JSON.parse(line);
  process.stdout.write(JSON.stringify({ note: "no id" }) + "\\n");
  const result = m.method === "plugin.handshake"
    ? { plugin: { name: "fake", version: "1", repo: "https://example.com" }, protocolVersion: "1.0", capabilities: ["state"], testHooks: [], durable: false, limits: { maxMessageBytes: 1000, maxChunkBytes: 100, maxPageSize: 10, maxWaitMs: 100 } }
    : { status: "ok", time: new Date().toISOString() };
  process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: m.id, result }) + "\\n");
});`,
    );
    const logs: Array<Record<string, unknown>> = [];
    const c = await PluginClient.startStdio({ command: process.execPath, args: [fake], host, log: (e) => logs.push(e), restart: false });
    try {
      assert.equal((await c.ping()).status, "ok");
      assert.ok(logs.some((l) => l.event === "plugin_stdout_ignored"));
    } finally {
      await c.close();
    }
  });
});

describe("http", () => {
  let child: ChildProcess;
  let url = "";
  before(async () => {
    const port = await freePort();
    child = spawn(bin, ["--listen", `127.0.0.1:${port}`, "--token", "tok"], { stdio: "ignore" });
    url = `http://127.0.0.1:${port}/rpc`;
    await waitHttp(url);
  });
  after(() => {
    child.kill();
  });

  test("the handshake and every call (R11.S1)", async () => {
    const c = await PluginClient.connectHttp({ url, token: "tok", host, onMessage: observe });
    assert.ok(c.info.session);
    await everyCall(c, "http");
    await typedConflict(c, "http");
  });

  test("after a restart of the HTTP plugin, the client does the handshake again", async () => {
    const port = await freePort();
    const start = async () => {
      const p = spawn(bin, ["--listen", `127.0.0.1:${port}`], { stdio: "ignore" });
      await waitHttp(`http://127.0.0.1:${port}/rpc`);
      return p;
    };
    let p = await start();
    const logs: Array<Record<string, unknown>> = [];
    try {
      const c = await PluginClient.connectHttp({ url: `http://127.0.0.1:${port}/rpc`, host, log: (e) => logs.push(e) });
      const first = c.info.session;
      p.kill();
      await new Promise((r) => p.once("exit", r));
      p = await start();
      assert.equal((await c.ping()).status, "ok");
      assert.notEqual(c.info.session, first);
      assert.ok(logs.some((l) => l.event === "plugin_session_lost"));
    } finally {
      p.kill();
    }
  });

  test("a wrong token is refused (R8.S3)", async () => {
    await assert.rejects(PluginClient.connectHttp({ url, token: "wrong", host }), (e) => isPluginError(e, "refused") && /401/.test(e.message));
  });

  test("shutdown stops the HTTP plugin (R9.S2)", async () => {
    const c = await PluginClient.connectHttp({ url, token: "tok", host });
    await c.shutdown(2000);
    await waitFor(() => child.exitCode !== null);
    assert.equal(child.exitCode, 0);
  });
});

test("every message matches its JSON Schema (R13.S2)", () => {
  assert.ok(schemaChecked > 50, `only ${schemaChecked} messages were checked`);
  assert.deepEqual(schemaFailures, []);
});
