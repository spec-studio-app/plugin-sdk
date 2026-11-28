// Writes the JSON Schema files in schema/ from the definitions below.
// Run it after a change to the protocol: node scripts/schemas.mjs
// Each method has <method>.request.json (the params) and <method>.result.json.
// The files inline the shared definitions, so a validator needs no $ref to another file.
import { mkdirSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const out = join(root, "schema");
const BASE = "https://spec-studio-app.github.io/plugin-sdk/schema/";

const name = { type: "string", pattern: "^[A-Za-z0-9][A-Za-z0-9._:@-]{0,199}$" };
const version = { type: "string", minLength: 1 };
const protocolVersion = { type: "string", pattern: "^[0-9]+\\.[0-9]+$" };
const time = { type: "string", format: "date-time" };
const sha256 = { type: "string", pattern: "^[0-9a-f]{64}$" };
const base64 = { type: "string", contentEncoding: "base64" };
const scalar = { type: ["string", "number", "boolean", "null"] };
const attributes = { type: "object", additionalProperties: scalar };
const capability = { enum: ["state", "events", "blobs", "watch"] };
const capabilities = { type: "array", items: capability, uniqueItems: true, contains: { const: "state" } };
const testHooks = { type: "array", items: { enum: ["continueStream"] }, uniqueItems: true };
const setting = {
  type: "object",
  required: ["name", "description", "required", "secret"],
  additionalProperties: false,
  properties: {
    name: { type: "string", pattern: "^[a-z][a-z0-9_]{0,63}$" },
    description: { type: "string" },
    required: { type: "boolean" },
    secret: { type: "boolean" },
  },
};
const pluginInfo = {
  type: "object",
  required: ["name", "version", "repo"],
  properties: { name: { type: "string", minLength: 1 }, version: { type: "string", minLength: 1 }, repo: { type: "string", format: "uri" } },
};
const limits = {
  type: "object",
  required: ["maxMessageBytes", "maxChunkBytes", "maxPageSize", "maxWaitMs"],
  properties: {
    maxMessageBytes: { type: "integer", minimum: 1 },
    maxChunkBytes: { type: "integer", minimum: 1 },
    maxPageSize: { type: "integer", minimum: 1 },
    maxWaitMs: { type: "integer", minimum: 1 },
  },
};
const empty = { type: "object" };
const obj = (required, properties) => ({ type: "object", required, properties });
const newEvent = obj(["type", "data"], { type: { type: "string", minLength: 1, maxLength: 200 }, data: {}, at: time });
const event = obj(["cursor", "type", "at", "data"], { cursor: version, type: { type: "string", minLength: 1, maxLength: 200 }, at: time, data: {} });
const document = obj(["key", "attributes", "version", "updatedAt"], {
  key: name,
  attributes,
  version,
  updatedAt: time,
  value: {},
});

const methods = {
  "plugin.describe": {
    request: { type: ["object", "null"] },
    result: obj(["plugin", "protocolVersions", "capabilities", "settings", "testHooks", "durable"], {
      plugin: pluginInfo,
      protocolVersions: { type: "array", items: protocolVersion, minItems: 1 },
      capabilities,
      settings: { type: "array", items: setting },
      testHooks,
      durable: { type: "boolean" },
    }),
  },
  "plugin.handshake": {
    request: obj(["host", "protocolVersions", "settings"], {
      host: obj(["kind", "version"], { kind: { enum: ["app", "relay", "cli", "test"] }, version: { type: "string", minLength: 1 } }),
      protocolVersions: { type: "array", items: protocolVersion, minItems: 1 },
      settings: { type: "object", additionalProperties: { type: "string" } },
    }),
    result: obj(["plugin", "protocolVersion", "capabilities", "testHooks", "durable", "limits"], {
      plugin: pluginInfo,
      protocolVersion,
      capabilities,
      testHooks,
      durable: { type: "boolean" },
      limits,
      session: { type: "string" },
      // Protocol 1.1, optional: two plugin processes on the same data give the same storeId.
      storeId: { type: "string", minLength: 1 },
    }),
  },
  "plugin.ping": {
    request: { type: ["object", "null"] },
    result: obj(["status", "time"], { status: { const: "ok" }, time }),
  },
  "plugin.shutdown": {
    request: obj([], { timeoutMs: { type: "integer", minimum: 0, maximum: 600000 } }),
    result: empty,
  },
  "state.get": {
    request: obj(["collection", "key"], { collection: name, key: name }),
    result: {
      type: "object",
      required: ["exists"],
      properties: { exists: { type: "boolean" }, value: {}, attributes, version, updatedAt: time },
      if: { properties: { exists: { const: true } } },
      then: { required: ["value", "attributes", "version", "updatedAt"] },
    },
  },
  "state.put": {
    request: obj(["collection", "key", "value", "expectedVersion"], {
      collection: name,
      key: name,
      value: {},
      attributes,
      expectedVersion: { anyOf: [version, { type: "null" }] },
    }),
    result: obj(["version", "updatedAt"], { version, updatedAt: time }),
  },
  "state.delete": {
    request: obj(["collection", "key", "expectedVersion"], { collection: name, key: name, expectedVersion: version }),
    result: obj(["deleted"], { deleted: { const: true } }),
  },
  "state.list": {
    request: obj(["collection"], {
      collection: name,
      filter: attributes,
      pageSize: { type: "integer", minimum: 1 },
      cursor: { type: ["string", "null"] },
      withValues: { type: "boolean" },
    }),
    result: obj(["items", "cursor"], { items: { type: "array", items: document }, cursor: { type: ["string", "null"] } }),
  },
  // Protocol 1.1: the document and its events in one atomic step. events can be empty.
  "state.commit": {
    request: obj(["collection", "key", "value", "expectedVersion", "stream", "events"], {
      collection: name,
      key: name,
      value: {},
      attributes,
      expectedVersion: { anyOf: [version, { type: "null" }] },
      stream: name,
      events: { type: "array", items: newEvent },
    }),
    result: obj(["version", "updatedAt", "cursors"], { version, updatedAt: time, cursors: { type: "array", items: version } }),
  },
  "events.append": {
    request: obj(["stream", "events"], {
      stream: name,
      events: {
        // An empty list is valid JSON for the schema; the plugin refuses it with `refused`.
        type: "array",
        items: newEvent,
      },
    }),
    result: obj(["cursors"], { cursors: { type: "array", items: version, minItems: 1 } }),
  },
  "events.read": {
    request: obj(["stream"], {
      stream: name,
      after: { type: ["string", "null"] },
      from: time,
      to: time,
      limit: { type: "integer", minimum: 1 },
    }),
    result: obj(["events", "cursor", "more"], {
      events: { type: "array", items: event },
      cursor: { type: ["string", "null"] },
      more: { type: "boolean" },
    }),
  },
  "events.delete": {
    request: obj(["stream"], { stream: name }),
    result: obj(["deleted"], { deleted: { type: "boolean" } }),
  },
  "blobs.begin": {
    request: obj(["sha256", "size"], { sha256, size: { type: "integer", minimum: 0 } }),
    result: obj(["exists"], { exists: { type: "boolean" }, uploadId: { type: "string" } }),
  },
  "blobs.write": {
    request: obj(["uploadId", "offset", "data"], { uploadId: { type: "string", minLength: 1 }, offset: { type: "integer", minimum: 0 }, data: base64 }),
    result: obj(["received"], { received: { type: "integer", minimum: 0 } }),
  },
  "blobs.commit": {
    request: obj(["uploadId"], { uploadId: { type: "string", minLength: 1 } }),
    result: obj(["sha256", "size"], { sha256, size: { type: "integer", minimum: 0 } }),
  },
  "blobs.get": {
    request: obj(["sha256"], { sha256, offset: { type: "integer", minimum: 0 }, length: { type: "integer", minimum: 1 } }),
    result: obj(["data", "offset", "size"], { data: base64, offset: { type: "integer", minimum: 0 }, size: { type: "integer", minimum: 0 } }),
  },
  "blobs.has": {
    request: obj(["sha256s"], { sha256s: { type: "array", items: sha256, maxItems: 1000 } }),
    result: obj(["present"], { present: { type: "array", items: sha256 } }),
  },
  "blobs.delete": {
    request: obj(["sha256"], { sha256 }),
    result: obj(["deleted"], { deleted: { type: "boolean" } }),
  },
  "watch.wait": {
    request: {
      type: "object",
      required: ["timeoutMs"],
      properties: {
        collection: name,
        key: name,
        version: { type: ["string", "null"] },
        stream: name,
        after: { type: ["string", "null"] },
        timeoutMs: { type: "integer", minimum: 0 },
      },
      oneOf: [{ required: ["collection", "key", "version"] }, { required: ["stream", "after"] }],
    },
    result: obj(["changed"], {
      changed: { type: "boolean" },
      version: { type: ["string", "null"] },
      cursor: { type: ["string", "null"] },
    }),
  },
  "test.continueStream": {
    request: obj(["stream"], { stream: name }),
    result: obj(["run"], { run: { type: "string", minLength: 1 } }),
  },
};

const error = {
  type: "object",
  required: ["code", "message"],
  properties: {
    code: { type: "integer" },
    message: { type: "string" },
    data: obj(["code"], {
      code: { enum: ["not_found", "conflict", "closing", "dropped", "refused", "unavailable", "bad_settings"] },
      version: { type: ["string", "null"] },
      reason: { type: "string" },
    }),
  },
};

rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });
const write = (file, schema) =>
  writeFileSync(join(out, file), JSON.stringify({ $schema: "https://json-schema.org/draft/2020-12/schema", $id: BASE + file, ...schema }, null, 2) + "\n");
for (const [method, { request, result }] of Object.entries(methods)) {
  write(`${method}.request.json`, { title: `${method} params`, ...request });
  write(`${method}.result.json`, { title: `${method} result`, ...result });
}
write("error.json", { title: "error", ...error });
console.log(`wrote ${readdirSync(out).length} files to schema/`);
