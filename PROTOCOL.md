# The Spec Studio backend plugin protocol

Protocol version: **1.0**

This document defines the protocol between a Spec Studio host and a backend plugin. A host is the Spec Studio relay or the Spec Studio app. A plugin keeps the host's data: documents, event logs and files. The JSON Schema of each message is in [`schema/`](schema/).

## Contents

1. [Parts and words](#1-parts-and-words)
2. [Where the rules live](#2-where-the-rules-live)
3. [Transports](#3-transports)
4. [Messages and errors](#4-messages-and-errors)
5. [Versions](#5-versions)
6. [The handshake and the settings](#6-the-handshake-and-the-settings)
7. [Health and shutdown](#7-health-and-shutdown)
8. [The state capability](#8-the-state-capability)
9. [The events capability](#9-the-events-capability)
10. [The blobs capability](#10-the-blobs-capability)
11. [The watch capability](#11-the-watch-capability)
12. [Test hooks](#12-test-hooks)
13. [Rules for plugin authors](#13-rules-for-plugin-authors)
14. [How the relay uses the protocol](#14-how-the-relay-uses-the-protocol)
15. [How the app uses the protocol](#15-how-the-app-uses-the-protocol)

## 1. Parts and words

| Word | Meaning |
|---|---|
| Host | The program that starts or calls the plugin: the relay (`relay`), the app (`app`), or a test tool (`test`). |
| Plugin | The program that keeps the data. It answers the host's calls. |
| Capability | A set of methods. A plugin gives `state` always, and `events`, `blobs` and `watch` when it can. |
| Document | One JSON value in a collection, with a key, a version and attributes. |
| Stream | One ordered log of events, for example the events of one spec. |
| Blob | One file, found by its SHA-256. |
| Version | An opaque string that the plugin gives to each write of a document. |
| Cursor | An opaque string that the plugin gives to each event. |

A **name** is the name of a collection, a key or a stream. A name has 1 to 200 characters from `A-Z a-z 0-9 . _ : @ -`, and starts with a letter or a digit. The plugin refuses any other name with `refused`. A name has no `/` and is never `.` or `..`, so a name is always one safe path segment. A name can contain `..` inside it, as a relay slug such as `fix..login` can.

## 2. Where the rules live

**The rules live in the host. A plugin keeps data and knows no Spec Studio rule.**

The rules are, for example: how a sync applies activity lines, the message sequence numbers, the inbox cursors, the desk rules and the token rotation. In the relay they are in `src/workflows/*Rules.ts`. In the app they are in the Swift core.

The two shapes that we compared:

| | Thin plugins (chosen) | Thick plugins |
|---|---|---|
| Where the rules are | Once, in each host. | Again in each plugin, in Go. |
| Size of a plugin | About 15 generic methods. | About 35 methods that copy `SpecStore`, `DeskStore` and `RelayConfigStore`. |
| A change to a rule | A change in the host only. | A change in every plugin, released with the host. |
| An atomic change | The host reads a document, applies the rule, and writes with the expected version. On `conflict` it reads again and retries. | The plugin applies the rule in one step. |
| The app | Uses the same generic methods. | Needs a second set of methods. |
| History and replay | The `events` capability, the same for both hosts. | A side effect of each method. |

Why thin:

1. The relay already keeps its rules in one place so that two stores cannot drift. Thick plugins would make that drift possible again, in a second language.
2. A user can write a plugin for their own storage in a short time.
3. A Temporal plugin can still do each compare and swap in one update handler, so a write stays atomic. The workflow history then holds the event log.
4. The app needs documents, events and blobs. It does not need the relay's methods.

What the thin shape costs, and how the protocol pays for it:

- **Lists by summary.** The relay lists specs that have messages for the Mac, and so on. Each document has **attributes**, and `state.list` filters on them.
- **Long polls.** The relay waits for a new message for Claude AI. `watch.wait` ends at the next change of a document or a stream.
- **Retries.** A read, a rule and a write cost one more round trip when a conflict occurs. The relay runs one process for each deploy, so a conflict is rare.
- **Timers.** The grace period of a finished spec, the list of closed specs and the token expiry stay in the host.

## 3. Transports

### stdio

The host starts the plugin as a child process. The app always uses stdio.

- Each message is one line of UTF-8 JSON, ended by `\n`, on stdin (host to plugin) and on stdout (plugin to host).
- The plugin writes its log to stderr only. It never writes anything that is not a protocol message to stdout.
- The plugin answers calls in any order. It can run calls at the same time. The host matches each answer to its call by `id`.
- When stdin closes, the plugin shuts down as for `plugin.shutdown` with a timeout of 5 seconds.
- One stdio process is one connection. The handshake is for that connection.

### HTTP

The relay can call a plugin that runs as a sidecar container.

- The plugin listens on one address, for example `--listen 127.0.0.1:7070`.
- The host sends each call as `POST /rpc` with `Content-Type: application/json`. The body is one JSON-RPC request. The answer is one JSON-RPC response with HTTP status 200.
- With a token set (`--token` or the `PLUGIN_TOKEN` environment variable), each request must have `Authorization: Bearer <token>`. A request with no token or a wrong token gets HTTP 401, and the plugin does not run the call.
- A plugin with no token refuses to listen on an address that is not a loopback address.
- The handshake result has a `session` string. The host sends it in the `Plugin-Session` header of each later request. A request with no valid session gets `refused`, as a call before the handshake on stdio, with `data.reason: "handshake_required"`. The sessions live in the plugin's memory: after a restart of the plugin, every old session is gone. A host that gets this reason sends the handshake again and retries the call once. The TypeScript client does this.
- The plugin refuses a request from a browser page: a request with an `Origin` header (HTTP 403), a request whose `Content-Type` is not `application/json` (HTTP 415), and, with no token, a request whose `Host` is not a loopback name (HTTP 403, against DNS rebinding).
- `watch.wait` is one long request. Set the HTTP client timeout to more than `timeoutMs`.

### Limits

The handshake result gives the plugin's limits. The SDK's defaults:

| Limit | Default | Meaning |
|---|---|---|
| `maxMessageBytes` | 2 MiB | The largest message in either direction. A larger request gets -32600 with its own `id`. A larger answer becomes a `refused` error: ask for a smaller page (`pageSize`, `limit`) or a smaller chunk. So that a read can always return what a write stored, the SDK refuses a document (value and attributes) or an event larger than `maxMessageBytes` less 4 KiB. Store large data as a blob. |
| `maxChunkBytes` | 1 MiB | The largest blob chunk, before base64. |
| `maxPageSize` | 1000 | The largest page of `state.list` and `events.read`. |
| `maxWaitMs` | 60000 | The longest `watch.wait`. A longer timeout is cut to this. |

## 4. Messages and errors

The protocol is [JSON-RPC 2.0](https://www.jsonrpc.org/specification). Each request has `jsonrpc: "2.0"`, an `id`, a `method` and `params` (an object). The protocol uses no notifications.

Each error is a JSON-RPC error object. The plugin's own errors have a code from the table below, and `data.code` gives the same error as a word. The message is for the owner: it says what went wrong and what to do. It never contains the value of a secret setting.

| `data.code` | JSON-RPC code | When | The relay's error (`src/store/errors.ts`) |
|---|---|---|---|
| `not_found` | -32001 | The item does not exist (a document to delete, a blob to get, an upload). | `SpecNotFoundError`, `DeskNotFoundError` |
| `conflict` | -32002 | The expected version is not the stored version. `data.version` is the stored version (`null` when no document exists). | (the host reads again and retries) |
| `closing` | -32003 | The item is closing and takes no more changes. A thin plugin does not send it; the host's rules make it. | `SpecClosingError` |
| `dropped` | -32004 | The item no longer exists at its source. A thin plugin does not send it; the host's rules make it. | `SpecDroppedError` |
| `refused` | -32005 | The plugin will not do the call: no handshake (`data.reason: "handshake_required"`), a capability that it does not give, a bad name, a bad hash, a second handshake, an empty event list, an answer over the size limit. | `DeskRefusedError` (made by the host) |
| `unavailable` | -32006 | The storage does not answer, or the plugin shuts down. The host can retry later. | `TimeoutError` |
| `bad_settings` | -32007 | A setting is missing, unknown or wrong. The message names the setting, never its value. | (a start error) |

The standard JSON-RPC errors also apply: -32700 (the line is not JSON; the connection stays open), -32600 (not a valid request), -32601 (no such method), -32602 (the params do not match the schema), -32603 (an error inside the plugin).

`closing` and `dropped` are in the list so that a host maps all store errors in one place, and so a later plugin can use them.

### The schemas of the methods

Each method has a JSON Schema for its params and for its result. `error.json` is the schema of an error object. `scripts/schemas.mjs` writes them.

| Method | Params | Result |
|---|---|---|
| `plugin.describe` | [plugin.describe.request.json](schema/plugin.describe.request.json) | [plugin.describe.result.json](schema/plugin.describe.result.json) |
| `plugin.handshake` | [plugin.handshake.request.json](schema/plugin.handshake.request.json) | [plugin.handshake.result.json](schema/plugin.handshake.result.json) |
| `plugin.ping` | [plugin.ping.request.json](schema/plugin.ping.request.json) | [plugin.ping.result.json](schema/plugin.ping.result.json) |
| `plugin.shutdown` | [plugin.shutdown.request.json](schema/plugin.shutdown.request.json) | [plugin.shutdown.result.json](schema/plugin.shutdown.result.json) |
| `state.get` | [state.get.request.json](schema/state.get.request.json) | [state.get.result.json](schema/state.get.result.json) |
| `state.put` | [state.put.request.json](schema/state.put.request.json) | [state.put.result.json](schema/state.put.result.json) |
| `state.delete` | [state.delete.request.json](schema/state.delete.request.json) | [state.delete.result.json](schema/state.delete.result.json) |
| `state.list` | [state.list.request.json](schema/state.list.request.json) | [state.list.result.json](schema/state.list.result.json) |
| `events.append` | [events.append.request.json](schema/events.append.request.json) | [events.append.result.json](schema/events.append.result.json) |
| `events.read` | [events.read.request.json](schema/events.read.request.json) | [events.read.result.json](schema/events.read.result.json) |
| `events.delete` | [events.delete.request.json](schema/events.delete.request.json) | [events.delete.result.json](schema/events.delete.result.json) |
| `blobs.begin` | [blobs.begin.request.json](schema/blobs.begin.request.json) | [blobs.begin.result.json](schema/blobs.begin.result.json) |
| `blobs.write` | [blobs.write.request.json](schema/blobs.write.request.json) | [blobs.write.result.json](schema/blobs.write.result.json) |
| `blobs.commit` | [blobs.commit.request.json](schema/blobs.commit.request.json) | [blobs.commit.result.json](schema/blobs.commit.result.json) |
| `blobs.get` | [blobs.get.request.json](schema/blobs.get.request.json) | [blobs.get.result.json](schema/blobs.get.result.json) |
| `blobs.has` | [blobs.has.request.json](schema/blobs.has.request.json) | [blobs.has.result.json](schema/blobs.has.result.json) |
| `blobs.delete` | [blobs.delete.request.json](schema/blobs.delete.request.json) | [blobs.delete.result.json](schema/blobs.delete.result.json) |
| `watch.wait` | [watch.wait.request.json](schema/watch.wait.request.json) | [watch.wait.result.json](schema/watch.wait.result.json) |
| `test.continueStream` | [test.continueStream.request.json](schema/test.continueStream.request.json) | [test.continueStream.result.json](schema/test.continueStream.result.json) |
| (any error) | | [error.json](schema/error.json) |

## 5. Versions

The protocol version has the form `major.minor`.

- A new minor version only adds: a new method, a new optional field, a new capability. A host and a plugin that share a major version can always work together.
- A new major version can change or remove anything.
- In the handshake, the host sends all the versions it speaks. The plugin finds the highest major version that both speak. It chooses the lower of the two minor versions in that major version. Example: the host speaks `1.3` and the plugin speaks `1.0`: the plugin chooses `1.0`, and the host uses only the methods of `1.0`.
- When no major version is shared, the handshake fails with `refused`. The message names the host's versions and the plugin's versions.

## 6. The handshake and the settings

### `plugin.describe`

The host can call it before the handshake. It tells the host what the plugin is and which settings it takes. Settings in the app and the relay's log use it.

Result:

```json
{
  "plugin": { "name": "memory", "version": "1.0.0", "repo": "https://github.com/spec-studio-app/plugin-sdk" },
  "protocolVersions": ["1.0"],
  "capabilities": ["state", "events", "blobs", "watch"],
  "settings": [
    { "name": "address", "description": "The Temporal address, host:port.", "required": true, "secret": false },
    { "name": "api_key", "description": "The Temporal Cloud API key.", "required": false, "secret": true }
  ],
  "testHooks": ["continueStream"],
  "durable": false
}
```

`durable` is true when the data stays after the plugin stops.

### `plugin.handshake`

The first call on a connection, after any `plugin.describe` calls. Every other call before it gets `refused` with the message "Send plugin.handshake first.". A second handshake on the same connection gets `refused`.

Params:

```json
{
  "host": { "kind": "relay", "version": "0.6.0" },
  "protocolVersions": ["1.0"],
  "settings": { "address": "temporal:7233", "api_key": "..." }
}
```

Result:

```json
{
  "plugin": { "name": "memory", "version": "1.0.0", "repo": "https://github.com/spec-studio-app/plugin-sdk" },
  "protocolVersion": "1.0",
  "capabilities": ["state", "events", "blobs", "watch"],
  "testHooks": [],
  "durable": false,
  "limits": { "maxMessageBytes": 2097152, "maxChunkBytes": 1048576, "maxPageSize": 1000, "maxWaitMs": 60000 },
  "session": "…"
}
```

`session` is in the result over HTTP only.

The host shows the plugin's name, version and repo link: the app in Settings, the relay in its log at start.

### Settings

- The host passes the settings in the handshake, from its own environment or settings. A value is a string.
- A setting name has 1 to 64 characters from `a-z 0-9 _`, and starts with a letter. The Go SDK refuses to start a plugin that declares another name.
- The plugin checks them: each required setting has a value, and no unknown setting is present. Else the handshake fails with `bad_settings`, and the message names the setting.
- The plugin then opens its storage with the settings. A storage that cannot open gives `bad_settings` (a wrong value) or `unavailable` (the storage does not answer).
- **A plugin never writes the value of a secret setting** to its log, to an error message or to any result. The Go SDK replaces each secret value with `[secret]` in its log and in each error message.
- Over HTTP, the first successful handshake opens the storage. A later handshake with other settings fails with `bad_settings`.

## 7. Health and shutdown

### `plugin.ping`

Result: `{ "status": "ok", "time": "2026-11-27T10:00:00Z" }`. The host can ping at any time after the handshake.

### `plugin.shutdown`

Params: `{ "timeoutMs": 5000 }`. The plugin:

1. Refuses new calls with `unavailable`.
2. Ends each `watch.wait` at once with `changed: false`.
3. Waits for the calls in progress, for at most `timeoutMs`.
4. Closes its storage, answers `{}`, and exits with code 0 (stdio) or stops to listen (HTTP).

### Restarts

The host restarts a stdio plugin that exits when the host did not ask it to. The host:

1. Logs the exit code and the last lines of stderr.
2. Fails each call in progress with `unavailable`.
3. Starts the plugin again after a backoff: 250 ms, then twice as long each time, at most 30 s. A successful handshake resets the backoff.
4. Sends the same handshake again.

The TypeScript client in `ts/` does this.

## 8. The state capability

Every plugin gives `state`. A document has:

- `value`: any JSON value.
- `attributes`: a flat object. Each value is a string, a number, a boolean or `null`. The host writes them with each document. `state.list` filters on them.
- `version`: an opaque string. Each write makes a new version. A version is never used again for the same collection and key, also after a delete.
- `updatedAt`: the time of the last write.

### `state.get`

Params: `{ "collection": "specs", "key": "1a2b3c4d5e6f:drop-spec" }`

Result: `{ "exists": false }`, or `{ "exists": true, "value": …, "attributes": {…}, "version": "…", "updatedAt": "…" }`. A missing document is not an error.

### `state.put`

Params: `{ "collection", "key", "value", "attributes", "expectedVersion" }`

- `expectedVersion: null` means "the document must not exist". Use it to make a document.
- `expectedVersion: "<v>"` means "the stored version must be `<v>`".
- When the condition is false, the plugin answers `conflict` with `data.version` set to the stored version (or `null`), and changes nothing.
- The check and the write are one atomic step. When two writes give the same expected version, exactly one succeeds.
- A missing `attributes` means `{}`.

Result: `{ "version": "…", "updatedAt": "…" }`

### `state.delete`

Params: `{ "collection", "key", "expectedVersion" }`. A missing document gives `not_found`. A wrong version gives `conflict`. Result: `{ "deleted": true }`.

### `state.list`

Params: `{ "collection", "filter", "pageSize", "cursor", "withValues" }`

- `filter` is an object of attributes. A document matches when each attribute in the filter has the same value in the document. A missing attribute has the value `null`.
- The items come in the order of their keys (byte order).
- `pageSize` is 100 when it is missing, and at most `maxPageSize`.
- `cursor` is `null` or missing for the first page. The result's `cursor` is the cursor of the next page, or `null` on the last page.
- With `withValues: true`, each item has its `value` too.

Result: `{ "items": [{ "key", "attributes", "version", "updatedAt", "value"? }], "cursor": "…" | null }`

A document that a host writes while it reads pages can show once or not at all. A document that does not change shows exactly once.

## 9. The events capability

A stream is an ordered log of events. The relay writes one stream for each spec and each desk; the app writes the activity lines of each spec.

### `events.append`

Params: `{ "stream": "spec:1a2b3c4d5e6f:drop-spec", "events": [{ "type": "sync", "data": {…}, "at": "…"? }] }`

- The plugin stores the events of one call in their order, and after every event of an earlier call.
- `at` is the time of the event. When it is missing, the plugin uses the time it stores the event. A host that sends events late (the app after it was offline) sets `at`.
- An empty `events` list gets `refused`.

Result: `{ "cursors": ["…", "…"] }`, one cursor for each event, in order.

### `events.read`

Params: `{ "stream", "after", "from", "to", "limit" }`

- With no `after`, the read starts at the first event. With `after`, it starts after that event.
- `from` and `to` limit the events to `from <= at < to`. Either one can be missing.
- `limit` is 100 when it is missing, and at most `maxPageSize`.
- A stream with no events gives an empty list. An `after` cursor that the plugin did not make gets `refused`.

Result: `{ "events": [{ "cursor", "type", "at", "data" }], "cursor": "…" | null, "more": true | false }`. `cursor` is the cursor of the last event in the list (or the `after` of the request when the list is empty). `more` is true when more events exist after it.

### `events.delete`

Params: `{ "stream" }`. Removes every event of the stream. Result: `{ "deleted": true }`, or `false` when the stream had no events.

### History across continued runs

Some storage splits a long stream into parts. For example, a Temporal plugin can continue a spec's workflow as new, and Temporal deletes a closed run after the namespace's retention period (30 days by default). **The host never sees these parts:**

- A cursor is opaque. A cursor of an old part stays valid.
- One `events.read` can return events from more than one part, in order.
- See rule 1 in [Rules for plugin authors](#13-rules-for-plugin-authors).

## 10. The blobs capability

A blob is a file. Its name is its SHA-256 in lowercase hex. A blob moves in chunks, so no message is larger than `maxMessageBytes`.

### Put a blob

1. `blobs.begin { "sha256", "size" }` → `{ "exists": true }` when the plugin has the blob already (stop here), or `{ "exists": false, "uploadId": "…" }`.
2. `blobs.write { "uploadId", "offset", "data" }` for each chunk, in order. `data` is base64, at most `maxChunkBytes` before base64. `offset` must be the number of bytes received so far. Result: `{ "received": <n> }`.
3. `blobs.commit { "uploadId" }`. The plugin checks the size and the SHA-256. When they are wrong, it answers `refused` and keeps nothing. Else it stores the blob and answers `{ "sha256", "size" }`.

An upload that gets no call for 10 minutes ends, and the plugin removes its bytes. A later call with its `uploadId` gets `not_found`.

### `blobs.get`

Params: `{ "sha256", "offset", "length" }`. `offset` is 0 when it is missing. `length` is `maxChunkBytes` when it is missing, and at most `maxChunkBytes`. A missing blob gets `not_found`. Result: `{ "data", "offset", "size" }`, where `size` is the size of the whole blob.

### `blobs.has`

Params: `{ "sha256s": [ … ] }`, at most 1000. Result: `{ "present": [ … ] }`, the hashes that the plugin has.

### `blobs.delete`

Params: `{ "sha256" }`. Result: `{ "deleted": true }`, or `false` when the plugin did not have it.

## 11. The watch capability

### `watch.wait`

The host waits for a change to one document or one stream.

- Document: `{ "collection", "key", "version", "timeoutMs" }`. `version` is the version that the host has, or `null` when it has no document. The wait ends when the stored version is not `version`.
- Stream: `{ "stream", "after", "timeoutMs" }`. `after` is the last cursor that the host has, or `null`. The wait ends when the stream has an event after `after`.

When the condition is already true, the wait ends at once. Else it ends at the first change, or after `timeoutMs` (at most `maxWaitMs`), or at a shutdown.

Result: `{ "changed": true, "version": "…" | null }` for a document, `{ "changed": true, "cursor": "…" }` for a stream, or `{ "changed": false }`.

## 12. Test hooks

A plugin can declare test hooks in `testHooks`. The conformance runner uses them. A host never calls them.

### `test.continueStream`

Params: `{ "stream" }`. The plugin starts a new part of the stream, as it does when the storage continues a run (for Temporal: continue as new). Result: `{ "run": "<id of the new part>" }`. The runner then checks the reads across the parts.

## 13. Rules for plugin authors

1. **Keep every event.** A plugin that gives `events` keeps every event of a stream until the host calls `events.delete` for that stream. It does not lose events when the storage ends a part of a stream. For Temporal: export the history of each closed run to blobs (or other durable storage) before the namespace's retention removes it, and read from that export when a read reaches an old run.
2. **Never log a secret.** Not in the log, not in an error message, not in a result.
3. **Make each compare and swap atomic.** Two writes with the same expected version: exactly one succeeds. This applies across processes too: a host can start a second plugin process on the same storage (for example `relay pair-code` while the relay runs). A plugin whose storage cannot do an atomic compare and swap across processes must refuse to open storage that another process holds, with `unavailable`. The memory example locks its data file for this reason.
4. **Never reuse a version or a cursor** for the same document or stream.
5. **Write only protocol messages to stdout.** Log to stderr.
6. **Check names.** Refuse a name that is not valid (see [Parts and words](#1-parts-and-words)). Never make a path from a name without this check.
7. **Know no Spec Studio rule.** Store what the host gives. Do not change values or attributes.
8. **Pass the conformance runner** in CI: `plugin-conformance -- <your plugin command>`.

## 14. How the relay uses the protocol

The relay keeps its rules in `src/workflows/*Rules.ts`. With a plugin, the relay's store becomes one adapter over the protocol. The adapter uses these names:

| Data | Where |
|---|---|
| A live spec's state (`SpecWorkflowState` and the last activity time) | Document `specs` / `<repoKey>:<slug>`. Attributes: `repoKey`, `open` (stage is not `merged`), `pendingForMac`, `pendingForCoordinator`, `deployId`. |
| A closed spec (its memo and the time it closed) | Document `closed-specs` / `<repoKey>:<slug>`. Attributes: `repoKey`, `closedAt`. The host deletes it after 30 days. A closed spec has no `specs` document, so `SpecStore.state` gives null and `send`, `ack`, `refresh` and `gone` see no spec, as in the JSON store today. |
| A spec's sync batches and messages | Stream `spec:<repoKey>:<slug>`. |
| A desk's state | Document `desks` / `<repoKey>`. Attributes: `repoKey`, `pendingForMac`, `pendingForCoordinator`, `deployId`. |
| A desk's events | Stream `desk:<repoKey>`. |
| The relay config (pairing, repos, OAuth clients, tokens) | Document `relay` / `config`. |

A "read, apply, write" below means: `state.get`; apply the rule from `*Rules.ts`; `state.put` with the read version; on `conflict`, do it again (at most 5 times, then `unavailable`).

### SpecStore

| Method | Protocol calls |
|---|---|
| `SpecStore.sync` | Read, apply (`applySync`), write on `specs`; the first sync makes the document with `expectedVersion: null`. Then `events.append` on `spec:…` with the applied lines. When no `specs` document exists, `state.get` on `closed-specs` first: a late line for a closed spec is not applied unless it starts a new spec (then the host deletes the `closed-specs` document). |
| `SpecStore.state` | `state.get` on `specs`. |
| `SpecStore.refresh` | Read, apply (`applyState`), write. |
| `SpecStore.gone` | Read, apply (`applyGone`), write. |
| `SpecStore.send` | Read, apply (`checkSend`, `applySend`), write; then `events.append`. |
| `SpecStore.ack` | Read, apply (`applyAck`), write. |
| `SpecStore.read` | `state.get`; when no message for Claude AI is after the cursor and the spec is not closing, `watch.wait` on the document with its version. Each write to the spec (a sync, a refresh, an ack) ends the wait, so the host reads the document again and, when there is still no new message, waits again with the new version, until `waitMs` ends in total. A deleted document (the spec closed) ends the read. Then read, apply (`collectRead`), write. |
| `SpecStore.list` | `state.list` on `specs` with `withValues: true` and a filter from `ListFilter`: `repoKey`, `open: true` (for `openOnly`), `pendingForMac: true`, `pendingForCoordinator: true`. With `all`, also `state.list` on `closed-specs` with the `repoKey` filter. |
| The grace period and the removal | A host timer: `state.put` on `closed-specs` with the memo, then `state.delete` on `specs`, then `events.delete` when the host does not keep the history. The host deletes the `closed-specs` document after 30 days. |

### DeskStore

| Method | Protocol calls |
|---|---|
| `DeskStore.report` | Read, apply (`applyReport`), write on `desks`; makes the document when it is missing. |
| `DeskStore.get` | `state.get` on `desks`. |
| `DeskStore.send` | Read, apply (`deskRefusal`, `applyDeskSend`), write; then `events.append` on `desk:…`. |
| `DeskStore.ack` | Read, apply (`applyDeskAck`), write. |
| `DeskStore.sync` | Read, apply (`applyDeskSync`), write; makes the document when it is missing. |
| `DeskStore.read` | As `SpecStore.read` (wait again after each write with no new message), with `collectDeskRead`. |
| `DeskStore.list` | `state.list` on `desks` with `withValues: true` and a filter from `DeskListFilter`. |

### RelayConfigStore

All methods use the one document `relay` / `config`. A method that only reads does `state.get` and applies the read rule from `configRules.ts`. A method that changes the config does a read, apply, write. `relay pair-code` (another process) does the same read, apply, write, so the compare and swap keeps the two processes apart.

| Method | Protocol calls |
|---|---|
| `RelayConfigStore.verifyAgent` | `state.get` (`verifyAgent`). |
| `RelayConfigStore.paired` | `state.get`. |
| `RelayConfigStore.pair` | Read, apply (`pair`), write. |
| `RelayConfigStore.pairCodeValid` | `state.get` (`pairCodeValid`). |
| `RelayConfigStore.setPairCode` | Read, apply (`setPairCode`), write. |
| `RelayConfigStore.unpair` | Read, apply (`unpair`), write. |
| `RelayConfigStore.setRepos` | Read, apply (`setRepos`), write. |
| `RelayConfigStore.repos` | `state.get`. |
| `RelayConfigStore.getClient` | `state.get` (`client`). |
| `RelayConfigStore.registerClient` | Read, apply (`registerClient`), write. |
| `RelayConfigStore.markSignedIn` | Read, apply (`markSignedIn`), write. |
| `RelayConfigStore.issueTokens` | Read, apply (`issueTokens`), write. |
| `RelayConfigStore.rotateRefresh` | Read, apply (`rotateRefresh`), write. |
| `RelayConfigStore.token` | `state.get` (`token`, with `sweep` for the expiry). |
| `RelayConfigStore.revoke` | Read, apply (`revoke`), write. |

### RelayStore

| Method | Protocol calls |
|---|---|
| `RelayStore.start` | Start or connect to the plugin, `plugin.handshake`, log the plugin's name, version and repo. The first config: `state.put` on `relay` / `config` with `expectedVersion: null`; a `conflict` means that a config exists. |
| `RelayStore.close` | A stdio plugin (one for each relay process): `plugin.shutdown`. An HTTP sidecar is shared by every relay process, `relay pair-code` too: the host only stops to call it and never sends `plugin.shutdown`. The sidecar's own container stop (SIGTERM) shuts it down. |

## 15. How the app uses the protocol

The app keeps its local files as the working state. The `studio` CLI reads them, also offline. The app writes through to the plugin, and keeps a queue while the Mac is offline.

| Data | Where |
|---|---|
| The spec's `state.json` | Document `app-specs` / `<repoKey>:<slug>`. Attributes: `repoKey`, `stage`, `status`. |
| Each line of `activity.jsonl` | Stream `spec:<repoKey>:<slug>`, with `at` set to the line's time. |
| A snapshot of a spec document, a recording of a terminal, an attachment | A blob. The event that names it holds its SHA-256. |
| The scrubber | `events.read` with `from` and `to`, and `blobs.get` for each snapshot. |
