# Spec Studio plugin SDK

This repo defines how Spec Studio keeps its data in a **backend plugin**. It has:

- [`PROTOCOL.md`](PROTOCOL.md): the protocol between a host and a plugin. The JSON Schema of each message is in [`schema/`](schema/).
- [`go/`](go/): the Go SDK to write a plugin.
- [`ts/`](ts/): the TypeScript host client (`@spec-studio-app/plugin-sdk`).
- [`conformance/`](conformance/): `plugin-conformance`, the protocol tests that every plugin runs.
- [`examples/memory/`](examples/memory/): a small plugin that keeps everything in memory.

## What a backend plugin is

Spec Studio has two hosts that keep data: the **relay** (a remote MCP server that you host) and the **app** (on your Mac). A backend plugin is a separate program that keeps this data for a host:

- **state**: one JSON document for each spec, each desk and the relay config, with a version for each write.
- **events**: the ordered log of each spec. History and replay use it.
- **blobs**: files by SHA-256, for example terminal recordings and document snapshots.
- **watch**: a wait for the next change, for long polls.

The host keeps all Spec Studio rules. A plugin only keeps data, so a plugin is small. [`PROTOCOL.md`](PROTOCOL.md#2-where-the-rules-live) gives the reasons.

The host starts the plugin as a child process and talks JSON-RPC 2.0 on its stdin and stdout. The relay can also call a plugin that runs as a sidecar, over HTTP.

## The official plugins

| Plugin | Storage | Hosts |
|---|---|---|
| [plugin-json](https://github.com/spec-studio-app/plugin-json) | JSON files. The default. | The relay and the app |
| [plugin-temporal](https://github.com/spec-studio-app/plugin-temporal) | Temporal: one workflow for each spec, with history and replay. | The relay and the app |

Both are written in Go with this SDK. Each is one static binary for each platform.

## Write a plugin

You can write a plugin in any language that reads and writes JSON lines. In Go, the SDK does the transports, the handshake, the settings, the error codes and the routing. You write only the storage calls.

1. Make a Go module and get the SDK:

   ```sh
   go get github.com/spec-studio-app/plugin-sdk/go
   ```

2. Write a store. It must implement `plugin.StateStore`, `Open` and `Close`. Implement `plugin.EventStore`, `plugin.BlobStore` or `plugin.Watcher` to give more capabilities:

   ```go
   package main

   import (
   	"context"

   	"github.com/spec-studio-app/plugin-sdk/go/plugin"
   	"github.com/spec-studio-app/plugin-sdk/go/protocol"
   )

   type store struct{ /* your client */ }

   func (s *store) Open(ctx context.Context, o plugin.OpenOptions) error {
   	// o.Settings["address"] has a value: the SDK checked it.
   	// o.Host.Kind is "app", "relay" or "test": use it for a default.
   	return nil
   }
   func (s *store) Close(ctx context.Context) error { return nil }

   func (s *store) Get(ctx context.Context, p protocol.StateGetParams) (protocol.StateGetResult, error) { /* … */ }
   func (s *store) Put(ctx context.Context, p protocol.StatePutParams) (protocol.StatePutResult, error) {
   	// Check p.ExpectedVersion and write in one atomic step.
   	// On a mismatch: return protocol.StatePutResult{}, protocol.ConflictError(&stored)
   }
   func (s *store) Delete(ctx context.Context, p protocol.StateDeleteParams) (protocol.StateDeleteResult, error) { /* … */ }
   func (s *store) List(ctx context.Context, p protocol.StateListParams) (protocol.StateListResult, error) { /* … */ }

   func main() {
   	plugin.Main(plugin.Plugin{
   		Info: protocol.PluginInfo{Name: "mine", Version: "0.1.0", Repo: "https://github.com/me/plugin-mine"},
   		Settings: []protocol.Setting{
   			{Name: "address", Description: "The storage address, host:port.", Required: true},
   			{Name: "api_key", Description: "The API key.", Secret: true},
   		},
   		Store: &store{},
   	})
   }
   ```

   [`examples/memory/store.go`](examples/memory/store.go) is a full store with every capability.

3. Build and run it. With no flag, it serves stdio. With `--listen 127.0.0.1:7070`, it serves HTTP at `/rpc`. On an address that is not a loopback address, also set `--token` (or `PLUGIN_TOKEN`).

Obey the [rules for plugin authors](PROTOCOL.md#13-rules-for-plugin-authors). The most important rules:

- Make each compare and swap atomic.
- Keep every event of a stream until the host deletes the stream, also when your storage splits the stream into parts.
- Never log the value of a secret setting. The SDK redacts the secrets in its log and in error messages; do not write them to stderr yourself.

## Test a plugin

Get `plugin-conformance` from the [releases](https://github.com/spec-studio-app/plugin-sdk/releases) (macOS and Linux, arm64 and amd64). Each archive has the `schema/` folder too.

```sh
# stdio
plugin-conformance --schemas schema -- ./my-plugin --some-flag

# HTTP
plugin-conformance --schemas schema --url http://127.0.0.1:7070/rpc --token "$PLUGIN_TOKEN"

# settings for the handshake
plugin-conformance --setting address=localhost:7233 --setting api_key="$KEY" -- ./my-plugin
```

It prints each test as `pass`, `fail` (with the reason) or `skip`, and exits with code 1 when a test fails. It tests only the capabilities that your plugin declares. `--run <pattern>` runs some tests only; `-v` shows the plugin's stderr.

Run it in your plugin's CI. A plugin that declares the `continueStream` test hook also gets the test of a read across a continued run. A durable plugin also gets the test that the data stays after a restart.

## Use a plugin from a host (TypeScript)

```ts
import { PluginClient, isPluginError } from "@spec-studio-app/plugin-sdk";

const plugin = await PluginClient.startStdio({
  command: "plugin-json",
  args: ["--data", "/data"],
  host: { kind: "relay", version: "0.6.0" },
  settings: {},
  log: (entry) => console.log(JSON.stringify(entry)),
});
console.log(plugin.info.plugin); // name, version, repo

const doc = await plugin.state.get({ collection: "specs", key: "1a2b3c4d5e6f:drop-spec" });
try {
  await plugin.state.put({ collection: "specs", key: "1a2b3c4d5e6f:drop-spec", value: {}, expectedVersion: doc.exists ? doc.version : null });
} catch (err) {
  if (isPluginError(err, "conflict")) {
    // read again and retry
  }
}
await plugin.shutdown();
```

When a stdio plugin exits, the client logs the exit code and the last lines of stderr, fails the calls in progress with `unavailable`, and starts the plugin again with backoff. `PluginClient.connectHttp({ url, token, host })` connects to an HTTP plugin.

## Develop this repo

You need Go 1.24 and Node 22 (see `.tool-versions`). `go.work` joins the three Go modules.

```sh
(cd go && go test -race ./...)
(cd examples/memory && go test ./...)
(cd conformance && go test ./...)        # builds the memory plugin and runs the runner on it
(cd ts && npm ci && npm test)            # builds the memory plugin with go build
node scripts/schemas.mjs                 # after a change to the messages: writes schema/
```

A tag `vX.Y.Z` makes a release: the Go module tag `go/vX.Y.Z`, the npm package (with the `NPM_TOKEN` secret), and the `plugin-conformance` binaries.
