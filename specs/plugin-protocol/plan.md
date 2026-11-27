# Plan: The backend plugin protocol

## Approach

### The decision: thin plugins

The rules stay in the host. The protocol has a small set of generic calls: documents with a version and compare and swap, an event log per stream, blobs by SHA-256, and a wait for a change. The plan compared the two shapes against the relay's `src/store/types.ts` and the app's later specs:

| | Thin plugins (chosen) | Thick plugins |
|---|---|---|
| Where the rules are | Once, in the host. The relay keeps `src/workflows/*Rules.ts`; the app keeps its Swift rules. | Again in each Go plugin, and again for each new store method. |
| Size of a plugin | About 10 calls with no domain knowledge. A user can write one in a day. | About 35 calls that mirror SpecStore, DeskStore and RelayConfigStore. |
| A change to a rule | A change in the host only. No plugin release. | A change in every plugin, in step with the host. Drift between JSON and Temporal is the risk that `specRules.ts` exists to stop today. |
| Atomic changes | The host reads a document, applies the rule, and writes it with the expected version. On `conflict`, it reads again and retries. Temporal does the compare and swap in one update handler, so it stays atomic. | The plugin applies the rule in one step. |
| The app | The app has its own rules in Swift and writes through. It needs generic documents, events and blobs, not relay methods. | The relay methods do not fit the app; the app needs a second set of calls. |
| History and replay | The event log is a first-class capability, the same for both hosts. | The history is a side effect of each method. |

The costs of the thin shape, and what the protocol does about them:

- The relay's lists filter on the memo (`pendingForMac`, `pendingForCoordinator`, `repoKey`, open or closed). A document has **attributes** (a flat map of JSON scalars) and `state.list` filters on attribute equality. The host writes the attributes with each document, as the Temporal store sets search attributes today.
- The long poll of `read_messages` needs a wait. `watch.wait` ends at the next change of a document or a stream.
- A rule that needs a retry loop (read, apply, write) costs one more round trip on a conflict. The relay has one process for each deploy, so conflicts are rare.
- Timers (the grace period of a finished spec, the closed list, the token expiry) stay in the host, as the JSON store does today.

The errors `closing` and `dropped` stay in the code list: the host's rules make them, and a host maps every store error in one place.

### The protocol

JSON-RPC 2.0. Methods:

- `plugin.describe` (allowed before the handshake): name, version, repo URL, protocol versions, declared settings, capabilities.
- `plugin.handshake`: host kind, host version, protocol versions, settings → plugin info, the chosen version, the capabilities, the test hooks.
- `plugin.ping`, `plugin.shutdown {timeoutMs}`.
- `state.get`, `state.put {expectedVersion}` (null means "must not exist"), `state.delete {expectedVersion}`, `state.list {filter, pageSize, cursor}`.
- `events.append`, `events.read {after | from, to, limit}`, `events.delete`.
- `blobs.begin {sha256, size}` → upload id, `blobs.write {uploadId, offset, data}` (base64, at most 1 MB raw), `blobs.commit` (the plugin checks the hash), `blobs.get {sha256, offset, length}`, `blobs.has {sha256s}`, `blobs.delete`.
- `watch.wait {collection, key, version} | {stream, after}, timeoutMs`.
- `test.continueStream {stream}`: a test hook a plugin can declare.

Errors are JSON-RPC errors with codes -32001 to -32007 and `data.code` one of `not_found`, `conflict`, `closing`, `dropped`, `refused`, `unavailable`, `bad_settings`. Versions and cursors are opaque strings. Names of collections, keys and streams match `^[A-Za-z0-9][A-Za-z0-9._:@-]{0,199}$` with no `..`.

stdio: one JSON message per line; logs to stderr. HTTP: `POST /rpc`, one message per request, optional bearer token; the SDK refuses to listen on an address other than loopback with no token.

### The repo layout

- `PROTOCOL.md`, `schema/` (one JSON Schema file for each method: `<method>.request.json`, `<method>.result.json`, and `error.json`).
- `go/` (module `github.com/spec-studio-app/plugin-sdk/go`): `protocol` (the types and error codes), `plugin` (the SDK: transports, handshake, settings, routing, shutdown), `client` (a Go host client that the runner uses).
- `examples/memory/` (its own module): the memory plugin, with `--data <file>` for a restart and the `test.continueStream` hook.
- `conformance/` (its own module): `conformance` package (the tests) and `cmd/plugin-conformance`.
- `go.work` at the root joins the three Go modules. The two others require the SDK at a placeholder version that the workspace replaces.
- `ts/` (`@spec-studio-app/plugin-sdk`): the host client, no runtime dependencies; `ajv` as a dev dependency for the schema checks in the tests.
- `.tool-versions` (Go 1.24.11, Node 22.23.4). The `go.mod` files say `go 1.21`, as the spec assumes.

The Go modules use only the standard library, except the runner and the schema tests, which use `github.com/santhosh-tekuri/jsonschema/v5`.

## Repos

| Repo | Why it changes | Merge order |
|---|---|---|
| plugin-sdk | The protocol, the SDK, the client, the runner, CI | 1 |

## Steps

1. plugin-sdk: `PROTOCOL.md` and `schema/`: every method, request, result and error code, the versions, the transports, the settings, the rule on event history across continued runs, the decision and the mapping from the relay's stores. Covers R1, R2, R3, R4, R5, R6, R7, R8, R9, R13.
2. plugin-sdk: `go/protocol` and `go/plugin`: the types, the stdio and HTTP transports, the handshake and version choice, the settings check with secret redaction, the error codes, the capability routing, name checks, ping and shutdown with in-flight calls. Unit tests. Covers R1, R2, R7, R8, R9, R10.
3. plugin-sdk: `go/client` and `examples/memory`: the Go host client, and the memory plugin with state, events (with runs), blobs, watch, the data file and the test hook. Covers R3, R4, R5, R6, R9, R10.
4. plugin-sdk: `conformance/`: the runner with the test list, the skip rules, the secret canary, the restart test, the schema check, and its own tests against the memory plugin, a state-only plugin and a broken plugin. Covers R10, R12, R13.
5. plugin-sdk: `ts/`: the host client with stdio and HTTP, typed calls, `PluginError` with a code, restart with backoff, and tests against the memory plugin with ajv schema checks. Covers R9, R11, R13.
6. plugin-sdk: `.github/workflows/ci.yml` and `release.yml`. Covers R14.
7. plugin-sdk: README: what a backend plugin is, the official plugins, a guide to write and test a plugin. Covers R10, R13.

## Tests

- R1.S1: `go/plugin` handshake test; conformance `handshake` test on the memory plugin.
- R1.S2: `go/plugin` test with host versions ["2.0"]; conformance `handshake-no-major`.
- R1.S3: `go/plugin` test; conformance `call-before-handshake`.
- R1.S4: `go/plugin` test with host versions ["1.3"]: chosen 1.0.
- R2.S1: `go/plugin` describe test; conformance `describe`.
- R2.S2: `go/plugin` test with a required setting missing.
- R2.S3: `go/plugin` test that captures the log and the error with a canary value; conformance `secret-canary` scans stderr and errors for each declared secret.
- R3.S1 to R3.S7: conformance `state-*` tests; `examples/memory` unit tests; R3.S4 runs 20 parallel writes and counts one success.
- R4.S1 to R4.S3, R4.S5: conformance `events-*` tests.
- R4.S4: conformance `events-continued-run`, run when the plugin declares `test.continueStream`.
- R5.S1 to R5.S4: conformance `blobs-*` tests; R5.S4 checks the chunk limit in the client.
- R6.S1 to R6.S3: conformance `watch-*` tests with timing bounds.
- R7.S1, R7.S2: conformance `unknown-method`, `capability-refused`; R7.S3: `go/plugin` test with a store that returns `unavailable`.
- R8.S1: the stdio runner run and the stderr check; R8.S2: the runner with `--url`; R8.S3: `go/plugin` HTTP token test and a TS client test.
- R9.S1, R9.S2: conformance `ping`, `shutdown`; `go/plugin` test with a slow write.
- R9.S3: TS test with a plugin that exits: the client logs the code and restarts.
- R9.S4: conformance `restart` (stdio with `--data`), and a memory unit test.
- R10.S1: `conformance` Go test that runs the runner on the memory plugin over stdio and HTTP.
- R10.S2, R12.S3: `conformance` Go test with a state-only fixture plugin.
- R11.S1, R11.S2: `ts/test/client.test.ts` over stdio and HTTP.
- R12.S1, R12.S2, R12.S4: `conformance` Go tests: the memory plugin passes; a broken fixture fails the CAS test with exit code 1; the continued-run test is skipped without the hook.
- R13.S1: `go/protocol` test reads `PROTOCOL.md` and checks that each method name of the three relay interfaces appears in the mapping table.
- R13.S2: the runner's `--schemas schema` check in CI; ajv checks in the TS tests; a `go/protocol` schema test.
- R14.S1: the CI workflow runs on the PR (seen on GitHub).
- R14.S2: the release workflow, checked with `actionlint` if present and by a dry review; it runs only on a tag.

## Risks

- `go install` of the runner from outside the repo needs a real version of the SDK module. The release ships binaries, and the release workflow tags `go/vX.Y.Z` before it builds. The README says to use the binary.
- Timing tests for watch can be flaky in CI. The bounds are wide (a change within 1 s, a timeout within 2 s ± 1 s).
- The npm publish needs `NPM_TOKEN` in the repo secrets. The release workflow skips the publish with a warning when the secret is missing.
- The memory plugin is not a production store. The README says so.
