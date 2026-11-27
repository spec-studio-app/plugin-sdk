# The backend plugin protocol

Spec Studio keeps its state in a store that is built into each host: the relay has a JSON store and a Temporal store, and the app has files. This spec defines one protocol between a host (the relay or the app) and a backend plugin, and ships it in this repo with a Go SDK, a TypeScript host client and a conformance test runner. Later specs make the plugins and move the hosts onto them.

## Scope

### In scope
- `PROTOCOL.md`: JSON-RPC 2.0 over stdio and over HTTP, the handshake, the settings, the capabilities (`state`, `events`, `blobs`, `watch`), the error codes, the versions, health and shutdown.
- A JSON Schema for each request and each result, in `schema/`.
- The decision where the rules live (thin or thick plugins), with the reasons, in the plan and in `PROTOCOL.md`.
- The rule for plugin authors that a plugin with `events` keeps every event of a spec until the host deletes the spec, across continued runs.
- The Go SDK (`go/`, module `github.com/spec-studio-app/plugin-sdk/go`) and the memory example plugin (`examples/memory`).
- The TypeScript host client (`ts/`, an npm package).
- The conformance runner (`conformance/`, the `plugin-conformance` binary).
- CI for this repo, and a release workflow for a tag: the Go module version, the npm package and the conformance binaries for macOS and Linux (arm64, amd64).
- The README: what a backend plugin is, the official plugins with links, and a short guide to write and test a plugin.
- A mapping of each method of the relay's SpecStore, DeskStore and RelayConfigStore to the protocol calls that replace it, in the PR description and in `PROTOCOL.md`.

### Out of scope
- plugin-json and plugin-temporal.
- Any change in the relay repo or the app repo.
- The app's Swift client (later spec 4).
- A plugin registry, or a plugin download in the app.
- A license file.

## Requirements

### R1 The handshake
The first call on a connection is the handshake. The host sends its kind (`app` or `relay`), its version, the protocol versions it speaks and the plugin's settings. The plugin answers with its name, its version, the URL of its repo, the protocol version it chose and its capabilities. The plugin refuses every other call before the handshake.

#### R1.S1 A shared major version
- Given the memory plugin that speaks protocol 1.0
- When a host of kind `relay` that speaks 1.0 sends the handshake
- Then the plugin answers with its name, its version, its repo URL, the version 1.0 and the capabilities `state`, `events`, `blobs` and `watch`

#### R1.S2 No shared major version
- Given the memory plugin that speaks protocol 1.x only
- When a host that speaks only 2.0 sends the handshake
- Then the plugin answers with the error code `refused`
- And the message names the versions of the host and the versions of the plugin

#### R1.S3 A call before the handshake
- Given a new connection to the memory plugin
- When the host calls `state.get` before the handshake
- Then the plugin answers with the error code `refused` and a message that says to send the handshake first

#### R1.S4 A newer minor version
- Given a plugin that speaks 1.0 and a host that speaks 1.3
- When the host sends the handshake
- Then the plugin chooses 1.0
- And the host uses only the calls of 1.0

### R2 The settings
A plugin declares its settings: the name, a description, required or not, and secret or not. The host passes the values in the handshake. The plugin checks them, and never writes the value of a secret setting to a log or to an error message.

#### R2.S1 The host reads the declared settings
- Given a plugin with a required secret setting `api_key`
- When the host calls `plugin.describe` before the handshake
- Then the result lists `api_key` as required and secret, with its description

#### R2.S2 A missing required setting
- Given a plugin with a required setting `address`
- When the host sends the handshake with no value for `address`
- Then the plugin answers with the error code `bad_settings`
- And the message names `address`

#### R2.S3 A secret stays out of the logs
- Given a plugin with a secret setting `api_key` set to a known value
- When the handshake fails for any reason
- Then no line on the plugin's stderr and no error message contains that value

### R3 The state capability
Every plugin gives `state`. It keeps documents by collection and key: a value (JSON), a version, and a set of named attributes for lists. A write gives the version that the host read, and the plugin refuses it with `conflict` when the stored version is not that version. A list filters on attributes. A delete removes a document.

#### R3.S1 Read a document that does not exist
- Given an empty plugin
- When the host reads the document `specs/abc` 
- Then the result says that the document does not exist, with no error

#### R3.S2 Make a document
- Given no document `specs/abc`
- When the host writes `specs/abc` with the expected version "none"
- Then the write succeeds and returns a new version
- And a read returns the same value, attributes and version

#### R3.S3 A write with a stale version
- Given the document `specs/abc` at version V2, and a host that read it at V1
- When the host writes it with the expected version V1
- Then the plugin answers with the error code `conflict`
- And the stored document stays at V2

#### R3.S4 Two writes at the same time
- Given the document `specs/abc` at version V1
- When two hosts each write it with the expected version V1 at the same time
- Then exactly one write succeeds and the other gets `conflict`

#### R3.S5 A list by attribute
- Given three documents in `specs`, two with the attribute `pendingForMac` true
- When the host lists `specs` with the filter `pendingForMac` equal to true
- Then the result has the two documents, with their keys, attributes and versions

#### R3.S6 A long list
- Given 250 documents in `specs`
- When the host lists `specs` with a page size of 100
- Then it gets every document once, in three pages, with a cursor between them

#### R3.S7 Delete a document
- Given the document `specs/abc` at version V1
- When the host deletes it with the expected version V1
- Then a later read says that it does not exist

### R4 The events capability
A plugin that gives `events` keeps an ordered log of events for each stream (a spec or a desk). The host appends events and reads them back in order, after a cursor or in a time range. Cursors are opaque. The plugin keeps every event of a stream until the host deletes the stream.

#### R4.S1 Append and read in order
- Given an empty stream `spec:abc`
- When the host appends the events A, B and C in one call, then D in a second call
- Then a read from the start returns A, B, C and D in that order, each with a cursor and the time the plugin stored it

#### R4.S2 Read after a cursor
- Given the stream with A, B, C and D
- When the host reads after the cursor of B
- Then the result is C and D

#### R4.S3 Read a time range
- Given events stored at 10:00, 10:05 and 10:10
- When the host reads the range from 10:01 to 10:09
- Then the result is the one event of 10:05

#### R4.S4 A read across a continued run
- Given a plugin that continues a stream in a new run after its events A and B (for Temporal: continue as new)
- And the host appends C after the new run starts
- When the host reads from the start
- Then the result is A, B and C, and the cursor of B still works

#### R4.S5 Delete a stream
- Given the stream `spec:abc` with events
- When the host deletes the stream
- Then a read returns no events

### R5 The blobs capability
A plugin that gives `blobs` keeps files by their SHA-256. The host puts a file, gets it back, checks if it exists, and deletes it.

#### R5.S1 Put and get a file
- Given an empty plugin
- When the host puts a 1 MB file with its SHA-256
- Then a get by that hash returns the same bytes

#### R5.S2 A wrong hash
- Given a file and a SHA-256 that is not the hash of that file
- When the host puts the file with that hash
- Then the plugin answers with `refused` and keeps nothing

#### R5.S3 Check files
- Given a plugin that has file A and not file B
- When the host checks the hashes of A and B
- Then the result says that A exists and B does not

#### R5.S4 A large file
- Given a 20 MB file
- When the host puts it and gets it back
- Then the bytes are the same, and no single message on the transport is larger than the protocol's chunk limit

### R6 The watch capability
A plugin that gives `watch` lets the host wait for a change to a document or a stream. The wait ends at a change or at a timeout.

#### R6.S1 A change ends the wait
- Given a host that waits on `specs/abc` at version V1 with a timeout of 20 s
- When another call writes `specs/abc`
- Then the wait ends within 1 s and returns the new version

#### R6.S2 No change
- Given a host that waits on `specs/abc` at version V1 with a timeout of 2 s
- When nothing writes `specs/abc`
- Then the wait ends after about 2 s and says that nothing changed

#### R6.S3 The document is already newer
- Given the document at version V2
- When the host waits with the version V1
- Then the wait ends at once with V2

### R7 The error codes
Every error has one code from a fixed list: `not_found`, `conflict`, `closing`, `dropped`, `refused`, `unavailable`, `bad_settings`. Each error has a message for the owner. `PROTOCOL.md` maps each code to the relay's errors in `src/store/errors.ts`.

#### R7.S1 An unknown method
- Given a plugin after the handshake
- When the host calls a method that does not exist
- Then the plugin answers with the JSON-RPC error "method not found"

#### R7.S2 A capability that the plugin does not give
- Given a plugin that gives only `state`
- When the host calls `blobs.get`
- Then the plugin answers with `refused` and a message that names the capability

#### R7.S3 The store is down
- Given a plugin whose storage does not answer
- When the host calls `state.get`
- Then the plugin answers with `unavailable` and a message that the owner can act on

### R8 The two transports
The same calls work over stdio (one JSON message per line on stdin and stdout, logs on stderr) and over HTTP (a POST of one JSON-RPC message to one path).

#### R8.S1 stdio
- Given the host starts the memory plugin as a child process
- When it sends the handshake on stdin
- Then the answer arrives on stdout, and the plugin's log lines go to stderr only

#### R8.S2 HTTP
- Given the memory plugin runs with `--listen 127.0.0.1:7070`
- When the host POSTs the handshake to `http://127.0.0.1:7070/rpc`
- Then the answer is the same as over stdio

#### R8.S3 HTTP with a token
- Given the plugin runs over HTTP with a bearer token set
- When a request has no token or a wrong token
- Then the plugin answers with HTTP 401 and does not run the call

### R9 Health, shutdown and restart
The host can ping the plugin and ask it to shut down with a timeout. The TypeScript client restarts a stdio plugin that exits, with backoff, and logs why.

#### R9.S1 Ping
- Given a plugin after the handshake
- When the host calls `plugin.ping`
- Then the plugin answers with its status

#### R9.S2 Shutdown
- Given a stdio plugin with a write in progress
- When the host calls `plugin.shutdown` with a timeout of 5 s
- Then the plugin finishes the write, answers, and exits with code 0 within 5 s

#### R9.S3 The plugin exits
- Given a host that uses the TypeScript client with a stdio plugin
- When the plugin process exits with an error
- Then the client logs the exit code and the last stderr lines, starts the plugin again after a backoff, and runs the handshake again
- And a call that was in progress fails with `unavailable`

#### R9.S4 The data survives a restart
- Given the memory plugin started with a data file, and a document written to it
- When the plugin restarts
- Then a read returns the document at the same version

### R10 The Go SDK
A plugin author writes only the storage calls. The SDK does the transports, the handshake, the settings check, the error codes, the capability routing and shutdown.

#### R10.S1 The memory example
- Given the `examples/memory` plugin, written with the SDK
- When it runs against the conformance runner over stdio and over HTTP
- Then every test passes

#### R10.S2 A plugin with only state
- Given a plugin written with the SDK that implements only the state calls
- When the host sends the handshake
- Then the capabilities are only `state`, and calls to other capabilities get `refused`

### R11 The TypeScript host client
The npm package starts a stdio plugin or connects to an HTTP plugin, does the handshake, and gives one typed call for each protocol method.

#### R11.S1 Every call against the memory example
- Given the memory example built from this repo
- When the client tests run
- Then they do the handshake and every call of every capability over stdio and over HTTP, and pass

#### R11.S2 A typed error
- Given a write with a stale version
- When the client gets the `conflict` error
- Then the caller can tell it apart from other errors by its code, with no string match

### R12 The conformance runner
`plugin-conformance -- <plugin command>` (stdio) or `plugin-conformance --url <url>` (HTTP) runs the protocol tests against any plugin. It tests only the capabilities that the plugin declares.

#### R12.S1 A passing plugin
- Given the memory example
- When `plugin-conformance -- ./memory` runs
- Then it prints each test with "pass", and exits with code 0

#### R12.S2 A failing plugin
- Given a plugin that accepts a write with a stale version
- When the runner runs
- Then the compare and swap test prints "fail" with the reason, and the runner exits with a code that is not 0

#### R12.S3 Only the declared capabilities
- Given a plugin that declares only `state`
- When the runner runs
- Then it runs the handshake, state, error and shutdown tests, and prints the events, blobs and watch tests as skipped

#### R12.S4 A continued run
- Given a plugin that declares the `events.continue` test hook
- When the runner runs
- Then it runs the read across a continued run; without the hook, it prints that test as skipped

### R13 The protocol document and the decision
`PROTOCOL.md` describes every method, its request and its result, and links to its JSON Schema. It records where the rules live and why, the rule for plugin authors on event history, and the mapping from the relay's stores to the protocol.

#### R13.S1 Every store method has a mapping
- Given the relay's `src/store/types.ts`
- When a reader looks up any method of SpecStore, DeskStore or RelayConfigStore in `PROTOCOL.md`
- Then the document names the protocol calls that replace it

#### R13.S2 The schemas match the messages
- Given the JSON Schemas in `schema/`
- When the conformance runner and the client tests run
- Then each request and result that they send or get is valid against its schema

### R14 CI and releases
CI runs on each push and pull request. A tag `v*` makes a release.

#### R14.S1 CI on a pull request
- Given a pull request in this repo
- When CI runs
- Then it runs the Go tests, the TypeScript build and tests, and the conformance runner against the memory example over stdio and HTTP

#### R14.S2 A release
- Given a tag `v1.0.0`
- When the release workflow runs
- Then it publishes the npm package, attaches the conformance binaries for darwin-arm64, darwin-amd64, linux-arm64 and linux-amd64, and the Go module is available at the tag `go/v1.0.0`

## Edge cases

- A message that is not valid JSON on stdio: the plugin answers with the JSON-RPC parse error and keeps the connection open.
- A plugin that writes text that is not JSON-RPC to stdout: the client logs it and ignores it, and does not crash.
- A handshake sent twice on one connection: the plugin answers with `refused`.
- A watch while the host calls shutdown: the wait ends at once with "nothing changed".
- An empty event batch: the plugin refuses it with `refused`.
- A collection or key name with `/`, `..` or a control character: the plugin refuses it with `refused`, so no name makes a path.
- An HTTP plugin with no token set that listens on an address other than 127.0.0.1: the SDK refuses to start and says why.
- The value of a secret setting never appears in a log line, an error message or a conformance report.

## Assumptions

- The rules stay in the host (thin plugins), as in the request. The plan records the comparison and the reasons. The conformance tests and the Go SDK follow that choice.
- The errors `closing` and `dropped` come from the host's rules. A thin plugin does not make them, but the code list keeps them so a host can map them in one place and a thick plugin can use them later.
- The relay's spec grace period, the closed-spec list and the token expiry stay host rules. The host keeps them in documents and attributes.
- Blobs move in chunks of at most 1 MB, encoded in base64, so no JSON-RPC message is large.
- Over HTTP, `watch` uses one long request per wait. There is no HTTP stream or WebSocket in version 1.
- The HTTP transport has an optional bearer token. The relay sets it when the plugin runs as a sidecar.
- The memory example can keep its data in one JSON file (`--data <file>`), so the restart test has something to read back. Without the flag it keeps everything in memory.
- The read across a continued run uses a test hook (`test.continueStream`) that a plugin can declare. The memory example declares it and starts a new "run" segment for the stream.
- The Go module is in `go/`, so its tags are `go/vX.Y.Z`. The npm package name is `@spec-studio-app/plugin-sdk`. The release needs an npm token in the repo secrets.
- Go 1.21 or later, Node 22 or later.

## The request

The user's description, as given:

Define the protocol between Spec Studio and a backend plugin, and ship it in this repo with a Go SDK, a TypeScript host client and a conformance test runner. Both hosts use this protocol: the relay (TypeScript, in spec-studio-app/relay) and the Mac app (Swift, in spec-studio-app/app). This spec makes the contract only. Later specs make the plugins and move the hosts onto them.

The later specs that the contract must serve:

1. plugin-json (Go): the default, for both hosts.
2. The relay as a plugin host: the relay's JSON and Temporal stores move out into the plugins, with no change in behavior. The relay image ships both plugins.
3. plugin-temporal (Go): one workflow per spec for both hosts, so relay messages and app events share one history.
4. The app as a plugin host: the local files stay the working state, and the `studio` CLI keeps reading them. The app writes each event and snapshot through to the plugin, and queues them while the Mac is offline. Settings gets a Backend pane that names the plugin, its version and a link to its repo.
5. Session recording: the timed pty bytes of each terminal and a snapshot of each spec document, in blobs.
6. The scrubber: a timeline on a spec, the state at any time, and playback.

Done when:

- `PROTOCOL.md` and the schemas cover every operation in the relay's `src/store/types.ts` and the app's needs above, with the rules-location decision recorded.
- The memory example passes the conformance runner over stdio and over HTTP, in CI.
- The TypeScript client can run the handshake and every call against the memory example in its tests.
- The PR description lists, for each method of the relay's SpecStore, DeskStore and RelayConfigStore, the protocol calls that replace it.

Notes: a new repo with only a README; no license; Go module path `github.com/spec-studio-app/plugin-sdk/go`; docs in ASD-STE100 Simplified Technical English.
