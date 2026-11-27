// Package protocol has the messages and the error codes of the Spec Studio
// backend plugin protocol. PROTOCOL.md at the root of the repo defines them.
package protocol

import (
	"bytes"
	"encoding/json"
	"time"
)

// Version is the newest protocol version that this package speaks.
const Version = "1.1"

// Versions are all the protocol versions that this package speaks.
var Versions = []string{"1.0", "1.1"}

// Version10 is protocol 1.0. A plugin with no state.commit speaks only this version.
const Version10 = "1.0"

// Version11 is protocol 1.1: it adds state.commit and the storeId of the handshake.
const Version11 = "1.1"

// AtLeast is true when the version v is want or a later minor version of the
// same major version. A version that does not parse gives false.
func AtLeast(v, want string) bool {
	vm, vn, err := ParseVersion(v)
	if err != nil {
		return false
	}
	wm, wn, err := ParseVersion(want)
	if err != nil {
		return false
	}
	return vm == wm && vn >= wn
}

// The method names.
const (
	MethodDescribe       = "plugin.describe"
	MethodHandshake      = "plugin.handshake"
	MethodPing           = "plugin.ping"
	MethodShutdown       = "plugin.shutdown"
	MethodStateGet       = "state.get"
	MethodStatePut       = "state.put"
	MethodStateDelete    = "state.delete"
	MethodStateList      = "state.list"
	MethodStateCommit    = "state.commit"
	MethodEventsAppend   = "events.append"
	MethodEventsRead     = "events.read"
	MethodEventsDelete   = "events.delete"
	MethodBlobsBegin     = "blobs.begin"
	MethodBlobsWrite     = "blobs.write"
	MethodBlobsCommit    = "blobs.commit"
	MethodBlobsGet       = "blobs.get"
	MethodBlobsHas       = "blobs.has"
	MethodBlobsDelete    = "blobs.delete"
	MethodWatchWait      = "watch.wait"
	MethodContinueStream = "test.continueStream"
)

// Capability is a set of methods.
type Capability string

const (
	CapState  Capability = "state"
	CapEvents Capability = "events"
	CapBlobs  Capability = "blobs"
	CapWatch  Capability = "watch"
)

// CapabilityOf gives the capability of a method, and false for a method that
// every plugin has (plugin.*) or a test hook.
func CapabilityOf(method string) (Capability, bool) {
	for i := 0; i < len(method); i++ {
		if method[i] == '.' {
			switch method[:i] {
			case "state":
				return CapState, true
			case "events":
				return CapEvents, true
			case "blobs":
				return CapBlobs, true
			case "watch":
				return CapWatch, true
			}
			return "", false
		}
	}
	return "", false
}

// HookContinueStream is the test hook for test.continueStream.
const HookContinueStream = "continueStream"

// The default limits of the SDK.
const (
	DefaultMaxMessageBytes = 2 << 20
	DefaultMaxChunkBytes   = 1 << 20
	DefaultMaxPageSize     = 1000
	DefaultMaxWaitMs       = 60000
	DefaultPageSize        = 100
)

// Limits are the plugin's limits, from the handshake.
type Limits struct {
	MaxMessageBytes int `json:"maxMessageBytes"`
	MaxChunkBytes   int `json:"maxChunkBytes"`
	MaxPageSize     int `json:"maxPageSize"`
	MaxWaitMs       int `json:"maxWaitMs"`
}

// DefaultLimits are the limits of a plugin that sets none.
func DefaultLimits() Limits {
	return Limits{
		MaxMessageBytes: DefaultMaxMessageBytes,
		MaxChunkBytes:   DefaultMaxChunkBytes,
		MaxPageSize:     DefaultMaxPageSize,
		MaxWaitMs:       DefaultMaxWaitMs,
	}
}

// PluginInfo names a plugin.
type PluginInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Repo    string `json:"repo"`
}

// Setting is one setting that a plugin declares.
type Setting struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Required    bool   `json:"required"`
	Secret      bool   `json:"secret"`
}

// DescribeResult is the result of plugin.describe.
type DescribeResult struct {
	Plugin           PluginInfo   `json:"plugin"`
	ProtocolVersions []string     `json:"protocolVersions"`
	Capabilities     []Capability `json:"capabilities"`
	Settings         []Setting    `json:"settings"`
	TestHooks        []string     `json:"testHooks"`
	Durable          bool         `json:"durable"`
}

// The host kinds.
const (
	HostApp   = "app"
	HostRelay = "relay"
	HostCLI   = "cli"
	HostTest  = "test"
)

// ValidHostKind is true for the host kinds of the protocol.
func ValidHostKind(kind string) bool {
	switch kind {
	case HostApp, HostRelay, HostCLI, HostTest:
		return true
	}
	return false
}

// Host names the host in a handshake.
type Host struct {
	// Kind is "app", "relay", "cli" or "test".
	Kind    string `json:"kind"`
	Version string `json:"version"`
}

// HandshakeParams are the params of plugin.handshake.
type HandshakeParams struct {
	Host             Host              `json:"host"`
	ProtocolVersions []string          `json:"protocolVersions"`
	Settings         map[string]string `json:"settings"`
}

// HandshakeResult is the result of plugin.handshake.
type HandshakeResult struct {
	Plugin          PluginInfo   `json:"plugin"`
	ProtocolVersion string       `json:"protocolVersion"`
	Capabilities    []Capability `json:"capabilities"`
	TestHooks       []string     `json:"testHooks"`
	Durable         bool         `json:"durable"`
	Limits          Limits       `json:"limits"`
	// Session is set over HTTP only.
	Session string `json:"session,omitempty"`
	// StoreID names the data that the plugin reads and writes (protocol 1.1,
	// optional). Two plugin processes that read and write the same data give
	// the same StoreID.
	StoreID string `json:"storeId,omitempty"`
}

// Has is true when the plugin gives the capability.
func (r HandshakeResult) Has(c Capability) bool {
	for _, x := range r.Capabilities {
		if x == c {
			return true
		}
	}
	return false
}

// HasHook is true when the plugin declares the test hook.
func (r HandshakeResult) HasHook(h string) bool {
	for _, x := range r.TestHooks {
		if x == h {
			return true
		}
	}
	return false
}

// PingResult is the result of plugin.ping.
type PingResult struct {
	Status string    `json:"status"`
	Time   time.Time `json:"time"`
}

// ShutdownParams are the params of plugin.shutdown.
type ShutdownParams struct {
	TimeoutMs int `json:"timeoutMs,omitempty"`
}

// Empty is an empty result.
type Empty struct{}

// Attributes are the attributes of a document: a flat map of JSON scalars.
type Attributes map[string]any

// StateGetParams are the params of state.get.
type StateGetParams struct {
	Collection string `json:"collection"`
	Key        string `json:"key"`
}

// StateGetResult is the result of state.get.
type StateGetResult struct {
	Exists     bool            `json:"exists"`
	Value      json.RawMessage `json:"value,omitempty"`
	Attributes Attributes      `json:"attributes,omitempty"`
	Version    string          `json:"version,omitempty"`
	UpdatedAt  *time.Time      `json:"updatedAt,omitempty"`
}

// MarshalJSON writes every field of a document that exists, and only
// "exists" for one that does not.
func (r StateGetResult) MarshalJSON() ([]byte, error) {
	if !r.Exists {
		return []byte(`{"exists":false}`), nil
	}
	attrs := r.Attributes
	if attrs == nil {
		attrs = Attributes{}
	}
	value := r.Value
	if len(value) == 0 {
		value = json.RawMessage("null")
	}
	return MarshalNoEscape(struct {
		Exists     bool            `json:"exists"`
		Value      json.RawMessage `json:"value"`
		Attributes Attributes      `json:"attributes"`
		Version    string          `json:"version"`
		UpdatedAt  *time.Time      `json:"updatedAt"`
	}{true, value, attrs, r.Version, r.UpdatedAt})
}

// StatePutParams are the params of state.put. A nil ExpectedVersion means
// that the document must not exist.
type StatePutParams struct {
	Collection      string          `json:"collection"`
	Key             string          `json:"key"`
	Value           json.RawMessage `json:"value"`
	Attributes      Attributes      `json:"attributes,omitempty"`
	ExpectedVersion *string         `json:"expectedVersion"`
}

// StatePutResult is the result of state.put.
type StatePutResult struct {
	Version   string    `json:"version"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// StateDeleteParams are the params of state.delete.
type StateDeleteParams struct {
	Collection      string `json:"collection"`
	Key             string `json:"key"`
	ExpectedVersion string `json:"expectedVersion"`
}

// StateDeleteResult is the result of state.delete.
type StateDeleteResult struct {
	Deleted bool `json:"deleted"`
}

// StateListParams are the params of state.list.
type StateListParams struct {
	Collection string     `json:"collection"`
	Filter     Attributes `json:"filter,omitempty"`
	PageSize   int        `json:"pageSize,omitempty"`
	Cursor     *string    `json:"cursor,omitempty"`
	WithValues bool       `json:"withValues,omitempty"`
}

// Document is one item of a state.list result.
type Document struct {
	Key        string          `json:"key"`
	Attributes Attributes      `json:"attributes"`
	Version    string          `json:"version"`
	UpdatedAt  time.Time       `json:"updatedAt"`
	Value      json.RawMessage `json:"value,omitempty"`
}

// StateListResult is the result of state.list.
type StateListResult struct {
	Items  []Document `json:"items"`
	Cursor *string    `json:"cursor"`
}

// StateCommitParams are the params of state.commit (protocol 1.1): a
// document, the events to append to a stream, and the expected version of
// the document. A nil ExpectedVersion means that the document must not
// exist. Events can be empty.
type StateCommitParams struct {
	Collection      string          `json:"collection"`
	Key             string          `json:"key"`
	Value           json.RawMessage `json:"value"`
	Attributes      Attributes      `json:"attributes,omitempty"`
	ExpectedVersion *string         `json:"expectedVersion"`
	Stream          string          `json:"stream"`
	Events          []NewEvent      `json:"events"`
}

// StateCommitResult is the result of state.commit. Cursors has one cursor
// for each event, in order, and is empty when the commit had no events.
type StateCommitResult struct {
	Version   string    `json:"version"`
	UpdatedAt time.Time `json:"updatedAt"`
	Cursors   []string  `json:"cursors"`
}

// MarshalJSON writes an empty list, not null, for no cursors.
func (r StateCommitResult) MarshalJSON() ([]byte, error) {
	type plain StateCommitResult
	if r.Cursors == nil {
		r.Cursors = []string{}
	}
	return MarshalNoEscape(plain(r))
}

// NewEvent is one event of events.append and state.commit.
type NewEvent struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
	At   *time.Time      `json:"at,omitempty"`
}

// EventsAppendParams are the params of events.append.
type EventsAppendParams struct {
	Stream string     `json:"stream"`
	Events []NewEvent `json:"events"`
}

// EventsAppendResult is the result of events.append.
type EventsAppendResult struct {
	Cursors []string `json:"cursors"`
}

// EventsReadParams are the params of events.read.
type EventsReadParams struct {
	Stream string     `json:"stream"`
	After  *string    `json:"after,omitempty"`
	From   *time.Time `json:"from,omitempty"`
	To     *time.Time `json:"to,omitempty"`
	Limit  int        `json:"limit,omitempty"`
}

// Event is one stored event.
type Event struct {
	Cursor string          `json:"cursor"`
	Type   string          `json:"type"`
	At     time.Time       `json:"at"`
	Data   json.RawMessage `json:"data"`
}

// EventsReadResult is the result of events.read.
type EventsReadResult struct {
	Events []Event `json:"events"`
	Cursor *string `json:"cursor"`
	More   bool    `json:"more"`
}

// EventsDeleteParams are the params of events.delete.
type EventsDeleteParams struct {
	Stream string `json:"stream"`
}

// EventsDeleteResult is the result of events.delete.
type EventsDeleteResult struct {
	Deleted bool `json:"deleted"`
}

// BlobsBeginParams are the params of blobs.begin.
type BlobsBeginParams struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// BlobsBeginResult is the result of blobs.begin.
type BlobsBeginResult struct {
	Exists   bool   `json:"exists"`
	UploadID string `json:"uploadId,omitempty"`
}

// BlobsWriteParams are the params of blobs.write. Data is base64 in JSON.
type BlobsWriteParams struct {
	UploadID string `json:"uploadId"`
	Offset   int64  `json:"offset"`
	Data     []byte `json:"data"`
}

// BlobsWriteResult is the result of blobs.write.
type BlobsWriteResult struct {
	Received int64 `json:"received"`
}

// BlobsCommitParams are the params of blobs.commit.
type BlobsCommitParams struct {
	UploadID string `json:"uploadId"`
}

// BlobsCommitResult is the result of blobs.commit.
type BlobsCommitResult struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// BlobsGetParams are the params of blobs.get.
type BlobsGetParams struct {
	SHA256 string `json:"sha256"`
	Offset int64  `json:"offset,omitempty"`
	Length int    `json:"length,omitempty"`
}

// BlobsGetResult is the result of blobs.get. Data is base64 in JSON.
type BlobsGetResult struct {
	Data   []byte `json:"data"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

// BlobsHasParams are the params of blobs.has.
type BlobsHasParams struct {
	SHA256s []string `json:"sha256s"`
}

// BlobsHasResult is the result of blobs.has.
type BlobsHasResult struct {
	Present []string `json:"present"`
}

// BlobsDeleteParams are the params of blobs.delete.
type BlobsDeleteParams struct {
	SHA256 string `json:"sha256"`
}

// BlobsDeleteResult is the result of blobs.delete.
type BlobsDeleteResult struct {
	Deleted bool `json:"deleted"`
}

// WatchWaitParams are the params of watch.wait: a document (Collection, Key
// and Version) or a stream (Stream and After).
type WatchWaitParams struct {
	Collection string  `json:"collection,omitempty"`
	Key        string  `json:"key,omitempty"`
	Version    *string `json:"version"`
	Stream     string  `json:"stream,omitempty"`
	After      *string `json:"after"`
	TimeoutMs  int     `json:"timeoutMs"`
}

// IsStream is true when the wait is on a stream.
func (p WatchWaitParams) IsStream() bool { return p.Stream != "" }

// MarshalJSON writes only the fields of the target.
func (p WatchWaitParams) MarshalJSON() ([]byte, error) {
	if p.IsStream() {
		return MarshalNoEscape(struct {
			Stream    string  `json:"stream"`
			After     *string `json:"after"`
			TimeoutMs int     `json:"timeoutMs"`
		}{p.Stream, p.After, p.TimeoutMs})
	}
	return MarshalNoEscape(struct {
		Collection string  `json:"collection"`
		Key        string  `json:"key"`
		Version    *string `json:"version"`
		TimeoutMs  int     `json:"timeoutMs"`
	}{p.Collection, p.Key, p.Version, p.TimeoutMs})
}

// WatchWaitResult is the result of watch.wait.
type WatchWaitResult struct {
	Changed bool    `json:"changed"`
	Version *string `json:"version,omitempty"`
	Cursor  *string `json:"cursor,omitempty"`
}

// MarshalJSON keeps a null version or cursor when the wait changed.
func (r WatchWaitResult) MarshalJSON() ([]byte, error) {
	m := map[string]any{"changed": r.Changed}
	if r.Changed {
		if r.Cursor != nil {
			m["cursor"] = *r.Cursor
		} else {
			m["version"] = r.Version
		}
	}
	return MarshalNoEscape(m)
}

// ContinueStreamParams are the params of test.continueStream.
type ContinueStreamParams struct {
	Stream string `json:"stream"`
}

// ContinueStreamResult is the result of test.continueStream.
type ContinueStreamResult struct {
	Run string `json:"run"`
}

// MarshalNoEscape writes JSON with no HTML escapes (< > & stay one byte), so
// a value takes the same bytes in an answer as in the request that stored it.
func MarshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Ptr gives a pointer to a value.
func Ptr[T any](v T) *T { return &v }
