// Package plugin is the Go SDK to write a Spec Studio backend plugin.
//
// A plugin author writes a Store: the storage calls. The SDK does the
// transports (stdio and HTTP), the handshake, the settings check, the error
// codes, the capability routing and the shutdown.
//
//	func main() {
//		plugin.Main(plugin.Plugin{
//			Info:  protocol.PluginInfo{Name: "mine", Version: "0.1.0", Repo: "https://github.com/me/mine"},
//			Store: newStore(),
//		})
//	}
//
// A Store must implement StateStore. It gives a capability when it also
// implements EventStore (events), BlobStore (blobs) or Watcher (watch).
// It speaks protocol 1.1 (state.commit) when it implements EventStore and
// Committer. Else it speaks only protocol 1.0.
package plugin

import (
	"context"
	"io"
	"log/slog"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// Plugin is one plugin: its name, its settings and its store.
type Plugin struct {
	Info protocol.PluginInfo
	// Settings are the settings that the plugin declares.
	Settings []protocol.Setting
	// Store is the storage. The SDK calls Open at the first handshake.
	Store Store
	// Limits overrides the default limits. A zero field keeps the default.
	Limits protocol.Limits
	// ProtocolVersions overrides the versions that the plugin speaks (tests only).
	ProtocolVersions []string
}

// OpenOptions are given to Store.Open.
type OpenOptions struct {
	// Settings are the checked settings: each required setting has a value.
	Settings map[string]string
	// Host is the host of the first handshake: its kind (app, relay, cli or test)
	// and its version. A store can use the kind for a default, for example a
	// default data folder.
	Host protocol.Host
	// Log writes to stderr. It replaces the value of each secret setting with [secret].
	Log *slog.Logger
}

// Store is the storage of a plugin. Every Store gives state.
type Store interface {
	StateStore
	// Open opens the storage with the settings. Return a protocol error with
	// the code bad_settings or unavailable when it cannot open.
	Open(ctx context.Context, o OpenOptions) error
	// Close closes the storage at shutdown.
	Close(ctx context.Context) error
}

// StateStore is the state capability.
//
// Get returns Exists false for a missing document. Put checks the expected
// version and writes in one atomic step; it returns protocol.ConflictError
// on a mismatch. Delete returns not_found or conflict. List returns the
// documents in key order. The SDK checks the names and the page size first.
type StateStore interface {
	Get(ctx context.Context, p protocol.StateGetParams) (protocol.StateGetResult, error)
	Put(ctx context.Context, p protocol.StatePutParams) (protocol.StatePutResult, error)
	Delete(ctx context.Context, p protocol.StateDeleteParams) (protocol.StateDeleteResult, error)
	List(ctx context.Context, p protocol.StateListParams) (protocol.StateListResult, error)
}

// EventStore is the events capability. The SDK checks the stream name, the
// limit and that the event list is not empty.
type EventStore interface {
	Append(ctx context.Context, p protocol.EventsAppendParams) (protocol.EventsAppendResult, error)
	Read(ctx context.Context, p protocol.EventsReadParams) (protocol.EventsReadResult, error)
	DeleteStream(ctx context.Context, p protocol.EventsDeleteParams) (protocol.EventsDeleteResult, error)
}

// BlobStore is the blobs capability. The SDK keeps the chunks of an upload
// and checks the size and the SHA-256 before it calls PutBlob.
type BlobStore interface {
	// HasBlobs returns the hashes that the store has.
	HasBlobs(ctx context.Context, sha256s []string) ([]string, error)
	// PutBlob stores a blob whose hash the SDK checked.
	PutBlob(ctx context.Context, sha256 string, size int64, r io.Reader) error
	// GetBlob returns length bytes at offset (fewer at the end) and the full size.
	// It returns not_found for a missing blob.
	GetBlob(ctx context.Context, sha256 string, offset int64, length int) ([]byte, int64, error)
	// DeleteBlob returns false when the store did not have the blob.
	DeleteBlob(ctx context.Context, sha256 string) (bool, error)
}

// Watcher is the watch capability. Wait returns at once when the condition
// is true, else at the first change, or with Changed false when ctx ends.
// The SDK sets a deadline on ctx from timeoutMs and cancels it at shutdown.
type Watcher interface {
	Wait(ctx context.Context, p protocol.WatchWaitParams) (protocol.WatchWaitResult, error)
}

// Committer is the state.commit call of protocol 1.1. Commit checks the
// expected version, writes the document and appends the events to the
// stream in one atomic step, or does none of them. On a mismatch it returns
// protocol.ConflictError with the stored version (nil when no document
// exists), and writes no document and no event. p.Events can be empty: then
// Commit writes only the document and returns no cursor.
//
// The SDK checks the names, the sizes and the event types first, and calls
// Commit only when the store also implements EventStore. A store that
// implements Committer and EventStore lists protocol 1.1.
type Committer interface {
	Commit(ctx context.Context, p protocol.StateCommitParams) (protocol.StateCommitResult, error)
}

// StoreIdentity is an optional interface. StoreID names the data that the
// store reads and writes, for the storeId of the handshake (protocol 1.1).
// Two plugin processes that read and write the same data must give the same
// StoreID. The SDK calls it after Open. An empty string gives no storeId.
type StoreIdentity interface {
	StoreID() string
}

// StreamContinuer is the continueStream test hook.
type StreamContinuer interface {
	ContinueStream(ctx context.Context, p protocol.ContinueStreamParams) (protocol.ContinueStreamResult, error)
}

// Durable is an optional interface: a store that keeps its data after the
// plugin stops says so.
type Durable interface {
	Durable() bool
}

// Capabilities gives the capabilities of a store.
func Capabilities(s Store) []protocol.Capability {
	caps := []protocol.Capability{protocol.CapState}
	if _, ok := s.(EventStore); ok {
		caps = append(caps, protocol.CapEvents)
	}
	if _, ok := s.(BlobStore); ok {
		caps = append(caps, protocol.CapBlobs)
	}
	if _, ok := s.(Watcher); ok {
		caps = append(caps, protocol.CapWatch)
	}
	return caps
}

// TestHooks gives the test hooks of a store.
func TestHooks(s Store) []string {
	hooks := []string{}
	if _, ok := s.(StreamContinuer); ok {
		hooks = append(hooks, protocol.HookContinueStream)
	}
	return hooks
}

// CanCommit is true when the store gives state.commit: it implements
// Committer and EventStore.
func CanCommit(s Store) bool {
	_, commits := s.(Committer)
	_, events := s.(EventStore)
	return commits && events
}

// Versions gives the protocol versions of a store: 1.0 and 1.1 when it can
// commit, else 1.0 only.
func Versions(s Store) []string {
	if CanCommit(s) {
		return append([]string(nil), protocol.Versions...)
	}
	return []string{protocol.Version10}
}

func isDurable(s Store) bool {
	d, ok := s.(Durable)
	return ok && d.Durable()
}

func (p *Plugin) limits() protocol.Limits {
	l := protocol.DefaultLimits()
	if p.Limits.MaxMessageBytes > 0 {
		l.MaxMessageBytes = p.Limits.MaxMessageBytes
	}
	if p.Limits.MaxChunkBytes > 0 {
		l.MaxChunkBytes = p.Limits.MaxChunkBytes
	}
	if p.Limits.MaxPageSize > 0 {
		l.MaxPageSize = p.Limits.MaxPageSize
	}
	if p.Limits.MaxWaitMs > 0 {
		l.MaxWaitMs = p.Limits.MaxWaitMs
	}
	return l
}

func (p *Plugin) versions() []string {
	if len(p.ProtocolVersions) > 0 {
		return p.ProtocolVersions
	}
	return Versions(p.Store)
}

func (p *Plugin) describe() protocol.DescribeResult {
	settings := p.Settings
	if settings == nil {
		settings = []protocol.Setting{}
	}
	return protocol.DescribeResult{
		Plugin:           p.Info,
		ProtocolVersions: p.versions(),
		Capabilities:     Capabilities(p.Store),
		Settings:         settings,
		TestHooks:        TestHooks(p.Store),
		Durable:          isDurable(p.Store),
	}
}
