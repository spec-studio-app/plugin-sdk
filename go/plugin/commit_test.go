package plugin_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/plugin"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// commitStore is a fake store with events and state.commit (protocol 1.1).
type commitStore struct {
	*fakeStore
	last *protocol.StateCommitParams
}

func (c *commitStore) Append(ctx context.Context, p protocol.EventsAppendParams) (protocol.EventsAppendResult, error) {
	return protocol.EventsAppendResult{Cursors: []string{"c1"}}, nil
}
func (c *commitStore) Read(ctx context.Context, p protocol.EventsReadParams) (protocol.EventsReadResult, error) {
	return protocol.EventsReadResult{}, nil
}
func (c *commitStore) DeleteStream(ctx context.Context, p protocol.EventsDeleteParams) (protocol.EventsDeleteResult, error) {
	return protocol.EventsDeleteResult{}, nil
}
func (c *commitStore) Commit(ctx context.Context, p protocol.StateCommitParams) (protocol.StateCommitResult, error) {
	c.last = &p
	var cursors []string
	for i := range p.Events {
		cursors = append(cursors, "c"+string(rune('1'+i)))
	}
	return protocol.StateCommitResult{Version: "v1", UpdatedAt: time.Now().UTC(), Cursors: cursors}, nil
}
func (c *commitStore) StoreID() string { return "fake:/data" }

// commitNoEvents has Commit but no events: it cannot give state.commit.
type commitNoEvents struct{ *fakeStore }

func (c *commitNoEvents) Commit(ctx context.Context, p protocol.StateCommitParams) (protocol.StateCommitResult, error) {
	return protocol.StateCommitResult{}, errors.New("must not be called")
}

func newCommitStore() *commitStore { return &commitStore{fakeStore: newFake()} }

// R2.S4: a store with no Commit lists 1.0 only, chooses 1.0 for a 1.1 host,
// gives no storeId, and has no state.commit.
func TestStoreWithNoCommitSpeaks10(t *testing.T) {
	for name, store := range map[string]plugin.Store{"state only": newFake(), "commit with no events": &commitNoEvents{newFake()}} {
		t.Run(name, func(t *testing.T) {
			h := start(t, plugin.Plugin{Info: testInfo, Store: store})
			d, err := h.c.Describe(ctx(t))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(d.ProtocolVersions, []string{"1.0"}) {
				t.Fatalf("versions %v, want [1.0]", d.ProtocolVersions)
			}
			r, err := h.c.Handshake(ctx(t), hello(nil, "1.0", "1.1"))
			if err != nil {
				t.Fatal(err)
			}
			if r.ProtocolVersion != "1.0" || r.StoreID != "" {
				t.Fatalf("chose %s with storeId %q; want 1.0 and no storeId", r.ProtocolVersion, r.StoreID)
			}
			_, err = h.c.Commit(ctx(t), protocol.StateCommitParams{Collection: "c", Key: "k", Value: json.RawMessage(`1`), Stream: "s"})
			var pe *protocol.Error
			if !errors.As(err, &pe) || pe.RPC != protocol.RPCMethodNotFound {
				t.Fatalf("state.commit on a 1.0 plugin: %v, want -32601", err)
			}
		})
	}
}

// A store with Commit and events lists 1.0 and 1.1, and gives the storeId in 1.1.
func TestCommitterSpeaks11(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newCommitStore()})
	d, err := h.c.Describe(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(d.ProtocolVersions, []string{"1.0", "1.1"}) {
		t.Fatalf("versions %v, want [1.0 1.1]", d.ProtocolVersions)
	}
	r, err := h.c.Handshake(ctx(t), hello(nil, "1.1"))
	if err != nil {
		t.Fatal(err)
	}
	if r.ProtocolVersion != "1.1" || r.StoreID != "fake:/data" {
		t.Fatalf("chose %s with storeId %q; want 1.1 and fake:/data", r.ProtocolVersion, r.StoreID)
	}
}

// Section 5: a 1.0 host and a 1.1 plugin agree on 1.0, with no storeId.
func TestOldHostGets10(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newCommitStore()})
	r, err := h.c.Handshake(ctx(t), hello(nil, "1.0"))
	if err != nil {
		t.Fatal(err)
	}
	if r.ProtocolVersion != "1.0" || r.StoreID != "" {
		t.Fatalf("chose %s with storeId %q; want 1.0 and no storeId", r.ProtocolVersion, r.StoreID)
	}
}

func TestCommitChecksTheParams(t *testing.T) {
	store := newCommitStore()
	h := start(t, plugin.Plugin{Info: testInfo, Store: store})
	if _, err := h.c.Handshake(ctx(t), hello(nil, "1.1")); err != nil {
		t.Fatal(err)
	}

	// No events: the store gets an empty list and the result has an empty list.
	var raw json.RawMessage
	if err := h.c.CallRaw(ctx(t), protocol.MethodStateCommit, json.RawMessage(`{"collection":"c","key":"k","value":1,"expectedVersion":null,"stream":"s","events":[]}`), &raw); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"cursors":[]`) {
		t.Fatalf("the result %s has no empty cursors list", raw)
	}
	if store.last == nil || store.last.Events == nil || len(store.last.Events) != 0 || store.last.Attributes == nil {
		t.Fatalf("the store got %+v", store.last)
	}

	// Events with no data get null data.
	r, err := h.c.Commit(ctx(t), protocol.StateCommitParams{Collection: "c", Key: "k", Value: json.RawMessage(`1`), Stream: "s", Events: []protocol.NewEvent{{Type: "a"}, {Type: "b", Data: json.RawMessage(`2`)}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Cursors) != 2 || string(store.last.Events[0].Data) != "null" {
		t.Fatalf("cursors %v, data %s", r.Cursors, store.last.Events[0].Data)
	}

	cases := []struct {
		name string
		p    protocol.StateCommitParams
		want func(error) bool
	}{
		{"bad stream", protocol.StateCommitParams{Collection: "c", Key: "k", Value: json.RawMessage(`1`), Stream: "../s"}, func(err error) bool { return protocol.Is(err, protocol.Refused) }},
		{"no stream", protocol.StateCommitParams{Collection: "c", Key: "k", Value: json.RawMessage(`1`)}, func(err error) bool { return protocol.Is(err, protocol.Refused) }},
		{"bad key", protocol.StateCommitParams{Collection: "c", Key: "a/b", Value: json.RawMessage(`1`), Stream: "s"}, func(err error) bool { return protocol.Is(err, protocol.Refused) }},
		{"event with no type", protocol.StateCommitParams{Collection: "c", Key: "k", Value: json.RawMessage(`1`), Stream: "s", Events: []protocol.NewEvent{{Data: json.RawMessage(`1`)}}}, isRPC(protocol.RPCInvalidParams)},
		{"empty expected version", protocol.StateCommitParams{Collection: "c", Key: "k", Value: json.RawMessage(`1`), Stream: "s", ExpectedVersion: protocol.Ptr("")}, isRPC(protocol.RPCInvalidParams)},
	}
	for _, tc := range cases {
		store.last = nil
		_, err := h.c.Commit(ctx(t), tc.p)
		if !tc.want(err) {
			t.Errorf("%s: %v", tc.name, err)
		}
		if store.last != nil {
			t.Errorf("%s: the store got the call", tc.name)
		}
	}
}

// The CLI is a host kind of its own.
func TestCLIHostKind(t *testing.T) {
	store := newFake()
	h := start(t, plugin.Plugin{Info: testInfo, Store: store})
	p := hello(nil)
	p.Host.Kind = protocol.HostCLI
	if _, err := h.c.Handshake(ctx(t), p); err != nil {
		t.Fatal(err)
	}
	if store.host.Kind != "cli" {
		t.Fatalf("the store got the host %+v", store.host)
	}
}

func isRPC(code int) func(error) bool {
	return func(err error) bool {
		var pe *protocol.Error
		return errors.As(err, &pe) && pe.RPC == code
	}
}
