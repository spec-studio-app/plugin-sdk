package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/spec-studio-app/plugin-sdk/go/plugin"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

func opened(t *testing.T, file string) *memoryStore {
	t.Helper()
	m := newMemoryStore(&file)
	if err := m.Open(context.Background(), plugin.OpenOptions{Settings: map[string]string{}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}); err != nil {
		t.Fatal(err)
	}
	return m
}

// R9.S4
func TestDataFileSurvivesARestart(t *testing.T) {
	ctx := context.Background()
	file := filepath.Join(t.TempDir(), "data.json")
	m := opened(t, file)
	put, err := m.Put(ctx, protocol.StatePutParams{Collection: "specs", Key: "abc", Value: json.RawMessage(`{"a":1}`), Attributes: protocol.Attributes{"open": true}})
	if err != nil {
		t.Fatal(err)
	}
	m.Append(ctx, protocol.EventsAppendParams{Stream: "spec:abc", Events: []protocol.NewEvent{{Type: "x", Data: json.RawMessage(`1`)}}})

	// A second process cannot open the same data file while the first holds it.
	second := newMemoryStore(&file)
	if err := second.Open(ctx, plugin.OpenOptions{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}); !protocol.Is(err, protocol.Unavailable) {
		t.Fatalf("a second open: %v, want unavailable", err)
	}
	m.Close(ctx)

	again := opened(t, file)
	got, _ := again.Get(ctx, protocol.StateGetParams{Collection: "specs", Key: "abc"})
	if !got.Exists || got.Version != put.Version || string(got.Value) != `{"a":1}` || got.Attributes["open"] != true {
		t.Fatalf("after restart: %+v", got)
	}
	// A new write after the restart gets a new version.
	next, _ := again.Put(ctx, protocol.StatePutParams{Collection: "specs", Key: "other", Value: json.RawMessage(`1`)})
	if next.Version == put.Version {
		t.Fatal("a version was used again")
	}
	ev, _ := again.Read(ctx, protocol.EventsReadParams{Stream: "spec:abc", Limit: 10})
	if len(ev.Events) != 1 {
		t.Fatalf("events after restart: %+v", ev)
	}
}

// R4.S4
func TestReadAcrossRuns(t *testing.T) {
	ctx := context.Background()
	m := opened(t, "")
	a, _ := m.Append(ctx, protocol.EventsAppendParams{Stream: "s", Events: []protocol.NewEvent{{Type: "A"}, {Type: "B"}}})
	m.ContinueStream(ctx, protocol.ContinueStreamParams{Stream: "s"})
	m.Append(ctx, protocol.EventsAppendParams{Stream: "s", Events: []protocol.NewEvent{{Type: "C"}}})
	all, _ := m.Read(ctx, protocol.EventsReadParams{Stream: "s", Limit: 10})
	if len(all.Events) != 3 || all.Events[2].Type != "C" {
		t.Fatalf("read %+v", all)
	}
	after, _ := m.Read(ctx, protocol.EventsReadParams{Stream: "s", After: &a.Cursors[1], Limit: 10})
	if len(after.Events) != 1 || after.Events[0].Type != "C" {
		t.Fatalf("after B: %+v", after)
	}
}

// A failed open lets the next open of the same process take the lock again.
func TestFailedOpenReleasesTheLock(t *testing.T) {
	file := filepath.Join(t.TempDir(), "data.json")
	os.WriteFile(file, []byte("not json"), 0o600)
	m := newMemoryStore(&file)
	o := plugin.OpenOptions{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := m.Open(context.Background(), o); !protocol.Is(err, protocol.BadSettings) {
		t.Fatalf("open of a bad file: %v", err)
	}
	os.WriteFile(file, []byte("{}"), 0o600)
	if err := m.Open(context.Background(), o); err != nil {
		t.Fatalf("the second open: %v", err)
	}
}

// A failed write of the data file keeps no change.
func TestFailedWriteKeepsNoChange(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	file := filepath.Join(dir, "data.json")
	m := opened(t, file)
	v1, err := m.Put(ctx, protocol.StatePutParams{Collection: "c", Key: "k", Value: json.RawMessage(`1`)})
	if err != nil {
		t.Fatal(err)
	}
	os.Chmod(dir, 0o500)
	defer os.Chmod(dir, 0o700)
	if _, err := m.Put(ctx, protocol.StatePutParams{Collection: "c", Key: "k", Value: json.RawMessage(`2`), ExpectedVersion: &v1.Version}); err == nil {
		t.Skip("the folder is still writable (root?)")
	}
	got, _ := m.Get(ctx, protocol.StateGetParams{Collection: "c", Key: "k"})
	if got.Version != v1.Version || string(got.Value) != "1" {
		t.Fatalf("the failed write changed the document: %+v", got)
	}
}
