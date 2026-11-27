package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
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
