// fixture is a small plugin for the tests of the conformance runner. It
// gives state only, or state and events (--events, with no test hook). With
// --broken-cas it accepts a write with a stale version. With --secret it
// declares a secret setting and logs every setting at open. With --watch it
// gives watch on documents too, with no events. With --broken-commit it gives
// state.commit (protocol 1.1), but appends the events before it checks the
// version, so a conflict leaves the events in the stream.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/plugin"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

type doc struct {
	value json.RawMessage
	attrs protocol.Attributes
	ver   string
	at    time.Time
}

type stateStore struct {
	broken bool
	mu     sync.Mutex
	n      int
	docs   map[string]map[string]*doc
}

func (s *stateStore) Open(ctx context.Context, o plugin.OpenOptions) error {
	o.Log.Info("open", "settings", fmt.Sprint(o.Settings))
	return nil
}

func (s *stateStore) Close(ctx context.Context) error { return nil }

func (s *stateStore) Get(ctx context.Context, p protocol.StateGetParams) (protocol.StateGetResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.docs[p.Collection][p.Key]
	if d == nil {
		return protocol.StateGetResult{}, nil
	}
	at := d.at
	return protocol.StateGetResult{Exists: true, Value: d.value, Attributes: d.attrs, Version: d.ver, UpdatedAt: &at}, nil
}

func (s *stateStore) Put(ctx context.Context, p protocol.StatePutParams) (protocol.StatePutResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.docs[p.Collection][p.Key]
	if !s.broken {
		if p.ExpectedVersion == nil && cur != nil {
			return protocol.StatePutResult{}, protocol.ConflictError(&cur.ver)
		}
		if p.ExpectedVersion != nil && (cur == nil || cur.ver != *p.ExpectedVersion) {
			if cur == nil {
				return protocol.StatePutResult{}, protocol.ConflictError(nil)
			}
			return protocol.StatePutResult{}, protocol.ConflictError(&cur.ver)
		}
	}
	if s.docs[p.Collection] == nil {
		s.docs[p.Collection] = map[string]*doc{}
	}
	s.n++
	d := &doc{value: p.Value, attrs: p.Attributes, ver: strconv.Itoa(s.n), at: time.Now().UTC()}
	s.docs[p.Collection][p.Key] = d
	return protocol.StatePutResult{Version: d.ver, UpdatedAt: d.at}, nil
}

func (s *stateStore) Delete(ctx context.Context, p protocol.StateDeleteParams) (protocol.StateDeleteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.docs[p.Collection][p.Key]
	if cur == nil {
		return protocol.StateDeleteResult{}, protocol.NewError(protocol.NotFound, "No such document.")
	}
	if cur.ver != p.ExpectedVersion {
		return protocol.StateDeleteResult{}, protocol.ConflictError(&cur.ver)
	}
	delete(s.docs[p.Collection], p.Key)
	return protocol.StateDeleteResult{Deleted: true}, nil
}

func (s *stateStore) List(ctx context.Context, p protocol.StateListParams) (protocol.StateListResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	after := ""
	if p.Cursor != nil {
		after = *p.Cursor
	}
	var keys []string
	for k, d := range s.docs[p.Collection] {
		ok := k > after
		for fk, fv := range p.Filter {
			if d.attrs[fk] != fv {
				ok = false
			}
		}
		if ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	res := protocol.StateListResult{Items: []protocol.Document{}}
	for i, k := range keys {
		if i == p.PageSize {
			c := keys[i-1]
			res.Cursor = &c
			break
		}
		d := s.docs[p.Collection][k]
		item := protocol.Document{Key: k, Attributes: d.attrs, Version: d.ver, UpdatedAt: d.at}
		if item.Attributes == nil {
			item.Attributes = protocol.Attributes{}
		}
		if p.WithValues {
			item.Value = d.value
		}
		res.Items = append(res.Items, item)
	}
	return res, nil
}

// eventsStore adds a simple events capability, with no test hook.
type eventsStore struct {
	*stateStore
	emu     sync.Mutex
	streams map[string][]protocol.Event
	seq     int
}

func (s *eventsStore) Append(ctx context.Context, p protocol.EventsAppendParams) (protocol.EventsAppendResult, error) {
	s.emu.Lock()
	defer s.emu.Unlock()
	var cursors []string
	for _, e := range p.Events {
		s.seq++
		at := time.Now().UTC()
		if e.At != nil {
			at = *e.At
		}
		c := fmt.Sprintf("%012d", s.seq)
		s.streams[p.Stream] = append(s.streams[p.Stream], protocol.Event{Cursor: c, Type: e.Type, At: at, Data: e.Data})
		cursors = append(cursors, c)
	}
	return protocol.EventsAppendResult{Cursors: cursors}, nil
}

func (s *eventsStore) Read(ctx context.Context, p protocol.EventsReadParams) (protocol.EventsReadResult, error) {
	s.emu.Lock()
	defer s.emu.Unlock()
	res := protocol.EventsReadResult{Events: []protocol.Event{}, Cursor: p.After}
	for _, e := range s.streams[p.Stream] {
		if p.After != nil && e.Cursor <= *p.After {
			continue
		}
		if (p.From != nil && e.At.Before(*p.From)) || (p.To != nil && !e.At.Before(*p.To)) {
			continue
		}
		if len(res.Events) == p.Limit {
			res.More = true
			break
		}
		c := e.Cursor
		res.Events = append(res.Events, e)
		res.Cursor = &c
	}
	return res, nil
}

func (s *eventsStore) DeleteStream(ctx context.Context, p protocol.EventsDeleteParams) (protocol.EventsDeleteResult, error) {
	s.emu.Lock()
	defer s.emu.Unlock()
	had := len(s.streams[p.Stream]) > 0
	delete(s.streams, p.Stream)
	return protocol.EventsDeleteResult{Deleted: had}, nil
}

// brokenCommitStore gives state.commit that is not atomic: it appends the
// events first, then writes the document with the version check.
type brokenCommitStore struct {
	*eventsStore
}

func (s *brokenCommitStore) Commit(ctx context.Context, p protocol.StateCommitParams) (protocol.StateCommitResult, error) {
	var cursors []string
	if len(p.Events) > 0 {
		r, err := s.Append(ctx, protocol.EventsAppendParams{Stream: p.Stream, Events: p.Events})
		if err != nil {
			return protocol.StateCommitResult{}, err
		}
		cursors = r.Cursors
	}
	put, err := s.Put(ctx, protocol.StatePutParams{Collection: p.Collection, Key: p.Key, Value: p.Value, Attributes: p.Attributes, ExpectedVersion: p.ExpectedVersion})
	if err != nil {
		return protocol.StateCommitResult{}, err
	}
	return protocol.StateCommitResult{Version: put.Version, UpdatedAt: put.UpdatedAt, Cursors: cursors}, nil
}

// watchStore gives watch on documents, with no events: a wait on a stream
// gets refused.
type watchStore struct {
	*stateStore
}

func (s *watchStore) Wait(ctx context.Context, p protocol.WatchWaitParams) (protocol.WatchWaitResult, error) {
	if p.IsStream() {
		return protocol.WatchWaitResult{}, protocol.NewError(protocol.Refused, "The fixture gives no events, so it cannot wait on a stream.")
	}
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		s.mu.Lock()
		var cur *string
		if d := s.docs[p.Collection][p.Key]; d != nil {
			v := d.ver
			cur = &v
		}
		s.mu.Unlock()
		if (cur == nil) != (p.Version == nil) || (cur != nil && *cur != *p.Version) {
			return protocol.WatchWaitResult{Changed: true, Version: cur}, nil
		}
		select {
		case <-ctx.Done():
			return protocol.WatchWaitResult{}, ctx.Err()
		case <-t.C:
		}
	}
}

func main() {
	broken := flag.Bool("broken-cas", false, "accept a write with a stale version")
	secret := flag.Bool("secret", false, "declare a secret setting")
	events := flag.Bool("events", false, "give the events capability")
	watch := flag.Bool("watch", false, "give the watch capability on documents, with no events")
	brokenCommit := flag.Bool("broken-commit", false, "give events and a state.commit that is not atomic")
	// Parse the fixture's flags now: they choose the store.
	flag.String("listen", "", "")
	flag.String("token", "", "")
	flag.Parse()
	if len(os.Args) > 1 && flag.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "unexpected arguments")
		os.Exit(2)
	}
	state := &stateStore{broken: *broken, docs: map[string]map[string]*doc{}}
	var store plugin.Store = state
	if *events {
		store = &eventsStore{stateStore: state, streams: map[string][]protocol.Event{}}
	}
	if *watch {
		store = &watchStore{stateStore: state}
	}
	if *brokenCommit {
		store = &brokenCommitStore{eventsStore: &eventsStore{stateStore: state, streams: map[string][]protocol.Event{}}}
	}
	var settings []protocol.Setting
	if *secret {
		settings = []protocol.Setting{{Name: "api_key", Description: "A key for the tests.", Secret: true}}
	}
	err := plugin.Run(context.Background(), plugin.Plugin{
		Info:     protocol.PluginInfo{Name: "fixture", Version: "0.0.1", Repo: "https://github.com/spec-studio-app/plugin-sdk"},
		Settings: settings,
		Store:    store,
	}, plugin.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
