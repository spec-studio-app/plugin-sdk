package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/plugin"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// doc is one stored document.
type doc struct {
	Value      json.RawMessage     `json:"value"`
	Attributes protocol.Attributes `json:"attributes"`
	Version    string              `json:"version"`
	UpdatedAt  time.Time           `json:"updatedAt"`
}

// storedEvent is one event. Run is the part of the stream that holds it.
type storedEvent struct {
	Run  int             `json:"run"`
	Seq  int64           `json:"seq"`
	Type string          `json:"type"`
	At   time.Time       `json:"at"`
	Data json.RawMessage `json:"data"`
}

// stream is one event log. A new run starts at test.continueStream, as a
// Temporal workflow continues as new. The cursors of every run stay valid.
type stream struct {
	Run    int           `json:"run"`
	Seq    int64         `json:"seq"`
	Events []storedEvent `json:"events"`
}

// snapshot is the data file.
type snapshot struct {
	Version int                        `json:"version"`
	Counter int64                      `json:"counter"`
	Docs    map[string]map[string]*doc `json:"docs"`
	Streams map[string]*stream         `json:"streams"`
	Blobs   map[string]string          `json:"blobs"`
}

// memoryStore keeps everything in memory, and in a data file when one is set.
type memoryStore struct {
	dataFile *string

	mu      sync.Mutex
	counter int64
	docs    map[string]map[string]*doc
	streams map[string]*stream
	blobs   map[string][]byte
	// changed is closed and replaced at each change, to wake the waits.
	changed chan struct{}
	log     interface{ Error(string, ...any) }
	// lock holds the data file's lock while the store is open.
	lock *os.File
}

func newMemoryStore(dataFile *string) *memoryStore {
	return &memoryStore{
		dataFile: dataFile,
		docs:     map[string]map[string]*doc{},
		streams:  map[string]*stream{},
		blobs:    map[string][]byte{},
		changed:  make(chan struct{}),
	}
}

func (m *memoryStore) file() string {
	if m.dataFile == nil {
		return ""
	}
	return *m.dataFile
}

// Durable is true with a data file.
func (m *memoryStore) Durable() bool { return m.file() != "" }

func (m *memoryStore) Open(ctx context.Context, o plugin.OpenOptions) error {
	m.log = o.Log
	path := m.file()
	if path == "" {
		return nil
	}
	// One process at a time: the compare and swap is atomic in this process only.
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return protocol.NewError(protocol.Unavailable, "The memory plugin cannot open the lock file %s.lock: %s", path, err.Error())
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return protocol.NewError(protocol.Unavailable, "Another process uses the data file %s. Stop it first.", path)
	}
	m.lock = lock
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return protocol.NewError(protocol.Unavailable, "The memory plugin cannot read its data file %s: %s", path, err.Error())
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return protocol.NewError(protocol.BadSettings, "The data file %s is not valid JSON. Correct it or remove it.", path)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counter = snap.Counter
	if snap.Docs != nil {
		m.docs = snap.Docs
	}
	if snap.Streams != nil {
		m.streams = snap.Streams
	}
	for sha, b64 := range snap.Blobs {
		b, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return protocol.NewError(protocol.BadSettings, "The data file %s has a blob that is not base64.", path)
		}
		m.blobs[sha] = b
	}
	return nil
}

func (m *memoryStore) Close(ctx context.Context) error {
	if m.lock != nil {
		m.lock.Close()
		m.lock = nil
	}
	return nil
}

// commit writes the data file, then wakes the waits. When the write fails,
// it runs undo, so the store keeps no change that the host was told failed.
// Call it with mu held.
func (m *memoryStore) commit(undo func()) error {
	if err := m.write(); err != nil {
		undo()
		return err
	}
	close(m.changed)
	m.changed = make(chan struct{})
	return nil
}

func (m *memoryStore) write() error {
	path := m.file()
	if path == "" {
		return nil
	}
	snap := snapshot{Version: 1, Counter: m.counter, Docs: m.docs, Streams: m.streams, Blobs: map[string]string{}}
	for sha, b := range m.blobs {
		snap.Blobs[sha] = base64.StdEncoding.EncodeToString(b)
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".memory-*.tmp")
	if err != nil {
		return protocol.NewError(protocol.Unavailable, "The memory plugin cannot write next to its data file: %s", err.Error())
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return protocol.NewError(protocol.Unavailable, "The memory plugin cannot write its data file: %s", err.Error())
	}
	tmp.Sync()
	tmp.Close()
	if err := os.Rename(tmp.Name(), path); err != nil {
		os.Remove(tmp.Name())
		return protocol.NewError(protocol.Unavailable, "The memory plugin cannot replace its data file: %s", err.Error())
	}
	return nil
}

func (m *memoryStore) nextVersion() string {
	m.counter++
	return "v" + strconv.FormatInt(m.counter, 10)
}

// ---- state ----

func (m *memoryStore) Get(ctx context.Context, p protocol.StateGetParams) (protocol.StateGetResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.docs[p.Collection][p.Key]
	if d == nil {
		return protocol.StateGetResult{Exists: false}, nil
	}
	at := d.UpdatedAt
	return protocol.StateGetResult{Exists: true, Value: d.Value, Attributes: copyAttrs(d.Attributes), Version: d.Version, UpdatedAt: &at}, nil
}

func (m *memoryStore) Put(ctx context.Context, p protocol.StatePutParams) (protocol.StatePutResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	col := m.docs[p.Collection]
	cur := col[p.Key]
	if p.ExpectedVersion == nil && cur != nil {
		return protocol.StatePutResult{}, protocol.ConflictError(&cur.Version)
	}
	if p.ExpectedVersion != nil && (cur == nil || cur.Version != *p.ExpectedVersion) {
		if cur == nil {
			return protocol.StatePutResult{}, protocol.ConflictError(nil)
		}
		return protocol.StatePutResult{}, protocol.ConflictError(&cur.Version)
	}
	if col == nil {
		col = map[string]*doc{}
		m.docs[p.Collection] = col
	}
	d := &doc{Value: append(json.RawMessage(nil), p.Value...), Attributes: copyAttrs(p.Attributes), Version: m.nextVersion(), UpdatedAt: time.Now().UTC()}
	col[p.Key] = d
	if err := m.commit(func() {
		if cur == nil {
			delete(col, p.Key)
		} else {
			col[p.Key] = cur
		}
	}); err != nil {
		return protocol.StatePutResult{}, err
	}
	return protocol.StatePutResult{Version: d.Version, UpdatedAt: d.UpdatedAt}, nil
}

func (m *memoryStore) Delete(ctx context.Context, p protocol.StateDeleteParams) (protocol.StateDeleteResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := m.docs[p.Collection][p.Key]
	if cur == nil {
		return protocol.StateDeleteResult{}, protocol.NewError(protocol.NotFound, "The document %s/%s does not exist.", p.Collection, p.Key)
	}
	if cur.Version != p.ExpectedVersion {
		return protocol.StateDeleteResult{}, protocol.ConflictError(&cur.Version)
	}
	delete(m.docs[p.Collection], p.Key)
	if err := m.commit(func() { m.docs[p.Collection][p.Key] = cur }); err != nil {
		return protocol.StateDeleteResult{}, err
	}
	return protocol.StateDeleteResult{Deleted: true}, nil
}

func (m *memoryStore) List(ctx context.Context, p protocol.StateListParams) (protocol.StateListResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	after := ""
	if p.Cursor != nil && *p.Cursor != "" {
		b, err := base64.RawURLEncoding.DecodeString(*p.Cursor)
		if err != nil {
			return protocol.StateListResult{}, protocol.NewError(protocol.Refused, "The cursor is not a cursor of this plugin.")
		}
		after = string(b)
	}
	col := m.docs[p.Collection]
	keys := make([]string, 0, len(col))
	for k := range col {
		if k > after && matches(col[k].Attributes, p.Filter) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	res := protocol.StateListResult{Items: []protocol.Document{}}
	for i, k := range keys {
		if i == p.PageSize {
			c := base64.RawURLEncoding.EncodeToString([]byte(keys[i-1]))
			res.Cursor = &c
			break
		}
		d := col[k]
		item := protocol.Document{Key: k, Attributes: copyAttrs(d.Attributes), Version: d.Version, UpdatedAt: d.UpdatedAt}
		if p.WithValues {
			item.Value = d.Value
		}
		res.Items = append(res.Items, item)
	}
	return res, nil
}

func matches(attrs, filter protocol.Attributes) bool {
	for k, want := range filter {
		got := attrs[k]
		if !scalarEqual(got, want) {
			return false
		}
	}
	return true
}

func scalarEqual(a, b any) bool {
	if fa, ok := toFloat(a); ok {
		fb, ok := toFloat(b)
		return ok && fa == fb
	}
	return a == b
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

func copyAttrs(a protocol.Attributes) protocol.Attributes {
	out := protocol.Attributes{}
	for k, v := range a {
		out[k] = v
	}
	return out
}

// ---- events ----

func cursorOf(e storedEvent) string { return fmt.Sprintf("r%d.%d", e.Run, e.Seq) }

func parseCursor(c string) (int64, bool) {
	dot := strings.IndexByte(c, '.')
	if !strings.HasPrefix(c, "r") || dot < 0 {
		return 0, false
	}
	if _, err := strconv.Atoi(c[1:dot]); err != nil {
		return 0, false
	}
	seq, err := strconv.ParseInt(c[dot+1:], 10, 64)
	return seq, err == nil
}

func (m *memoryStore) Append(ctx context.Context, p protocol.EventsAppendParams) (protocol.EventsAppendResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.streams[p.Stream]
	if st == nil {
		st = &stream{Run: 1}
		m.streams[p.Stream] = st
	}
	now := time.Now().UTC()
	oldSeq, oldLen := st.Seq, len(st.Events)
	cursors := make([]string, 0, len(p.Events))
	for _, e := range p.Events {
		st.Seq++
		at := now
		if e.At != nil {
			at = e.At.UTC()
		}
		se := storedEvent{Run: st.Run, Seq: st.Seq, Type: e.Type, At: at, Data: append(json.RawMessage(nil), e.Data...)}
		st.Events = append(st.Events, se)
		cursors = append(cursors, cursorOf(se))
	}
	if err := m.commit(func() { st.Seq, st.Events = oldSeq, st.Events[:oldLen] }); err != nil {
		return protocol.EventsAppendResult{}, err
	}
	return protocol.EventsAppendResult{Cursors: cursors}, nil
}

func (m *memoryStore) Read(ctx context.Context, p protocol.EventsReadParams) (protocol.EventsReadResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var after int64
	if p.After != nil && *p.After != "" {
		seq, ok := parseCursor(*p.After)
		if !ok {
			return protocol.EventsReadResult{}, protocol.NewError(protocol.Refused, "The cursor %q is not a cursor of this plugin.", *p.After)
		}
		after = seq
	}
	res := protocol.EventsReadResult{Events: []protocol.Event{}, Cursor: p.After}
	st := m.streams[p.Stream]
	if st == nil {
		return res, nil
	}
	for _, e := range st.Events {
		if e.Seq <= after {
			continue
		}
		if p.From != nil && e.At.Before(*p.From) {
			continue
		}
		if p.To != nil && !e.At.Before(*p.To) {
			continue
		}
		if len(res.Events) == p.Limit {
			res.More = true
			break
		}
		c := cursorOf(e)
		res.Events = append(res.Events, protocol.Event{Cursor: c, Type: e.Type, At: e.At, Data: e.Data})
		res.Cursor = &c
	}
	return res, nil
}

func (m *memoryStore) DeleteStream(ctx context.Context, p protocol.EventsDeleteParams) (protocol.EventsDeleteResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.streams[p.Stream]
	had := st != nil && len(st.Events) > 0
	var old []storedEvent
	if st != nil {
		// Keep the counters, so a cursor is never used again.
		old = st.Events
		st.Events = nil
	}
	if err := m.commit(func() {
		if st != nil {
			st.Events = old
		}
	}); err != nil {
		return protocol.EventsDeleteResult{}, err
	}
	return protocol.EventsDeleteResult{Deleted: had}, nil
}

// ContinueStream starts a new run of the stream (the continueStream test hook).
func (m *memoryStore) ContinueStream(ctx context.Context, p protocol.ContinueStreamParams) (protocol.ContinueStreamResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.streams[p.Stream]
	if st == nil {
		st = &stream{Run: 1}
		m.streams[p.Stream] = st
	}
	st.Run++
	if err := m.commit(func() { st.Run-- }); err != nil {
		return protocol.ContinueStreamResult{}, err
	}
	return protocol.ContinueStreamResult{Run: "r" + strconv.Itoa(st.Run)}, nil
}

// ---- blobs ----

func (m *memoryStore) HasBlobs(ctx context.Context, shas []string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []string{}
	for _, h := range shas {
		if _, ok := m.blobs[h]; ok {
			out = append(out, h)
		}
	}
	return out, nil
}

func (m *memoryStore) PutBlob(ctx context.Context, sha string, size int64, r io.Reader) error {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	old, had := m.blobs[sha]
	m.blobs[sha] = buf.Bytes()
	return m.commit(func() {
		if had {
			m.blobs[sha] = old
		} else {
			delete(m.blobs, sha)
		}
	})
}

func (m *memoryStore) GetBlob(ctx context.Context, sha string, offset int64, length int) ([]byte, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.blobs[sha]
	if !ok {
		return nil, 0, protocol.NewError(protocol.NotFound, "The blob %s does not exist.", sha)
	}
	size := int64(len(b))
	if offset >= size {
		return []byte{}, size, nil
	}
	end := offset + int64(length)
	if end > size {
		end = size
	}
	return append([]byte(nil), b[offset:end]...), size, nil
}

func (m *memoryStore) DeleteBlob(ctx context.Context, sha string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.blobs[sha]; !ok {
		return false, nil
	}
	old := m.blobs[sha]
	delete(m.blobs, sha)
	return true, m.commit(func() { m.blobs[sha] = old })
}

// ---- watch ----

func (m *memoryStore) Wait(ctx context.Context, p protocol.WatchWaitParams) (protocol.WatchWaitResult, error) {
	for {
		m.mu.Lock()
		res, done := m.check(p)
		ch := m.changed
		m.mu.Unlock()
		if done {
			return res, nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return protocol.WatchWaitResult{Changed: false}, nil
		}
	}
}

// check is true when the wait can end. Call it with mu held.
func (m *memoryStore) check(p protocol.WatchWaitParams) (protocol.WatchWaitResult, bool) {
	if p.IsStream() {
		var last *string
		if st := m.streams[p.Stream]; st != nil && len(st.Events) > 0 {
			c := cursorOf(st.Events[len(st.Events)-1])
			last = &c
		}
		if last == nil {
			return protocol.WatchWaitResult{}, false
		}
		if p.After == nil {
			return protocol.WatchWaitResult{Changed: true, Cursor: last}, true
		}
		afterSeq, _ := parseCursor(*p.After)
		lastSeq, _ := parseCursor(*last)
		if lastSeq > afterSeq {
			return protocol.WatchWaitResult{Changed: true, Cursor: last}, true
		}
		return protocol.WatchWaitResult{}, false
	}
	var cur *string
	if d := m.docs[p.Collection][p.Key]; d != nil {
		v := d.Version
		cur = &v
	}
	same := (cur == nil && p.Version == nil) || (cur != nil && p.Version != nil && *cur == *p.Version)
	if same {
		return protocol.WatchWaitResult{}, false
	}
	return protocol.WatchWaitResult{Changed: true, Version: cur}, true
}
