package conformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/client"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// tests gives the tests in the order they run. The handshake tests come
// first; the shutdown and the restart come last.
func tests() []test {
	return []test{
		{name: "describe", covers: "R2.S1", run: testDescribe},
		{name: "call-before-handshake", covers: "R1.S3", run: testCallBeforeHandshake},
		{name: "handshake-no-major", covers: "R1.S2", run: testNoMajor},
		{name: "secret-canary", covers: "R2.S3", run: testSecretCanary},
		{name: "handshake", covers: "R1.S1", run: testHandshake},
		{name: "version-choice", covers: "R1.S4", run: testVersionChoice},
		{name: "handshake-twice", covers: "R1", run: testHandshakeTwice},
		{name: "ping", covers: "R9.S1", run: testPing},
		{name: "unknown-method", covers: "R7.S1", run: testUnknownMethod},
		{name: "parse-error", covers: "R8.S1", run: testParseError},
		{name: "capability-refused", covers: "R7.S2", run: testCapabilityRefused},
		{name: "bad-name", covers: "R3", run: testBadName},

		{name: "state-missing", covers: "R3.S1", run: testStateMissing},
		{name: "state-create", covers: "R3.S2", run: testStateCreate},
		{name: "state-stale-version", covers: "R3.S3", run: testStateStale},
		{name: "state-race", covers: "R3.S4", run: testStateRace},
		{name: "state-list-filter", covers: "R3.S5", run: testListFilter},
		{name: "state-list-pages", covers: "R3.S6", run: testListPages},
		{name: "state-delete", covers: "R3.S7", run: testStateDelete},

		{name: "events-order", covers: "R4.S1", needs: protocol.CapEvents, run: testEventsOrder},
		{name: "events-after", covers: "R4.S2", needs: protocol.CapEvents, run: testEventsAfter},
		{name: "events-range", covers: "R4.S3", needs: protocol.CapEvents, run: testEventsRange},
		{name: "events-continued-run", covers: "R4.S4", needs: protocol.CapEvents, run: testContinuedRun},
		{name: "events-delete", covers: "R4.S5", needs: protocol.CapEvents, run: testEventsDelete},
		{name: "events-empty", covers: "R4", needs: protocol.CapEvents, run: testEventsEmpty},

		{name: "blobs-roundtrip", covers: "R5.S1", needs: protocol.CapBlobs, run: testBlobsRoundtrip},
		{name: "blobs-wrong-hash", covers: "R5.S2", needs: protocol.CapBlobs, run: testBlobsWrongHash},
		{name: "blobs-has", covers: "R5.S3", needs: protocol.CapBlobs, run: testBlobsHas},
		{name: "blobs-large", covers: "R5.S4", needs: protocol.CapBlobs, run: testBlobsLarge},
		{name: "blobs-missing", covers: "R5", needs: protocol.CapBlobs, run: testBlobsMissing},

		{name: "watch-change", covers: "R6.S1", needs: protocol.CapWatch, run: testWatchChange},
		{name: "watch-timeout", covers: "R6.S2", needs: protocol.CapWatch, run: testWatchTimeout},
		{name: "watch-already-newer", covers: "R6.S3", needs: protocol.CapWatch, run: testWatchNewer},
		{name: "watch-stream", covers: "R6", needs: protocol.CapWatch, run: testWatchStream},

		{name: "restart", covers: "R9.S4", run: testRestart},
		{name: "shutdown", covers: "R9.S2", run: testShutdown},
	}
}

// ---- handshake ----

func testDescribe(e *env) error {
	d, err := e.c.Describe(e.ctx)
	if err != nil {
		return fmt.Errorf("plugin.describe before the handshake: %w", err)
	}
	e.describe = d
	if d.Plugin.Name == "" || d.Plugin.Version == "" {
		return errors.New("the plugin has no name or no version")
	}
	if len(d.ProtocolVersions) == 0 {
		return errors.New("the plugin declares no protocol version")
	}
	if !contains(d.Capabilities, protocol.CapState) {
		return errors.New("the plugin does not declare the state capability")
	}
	for _, s := range d.Settings {
		if s.Name == "" || s.Description == "" {
			return fmt.Errorf("the setting %q has no name or no description", s.Name)
		}
	}
	return nil
}

func testCallBeforeHandshake(e *env) error {
	_, err := e.c.Get(e.ctx, e.name("c"), "k")
	return expectCode(err, protocol.Refused)
}

func testNoMajor(e *env) error {
	_, err := e.c.Handshake(e.ctx, e.hello("999.0"))
	if err := expectCode(err, protocol.Refused); err != nil {
		return err
	}
	if !strings.Contains(err.Error(), "999.0") {
		return fmt.Errorf("the message does not name the host's versions: %q", err.Error())
	}
	for _, v := range e.describe.ProtocolVersions {
		if !strings.Contains(err.Error(), v) {
			return fmt.Errorf("the message does not name the plugin's version %s: %q", v, err.Error())
		}
	}
	return nil
}

func testSecretCanary(e *env) error {
	var secrets []string
	for _, s := range e.describe.Settings {
		if s.Secret {
			secrets = append(secrets, s.Name)
		}
	}
	if len(secrets) == 0 {
		return skip("the plugin declares no secret setting")
	}
	canary := "conformance-canary-" + randomHex(8)
	p := e.hello()
	for _, name := range secrets {
		p.Settings[name] = canary
	}
	// An unknown setting makes the handshake fail after the plugin read the secrets.
	p.Settings["conformance_unknown"] = canary
	_, err := e.c.Handshake(e.ctx, p)
	if err == nil {
		return errors.New("the handshake with an unknown setting succeeded")
	}
	if strings.Contains(err.Error(), canary) {
		return fmt.Errorf("the error shows the value of a secret setting: %q", err.Error())
	}
	if e.stdio() {
		time.Sleep(100 * time.Millisecond)
		if strings.Contains(e.stderr.String(), canary) {
			return errors.New("the plugin's stderr shows the value of a secret setting")
		}
	}
	return nil
}

func testHandshake(e *env) error {
	r, err := e.c.Handshake(e.ctx, e.hello())
	if err != nil {
		return fmt.Errorf("the handshake failed: %w", err)
	}
	e.info = r
	if r.Plugin.Name == "" || r.Plugin.Version == "" {
		return errors.New("the result has no plugin name or version")
	}
	if u, err := url.Parse(r.Plugin.Repo); err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("the repo %q is not a URL", r.Plugin.Repo)
	}
	if !r.Has(protocol.CapState) {
		return errors.New("the plugin does not give state")
	}
	if !e.stdio() && r.Session == "" {
		return errors.New("the HTTP handshake has no session")
	}
	if r.Limits.MaxChunkBytes <= 0 || r.Limits.MaxMessageBytes <= 0 || r.Limits.MaxPageSize <= 0 || r.Limits.MaxWaitMs <= 0 {
		return fmt.Errorf("the limits are not complete: %+v", r.Limits)
	}
	return nil
}

func testVersionChoice(e *env) error {
	// The runner speaks 1.99: the plugin must choose its highest 1.x version.
	want := ""
	best := -1
	for _, v := range e.describe.ProtocolVersions {
		major, minor, err := protocol.ParseVersion(v)
		if err == nil && major == 1 && minor > best {
			best, want = minor, v
		}
	}
	if want == "" {
		return skip("the plugin speaks no 1.x version")
	}
	if e.info.ProtocolVersion != want {
		return fmt.Errorf("the plugin chose %s for a host of 1.99; want %s", e.info.ProtocolVersion, want)
	}
	return nil
}

func testHandshakeTwice(e *env) error {
	_, err := e.c.Handshake(e.ctx, e.hello())
	return expectCode(err, protocol.Refused)
}

func testPing(e *env) error {
	r, err := e.c.Ping(e.ctx)
	if err != nil {
		return err
	}
	if r.Status != "ok" || r.Time.IsZero() {
		return fmt.Errorf("ping %+v", r)
	}
	return nil
}

func testUnknownMethod(e *env) error {
	return expectRPC(e.c.Call(e.ctx, "state.conformanceUnknown", nil, nil), protocol.RPCMethodNotFound)
}

func testParseError(e *env) error {
	if !e.stdio() {
		return skip("stdio only")
	}
	raw, err := e.c.SendLine(e.ctx, []byte(`{"jsonrpc": "2.0", not json`))
	if err != nil {
		return err
	}
	var resp protocol.Response
	if json.Unmarshal(raw, &resp) != nil || resp.Error == nil || resp.Error.RPC != protocol.RPCParseError {
		return fmt.Errorf("the answer to a line that is not JSON is %s; want the error -32700", raw)
	}
	if _, err := e.c.Ping(e.ctx); err != nil {
		return fmt.Errorf("the connection did not stay open: %w", err)
	}
	return nil
}

func testCapabilityRefused(e *env) error {
	calls := map[protocol.Capability]func() error{
		protocol.CapEvents: func() error {
			_, err := e.c.Read(e.ctx, protocol.EventsReadParams{Stream: e.name("s")})
			return err
		},
		protocol.CapBlobs: func() error {
			_, err := e.c.HasBlobs(e.ctx)
			return err
		},
		protocol.CapWatch: func() error {
			_, err := e.c.Wait(e.ctx, protocol.WatchWaitParams{Collection: e.name("c"), Key: "k", TimeoutMs: 10})
			return err
		},
	}
	tested := 0
	for _, cap := range []protocol.Capability{protocol.CapEvents, protocol.CapBlobs, protocol.CapWatch} {
		if e.info.Has(cap) {
			continue
		}
		tested++
		err := calls[cap]()
		if err := expectCode(err, protocol.Refused); err != nil {
			return fmt.Errorf("%s: %w", cap, err)
		}
		if !strings.Contains(err.Error(), string(cap)) {
			return fmt.Errorf("the message does not name the capability %s: %q", cap, err.Error())
		}
	}
	if tested == 0 {
		return skip("the plugin gives every capability")
	}
	return nil
}

func testBadName(e *env) error {
	return e.expectInvalid(func() error {
		for _, key := range []string{"../escape", "a/b", "a..b", "x\x00y"} {
			if _, err := e.c.Get(e.ctx, e.name("c"), key); expectCode(err, protocol.Refused) != nil {
				return fmt.Errorf("the key %q: got %v; want refused", key, err)
			}
		}
		return nil
	})
}

// ---- state ----

func raw(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func testStateMissing(e *env) error {
	r, err := e.c.Get(e.ctx, e.name("specs"), "missing")
	if err != nil {
		return err
	}
	if r.Exists {
		return errors.New("a missing document exists")
	}
	return nil
}

func testStateCreate(e *env) error {
	col := e.name("specs")
	value := map[string]any{"title": "Drop a spec", "n": 3.0, "nested": map[string]any{"ok": true}}
	attrs := protocol.Attributes{"repoKey": "1a2b3c", "pendingForMac": false, "n": 2.0, "none": nil}
	put, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw(value), Attributes: attrs})
	if err != nil {
		return err
	}
	if put.Version == "" || put.UpdatedAt.IsZero() {
		return fmt.Errorf("the write result %+v has no version or no time", put)
	}
	got, err := e.c.Get(e.ctx, col, "abc")
	if err != nil {
		return err
	}
	if !got.Exists || got.Version != put.Version {
		return fmt.Errorf("the read %+v does not match the write %+v", got, put)
	}
	var back any
	json.Unmarshal(got.Value, &back)
	if !reflect.DeepEqual(back, value) {
		return fmt.Errorf("the value %s is not the value written", got.Value)
	}
	if !reflect.DeepEqual(map[string]any(got.Attributes), map[string]any(attrs)) {
		return fmt.Errorf("the attributes %v are not the attributes written %v", got.Attributes, attrs)
	}
	// A second make of the same document is a conflict.
	_, err = e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw(1)})
	if err := expectCode(err, protocol.Conflict); err != nil {
		return fmt.Errorf("a make of a document that exists: %w", err)
	}
	return nil
}

func testStateStale(e *env) error {
	col := e.name("stale")
	v1, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw("one")})
	if err != nil {
		return err
	}
	v2, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw("two"), ExpectedVersion: &v1.Version})
	if err != nil {
		return err
	}
	if v2.Version == v1.Version {
		return errors.New("a write did not make a new version")
	}
	_, err = e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw("three"), ExpectedVersion: &v1.Version})
	if err := expectCode(err, protocol.Conflict); err != nil {
		return err
	}
	var pe *protocol.Error
	if errors.As(err, &pe) && (pe.Data.Version == nil || *pe.Data.Version != v2.Version) {
		return fmt.Errorf("the conflict does not give the stored version %s", v2.Version)
	}
	got, _ := e.c.Get(e.ctx, col, "abc")
	if got.Version != v2.Version || string(got.Value) != `"two"` {
		return fmt.Errorf("the stale write changed the document: %+v", got)
	}
	// An expected version on a missing document is a conflict too.
	_, err = e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "nothing", Value: raw(1), ExpectedVersion: &v1.Version})
	return expectCode(err, protocol.Conflict)
}

func testStateRace(e *env) error {
	col := e.name("race")
	v1, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw(0)})
	if err != nil {
		return err
	}
	const writers = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, conflicts := 0, 0
	var other error
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw(i), ExpectedVersion: &v1.Version})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case protocol.Is(err, protocol.Conflict):
				conflicts++
			default:
				other = err
			}
		}(i)
	}
	wg.Wait()
	if other != nil {
		return other
	}
	if ok != 1 || conflicts != writers-1 {
		return fmt.Errorf("%d writes with the same expected version: %d succeeded and %d got conflict; want 1 and %d", writers, ok, conflicts, writers-1)
	}
	return nil
}

func testListFilter(e *env) error {
	col := e.name("list")
	docs := []struct {
		key     string
		pending bool
	}{{"a", true}, {"b", false}, {"c", true}}
	for _, d := range docs {
		if _, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: d.key, Value: raw(d.key), Attributes: protocol.Attributes{"pendingForMac": d.pending, "repoKey": "r1"}}); err != nil {
			return err
		}
	}
	r, err := e.c.List(e.ctx, protocol.StateListParams{Collection: col, Filter: protocol.Attributes{"pendingForMac": true}})
	if err != nil {
		return err
	}
	if keys := keysOf(r.Items); strings.Join(keys, ",") != "a,c" {
		return fmt.Errorf("the filter gave %v; want [a c]", keys)
	}
	for _, it := range r.Items {
		if it.Version == "" || it.Attributes["repoKey"] != "r1" {
			return fmt.Errorf("the item %+v has no version or no attributes", it)
		}
		if len(it.Value) != 0 {
			return errors.New("an item has a value without withValues")
		}
	}
	with, err := e.c.List(e.ctx, protocol.StateListParams{Collection: col, WithValues: true, Filter: protocol.Attributes{"repoKey": "r1", "pendingForMac": false}})
	if err != nil {
		return err
	}
	if len(with.Items) != 1 || string(with.Items[0].Value) != `"b"` {
		return fmt.Errorf("withValues gave %+v", with.Items)
	}
	// A missing attribute has the value null.
	none, err := e.c.List(e.ctx, protocol.StateListParams{Collection: col, Filter: protocol.Attributes{"absent": nil}})
	if err != nil {
		return err
	}
	if len(none.Items) != 3 {
		return fmt.Errorf("a filter on a missing attribute with null gave %d items; want 3", len(none.Items))
	}
	return nil
}

func testListPages(e *env) error {
	col := e.name("pages")
	const n = 250
	for i := 0; i < n; i++ {
		if _, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: fmt.Sprintf("k%03d", i), Value: raw(i)}); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	pages := 0
	var cursor *string
	var last string
	for {
		r, err := e.c.List(e.ctx, protocol.StateListParams{Collection: col, PageSize: 100, Cursor: cursor})
		if err != nil {
			return err
		}
		pages++
		if len(r.Items) > 100 {
			return fmt.Errorf("a page has %d items; the page size is 100", len(r.Items))
		}
		for _, it := range r.Items {
			if seen[it.Key] {
				return fmt.Errorf("the key %s came twice", it.Key)
			}
			if it.Key <= last {
				return fmt.Errorf("the key %s came after %s: not in key order", it.Key, last)
			}
			seen[it.Key] = true
			last = it.Key
		}
		if r.Cursor == nil {
			break
		}
		if pages > 10 {
			return errors.New("the pages do not end")
		}
		cursor = r.Cursor
	}
	if len(seen) != n || pages != 3 {
		return fmt.Errorf("got %d documents in %d pages; want %d in 3", len(seen), pages, n)
	}
	return nil
}

func testStateDelete(e *env) error {
	col := e.name("del")
	v, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw(1)})
	if err != nil {
		return err
	}
	if err := expectCode(e.c.Delete(e.ctx, col, "abc", "conformance-wrong-version"), protocol.Conflict); err != nil {
		return fmt.Errorf("a delete with a wrong version: %w", err)
	}
	if err := e.c.Delete(e.ctx, col, "abc", v.Version); err != nil {
		return err
	}
	got, err := e.c.Get(e.ctx, col, "abc")
	if err != nil {
		return err
	}
	if got.Exists {
		return errors.New("the document exists after the delete")
	}
	if err := expectCode(e.c.Delete(e.ctx, col, "abc", v.Version), protocol.NotFound); err != nil {
		return fmt.Errorf("a delete of a missing document: %w", err)
	}
	// A new document has a new version: a version is never used again.
	again, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw(2)})
	if err != nil {
		return err
	}
	if again.Version == v.Version {
		return errors.New("a version was used again after a delete")
	}
	return nil
}

func keysOf(items []protocol.Document) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Key
	}
	return out
}

// ---- events ----

func ev(t string, data any) protocol.NewEvent { return protocol.NewEvent{Type: t, Data: raw(data)} }

func typesOf(events []protocol.Event) string {
	out := make([]string, len(events))
	for i, x := range events {
		out[i] = x.Type
	}
	return strings.Join(out, ",")
}

func testEventsOrder(e *env) error {
	s := e.name("order")
	r1, err := e.c.Append(e.ctx, s, ev("A", 1), ev("B", 2), ev("C", 3))
	if err != nil {
		return err
	}
	if len(r1.Cursors) != 3 {
		return fmt.Errorf("3 events gave %d cursors", len(r1.Cursors))
	}
	if _, err := e.c.Append(e.ctx, s, ev("D", 4)); err != nil {
		return err
	}
	all, err := e.c.ReadAll(e.ctx, protocol.EventsReadParams{Stream: s})
	if err != nil {
		return err
	}
	if got := typesOf(all); got != "A,B,C,D" {
		return fmt.Errorf("the read gave %s; want A,B,C,D", got)
	}
	for i, x := range all {
		if x.Cursor == "" || x.At.IsZero() {
			return fmt.Errorf("event %d has no cursor or no time", i)
		}
		if i < 3 && x.Cursor != r1.Cursors[i] {
			return fmt.Errorf("event %d has the cursor %s; the append gave %s", i, x.Cursor, r1.Cursors[i])
		}
	}
	if string(all[3].Data) != "4" {
		return fmt.Errorf("the data of D is %s", all[3].Data)
	}
	// A page of 2 says that more events exist.
	page, err := e.c.Read(e.ctx, protocol.EventsReadParams{Stream: s, Limit: 2})
	if err != nil {
		return err
	}
	if len(page.Events) != 2 || !page.More || page.Cursor == nil || *page.Cursor != page.Events[1].Cursor {
		return fmt.Errorf("a read with limit 2 gave %d events, more %v", len(page.Events), page.More)
	}
	empty, err := e.c.Read(e.ctx, protocol.EventsReadParams{Stream: e.name("never")})
	if err != nil {
		return err
	}
	if len(empty.Events) != 0 || empty.More {
		return errors.New("a stream with no events is not empty")
	}
	return nil
}

func testEventsAfter(e *env) error {
	s := e.name("after")
	r, err := e.c.Append(e.ctx, s, ev("A", 1), ev("B", 2), ev("C", 3), ev("D", 4))
	if err != nil {
		return err
	}
	got, err := e.c.ReadAll(e.ctx, protocol.EventsReadParams{Stream: s, After: &r.Cursors[1]})
	if err != nil {
		return err
	}
	if typesOf(got) != "C,D" {
		return fmt.Errorf("the read after B gave %s; want C,D", typesOf(got))
	}
	end, err := e.c.Read(e.ctx, protocol.EventsReadParams{Stream: s, After: &r.Cursors[3]})
	if err != nil {
		return err
	}
	if len(end.Events) != 0 || end.More {
		return errors.New("the read after the last event is not empty")
	}
	return nil
}

func testEventsRange(e *env) error {
	s := e.name("range")
	base := time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC)
	var events []protocol.NewEvent
	for i, min := range []int{0, 5, 10} {
		at := base.Add(time.Duration(min) * time.Minute)
		events = append(events, protocol.NewEvent{Type: fmt.Sprintf("E%d", i), Data: raw(i), At: &at})
	}
	if _, err := e.c.Append(e.ctx, s, events...); err != nil {
		return err
	}
	from := base.Add(time.Minute)
	to := base.Add(9 * time.Minute)
	got, err := e.c.ReadAll(e.ctx, protocol.EventsReadParams{Stream: s, From: &from, To: &to})
	if err != nil {
		return err
	}
	if typesOf(got) != "E1" {
		return fmt.Errorf("the range 10:01 to 10:09 gave %s; want E1", typesOf(got))
	}
	if !got[0].At.Equal(base.Add(5 * time.Minute)) {
		return fmt.Errorf("the event time is %s; want 10:05", got[0].At)
	}
	// from is in the range, to is not.
	edge, err := e.c.ReadAll(e.ctx, protocol.EventsReadParams{Stream: s, From: &base, To: events[2].At})
	if err != nil {
		return err
	}
	if typesOf(edge) != "E0,E1" {
		return fmt.Errorf("the range 10:00 to 10:10 gave %s; want E0,E1", typesOf(edge))
	}
	return nil
}

func testContinuedRun(e *env) error {
	if !e.info.HasHook(protocol.HookContinueStream) {
		return skip("the plugin does not declare the continueStream test hook")
	}
	s := e.name("runs")
	r, err := e.c.Append(e.ctx, s, ev("A", 1), ev("B", 2))
	if err != nil {
		return err
	}
	if _, err := e.c.ContinueStream(e.ctx, s); err != nil {
		return err
	}
	if _, err := e.c.Append(e.ctx, s, ev("C", 3)); err != nil {
		return err
	}
	all, err := e.c.ReadAll(e.ctx, protocol.EventsReadParams{Stream: s})
	if err != nil {
		return err
	}
	if typesOf(all) != "A,B,C" {
		return fmt.Errorf("the read across runs gave %s; want A,B,C", typesOf(all))
	}
	after, err := e.c.ReadAll(e.ctx, protocol.EventsReadParams{Stream: s, After: &r.Cursors[1]})
	if err != nil {
		return fmt.Errorf("the cursor of B from the old run: %w", err)
	}
	if typesOf(after) != "C" {
		return fmt.Errorf("the read after B gave %s; want C", typesOf(after))
	}
	return nil
}

func testEventsDelete(e *env) error {
	s := e.name("gone")
	if _, err := e.c.Append(e.ctx, s, ev("A", 1)); err != nil {
		return err
	}
	deleted, err := e.c.DeleteStream(e.ctx, s)
	if err != nil {
		return err
	}
	if !deleted {
		return errors.New("the delete says that the stream had no events")
	}
	got, err := e.c.Read(e.ctx, protocol.EventsReadParams{Stream: s})
	if err != nil {
		return err
	}
	if len(got.Events) != 0 {
		return fmt.Errorf("the stream has %d events after the delete", len(got.Events))
	}
	return nil
}

func testEventsEmpty(e *env) error {
	return e.expectInvalid(func() error {
		_, err := e.c.Append(e.ctx, e.name("empty"))
		return expectCode(err, protocol.Refused)
	})
}

// ---- blobs ----

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	x := uint32(seed) + 1
	for i := range b {
		x = x*1664525 + 1013904223
		b[i] = byte(x >> 24)
	}
	return b
}

func shaOf(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func testBlobsRoundtrip(e *env) error {
	data := pattern(1<<20, 1)
	sha, err := e.c.PutBlob(e.ctx, data)
	if err != nil {
		return err
	}
	got, err := e.c.GetBlob(e.ctx, sha)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, data) {
		return fmt.Errorf("the blob has %d bytes and is not the same as the %d bytes put", len(got), len(data))
	}
	// A second put of the same blob says that it exists.
	var begin protocol.BlobsBeginResult
	if err := e.c.Call(e.ctx, protocol.MethodBlobsBegin, protocol.BlobsBeginParams{SHA256: sha, Size: int64(len(data))}, &begin); err != nil {
		return err
	}
	if !begin.Exists {
		return errors.New("blobs.begin of a stored blob does not say that it exists")
	}
	// An empty blob works too.
	esha, err := e.c.PutBlob(e.ctx, []byte{})
	if err != nil {
		return fmt.Errorf("an empty blob: %w", err)
	}
	if empty, err := e.c.GetBlob(e.ctx, esha); err != nil || len(empty) != 0 {
		return fmt.Errorf("the empty blob: %d bytes, %v", len(empty), err)
	}
	return nil
}

func testBlobsWrongHash(e *env) error {
	data := pattern(1000, 2)
	wrong := shaOf(pattern(1000, 3))
	err := e.c.PutBlobAs(e.ctx, wrong, data)
	if err := expectCode(err, protocol.Refused); err != nil {
		return err
	}
	present, err := e.c.HasBlobs(e.ctx, wrong, shaOf(data))
	if err != nil {
		return err
	}
	if len(present) != 0 {
		return fmt.Errorf("the plugin kept a blob with a wrong hash: %v", present)
	}
	return nil
}

func testBlobsHas(e *env) error {
	a := pattern(100, 4)
	b := pattern(100, 5)
	shaA, err := e.c.PutBlob(e.ctx, a)
	if err != nil {
		return err
	}
	present, err := e.c.HasBlobs(e.ctx, shaA, shaOf(b))
	if err != nil {
		return err
	}
	if len(present) != 1 || present[0] != shaA {
		return fmt.Errorf("has gave %v; want only %s", present, shaA)
	}
	deleted, err := e.c.DeleteBlob(e.ctx, shaA)
	if err != nil || !deleted {
		return fmt.Errorf("the delete: %v, %v", deleted, err)
	}
	if present, _ := e.c.HasBlobs(e.ctx, shaA); len(present) != 0 {
		return errors.New("the blob exists after the delete")
	}
	return nil
}

func testBlobsLarge(e *env) error {
	data := pattern(20<<20, 6)
	sha, err := e.c.PutBlob(e.ctx, data)
	if err != nil {
		return err
	}
	got, err := e.c.GetBlob(e.ctx, sha)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, data) {
		return errors.New("the 20 MB blob is not the same after a get")
	}
	if largest := e.largest.Load(); largest > int64(e.info.Limits.MaxMessageBytes) {
		return fmt.Errorf("a message had %d bytes; maxMessageBytes is %d", largest, e.info.Limits.MaxMessageBytes)
	}
	// A chunk larger than the limit is refused.
	var begin protocol.BlobsBeginResult
	big := pattern(e.info.Limits.MaxChunkBytes+1, 7)
	if err := e.c.Call(e.ctx, protocol.MethodBlobsBegin, protocol.BlobsBeginParams{SHA256: shaOf(big), Size: int64(len(big))}, &begin); err != nil {
		return err
	}
	if begin.Exists {
		return nil
	}
	err = e.c.Call(e.ctx, protocol.MethodBlobsWrite, protocol.BlobsWriteParams{UploadID: begin.UploadID, Data: big}, nil)
	var pe *protocol.Error
	if err == nil || !errors.As(err, &pe) {
		return fmt.Errorf("a chunk larger than maxChunkBytes: got %v; want an error", err)
	}
	return nil
}

func testBlobsMissing(e *env) error {
	_, err := e.c.GetBlob(e.ctx, shaOf([]byte("conformance-missing-"+e.ns)))
	return expectCode(err, protocol.NotFound)
}

// ---- watch ----

func testWatchChange(e *env) error {
	col := e.name("watch")
	v1, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw(1)})
	if err != nil {
		return err
	}
	type out struct {
		r   protocol.WatchWaitResult
		err error
		at  time.Time
	}
	done := make(chan out, 1)
	go func() {
		r, err := e.c.Wait(e.ctx, protocol.WatchWaitParams{Collection: col, Key: "abc", Version: &v1.Version, TimeoutMs: 20000})
		done <- out{r, err, time.Now()}
	}()
	time.Sleep(300 * time.Millisecond)
	select {
	case o := <-done:
		return fmt.Errorf("the wait ended before the change: %+v %v", o.r, o.err)
	default:
	}
	wrote := time.Now()
	v2, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw(2), ExpectedVersion: &v1.Version})
	if err != nil {
		return err
	}
	select {
	case o := <-done:
		if o.err != nil {
			return o.err
		}
		if !o.r.Changed || o.r.Version == nil || *o.r.Version != v2.Version {
			return fmt.Errorf("the wait gave %+v; want changed with %s", o.r, v2.Version)
		}
		if d := o.at.Sub(wrote); d > time.Second {
			return fmt.Errorf("the wait ended %s after the write; want less than 1 s", d)
		}
	case <-time.After(5 * time.Second):
		return errors.New("the wait did not end within 5 s of the write")
	}
	return nil
}

func testWatchTimeout(e *env) error {
	col := e.name("watch")
	v, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "quiet", Value: raw(1)})
	if err != nil {
		return err
	}
	start := time.Now()
	r, err := e.c.Wait(e.ctx, protocol.WatchWaitParams{Collection: col, Key: "quiet", Version: &v.Version, TimeoutMs: 2000})
	if err != nil {
		return err
	}
	d := time.Since(start)
	if r.Changed {
		return fmt.Errorf("the wait says that the document changed: %+v", r)
	}
	if d < 1500*time.Millisecond || d > 3*time.Second {
		return fmt.Errorf("the wait of 2 s took %s", d.Round(time.Millisecond))
	}
	return nil
}

func testWatchNewer(e *env) error {
	col := e.name("watch")
	v1, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "newer", Value: raw(1)})
	if err != nil {
		return err
	}
	v2, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "newer", Value: raw(2), ExpectedVersion: &v1.Version})
	if err != nil {
		return err
	}
	start := time.Now()
	r, err := e.c.Wait(e.ctx, protocol.WatchWaitParams{Collection: col, Key: "newer", Version: &v1.Version, TimeoutMs: 10000})
	if err != nil {
		return err
	}
	if !r.Changed || r.Version == nil || *r.Version != v2.Version {
		return fmt.Errorf("the wait gave %+v; want changed with %s", r, v2.Version)
	}
	if time.Since(start) > time.Second {
		return errors.New("the wait did not end at once")
	}
	// A host with no document waits for its first write; a missing one stays missing.
	r, err = e.c.Wait(e.ctx, protocol.WatchWaitParams{Collection: col, Key: "newer", Version: nil, TimeoutMs: 10000})
	if err != nil || !r.Changed {
		return fmt.Errorf("a wait with no version on a document that exists: %+v %v", r, err)
	}
	return nil
}

func testWatchStream(e *env) error {
	s := e.name("wstream")
	r, err := e.c.Append(e.ctx, s, ev("A", 1))
	if err != nil {
		return err
	}
	done := make(chan protocol.WatchWaitResult, 1)
	errc := make(chan error, 1)
	go func() {
		w, err := e.c.Wait(e.ctx, protocol.WatchWaitParams{Stream: s, After: &r.Cursors[0], TimeoutMs: 20000})
		if err != nil {
			errc <- err
			return
		}
		done <- w
	}()
	time.Sleep(300 * time.Millisecond)
	r2, err := e.c.Append(e.ctx, s, ev("B", 2))
	if err != nil {
		return err
	}
	select {
	case w := <-done:
		if !w.Changed || w.Cursor == nil || *w.Cursor != r2.Cursors[0] {
			return fmt.Errorf("the wait gave %+v; want changed with %s", w, r2.Cursors[0])
		}
	case err := <-errc:
		return err
	case <-time.After(5 * time.Second):
		return errors.New("the wait on the stream did not end within 5 s of the append")
	}
	return nil
}

// ---- restart and shutdown ----

func testRestart(e *env) error {
	if !e.stdio() {
		return skip("stdio only: the runner cannot restart an HTTP plugin")
	}
	col := e.name("restart")
	v, err := e.c.Put(e.ctx, protocol.StatePutParams{Collection: col, Key: "abc", Value: raw("kept")})
	if err != nil {
		return err
	}
	if err := e.stop(); err != nil {
		return err
	}
	if err := e.connect(); err != nil {
		return err
	}
	if _, err := e.c.Handshake(e.ctx, e.hello()); err != nil {
		return fmt.Errorf("the handshake after the restart: %w", err)
	}
	if _, err := e.c.Ping(e.ctx); err != nil {
		return err
	}
	if !e.info.Durable {
		return skip("the plugin restarts, but it is not durable: no data to read back")
	}
	got, err := e.c.Get(e.ctx, col, "abc")
	if err != nil {
		return err
	}
	if !got.Exists || got.Version != v.Version || string(got.Value) != `"kept"` {
		return fmt.Errorf("after the restart the document is %+v; want version %s", got, v.Version)
	}
	return nil
}

// stop shuts the stdio plugin down and checks that it exits with code 0.
func (e *env) stop() error {
	if err := e.c.Shutdown(e.ctx, 5*time.Second); err != nil {
		return fmt.Errorf("plugin.shutdown: %w", err)
	}
	select {
	case <-e.proc.Exited():
	case <-time.After(6 * time.Second):
		e.proc.Kill()
		return errors.New("the plugin did not exit within 5 s of plugin.shutdown")
	}
	if code := e.proc.ExitCode(); code != 0 {
		return fmt.Errorf("the plugin exited with code %d; want 0", code)
	}
	return nil
}

func testShutdown(e *env) error {
	if e.stdio() {
		return e.stop()
	}
	if err := e.c.Shutdown(e.ctx, 5*time.Second); err != nil {
		return fmt.Errorf("plugin.shutdown: %w", err)
	}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := e.timeout(time.Second)
		_, err := e.c.Ping(ctx)
		cancel()
		if err != nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("the plugin still answers 6 s after plugin.shutdown")
}

func contains[T comparable](list []T, v T) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

var _ = client.Sent
