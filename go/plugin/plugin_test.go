package plugin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/client"
	"github.com/spec-studio-app/plugin-sdk/go/plugin"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// fakeStore is a state-only store for the SDK tests.
type fakeStore struct {
	mu      sync.Mutex
	docs    map[string]protocol.StateGetResult
	n       int
	down    bool
	putWait time.Duration
	// big makes Get return a value of this many bytes.
	big     int
	opened  map[string]string
	closed  bool
	openErr error
}

func newFake() *fakeStore { return &fakeStore{docs: map[string]protocol.StateGetResult{}} }

func (f *fakeStore) Open(ctx context.Context, o plugin.OpenOptions) error {
	o.Log.Info("open", "settings", o.Settings)
	f.opened = o.Settings
	return f.openErr
}
func (f *fakeStore) Close(ctx context.Context) error { f.closed = true; return nil }
func (f *fakeStore) Get(ctx context.Context, p protocol.StateGetParams) (protocol.StateGetResult, error) {
	if f.down {
		return protocol.StateGetResult{}, protocol.NewError(protocol.Unavailable, "The fake storage does not answer. Start it again.")
	}
	if f.big > 0 {
		now := time.Now()
		return protocol.StateGetResult{Exists: true, Value: json.RawMessage(`"` + strings.Repeat("x", f.big) + `"`), Version: "v", UpdatedAt: &now}, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.docs[p.Collection+"/"+p.Key], nil
}
func (f *fakeStore) Put(ctx context.Context, p protocol.StatePutParams) (protocol.StatePutResult, error) {
	time.Sleep(f.putWait)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	v := string(rune('a' + f.n))
	now := time.Now().UTC()
	f.docs[p.Collection+"/"+p.Key] = protocol.StateGetResult{Exists: true, Value: p.Value, Attributes: p.Attributes, Version: v, UpdatedAt: &now}
	return protocol.StatePutResult{Version: v, UpdatedAt: now}, nil
}
func (f *fakeStore) Delete(ctx context.Context, p protocol.StateDeleteParams) (protocol.StateDeleteResult, error) {
	return protocol.StateDeleteResult{Deleted: true}, nil
}
func (f *fakeStore) List(ctx context.Context, p protocol.StateListParams) (protocol.StateListResult, error) {
	return protocol.StateListResult{}, nil
}

var testInfo = protocol.PluginInfo{Name: "fake", Version: "1.2.3", Repo: "https://example.com/fake"}

type harness struct {
	c      *client.Client
	stderr *syncBuffer
	done   chan error
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// start serves p over an in-process stdio pipe.
func start(t *testing.T, p plugin.Plugin) *harness {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	h := &harness{stderr: &syncBuffer{}, done: make(chan error, 1)}
	go func() {
		err := plugin.Run(context.Background(), p, plugin.Options{Stdin: inR, Stdout: outW, Stderr: h.stderr})
		outW.Close()
		h.done <- err
	}()
	h.c = client.Connect(outR, inW)
	t.Cleanup(func() { inW.Close() })
	return h
}

func hello(settings map[string]string, versions ...string) protocol.HandshakeParams {
	if len(versions) == 0 {
		versions = []string{"1.0"}
	}
	return protocol.HandshakeParams{Host: protocol.Host{Kind: "relay", Version: "0.6.0"}, ProtocolVersions: versions, Settings: settings}
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

// R1.S1
func TestHandshake(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newFake()})
	r, err := h.c.Handshake(ctx(t), hello(nil))
	if err != nil {
		t.Fatal(err)
	}
	if r.Plugin != testInfo || r.ProtocolVersion != "1.0" {
		t.Fatalf("result %+v", r)
	}
	if len(r.Capabilities) != 1 || r.Capabilities[0] != protocol.CapState {
		t.Fatalf("capabilities %v, want [state]", r.Capabilities)
	}
	if r.Limits != protocol.DefaultLimits() {
		t.Fatalf("limits %+v", r.Limits)
	}
}

// R1.S2
func TestHandshakeNoSharedMajor(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newFake()})
	_, err := h.c.Handshake(ctx(t), hello(nil, "2.0"))
	if !protocol.Is(err, protocol.Refused) {
		t.Fatalf("err %v, want refused", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "2.0") || !strings.Contains(msg, "1.0") {
		t.Fatalf("message %q does not name both versions", msg)
	}
	// The connection stays usable.
	if _, err := h.c.Handshake(ctx(t), hello(nil)); err != nil {
		t.Fatal(err)
	}
}

// R1.S3
func TestCallBeforeHandshake(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newFake()})
	_, err := h.c.Get(ctx(t), "specs", "abc")
	if !protocol.Is(err, protocol.Refused) || !strings.Contains(err.Error(), "plugin.handshake first") {
		t.Fatalf("err %v", err)
	}
}

// R1.S4
func TestHandshakeNewerMinor(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newFake()})
	r, err := h.c.Handshake(ctx(t), hello(nil, "1.3"))
	if err != nil {
		t.Fatal(err)
	}
	if r.ProtocolVersion != "1.0" {
		t.Fatalf("chose %s, want 1.0", r.ProtocolVersion)
	}
}

func TestChooseVersion(t *testing.T) {
	cases := []struct {
		host, plugin []string
		want         string
		ok           bool
	}{
		{[]string{"1.0"}, []string{"1.0"}, "1.0", true},
		{[]string{"1.3"}, []string{"1.0"}, "1.0", true},
		{[]string{"1.1"}, []string{"1.4"}, "1.1", true},
		{[]string{"1.2", "2.1"}, []string{"1.5", "2.0"}, "2.0", true},
		{[]string{"2.0"}, []string{"1.0"}, "", false},
		{[]string{"x"}, []string{"1.0"}, "", false},
	}
	for _, c := range cases {
		got, ok := protocol.ChooseVersion(c.host, c.plugin)
		if got != c.want || ok != c.ok {
			t.Errorf("ChooseVersion(%v, %v) = %q, %v; want %q, %v", c.host, c.plugin, got, ok, c.want, c.ok)
		}
	}
}

func TestHandshakeTwice(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newFake()})
	if _, err := h.c.Handshake(ctx(t), hello(nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.c.Handshake(ctx(t), hello(nil)); !protocol.Is(err, protocol.Refused) {
		t.Fatalf("err %v, want refused", err)
	}
}

var settings = []protocol.Setting{
	{Name: "address", Description: "The storage address, host:port.", Required: true},
	{Name: "api_key", Description: "The API key.", Secret: true},
}

// R2.S1
func TestDescribeSettings(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Settings: settings, Store: newFake()})
	d, err := h.c.Describe(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Settings) != 2 || d.Settings[1].Name != "api_key" || !d.Settings[1].Secret || d.Settings[1].Description == "" || !d.Settings[0].Required {
		t.Fatalf("settings %+v", d.Settings)
	}
}

// R2.S2
func TestMissingRequiredSetting(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Settings: settings, Store: newFake()})
	_, err := h.c.Handshake(ctx(t), hello(map[string]string{"api_key": "k"}))
	if !protocol.Is(err, protocol.BadSettings) || !strings.Contains(err.Error(), "address") {
		t.Fatalf("err %v", err)
	}
}

func TestUnknownSetting(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Settings: settings, Store: newFake()})
	_, err := h.c.Handshake(ctx(t), hello(map[string]string{"address": "a:1", "colour": "red"}))
	if !protocol.Is(err, protocol.BadSettings) || !strings.Contains(err.Error(), "colour") {
		t.Fatalf("err %v", err)
	}
}

// R2.S3
func TestSecretStaysOutOfLogsAndErrors(t *testing.T) {
	const canary = "canary-7d1f0c2e9b"
	f := newFake()
	f.openErr = protocol.NewError(protocol.BadSettings, "The key %s is wrong.", canary)
	h := start(t, plugin.Plugin{Info: testInfo, Settings: settings, Store: f})
	_, err := h.c.Handshake(ctx(t), hello(map[string]string{"address": "a:1", "api_key": canary}))
	if err == nil {
		t.Fatal("handshake succeeded")
	}
	if strings.Contains(err.Error(), canary) {
		t.Fatalf("the error shows the secret: %v", err)
	}
	// An unknown setting fails too, with the canary in the values.
	_, err = h.c.Handshake(ctx(t), hello(map[string]string{"address": "a:1", "api_key": canary, "x": canary}))
	if err == nil || strings.Contains(err.Error(), canary) {
		t.Fatalf("err %v", err)
	}
	// The store's log line has the settings: the secret must be redacted.
	f.openErr = nil
	if _, err := h.c.Handshake(ctx(t), hello(map[string]string{"address": "a:1", "api_key": canary})); err != nil {
		t.Fatal(err)
	}
	h.c.Shutdown(ctx(t), time.Second)
	<-h.done
	if log := h.stderr.String(); strings.Contains(log, canary) || !strings.Contains(log, "[secret]") {
		t.Fatalf("the log shows the secret or no redaction:\n%s", log)
	}
}

// R7.S1
func TestUnknownMethod(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newFake()})
	h.c.Handshake(ctx(t), hello(nil))
	err := h.c.Call(ctx(t), "state.frobnicate", nil, nil)
	var e *protocol.Error
	if !asErr(err, &e) || e.RPC != protocol.RPCMethodNotFound {
		t.Fatalf("err %v", err)
	}
}

// R7.S2, R10.S2
func TestStateOnlyRefusesOtherCapabilities(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newFake()})
	r, _ := h.c.Handshake(ctx(t), hello(nil))
	if len(r.Capabilities) != 1 {
		t.Fatalf("capabilities %v", r.Capabilities)
	}
	for _, m := range []string{"blobs.get", "events.read", "watch.wait"} {
		err := h.c.Call(ctx(t), m, map[string]any{}, nil)
		if !protocol.Is(err, protocol.Refused) {
			t.Fatalf("%s: err %v, want refused", m, err)
		}
	}
	_, err := h.c.HasBlobs(ctx(t))
	if !strings.Contains(err.Error(), "blobs") {
		t.Fatalf("the message does not name the capability: %v", err)
	}
}

// R7.S3
func TestStoreDown(t *testing.T) {
	f := newFake()
	f.down = true
	h := start(t, plugin.Plugin{Info: testInfo, Store: f})
	h.c.Handshake(ctx(t), hello(nil))
	_, err := h.c.Get(ctx(t), "specs", "abc")
	if !protocol.Is(err, protocol.Unavailable) || !strings.Contains(err.Error(), "Start it again") {
		t.Fatalf("err %v", err)
	}
}

func TestBadName(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newFake()})
	h.c.Handshake(ctx(t), hello(nil))
	for _, name := range []string{"../etc", "a/b", "..", ".", "", "x\n"} {
		if _, err := h.c.Get(ctx(t), "specs", name); !protocol.Is(err, protocol.Refused) {
			t.Fatalf("key %q: err %v, want refused", name, err)
		}
	}
}

func TestParseErrorKeepsConnection(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newFake()})
	raw, err := h.c.SendLine(ctx(t), []byte("{not json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "-32700") {
		t.Fatalf("answer %s", raw)
	}
	if _, err := h.c.Handshake(ctx(t), hello(nil)); err != nil {
		t.Fatal(err)
	}
}

// R9.S1, R9.S2
func TestShutdownWaitsForAWrite(t *testing.T) {
	f := newFake()
	f.putWait = 300 * time.Millisecond
	h := start(t, plugin.Plugin{Info: testInfo, Store: f})
	h.c.Handshake(ctx(t), hello(nil))
	if p, err := h.c.Ping(ctx(t)); err != nil || p.Status != "ok" {
		t.Fatalf("ping %+v %v", p, err)
	}
	putDone := make(chan error, 1)
	go func() {
		_, err := h.c.Put(ctx(t), protocol.StatePutParams{Collection: "specs", Key: "abc", Value: json.RawMessage(`1`)})
		putDone <- err
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	if err := h.c.Shutdown(ctx(t), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := <-putDone; err != nil {
		t.Fatalf("the write failed: %v", err)
	}
	if err := <-h.done; err != nil {
		t.Fatal(err)
	}
	if !f.closed {
		t.Fatal("the store was not closed")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the shutdown took too long")
	}
}

// R8.S2, R8.S3
func TestHTTPToken(t *testing.T) {
	p := plugin.Plugin{Info: testInfo, Store: newFake()}
	s := plugin.NewServer(&p, io.Discard)
	srv := httptest.NewServer(s.Handler("tok", nil))
	defer srv.Close()

	for _, auth := range []string{"", "Bearer wrong"} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"plugin.ping"}`))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("auth %q: status %d, want 401", auth, resp.StatusCode)
		}
	}

	c := client.Dial(srv.URL+"/rpc", "tok")
	if _, err := c.Get(ctx(t), "specs", "a"); !protocol.Is(err, protocol.Refused) {
		t.Fatalf("before the handshake: %v", err)
	}
	r, err := c.Handshake(ctx(t), hello(nil))
	if err != nil || r.Session == "" {
		t.Fatalf("handshake %+v %v", r, err)
	}
	if _, err := c.Put(ctx(t), protocol.StatePutParams{Collection: "specs", Key: "a", Value: json.RawMessage(`{"x":1}`)}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx(t), "specs", "a")
	if err != nil || !got.Exists {
		t.Fatalf("get %+v %v", got, err)
	}
}

// A later HTTP handshake with other values for declared settings is refused.
func TestHTTPOtherSettings(t *testing.T) {
	p := plugin.Plugin{Info: testInfo, Settings: []protocol.Setting{{Name: "label", Description: "A label."}}, Store: newFake()}
	s := plugin.NewServer(&p, io.Discard)
	srv := httptest.NewServer(s.Handler("tok", nil))
	defer srv.Close()
	if _, err := client.Dial(srv.URL+"/rpc", "tok").Handshake(ctx(t), hello(map[string]string{"label": "a"})); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Dial(srv.URL+"/rpc", "tok").Handshake(ctx(t), hello(map[string]string{"label": "a"})); err != nil {
		t.Fatalf("the same settings: %v", err)
	}
	_, err := client.Dial(srv.URL+"/rpc", "tok").Handshake(ctx(t), hello(map[string]string{"label": "b"}))
	if !protocol.Is(err, protocol.BadSettings) || !strings.Contains(err.Error(), "other settings") {
		t.Fatalf("other settings: %v", err)
	}
}

// A browser page cannot reach a loopback plugin (DNS rebinding, simple POSTs).
func TestHTTPRefusesBrowserRequests(t *testing.T) {
	p := plugin.Plugin{Info: testInfo, Store: newFake()}
	s := plugin.NewServer(&p, io.Discard)
	srv := httptest.NewServer(s.Handler("", nil))
	defer srv.Close()
	body := `{"jsonrpc":"2.0","id":1,"method":"plugin.describe"}`
	cases := []struct {
		name   string
		header map[string]string
		host   string
		want   int
	}{
		{"origin", map[string]string{"Content-Type": "application/json", "Origin": "http://evil.example"}, "", http.StatusForbidden},
		{"text/plain", map[string]string{"Content-Type": "text/plain"}, "", http.StatusUnsupportedMediaType},
		{"rebound host", map[string]string{"Content-Type": "application/json"}, "evil.example:7070", http.StatusForbidden},
		{"loopback", map[string]string{"Content-Type": "application/json"}, "", http.StatusOK},
		{"localhost", map[string]string{"Content-Type": "application/json"}, "localhost:7070", http.StatusOK},
	}
	for _, c := range cases {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/rpc", strings.NewReader(body))
		for k, v := range c.header {
			req.Header.Set(k, v)
		}
		if c.host != "" {
			req.Host = c.host
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s: status %d, want %d", c.name, resp.StatusCode, c.want)
		}
	}
}

// A secret with a quote or a backslash is redacted in its escaped forms too.
func TestEscapedSecretIsRedacted(t *testing.T) {
	const secret = `pa"ss\word`
	f := newFake()
	f.openErr = protocol.NewError(protocol.BadSettings, "The key %q is wrong.", secret)
	h := start(t, plugin.Plugin{Info: testInfo, Settings: settings, Store: f})
	_, err := h.c.Handshake(ctx(t), hello(map[string]string{"address": "a:1", "api_key": secret}))
	if err == nil || strings.Contains(err.Error(), `pa\"ss`) || strings.Contains(err.Error(), "word") {
		t.Fatalf("the error shows the secret: %v", err)
	}
	f.openErr = nil
	if _, err := h.c.Handshake(ctx(t), hello(map[string]string{"address": "a:1", "api_key": secret})); err != nil {
		t.Fatal(err)
	}
	h.c.Shutdown(ctx(t), time.Second)
	<-h.done
	if log := h.stderr.String(); strings.Contains(log, "word") {
		t.Fatalf("the log shows the secret:\n%s", log)
	}
}

// An oversized line gets an answer with its own id, so the host call ends.
func TestOversizedLineKeepsTheID(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newFake(), Limits: protocol.Limits{MaxMessageBytes: 1000}})
	h.c.Handshake(ctx(t), hello(nil))
	_, err := h.c.Put(ctx(t), protocol.StatePutParams{Collection: "c", Key: "k", Value: json.RawMessage(`"` + strings.Repeat("x", 2000) + `"`)})
	var e *protocol.Error
	if !asErr(err, &e) || e.RPC != protocol.RPCInvalidRequest {
		t.Fatalf("err %v", err)
	}
}

// An answer over the limit becomes a refused error.
func TestLargeAnswerIsRefused(t *testing.T) {
	f := newFake()
	f.big = 2000
	h := start(t, plugin.Plugin{Info: testInfo, Store: f, Limits: protocol.Limits{MaxMessageBytes: 1000}})
	h.c.Handshake(ctx(t), hello(nil))
	_, err := h.c.Get(ctx(t), "c", "k")
	if !protocol.Is(err, protocol.Refused) || !strings.Contains(err.Error(), "smaller") {
		t.Fatalf("err %v", err)
	}
}

// A document too large for a read to return is refused at the write.
func TestDocumentTooLargeToRead(t *testing.T) {
	h := start(t, plugin.Plugin{Info: testInfo, Store: newFake(), Limits: protocol.Limits{MaxMessageBytes: 20000}})
	h.c.Handshake(ctx(t), hello(nil))
	_, err := h.c.Put(ctx(t), protocol.StatePutParams{Collection: "c", Key: "k", Value: json.RawMessage(`"` + strings.Repeat("x", 19000) + `"`)})
	if !protocol.Is(err, protocol.Refused) || !strings.Contains(err.Error(), "blob") {
		t.Fatalf("err %v", err)
	}
	// A value with < > & reads back at its own size: no HTML escapes.
	v := json.RawMessage(`"` + strings.Repeat("<", 2000) + `"`)
	if _, err := h.c.Put(ctx(t), protocol.StatePutParams{Collection: "c", Key: "k", Value: v}); err != nil {
		t.Fatal(err)
	}
	got, err := h.c.Get(ctx(t), "c", "k")
	var back string
	if err != nil || json.Unmarshal(got.Value, &back) != nil || back != strings.Repeat("<", 2000) {
		t.Fatalf("get: %v", err)
	}
}

// R9.S2: a stuck call does not stop the shutdown answer or the exit.
func TestShutdownWithAStuckCall(t *testing.T) {
	f := newFake()
	f.putWait = time.Hour
	h := start(t, plugin.Plugin{Info: testInfo, Store: f})
	h.c.Handshake(ctx(t), hello(nil))
	go h.c.Put(context.Background(), protocol.StatePutParams{Collection: "c", Key: "k", Value: json.RawMessage(`1`)})
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	if err := h.c.Shutdown(ctx(t), 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.done:
	case <-time.After(3 * time.Second):
		t.Fatal("the plugin did not stop")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the shutdown took %s", d)
	}
}

// A signal (ctx) stops a stdio plugin while the host keeps stdin open.
func TestSignalStopsStdio(t *testing.T) {
	inR, inW := io.Pipe()
	defer inW.Close()
	c, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- plugin.Run(c, plugin.Plugin{Info: testInfo, Store: newFake()}, plugin.Options{Stdin: inR, Stdout: io.Discard, Stderr: io.Discard})
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the signal")
	}
}

func TestBadSettingName(t *testing.T) {
	err := plugin.Run(context.Background(), plugin.Plugin{Info: testInfo, Settings: []protocol.Setting{{Name: "apiKey"}}, Store: newFake()}, plugin.Options{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "apiKey") {
		t.Fatalf("err %v", err)
	}
}

func TestHTTPNeedsTokenOffLoopback(t *testing.T) {
	p := plugin.Plugin{Info: testInfo, Store: newFake()}
	s := plugin.NewServer(&p, io.Discard)
	err := s.ServeHTTP(context.Background(), "0.0.0.0:0", "")
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("err %v", err)
	}
	// Loopback with no token works.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	c, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.ServeHTTP(c, addr, "") }()
	time.Sleep(100 * time.Millisecond)
	cl := client.Dial("http://"+addr+"/rpc", "")
	if _, err := cl.Describe(ctx(t)); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-done
}

func asErr(err error, target **protocol.Error) bool {
	e, ok := err.(*protocol.Error)
	if ok {
		*target = e
	}
	return ok
}
